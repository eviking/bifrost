//! HTTP bridge exposing `datafusion-loki` over a small JSON API.
//!
//! Grafana's Go plugin SDK has no way to embed a Rust `TableProvider`
//! directly, so this process is the real DataFusion execution boundary: it
//! registers a `LokiTableProvider` against a running Loki instance, accepts
//! arbitrary SQL over HTTP, executes it through DataFusion's query engine
//! (predicate/limit pushdown into LogQL happens inside `datafusion-loki`
//! exactly as it would for any other caller), and serializes the resulting
//! Arrow `RecordBatch`es into a simple `{columns, rows}` JSON shape that the
//! Grafana backend plugin reshapes into a Grafana `DataFrame`.

use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::Array;
use arrow::datatypes::DataType;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use axum::routing::{get, post};
use axum::Router;
use datafusion::prelude::SessionContext;
use datafusion_loki::{LokiConfig, LokiTableProvider};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

#[derive(Clone)]
struct AppState {
    ctx: Arc<SessionContext>,
}

#[derive(Deserialize)]
struct QueryRequest {
    sql: String,
}

#[derive(Serialize)]
struct QueryResponse {
    columns: Vec<ColumnMeta>,
    rows: Vec<Vec<Value>>,
}

#[derive(Serialize)]
struct ColumnMeta {
    name: String,
    #[serde(rename = "type")]
    ty: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let loki_url = env::var("LOKI_URL").unwrap_or_else(|_| "http://localhost:3100".to_string());
    let stream_selector = env::var("LOKI_STREAM_SELECTOR").unwrap_or_else(|_| r#"{job="myapp"}"#.to_string());
    let bind_addr: SocketAddr = env::var("BRIDGE_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8090".to_string())
        .parse()
        .expect("BRIDGE_ADDR must be a valid socket address");

    tracing::info!(%loki_url, %stream_selector, "starting bifrost-bridge");

    let config = LokiConfig::new(&loki_url, &stream_selector)
        .with_query_limit(5000)
        .with_timeout(Duration::from_secs(20));

    let labels = vec![
        "job".to_string(),
        "level".to_string(),
        "env".to_string(),
        "pod".to_string(),
    ];
    let provider = LokiTableProvider::new(config, labels);

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(provider))
        .expect("failed to register logs table");

    let state = AppState { ctx: Arc::new(ctx) };

    let app = Router::new()
        .route("/health", get(health))
        .route("/query", post(run_query))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    tracing::info!(%bind_addr, "listening");
    let listener = tokio::net::TcpListener::bind(bind_addr).await.expect("failed to bind");
    axum::serve(listener, app).await.expect("server error");
}

async fn health() -> &'static str {
    "ok"
}

async fn run_query(State(state): State<AppState>, Json(req): Json<QueryRequest>) -> impl IntoResponse {
    match execute(&state, &req.sql).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(err) => {
            tracing::error!(error = %err, sql = %req.sql, "query failed");
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse { error: err }),
            )
                .into_response()
        }
    }
}

async fn execute(state: &AppState, sql: &str) -> Result<QueryResponse, String> {
    let df = state.ctx.sql(sql).await.map_err(|e| e.to_string())?;
    let schema = df.schema().clone();
    let batches = df.collect().await.map_err(|e| e.to_string())?;

    let columns: Vec<ColumnMeta> = schema
        .fields()
        .iter()
        .map(|f| ColumnMeta {
            name: f.name().clone(),
            ty: arrow_type_name(f.data_type()),
        })
        .collect();

    let mut rows: Vec<Vec<Value>> = Vec::new();
    for batch in &batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                row.push(cell_to_json(array.as_ref(), row_idx));
            }
            rows.push(row);
        }
    }

    Ok(QueryResponse { columns, rows })
}

fn arrow_type_name(dt: &DataType) -> String {
    match dt {
        DataType::Utf8 | DataType::LargeUtf8 => "string".to_string(),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => "number".to_string(),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "number".to_string(),
        DataType::Timestamp(_, _) => "time".to_string(),
        DataType::Boolean => "bool".to_string(),
        other => format!("{other:?}"),
    }
}

/// Converts a single Arrow array cell to a `serde_json::Value`, using
/// `arrow::util` display formatting for anything not worth hand-rolling
/// (timestamps get ISO8601-ish string output via `arrow_cast::pretty`-style
/// scalar access instead, to keep them Grafana-friendly and unambiguous).
fn cell_to_json(array: &dyn Array, row_idx: usize) -> Value {
    use arrow::array::*;
    use arrow::datatypes::TimeUnit;

    if array.is_null(row_idx) {
        return Value::Null;
    }

    match array.data_type() {
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(a.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Value::String(a.value(row_idx).to_string())
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::from(a.value(row_idx))
        }
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Value::from(a.value(row_idx))
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Value::from(a.value(row_idx))
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Value::from(a.value(row_idx))
        }
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(a.value(row_idx))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
            let ns = a.value(row_idx);
            // Grafana's JSON/time-series ingestion expects epoch milliseconds
            // for time-typed fields.
            Value::from(ns.div_euclid(1_000_000))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            Value::from(a.value(row_idx).div_euclid(1_000))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
            Value::from(a.value(row_idx))
        }
        _ => Value::String(arrow::util::display::array_value_to_string(array, row_idx).unwrap_or_default()),
    }
}
