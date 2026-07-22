//! Demo: the `Map<Utf8, Utf8>` label schema mode, plus a GROUP BY aggregation
//! computed by DataFusion on top of scanned Loki rows.
//!
//! ```sh
//! cargo run --example demo_map_and_agg
//! ```

use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use datafusion_loki::{LokiConfig, LokiTableProvider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base_url = std::env::var("LOKI_URL").unwrap_or_else(|_| "http://localhost:3100".to_string());

    let config = LokiConfig::new(&base_url, r#"{job="myapp"}"#)
        .with_query_limit(1000)
        .with_timeout(Duration::from_secs(20));

    // --- Map label schema: no schema discovery call needed, arbitrary labels supported ---
    let map_provider = LokiTableProvider::new_with_map_labels(config.clone());
    let ctx = SessionContext::new();
    ctx.register_table("logs_map", Arc::new(map_provider))?;

    println!("=== Map label schema: labels['level'], labels['pod'] ===");
    let df = ctx
        .sql(
            r#"
            SELECT line, labels['level'] AS level, labels['pod'] AS pod
            FROM logs_map
            WHERE labels['level'] = 'error'
            LIMIT 5
            "#,
        )
        .await?;
    df.show().await?;

    // --- Flattened schema + GROUP BY aggregation (computed by DataFusion, not Loki) ---
    let flat_provider = LokiTableProvider::new(
        config,
        vec!["job".into(), "level".into(), "env".into(), "pod".into()],
    );
    ctx.register_table("logs", Arc::new(flat_provider))?;

    println!("\n=== GROUP BY level, env (DataFusion aggregation over pushed-down scan) ===");
    let df = ctx
        .sql(
            r#"
            SELECT level, env, COUNT(*) AS n
            FROM logs
            GROUP BY level, env
            ORDER BY level, env
            "#,
        )
        .await?;
    df.show().await?;

    Ok(())
}
