use std::sync::Arc;

use arrow::array::Array;
use datafusion::prelude::SessionContext;
use datafusion_loki::{LokiConfig, LokiTableProvider};
use serde_json::json;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn sample_streams_response() -> serde_json::Value {
    json!({
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [
                {
                    "stream": {"job": "myapp", "level": "error"},
                    "values": [
                        ["1700000000000000000", "panic: nil pointer dereference"],
                        ["1700000001000000000", "connection reset by peer"]
                    ]
                },
                {
                    "stream": {"job": "myapp", "level": "info"},
                    "values": [
                        ["1700000002000000000", "request completed in 12ms"]
                    ]
                }
            ]
        }
    })
}

#[tokio::test]
async fn queries_loki_and_returns_arrow_rows() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .and(query_param("direction", "forward"))
        .respond_with(ResponseTemplate::new(200).set_body_json(sample_streams_response()))
        .mount(&server)
        .await;

    let config = LokiConfig::new(server.uri(), r#"{job="myapp"}"#);
    let provider = LokiTableProvider::new(config, vec!["job".to_string(), "level".to_string()]);

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(provider)).unwrap();

    let df = ctx.sql("SELECT line, level FROM logs ORDER BY line").await.unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

/// Grafana Cloud Loki authenticates via HTTP Basic Auth (stack instance ID as
/// username, access-policy token as password) rather than a bearer token or
/// X-Scope-OrgID — this confirms `with_basic_auth` actually sends a correct
/// `Authorization: Basic <base64(id:token)>` header on every request.
#[tokio::test]
async fn sends_basic_auth_header_for_grafana_cloud_style_credentials() {
    let server = MockServer::start().await;

    // base64("123456:glc_test-token")
    let expected_header = "Basic MTIzNDU2OmdsY190ZXN0LXRva2Vu";

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .and(header("Authorization", expected_header))
        .respond_with(ResponseTemplate::new(200).set_body_json(sample_streams_response()))
        .mount(&server)
        .await;

    let config = LokiConfig::new(server.uri(), r#"{job="myapp"}"#)
        .with_basic_auth("123456", "glc_test-token");
    let provider = LokiTableProvider::new(config, vec!["job".to_string()]);

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(provider)).unwrap();

    let df = ctx.sql("SELECT line FROM logs").await.unwrap();
    // Only succeeds if the mock's exact-header match above matched, i.e. the
    // request actually carried correctly-encoded Basic Auth.
    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());
}

#[tokio::test]
async fn pushes_down_label_equality_into_logql() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .and(query_param("query", r#"{job="myapp", level="error"}"#))
        .respond_with(ResponseTemplate::new(200).set_body_json(sample_streams_response()))
        .mount(&server)
        .await;

    let config = LokiConfig::new(server.uri(), r#"{job="myapp"}"#);
    let provider = LokiTableProvider::new(config, vec!["job".to_string(), "level".to_string()]);

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(provider)).unwrap();

    let df = ctx.sql("SELECT line FROM logs WHERE level = 'error'").await.unwrap();
    // This will only succeed if the wiremock query_param matcher above matched,
    // i.e. the WHERE clause was correctly translated into the LogQL selector.
    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());
}

#[tokio::test]
async fn pushes_down_line_filter() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .and(query_param("query", r#"{job="myapp"} |= "panic""#))
        .respond_with(ResponseTemplate::new(200).set_body_json(sample_streams_response()))
        .mount(&server)
        .await;

    let config = LokiConfig::new(server.uri(), r#"{job="myapp"}"#);
    let provider = LokiTableProvider::new(config, vec!["job".to_string(), "level".to_string()]);

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(provider)).unwrap();

    let df = ctx.sql(r#"SELECT line FROM logs WHERE line LIKE '%panic%'"#).await.unwrap();
    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());
}

#[tokio::test]
async fn map_label_schema_round_trips() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .respond_with(ResponseTemplate::new(200).set_body_json(sample_streams_response()))
        .mount(&server)
        .await;

    let config = LokiConfig::new(server.uri(), r#"{job="myapp"}"#);
    let provider = LokiTableProvider::new_with_map_labels(config);

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(provider)).unwrap();

    let df = ctx.sql("SELECT line, labels FROM logs").await.unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
}

#[tokio::test]
async fn pushes_down_in_list_as_regex_alternation() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .and(query_param("query", r#"{job="myapp", level=~"error|warn"}"#))
        .respond_with(ResponseTemplate::new(200).set_body_json(sample_streams_response()))
        .mount(&server)
        .await;

    let config = LokiConfig::new(server.uri(), r#"{job="myapp"}"#);
    let provider = LokiTableProvider::new(config, vec!["job".to_string(), "level".to_string()]);

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT line FROM logs WHERE level IN ('error', 'warn')")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());
}

#[tokio::test]
async fn pushes_down_same_column_or_as_regex_alternation() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .and(query_param("query", r#"{job="myapp", level=~"error|warn"}"#))
        .respond_with(ResponseTemplate::new(200).set_body_json(sample_streams_response()))
        .mount(&server)
        .await;

    let config = LokiConfig::new(server.uri(), r#"{job="myapp"}"#);
    let provider = LokiTableProvider::new(config, vec!["job".to_string(), "level".to_string()]);

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT line FROM logs WHERE level = 'error' OR level = 'warn'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());
}

#[tokio::test]
async fn pushes_down_map_label_equality_via_real_sql() {
    let server = MockServer::start().await;

    // Confirms the SQL-parsed `labels['level'] = 'error'` path (which
    // resolves to a `get_field` scalar function against the real Map-typed
    // schema, distinct from the `array_element` shape the builder API
    // produces) is recognized and pushed into the LogQL selector.
    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .and(query_param("query", r#"{job="myapp", level="error"}"#))
        .respond_with(ResponseTemplate::new(200).set_body_json(sample_streams_response()))
        .mount(&server)
        .await;

    let config = LokiConfig::new(server.uri(), r#"{job="myapp"}"#);
    let provider = LokiTableProvider::new_with_map_labels(config);

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT line FROM logs WHERE labels['level'] = 'error'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());
}

#[tokio::test]
async fn paginates_and_dedups_entries_sharing_a_boundary_timestamp() {
    let server = MockServer::start().await;

    // Page limit is 2. First page comes back full (2 entries, both tied at
    // timestamp 2000) — a full page signals "there might be more", so the
    // provider re-queries with start=2000 (forward direction, start is
    // inclusive). That second page redelivers only "tied-a" (already
    // emitted) — a page shorter than the limit signals Loki is exhausted, so
    // pagination stops there with "tied-a" correctly deduplicated away.
    let page1 = json!({
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [{
                "stream": {"job": "myapp"},
                "values": [
                    ["2000", "tied-a"],
                    ["2000", "tied-b"]
                ]
            }]
        }
    });
    // Page 2 returns fewer entries than the limit (1 < 2), which is the
    // signal that Loki is exhausted — so pagination stops after this page
    // even though it redelivers "tied-a" (already emitted).
    let page2 = json!({
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [{
                "stream": {"job": "myapp"},
                "values": [
                    ["2000", "tied-a"]
                ]
            }]
        }
    });

    // No `start` param on the very first request (unbounded time range).
    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page1))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .and(query_param("start", "2000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page2))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    let config = LokiConfig::new(server.uri(), r#"{job="myapp"}"#).with_query_limit(2);
    let provider = LokiTableProvider::new(config, vec!["job".to_string()]);

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(provider)).unwrap();

    let df = ctx.sql("SELECT line FROM logs").await.unwrap();
    let batches = df.collect().await.unwrap();

    let lines: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let col = b
                .column_by_name("line")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            (0..col.len()).map(|i| col.value(i).to_string()).collect::<Vec<_>>()
        })
        .collect();

    // "tied-a" appears in both pages (a real Loki behavior at page
    // boundaries with tied timestamps) but must surface exactly once in the
    // final result, alongside "tied-b" from the first page.
    let tied_a_count = lines.iter().filter(|l| *l == "tied-a").count();
    assert_eq!(tied_a_count, 1, "boundary-duplicate entry should be deduplicated, got: {lines:?}");
    assert!(lines.contains(&"tied-b".to_string()), "got: {lines:?}");
    assert_eq!(lines.len(), 2, "expected exactly 2 distinct entries, got: {lines:?}");
}

/// Regression test: if every entry at a boundary timestamp keeps coming back
/// as an already-seen duplicate on a *full* page (more entries are tied at
/// that exact nanosecond than fit in one page), naive pagination would
/// re-issue the identical request forever — this must surface a clear error
/// instead of hanging indefinitely.
#[tokio::test]
async fn errors_instead_of_looping_when_boundary_tie_exceeds_page_limit() {
    let server = MockServer::start().await;

    // Every page returns the exact same 2 entries, both tied at timestamp
    // 2000 — simulates more than `query_limit` entries sharing one
    // nanosecond, so no page can ever make progress past the tie.
    let page = json!({
        "status": "success",
        "data": {
            "resultType": "streams",
            "result": [{
                "stream": {"job": "myapp"},
                "values": [
                    ["2000", "tied-a"],
                    ["2000", "tied-b"]
                ]
            }]
        }
    });

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page))
        .mount(&server)
        .await;

    let config = LokiConfig::new(server.uri(), r#"{job="myapp"}"#).with_query_limit(2);
    let provider = LokiTableProvider::new(config, vec!["job".to_string()]);

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(provider)).unwrap();

    let df = ctx.sql("SELECT line FROM logs").await.unwrap();
    // This must resolve (err or not) promptly rather than hang; the test
    // harness's own timeout is the backstop if this regresses to a loop.
    let result = df.collect().await;
    assert!(result.is_err(), "expected a clear error, not silent looping or wrong data");
}

#[tokio::test]
async fn surfaces_loki_error_responses() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/loki/api/v1/query_range"))
        .respond_with(ResponseTemplate::new(400).set_body_string("parse error: unexpected end of query"))
        .mount(&server)
        .await;

    let config = LokiConfig::new(server.uri(), r#"{job="myapp"}"#);
    let provider = LokiTableProvider::new(config, vec!["job".to_string()]);

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(provider)).unwrap();

    let df = ctx.sql("SELECT line FROM logs").await.unwrap();
    let result = df.collect().await;
    assert!(result.is_err());
}
