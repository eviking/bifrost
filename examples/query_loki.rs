//! Example: register a Loki-backed table and run SQL against it.
//!
//! Against a local/self-hosted Loki (no auth):
//! ```sh
//! LOKI_URL=http://localhost:3100 cargo run --example query_loki
//! ```
//!
//! Against Grafana Cloud Loki: authenticate with HTTP Basic Auth, where the
//! *username* is your stack's numeric Loki instance ID and the *password* is
//! an access-policy token scoped for `logs:read` (NOT a bearer token, and NOT
//! `X-Scope-OrgID` — that header is only for self-hosted multi-tenant Loki;
//! Grafana Cloud's tenancy is already implied by the per-stack URL + instance
//! ID). Find your instance ID and URL on your stack's Loki service details
//! page in the Grafana Cloud portal, and generate a token under Access
//! Policies. See <https://grafana.com/docs/loki/latest/reference/python-client-examples/>.
//! ```sh
//! LOKI_URL=https://logs-prod-006.grafana.net \
//! LOKI_INSTANCE_ID=123456 \
//! LOKI_API_TOKEN=glc_xxx \
//! cargo run --example query_loki
//! ```

use std::sync::Arc;
use std::time::Duration;

use datafusion::prelude::SessionContext;
use datafusion_loki::{LokiConfig, LokiTableProvider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let base_url = std::env::var("LOKI_URL").unwrap_or_else(|_| "http://localhost:3100".to_string());
    let instance_id = std::env::var("LOKI_INSTANCE_ID").ok();
    let api_token = std::env::var("LOKI_API_TOKEN").ok();

    let mut config = LokiConfig::new(&base_url, r#"{job="myapp"}"#)
        .with_query_limit(1000)
        .with_timeout(Duration::from_secs(20));

    if let (Some(instance_id), Some(api_token)) = (instance_id, api_token) {
        config = config.with_basic_auth(instance_id, api_token);
    }

    // Discover labels dynamically. In production, prefer `LokiTableProvider::new`
    // with an explicit label list for a stable schema.
    let provider = LokiTableProvider::connect(config).await?;

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(provider))?;

    let df = ctx
        .sql(
            r#"
            SELECT timestamp, line, level
            FROM logs
            WHERE level = 'error'
              AND line LIKE '%panic%'
              AND timestamp > now() - INTERVAL '1' HOUR
            ORDER BY timestamp DESC
            LIMIT 50
            "#,
        )
        .await?;

    df.show().await?;

    Ok(())
}
