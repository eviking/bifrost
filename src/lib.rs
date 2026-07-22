//! # datafusion-loki
//!
//! An Apache DataFusion [`TableProvider`](datafusion::datasource::TableProvider)
//! for querying [Grafana Loki](https://grafana.com/oss/loki/) log streams with SQL,
//! via LogQL under the hood.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use datafusion::prelude::SessionContext;
//! use datafusion_loki::{LokiConfig, LokiTableProvider};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Grafana Cloud: Basic Auth with your stack's numeric instance ID as the
//! // username and an access-policy token (logs:read) as the password. Not
//! // bearer-token or X-Scope-OrgID auth — see the README for details.
//! let config = LokiConfig::new("https://logs-prod-006.grafana.net", r#"{job="myapp"}"#)
//!     .with_basic_auth("123456", "glc_your-access-policy-token");
//!
//! let provider = LokiTableProvider::new(config, vec!["job".into(), "level".into(), "pod".into()]);
//!
//! let ctx = SessionContext::new();
//! ctx.register_table("logs", Arc::new(provider))?;
//!
//! let df = ctx
//!     .sql("SELECT timestamp, line FROM logs WHERE level = 'error' AND line LIKE '%panic%' LIMIT 100")
//!     .await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Schema
//!
//! Every table has two fixed columns:
//!
//! - `timestamp` — `Timestamp(Nanosecond)`, the log entry's time
//! - `line` — `Utf8`, the raw log line
//!
//! Labels are exposed either as individual flattened `Utf8` columns
//! ([`LokiTableProvider::new`]) or as a single `labels` `Map<Utf8, Utf8>`
//! column ([`LokiTableProvider::new_with_map_labels`]).
//!
//! ## Pushdown behavior
//!
//! | SQL construct | Pushed down as |
//! |---|---|
//! | `label_col = 'x'` | LogQL label matcher `label="x"` |
//! | `label_col != 'x'` | LogQL label matcher `label!="x"` |
//! | `label_col ~ 'regex'` | LogQL label matcher `label=~"regex"` |
//! | `line = 'x'` | LogQL line filter `\|= "x"` |
//! | `line LIKE '%x%'` | LogQL line filter `\|= "x"` |
//! | `timestamp > / >= / < / <= / = ts` | Loki `start` / `end` query params |
//! | `LIMIT n` | Row cap across paginated `query_range` calls |
//!
//! Anything not in this table (numeric comparisons, `OR` across label/line
//! predicates, UDFs, etc.) is still evaluated correctly — DataFusion applies it
//! after the scan — it's just not sent to Loki, so more data may be fetched
//! than strictly necessary.

mod client;
mod config;
mod convert;
mod error;
mod exec;
mod logql;
mod pushdown;
mod schema;
mod table_provider;
mod time_range;

pub use client::LokiClient;
pub use config::{Direction, LokiAuth, LokiConfig};
pub use error::LokiError;
pub use schema::LabelSchema;
pub use table_provider::LokiTableProvider;
