use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use reqwest::{header, Client as HttpClient};
use serde::Deserialize;
use url::Url;

use crate::config::{LokiAuth, LokiConfig};
use crate::error::{LokiError, Result};

/// A thin async client over Loki's HTTP query API.
#[derive(Clone)]
pub struct LokiClient {
    http: HttpClient,
    base_url: Url,
}

impl LokiClient {
    pub fn new(config: &LokiConfig) -> Result<Self> {
        let base_url = Url::parse(&config.base_url)?;

        let mut headers = header::HeaderMap::new();
        if let Some(tenant) = &config.tenant_id {
            headers.insert(
                "X-Scope-OrgID",
                header::HeaderValue::from_str(tenant)
                    .map_err(|e| LokiError::Unsupported(format!("invalid tenant id header: {e}")))?,
            );
        }
        if let LokiAuth::Bearer { token } = &config.auth {
            let value = format!("Bearer {token}");
            headers.insert(
                header::AUTHORIZATION,
                header::HeaderValue::from_str(&value)
                    .map_err(|e| LokiError::Unsupported(format!("invalid bearer token: {e}")))?,
            );
        }

        // Basic auth credentials (if configured) are applied per-request via
        // `basic_auth()` + `RequestBuilder::basic_auth`, since reqwest has no
        // client-level default for Basic auth the way it does for headers.
        let builder = HttpClient::builder()
            .timeout(config.timeout)
            .default_headers(headers)
            .gzip(true);

        let http = builder.build().map_err(LokiError::Http)?;

        Ok(Self { http, base_url })
    }

    fn basic_auth<'a>(&self, config: &'a LokiConfig) -> Option<(&'a str, Option<&'a str>)> {
        match &config.auth {
            LokiAuth::Basic { username, password } => Some((username.as_str(), Some(password.as_str()))),
            _ => None,
        }
    }

    /// Executes a `query_range` request against `/loki/api/v1/query_range`.
    pub async fn query_range(&self, config: &LokiConfig, params: &QueryRangeParams<'_>) -> Result<QueryRangeResponse> {
        let mut url = self.base_url.clone();
        url.set_path(&join_path(self.base_url.path(), "loki/api/v1/query_range"));

        {
            let mut qp = url.query_pairs_mut();
            qp.append_pair("query", params.logql);
            qp.append_pair("limit", &params.limit.to_string());
            qp.append_pair("direction", params.direction);
            if let Some(start) = params.start {
                qp.append_pair("start", &to_ns_string(start));
            }
            if let Some(end) = params.end {
                qp.append_pair("end", &to_ns_string(end));
            }
        }

        let mut req = self.http.get(url);
        if let Some((user, pass)) = self.basic_auth(config) {
            req = req.basic_auth(user, pass);
        }

        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(LokiError::LokiApi {
                status: status.as_u16(),
                body,
            });
        }

        let body: RawQueryResponse = resp.json().await?;
        body.try_into()
    }

    /// Fetches the set of known label names via `/loki/api/v1/labels`, used for
    /// schema inference when the caller doesn't specify labels explicitly.
    pub async fn labels(&self, config: &LokiConfig, start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> Result<Vec<String>> {
        let mut url = self.base_url.clone();
        url.set_path(&join_path(self.base_url.path(), "loki/api/v1/labels"));
        {
            let mut qp = url.query_pairs_mut();
            if let Some(start) = start {
                qp.append_pair("start", &to_ns_string(start));
            }
            if let Some(end) = end {
                qp.append_pair("end", &to_ns_string(end));
            }
        }

        let mut req = self.http.get(url);
        if let Some((user, pass)) = self.basic_auth(config) {
            req = req.basic_auth(user, pass);
        }

        let resp = req.send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(LokiError::LokiApi { status: status.as_u16(), body });
        }
        let body: LabelsResponse = resp.json().await?;
        Ok(body.data)
    }
}

fn join_path(base: &str, suffix: &str) -> String {
    let base = base.trim_end_matches('/');
    format!("{base}/{suffix}")
}

fn to_ns_string(dt: DateTime<Utc>) -> String {
    dt.timestamp_nanos_opt()
        .map(|ns| ns.to_string())
        .unwrap_or_else(|| dt.timestamp().to_string())
}

pub struct QueryRangeParams<'a> {
    pub logql: &'a str,
    pub limit: u32,
    pub direction: &'static str,
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
}

// --- Loki response wire format ---
//
// {
//   "status": "success",
//   "data": {
//     "resultType": "streams",
//     "result": [
//       {
//         "stream": {"job": "myapp", "level": "info"},
//         "values": [["<unix_nano_str>", "<line>"], ...]
//       }
//     ]
//   }
// }

#[derive(Debug, Deserialize)]
struct RawQueryResponse {
    status: String,
    data: RawQueryData,
}

#[derive(Debug, Deserialize)]
struct RawQueryData {
    #[serde(rename = "resultType")]
    result_type: String,
    result: Vec<RawStreamResult>,
}

#[derive(Debug, Deserialize)]
struct RawStreamResult {
    stream: BTreeMap<String, String>,
    values: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct LabelsResponse {
    #[allow(dead_code)]
    status: String,
    data: Vec<String>,
}

/// A single decoded log entry, ready to feed into an Arrow builder.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp_ns: i64,
    pub line: String,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct QueryRangeResponse {
    pub entries: Vec<LogEntry>,
}

impl TryFrom<RawQueryResponse> for QueryRangeResponse {
    type Error = LokiError;

    fn try_from(raw: RawQueryResponse) -> Result<Self> {
        if raw.status != "success" {
            return Err(LokiError::MalformedStream(format!(
                "Loki query status was {:?}",
                raw.status
            )));
        }
        if raw.data.result_type != "streams" {
            return Err(LokiError::Unsupported(format!(
                "expected resultType=streams (log query), got {:?} — this table provider only supports log queries, not metric/instant queries",
                raw.data.result_type
            )));
        }

        let mut entries = Vec::new();
        for stream in raw.data.result {
            for [ts, line] in stream.values {
                let timestamp_ns: i64 = ts
                    .parse()
                    .map_err(|_| LokiError::MalformedStream(format!("non-numeric timestamp: {ts}")))?;
                entries.push(LogEntry {
                    timestamp_ns,
                    line,
                    labels: stream.stream.clone(),
                });
            }
        }

        Ok(QueryRangeResponse { entries })
    }
}

/// Applies request-level timeout wrapping for callers that want a hard
/// deadline distinct from the per-connection HTTP timeout (e.g. bounding total
/// pagination time across many requests).
pub async fn with_deadline<T, F>(fut: F, timeout: Duration) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(res) => res,
        Err(_) => Err(LokiError::Timeout(timeout)),
    }
}
