use std::time::Duration;

/// Configuration for connecting to a Loki instance.
#[derive(Debug, Clone)]
pub struct LokiConfig {
    /// Base URL of the Loki instance, e.g. `https://logs-prod-us-central1.grafana.net`.
    pub base_url: String,

    /// Optional tenant / organization ID, sent as the `X-Scope-OrgID` header.
    /// Only relevant for self-hosted Loki running with multi-tenancy enabled
    /// (`auth_enabled: true`). **Not used by Grafana Cloud** — there, tenancy
    /// is already implied by the per-stack URL and Basic Auth instance ID, and
    /// `X-Scope-OrgID` is not part of Grafana Cloud's auth contract; use
    /// `LokiAuth::Basic` via `with_basic_auth` instead. See
    /// <https://grafana.com/docs/loki/latest/operations/multi-tenancy/> (OSS
    /// tenancy) vs. <https://grafana.com/docs/loki/latest/reference/python-client-examples/>
    /// (Grafana Cloud auth).
    pub tenant_id: Option<String>,

    /// Authentication mode.
    pub auth: LokiAuth,

    /// The LogQL stream selector identifying which streams this table exposes,
    /// e.g. `{job="myapp", env="prod"}`. Additional label matchers from SQL
    /// `WHERE` clauses are merged into this selector at query time.
    pub stream_selector: String,

    /// Maximum number of entries requested per `query_range` call (maps to Loki's
    /// `limit` query parameter). Loki's server-side hard cap is typically 5000.
    pub query_limit: u32,

    /// HTTP request timeout.
    pub timeout: Duration,

    /// Number of rows placed in each Arrow `RecordBatch` yielded by the stream.
    pub batch_size: usize,

    /// Direction to fetch entries in ("forward" or "backward"). Loki paginates
    /// by adjusting the time range in this direction, so it also determines how
    /// automatic pagination advances.
    pub direction: Direction,

    /// Step used for pagination sub-queries: when a query returns exactly
    /// `query_limit` rows we assume it was truncated and continue from the last
    /// timestamp. This guards against infinite loops by capping total pages.
    pub max_pages: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Backward,
}

impl Direction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Direction::Forward => "forward",
            Direction::Backward => "backward",
        }
    }
}

#[derive(Debug, Clone)]
pub enum LokiAuth {
    None,
    Basic { username: String, password: String },
    Bearer { token: String },
}

impl LokiConfig {
    pub fn new(base_url: impl Into<String>, stream_selector: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            tenant_id: None,
            auth: LokiAuth::None,
            stream_selector: stream_selector.into(),
            query_limit: 5000,
            timeout: Duration::from_secs(30),
            batch_size: 8192,
            direction: Direction::Forward,
            max_pages: 200,
        }
    }

    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    /// This is the auth mode Grafana Cloud Loki expects: `username` is your
    /// stack's numeric Loki instance ID (shown alongside the base URL on the
    /// stack's Loki service details page in the Grafana Cloud portal), and
    /// `password` is an access-policy token scoped for `logs:read` (create
    /// one under Access Policies). Grafana Cloud does not use bearer tokens
    /// or `X-Scope-OrgID` for this API.
    pub fn with_basic_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = LokiAuth::Basic {
            username: username.into(),
            password: password.into(),
        };
        self
    }

    /// Sends `Authorization: Bearer <token>`. Not the auth mode Grafana Cloud
    /// expects (use `with_basic_auth` for that) — this is for self-hosted
    /// Loki deployments sitting behind a reverse proxy or gateway that
    /// enforces bearer-token auth itself (Loki has no native bearer-auth
    /// concept of its own).
    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.auth = LokiAuth::Bearer { token: token.into() };
        self
    }

    pub fn with_query_limit(mut self, limit: u32) -> Self {
        self.query_limit = limit;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn with_direction(mut self, direction: Direction) -> Self {
        self.direction = direction;
        self
    }
}
