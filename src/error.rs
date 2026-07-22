use datafusion::error::DataFusionError;
use thiserror::Error;

/// Errors that can occur while talking to Loki or translating results into Arrow.
#[derive(Debug, Error)]
pub enum LokiError {
    #[error("HTTP request to Loki failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Loki returned an error response (status {status}): {body}")]
    LokiApi { status: u16, body: String },

    #[error("failed to parse Loki response as JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid Loki base URL: {0}")]
    InvalidUrl(#[from] url::ParseError),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("unsupported LogQL construct: {0}")]
    Unsupported(String),

    #[error("malformed log stream result: {0}")]
    MalformedStream(String),

    #[error("query timed out after {0:?}")]
    Timeout(std::time::Duration),
}

impl From<LokiError> for DataFusionError {
    fn from(err: LokiError) -> Self {
        DataFusionError::External(Box::new(err))
    }
}

pub type Result<T> = std::result::Result<T, LokiError>;
