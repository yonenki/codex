use std::path::PathBuf;
use thiserror::Error;

pub type TeamGraphResult<T> = Result<T, TeamGraphError>;

#[derive(Debug, Error)]
pub enum TeamGraphError {
    #[error("{0}")]
    InvalidDefinition(String),
    #[error("failed to read team graph {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse team graph {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
}

impl TeamGraphError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::InvalidDefinition(message.into())
    }
}
