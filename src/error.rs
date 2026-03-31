//! Error types for the Zoo operator.

use thiserror::Error;

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum OperatorError {
    #[error("Kubernetes API error: {0}")]
    KubeApi(#[from] kube::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Resource not found: {0}")]
    NotFound(String),

    #[error("Reconciliation error: {0}")]
    Reconcile(String),
}

pub type Result<T> = std::result::Result<T, OperatorError>;
