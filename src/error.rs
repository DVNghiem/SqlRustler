use pyo3::prelude::*;
use sqlx::Error as SqlxError;

#[derive(Debug)]
pub enum DatabaseError {
    Sqlx(SqlxError),
    Configuration(String),
    NotConnected,
    TransactionNotFound,
    ConnectionPoolExhausted,
    QueryTimeout,
    InvalidQuery(String),
    SerializationError(String),
}

impl std::error::Error for DatabaseError {}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DatabaseError::Sqlx(e) => write!(f, "Database error: {}", e),
            DatabaseError::Configuration(e) => write!(f, "Configuration error: {}", e),
            DatabaseError::NotConnected => write!(f, "No database connection available. Please call DatabaseConnection.connect() first."),
            DatabaseError::TransactionNotFound => write!(f, "Transaction not found. The transaction may have already been committed or rolled back."),
            DatabaseError::ConnectionPoolExhausted => write!(f, "Connection pool exhausted. Consider increasing max_connections."),
            DatabaseError::QueryTimeout => write!(f, "Query execution timeout exceeded."),
            DatabaseError::InvalidQuery(msg) => write!(f, "Invalid query: {}", msg),
            DatabaseError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
        }
    }
}

impl From<SqlxError> for DatabaseError {
    fn from(err: SqlxError) -> Self {
        match &err {
            SqlxError::PoolTimedOut => DatabaseError::ConnectionPoolExhausted,
            SqlxError::PoolClosed => DatabaseError::NotConnected,
            _ => DatabaseError::Sqlx(err),
        }
    }
}

impl From<DatabaseError> for PyErr {
    fn from(err: DatabaseError) -> PyErr {
        match err {
            DatabaseError::NotConnected => pyo3::exceptions::PyConnectionError::new_err(err.to_string()),
            DatabaseError::Configuration(_) => pyo3::exceptions::PyValueError::new_err(err.to_string()),
            DatabaseError::InvalidQuery(_) => pyo3::exceptions::PyValueError::new_err(err.to_string()),
            _ => pyo3::exceptions::PyRuntimeError::new_err(err.to_string()),
        }
    }
}