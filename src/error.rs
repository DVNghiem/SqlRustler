use pyo3::prelude::*;
use sqlx::Error as SqlxError;

#[derive(Debug)]
pub enum DatabaseError {
    Sqlx(SqlxError),
    Configuration(String),
    NotConnected,
    TransactionNotFound,
}

impl std::error::Error for DatabaseError {}

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DatabaseError::Sqlx(e) => write!(f, "SQLx error: {}", e),
            DatabaseError::Configuration(e) => write!(f, "Configuration error: {}", e),
            DatabaseError::NotConnected => write!(f, "No database connection available"),
            DatabaseError::TransactionNotFound => write!(f, "Transaction not found for session"),
        }
    }
}

impl From<SqlxError> for DatabaseError {
    fn from(err: SqlxError) -> Self {
        DatabaseError::Sqlx(err)
    }
}

impl From<DatabaseError> for PyErr {
    fn from(err: DatabaseError) -> PyErr {
        pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
    }
}