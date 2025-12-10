use dashmap::DashMap;
use lazy_static::lazy_static;
use pyo3::prelude::*;
use uuid::Uuid;

use crate::{
    connection::{get_connection, get_runtime},
    transaction::{Transaction, TransactionWrapper},
};

lazy_static! {
    pub static ref SESSION_MAP: DashMap<String, Transaction> = DashMap::new();
}

#[pyclass]
pub struct Session {
    context_id: String,
    alias: String,
}

#[pymethods]
impl Session {
    #[new]
    pub fn new(context_id: Option<String>, alias: Option<String>) -> Self {
        let context_id = context_id.unwrap_or_else(|| Uuid::new_v4().to_string());
        let alias = alias.unwrap_or_else(|| "default".to_string());
        Session { context_id, alias }
    }

    #[getter]
    pub fn context_id(&self) -> String {
        self.context_id.clone()
    }

    pub fn __enter__(&self, py: Python) -> PyResult<TransactionWrapper> {
        let connection = get_connection(&self.alias)?;
        let tx = py.detach(|| {
            get_runtime().block_on(async { connection.begin_transaction().await })
        })?;
        SESSION_MAP.insert(self.context_id.clone(), tx);
        Ok(TransactionWrapper::new(self.context_id.clone()))
    }

    pub fn __exit__(
        &self,
        _exc_type: Option<Py<PyAny>>,
        exc_val: Option<Py<PyAny>>,
        _exc_tb: Option<Py<PyAny>>,
        py: Python,
    ) -> PyResult<()> {
        // Always remove transaction from map to prevent memory leak
        if let Some(tx_entry) = SESSION_MAP.remove(&self.context_id) {
            let tx = tx_entry.1;
            let result = py.detach(|| {
                get_runtime().block_on(async {
                    if exc_val.is_some() {
                        tx.rollback().await
                    } else {
                        tx.commit().await
                    }
                })
            });
            
            // Log error but don't propagate to avoid masking original exception
            if let Err(e) = result {
                eprintln!("Warning: Transaction cleanup failed for session {}: {}", self.context_id, e);
            }
        } else {
            eprintln!("Warning: Transaction not found in SESSION_MAP for session {}", self.context_id);
        }
        Ok(())
    }
}
