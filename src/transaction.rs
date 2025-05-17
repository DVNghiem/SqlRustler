use pyo3::prelude::*;
use pyo3::types::PyAny;
use sqlx::{Postgres, MySql, Sqlite};

use crate::connection::get_runtime;
use crate::db_operations::Database;
use crate::db_trait::{DatabaseExecutor, DatabaseFetcher, DatabaseBulkUpdater};
use crate::error::DatabaseError;
use crate::session::SESSION_MAP;

pub enum Transaction {
    Postgres(sqlx::Transaction<'static, Postgres>),
    MySql(sqlx::Transaction<'static, MySql>),
    Sqlite(sqlx::Transaction<'static, Sqlite>),
}

impl Transaction {
    pub async fn commit(self) -> Result<(), DatabaseError> {
        match self {
            Transaction::Postgres(tx) => tx.commit().await?,
            Transaction::MySql(tx) => tx.commit().await?,
            Transaction::Sqlite(tx) => tx.commit().await?,
        }
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), DatabaseError> {
        match self {
            Transaction::Postgres(tx) => tx.rollback().await?,
            Transaction::MySql(tx) => tx.rollback().await?,
            Transaction::Sqlite(tx) => tx.rollback().await?,
        }
        Ok(())
    }
}

#[pyclass]
pub struct TransactionWrapper {
    session_id: String,
}

impl TransactionWrapper {
    pub fn new(session_id: String) -> Self {
        TransactionWrapper { session_id }
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }
}

#[pymethods]
impl TransactionWrapper {
    pub fn execute(&self, query: &str, params: Vec<&PyAny>) -> PyResult<u64> {
        let db = Database::new();
        // Convert &PyAny to PyObject before entering allow_threads
        let result = get_runtime().block_on(async move {
           let tx_entry = SESSION_MAP
                    .remove(&self.session_id)
                    .ok_or(DatabaseError::TransactionNotFound)?;
                let mut tx = tx_entry.1;
                // Convert PyObject back to &PyAny inside the GIL scope
                let result = match &mut tx {
                    Transaction::Postgres(_) => {
                        db.postgres_executor.execute(&mut tx, query, &params).await
                    }
                    Transaction::MySql(_) => {
                        db.mysql_executor.execute(&mut tx, query, &params).await
                    }
                    Transaction::Sqlite(_) => {
                        db.sqlite_executor.execute(&mut tx, query, &params).await
                    }
                };
                SESSION_MAP.insert(self.session_id.clone(), tx);
                result
        })?;
        Ok(result)
    }

    pub fn fetch_all(&self, query: &str, params: Vec<&PyAny>, py: Python) -> PyResult<Vec<PyObject>> {
        let db = Database::new();
        let result = get_runtime().block_on(async move {
            let tx_entry = SESSION_MAP
                    .remove(&self.session_id)
                    .ok_or(DatabaseError::TransactionNotFound)?;
                let mut tx = tx_entry.1;
                let result = match &mut tx {
                    Transaction::Postgres(_) => {
                        db.postgres_fetcher.fetch_all(py, &mut tx, query, &params).await
                    }
                    Transaction::MySql(_) => {
                        db.mysql_fetcher.fetch_all(py, &mut tx, query, &params).await
                    }
                    Transaction::Sqlite(_) => {
                        db.sqlite_fetcher.fetch_all(py, &mut tx, query, &params).await
                    }
                };
                SESSION_MAP.insert(self.session_id.clone(), tx);
                result
        })?;
        Ok(result)
    }

    pub fn stream_data(
        &self,
        query: &str,
        params: Vec<&PyAny>,
        chunk_size: usize,
        py: Python,
    ) -> PyResult<Vec<Vec<PyObject>>> {
        let db = Database::new();
        let result = get_runtime().block_on(async move {
           let tx_entry = SESSION_MAP
                    .remove(&self.session_id)
                    .ok_or(DatabaseError::TransactionNotFound)?;
                let mut tx = tx_entry.1;
                let result = match &mut tx {
                    Transaction::Postgres(_) => {
                        db.postgres_fetcher
                            .stream_data(py, &mut tx, query, &params, chunk_size)
                            .await
                    }
                    Transaction::MySql(_) => {
                        db.mysql_fetcher
                            .stream_data(py, &mut tx, query, &params, chunk_size)
                            .await
                    }
                    Transaction::Sqlite(_) => {
                        db.sqlite_fetcher
                            .stream_data(py, &mut tx, query, &params, chunk_size)
                            .await
                    }
                };
                SESSION_MAP.insert(self.session_id.clone(), tx);
                result
        })?;
        Ok(result)
    }

    pub fn bulk_change(
        &self,
        query: &str,
        params: Vec<Vec<&PyAny>>,
        batch_size: usize,
    ) -> PyResult<u64> {
        let db = Database::new();

        let result = get_runtime().block_on(async move {
            let tx_entry = SESSION_MAP
                    .remove(&self.session_id)
                    .ok_or(DatabaseError::TransactionNotFound)?;
                let mut tx = tx_entry.1;
                let result = match &mut tx {
                    Transaction::Postgres(_) => {
                        db.postgres_bulk_updater
                            .bulk_change(&mut tx, query, &params, batch_size)
                            .await
                    }
                    Transaction::MySql(_) => {
                        db.mysql_bulk_updater
                            .bulk_change(&mut tx, query, &params, batch_size)
                            .await
                    }
                    Transaction::Sqlite(_) => {
                        db.sqlite_bulk_updater
                            .bulk_change(&mut tx, query, &params, batch_size)
                            .await
                    }
                };
                SESSION_MAP.insert(self.session_id.clone(), tx);
                result
        })?;
        Ok(result)
    }
}