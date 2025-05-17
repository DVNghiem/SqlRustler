use futures::TryStreamExt;
use pyo3::{prelude::*, types::PyDict};
use sqlx::{MySql, Row};
use crate::{
    db_trait::{ParameterBinder, ResultMapper, DatabaseExecutor, DatabaseFetcher, DatabaseBulkUpdater},
    transaction::Transaction,
    error::DatabaseError,
};

// MySQL Implementation
pub struct MySqlBinder;

impl ParameterBinder for MySqlBinder {
    type Arguments = sqlx::mysql::MySqlArguments;
    type Database = MySql;

    fn bind_parameters(
        &self,
        query: &str,
        params: &[&PyAny],
    ) -> Result<sqlx::query::Query<Self::Database, Self::Arguments>, PyErr> {
        let mut q = sqlx::query(query);
        for param in params {
            if param.is_none() {
                q = q.bind(None::<i32>);
            } else if let Ok(val) = param.extract::<i64>() {
                q = q.bind(val);
            } else if let Ok(val) = param.extract::<f64>() {
                q = q.bind(val);
            } else if let Ok(val) = param.extract::<&str>() {
                q = q.bind(val);
            } else if let Ok(val) = param.extract::<bool>() {
                q = q.bind(val);
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    format!("Unsupported parameter type: {}", param.get_type().name()?)
                ));
            }
        }
        Ok(q)
    }
}

pub struct MySqlMapper;

impl ResultMapper for MySqlMapper {
    type Row = sqlx::mysql::MySqlRow;
    type Database = MySql;

    fn map_result(&self, py: Python<'_>, row: &Self::Row) -> Result<PyObject, PyErr> {
        let dict = PyDict::new(py);
        for (i, col) in row.columns().iter().enumerate() {
            let key = col.name();
            let value: PyObject = match row.try_get::<Option<i64>, _>(i) {
                Ok(Some(val)) => Ok(val.to_object(py)),
                Ok(None) => Ok(py.None()),
                Err(_) => match row.try_get::<Option<f64>, _>(i) {
                    Ok(Some(val)) => Ok(val.to_object(py)),
                    Ok(None) => Ok(py.None()),
                    Err(_) => match row.try_get::<Option<&str>, _>(i) {
                        Ok(Some(val)) => Ok(val.to_object(py)),
                        Ok(None) => Ok(py.None()),
                        Err(_) => match row.try_get::<Option<bool>, _>(i) {
                            Ok(Some(val)) => Ok(val.to_object(py)),
                            Ok(None) => Ok(py.None()),
                            Err(_) => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                format!("Unsupported column type for {}", key)
                            )),
                        },
                    },
                },
            }?;
            dict.set_item(key, value)?;
        }
        Ok(dict.to_object(py))
    }
}

pub struct MySqlExecutor;

impl DatabaseExecutor for MySqlExecutor {
    type Database = MySql;
    type Arguments = sqlx::mysql::MySqlArguments;
    type ParameterBinder = MySqlBinder;

    async fn execute(
        &self,
        transaction: &mut Transaction,
        query: &str,
        params: &[&PyAny],
    ) -> Result<u64, PyErr> {
        let binder = MySqlBinder;
        match transaction {
            Transaction::MySql(tx) => {
                let q = binder.bind_parameters(query, params)?;
                let result = q.execute(&mut **tx).await.map_err(DatabaseError::Sqlx)?;
                Ok(result.rows_affected())
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction type mismatch"
            )),
        }
    }
}

pub struct MySqlFetcher;

impl DatabaseFetcher for MySqlFetcher {
    type Database = MySql;
    type Row = sqlx::mysql::MySqlRow;
    type Arguments = sqlx::mysql::MySqlArguments;
    type ParameterBinder = MySqlBinder;
    type ResultMapper = MySqlMapper;

    async fn fetch_all(
        &self,
        py: Python<'_>,
        transaction: &mut Transaction,
        query: &str,
        params: &[&PyAny],
    ) -> Result<Vec<PyObject>, PyErr> {
        let binder = MySqlBinder;
        let mapper = MySqlMapper;
        match transaction {
            Transaction::MySql(tx) => {
                let q = binder.bind_parameters(query, params)?;
                let rows = q.fetch_all(&mut **tx).await.map_err(DatabaseError::Sqlx)?;
                let mut results = Vec::new();
                for row in rows {
                    results.push(mapper.map_result(py, &row)?);
                }
                Ok(results)
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction type mismatch"
            )),
        }
    }

    async fn stream_data(
        &self,
        py: Python<'_>,
        transaction: &mut Transaction,
        query: &str,
        params: &[&PyAny],
        chunk_size: usize,
    ) -> Result<Vec<Vec<PyObject>>, PyErr> {
        let binder = MySqlBinder;
        let mapper = MySqlMapper;
        match transaction {
            Transaction::MySql(tx) => {
                let q = binder.bind_parameters(query, params)?;
                let mut stream = q.fetch(&mut **tx);
                let mut chunks = Vec::new();
                let mut current_chunk = Vec::new();
                while let Ok(Some(row)) = stream.try_next().await {
                    current_chunk.push(mapper.map_result(py, &row)?);
                    if current_chunk.len() >= chunk_size {
                        chunks.push(std::mem::take(&mut current_chunk));
                    }
                }
                if !current_chunk.is_empty() {
                    chunks.push(current_chunk);
                }
                Ok(chunks)
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction type mismatch"
            )),
        }
    }
}

pub struct MySqlBulkUpdater;

impl DatabaseBulkUpdater for MySqlBulkUpdater {
    type Database = MySql;
    type Arguments = sqlx::mysql::MySqlArguments;
    type ParameterBinder = MySqlBinder;

    async fn bulk_change(
        &self,
        transaction: &mut Transaction,
        query: &str,
        params: &[Vec<&PyAny>],
        batch_size: usize,
    ) -> Result<u64, PyErr> {
        let binder = MySqlBinder;
        match transaction {
            Transaction::MySql(tx) => {
                let mut total_affected = 0;
                for chunk in params.chunks(batch_size) {
                    for params in chunk {
                        let q = binder.bind_parameters(query, params)?;
                        let result = q.execute(&mut **tx).await.map_err(DatabaseError::Sqlx)?;
                        total_affected += result.rows_affected();
                    }
                }
                Ok(total_affected)
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction type mismatch"
            )),
        }
    }
}
