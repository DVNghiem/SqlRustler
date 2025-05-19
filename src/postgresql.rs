use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use futures::TryStreamExt;
use pyo3::{prelude::*, types::PyList};
use sqlx::{Postgres, Row, Column};
use serde_json::Value as JsonValue;
use crate::{
    db_trait::{ParameterBinder, ResultMapper, DatabaseExecutor, DatabaseFetcher, DatabaseBulkUpdater},
    transaction::Transaction,
    error::DatabaseError,
};

// Postgres Implementation
pub struct PostgresBinder;

impl ParameterBinder for PostgresBinder {
    type Arguments = sqlx::postgres::PgArguments;
    type Database = Postgres;

    fn bind_parameters<'a>(
        &self,
        query: &'a str,
        params: &'a [&'a PyAny],
    ) -> Result<sqlx::query::Query<'a, Self::Database, Self::Arguments>, PyErr> {
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
            } else if let Ok(list) = param.downcast::<PyList>() {
                let vec: Vec<String> = list.extract()?;
                q = q.bind(vec);
            } else if let Ok(json) = param.to_string().parse::<JsonValue>() {
                q = q.bind(json);
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                    format!("Unsupported parameter type: {}", param.get_type().name()?)
                ));
            }
        }
        Ok(q)
    }
}

pub struct PostgresMapper;


impl ResultMapper for PostgresMapper {
    type Row = sqlx::postgres::PgRow;
    type Database = Postgres;

    fn map_result(&self, py: Python<'_>, row: &Self::Row) -> Result<PyObject, PyErr> {
        // Create a list of (name, value, type) tuples for each column
        let columns = PyList::empty(py);
        for col in row.columns().iter() {
            let name = col.name();
            let col_type = col.type_info().to_string();
            let value: PyObject = match col_type.as_str() {
                "INT8" | "BIGINT" => row.try_get::<Option<i64>, _>(name).map_or(py.None(), |v| v.to_object(py)),
                "INT4" | "INTEGER" | "SERIAL" => row.try_get::<Option<i32>, _>(name).map_or(py.None(), |v| v.to_object(py)),
                "FLOAT8" | "DOUBLE PRECISION" | "NUMERIC" => row.try_get::<Option<f64>, _>(name).map_or(py.None(), |v| v.to_object(py)),
                "TEXT" | "VARCHAR" | "CHAR" => row.try_get::<Option<String>, _>(name).map_or(py.None(), |v| v.to_object(py)),
                "BOOL" | "BOOLEAN" => row.try_get::<Option<bool>, _>(name).map_or(py.None(), |v| v.to_object(py)),
                "TIMESTAMP" => {
                    row.try_get::<Option<NaiveDateTime>, _>(name)
                        .map_or(py.None(), |opt| opt.map_or(py.None(), |v| v.format("%Y-%m-%dT%H:%M:%S").to_string().to_object(py)))
                }
                "TIMESTAMPTZ" => {
                    row.try_get::<Option<DateTime<Utc>>, _>(name)
                        .map_or(py.None(), |opt| opt.map_or(py.None(), |v| v.format("%Y-%m-%dT%H:%M:%S%Z").to_string().to_object(py)))
                }
                "DATE" => {
                    row.try_get::<Option<NaiveDate>, _>(name)
                        .map_or(py.None(), |opt| opt.map_or(py.None(), |v| v.format("%Y-%m-%d").to_string().to_object(py)))
                }
                "UUID" => row.try_get::<Option<String>, _>(name).map_or(py.None(), |v| v.to_object(py)),
                "JSON" | "JSONB" => {
                    row.try_get::<Option<serde_json::Value>, _>(name)
                        .map_or(py.None(), |v| serde_json::to_string(&v).unwrap().to_object(py))
                }
                "TEXT[]" | "VARCHAR[]" => {
                    row.try_get::<Option<Vec<String>>, _>(name)
                        .map_or(py.None(), |v| PyList::new(py, v.iter()).to_object(py))
                }
                _ => {
                    // Fallback to String for unknown types
                    row.try_get::<Option<String>, _>(name).map_or(py.None(), |v| v.to_object(py))
                }
            };
            let col_tuple = (name, value, col_type).to_object(py);
            columns.append(col_tuple)?;
        }
        Ok(columns.to_object(py))
    }
}

pub struct PostgresExecutor;

impl DatabaseExecutor for PostgresExecutor {
    type Database = Postgres;
    type Arguments = sqlx::postgres::PgArguments;
    type ParameterBinder = PostgresBinder;

    async fn execute<'a>(
        &self,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [&'a PyAny],
    ) -> Result<u64, PyErr> {
        let binder = PostgresBinder;
        match transaction {
            Transaction::Postgres(tx) => {
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

pub struct PostgresFetcher;

impl DatabaseFetcher for PostgresFetcher {
    type Database = Postgres;
    type Row = sqlx::postgres::PgRow;
    type Arguments = sqlx::postgres::PgArguments;
    type ParameterBinder = PostgresBinder;
    type ResultMapper = PostgresMapper;

    async fn fetch_all<'a>(
        &self,
        py: Python<'_>,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [&'a PyAny],
    ) -> Result<Vec<PyObject>, PyErr> {
        let binder = PostgresBinder;
        let mapper = PostgresMapper;
        match transaction {
            Transaction::Postgres(tx) => {
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

    async fn stream_data<'a>(
        &self,
        py: Python<'_>,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [&'a PyAny],
        chunk_size: usize,
    ) -> Result<Vec<Vec<PyObject>>, PyErr> {
        let binder = PostgresBinder;
        let mapper = PostgresMapper;
        match transaction {
            Transaction::Postgres(tx) => {
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

pub struct PostgresBulkUpdater;

impl DatabaseBulkUpdater for PostgresBulkUpdater {
    type Database = Postgres;
    type Arguments = sqlx::postgres::PgArguments;
    type ParameterBinder = PostgresBinder;

    async fn bulk_change<'a>(
        &self,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [Vec<&'a PyAny>],
        batch_size: usize,
    ) -> Result<u64, PyErr> {
        let binder = PostgresBinder;
        match transaction {
            Transaction::Postgres(tx) => {
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
