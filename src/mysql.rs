use crate::{
    db_trait::{
        DatabaseBulkUpdater, DatabaseExecutor, DatabaseFetcher, ParameterBinder, ResultMapper,
    },
    error::DatabaseError,
    transaction::Transaction,
};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use futures::TryStreamExt;
use pyo3::{prelude::*, types::PyList};
use sqlx::{Column, MySql, Row};

pub struct MySqlBinder;

impl ParameterBinder for MySqlBinder {
    type Arguments = sqlx::mysql::MySqlArguments;
    type Database = MySql;

    fn bind_parameters<'a>(
        &self,
        py: Python<'_>,
        query: &'a str,
        params: &'a [Py<PyAny>],
    ) -> Result<sqlx::query::Query<'a, Self::Database, Self::Arguments>, PyErr> {
        let mut q = sqlx::query(query);
        for param in params {
            let param = param.bind(py);
            if param.is_none() {
                q = q.bind(None::<i32>);
            } else if let Ok(val) = param.extract::<i64>() {
                q = q.bind(val);
            } else if let Ok(val) = param.extract::<f64>() {
                q = q.bind(val);
            } else if let Ok(val) = param.extract::<&str>() {
                q = q.bind(val.to_string());
            } else if let Ok(val) = param.extract::<bool>() {
                q = q.bind(val);
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                    "Unsupported parameter type: {}",
                    param.get_type().name()?
                )));
            }
        }
        Ok(q)
    }
}

pub struct MySqlMapper;

impl ResultMapper for MySqlMapper {
    type Row = sqlx::mysql::MySqlRow;
    type Database = MySql;

    fn map_result(&self, py: Python<'_>, row: &Self::Row) -> Result<Py<PyAny>, PyErr> {
        // Create a list of (name, value, type) tuples for each column
        let columns = PyList::empty(py);
        for col in row.columns().iter() {
            let name = col.name();
            let col_type = col.type_info().to_string();
            let value: Py<PyAny> = match col_type.as_str() {
                "INT8" | "BIGINT" => row
                    .try_get::<Option<i64>, _>(name)
                    .map_or(py.None(), |v| v.into_pyobject(py).unwrap().unbind().into()),
                "INT4" | "INTEGER" | "SERIAL" => row
                    .try_get::<Option<i32>, _>(name)
                    .map_or(py.None(), |v| v.into_pyobject(py).unwrap().unbind().into()),
                "FLOAT8" | "DOUBLE PRECISION" | "NUMERIC" => row
                    .try_get::<Option<f64>, _>(name)
                    .map_or(py.None(), |v| v.into_pyobject(py).unwrap().unbind().into()),
                "TEXT" | "VARCHAR" | "CHAR" => row
                    .try_get::<Option<String>, _>(name)
                    .map_or(py.None(), |v| v.into_pyobject(py).unwrap().unbind().into()),
                "BOOL" | "BOOLEAN" => row
                    .try_get::<Option<bool>, _>(name)
                    .map_or(py.None(), |v| v.into_pyobject(py).unwrap().unbind().into()),
                "TIMESTAMP" => {
                    row.try_get::<Option<NaiveDateTime>, _>(name)
                        .map_or(py.None(), |opt| {
                            opt.map_or(py.None(), |v| {
                                v.format("%Y-%m-%dT%H:%M:%S")
                                    .to_string()
                                    .into_pyobject(py)
                                    .unwrap()
                                    .unbind()
                                    .into()
                            })
                        })
                }
                "TIMESTAMPTZ" => {
                    row.try_get::<Option<DateTime<Utc>>, _>(name)
                        .map_or(py.None(), |opt| {
                            opt.map_or(py.None(), |v| {
                                v.format("%Y-%m-%dT%H:%M:%S%Z")
                                    .to_string()
                                    .into_pyobject(py)
                                    .unwrap()
                                    .unbind()
                                    .into()
                            })
                        })
                }
                "DATE" => row
                    .try_get::<Option<NaiveDate>, _>(name)
                    .map_or(py.None(), |opt| {
                        opt.map_or(py.None(), |v| {
                            v.format("%Y-%m-%d")
                                .to_string()
                                .into_pyobject(py)
                                .unwrap()
                                .unbind()
                                .into()
                        })
                    }),
                "UUID" => row
                    .try_get::<Option<String>, _>(name)
                    .map_or(py.None(), |v| v.into_pyobject(py).unwrap().unbind().into()),

                _ => {
                    // Fallback to String for unknown types
                    row.try_get::<Option<String>, _>(name)
                        .map_or(py.None(), |v| v.into_pyobject(py).unwrap().unbind().into())
                }
            };
            let col_tuple = (name, value, col_type).into_pyobject(py)?.unbind();
            columns.append(col_tuple)?;
        }
        Ok(columns.unbind().into())
    }
}

pub struct MySqlExecutor;

impl DatabaseExecutor for MySqlExecutor {
    type Database = MySql;
    type Arguments = sqlx::mysql::MySqlArguments;
    type ParameterBinder = MySqlBinder;

    async fn execute<'a>(
        &self,
        py: Python<'_>,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [Py<PyAny>],
    ) -> Result<u64, PyErr> {
        let binder = MySqlBinder;
        match transaction {
            Transaction::MySql(tx) => {
                let q = binder.bind_parameters(py, query, params)?;
                let result = q.execute(&mut **tx).await.map_err(DatabaseError::Sqlx)?;
                Ok(result.rows_affected())
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction type mismatch",
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

    async fn fetch_all<'a>(
        &self,
        py: Python<'_>,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [Py<PyAny>],
    ) -> Result<Vec<Py<PyAny>>, PyErr> {
        let binder = MySqlBinder;
        let mapper = MySqlMapper;
        match transaction {
            Transaction::MySql(tx) => {
                let q = binder.bind_parameters(py, query, params)?;
                let rows = q.fetch_all(&mut **tx).await.map_err(DatabaseError::Sqlx)?;
                let mut results = Vec::new();
                for row in rows {
                    results.push(mapper.map_result(py, &row)?);
                }
                Ok(results)
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction type mismatch",
            )),
        }
    }

    async fn stream_data<'a>(
        &self,
        py: Python<'_>,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [Py<PyAny>],
        chunk_size: usize,
    ) -> Result<Vec<Vec<Py<PyAny>>>, PyErr> {
        let binder = MySqlBinder;
        let mapper = MySqlMapper;
        match transaction {
            Transaction::MySql(tx) => {
                let q = binder.bind_parameters(py, query, params)?;
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
                "Transaction type mismatch",
            )),
        }
    }
}

pub struct MySqlBulkUpdater;

impl DatabaseBulkUpdater for MySqlBulkUpdater {
    type Database = MySql;
    type Arguments = sqlx::mysql::MySqlArguments;
    type ParameterBinder = MySqlBinder;

    async fn bulk_change<'a>(
        &self,
        py: Python<'_>,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [Vec<Py<PyAny>>],
        batch_size: usize,
    ) -> Result<u64, PyErr> {
        let binder = MySqlBinder;
        match transaction {
            Transaction::MySql(tx) => {
                let mut total_affected = 0;
                for chunk in params.chunks(batch_size) {
                    for params in chunk {
                        let q = binder.bind_parameters(py, query, params)?;
                        let result = q.execute(&mut **tx).await.map_err(DatabaseError::Sqlx)?;
                        total_affected += result.rows_affected();
                    }
                }
                Ok(total_affected)
            }
            _ => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Transaction type mismatch",
            )),
        }
    }
}
