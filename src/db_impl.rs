use futures::TryStreamExt;
use pyo3::{prelude::*, types::{PyDict, PyList}};
use sqlx::{Postgres, MySql, Sqlite, Row, Column};
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

// MySQL Implementation
pub struct MySqlBinder;

impl ParameterBinder for MySqlBinder {
    type Arguments = sqlx::mysql::MySqlArguments;
    type Database = MySql;

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

    async fn execute<'a>(
        &self,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [&'a PyAny],
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

    async fn fetch_all<'a>(
        &self,
        py: Python<'_>,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [&'a PyAny],
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

    async fn stream_data<'a>(
        &self,
        py: Python<'_>,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [&'a PyAny],
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

    async fn bulk_change<'a>(
        &self,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [Vec<&'a PyAny>],
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

// SQLite Implementation
pub struct SqliteBinder;

impl ParameterBinder for SqliteBinder {
    type Arguments = sqlx::sqlite::SqliteArguments<'static>;
    type Database = Sqlite;

    fn bind_parameters<'a>(
        &self,
        query: &'a str,
        params: &'a [&'a PyAny],
    ) -> Result<sqlx::query::Query<'a, Self::Database, Self::Arguments>, PyErr> {
        let mut q = sqlx::query::<Sqlite>(query);
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
        unsafe { std::mem::transmute(q) }
    }
}

pub struct SqliteMapper;

impl ResultMapper for SqliteMapper {
    type Row = sqlx::sqlite::SqliteRow;
    type Database = Sqlite;

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

pub struct SqliteExecutor;

impl DatabaseExecutor for SqliteExecutor {
    type Database = Sqlite;
    type Arguments = sqlx::sqlite::SqliteArguments<'static>;
    type ParameterBinder = SqliteBinder;

    async fn execute<'a>(
        &self,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [&'a PyAny],
    ) -> Result<u64, PyErr> {
        let binder = SqliteBinder;
        match transaction {
            Transaction::Sqlite(tx) => {
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

pub struct SqliteFetcher;

impl DatabaseFetcher for SqliteFetcher {
    type Database = Sqlite;
    type Row = sqlx::sqlite::SqliteRow;
    type Arguments = sqlx::sqlite::SqliteArguments<'static>;
    type ParameterBinder = SqliteBinder;
    type ResultMapper = SqliteMapper;

    async fn fetch_all<'a>(
        &self,
        py: Python<'_>,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [&'a PyAny],
    ) -> Result<Vec<PyObject>, PyErr> {
        let binder = SqliteBinder;
        let mapper = SqliteMapper;
        match transaction {
            Transaction::Sqlite(tx) => {
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
        let binder = SqliteBinder;
        let mapper = SqliteMapper;
        match transaction {
            Transaction::Sqlite(tx) => {
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

pub struct SqliteBulkUpdater;

impl DatabaseBulkUpdater for SqliteBulkUpdater {
    type Database = Sqlite;
    type Arguments = sqlx::sqlite::SqliteArguments<'static>;
    type ParameterBinder = SqliteBinder;

    async fn bulk_change<'a>(
        &self,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [Vec<&'a PyAny>],
        batch_size: usize,
    ) -> Result<u64, PyErr> {
        let binder = SqliteBinder;
        match transaction {
            Transaction::Sqlite(tx) => {
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