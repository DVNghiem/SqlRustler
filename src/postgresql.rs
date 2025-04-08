use std::sync::Arc;

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use futures::StreamExt;
use pyo3::{
    prelude::*,
    types::{
        PyBool, PyDate, PyDateAccess, PyDateTime, PyDict, PyFloat, PyInt, PyList, PyString, PyTime,
        PyTimeAccess,
    },
};
use serde_json::{from_str, to_string};
use sqlx::{
    postgres::{PgArguments, PgQueryResult, PgRow},
    types::{Json, JsonValue},
    Column, Row, ValueRef,
};
use tokio::sync::Mutex;

use super::db_trait::{DatabaseOperations, DynamicParameterBinder};

pub struct PostgresParameterBinder;

impl DynamicParameterBinder for PostgresParameterBinder {
    type Arguments = PgArguments;
    type Database = sqlx::Postgres;
    type Row = PgRow;

    fn bind_parameters<'q>(
        &self,
        query: &'q str,
        params: Vec<&PyAny>,
    ) -> Result<sqlx::query::Query<'q, Self::Database, PgArguments>, PyErr> {
        let mut query_builder = sqlx::query(query);

        for param in params {
            query_builder = match param {
                p if p.is_none() => query_builder.bind(None::<String>),
                p if p.is_instance_of::<PyString>() => query_builder.bind(p.extract::<String>()?),
                p if p.is_instance_of::<PyInt>() => query_builder.bind(p.extract::<i64>()?),
                p if p.is_instance_of::<PyFloat>() => query_builder.bind(p.extract::<f64>()?),
                p if p.is_instance_of::<PyBool>() => query_builder.bind(p.extract::<bool>()?),
                p if p.is_instance_of::<PyDateTime>() => query_builder.bind(extract_datetime(p)?),
                p if p.is_instance_of::<PyDate>() => query_builder.bind(extract_date(p)?),
                p if p.is_instance_of::<PyTime>() => query_builder.bind(extract_time(p)?),
                p if p.is_instance_of::<PyDict>() || p.is_instance_of::<PyList>() => {
                    let json_value = from_str(&p.to_string()).unwrap_or(JsonValue::Null);
                    query_builder.bind(Json(json_value))
                }
                _ => {
                    return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(format!(
                        "Unsupported parameter type: {:?}",
                        param.get_type()
                    )))
                }
            };
        }
        Ok(query_builder)
    }

    fn bind_result(&self, py: Python<'_>, row: &PgRow) -> Result<PyObject, PyErr> {
        let dict = PyDict::new(py);

        for (i, column) in row.columns().iter().enumerate() {
            let name = column.name();
            let value = match row.try_get_raw(i) {
                Ok(val) if val.is_null() => py.None(),
                Ok(_) => extract_column_value(py, row, i)?,
                Err(_) => py.None(),
            };
            dict.set_item(name, value)?;
        }
        Ok(dict.into())
    }
}

#[derive(Debug, Clone, Default)]
pub struct PostgresDatabase;

impl DatabaseOperations for PostgresDatabase {
    type Row = PgRow;
    type Arguments = PgArguments;
    type DatabaseType = sqlx::Postgres;
    type ParameterBinder = PostgresParameterBinder;

    async fn execute(
        &mut self,
        transaction: Arc<Mutex<Option<sqlx::Transaction<'static, Self::DatabaseType>>>>,
        query: &str,
        params: Vec<&PyAny>,
    ) -> Result<u64, PyErr> {
        let query_builder = PostgresParameterBinder.bind_parameters(query, params)?;
        let mut guard = transaction.lock().await;
        let result = query_builder
            .execute(&mut **guard.as_mut().unwrap())
            .await
            .unwrap_or(PgQueryResult::default());
        Ok(result.rows_affected())
    }

    async fn fetch_all(
        &mut self,
        py: Python<'_>,
        transaction: Arc<Mutex<Option<sqlx::Transaction<'static, Self::DatabaseType>>>>,
        query: &str,
        params: Vec<&PyAny>,
    ) -> Result<Vec<PyObject>, PyErr> {
        let query_builder = PostgresParameterBinder.bind_parameters(query, params)?;
        let mut guard = transaction.lock().await;
        let rows = query_builder
            .fetch_all(&mut **guard.as_mut().unwrap())
            .await
            .unwrap_or(Vec::new());
        rows.iter()
            .map(|row| PostgresParameterBinder.bind_result(py, row))
            .collect()
    }

    async fn stream_data(
        &mut self,
        py: Python<'_>,
        transaction: Arc<Mutex<Option<sqlx::Transaction<'static, sqlx::Postgres>>>>,
        query: &str,
        params: Vec<&PyAny>,
        chunk_size: usize,
    ) -> PyResult<Vec<Vec<PyObject>>> {
        let query_builder = PostgresParameterBinder.bind_parameters(query, params)?;
        let mut guard = transaction.lock().await.take().unwrap();
        let mut stream = query_builder.fetch(&mut *guard);
        let mut chunks: Vec<Vec<PyObject>> = Vec::new();
        let mut current_chunk: Vec<PyObject> = Vec::new();

        while let Some(row_result) = stream.next().await {
            match row_result {
                Ok(row) => {
                    let row_data: PyObject = PostgresParameterBinder.bind_result(py, &row)?;
                    current_chunk.push(row_data);

                    if current_chunk.len() >= chunk_size {
                        chunks.push(current_chunk);
                        current_chunk = Vec::new();
                    }
                }
                Err(e) => {
                    return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        e.to_string(),
                    ));
                }
            }
        }

        if !current_chunk.is_empty() {
            chunks.push(current_chunk);
        }
        Ok(chunks)
    }

    async fn bulk_change(
        &mut self,
        transaction: Arc<Mutex<Option<sqlx::Transaction<'static, Self::DatabaseType>>>>,
        query: &str,
        params: Vec<Vec<&PyAny>>,
        batch_size: usize,
    ) -> Result<u64, PyErr> {
        let mut total_affected: u64 = 0;
        let mut guard = transaction.lock().await;
        let tx = guard.as_mut().ok_or_else(|| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No active transaction")
        })?;

        // Process in batches
        for chunk in params.chunks(batch_size) {
            for param_set in chunk {
                // Build query with current parameters
                let query_builder =
                    PostgresParameterBinder.bind_parameters(query, param_set.to_vec())?;
                // Execute query and accumulate affected rows
                let result = query_builder.execute(&mut **tx).await.map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())
                })?;

                total_affected += result.rows_affected();
            }
        }
        Ok(total_affected)
    }
}

// Helper functions
fn extract_datetime(param: &PyAny) -> PyResult<NaiveDateTime> {
    let dt: &PyDateTime = param.downcast()?;
    Ok(NaiveDateTime::new(
        NaiveDate::from_ymd_opt(dt.get_year(), dt.get_month() as u32, dt.get_day() as u32).unwrap(),
        NaiveTime::from_hms_nano_opt(
            dt.get_hour() as u32,
            dt.get_minute() as u32,
            dt.get_second() as u32,
            dt.get_microsecond() as u32 * 1000,
        )
        .unwrap(),
    ))
}

fn extract_date(param: &PyAny) -> PyResult<NaiveDate> {
    let date: &PyDate = param.downcast()?;
    Ok(NaiveDate::from_ymd_opt(
        date.get_year(),
        date.get_month() as u32,
        date.get_day() as u32,
    )
    .unwrap())
}

fn extract_time(param: &PyAny) -> PyResult<NaiveTime> {
    let time: &PyTime = param.downcast()?;
    Ok(NaiveTime::from_hms_nano_opt(
        time.get_hour() as u32,
        time.get_minute() as u32,
        time.get_second() as u32,
        time.get_microsecond() as u32 * 1000,
    )
    .unwrap())
}

fn extract_column_value(py: Python<'_>, row: &PgRow, index: usize) -> PyResult<PyObject> {
    Ok(if let Ok(v) = row.try_get::<i32, _>(index) {
        v.into_py(py)
    } else if let Ok(v) = row.try_get::<i64, _>(index) {
        v.into_py(py)
    } else if let Ok(v) = row.try_get::<String, _>(index) {
        v.into_py(py)
    } else if let Ok(v) = row.try_get::<f64, _>(index) {
        v.into_py(py)
    } else if let Ok(v) = row.try_get::<bool, _>(index) {
        v.into_py(py)
    } else if let Ok(v) = row.try_get::<NaiveDateTime, _>(index) {
        PyDateTime::new(
            py,
            v.year(),
            v.month() as u8,
            v.day() as u8,
            v.hour() as u8,
            v.minute() as u8,
            v.second() as u8,
            (v.nanosecond() / 1000) as u32,
            None,
        )?
        .into()
    } else if let Ok(v) = row.try_get::<NaiveDate, _>(index) {
        PyDate::new(py, v.year(), v.month() as u8, v.day() as u8)?.into()
    } else if let Ok(v) = row.try_get::<NaiveTime, _>(index) {
        PyTime::new(
            py,
            v.hour() as u8,
            v.minute() as u8,
            v.second() as u8,
            (v.nanosecond() / 1000) as u32,
            None,
        )?
        .into()
    } else if let Ok(v) = row.try_get::<Json<JsonValue>, _>(index) {
        to_string(&v.0)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?
            .into_py(py)
    } else if let Ok(v) = row.try_get::<Vec<String>, _>(index) {
        PyList::new(py, &v).into()
    } else if let Ok(v) = row.try_get::<Vec<i32>, _>(index) {
        PyList::new(py, &v).into()
    } else {
        py.None()
    })
}
