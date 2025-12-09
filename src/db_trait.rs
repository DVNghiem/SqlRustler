use crate::transaction::Transaction;
use pyo3::prelude::*;
use sqlx::Database;

// Trait for binding parameters
pub trait ParameterBinder {
    type Arguments;
    type Database: Database;

    fn bind_parameters<'a>(
        &self,
        py: Python<'_>,
        query: &'a str,
        params: &'a [Py<PyAny>],
    ) -> Result<sqlx::query::Query<'a, Self::Database, Self::Arguments>, PyErr>;
}

// Trait for mapping query results to Python objects
pub trait ResultMapper {
    type Row;
    type Database: Database;

    fn map_result(&self, py: Python<'_>, row: &Self::Row) -> Result<Py<PyAny>, PyErr>;
}

// Trait for executing queries (INSERT, UPDATE, DELETE)
pub trait DatabaseExecutor {
    type Database: Database;
    type Arguments;
    type ParameterBinder: ParameterBinder<Arguments = Self::Arguments, Database = Self::Database>;

    async fn execute<'a>(
        &self,
        py: Python<'_>,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [Py<PyAny>],
    ) -> Result<u64, PyErr>;
}

// Trait for fetching data (SELECT)
pub trait DatabaseFetcher {
    type Database: Database;
    type Row;
    type Arguments;
    type ParameterBinder: ParameterBinder<Arguments = Self::Arguments, Database = Self::Database>;
    type ResultMapper: ResultMapper<Row = Self::Row, Database = Self::Database>;

    async fn fetch_all<'a>(
        &self,
        py: Python<'_>,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [Py<PyAny>],
    ) -> Result<Vec<Py<PyAny>>, PyErr>;

    async fn stream_data<'a>(
        &self,
        py: Python<'_>,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [Py<PyAny>],
        chunk_size: usize,
    ) -> Result<Vec<Vec<Py<PyAny>>>, PyErr>;
}

// Trait for bulk operations
pub trait DatabaseBulkUpdater {
    type Database: Database;
    type Arguments;
    type ParameterBinder: ParameterBinder<Arguments = Self::Arguments, Database = Self::Database>;

    async fn bulk_change<'a>(
        &self,
        py: Python<'_>,
        transaction: &mut Transaction,
        query: &'a str,
        params: &'a [Vec<Py<PyAny>>],
        batch_size: usize,
    ) -> Result<u64, PyErr>;
}
