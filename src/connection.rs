use pyo3::prelude::*;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;
use std::sync::Arc;

use crate::{
    config::{DatabaseConfig, DatabasePool},
    error::DatabaseError,
    transaction::Transaction,
};

static RUNTIME: OnceCell<Runtime> = OnceCell::new();
static CONNECTION: OnceCell<Arc<Connection>> = OnceCell::new();

pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

#[derive(Clone)]
pub struct Connection {
    pool: DatabasePool,
}

impl Connection {
    pub async fn new(config: DatabaseConfig) -> Result<Self, DatabaseError> {
        let pool = config.create_pool().await?;
        Ok(Connection { pool })
    }

    pub async fn begin_transaction(&self) -> Result<Transaction, DatabaseError> {
        match &self.pool {
            DatabasePool::Postgres(pool) => {
                let tx = pool.begin().await?;
                Ok(Transaction::Postgres(tx))
            }
            DatabasePool::MySql(pool) => {
                let tx = pool.begin().await?;
                Ok(Transaction::MySql(tx))
            }
            DatabasePool::Sqlite(pool) => {
                let tx = pool.begin().await?;
                Ok(Transaction::Sqlite(tx))
            }
        }
    }
}

pub fn get_connection() -> Result<&'static Arc<Connection>, DatabaseError> {
    CONNECTION
        .get()
        .ok_or(DatabaseError::NotConnected)
}

pub fn set_connection(connection: Connection) -> Result<(), DatabaseError> {
    CONNECTION
        .set(Arc::new(connection))
        .map_err(|_| DatabaseError::Configuration("Connection already set".into()))
}

#[pyclass]
pub struct DatabaseConnection;

#[pymethods]
impl DatabaseConnection {
    #[staticmethod]
    pub fn connect(config: DatabaseConfig, py: Python) -> PyResult<()> {
        let connection = py.allow_threads(|| {
            get_runtime().block_on(async { Connection::new(config).await })
        })?;
        set_connection(connection)?;
        Ok(())
    }
}