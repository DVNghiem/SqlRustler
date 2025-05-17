use dashmap::DashMap;
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
static CONNECTION: OnceCell<DashMap<String, Arc<Connection>>> = OnceCell::new();

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
pub fn get_connection(alias: &str) -> Result<Arc<Connection>, DatabaseError> {
    CONNECTION
        .get()
        .ok_or(DatabaseError::NotConnected)
        .and_then(|conn_map| {
            conn_map.get(alias)
                .map(|c| Arc::clone(&c))
                .ok_or(DatabaseError::NotConnected)
        })
}

pub fn set_connection(connection: Connection, alias: String) -> Result<(), DatabaseError> {
    let conn_map = CONNECTION.get_or_init(|| DashMap::new());
    if conn_map.contains_key(&alias) {
        return Err(DatabaseError::Configuration("Connection already set".into()));
    }
    conn_map.insert(alias, Arc::new(connection));
    Ok(())
}
#[pyclass]
pub struct DatabaseConnection;

#[pymethods]
impl DatabaseConnection {
    #[staticmethod]
    pub fn connect(config: DatabaseConfig, alias: Option<String>, py: Python) -> PyResult<()> {
        let alias = alias.unwrap_or_else(|| "default".to_string());
        let connection = py.allow_threads(|| {
            get_runtime().block_on(async { Connection::new(config).await })
        })?;
        set_connection(connection, alias)?;
        Ok(())
    }
}