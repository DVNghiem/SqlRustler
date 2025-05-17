use dashmap::DashMap;
use pyo3::prelude::*;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;
use std::sync::Arc;

use crate::{
    config::{DatabaseConfig, DatabasePool, DatabaseType},
    error::DatabaseError,
    transaction::Transaction,
};

static RUNTIME: OnceCell<Runtime> = OnceCell::new();
static CONNECTION: OnceCell<DashMap<String, Arc<Connection>>> = OnceCell::new();
static DB_TYPE_WITH_ALIAS: OnceCell<DashMap<String, DatabaseType>> = OnceCell::new();

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

fn set_db_type_with_alias(db_type: DatabaseType, alias: String) -> Result<(), DatabaseError> {
    let db_type_map = DB_TYPE_WITH_ALIAS.get_or_init(|| DashMap::new());
    if db_type_map.contains_key(&alias) {
        return Err(DatabaseError::Configuration("Database type already set".into()));
    }
    db_type_map.insert(alias, db_type);
    Ok(())
}

#[pyfunction]
pub fn get_db_type_with_alias(alias: &str) -> Result<DatabaseType, DatabaseError> {
    DB_TYPE_WITH_ALIAS
        .get()
        .ok_or(DatabaseError::NotConnected)
        .and_then(|db_map| {
            db_map.get(alias)
                .map(|db| db.clone())
                .ok_or(DatabaseError::NotConnected)
        })
}

#[pyclass]
pub struct DatabaseConnection;

#[pymethods]
impl DatabaseConnection {
    #[staticmethod]
    pub fn connect(config: DatabaseConfig, alias: Option<String>, py: Python) -> PyResult<()> {
        let alias = alias.unwrap_or_else(|| "default".to_string());
        let connection = py.allow_threads(|| {
            get_runtime().block_on(async { Connection::new(config.clone()).await })
        })?;
        set_connection(connection, alias.clone())?;
        set_db_type_with_alias(config.driver, alias.clone())?;
        Ok(())
    }
}