use std::sync::Arc;

use crate::context::{get_runtime, set_sql_connect};

use super::{
    config::DatabaseConfig,
    postgresql::PostgresDatabase,
    mysql::MySqlDatabase,
    sqlite::SqliteDatabase,
    transaction::{DatabaseTransaction, DatabaseTransactionType},
};
use pyo3::prelude::*;
use sqlx::{Error as SqlxError, Pool};
use sqlx::{MySql, Postgres, Sqlite};
use tokio::sync::Mutex;

#[derive(Clone)]
enum DatabaseType {
    Postgres(Arc<Pool<sqlx::Postgres>>),
    MySql(Arc<Pool<sqlx::MySql>>),
    Sqlite(Arc<Pool<sqlx::Sqlite>>),
}

#[derive(Clone)]
pub struct __Connection {
    connection: DatabaseType,
}

impl __Connection {
    pub async fn new(config: DatabaseConfig) -> Self {
        let connection = match config.driver {
            super::config::DatabaseType::Postgres => {
                let pool = config.create_postgres_pool().await.unwrap();
                Ok::<DatabaseType, SqlxError>(DatabaseType::Postgres(Arc::new(pool)))
            }
            super::config::DatabaseType::Mysql => {
                let pool = config.create_mysql_pool().await.unwrap();
                Ok::<DatabaseType, SqlxError>(DatabaseType::MySql(Arc::new(pool)))
            }
            super::config::DatabaseType::Sqlite => {
                let pool = config.create_sqlite_pool().await.unwrap();
                Ok::<DatabaseType, SqlxError>(DatabaseType::Sqlite(Arc::new(pool)))
            }
        }
        .unwrap();

        Self { connection }
    }

    // get transaction
    pub async fn transaction(&self) -> DatabaseTransaction {
        match &self.connection {
            DatabaseType::Postgres(pool) => {
                let transaction = pool
                    .begin()
                    .await
                    .map_err(|e| SqlxError::Configuration(e.to_string().into()));
                DatabaseTransaction::from_transaction(DatabaseTransactionType::Postgres(
                    PostgresDatabase,
                    Arc::new(Mutex::new(Some(transaction.unwrap()))),
                ))
            }
            DatabaseType::MySql(pool) => {
                let transaction = pool
                    .begin()
                    .await
                    .map_err(|e| SqlxError::Configuration(e.to_string().into()));
                DatabaseTransaction::from_transaction(DatabaseTransactionType::MySql(
                    MySqlDatabase,
                    Arc::new(Mutex::new(Some(transaction.unwrap()))),
                ))
            }
            DatabaseType::Sqlite(pool) => {
                let transaction = pool
                    .begin()
                    .await
                    .map_err(|e| SqlxError::Configuration(e.to_string().into()));
                DatabaseTransaction::from_transaction(DatabaseTransactionType::SQLite(
                    SqliteDatabase,
                    Arc::new(Mutex::new(Some(transaction.unwrap()))),
                ))
            }
        }
    }

    pub async fn begin_transaction(&self) -> Option<Box<dyn std::any::Any + Send>> {

        match &self.connection {
            DatabaseType::Postgres(pool) => {
                let transaction: sqlx::Transaction<Postgres> = pool.begin().await.ok()?;
                Some(Box::new(transaction))
            }
            DatabaseType::MySql(pool) => {
                let transaction: sqlx::Transaction<MySql> = pool.begin().await.ok()?;
                Some(Box::new(transaction))
            }
            DatabaseType::Sqlite(pool) => {
                let transaction: sqlx::Transaction<Sqlite> = pool.begin().await.ok()?;
                Some(Box::new(transaction))
            }
        }
    }
}

#[derive(Clone)]
#[pyclass]
pub struct DatabaseConnection;

#[pymethods]
impl DatabaseConnection {

    #[staticmethod]
    pub fn connect(config: DatabaseConfig) {
        get_runtime().block_on(async move {
            let database = __Connection::new(config).await;
            set_sql_connect(database);
        });
    }
}
