use pyo3::prelude::*;
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    postgres::{PgConnectOptions, PgPoolOptions},
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    Pool,
};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub enum DatabaseType {
    Postgres,
    MySql,
    Sqlite,
}

#[pymethods]
impl DatabaseType {
    #[staticmethod]
    fn postgres() -> Self {
        DatabaseType::Postgres
    }

    #[staticmethod]
    fn mysql() -> Self {
        DatabaseType::MySql
    }

    #[staticmethod]
    fn sqlite() -> Self {
        DatabaseType::Sqlite
    }
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct DatabaseConfig {
    #[pyo3(get, set)]
    pub driver: DatabaseType,
    #[pyo3(get, set)]
    pub url: String,
    #[pyo3(get, set)]
    pub max_connections: u32,
    #[pyo3(get, set)]
    pub min_connections: u32,
    #[pyo3(get, set)]
    pub idle_timeout: u64,
    #[pyo3(get, set)]
    pub acquire_timeout: u64,
    #[pyo3(get, set)]
    pub max_lifetime: u64,
    #[pyo3(get, set)]
    pub test_before_acquire: bool,
}

#[pymethods]
impl DatabaseConfig {
    #[new]
    fn new(
        driver: DatabaseType,
        url: String,
        max_connections: Option<u32>,
        min_connections: Option<u32>,
        idle_timeout: Option<u64>,
        acquire_timeout: Option<u64>,
        max_lifetime: Option<u64>,
        test_before_acquire: Option<bool>,
    ) -> Self {
        DatabaseConfig {
            driver,
            url,
            max_connections: max_connections.unwrap_or(20),
            min_connections: min_connections.unwrap_or(1),
            idle_timeout: idle_timeout.unwrap_or(300),
            acquire_timeout: acquire_timeout.unwrap_or(30),
            max_lifetime: max_lifetime.unwrap_or(1800),
            test_before_acquire: test_before_acquire.unwrap_or(true),
        }
    }

    #[staticmethod]
    fn default_postgres(url: String) -> Self {
        DatabaseConfig {
            driver: DatabaseType::Postgres,
            url,
            max_connections: 20,
            min_connections: 1,
            idle_timeout: 300,
            acquire_timeout: 30,
            max_lifetime: 1800,
            test_before_acquire: true,
        }
    }

    #[staticmethod]
    fn default_mysql(url: String) -> Self {
        DatabaseConfig {
            driver: DatabaseType::MySql,
            url,
            max_connections: 20,
            min_connections: 1,
            idle_timeout: 300,
            acquire_timeout: 30,
            max_lifetime: 1800,
            test_before_acquire: true,
        }
    }

    #[staticmethod]
    fn default_sqlite(url: String) -> Self {
        DatabaseConfig {
            driver: DatabaseType::Sqlite,
            url,
            max_connections: 20,
            min_connections: 1,
            idle_timeout: 300,
            acquire_timeout: 30,
            max_lifetime: 1800,
            test_before_acquire: true,
        }
    }
}

impl DatabaseConfig {
    pub async fn create_pool(&self) -> Result<DatabasePool, crate::error::DatabaseError> {
        match self.driver {
            DatabaseType::Postgres => {
                let options = self.url.parse::<PgConnectOptions>()?;
                let mut pool_options = PgPoolOptions::new()
                    .max_connections(self.max_connections)
                    .min_connections(self.min_connections)
                    .idle_timeout(Some(Duration::from_secs(self.idle_timeout)))
                    .acquire_timeout(Duration::from_secs(self.acquire_timeout))
                    .max_lifetime(Some(Duration::from_secs(self.max_lifetime)));

                if self.test_before_acquire {
                    pool_options = pool_options.test_before_acquire(true);
                }

                let pool = pool_options.connect_with(options).await?;
                Ok(DatabasePool::Postgres(pool))
            }
            DatabaseType::MySql => {
                let options = self.url.parse::<MySqlConnectOptions>()?;
                let mut pool_options = MySqlPoolOptions::new()
                    .max_connections(self.max_connections)
                    .min_connections(self.min_connections)
                    .idle_timeout(Some(Duration::from_secs(self.idle_timeout)))
                    .acquire_timeout(Duration::from_secs(self.acquire_timeout))
                    .max_lifetime(Some(Duration::from_secs(self.max_lifetime)));

                if self.test_before_acquire {
                    pool_options = pool_options.test_before_acquire(true);
                }

                let pool = pool_options.connect_with(options).await?;
                Ok(DatabasePool::MySql(pool))
            }
            DatabaseType::Sqlite => {
                let options = self.url.parse::<SqliteConnectOptions>()?;
                let mut pool_options = SqlitePoolOptions::new()
                    .max_connections(self.max_connections)
                    .min_connections(self.min_connections)
                    .idle_timeout(Some(Duration::from_secs(self.idle_timeout)))
                    .acquire_timeout(Duration::from_secs(self.acquire_timeout))
                    .max_lifetime(Some(Duration::from_secs(self.max_lifetime)));

                if self.test_before_acquire {
                    pool_options = pool_options.test_before_acquire(true);
                }

                let pool = pool_options.connect_with(options).await?;
                Ok(DatabasePool::Sqlite(pool))
            }
        }
    }
}

#[derive(Clone)]
pub enum DatabasePool {
    Postgres(Pool<sqlx::Postgres>),
    MySql(Pool<sqlx::MySql>),
    Sqlite(Pool<sqlx::Sqlite>),
}
