use std::sync::Arc;

use crate::db_impl::{
    PostgresExecutor, PostgresFetcher, PostgresBulkUpdater,
    MySqlExecutor, MySqlFetcher, MySqlBulkUpdater,
    SqliteExecutor, SqliteFetcher, SqliteBulkUpdater,
};

// Struct to hold database operation implementations
#[derive(Clone)]
pub struct Database {
    pub postgres_executor: Arc<PostgresExecutor>,
    pub postgres_fetcher: Arc<PostgresFetcher>,
    pub postgres_bulk_updater: Arc<PostgresBulkUpdater>,
    pub mysql_executor: Arc<MySqlExecutor>,
    pub mysql_fetcher: Arc<MySqlFetcher>,
    pub mysql_bulk_updater: Arc<MySqlBulkUpdater>,
    pub sqlite_executor: Arc<SqliteExecutor>,
    pub sqlite_fetcher: Arc<SqliteFetcher>,
    pub sqlite_bulk_updater: Arc<SqliteBulkUpdater>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            postgres_executor: Arc::new(PostgresExecutor),
            postgres_fetcher: Arc::new(PostgresFetcher),
            postgres_bulk_updater: Arc::new(PostgresBulkUpdater),
            mysql_executor: Arc::new(MySqlExecutor),
            mysql_fetcher: Arc::new(MySqlFetcher),
            mysql_bulk_updater: Arc::new(MySqlBulkUpdater),
            sqlite_executor: Arc::new(SqliteExecutor),
            sqlite_fetcher: Arc::new(SqliteFetcher),
            sqlite_bulk_updater: Arc::new(SqliteBulkUpdater),
        }
    }
}