use pyo3::prelude::*;

mod config;
mod connection;
mod db_trait;
mod mysql;
mod postgresql;
mod sqlite;
mod transaction;
mod context;
mod session;
mod runtime;

#[pymodule]
fn sqlrustler(_py: Python, module: &PyModule) -> PyResult<()>  {

    module.add_class::<config::DatabaseType>()?;
    module.add_class::<config::DatabaseConfig>()?;
    module.add_class::<transaction::DatabaseTransaction>()?;
    module.add_class::<connection::DatabaseConnection>()?;

    pyo3::prepare_freethreaded_python();
    Ok(())
}
