use pyo3::prelude::*;

mod config;
mod connection;
mod db_trait;
mod transaction;
mod session;
mod error;
mod  db_impl;
mod db_operations;

#[pymodule]
fn sqlrustler(_py: Python, module: &PyModule) -> PyResult<()>  {

    module.add_class::<config::DatabaseType>()?;
    module.add_class::<config::DatabaseConfig>()?;
    module.add_class::<transaction::TransactionWrapper>()?;
    module.add_class::<connection::DatabaseConnection>()?;
    module.add_class::<session::Session>()?;

    // add the functions to the module
    module.add_function(wrap_pyfunction!(connection::get_db_type_with_alias, module)?)?;

    pyo3::prepare_freethreaded_python();
    Ok(())
}
