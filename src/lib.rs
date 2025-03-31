use pyo3::prelude::*;

mod config;

#[pymodule(gil_used = false)]
fn sqlrustler(_py: Python, module: &Bound<PyModule>) -> PyResult<()>  {

    module.add_class::<config::DatabaseType>()?;
    module.add_class::<config::DatabaseConfig>()?;

    pyo3::prepare_freethreaded_python();
    Ok(())
}