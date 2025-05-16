use uuid::Uuid;
use pyo3::prelude::*;

use crate::{context::get_sql_connect, transaction::DatabaseTransaction};

#[pyclass]
pub struct Session{
    context_id: String
}

#[pymethods]
impl Session {

    #[new]
    pub fn new(context_id: String) -> Self {
        Self { context_id }
    }

    pub fn __enter__(&self) -> DatabaseTransaction {
        // get connection
        todo!()
    }

    pub fn __exit__(&self) -> DatabaseTransaction {
        // return connection to pool
        todo!()
    }
}
