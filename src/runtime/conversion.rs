use pyo3::prelude::*;

pub(crate) enum FutureResultToPy {
    None,
    Err(PyResult<()>),
    PyAny(Py<PyAny>),
}

impl<'p> ToPyObject for FutureResultToPy {
    fn to_object(&self, py: Python<'_>) -> PyObject {
        match self {
            Self::None => py.None(),
            Self::Err(res) => match res {
                Ok(_) => py.None(),
                Err(err) => err.to_object(py),
            },
            Self::PyAny(inner) => inner.to_object(py),
        }
    }
}
