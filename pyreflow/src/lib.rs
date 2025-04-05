use fireflow_core::api;
use fireflow_core::config;
use fireflow_core::error;

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyWarning};
use pyo3::prelude::*;
use std::ffi::CString;
use std::path;

#[pymodule]
fn pyreflow(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("PyreflowException", py.get_type::<PyreflowException>())?;
    m.add("PyreflowWarning", py.get_type::<PyreflowWarning>())?;
    m.add_function(wrap_pyfunction!(read_fcs_header, m)?)?;
    m.add_function(wrap_pyfunction!(read_fcs_raw_text, m)?)
}

#[pyfunction]
fn read_fcs_header(p: path::PathBuf) -> PyResult<PyHeader> {
    handle_errors(api::read_fcs_header(&p))
}

#[pyfunction]
fn read_fcs_raw_text(p: path::PathBuf, conf: PyConfig) -> PyResult<PyRawTEXT> {
    handle_errors(api::read_fcs_raw_text(&p, &conf.0))
}

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
struct PyConfig(config::Config);

#[pyclass]
#[repr(transparent)]
struct PyHeader(api::Header);

impl From<api::Header> for PyHeader {
    fn from(value: api::Header) -> Self {
        Self(value)
    }
}

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
struct PyRawTEXT(api::RawTEXT);

impl From<api::RawTEXT> for PyRawTEXT {
    fn from(value: api::RawTEXT) -> Self {
        Self(value)
    }
}

// cheatcode to get around orphan rule
struct FailWrapper(error::ImpureFailure);

fn handle_errors<X, Y>(res: error::ImpureResult<X>) -> PyResult<Y>
where
    Y: From<X>,
{
    let succ = res.map_err(FailWrapper)?;
    let warn = succ.deferred.into_warnings();
    Python::with_gil(|py| -> PyResult<()> {
        let wt = py.get_type::<PyreflowWarning>();
        for w in warn {
            let s = CString::new(w)?;
            PyErr::warn(py, &wt, &s, 0)?;
        }
        Ok(())
    })?;
    Ok(succ.data.into())
}

impl From<error::ImpureFailure> for FailWrapper {
    fn from(value: error::ImpureFailure) -> Self {
        Self(value)
    }
}

impl From<FailWrapper> for PyErr {
    fn from(err: FailWrapper) -> Self {
        let inner = err.0;
        let reason = match inner.reason {
            error::ImpureError::IO(e) => format!("IO ERROR: {e}"),
            error::ImpureError::Pure(e) => format!("CRITICAL PYREFLOW ERROR: {e}"),
        };
        let deferred = inner.deferred.into_errors().join("\n");
        let msg = format!("{reason}\n\nOther errors encountered:\n{deferred}");
        PyreflowException::new_err(msg)
    }
}

create_exception!(
    pyreflow,
    PyreflowException,
    PyException,
    "Exception created by internal pyreflow."
);

create_exception!(
    pyreflow,
    PyreflowWarning,
    PyWarning,
    "Warning created by internal pyreflow."
);
