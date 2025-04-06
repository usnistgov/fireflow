use fireflow_core::api;
use fireflow_core::config;
use fireflow_core::error;

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyWarning};
use pyo3::prelude::*;
use pyo3::types::IntoPyDict;
use pyo3::types::PyDict;
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
#[derive(Clone)]
struct PyConfig(config::Config);

#[pyclass]
struct PySegment(api::Segment);

#[pymethods]
impl PySegment {
    fn begin(&self) -> u32 {
        self.0.begin()
    }

    fn end(&self) -> u32 {
        self.0.end()
    }

    fn nbytes(&self) -> u32 {
        self.0.nbytes()
    }

    fn __repr__(&self) -> String {
        format!("({},{})", self.begin(), self.end())
    }
}

#[pyclass]
struct PyVersion(api::Version);

#[pymethods]
impl PyVersion {
    fn __repr__(&self) -> String {
        self.0.to_string()
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

#[pyclass]
#[repr(transparent)]
struct PyHeader(api::Header);

#[pymethods]
impl PyHeader {
    #[getter]
    fn version(&self) -> PyVersion {
        PyVersion(self.0.version)
    }

    #[getter]
    fn text(&self) -> PySegment {
        PySegment(self.0.text)
    }

    #[getter]
    fn data(&self) -> PySegment {
        PySegment(self.0.data)
    }

    #[getter]
    fn analysis(&self) -> PySegment {
        PySegment(self.0.analysis)
    }
}

impl From<api::Header> for PyHeader {
    fn from(value: api::Header) -> Self {
        Self(value)
    }
}

#[pyclass]
#[repr(transparent)]
#[derive(Clone)]
struct PyRawTEXT(api::RawTEXT);

#[pymethods]
impl PyRawTEXT {
    #[getter]
    fn version(&self) -> PyVersion {
        PyVersion(self.0.version)
    }

    #[getter]
    fn delimiter(&self) -> u8 {
        self.0.delimiter
    }

    #[getter]
    fn keywords<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        // TODO clone?
        self.0.keywords.clone().into_py_dict(py)
    }
}

impl From<api::RawTEXT> for PyRawTEXT {
    fn from(value: api::RawTEXT) -> Self {
        Self(value)
    }
}

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
