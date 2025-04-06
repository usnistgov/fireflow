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
    m.add_class::<PyConfig>()?;
    m.add_function(wrap_pyfunction!(read_fcs_header, m)?)?;
    m.add_function(wrap_pyfunction!(read_fcs_raw_text, m)?)?;
    m.add_function(wrap_pyfunction!(read_fcs_std_text, m)?)
}

#[pyfunction]
fn read_fcs_header(p: path::PathBuf) -> PyResult<PyHeader> {
    handle_errors(api::read_fcs_header(&p))
}

#[pyfunction]
fn read_fcs_raw_text(p: path::PathBuf, conf: PyConfig) -> PyResult<PyRawTEXT> {
    handle_errors(api::read_fcs_raw_text(&p, &conf.0))
}

#[pyfunction]
fn read_fcs_std_text(p: path::PathBuf, conf: PyConfig) -> PyResult<PyStandardizedTEXT> {
    handle_errors(api::read_fcs_std_text(&p, &conf.0))
}

macro_rules! pywrap {
    ($pytype:ident, $rstype:path, $name:expr) => {
        #[pyclass]
        #[derive(Clone)]
        #[repr(transparent)]
        struct $pytype($rstype);

        impl From<$rstype> for $pytype {
            fn from(value: $rstype) -> Self {
                Self(value)
            }
        }

        impl From<$pytype> for $rstype {
            fn from(value: $pytype) -> Self {
                value.0
            }
        }
    };
}

pywrap!(PyConfig, config::Config, "Config");
pywrap!(PySegment, api::Segment, "Segment");
pywrap!(PyVersion, api::Version, "Version");
pywrap!(PyHeader, api::Header, "Header");
pywrap!(PyRawTEXT, api::RawTEXT, "RawTEXT");
pywrap!(PyOffsets, api::Offsets, "Offsets");
pywrap!(
    PyStandardizedTEXT,
    api::StandardizedTEXT,
    "StandardizedTEXT"
);
pywrap!(PyAnyCoreTEXT, api::AnyCoreTEXT, "AnyCoreTEXT");

#[pymethods]
impl PyConfig {
    #[new]
    fn new() -> Self {
        // TODO make this more interesting...
        config::Config::default().into()
    }
}

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

#[pymethods]
impl PyVersion {
    fn __repr__(&self) -> String {
        self.0.to_string()
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

#[pymethods]
impl PyHeader {
    #[getter]
    fn version(&self) -> PyVersion {
        self.0.version.into()
    }

    #[getter]
    fn text(&self) -> PySegment {
        self.0.text.into()
    }

    #[getter]
    fn data(&self) -> PySegment {
        self.0.data.into()
    }

    #[getter]
    fn analysis(&self) -> PySegment {
        self.0.analysis.into()
    }
}

#[pymethods]
impl PyRawTEXT {
    #[getter]
    fn version(&self) -> PyVersion {
        self.0.version.into()
    }

    #[getter]
    fn offsets(&self) -> PyOffsets {
        self.0.offsets.clone().into()
    }

    #[getter]
    fn delimiter(&self) -> u8 {
        self.0.delimiter
    }

    #[getter]
    fn keywords<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.0.keywords.clone().into_py_dict(py)
    }
}

#[pymethods]
impl PyOffsets {
    #[getter]
    fn prim_text(&self) -> PySegment {
        self.0.prim_text.into()
    }

    #[getter]
    fn supp_text(&self) -> Option<PySegment> {
        self.0.supp_text.map(|x| x.into())
    }

    #[getter]
    fn data(&self) -> PySegment {
        self.0.data.into()
    }

    #[getter]
    fn analysis(&self) -> PySegment {
        self.0.analysis.into()
    }

    #[getter]
    fn nextdata(&self) -> Option<u32> {
        self.0.nextdata
    }
}

#[pymethods]
impl PyStandardizedTEXT {
    #[getter]
    fn offsets(&self) -> PyOffsets {
        self.0.offsets.clone().into()
    }

    #[getter]
    fn delimiter(&self) -> u8 {
        self.0.delimiter
    }

    #[getter]
    fn keywords(&self) -> PyAnyCoreTEXT {
        self.0.keywords.clone().into()
    }

    // TODO do I really need to expose this?
    #[getter]
    fn remainder<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.0.remainder.clone().into_py_dict(py)
    }
}

#[pymethods]
impl PyAnyCoreTEXT {
    #[getter]
    fn version(&self) -> PyVersion {
        self.0.version().into()
    }

    #[getter]
    // TODO this will be in arbitrary order, might make sense to sort it
    //
    // TODO also might make sense to add flags which can filter the results
    // (required, optional, measurements, non-measurements, etc)
    fn raw_keywords<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.0.raw_keywords().clone().into_py_dict(py)
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
