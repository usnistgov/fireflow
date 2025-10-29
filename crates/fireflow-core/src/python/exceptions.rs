use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyWarning};
use pyo3::prelude::*;
use std::fmt;

create_exception!(
    _pyreflow,
    PyreflowException,
    PyException,
    "Exception created by internal pyreflow."
);

create_exception!(
    _pyreflow,
    PyreflowWarning,
    PyWarning,
    "Warning created by internal pyreflow."
);
