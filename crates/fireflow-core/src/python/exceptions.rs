use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyWarning};

create_exception!(
    _pyreflow,
    PyreflowException,
    PyException,
    "Exception created by internal pyreflow."
);

create_exception!(
    _pyreflow,
    MeasurementException,
    PyreflowException,
    "Exception caused by manipulating measurement vector"
);

create_exception!(
    _pyreflow,
    InvalidKeywordValueError,
    PyreflowException,
    "Exception caused by an individual, invalid keyword assignment"
);

create_exception!(
    _pyreflow,
    ConversionException,
    PyreflowException,
    "Exception caused by converting FCS data between versions"
);

create_exception!(
    _pyreflow,
    RelationalException,
    PyreflowException,
    "Exception caused by an FCS keyword that incorrectly references another"
);

create_exception!(
    _pyreflow,
    PyreflowWarning,
    PyWarning,
    "Warning created by internal pyreflow."
);
