use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyWarning};

create_exception!(
    _pyreflow,
    PyreflowError,
    PyException,
    "Exception created by internal pyreflow."
);

create_exception!(
    _pyreflow,
    FileLayoutError,
    PyreflowError,
    "Exception caused by a malformed FCS file"
);

create_exception!(
    _pyreflow,
    ParseKeyError,
    PyreflowError,
    "Exception caused by parsing a standard or nonstandard key from string"
);

create_exception!(
    _pyreflow,
    ParseKeywordValueError,
    PyreflowError,
    "Exception caused by parsing a keyword from a string to its native type"
);

create_exception!(
    _pyreflow,
    InvalidKeywordValueError,
    PyreflowError,
    "Exception caused by an individual, invalid keyword assignment"
);

create_exception!(
    _pyreflow,
    ExtraKeywordError,
    PyreflowError,
    "Exception caused when extra standard keywords are found and not used"
);

create_exception!(
    _pyreflow,
    FCSDeprecatedError,
    PyreflowError,
    "Exception for FCS features/keywords which are deprecated"
);

create_exception!(
    _pyreflow,
    ConversionError,
    PyreflowError,
    "Exception caused by converting FCS data between versions"
);

create_exception!(
    _pyreflow,
    RelationalError,
    PyreflowError,
    "Exception caused by an FCS keyword that incorrectly references another"
);

create_exception!(
    _pyreflow,
    EventDataError,
    PyreflowError,
    "Exception caused by invalid values in DATA segment"
);

create_exception!(
    _pyreflow,
    DataLossError,
    PyreflowError,
    "Exception caused by loss of precision for values in DATA segment"
);

create_exception!(
    _pyreflow,
    ConfigError,
    PyreflowError,
    "Exception caused by invalid values for configuration"
);

create_exception!(
    _pyreflow,
    PyreflowWarning,
    PyWarning,
    "Warning created by internal pyreflow."
);
