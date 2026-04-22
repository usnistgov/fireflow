use crate::config::EnumStrIter;
use crate::nonempty_string::NEStr;
use crate::{impl_str_enum, ne_str};

use fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString};

use derive_more::Display;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyWarning};

// Each of these docstrings needs to conform to PEP8 (72 chars or less) and
// follow sphinx formatting. They also refer to stuff in the .rst docs
// themselves on the python side. This isn't very elegant and there is hopefully
// a better way to do this. At least there aren't that many exceptions (for now)

create_exception!(
    _pyreflow,
    PyreflowError,
    PyException,
    "Base class for all exceptions raised by ``pyreflow``."
);

create_exception!(
    _pyreflow,
    FileLayoutError,
    PyreflowError,
    "Raised if FCS file was malformed.\n\
     \n\
     This includes:\n\
     \n\
     * invalid FCS version\n\
     * unparsable offsets in *HEADER*\n\
     * unparsable TEXT segment (primary and/or secondary)\n\
     * overlapping segment coordinates\n\
     * mismatches between indicated event number and actual size of *DATA*"
);

create_exception!(
    _pyreflow,
    ParseKeyError,
    PyreflowError,
    "Raised if key from *TEXT* could not be parsed from bytestring.\n\
     \n\
     This includes:\n\
     \n\
     * Standard keys not starting with a ``\"$\"``\n\
     * Non-standard keys starting with a ``\"$\"``\n\
     * blank keys\n\
     * keys already present\n\
     * keys with non-ASCII or non-UTF-8 characters"
);

create_exception!(
    _pyreflow,
    ParseKeywordValueError,
    PyreflowError,
    "Raised if keyword value could not be parsed from a string.\n\
     \n\
     The source string is that which is literally encoded in *TEXT*. The\n\
     final type for the conversion will depend on the keyword and is dictated\n\
     by its type in the standardized data structure (see :ref:`coretext` and\n\
     :ref:`coredataset`). For instance, *$ABRT* is an unsigned integer and\n\
     will raise this exception if string value for this keyword contains\n\
     invalid digits or if the resulting number is out of range.\n\
     \n\
     This exception will generally only be raised in standard mode, but may\n\
     also be raised in flat mode when the *DATA* segment needs to be read\n\
     (this requires parsing *$PnB*, *$PnR*, etc)."
);

create_exception!(
    _pyreflow,
    InvalidKeywordValueError,
    PyreflowError,
    "Raised if a standardized keyword value is incorrectly specified.\n\
     \n\
     The difference between :py:exc:`~pyreflow.ParseKeywordValueError` and\n\
     this error is that the former applies to string conversion, and this\n\
     applies to an invalid value within the keyword value's native type.\n\
     \n\
     This is mostly used when using class constructors to build the classes\n\
     from :ref:`coretext` and :ref:`coredataset` from scratch without reading\n\
     an FCS file.\n\
     \n\
     Furthermore, this is only needed for complicated keyword values whose\n\
     failure mode cannot be described by a build-in Python exception. For\n\
     instance, *$SPILLOVER* is a numpy matrix that must follow certain rules.\n\
     Violations of these rules will trigger this error."
);

create_exception!(
    _pyreflow,
    ExtraKeywordError,
    PyreflowError,
    "Raised when extra standard keywords are left unused in standard mode."
);

create_exception!(
    _pyreflow,
    ConversionError,
    PyreflowError,
    "Raised upon failure when converting between FCS versions.\n\
     \n\
     This covers two broad classes of failures:\n\
     \n\
     1. data is required in target version but not specified in source version\n\
     2. data in source version is incompatible with target version\n\
     \n\
     For (1), this generally happens if a keyword in the source version is\n\
     optional and missing and required in the target version (*$PnN* for\n\
     example when going from FCS 3.0 to FCS 3.1).\n\
     \n\
     For (2), this may/may not trigger this exception depending on user\n\
     configuration. In the non-fatal case, incompatible keys will be dropped\n\
     with a warning. In the fatal case, this exception will be raised since\n\
     dropping keywords is a destructive operation."
);

create_exception!(
    _pyreflow,
    RelationalError,
    PyreflowError,
    "Raised when a keyword's value is incorrect given its context.\n\
     \n\
     This can be triggered by the following (and more):\n\
     \n\
     1. keywords which reference other data which does not exist\n\
     2. attempting to remove a keyword on which data depends\n\
     3. mismatches between *$PnB*, *$PnR*, *$DATATYPE*, and *$PnDATATYPE*\n\
     4. specifying a temporal value to an optical measurement (and vice versa)\n\
     5. mismatched length between measurements, dataframe, and/or layout"
);

create_exception!(
    _pyreflow,
    EventDataError,
    PyreflowError,
    "Raised when values in *DATA* segment are invalid."
);

create_exception!(
    _pyreflow,
    DataLossError,
    PyreflowError,
    "Raised when values in *DATA* segment must be truncated.\n\
     \n\
     This can occur because the dataframe used to represent *DATA* is \n\
     allowed to contain arbitrary data types, but these must be coerced to\n\
     a given *DATA* layout when written to an FCS file. This coercion may\n\
     result in data loss, which is indicated by this error."
);

create_exception!(
    _pyreflow,
    ConfigError,
    PyreflowError,
    "Raised when a configuration value is invalid.\n\
     \n\
     This is used for values whose failure mode cannot be captured using a\n\
     built-in Python exception or another exception in ``pyreflow``."
);

create_exception!(
    _pyreflow,
    WriteFCSError,
    PyreflowError,
    "Raised when an FCS file cannot be written."
);

create_exception!(
    _pyreflow,
    PyreflowWarning,
    PyWarning,
    "Generic warning created by ``pyreflow``."
);

const I08: &NEStr = ne_str!("I08");
const I16: &NEStr = ne_str!("I16");
const I24: &NEStr = ne_str!("I24");
const I32: &NEStr = ne_str!("I32");
const I40: &NEStr = ne_str!("I40");
const I48: &NEStr = ne_str!("I48");
const I56: &NEStr = ne_str!("I56");
const I64: &NEStr = ne_str!("I64");

pub const COL_TYPE_ASCII: &NEStr = ne_str!("A");
pub const COL_TYPE_F32: &NEStr = ne_str!("F");
pub const COL_TYPE_F64: &NEStr = ne_str!("D");

impl_str_enum!(
    /// All supported integer widths.
    ///
    /// This is used to interpret that value of other python types (ie
    /// integers for $PnR) which have a specific width according to a
    /// layout.
    #[derive(Display)]
    #[display("{}", self.as_str())]
    #[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
    pub IntegerWidth,
    /// Error when parsing [`IntegerWidth`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
    pub IntegerWidthError,
    I08 => I08,
    I16 => I16,
    I24 => I24,
    I32 => I32,
    I40 => I40,
    I48 => I48,
    I56 => I56,
    I64 => I64
);

impl_str_enum!(
    /// All supported column types
    ///
    /// This is used to interpret that value of other python types (ie
    /// integers for $PnR) which have a specific width according to a
    /// layout.
    #[derive(Display)]
    #[display("{}", self.as_str())]
    #[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
    pub ColumnType,
    /// Error when parsing [`ColumnType`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
    pub ColumnTypeError,
    A => COL_TYPE_ASCII,
    F => COL_TYPE_F32,
    D => COL_TYPE_F64,
    I08 => I08,
    I16 => I16,
    I24 => I24,
    I32 => I32,
    I40 => I40,
    I48 => I48,
    I56 => I56,
    I64 => I64
);
