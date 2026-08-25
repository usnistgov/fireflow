use crate::{
    config::EnumStrIter as _,
    nonempty_string::NEStr,
    {impl_str_enum, ne_str},
};

use fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString};

use derive_more::Display;
use pyo3::{
    create_exception,
    exceptions::{PyException, PyWarning},
};

// Each of these docstrings needs to conform to PEP8 (72 chars or less) and
// follow sphinx formatting. They also refer to stuff in the .rst docs
// themselves on the python side. This isn't very elegant and there is hopefully
// a better way to do this. At least there aren't that many exceptions (for now)

create_exception!(
    pyreflow,
    PyreflowError,
    PyException,
    "Base class for all exceptions raised by ``pyreflow``."
);

create_exception!(
    pyreflow,
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
    pyreflow,
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
    pyreflow,
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
    pyreflow,
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
    pyreflow,
    ExtraKeywordError,
    PyreflowError,
    "Raised when extra standard keywords are left unused in standard mode."
);

create_exception!(
    pyreflow,
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
    pyreflow,
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
    pyreflow,
    EventDataError,
    PyreflowError,
    "Raised when values in *DATA* segment are invalid."
);

create_exception!(
    pyreflow,
    DataLossError,
    PyreflowError,
    "Raised when values in *DATA* segment must be truncated.\n\
     \n\
     This can occur because input data is the wrong type for the target data\n\
     schema or the data is out of range."
);

create_exception!(
    pyreflow,
    ConfigError,
    PyreflowError,
    "Raised when a configuration value is invalid.\n\
     \n\
     This is used for values whose failure mode cannot be captured using a\n\
     built-in Python exception or another exception in ``pyreflow``."
);

create_exception!(
    pyreflow,
    WriteFCSError,
    PyreflowError,
    "Raised when an FCS file cannot be written."
);

create_exception!(
    pyreflow,
    PyreflowWarning,
    PyWarning,
    "Generic warning created by ``pyreflow``."
);

// Identifiers for column data types

const U08: &NEStr = ne_str!("U08");
const U16: &NEStr = ne_str!("U16");
const U24: &NEStr = ne_str!("U24");
const U32: &NEStr = ne_str!("U32");
const U40: &NEStr = ne_str!("U40");
const U48: &NEStr = ne_str!("U48");
const U56: &NEStr = ne_str!("U56");
const U64: &NEStr = ne_str!("U64");

pub const COL_TYPE_ASCII: &NEStr = ne_str!("A");
pub const COL_TYPE_F32: &NEStr = ne_str!("F32");
pub const COL_TYPE_F64: &NEStr = ne_str!("F64");

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
    U08 => U08,
    U16 => U16,
    U24 => U24,
    U32 => U32,
    U40 => U40,
    U48 => U48,
    U56 => U56,
    U64 => U64
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
    F32 => COL_TYPE_F32,
    F64 => COL_TYPE_F64,
    U08 => U08,
    U16 => U16,
    U24 => U24,
    U32 => U32,
    U40 => U40,
    U48 => U48,
    U56 => U56,
    U64 => U64
);

// Supplemental TEXT output enum

pub const SUPP_OFFSET_ORIGIN_EMPTY_LEVEL: &NEStr = ne_str!("empty");
pub const SUPP_OFFSET_ORIGIN_UNPARSED_LEVEL: &NEStr = ne_str!("unparsed");
pub const SUPP_OFFSET_ORIGIN_MALFORMED_LEVEL: &NEStr = ne_str!("malformed");
pub const SUPP_OFFSET_ORIGIN_DUP_PTEXT_LEVEL: &NEStr = ne_str!("dup_ptext");
pub const SUPP_OFFSET_ORIGIN_DUP_ANALYSIS_LEVEL: &NEStr = ne_str!("dup_analysis");
pub const SUPP_OFFSET_ORIGIN_IGNORED_LEVEL: &NEStr = ne_str!("ignored");
pub const SUPP_OFFSET_ORIGIN_DUP_OTHER_LEVEL: &NEStr = ne_str!("dup_other");
pub const SUPP_OFFSET_ORIGIN_VALID_LEVEL: &NEStr = ne_str!("valid");

impl_str_enum!(
    #[derive(Display)]
    #[display("{}", self.as_str())]
    #[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
    pub SuppTEXTOffsetOriginType,
    /// Error when parsing [`SuppTEXTOffsetOriginLevel`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
    pub SuppTEXTOffsetOriginLevelError,
    Empty                 => SUPP_OFFSET_ORIGIN_EMPTY_LEVEL,
    Unparsed              => SUPP_OFFSET_ORIGIN_UNPARSED_LEVEL,
    Malformed             => SUPP_OFFSET_ORIGIN_MALFORMED_LEVEL,
    DuplicatesPrimaryTEXT => SUPP_OFFSET_ORIGIN_DUP_PTEXT_LEVEL,
    DuplicatesAnalysis    => SUPP_OFFSET_ORIGIN_DUP_ANALYSIS_LEVEL,
    Ignored               => SUPP_OFFSET_ORIGIN_IGNORED_LEVEL,
    DuplicatesOther       => SUPP_OFFSET_ORIGIN_DUP_OTHER_LEVEL,
    Valid                 => SUPP_OFFSET_ORIGIN_VALID_LEVEL
);

// TEXT offset origin enum

pub const TEXT_OFFSET_ORIGIN_EMPTY_TEXT_LEVEL: &NEStr = ne_str!("empty_text");
pub const TEXT_OFFSET_ORIGIN_IGNORED_LEVEL: &NEStr = ne_str!("ignored");
pub const TEXT_OFFSET_ORIGIN_UNPARSED_LEVEL: &NEStr = ne_str!("unparsed");
pub const TEXT_OFFSET_ORIGIN_MALFORMED_LEVEL: &NEStr = ne_str!("malformed");
pub const TEXT_OFFSET_ORIGIN_MATCH_LEVEL: &NEStr = ne_str!("match");
pub const TEXT_OFFSET_ORIGIN_MISMATCH_HEADER_LEVEL: &NEStr = ne_str!("mismatch_header");
pub const TEXT_OFFSET_ORIGIN_MISMATCH_TEXT_LEVEL: &NEStr = ne_str!("mismatch_text");
pub const TEXT_OFFSET_ORIGIN_EMPTY_HEADER_LEVEL: &NEStr = ne_str!("empty_header");

impl_str_enum!(
    #[derive(Display)]
    #[display("{}", self.as_str())]
    #[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
    pub TEXTOffsetOriginType,
    /// Error when parsing [`TEXTOffsetOriginLevel`] from [`String`]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
    pub TEXTOffsetOriginLevelError,
    EmptyTEXT      => TEXT_OFFSET_ORIGIN_EMPTY_TEXT_LEVEL,
    Ignored        => TEXT_OFFSET_ORIGIN_IGNORED_LEVEL,
    Unparsed       => TEXT_OFFSET_ORIGIN_UNPARSED_LEVEL,
    Malformed      => TEXT_OFFSET_ORIGIN_MALFORMED_LEVEL,
    Match          => TEXT_OFFSET_ORIGIN_MATCH_LEVEL,
    MismatchHeader => TEXT_OFFSET_ORIGIN_MISMATCH_HEADER_LEVEL,
    MismatchTEXT   => TEXT_OFFSET_ORIGIN_MISMATCH_TEXT_LEVEL,
    EmptyHeader    => TEXT_OFFSET_ORIGIN_EMPTY_HEADER_LEVEL
);

// Segment name constants

pub const SELECTOR_IF: &NEStr = ne_str!("if");
pub const SELECTOR_COND: &NEStr = ne_str!("cond");

pub const SEGMENT_NAME_TEXT: &NEStr = ne_str!("text");
pub const SEGMENT_NAME_STEXT: &NEStr = ne_str!("supp_text");
pub const SEGMENT_NAME_DATA: &NEStr = ne_str!("data");
pub const SEGMENT_NAME_ANALYSIS: &NEStr = ne_str!("analysis");

// Selector operators

pub const CONDITION_AND: &NEStr = ne_str!("and");
pub const CONDITION_OR: &NEStr = ne_str!("or");
pub const CONDITION_NOT: &NEStr = ne_str!("not");

pub const STATEMENT_HAS_KEY: &NEStr = ne_str!("has_key");
pub const STATEMENT_KEY_IS: &NEStr = ne_str!("key_is");
pub const STATEMENT_KEY_MATCHES: &NEStr = ne_str!("key_matches");
