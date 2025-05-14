use fireflow_core::api::VersionedTime;
use fireflow_core::api::{self};
use fireflow_core::config::Strict;
use fireflow_core::config::{self, OffsetCorrection};
use fireflow_core::error;
use fireflow_core::text::byteord::*;
use fireflow_core::text::ranged_float::*;
use fireflow_core::text::scale::*;
use fireflow_core::text::spillover::*;
use fireflow_core::validated::dataframe::*;
use fireflow_core::validated::datepattern::DatePattern;
use fireflow_core::validated::nonstandard::*;
use fireflow_core::validated::pattern::*;
use fireflow_core::validated::shortname::*;
use fireflow_core::validated::textdelim::TEXTDelim;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use itertools::Itertools;
use nonempty::NonEmpty;
use numpy::{PyArray2, PyReadonlyArray2, ToPyArray};
use polars::prelude::*;
use polars_arrow::array::PrimitiveArray;
use pyo3::class::basic::CompareOp;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyWarning};
use pyo3::prelude::*;
use pyo3::type_object::PyTypeInfo;
use pyo3::types::IntoPyDict;
use pyo3::types::PyDict;
use pyo3::IntoPyObjectExt;
use pyo3_polars::PyDataFrame;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::ffi::CString;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path;

#[pymodule]
fn pyreflow(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("PyreflowException", py.get_type::<PyreflowException>())?;
    m.add("PyreflowWarning", py.get_type::<PyreflowWarning>())?;
    m.add_class::<PyTEXTDelim>()?;
    m.add_class::<PyNonStdMeasPattern>()?;
    m.add_class::<PyDatePattern>()?;
    m.add_class::<PyShortname>()?;
    m.add_function(wrap_pyfunction!(read_fcs_header, m)?)?;
    m.add_function(wrap_pyfunction!(read_fcs_raw_text, m)?)?;
    m.add_function(wrap_pyfunction!(read_fcs_std_text, m)?)?;
    m.add_function(wrap_pyfunction!(read_fcs_file, m)?)
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (p, begin_text=0, end_text=0, begin_data=0, end_data=0,
                    begin_analysis=0, end_analysis=0, version_override=None))]
fn read_fcs_header(
    p: path::PathBuf,
    begin_text: i32,
    end_text: i32,
    begin_data: i32,
    end_data: i32,
    begin_analysis: i32,
    end_analysis: i32,
    version_override: Option<PyVersion>,
) -> PyResult<PyHeader> {
    let conf = config::HeaderConfig {
        version_override: version_override.map(|x| x.0),
        text: config::OffsetCorrection {
            begin: begin_text,
            end: end_text,
        },
        data: config::OffsetCorrection {
            begin: begin_data,
            end: end_data,
        },
        analysis: config::OffsetCorrection {
            begin: begin_analysis,
            end: end_analysis,
        },
    };
    handle_errors(api::read_fcs_header(&p, &conf))
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    p,

    strict=false,

    begin_text=0,
    end_text=0,
    begin_data=0,
    end_data=0,
    begin_analysis=0,
    end_analysis=0,

    text_begin_stext=0,
    text_end_stext=0,
    allow_double_delim=false,
    force_ascii_delim=false,
    enforce_final_delim=false,
    enforce_unique=false,
    enforce_even=false,
    enforce_nonempty=false,
    error_on_invalid_utf8=false,
    enforce_keyword_ascii=false,
    enforce_stext=false,
    repair_offset_spaces=false,
    date_pattern=None,
    version_override=None)
)]
fn read_fcs_raw_text(
    p: path::PathBuf,

    strict: bool,

    begin_text: i32,
    end_text: i32,
    begin_data: i32,
    end_data: i32,
    begin_analysis: i32,
    end_analysis: i32,

    text_begin_stext: i32,
    text_end_stext: i32,
    allow_double_delim: bool,
    force_ascii_delim: bool,
    enforce_final_delim: bool,
    enforce_unique: bool,
    enforce_even: bool,
    enforce_nonempty: bool,
    error_on_invalid_utf8: bool,
    enforce_keyword_ascii: bool,
    enforce_stext: bool,
    repair_offset_spaces: bool,
    date_pattern: Option<PyDatePattern>,
    version_override: Option<PyVersion>,
) -> PyResult<PyRawTEXT> {
    let header = config::HeaderConfig {
        version_override: version_override.map(|x| x.0),
        text: config::OffsetCorrection {
            begin: begin_text,
            end: end_text,
        },
        data: config::OffsetCorrection {
            begin: begin_data,
            end: end_data,
        },
        analysis: config::OffsetCorrection {
            begin: begin_analysis,
            end: end_analysis,
        },
    };

    let conf = config::RawTextReadConfig {
        header,
        stext: config::OffsetCorrection {
            begin: text_begin_stext,
            end: text_end_stext,
        },
        allow_double_delim,
        force_ascii_delim,
        enforce_final_delim,
        enforce_unique,
        enforce_even,
        enforce_nonempty,
        error_on_invalid_utf8,
        enforce_keyword_ascii,
        enforce_stext,
        repair_offset_spaces,
        date_pattern: date_pattern.map(|x| x.0),
    };
    handle_errors(api::read_fcs_raw_text(&p, &conf.set_strict(strict)))
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    p,

    strict=false,

    begin_text=0,
    end_text=0,
    begin_data=0,
    end_data=0,
    begin_analysis=0,
    end_analysis=0,

    text_begin_stext=0,
    text_end_stext=0,
    allow_double_delim=false,
    force_ascii_delim=false,
    enforce_final_delim=false,
    enforce_unique=false,
    enforce_even=false,
    enforce_nonempty=false,
    error_on_invalid_utf8=false,
    enforce_keyword_ascii=false,
    enforce_stext=false,
    repair_offset_spaces=false,
    disallow_deprecated=false,

    time_ensure=false,
    time_ensure_timestep=false,
    time_ensure_linear=false,
    time_ensure_nogain=false,
    disallow_deviant=false,
    disallow_nonstandard=false,

    shortname_prefix=None,
    nonstandard_measurement_pattern=None,
    time_pattern=None,
    date_pattern=None,
    version_override=None)
)]
fn read_fcs_std_text(
    py: Python<'_>,

    p: path::PathBuf,

    strict: bool,

    begin_text: i32,
    end_text: i32,
    begin_data: i32,
    end_data: i32,
    begin_analysis: i32,
    end_analysis: i32,

    text_begin_stext: i32,
    text_end_stext: i32,
    allow_double_delim: bool,
    force_ascii_delim: bool,
    enforce_final_delim: bool,
    enforce_unique: bool,
    enforce_even: bool,
    enforce_nonempty: bool,
    error_on_invalid_utf8: bool,
    enforce_keyword_ascii: bool,
    enforce_stext: bool,
    repair_offset_spaces: bool,
    disallow_deprecated: bool,

    time_ensure: bool,
    time_ensure_timestep: bool,
    time_ensure_linear: bool,
    time_ensure_nogain: bool,

    disallow_deviant: bool,
    disallow_nonstandard: bool,

    shortname_prefix: Option<PyShortnamePrefix>,
    nonstandard_measurement_pattern: Option<PyNonStdMeasPattern>,
    time_pattern: Option<PyTimePattern>,
    date_pattern: Option<PyDatePattern>,
    version_override: Option<PyVersion>,
) -> PyResult<(Bound<'_, PyAny>, PyParseParameters, Bound<'_, PyDict>)> {
    let header = config::HeaderConfig {
        version_override: version_override.map(|x| x.0),
        text: config::OffsetCorrection {
            begin: begin_text,
            end: end_text,
        },
        data: config::OffsetCorrection {
            begin: begin_data,
            end: end_data,
        },
        analysis: config::OffsetCorrection {
            begin: begin_analysis,
            end: end_analysis,
        },
    };

    let raw = config::RawTextReadConfig {
        header,
        stext: config::OffsetCorrection {
            begin: text_begin_stext,
            end: text_end_stext,
        },
        allow_double_delim,
        force_ascii_delim,
        enforce_final_delim,
        enforce_unique,
        enforce_even,
        enforce_nonempty,
        error_on_invalid_utf8,
        enforce_keyword_ascii,
        enforce_stext,
        repair_offset_spaces,
        date_pattern: date_pattern.map(|x| x.0),
    };

    let conf = config::StdTextReadConfig {
        raw,
        shortname_prefix: shortname_prefix.map(|x| x.0).unwrap_or_default(),
        time: config::TimeConfig {
            pattern: time_pattern.map(|x| x.0),
            ensure: time_ensure,
            ensure_timestep: time_ensure_timestep,
            ensure_linear: time_ensure_linear,
            ensure_nogain: time_ensure_nogain,
        },
        disallow_deviant,
        disallow_nonstandard,
        disallow_deprecated,
        nonstandard_measurement_pattern: nonstandard_measurement_pattern.map(|x| x.0),
    };

    let out: api::StandardizedTEXT =
        handle_errors(api::read_fcs_std_text(&p, &conf.set_strict(strict)))?;

    let text = match &out.standardized {
        // TODO this copies all data from the "union type" into a new
        // version-specific type. This might not be a big deal, but these
        // types might be rather large with lots of strings.
        api::AnyCoreTEXT::FCS2_0(x) => PyCoreTEXT2_0::from((**x).clone()).into_bound_py_any(py),
        api::AnyCoreTEXT::FCS3_0(x) => PyCoreTEXT3_0::from((**x).clone()).into_bound_py_any(py),
        api::AnyCoreTEXT::FCS3_1(x) => PyCoreTEXT3_1::from((**x).clone()).into_bound_py_any(py),
        api::AnyCoreTEXT::FCS3_2(x) => PyCoreTEXT3_2::from((**x).clone()).into_bound_py_any(py),
    }?;

    Ok((text, out.parse.into(), out.deviant.into_py_dict(py)?))
}

#[allow(clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (
    p,

    strict=false,

    header_begin_text=0,
    header_end_text=0,
    header_begin_data=0,
    header_end_data=0,
    header_begin_analysis=0,
    header_end_analysis=0,
    text_begin_stext=0,
    text_end_stext=0,
    text_begin_data=0,
    text_end_data=0,
    text_begin_analysis=0,
    text_end_analysis=0,

    allow_double_delim=false,
    force_ascii_delim=false,
    enforce_final_delim=false,
    enforce_unique=false,
    enforce_even=false,
    enforce_nonempty=false,
    error_on_invalid_utf8=false,
    enforce_keyword_ascii=false,
    enforce_stext=false,
    repair_offset_spaces=false,
    disallow_deprecated=false,

    time_ensure=false,
    time_ensure_timestep=false,
    time_ensure_linear=false,
    time_ensure_nogain=false,

    disallow_deviant=false,
    disallow_nonstandard=false,
    enforce_data_width_divisibility=false,
    enforce_matching_tot=false,

    shortname_prefix=None,
    nonstandard_measurement_pattern=None,
    time_pattern=None,
    date_pattern=None,
    version_override=None)
)]
fn read_fcs_file(
    py: Python<'_>,

    p: path::PathBuf,

    strict: bool,

    header_begin_text: i32,
    header_end_text: i32,
    header_begin_data: i32,
    header_end_data: i32,
    header_begin_analysis: i32,
    header_end_analysis: i32,

    text_begin_stext: i32,
    text_end_stext: i32,
    text_begin_data: i32,
    text_end_data: i32,
    text_begin_analysis: i32,
    text_end_analysis: i32,

    allow_double_delim: bool,
    force_ascii_delim: bool,
    enforce_final_delim: bool,
    enforce_unique: bool,
    enforce_even: bool,
    enforce_nonempty: bool,
    error_on_invalid_utf8: bool,
    enforce_keyword_ascii: bool,
    enforce_stext: bool,
    repair_offset_spaces: bool,
    disallow_deprecated: bool,

    time_ensure: bool,
    time_ensure_timestep: bool,
    time_ensure_linear: bool,
    time_ensure_nogain: bool,

    disallow_deviant: bool,
    disallow_nonstandard: bool,
    enforce_data_width_divisibility: bool,
    enforce_matching_tot: bool,

    shortname_prefix: Option<PyShortnamePrefix>,
    nonstandard_measurement_pattern: Option<PyNonStdMeasPattern>,
    time_pattern: Option<PyTimePattern>,
    date_pattern: Option<PyDatePattern>,
    version_override: Option<PyVersion>,
) -> PyResult<(Bound<'_, PyAny>, PyParseParameters, Bound<'_, PyDict>)> {
    let header = config::HeaderConfig {
        version_override: version_override.map(|x| x.0),
        text: config::OffsetCorrection {
            begin: header_begin_text,
            end: header_end_text,
        },
        data: config::OffsetCorrection {
            begin: header_begin_data,
            end: header_end_data,
        },
        analysis: config::OffsetCorrection {
            begin: header_begin_analysis,
            end: header_end_analysis,
        },
    };

    let raw = config::RawTextReadConfig {
        header,
        stext: config::OffsetCorrection {
            begin: text_begin_stext,
            end: text_end_stext,
        },
        allow_double_delim,
        force_ascii_delim,
        enforce_final_delim,
        enforce_unique,
        enforce_even,
        enforce_nonempty,
        error_on_invalid_utf8,
        enforce_keyword_ascii,
        enforce_stext,
        repair_offset_spaces,
        date_pattern: date_pattern.map(|x| x.0),
    };

    let standard = config::StdTextReadConfig {
        raw,
        shortname_prefix: shortname_prefix.map(|x| x.0).unwrap_or_default(),
        time: config::TimeConfig {
            pattern: time_pattern.map(|x| x.0),
            ensure: time_ensure,
            ensure_timestep: time_ensure_timestep,
            ensure_linear: time_ensure_linear,
            ensure_nogain: time_ensure_nogain,
        },
        disallow_deviant,
        disallow_deprecated,
        disallow_nonstandard,
        nonstandard_measurement_pattern: nonstandard_measurement_pattern.map(|x| x.0),
    };

    let conf = config::DataReadConfig {
        standard,
        data: OffsetCorrection {
            begin: text_begin_data,
            end: text_end_data,
        },
        analysis: OffsetCorrection {
            begin: text_begin_analysis,
            end: text_end_analysis,
        },
        enforce_data_width_divisibility,
        enforce_matching_tot,
    };

    let out: api::StandardizedDataset =
        handle_errors(api::read_fcs_file(&p, &conf.set_strict(strict)))?;

    let dataset = match &out.dataset {
        // TODO this copies all data from the "union type" into a new
        // version-specific type. This might not be a big deal, but these
        // types might be rather large with lots of strings.
        api::AnyCoreDataset::FCS2_0(x) => PyCoreDataset2_0::from(x.clone()).into_bound_py_any(py),
        api::AnyCoreDataset::FCS3_0(x) => PyCoreDataset3_0::from(x.clone()).into_bound_py_any(py),
        api::AnyCoreDataset::FCS3_1(x) => PyCoreDataset3_1::from(x.clone()).into_bound_py_any(py),
        api::AnyCoreDataset::FCS3_2(x) => PyCoreDataset3_2::from(x.clone()).into_bound_py_any(py),
    }?;

    Ok((dataset, out.parse.into(), out.deviant.into_py_dict(py)?))
}

macro_rules! pywrap {
    ($pytype:ident, $rstype:path, $name:expr) => {
        #[pyclass(name = $name)]
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

macro_rules! py_eq {
    ($pytype:ident) => {
        impl PartialEq for $pytype {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }

        impl Eq for $pytype {}

        #[pymethods]
        impl $pytype {
            fn __eq__(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }
    };
}

macro_rules! py_ord {
    ($pytype:ident) => {
        impl PartialEq for $pytype {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }

        impl Eq for $pytype {}

        impl Ord for $pytype {
            fn cmp(&self, other: &Self) -> Ordering {
                self.0.cmp(&other.0)
            }
        }

        impl PartialOrd for $pytype {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.0.cmp(&other.0))
            }
        }

        #[pymethods]
        impl $pytype {
            fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
                op.matches(self.0.cmp(&other.0))
            }
        }
    };
}

macro_rules! py_disp {
    ($pytype:ident) => {
        impl fmt::Display for $pytype {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                self.0.fmt(f)
            }
        }

        #[pymethods]
        impl $pytype {
            fn __repr__(&self) -> String {
                let name = <$pytype as PyTypeInfo>::NAME;
                format!("{}({})", name, self.0.to_string())
            }
        }
    };
}

macro_rules! py_parse {
    ($pytype:ident, $from:ident) => {
        #[pymethods]
        impl $pytype {
            #[new]
            fn new(s: String) -> PyResult<$pytype> {
                s.parse::<$from>()
                    .map($pytype::from)
                    .map_err(|e| PyreflowException::new_err(e.to_string()))
            }
        }
    };
}

pywrap!(PySegment, api::Segment, "Segment");
pywrap!(PyVersion, api::Version, "Version");
pywrap!(PyHeader, api::Header, "Header");
pywrap!(PyRawTEXT, api::RawTEXT, "RawTEXT");
pywrap!(PyOffsets, api::ParseParameters, "Offsets");
pywrap!(PyParseParameters, api::ParseParameters, "ParseParameters");

pywrap!(PyCoreTEXT2_0, api::CoreTEXT2_0, "CoreTEXT2_0");
pywrap!(PyCoreTEXT3_0, api::CoreTEXT3_0, "CoreTEXT3_0");
pywrap!(PyCoreTEXT3_1, api::CoreTEXT3_1, "CoreTEXT3_1");
pywrap!(PyCoreTEXT3_2, api::CoreTEXT3_2, "CoreTEXT3_2");

pywrap!(PyCoreDataset2_0, api::CoreDataset2_0, "CoreDataset2_0");
pywrap!(PyCoreDataset3_0, api::CoreDataset3_0, "CoreDataset3_0");
pywrap!(PyCoreDataset3_1, api::CoreDataset3_1, "CoreDataset3_1");
pywrap!(PyCoreDataset3_2, api::CoreDataset3_2, "CoreDataset3_2");

pywrap!(PyMeasurement2_0, api::Measurement2_0, "Measurement2_0");
pywrap!(PyMeasurement3_0, api::Measurement3_0, "Measurement3_0");
pywrap!(PyMeasurement3_1, api::Measurement3_1, "Measurement3_1");
pywrap!(PyMeasurement3_2, api::Measurement3_2, "Measurement3_2");

pywrap!(PyTimeChannel2_0, api::TimeChannel2_0, "TimeChannel2_0");
pywrap!(PyTimeChannel3_0, api::TimeChannel3_0, "TimeChannel3_0");
pywrap!(PyTimeChannel3_1, api::TimeChannel3_1, "TimeChannel3_1");
pywrap!(PyTimeChannel3_2, api::TimeChannel3_2, "TimeChannel3_2");

pywrap!(PyDatePattern, DatePattern, "DatePattern");

pywrap!(PyShortname, Shortname, "Shortname");
pywrap!(PyNumRangeSetter, api::NumRangeSetter, "NumRangeSetter");
pywrap!(
    PyAsciiRangeSetter,
    api::AsciiRangeSetter,
    "AsciiRangeSetter"
);
pywrap!(
    PyMixedColumnSetter,
    api::MixedColumnSetter,
    "MixedColumnSetter"
);
pywrap!(PyTimePattern, TimePattern, "TimePattern");
pywrap!(PyUnicode, api::Unicode, "Unicode");
pywrap!(PyShortnamePrefix, ShortnamePrefix, "ShortnamePrefix");
pywrap!(PyNonStdMeasPattern, NonStdMeasPattern, "NonStdMeasPattern");
pywrap!(PyNonStdMeasKey, NonStdMeasKey, "NonStdMeasKey");
pywrap!(PyNonStdKey, NonStdKey, "NonStdKey");
pywrap!(PyTEXTDelim, TEXTDelim, "TEXTDelim");
pywrap!(PyCytSetter, MetaKwSetter<api::Cyt>, "CytSetter");
pywrap!(PyCalibration3_1, api::Calibration3_1, "Calibration3_1");
pywrap!(PyCalibration3_2, api::Calibration3_2, "Calibration3_2");
pywrap!(PyMeasurementType, api::MeasurementType, "MeasurementType");
pywrap!(PyFeature, api::Feature, "Feature");
pywrap!(PyPositiveFloat, PositiveFloat, "PositiveFloat");
pywrap!(PyNonNegFloat, NonNegFloat, "NonNegFloat");

impl From<api::DetectorVoltage> for PyNonNegFloat {
    fn from(value: api::DetectorVoltage) -> Self {
        value.0.into()
    }
}

impl From<PyNonNegFloat> for api::DetectorVoltage {
    fn from(value: PyNonNegFloat) -> Self {
        value.0.into()
    }
}

impl From<api::Gain> for PyPositiveFloat {
    fn from(value: api::Gain) -> Self {
        value.0.into()
    }
}

impl From<PyPositiveFloat> for api::Gain {
    fn from(value: PyPositiveFloat) -> Self {
        value.0.into()
    }
}

impl From<api::Vol> for PyNonNegFloat {
    fn from(value: api::Vol) -> Self {
        value.0.into()
    }
}

impl From<PyNonNegFloat> for api::Vol {
    fn from(value: PyNonNegFloat) -> Self {
        value.0.into()
    }
}

pywrap!(PyEndian, Endian, "Endian");
pywrap!(PyByteOrd, ByteOrd, "ByteOrd");
pywrap!(PyMode, api::Mode, "Mode");
pywrap!(PyOriginality, api::Originality, "Originality");
pywrap!(PyAlphaNumType, api::AlphaNumType, "AlphaNumType");
pywrap!(PyScale, Scale, "Scale");
pywrap!(PySpillover, Spillover, "Spillover");

py_parse!(PyDatePattern, DatePattern);
py_disp!(PyDatePattern);

py_parse!(PyShortname, Shortname);
py_disp!(PyShortname);
py_eq!(PyShortname);

impl Hash for PyShortname {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

py_parse!(PyNonStdMeasPattern, NonStdMeasPattern);
py_disp!(PyNonStdMeasPattern);

#[pymethods]
impl PyTEXTDelim {
    #[new]
    fn new(x: u8) -> PyResult<PyTEXTDelim> {
        TEXTDelim::new(x)
            .map(PyTEXTDelim::from)
            .map_err(|e| PyreflowException::new_err(e.to_string()))
    }
}

py_ord!(PyVersion);
py_disp!(PyVersion);

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
        self.0.parse.clone().into()
    }

    // TODO this is a gotcha because if someone tries to modify a keyword like
    // 'std.keywords.cells = "2112"' then it the modification will actually be
    // done to a copy of 'keywords' rather than 'std'.
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

macro_rules! get_set_str {
    ($pytype:ident, $($rest:ident,)+ [$($root:ident),*], $get:ident, $set:ident, $field:ident) => {
        get_set_str!($pytype, [$($root),*], $get, $set, $field);
        get_set_str!($($rest,)+ [$($root),*], $get, $set, $field);
    };

    ($pytype:ident, [$($root:ident),*], $get:ident, $set:ident, $field:ident) => {
        #[pymethods]
        impl $pytype {
            #[getter]
            fn $get(&self) -> Option<String> {
                self.0.metadata.$($root.)*$field.as_ref_opt().map(|x| x.clone().into())
            }

            #[setter]
            fn $set(&mut self, s: Option<String>) {
                self.0.metadata.$($root.)*$field = s.map(|x| x.into()).into()
            }
        }
    };

    ($($pytype:ident),*; $get:ident, $set:ident) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn $get(&self) -> Option<String> {
                    self.0.$get().map(|x| x.clone().into())
                }

                #[setter]
                fn $set(&mut self, s: Option<String>) {
                    self.0.$set(s.map(|x| x.into()))
                }
            }
        )*
    };
}

macro_rules! get_set_copied {
    ($pytype:ident, $($rest:ident,)+ [$($root:ident),*], $get:ident, $set:ident, $field:ident, $out:ty) => {
        get_set_copied!($pytype, [$($root),*], $get, $set, $field, $out);
        get_set_copied!($($rest,)+ [$($root),*], $get, $set, $field, $out);
    };

    ($pytype:ident, [$($root:ident),*], $get:ident, $set:ident, $field:ident, $out:ty) => {
        #[pymethods]
        impl $pytype {
            #[getter]
            fn $get(&self) -> Option<$out> {
                self.0.metadata.$($root.)*$field.0.map(|x| x.into())
            }

            #[setter]
            fn $set(&mut self, s: Option<$out>) {
                self.0.metadata.$($root.)*$field = s.map(|x| x.into()).into()
            }
        }
    };

    ($($pytype:ident),*; $get:ident, $set:ident, $out:ty) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn $get(&self) -> Option<$out> {
                    self.0.$get().map(|x| (*x).into())
                }

                #[setter]
                fn $set(&mut self, s: Option<$out>) {
                    self.0.$set(s.map(|x| x.into()).into())
                }
            }
        )*
    };
}

macro_rules! meas_get_set {
    ($get:ident, $set:ident, $t:path, $($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn $get(&self) -> Vec<(usize, Option<$t>)> {
                    self.0
                        .$get()
                        .into_iter()
                        .map(|(i, x)| (i.into(), x.map(|y| y.clone().into())))
                        .collect()
                }

                #[setter]
                fn $set(&mut self, xs: Vec<Option<$t>>) -> bool {
                    self.0
                        .$set(xs.into_iter().map(|x| x.map(|y| y.into())).collect())
                }
            }
        )*
    };
}

macro_rules! convert_methods {
    ($pytype:ident, $inner:ident, $([$fn:ident, $to:ident]),+) => {
        #[pymethods]
        impl $pytype {
            $(
                fn $fn(&self) -> PyResult<$to> {
                    let new = self.0.clone().$inner();
                    handle_errors(new.map_err(|e| e.into()))
                }
            )*
        }
    };
}

convert_methods!(
    PyCoreTEXT2_0,
    try_convert,
    [version_3_0, PyCoreTEXT3_0],
    [version_3_1, PyCoreTEXT3_1],
    [version_3_2, PyCoreTEXT3_2]
);

convert_methods!(
    PyCoreTEXT3_0,
    try_convert,
    [version_2_0, PyCoreTEXT2_0],
    [version_3_1, PyCoreTEXT3_1],
    [version_3_2, PyCoreTEXT3_2]
);

convert_methods!(
    PyCoreTEXT3_1,
    try_convert,
    [version_2_0, PyCoreTEXT2_0],
    [version_3_0, PyCoreTEXT3_0],
    [version_3_2, PyCoreTEXT3_2]
);

convert_methods!(
    PyCoreTEXT3_2,
    try_convert,
    [version_2_0, PyCoreTEXT2_0],
    [version_3_0, PyCoreTEXT3_0],
    [version_3_1, PyCoreTEXT3_1]
);

convert_methods!(
    PyCoreDataset2_0,
    try_convert,
    [version_3_0, PyCoreDataset3_0],
    [version_3_1, PyCoreDataset3_1],
    [version_3_2, PyCoreDataset3_2]
);

convert_methods!(
    PyCoreDataset3_0,
    try_convert,
    [version_2_0, PyCoreDataset2_0],
    [version_3_1, PyCoreDataset3_1],
    [version_3_2, PyCoreDataset3_2]
);

convert_methods!(
    PyCoreDataset3_1,
    try_convert,
    [version_2_0, PyCoreDataset2_0],
    [version_3_0, PyCoreDataset3_0],
    [version_3_2, PyCoreDataset3_2]
);

convert_methods!(
    PyCoreDataset3_2,
    try_convert,
    [version_2_0, PyCoreDataset2_0],
    [version_3_0, PyCoreDataset3_0],
    [version_3_1, PyCoreDataset3_1]
);

#[pymethods]
impl PyCoreTEXT2_0 {
    #[new]
    fn new(datatype: PyAlphaNumType, byteord: PyByteOrd, mode: PyMode) -> Self {
        api::CoreTEXT2_0::new(datatype.into(), byteord.into(), mode.into()).into()
    }
}

#[pymethods]
impl PyCoreTEXT3_0 {
    #[new]
    fn new(datatype: PyAlphaNumType, byteord: PyByteOrd, mode: PyMode) -> Self {
        api::CoreTEXT3_0::new(datatype.into(), byteord.into(), mode.into()).into()
    }

    #[getter]
    fn get_unicode(&self) -> Option<PyUnicode> {
        self.0
            .metadata
            .specific
            .unicode
            .as_ref_opt()
            .map(|x| x.clone().into())
    }

    #[setter]
    fn set_unicode(&mut self, x: Option<PyUnicode>) {
        self.0.metadata.specific.unicode = x.map(|y| y.into()).into();
    }
}

#[pymethods]
impl PyCoreTEXT3_1 {
    #[new]
    fn new(datatype: PyAlphaNumType, is_big: bool, mode: PyMode) -> Self {
        api::CoreTEXT3_1::new(datatype.into(), is_big, mode.into()).into()
    }
}

#[pymethods]
impl PyCoreTEXT3_2 {
    #[new]
    fn new(datatype: PyAlphaNumType, is_big: bool, cyt: String) -> Self {
        api::CoreTEXT3_2::new(datatype.into(), is_big, cyt).into()
    }

    #[getter]
    fn get_datetime_begin(&self) -> Option<DateTime<FixedOffset>> {
        self.0.metadata.specific.datetimes.begin_naive()
    }

    #[setter]
    fn set_datetime_begin(&mut self, x: Option<DateTime<FixedOffset>>) -> PyResult<()> {
        self.0
            .metadata
            .specific
            .datetimes
            .set_begin_naive(x)
            .map_err(|e| PyreflowException::new_err(e.to_string()))
    }

    #[getter]
    fn get_datetime_end(&self) -> Option<DateTime<FixedOffset>> {
        self.0.metadata.specific.datetimes.end_naive()
    }

    #[setter]
    fn set_datetime_end(&mut self, x: Option<DateTime<FixedOffset>>) -> PyResult<()> {
        self.0
            .metadata
            .specific
            .datetimes
            .set_end_naive(x)
            .map_err(|e| PyreflowException::new_err(e.to_string()))
    }

    #[getter]
    fn get_datatypes(&self) -> Vec<PyAlphaNumType> {
        self.0.datatypes().into_iter().map(|x| x.into()).collect()
    }

    #[getter]
    fn get_cyt(&self) -> String {
        self.0.metadata.specific.cyt.0.clone()
    }

    #[setter]
    fn set_cyt(&mut self, x: String) {
        self.0.metadata.specific.cyt = x.into()
    }

    #[getter]
    fn get_unstainedinfo(&self) -> Option<String> {
        self.0
            .metadata
            .specific
            .unstained
            .unstainedinfo
            .0
            .as_ref()
            .map(|x| x.clone().into())
    }

    #[setter]
    fn set_unstainedinfo(&mut self, x: Option<String>) {
        self.0.metadata.specific.unstained.unstainedinfo = x.map(|x| x.into()).into()
    }

    #[getter]
    fn get_unstained_centers(&self) -> Option<HashMap<PyShortname, f32>> {
        self.0.unstained_centers().map(|x| {
            <HashMap<Shortname, f32>>::from(x.clone())
                .into_iter()
                .map(|(k, v)| (k.into(), v))
                .collect()
        })
    }

    fn insert_unstained_center(&mut self, k: PyShortname, v: f32) -> Option<f32> {
        self.0.insert_unstained_center(k.into(), v)
    }

    fn remove_unstained_center(&mut self, k: PyShortname) -> Option<f32> {
        self.0.remove_unstained_center(&k.into())
    }

    fn clear_unstained_centers(&mut self) {
        self.0.clear_unstained_centers()
    }

    fn set_data_mixed(&mut self, cs: Vec<PyMixedColumnSetter>) -> bool {
        self.0
            .set_data_mixed(cs.into_iter().map(|x| x.into()).collect())
    }
}

// TODO make this common
#[pymethods]
impl PyCoreDataset3_2 {
    #[getter]
    fn data(&self) -> PyDataFrame {
        let ns = self.0.all_shortnames();
        let columns = self
            .0
            .data()
            .iter_columns()
            .zip(ns)
            .map(|(c, n)| {
                // ASSUME this will not fail because the we know the types and
                // we don't have a validity array
                Series::from_arrow(n.as_ref().into(), c.as_array())
                    .unwrap()
                    .into()
            })
            .collect();
        // ASSUME this will not fail because all columns should have unique
        // names and the same length
        PyDataFrame(DataFrame::new(columns).unwrap())
    }

    #[getter]
    fn analysis(&self) -> Vec<u8> {
        self.0.analysis.0.clone()
    }

    #[setter]
    fn set_analysis(&mut self, xs: Vec<u8>) {
        self.0.analysis = xs.into();
    }
}

// Get/set methods for all versions
macro_rules! common_methods {
    ($pytype:ident, $($rest:ident),*) => {
        common_methods!($pytype);
        common_methods!($($rest),+);

    };

    ($pytype:ident) => {
        // common measurement keywords
        meas_get_set!(filters,           set_filters,           String,        $pytype);
        meas_get_set!(powers,            set_powers,            u32,           $pytype);
        meas_get_set!(detector_types,    set_detector_types,    String,        $pytype);
        meas_get_set!(percents_emitted,  set_percents_emitted,  String,        $pytype);
        meas_get_set!(detector_voltages, set_detector_voltages, PyNonNegFloat, $pytype);

        get_set_copied!($pytype, [], get_abrt, set_abrt, abrt, u32);
        get_set_copied!($pytype, [], get_lost, set_lost, lost, u32);

        get_set_str!($pytype, [], get_cells, set_cells, cells);
        get_set_str!($pytype, [], get_com,   set_com,   com);
        get_set_str!($pytype, [], get_exp,   set_exp,   exp);
        get_set_str!($pytype, [], get_fil,   set_fil,   fil);
        get_set_str!($pytype, [], get_inst,  set_inst,  inst);
        get_set_str!($pytype, [], get_op,    set_op,    op);
        get_set_str!($pytype, [], get_proj,  set_proj,  proj);
        get_set_str!($pytype, [], get_smno,  set_smno,  smno);
        get_set_str!($pytype, [], get_src,   set_src,   src);
        get_set_str!($pytype, [], get_sys,   set_sys,   sys);

        #[pymethods]
        impl $pytype {
            fn insert_nonstandard(&mut self, k: PyNonStdKey, v: String) -> Option<String> {
                self.0.metadata.nonstandard_keywords.insert(k.into(), v)
            }

            fn remove_nonstandard(&mut self, k: PyNonStdKey) -> Option<String> {
                self.0.metadata.nonstandard_keywords.remove(&k.into())
            }

            fn get_nonstandard(&mut self, k: PyNonStdKey) -> Option<String> {
                self.0.metadata.nonstandard_keywords.get(&k.into()).cloned()
            }
        }

        #[pymethods]
        impl $pytype {
            // TODO add way to remove nonstandard
            #[pyo3(signature = (want_req=None, want_meta=None))]
            fn raw_keywords<'py>(
                &self,
                py: Python<'py>,
                want_req: Option<bool>,
                want_meta: Option<bool>,
            ) -> PyResult<Bound<'py, PyDict>> {
                self.0.raw_keywords(want_req, want_meta).clone().into_py_dict(py)
            }

            #[getter]
            fn par(&self) -> usize {
                self.0.par().0
            }

            fn insert_meas_nonstandard(
                &mut self,
                xs: Vec<(PyNonStdKey, String)>,
            ) -> Option<Vec<Option<String>>> {
                let ys = xs.into_iter().map(|(k, v)| (k.into(), v)).collect();
                self.0.insert_meas_nonstandard(ys)
            }

            fn remove_meas_nonstandard(&mut self, ks: Vec<PyNonStdKey>) -> Option<Vec<Option<String>>> {
                let ys: Vec<_> = ks.into_iter().map(|k| k.into()).collect();
                self.0.remove_meas_nonstandard(ys.iter().collect())
            }

            fn get_meas_nonstandard(&mut self, ks: Vec<PyNonStdKey>) -> Option<Vec<Option<String>>> {
                let ys: Vec<_> = ks.into_iter().map(|k| k.into()).collect();
                self.0
                    .get_meas_nonstandard(ys.iter().collect())
                    .map(|rs| rs.into_iter().map(|r| r.cloned()).collect())
            }

            #[getter]
            fn get_btim(&self) -> Option<NaiveTime> {
                self.0.timestamps().btim_naive()
            }

            #[setter]
            fn set_btim(&mut self, x: Option<NaiveTime>) -> PyResult<()> {
                self.0
                    .timestamps_mut()
                    .set_btim_naive(x)
                    .map_err(|e| PyreflowException::new_err(e.to_string()))
            }

            #[getter]
            fn get_etim(&self) -> Option<NaiveTime> {
                self.0.timestamps().etim_naive()
            }

            #[setter]
            fn set_etim(&mut self, x: Option<NaiveTime>) -> PyResult<()> {
                self.0
                    .timestamps_mut()
                    .set_etim_naive(x)
                    .map_err(|e| PyreflowException::new_err(e.to_string()))
            }

            #[getter]
            fn get_date(&self) -> Option<NaiveDate> {
                self.0.timestamps().date_naive()
            }

            #[setter]
            fn set_date(&mut self, x: Option<NaiveDate>) -> PyResult<()> {
                self.0
                    .timestamps_mut()
                    .set_date_naive(x)
                    .map_err(|e| PyreflowException::new_err(e.to_string()))
            }

            #[getter]
            fn trigger_name(&self) -> Option<PyShortname> {
                self.0.trigger_name().map(|x| x.clone().into())
            }

            #[getter]
            fn trigger_threshold(&self) -> Option<u32> {
                self.0.trigger_threshold()
            }

            #[setter]
            fn set_trigger_name(&mut self, n: PyShortname) -> bool {
                self.0.set_trigger_name(n.into())
            }

            #[setter]
            fn set_trigger_threshold(&mut self, x: u32) -> bool {
                self.0.set_trigger_threshold(x)
            }

            fn clear_trigger(&mut self) {
                self.0.clear_trigger()
            }

            fn set_time_channnel(&mut self, n: PyShortname) -> PyResult<()> {
                self.0.set_time_channel(&n.into()).map_err(|es| {
                    let f = es.into_iter().map(|e| e.to_string()).join(", ");
                    let s = format!("Error(s) when converting measurement to time: {f}");
                    PyreflowException::new_err(s)
                })
            }

            fn unset_time_channel(&mut self) -> bool {
                self.0.unset_time_channel()
            }

            #[getter]
            fn get_bytes(&self) -> Option<Vec<u8>> {
                self.0.bytes()
            }

            #[getter]
            fn get_ranges(&self) -> Vec<String> {
                // TODO strings, lame
                self.0.ranges().iter().map(|r| r.as_ref().to_string()).collect()
            }

            #[getter]
            fn get_longnames(&self) -> Vec<Option<String>> {
                self.0
                    .longnames()
                    .into_iter()
                    .map(|x| x.map(|y| y.0.to_string()))
                    .collect()
            }

            #[setter]
            fn set_longnames(&mut self, ns: Vec<Option<String>>) -> bool {
                self.0.set_longnames(ns)
            }

            #[getter]
            fn shortnames_maybe(&self) -> Vec<Option<PyShortname>> {
                self.0
                    .shortnames_maybe()
                    .into_iter()
                    .map(|x| x.map(|y| y.clone().into()))
                    .collect()
            }

            #[getter]
            fn all_shortnames(&self) -> Vec<PyShortname> {
                self.0
                    .all_shortnames()
                    .into_iter()
                    .map(|x| x.into())
                    .collect()
            }

            #[setter]
            fn set_all_shortnames(&mut self, ns: Vec<PyShortname>) -> PyResult<()> {
                self.0
                    .set_all_shortnames(ns.into_iter().map(|x| x.into()).collect())
                    .map_err(|e| PyreflowException::new_err(e.to_string()))
                    .map(|_| ())
            }

            fn set_data_f32(&mut self, ranges: Vec<f32>) -> bool {
                self.0.set_data_f32(ranges)
            }

            fn set_data_f64(&mut self, ranges: Vec<f64>) -> bool {
                self.0.set_data_f64(ranges)
            }

            fn set_data_ascii(&mut self, rs: Vec<PyAsciiRangeSetter>) -> bool {
                self.0.set_data_ascii(rs.into_iter().map(|x| x.into()).collect())
            }

            fn set_data_delimited(&mut self, ranges: Vec<u64>) -> bool {
                self.0.set_data_delimited(ranges)
            }
        }
    };
}

common_methods!(
    PyCoreTEXT2_0,
    PyCoreTEXT3_0,
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset2_0,
    PyCoreDataset3_0,
    PyCoreDataset3_1,
    PyCoreDataset3_2
);

macro_rules! common_meas_get_set {
    ($([$pytype:ident, $meastype:ident, $timetype:ident]),*) => {
        $(
            #[pymethods]
            impl $pytype {
                fn remove_measurement_by_name<'py>(
                    &mut self,
                    n: PyShortname,
                    py: Python<'py>,
                ) -> PyResult<Option<(usize, Bound<'py, PyAny>)>> {
                    self.0
                        .remove_measurement_by_name(&Shortname::from(n))
                        .map(|(i, x)| {
                            x.map_or_else(
                                |l| $timetype::from(l).into_bound_py_any(py),
                                |r| $meastype::from(r).into_bound_py_any(py)
                            ).map(|x| (usize::from(i), x))
                        }).transpose()
                }
            }
        )*

    };
}
common_meas_get_set!([PyCoreTEXT2_0, PyMeasurement2_0, PyTimeChannel2_0]);

// Get/set methods for setting $PnN (2.0-3.0)
macro_rules! shortnames_methods {
    ($($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                fn set_measurement_shortnames_maybe(
                    &mut self,
                    ns: Vec<Option<PyShortname>>,
                ) -> PyResult<()> {
                    let xs = ns.into_iter().map(|n| n.map(|y| y.into())).collect();
                    self.0
                        .set_measurement_shortnames_maybe(xs)
                        .map_err(|e| PyreflowException::new_err(e.to_string()))
                        .map(|_| ())
                }
            }
        )*
    };
}

shortnames_methods!(
    PyCoreTEXT2_0,
    PyCoreTEXT3_0,
    PyCoreDataset2_0,
    PyCoreDataset3_0
);

// Get/set methods for setting integer measurement types (2.0-3.0)
macro_rules! integer_2_0_methods {
    ($($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                fn set_data_integer(&mut self, rs: Vec<u64>, byteord: PyByteOrd) -> bool {
                    self.0.set_data_integer(rs, byteord.into())
                }
            }
        )*
    };
}

integer_2_0_methods!(
    PyCoreTEXT2_0,
    PyCoreTEXT3_0,
    PyCoreDataset2_0,
    PyCoreDataset3_0
);

// Get/set methods for setting integer measurement types (3.1-3.2)
macro_rules! integer_methods {
    ($($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                fn set_data_integer(&mut self, rs: Vec<PyNumRangeSetter>) -> bool {
                    self.0
                        .set_data_integer(rs.into_iter().map(|x| x.into()).collect())
                }
            }
        )*
    };
}

integer_methods!(
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_1,
    PyCoreDataset3_2
);

// Get/set methods for $BYTEORD (3.1-3.2)
macro_rules! endian_methods {
    ($($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn get_big_endian(&self) -> bool {
                    self.0.get_big_endian()
                }

                #[setter]
                fn set_big_endian(&mut self, is_big: bool) {
                    self.0.set_big_endian(is_big)
                }
            }
        )*
    };
}

endian_methods!(
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_1,
    PyCoreDataset3_2
);

// Get/set methods for $PnE (3.0-3.2)
macro_rules! scales_methods {
    ($($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn get_all_scales(&self) -> Vec<PyScale> {
                    self.0.all_scales().into_iter().map(|x| x.into()).collect()
                }

                #[getter]
                fn get_scales(&self) -> Vec<(usize, PyScale)> {
                    self.0
                        .scales()
                        .into_iter()
                        .map(|(i, x)| (i.into(), x.into()))
                        .collect()
                }

                #[setter]
                fn set_scales(&mut self, xs: Vec<PyScale>) -> bool {
                    self.0.set_scales(xs.into_iter().map(|x| x.into()).collect())
                }
            }
        )*
    };
}

scales_methods!(
    PyCoreTEXT3_0,
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_0,
    PyCoreDataset3_1,
    PyCoreDataset3_2
);

// Get/set methods for $TIMESTEP (3.0-3.2)
macro_rules! timestep_methods {
    ($($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn get_timestep(&self) -> Option<PyPositiveFloat> {
                    self.0
                        .measurements_named_vec()
                        .as_center()
                        .and_then(|x| x.value.specific.timestep())
                        .map(|x| x.0.into())
                }

                #[setter]
                fn set_timestep(&mut self, x: PyPositiveFloat) -> bool {
                    self.0
                        .as_center_mut()
                        .map(|y| y.value.specific.set_timestep(api::Timestep(x.into())))
                        .is_some()
                }
            }
        )*
    };
}

timestep_methods!(
    PyCoreTEXT3_0,
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_0,
    PyCoreDataset3_1,
    PyCoreDataset3_2
);

// Get/set methods for scaler $PnL (3.1-3.2)
macro_rules! wavelength_methods {
    ($($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn get_wavelengths(&self) -> Vec<(usize, Option<u32>)> {
                    self.0
                        .wavelengths()
                        .into_iter()
                        .map(|(i, x)| (i.into(), x.map(|y| y.0)))
                        .collect()
                }

                #[setter]
                fn set_wavelengths(&mut self, xs: Vec<Option<u32>>) -> bool {
                    self.0
                        .set_wavelengths(xs.into_iter().map(|x| x.map(|y| y.into())).collect())
                }
            }
        )*
    };
}

wavelength_methods!(
    PyCoreTEXT2_0,
    PyCoreTEXT3_0,
    PyCoreDataset2_0,
    PyCoreDataset3_0
);

// Get/set methods for vector $PnL (3.1-3.2)
macro_rules! wavelengths_methods {
    ($($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn get_wavelengths(&self) -> Vec<(usize, Vec<u32>)> {
                    self.0
                        .wavelengths()
                        .into_iter()
                        .map(|(i, x)| {
                            (
                                i.into(),
                                x.map(|y| y.0.iter().copied().collect()).unwrap_or_default(),
                            )
                        })
                        .collect()
                }

                #[setter]
                fn set_wavelengths(&mut self, xs: Vec<Vec<u32>>) -> bool {
                    self.0
                        .set_wavelengths(
                            xs.into_iter()
                                .map(|x| NonEmpty::from_vec(x).map(api::Wavelengths))
                                .collect(),
                        )
                }
            }
        )*
    };
}

wavelengths_methods!(
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_1,
    PyCoreDataset3_2
);

// Get/set methods for $LAST_MODIFIER/$LAST_MODIFIED/$ORIGINALITY (3.1-3.2)
macro_rules! modification_methods {
    ($($pytype:ident),+) => {
        get_set_copied!(
            $($pytype,)*
            [specific, modification],
            get_originality,
            set_originality,
            originality,
            PyOriginality
        );

        get_set_copied!(
            $($pytype,)*
            [specific, modification],
            get_last_modified,
            set_last_modified,
            last_modified,
            NaiveDateTime
        );

        get_set_str!(
            $($pytype,)*
            [specific, modification],
            get_last_modifier,
            set_last_modifier,
            last_modifier
        );
    };
}

modification_methods!(
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_1,
    PyCoreDataset3_2
);

// Get/set methods for $CARRIERID/$CARRIERTYPE/$LOCATIONID (3.2)
macro_rules! carrier_methods {
    ($($pytype:ident),*) => {
        get_set_str!($($pytype,)* [specific, carrier], get_carriertype, set_carriertype, carriertype);
        get_set_str!($($pytype,)* [specific, carrier], get_carrierid,   set_carrierid,   carrierid);
        get_set_str!($($pytype,)* [specific, carrier], get_locationid,  set_locationid,  locationid);
    };
}

carrier_methods!(PyCoreTEXT3_2, PyCoreDataset3_2);

// Get/set methods for $PLATEID/$WELLID/$PLATENAME (3.1-3.2)
macro_rules! plate_methods {
    ($($pytype:ident),*) => {
        get_set_str!($($pytype,)* [specific, plate], get_wellid,    set_wellid,    wellid);
        get_set_str!($($pytype,)* [specific, plate], get_plateid,   set_plateid,   plateid);
        get_set_str!($($pytype,)* [specific, plate], get_platename, set_platename, platename);
    };
}

plate_methods!(
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_1,
    PyCoreDataset3_2
);

// Get/set methods for $SPILLOVER (3.1-3.2)
macro_rules! spillover_methods {
    ($($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn get_spillover_matrix<'a>(&self, py: Python<'a>) -> Option<Bound<'a, PyArray2<f32>>> {
                    self.0.spillover().map(|x| x.matrix().to_pyarray(py))
                }

                #[getter]
                fn get_spillover_names(&self) -> Vec<String> {
                    self.0
                        .spillover()
                        .map(|x| x.measurements())
                        .unwrap_or_default()
                        .iter()
                        .map(|x| x.as_ref().to_string())
                        .collect()
                }

                fn set_spillover(
                    &mut self,
                    ns: Vec<PyShortname>,
                    a: PyReadonlyArray2<f32>,
                ) -> Result<(), PyErr> {
                    let m = a.as_matrix().into_owned();
                    self.0
                        .set_spillover(ns.into_iter().map(|x| x.into()).collect(), m)
                        .map_err(|e| PyreflowException::new_err(e.to_string()))
                }

                fn unset_spillover(&mut self) {
                    self.0.unset_spillover()
                }
            }
        )*
    };
}

spillover_methods!(
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_1,
    PyCoreDataset3_2
);

// Get/set methods for (optional) $CYT (2.0-3.1)
//
// 3.2 is required which is why it is not included here
get_set_str!(
    PyCoreTEXT2_0,
    PyCoreTEXT3_0,
    PyCoreTEXT3_1,
    PyCoreDataset2_0,
    PyCoreDataset3_0,
    PyCoreDataset3_1,
    [specific],
    get_cyt,
    set_cyt,
    cyt
);

// Get/set methods for $FLOWRATE (3.2)
get_set_str!(
    PyCoreTEXT3_2,
    PyCoreDataset3_2,
    [specific],
    get_flowrate,
    set_flowrate,
    flowrate
);

// Get/set methods for $VOL (3.1-3.2)
get_set_copied!(
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_1,
    PyCoreDataset3_2,
    [specific],
    get_vol,
    set_vol,
    vol,
    PyNonNegFloat
);

// Get/set methods for $CYTSN (3.0-3.2)
get_set_str!(
    PyCoreTEXT3_0,
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_0,
    PyCoreDataset3_1,
    PyCoreDataset3_2,
    [specific],
    get_cytsn,
    set_cytsn,
    cytsn
);

// Get/set methods for $PnG (3.0-3.2)
meas_get_set!(
    gains,
    set_gains,
    PyPositiveFloat,
    PyCoreTEXT3_0,
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_0,
    PyCoreDataset3_1
);

// Get/set methods for $PnDET (3.2)
meas_get_set!(
    det_names,
    set_det_names,
    String,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnCALIBRATION (3.1)
meas_get_set!(
    calibrations,
    set_calibrations,
    PyCalibration3_1,
    PyCoreTEXT3_1,
    PyCoreDataset3_1
);

// Get/set methods for $PnCALIBRATION (3.2)
meas_get_set!(
    calibrations,
    set_calibrations,
    PyCalibration3_2,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnTAG (3.2)
meas_get_set!(tags, set_tags, String, PyCoreTEXT3_2, PyCoreDataset3_2);

// Get/set methods for $PnTYPE (3.2)
meas_get_set!(
    measurement_types,
    set_measurement_types,
    PyMeasurementType,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnFEATURE (3.2)
meas_get_set!(
    features,
    set_features,
    PyFeature,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnANALYTE (3.2)
meas_get_set!(
    analytes,
    set_analytes,
    String,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Add method to convert CoreTEXT* to CoreDataset* by adding DATA and ANALYSIS
// (all versions)
macro_rules! to_dataset_method {
    ($from:ident, $to:ident) => {
        #[pymethods]
        impl $from {
            fn to_dataset(&self, df: PyDataFrame, analysis: Vec<u8>) -> PyResult<$to> {
                let cols = polars_to_fcs(df.into())
                    .map_err(|e| PyreflowException::new_err(e.to_string()))?;
                api::CoreDataset::from_coretext(self.0.clone(), cols, analysis.into())
                    .map_err(|e| PyreflowException::new_err(e.to_string()))
                    .map(|df| df.into())
            }
        }
    };
}

to_dataset_method!(PyCoreTEXT2_0, PyCoreDataset2_0);
to_dataset_method!(PyCoreTEXT3_0, PyCoreDataset3_0);
to_dataset_method!(PyCoreTEXT3_1, PyCoreDataset3_1);
to_dataset_method!(PyCoreTEXT3_2, PyCoreDataset3_2);

struct PyImpureError(error::ImpureFailure);

fn handle_errors<X, Y>(res: error::ImpureResult<X>) -> PyResult<Y>
where
    Y: From<X>,
{
    handle_pure(res.map_err(PyImpureError)?)
}

// TODO use warnings_are_errors flag
fn handle_pure<X, Y>(succ: error::PureSuccess<X>) -> PyResult<Y>
where
    Y: From<X>,
{
    let (err, warn) = succ.deferred.split();
    Python::with_gil(|py| -> PyResult<()> {
        let wt = py.get_type::<PyreflowWarning>();
        for w in warn {
            let s = CString::new(w)?;
            PyErr::warn(py, &wt, &s, 0)?;
        }
        Ok(())
    })?;
    if err.is_empty() {
        Ok(succ.data.into())
    } else {
        let deferred = err.join("\n");
        let msg = format!("Errors encountered:\n{deferred}");
        Err(PyreflowException::new_err(msg))
    }
}

impl From<error::ImpureFailure> for PyImpureError {
    fn from(value: error::ImpureFailure) -> Self {
        Self(value)
    }
}

impl From<PyImpureError> for PyErr {
    fn from(err: PyImpureError) -> Self {
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

macro_rules! column_to_buf {
    ($cols:expr, $col:expr, $prim:ident) => {
        let ca = $col.$prim().unwrap();
        if ca.first_non_null().is_some() {
            return Err(format!("column {} has null values", $col.name()));
        }
        let buf = ca.chunks()[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<$prim>>()
            .unwrap()
            .values()
            .clone();
        $cols.push(FCSColumn(buf).into());
    };
}

fn polars_to_fcs(mut df: DataFrame) -> Result<Vec<AnyFCSColumn>, String> {
    // make sure data is contiguous
    df.rechunk_mut();
    let mut cols = Vec::with_capacity(df.width());
    for c in df.get_columns() {
        match c.dtype() {
            DataType::UInt8 => {
                column_to_buf!(cols, c, u8);
            }
            DataType::UInt16 => {
                column_to_buf!(cols, c, u16);
            }
            DataType::UInt32 => {
                column_to_buf!(cols, c, u32);
            }
            DataType::UInt64 => {
                column_to_buf!(cols, c, u64);
            }
            DataType::Float32 => {
                column_to_buf!(cols, c, f32);
            }
            DataType::Float64 => {
                column_to_buf!(cols, c, f64);
            }
            t => return Err(format!("invalid datatype: {t}")),
        }
    }
    Ok(cols)
}
