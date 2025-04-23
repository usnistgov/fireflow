use fireflow_core::api;
use fireflow_core::config::Strict;
use fireflow_core::config::{self, OffsetCorrection};
use fireflow_core::error;
use fireflow_core::validated::datepattern::DatePattern;
use fireflow_core::validated::nonstandard::*;
use fireflow_core::validated::pattern::*;
use fireflow_core::validated::ranged_float::*;
use fireflow_core::validated::shortname::*;
use fireflow_core::validated::textdelim::TEXTDelim;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use itertools::Itertools;
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
use std::ffi::CString;
use std::fmt;
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
    disallow_deprecated=false,
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
    disallow_deprecated: bool,
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
        disallow_deprecated,
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
) -> PyResult<PyStandardizedTEXT> {
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
        disallow_deprecated,
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
        nonstandard_measurement_pattern: nonstandard_measurement_pattern.map(|x| x.0),
    };

    handle_errors(api::read_fcs_std_text(&p, &conf.set_strict(strict)))
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
) -> PyResult<PyStandardizedDataset> {
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
        disallow_deprecated,
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

    handle_errors(api::read_fcs_file(&p, &conf.set_strict(strict)))
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
pywrap!(PyOffsets, api::Offsets, "Offsets");
pywrap!(
    PyStandardizedTEXT,
    api::StandardizedTEXT,
    "StandardizedTEXT"
);
pywrap!(
    PyStandardizedDataset,
    api::StandardizedDataset,
    "StandardizedDataset"
);
pywrap!(PyAnyCoreTEXT, api::AnyCoreTEXT, "AnyCoreTEXT");

pywrap!(PyCoreTEXT2_0, api::CoreTEXT2_0, "CoreTEXT2_0");
pywrap!(PyCoreTEXT3_0, api::CoreTEXT3_0, "CoreTEXT3_0");
pywrap!(PyCoreTEXT3_1, api::CoreTEXT3_1, "CoreTEXT3_1");
pywrap!(PyCoreTEXT3_2, api::CoreTEXT3_2, "CoreTEXT3_2");

pywrap!(PyMeasurement2_0, api::Measurement2_0, "Measurement2_0");
pywrap!(PyMeasurement3_0, api::Measurement3_0, "Measurement3_0");
pywrap!(PyMeasurement3_1, api::Measurement3_1, "Measurement3_1");
pywrap!(PyMeasurement3_2, api::Measurement3_2, "Measurement3_2");
pywrap!(PyDatePattern, DatePattern, "DatePattern");
pywrap!(PyShortname, Shortname, "Shortname");
pywrap!(PyTimePattern, TimePattern, "TimePattern");
pywrap!(PyShortnamePrefix, ShortnamePrefix, "ShortnamePrefix");
pywrap!(PyNonStdMeasPattern, NonStdMeasPattern, "NonStdMeasPattern");
pywrap!(PyNonStdMeasKey, NonStdMeasKey, "NonStdMeasKey");
pywrap!(PyNonStdKey, NonStdKey, "NonStdKey");
pywrap!(PyTEXTDelim, TEXTDelim, "TEXTDelim");
pywrap!(PyCytSetter, MetaKwSetter<api::Cyt>, "CytSetter");

pywrap!(PyEndian, api::Endian, "Endian");
pywrap!(PyOriginality, api::Originality, "Originality");
pywrap!(PyTrigger, api::Trigger, "Trigger");
pywrap!(PyAlphaNumType, api::AlphaNumType, "AlphaNumType");

pywrap!(PyColumnType, api::ColumnType, "ColumnType");
pywrap!(PyUint08Type, api::Uint08Type, "Uint08Type");
pywrap!(PyUint16Type, api::Uint16Type, "Uint16Type");
pywrap!(PyUint24Type, api::Uint24Type, "Uint24Type");
pywrap!(PyUint32Type, api::Uint32Type, "Uint32Type");
pywrap!(PyUint40Type, api::Uint40Type, "Uint40Type");
pywrap!(PyUint48Type, api::Uint48Type, "Uint48Type");
pywrap!(PyUint56Type, api::Uint56Type, "Uint56Type");
pywrap!(PyUint64Type, api::Uint64Type, "Uint64Type");
pywrap!(PySingleType, api::SingleType, "SingleType");
pywrap!(PyDoubleType, api::DoubleType, "SingleType");

py_parse!(PyDatePattern, DatePattern);
py_disp!(PyDatePattern);

py_parse!(PyShortname, Shortname);
py_disp!(PyShortname);

py_parse!(PyNonStdMeasPattern, NonStdMeasPattern);
py_disp!(PyNonStdMeasPattern);

// #[pymethods]
// impl PyCytSetter {
//     #[new]
//     fn new(def_key: bool, default: Option<String>, key: Option<PyNonStdKey>) -> PyCytSetter {
//         api::OptMetaKey::setter(default.map(|x| x.into()), def_key, key.map(|x| x.0)).into()
//     }
// }

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
        self.0.offsets.clone().into()
    }

    #[getter]
    fn delimiter(&self) -> u8 {
        self.0.delimiter
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
    fn deviant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.0.deviant.clone().into_py_dict(py)
    }

    // TODO this will be in arbitrary order, might make sense to sort it
    // TODO add flag to remove nonstandard
    #[pyo3(signature = (want_req=None, want_meta=None))]
    fn raw_keywords<'py>(
        &self,
        py: Python<'py>,
        want_req: Option<bool>,
        want_meta: Option<bool>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self.0
            .standardized
            .raw_keywords(want_req, want_meta)
            .clone()
            .into_py_dict(py)
    }

    #[getter]
    fn shortnames(&self) -> Vec<String> {
        self.0
            .standardized
            .shortnames()
            .iter()
            .map(|x| x.to_string())
            .collect()
    }

    // TODO add other converters here

    #[getter]
    fn inner(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.0.standardized {
            // TODO this copies all data from the "union type" into a new
            // version-specific type. This might not be a big deal, but these
            // types might be rather large with lots of strings.
            api::AnyCoreTEXT::FCS2_0(x) => PyCoreTEXT2_0::from((**x).clone()).into_py_any(py),
            api::AnyCoreTEXT::FCS3_0(x) => PyCoreTEXT3_0::from((**x).clone()).into_py_any(py),
            api::AnyCoreTEXT::FCS3_1(x) => PyCoreTEXT3_1::from((**x).clone()).into_py_any(py),
            api::AnyCoreTEXT::FCS3_2(x) => PyCoreTEXT3_2::from((**x).clone()).into_py_any(py),
        }
    }
}

// TODO not DRY
#[pymethods]
impl PyStandardizedDataset {
    #[getter]
    fn offsets(&self) -> PyOffsets {
        self.0.offsets.clone().into()
    }

    #[getter]
    fn delimiter(&self) -> u8 {
        self.0.delimiter
    }

    #[getter]
    fn deviant<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.0.deviant.clone().into_py_dict(py)
    }

    // // TODO this will be in arbitrary order, might make sense to sort it
    // // TODO add flag to remove nonstandard
    // #[pyo3(signature = (want_req=None, want_meta=None))]
    // fn raw_keywords<'py>(
    //     &self,
    //     py: Python<'py>,
    //     want_req: Option<bool>,
    //     want_meta: Option<bool>,
    // ) -> PyResult<Bound<'py, PyDict>> {
    //     self.0
    //         .dataset
    //         .as_text
    //         .raw_keywords(want_req, want_meta)
    //         .clone()
    //         .into_py_dict(py)
    // }

    // #[getter]
    // fn shortnames(&self) -> Vec<String> {
    //     self.0
    //         .dataset
    //         .as_text
    //         .shortnames()
    //         .iter()
    //         .map(|x| x.to_string())
    //         .collect()
    // }

    // // TODO add other converters here

    // #[getter]
    // fn inner(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
    //     match &self.0.dataset.as_text {
    //         // TODO this copies all data from the "union type" into a new
    //         // version-specific type. This might not be a big deal, but these
    //         // types might be rather large with lots of strings.
    //         api::AnyCoreTEXT::FCS2_0(x) => PyCoreTEXT2_0::from((**x).clone()).into_py_any(py),
    //         api::AnyCoreTEXT::FCS3_0(x) => PyCoreTEXT3_0::from((**x).clone()).into_py_any(py),
    //         api::AnyCoreTEXT::FCS3_1(x) => PyCoreTEXT3_1::from((**x).clone()).into_py_any(py),
    //         api::AnyCoreTEXT::FCS3_2(x) => PyCoreTEXT3_2::from((**x).clone()).into_py_any(py),
    //     }
    // }

    #[getter]
    fn data(&self) -> PyDataFrame {
        // NOTE polars Series is a wrapper around an Arc so clone just
        // increments the ref count for each column rather than "deepcopy" the
        // whole dataset.
        PyDataFrame(self.0.dataset.as_data().clone())
    }
}

macro_rules! get_set_str {
    ($pytype:ident, [$($root:ident,)*], $get:ident, $set:ident) => {
        #[pymethods]
        impl $pytype {
            #[getter]
            fn $get(&self) -> Option<String> {
                self.0.$($root.)*$get().map(String::from)
            }

            #[setter]
            fn $set(&mut self, s: Option<String>) {
                self.0.$($root.)*$set(s)
            }
        }
    };
}

macro_rules! get_set_copied {
    ($pytype:ident, [$($root:ident,)*], $get:ident, $set:ident, $in:expr, $out:ty) => {
        #[pymethods]
        impl $pytype {
            #[getter]
            fn $get(&self) -> Option<$out> {
                self.0.$($root.)*$get().map(|x| x.into())
            }

            #[setter]
            fn $set(&mut self, s: Option<$out>) {
                self.0.$($root.)*$set(s.map($in))
            }
        }
    };
}

macro_rules! get_set_datetime {
    ($pytype:ident, [$($root:ident,)*]) => {
        #[pymethods]
        impl $pytype {
            #[getter]
            fn begin_date(&self) -> Option<NaiveDate> {
                self.0.$($root.)*begin_date()
            }

            #[getter]
            fn end_date(&self) -> Option<NaiveDate> {
                self.0.$($root.)*end_date()
            }

            #[getter]
            fn begin_time(&self) -> Option<NaiveTime> {
                self.0.$($root.)*begin_time()
            }

            #[getter]
            fn end_time(&self) -> Option<NaiveTime> {
                self.0.$($root.)*end_time()
            }

            fn set_datetimes(
                &mut self,
                begin: DateTime<FixedOffset>,
                end: DateTime<FixedOffset>,
            ) -> bool {
                self.0.$($root.)*set_datetimes(begin, end)
            }

            fn clear_datetimes(&mut self) {
                self.0.$($root.)*clear_datetimes()
            }
        }
    };
}

macro_rules! core_text_methods {
    ($pytype:ident, [$($root:ident)*]) => {
        get_set_datetime!($pytype, [$($root,)*]);
        get_set_copied!( $pytype, [$($root,)*], abrt, set_abrt, api::Abrt, u32);
        get_set_copied!( $pytype, [$($root,)*], lost, set_lost, api::Lost, u32);
        get_set_str!(    $pytype, [$($root,)*], cells, set_cells);
        get_set_str!(    $pytype, [$($root,)*], com, set_com);
        get_set_str!(    $pytype, [$($root,)*], exp, set_exp);
        get_set_str!(    $pytype, [$($root,)*], fil, set_fil);
        get_set_str!(    $pytype, [$($root,)*], inst, set_inst);
        get_set_str!(    $pytype, [$($root,)*], op, set_op);
        get_set_str!(    $pytype, [$($root,)*], proj, set_proj);
        get_set_str!(    $pytype, [$($root,)*], smno, set_smno);
        get_set_str!(    $pytype, [$($root,)*], src, set_src);
        get_set_str!(    $pytype, [$($root,)*], sys, set_sys);

        #[pymethods]
        impl $pytype {
            // #[getter]
            // fn get_trigger(&self) -> Option<PyTrigger> {
            //     self.0.$($root.)*trigger().map(|x| x.clone().into())
            // }

            // #[setter]
            // fn set_trigger(&mut self, t: Option<PyTrigger>) {
            //     self.0.$($root.)*set_trigger(t.map(|x| x.into()))
            // }

            #[getter]
            fn get_datatype(&self) -> PyAlphaNumType {
                self.0.$($root.)*datatype().into()
            }

            #[setter]
            fn set_datatype(&mut self, t: PyAlphaNumType) {
                self.0.$($root.)*set_datatype(t.into())
            }
        }
    };
}

core_text_methods!(PyStandardizedTEXT, [standardized]);
core_text_methods!(PyCoreTEXT2_0, []);
core_text_methods!(PyCoreTEXT3_0, []);
core_text_methods!(PyCoreTEXT3_1, []);
core_text_methods!(PyCoreTEXT3_2, []);

#[pymethods]
impl PyCoreTEXT3_2 {
    fn version_2_0(&self) -> PyResult<PyCoreTEXT2_0> {
        let new = self.0.clone().try_convert();
        handle_errors(new.map_err(|e| e.into()))
    }

    fn version_3_0(&self) -> PyResult<PyCoreTEXT3_0> {
        let new = self.0.clone().try_convert();
        handle_errors(new.map_err(|e| e.into()))
    }

    fn version_3_1(&self) -> PyResult<PyCoreTEXT3_1> {
        let new = self.0.clone().try_convert();
        handle_errors(new.map_err(|e| e.into()))
    }

    #[getter]
    fn get_byteord(&self) -> PyEndian {
        self.0.metadata.specific.byteord.into()
    }

    #[setter]
    fn set_byteord(&mut self, x: PyEndian) {
        self.0.metadata.specific.byteord = x.into()
    }

    #[getter]
    fn get_cyt(&self) -> String {
        self.0.metadata.specific.cyt.0.clone()
    }

    #[setter]
    fn set_cyt(&mut self, x: String) {
        self.0.metadata.specific.cyt = x.into()
    }

    // TODO add get/set $SPILLOVER matrix, which will need some validation

    #[getter]
    fn get_cytsn(&self) -> Option<String> {
        self.0
            .metadata
            .specific
            .cytsn
            .0
            .as_ref()
            .map(|x| x.0.clone())
    }

    #[setter]
    fn set_cytsn(&mut self, x: Option<String>) {
        self.0.metadata.specific.cytsn = x.map(|x| x.into()).into()
    }

    // #[getter]
    // fn get_timestep(&self) -> Option<f32> {
    //     self.0
    //         .metadata
    //         .specific
    //         .timestep
    //         .0
    //         .as_ref()
    //         .map(|x| x.0.into())
    // }

    // #[setter]
    // fn set_timestep(&mut self, x: Option<f32>) -> PyResult<()> {
    //     let t = x
    //         .map(PositiveFloat::try_from)
    //         .transpose()
    //         .map_err(|e| PyreflowException::new_err(e.to_string()))?;
    //     self.0.metadata.specific.timestep = t.map(api::Timestep).into();
    //     Ok(())
    // }

    #[getter]
    fn get_vol(&self) -> Option<f32> {
        self.0.metadata.specific.vol.0.as_ref().map(|x| x.0.into())
    }

    #[setter]
    fn set_vol(&mut self, x: Option<f32>) -> PyResult<()> {
        let t = x
            .map(NonNegFloat::try_from)
            .transpose()
            .map_err(|e| PyreflowException::new_err(e.to_string()))?;
        self.0.metadata.specific.vol = t.map(api::Vol).into();
        Ok(())
    }

    #[getter]
    fn get_flowrate(&self) -> Option<String> {
        self.0
            .metadata
            .specific
            .flowrate
            .0
            .as_ref()
            .map(|x| x.0.clone())
    }

    #[setter]
    fn set_flowrate(&mut self, x: Option<String>) {
        self.0.metadata.specific.flowrate = x.map(api::Flowrate).into()
    }

    #[getter]
    fn get_last_modifier(&self) -> Option<String> {
        self.0
            .metadata
            .specific
            .modification
            .last_modifier
            .0
            .as_ref()
            .map(|x| x.0.clone())
    }

    #[setter]
    fn set_last_modifier(&mut self, x: Option<String>) {
        self.0.metadata.specific.modification.last_modifier = x.map(api::LastModifier).into()
    }

    #[getter]
    fn get_last_modified(&self) -> Option<NaiveDateTime> {
        self.0
            .metadata
            .specific
            .modification
            .last_modified
            .0
            .as_ref()
            .map(|x| x.0)
    }

    #[setter]
    fn set_last_modified(&mut self, x: Option<NaiveDateTime>) {
        self.0.metadata.specific.modification.last_modified = x.map(api::ModifiedDateTime).into()
    }

    // TODO how do I make one of these?
    #[getter]
    fn get_originality(&self) -> Option<PyOriginality> {
        self.0
            .metadata
            .specific
            .modification
            .originality
            .0
            .as_ref()
            .map(|x| x.clone().into())
    }

    #[setter]
    fn set_originality(&mut self, x: Option<PyOriginality>) {
        self.0.metadata.specific.modification.originality = x.map(|x| x.0).into()
    }

    #[getter]
    fn get_carrierid(&self) -> Option<String> {
        self.0
            .metadata
            .specific
            .carrier
            .carrierid
            .0
            .as_ref()
            .map(|x| x.clone().into())
    }

    #[setter]
    fn set_carrierid(&mut self, x: Option<String>) {
        self.0.metadata.specific.carrier.carrierid = x.map(|x| x.into()).into()
    }

    #[getter]
    fn get_carriertype(&self) -> Option<String> {
        self.0
            .metadata
            .specific
            .carrier
            .carriertype
            .0
            .as_ref()
            .map(|x| x.clone().into())
    }

    #[setter]
    fn set_carriertype(&mut self, x: Option<String>) {
        self.0.metadata.specific.carrier.carriertype = x.map(|x| x.into()).into()
    }

    #[getter]
    fn get_locationid(&self) -> Option<String> {
        self.0
            .metadata
            .specific
            .carrier
            .locationid
            .0
            .as_ref()
            .map(|x| x.clone().into())
    }

    #[setter]
    fn set_locationid(&mut self, x: Option<String>) {
        self.0.metadata.specific.carrier.locationid = x.map(|x| x.into()).into()
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

    // TODO unstainedcenters?

    #[getter]
    fn get_platename(&self) -> Option<String> {
        self.0
            .metadata
            .specific
            .plate
            .platename
            .0
            .as_ref()
            .map(|x| x.clone().into())
    }

    #[setter]
    fn set_platename(&mut self, x: Option<String>) {
        self.0.metadata.specific.plate.platename = x.map(|x| x.into()).into()
    }

    #[getter]
    fn get_plateid(&self) -> Option<String> {
        self.0
            .metadata
            .specific
            .plate
            .plateid
            .0
            .as_ref()
            .map(|x| x.clone().into())
    }

    #[setter]
    fn set_plateid(&mut self, x: Option<String>) {
        self.0.metadata.specific.plate.plateid = x.map(|x| x.into()).into()
    }

    #[getter]
    fn get_wellid(&self) -> Option<String> {
        self.0
            .metadata
            .specific
            .plate
            .wellid
            .0
            .as_ref()
            .map(|x| x.clone().into())
    }

    #[setter]
    fn set_wellid(&mut self, x: Option<String>) {
        self.0.metadata.specific.plate.wellid = x.map(|x| x.into()).into()
    }

    #[getter]
    // python type is Union[int, list[Union[int, UintXType, SingleType, FloatType]]]
    fn get_column_layout(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let x = self.0.as_column_layout();
        let y = error::PureMaybe::from_result_strs(x, error::PureErrorLevel::Error)
            .into_result("could not make column layout".to_string())
            .map_err(|a| a.into());
        match handle_errors(y)? {
            api::DataLayout::AsciiDelimited { nrows: _, ncols } => ncols.into_py_any(py),
            api::DataLayout::AlphaNum { nrows: _, columns } => {
                let (pass, fail): (Vec<_>, Vec<_>) = columns
                    .iter()
                    .map(|ct| match ct {
                        api::ColumnType::Ascii { bytes } => bytes.into_py_any(py),
                        api::ColumnType::Integer(u) => match u {
                            api::AnyUintType::Uint08(t) => PyUint08Type(t.clone()).into_py_any(py),
                            api::AnyUintType::Uint16(t) => PyUint16Type(t.clone()).into_py_any(py),
                            api::AnyUintType::Uint24(t) => PyUint24Type(t.clone()).into_py_any(py),
                            api::AnyUintType::Uint32(t) => PyUint32Type(t.clone()).into_py_any(py),
                            api::AnyUintType::Uint40(t) => PyUint40Type(t.clone()).into_py_any(py),
                            api::AnyUintType::Uint48(t) => PyUint48Type(t.clone()).into_py_any(py),
                            api::AnyUintType::Uint56(t) => PyUint56Type(t.clone()).into_py_any(py),
                            api::AnyUintType::Uint64(t) => PyUint64Type(t.clone()).into_py_any(py),
                        },
                        api::ColumnType::Float(u) => PySingleType(u.clone()).into_py_any(py),
                        api::ColumnType::Double(u) => PyDoubleType(u.clone()).into_py_any(py),
                    })
                    .partition_result();
                if let Some(err) = fail.into_iter().next() {
                    Err(err)
                } else {
                    pass.into_py_any(py)
                }
            }
        }
    }

    // TODO add rest of metadata keywords
    // TODO add option to get/set measurements
    // TODO add option to populate fields based on nonstandard keywords?

    // TODO make function to add DATA/ANALYSIS, which will convert this to a CoreDataset
}

macro_rules! pyuint_methods {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            fn __repr__(&self) -> String {
                format!("UintType(bitmask={}, size={})", self.0.bitmask, self.0.size)
            }
        }
    };
}

pyuint_methods!(PyUint08Type);
pyuint_methods!(PyUint16Type);
pyuint_methods!(PyUint24Type);
pyuint_methods!(PyUint32Type);
pyuint_methods!(PyUint40Type);
pyuint_methods!(PyUint48Type);
pyuint_methods!(PyUint56Type);
pyuint_methods!(PyUint64Type);

#[pymethods]
impl PySingleType {
    fn __repr__(&self) -> String {
        format!(
            "SingleType(maxval={}, order={})",
            self.0.range, self.0.order
        )
    }
}

// #[pymethods]
// impl PyColumnType {
//     fn __repr__(&self) -> String {
//         match &self.0 {
//             api::ColumnType::Ascii { bytes } => format!("AsciiColumnType"),
//             api::ColumnType::Integer(u) => format!("IntColumnType"),
//             api::ColumnType::Float(u) => format!("FloatColumnType"),
//             api::ColumnType::Double(u) => format!("DoubleColumnType"),
//         }
//     }
// }

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
