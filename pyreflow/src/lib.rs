use fireflow_core::api;
use fireflow_core::api::IntoCore;
use fireflow_core::config::Strict;
use fireflow_core::config::{self, OffsetCorrection};
use fireflow_core::error;
use fireflow_core::validated::datepattern::DatePattern;
use fireflow_core::validated::nonstandard::NonStdMeasPattern;
use fireflow_core::validated::shortname::Shortname;
use fireflow_core::validated::textdelim::TEXTDelim;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};
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

    nonstandard_measurement_pattern=None,
    time_shortname=None,
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

    nonstandard_measurement_pattern: Option<PyNonStdMeasPattern>,
    time_shortname: Option<PyShortname>,
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
        time: config::TimeConfig {
            shortname: time_shortname.map(|x| x.0),
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
    enfore_data_width_divisibility=false,
    enfore_matching_tot=false,

    nonstandard_measurement_pattern=None,
    time_shortname=None,
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
    enfore_data_width_divisibility: bool,
    enfore_matching_tot: bool,

    nonstandard_measurement_pattern: Option<PyNonStdMeasPattern>,
    time_shortname: Option<PyShortname>,
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
        time: config::TimeConfig {
            shortname: time_shortname.map(|x| x.0),
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
        enfore_data_width_divisibility,
        enfore_matching_tot,
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
pywrap!(PyNonStdMeasPattern, NonStdMeasPattern, "NonStdMeasPattern");
pywrap!(PyTEXTDelim, TEXTDelim, "TEXTDelim");

py_parse!(PyDatePattern, DatePattern);
py_disp!(PyDatePattern);

py_parse!(PyShortname, Shortname);
py_disp!(PyShortname);

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
            .dataset
            .text
            .raw_keywords(want_req, want_meta)
            .clone()
            .into_py_dict(py)
    }

    #[getter]
    fn shortnames(&self) -> Vec<String> {
        self.0
            .dataset
            .text
            .shortnames()
            .iter()
            .map(|x| x.to_string())
            .collect()
    }

    // TODO add other converters here

    #[getter]
    fn inner(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.0.dataset.text {
            // TODO this copies all data from the "union type" into a new
            // version-specific type. This might not be a big deal, but these
            // types might be rather large with lots of strings.
            api::AnyCoreTEXT::FCS2_0(x) => PyCoreTEXT2_0::from((**x).clone()).into_py_any(py),
            api::AnyCoreTEXT::FCS3_0(x) => PyCoreTEXT3_0::from((**x).clone()).into_py_any(py),
            api::AnyCoreTEXT::FCS3_1(x) => PyCoreTEXT3_1::from((**x).clone()).into_py_any(py),
            api::AnyCoreTEXT::FCS3_2(x) => PyCoreTEXT3_2::from((**x).clone()).into_py_any(py),
        }
    }

    #[getter]
    fn data(&self) -> PyDataFrame {
        // NOTE polars Series is a wrapper around an Arc so clone just
        // increments the ref count for each column rather than "deepcopy" the
        // whole dataset.
        PyDataFrame(self.0.dataset.data.clone())
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
    ($pytype:ident, [$($root:ident,)*], $get:ident, $set:ident, $t:ty) => {
        #[pymethods]
        impl $pytype {
            #[getter]
            fn $get(&self) -> Option<$t> {
                self.0.$($root.)*$get()
            }

            #[setter]
            fn $set(&mut self, s: Option<$t>) {
                self.0.$($root.)*$set(s)
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
        // get_set_copied!(  $pytype, [$($root,)*], abrt, set_abrt, api::Abrt);
        // get_set_copied!(  $pytype, [$($root,)*], lost, set_lost, api::Lost);
        get_set_str!(     $pytype, [$($root,)*], cells, set_cells);
        get_set_str!(     $pytype, [$($root,)*], com, set_com);
        get_set_str!(     $pytype, [$($root,)*], exp, set_exp);
        get_set_str!(     $pytype, [$($root,)*], fil, set_fil);
        get_set_str!(     $pytype, [$($root,)*], inst, set_inst);
        get_set_str!(     $pytype, [$($root,)*], op, set_op);
        get_set_str!(     $pytype, [$($root,)*], proj, set_proj);
        get_set_str!(     $pytype, [$($root,)*], smno, set_smno);
        get_set_str!(     $pytype, [$($root,)*], src, set_src);
        get_set_str!(     $pytype, [$($root,)*], sys, set_sys);
    };
}

core_text_methods!(PyStandardizedTEXT, [standardized]);
core_text_methods!(PyCoreTEXT2_0, []);
core_text_methods!(PyCoreTEXT3_0, []);
core_text_methods!(PyCoreTEXT3_1, []);
core_text_methods!(PyCoreTEXT3_2, []);

#[pymethods]
impl PyCoreTEXT3_2 {
    // TODO allow user to set $COMP if they really want
    fn version_2_0(&self) -> PyResult<PyCoreTEXT2_0> {
        let new = api::CoreTEXT3_2::convert_core_def(
            self.0.clone(),
            api::CoreDefaults::new(
                api::MetadataDefaults3_2To2_0::default(),
                api::MeasurementDefaults3_2To2_0,
                self.0.par(),
            ),
        );
        handle_pure(new)
    }

    // TODO allow user to set $COMP/$UNICODE if they really want
    fn version_3_0(&self) -> PyResult<PyCoreTEXT3_0> {
        let new = api::CoreTEXT3_2::convert_core_def(
            self.0.clone(),
            api::CoreDefaults::new(
                api::MetadataDefaults3_2To3_0::default(),
                api::MeasurementDefaults3_2To3_0,
                self.0.par(),
            ),
        );
        handle_pure(new)
    }

    fn version_3_1(&self) -> PyResult<PyCoreTEXT3_1> {
        let new = api::CoreTEXT3_2::convert_core_def(
            self.0.clone(),
            api::CoreDefaults::new(
                api::MetadataDefaults3_2To3_1,
                api::MeasurementDefaults3_2To3_1,
                self.0.par(),
            ),
        );
        handle_pure(new)
    }

    // TODO make function to add DATA/ANALYSIS, which will convert this to a CoreDataset
}

struct FailWrapper(error::ImpureFailure);

fn handle_errors<X, Y>(res: error::ImpureResult<X>) -> PyResult<Y>
where
    Y: From<X>,
{
    handle_pure(res.map_err(FailWrapper)?)
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
