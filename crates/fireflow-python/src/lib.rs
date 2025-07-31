use fireflow_core::api;
use fireflow_core::config as cfg;
use fireflow_core::core;
use fireflow_core::data::{
    AnyAsciiLayout, AnyNullBitmask, AnyOrderedLayout, AnyOrderedUintLayout, DataLayout2_0,
    DataLayout3_0, DataLayout3_1, DataLayout3_2, DelimAsciiLayout, EndianLayout, F32Range,
    F64Range, FixedAsciiLayout, FixedLayout, FloatRange, KnownTot, LayoutOps, MixedLayout,
    NoMeasDatatype, NonMixedEndianLayout, NullMixedType, OrderedLayout, OrderedLayoutOps,
};
use fireflow_core::error::ResultExt;
use fireflow_core::header::{Header, Version};
use fireflow_core::python::exceptions::{PyTerminalNoWarnResultExt, PyTerminalResultExt};
use fireflow_core::segment::{HeaderAnalysisSegment, HeaderDataSegment, OtherSegment};
use fireflow_core::text::byteord::{Endian, SizedByteOrd};
use fireflow_core::text::compensation::Compensation;
use fireflow_core::text::index::MeasIndex;
use fireflow_core::text::keywords as kws;
use fireflow_core::text::named_vec::{Element, NamedVec, RawInput};
use fireflow_core::text::optional::{AlwaysFamily, MaybeFamily};
use fireflow_core::text::scale::Scale;
use fireflow_core::text::unstainedcenters::UnstainedCenters;
use fireflow_core::validated::bitmask as bm;
use fireflow_core::validated::dataframe::{AnyFCSColumn, FCSDataFrame};
use fireflow_core::validated::keys::{NonStdKey, StdKeywords, ValidKeywords};
use fireflow_core::validated::shortname::{Shortname, ShortnamePrefix};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};
use derive_more::{From, Into};
use nonempty::NonEmpty;
use numpy::{PyArray2, PyReadonlyArray2, ToPyArray};
use polars::prelude::*;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3_polars::PyDataFrame;
use std::collections::HashMap;
use std::num::NonZeroU8;
use std::path::PathBuf;

#[pyfunction]
#[pyo3(name = "_fcs_read_header")]
pub fn py_fcs_read_header(p: PathBuf, conf: cfg::ReadHeaderConfig) -> PyResult<Header> {
    api::fcs_read_header(&p, &conf).py_term_resolve_nowarn()
}

#[pyfunction]
#[pyo3(name = "_fcs_read_raw_text")]
pub fn py_fcs_read_raw_text(
    p: PathBuf,
    conf: cfg::ReadRawTEXTConfig,
) -> PyResult<api::RawTEXTOutput> {
    api::fcs_read_raw_text(&p, &conf).py_term_resolve()
}

#[pyfunction]
#[pyo3(name = "_fcs_read_std_text")]
pub fn py_fcs_read_std_text(
    p: PathBuf,
    conf: cfg::ReadStdTEXTConfig,
) -> PyResult<(PyAnyCoreTEXT, api::StdTEXTOutput)> {
    let (core, data) = api::fcs_read_std_text(&p, &conf).py_term_resolve()?;
    Ok((core.into(), data))
}

#[pyfunction]
#[pyo3(name = "_fcs_read_raw_dataset")]
pub fn py_fcs_read_raw_dataset(
    p: PathBuf,
    conf: cfg::ReadRawDatasetConfig,
) -> PyResult<api::RawDatasetOutput> {
    api::fcs_read_raw_dataset(&p, &conf).py_term_resolve()
}

#[pyfunction]
#[pyo3(name = "_fcs_read_std_dataset")]
pub fn py_fcs_read_std_dataset(
    p: PathBuf,
    conf: cfg::ReadStdDatasetConfig,
) -> PyResult<(PyAnyCoreDataset, api::StdDatasetOutput)> {
    let (core, data) = api::fcs_read_std_dataset(&p, &conf).py_term_resolve()?;
    Ok((core.into(), data))
}

#[pyfunction]
#[pyo3(name = "_fcs_read_raw_dataset_with_keywords")]
pub fn py_fcs_read_raw_dataset_with_keywords(
    p: PathBuf,
    version: Version,
    std: StdKeywords,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
    other_segs: Vec<OtherSegment>,
    conf: cfg::ReadRawDatasetFromKeywordsConfig,
) -> PyResult<api::RawDatasetWithKwsOutput> {
    api::fcs_read_raw_dataset_with_keywords(
        p,
        version,
        &std,
        data_seg,
        analysis_seg,
        other_segs,
        &conf,
    )
    .py_term_resolve()
}

#[pyfunction]
#[pyo3(name = "_fcs_read_std_dataset_with_keywords")]
pub fn py_fcs_read_std_dataset_with_keywords(
    p: PathBuf,
    version: Version,
    kws: ValidKeywords,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
    other_segs: Vec<OtherSegment>,
    conf: cfg::ReadStdDatasetFromKeywordsConfig,
) -> PyResult<(PyAnyCoreDataset, api::StdDatasetWithKwsOutput)> {
    let (core, data) = api::fcs_read_std_dataset_with_keywords(
        &p,
        version,
        kws,
        data_seg,
        analysis_seg,
        other_segs,
        &conf,
    )
    .py_term_resolve()?;
    Ok((core.into(), data))
}

macro_rules! py_wrap {
    ($pytype:ident, $rstype:path, $name:expr) => {
        #[pyclass(name = $name)]
        #[derive(Clone, From, Into)]
        pub struct $pytype($rstype);
    };
}

// core* objects
py_wrap!(PyCoreTEXT2_0, core::CoreTEXT2_0, "CoreTEXT2_0");
py_wrap!(PyCoreTEXT3_0, core::CoreTEXT3_0, "CoreTEXT3_0");
py_wrap!(PyCoreTEXT3_1, core::CoreTEXT3_1, "CoreTEXT3_1");
py_wrap!(PyCoreTEXT3_2, core::CoreTEXT3_2, "CoreTEXT3_2");

py_wrap!(PyCoreDataset2_0, core::CoreDataset2_0, "CoreDataset2_0");
py_wrap!(PyCoreDataset3_0, core::CoreDataset3_0, "CoreDataset3_0");
py_wrap!(PyCoreDataset3_1, core::CoreDataset3_1, "CoreDataset3_1");
py_wrap!(PyCoreDataset3_2, core::CoreDataset3_2, "CoreDataset3_2");

py_wrap!(PyOptical2_0, core::Optical2_0, "Optical2_0");
py_wrap!(PyOptical3_0, core::Optical3_0, "Optical3_0");
py_wrap!(PyOptical3_1, core::Optical3_1, "Optical3_1");
py_wrap!(PyOptical3_2, core::Optical3_2, "Optical3_2");

py_wrap!(PyTemporal2_0, core::Temporal2_0, "Temporal2_0");
py_wrap!(PyTemporal3_0, core::Temporal3_0, "Temporal3_0");
py_wrap!(PyTemporal3_1, core::Temporal3_1, "Temporal3_1");
py_wrap!(PyTemporal3_2, core::Temporal3_2, "Temporal3_2");

type AsciiDelim = FixedAsciiLayout<KnownTot, NoMeasDatatype, false>;
py_wrap!(PyAsciiFixedLayout, AsciiDelim, "AsciiFixedLayout");

type AsciiFixed = DelimAsciiLayout<KnownTot, NoMeasDatatype, false>;
py_wrap!(PyAsciiDelimLayout, AsciiFixed, "AsciiDelimLayout");

py_wrap!(PyOrderedUint08Layout, OrderedLayout<bm::Bitmask08, KnownTot>, "OrderedUint08Layout");
py_wrap!(PyOrderedUint16Layout, OrderedLayout<bm::Bitmask16, KnownTot>, "OrderedUint16Layout");
py_wrap!(PyOrderedUint24Layout, OrderedLayout<bm::Bitmask24, KnownTot>, "OrderedUint24Layout");
py_wrap!(PyOrderedUint32Layout, OrderedLayout<bm::Bitmask32, KnownTot>, "OrderedUint32Layout");
py_wrap!(PyOrderedUint40Layout, OrderedLayout<bm::Bitmask40, KnownTot>, "OrderedUint40Layout");
py_wrap!(PyOrderedUint48Layout, OrderedLayout<bm::Bitmask48, KnownTot>, "OrderedUint48Layout");
py_wrap!(PyOrderedUint56Layout, OrderedLayout<bm::Bitmask56, KnownTot>, "OrderedUint56Layout");
py_wrap!(PyOrderedUint64Layout, OrderedLayout<bm::Bitmask64, KnownTot>, "OrderedUint64Layout");

py_wrap!(PyOrderedF32Layout, OrderedLayout<F32Range, KnownTot>, "OrderedF32Layout");
py_wrap!(PyOrderedF64Layout, OrderedLayout<F64Range, KnownTot>, "OrderedF64Layout");

py_wrap!(PyEndianF32Layout, EndianLayout<F32Range, NoMeasDatatype>, "EndianF32Layout");
py_wrap!(PyEndianF64Layout, EndianLayout<F64Range, NoMeasDatatype>, "EndianF64Layout");

py_wrap!(PyEndianUintLayout, EndianLayout<AnyNullBitmask, NoMeasDatatype>, "EndianUintLayout");

py_wrap!(PyMixedLayout, MixedLayout, "MixedLayout");

#[derive(IntoPyObject, From)]
pub enum PyAnyCoreTEXT {
    #[from(core::CoreTEXT2_0)]
    FCS2_0(PyCoreTEXT2_0),
    #[from(core::CoreTEXT3_0)]
    FCS3_0(PyCoreTEXT3_0),
    #[from(core::CoreTEXT3_1)]
    FCS3_1(PyCoreTEXT3_1),
    #[from(core::CoreTEXT3_2)]
    FCS3_2(PyCoreTEXT3_2),
}

impl From<core::AnyCoreTEXT> for PyAnyCoreTEXT {
    fn from(value: core::AnyCoreTEXT) -> PyAnyCoreTEXT {
        match value {
            core::AnyCoreTEXT::FCS2_0(x) => (*x).into(),
            core::AnyCoreTEXT::FCS3_0(x) => (*x).into(),
            core::AnyCoreTEXT::FCS3_1(x) => (*x).into(),
            core::AnyCoreTEXT::FCS3_2(x) => (*x).into(),
        }
    }
}

#[derive(IntoPyObject, From)]
pub enum PyAnyCoreDataset {
    #[from(core::CoreDataset2_0)]
    FCS2_0(PyCoreDataset2_0),
    #[from(core::CoreDataset3_0)]
    FCS3_0(PyCoreDataset3_0),
    #[from(core::CoreDataset3_1)]
    FCS3_1(PyCoreDataset3_1),
    #[from(core::CoreDataset3_2)]
    FCS3_2(PyCoreDataset3_2),
}

impl From<core::AnyCoreDataset> for PyAnyCoreDataset {
    fn from(value: core::AnyCoreDataset) -> PyAnyCoreDataset {
        match value {
            core::AnyCoreDataset::FCS2_0(x) => (*x).into(),
            core::AnyCoreDataset::FCS3_0(x) => (*x).into(),
            core::AnyCoreDataset::FCS3_1(x) => (*x).into(),
            core::AnyCoreDataset::FCS3_2(x) => (*x).into(),
        }
    }
}

#[derive(FromPyObject, IntoPyObject)]
pub enum PyOrderedLayout {
    AsciiFixed(PyAsciiFixedLayout),
    AsciiDelim(PyAsciiDelimLayout),
    Uint08(PyOrderedUint08Layout),
    Uint16(PyOrderedUint16Layout),
    Uint24(PyOrderedUint24Layout),
    Uint32(PyOrderedUint32Layout),
    Uint40(PyOrderedUint40Layout),
    Uint48(PyOrderedUint48Layout),
    Uint56(PyOrderedUint56Layout),
    Uint64(PyOrderedUint64Layout),
    F32(PyOrderedF32Layout),
    F64(PyOrderedF64Layout),
}

#[derive(FromPyObject, IntoPyObject, From)]
pub enum PyNonMixedLayout {
    #[from(
        PyAsciiFixedLayout,
        FixedAsciiLayout<KnownTot, NoMeasDatatype, false>
    )]
    AsciiFixed(PyAsciiFixedLayout),

    #[from(
        PyAsciiDelimLayout,
        DelimAsciiLayout<KnownTot, NoMeasDatatype, false>
    )]
    AsciiDelim(PyAsciiDelimLayout),

    #[from(PyEndianUintLayout, EndianLayout<AnyNullBitmask, NoMeasDatatype>)]
    Uint(PyEndianUintLayout),

    #[from(PyEndianF32Layout, EndianLayout<F32Range, NoMeasDatatype>)]
    F32(PyEndianF32Layout),

    #[from(PyEndianF64Layout, EndianLayout<F64Range, NoMeasDatatype>)]
    F64(PyEndianF64Layout),
}

#[derive(FromPyObject, IntoPyObject, From)]
pub enum PyLayout3_2 {
    NonMixed(PyNonMixedLayout),
    Mixed(PyMixedLayout),
}

impl From<PyOrderedLayout> for DataLayout2_0 {
    fn from(value: PyOrderedLayout) -> Self {
        Self(AnyOrderedLayout::from(value).phantom_into())
    }
}

impl From<PyOrderedLayout> for DataLayout3_0 {
    fn from(value: PyOrderedLayout) -> Self {
        Self(AnyOrderedLayout::from(value))
    }
}

impl From<PyNonMixedLayout> for DataLayout3_1 {
    fn from(value: PyNonMixedLayout) -> Self {
        Self(NonMixedEndianLayout::from(value))
    }
}

impl From<PyLayout3_2> for DataLayout3_2 {
    fn from(value: PyLayout3_2) -> Self {
        match value {
            PyLayout3_2::Mixed(x) => Self::Mixed(x.into()),
            PyLayout3_2::NonMixed(x) => {
                Self::NonMixed(NonMixedEndianLayout::from(x).phantom_into())
            }
        }
    }
}

impl From<DataLayout2_0> for PyOrderedLayout {
    fn from(value: DataLayout2_0) -> Self {
        value.0.phantom_into().into()
    }
}

impl From<DataLayout3_0> for PyOrderedLayout {
    fn from(value: DataLayout3_0) -> Self {
        value.0.into()
    }
}

impl From<DataLayout3_1> for PyNonMixedLayout {
    fn from(value: DataLayout3_1) -> Self {
        value.0.into()
    }
}

impl From<DataLayout3_2> for PyLayout3_2 {
    fn from(value: DataLayout3_2) -> Self {
        match value {
            DataLayout3_2::Mixed(x) => Self::Mixed(x.into()),
            DataLayout3_2::NonMixed(x) => Self::NonMixed(x.phantom_into().into()),
        }
    }
}

impl From<PyOrderedLayout> for AnyOrderedLayout<KnownTot> {
    fn from(value: PyOrderedLayout) -> Self {
        match value {
            PyOrderedLayout::AsciiFixed(x) => AnyAsciiLayout::from(x.0).phantom_into().into(),
            PyOrderedLayout::AsciiDelim(x) => AnyAsciiLayout::from(x.0).phantom_into().into(),
            PyOrderedLayout::Uint08(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::Uint16(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::Uint24(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::Uint32(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::Uint40(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::Uint48(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::Uint56(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::Uint64(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::F32(x) => x.0.into(),
            PyOrderedLayout::F64(x) => x.0.into(),
        }
    }
}

impl From<AnyOrderedLayout<KnownTot>> for PyOrderedLayout {
    fn from(value: AnyOrderedLayout<KnownTot>) -> Self {
        match value {
            AnyOrderedLayout::Ascii(x) => match x.phantom_into() {
                AnyAsciiLayout::Delimited(y) => Self::AsciiDelim(y.into()),
                AnyAsciiLayout::Fixed(y) => Self::AsciiFixed(y.into()),
            },
            AnyOrderedLayout::Integer(x) => match x {
                AnyOrderedUintLayout::Uint08(y) => Self::Uint08(y.into()),
                AnyOrderedUintLayout::Uint16(y) => Self::Uint16(y.into()),
                AnyOrderedUintLayout::Uint24(y) => Self::Uint24(y.into()),
                AnyOrderedUintLayout::Uint32(y) => Self::Uint32(y.into()),
                AnyOrderedUintLayout::Uint40(y) => Self::Uint40(y.into()),
                AnyOrderedUintLayout::Uint48(y) => Self::Uint48(y.into()),
                AnyOrderedUintLayout::Uint56(y) => Self::Uint56(y.into()),
                AnyOrderedUintLayout::Uint64(y) => Self::Uint64(y.into()),
            },
            AnyOrderedLayout::F32(x) => Self::F32(x.into()),
            AnyOrderedLayout::F64(x) => Self::F64(x.into()),
        }
    }
}

impl From<NonMixedEndianLayout<NoMeasDatatype>> for PyNonMixedLayout {
    fn from(value: NonMixedEndianLayout<NoMeasDatatype>) -> Self {
        match value {
            NonMixedEndianLayout::Ascii(x) => match x {
                AnyAsciiLayout::Fixed(y) => y.into(),
                AnyAsciiLayout::Delimited(y) => y.into(),
            },
            NonMixedEndianLayout::Integer(x) => x.into(),
            NonMixedEndianLayout::F32(x) => x.into(),
            NonMixedEndianLayout::F64(x) => x.into(),
        }
    }
}

impl From<PyNonMixedLayout> for NonMixedEndianLayout<NoMeasDatatype> {
    fn from(value: PyNonMixedLayout) -> Self {
        match value {
            PyNonMixedLayout::AsciiFixed(x) => Self::Ascii(x.0.into()),
            PyNonMixedLayout::AsciiDelim(x) => Self::Ascii(x.0.into()),
            PyNonMixedLayout::Uint(x) => Self::Integer(x.into()),
            PyNonMixedLayout::F32(x) => Self::F32(x.into()),
            PyNonMixedLayout::F64(x) => Self::F64(x.into()),
        }
    }
}

macro_rules! get_set_metaroot {
    ($get:ident, $set:ident, $t:path, $($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn $get(&self) -> $t {
                    self.0.metaroot::<$t>().clone()
                }

                #[setter]
                fn $set(&mut self, x: $t) {
                    self.0.set_metaroot(x)
                }
            }
        )*
    };
}

macro_rules! get_set_metaroot_opt {
    ($get:ident, $set:ident, $t:path, $($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn $get(&self) -> Option<$t> {
                    self.0.metaroot_opt().cloned()
                }

                #[setter]
                fn $set(&mut self, s: Option<$t>) {
                    self.0.set_metaroot(s)
                }
            }
        )*
    };
}

macro_rules! get_set_all_meas {
    ($get:ident, $set:ident, $t:path, $($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn $get(&self) -> Vec<Option<$t>> {
                    self.0.meas_opt().map(|x| x.cloned()).collect()
                }

                #[setter]
                fn $set(&mut self, xs: Vec<Option<$t>>) -> PyResult<()> {
                    Ok(self.0.set_meas(xs)?)
                }
            }
        )*
    };
}

macro_rules! get_set_all_optical {
    ($get:ident, $set:ident, $t:path, $($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn $get(&self) -> Vec<(MeasIndex, Option<$t>)> {
                    self.0
                        .optical_opt()
                        .map(|(i, x)| (i, x.cloned()))
                        .collect()
                }

                fn $set(&mut self, xs: Vec<Option<$t>>) -> PyResult<()> {
                    Ok(self.0.set_optical(xs)?)
                }
            }
        )*
    };
}

macro_rules! convert_methods {
    ($pytype:ident, $([$fn:ident, $to:ident]),+) => {
        #[pymethods]
        impl $pytype {
            $(
                fn $fn(&self, lossless: bool) -> PyResult<$to> {
                    self.0.clone().try_convert(lossless).py_term_resolve().map(|x| x.into())
                }
            )*
        }
    };
}

convert_methods!(
    PyCoreTEXT2_0,
    [version_3_0, PyCoreTEXT3_0],
    [version_3_1, PyCoreTEXT3_1],
    [version_3_2, PyCoreTEXT3_2]
);

convert_methods!(
    PyCoreTEXT3_0,
    [version_2_0, PyCoreTEXT2_0],
    [version_3_1, PyCoreTEXT3_1],
    [version_3_2, PyCoreTEXT3_2]
);

convert_methods!(
    PyCoreTEXT3_1,
    [version_2_0, PyCoreTEXT2_0],
    [version_3_0, PyCoreTEXT3_0],
    [version_3_2, PyCoreTEXT3_2]
);

convert_methods!(
    PyCoreTEXT3_2,
    [version_2_0, PyCoreTEXT2_0],
    [version_3_0, PyCoreTEXT3_0],
    [version_3_1, PyCoreTEXT3_1]
);

convert_methods!(
    PyCoreDataset2_0,
    [version_3_0, PyCoreDataset3_0],
    [version_3_1, PyCoreDataset3_1],
    [version_3_2, PyCoreDataset3_2]
);

convert_methods!(
    PyCoreDataset3_0,
    [version_2_0, PyCoreDataset2_0],
    [version_3_1, PyCoreDataset3_1],
    [version_3_2, PyCoreDataset3_2]
);

convert_methods!(
    PyCoreDataset3_1,
    [version_2_0, PyCoreDataset2_0],
    [version_3_0, PyCoreDataset3_0],
    [version_3_2, PyCoreDataset3_2]
);

convert_methods!(
    PyCoreDataset3_2,
    [version_2_0, PyCoreDataset2_0],
    [version_3_0, PyCoreDataset3_0],
    [version_3_1, PyCoreDataset3_1]
);

#[pymethods]
impl PyCoreTEXT2_0 {
    #[new]
    fn new(mode: kws::Mode) -> PyResult<Self> {
        Ok(core::CoreTEXT2_0::new(mode).into())
    }
}

#[pymethods]
impl PyCoreTEXT3_0 {
    #[new]
    fn new(mode: kws::Mode) -> PyResult<Self> {
        Ok(core::CoreTEXT3_0::new(mode).into())
    }
}

#[pymethods]
impl PyCoreTEXT3_1 {
    #[new]
    fn new(mode: kws::Mode) -> Self {
        core::CoreTEXT3_1::new(mode).into()
    }
}

#[pymethods]
impl PyCoreTEXT3_2 {
    #[new]
    fn new(cyt: String) -> Self {
        core::CoreTEXT3_2::new(cyt).into()
    }

    #[getter]
    fn get_begindatetime(&self) -> Option<DateTime<FixedOffset>> {
        self.0.begindatetime()
    }

    #[setter]
    fn set_begindatetime(&mut self, x: Option<DateTime<FixedOffset>>) -> PyResult<()> {
        Ok(self.0.set_begindatetime(x)?)
    }

    #[getter]
    fn get_enddatetime(&self) -> Option<DateTime<FixedOffset>> {
        self.0.enddatetime()
    }

    #[setter]
    fn set_enddatetime(&mut self, x: Option<DateTime<FixedOffset>>) -> PyResult<()> {
        Ok(self.0.set_enddatetime(x)?)
    }

    #[getter]
    fn get_unstained_centers(&self) -> Option<HashMap<Shortname, f32>> {
        self.0.metaroot_opt::<UnstainedCenters>().map(|y| {
            <HashMap<Shortname, f32>>::from(y.clone())
                .into_iter()
                .collect()
        })
    }

    fn insert_unstained_center(&mut self, name: Shortname, value: f32) -> PyResult<Option<f32>> {
        Ok(self.0.insert_unstained_center(name, value)?)
    }

    fn remove_unstained_center(&mut self, name: Shortname) -> Option<f32> {
        self.0.remove_unstained_center(&name)
    }

    fn clear_unstained_centers(&mut self) {
        self.0.clear_unstained_centers()
    }
}

// Get/set methods for all versions
macro_rules! common_methods {
    ($pytype:ident) => {
        get_set_metaroot_opt!(get_abrt, set_abrt, kws::Abrt, $pytype);
        get_set_metaroot_opt!(get_cells, set_cells, kws::Cells, $pytype);
        get_set_metaroot_opt!(get_com, set_com, kws::Com, $pytype);
        get_set_metaroot_opt!(get_exp, set_exp, kws::Exp, $pytype);
        get_set_metaroot_opt!(get_fil, set_fil, kws::Fil, $pytype);
        get_set_metaroot_opt!(get_inst, set_inst, kws::Inst, $pytype);
        get_set_metaroot_opt!(get_lost, set_lost, kws::Lost, $pytype);
        get_set_metaroot_opt!(get_op, set_op, kws::Op, $pytype);
        get_set_metaroot_opt!(get_proj, set_proj, kws::Proj, $pytype);
        get_set_metaroot_opt!(get_smno, set_smno, kws::Smno, $pytype);
        get_set_metaroot_opt!(get_src, set_src, kws::Src, $pytype);
        get_set_metaroot_opt!(get_sys, set_sys, kws::Sys, $pytype);

        // common measurement keywords
        get_set_all_meas!(get_longnames, set_longnames, kws::Longname, $pytype);

        get_set_all_optical!(get_filters, set_filters, kws::Filter, $pytype);
        get_set_all_optical!(get_powers, set_powers, kws::Power, $pytype);

        get_set_all_optical!(
            get_percents_emitted,
            set_percents_emitted,
            kws::PercentEmitted,
            $pytype
        );

        get_set_all_optical!(
            get_detector_types,
            set_detector_types,
            kws::DetectorType,
            $pytype
        );

        get_set_all_optical!(
            get_detector_voltages,
            set_detector_voltages,
            kws::DetectorVoltage,
            $pytype
        );

        #[pymethods]
        impl $pytype {
            fn insert_nonstandard(&mut self, key: NonStdKey, value: String) -> Option<String> {
                self.0.metaroot.nonstandard_keywords.insert(key, value)
            }

            fn remove_nonstandard(&mut self, key: NonStdKey) -> Option<String> {
                self.0.metaroot.nonstandard_keywords.remove(&key)
            }

            fn get_nonstandard(&mut self, key: NonStdKey) -> Option<String> {
                self.0.metaroot.nonstandard_keywords.get(&key).cloned()
            }

            // TODO add way to remove nonstandard from the returned dict
            #[pyo3(signature = (want_req=None, want_meta=None))]
            fn raw_keywords(
                &self,
                want_req: Option<bool>,
                want_meta: Option<bool>,
            ) -> HashMap<String, String> {
                self.0.raw_keywords(want_req, want_meta).clone()
            }

            #[getter]
            fn par(&self) -> usize {
                self.0.par().0
            }

            // fn insert_meas_nonstandard(
            //     &mut self,
            //     keyvals: Vec<(NonStdKey, String)>,
            // ) -> PyResult<Vec<Option<String>>> {
            //     Ok(self.0.insert_meas_nonstandard(keyvals)?)
            // }

            // fn remove_meas_nonstandard(
            //     &mut self,
            //     keys: Vec<NonStdKey>,
            // ) -> PyResult<Vec<Option<String>>> {
            //     Ok(self.0.remove_meas_nonstandard(keys.iter().collect())?)
            // }

            // fn get_meas_nonstandard(
            //     &mut self,
            //     keys: Vec<NonStdKey>,
            // ) -> Option<Vec<Option<String>>> {
            //     self.0
            //         .get_meas_nonstandard(&keys[..])
            //         .map(|rs| rs.into_iter().map(|r| r.cloned()).collect())
            // }

            #[getter]
            fn get_btim(&self) -> Option<NaiveTime> {
                self.0.btim_naive()
            }

            #[setter]
            fn set_btim(&mut self, x: Option<NaiveTime>) -> PyResult<()> {
                Ok(self.0.set_btim_naive(x)?)
            }

            #[getter]
            fn get_etim(&self) -> Option<NaiveTime> {
                self.0.etim_naive()
            }

            #[setter]
            fn set_etim(&mut self, x: Option<NaiveTime>) -> PyResult<()> {
                Ok(self.0.set_etim_naive(x)?)
            }

            #[getter]
            fn get_date(&self) -> Option<NaiveDate> {
                self.0.date_naive()
            }

            #[setter]
            fn set_date(&mut self, x: Option<NaiveDate>) -> PyResult<()> {
                Ok(self.0.set_date_naive(x)?)
            }

            #[getter]
            fn trigger(&self) -> Option<kws::Trigger> {
                self.0.metaroot_opt().cloned()
            }

            #[setter]
            fn set_trigger(&mut self, tr: Option<kws::Trigger>) -> PyResult<()> {
                Ok(self.0.set_trigger(tr)?)
            }

            fn set_trigger_threshold(&mut self, threshold: u32) -> bool {
                self.0.set_trigger_threshold(threshold)
            }

            #[getter]
            fn shortnames_maybe(&self) -> Vec<Option<Shortname>> {
                self.0
                    .shortnames_maybe()
                    .into_iter()
                    .map(|x| x.cloned())
                    .collect()
            }

            #[getter]
            fn all_shortnames(&self) -> Vec<Shortname> {
                self.0.all_shortnames()
            }

            #[setter]
            fn set_all_shortnames(&mut self, names: Vec<Shortname>) -> PyResult<()> {
                Ok(self.0.set_all_shortnames(names).void()?)
            }
        }
    };
}

common_methods!(PyCoreTEXT2_0);
common_methods!(PyCoreTEXT3_0);
common_methods!(PyCoreTEXT3_1);
common_methods!(PyCoreTEXT3_2);
common_methods!(PyCoreDataset2_0);
common_methods!(PyCoreDataset3_0);
common_methods!(PyCoreDataset3_1);
common_methods!(PyCoreDataset3_2);

macro_rules! temporal_get_set_2_0 {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            fn set_temporal(&mut self, name: Shortname, force: bool) -> PyResult<bool> {
                self.0.set_temporal(&name, (), force).py_term_resolve()
            }

            fn set_temporal_at(&mut self, index: MeasIndex, force: bool) -> PyResult<bool> {
                self.0.set_temporal_at(index, (), force).py_term_resolve()
            }

            fn unset_temporal(&mut self, force: bool) -> PyResult<bool> {
                self.0
                    .unset_temporal(force)
                    .py_term_resolve()
                    .map(|x| x.is_some())
            }
        }
    };
}

temporal_get_set_2_0!(PyCoreTEXT2_0);
temporal_get_set_2_0!(PyCoreDataset2_0);

macro_rules! temporal_get_set_3_0 {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            fn set_temporal(
                &mut self,
                name: Shortname,
                timestep: kws::Timestep,
                force: bool,
            ) -> PyResult<bool> {
                self.0
                    .set_temporal(&name, timestep, force)
                    .py_term_resolve()
            }

            fn set_temporal_at(
                &mut self,
                index: MeasIndex,
                timestep: kws::Timestep,
                force: bool,
            ) -> PyResult<bool> {
                self.0
                    .set_temporal_at(index, timestep, force)
                    .py_term_resolve()
            }

            fn unset_temporal(&mut self, force: bool) -> PyResult<Option<kws::Timestep>> {
                self.0.unset_temporal(force).py_term_resolve()
            }
        }
    };
}

temporal_get_set_3_0!(PyCoreTEXT3_0);
temporal_get_set_3_0!(PyCoreTEXT3_1);
temporal_get_set_3_0!(PyCoreTEXT3_2);
temporal_get_set_3_0!(PyCoreDataset3_0);
temporal_get_set_3_0!(PyCoreDataset3_1);
temporal_get_set_3_0!(PyCoreDataset3_2);

macro_rules! common_meas_get_set {
    ($pytype:ident, $o:ident, $t:ident, $n:path, $fam:ident) => {
        #[pymethods]
        impl $pytype {
            #[getter]
            fn get_temporal(&self) -> Option<(MeasIndex, Shortname, $t)> {
                self.0
                    .temporal()
                    .map(|t| (t.index, t.key.clone(), t.value.clone().into()))
            }

            #[getter]
            fn measurements(&self) -> Vec<Element<$t, $o>> {
                // This might seem inefficient since we are cloning
                // everything, but if we want to map a python lambda
                // function over the measurements we would need to to do
                // this anyways, so simply returnig a copied list doesn't
                // lose anything and keeps this API simpler.
                let ms: &NamedVec<_, _, _, _> = self.0.as_ref();
                ms.iter()
                    .map(|(_, e)| e.bimap(|t| t.value.clone(), |o| o.value.clone()))
                    .map(|v| v.inner_into())
                    .collect()
            }

            // TODO this seems like it should return a key error
            fn remove_measurement_by_name(
                &mut self,
                name: Shortname,
            ) -> Option<(MeasIndex, Element<$t, $o>)> {
                self.0
                    .remove_measurement_by_name(&name)
                    .map(|(i, x)| (i, x.inner_into()))
            }

            fn remove_measurement_by_index(
                &mut self,
                index: MeasIndex,
            ) -> PyResult<($n, Element<$t, $o>)> {
                let r = self.0.remove_measurement_by_index(index)?;
                let (n, v) = Element::unzip::<$fam>(r);
                Ok((n.0, v.inner_into()))
            }

            fn measurement_at(&self, index: MeasIndex) -> PyResult<Element<$t, $o>> {
                let ms: &NamedVec<_, _, _, _> = self.0.as_ref();
                let m = ms.get(index)?;
                Ok(m.bimap(|x| x.1.clone(), |x| x.1.clone()).inner_into())
            }

            fn replace_optical_at(
                &mut self,
                index: MeasIndex,
                meas: $o,
            ) -> PyResult<Element<$t, $o>> {
                let ret = self.0.replace_optical_at(index, meas.into())?;
                Ok(ret.inner_into())
            }

            fn replace_optical_named(
                &mut self,
                name: Shortname,
                meas: $o,
            ) -> Option<Element<$t, $o>> {
                self.0
                    .replace_optical_named(&name, meas.into())
                    .map(|r| r.inner_into())
            }

            fn rename_temporal(&mut self, name: Shortname) -> Option<Shortname> {
                self.0.rename_temporal(name)
            }

            fn replace_temporal_at(
                &mut self,
                index: MeasIndex,
                meas: $t,
                force: bool,
            ) -> PyResult<Element<$t, $o>> {
                let ret = self
                    .0
                    .replace_temporal_at(index, meas.into(), force)
                    .py_term_resolve()?;
                Ok(ret.inner_into())
            }

            fn replace_temporal_named(
                &mut self,
                name: Shortname,
                meas: $t,
                force: bool,
            ) -> PyResult<Option<Element<$t, $o>>> {
                let ret = self
                    .0
                    .replace_temporal_named(&name, meas.into(), force)
                    .py_term_resolve()?;
                Ok(ret.map(|r| r.inner_into()))
            }
        }
    };
}

common_meas_get_set!(
    PyCoreTEXT2_0,
    PyOptical2_0,
    PyTemporal2_0,
    Option<Shortname>,
    MaybeFamily
);
common_meas_get_set!(
    PyCoreTEXT3_0,
    PyOptical3_0,
    PyTemporal3_0,
    Option<Shortname>,
    MaybeFamily
);
common_meas_get_set!(
    PyCoreTEXT3_1,
    PyOptical3_1,
    PyTemporal3_1,
    Shortname,
    AlwaysFamily
);
common_meas_get_set!(
    PyCoreTEXT3_2,
    PyOptical3_2,
    PyTemporal3_2,
    Shortname,
    AlwaysFamily
);

common_meas_get_set!(
    PyCoreDataset2_0,
    PyOptical2_0,
    PyTemporal2_0,
    Option<Shortname>,
    MaybeFamily
);
common_meas_get_set!(
    PyCoreDataset3_0,
    PyOptical3_0,
    PyTemporal3_0,
    Option<Shortname>,
    MaybeFamily
);
common_meas_get_set!(
    PyCoreDataset3_1,
    PyOptical3_1,
    PyTemporal3_1,
    Shortname,
    AlwaysFamily
);
common_meas_get_set!(
    PyCoreDataset3_2,
    PyOptical3_2,
    PyTemporal3_2,
    Shortname,
    AlwaysFamily
);

macro_rules! common_coretext_meas_get_set {
    ($pytype:ident, $o:ident, $t:ident, $n:path) => {
        #[pymethods]
        impl $pytype {
            #[pyo3(signature = (meas, name, range, notrunc = false))]
            fn push_optical(
                &mut self,
                meas: $o,
                name: $n,
                range: kws::Range,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .push_optical(name.into(), meas.into(), range, notrunc)
                    .py_term_resolve()
                    .void()
            }

            #[pyo3(signature = (index, meas, name, range, notrunc = false))]
            fn insert_optical(
                &mut self,
                index: MeasIndex,
                meas: $o,
                name: $n,
                range: kws::Range,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .insert_optical(index, name.into(), meas.into(), range, notrunc)
                    .py_term_resolve()
                    .void()
            }

            #[pyo3(signature = (meas, name, range, notrunc = false))]
            fn push_temporal(
                &mut self,
                meas: $t,
                name: Shortname,
                range: kws::Range,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .push_temporal(name, meas.into(), range, notrunc)
                    .py_term_resolve()
            }

            #[pyo3(signature = (index, meas, name, range, notrunc = false))]
            fn insert_temporal(
                &mut self,
                index: MeasIndex,
                meas: $t,
                name: Shortname,
                range: kws::Range,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .insert_temporal(index, name, meas.into(), range, notrunc)
                    .py_term_resolve()
            }

            fn unset_measurements(&mut self) -> PyResult<()> {
                Ok(self.0.unset_measurements()?)
            }
        }
    };
}

common_coretext_meas_get_set!(
    PyCoreTEXT2_0,
    PyOptical2_0,
    PyTemporal2_0,
    Option<Shortname>
);
common_coretext_meas_get_set!(
    PyCoreTEXT3_0,
    PyOptical3_0,
    PyTemporal3_0,
    Option<Shortname>
);
common_coretext_meas_get_set!(PyCoreTEXT3_1, PyOptical3_1, PyTemporal3_1, Shortname);
common_coretext_meas_get_set!(PyCoreTEXT3_2, PyOptical3_2, PyTemporal3_2, Shortname);

macro_rules! coredata_meas_get_set {
    ($pytype:ident, $o:ident, $t:ident, $n:path) => {
        #[pymethods]
        impl $pytype {
            #[pyo3(signature = (meas, col, name, range, notrunc = false))]
            fn push_optical(
                &mut self,
                meas: $o,
                col: AnyFCSColumn,
                name: $n,
                range: kws::Range,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .push_optical(name.into(), meas.into(), col, range, notrunc)
                    .py_term_resolve()
                    .void()
            }

            #[pyo3(signature = (index, meas, col, name, range, notrunc = false))]
            fn insert_optical(
                &mut self,
                index: MeasIndex,
                meas: $o,
                col: AnyFCSColumn,
                name: $n,
                range: kws::Range,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .insert_optical(index, name.into(), meas.into(), col, range, notrunc)
                    .py_term_resolve()
                    .void()
            }

            #[pyo3(signature = (meas, col, name, range, notrunc = false))]
            fn push_temporal(
                &mut self,
                meas: $t,
                col: AnyFCSColumn,
                name: Shortname,
                range: kws::Range,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .push_temporal(name, meas.into(), col, range, notrunc)
                    .py_term_resolve()
            }

            #[pyo3(signature = (index, meas, col, name, range, notrunc = false))]
            fn insert_temporal(
                &mut self,
                index: MeasIndex,
                meas: $t,
                col: AnyFCSColumn,
                name: Shortname,
                range: kws::Range,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .insert_temporal(index, name, meas.into(), col, range, notrunc)
                    .py_term_resolve()
            }

            fn unset_data(&mut self) -> PyResult<()> {
                Ok(self.0.unset_data()?)
            }

            #[getter]
            fn data(&self) -> PyDataFrame {
                let ns = self.0.all_shortnames();
                let df: &FCSDataFrame = self.0.as_ref();
                let columns = df
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
            fn analysis(&self) -> core::Analysis {
                self.0.analysis.clone()
            }

            #[setter]
            fn set_analysis(&mut self, xs: core::Analysis) {
                self.0.analysis = xs.into();
            }

            #[getter]
            fn others(&self) -> core::Others {
                self.0.others.clone()
            }

            #[setter]
            fn set_others(&mut self, xs: core::Others) {
                self.0.others = xs
            }
        }
    };
}

coredata_meas_get_set!(
    PyCoreDataset2_0,
    PyOptical2_0,
    PyTemporal2_0,
    Option<Shortname>
);
coredata_meas_get_set!(
    PyCoreDataset3_0,
    PyOptical3_0,
    PyTemporal3_0,
    Option<Shortname>
);
coredata_meas_get_set!(PyCoreDataset3_1, PyOptical3_1, PyTemporal3_1, Shortname);
coredata_meas_get_set!(PyCoreDataset3_2, PyOptical3_2, PyTemporal3_2, Shortname);

macro_rules! set_measurements_ordered {
    ($pytype:ident, $t:ident, $o:ident) => {
        #[pymethods]
        impl $pytype {
            fn set_measurements(
                &mut self,
                measurements: RawInput<MaybeFamily, $t, $o>,
                prefix: ShortnamePrefix,
            ) -> PyResult<()> {
                self.0
                    .set_measurements(measurements.inner_into(), prefix)
                    .py_term_resolve_nowarn()
            }

            fn set_measurements_and_layout(
                &mut self,
                measurements: RawInput<MaybeFamily, $t, $o>,
                layout: PyOrderedLayout,
                prefix: ShortnamePrefix,
            ) -> PyResult<()> {
                self.0
                    .set_measurements_and_layout(measurements.inner_into(), layout.into(), prefix)
                    .py_term_resolve_nowarn()
            }

            #[getter]
            fn get_layout(&self) -> Option<PyOrderedLayout> {
                let x: &Option<_> = self.0.as_ref();
                x.as_ref().map(|y| y.clone().into())
            }

            fn set_layout(&mut self, layout: PyOrderedLayout) -> PyResult<()> {
                self.0.set_layout(layout.into()).py_term_resolve_nowarn()
            }
        }
    };
}

set_measurements_ordered!(PyCoreTEXT2_0, PyTemporal2_0, PyOptical2_0);
set_measurements_ordered!(PyCoreTEXT3_0, PyTemporal3_0, PyOptical3_0);
set_measurements_ordered!(PyCoreDataset2_0, PyTemporal2_0, PyOptical2_0);
set_measurements_ordered!(PyCoreDataset3_0, PyTemporal3_0, PyOptical3_0);

macro_rules! set_measurements_endian {
    ($pytype:ident, $t:ident, $o:ident, $l:ident) => {
        #[pymethods]
        impl $pytype {
            pub fn set_measurements(
                &mut self,
                measurements: RawInput<AlwaysFamily, $t, $o>,
            ) -> PyResult<()> {
                self.0
                    .set_measurements_noprefix(measurements.inner_into())
                    .py_term_resolve_nowarn()
            }

            fn set_measurements_and_layout(
                &mut self,
                measurements: RawInput<AlwaysFamily, $t, $o>,
                layout: $l,
            ) -> PyResult<()> {
                self.0
                    .set_measurements_and_layout_noprefix(measurements.inner_into(), layout.into())
                    .py_term_resolve_nowarn()
            }

            #[getter]
            fn get_layout(&self) -> Option<$l> {
                let x: &Option<_> = self.0.as_ref();
                x.as_ref().map(|y| y.clone().into())
            }

            fn set_layout(&mut self, layout: $l) -> PyResult<()> {
                self.0.set_layout(layout.into()).py_term_resolve_nowarn()
            }
        }
    };
}

set_measurements_endian!(PyCoreTEXT3_1, PyTemporal3_1, PyOptical3_1, PyNonMixedLayout);
set_measurements_endian!(PyCoreTEXT3_2, PyTemporal3_2, PyOptical3_2, PyLayout3_2);
set_measurements_endian!(
    PyCoreDataset3_1,
    PyTemporal3_1,
    PyOptical3_1,
    PyNonMixedLayout
);
set_measurements_endian!(PyCoreDataset3_2, PyTemporal3_2, PyOptical3_2, PyLayout3_2);

// TODO use a real dataframe here rather than a list of series
macro_rules! coredata2_0_meas_methods {
    ($pytype:ident, $t:ident, $o:ident) => {
        #[pymethods]
        impl $pytype {
            fn set_measurements_and_data(
                &mut self,
                measurements: RawInput<MaybeFamily, $t, $o>,
                cols: Vec<AnyFCSColumn>,
                prefix: ShortnamePrefix,
            ) -> PyResult<()> {
                self.0
                    .set_measurements_and_data(measurements.inner_into(), cols, prefix)
                    .py_term_resolve_nowarn()
            }
        }
    };
}

coredata2_0_meas_methods!(PyCoreDataset2_0, PyTemporal2_0, PyOptical2_0);
coredata2_0_meas_methods!(PyCoreDataset3_0, PyTemporal3_0, PyOptical3_0);

macro_rules! coredata3_1_meas_methods {
    ($pytype:ident, $t:ident, $o:ident) => {
        #[pymethods]
        impl $pytype {
            fn set_measurements_and_data(
                &mut self,
                measurements: RawInput<AlwaysFamily, $t, $o>,
                cols: Vec<AnyFCSColumn>,
            ) -> PyResult<()> {
                self.0
                    .set_measurements_and_data_noprefix(measurements.inner_into(), cols)
                    .py_term_resolve_nowarn()
            }
        }
    };
}

coredata3_1_meas_methods!(PyCoreDataset3_1, PyTemporal3_1, PyOptical3_1);
coredata3_1_meas_methods!(PyCoreDataset3_2, PyTemporal3_2, PyOptical3_2);

// Get/set methods for setting $PnN (2.0-3.0)
macro_rules! shortnames_methods {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            fn set_measurement_shortnames_maybe(
                &mut self,
                names: Vec<Option<Shortname>>,
            ) -> PyResult<()> {
                Ok(self.0.set_measurement_shortnames_maybe(names).void()?)
            }
        }
    };
}

shortnames_methods!(PyCoreTEXT2_0);
shortnames_methods!(PyCoreTEXT3_0);
shortnames_methods!(PyCoreDataset2_0);
shortnames_methods!(PyCoreDataset3_0);

// Get/set methods for $PnE (2.0)
macro_rules! scales_methods {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            #[getter]
            fn get_all_scales(&self) -> Vec<Option<Scale>> {
                self.0.all_scales().collect()
            }

            #[getter]
            fn get_scales(&self) -> Vec<(MeasIndex, Option<Scale>)> {
                self.0
                    .optical_opt::<Scale>()
                    .map(|(i, s)| (i, s.map(|&x| x)))
                    .collect()
            }

            #[setter]
            fn set_scales(&mut self, scales: Vec<Option<Scale>>) -> PyResult<()> {
                self.0.set_scales(scales).py_term_resolve_nowarn()
            }
        }
    };
}

scales_methods!(PyCoreTEXT2_0);
scales_methods!(PyCoreDataset2_0);

// Get/set methods for $PnE (3.0-3.2)
macro_rules! transforms_methods {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            #[getter]
            fn get_all_transforms(&self) -> Vec<core::ScaleTransform> {
                self.0.all_transforms().collect()
            }

            #[getter]
            fn get_transforms(&self) -> Vec<(MeasIndex, core::ScaleTransform)> {
                self.0.optical().map(|(i, &s)| (i, s)).collect()
            }

            #[setter]
            fn set_transforms(&mut self, transforms: Vec<core::ScaleTransform>) -> PyResult<()> {
                self.0.set_transforms(transforms).py_term_resolve_nowarn()
            }
        }
    };
}

transforms_methods!(PyCoreTEXT3_0);
transforms_methods!(PyCoreTEXT3_1);
transforms_methods!(PyCoreTEXT3_2);
transforms_methods!(PyCoreDataset3_0);
transforms_methods!(PyCoreDataset3_1);
transforms_methods!(PyCoreDataset3_2);

// Get/set methods for $TIMESTEP (3.0-3.2)
macro_rules! timestep_methods {
    ($($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn get_timestep(&self) -> Option<kws::Timestep> {
                    self.0.timestep().copied()
                }

                fn set_timestep(&mut self, timestep: kws::Timestep) -> bool {
                    self.0.set_timestep(timestep)
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

// Get/set methods for scaler $PnL (2.0-3.0)
get_set_all_optical!(
    get_wavelengths,
    set_wavelengths,
    kws::Wavelength,
    PyCoreTEXT2_0,
    PyCoreTEXT3_0,
    PyCoreDataset2_0,
    PyCoreDataset3_0
);

// Get/set methods for vector $PnL (3.1-3.2)
get_set_all_optical!(
    get_wavelengths,
    set_wavelengths,
    kws::Wavelengths,
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_1,
    PyCoreDataset3_2
);

// Get/set methods for $LAST_MODIFIER/$LAST_MODIFIED/$ORIGINALITY (3.1-3.2)
macro_rules! modification_methods {
    ($pytype:ident) => {
        get_set_metaroot_opt!(get_originality, set_originality, kws::Originality, $pytype);

        get_set_metaroot_opt!(
            get_last_modified,
            set_last_modified,
            kws::ModifiedDateTime,
            $pytype
        );

        get_set_metaroot_opt!(
            get_last_modifier,
            set_last_modifier,
            kws::LastModifier,
            $pytype
        );
    };
}

modification_methods!(PyCoreTEXT3_1);
modification_methods!(PyCoreTEXT3_2);
modification_methods!(PyCoreDataset3_1);
modification_methods!(PyCoreDataset3_2);

// Get/set methods for $CARRIERID/$CARRIERTYPE/$LOCATIONID (3.2)
macro_rules! carrier_methods {
    ($pytype:ident) => {
        get_set_metaroot_opt!(get_carriertype, set_carriertype, kws::Carriertype, $pytype);
        get_set_metaroot_opt!(get_carrierid, set_carrierid, kws::Carrierid, $pytype);
        get_set_metaroot_opt!(get_locationid, set_locationid, kws::Locationid, $pytype);
    };
}

carrier_methods!(PyCoreTEXT3_2);
carrier_methods!(PyCoreDataset3_2);

// Get/set methods for $PLATEID/$WELLID/$PLATENAME (3.1-3.2)
macro_rules! plate_methods {
    ($pytype:ident) => {
        get_set_metaroot_opt!(get_wellid, set_wellid, kws::Wellid, $pytype);
        get_set_metaroot_opt!(get_plateid, set_plateid, kws::Plateid, $pytype);
        get_set_metaroot_opt!(get_platename, set_platename, kws::Platename, $pytype);
    };
}

plate_methods!(PyCoreTEXT3_1);
plate_methods!(PyCoreTEXT3_2);
plate_methods!(PyCoreDataset3_1);
plate_methods!(PyCoreDataset3_2);

// get/set methods for $COMP (2.0-3.0)
macro_rules! comp_methods {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            #[getter]
            fn get_compensation(&self) -> Option<Compensation> {
                self.0.compensation().cloned()
            }

            #[setter]
            fn set_compensation(&mut self, m: Option<Compensation>) -> PyResult<()> {
                Ok(self.0.set_compensation(m)?)
            }
        }
    };
}

comp_methods!(PyCoreTEXT2_0);
comp_methods!(PyCoreTEXT3_0);
comp_methods!(PyCoreDataset2_0);
comp_methods!(PyCoreDataset3_0);

// Get/set methods for $SPILLOVER (3.1-3.2)
macro_rules! spillover_methods {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            #[getter]
            fn get_spillover_matrix<'a>(&self, py: Python<'a>) -> Option<Bound<'a, PyArray2<f32>>> {
                self.0.spillover_matrix().map(|x| x.to_pyarray(py))
            }

            #[getter]
            fn get_spillover_names(&self) -> Vec<Shortname> {
                self.0
                    .spillover_names()
                    .map(|x| x.to_vec())
                    .unwrap_or_default()
            }

            fn set_spillover(
                &mut self,
                names: Vec<Shortname>,
                matrix: PyReadonlyArray2<f32>,
            ) -> PyResult<()> {
                let m = matrix.as_matrix().into_owned();
                Ok(self.0.set_spillover(names, m)?)
            }

            fn unset_spillover(&mut self) {
                self.0.unset_spillover()
            }
        }
    };
}

spillover_methods!(PyCoreTEXT3_1);
spillover_methods!(PyCoreTEXT3_2);
spillover_methods!(PyCoreDataset3_1);
spillover_methods!(PyCoreDataset3_2);

get_set_metaroot_opt!(
    get_unicode,
    set_unicode,
    kws::Unicode,
    PyCoreTEXT3_0,
    PyCoreDataset3_0
);

get_set_metaroot_opt!(
    get_vol,
    set_vol,
    kws::Vol,
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_1,
    PyCoreDataset3_2
);

// Get/set methods for $MODE (2.0-3.1)
get_set_metaroot!(
    get_mode,
    set_mode,
    kws::Mode,
    PyCoreTEXT2_0,
    PyCoreTEXT3_0,
    PyCoreTEXT3_1,
    PyCoreDataset2_0,
    PyCoreDataset3_0,
    PyCoreDataset3_1
);

// Get/set methods for $MODE (3.2)
get_set_metaroot_opt!(
    get_mode,
    set_mode,
    kws::Mode3_2,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for (optional) $CYT (2.0-3.1)
//
// 3.2 is required which is why it is not included here
get_set_metaroot_opt!(
    get_cyt,
    set_cyt,
    kws::Cyt,
    PyCoreTEXT2_0,
    PyCoreTEXT3_0,
    PyCoreTEXT3_1,
    PyCoreDataset2_0,
    PyCoreDataset3_0,
    PyCoreDataset3_1
);

// Get/set methods for $FLOWRATE (3.2)
get_set_metaroot_opt!(
    get_flowrate,
    set_flowrate,
    kws::Flowrate,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $CYTSN (3.0-3.2)
get_set_metaroot_opt!(
    get_cytsn,
    set_cytsn,
    kws::Cytsn,
    PyCoreTEXT3_0,
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_0,
    PyCoreDataset3_1,
    PyCoreDataset3_2
);

// Get/set methods for $CYT (required) (3.2)
get_set_metaroot!(get_cyt, set_cyt, kws::Cyt, PyCoreTEXT3_2, PyCoreDataset3_2);

// Get/set methods for $UNSTAINEDINFO (3.2)
get_set_metaroot_opt!(
    get_unstainedinfo,
    set_unstainedinfo,
    kws::UnstainedInfo,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnD (3.1+)
//
// This is valid for the time channel so don't set on just optical
get_set_all_meas!(
    get_displays,
    set_displays,
    kws::Display,
    PyCoreTEXT3_1,
    PyCoreDataset3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnDET (3.2)
get_set_all_optical!(
    get_detector_names,
    set_detector_names,
    kws::DetectorName,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnCALIBRATION (3.1)
get_set_all_optical!(
    get_calibrations,
    set_calibrations,
    kws::Calibration3_1,
    PyCoreTEXT3_1,
    PyCoreDataset3_1
);

// Get/set methods for $PnCALIBRATION (3.2)
get_set_all_optical!(
    get_calibrations,
    set_calibrations,
    kws::Calibration3_2,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnTAG (3.2)
get_set_all_optical!(
    get_tags,
    set_tags,
    kws::Tag,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnTYPE (3.2)
get_set_all_optical!(
    get_measurement_types,
    set_measurement_types,
    kws::OpticalType,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnFEATURE (3.2)
get_set_all_optical!(
    get_features,
    set_features,
    kws::Feature,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnANALYTE (3.2)
get_set_all_optical!(
    get_analytes,
    set_analytes,
    kws::Analyte,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Add method to convert CoreTEXT* to CoreDataset* by adding DATA, ANALYSIS, and
// OTHER(s) (all versions)
macro_rules! to_dataset_method {
    ($from:ident, $to:ident) => {
        #[pymethods]
        impl $from {
            fn to_dataset(
                &self,
                cols: Vec<AnyFCSColumn>,
                analysis: core::Analysis,
                others: core::Others,
            ) -> PyResult<$to> {
                let df = self.0.clone().into_coredataset(cols, analysis, others)?;
                Ok(df.into())
            }
        }
    };
}

to_dataset_method!(PyCoreTEXT2_0, PyCoreDataset2_0);
to_dataset_method!(PyCoreTEXT3_0, PyCoreDataset3_0);
to_dataset_method!(PyCoreTEXT3_1, PyCoreDataset3_1);
to_dataset_method!(PyCoreTEXT3_2, PyCoreDataset3_2);

#[pymethods]
impl PyOptical2_0 {
    #[new]
    fn new() -> Self {
        core::Optical2_0::default().into()
    }

    #[getter]
    fn get_scale(&self) -> Option<Scale> {
        self.0.specific.scale.0.as_ref().map(|&x| x)
    }

    #[setter]
    fn set_scale(&mut self, x: Option<Scale>) {
        self.0.specific.scale = x.into()
    }
}

#[pymethods]
impl PyOptical3_0 {
    #[new]
    fn new(scale: Scale) -> Self {
        core::Optical3_0::new(scale).into()
    }
}

#[pymethods]
impl PyOptical3_1 {
    #[new]
    fn new(scale: Scale) -> Self {
        core::Optical3_1::new(scale).into()
    }
}

#[pymethods]
impl PyOptical3_2 {
    #[new]
    fn new(scale: Scale) -> Self {
        core::Optical3_2::new(scale).into()
    }
}

#[pymethods]
impl PyTemporal2_0 {
    #[new]
    fn new() -> Self {
        core::Temporal2_0::default().into()
    }
}

#[pymethods]
impl PyTemporal3_0 {
    #[new]
    fn new(timestep: kws::Timestep) -> Self {
        core::Temporal3_0::new(timestep).into()
    }
}

#[pymethods]
impl PyTemporal3_1 {
    #[new]
    fn new(timestep: kws::Timestep) -> Self {
        core::Temporal3_1::new(timestep).into()
    }
}

#[pymethods]
impl PyTemporal3_2 {
    #[new]
    fn new(timestep: kws::Timestep) -> Self {
        core::Temporal3_2::new(timestep).into()
    }

    #[getter]
    fn get_measurement_type(&self) -> bool {
        self.0.specific.measurement_type.0.is_some()
    }

    #[setter]
    fn set_measurement_type(&mut self, x: bool) {
        self.0.specific.measurement_type = if x { Some(kws::TemporalType) } else { None }.into();
    }
}

macro_rules! get_set_meas {
    ($get:ident, $set:ident, $t:path, $($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn $get(&self) -> Option<$t> {
                    let x: &Option<$t> = self.0.as_ref();
                    x.as_ref().cloned()
                }

                #[setter]
                fn $set(&mut self, x: Option<$t>) {
                    *self.0.as_mut() = x
                }
            }
        )*
    };
}

macro_rules! shared_meas_get_set {
    ($pytype:ident) => {
        get_set_meas!(get_longname, set_longname, kws::Longname, $pytype);

        #[pymethods]
        impl $pytype {
            #[getter]
            fn nonstandard_keywords(&self) -> HashMap<NonStdKey, String> {
                self.0.common.nonstandard_keywords.clone()
            }

            #[setter]
            fn set_nonstandard_keywords(&mut self, keyvals: HashMap<NonStdKey, String>) {
                self.0.common.nonstandard_keywords = keyvals;
            }

            fn nonstandard_insert(&mut self, key: NonStdKey, value: String) -> Option<String> {
                self.0.common.nonstandard_keywords.insert(key, value)
            }

            fn nonstandard_get(&self, key: NonStdKey) -> Option<String> {
                self.0.common.nonstandard_keywords.get(&key).cloned()
            }

            fn nonstandard_remove(&mut self, key: NonStdKey) -> Option<String> {
                self.0.common.nonstandard_keywords.remove(&key)
            }
        }
    };
}

shared_meas_get_set!(PyOptical2_0);
shared_meas_get_set!(PyOptical3_0);
shared_meas_get_set!(PyOptical3_1);
shared_meas_get_set!(PyOptical3_2);
shared_meas_get_set!(PyTemporal2_0);
shared_meas_get_set!(PyTemporal3_0);
shared_meas_get_set!(PyTemporal3_1);
shared_meas_get_set!(PyTemporal3_2);

macro_rules! optical_common {
    ($pytype:ident) => {
        get_set_meas!(get_filter, set_filter, kws::Filter, $pytype);
        get_set_meas!(
            get_detector_type,
            set_detector_type,
            kws::DetectorType,
            $pytype
        );
        get_set_meas!(
            get_percent_emitted,
            set_percent_emitted,
            kws::PercentEmitted,
            $pytype
        );
        get_set_meas!(
            get_detector_voltage,
            set_detector_voltage,
            kws::DetectorVoltage,
            $pytype
        );
        get_set_meas!(get_power, set_power, kws::Power, $pytype);
    };
}

optical_common!(PyOptical2_0);
optical_common!(PyOptical3_0);
optical_common!(PyOptical3_1);
optical_common!(PyOptical3_2);

// $PnE (3.0-3.2)
macro_rules! get_set_meas_transform {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            #[getter]
            fn get_transform(&self) -> core::ScaleTransform {
                self.0.specific.scale
            }

            #[setter]
            fn set_transform(&mut self, transform: core::ScaleTransform) {
                self.0.specific.scale = transform;
            }
        }
    };
}

get_set_meas_transform!(PyOptical3_0);
get_set_meas_transform!(PyOptical3_1);
get_set_meas_transform!(PyOptical3_2);

// $PnL (2.0/3.0)
get_set_meas!(
    get_wavelength,
    set_wavelength,
    kws::Wavelength,
    PyOptical2_0,
    PyOptical3_0
);

// #PnL (3.1-3.2)
get_set_meas!(
    get_wavelength,
    set_wavelength,
    kws::Wavelengths,
    PyOptical3_1,
    PyOptical3_2
);

// #TIMESTEP (3.0-3.2)
macro_rules! meas_get_set_timestep {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            #[getter]
            fn get_timestep(&self) -> kws::Timestep {
                self.0.specific.timestep
            }

            #[setter]
            fn set_timestep(&mut self, timestep: kws::Timestep) {
                self.0.specific.timestep = timestep
            }
        }
    };
}

meas_get_set_timestep!(PyTemporal3_0);
meas_get_set_timestep!(PyTemporal3_1);
meas_get_set_timestep!(PyTemporal3_2);

// $PnCalibration (3.1)
get_set_meas!(
    get_calibration,
    set_calibration,
    kws::Calibration3_1,
    PyOptical3_1
);

// $PnD (3.1-3.2)
get_set_meas!(
    get_display,
    set_display,
    kws::Display,
    PyOptical3_1,
    PyOptical3_2,
    PyTemporal3_1,
    PyTemporal3_2
);

// $PnDET (3.2)
get_set_meas!(
    get_detector_name,
    set_detector_name,
    kws::DetectorName,
    PyOptical3_2
);

// $PnTAG (3.2)
get_set_meas!(get_tag, set_tag, kws::Tag, PyOptical3_2);

// $PnTYPE (3.2)
get_set_meas!(
    get_measurement_type,
    set_measurement_type,
    kws::OpticalType,
    PyOptical3_2
);

// $PnFEATURE (3.2)
get_set_meas!(get_feature, set_feature, kws::Feature, PyOptical3_2);

// $PnANALYTE (3.2)
get_set_meas!(get_analyte, set_analyte, kws::Analyte, PyOptical3_2);

// $PnCalibration (3.2)
get_set_meas!(
    get_calibration,
    set_calibration,
    kws::Calibration3_2,
    PyOptical3_2
);

macro_rules! common_layout_methods {
    ($t:ident) => {
        #[pymethods]
        impl $t {
            /// Return the widths of each column (ie the $PnB keyword).
            ///
            /// This will be a list of integers equal to the number of columns
            /// or an empty list if the layout is delimited Ascii (in which case
            /// it has no column widths).
            #[getter]
            fn widths(&self) -> Vec<u8> {
                self.0.widths().into_iter().map(u8::from).collect()
            }

            /// Return a list of ranges for each column.
            ///
            /// The elements of the list will be either a float or int and
            /// will depend on the underlying layout structure.
            #[getter]
            fn ranges(&self) -> Vec<kws::Range> {
                self.0.ranges().into()
            }

            #[getter]
            /// Return the datatype.
            fn datatype(&self) -> kws::AlphaNumType {
                self.0.datatype().into()
            }

            #[getter]
            /// Return a list of datatypes corresponding to each column.
            fn datatypes(&self) -> Vec<kws::AlphaNumType> {
                self.0.datatypes().map(|d| d.into()).into()
            }
        }
    };
}

common_layout_methods!(PyAsciiFixedLayout);
common_layout_methods!(PyAsciiDelimLayout);
common_layout_methods!(PyOrderedUint08Layout);
common_layout_methods!(PyOrderedUint16Layout);
common_layout_methods!(PyOrderedUint24Layout);
common_layout_methods!(PyOrderedUint32Layout);
common_layout_methods!(PyOrderedUint40Layout);
common_layout_methods!(PyOrderedUint48Layout);
common_layout_methods!(PyOrderedUint56Layout);
common_layout_methods!(PyOrderedUint64Layout);
common_layout_methods!(PyOrderedF32Layout);
common_layout_methods!(PyOrderedF64Layout);
common_layout_methods!(PyEndianF32Layout);
common_layout_methods!(PyEndianF64Layout);
common_layout_methods!(PyEndianUintLayout);
common_layout_methods!(PyMixedLayout);

macro_rules! byte_order_methods {
    ($t:ident) => {
        #[pymethods]
        impl $t {
            #[getter]
            /// Return the byte order of the layout.
            fn byte_order(&self) -> Vec<NonZeroU8> {
                self.0.byte_order().as_vec()
            }

            #[getter]
            /// Return the endianness if applicable.
            ///
            /// Return true for big endian, false for little endian, and
            /// None if byte order is mixed.
            fn is_big_endian(&self) -> Option<bool> {
                self.0.endianness().map(|x| x == Endian::Big)
            }
        }
    };
}

byte_order_methods!(PyOrderedUint08Layout);
byte_order_methods!(PyOrderedUint16Layout);
byte_order_methods!(PyOrderedUint24Layout);
byte_order_methods!(PyOrderedUint32Layout);
byte_order_methods!(PyOrderedUint40Layout);
byte_order_methods!(PyOrderedUint48Layout);
byte_order_methods!(PyOrderedUint56Layout);
byte_order_methods!(PyOrderedUint64Layout);
byte_order_methods!(PyOrderedF32Layout);
byte_order_methods!(PyOrderedF64Layout);

macro_rules! endianness_methods {
    ($t:ident) => {
        #[pymethods]
        impl $t {
            #[getter]
            /// Return true if big endian, false otherwise.
            fn is_big_endian(&self) -> bool {
                *self.0.as_ref() == Endian::Big
            }
        }
    };
}

endianness_methods!(PyEndianF32Layout);
endianness_methods!(PyEndianF64Layout);
endianness_methods!(PyEndianUintLayout);
endianness_methods!(PyMixedLayout);

#[pymethods]
impl PyAsciiDelimLayout {
    #[new]
    fn new(ranges: PyNonEmpty<u64>) -> Self {
        DelimAsciiLayout::new(ranges.0).into()
    }
}

#[pymethods]
impl PyAsciiFixedLayout {
    #[new]
    fn new(ranges: PyNonEmpty<u64>) -> Self {
        FixedLayout::new_ascii_u64(ranges.0).into()
    }

    // TODO make a constructor that takes char/range pairs
    // #[classmethod]
    // fn from_pairs(ranges: PyNonEmpty<u64>) -> Self {
    //     FixedLayout::new(columns, NoByteOrd)
    // }

    //             #[classmethod]
    //             fn new_ascii_fixed_pairs(
    //                 _: &Bound<'_, PyType>,
    //                 ranges: PyNonEmpty<(u64, u8)>,
    //             ) -> PyResult<Self> {
    //                 // TODO clean these types up
    //                 let ys = ranges
    //                     .0
    //                     .try_map(|(x, c)| Chars::try_from(c).map(|y| (x, y)))
    //                     .map_err(|e| PyreflowException::new_err(e.to_string()))?;
    //                 let rs = ys
    //                     .try_map(|(x, c)| AsciiRange::try_new(x, c))
    //                     .map_err(|e| PyreflowException::new_err(e.to_string()))?;
    //                 Ok($wrap($subwrap::new_ascii_fixed(rs)).into())
    //             }
}

macro_rules! new_ordered_uint {
    ($t:ident, $uint:ident, $size:expr) => {
        #[pymethods]
        impl $t {
            /// Make a new layout for $size-byte Uints with a given endian-ness.
            #[new]
            fn new(ranges: PyNonEmpty<bm::Bitmask<$uint, $size>>, is_big: bool) -> Self {
                FixedLayout::new_endian_uint(ranges.0, is_big.into()).into()
            }

            #[classmethod]
            /// Make a new layout for $size-byte Uints with a given byte order.
            fn new_ordered(
                _: &Bound<'_, PyType>,
                ranges: PyNonEmpty<bm::Bitmask<$uint, $size>>,
                byteord: SizedByteOrd<$size>,
            ) -> Self {
                FixedLayout::new(ranges.0, byteord).into()
            }
        }
    };
}

new_ordered_uint!(PyOrderedUint08Layout, u8, 1);
new_ordered_uint!(PyOrderedUint16Layout, u16, 2);
new_ordered_uint!(PyOrderedUint24Layout, u32, 3);
new_ordered_uint!(PyOrderedUint32Layout, u32, 4);
new_ordered_uint!(PyOrderedUint40Layout, u64, 5);
new_ordered_uint!(PyOrderedUint48Layout, u64, 6);
new_ordered_uint!(PyOrderedUint56Layout, u64, 7);
new_ordered_uint!(PyOrderedUint64Layout, u64, 8);

macro_rules! new_ordered_float {
    ($t:ident, $num:ident, $size:expr) => {
        #[pymethods]
        impl $t {
            /// Make a new $num layout with a given endian-ness.
            #[new]
            fn new(ranges: PyNonEmpty<FloatRange<$num, $size>>, is_big: bool) -> Self {
                FixedLayout::new_endian_float(ranges.0, is_big.into()).into()
            }

            #[classmethod]
            /// Make a new $num layout with a given byte order.
            fn new_ordered(
                _: &Bound<'_, PyType>,
                ranges: PyNonEmpty<FloatRange<$num, $size>>,
                byteord: SizedByteOrd<$size>,
            ) -> Self {
                FixedLayout::new(ranges.0, byteord).into()
            }
        }
    };
}

new_ordered_float!(PyOrderedF32Layout, f32, 4);
new_ordered_float!(PyOrderedF64Layout, f64, 8);

// float layouts for 3.1/3.2
macro_rules! new_endian_float {
    ($t:ident, $num:ident, $size:expr) => {
        #[pymethods]
        impl $t {
            #[new]
            /// Make a new $num layout with a given endian-ness.
            fn new(ranges: PyNonEmpty<FloatRange<$num, $size>>, is_big: bool) -> Self {
                FixedLayout::new(ranges.0, is_big.into()).into()
            }
        }
    };
}

new_endian_float!(PyEndianF32Layout, f32, 4);
new_endian_float!(PyEndianF64Layout, f64, 8);

#[pymethods]
impl PyEndianUintLayout {
    /// Make a new Uint layout with a given endian-ness.
    ///
    /// Width of each column (in bytes) will depend in the input range.
    #[new]
    fn new(ranges: PyNonEmpty<u64>, is_big: bool) -> Self {
        let rs = ranges.0.map(AnyNullBitmask::from_u64);
        FixedLayout::new(rs, is_big.into()).into()
    }
}

#[pymethods]
impl PyMixedLayout {
    #[new]
    /// Make a new mixed layout with a given endian-ness.
    ///
    /// Columns must be specified as pairs like (flag, value) where 'flag'
    /// is one of "A", "I", "F", or "D" corresponding to Ascii, Integer, Float,
    /// or Double datatypes. The 'value' field should be an integer for "A" or
    /// "I" and a float for "F" or "D".
    fn new(ranges: PyNonEmpty<NullMixedType>, is_big: bool) -> Self {
        FixedLayout::new(ranges.0, is_big.into()).into()
    }
}

pub(crate) struct PyNonEmpty<T>(pub(crate) NonEmpty<T>);

impl<'py, T: FromPyObject<'py>> FromPyObject<'py> for PyNonEmpty<T> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let xs: Vec<_> = ob.extract()?;
        NonEmpty::from_vec(xs)
            .ok_or(PyValueError::new_err("list must not be empty"))
            .map(Self)
    }
}
