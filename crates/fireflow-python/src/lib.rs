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
use fireflow_core::nonempty::FCSNonEmpty;
use fireflow_core::python::exceptions::{PyTerminalNoWarnResultExt, PyTerminalResultExt};
use fireflow_core::segment::{HeaderAnalysisSegment, HeaderDataSegment, OtherSegment};
use fireflow_core::text::byteord::{Endian, SizedByteOrd};
use fireflow_core::text::compensation::Compensation;
use fireflow_core::text::gating::{
    AppliedGates2_0, AppliedGates3_0, AppliedGates3_2, BivariateRegion, GatedMeasurement,
    GatingScheme, Region, UnivariateRegion,
};
use fireflow_core::text::index::{GateIndex, MeasIndex, RegionIndex};
use fireflow_core::text::keywords as kws;
use fireflow_core::text::named_vec::{Eithers, Element, NamedVec, NonCenterElement};
use fireflow_core::text::optional::{AlwaysFamily, MaybeFamily, MaybeValue};
use fireflow_core::text::scale::Scale;
use fireflow_core::text::unstainedcenters::UnstainedCenters;
use fireflow_core::validated::bitmask as bm;
use fireflow_core::validated::dataframe::{AnyFCSColumn, FCSDataFrame};
use fireflow_core::validated::keys::{NonStdKey, StdKeywords, ValidKeywords};
use fireflow_core::validated::shortname::{Shortname, ShortnamePrefix};
use fireflow_core::validated::textdelim::TEXTDelim;
use fireflow_python_proc::{
    impl_convert_version, impl_get_set_all_meas, impl_get_set_meas_obj_common,
    impl_get_set_metaroot,
};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};
use derive_more::{From, Into};
use numpy::{PyArray2, PyReadonlyArray2, ToPyArray};
use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3_polars::PyDataFrame;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
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
        #[pyclass(name = $name, eq)]
        #[derive(Clone, From, Into, PartialEq)]
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

// gating objects

py_wrap!(PyAppliedGates2_0, AppliedGates2_0, "AppliedGates2_0");
py_wrap!(PyAppliedGates3_0, AppliedGates3_0, "AppliedGates3_0");
py_wrap!(PyAppliedGates3_2, AppliedGates3_2, "AppliedGates3_2");

py_wrap!(PyGatedMeasurement, GatedMeasurement, "GatedMeasurement");

py_wrap!(
    PyUnivariateRegion2_0,
    UnivariateRegion<GateIndex>,
    "UnivariateRegion2_0"
);

py_wrap!(
    PyUnivariateRegion3_0,
    UnivariateRegion<kws::MeasOrGateIndex>,
    "UnivariateRegion3_0"
);

py_wrap!(
    PyUnivariateRegion3_2,
    UnivariateRegion<kws::PrefixedMeasIndex>,
    "UnivariateRegion3_2"
);

py_wrap!(
    PyBivariateRegion2_0,
    BivariateRegion<GateIndex>,
    "BivariateRegion2_0"
);

py_wrap!(
    PyBivariateRegion3_0,
    BivariateRegion<kws::MeasOrGateIndex>,
    "BivariateRegion3_0"
);

py_wrap!(
    PyBivariateRegion3_2,
    BivariateRegion<kws::PrefixedMeasIndex>,
    "BivariateRegion3_2"
);

// layout objects
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

impl_convert_version! {PyCoreTEXT2_0}
impl_convert_version! {PyCoreTEXT3_0}
impl_convert_version! {PyCoreTEXT3_1}
impl_convert_version! {PyCoreTEXT3_2}
impl_convert_version! {PyCoreDataset2_0}
impl_convert_version! {PyCoreDataset3_0}
impl_convert_version! {PyCoreDataset3_1}
impl_convert_version! {PyCoreDataset3_2}

#[pymethods]
impl PyCoreTEXT2_0 {
    #[new]
    fn new(mode: kws::Mode, datatype: kws::AlphaNumType) -> PyResult<Self> {
        Ok(core::CoreTEXT2_0::new(mode, datatype).into())
    }
}

#[pymethods]
impl PyCoreTEXT3_0 {
    #[new]
    fn new(mode: kws::Mode, datatype: kws::AlphaNumType) -> PyResult<Self> {
        Ok(core::CoreTEXT3_0::new(mode, datatype).into())
    }
}

#[pymethods]
impl PyCoreTEXT3_1 {
    #[new]
    fn new(mode: kws::Mode, datatype: kws::AlphaNumType) -> Self {
        core::CoreTEXT3_1::new(mode, datatype).into()
    }
}

#[pymethods]
impl PyCoreTEXT3_2 {
    #[new]
    fn new(cyt: String, datatype: kws::AlphaNumType) -> Self {
        core::CoreTEXT3_2::new(cyt, datatype).into()
    }
}

// Get/set methods for all versions
macro_rules! impl_common {
    ($pytype:ident) => {
        impl_get_set_metaroot! {Option<kws::Abrt>, "int", $pytype}
        impl_get_set_metaroot! {Option<kws::Cells>, "str", $pytype}
        impl_get_set_metaroot! {Option<kws::Com>, "str", $pytype}
        impl_get_set_metaroot! {Option<kws::Exp>, "str", $pytype}
        impl_get_set_metaroot! {Option<kws::Fil>, "str", $pytype}
        impl_get_set_metaroot! {Option<kws::Inst>, "str", $pytype}
        impl_get_set_metaroot! {Option<kws::Lost>, "int", $pytype}
        impl_get_set_metaroot! {Option<kws::Op>, "str", $pytype}
        impl_get_set_metaroot! {Option<kws::Proj>, "str", $pytype}
        impl_get_set_metaroot! {Option<kws::Smno>, "str", $pytype}
        impl_get_set_metaroot! {Option<kws::Src>, "str", $pytype}
        impl_get_set_metaroot! {Option<kws::Sys>, "str", $pytype}

        // common measurement keywords
        impl_get_set_all_meas!(Option<kws::Longname>, "S", "str", $pytype);

        impl_get_set_all_meas!(NonCenterElement<Option<kws::Filter>>, "F", "str", $pytype);
        impl_get_set_all_meas!(NonCenterElement<Option<kws::Power>>, "O", "float", $pytype);

        impl_get_set_all_meas!(
            NonCenterElement<Option<kws::PercentEmitted>>,
            "P",
            "str",
            $pytype
        );

        impl_get_set_all_meas!(
            NonCenterElement<Option<kws::DetectorType>>,
            "T",
            "str",
            $pytype
        );

        impl_get_set_all_meas!(
            NonCenterElement<Option<kws::DetectorVoltage>>,
            "V",
            "float",
            $pytype
        );

        #[pymethods]
        impl $pytype {
            /// Insert a nonstandard key.
            ///
            /// :param str key: Key to insert. Must not start with *$*.
            /// :param str value: Value to insert.
            ///
            /// :return: Previous value for ``key`` if it exists.
            /// :rtype: str | None
            fn insert_nonstandard(&mut self, key: NonStdKey, value: String) -> Option<String> {
                self.0.metaroot.nonstandard_keywords.insert(key, value)
            }

            /// Remove a nonstandard key.
            ///
            /// :param str key: Key to remove. Must not start with *$*.
            ///
            /// :return: Value for ``key`` if it exists.
            /// :rtype: str | None
            fn remove_nonstandard(&mut self, key: NonStdKey) -> Option<String> {
                self.0.metaroot.nonstandard_keywords.remove(&key)
            }

            /// Look up a nonstandard key.
            ///
            /// :param str key: Key to find. Must not start with *$*.
            ///
            /// :return: Value for ``key`` if it exists.
            /// :rtype: str | None
            fn get_nonstandard(&mut self, key: NonStdKey) -> Option<String> {
                self.0.metaroot.nonstandard_keywords.get(&key).cloned()
            }

            /// Return standard keywords as string pairs.
            ///
            /// Each key will be prefixed with *$*.
            ///
            /// This will not include *$TOT*, *$NEXTDATA* or any of the
            /// offset keywords since these are not encoded in this class.
            ///
            /// :param bool exclude_req_root: Do not include required non-measurement keywords
            /// :param bool exclude_opt_root: Do not include optional non-measurement keywords
            /// :param bool exclude_req_meas: Do not include required measurement keywords
            /// :param bool exclude_opt_meas: Do not include optional measurement keywords
            ///
            /// :return: A list of standard keywords.
            /// :rtype: dict[str, str]
            #[pyo3(signature = (
                exclude_req_root=false, exclude_opt_root=false, exclude_req_meas=false, exclude_opt_meas=false
            ))]
            fn standard_keywords(
                &self,
                exclude_req_root: bool,
                exclude_opt_root: bool,
                exclude_req_meas: bool,
                exclude_opt_meas: bool,
            ) -> HashMap<String, String> {
                self.0.standard_keywords(
                    exclude_req_root,
                    exclude_opt_root,
                    exclude_req_meas,
                    exclude_opt_meas
                )
            }

            /// The value for *$PAR*
            ///
            /// :type: int
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

            /// The value for *$BTIM*.
            ///
            /// If ``date``, ``btim``, and ``etim`` are all not ``None`` then
            /// ``btim`` must be before ``etim``.
            ///
            /// :type: :py:class:`datetime.time` | None
            #[getter]
            fn get_btim(&self) -> Option<NaiveTime> {
                self.0.btim_naive()
            }

            #[setter]
            fn set_btim(&mut self, x: Option<NaiveTime>) -> PyResult<()> {
                Ok(self.0.set_btim_naive(x)?)
            }

            /// The value for *$ETIM*.
            ///
            /// If ``date``, ``btim``, and ``etim`` are all not ``None`` then
            /// ``btim`` must be before ``etim``.
            ///
            /// :type: :py:class:`datetime.time` | None
            #[getter]
            fn get_etim(&self) -> Option<NaiveTime> {
                self.0.etim_naive()
            }

            #[setter]
            fn set_etim(&mut self, x: Option<NaiveTime>) -> PyResult<()> {
                Ok(self.0.set_etim_naive(x)?)
            }

            /// The value for *$DATE*.
            ///
            /// If ``date``, ``btim``, and ``etim`` are all not ``None`` then
            /// ``btim`` must be before ``etim``.
            ///
            /// :type: :py:class:`datetime.date` | None
            #[getter]
            fn get_date(&self) -> Option<NaiveDate> {
                self.0.date_naive()
            }

            #[setter]
            fn set_date(&mut self, x: Option<NaiveDate>) -> PyResult<()> {
                Ok(self.0.set_date_naive(x)?)
            }

            /// The value for *$TR*.
            ///
            /// This is represented as a tuple where the first member is the
            /// threshold and the second member is a measurement name which
            /// must exist in the set of all *$PnN*.
            ///
            /// :type: (int, str) | None
            #[getter]
            fn trigger(&self) -> Option<kws::Trigger> {
                self.0.metaroot_opt().cloned()
            }

            #[setter]
            fn set_trigger(&mut self, tr: Option<kws::Trigger>) -> PyResult<()> {
                Ok(self.0.set_trigger(tr)?)
            }

            /// Set the threshold for *$TR*.
            ///
            /// :param int threshold: The threshold to set
            ///
            /// :return: ``True`` if trigger is set and was updated
            /// :rtype: bool
            fn set_trigger_threshold(&mut self, threshold: u32) -> bool {
                self.0.set_trigger_threshold(threshold)
            }

            /// Rename temporal measurement if present.
            ///
            /// :param str name: New name to assign. Must not have commas.
            ///
            /// :return: Previous name if present.
            /// :rtype: str | None
            fn rename_temporal(&mut self, name: Shortname) -> Option<Shortname> {
                self.0.rename_temporal(name)
            }
        }
    };
}

impl_common!(PyCoreTEXT2_0);
impl_common!(PyCoreTEXT3_0);
impl_common!(PyCoreTEXT3_1);
impl_common!(PyCoreTEXT3_2);
impl_common!(PyCoreDataset2_0);
impl_common!(PyCoreDataset3_0);
impl_common!(PyCoreDataset3_1);
impl_common!(PyCoreDataset3_2);

macro_rules! impl_write_text {
    ($pytype:ident, $exc:expr) => {
        #[pymethods]
        impl $pytype {
            /// Write data to path.
            ///
            /// Resulting FCS file will include *HEADER* and *TEXT*.
            ///
            #[doc = $exc]
            /// :param path: path to write
            /// :type path: :py:class:`pathlib.Path`
            ///
            /// :param int delim: Delimiter to use when writing *TEXT*.
            ///     Defaults to 30 (record separator).
            #[pyo3(signature = (path, delim = TEXTDelim::default()))]
            fn write_text(&self, path: PathBuf, delim: TEXTDelim) -> PyResult<()> {
                let f = File::options().write(true).create(true).open(path)?;
                let mut h = BufWriter::new(f);
                self.0.h_write_text(&mut h, delim).py_term_resolve_nowarn()
            }
        }
    };
}

impl_write_text!(
    PyCoreTEXT2_0,
    "Will raise exception if file cannot fit within 99,999,999 bytes.\n"
);
impl_write_text!(PyCoreTEXT3_0, "");
impl_write_text!(PyCoreTEXT3_1, "");
impl_write_text!(PyCoreTEXT3_2, "");
impl_write_text!(
    PyCoreDataset2_0,
    "Will raise exception if file cannot fit within 99,999,999 bytes.\n"
);
impl_write_text!(PyCoreDataset3_0, "");
impl_write_text!(PyCoreDataset3_1, "");
impl_write_text!(PyCoreDataset3_2, "");

macro_rules! impl_get_set_pnn {
    ($(#[$meta:meta])* $pytype:ident) => {
        #[pymethods]
        impl $pytype {
            // TODO pretty sure there is no way to change prefix once a core
            // object is created
            /// Value of *$PnN* for all measurements.
            ///
            /// Strings are unique and cannot contain commas.
            ///
            $(#[$meta])*
            /// :type: list[str]
            #[getter]
            fn get_all_pnn(&self) -> Vec<Shortname> {
                self.0.all_shortnames()
            }

            #[setter]
            fn set_all_pnn(&mut self, names: Vec<Shortname>) -> PyResult<()> {
                Ok(self.0.set_all_shortnames(names).void()?)
            }
        }
    };
}

impl_get_set_pnn!(
    /// When reading, missing *$PnN* will be replaced with '<prefix>n' where 'n'
    /// is the measurement index starting at 1 and '<prefix>' is a fixed prefix.
    ///
    PyCoreTEXT2_0
);
impl_get_set_pnn!(
    /// When reading, missing *$PnN* will be replaced with '<prefix>n' where 'n'
    /// is the measurement index starting at 1 and '<prefix>' is a fixed prefix.
    ///
    PyCoreTEXT3_0
);
impl_get_set_pnn!(PyCoreTEXT3_1);
impl_get_set_pnn!(PyCoreTEXT3_2);
impl_get_set_pnn!(
    /// When reading, missing *$PnN* will be replaced with '<prefix>n' where 'n'
    /// is the measurement index starting at 1 and '<prefix>' is a fixed prefix.
    ///
    PyCoreDataset2_0
);
impl_get_set_pnn!(
    /// When reading, missing *$PnN* will be replaced with '<prefix>n' where 'n'
    /// is the measurement index starting at 1 and '<prefix>' is a fixed prefix.
    ///
    PyCoreDataset3_0
);
impl_get_set_pnn!(PyCoreDataset3_1);
impl_get_set_pnn!(PyCoreDataset3_2);

macro_rules! impl_get_set_pnn_maybe {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// The possibly-empty values of *$PnN* for all measurements.
            ///
            /// For this FCS version, *$PnN* is optional which is why values
            /// may be ``None``.
            ///
            /// :rtype: list[str | None]
            #[getter]
            fn get_all_pnn_maybe(&self) -> Vec<Option<Shortname>> {
                self.0
                    .shortnames_maybe()
                    .into_iter()
                    .map(|x| x.cloned())
                    .collect()
            }

            #[setter]
            fn set_all_pnn_maybe(&mut self, names: Vec<Option<Shortname>>) -> PyResult<()> {
                Ok(self.0.set_measurement_shortnames_maybe(names).void()?)
            }
        }
    };
}

impl_get_set_pnn_maybe!(PyCoreTEXT2_0);
impl_get_set_pnn_maybe!(PyCoreTEXT3_0);
impl_get_set_pnn_maybe!(PyCoreDataset2_0);
impl_get_set_pnn_maybe!(PyCoreDataset3_0);

macro_rules! impl_set_temporal_no_timestep {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// Set the temporal measurement to a given name.
            ///
            /// :param str name: Name to set. Must be a *$PnN* which is present.
            /// :param bool force: If ``True`` remove any optical-specific metadata
            ///     (detectors, lasers, etc) without raising an exception.
            ///     Defauls to ``False``.
            ///
            /// :return: ``True`` if temporal measurement was set, which will
            ///     happen for all cases except when the time measurement is
            ///     already set to ``name``.
            /// :rtype: bool
            #[pyo3(signature = (name, force = false))]
            fn set_temporal(&mut self, name: Shortname, force: bool) -> PyResult<bool> {
                self.0.set_temporal(&name, (), force).py_term_resolve()
            }

            /// Set the temporal measurement to a given index.
            ///
            /// :param int index: Index to set. Must be between 0 and ``par``.
            /// :param force: If ``True`` remove any optical-specific metadata
            ///     (detectors, lasers, etc) without raising an exception.
            ///     Defauls to ``False``.
            ///
            /// :return: ``True`` if temporal measurement was set, which will
            ///     happen for all cases except when the time measurement is
            ///     already set to ``index``.
            /// :rtype: bool
            #[pyo3(signature = (index, force = false))]
            fn set_temporal_at(&mut self, index: MeasIndex, force: bool) -> PyResult<bool> {
                self.0.set_temporal_at(index, (), force).py_term_resolve()
            }
        }
    };
}

impl_set_temporal_no_timestep!(PyCoreTEXT2_0);
impl_set_temporal_no_timestep!(PyCoreDataset2_0);

macro_rules! impl_set_temporal_timestep {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// Set the temporal measurement to a given name.
            ///
            /// :param str name: Name to set. Must be a *$PnN* which is present.
            /// :param float timestep: The value of *$TIMESTEP* to use.
            /// :param bool force: If ``True`` remove any optical-specific metadata
            ///     (detectors, lasers, etc) without raising an exception.
            ///     Defauls to ``False``.
            ///
            /// :return: ``True`` if temporal measurement was set, which will
            ///     happen for all cases except when the time measurement is
            ///     already set to ``name``.
            /// :rtype: bool
            #[pyo3(signature = (name, timestep, force = false))]
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

            /// Set the temporal measurement to a given index.
            ///
            /// :param int index: Index to set. Must be between 0 and ``par``.
            /// :param float timestep: The value of *$TIMESTEP* to use.
            /// :param force: If ``True`` remove any optical-specific metadata
            ///     (detectors, lasers, etc) without raising an exception.
            ///     Defauls to ``False``.
            ///
            /// :return: ``True`` if temporal measurement was set, which will
            ///     happen for all cases except when the time measurement is
            ///     already set to ``index``.
            /// :rtype: bool
            #[pyo3(signature = (index, timestep, force = false))]
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
        }
    };
}

impl_set_temporal_timestep!(PyCoreTEXT3_0);
impl_set_temporal_timestep!(PyCoreTEXT3_1);
impl_set_temporal_timestep!(PyCoreTEXT3_2);
impl_set_temporal_timestep!(PyCoreDataset3_0);
impl_set_temporal_timestep!(PyCoreDataset3_1);
impl_set_temporal_timestep!(PyCoreDataset3_2);

macro_rules! impl_unset_temporal_notimestep {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// Convert the temporal measurement to an optical measurement.
            ///
            /// :return: ``True`` if temporal measurement was present and
            ///     converted, ``False`` if there was not a temporal measurement.
            /// :rtype: bool
            fn unset_temporal(&mut self) -> bool {
                self.0.unset_temporal().is_some()
            }
        }
    };
}

impl_unset_temporal_notimestep!(PyCoreTEXT2_0);
impl_unset_temporal_notimestep!(PyCoreDataset2_0);

macro_rules! impl_unset_temporal_timestep {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// Convert the temporal measurement to an optical measurement.
            ///
            /// :return: Value of *$TIMESTEP* if time measurement was present.
            /// :rtype: float | None
            fn unset_temporal(&mut self) -> Option<kws::Timestep> {
                self.0.unset_temporal()
            }
        }
    };
}

impl_unset_temporal_timestep!(PyCoreTEXT3_0);
impl_unset_temporal_timestep!(PyCoreTEXT3_1);
impl_unset_temporal_timestep!(PyCoreDataset3_0);
impl_unset_temporal_timestep!(PyCoreDataset3_1);

macro_rules! impl_unset_temporal_timestep_lossy {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// Convert the temporal measurement to an optical measurement.
            ///
            /// :param bool force: If ``True`` and current time measurement has
            ///     data which cannot be converted to optical, force the conversion
            ///     anyways. Otherwise raise an exception.
            ///
            /// :return: Value of *$TIMESTEP* if time measurement was present.
            /// :rtype: float | None
            #[pyo3(signature = (force = false))]
            fn unset_temporal(&mut self, force: bool) -> PyResult<Option<kws::Timestep>> {
                self.0.unset_temporal_lossy(force).py_term_resolve()
            }
        }
    };
}

impl_unset_temporal_timestep_lossy!(PyCoreTEXT3_2);
impl_unset_temporal_timestep_lossy!(PyCoreDataset3_2);

impl_get_set_meas_obj_common! {PyCoreTEXT2_0, Option<Shortname>, MaybeFamily}
impl_get_set_meas_obj_common! {PyCoreTEXT3_0, Option<Shortname>, MaybeFamily}
impl_get_set_meas_obj_common! {PyCoreTEXT3_1, Shortname, AlwaysFamily}
impl_get_set_meas_obj_common! {PyCoreTEXT3_2, Shortname, AlwaysFamily}
impl_get_set_meas_obj_common! {PyCoreDataset2_0, Option<Shortname>, MaybeFamily}
impl_get_set_meas_obj_common! {PyCoreDataset3_0, Option<Shortname>, MaybeFamily}
impl_get_set_meas_obj_common! {PyCoreDataset3_1, Shortname, AlwaysFamily}
impl_get_set_meas_obj_common! {PyCoreDataset3_2, Shortname, AlwaysFamily}

macro_rules! impl_get_set_meas_obj_coretext {
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

impl_get_set_meas_obj_coretext!(
    PyCoreTEXT2_0,
    PyOptical2_0,
    PyTemporal2_0,
    Option<Shortname>
);
impl_get_set_meas_obj_coretext!(
    PyCoreTEXT3_0,
    PyOptical3_0,
    PyTemporal3_0,
    Option<Shortname>
);
impl_get_set_meas_obj_coretext!(PyCoreTEXT3_1, PyOptical3_1, PyTemporal3_1, Shortname);
impl_get_set_meas_obj_coretext!(PyCoreTEXT3_2, PyOptical3_2, PyTemporal3_2, Shortname);

macro_rules! impl_replace_temporal {
    ($pytype:ident, $o:ident, $t:ident) => {
        #[pymethods]
        impl $pytype {
            fn replace_temporal_at(
                &mut self,
                index: MeasIndex,
                meas: $t,
            ) -> PyResult<Element<$t, $o>> {
                Ok(self.0.replace_temporal_at(index, meas.into())?.inner_into())
            }

            fn replace_temporal_named(
                &mut self,
                name: Shortname,
                meas: $t,
            ) -> Option<Element<$t, $o>> {
                self.0
                    .replace_temporal_named(&name, meas.into())
                    .map(|r| r.inner_into())
            }
        }
    };
}

impl_replace_temporal!(PyCoreTEXT2_0, PyOptical2_0, PyTemporal2_0);
impl_replace_temporal!(PyCoreTEXT3_0, PyOptical3_0, PyTemporal3_0);
impl_replace_temporal!(PyCoreTEXT3_1, PyOptical3_1, PyTemporal3_1);
impl_replace_temporal!(PyCoreDataset2_0, PyOptical2_0, PyTemporal2_0);
impl_replace_temporal!(PyCoreDataset3_0, PyOptical3_0, PyTemporal3_0);
impl_replace_temporal!(PyCoreDataset3_1, PyOptical3_1, PyTemporal3_1);

// different set of functions for 3.2 since these can be "lossy"
macro_rules! impl_replace_temporal_3_2 {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// Replace measurement at index with temporal measurement.
            ///
            /// Raise exception if index is output of bounds or there is already
            /// a temporal measurement at a different index.
            ///
            /// :param int index: Index to be replaced.
            /// :param meas: Temporal measurement with which to replace.
            /// :type meas: :py:class:`Temporal3_2`
            ///
            /// :return: Replaced measurement object.
            /// :rtype: :py:class:`Temporal3_2` | :py:class:`Optical3_2`
            #[pyo3(signature = (index, meas, force = false))]
            fn replace_temporal_at(
                &mut self,
                index: MeasIndex,
                meas: PyTemporal3_2,
                force: bool,
            ) -> PyResult<Element<PyTemporal3_2, PyOptical3_2>> {
                let ret = self
                    .0
                    .replace_temporal_at_lossy(index, meas.into(), force)
                    .py_term_resolve()?;
                Ok(ret.inner_into())
            }

            /// Replace measurement with name with temporal measurement.
            ///
            /// Raise exception if name is not found or there is already
            /// a temporal measurement at a different index.
            ///
            /// :param str name: Name to replace. Must not contain commas.
            /// :param meas: Temporal measurement with which to replace.
            /// :type meas: :py:class:`Temporal3_2`
            ///
            /// :return: Replaced measurement object.
            /// :rtype: :py:class:`Temporal3_2` | :py:class:`Optical3_2`
            #[pyo3(signature = (name, meas, force = false))]
            fn replace_temporal_named(
                &mut self,
                name: Shortname,
                meas: PyTemporal3_2,
                force: bool,
            ) -> PyResult<Option<Element<PyTemporal3_2, PyOptical3_2>>> {
                let ret = self
                    .0
                    .replace_temporal_named_lossy(&name, meas.into(), force)
                    .py_term_resolve()?;
                Ok(ret.map(|r| r.inner_into()))
            }
        }
    };
}

impl_replace_temporal_3_2!(PyCoreTEXT3_2);
impl_replace_temporal_3_2!(PyCoreDataset3_2);

macro_rules! impl_get_set_meas_obj_coredata {
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
        }
    };
}

impl_get_set_meas_obj_coredata!(
    PyCoreDataset2_0,
    PyOptical2_0,
    PyTemporal2_0,
    Option<Shortname>
);
impl_get_set_meas_obj_coredata!(
    PyCoreDataset3_0,
    PyOptical3_0,
    PyTemporal3_0,
    Option<Shortname>
);
impl_get_set_meas_obj_coredata!(PyCoreDataset3_1, PyOptical3_1, PyTemporal3_1, Shortname);
impl_get_set_meas_obj_coredata!(PyCoreDataset3_2, PyOptical3_2, PyTemporal3_2, Shortname);

macro_rules! impl_coredata_common {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            // TODO be more specific about which keywords, requires splitting
            // out by version
            /// Remove all measurements and their data.
            ///
            /// Raise exception if any keywords reference *$PnN*.
            fn unset_data(&mut self) -> PyResult<()> {
                Ok(self.0.unset_data()?)
            }

            // TODO reference polars docs
            /// The dataframe corresponding to *DATA*.
            ///
            /// :type: :py:class:`polars.DataFrame`
            #[getter]
            fn data(&self) -> PyDataFrame {
                let ns = self.0.all_shortnames();
                let data = self.0.data();
                PyDataFrame(data.as_polars_dataframe(&ns[..]))
            }

            #[setter]
            fn set_data(&mut self, df: PyDataFrame) -> PyResult<()> {
                let data = df.0.try_into()?;
                Ok(self.0.set_data(data)?)
            }

            /// The byte string corresponding to *ANALYSIS*.
            ///
            /// :type: bytes
            #[getter]
            fn analysis(&self) -> core::Analysis {
                self.0.analysis.clone()
            }

            #[setter]
            fn set_analysis(&mut self, xs: core::Analysis) {
                self.0.analysis = xs.into();
            }

            /// The byte strings corresponding to *OTHER* segments.
            ///
            /// :type: list[bytes]
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

impl_coredata_common!(PyCoreDataset2_0);
impl_coredata_common!(PyCoreDataset3_0);
impl_coredata_common!(PyCoreDataset3_1);
impl_coredata_common!(PyCoreDataset3_2);

macro_rules! impl_set_measurements_ordered {
    ($pytype:ident, $t:ident, $o:ident) => {
        #[pymethods]
        impl $pytype {
            fn set_measurements(
                &mut self,
                measurements: Eithers<MaybeFamily, $t, $o>,
                prefix: ShortnamePrefix,
                allow_shared_names: bool,
                skip_index_check: bool,
            ) -> PyResult<()> {
                self.0
                    .set_measurements(
                        measurements.inner_into(),
                        prefix,
                        allow_shared_names,
                        skip_index_check,
                    )
                    .py_term_resolve_nowarn()
            }

            fn set_measurements_and_layout(
                &mut self,
                measurements: Eithers<MaybeFamily, $t, $o>,
                layout: PyOrderedLayout,
                prefix: ShortnamePrefix,
                allow_shared_names: bool,
                skip_index_check: bool,
            ) -> PyResult<()> {
                self.0
                    .set_measurements_and_layout(
                        measurements.inner_into(),
                        layout.into(),
                        prefix,
                        allow_shared_names,
                        skip_index_check,
                    )
                    .py_term_resolve_nowarn()
            }

            #[getter]
            fn get_layout(&self) -> PyOrderedLayout {
                self.0.layout().clone().into()
            }

            #[setter]
            fn set_layout(&mut self, layout: PyOrderedLayout) -> PyResult<()> {
                self.0.set_layout(layout.into()).py_term_resolve_nowarn()
            }
        }
    };
}

impl_set_measurements_ordered!(PyCoreTEXT2_0, PyTemporal2_0, PyOptical2_0);
impl_set_measurements_ordered!(PyCoreTEXT3_0, PyTemporal3_0, PyOptical3_0);
impl_set_measurements_ordered!(PyCoreDataset2_0, PyTemporal2_0, PyOptical2_0);
impl_set_measurements_ordered!(PyCoreDataset3_0, PyTemporal3_0, PyOptical3_0);

macro_rules! impl_set_measurements_endian {
    ($pytype:ident, $t:ident, $o:ident, $l:ident) => {
        #[pymethods]
        impl $pytype {
            pub fn set_measurements(
                &mut self,
                measurements: Eithers<AlwaysFamily, $t, $o>,
                allow_shared_names: bool,
                skip_index_check: bool,
            ) -> PyResult<()> {
                self.0
                    .set_measurements_noprefix(
                        measurements.inner_into(),
                        allow_shared_names,
                        skip_index_check,
                    )
                    .py_term_resolve_nowarn()
            }

            fn set_measurements_and_layout(
                &mut self,
                measurements: Eithers<AlwaysFamily, $t, $o>,
                layout: $l,
                allow_shared_names: bool,
                skip_index_check: bool,
            ) -> PyResult<()> {
                self.0
                    .set_measurements_and_layout_noprefix(
                        measurements.inner_into(),
                        layout.into(),
                        allow_shared_names,
                        skip_index_check,
                    )
                    .py_term_resolve_nowarn()
            }

            #[getter]
            fn get_layout(&self) -> $l {
                self.0.layout().clone().into()
            }

            #[setter]
            fn set_layout(&mut self, layout: $l) -> PyResult<()> {
                self.0.set_layout(layout.into()).py_term_resolve_nowarn()
            }
        }
    };
}

impl_set_measurements_endian!(PyCoreTEXT3_1, PyTemporal3_1, PyOptical3_1, PyNonMixedLayout);
impl_set_measurements_endian!(PyCoreTEXT3_2, PyTemporal3_2, PyOptical3_2, PyLayout3_2);
impl_set_measurements_endian!(
    PyCoreDataset3_1,
    PyTemporal3_1,
    PyOptical3_1,
    PyNonMixedLayout
);
impl_set_measurements_endian!(PyCoreDataset3_2, PyTemporal3_2, PyOptical3_2, PyLayout3_2);

// TODO use a real dataframe here rather than a list of series
macro_rules! impl_set_meas_and_data_prefix {
    ($pytype:ident, $t:ident, $o:ident) => {
        #[pymethods]
        impl $pytype {
            fn set_measurements_and_data(
                &mut self,
                measurements: Eithers<MaybeFamily, $t, $o>,
                df: FCSDataFrame,
                prefix: ShortnamePrefix,
                allow_shared_names: bool,
                skip_index_check: bool,
            ) -> PyResult<()> {
                self.0
                    .set_measurements_and_data(
                        measurements.inner_into(),
                        df,
                        prefix,
                        allow_shared_names,
                        skip_index_check,
                    )
                    .py_term_resolve_nowarn()
            }
        }
    };
}

impl_set_meas_and_data_prefix!(PyCoreDataset2_0, PyTemporal2_0, PyOptical2_0);
impl_set_meas_and_data_prefix!(PyCoreDataset3_0, PyTemporal3_0, PyOptical3_0);

macro_rules! impl_set_meas_and_data_noprefix {
    ($pytype:ident, $t:ident, $o:ident) => {
        #[pymethods]
        impl $pytype {
            fn set_measurements_and_data(
                &mut self,
                measurements: Eithers<AlwaysFamily, $t, $o>,
                df: FCSDataFrame,
                allow_shared_names: bool,
                skip_index_check: bool,
            ) -> PyResult<()> {
                self.0
                    .set_measurements_and_data_noprefix(
                        measurements.inner_into(),
                        df,
                        allow_shared_names,
                        skip_index_check,
                    )
                    .py_term_resolve_nowarn()
            }
        }
    };
}

impl_set_meas_and_data_noprefix!(PyCoreDataset3_1, PyTemporal3_1, PyOptical3_1);
impl_set_meas_and_data_noprefix!(PyCoreDataset3_2, PyTemporal3_2, PyOptical3_2);

macro_rules! impl_core3_2 {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// Value for *$BEGINDATETIME*
            ///
            /// Must come before *$ENDDATETIME* if it exists.
            ///
            /// :type: :py:class:`datetime.datetime` | None
            #[getter]
            fn get_begindatetime(&self) -> Option<DateTime<FixedOffset>> {
                self.0.begindatetime()
            }

            #[setter]
            fn set_begindatetime(&mut self, x: Option<DateTime<FixedOffset>>) -> PyResult<()> {
                Ok(self.0.set_begindatetime(x)?)
            }

            /// Value for *$ENDDATETIME*
            ///
            /// Must come after *$BEGINDATETIME* if it exists.
            ///
            /// :type: :py:class:`datetime.datetime` | None
            #[getter]
            fn get_enddatetime(&self) -> Option<DateTime<FixedOffset>> {
                self.0.enddatetime()
            }

            #[setter]
            fn set_enddatetime(&mut self, x: Option<DateTime<FixedOffset>>) -> PyResult<()> {
                Ok(self.0.set_enddatetime(x)?)
            }

            /// Value for *$UNSTAINEDCENTERS*
            ///
            /// This is a dictionary where keys are names which must reference
            /// a *$PnN*. Names must not contain commas.
            ///
            /// :type: dict[str, float]
            #[getter]
            fn get_unstained_centers(&self) -> Option<UnstainedCenters> {
                self.0.metaroot_opt::<UnstainedCenters>().map(|y| y.clone())
            }

            #[setter]
            fn set_unstained_centers(&mut self, us: Option<UnstainedCenters>) -> PyResult<()> {
                self.0.set_unstained_centers(us).py_term_resolve_nowarn()
            }

            /// Value for *$RnI/$RnW/$GATING* keywords.
            ///
            /// :type: :py:class:`AppliedGates3_2`
            #[getter]
            fn get_applied_gates(&self) -> PyAppliedGates3_2 {
                self.0.metaroot::<AppliedGates3_2>().clone().into()
            }

            #[setter]
            fn set_applied_gates(&mut self, ag: PyAppliedGates3_2) -> PyResult<()> {
                Ok(self.0.set_applied_gates_3_2(ag.into())?)
            }
        }
    };
}

impl_core3_2!(PyCoreTEXT3_2);
impl_core3_2!(PyCoreDataset3_2);

// gating for 2.0
macro_rules! impl_get_set_applied_gates_2_0 {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// Value for *$Gm*/$RnI/$RnW/$GATING/$GATE* keywords.
            ///
            /// :type: :py:class:`AppliedGates2_0`
            #[getter]
            fn get_applied_gates(&self) -> PyAppliedGates2_0 {
                self.0.metaroot::<AppliedGates2_0>().clone().into()
            }

            #[setter]
            fn set_applied_gates(&mut self, ag: PyAppliedGates2_0) {
                self.0.set_metaroot(ag.0)
            }
        }
    };
}

impl_get_set_applied_gates_2_0!(PyCoreTEXT2_0);
impl_get_set_applied_gates_2_0!(PyCoreDataset2_0);

// gating for 3.0/3.1
macro_rules! impl_get_set_applied_gates_3_0 {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// Value for *$Gm*/$RnI/$RnW/$GATING/$GATE* keywords.
            ///
            /// :type: :py:class:`AppliedGates3_0`
            #[getter]
            fn get_applied_gates(&self) -> PyAppliedGates3_0 {
                self.0.metaroot::<AppliedGates3_0>().clone().into()
            }

            #[setter]
            fn set_applied_gates(&mut self, ag: PyAppliedGates3_0) -> PyResult<()> {
                Ok(self.0.set_applied_gates_3_0(ag.into())?)
            }
        }
    };
}

impl_get_set_applied_gates_3_0!(PyCoreTEXT3_0);
impl_get_set_applied_gates_3_0!(PyCoreDataset3_0);
impl_get_set_applied_gates_3_0!(PyCoreTEXT3_1);
impl_get_set_applied_gates_3_0!(PyCoreDataset3_1);

// Get/set methods for $PnE (2.0)
macro_rules! impl_get_set_all_pne {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// The value for *$PnE* for all measurements.
            ///
            /// Will be ``()`` for linear scaling (``0,0`` in FCS encoding), a
            /// 2-tuple for log scaling, and ``None`` if missing.
            ///
            /// The temporal measurement must always be ``()``. Setting it
            /// to another value will raise an exception.
            ///
            /// :type: list[() | (float, float) | None]
            #[getter]
            fn get_scales(&self) -> Vec<Option<Scale>> {
                self.0.scales().collect()
            }

            #[setter]
            fn set_scales(&mut self, scales: Vec<Option<Scale>>) -> PyResult<()> {
                self.0.set_scales(scales).py_term_resolve_nowarn()
            }
        }
    };
}

impl_get_set_all_pne!(PyCoreTEXT2_0);
impl_get_set_all_pne!(PyCoreDataset2_0);

// Get/set methods for $PnE/$PnG (3.0-3.2)
macro_rules! impl_get_set_all_transform {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// The value for *$PnE* and/or *$PnG* for all measurements.
            ///
            /// Collectively these keywords correspond to scale transforms.
            ///
            /// If scaling is linear, return a float which corresponds to the
            /// value of *$PnG* when *$PnE* is ``0,0``. If scaling is logarithmic,
            /// return a pair of floats, corresponding to unset *$PnG* and the
            /// non-``0,0`` value of *$PnE*.
            ///
            /// The FCS standards disallow any other combinations.
            ///
            /// The temporal measurement will always be ``1.0``, corresponding
            /// to an identity transform. Setting it to another value will
            /// raise an exception.
            ///
            /// :type: list[float | (float, float)]
            #[getter]
            fn get_transforms(&self) -> Vec<core::ScaleTransform> {
                self.0.transforms().collect()
            }

            #[setter]
            fn set_transforms(&mut self, transforms: Vec<core::ScaleTransform>) -> PyResult<()> {
                self.0.set_transforms(transforms).py_term_resolve_nowarn()
            }
        }
    };
}

impl_get_set_all_transform!(PyCoreTEXT3_0);
impl_get_set_all_transform!(PyCoreTEXT3_1);
impl_get_set_all_transform!(PyCoreTEXT3_2);
impl_get_set_all_transform!(PyCoreDataset3_0);
impl_get_set_all_transform!(PyCoreDataset3_1);
impl_get_set_all_transform!(PyCoreDataset3_2);

// Get/set methods for $TIMESTEP (3.0-3.2)
macro_rules! impl_get_set_timestep {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// The value of *$TIMESTEP*.
            ///
            /// :type: float | None
            #[getter]
            fn get_timestep(&self) -> Option<kws::Timestep> {
                self.0.timestep().copied()
            }

            /// Set the *$TIMESTEP* if time measurement is present.
            ///
            /// :param float timestep: The timestep to set. Must be greater
            ///     than zero.
            ///
            /// :return: Previous *$TIMESTEP* if present.
            /// :rtype: float | None
            fn set_timestep(&mut self, timestep: kws::Timestep) -> Option<kws::Timestep> {
                self.0.set_timestep(timestep)
            }
        }
    };
}

impl_get_set_timestep!(PyCoreTEXT3_0);
impl_get_set_timestep!(PyCoreTEXT3_1);
impl_get_set_timestep!(PyCoreTEXT3_2);
impl_get_set_timestep!(PyCoreDataset3_0);
impl_get_set_timestep!(PyCoreDataset3_1);
impl_get_set_timestep!(PyCoreDataset3_2);

// Get/set methods for $LAST_MODIFIER/$LAST_MODIFIED/$ORIGINALITY (3.1-3.2)
macro_rules! impl_modification_attrs {
    ($pytype:ident) => {
        impl_get_set_metaroot!(
            Option<kws::Originality>,
            "Literal[\"Original\", \"NonDataModified\", \"Appended\", \"DataModified\"]",
            $pytype
        );

        impl_get_set_metaroot!(
            Option<kws::LastModified>,
            ":py:class:`datetime.datetime`",
            "LAST_MODIFIED",
            $pytype
        );

        impl_get_set_metaroot!(Option<kws::LastModifier>, "str", "LAST_MODIFIER", $pytype);
    };
}

impl_modification_attrs!(PyCoreTEXT3_1);
impl_modification_attrs!(PyCoreTEXT3_2);
impl_modification_attrs!(PyCoreDataset3_1);
impl_modification_attrs!(PyCoreDataset3_2);

// Get/set methods for $CARRIERID/$CARRIERTYPE/$LOCATIONID (3.2)
macro_rules! impl_carrier_attrs {
    ($pytype:ident) => {
        impl_get_set_metaroot!(Option<kws::Carriertype>, "str", $pytype);
        impl_get_set_metaroot!(Option<kws::Carrierid>, "str", $pytype);
        impl_get_set_metaroot!(Option<kws::Locationid>, "str", $pytype);
    };
}

impl_carrier_attrs!(PyCoreTEXT3_2);
impl_carrier_attrs!(PyCoreDataset3_2);

// Get/set methods for $PLATEID/$WELLID/$PLATENAME (3.1-3.2)
macro_rules! impl_plate_attrs {
    ($pytype:ident) => {
        impl_get_set_metaroot!(Option<kws::Wellid>, "str", $pytype);
        impl_get_set_metaroot!(Option<kws::Plateid>, "str", $pytype);
        impl_get_set_metaroot!(Option<kws::Platename>, "str", $pytype);
    };
}

impl_plate_attrs!(PyCoreTEXT3_1);
impl_plate_attrs!(PyCoreTEXT3_2);
impl_plate_attrs!(PyCoreDataset3_1);
impl_plate_attrs!(PyCoreDataset3_2);

// get/set methods for $COMP (2.0-3.0)
macro_rules! impl_get_set_comp {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            // TODO docstring will be different for 2.0 and 3.0
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

impl_get_set_comp!(PyCoreTEXT2_0);
impl_get_set_comp!(PyCoreTEXT3_0);
impl_get_set_comp!(PyCoreDataset2_0);
impl_get_set_comp!(PyCoreDataset3_0);

// Get/set methods for $SPILLOVER (3.1-3.2)
macro_rules! impl_spillover {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// The matrix component of *$SPILLOVER*.
            ///
            /// Will always be at least 2x2.
            ///
            /// :type: :py:class:`numpy.ndarray`
            #[getter]
            fn get_spillover_matrix<'a>(&self, py: Python<'a>) -> Option<Bound<'a, PyArray2<f32>>> {
                self.0.spillover_matrix().map(|x| x.to_pyarray(py))
            }

            /// The measurement names component of *$SPILLOVER*.
            ///
            /// Will always contain at least 2 names.
            ///
            /// :type: list[str] | None
            #[getter]
            fn get_spillover_names(&self) -> Option<Vec<Shortname>> {
                self.0.spillover_names().map(|x| x.to_vec())
            }

            /// Set the value of *$SPILLOVER*.
            ///
            /// :param names: Measurement names for matrix. Must refer to exising
            ///     *$PnN* and must not have commas.
            /// :type names: list[str]
            /// :param matrix: Spillover matrix values in row-major order.
            /// :type matrix: :py:class:`numpy.ndarray`
            fn set_spillover(
                &mut self,
                names: Vec<Shortname>,
                matrix: PyReadonlyArray2<f32>,
            ) -> PyResult<()> {
                let m = matrix.as_matrix().into_owned();
                Ok(self.0.set_spillover(names, m)?)
            }

            /// Unset value of *$SPILLOVER*.
            fn unset_spillover(&mut self) {
                self.0.unset_spillover()
            }
        }
    };
}

impl_spillover!(PyCoreTEXT3_1);
impl_spillover!(PyCoreTEXT3_2);
impl_spillover!(PyCoreDataset3_1);
impl_spillover!(PyCoreDataset3_2);

macro_rules! impl_get_set_all_peak {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// The value of *$PKn* for all measurements.
            ///
            /// :type: list[int]
            #[getter]
            fn get_all_pkn(&self) -> Vec<Option<kws::PeakBin>> {
                self.0
                    .get_temporal_optical::<Option<kws::PeakBin>>()
                    .map(|x| x.as_ref().copied())
                    .collect()
            }

            #[setter]
            fn set_all_pkn(&mut self, xs: Vec<Option<kws::PeakBin>>) -> PyResult<()> {
                Ok(self.0.set_temporal_optical(xs)?)
            }

            /// The value of *$PKNn* for all measurements.
            ///
            /// :type: list[int]
            #[getter]
            fn get_all_pknn(&self) -> Vec<Option<kws::PeakNumber>> {
                self.0
                    .get_temporal_optical::<Option<kws::PeakNumber>>()
                    .map(|x| x.as_ref().copied())
                    .collect()
            }

            #[setter]
            fn set_all_pknn(&mut self, xs: Vec<Option<kws::PeakNumber>>) -> PyResult<()> {
                Ok(self.0.set_temporal_optical(xs)?)
            }
        }
    };
}

impl_get_set_all_peak!(PyCoreTEXT2_0);
impl_get_set_all_peak!(PyCoreTEXT3_0);
impl_get_set_all_peak!(PyCoreTEXT3_1);
impl_get_set_all_peak!(PyCoreDataset2_0);
impl_get_set_all_peak!(PyCoreDataset3_0);
impl_get_set_all_peak!(PyCoreDataset3_1);

macro_rules! impl_get_set_subset {
    ($pytype:ident) => {
        impl_get_set_metaroot!(Option<kws::CSTot>, "int", $pytype);
        impl_get_set_metaroot!(Option<kws::CSVBits>, "int", $pytype);
        impl_get_set_metaroot!(Option<core::CSVFlags>, "list[int | None]", $pytype);
    };
}

impl_get_set_subset!(PyCoreTEXT3_0);
impl_get_set_subset!(PyCoreTEXT3_1);
impl_get_set_subset!(PyCoreDataset3_0);
impl_get_set_subset!(PyCoreDataset3_1);

impl_get_set_metaroot! {
    Option<kws::Unicode>,
    "(int, list[str])",
    PyCoreTEXT3_0,
    PyCoreDataset3_0
}

impl_get_set_metaroot! {
    Option<kws::Vol>,
    "float",
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_1,
    PyCoreDataset3_2
}

// Get/set methods for $MODE (2.0-3.1)
impl_get_set_metaroot! {
    kws::Mode,
    "Literal[\"L\", \"U\", \"C\"]",
    PyCoreTEXT2_0,
    PyCoreTEXT3_0,
    PyCoreTEXT3_1,
    PyCoreDataset2_0,
    PyCoreDataset3_0,
    PyCoreDataset3_1
}

// Get/set methods for $MODE (3.2)
impl_get_set_metaroot! {
    Option<kws::Mode3_2>,
    "Literal[\"L\"]",
    "MODE",
    PyCoreTEXT3_2,
    PyCoreDataset3_2
}

// Get/set methods for (optional) $CYT (2.0-3.1)
//
// 3.2 is required which is why it is not included here
impl_get_set_metaroot! {
    Option<kws::Cyt>,
    "str",
    PyCoreTEXT2_0,
    PyCoreTEXT3_0,
    PyCoreTEXT3_1,
    PyCoreDataset2_0,
    PyCoreDataset3_0,
    PyCoreDataset3_1
}

// Get/set methods for $FLOWRATE (3.2)
impl_get_set_metaroot! {
    Option<kws::Flowrate>,
    "str",
    PyCoreTEXT3_2,
    PyCoreDataset3_2
}

// Get/set methods for $CYTSN (3.0-3.2)
impl_get_set_metaroot! {
    Option<kws::Cytsn>,
    "str",
    PyCoreTEXT3_0,
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_0,
    PyCoreDataset3_1,
    PyCoreDataset3_2
}

// Get/set methods for $CYT (required) (3.2)
impl_get_set_metaroot! {kws::Cyt, "str", PyCoreTEXT3_2, PyCoreDataset3_2}

// Get/set methods for $UNSTAINEDINFO (3.2)
impl_get_set_metaroot! {
    Option<kws::UnstainedInfo>,
    "str",
    PyCoreTEXT3_2,
    PyCoreDataset3_2
}

// Get/set methods for scaler $PnL (2.0-3.0)
impl_get_set_all_meas! {
    NonCenterElement<Option<kws::Wavelength>>,
    "L",
    "float",
    PyCoreTEXT2_0,
    PyCoreTEXT3_0,
    PyCoreDataset2_0,
    PyCoreDataset3_0
}

// Get/set methods for vector $PnL (3.1-3.2)
impl_get_set_all_meas! {
    NonCenterElement<Option<kws::Wavelengths>>,
    "L",
    "list[float]",
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_1,
    PyCoreDataset3_2
}

// Get/set methods for $PnD (3.1+)
//
// This is valid for the time channel so don't set on just optical
impl_get_set_all_meas! {
    Option<kws::Display>,
    "D",
    "(bool, float, float)",
    PyCoreTEXT3_1,
    PyCoreDataset3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
}

// Get/set methods for $PnDET (3.2)
impl_get_set_all_meas! {
    NonCenterElement<Option<kws::DetectorName>>,
    "DET",
    "str",
    PyCoreTEXT3_2,
    PyCoreDataset3_2
}

// Get/set methods for $PnCALIBRATION (3.1)
impl_get_set_all_meas! {
    NonCenterElement<Option<kws::Calibration3_1>>,
    "CALIBRATION",
    "(float, str)",
    PyCoreTEXT3_1,
    PyCoreDataset3_1
}

// Get/set methods for $PnCALIBRATION (3.2)
impl_get_set_all_meas! {
    NonCenterElement<Option<kws::Calibration3_2>>,
    "CALIBRATION",
    "(float, float, str)",
    PyCoreTEXT3_2,
    PyCoreDataset3_2
}

// Get/set methods for $PnTAG (3.2)
impl_get_set_all_meas! {
    NonCenterElement<Option<kws::Tag>>,
    "TAG",
    "str",
    PyCoreTEXT3_2,
    PyCoreDataset3_2
}

// Get/set methods for $PnTYPE (3.2)
impl_get_set_all_meas! {
    NonCenterElement<Option<kws::OpticalType>>,
    "TYPE",
    "str",
    PyCoreTEXT3_2,
    PyCoreDataset3_2
}

// Get/set methods for $PnFEATURE (3.2)
impl_get_set_all_meas! {
    NonCenterElement<Option<kws::Feature>>,
    "FEATURE",
    "Literal[\"Area\", \"Width\", \"Height\"]",
    PyCoreTEXT3_2,
    PyCoreDataset3_2
}

// Get/set methods for $PnANALYTE (3.2)
impl_get_set_all_meas! {
    NonCenterElement<Option<kws::Analyte>>,
    "ANALYTE",
    "str",
    PyCoreTEXT3_2,
    PyCoreDataset3_2
}

// Add method to convert CoreTEXT* to CoreDataset* by adding DATA, ANALYSIS, and
// OTHER(s) (all versions)
macro_rules! impl_to_dataset {
    ($from:ident, $to:ident) => {
        #[pymethods]
        // TODO use proc macro to get return type in docstring
        impl $from {
            /// Convert to a dataset object.
            ///
            /// This will fully represent an FCS file, as opposed to just
            /// representing *HEADER* and *TEXT*.
            ///
            /// :param df: Columns corresponding to *DATA*.
            /// :type df: :py:class:`polars.DataFrame`
            /// :param analysis: Bytes corresponding to *ANALYSIS*.
            /// :type analysis: bytes
            /// :param others: Bytes corresponding to *OTHERS*.
            /// :type others: list[bytes]
            #[pyo3(signature = (df, analysis = core::Analysis::default(), others = core::Others::default()))]
            fn to_dataset(
                &self,
                df: FCSDataFrame,
                analysis: core::Analysis,
                others: core::Others,
            ) -> PyResult<$to> {
                Ok(self.0.clone().into_coredataset(df, analysis, others)?.into())
            }
        }
    };
}

impl_to_dataset!(PyCoreTEXT2_0, PyCoreDataset2_0);
impl_to_dataset!(PyCoreTEXT3_0, PyCoreDataset3_0);
impl_to_dataset!(PyCoreTEXT3_1, PyCoreDataset3_1);
impl_to_dataset!(PyCoreTEXT3_2, PyCoreDataset3_2);

macro_rules! impl_write_dataset {
    ($pytype:ident, $exc:expr) => {
        #[pymethods]
        impl $pytype {
            /// Write data as an FCS file.
            ///
            /// The resulting file will include *HEADER*, *TEXT*, *DATA*,
            /// *ANALYSIS*, and *OTHER* as they present from this class.
            ///
            #[doc = $exc]
            /// :param path: Path to be written.
            /// :type path: :py:class:`pathlib.Path`
            ///
            /// :param int delim: Delimiter to use when writing *TEXT*.
            ///
            /// :param bool skip_conversion_check: Skip check to ensure that
            ///     types of the dataframe match the columns (*$PnB*,
            ///     *$DATATYPE*, etc). If this is ``False``, perform this check
            ///     before writing, and raise exception on failure. If ``True``,
            ///     raise warnings as file is being written. Skipping this is
            ///     faster since the data needs to be traversed twice to perform
            ///     the conversion check, but may result in loss of precision
            ///     and/or truncation.
            #[pyo3(signature = (path, delim = TEXTDelim::default(), skip_conversion_check = false))]
            fn write_dataset(
                &self,
                path: PathBuf,
                delim: TEXTDelim,
                skip_conversion_check: bool,
            ) -> PyResult<()> {
                let f = File::options().write(true).create(true).open(path)?;
                let mut h = BufWriter::new(f);
                let conf = cfg::WriteConfig {
                    delim,
                    skip_conversion_check,
                };
                self.0.h_write_dataset(&mut h, &conf).py_term_resolve()
            }
        }
    };
}

impl_write_dataset!(
    PyCoreDataset2_0,
    "Raise exception if file cannot fit within 99,999,999 bytes.\n"
);
impl_write_dataset!(PyCoreDataset3_0, "");
impl_write_dataset!(PyCoreDataset3_1, "");
impl_write_dataset!(PyCoreDataset3_2, "");

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

macro_rules! impl_meas_get_set {
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

macro_rules! impl_meas_get_set_common {
    ($pytype:ident) => {
        impl_meas_get_set!(get_longname, set_longname, kws::Longname, $pytype);

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

impl_meas_get_set_common!(PyOptical2_0);
impl_meas_get_set_common!(PyOptical3_0);
impl_meas_get_set_common!(PyOptical3_1);
impl_meas_get_set_common!(PyOptical3_2);
impl_meas_get_set_common!(PyTemporal2_0);
impl_meas_get_set_common!(PyTemporal3_0);
impl_meas_get_set_common!(PyTemporal3_1);
impl_meas_get_set_common!(PyTemporal3_2);

macro_rules! impl_optical_get_set {
    ($pytype:ident) => {
        impl_meas_get_set!(get_filter, set_filter, kws::Filter, $pytype);
        impl_meas_get_set!(
            get_detector_type,
            set_detector_type,
            kws::DetectorType,
            $pytype
        );
        impl_meas_get_set!(
            get_percent_emitted,
            set_percent_emitted,
            kws::PercentEmitted,
            $pytype
        );
        impl_meas_get_set!(
            get_detector_voltage,
            set_detector_voltage,
            kws::DetectorVoltage,
            $pytype
        );
        impl_meas_get_set!(get_power, set_power, kws::Power, $pytype);
    };
}

impl_optical_get_set!(PyOptical2_0);
impl_optical_get_set!(PyOptical3_0);
impl_optical_get_set!(PyOptical3_1);
impl_optical_get_set!(PyOptical3_2);

// $PnE (3.0-3.2)
macro_rules! impl_optical_get_set_transform {
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

impl_optical_get_set_transform!(PyOptical3_0);
impl_optical_get_set_transform!(PyOptical3_1);
impl_optical_get_set_transform!(PyOptical3_2);

// $PnL (2.0/3.0)
impl_meas_get_set!(
    get_wavelength,
    set_wavelength,
    kws::Wavelength,
    PyOptical2_0,
    PyOptical3_0
);

// #PnL (3.1-3.2)
impl_meas_get_set!(
    get_wavelength,
    set_wavelength,
    kws::Wavelengths,
    PyOptical3_1,
    PyOptical3_2
);

// #TIMESTEP (3.0-3.2)
macro_rules! impl_temporal_get_set_timestep {
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

impl_temporal_get_set_timestep!(PyTemporal3_0);
impl_temporal_get_set_timestep!(PyTemporal3_1);
impl_temporal_get_set_timestep!(PyTemporal3_2);

// $PnCalibration (3.1)
impl_meas_get_set!(
    get_calibration,
    set_calibration,
    kws::Calibration3_1,
    PyOptical3_1
);

// $PnD (3.1-3.2)
impl_meas_get_set!(
    get_display,
    set_display,
    kws::Display,
    PyOptical3_1,
    PyOptical3_2,
    PyTemporal3_1,
    PyTemporal3_2
);

// $PnDET (3.2)
impl_meas_get_set!(
    get_detector_name,
    set_detector_name,
    kws::DetectorName,
    PyOptical3_2
);

// $PnTAG (3.2)
impl_meas_get_set!(get_tag, set_tag, kws::Tag, PyOptical3_2);

// $PnTYPE (3.2)
impl_meas_get_set!(
    get_measurement_type,
    set_measurement_type,
    kws::OpticalType,
    PyOptical3_2
);

// $PnFEATURE (3.2)
impl_meas_get_set!(get_feature, set_feature, kws::Feature, PyOptical3_2);

// $PnANALYTE (3.2)
impl_meas_get_set!(get_analyte, set_analyte, kws::Analyte, PyOptical3_2);

// $PnCalibration (3.2)
impl_meas_get_set!(
    get_calibration,
    set_calibration,
    kws::Calibration3_2,
    PyOptical3_2
);

#[derive(IntoPyObject, FromPyObject)]
enum PyRegion<U, B> {
    Uni(U),
    Bi(B),
}

type PyRegion2_0 = PyRegion<PyUnivariateRegion2_0, PyBivariateRegion2_0>;
type PyRegion3_0 = PyRegion<PyUnivariateRegion3_0, PyBivariateRegion3_0>;
type PyRegion3_2 = PyRegion<PyUnivariateRegion3_2, PyBivariateRegion3_2>;

impl<U, B, I> From<PyRegion<U, B>> for Region<I>
where
    UnivariateRegion<I>: From<U>,
    BivariateRegion<I>: From<B>,
{
    fn from(value: PyRegion<U, B>) -> Self {
        match value {
            PyRegion::Uni(u) => Self::Univariate(u.into()),
            PyRegion::Bi(b) => Self::Bivariate(b.into()),
        }
    }
}

impl<U, B, I> From<Region<I>> for PyRegion<U, B>
where
    U: From<UnivariateRegion<I>>,
    B: From<BivariateRegion<I>>,
{
    fn from(value: Region<I>) -> Self {
        match value {
            Region::Univariate(u) => Self::Uni(u.into()),
            Region::Bivariate(b) => Self::Bi(b.into()),
        }
    }
}

#[derive(IntoPyObject)]
struct PyRegionMapping<R>(HashMap<RegionIndex, R>);

impl<'py, R> FromPyObject<'py> for PyRegionMapping<R>
where
    R: FromPyObject<'py>,
{
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let xs: HashMap<RegionIndex, R> = ob.extract()?;
        Ok(Self(xs))
    }
}

impl<I, R> From<PyRegionMapping<R>> for HashMap<RegionIndex, Region<I>>
where
    Region<I>: From<R>,
{
    fn from(value: PyRegionMapping<R>) -> Self {
        value.0.into_iter().map(|(k, v)| (k, v.into())).collect()
    }
}

impl<I, R> From<HashMap<RegionIndex, Region<I>>> for PyRegionMapping<R>
where
    R: From<Region<I>>,
{
    fn from(value: HashMap<RegionIndex, Region<I>>) -> Self {
        Self(value.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

#[pymethods]
impl PyUnivariateRegion2_0 {
    /// Make a new FCS 2.0-compatible univariate region.
    ///
    /// :param int index: The index corresponding to a gating measurement
    ///     (the *m* in the *$Gm\** keywords).
    ///
    /// :param gate: The lower and upper bounds of the gate.
    /// :type gate: (float, float)
    #[new]
    fn new(index: GateIndex, gate: kws::UniGate) -> Self {
        UnivariateRegion { index, gate }.into()
    }

    /// The value of the index.
    ///
    /// :rtype: int
    #[getter]
    fn index(&self) -> GateIndex {
        self.0.index
    }
}

#[pymethods]
impl PyUnivariateRegion3_0 {
    /// Make a new FCS 3.0/3.1-compatible univariate region.
    ///
    /// :param str index: The index corresponding to either a gating
    ///     or a physical measurement (the *m* and *n* in the *$Gm\** or *$Pn\**
    ///     keywords). Must be a string like either ``Gm`` or ``Pn`` where
    ///     ``m`` is an integer and the prefix corresponds to a gating or
    ///     physical measurement respectively.
    ///
    /// :param gate: The lower and upper bounds of the gate.
    /// :type gate: (float, float)
    #[new]
    fn new(index: kws::MeasOrGateIndex, gate: kws::UniGate) -> Self {
        UnivariateRegion { index, gate }.into()
    }

    /// The value of the index.
    ///
    /// :return: A string like either ``Gm`` or ``Pn`` where ``m`` is an
    ///     integer and the prefix corresponds to a gating or physical
    ///     measurement respectively.
    /// :rtype: str
    #[getter]
    fn index(&self) -> kws::MeasOrGateIndex {
        self.0.index
    }
}

#[pymethods]
impl PyUnivariateRegion3_2 {
    /// Make a new FCS 3.2-compatible univariate region.
    ///
    /// :param int index: The index corresponding to either a physical
    ///     measurement (the *n* in the *$Pn\** keywords).
    ///
    /// :param gate: The lower and upper bounds of the gate.
    /// :type gate: (float, float)
    #[new]
    fn new(index: kws::PrefixedMeasIndex, gate: kws::UniGate) -> Self {
        UnivariateRegion { index, gate }.into()
    }

    /// The value of the index.
    ///
    /// :rtype: int
    #[getter]
    fn index(&self) -> kws::PrefixedMeasIndex {
        self.0.index
    }
}

macro_rules! impl_univarate_get_gate {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// The lower and upper bounds of the gate.
            ///
            /// :rtype: (float, float)
            #[getter]
            fn gate(&self) -> kws::UniGate {
                self.0.gate.clone()
            }
        }
    };
}

impl_univarate_get_gate!(PyUnivariateRegion2_0);
impl_univarate_get_gate!(PyUnivariateRegion3_0);
impl_univarate_get_gate!(PyUnivariateRegion3_2);

#[pymethods]
impl PyBivariateRegion2_0 {
    /// Make a new FCS 2.0-compatible univariate region.
    ///
    /// :param index: The x/y indices corresponding to gating measurements
    ///     (the *m* in the *$Gm\** keywords).
    /// :type index: (int, int)
    ///
    /// :param gate: The vertices of a polygon gate. Must not be empty.
    /// :type gate: list[(float, float)]
    #[new]
    fn new(index: kws::IndexPair<GateIndex>, vertices: FCSNonEmpty<kws::Vertex>) -> Self {
        BivariateRegion { index, vertices }.into()
    }

    /// The value of the x/y indices.
    ///
    /// :rtype: (int, int)
    #[getter]
    fn index(&self) -> kws::IndexPair<GateIndex> {
        self.0.index
    }
}

#[pymethods]
impl PyBivariateRegion3_0 {
    /// Make a new FCS 3.0/3.1-compatible univariate region.
    ///
    /// :param index: The x/y indices corresponding to either gating or
    ///     physical measurements (the *m* or *n* in the *$Gm\** or *$Pn\**
    ///     keywords). Each must be a string like either ``Gm`` or ``Pn``
    ///     where ``m`` is an integer and the prefix corresponds to a gating
    ///     or physical measurement respectively.
    /// :type index: (str, str)
    ///
    /// :param gate: The vertices of a polygon gate. Must not be empty.
    /// :type gate: list[(float, float)]
    #[new]
    fn new(
        index: kws::IndexPair<kws::MeasOrGateIndex>,
        vertices: FCSNonEmpty<kws::Vertex>,
    ) -> Self {
        BivariateRegion { index, vertices }.into()
    }

    /// The value of the x/y indices.
    ///
    /// :return: Two strings like either ``Gm`` or ``Pn`` where ``m`` or ``n``
    ///     is an integer and the prefix corresponds to a gating or physical
    ///     measurement respectively.
    /// :rtype: (str, str)
    #[getter]
    fn index(&self) -> kws::IndexPair<kws::MeasOrGateIndex> {
        self.0.index
    }
}

#[pymethods]
impl PyBivariateRegion3_2 {
    /// Make a new FCS 3.2-compatible univariate region.
    ///
    /// :param index: The x/y indices corresponding to physical measurements
    ///     (the *n* in the *$Pn\** keywords).
    /// :type index: (int, int)
    ///
    /// :param gate: The vertices of a polygon gate. Must not be empty.
    /// :type gate: list[(float, float)]
    #[new]
    fn new(
        index: kws::IndexPair<kws::PrefixedMeasIndex>,
        vertices: FCSNonEmpty<kws::Vertex>,
    ) -> Self {
        BivariateRegion { index, vertices }.into()
    }

    /// The value of the x/y indices.
    ///
    /// :rtype: (int, int)
    #[getter]
    fn index(&self) -> kws::IndexPair<kws::PrefixedMeasIndex> {
        self.0.index
    }
}

macro_rules! impl_bivarate_get_vertices {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// The vertices of a polygon gate.
            ///
            /// :rtype: list[(float, float)]
            #[getter]
            fn vertices(&self) -> FCSNonEmpty<kws::Vertex> {
                self.0.vertices.clone()
            }
        }
    };
}

impl_bivarate_get_vertices!(PyBivariateRegion2_0);
impl_bivarate_get_vertices!(PyBivariateRegion3_0);
impl_bivarate_get_vertices!(PyBivariateRegion3_2);

#[pymethods]
impl PyAppliedGates2_0 {
    /// Make new FCS 2.0-compatible gates.
    ///
    /// :param gated_measurements: Gated measurements corresponding to the
    ///     *$Gm\** keywords.
    /// :type gated_measurements: list[:py:class:`GatedMeasurement`]
    ///
    /// :param regions: Mapping of regions and windows to be used in gating
    ///     scheme. Corresponds to *$RnI* and *$RnW* keywords. Keys in
    ///     dictionary are the region indices (the *n* in *$RnI* and
    ///     *$RnW*). The values in the dictionary are either univariate or
    ///     bivariate gates and must correspond to an index in
    ///     ``gated_measurements``.
    /// :type regions: dict[int, :py:class:`UnivariateGate2_0` | :py:class:`BnivariateGate2_0`]
    ///
    /// :param gating: The gating scheme. Corresponds to *$GATING* keyword.
    ///      All 'Rn' in this string must reference a key in ``regions``.
    /// :param gating: str | None
    #[new]
    #[pyo3(signature = (
            gated_measurements = vec![],
            regions = PyRegionMapping(HashMap::new()),
            gating = None,
        ))]
    fn new(
        gated_measurements: Vec<PyGatedMeasurement>,
        regions: PyRegionMapping<PyRegion2_0>,
        gating: Option<kws::Gating>,
    ) -> PyResult<Self> {
        let scheme = GatingScheme::try_new(gating, regions.into())?;
        let gs: Vec<_> = gated_measurements.into_iter().map(|x| x.into()).collect();
        Ok(AppliedGates2_0::try_new(gs.into(), scheme)?.into())
    }

    /// Value of all gating regions, corresponding to *$RnI* and *$RnW*.
    ///
    /// :type regions: dict[int, :py:class:`UnivariateGate2_0` | :py:class:`BnivariateGate2_0`]
    #[getter]
    fn regions(&self) -> PyRegionMapping<PyRegion2_0> {
        let r: &HashMap<_, _> = self.0.as_ref();
        r.clone().into()
    }
}

#[pymethods]
impl PyAppliedGates3_0 {
    /// Make new FCS 3.0/3.1-compatible gates.
    ///
    /// :param gated_measurements: Gated measurements corresponding to the
    ///     *$Gm\** keywords.
    /// :type gated_measurements: list[:py:class:`GatedMeasurement`]
    ///
    /// :param regions: Mapping of regions and windows to be used in gating
    ///     scheme. Corresponds to *$RnI* and *$RnW* keywords. Keys in
    ///     dictionary are the region indices (the *n* in *$RnI* and
    ///     *$RnW*). The values in the dictionary are either univariate or
    ///     bivariate gates and must correspond to either physical
    ///     measurements of an index in ``gated_measurements``.
    /// :type regions: dict[int, :py:class:`UnivariateGate3_0` | :py:class:`BnivariateGate3_0`]
    ///
    /// :param gating: The gating scheme. Corresponds to *$GATING* keyword.
    ///      All 'Rn' in this string must reference a key in ``regions``.
    /// :param gating: str | None
    #[new]
    #[pyo3(signature = (
            gated_measurements = vec![],
            regions = PyRegionMapping(HashMap::new()),
            gating = None,
        ))]
    fn new(
        gated_measurements: Vec<PyGatedMeasurement>,
        regions: PyRegionMapping<PyRegion3_0>,
        gating: Option<kws::Gating>,
    ) -> PyResult<Self> {
        let scheme = GatingScheme::try_new(gating, regions.into())?;
        let gs: Vec<_> = gated_measurements.into_iter().map(|x| x.into()).collect();
        Ok(AppliedGates3_0::try_new(gs.into(), scheme)?.into())
    }

    /// Value of all gating regions, corresponding to *$RnI* and *$RnW*.
    ///
    /// :type regions: dict[int, :py:class:`UnivariateGate3_0` | :py:class:`BnivariateGate3_0`]
    #[getter]
    fn regions(&self) -> PyRegionMapping<PyRegion3_0> {
        let r: &HashMap<_, _> = self.0.as_ref();
        r.clone().into()
    }
}

#[pymethods]
impl PyAppliedGates3_2 {
    /// Make new FCS 3.0/3.1-compatible gates.
    ///
    /// :param regions: Mapping of regions and windows to be used in gating
    ///     scheme. Corresponds to *$RnI* and *$RnW* keywords. Keys in
    ///     dictionary are the region indices (the *n* in *$RnI* and
    ///     *$RnW*). The values in the dictionary are either univariate or
    ///     bivariate gates and must correspond to physical measurements.
    /// :type regions: dict[int, :py:class:`UnivariateGate3_2` | :py:class:`BnivariateGate3_2`]
    ///
    /// :param gating: The gating scheme. Corresponds to *$GATING* keyword.
    ///      All 'Rn' in this string must reference a key in ``regions``.
    /// :param gating: str | None
    #[new]
    #[pyo3(signature = (
            regions = PyRegionMapping(HashMap::new()),
            gating = None,
        ))]
    fn new(regions: PyRegionMapping<PyRegion3_2>, gating: Option<kws::Gating>) -> PyResult<Self> {
        Ok(AppliedGates3_2::try_new(gating, regions.into())?.into())
    }

    /// Value of all gating regions, corresponding to *$RnI* and *$RnW*.
    ///
    /// :type regions: dict[int, :py:class:`UnivariateGate3_2` | :py:class:`BnivariateGate3_2`]
    #[getter]
    fn regions(&self) -> PyRegionMapping<PyRegion3_2> {
        let r: &HashMap<_, _> = self.0.as_ref();
        r.clone().into()
    }
}

macro_rules! impl_applied_gates_scheme {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// Value of the *$GATING* keyword.
            ///
            /// :rtype: str | None
            #[getter]
            fn scheme(&self) -> Option<kws::Gating> {
                let g: &Option<kws::Gating> = self.0.as_ref();
                g.as_ref().cloned()
            }
        }
    };
}

impl_applied_gates_scheme!(PyAppliedGates2_0);
impl_applied_gates_scheme!(PyAppliedGates3_0);
impl_applied_gates_scheme!(PyAppliedGates3_2);

macro_rules! impl_applied_gated_meas {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// All gated measurements, corresponding to each *$Gm\** keyword.
            ///
            /// *$GATE* is implied by the length of this list.
            ///
            /// :rtype: list[:py:class:`GatedMeasurement`]
            #[getter]
            fn gated_measurements(&self) -> Vec<PyGatedMeasurement> {
                let gs: &[GatedMeasurement] = self.0.as_ref();
                gs.iter().map(|x| x.clone().into()).collect()
            }
        }
    };
}

impl_applied_gated_meas!(PyAppliedGates2_0);
impl_applied_gated_meas!(PyAppliedGates3_0);

#[pymethods]
impl PyGatedMeasurement {
    /// The *$Gm\** keywords for one gated measurement.
    ///
    /// :param scale: The *$$GmE* keyword. ``()`` means linear scaling and
    ///     2-tuple specifies decades and offset for log scaling.
    /// :type scale: () | (float, float) | None
    ///
    /// :param filter: The *$GmF* keyword.
    /// :type filter: str | None
    ///
    /// :param shortname: The *$GmN* keyword. Must not contain commas.
    /// :type filter: str | None
    ///
    /// :param percent_emitted: The *$GmP* keyword.
    /// :type filter: str | None
    ///
    /// :param range: The *$GmR* keyword.
    /// :type filter: float | None
    ///
    /// :param longname: The *$GmS* keyword.
    /// :type filter: str | None
    ///
    /// :param detector_type: The *$GmT* keyword.
    /// :type filter: str | None
    ///
    /// :param detector_voltage: The *$GmV* keyword.
    /// :type filter: float | None
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
            scale = None.into(),
            filter = None.into(),
            shortname = None.into(),
            percent_emitted = None.into(),
            range = None.into(),
            longname = None.into(),
            detector_type = None.into(),
            detector_voltage = None.into(),
        ))]
    fn new(
        scale: MaybeValue<kws::GateScale>,
        filter: MaybeValue<kws::GateFilter>,
        shortname: MaybeValue<kws::GateShortname>,
        percent_emitted: MaybeValue<kws::GatePercentEmitted>,
        range: MaybeValue<kws::GateRange>,
        longname: MaybeValue<kws::GateLongname>,
        detector_type: MaybeValue<kws::GateDetectorType>,
        detector_voltage: MaybeValue<kws::GateDetectorVoltage>,
    ) -> Self {
        GatedMeasurement {
            scale,
            filter,
            shortname,
            percent_emitted,
            range,
            longname,
            detector_type,
            detector_voltage,
        }
        .into()
    }
}

macro_rules! impl_gated_meas_get_set {
    ($(#[$meta:meta])* $get:ident, $set:ident, $t:path, $inner:ident) => {
        $(#[$meta])*
        #[pymethods]
        impl PyGatedMeasurement {
            #[getter]
            fn $get(&self) -> Option<$t> {
                self.0.$inner.0.as_ref().cloned()
            }

            #[setter]
            fn $set(&mut self, x: Option<$t>) {
                self.0.$inner.0 = x.into();
            }
        }
    };
}

impl_gated_meas_get_set!(
    /// Value of the *$GmE* keyword.
    ///
    /// :type: () | (float, float) | None
    get_gme,
    set_gme,
    kws::GateScale,
    scale
);

impl_gated_meas_get_set!(
    /// Value of the *$GmF* keyword.
    ///
    /// :type: str | None
    get_gmf,
    set_gmf,
    kws::GateFilter,
    filter
);

impl_gated_meas_get_set!(
    /// Value of the *$GmN* keyword.
    ///
    /// :type: str | None
    get_gmn,
    set_gmn,
    kws::GateShortname,
    shortname
);

impl_gated_meas_get_set!(
    /// Value of the *$GmP* keyword.
    ///
    /// :type: str | None
    get_gmp,
    set_gmp,
    kws::GatePercentEmitted,
    percent_emitted
);

impl_gated_meas_get_set!(
    /// Value of the *$GmR* keyword.
    ///
    /// :type: float | None
    get_gmr,
    set_gmr,
    kws::GateRange,
    range
);

impl_gated_meas_get_set!(
    /// Value of the *$GmS* keyword.
    ///
    /// :type: str | None
    get_gms,
    set_gms,
    kws::GateLongname,
    longname
);

impl_gated_meas_get_set!(
    /// Value of the *$GmT* keyword.
    ///
    /// :type: str | None
    get_gmt,
    set_gmt,
    kws::GateDetectorType,
    detector_type
);

impl_gated_meas_get_set!(
    /// Value of the *$GmV* keyword.
    ///
    /// :type: float | None
    get_gmv,
    set_gmv,
    kws::GateDetectorVoltage,
    detector_voltage
);

macro_rules! impl_layout_common {
    ($t:ident) => {
        #[pymethods]
        impl $t {
            /// Return the widths of each column (ie the $PnB keyword).
            ///
            /// This will be a list of integers equal to the number of columns
            /// or an empty list if the layout is delimited Ascii (in which case
            /// it has no column widths).
            #[getter]
            fn widths(&self) -> Option<Vec<u32>> {
                self.0
                    .widths()
                    .map(|ws| ws.into_iter().map(|x| u32::from(u8::from(x))).collect())
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
                self.0.datatypes().into_iter().map(|d| d.into()).collect()
            }
        }
    };
}

impl_layout_common!(PyAsciiFixedLayout);
impl_layout_common!(PyAsciiDelimLayout);
impl_layout_common!(PyOrderedUint08Layout);
impl_layout_common!(PyOrderedUint16Layout);
impl_layout_common!(PyOrderedUint24Layout);
impl_layout_common!(PyOrderedUint32Layout);
impl_layout_common!(PyOrderedUint40Layout);
impl_layout_common!(PyOrderedUint48Layout);
impl_layout_common!(PyOrderedUint56Layout);
impl_layout_common!(PyOrderedUint64Layout);
impl_layout_common!(PyOrderedF32Layout);
impl_layout_common!(PyOrderedF64Layout);
impl_layout_common!(PyEndianF32Layout);
impl_layout_common!(PyEndianF64Layout);
impl_layout_common!(PyEndianUintLayout);
impl_layout_common!(PyMixedLayout);

macro_rules! impl_layout_byteord {
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

impl_layout_byteord!(PyOrderedUint08Layout);
impl_layout_byteord!(PyOrderedUint16Layout);
impl_layout_byteord!(PyOrderedUint24Layout);
impl_layout_byteord!(PyOrderedUint32Layout);
impl_layout_byteord!(PyOrderedUint40Layout);
impl_layout_byteord!(PyOrderedUint48Layout);
impl_layout_byteord!(PyOrderedUint56Layout);
impl_layout_byteord!(PyOrderedUint64Layout);
impl_layout_byteord!(PyOrderedF32Layout);
impl_layout_byteord!(PyOrderedF64Layout);

macro_rules! impl_layout_endianness {
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

impl_layout_endianness!(PyEndianF32Layout);
impl_layout_endianness!(PyEndianF64Layout);
impl_layout_endianness!(PyEndianUintLayout);
impl_layout_endianness!(PyMixedLayout);

#[pymethods]
impl PyAsciiDelimLayout {
    #[new]
    fn new(ranges: Vec<u64>) -> Self {
        DelimAsciiLayout::new(ranges).into()
    }
}

#[pymethods]
impl PyAsciiFixedLayout {
    #[new]
    fn new(ranges: Vec<u64>) -> Self {
        FixedLayout::new_ascii_u64(ranges).into()
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

macro_rules! impl_layout_new_ordered_uint {
    ($t:ident, $uint:ident, $size:expr) => {
        #[pymethods]
        impl $t {
            /// Make a new layout for $size-byte Uints with a given endian-ness.
            #[new]
            fn new(ranges: Vec<bm::Bitmask<$uint, $size>>, is_big: bool) -> Self {
                FixedLayout::new_endian_uint(ranges, is_big.into()).into()
            }

            #[classmethod]
            /// Make a new layout for $size-byte Uints with a given byte order.
            fn new_ordered(
                _: &Bound<'_, PyType>,
                ranges: Vec<bm::Bitmask<$uint, $size>>,
                byteord: SizedByteOrd<$size>,
            ) -> Self {
                FixedLayout::new(ranges, byteord).into()
            }
        }
    };
}

impl_layout_new_ordered_uint!(PyOrderedUint08Layout, u8, 1);
impl_layout_new_ordered_uint!(PyOrderedUint16Layout, u16, 2);
impl_layout_new_ordered_uint!(PyOrderedUint24Layout, u32, 3);
impl_layout_new_ordered_uint!(PyOrderedUint32Layout, u32, 4);
impl_layout_new_ordered_uint!(PyOrderedUint40Layout, u64, 5);
impl_layout_new_ordered_uint!(PyOrderedUint48Layout, u64, 6);
impl_layout_new_ordered_uint!(PyOrderedUint56Layout, u64, 7);
impl_layout_new_ordered_uint!(PyOrderedUint64Layout, u64, 8);

macro_rules! impl_layout_new_ordered_float {
    ($t:ident, $num:ident, $size:expr) => {
        #[pymethods]
        impl $t {
            /// Make a new $num layout with a given endian-ness.
            #[new]
            fn new(ranges: Vec<FloatRange<$num, $size>>, is_big: bool) -> Self {
                FixedLayout::new_endian_float(ranges, is_big.into()).into()
            }

            #[classmethod]
            /// Make a new $num layout with a given byte order.
            fn new_ordered(
                _: &Bound<'_, PyType>,
                ranges: Vec<FloatRange<$num, $size>>,
                byteord: SizedByteOrd<$size>,
            ) -> Self {
                FixedLayout::new(ranges, byteord).into()
            }
        }
    };
}

impl_layout_new_ordered_float!(PyOrderedF32Layout, f32, 4);
impl_layout_new_ordered_float!(PyOrderedF64Layout, f64, 8);

// float layouts for 3.1/3.2
macro_rules! impl_layout_new_endian_float {
    ($t:ident, $num:ident, $size:expr) => {
        #[pymethods]
        impl $t {
            #[new]
            /// Make a new $num layout with a given endian-ness.
            fn new(ranges: Vec<FloatRange<$num, $size>>, is_big: bool) -> Self {
                FixedLayout::new(ranges, is_big.into()).into()
            }
        }
    };
}

impl_layout_new_endian_float!(PyEndianF32Layout, f32, 4);
impl_layout_new_endian_float!(PyEndianF64Layout, f64, 8);

#[pymethods]
impl PyEndianUintLayout {
    /// Make a new Uint layout with a given endian-ness.
    ///
    /// Width of each column (in bytes) will depend in the input range.
    #[new]
    fn new(ranges: Vec<u64>, is_big: bool) -> Self {
        let rs = ranges.into_iter().map(AnyNullBitmask::from_u64).collect();
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
    fn new(ranges: Vec<NullMixedType>, is_big: bool) -> Self {
        FixedLayout::new(ranges, is_big.into()).into()
    }
}

// pub(crate) struct PyNonEmpty<T>(pub(crate) NonEmpty<T>);

// impl<'py, T: FromPyObject<'py>> FromPyObject<'py> for PyNonEmpty<T> {
//     fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
//         let xs: Vec<_> = ob.extract()?;
//         NonEmpty::from_vec(xs)
//             .ok_or(PyValueError::new_err("list must not be empty"))
//             .map(Self)
//     }
// }
