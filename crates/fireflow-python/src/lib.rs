use fireflow_core::api;
use fireflow_core::config as cfg;
use fireflow_core::core;
use fireflow_core::data::{
    AnyAsciiLayout, AnyNullBitmask, AnyOrderedLayout, AnyOrderedUintLayout, DataLayout2_0,
    DataLayout3_0, DataLayout3_1, DataLayout3_2, DelimAsciiLayout, EndianLayout, F32Range,
    F64Range, FixedAsciiLayout, FixedLayout, KnownTot, LayoutOps, MixedLayout, NoMeasDatatype,
    NonMixedEndianLayout, NullMixedType, OrderedLayout, OrderedLayoutOps,
};
use fireflow_core::error::{MultiResultExt, ResultExt};
use fireflow_core::header::{Header, Version};
use fireflow_core::nonempty::FCSNonEmpty;
use fireflow_core::python::exceptions::{PyTerminalNoWarnResultExt, PyTerminalResultExt};
use fireflow_core::segment::{HeaderAnalysisSegment, HeaderDataSegment, OtherSegment};
use fireflow_core::text::byteord::{Endian, SizedByteOrd};
use fireflow_core::text::compensation::{Compensation, Compensation2_0, Compensation3_0};
use fireflow_core::text::datetimes::{BeginDateTime, EndDateTime};
use fireflow_core::text::gating::{
    AppliedGates2_0, AppliedGates3_0, AppliedGates3_2, BivariateRegion, GatedMeasurement,
    GatingScheme, Region, UnivariateRegion,
};
use fireflow_core::text::index::{GateIndex, MeasIndex, RegionIndex};
use fireflow_core::text::keywords as kws;
use fireflow_core::text::named_vec::{Eithers, Element, NamedVec, NonCenterElement};
use fireflow_core::text::optional::{AlwaysFamily, MaybeFamily, MightHave};
use fireflow_core::text::scale::Scale;
use fireflow_core::text::spillover::Spillover;
use fireflow_core::text::timestamps::{Btim, Etim, FCSDate, FCSTime, FCSTime100, FCSTime60};
use fireflow_core::text::unstainedcenters::UnstainedCenters;
use fireflow_core::validated::dataframe::{AnyFCSColumn, FCSDataFrame};
use fireflow_core::validated::keys::{NonStdKey, StdKeywords, ValidKeywords};
use fireflow_core::validated::shortname::Shortname;
use fireflow_python_proc::{
    impl_convert_version, impl_gated_meas, impl_get_set_all_meas, impl_get_set_meas_obj_common,
    impl_get_set_metaroot, impl_meas_get_set, impl_new_core, impl_new_meas,
    impl_new_ordered_layout,
};

use derive_more::{From, Into};
use pyo3::prelude::*;
use pyo3::types::{PyTuple, PyType};
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
    // pyo3 currently cannot add docstrings to __new__ methods, see
    // https://github.com/PyO3/pyo3/issues/4326
    //
    // workaround, put them on the structs themselves, which works but has the
    // disadvantage of being not next to the method def itself
    ($(#[$meta:meta])* $pytype:ident, $rstype:path, $name:expr) => {
        $(#[$meta])*
        #[pyclass(name = $name, eq)]
        #[derive(Clone, From, Into, PartialEq)]
        pub struct $pytype($rstype);
    };
}

impl_new_core! {
    core::CoreTEXT2_0,
    core::CoreDataset2_0,
    core::CoreTEXT2_0::try_new_2_0
}

impl_new_core! {
    core::CoreTEXT3_0,
    core::CoreDataset3_0,
    core::CoreTEXT3_0::try_new_3_0
}

impl_new_core! {
    core::CoreTEXT3_1,
    core::CoreDataset3_1,
    core::CoreTEXT3_1::try_new_3_1
}

impl_new_core! {
    core::CoreTEXT3_2,
    core::CoreDataset3_2,
    core::CoreTEXT3_2::try_new_3_2
}

// TODO nonstandard_keywords could be "enforced" by storing the prefix somehow
impl_new_meas!("FCS2.0", false);
impl_new_meas!("FCS3.0", false);
impl_new_meas!("FCS3.1", false);
impl_new_meas!("FCS3.2", false);

impl_new_meas!("FCS2.0", true);
impl_new_meas!("FCS3.0", true);
impl_new_meas!("FCS3.1", true);
impl_new_meas!("FCS3.2", true);

#[pymethods]
impl PyTemporal3_2 {
    #[getter]
    fn get_measurement_type(&self) -> bool {
        self.0.specific.measurement_type.0.is_some()
    }

    #[setter]
    fn set_measurement_type(&mut self, x: bool) {
        self.0.specific.measurement_type = if x { Some(kws::TemporalType) } else { None }.into();
    }
}

// impl_convert_version! {PyCoreTEXT2_0}
// impl_convert_version! {PyCoreTEXT3_0}
// impl_convert_version! {PyCoreTEXT3_1}
// impl_convert_version! {PyCoreTEXT3_2}
// impl_convert_version! {PyCoreDataset2_0}
// impl_convert_version! {PyCoreDataset3_0}
// impl_convert_version! {PyCoreDataset3_1}
// impl_convert_version! {PyCoreDataset3_2}

// Get/set methods for all versions
macro_rules! impl_common {
    ($pytype:ident) => {
        // impl_get_set_metaroot! {Option<kws::Abrt>, $pytype}
        // impl_get_set_metaroot! {Option<kws::Cells>, $pytype}
        // impl_get_set_metaroot! {Option<kws::Com>, $pytype}
        // impl_get_set_metaroot! {Option<kws::Exp>, $pytype}
        // impl_get_set_metaroot! {Option<kws::Fil>, $pytype}
        // impl_get_set_metaroot! {Option<kws::Inst>, $pytype}
        // impl_get_set_metaroot! {Option<kws::Lost>, $pytype}
        // impl_get_set_metaroot! {Option<kws::Op>, $pytype}
        // impl_get_set_metaroot! {Option<kws::Proj>, $pytype}
        // impl_get_set_metaroot! {Option<kws::Smno>, $pytype}
        // impl_get_set_metaroot! {Option<kws::Src>, $pytype}
        // impl_get_set_metaroot! {Option<kws::Sys>, $pytype}

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
            // /// Insert a nonstandard key.
            // ///
            // /// :param str key: Key to insert. Must not start with *$*.
            // /// :param str value: Value to insert.
            // ///
            // /// :return: Previous value for ``key`` if it exists.
            // /// :rtype: str | None
            // fn insert_nonstandard(&mut self, key: NonStdKey, value: String) -> Option<String> {
            //     self.0.metaroot.nonstandard_keywords.insert(key, value)
            // }

            // /// Remove a nonstandard key.
            // ///
            // /// :param str key: Key to remove. Must not start with *$*.
            // ///
            // /// :return: Value for ``key`` if it exists.
            // /// :rtype: str | None
            // fn remove_nonstandard(&mut self, key: NonStdKey) -> Option<String> {
            //     self.0.metaroot.nonstandard_keywords.remove(&key)
            // }

            // /// Look up a nonstandard key.
            // ///
            // /// :param str key: Key to find. Must not start with *$*.
            // ///
            // /// :return: Value for ``key`` if it exists.
            // /// :rtype: str | None
            // fn get_nonstandard(&mut self, key: NonStdKey) -> Option<String> {
            //     self.0.metaroot.nonstandard_keywords.get(&key).cloned()
            // }

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

impl_get_set_meas_obj_common!(
    PyCoreTEXT2_0,
    PyCoreDataset2_0,
    Option<Shortname>,
    MaybeFamily
);
impl_get_set_meas_obj_common!(
    PyCoreTEXT3_0,
    PyCoreDataset3_0,
    Option<Shortname>,
    MaybeFamily
);
impl_get_set_meas_obj_common!(PyCoreTEXT3_1, PyCoreDataset3_1, Shortname, AlwaysFamily);
impl_get_set_meas_obj_common!(PyCoreTEXT3_2, PyCoreDataset3_2, Shortname, AlwaysFamily);

// gating for 2.0
macro_rules! impl_get_set_applied_gates_2_0 {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
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

// impl_get_set_applied_gates_2_0!(PyCoreTEXT2_0);
// impl_get_set_applied_gates_2_0!(PyCoreDataset2_0);

// gating for 3.0/3.1
macro_rules! impl_get_set_applied_gates_3_0 {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
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

// impl_get_set_applied_gates_3_0!(PyCoreTEXT3_0);
// impl_get_set_applied_gates_3_0!(PyCoreDataset3_0);
// impl_get_set_applied_gates_3_0!(PyCoreTEXT3_1);
// impl_get_set_applied_gates_3_0!(PyCoreDataset3_1);

macro_rules! impl_get_set_applied_gates_3_2 {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
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

// impl_get_set_applied_gates_3_2!(PyCoreTEXT3_2);
// impl_get_set_applied_gates_3_2!(PyCoreDataset3_2);

// Get/set methods for $LAST_MODIFIER/$LAST_MODIFIED/$ORIGINALITY (3.1-3.2)
macro_rules! impl_modification_attrs {
    ($pytype:ident) => {
        impl_get_set_metaroot!(Option<kws::Originality>, $pytype);
        impl_get_set_metaroot!(Option<kws::LastModified>, "LAST_MODIFIED", $pytype);
        impl_get_set_metaroot!(Option<kws::LastModifier>, "LAST_MODIFIER", $pytype);
    };
}

// impl_modification_attrs!(PyCoreTEXT3_1);
// impl_modification_attrs!(PyCoreTEXT3_2);
// impl_modification_attrs!(PyCoreDataset3_1);
// impl_modification_attrs!(PyCoreDataset3_2);

// Get/set methods for $CARRIERID/$CARRIERTYPE/$LOCATIONID (3.2)
macro_rules! impl_carrier_attrs {
    ($pytype:ident) => {
        impl_get_set_metaroot!(Option<kws::Carriertype>, $pytype);
        impl_get_set_metaroot!(Option<kws::Carrierid>, $pytype);
        impl_get_set_metaroot!(Option<kws::Locationid>, $pytype);
    };
}

// impl_carrier_attrs!(PyCoreTEXT3_2);
// impl_carrier_attrs!(PyCoreDataset3_2);

// Get/set methods for $PLATEID/$WELLID/$PLATENAME (3.1-3.2)
macro_rules! impl_plate_attrs {
    ($pytype:ident) => {
        impl_get_set_metaroot!(Option<kws::Wellid>, $pytype);
        impl_get_set_metaroot!(Option<kws::Plateid>, $pytype);
        impl_get_set_metaroot!(Option<kws::Platename>, $pytype);
    };
}

// impl_plate_attrs!(PyCoreTEXT3_1);
// impl_plate_attrs!(PyCoreTEXT3_2);
// impl_plate_attrs!(PyCoreDataset3_1);
// impl_plate_attrs!(PyCoreDataset3_2);

macro_rules! impl_get_set_subset {
    ($pytype:ident) => {
        impl_get_set_metaroot!(Option<kws::CSTot>, $pytype);
        impl_get_set_metaroot!(Option<kws::CSVBits>, $pytype);
        impl_get_set_metaroot!(Option<core::CSVFlags>, $pytype);
    };
}

// impl_get_set_subset!(PyCoreTEXT3_0);
// impl_get_set_subset!(PyCoreTEXT3_1);
// impl_get_set_subset!(PyCoreDataset3_0);
// impl_get_set_subset!(PyCoreDataset3_1);

// impl_get_set_metaroot! {
//     Option<kws::Unicode>,
//     PyCoreTEXT3_0,
//     PyCoreDataset3_0
// }

// impl_get_set_metaroot! {
//     Option<kws::Vol>,
//     PyCoreTEXT3_1,
//     PyCoreTEXT3_2,
//     PyCoreDataset3_1,
//     PyCoreDataset3_2
// }

// // Get/set methods for (optional) $CYT (2.0-3.1)
// //
// // 3.2 is required which is why it is not included here
// impl_get_set_metaroot! {
//     Option<kws::Cyt>,
//     PyCoreTEXT2_0,
//     PyCoreTEXT3_0,
//     PyCoreTEXT3_1,
//     PyCoreDataset2_0,
//     PyCoreDataset3_0,
//     PyCoreDataset3_1
// }

// Get/set methods for $FLOWRATE (3.2)
// impl_get_set_metaroot! {
//     Option<kws::Flowrate>,
//     PyCoreTEXT3_2,
//     PyCoreDataset3_2
// }

// Get/set methods for $CYTSN (3.0-3.2)
// impl_get_set_metaroot! {
//     Option<kws::Cytsn>,
//     PyCoreTEXT3_0,
//     PyCoreTEXT3_1,
//     PyCoreTEXT3_2,
//     PyCoreDataset3_0,
//     PyCoreDataset3_1,
//     PyCoreDataset3_2
// }

// // // Get/set methods for $CYT (required) (3.2)
// // impl_get_set_metaroot! {kws::Cyt, PyCoreTEXT3_2, PyCoreDataset3_2}

// // Get/set methods for $UNSTAINEDINFO (3.2)
// impl_get_set_metaroot! {
//     Option<kws::UnstainedInfo>,
//     PyCoreTEXT3_2,
//     PyCoreDataset3_2
// }

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

// TODO move all this stuff to proc
macro_rules! impl_meas_get_set_common {
    ($pytype:ident) => {
        impl_meas_get_set! {Option<kws::Longname>, "S", "str", $pytype}
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
        impl_meas_get_set! {Option<kws::Filter>, "F", "str", $pytype}
        impl_meas_get_set! {Option<kws::DetectorType>, "T", "str", $pytype}
        impl_meas_get_set! {Option<kws::PercentEmitted>, "P", "str", $pytype}
        impl_meas_get_set! {Option<kws::DetectorVoltage>, "V", "float", $pytype}
        impl_meas_get_set! {Option<kws::Power>, "O", "float", $pytype}
    };
}

impl_optical_get_set!(PyOptical2_0);
impl_optical_get_set!(PyOptical3_0);
impl_optical_get_set!(PyOptical3_1);
impl_optical_get_set!(PyOptical3_2);

// $PnL (2.0/3.0)
impl_meas_get_set! {
    Option<kws::Wavelength>,
    "L",
    "float",
    PyOptical2_0,
    PyOptical3_0
}

// #PnL (3.1-3.2)
impl_meas_get_set! {
    Option<kws::Wavelengths>,
    "L",
    "list[float]",
    PyOptical3_1,
    PyOptical3_2
}

// $PnCalibration (3.1)
impl_meas_get_set! {
    Option<kws::Calibration3_1>,
    "CALIBRATION",
    "tuple[float, str]",
    PyOptical3_1
}

// $PnD (3.1-3.2)
impl_meas_get_set! {
    Option<kws::Display>,
    "D",
    "tuple[bool, float, float]",
    PyOptical3_1,
    PyOptical3_2,
    PyTemporal3_1,
    PyTemporal3_2
}

// $PnDET (3.2)
impl_meas_get_set! {Option<kws::DetectorName>, "DET", "str", PyOptical3_2}

// $PnTAG (3.2)
impl_meas_get_set! {Option<kws::Tag>, "TAG", "str", PyOptical3_2}

// $PnTYPE (3.2)
impl_meas_get_set! {
    Option<kws::OpticalType>,
    "TYPE",
    "str",
    PyOptical3_2
}

// $PnFEATURE (3.2)
impl_meas_get_set! {
    Option<kws::Feature>,
    "FEATURE",
    "Literal[\"Area\", \"Width\", \"Height\"]",
    PyOptical3_2
}

// $PnANALYTE (3.2)
impl_meas_get_set! {
    Option<kws::Analyte>,
    "ANALYTE",
    "str",
    PyOptical3_2
}

// $PnCalibration (3.2)
impl_meas_get_set! {
    Option<kws::Calibration3_2>,
    "CALIBRATION",
    "tuple[float, float, str]",
    PyOptical3_2
}

#[derive(From, Into, Default)]
struct PyAppliedGates2_0(AppliedGates2_0);

#[derive(From, Into, Default)]
struct PyAppliedGates3_0(AppliedGates3_0);

#[derive(From, Into, Default)]
struct PyAppliedGates3_2(AppliedGates3_2);

impl<'py> FromPyObject<'py> for PyAppliedGates2_0 {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let (gated_measurements, regions, gating): (
            PyGatedMeasurements,
            PyRegionMapping<PyRegion2_0>,
            Option<kws::Gating>,
        ) = ob.extract()?;
        let scheme = GatingScheme::try_new(gating, regions.into())?;
        Ok(AppliedGates2_0::try_new(gated_measurements.into(), scheme)?.into())
    }
}

impl<'py> IntoPyObject<'py> for PyAppliedGates2_0 {
    type Target = PyTuple;
    type Output = Bound<'py, PyTuple>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let (gms, rs, g) = self.0.split();
        (
            PyGatedMeasurements::from(gms),
            PyRegionMapping::<PyRegion2_0>::from(rs),
            g,
        )
            .into_pyobject(py)
    }
}

impl<'py> FromPyObject<'py> for PyAppliedGates3_0 {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let (gated_measurements, regions, gating): (
            PyGatedMeasurements,
            PyRegionMapping<PyRegion3_0>,
            Option<kws::Gating>,
        ) = ob.extract()?;
        let scheme = GatingScheme::try_new(gating, regions.into())?;
        Ok(AppliedGates3_0::try_new(gated_measurements.into(), scheme)?.into())
    }
}

impl<'py> IntoPyObject<'py> for PyAppliedGates3_0 {
    type Target = PyTuple;
    type Output = Bound<'py, PyTuple>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let (gms, rs, g) = self.0.split();
        (
            PyGatedMeasurements::from(gms),
            PyRegionMapping::<PyRegion3_0>::from(rs),
            g,
        )
            .into_pyobject(py)
    }
}

impl<'py> FromPyObject<'py> for PyAppliedGates3_2 {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let (regions, gating): (PyRegionMapping<PyRegion3_2>, Option<kws::Gating>) =
            ob.extract()?;
        Ok(AppliedGates3_2::try_new(gating, regions.into())?.into())
    }
}

impl<'py> IntoPyObject<'py> for PyAppliedGates3_2 {
    type Target = PyTuple;
    type Output = Bound<'py, PyTuple>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let (rs, g) = self.0.split();
        (PyRegionMapping::<PyRegion3_2>::from(rs), g).into_pyobject(py)
    }
}

// TODO could clean this stuff up by moving to gating module and implementing
// py conversion b/t regions to these things. Would avoid having to unpack
// and repack a hashmap
py_wrap! {
    /// Make a new FCS 2.0-compatible univariate region.
    ///
    /// :param int index: The index corresponding to a gating measurement
    ///     (the *m* in the *$Gm\** keywords).
    ///
    /// :param gate: The lower and upper bounds of the gate.
    /// :type gate: (float, float)
    PyUnivariateRegion2_0,
    UnivariateRegion<GateIndex>,
    "UnivariateRegion2_0"
}

#[pymethods]
impl PyUnivariateRegion2_0 {
    #[new]
    fn new(index: GateIndex, gate: kws::UniGate) -> Self {
        UnivariateRegion { index, gate }.into()
    }

    /// The value of the index (read-only).
    ///
    /// :rtype: int
    #[getter]
    fn index(&self) -> GateIndex {
        self.0.index
    }
}

py_wrap! {
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
    PyUnivariateRegion3_0,
    UnivariateRegion<kws::MeasOrGateIndex>,
    "UnivariateRegion3_0"
}

#[pymethods]
impl PyUnivariateRegion3_0 {
    #[new]
    fn new(index: kws::MeasOrGateIndex, gate: kws::UniGate) -> Self {
        UnivariateRegion { index, gate }.into()
    }

    /// The value of the index (read-only).
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

py_wrap! {
    /// Make a new FCS 3.2-compatible univariate region.
    ///
    /// :param int index: The index corresponding to either a physical
    ///     measurement (the *n* in the *$Pn\** keywords).
    ///
    /// :param gate: The lower and upper bounds of the gate.
    /// :type gate: (float, float)
    PyUnivariateRegion3_2,
    UnivariateRegion<kws::PrefixedMeasIndex>,
    "UnivariateRegion3_2"
}

#[pymethods]
impl PyUnivariateRegion3_2 {
    #[new]
    fn new(index: kws::PrefixedMeasIndex, gate: kws::UniGate) -> Self {
        UnivariateRegion { index, gate }.into()
    }

    /// The value of the index (read-only).
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
            /// The lower and upper bounds of the gate (read-only).
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

py_wrap! {
    /// Make a new FCS 2.0-compatible univariate region.
    ///
    /// :param index: The x/y indices corresponding to gating measurements
    ///     (the *m* in the *$Gm\** keywords).
    /// :type index: (int, int)
    ///
    /// :param gate: The vertices of a polygon gate. Must not be empty.
    /// :type gate: list[(float, float)]
    PyBivariateRegion2_0,
    BivariateRegion<GateIndex>,
    "BivariateRegion2_0"
}

#[pymethods]
impl PyBivariateRegion2_0 {
    #[new]
    fn new(index: kws::IndexPair<GateIndex>, vertices: FCSNonEmpty<kws::Vertex>) -> Self {
        BivariateRegion { index, vertices }.into()
    }

    /// The value of the x/y indices (read-only).
    ///
    /// :rtype: (int, int)
    #[getter]
    fn index(&self) -> kws::IndexPair<GateIndex> {
        self.0.index
    }
}

py_wrap! {
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
    PyBivariateRegion3_0,
    BivariateRegion<kws::MeasOrGateIndex>,
    "BivariateRegion3_0"
}

#[pymethods]
impl PyBivariateRegion3_0 {
    #[new]
    fn new(
        index: kws::IndexPair<kws::MeasOrGateIndex>,
        vertices: FCSNonEmpty<kws::Vertex>,
    ) -> Self {
        BivariateRegion { index, vertices }.into()
    }

    /// The value of the x/y indices (read-only).
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

py_wrap!(
    /// Make a new FCS 3.2-compatible univariate region.
    ///
    /// :param index: The x/y indices corresponding to physical measurements
    ///     (the *n* in the *$Pn\** keywords).
    /// :type index: (int, int)
    ///
    /// :param gate: The vertices of a polygon gate. Must not be empty.
    /// :type gate: list[(float, float)]
    PyBivariateRegion3_2,
    BivariateRegion<kws::PrefixedMeasIndex>,
    "BivariateRegion3_2"
);

#[pymethods]
impl PyBivariateRegion3_2 {
    #[new]
    fn new(
        index: kws::IndexPair<kws::PrefixedMeasIndex>,
        vertices: FCSNonEmpty<kws::Vertex>,
    ) -> Self {
        BivariateRegion { index, vertices }.into()
    }

    /// The value of the x/y indices (read-only).
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
            /// The vertices of a polygon gate (read-only).
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

struct PyEithers<K: MightHave, U, V>(Eithers<K, U, V>);

impl<'py, K, U, V> FromPyObject<'py> for PyEithers<K, U, V>
where
    K: MightHave,
    K::Wrapper<Shortname>: FromPyObject<'py>,
    U: FromPyObject<'py>,
    V: FromPyObject<'py>,
{
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let xs: Eithers<K, U, V> = ob.extract()?;
        Ok(Self(xs))
    }
}

impl<K, U, V, X, Y> From<PyEithers<K, U, V>> for Eithers<K, X, Y>
where
    K: MightHave,
    X: From<U>,
    Y: From<V>,
{
    fn from(value: PyEithers<K, U, V>) -> Self {
        value.0.inner_into()
    }
}

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

impl_gated_meas!();

#[derive(FromPyObject, IntoPyObject)]
struct PyGatedMeasurements(Vec<PyGatedMeasurement>);

impl From<PyGatedMeasurements> for Vec<GatedMeasurement> {
    fn from(value: PyGatedMeasurements) -> Vec<GatedMeasurement> {
        value.0.into_iter().map(|x| x.0).collect()
    }
}

impl From<Vec<GatedMeasurement>> for PyGatedMeasurements {
    fn from(value: Vec<GatedMeasurement>) -> PyGatedMeasurements {
        Self(value.into_iter().map(|x| x.into()).collect())
    }
}

type AsciiDelim = FixedAsciiLayout<KnownTot, NoMeasDatatype, false>;
py_wrap! {
    /// A fixed-width ASCII layout.
    ///
    /// :param chars: The number of characters for each measurement. Equivalent
    ///     to the value of *$PnB*. Must be a number between 1 and 20
    ///     (inclusive).
    /// :type chars: list[int].
    PyAsciiFixedLayout,
    AsciiDelim,
    "AsciiFixedLayout"
}

#[pymethods]
impl PyAsciiFixedLayout {
    #[new]
    fn new(chars: Vec<u64>) -> Self {
        FixedLayout::new_ascii_u64(chars).into()
    }

    /// The number of chars for each measurement (read-only).
    ///
    /// This corresponds to the value of *$PnB* for each measurement.
    ///
    /// :rtype: list[int]
    #[getter]
    fn char_widths(&self) -> Vec<u32> {
        self.0
            .widths()
            .into_iter()
            .map(|x| u32::from(u8::from(x)))
            .collect()
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

type AsciiFixed = DelimAsciiLayout<KnownTot, NoMeasDatatype, false>;
py_wrap! {
    /// A delimited ASCII layout.
    ///
    /// :param ranges: The range for each measurement. Equivalent to the *$PnR*
    ///     keyword. This is not used internally and thus only represents
    ///     documentation at the user level.
    /// :type ranges: list[int]
    PyAsciiDelimLayout,
    AsciiFixed,
    "AsciiDelimLayout"
}

#[pymethods]
impl PyAsciiDelimLayout {
    #[new]
    fn new(ranges: Vec<u64>) -> Self {
        DelimAsciiLayout::new(ranges).into()
    }
}

impl_new_ordered_layout!(1);
impl_new_ordered_layout!(2);
impl_new_ordered_layout!(3);
impl_new_ordered_layout!(4);
impl_new_ordered_layout!(5);
impl_new_ordered_layout!(6);
impl_new_ordered_layout!(7);
impl_new_ordered_layout!(8);

macro_rules! impl_layout_new_ordered_float {
    ($pytype:ident, $range:path, $name:expr, $size:expr, $summary:expr) => {
        py_wrap! {
            #[doc = $summary]
            ///
            /// :param ranges: The range for each measurement. Corresponds to
            ///     *$PnR*. This is not used internally so only serves for
            ///     users' own purposes.
            /// :type ranges: list[float]
            ///
            /// :param bool is_big: If ``True`` use big endian for encoding
            ///     values, otherwise use little endian.
            $pytype,
            OrderedLayout<$range, KnownTot>,
            $name
        }

        #[pymethods]
        impl $pytype {
            #[new]
            #[pyo3(signature = (ranges, is_big = false))]
            fn new(ranges: Vec<$range>, is_big: bool) -> Self {
                FixedLayout::new_endian_float(ranges, is_big.into()).into()
            }

            #[doc = $summary]
            ///
            /// :param ranges: The range for each measurement. Corresponds to
            ///     *$PnR*. This is not used internally so only serves for
            ///     users' own purposes.
            /// :type ranges: list[float]
            ///
            /// :param byteord: The byte order to use when encoding values.
            ///     Must be a list of indices starting at 0.
            /// :type byteord: list[int]
            #[classmethod]
            fn new_ordered(
                _: &Bound<'_, PyType>,
                ranges: Vec<$range>,
                byteord: SizedByteOrd<$size>,
            ) -> Self {
                FixedLayout::new(ranges, byteord).into()
            }
        }
    };
}

impl_layout_new_ordered_float!(
    PyOrderedF32Layout,
    F32Range,
    "OrderedF32Layout",
    4,
    "A 32-bit ordered float layout."
);

impl_layout_new_ordered_float!(
    PyOrderedF64Layout,
    F64Range,
    "OrderedF64Layout",
    8,
    "A 64-bit ordered float layout."
);

macro_rules! impl_layout_new_endian_float {
    ($pytype:ident, $range:path, $name:expr, $summary:expr) => {
        py_wrap! {
            #[doc = $summary]
            ///
            /// :param ranges: The range for each measurement. Corresponds to
            ///     *$PnR*. This is not used internally so only serves for
            ///     users' own purposes.
            /// :type ranges: list[float]
            ///
            /// :param bool is_big: If ``True`` use big endian for encoding
            ///     values, otherwise use little endian.
            $pytype,
            EndianLayout<$range, NoMeasDatatype>,
            $name
        }

        #[pymethods]
        impl $pytype {
            #[new]
            #[pyo3(signature = (ranges, is_big = false))]
            fn new(ranges: Vec<$range>, is_big: bool) -> Self {
                FixedLayout::new(ranges, is_big.into()).into()
            }
        }
    };
}

impl_layout_new_endian_float!(
    PyEndianF32Layout,
    F32Range,
    "EndianF32Layout",
    "A 32-bit endian float layout"
);

impl_layout_new_endian_float!(
    PyEndianF64Layout,
    F64Range,
    "EndianF64Layout",
    "A 64-bit endian float layout"
);

py_wrap! {
    /// A mixed-width integer layout.
    ///
    /// :param ranges: The range of each measurement. Corresponds to the *$PnR*
    ///     keyword less one. The number of bytes used to encode each
    ///     measurement (*$PnB*) will be the minimum required to express this
    ///     value. For instance, a value of ``1024`` will set *$PnB* to ``16``
    ///     and the values in this measurement will be encoded as 16-bit
    ///     integer. The values of a measurement will be less than or equal to
    ///     this value.
    /// :type ranges: list[int]
    ///
    /// :param bool is_big: If ``True`` use big endian for encoding values,
    ///     otherwise use little endian.
    PyEndianUintLayout,
    EndianLayout<AnyNullBitmask, NoMeasDatatype>,
    "EndianUintLayout"
}

#[pymethods]
impl PyEndianUintLayout {
    #[new]
    #[pyo3(signature = (ranges, is_big = false))]
    fn new(ranges: Vec<u64>, is_big: bool) -> Self {
        let rs = ranges.into_iter().map(AnyNullBitmask::from_u64).collect();
        FixedLayout::new(rs, is_big.into()).into()
    }
}

py_wrap! {
    /// A mixed-type layout.
    ///
    /// :param types: The type and range for each measurement. These are given
    ///     as 2-tuples like ``(<flag>, <range>)`` where ``flag`` is one of
    ///     ``"A"``, ``"I"``, ``"F"``, or ``"D"`` corresponding to Ascii,
    ///     Integer, Float, or Double datatypes respectively. The ``range``
    ///     field should be an ``int`` for ``"A"`` or ``"I"`` and a ``float``
    ///     for ``"F"`` or ``"D"``.
    /// :type types: list[tuple[Literal["A", "I"], int] | tuple[Literal["F", "D"], float]]
    ///
    /// :param bool is_big: If ``True`` use big endian for encoding values,
    ///     otherwise use little endian.
    PyMixedLayout,
    MixedLayout,
    "MixedLayout"
}

#[pymethods]
impl PyMixedLayout {
    #[new]
    #[pyo3(signature = (types, is_big = false))]
    fn new(types: Vec<NullMixedType>, is_big: bool) -> Self {
        FixedLayout::new(types, is_big.into()).into()
    }

    /// The datatypes for each measurement (read-only).
    ///
    /// When given, this will be *$PnDATATYPE*, otherwise *$DATATYPE*.
    ///
    /// :rtype: list[Literal["A", "I", "F", "D"]]
    #[getter]
    fn datatypes(&self) -> Vec<kws::AlphaNumType> {
        self.0.datatypes()
    }
}

// TODO these should be ints or floats depending on layout
macro_rules! impl_layout_ranges {
    ($t:ident) => {
        #[pymethods]
        impl $t {
            /// The value of *$PnR* for each measurement (read-only).
            ///
            /// :rtype: list[float]
            #[getter]
            fn ranges(&self) -> Vec<kws::Range> {
                self.0.ranges().into()
            }
        }
    };
}

impl_layout_ranges!(PyAsciiFixedLayout);
impl_layout_ranges!(PyAsciiDelimLayout);
impl_layout_ranges!(PyOrderedUint08Layout);
impl_layout_ranges!(PyOrderedUint16Layout);
impl_layout_ranges!(PyOrderedUint24Layout);
impl_layout_ranges!(PyOrderedUint32Layout);
impl_layout_ranges!(PyOrderedUint40Layout);
impl_layout_ranges!(PyOrderedUint48Layout);
impl_layout_ranges!(PyOrderedUint56Layout);
impl_layout_ranges!(PyOrderedUint64Layout);
impl_layout_ranges!(PyOrderedF32Layout);
impl_layout_ranges!(PyOrderedF64Layout);
impl_layout_ranges!(PyEndianF32Layout);
impl_layout_ranges!(PyEndianF64Layout);
impl_layout_ranges!(PyEndianUintLayout);
impl_layout_ranges!(PyMixedLayout);

macro_rules! impl_layout_datatype {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            /// The value of *$DATATYPE* (read-only).
            ///
            /// :rtype: Literal["A", "I", "F", "D"]
            #[getter]
            fn datatype(&self) -> kws::AlphaNumType {
                self.0.datatype().into()
            }
        }
    };
}

impl_layout_datatype!(PyAsciiFixedLayout);
impl_layout_datatype!(PyAsciiDelimLayout);
impl_layout_datatype!(PyOrderedUint08Layout);
impl_layout_datatype!(PyOrderedUint16Layout);
impl_layout_datatype!(PyOrderedUint24Layout);
impl_layout_datatype!(PyOrderedUint32Layout);
impl_layout_datatype!(PyOrderedUint40Layout);
impl_layout_datatype!(PyOrderedUint48Layout);
impl_layout_datatype!(PyOrderedUint56Layout);
impl_layout_datatype!(PyOrderedUint64Layout);
impl_layout_datatype!(PyOrderedF32Layout);
impl_layout_datatype!(PyOrderedF64Layout);
impl_layout_datatype!(PyEndianF32Layout);
impl_layout_datatype!(PyEndianF64Layout);
impl_layout_datatype!(PyEndianUintLayout);

macro_rules! impl_layout_byteord {
    ($t:ident) => {
        #[pymethods]
        impl $t {
            /// The value of *$BYTEORD* (read-only).
            ///
            /// This will be a list of indices starting at 0 (rather than 1
            /// as is written in an FCS file).
            ///
            /// :rtype: list[int]
            #[getter]
            fn byteord(&self) -> Vec<NonZeroU8> {
                self.0.byte_order().as_vec()
            }

            /// The endianness if applicable (read-only).
            ///
            /// Return ``True`` for big endian, ``False`` for little endian,
            /// and ``None`` for mixed.
            ///
            /// :rtype: bool | None
            #[getter]
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
            /// The value of *$BYTEORD* (read-only).
            ///
            /// Return ``True`` for big endian (``4,3,2,1``), ``False`` for
            /// little endian (``1,2,3,4``).
            ///
            /// :rtype: bool
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

// Many layouts have the same width for each column, so this is just a simple
// const method which will return that width (in bits)
macro_rules! impl_layout_bytes_fixed {
    ($t:ident, $width:expr, $doc:expr) => {
        #[pymethods]
        impl $t {
            /// The width of each measurement in bytes (read-only).
            ///
            #[doc = $doc]
            ///
            /// This corresponds to the value of *$PnB* divided by 8, which are
            /// all the same for this layout.
            ///
            /// :rtype: int
            #[getter]
            fn byte_width(&self) -> u32 {
                $width
            }
        }
    };
}

impl_layout_bytes_fixed!(PyOrderedUint08Layout, 1, "Will always return 1.");
impl_layout_bytes_fixed!(PyOrderedUint16Layout, 2, "Will always return 2.");
impl_layout_bytes_fixed!(PyOrderedUint24Layout, 3, "Will always return 3.");
impl_layout_bytes_fixed!(PyOrderedUint32Layout, 4, "Will always return 4.");
impl_layout_bytes_fixed!(PyOrderedUint40Layout, 5, "Will always return 5.");
impl_layout_bytes_fixed!(PyOrderedUint48Layout, 6, "Will always return 6.");
impl_layout_bytes_fixed!(PyOrderedUint56Layout, 7, "Will always return 7.");
impl_layout_bytes_fixed!(PyOrderedUint64Layout, 8, "Will always return 8.");
impl_layout_bytes_fixed!(PyOrderedF32Layout, 4, "Will always return 4.");
impl_layout_bytes_fixed!(PyOrderedF64Layout, 8, "Will always return 8.");
impl_layout_bytes_fixed!(PyEndianF32Layout, 4, "Will always return 4.");
impl_layout_bytes_fixed!(PyEndianF64Layout, 8, "Will always return 8.");

macro_rules! impl_layout_bytes_variable {
    ($t:ident) => {
        #[pymethods]
        impl $t {
            /// The width of each measurement in bytes (read-only).
            ///
            /// This corresponds to the value of *$PnB* for each measurement
            /// divided by 8. Values for each measurement may be different.
            ///
            /// :rtype: list[int]
            #[getter]
            fn byte_widths(&self) -> Vec<u32> {
                self.0
                    .widths()
                    .into_iter()
                    .map(|x| u32::from(u8::from(x)))
                    .collect()
            }
        }
    };
}

impl_layout_bytes_variable!(PyEndianUintLayout);
impl_layout_bytes_variable!(PyMixedLayout);

// pub(crate) struct PyNonEmpty<T>(pub(crate) NonEmpty<T>);

// impl<'py, T: FromPyObject<'py>> FromPyObject<'py> for PyNonEmpty<T> {
//     fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
//         let xs: Vec<_> = ob.extract()?;
//         NonEmpty::from_vec(xs)
//             .ok_or(PyValueError::new_err("list must not be empty"))
//             .map(Self)
//     }
// }

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
