//! Python interface for pyreflow
//!
//! Just turn back now, this is almost pure macro-insanity.
//!
//! This is utter nonsense due to a variety of design goals and constraints:
//!
//! * the interface is inherently repetitive as we have multiple versions
//! * the parts that are not repetitive are only slightly different
//! * even if the code is the same, the docstrings are often slightly different
//! * the docstrings should conform to PEP-8 (72 chars wide, structure, etc)
//! * the docstrings should render nicely with sphinx
//! * many methods have defaults, which need to be added with pyo3 signatures
//! * many native rust types are generic, which means they need to be newtype-ed
//! * docstrings can't be put on __new__ (yet)
//!
//! The only way to get all this is to use proc-macros for just about everything.
//! The main bottleneck is the docstrings, which can't be manipulated well using
//! dec-macros (nevermind the formatting and line-wrapping needed for PEP-8).
//!
//! Also, many classes should be created with instance vars. These need to be
//! kept in sync in multiple places since the code is defined using #[getter]
//! and #[setter] in pyo3 methods but the docstrings for these are defined
//! on the struct definition (not the __new__ method, not that that would help)
//! and the arguments for the constructor often take defaults which needs a
//! signature. To keep this all in sync, we need a proc macro that defines
//! "constructors" comprehensively, including the newtype struct, its docstring,
//! the __new__ method, its signature, and the get/set methods for instance
//! attributes.
//!
//! Totally reasonable ;)
//!
//! Other methods are less insane, but still need docstring formatting and are
//! highly repetitive (many are defined for each FCS version).
//!
//! In order to make this slightly more sane, some conventions:
//!
//! * Constructors are implemented with "impl_new_*" macros. These define
//!   __new__ and any getters/setters needed for instance variables. They also
//!   make a newtype wrapper for a generic rust type in most cases. These are
//!   the really nasty macros since they are yuuuuuuuuge.
//! * Other proc macros are "small"; besides the "constructor" macros, this
//!   often means 1-3 methods defined per invocation. This makes debugging
//!   easier since a single macro invocation will light up if there is one error
//!   anywhere inside it. Keeping the "inside" small make this triage easier.
//! * Getters and setters are paired together
//! * Non-constructor macros simply take one argument for the Python-rust type
//!   and defined methods on that type. Sometimes this will "magically" read
//!   the version and define slightly different methods given the version.
//!   This is unavoidable if we want to keep the code small (ish). The tradeoff
//!   is that it's easy to see which macros are being applied to each type/class
//!   and it is easy to bundle them in case multiple types use it.
//! * Docstring rendering is handled entirely internal to the proc macros. This
//!   is reasonable since the docstrings only matter for the python interface
//!   and can't cause compile errors. This is also almost-necessary since the
//!   internal proc-macro code has rendering logic for sphinx rst syntax, which
//!   would be a pain to keep in sync at the macro call level.
use fireflow_core::api;
use fireflow_core::config as cfg;
use fireflow_core::core;
use fireflow_core::data::{self, LayoutByteOrder as _, LayoutDatatype as _, PhantomInto as _};
use fireflow_core::header;
use fireflow_core::match_map_uint;
use fireflow_core::meas;
use fireflow_core::text::byteord::{ArrayByteOrd, Endian};
use fireflow_core::text::gating::{self, Region};
use fireflow_core::text::index::{GateIndex, RegionIndex};
use fireflow_core::text::keywords as kws;
use fireflow_core::text::named_vec::Element;
use fireflow_core::validated::dataframe::{
    AnyPrimitiveSeries, PrimitiveDataFrame, PrimitiveSeries,
};
use fireflow_core::validated::header_segments;
use fireflow_core::validated::keys;
use fireflow_core::validated::shortname as sn;

use fireflow_python_proc as fpp;

use fireflow_types::keywords as ftk;
use fireflow_types::python::EventDataError;
use type_families::{BifunctorOnce as _, Functor as _};

use derive_more::{From, Into};
use polars::prelude as pl;
use polars_arrow::array::{Array, PrimitiveArray};
use polars_arrow::datatypes::ArrowDataType;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use pyo3_polars::{PyDataFrame, PySeries};

use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::num::NonZeroU8;

fpp::def_fcs_read_header!(api::fcs_read_header);
fpp::def_fcs_read_flat_text!(api::fcs_read_flat_text, api::fcs_read_flat_texts);
fpp::def_fcs_read_std_text!(api::fcs_read_std_text, api::fcs_read_std_texts);
fpp::def_fcs_read_flat_dataset!(
    api::fcs_read_flat_dataset,
    api::fcs_read_flat_datasets,
    api::fcs_summarize
);
fpp::def_fcs_read_std_dataset!(api::fcs_read_std_dataset, api::fcs_read_std_datasets);
fpp::def_fcs_read_flat_dataset_with_keywords!(api::fcs_read_flat_dataset_with_keywords);
fpp::def_fcs_write_datasets!(api::fcs_write_datasets);

fpp::impl_config_defaults!(cfg::ReadHeaderConfig);
fpp::impl_config_defaults!(cfg::ReadFlatTEXTConfig);
fpp::impl_config_defaults!(cfg::ReadStdTEXTConfig);
fpp::impl_config_defaults!(cfg::ReadFlatDatasetConfig);
fpp::impl_config_defaults!(cfg::ReadStdDatasetConfig);
fpp::impl_config_defaults!(cfg::ReadFlatDatasetFromKeywordsConfig);
fpp::impl_config_defaults!(cfg::NewCoreTEXTConfig);
fpp::impl_config_defaults!(cfg::NewCoreDatasetConfig);

fpp::impl_py_header!(header::Header);
fpp::impl_py_header_segments!(header_segments::ParsedHeaderSegments);
fpp::impl_py_uncorrected_header_segments!(header::UncorrectedHeaderSegments);
fpp::impl_py_valid_keywords!(keys::ValidKeywords);
fpp::impl_py_std_diagnostics!(core::StdTEXTDiagnostics);
fpp::impl_py_dataset_segments!(core::DatasetSegments);

fpp::impl_py_flat_text_output!(api::FlatTEXTOutput);
fpp::impl_py_header_supp!(api::HeaderAndSuppOffsets);
fpp::impl_py_flat_dataset_output!(api::FlatDatasetOutput);
fpp::impl_py_flat_text_diagnostics!(api::FlatTEXTDiagnostics);
fpp::impl_py_split_text_diagnostics!(api::SplitTEXTDiagnostics);
fpp::impl_py_flat_dataset_with_kws_output!(api::FlatDatasetFromKwsOutput);
fpp::impl_py_new_flat_dataset_with_kws_output!(api::NewFlatDatasetFromKwsOutput);
fpp::impl_py_read_events_diagnostics!(data::EventsDiagnostics);
fpp::impl_py_keyword_version_score!(kws::KeywordVersionScore);

fpp::impl_py_std_text_output!(api::StdTEXTOutput);
fpp::impl_py_std_dataset_output!(api::StdDatasetOutput);
fpp::impl_py_std_dataset_with_kws_output!(core::StdDatasetFromKwsOutput);
fpp::impl_py_new_std_dataset_with_kws_output!(core::NewStdDatasetFromKwsOutput);

fpp::impl_py_dataset_summary!(api::DatasetSummary);

// Implement python classes for core* structs
//
// Will actually make classes called PyCoreTEXT* and PyCoreDataset* which
// can be referred as such elsewhere
//
// This will include the __new__ methods and all attributes corresponding to
// "instance variables" supplied to __new__
fpp::impl_new_core!(core::CoreTEXT2_0, core::CoreDataset2_0);
fpp::impl_new_core!(core::CoreTEXT3_0, core::CoreDataset3_0);
fpp::impl_new_core!(core::CoreTEXT3_1, core::CoreDataset3_1);
fpp::impl_new_core!(core::CoreTEXT3_2, core::CoreDataset3_2);

// Implement python classes for Optical* structs (as PyOptical*)
//
// This will include the __new__ methods and all attributes corresponding to
// "instance variables" supplied to __new__
fpp::impl_new_meas!(meas::Optical2_0);
fpp::impl_new_meas!(meas::Optical3_0);
fpp::impl_new_meas!(meas::Optical3_1);
fpp::impl_new_meas!(meas::Optical3_2);

// Implement $PnFEATURE (area/width/height) get/set for 3.2
fpp::impl_meas_awh_pnfeature!(PyOptical3_2);

// Implement python classes for Temporal* structs (as PyTemporal*)
//
// This will include the __new__ methods and all attributes corresponding to
// "instance variables" supplied to __new__
fpp::impl_new_meas!(meas::Temporal2_0);
fpp::impl_new_meas!(meas::Temporal3_0);
fpp::impl_new_meas!(meas::Temporal3_1);
fpp::impl_new_meas!(meas::Temporal3_2);

// Common methods for all Core* versions. Some of these macros will implement a
// slightly different method depending on version.
macro_rules! impl_common {
    ($pytype:ident) => {
        // get FCS version as read-only value
        fpp::impl_core_version!($pytype);

        // get $PAR as read-only value
        fpp::impl_core_par!($pytype);

        // method to set $TR threshold without changing its reference
        fpp::impl_core_set_tr_threshold!($pytype);

        // method to write HEADER+TEXT to file
        fpp::impl_core_write_text!($pytype);

        // $Shortnames attribute; for 2.0/3.0, this will not allow setting any to None
        fpp::impl_core_all_shortnames_attr!($pytype);

        // method to rename temporal measurement if it exists
        fpp::impl_core_rename_temporal!($pytype);

        // methods to set any measurement to temporal (using index or name)
        fpp::impl_core_set_temporal!($pytype);

        // method to convert temporal measurement to optical if it exists; these
        // are slightly different for each version
        fpp::impl_core_unset_temporal!($pytype);

        // method to get/set unnamed measurements
        fpp::impl_core_get_measurements!($pytype);

        // method to set all measurements; this cannot be combined with
        // impl_core_get_measurements! because this method takes arguments
        fpp::impl_core_set_named_measurements!($pytype);

        // method to get one measurement by index
        fpp::impl_core_get_measurement!($pytype);

        // method to get one measurement by name
        fpp::impl_core_get_named_measurement!($pytype);

        // method to get temporal measurement if it exists
        fpp::impl_core_get_temporal!($pytype);

        // method to set all measurements and data_schema at once
        fpp::impl_core_set_measurements_and_data_schema!($pytype);

        // methods to add optical or temporal measurement at last index
        fpp::impl_core_push_measurement!($pytype);

        // methods to add optical or temporal measurement at arbitrary index
        fpp::impl_core_insert_measurement!($pytype);

        // method to replace temporal measurement by index or name; slightly
        // different for each version since later versions are fallable
        fpp::impl_core_replace_temporal!($pytype);

        // method to replace optical measurement by index or name
        fpp::impl_core_replace_optical!($pytype);

        // method to replace measurement by index or name
        fpp::impl_core_remove_measurement!($pytype);

        // methods to convert this class to to a different version; actually
        // implements one method for each version that isn't this one
        fpp::impl_core_to_version_x_y!($pytype);

        // attribute for all $PnS keywords
        fpp::impl_core_all_pns!($pytype);

        // attribute for all $PnF keywords
        fpp::impl_core_all_pnf!($pytype);

        // attribute for all $PnO keywords
        fpp::impl_core_all_pno!($pytype);

        // attribute for all $PnP keywords
        fpp::impl_core_all_pnp!($pytype);

        // attribute for all $PnT keywords
        fpp::impl_core_all_pnt!($pytype);

        // attribute for all $PnV keywords
        fpp::impl_core_all_pnv!($pytype);

        // attribute for all scaling keywords ($PnE or $PnG if present);
        // 3.0 and later will return gain and scale combined
        fpp::impl_core_all_transforms_attr!($pytype);

        // attribute to get/set nonstandard keywords for all measurements
        fpp::impl_core_all_meas_nonstandard_keywords!($pytype);

        // method to return all standard keywords as read-only dict
        fpp::impl_core_standard_keywords!($pytype);
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

// impl from_kws for all CoreTEXT*
fpp::impl_coretext_from_kws!(core::CoreTEXT2_0);
fpp::impl_coretext_from_kws!(core::CoreTEXT3_0);
fpp::impl_coretext_from_kws!(core::CoreTEXT3_1);
fpp::impl_coretext_from_kws!(core::CoreTEXT3_2);

// impl from_kws for all CoreTEXT*
fpp::impl_coredataset_from_kws!(core::CoreDataset2_0);
fpp::impl_coredataset_from_kws!(core::CoreDataset3_0);
fpp::impl_coredataset_from_kws!(core::CoreDataset3_1);
fpp::impl_coredataset_from_kws!(core::CoreDataset3_2);

// impl write_multitext for all CoreTEXT*
fpp::impl_coretext_write_multi!(core::CoreTEXT2_0);
fpp::impl_coretext_write_multi!(core::CoreTEXT3_0);
fpp::impl_coretext_write_multi!(core::CoreTEXT3_1);
fpp::impl_coretext_write_multi!(core::CoreTEXT3_2);

// Common methods for all CoreTEXT* versions.
macro_rules! impl_coretext_common {
    ($pytype:ident) => {
        fpp::impl_coretext_to_dataset!($pytype);
        fpp::impl_coretext_unset_measurements!($pytype);
    };
}

impl_coretext_common!(PyCoreTEXT2_0);
impl_coretext_common!(PyCoreTEXT3_0);
impl_coretext_common!(PyCoreTEXT3_1);
impl_coretext_common!(PyCoreTEXT3_2);

// Common methods for all CoreDataset* versions.
macro_rules! impl_coredataset_common {
    ($pytype:ident) => {
        fpp::impl_coredataset_set_named_measurements_and_data!($pytype);
        fpp::impl_coredataset_set_measurements_data_schema_and_data!($pytype);
        fpp::impl_core_write_dataset!($pytype);
        fpp::impl_coredataset_unset_data!($pytype);
        fpp::impl_coredataset_check_ranges!($pytype);
    };
}

impl_coredataset_common!(PyCoreDataset2_0);
impl_coredataset_common!(PyCoreDataset3_0);
impl_coredataset_common!(PyCoreDataset3_1);
impl_coredataset_common!(PyCoreDataset3_2);

// methods to get/set timestep; this is not an attribute because the
// setter method returns something
fpp::impl_core_get_set_timestep!(PyCoreTEXT3_0);
fpp::impl_core_get_set_timestep!(PyCoreTEXT3_1);
fpp::impl_core_get_set_timestep!(PyCoreTEXT3_2);
fpp::impl_core_get_set_timestep!(PyCoreDataset3_0);
fpp::impl_core_get_set_timestep!(PyCoreDataset3_1);
fpp::impl_core_get_set_timestep!(PyCoreDataset3_2);

// Get/set $Shortnames for 2.0 and 3.0 where this field is optional
fpp::impl_core_all_shortnames_maybe_attr!(PyCoreTEXT2_0);
fpp::impl_core_all_shortnames_maybe_attr!(PyCoreTEXT3_0);
fpp::impl_core_all_shortnames_maybe_attr!(PyCoreDataset2_0);
fpp::impl_core_all_shortnames_maybe_attr!(PyCoreDataset3_0);

// Get/set methods for $PKn (2.0-3.1)
fpp::impl_core_all_pkn!(PyCoreTEXT2_0);
fpp::impl_core_all_pkn!(PyCoreTEXT3_0);
fpp::impl_core_all_pkn!(PyCoreTEXT3_1);
fpp::impl_core_all_pkn!(PyCoreDataset2_0);
fpp::impl_core_all_pkn!(PyCoreDataset3_0);
fpp::impl_core_all_pkn!(PyCoreDataset3_1);

// Get/set methods for $PKNn (2.0-3.1)
fpp::impl_core_all_pknn!(PyCoreTEXT2_0);
fpp::impl_core_all_pknn!(PyCoreTEXT3_0);
fpp::impl_core_all_pknn!(PyCoreTEXT3_1);
fpp::impl_core_all_pknn!(PyCoreDataset2_0);
fpp::impl_core_all_pknn!(PyCoreDataset3_0);
fpp::impl_core_all_pknn!(PyCoreDataset3_1);

// Get/set methods for scaler $PnL (2.0-3.0)
fpp::impl_core_all_pnl_old!(PyCoreTEXT2_0);
fpp::impl_core_all_pnl_old!(PyCoreTEXT3_0);
fpp::impl_core_all_pnl_old!(PyCoreDataset2_0);
fpp::impl_core_all_pnl_old!(PyCoreDataset3_0);

// Get/set methods for vector $PnL (3.1-3.2)
fpp::impl_core_all_pnl_new!(PyCoreTEXT3_1);
fpp::impl_core_all_pnl_new!(PyCoreTEXT3_2);
fpp::impl_core_all_pnl_new!(PyCoreDataset3_1);
fpp::impl_core_all_pnl_new!(PyCoreDataset3_2);

// Get/set methods for $PnD (3.1+)
//
// This is valid for the time channel so don't set on just optical
fpp::impl_core_all_pnd!(PyCoreTEXT3_1);
fpp::impl_core_all_pnd!(PyCoreDataset3_1);
fpp::impl_core_all_pnd!(PyCoreTEXT3_2);
fpp::impl_core_all_pnd!(PyCoreDataset3_2);

// Get/set methods for $PnDET (3.2)
fpp::impl_core_all_pndet!(PyCoreTEXT3_2);
fpp::impl_core_all_pndet!(PyCoreDataset3_2);

// Get/set methods for $PnCALIBRATION (3.1)
fpp::impl_core_all_pncal3_1!(PyCoreTEXT3_1);
fpp::impl_core_all_pncal3_1!(PyCoreDataset3_1);

// Get/set methods for $PnCALIBRATION (3.2)
fpp::impl_core_all_pncal3_2!(PyCoreTEXT3_2);
fpp::impl_core_all_pncal3_2!(PyCoreDataset3_2);

// Get/set methods for $PnTAG (3.2)
fpp::impl_core_all_pntag!(PyCoreTEXT3_2);
fpp::impl_core_all_pntag!(PyCoreDataset3_2);

// Get/set methods for $PnTYPE (3.2)
fpp::impl_core_all_pntype!(PyCoreTEXT3_2);
fpp::impl_core_all_pntype!(PyCoreDataset3_2);

// Get/set methods for $PnFEATURE (3.2)
fpp::impl_core_all_pnfeature!(PyCoreTEXT3_2);
fpp::impl_core_all_pnfeature!(PyCoreDataset3_2);

// Get/set methods for area/width/height $PnFEATURE (3.2)
fpp::impl_core_all_awh_pnfeature!(PyCoreTEXT3_2);
fpp::impl_core_all_awh_pnfeature!(PyCoreDataset3_2);

// Get/set methods for non-area/width/height $PnFEATURE (3.2)
fpp::impl_core_get_all_other_pnfeature!(PyCoreTEXT3_2);
fpp::impl_core_get_all_other_pnfeature!(PyCoreDataset3_2);

// Get/set methods for $PnANALYTE (3.2)
fpp::impl_core_all_pnanalyte!(PyCoreTEXT3_2);
fpp::impl_core_all_pnanalyte!(PyCoreDataset3_2);

#[derive(From, Into, Default)]
struct PyAppliedGates2_0(gating::AppliedGates2_0);

#[derive(From, Into, Default)]
struct PyAppliedGates3_0(gating::AppliedGates3_0);

#[derive(From, Into, Default)]
struct PyAppliedGates3_2(gating::AppliedGates3_2);

impl<'py> FromPyObject<'py> for PyAppliedGates2_0 {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let (gated_measurements, regions, gating): (
            PyGatedMeasurements,
            PyRegionMapping<PyRegion2_0>,
            Option<kws::Gating>,
        ) = ob.extract()?;
        let scheme = gating::GatingScheme::try_new(gating, regions.into())?;
        Ok(gating::AppliedGates2_0::try_new(gated_measurements.into(), scheme)?.into())
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
        let scheme = gating::GatingScheme::try_new(gating, regions.into())?;
        Ok(gating::AppliedGates3_0::try_new(Vec::from(gated_measurements), scheme)?.into())
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
        Ok(gating::AppliedGates3_2::try_new(gating, regions.into())?.into())
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

// Implement __new__ and attributes for PyUnivariate2_0
fpp::impl_new_gate_uni_regions!(gating::UnivariateRegion<GateIndex>);

// Implement __new__ and attributes for PyUnivariate3_0
fpp::impl_new_gate_uni_regions!(gating::UnivariateRegion<kws::MeasOrGateIndex>);

// Implement __new__ and attributes for PyUnivariate3_2
fpp::impl_new_gate_uni_regions!(gating::UnivariateRegion<kws::PrefixedMeasIndex>);

// Implement __new__ and attributes for PyBivariate2_0
fpp::impl_new_gate_bi_regions!(gating::BivariateRegion<GateIndex>);

// Implement __new__ and attributes for PyBivariate3_0
fpp::impl_new_gate_bi_regions!(gating::BivariateRegion<kws::MeasOrGateIndex>);

// Implement __new__ and attributes for PyBivariate3_2
fpp::impl_new_gate_bi_regions!(gating::BivariateRegion<kws::PrefixedMeasIndex>);

type MeasElements<K, U, V, S> = Vec<Element<(sn::Shortname, U), (K, V, S)>>;

struct PyEithers<K, U, V, S>(MeasElements<K, U, V, S>);

impl<'py, K, U, V, S> FromPyObject<'py> for PyEithers<K, U, V, S>
where
    V: FromPyObject<'py>,
    U: FromPyObject<'py>,
    S: FromPyObject<'py>,
    K: FromPyObject<'py>,
{
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let ret = ob.extract()?;
        Ok(Self(ret))
    }
}

impl<K, U, V, S, Uf, Vf> From<PyEithers<K, U, V, S>> for MeasElements<K, Uf, Vf, S>
where
    U: Into<Uf>,
    V: Into<Vf>,
{
    fn from(value: PyEithers<K, U, V, S>) -> Self {
        value.0.fmap(|x| {
            x.first_once(|(k, v)| (k, v.into()))
                .second_once(|(k, v, s)| (k, v.into(), s))
        })
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
    gating::UnivariateRegion<I>: From<U>,
    gating::BivariateRegion<I>: From<B>,
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
    U: From<gating::UnivariateRegion<I>>,
    B: From<gating::BivariateRegion<I>>,
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

impl<I, R, S> From<PyRegionMapping<R>> for HashMap<RegionIndex, Region<I>, S>
where
    Region<I>: From<R>,
    S: BuildHasher + Default,
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

fpp::impl_gated_meas!(gating::GatedMeasurement);

#[derive(FromPyObject, IntoPyObject)]
struct PyGatedMeasurements(Vec<PyGatedMeasurement>);

impl From<PyGatedMeasurements> for Vec<gating::GatedMeasurement> {
    fn from(value: PyGatedMeasurements) -> Self {
        value.0.into_iter().map(|x| x.0).collect()
    }
}

impl From<Vec<gating::GatedMeasurement>> for PyGatedMeasurements {
    fn from(value: Vec<gating::GatedMeasurement>) -> Self {
        Self(value.into_iter().map(Into::into).collect())
    }
}

// These are dummy markers for use inside python objects. They have no meaning
// in the python API so this is arbitrary.
type ColumnMarkers_ = data::ColumnMarkers<(), ()>;

type FixedAsciiDataSchema_ = data::FixedAsciiDataSchema<false, ColumnMarkers_>;

type DelimAsciiDataSchema_ = data::DelimAsciiDataSchema<false, ColumnMarkers_>;

// Implement __new__ and attributes for PyFixedAsciiDataSchema
fpp::impl_new_fixed_ascii_data_schema!("FixedAsciiDataSchema", FixedAsciiDataSchema_);

// Implement __new__ and attributes for PyDelimAsciiDataSchema
fpp::impl_new_delim_ascii_data_schema!("DelimAsciiDataSchema", DelimAsciiDataSchema_);

// TODO these can probably be combined

// Implement __new__ and attributes for all PyOrderedF*DataSchema structs
fpp::impl_new_ordered_float_data_schema!("OrderedF32DataSchema", data::OrderedF32DataSchema<()>, 4);
fpp::impl_new_ordered_float_data_schema!("OrderedF64DataSchema", data::OrderedF64DataSchema<()>, 8);

// Implement __new__ and attributes for all PyBigLittleF*DataSchema structs
fpp::impl_new_endian_float_data_schema!(
    "BigLittleF32DataSchema",
    data::BigLittleF32DataSchema<()>,
    4
);

fpp::impl_new_endian_float_data_schema!(
    "BigLittleF64DataSchema",
    data::BigLittleF64DataSchema<()>,
    8
);

// Implement __new__ and attributes for PyOrderedUintDataSchema
fpp::impl_new_ordered_uint_data_schema!(
    "OrderedUintDataSchema",
    data::AnyOrderedUintDataSchema<()>
);

// Implement __new__ and attributes for PySingleUintDataSchema
fpp::impl_new_single_uint_data_schema!("SingleUintDataSchema", data::AnySingleUintDataSchema<()>);

// TODO update docs to reflect new range parameters

// Implement __new__ and attributes for PyVariableUintDataSchema
fpp::impl_new_variable_uint_data_schema!(
    "VariableUintDataSchema",
    data::VariableUintDataSchema<()>
);

// Implement __new__ and attributes for PyMixedDataSchema
fpp::impl_new_mixed_data_schema!("MixedDataSchema", data::MixedDataSchema);

// Implement method to return the byte widths of variable-widths data_schema
fpp::impl_data_schema_byte_widths!(PyVariableUintDataSchema);
fpp::impl_data_schema_byte_widths!(PyMixedDataSchema);

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
    fn from(value: core::AnyCoreTEXT) -> Self {
        match value {
            core::AnyCoreTEXT::FCS2_0(x) => (*x).into(),
            core::AnyCoreTEXT::FCS3_0(x) => (*x).into(),
            core::AnyCoreTEXT::FCS3_1(x) => (*x).into(),
            core::AnyCoreTEXT::FCS3_2(x) => (*x).into(),
        }
    }
}

#[derive(FromPyObject, IntoPyObject, From)]
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
    fn from(value: core::AnyCoreDataset) -> Self {
        match value {
            core::AnyCoreDataset::FCS2_0(x) => (*x).into(),
            core::AnyCoreDataset::FCS3_0(x) => (*x).into(),
            core::AnyCoreDataset::FCS3_1(x) => (*x).into(),
            core::AnyCoreDataset::FCS3_2(x) => (*x).into(),
        }
    }
}

impl From<PyAnyCoreDataset> for core::AnyCoreDataset {
    fn from(value: PyAnyCoreDataset) -> Self {
        match value {
            PyAnyCoreDataset::FCS2_0(x) => x.0.into(),
            PyAnyCoreDataset::FCS3_0(x) => x.0.into(),
            PyAnyCoreDataset::FCS3_1(x) => x.0.into(),
            PyAnyCoreDataset::FCS3_2(x) => x.0.into(),
        }
    }
}

/// All data_schema used for 2.0/3.0 in Python.
#[derive(FromPyObject, IntoPyObject)]
pub enum PyOrderedDataSchema {
    AsciiFixed(PyFixedAsciiDataSchema),
    AsciiDelim(PyDelimAsciiDataSchema),
    Uint(PyOrderedUintDataSchema),
    F32(PyOrderedF32DataSchema),
    F64(PyOrderedF64DataSchema),
}

/// All data_schema used for 3.1 in Python.
#[derive(FromPyObject, IntoPyObject, From)]
pub enum PyNonMixedDataSchema {
    #[from(PyFixedAsciiDataSchema, FixedAsciiDataSchema_)]
    AsciiFixed(PyFixedAsciiDataSchema),

    #[from(PyDelimAsciiDataSchema, DelimAsciiDataSchema_)]
    AsciiDelim(PyDelimAsciiDataSchema),

    #[from(PyVariableUintDataSchema, data::VariableUintDataSchema<()>)]
    VariableUint(PyVariableUintDataSchema),

    #[from(PySingleUintDataSchema, data::AnySingleUintDataSchema<()>)]
    SingleUint(PySingleUintDataSchema),

    #[from(PyBigLittleF32DataSchema, data::BigLittleF32DataSchema<()>)]
    F32(PyBigLittleF32DataSchema),

    #[from(PyBigLittleF64DataSchema, data::BigLittleF64DataSchema<()>)]
    F64(PyBigLittleF64DataSchema),
}

/// All data_schema used for 3.2 in Python.
#[derive(FromPyObject, IntoPyObject, From)]
pub enum PyDataSchema3_2 {
    NonMixed(PyNonMixedDataSchema),
    Mixed(PyMixedDataSchema),
}

impl From<PyOrderedDataSchema> for data::DataSchema2_0 {
    fn from(value: PyOrderedDataSchema) -> Self {
        data::DataSchema3_0::from(value).phantom_into()
    }
}

impl From<data::DataSchema2_0> for PyOrderedDataSchema {
    fn from(value: data::DataSchema2_0) -> Self {
        let d: data::DataSchema3_0 = value.phantom_into();
        d.into()
    }
}

impl From<PyOrderedDataSchema> for data::DataSchema3_0 {
    fn from(value: PyOrderedDataSchema) -> Self {
        match value {
            PyOrderedDataSchema::AsciiFixed(x) => data::AnyAsciiDataSchema::from(x.0)
                .phantom_into()
                .byte_layout_into()
                .into(),
            PyOrderedDataSchema::AsciiDelim(x) => data::AnyAsciiDataSchema::from(x.0)
                .phantom_into()
                .byte_layout_into()
                .into(),
            PyOrderedDataSchema::Uint(x) => x.0.phantom_into().into(),
            PyOrderedDataSchema::F32(x) => x.0.phantom_into().into(),
            PyOrderedDataSchema::F64(x) => x.0.phantom_into().into(),
        }
    }
}

impl From<data::DataSchema3_0> for PyOrderedDataSchema {
    fn from(value: data::DataSchema3_0) -> Self {
        match value {
            data::AnyDatatype::Ascii(x) => match x.phantom_into() {
                data::AnyAsciiDataSchema::Delimited(y) => {
                    Self::AsciiDelim(y.byte_layout_into().into())
                }
                data::AnyAsciiDataSchema::Fixed(y) => {
                    Self::AsciiFixed(y.byte_layout_into().phantom_into().into())
                }
            },
            data::AnyDatatype::Uint(x) => Self::Uint(x.phantom_into().into()),
            data::AnyDatatype::F32(x) => Self::F32(x.phantom_into().into()),
            data::AnyDatatype::F64(x) => Self::F64(x.phantom_into().into()),
        }
    }
}

impl From<data::DataSchema3_1> for PyNonMixedDataSchema {
    fn from(value: data::DataSchema3_1) -> Self {
        match value {
            data::AnyDatatype::Ascii(x) => match x {
                data::AnyAsciiDataSchema::Fixed(y) => y.phantom_into().into(),
                data::AnyAsciiDataSchema::Delimited(y) => y.phantom_into().into(),
            },
            data::AnyDatatype::Uint(x) => match x {
                data::AnyBigLittleUintDataSchema::Single(y) => y.phantom_into().into(),
                data::AnyBigLittleUintDataSchema::Multi(y) => y.phantom_into().into(),
            },
            data::AnyDatatype::F32(x) => x.phantom_into().into(),
            data::AnyDatatype::F64(x) => x.phantom_into().into(),
        }
    }
}

impl From<PyNonMixedDataSchema> for data::DataSchema3_1 {
    fn from(value: PyNonMixedDataSchema) -> Self {
        match value {
            PyNonMixedDataSchema::AsciiFixed(x) => Self::Ascii(x.0.phantom_into().into()),
            PyNonMixedDataSchema::AsciiDelim(x) => Self::Ascii(x.0.phantom_into().into()),
            PyNonMixedDataSchema::SingleUint(x) => {
                Self::Uint(data::AnyBigLittleUintDataSchema::Single(x.0.phantom_into()))
            }
            PyNonMixedDataSchema::VariableUint(x) => {
                Self::Uint(data::AnyBigLittleUintDataSchema::Multi(x.0.phantom_into()))
            }
            PyNonMixedDataSchema::F32(x) => Self::F32(x.0.phantom_into()),
            PyNonMixedDataSchema::F64(x) => Self::F64(x.0.phantom_into()),
        }
    }
}

impl From<PyDataSchema3_2> for data::DataSchema3_2 {
    fn from(value: PyDataSchema3_2) -> Self {
        match value {
            PyDataSchema3_2::Mixed(x) => Self::Mixed(x.into()),
            PyDataSchema3_2::NonMixed(x) => {
                Self::NonMixed(data::NonMixedDataSchema::from(x).phantom_into())
            }
        }
    }
}

impl From<data::DataSchema3_2> for PyDataSchema3_2 {
    fn from(value: data::DataSchema3_2) -> Self {
        match value {
            data::DataSchema3_2::Mixed(x) => Self::Mixed(x.into()),
            data::DataSchema3_2::NonMixed(x) => Self::NonMixed(x.phantom_into().into()),
        }
    }
}

/// Any byte order that can be used in a 2.0/3.0 layout with a given size.
///
/// Meant for arguments to functions.
pub enum PyByteOrder<const LEN: usize> {
    Endian(Endian),
    Ordered(ArrayByteOrd<LEN>),
}

impl<const LEN: usize> Default for PyByteOrder<LEN> {
    fn default() -> Self {
        Self::Endian(Endian::default())
    }
}

impl<const LEN: usize> From<ArrayByteOrd<LEN>> for PyByteOrder<LEN> {
    fn from(value: ArrayByteOrd<LEN>) -> Self {
        if let Some(e) = value.as_endian() {
            Self::Endian(e)
        } else {
            Self::Ordered(value)
        }
    }
}

impl<const LEN: usize> From<PyByteOrder<LEN>> for ArrayByteOrd<LEN> {
    fn from(value: PyByteOrder<LEN>) -> Self {
        match value {
            PyByteOrder::Endian(e) => e.into(),
            PyByteOrder::Ordered(o) => o,
        }
    }
}

impl<'py, const LEN: usize> FromPyObject<'py> for PyByteOrder<LEN>
where
    Vec<NonZeroU8>: TryInto<ArrayByteOrd<LEN>>,
{
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(e) = ob.extract::<Endian>() {
            Ok(Self::Endian(e))
        } else if let Some(o) = ob
            .extract::<Vec<NonZeroU8>>()
            .ok()
            .and_then(|xs| xs.try_into().ok())
        {
            Ok(Self::Ordered(o))
        } else {
            let msg = format!(
                "must be '{}', '{}', or a list",
                ftk::BYTEORD_LITTLE,
                ftk::BYTEORD_BIG
            );
            Err(PyValueError::new_err(msg))
        }
    }
}

impl<'py, const LEN: usize> IntoPyObject<'py> for PyByteOrder<LEN> {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self {
            Self::Endian(e) => {
                let Ok(ret) = e.into_pyobject(py);
                Ok(ret.into_any())
            }
            Self::Ordered(o) => {
                let xs: [NonZeroU8; LEN] = o.into();
                // use u32 here since Vec<u8> converts to bytes in python
                let ret: Vec<_> = xs.into_iter().map(|x| u32::from(u8::from(x))).collect();
                ret.into_pyobject(py)
            }
        }
    }
}

// Wrappers for FCSDataframe and AnyFCSColumn which allow conversion to/from
// proper polars types which are also wrapped as Python types. This is confusing
// because we actually have 4 different types for df and column.
// 1. FCS* type which is validated for only a few datatypes FCS supports
// 2. Native polars type
// 3. Pyo3 type which wraps the native polars type
// 4. PyFCS* which are wrappers for (1) which are also valid Pyo3 types
//
// This is also the ordering of conversions that must be followed in order to
// go from Rust to Python and back. Note that the FromPyObject and IntoPyObject
// methods are utilized in the Pyo3 types (3).
//
// Going from 2 -> 1 and 3 -> 4 requires validation because polars dataframes
// can hold many more datatypes than what FCS supports. (4) itself is merely a
// wrapper for 1 to evade the orphan rule. If the orphan rule didn't exist, we
// could implement FromPyObject and IntoPyObject on (1) directly and avoid much
// of this confusion.
//
// However, this would require keeping all this machinery in fireflow-core,
// which means this crate also must depend on polars, which slows down build
// times because polars is massive.

#[derive(From, Into)]
pub struct PyFCSDataFrame(PrimitiveDataFrame);

#[derive(From, Into)]
pub struct PyAnyFCSColumn(AnyPrimitiveSeries);

#[derive(From, Into)]
pub struct PyVariableUintSeries(data::VariableUintSeries);

impl<'py> IntoPyObject<'py> for PyFCSDataFrame {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let columns = self
            .0
            .iter()
            .enumerate()
            .map(|(i, c)| {
                pl::Series::from_arrow(pl::PlSmallStr::from(format!("X{i}")), as_array(c))
                    .unwrap()
                    .into()
            })
            .collect();
        // ASSUME this will not fail because all columns should have unique
        // names and the same length
        PyDataFrame(pl::DataFrame::new(columns).unwrap()).into_pyobject(py)
    }
}

impl<'py> IntoPyObject<'py> for PyAnyFCSColumn {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let ser =
            pl::Series::from_arrow(pl::PlSmallStr::from("unnamed"), as_array(&self.0)).unwrap();
        PySeries(ser).into_pyobject(py)
    }
}

impl<'py> FromPyObject<'py> for PyFCSDataFrame {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PyDataFrame>()?.try_into()?)
    }
}

impl<'py> FromPyObject<'py> for PyAnyFCSColumn {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(ob.extract::<PySeries>()?.try_into()?)
    }
}

impl TryFrom<PyDataFrame> for PyFCSDataFrame {
    type Error = SeriesToColumnError;

    fn try_from(df: PyDataFrame) -> Result<Self, Self::Error> {
        let cs =
            df.0.column_iter()
                .map(|c| PySeries(c.as_materialized_series().clone()))
                .map(PyAnyFCSColumn::try_from)
                .collect::<Result<Vec<_>, _>>()?;
        // ASSUME this won't fail because all columns will have the same
        // length after pulling from a valid polars dataframe
        Ok(Self(PrimitiveDataFrame::new_unchecked(
            cs.into_iter().map(|c| c.0),
        )))
    }
}

impl TryFrom<PySeries> for PyAnyFCSColumn {
    type Error = SeriesToColumnError;

    fn try_from(pyser: PySeries) -> Result<Self, Self::Error> {
        fn column_to_buf<T>(ser: pl::Series) -> Result<PyAnyFCSColumn, SeriesToColumnError>
        where
            T: pl::NumericNative,
            AnyPrimitiveSeries: From<PrimitiveSeries<T>>,
        {
            if ser.null_count() > 0 {
                Err(SeriesToColumnError::HasNull(ser.name().clone()))
            } else {
                let chunks = ser.into_chunks();
                // ASSUME this will never fail because
                // FromPyObject<PySeries> will call rechunk. See
                // https://github.com/pola-rs/polars/blob/f91c3a865aaea6dc92cad7bc75572f2c9dd23ac9/pyo3-polars/pyo3-polars/src/types.rs#L177
                debug_assert!(chunks.len() == 1, "Series has more than one chunk");
                let buf = chunks[0]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<T>>()
                    .unwrap()
                    .values()
                    .clone();
                Ok(PyAnyFCSColumn(AnyPrimitiveSeries::from(PrimitiveSeries(
                    buf,
                ))))
            }
        }

        let ser = pyser.0;
        match ser.dtype() {
            pl::DataType::UInt8 => column_to_buf::<u8>(ser),
            pl::DataType::UInt16 => column_to_buf::<u16>(ser),
            pl::DataType::UInt32 => column_to_buf::<u32>(ser),
            pl::DataType::UInt64 => column_to_buf::<u64>(ser),
            pl::DataType::Float32 => column_to_buf::<f32>(ser),
            pl::DataType::Float64 => column_to_buf::<f64>(ser),
            t => Err(SeriesToColumnError::InvalidDatatype(
                ser.name().clone(),
                t.clone(),
            )),
        }
    }
}

impl PyAnyFCSColumn {
    fn with_range(self, range: data::FullRange) -> data::DecimalRangeAndSeries {
        (range, self.0)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn with_bitmask_range(
        self,
        range: data::MaybeTypedVariableBitmask,
    ) -> PyResult<data::MaybeTypedVariableUintSeries> {
        match range {
            data::MaybeTypedRange::Untyped(r) => Ok(data::MaybeTypedRange::Untyped((r, self.0))),
            data::MaybeTypedRange::Typed(r) => {
                let c = match_map_uint!(r, x, data::Series::from_prim(x, self.0)?);
                Ok(data::MaybeTypedRange::Typed(c))
            }
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn with_mixed_range(
        self,
        range: data::MaybeTypedMixedRange,
    ) -> PyResult<data::MaybeTypedMixedSeries> {
        match range {
            data::MaybeTypedRange::Untyped(r) => Ok(data::MaybeTypedRange::Untyped((r, self.0))),
            data::MaybeTypedRange::Typed(r) => {
                let c = match r {
                    data::AnyDatatype::Ascii(x) => {
                        data::AnyDatatype::Ascii(data::Series::from_prim(x, self.0)?)
                    }
                    data::AnyDatatype::Uint(x) => {
                        let z = match_map_uint!(x, y, data::Series::from_prim(y, self.0)?);
                        data::AnyDatatype::Uint(z)
                    }
                    data::AnyDatatype::F32(x) => {
                        data::AnyDatatype::F32(data::Series::from_prim(x, self.0)?)
                    }
                    data::AnyDatatype::F64(x) => {
                        data::AnyDatatype::F64(data::Series::from_prim(x, self.0)?)
                    }
                };
                Ok(data::MaybeTypedRange::Typed(c))
            }
        }
    }
}

pub enum SeriesToColumnError {
    InvalidDatatype(pl::PlSmallStr, pl::DataType),
    HasNull(pl::PlSmallStr),
}

impl From<SeriesToColumnError> for PyErr {
    fn from(value: SeriesToColumnError) -> Self {
        let s = match value {
            SeriesToColumnError::InvalidDatatype(n, t) => {
                format!("Datatype must be u8/16/32/64 or f32/64, got {t} for series '{n}'")
            }
            SeriesToColumnError::HasNull(n) => {
                format!("Series {n} contains null values which are not allowed")
            }
        };
        EventDataError::new_err(s)
    }
}

fn as_array(c: &AnyPrimitiveSeries) -> Box<dyn Array> {
    match c.clone() {
        AnyPrimitiveSeries::U08(xs) => {
            Box::new(PrimitiveArray::new(ArrowDataType::UInt8, xs.0, None))
        }
        AnyPrimitiveSeries::U16(xs) => {
            Box::new(PrimitiveArray::new(ArrowDataType::UInt16, xs.0, None))
        }
        AnyPrimitiveSeries::U32(xs) => {
            Box::new(PrimitiveArray::new(ArrowDataType::UInt32, xs.0, None))
        }
        AnyPrimitiveSeries::U64(xs) => {
            Box::new(PrimitiveArray::new(ArrowDataType::UInt64, xs.0, None))
        }
        AnyPrimitiveSeries::F32(xs) => {
            Box::new(PrimitiveArray::new(ArrowDataType::Float32, xs.0, None))
        }
        AnyPrimitiveSeries::F64(xs) => {
            Box::new(PrimitiveArray::new(ArrowDataType::Float64, xs.0, None))
        }
    }
}

impl PyFCSDataFrame {
    // this is confusing because it is the one instance where we want to return
    // a Pyo3 type directly from a python function vs a PyFCSDataFrame. The
    // reason is that we want to encode the names in the dataframe, and the only
    // way to do that is to have a function that takes names since FCSDataFrame
    // does not store then itself.
    fn as_polars_dataframe(&self, names: &[sn::Shortname]) -> pl::DataFrame {
        fn as_polars_column(c: &AnyPrimitiveSeries, name: &sn::Shortname) -> pl::Column {
            // ASSUME this will not fail because the we know that any of the 6
            // allowed types will be valid columns and we don't add a NULL array
            // when making the array
            pl::Series::from_arrow(AsRef::<str>::as_ref(&name).into(), as_array(c))
                .unwrap()
                .into()
        }
        debug_assert!(
            names.len() == self.0.ncols(),
            "names is not same length as column number"
        );
        debug_assert!(
            names.iter().collect::<HashSet<_>>().len() == names.len(),
            "Names are not unique"
        );
        let columns = self
            .0
            .iter()
            .zip(names)
            .map(|(c, n)| as_polars_column(c, n))
            .collect();
        // ASSUME this will not fail because all columns should have unique
        // names and the same length
        pl::DataFrame::new(columns).unwrap()
    }
}
