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
//!   the version and defined slightly different methods given the version.
//!   This is unavoidable is we want to keep the code small (ish). The tradeoff
//!   os that it's easy to see which macros are being applied to each type/class
//!   and it is easy to bundle them in case multiple types use it.
//! * Docstring rendering is handled entirely internal to the proc macros. This
//!   is reasonable since the docstrings only matter for the python interface
//!   and can't cause compile errors. This is also almost-necessary since the
//!   internal proc-macro code has rendering logic for sphinx rst syntax, which
//!   would be a pain to keep in sync at the macro call level.

use fireflow_core::api;
use fireflow_core::config as cfg;
use fireflow_core::core;
use fireflow_core::data::{
    AnyAsciiLayout, AnyNullBitmask, AnyOrderedLayout, AnyOrderedUintLayout, DataLayout2_0,
    DataLayout3_0, DataLayout3_1, DataLayout3_2, DelimAsciiLayout, EndianLayout, F32Range,
    F64Range, FixedAsciiLayout, KnownTot, LayoutOps, NoMeasDatatype, NonMixedEndianLayout,
};
use fireflow_core::error::{MultiResultExt, ResultExt};
use fireflow_core::header;
use fireflow_core::python::exceptions::{
    PyTerminalNoErrorResultExt, PyTerminalNoWarnResultExt, PyTerminalResultExt,
};
use fireflow_core::text::gating::{
    AppliedGates2_0, AppliedGates3_0, AppliedGates3_2, BivariateRegion, GatedMeasurement,
    GatingScheme, Region, UnivariateRegion,
};
use fireflow_core::text::index::{GateIndex, RegionIndex};
use fireflow_core::text::keywords as kws;
use fireflow_core::text::named_vec::Eithers;
use fireflow_core::text::optional::MightHave;
use fireflow_core::validated::ascii_uint::UintSpacePad20;
use fireflow_core::validated::shortname::Shortname;

use fireflow_python_proc::def_fcs_read_std_dataset_with_keywords;
use fireflow_python_proc::{
    def_fcs_read_header, def_fcs_read_raw_dataset, def_fcs_read_raw_dataset_with_keywords,
    def_fcs_read_raw_text, def_fcs_read_std_dataset, def_fcs_read_std_text,
    impl_core_all_meas_nonstandard_keywords, impl_core_all_peak_attrs, impl_core_all_pnanalyte,
    impl_core_all_pncal3_1, impl_core_all_pncal3_2, impl_core_all_pnd, impl_core_all_pndet,
    impl_core_all_pnf, impl_core_all_pnfeature, impl_core_all_pnl_new, impl_core_all_pnl_old,
    impl_core_all_pno, impl_core_all_pnp, impl_core_all_pns, impl_core_all_pnt,
    impl_core_all_pntag, impl_core_all_pntype, impl_core_all_pnv, impl_core_all_shortnames_attr,
    impl_core_all_shortnames_maybe_attr, impl_core_all_transforms_attr, impl_core_get_measurement,
    impl_core_get_measurements, impl_core_get_set_timestep, impl_core_get_temporal,
    impl_core_insert_measurement, impl_core_par, impl_core_push_measurement,
    impl_core_remove_measurement, impl_core_rename_temporal, impl_core_replace_optical,
    impl_core_replace_temporal, impl_core_set_measurements, impl_core_set_measurements_and_layout,
    impl_core_set_temporal, impl_core_set_tr_threshold, impl_core_standard_keywords,
    impl_core_to_version_x_y, impl_core_unset_temporal, impl_core_version, impl_core_write_dataset,
    impl_core_write_text, impl_coredataset_from_kws, impl_coredataset_set_measurements_and_data,
    impl_coredataset_truncate_data, impl_coredataset_unset_data, impl_coretext_from_kws,
    impl_coretext_to_dataset, impl_coretext_unset_measurements, impl_gated_meas,
    impl_layout_byte_widths, impl_new_core, impl_new_delim_ascii_layout,
    impl_new_endian_float_layout, impl_new_endian_uint_layout, impl_new_fixed_ascii_layout,
    impl_new_gate_bi_regions, impl_new_gate_uni_regions, impl_new_meas, impl_new_mixed_layout,
    impl_new_ordered_layout, impl_py_dataset_segments, impl_py_extra_std_keywords, impl_py_header,
    impl_py_header_segments, impl_py_raw_dataset_output, impl_py_raw_dataset_with_kws_output,
    impl_py_raw_text_output, impl_py_raw_text_parse_data, impl_py_std_dataset_output,
    impl_py_std_dataset_with_kws_output, impl_py_std_text_output, impl_py_valid_keywords,
};

use derive_more::{From, Into};
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use std::collections::HashMap;
use std::hash::BuildHasher;

def_fcs_read_header!(api::fcs_read_header);
def_fcs_read_raw_text!(api::fcs_read_raw_text);
def_fcs_read_std_text!(api::fcs_read_std_text);
def_fcs_read_raw_dataset!(api::fcs_read_raw_dataset);
def_fcs_read_std_dataset!(api::fcs_read_std_dataset);
def_fcs_read_raw_dataset_with_keywords!(api::fcs_read_raw_dataset_with_keywords);
def_fcs_read_std_dataset_with_keywords!(api::fcs_read_std_dataset_with_keywords);

impl_py_header!(header::Header);
impl_py_header_segments!(header::HeaderSegments<UintSpacePad20>);
impl_py_valid_keywords!(fireflow_core::validated::keys::ValidKeywords);
impl_py_extra_std_keywords!(fireflow_core::text::parser::ExtraStdKeywords);
impl_py_dataset_segments!(core::DatasetSegments);

impl_py_raw_text_output!(api::RawTEXTOutput);
impl_py_raw_dataset_output!(api::RawDatasetOutput);
impl_py_raw_text_parse_data!(api::RawTEXTParseData);
impl_py_raw_dataset_with_kws_output!(api::RawDatasetWithKwsOutput);

impl_py_std_text_output!(api::StdTEXTOutput);
impl_py_std_dataset_output!(api::StdDatasetOutput);
impl_py_std_dataset_with_kws_output!(core::StdDatasetWithKwsOutput);

// Implement python classes for core* structs
//
// Will actually make classes called PyCoreTEXT* and PyCoreDataset* which
// can be referred as such elsewhere
//
// This will include the __new__ methods and all attributes corresponding to
// "instance variables" supplied to __new__
impl_new_core!(core::CoreTEXT2_0, core::CoreDataset2_0);
impl_new_core!(core::CoreTEXT3_0, core::CoreDataset3_0);
impl_new_core!(core::CoreTEXT3_1, core::CoreDataset3_1);
impl_new_core!(core::CoreTEXT3_2, core::CoreDataset3_2);

// Implement python classes for Optical* structs (as PyOptical*)
//
// This will include the __new__ methods and all attributes corresponding to
// "instance variables" supplied to __new__
impl_new_meas!(core::Optical2_0);
impl_new_meas!(core::Optical3_0);
impl_new_meas!(core::Optical3_1);
impl_new_meas!(core::Optical3_2);

// Implement python classes for Temporal* structs (as PyTemporal*)
//
// This will include the __new__ methods and all attributes corresponding to
// "instance variables" supplied to __new__
impl_new_meas!(core::Temporal2_0);
impl_new_meas!(core::Temporal3_0);
impl_new_meas!(core::Temporal3_1);
impl_new_meas!(core::Temporal3_2);

// Common methods for all Core* versions. Some of these macros will implement a
// slightly different method depending on version.
macro_rules! impl_common {
    ($pytype:ident) => {
        // get FCS version as read-only value
        impl_core_version!($pytype);

        // get $PAR as read-only value
        impl_core_par!($pytype);

        // method to set $TR threshold without changing its reference
        impl_core_set_tr_threshold!($pytype);

        // method to write HEADER+TEXT to file
        impl_core_write_text!($pytype);

        // $Shortnames attribute; for 2.0/3.0, this will not allow setting any to None
        impl_core_all_shortnames_attr!($pytype);

        // method to rename temporal measurement if it exists
        impl_core_rename_temporal!($pytype);

        // methods to set any measurement to temporal (using index or name)
        impl_core_set_temporal!($pytype);

        // method to convert temporal measurement to optical if it exists; these
        // are slightly different for each version
        impl_core_unset_temporal!($pytype);

        // method to get all measurements as read-only list
        impl_core_get_measurements!($pytype);

        // method to set all measurements; this cannot be combined with
        // impl_core_get_measurements! because this method takes arguments
        impl_core_set_measurements!($pytype);

        // method to get one measurement by index
        impl_core_get_measurement!($pytype);

        // method to get temporal measurement if it exists
        impl_core_get_temporal!($pytype);

        // method to set all measurements and layout at once
        impl_core_set_measurements_and_layout!($pytype);

        // methods to add optical or temporal measurement at last index
        impl_core_push_measurement!($pytype);

        // methods to add optical or temporal measurement at arbitrary index
        impl_core_insert_measurement!($pytype);

        // method to replace temporal measurement by index or name; slightly
        // different for each version since later versions are fallable
        impl_core_replace_temporal!($pytype);

        // method to replace optical measurement by index or name
        impl_core_replace_optical!($pytype);

        // method to replace measurement by index or name
        impl_core_remove_measurement!($pytype);

        // methods to convert this class to to a different version; actually
        // implements one method for each version that isn't this one
        impl_core_to_version_x_y!($pytype);

        // attribute for all $PnS keywords
        impl_core_all_pns!($pytype);

        // attribute for all $PnF keywords
        impl_core_all_pnf!($pytype);

        // attribute for all $PnO keywords
        impl_core_all_pno!($pytype);

        // attribute for all $PnP keywords
        impl_core_all_pnp!($pytype);

        // attribute for all $PnT keywords
        impl_core_all_pnt!($pytype);

        // attribute for all $PnV keywords
        impl_core_all_pnv!($pytype);

        // attribute for all scaling keywords ($PnE or $PnG if present);
        // 3.0 and later will return gain and scale combined
        impl_core_all_transforms_attr!($pytype);

        // attribute to get/set nonstandard keywords for all measurements
        impl_core_all_meas_nonstandard_keywords!($pytype);

        // method to return all standard keywords as read-only dict
        impl_core_standard_keywords!($pytype);
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
impl_coretext_from_kws!(core::CoreTEXT2_0);
impl_coretext_from_kws!(core::CoreTEXT3_0);
impl_coretext_from_kws!(core::CoreTEXT3_1);
impl_coretext_from_kws!(core::CoreTEXT3_2);

// impl from_kws for all CoreTEXT*
impl_coredataset_from_kws!(core::CoreDataset2_0);
impl_coredataset_from_kws!(core::CoreDataset3_0);
impl_coredataset_from_kws!(core::CoreDataset3_1);
impl_coredataset_from_kws!(core::CoreDataset3_2);

// Common methods for all CoreTEXT* versions.
macro_rules! impl_coretext_common {
    ($pytype:ident) => {
        impl_coretext_to_dataset!($pytype);
        impl_coretext_unset_measurements!($pytype);
    };
}

impl_coretext_common!(PyCoreTEXT2_0);
impl_coretext_common!(PyCoreTEXT3_0);
impl_coretext_common!(PyCoreTEXT3_1);
impl_coretext_common!(PyCoreTEXT3_2);

// Common methods for all CoreDataset* versions.
macro_rules! impl_coredataset_common {
    ($pytype:ident) => {
        impl_coredataset_set_measurements_and_data!($pytype);
        impl_core_write_dataset!($pytype);
        impl_coredataset_unset_data!($pytype);
        impl_coredataset_truncate_data!($pytype);
    };
}

impl_coredataset_common!(PyCoreDataset2_0);
impl_coredataset_common!(PyCoreDataset3_0);
impl_coredataset_common!(PyCoreDataset3_1);
impl_coredataset_common!(PyCoreDataset3_2);

// methods to get/set timestep; this is not an attribute because the
// setter method returns something
impl_core_get_set_timestep!(PyCoreTEXT3_0);
impl_core_get_set_timestep!(PyCoreTEXT3_1);
impl_core_get_set_timestep!(PyCoreTEXT3_2);
impl_core_get_set_timestep!(PyCoreDataset3_0);
impl_core_get_set_timestep!(PyCoreDataset3_1);
impl_core_get_set_timestep!(PyCoreDataset3_2);

// Get/set $Shortnames for 2.0 and 3.0 where this field is optional
impl_core_all_shortnames_maybe_attr!(PyCoreTEXT2_0);
impl_core_all_shortnames_maybe_attr!(PyCoreTEXT3_0);
impl_core_all_shortnames_maybe_attr!(PyCoreDataset2_0);
impl_core_all_shortnames_maybe_attr!(PyCoreDataset3_0);

// Get/set methods for $PKn and $PKNn (2.0-3.1)
impl_core_all_peak_attrs!(PyCoreTEXT2_0);
impl_core_all_peak_attrs!(PyCoreTEXT3_0);
impl_core_all_peak_attrs!(PyCoreTEXT3_1);
impl_core_all_peak_attrs!(PyCoreDataset2_0);
impl_core_all_peak_attrs!(PyCoreDataset3_0);
impl_core_all_peak_attrs!(PyCoreDataset3_1);

// Get/set methods for scaler $PnL (2.0-3.0)
impl_core_all_pnl_old!(PyCoreTEXT2_0);
impl_core_all_pnl_old!(PyCoreTEXT3_0);
impl_core_all_pnl_old!(PyCoreDataset2_0);
impl_core_all_pnl_old!(PyCoreDataset3_0);

// Get/set methods for vector $PnL (3.1-3.2)
impl_core_all_pnl_new!(PyCoreTEXT3_1);
impl_core_all_pnl_new!(PyCoreTEXT3_2);
impl_core_all_pnl_new!(PyCoreDataset3_1);
impl_core_all_pnl_new!(PyCoreDataset3_2);

// Get/set methods for $PnD (3.1+)
//
// This is valid for the time channel so don't set on just optical
impl_core_all_pnd!(PyCoreTEXT3_1);
impl_core_all_pnd!(PyCoreDataset3_1);
impl_core_all_pnd!(PyCoreTEXT3_2);
impl_core_all_pnd!(PyCoreDataset3_2);

// Get/set methods for $PnDET (3.2)
impl_core_all_pndet!(PyCoreTEXT3_2);
impl_core_all_pndet!(PyCoreDataset3_2);

// Get/set methods for $PnCALIBRATION (3.1)
impl_core_all_pncal3_1!(PyCoreTEXT3_1);
impl_core_all_pncal3_1!(PyCoreDataset3_1);

// Get/set methods for $PnCALIBRATION (3.2)
impl_core_all_pncal3_2!(PyCoreTEXT3_2);
impl_core_all_pncal3_2!(PyCoreDataset3_2);

// Get/set methods for $PnTAG (3.2)
impl_core_all_pntag!(PyCoreTEXT3_2);
impl_core_all_pntag!(PyCoreDataset3_2);

// Get/set methods for $PnTYPE (3.2)
impl_core_all_pntype!(PyCoreTEXT3_2);
impl_core_all_pntype!(PyCoreDataset3_2);

// Get/set methods for $PnFEATURE (3.2)
impl_core_all_pnfeature!(PyCoreTEXT3_2);
impl_core_all_pnfeature!(PyCoreDataset3_2);

// Get/set methods for $PnANALYTE (3.2)
impl_core_all_pnanalyte!(PyCoreTEXT3_2);
impl_core_all_pnanalyte!(PyCoreDataset3_2);

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
        Ok(AppliedGates3_0::try_new(Vec::from(gated_measurements), scheme)?.into())
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

// Implement __new__ and attributes for PyUnivariate2_0
impl_new_gate_uni_regions!(UnivariateRegion<GateIndex>);

// Implement __new__ and attributes for PyUnivariate3_0
impl_new_gate_uni_regions!(UnivariateRegion<kws::MeasOrGateIndex>);

// Implement __new__ and attributes for PyUnivariate3_2
impl_new_gate_uni_regions!(UnivariateRegion<kws::PrefixedMeasIndex>);

// Implement __new__ and attributes for PyBivariate2_0
impl_new_gate_bi_regions!(BivariateRegion<GateIndex>);

// Implement __new__ and attributes for PyBivariate3_0
impl_new_gate_bi_regions!(BivariateRegion<kws::MeasOrGateIndex>);

// Implement __new__ and attributes for PyBivariate3_2
impl_new_gate_bi_regions!(BivariateRegion<kws::PrefixedMeasIndex>);

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

impl_gated_meas!(GatedMeasurement);

#[derive(FromPyObject, IntoPyObject)]
struct PyGatedMeasurements(Vec<PyGatedMeasurement>);

impl From<PyGatedMeasurements> for Vec<GatedMeasurement> {
    fn from(value: PyGatedMeasurements) -> Vec<GatedMeasurement> {
        value.0.into_iter().map(|x| x.0).collect()
    }
}

impl From<Vec<GatedMeasurement>> for PyGatedMeasurements {
    fn from(value: Vec<GatedMeasurement>) -> PyGatedMeasurements {
        Self(value.into_iter().map(Into::into).collect())
    }
}

// Implement __new__ and attributes for PyFixedAsciiLayout
impl_new_fixed_ascii_layout!(FixedAsciiLayout<KnownTot, NoMeasDatatype, false>);

// Implement __new__ and attributes for PyFixedDelimLayout
impl_new_delim_ascii_layout!(DelimAsciiLayout<KnownTot, NoMeasDatatype, false>);

// Implement __new__ and attributes for all PyOrderedUint*Layout structs
impl_new_ordered_layout!(1, false);
impl_new_ordered_layout!(2, false);
impl_new_ordered_layout!(3, false);
impl_new_ordered_layout!(4, false);
impl_new_ordered_layout!(5, false);
impl_new_ordered_layout!(6, false);
impl_new_ordered_layout!(7, false);
impl_new_ordered_layout!(8, false);

// Implement __new__ and attributes for all PyOrderedF*Layout structs
impl_new_ordered_layout!(4, true);
impl_new_ordered_layout!(8, true);

// Implement __new__ and attributes for all PyEndianF*Layout structs
impl_new_endian_float_layout!(4);
impl_new_endian_float_layout!(8);

// Implement __new__ and attributes for PyEndianUintLayout
impl_new_endian_uint_layout!();

// Implement __new__ and attributes for PyMixedLayout
impl_new_mixed_layout!();

// Implement method to return the byte widths of variable-widths layouts
impl_layout_byte_widths!(PyEndianUintLayout);
impl_layout_byte_widths!(PyMixedLayout);

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
    AsciiFixed(PyFixedAsciiLayout),
    AsciiDelim(PyDelimAsciiLayout),
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
        PyFixedAsciiLayout,
        FixedAsciiLayout<KnownTot, NoMeasDatatype, false>
    )]
    AsciiFixed(PyFixedAsciiLayout),

    #[from(
        PyDelimAsciiLayout,
        DelimAsciiLayout<KnownTot, NoMeasDatatype, false>
    )]
    AsciiDelim(PyDelimAsciiLayout),

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

/// Set of temporal optical keys.
///
/// This is a hack to get default arguments to work in python, which will
/// be interpreted as a list since there is no empty set symbol (yet).
#[derive(Into, Default)]
pub struct TemporalOpticalKeys(std::collections::HashSet<cfg::TemporalOpticalKey>);

impl<'py> FromPyObject<'py> for TemporalOpticalKeys {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let xs: Vec<_> = ob.extract()?;
        Ok(Self(xs.into_iter().collect()))
    }
}
