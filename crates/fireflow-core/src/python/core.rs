use crate::core;
use crate::error::ResultExt;
use crate::text::index::MeasIndex;
use crate::text::keywords as kws;
use crate::text::named_vec::{Element, NamedVec, RawInput};
use crate::text::optional::{AlwaysFamily, MaybeFamily};
use crate::text::scale::Scale;
use crate::text::unstainedcenters::UnstainedCenters;
use crate::validated::dataframe::{AnyFCSColumn, FCSDataFrame};
use crate::validated::keys::NonStdKey;
use crate::validated::shortname::{Shortname, ShortnamePrefix};

use super::data::{PyLayout3_2, PyNonMixedLayout, PyOrderedLayout};
use super::exceptions::{PyTerminalNoWarnResultExt, PyTerminalResultExt};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};
use derive_more::{From, Into};
use numpy::{PyArray2, PyReadonlyArray2, ToPyArray};
use polars::prelude::*;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyDict};
use pyo3_polars::PyDataFrame;
use std::collections::HashMap;

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

macro_rules! get_set_metaroot {
    ($get:ident, $set:ident, $t:path, $($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn $get(&self) -> $t {
                    self.0.get_metaroot::<$t>().clone()
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
                    self.0.get_metaroot_opt().cloned()
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
                    self.0.get_meas_opt().map(|x| x.cloned()).collect()
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
                        .get_optical_opt()
                        .map(|(i, x)| (i, x.cloned()))
                        .collect()
                }

                #[setter]
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
        self.0.get_begindatetime()
    }

    #[setter]
    fn set_begindatetime(&mut self, x: Option<DateTime<FixedOffset>>) -> PyResult<()> {
        Ok(self.0.set_begindatetime(x)?)
    }

    #[getter]
    fn get_enddatetime(&self) -> Option<DateTime<FixedOffset>> {
        self.0.get_enddatetime()
    }

    #[setter]
    fn set_enddatetime(&mut self, x: Option<DateTime<FixedOffset>>) -> PyResult<()> {
        Ok(self.0.set_enddatetime(x)?)
    }

    #[getter]
    fn get_unstained_centers(&self) -> Option<HashMap<Shortname, f32>> {
        self.0.get_metaroot_opt::<UnstainedCenters>().map(|y| {
            <HashMap<Shortname, f32>>::from(y.clone())
                .into_iter()
                .collect()
        })
    }

    fn insert_unstained_center(&mut self, k: Shortname, v: f32) -> PyResult<Option<f32>> {
        Ok(self.0.insert_unstained_center(k, v)?)
    }

    fn remove_unstained_center(&mut self, k: Shortname) -> Option<f32> {
        self.0.remove_unstained_center(&k)
    }

    fn clear_unstained_centers(&mut self) {
        self.0.clear_unstained_centers()
    }
}

// Get/set methods for all versions
macro_rules! common_methods {
    ($pytype:ident, $($rest:ident),*) => {
        common_methods!($pytype);
        common_methods!($($rest),+);

    };

    ($pytype:ident) => {
        get_set_metaroot_opt!(get_abrt,  set_abrt,  kws::Abrt,  $pytype);
        get_set_metaroot_opt!(get_cells, set_cells, kws::Cells, $pytype);
        get_set_metaroot_opt!(get_com,   set_com,   kws::Com,   $pytype);
        get_set_metaroot_opt!(get_exp,   set_exp,   kws::Exp,   $pytype);
        get_set_metaroot_opt!(get_fil,   set_fil,   kws::Fil,   $pytype);
        get_set_metaroot_opt!(get_inst,  set_inst,  kws::Inst,  $pytype);
        get_set_metaroot_opt!(get_lost,  set_lost,  kws::Lost,  $pytype);
        get_set_metaroot_opt!(get_op,    set_op,    kws::Op,    $pytype);
        get_set_metaroot_opt!(get_proj,  set_proj,  kws::Proj,  $pytype);
        get_set_metaroot_opt!(get_smno,  set_smno,  kws::Smno,  $pytype);
        get_set_metaroot_opt!(get_src,   set_src,   kws::Src,   $pytype);
        get_set_metaroot_opt!(get_sys,   set_sys,   kws::Sys,   $pytype);

        // common measurement keywords
        get_set_all_meas!(get_longnames, set_longnames, kws::Longname, $pytype);

        get_set_all_optical!(get_filters, set_filters, kws::Filter, $pytype);
        get_set_all_optical!(get_powers,  set_powers,  kws::Power,  $pytype);

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
            fn insert_nonstandard(&mut self, key: NonStdKey, v: String) -> Option<String> {
                self.0.metaroot.nonstandard_keywords.insert(key, v)
            }

            fn remove_nonstandard(&mut self, key: NonStdKey) -> Option<String> {
                self.0.metaroot.nonstandard_keywords.remove(&key)
            }

            fn get_nonstandard(&mut self, key: NonStdKey) -> Option<String> {
                self.0.metaroot.nonstandard_keywords.get(&key).cloned()
            }

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
                keyvals: Vec<(NonStdKey, String)>,
            ) -> PyResult<Vec<Option<String>>> {
                Ok(self.0.insert_meas_nonstandard(keyvals)?)

            }

            fn remove_meas_nonstandard(
                &mut self,
                keys: Vec<NonStdKey>
            ) -> PyResult<Vec<Option<String>>> {
                Ok(self.0.remove_meas_nonstandard(keys.iter().collect())?)
            }

            fn get_meas_nonstandard(
                &mut self,
                keys: Vec<NonStdKey>
            ) -> Option<Vec<Option<String>>> {
                self.0
                    .get_meas_nonstandard(&keys[..])
                    .map(|rs| rs.into_iter().map(|r| r.cloned()).collect())
            }

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
            fn trigger_name(&self) -> Option<Shortname> {
                self.0.trigger_name().cloned()
            }

            #[getter]
            fn trigger_threshold(&self) -> Option<u32> {
                self.0.trigger_threshold()
            }

            #[setter]
            fn set_trigger_name(&mut self, name: Shortname) -> bool {
                self.0.set_trigger_name(name)
            }

            #[setter]
            fn set_trigger_threshold(&mut self, x: u32) -> bool {
                self.0.set_trigger_threshold(x)
            }

            fn clear_trigger(&mut self) -> bool {
                self.0.clear_trigger()
            }

            #[getter]
            fn shortnames_maybe(&self) -> Vec<Option<Shortname>> {
                self.0.shortnames_maybe().into_iter().map(|x| x.cloned()).collect()
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
    ($pytype:ident, $o:ident, $t:ident) => {
        #[pymethods]
        impl $pytype {
            fn remove_measurement_by_name(
                &mut self,
                name: Shortname,
            ) -> Option<(MeasIndex, Element<$t, $o>)> {
                self.0
                    .remove_measurement_by_name(&name)
                    .map(|(i, x)| (i, x.inner_into()))
            }

            fn measurement_at(&self, i: MeasIndex) -> PyResult<Element<$t, $o>> {
                let ms: &NamedVec<_, _, _, _> = self.0.as_ref();
                let m = ms.get(i)?;
                Ok(m.bimap(|x| x.1.clone(), |x| x.1.clone()).inner_into())
            }

            fn replace_optical_at(&mut self, i: MeasIndex, m: $o) -> PyResult<Element<$t, $o>> {
                let ret = self.0.replace_optical_at(i, m.into())?;
                Ok(ret.inner_into())
            }

            fn replace_optical_named(&mut self, name: Shortname, m: $o) -> Option<Element<$t, $o>> {
                self.0
                    .replace_optical_named(&name, m.into())
                    .map(|r| r.inner_into())
            }

            fn rename_temporal(&mut self, name: Shortname) -> Option<Shortname> {
                self.0.rename_temporal(name)
            }

            fn replace_temporal_at(
                &mut self,
                i: MeasIndex,
                m: $t,
                force: bool,
            ) -> PyResult<Element<$t, $o>> {
                let ret = self
                    .0
                    .replace_temporal_at(i, m.into(), force)
                    .py_term_resolve()?;
                Ok(ret.inner_into())
            }

            fn replace_temporal_named(
                &mut self,
                name: Shortname,
                m: $t,
                force: bool,
            ) -> PyResult<Option<Element<$t, $o>>> {
                let ret = self
                    .0
                    .replace_temporal_named(&name, m.into(), force)
                    .py_term_resolve()?;
                Ok(ret.map(|r| r.inner_into()))
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
        }
    };
}

common_meas_get_set!(PyCoreTEXT2_0, PyOptical2_0, PyTemporal2_0);
common_meas_get_set!(PyCoreTEXT3_0, PyOptical3_0, PyTemporal3_0);
common_meas_get_set!(PyCoreTEXT3_1, PyOptical3_1, PyTemporal3_1);
common_meas_get_set!(PyCoreTEXT3_2, PyOptical3_2, PyTemporal3_2);
common_meas_get_set!(PyCoreDataset2_0, PyOptical2_0, PyTemporal2_0);
common_meas_get_set!(PyCoreDataset3_0, PyOptical3_0, PyTemporal3_0);
common_meas_get_set!(PyCoreDataset3_1, PyOptical3_1, PyTemporal3_1);
common_meas_get_set!(PyCoreDataset3_2, PyOptical3_2, PyTemporal3_2);

macro_rules! common_coretext_meas_get_set {
    ($pytype:ident, $timetype:ident) => {
        #[pymethods]
        impl $pytype {
            fn push_temporal(
                &mut self,
                name: Shortname,
                t: $timetype,
                r: kws::Range,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .push_temporal(name, t.into(), r, notrunc)
                    .py_term_resolve()
            }

            fn insert_temporal(
                &mut self,
                i: MeasIndex,
                name: Shortname,
                t: $timetype,
                r: kws::Range,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .insert_temporal(i, name, t.into(), r, notrunc)
                    .py_term_resolve()
            }

            fn unset_measurements(&mut self) -> PyResult<()> {
                Ok(self.0.unset_measurements()?)
            }
        }
    };
}

common_coretext_meas_get_set!(PyCoreTEXT2_0, PyTemporal2_0);
common_coretext_meas_get_set!(PyCoreTEXT3_0, PyTemporal3_0);
common_coretext_meas_get_set!(PyCoreTEXT3_1, PyTemporal3_1);
common_coretext_meas_get_set!(PyCoreTEXT3_2, PyTemporal3_2);

macro_rules! coredata_meas_get_set {
    ($pytype:ident, $timetype:ident) => {
        #[pymethods]
        impl $pytype {
            fn push_temporal(
                &mut self,
                name: Shortname,
                t: $timetype,
                col: AnyFCSColumn,
                r: kws::Range,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .push_temporal(name, t.into(), col, r, notrunc)
                    .py_term_resolve()
            }

            fn insert_temporal(
                &mut self,
                i: MeasIndex,
                name: Shortname,
                t: $timetype,
                col: AnyFCSColumn,
                r: kws::Range,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .insert_temporal(i, name, t.into(), col, r, notrunc)
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

coredata_meas_get_set!(PyCoreDataset2_0, PyTemporal2_0);
coredata_meas_get_set!(PyCoreDataset3_0, PyTemporal3_0);
coredata_meas_get_set!(PyCoreDataset3_1, PyTemporal3_1);
coredata_meas_get_set!(PyCoreDataset3_2, PyTemporal3_2);

macro_rules! coretext2_0_meas_methods {
    ($pytype:ident, $o:ident, $t:ident) => {
        #[pymethods]
        impl $pytype {
            fn remove_measurement_by_index(
                &mut self,
                index: MeasIndex,
            ) -> PyResult<(Option<Shortname>, Element<$t, $o>)> {
                let r = self.0.remove_measurement_by_index(index)?;
                let (n, v) = Element::unzip::<MaybeFamily>(r);
                Ok((n.0, v.inner_into()))
            }

            #[pyo3(signature = (m, r, notrunc=false, name=None))]
            fn push_optical(
                &mut self,
                m: $o,
                r: kws::Range,
                notrunc: bool,
                name: Option<Shortname>,
            ) -> PyResult<Shortname> {
                self.0
                    .push_optical(name.into(), m.into(), r, notrunc)
                    .py_term_resolve()
            }

            #[pyo3(signature = (i, m, r, notrunc=false, name=None))]
            fn insert_optical(
                &mut self,
                i: MeasIndex,
                m: $o,
                r: kws::Range,
                notrunc: bool,
                name: Option<Shortname>,
            ) -> PyResult<Shortname> {
                self.0
                    .insert_optical(i, name.into(), m.into(), r, notrunc)
                    .py_term_resolve()
            }
        }
    };
}

coretext2_0_meas_methods!(PyCoreTEXT2_0, PyOptical2_0, PyTemporal2_0);
coretext2_0_meas_methods!(PyCoreTEXT3_0, PyOptical3_0, PyTemporal3_0);

macro_rules! coretext3_1_meas_methods {
    ($pytype:ident, $o:ident, $t:ident) => {
        #[pymethods]
        impl $pytype {
            fn remove_measurement_by_index(
                &mut self,
                index: MeasIndex,
            ) -> PyResult<(Shortname, Element<$t, $o>)> {
                let r = self.0.remove_measurement_by_index(index)?;
                let (n, v) = Element::unzip::<AlwaysFamily>(r);
                Ok((n.0, v.inner_into()))
            }

            fn push_optical(
                &mut self,
                m: $o,
                name: Shortname,
                r: kws::Range,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .push_optical(name.into(), m.into(), r, notrunc)
                    .py_term_resolve()
                    .void()
            }

            fn insert_optical(
                &mut self,
                i: MeasIndex,
                m: $o,
                name: Shortname,
                r: kws::Range,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .insert_optical(i, name.into(), m.into(), r, notrunc)
                    .py_term_resolve()
                    .void()
            }
        }
    };
}

coretext3_1_meas_methods!(PyCoreTEXT3_1, PyOptical3_1, PyTemporal3_1);
coretext3_1_meas_methods!(PyCoreTEXT3_2, PyOptical3_2, PyTemporal3_2);

macro_rules! set_measurements_ordered {
    ($pytype:ident, $t:ident, $o:ident) => {
        #[pymethods]
        impl $pytype {
            fn set_measurements(
                &mut self,
                xs: RawInput<MaybeFamily, $t, $o>,
                prefix: ShortnamePrefix,
            ) -> PyResult<()> {
                self.0
                    .set_measurements(xs.inner_into(), prefix)
                    .py_term_resolve_nowarn()
            }

            fn set_measurements_and_layout(
                &mut self,
                xs: RawInput<MaybeFamily, $t, $o>,
                layout: PyOrderedLayout,
                prefix: ShortnamePrefix,
            ) -> PyResult<()> {
                self.0
                    .set_measurements_and_layout(xs.inner_into(), layout.into(), prefix)
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
            pub fn set_measurements(&mut self, xs: RawInput<AlwaysFamily, $t, $o>) -> PyResult<()> {
                self.0
                    .set_measurements_noprefix(xs.inner_into())
                    .py_term_resolve_nowarn()
            }

            fn set_measurements_and_layout(
                &mut self,
                xs: RawInput<AlwaysFamily, $t, $o>,
                layout: $l,
            ) -> PyResult<()> {
                self.0
                    .set_measurements_and_layout_noprefix(xs.inner_into(), layout.into())
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
                xs: RawInput<MaybeFamily, $t, $o>,
                cols: Vec<AnyFCSColumn>,
                prefix: ShortnamePrefix,
            ) -> PyResult<()> {
                self.0
                    .set_measurements_and_data(xs.inner_into(), cols, prefix)
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
                xs: RawInput<AlwaysFamily, $t, $o>,
                cols: Vec<AnyFCSColumn>,
            ) -> PyResult<()> {
                self.0
                    .set_measurements_and_data_noprefix(xs.inner_into(), cols)
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
                self.0.get_all_scales().collect()
            }

            #[getter]
            fn get_scales(&self) -> Vec<(MeasIndex, Option<Scale>)> {
                self.0
                    .get_optical_opt::<Scale>()
                    .map(|(i, s)| (i, s.map(|&x| x)))
                    .collect()
            }

            #[setter]
            fn set_scales(&mut self, xs: Vec<Option<Scale>>) -> PyResult<()> {
                self.0.set_scales(xs).py_term_resolve_nowarn()
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
                self.0.get_all_transforms().collect()
            }

            #[getter]
            fn get_transforms(&self) -> Vec<(MeasIndex, core::ScaleTransform)> {
                self.0.get_optical().map(|(i, &s)| (i, s)).collect()
            }

            #[setter]
            fn set_transforms(&mut self, xs: Vec<core::ScaleTransform>) -> PyResult<()> {
                self.0.set_transforms(xs).py_term_resolve_nowarn()
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

                #[setter]
                fn set_timestep(&mut self, ts: kws::Timestep) -> bool {
                    self.0.set_timestep(ts)
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
            fn get_compensation<'a>(&self, py: Python<'a>) -> Option<Bound<'a, PyArray2<f32>>> {
                self.0.compensation().map(|x| x.to_pyarray(py))
            }

            fn set_compensation(&mut self, a: PyReadonlyArray2<f32>) -> Result<(), PyErr> {
                let m = a.as_matrix().into_owned();
                Ok(self.0.set_compensation(m)?)
            }

            fn unset_compensation(&mut self) {
                self.0.unset_compensation()
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
                a: PyReadonlyArray2<f32>,
            ) -> PyResult<()> {
                let m = a.as_matrix().into_owned();
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
            fn set_nonstandard_keywords(&mut self, xs: HashMap<NonStdKey, String>) {
                self.0.common.nonstandard_keywords = xs;
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
            fn set_transform(&mut self, x: core::ScaleTransform) {
                self.0.specific.scale = x;
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
            fn set_timestep(&mut self, x: kws::Timestep) {
                self.0.specific.timestep = x
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
get_set_meas!(get_det, set_det, kws::DetectorName, PyOptical3_2);

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
