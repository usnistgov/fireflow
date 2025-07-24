use fireflow_core::api::*;
use fireflow_core::config::*;
use fireflow_core::core::*;
use fireflow_core::error::*;
use fireflow_core::header::*;
use fireflow_core::segment::{HeaderAnalysisSegment, HeaderDataSegment, OtherSegment};
use fireflow_core::text::datetimes::ReversedDatetimes;
use fireflow_core::text::index::MeasIndex;
use fireflow_core::text::keywords::*;
use fireflow_core::text::named_vec::{Element, KeyLengthError, NamedVec, RawInput};
use fireflow_core::text::optional::*;
use fireflow_core::text::scale::*;
use fireflow_core::text::timestamps::ReversedTimestamps;
use fireflow_core::text::unstainedcenters::UnstainedCenters;
use fireflow_core::validated::dataframe::*;
use fireflow_core::validated::keys::*;
use fireflow_core::validated::shortname::*;

use super::layout::{self, PyLayout3_2, PyNonMixedLayout, PyOrderedLayout};
use super::macros::py_wrap;

use bigdecimal::BigDecimal;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};
use derive_more::{Display, From};
use nonempty::NonEmpty;
use numpy::{PyArray2, PyReadonlyArray2, ToPyArray};
use polars::prelude::*;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyWarning};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyDict};
use pyo3_polars::PyDataFrame;
use std::collections::HashMap;
use std::ffi::CString;
use std::fmt;
use std::path;

#[pymodule]
fn pyreflow(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("PyreflowException", py.get_type::<PyreflowException>())?;
    m.add("PyreflowWarning", py.get_type::<PyreflowWarning>())?;

    m.add_class::<PyCoreTEXT2_0>()?;
    m.add_class::<PyCoreTEXT3_0>()?;
    m.add_class::<PyCoreTEXT3_1>()?;
    m.add_class::<PyCoreTEXT3_2>()?;

    m.add_class::<PyCoreDataset2_0>()?;
    m.add_class::<PyCoreDataset3_0>()?;
    m.add_class::<PyCoreDataset3_1>()?;
    m.add_class::<PyCoreDataset3_2>()?;

    m.add_class::<PyOptical2_0>()?;
    m.add_class::<PyOptical3_0>()?;
    m.add_class::<PyOptical3_1>()?;
    m.add_class::<PyOptical3_2>()?;

    m.add_class::<PyTemporal2_0>()?;
    m.add_class::<PyTemporal3_0>()?;
    m.add_class::<PyTemporal3_1>()?;
    m.add_class::<PyTemporal3_2>()?;

    m.add_class::<layout::PyAsciiFixedLayout>()?;
    m.add_class::<layout::PyAsciiDelimLayout>()?;
    m.add_class::<layout::PyOrderedUint08Layout>()?;
    m.add_class::<layout::PyOrderedUint16Layout>()?;
    m.add_class::<layout::PyOrderedUint24Layout>()?;
    m.add_class::<layout::PyOrderedUint32Layout>()?;
    m.add_class::<layout::PyOrderedUint40Layout>()?;
    m.add_class::<layout::PyOrderedUint48Layout>()?;
    m.add_class::<layout::PyOrderedUint56Layout>()?;
    m.add_class::<layout::PyOrderedUint64Layout>()?;
    m.add_class::<layout::PyOrderedF32Layout>()?;
    m.add_class::<layout::PyOrderedF64Layout>()?;
    m.add_class::<layout::PyEndianF32Layout>()?;
    m.add_class::<layout::PyEndianF64Layout>()?;
    m.add_class::<layout::PyEndianUintLayout>()?;
    m.add_class::<layout::PyMixedLayout>()?;

    m.add_function(wrap_pyfunction!(py_fcs_read_header, m)?)?;
    m.add_function(wrap_pyfunction!(py_fcs_read_raw_text, m)?)?;
    m.add_function(wrap_pyfunction!(py_fcs_read_std_text, m)?)?;
    m.add_function(wrap_pyfunction!(py_fcs_read_std_dataset, m)?)?;
    m.add_function(wrap_pyfunction!(py_fcs_read_raw_dataset, m)?)?;
    m.add_function(wrap_pyfunction!(py_fcs_read_raw_dataset_with_keywords, m)?)?;
    m.add_function(wrap_pyfunction!(py_fcs_read_std_dataset_with_keywords, m)?)
}

#[pyfunction]
#[pyo3(name = "fcs_read_header")]
fn py_fcs_read_header(p: path::PathBuf, conf: ReadHeaderConfig) -> PyResult<Header> {
    fcs_read_header(&p, &conf).py_term_resolve_nowarn()
}

#[pyfunction]
#[pyo3(name = "fcs_read_raw_text")]
fn py_fcs_read_raw_text(p: path::PathBuf, conf: ReadRawTEXTConfig) -> PyResult<RawTEXTOutput> {
    fcs_read_raw_text(&p, &conf).py_term_resolve()
}

#[pyfunction]
#[pyo3(name = "fcs_read_std_text")]
fn py_fcs_read_std_text(
    p: path::PathBuf,
    conf: ReadStdTEXTConfig,
) -> PyResult<(PyAnyCoreTEXT, StdTEXTOutput)> {
    let (core, data) = fcs_read_std_text(&p, &conf).py_term_resolve()?;
    Ok((core.into(), data))
}

#[pyfunction]
#[pyo3(name = "fcs_read_raw_dataset")]
fn py_fcs_read_raw_dataset(
    p: path::PathBuf,
    conf: ReadRawDatasetConfig,
) -> PyResult<RawDatasetOutput> {
    fcs_read_raw_dataset(&p, &conf).py_term_resolve()
}

#[pyfunction]
#[pyo3(name = "fcs_read_std_dataset")]
fn py_fcs_read_std_dataset(
    p: path::PathBuf,
    conf: ReadStdDatasetConfig,
) -> PyResult<(PyAnyCoreDataset, StdDatasetOutput)> {
    let (core, data) = fcs_read_std_dataset(&p, &conf).py_term_resolve()?;
    Ok((core.into(), data))
}

#[pyfunction]
#[pyo3(name = "fcs_read_raw_dataset_with_keywords")]
fn py_fcs_read_raw_dataset_with_keywords(
    p: path::PathBuf,
    version: Version,
    std: StdKeywords,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
    other_segs: Vec<OtherSegment>,
    conf: ReadRawDatasetFromKeywordsConfig,
) -> PyResult<RawDatasetWithKwsOutput> {
    fcs_read_raw_dataset_with_keywords(p, version, &std, data_seg, analysis_seg, other_segs, &conf)
        .py_term_resolve()
}

#[pyfunction]
#[pyo3(name = "fcs_read_std_dataset_with_keywords")]
fn py_fcs_read_std_dataset_with_keywords(
    p: path::PathBuf,
    version: Version,
    kws: ValidKeywords,
    data_seg: HeaderDataSegment,
    analysis_seg: HeaderAnalysisSegment,
    other_segs: Vec<OtherSegment>,
    conf: ReadStdDatasetFromKeywordsConfig,
) -> PyResult<(PyAnyCoreDataset, StdDatasetWithKwsOutput)> {
    let (core, data) = fcs_read_std_dataset_with_keywords(
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

// core* objects
py_wrap!(PyCoreTEXT2_0, CoreTEXT2_0, "CoreTEXT2_0");
py_wrap!(PyCoreTEXT3_0, CoreTEXT3_0, "CoreTEXT3_0");
py_wrap!(PyCoreTEXT3_1, CoreTEXT3_1, "CoreTEXT3_1");
py_wrap!(PyCoreTEXT3_2, CoreTEXT3_2, "CoreTEXT3_2");

py_wrap!(PyCoreDataset2_0, CoreDataset2_0, "CoreDataset2_0");
py_wrap!(PyCoreDataset3_0, CoreDataset3_0, "CoreDataset3_0");
py_wrap!(PyCoreDataset3_1, CoreDataset3_1, "CoreDataset3_1");
py_wrap!(PyCoreDataset3_2, CoreDataset3_2, "CoreDataset3_2");

#[derive(IntoPyObject, From)]
enum PyAnyCoreTEXT {
    #[from(CoreTEXT2_0)]
    FCS2_0(PyCoreTEXT2_0),
    #[from(CoreTEXT3_0)]
    FCS3_0(PyCoreTEXT3_0),
    #[from(CoreTEXT3_1)]
    FCS3_1(PyCoreTEXT3_1),
    #[from(CoreTEXT3_2)]
    FCS3_2(PyCoreTEXT3_2),
}

impl From<AnyCoreTEXT> for PyAnyCoreTEXT {
    fn from(value: AnyCoreTEXT) -> PyAnyCoreTEXT {
        match value {
            AnyCoreTEXT::FCS2_0(x) => (*x).into(),
            AnyCoreTEXT::FCS3_0(x) => (*x).into(),
            AnyCoreTEXT::FCS3_1(x) => (*x).into(),
            AnyCoreTEXT::FCS3_2(x) => (*x).into(),
        }
    }
}

#[derive(IntoPyObject, From)]
enum PyAnyCoreDataset {
    #[from(CoreDataset2_0)]
    FCS2_0(PyCoreDataset2_0),
    #[from(CoreDataset3_0)]
    FCS3_0(PyCoreDataset3_0),
    #[from(CoreDataset3_1)]
    FCS3_1(PyCoreDataset3_1),
    #[from(CoreDataset3_2)]
    FCS3_2(PyCoreDataset3_2),
}

impl From<AnyCoreDataset> for PyAnyCoreDataset {
    fn from(value: AnyCoreDataset) -> PyAnyCoreDataset {
        match value {
            AnyCoreDataset::FCS2_0(x) => (*x).into(),
            AnyCoreDataset::FCS3_0(x) => (*x).into(),
            AnyCoreDataset::FCS3_1(x) => (*x).into(),
            AnyCoreDataset::FCS3_2(x) => (*x).into(),
        }
    }
}

py_wrap!(PyOptical2_0, Optical2_0, "Optical2_0");
py_wrap!(PyOptical3_0, Optical3_0, "Optical3_0");
py_wrap!(PyOptical3_1, Optical3_1, "Optical3_1");
py_wrap!(PyOptical3_2, Optical3_2, "Optical3_2");

py_wrap!(PyTemporal2_0, Temporal2_0, "Temporal2_0");
py_wrap!(PyTemporal3_0, Temporal3_0, "Temporal3_0");
py_wrap!(PyTemporal3_1, Temporal3_1, "Temporal3_1");
py_wrap!(PyTemporal3_2, Temporal3_2, "Temporal3_2");

macro_rules! get_set_metaroot {
    ($get:ident, $set:ident, $t:ident, $($pytype:ident),*) => {
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
    ($get:ident, $set:ident, $outer:ident, $($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn $get(&self) -> Option<$outer> {
                    self.0.get_metaroot_opt().cloned()
                }

                #[setter]
                fn $set(&mut self, s: Option<$outer>) {
                    self.0.set_metaroot::<Option<$outer>>(s.map(|x| x.into()))
                }
            }
        )*
    };
}

macro_rules! get_set_all_meas {
    ($get:ident, $set:ident, $outer:ident, $($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn $get(&self) -> Vec<(MeasIndex, Option<$outer>)> {
                    self.0
                        .get_meas_opt()
                        .map(|(i, x)| (i, x.cloned()))
                        .collect()
                }

                #[setter]
                fn $set(&mut self, xs: Vec<Option<$outer>>) -> Result<(), PyKeyLengthError> {
                    self.0.set_meas(xs)?;
                    Ok(())
                }
            }
        )*
    };
}

macro_rules! get_set_all_optical {
    ($get:ident, $set:ident, $outer:ident, $($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn $get(&self) -> Vec<(MeasIndex, Option<$outer>)> {
                    self.0
                        .get_optical_opt()
                        .map(|(i, x)| (i, x.cloned()))
                        .collect()
                }

                #[setter]
                fn $set(&mut self, xs: Vec<Option<$outer>>) -> Result<(), PyKeyLengthError> {
                    self.0.set_optical(xs)?;
                    Ok(())
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
    fn new(mode: Mode) -> PyResult<Self> {
        Ok(CoreTEXT2_0::new(mode).into())
    }
}

#[pymethods]
impl PyCoreTEXT3_0 {
    #[new]
    fn new(mode: Mode) -> PyResult<Self> {
        Ok(CoreTEXT3_0::new(mode).into())
    }

    #[getter]
    fn get_unicode(&self) -> Option<Unicode> {
        self.0.metaroot.specific.unicode.as_ref_opt().cloned()
    }

    #[setter]
    fn set_unicode(&mut self, x: Option<Unicode>) {
        self.0.metaroot.specific.unicode = x.into();
    }
}

#[pymethods]
impl PyCoreTEXT3_1 {
    #[new]
    fn new(mode: Mode) -> Self {
        CoreTEXT3_1::new(mode).into()
    }
}

#[pymethods]
impl PyCoreTEXT3_2 {
    #[new]
    fn new(cyt: String) -> Self {
        CoreTEXT3_2::new(cyt).into()
    }

    #[getter]
    fn get_begindatetime(&self) -> Option<DateTime<FixedOffset>> {
        self.0.get_begindatetime()
    }

    #[setter]
    fn set_begindatetime(
        &mut self,
        x: Option<DateTime<FixedOffset>>,
    ) -> Result<(), PyReversedDatetimes> {
        self.0.set_begindatetime(x)?;
        Ok(())
    }

    #[getter]
    fn get_enddatetime(&self) -> Option<DateTime<FixedOffset>> {
        self.0.get_enddatetime()
    }

    #[setter]
    fn set_enddatetime(
        &mut self,
        x: Option<DateTime<FixedOffset>>,
    ) -> Result<(), PyReversedDatetimes> {
        self.0.set_enddatetime(x)?;
        Ok(())
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
        self.0
            .insert_unstained_center(k, v)
            .map_err(|e| PyreflowException::new_err(e.to_string()))
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
        get_set_metaroot_opt!(get_abrt, set_abrt, Abrt,  $pytype);
        get_set_metaroot_opt!(get_cells, set_cells, Cells,  $pytype);
        get_set_metaroot_opt!(get_com, set_com, Com,  $pytype);
        get_set_metaroot_opt!(get_exp, set_exp, Exp,  $pytype);
        get_set_metaroot_opt!(get_fil, set_fil, Fil,  $pytype);
        get_set_metaroot_opt!(get_inst, set_inst, Inst,  $pytype);
        get_set_metaroot_opt!(get_lost, set_lost, Lost,  $pytype);
        get_set_metaroot_opt!(get_op, set_op, Op,  $pytype);
        get_set_metaroot_opt!(get_proj, set_proj, Proj, $pytype);
        get_set_metaroot_opt!(get_smno, set_smno, Smno, $pytype);
        get_set_metaroot_opt!(get_src, set_src, Src, $pytype);
        get_set_metaroot_opt!(get_sys, set_sys, Sys, $pytype);

        // common measurement keywords
        get_set_all_optical!(get_filters, set_filters, Filter, $pytype);
        get_set_all_optical!(get_powers, set_powers, Power, $pytype);

        get_set_all_optical!(
            get_percents_emitted,
            set_percents_emitted,
            PercentEmitted,
            $pytype
        );

        get_set_all_optical!(
            get_detector_types,
            set_detector_types,
            DetectorType,
            $pytype
        );

        get_set_all_optical!(
            get_detector_voltages,
            set_detector_voltages,
            DetectorVoltage,
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
                self.0
                    .insert_meas_nonstandard(keyvals)
                    .map_err(|e| PyreflowException::new_err(e.to_string()))

            }

            fn remove_meas_nonstandard(
                &mut self,
                keys: Vec<NonStdKey>
            ) -> PyResult<Vec<Option<String>>> {
                self.0
                    .remove_meas_nonstandard(keys.iter().collect())
                    .map_err(|e| PyreflowException::new_err(e.to_string()))
            }

            fn get_meas_nonstandard(
                &mut self,
                keys: Vec<NonStdKey>
            ) -> PyResult<Option<Vec<Option<String>>>> {
                let res = self.0
                    .get_meas_nonstandard(&keys[..])
                    .map(|rs| rs.into_iter().map(|r| r.cloned()).collect());
                Ok(res)
            }

            #[getter]
            fn get_btim(&self) -> Option<NaiveTime> {
                self.0.btim_naive()
            }

            #[setter]
            fn set_btim(&mut self, x: Option<NaiveTime>) -> Result<(), PyReversedTimestamps> {
                self.0.set_btim_naive(x)?;
                Ok(())
            }

            #[getter]
            fn get_etim(&self) -> Option<NaiveTime> {
                self.0.etim_naive()
            }

            #[setter]
            fn set_etim(&mut self, x: Option<NaiveTime>) -> Result<(), PyReversedTimestamps> {
                self.0.set_etim_naive(x)?;
                Ok(())
            }

            #[getter]
            fn get_date(&self) -> Option<NaiveDate> {
                self.0.date_naive()
            }

            #[setter]
            fn set_date(&mut self, x: Option<NaiveDate>) -> Result<(), PyReversedTimestamps> {
                self.0.set_date_naive(x)?;
                Ok(())
            }

            #[getter]
            fn trigger_name(&self) -> Option<String> {
                self.0.trigger_name().map(|x| x.to_string())
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
            fn get_longnames(&self) -> Vec<Option<String>> {
                self.0
                    .longnames()
                    .into_iter()
                    .map(|x| x.map(|y| y.0.to_string()))
                    .collect()
            }

            #[setter]
            fn set_longnames(&mut self, ns: Vec<Option<String>>) -> PyResult<()> {
                self.0
                    .set_longnames(ns)
                    .map_err(|e| PyreflowException::new_err(e.to_string()))
            }

            #[getter]
            fn shortnames_maybe(&self) -> Vec<Option<String>> {
                self.0
                    .shortnames_maybe()
                    .into_iter()
                    .map(|x| x.map(|y| y.to_string()))
                    .collect()
            }

            #[getter]
            fn all_shortnames(&self) -> Vec<String> {
                self.0
                    .all_shortnames()
                    .into_iter()
                    .map(|x| x.to_string())
                    .collect()
            }

            #[setter]
            fn set_all_shortnames(&mut self, names: Vec<Shortname>) -> PyResult<()> {
                self.0
                    .set_all_shortnames(names)
                    // TODO this is a setkeyserror, could be more generalized
                    .map_err(|e| PyreflowException::new_err(e.to_string()))
                    .void()
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
                timestep: Timestep,
                force: bool,
            ) -> PyResult<bool> {
                self.0
                    .set_temporal(&name, timestep, force)
                    .py_term_resolve()
            }

            fn set_temporal_at(
                &mut self,
                index: MeasIndex,
                timestep: Timestep,
                force: bool,
            ) -> PyResult<bool> {
                self.0
                    .set_temporal_at(index, timestep, force)
                    .py_term_resolve()
            }

            fn unset_temporal(&mut self, force: bool) -> PyResult<Option<Timestep>> {
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
                r: BigDecimal,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .push_temporal(name, t.into(), Range(r), notrunc)
                    .py_term_resolve()
            }

            fn insert_temporal(
                &mut self,
                i: MeasIndex,
                name: Shortname,
                t: $timetype,
                r: BigDecimal,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .insert_temporal(i, name, t.into(), Range(r), notrunc)
                    .py_term_resolve()
            }

            fn unset_measurements(&mut self) -> PyResult<()> {
                self.0
                    .unset_measurements()
                    .map_err(|e| PyreflowException::new_err(e.to_string()))
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
                r: BigDecimal,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .push_temporal(name, t.into(), col, Range(r), notrunc)
                    .py_term_resolve()
            }

            fn insert_temporal(
                &mut self,
                i: MeasIndex,
                name: Shortname,
                t: $timetype,
                col: AnyFCSColumn,
                r: BigDecimal,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .insert_temporal(i, name, t.into(), col, Range(r), notrunc)
                    .py_term_resolve()
            }

            fn unset_data(&mut self) -> PyResult<()> {
                self.0
                    .unset_data()
                    .map_err(|e| PyreflowException::new_err(e.to_string()))
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
            fn analysis(&self) -> Analysis {
                self.0.analysis.clone()
            }

            #[setter]
            fn set_analysis(&mut self, xs: Analysis) {
                self.0.analysis = xs.into();
            }

            #[getter]
            fn others(&self) -> Others {
                self.0.others.clone()
            }

            #[setter]
            fn set_others(&mut self, xs: Others) {
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
                r: BigDecimal,
                notrunc: bool,
                name: Option<Shortname>,
            ) -> PyResult<Shortname> {
                self.0
                    .push_optical(name.into(), m.into(), r.into(), notrunc)
                    .py_term_resolve()
            }

            #[pyo3(signature = (i, m, r, notrunc=false, name=None))]
            fn insert_optical(
                &mut self,
                i: MeasIndex,
                m: $o,
                r: BigDecimal,
                notrunc: bool,
                name: Option<Shortname>,
            ) -> PyResult<Shortname> {
                self.0
                    .insert_optical(i, name.into(), m.into(), Range(r), notrunc)
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
                r: BigDecimal,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .push_optical(name.into(), m.into(), Range(r), notrunc)
                    .py_term_resolve()
                    .void()
            }

            fn insert_optical(
                &mut self,
                i: MeasIndex,
                m: $o,
                name: Shortname,
                r: BigDecimal,
                notrunc: bool,
            ) -> PyResult<()> {
                self.0
                    .insert_optical(i, name.into(), m.into(), Range(r), notrunc)
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
    ($($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                fn set_measurement_shortnames_maybe(
                    &mut self,
                    names: Vec<Option<Shortname>>,
                ) -> PyResult<()> {
                    self.0
                        .set_measurement_shortnames_maybe(names)
                        .map_err(|e| PyreflowException::new_err(e.to_string()))
                        .void()
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
            fn get_all_transforms(&self) -> Vec<ScaleTransform> {
                self.0.get_all_transforms().collect()
            }

            #[getter]
            fn get_transforms(&self) -> Vec<(MeasIndex, ScaleTransform)> {
                self.0.get_optical().map(|(i, &s)| (i, s)).collect()
            }

            #[setter]
            fn set_transforms(&mut self, xs: Vec<ScaleTransform>) -> PyResult<()> {
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
                fn get_timestep(&self) -> Option<Timestep> {
                    self.0.timestep().copied()
                }

                #[setter]
                fn set_timestep(&mut self, ts: Timestep) -> bool {
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
    Wavelength,
    PyCoreTEXT2_0,
    PyCoreTEXT3_0,
    PyCoreDataset2_0,
    PyCoreDataset3_0
);

// Get/set methods for vector $PnL (3.1-3.2)
get_set_all_optical!(
    get_wavelengths,
    set_wavelengths,
    Wavelengths,
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_1,
    PyCoreDataset3_2
);

// Get/set methods for $LAST_MODIFIER/$LAST_MODIFIED/$ORIGINALITY (3.1-3.2)
macro_rules! modification_methods {
    ($($pytype:ident),+) => {
        get_set_metaroot_opt!(get_originality,   set_originality,   Originality,      $($pytype),*);
        get_set_metaroot_opt!(get_last_modified, set_last_modified, ModifiedDateTime, $($pytype),*);
        get_set_metaroot_opt!(get_last_modifier, set_last_modifier, LastModifier,     $($pytype),*);
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
        get_set_metaroot_opt!(get_carriertype, set_carriertype, Carriertype, $($pytype),*);
        get_set_metaroot_opt!(get_carrierid,   set_carrierid,   Carrierid,   $($pytype),*);
        get_set_metaroot_opt!(get_locationid,  set_locationid,  Locationid,  $($pytype),*);
    };
}

carrier_methods!(PyCoreTEXT3_2, PyCoreDataset3_2);

// Get/set methods for $PLATEID/$WELLID/$PLATENAME (3.1-3.2)
macro_rules! plate_methods {
    ($($pytype:ident),*) => {
        get_set_metaroot_opt!(get_wellid,    set_wellid,    Wellid,    $($pytype),*);
        get_set_metaroot_opt!(get_plateid,   set_plateid,   Plateid,   $($pytype),*);
        get_set_metaroot_opt!(get_platename, set_platename, Platename, $($pytype),*);
    };
}

plate_methods!(
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_1,
    PyCoreDataset3_2
);

// get/set methods for $COMP (2.0-3.0)
macro_rules! comp_methods {
    ($($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn get_compensation<'a>(&self, py: Python<'a>) -> Option<Bound<'a, PyArray2<f32>>> {
                    self.0.compensation().map(|x| x.to_pyarray(py))
                }


                fn set_compensation(
                    &mut self,
                    a: PyReadonlyArray2<f32>,
                ) -> Result<(), PyErr> {
                    let m = a.as_matrix().into_owned();
                    self.0
                        .set_compensation(m)
                        .map_err(|e| PyreflowException::new_err(e.to_string()))
                }

                fn unset_compensation(&mut self) {
                    self.0.unset_compensation()
                }
            }
        )*
    };
}

comp_methods!(
    PyCoreTEXT2_0,
    PyCoreTEXT3_0,
    PyCoreDataset2_0,
    PyCoreDataset3_0
);

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
            ) -> Result<(), PyErr> {
                let m = a.as_matrix().into_owned();
                self.0
                    .set_spillover(names, m)
                    // TODO handle error better
                    .map_err(|e| PyreflowException::new_err(e.to_string()))
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
    get_vol,
    set_vol,
    Vol,
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
    Cyt,
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
    Flowrate,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $CYTSN (3.0-3.2)
get_set_metaroot_opt!(
    get_cytsn,
    set_cytsn,
    Cytsn,
    PyCoreTEXT3_0,
    PyCoreTEXT3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_0,
    PyCoreDataset3_1,
    PyCoreDataset3_2
);

// Get/set methods for $CYT (required) (3.2)
get_set_metaroot!(get_cyt, set_cyt, Cyt, PyCoreTEXT3_2, PyCoreDataset3_2);

// Get/set methods for $UNSTAINEDINFO (3.2)
get_set_metaroot_opt!(
    get_unstainedinfo,
    set_unstainedinfo,
    UnstainedInfo,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnD (3.1+)
//
// This is valid for the time channel so don't set on just optical
get_set_all_meas!(
    get_displays,
    set_displays,
    Display,
    PyCoreTEXT3_1,
    PyCoreDataset3_1,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnDET (3.2)
get_set_all_optical!(
    get_detector_names,
    set_detector_names,
    DetectorName,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnCALIBRATION (3.1)
get_set_all_optical!(
    get_calibrations,
    set_calibrations,
    Calibration3_1,
    PyCoreTEXT3_1,
    PyCoreDataset3_1
);

// Get/set methods for $PnCALIBRATION (3.2)
get_set_all_optical!(
    get_calibrations,
    set_calibrations,
    Calibration3_2,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnTAG (3.2)
get_set_all_optical!(get_tags, set_tags, Tag, PyCoreTEXT3_2, PyCoreDataset3_2);

// Get/set methods for $PnTYPE (3.2)
get_set_all_optical!(
    get_measurement_types,
    set_measurement_types,
    OpticalType,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnFEATURE (3.2)
get_set_all_optical!(
    get_features,
    set_features,
    Feature,
    PyCoreTEXT3_2,
    PyCoreDataset3_2
);

// Get/set methods for $PnANALYTE (3.2)
get_set_all_optical!(
    get_analytes,
    set_analytes,
    Analyte,
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
                analysis: Analysis,
                others: Others,
            ) -> PyResult<$to> {
                self.0
                    .clone()
                    .into_coredataset(cols, analysis, others)
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

// TODO there might a more natural way to emit all these warnings when
// converting from a rust type to a python type, that way I don't need to call
// this repeatedly
fn handle_warnings<X, W>(t: Terminal<X, W>) -> PyResult<X>
where
    W: fmt::Display,
{
    let (x, warn_res) = t.resolve(emit_warnings);
    warn_res?;
    Ok(x)
}

fn emit_warnings<W>(ws: Vec<W>) -> PyResult<()>
where
    W: fmt::Display,
{
    Python::with_gil(|py| -> PyResult<()> {
        let wt = py.get_type::<PyreflowWarning>();
        for w in ws {
            let s = CString::new(w.to_string())?;
            PyErr::warn(py, &wt, &s, 0)?;
        }
        Ok(())
    })
}

// TODO use warnings_are_errors flag
// TODO python has a way of handling multiple exceptions (ExceptionGroup)
// starting in 3.11
fn handle_failure<W, E, T>(f: TerminalFailure<W, E, T>) -> PyErr
where
    E: fmt::Display,
    T: fmt::Display,
    W: fmt::Display,
{
    let (warn_res, e) = f.resolve(emit_warnings, emit_failure);
    if let Err(w) = warn_res {
        w
    } else {
        e
    }
}

fn handle_failure_nowarn<E, T>(f: TerminalFailure<(), E, T>) -> PyErr
where
    E: fmt::Display,
    T: fmt::Display,
{
    f.resolve(|_| (), emit_failure).1
}

fn emit_failure<E, T>(es: NonEmpty<E>, r: T) -> PyErr
where
    E: fmt::Display,
    T: fmt::Display,
{
    let s = {
        let xs: Vec<_> = [format!("Toplevel Error: {r}")]
            .into_iter()
            .chain(es.into_iter().map(|x| x.to_string()))
            .collect();
        xs[..].join("\n").to_string()
    };
    PyreflowException::new_err(s)
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

#[pymethods]
impl PyOptical2_0 {
    #[new]
    fn new() -> Self {
        Optical2_0::default().into()
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
        Optical3_0::new(scale).into()
    }
}

#[pymethods]
impl PyOptical3_1 {
    #[new]
    fn new(scale: Scale) -> Self {
        Optical3_1::new(scale).into()
    }
}

#[pymethods]
impl PyOptical3_2 {
    #[new]
    fn new(scale: Scale) -> Self {
        Optical3_2::new(scale).into()
    }
}

#[pymethods]
impl PyTemporal2_0 {
    #[new]
    fn new() -> Self {
        Temporal2_0::default().into()
    }
}

#[pymethods]
impl PyTemporal3_0 {
    #[new]
    fn new(timestep: Timestep) -> Self {
        Temporal3_0::new(timestep).into()
    }
}

#[pymethods]
impl PyTemporal3_1 {
    #[new]
    fn new(timestep: Timestep) -> Self {
        Temporal3_1::new(timestep).into()
    }
}

#[pymethods]
impl PyTemporal3_2 {
    #[new]
    fn new(timestep: Timestep) -> Self {
        Temporal3_2::new(timestep).into()
    }

    #[getter]
    fn get_measurement_type(&self) -> bool {
        self.0.specific.measurement_type.0.is_some()
    }

    #[setter]
    fn set_measurement_type(&mut self, x: bool) {
        self.0.specific.measurement_type = if x { Some(TemporalType) } else { None }.into();
    }
}

macro_rules! shared_meas_get_set {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            #[getter]
            fn longname(&self) -> Option<Longname> {
                self.0.common.longname.0.as_ref().cloned()
            }

            #[setter]
            fn set_longname(&mut self, x: Option<Longname>) {
                self.0.common.longname = x.into();
            }

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

macro_rules! get_set_meas {
    ($get:ident, $set:ident, $t:ident, $($pytype:ident),*) => {
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

macro_rules! optical_common {
    ($($pytype:ident),*) => {
        get_set_meas!(get_filter,           set_filter,           Filter,          $($pytype),*);
        get_set_meas!(get_detector_type,    set_detector_type,    DetectorType,    $($pytype),*);
        get_set_meas!(get_percent_emitted,  set_percent_emitted,  PercentEmitted,  $($pytype),*);
        get_set_meas!(get_detector_voltage, set_detector_voltage, DetectorVoltage, $($pytype),*);
        get_set_meas!(get_power,            set_power,            Power,           $($pytype),*);
    };
}

optical_common!(PyOptical2_0, PyOptical3_0, PyOptical3_1, PyOptical3_2);

// $PnE (3.0-3.2)
macro_rules! get_set_meas_transform {
    ($($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn get_transform(&self) -> ScaleTransform {
                    self.0.specific.scale
                }

                #[setter]
                fn set_transform(&mut self, x: ScaleTransform) {
                    self.0.specific.scale = x;
                }
            }
        )*
    };
}

get_set_meas_transform!(PyOptical3_0, PyOptical3_1, PyOptical3_2);

// $PnL (2.0/3.0)
get_set_meas!(
    get_wavelength,
    set_wavelength,
    Wavelength,
    PyOptical2_0,
    PyOptical3_0
);

// #PnL (3.1-3.2)
get_set_meas!(
    get_wavelength,
    set_wavelength,
    Wavelengths,
    PyOptical3_1,
    PyOptical3_2
);

// #TIMESTEP (3.0-3.2)
macro_rules! meas_get_set_timestep {
    ($($pytype:ident),*) => {
        $(
            #[pymethods]
            impl $pytype {
                #[getter]
                fn get_timestep(&self) -> Timestep {
                    self.0.specific.timestep
                }

                #[setter]
                fn set_timestep(&mut self, x: Timestep) {
                    self.0.specific.timestep = x
                }
            }
        )*
    };
}

meas_get_set_timestep!(PyTemporal3_0, PyTemporal3_1, PyTemporal3_2);

// $PnCalibration (3.1)
get_set_meas!(
    get_calibration,
    set_calibration,
    Calibration3_1,
    PyOptical3_1
);

// $PnD (3.1-3.2)
get_set_meas!(
    get_display,
    set_display,
    Display,
    PyOptical3_1,
    PyOptical3_2,
    PyTemporal3_1,
    PyTemporal3_2
);

// $PnDET (3.2)
get_set_meas!(get_det, set_det, DetectorName, PyOptical3_2);

// $PnTAG (3.2)
get_set_meas!(get_tag, set_tag, Tag, PyOptical3_2);

// $PnTYPE (3.2)
get_set_meas!(
    get_measurement_type,
    set_measurement_type,
    OpticalType,
    PyOptical3_2
);

// $PnFEATURE (3.2)
get_set_meas!(get_feature, set_feature, Feature, PyOptical3_2);

// $PnANALYTE (3.2)
get_set_meas!(get_analyte, set_analyte, Analyte, PyOptical3_2);

// $PnCalibration (3.2)
get_set_meas!(
    get_calibration,
    set_calibration,
    Calibration3_2,
    PyOptical3_2
);

#[derive(Display, From)]
struct PyKeyLengthError(KeyLengthError);

impl From<PyKeyLengthError> for PyErr {
    fn from(value: PyKeyLengthError) -> Self {
        PyreflowException::new_err(value.to_string())
    }
}

#[derive(Display, From)]
struct PyReversedTimestamps(ReversedTimestamps);

impl From<PyReversedTimestamps> for PyErr {
    fn from(value: PyReversedTimestamps) -> Self {
        PyreflowException::new_err(value.to_string())
    }
}

#[derive(Display, From)]
struct PyReversedDatetimes(ReversedDatetimes);

impl From<PyReversedDatetimes> for PyErr {
    fn from(value: PyReversedDatetimes) -> Self {
        PyreflowException::new_err(value.to_string())
    }
}

trait PyTerminalResultExt {
    type V;

    fn py_term_resolve(self) -> PyResult<Self::V>;
}

impl<V, W: fmt::Display, E: fmt::Display, T: fmt::Display> PyTerminalResultExt
    for TerminalResult<V, W, E, T>
{
    type V = V;

    fn py_term_resolve(self) -> PyResult<Self::V> {
        self.map_or_else(|e| Err(handle_failure(e)), handle_warnings)
    }
}

trait PyTerminalNoWarnResultExt {
    type V;

    fn py_term_resolve_nowarn(self) -> PyResult<Self::V>;
}

impl<V, E: fmt::Display, T: fmt::Display> PyTerminalNoWarnResultExt
    for TerminalResult<V, (), E, T>
{
    type V = V;

    fn py_term_resolve_nowarn(self) -> PyResult<Self::V> {
        self.map_err(handle_failure_nowarn).map(|x| x.inner())
    }
}
