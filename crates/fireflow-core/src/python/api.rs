use crate::api::*;
use crate::config::*;
use crate::header::{Header, Version};
use crate::segment::{HeaderAnalysisSegment, HeaderDataSegment, OtherSegment};
use crate::validated::keys::{StdKeywords, ValidKeywords};

use super::core::{PyAnyCoreDataset, PyAnyCoreTEXT};
use super::exceptions::{PyTerminalNoWarnResultExt, PyTerminalResultExt};

use pyo3::prelude::*;
use std::path::PathBuf;

#[pyfunction]
#[pyo3(name = "fcs_read_header")]
pub fn py_fcs_read_header(p: PathBuf, conf: ReadHeaderConfig) -> PyResult<Header> {
    fcs_read_header(&p, &conf).py_term_resolve_nowarn()
}

#[pyfunction]
#[pyo3(name = "fcs_read_raw_text")]
pub fn py_fcs_read_raw_text(p: PathBuf, conf: ReadRawTEXTConfig) -> PyResult<RawTEXTOutput> {
    fcs_read_raw_text(&p, &conf).py_term_resolve()
}

#[pyfunction]
#[pyo3(name = "fcs_read_std_text")]
pub fn py_fcs_read_std_text(
    p: PathBuf,
    conf: ReadStdTEXTConfig,
) -> PyResult<(PyAnyCoreTEXT, StdTEXTOutput)> {
    let (core, data) = fcs_read_std_text(&p, &conf).py_term_resolve()?;
    Ok((core.into(), data))
}

#[pyfunction]
#[pyo3(name = "fcs_read_raw_dataset")]
pub fn py_fcs_read_raw_dataset(
    p: PathBuf,
    conf: ReadRawDatasetConfig,
) -> PyResult<RawDatasetOutput> {
    fcs_read_raw_dataset(&p, &conf).py_term_resolve()
}

#[pyfunction]
#[pyo3(name = "fcs_read_std_dataset")]
pub fn py_fcs_read_std_dataset(
    p: PathBuf,
    conf: ReadStdDatasetConfig,
) -> PyResult<(PyAnyCoreDataset, StdDatasetOutput)> {
    let (core, data) = fcs_read_std_dataset(&p, &conf).py_term_resolve()?;
    Ok((core.into(), data))
}

#[pyfunction]
#[pyo3(name = "fcs_read_raw_dataset_with_keywords")]
pub fn py_fcs_read_raw_dataset_with_keywords(
    p: PathBuf,
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
pub fn py_fcs_read_std_dataset_with_keywords(
    p: PathBuf,
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
