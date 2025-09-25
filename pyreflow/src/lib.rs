use fireflow_core::python::exceptions::{PyreflowException, PyreflowWarning};
use fireflow_python as ff;

use pyo3::prelude::*;
// use pyo3::wrap_pymodule;

#[pymodule]
fn _pyreflow(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    m.add("PyreflowException", py.get_type::<PyreflowException>())?;
    m.add("PyreflowWarning", py.get_type::<PyreflowWarning>())?;

    m.add_class::<ff::PyCoreTEXT2_0>()?;
    m.add_class::<ff::PyCoreTEXT3_0>()?;
    m.add_class::<ff::PyCoreTEXT3_1>()?;
    m.add_class::<ff::PyCoreTEXT3_2>()?;

    m.add_class::<ff::PyCoreDataset2_0>()?;
    m.add_class::<ff::PyCoreDataset3_0>()?;
    m.add_class::<ff::PyCoreDataset3_1>()?;
    m.add_class::<ff::PyCoreDataset3_2>()?;

    m.add_class::<ff::PyOptical2_0>()?;
    m.add_class::<ff::PyOptical3_0>()?;
    m.add_class::<ff::PyOptical3_1>()?;
    m.add_class::<ff::PyOptical3_2>()?;

    m.add_class::<ff::PyTemporal2_0>()?;
    m.add_class::<ff::PyTemporal3_0>()?;
    m.add_class::<ff::PyTemporal3_1>()?;
    m.add_class::<ff::PyTemporal3_2>()?;

    m.add_class::<ff::PyUnivariateRegion2_0>()?;
    m.add_class::<ff::PyUnivariateRegion3_0>()?;
    m.add_class::<ff::PyUnivariateRegion3_2>()?;

    m.add_class::<ff::PyBivariateRegion2_0>()?;
    m.add_class::<ff::PyBivariateRegion3_0>()?;
    m.add_class::<ff::PyBivariateRegion3_2>()?;

    m.add_class::<ff::PyGatedMeasurement>()?;

    m.add_class::<ff::PyFixedAsciiLayout>()?;
    m.add_class::<ff::PyDelimAsciiLayout>()?;
    m.add_class::<ff::PyOrderedUint08Layout>()?;
    m.add_class::<ff::PyOrderedUint16Layout>()?;
    m.add_class::<ff::PyOrderedUint24Layout>()?;
    m.add_class::<ff::PyOrderedUint32Layout>()?;
    m.add_class::<ff::PyOrderedUint40Layout>()?;
    m.add_class::<ff::PyOrderedUint48Layout>()?;
    m.add_class::<ff::PyOrderedUint56Layout>()?;
    m.add_class::<ff::PyOrderedUint64Layout>()?;
    m.add_class::<ff::PyOrderedF32Layout>()?;
    m.add_class::<ff::PyOrderedF64Layout>()?;
    m.add_class::<ff::PyEndianF32Layout>()?;
    m.add_class::<ff::PyEndianF64Layout>()?;
    m.add_class::<ff::PyEndianUintLayout>()?;
    m.add_class::<ff::PyMixedLayout>()?;

    m.add_class::<ff::PyHeader>()?;
    m.add_class::<ff::PyHeaderSegments>()?;

    m.add_class::<ff::PyRawTEXTOutput>()?;
    m.add_class::<ff::PyRawDatasetOutput>()?;
    m.add_class::<ff::PyRawDatasetWithKwsOutput>()?;

    m.add_class::<ff::PyStdTEXTOutput>()?;
    m.add_class::<ff::PyStdDatasetOutput>()?;
    m.add_class::<ff::PyStdDatasetWithKwsOutput>()?;

    m.add_class::<ff::PyRawTEXTParseData>()?;
    m.add_class::<ff::PyExtraStdKeywords>()?;
    m.add_class::<ff::PyValidKeywords>()?;
    m.add_class::<ff::PyDatasetSegments>()?;

    m.add_function(wrap_pyfunction!(ff::fcs_read_header, m)?)?;
    m.add_function(wrap_pyfunction!(ff::fcs_read_raw_text, m)?)?;
    m.add_function(wrap_pyfunction!(ff::fcs_read_std_text, m)?)?;
    m.add_function(wrap_pyfunction!(ff::fcs_read_raw_dataset, m)?)?;
    m.add_function(wrap_pyfunction!(ff::fcs_read_std_dataset, m)?)?;
    m.add_function(wrap_pyfunction!(ff::fcs_read_raw_dataset_with_keywords, m)?)?;
    m.add_function(wrap_pyfunction!(ff::fcs_read_std_dataset_with_keywords, m)?)?;

    Ok(())
}
