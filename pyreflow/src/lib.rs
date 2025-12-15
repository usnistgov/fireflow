use fireflow_python as ff;

use pyo3::prelude::*;

#[pymodule]
fn _pyreflow(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    macro_rules! exc {
        ($s:expr, $t:ident) => {
            m.add($s, py.get_type::<fireflow_core::python::$t>())?;
        };
    }

    exc!("PyreflowError", PyreflowError);
    exc!("FileLayoutError", FileLayoutError);
    exc!("ParseKeyError", ParseKeyError);
    exc!("ParseKeywordValueError", ParseKeywordValueError);
    exc!("InvalidKeywordValueError", InvalidKeywordValueError);
    exc!("ExtraKeywordError", ExtraKeywordError);
    exc!("FCSDeprecatedError", FCSDeprecatedError);
    exc!("ConversionError", ConversionError);
    exc!("RelationalError", RelationalError);
    exc!("EventDataError", EventDataError);
    exc!("DataLossError", DataLossError);
    exc!("ConfigError", ConfigError);
    exc!("PyreflowWarning", PyreflowWarning);

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

    m.add_class::<ff::PyFlatTEXTOutput>()?;
    m.add_class::<ff::PyFlatDatasetOutput>()?;
    m.add_class::<ff::PyFlatDatasetWithKwsOutput>()?;

    m.add_class::<ff::PyStdTEXTOutput>()?;
    m.add_class::<ff::PyStdDatasetOutput>()?;
    m.add_class::<ff::PyStdDatasetWithKwsOutput>()?;

    m.add_class::<ff::PyFlatTEXTParseData>()?;
    m.add_class::<ff::PyExtraStdKeywords>()?;
    m.add_class::<ff::PyValidKeywords>()?;
    m.add_class::<ff::PyDatasetSegments>()?;
    m.add_class::<ff::PyDatasetSummary>()?;

    m.add_function(wrap_pyfunction!(ff::fcs_read_header, m)?)?;
    m.add_function(wrap_pyfunction!(ff::fcs_read_flat_text, m)?)?;
    m.add_function(wrap_pyfunction!(ff::fcs_read_std_text, m)?)?;
    m.add_function(wrap_pyfunction!(ff::fcs_read_flat_dataset, m)?)?;
    m.add_function(wrap_pyfunction!(ff::fcs_read_std_dataset, m)?)?;
    m.add_function(wrap_pyfunction!(ff::fcs_read_flat_texts, m)?)?;
    m.add_function(wrap_pyfunction!(ff::fcs_read_std_texts, m)?)?;
    m.add_function(wrap_pyfunction!(ff::fcs_read_flat_datasets, m)?)?;
    m.add_function(wrap_pyfunction!(ff::fcs_read_std_datasets, m)?)?;
    m.add_function(wrap_pyfunction!(
        ff::fcs_read_flat_dataset_with_keywords,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(ff::fcs_summarize, m)?)?;

    Ok(())
}
