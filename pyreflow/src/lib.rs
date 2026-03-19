use fireflow_python as ff;

use pyo3::prelude::*;

#[pymodule]
fn _pyreflow(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    macro_rules! exc {
        ($s:expr, $t:ident) => {
            m.add($s, py.get_type::<fireflow_types::python::$t>())?;
        };
    }

    exc!("PyreflowError", PyreflowError);
    exc!("FileLayoutError", FileLayoutError);
    exc!("ParseKeyError", ParseKeyError);
    exc!("ParseKeywordValueError", ParseKeywordValueError);
    exc!("InvalidKeywordValueError", InvalidKeywordValueError);
    exc!("ExtraKeywordError", ExtraKeywordError);
    exc!("ConversionError", ConversionError);
    exc!("RelationalError", RelationalError);
    exc!("EventDataError", EventDataError);
    exc!("DataLossError", DataLossError);
    exc!("ConfigError", ConfigError);
    exc!("WriteFCSError", WriteFCSError);
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
    m.add_class::<ff::PyParsedHeaderSegments>()?;
    m.add_class::<ff::PyUncorrectedHeaderSegments>()?;

    m.add_class::<ff::PyFlatTEXTOutput>()?;
    m.add_class::<ff::PyHeaderAndSuppOffsets>()?;
    m.add_class::<ff::PyFlatDatasetOutput>()?;
    m.add_class::<ff::PyFlatDatasetFromKwsOutput>()?;
    m.add_class::<ff::PyNewFlatDatasetFromKwsOutput>()?;

    m.add_class::<ff::PyStdTEXTOutput>()?;
    m.add_class::<ff::PyStdDatasetOutput>()?;
    m.add_class::<ff::PyStdDatasetFromKwsOutput>()?;
    m.add_class::<ff::PyNewStdDatasetFromKwsOutput>()?;
    m.add_class::<ff::PyEventsDiagnostics>()?;
    m.add_class::<ff::PyKeywordVersionScore>()?;

    m.add_class::<ff::PyFlatTEXTDiagnostics>()?;
    m.add_class::<ff::PySplitTEXTDiagnostics>()?;
    m.add_class::<ff::PyStdTEXTDiagnostics>()?;
    m.add_class::<ff::PyValidKeywords>()?;
    m.add_class::<ff::PyDatasetSegments>()?;
    m.add_class::<ff::PyDatasetSummary>()?;

    m.add_class::<ff::PyReadHeaderConfig>()?;
    m.add_class::<ff::PyReadFlatTEXTConfig>()?;
    m.add_class::<ff::PyReadStdTEXTConfig>()?;
    m.add_class::<ff::PyReadFlatDatasetConfig>()?;
    m.add_class::<ff::PyReadStdDatasetConfig>()?;
    m.add_class::<ff::PyReadFlatDatasetFromKeywordsConfig>()?;
    m.add_class::<ff::PyNewCoreTEXTConfig>()?;
    m.add_class::<ff::PyNewCoreDatasetConfig>()?;

    macro_rules! fun {
        ($t:ident) => {
            m.add_function(wrap_pyfunction!(ff::$t, m)?)?;
        };
    }

    fun!(fcs_read_header);
    fun!(fcs_read_flat_text);
    fun!(fcs_read_std_text);
    fun!(fcs_read_flat_dataset);
    fun!(fcs_read_std_dataset);
    fun!(fcs_read_flat_texts);
    fun!(fcs_read_std_texts);
    fun!(fcs_read_flat_datasets);
    fun!(fcs_read_std_datasets);
    fun!(fcs_read_flat_dataset_with_keywords);
    fun!(fcs_summarize);
    fun!(fcs_write_datasets);

    Ok(())
}
