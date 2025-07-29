use fireflow_core::python::exceptions::{PyreflowException, PyreflowWarning};
use fireflow_python as ff;

use pyo3::prelude::*;
use pyo3::wrap_pymodule;

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

    m.add_class::<ff::PyAsciiFixedLayout>()?;
    m.add_class::<ff::PyAsciiDelimLayout>()?;
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

    m.add_wrapped(wrap_pymodule!(_api))?;

    Ok(())
}

#[pymodule]
fn _api(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(ff::py_fcs_read_header, m)?)?;
    m.add_function(wrap_pyfunction!(ff::py_fcs_read_raw_text, m)?)?;
    m.add_function(wrap_pyfunction!(ff::py_fcs_read_std_text, m)?)?;
    m.add_function(wrap_pyfunction!(ff::py_fcs_read_std_dataset, m)?)?;
    m.add_function(wrap_pyfunction!(ff::py_fcs_read_raw_dataset, m)?)?;
    m.add_function(wrap_pyfunction!(
        ff::py_fcs_read_raw_dataset_with_keywords,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        ff::py_fcs_read_std_dataset_with_keywords,
        m
    )?)?;

    Ok(())
}
