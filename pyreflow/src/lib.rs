use fireflow_core::python::api;
use fireflow_core::python::core;
use fireflow_core::python::data;
use fireflow_core::python::exceptions::{PyreflowException, PyreflowWarning};

use pyo3::prelude::*;

#[pymodule]
fn pyreflow(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("PyreflowException", py.get_type::<PyreflowException>())?;
    m.add("PyreflowWarning", py.get_type::<PyreflowWarning>())?;

    m.add_class::<core::PyCoreTEXT2_0>()?;
    m.add_class::<core::PyCoreTEXT3_0>()?;
    m.add_class::<core::PyCoreTEXT3_1>()?;
    m.add_class::<core::PyCoreTEXT3_2>()?;

    m.add_class::<core::PyCoreDataset2_0>()?;
    m.add_class::<core::PyCoreDataset3_0>()?;
    m.add_class::<core::PyCoreDataset3_1>()?;
    m.add_class::<core::PyCoreDataset3_2>()?;

    m.add_class::<core::PyOptical2_0>()?;
    m.add_class::<core::PyOptical3_0>()?;
    m.add_class::<core::PyOptical3_1>()?;
    m.add_class::<core::PyOptical3_2>()?;

    m.add_class::<core::PyTemporal2_0>()?;
    m.add_class::<core::PyTemporal3_0>()?;
    m.add_class::<core::PyTemporal3_1>()?;
    m.add_class::<core::PyTemporal3_2>()?;

    m.add_class::<data::PyAsciiFixedLayout>()?;
    m.add_class::<data::PyAsciiDelimLayout>()?;
    m.add_class::<data::PyOrderedUint08Layout>()?;
    m.add_class::<data::PyOrderedUint16Layout>()?;
    m.add_class::<data::PyOrderedUint24Layout>()?;
    m.add_class::<data::PyOrderedUint32Layout>()?;
    m.add_class::<data::PyOrderedUint40Layout>()?;
    m.add_class::<data::PyOrderedUint48Layout>()?;
    m.add_class::<data::PyOrderedUint56Layout>()?;
    m.add_class::<data::PyOrderedUint64Layout>()?;
    m.add_class::<data::PyOrderedF32Layout>()?;
    m.add_class::<data::PyOrderedF64Layout>()?;
    m.add_class::<data::PyEndianF32Layout>()?;
    m.add_class::<data::PyEndianF64Layout>()?;
    m.add_class::<data::PyEndianUintLayout>()?;
    m.add_class::<data::PyMixedLayout>()?;

    m.add_function(wrap_pyfunction!(api::py_fcs_read_header, m)?)?;
    m.add_function(wrap_pyfunction!(api::py_fcs_read_raw_text, m)?)?;
    m.add_function(wrap_pyfunction!(api::py_fcs_read_std_text, m)?)?;
    m.add_function(wrap_pyfunction!(api::py_fcs_read_std_dataset, m)?)?;
    m.add_function(wrap_pyfunction!(api::py_fcs_read_raw_dataset, m)?)?;
    m.add_function(wrap_pyfunction!(
        api::py_fcs_read_raw_dataset_with_keywords,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(
        api::py_fcs_read_std_dataset_with_keywords,
        m
    )?)
}
