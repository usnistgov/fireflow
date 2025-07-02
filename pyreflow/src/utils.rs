use fireflow_core::text::float_or_int::{FloatOrInt, NonNanF64};

use crate::class::PyreflowException;

use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

pub(crate) fn any_to_float_or_int(a: Bound<'_, PyAny>) -> PyResult<FloatOrInt> {
    a.clone()
        .extract::<f64>()
        .map_or(a.extract::<u64>().map(|x| x.into()), |x| {
            NonNanF64::try_from(x)
                .map(FloatOrInt::Float)
                .map_err(|e| PyreflowException::new_err(e.to_string()))
        })
}

pub(crate) fn float_or_int_to_any(r: FloatOrInt, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    match r {
        FloatOrInt::Float(x) => f64::from(x).into_bound_py_any(py),
        FloatOrInt::Int(x) => x.into_bound_py_any(py),
    }
}
