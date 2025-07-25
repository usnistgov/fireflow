// Convert any error to a python ValueError using its display trait
macro_rules! impl_value_err {
    ($t:ident) => {
        impl From<$t> for pyo3::prelude::PyErr {
            fn from(value: $t) -> Self {
                pyo3::exceptions::PyValueError::new_err(value.to_string())
            }
        }
    };
}

pub(crate) use impl_value_err;

macro_rules! impl_index_err {
    ($t:ident) => {
        impl From<$t> for pyo3::prelude::PyErr {
            fn from(value: $t) -> Self {
                pyo3::exceptions::PyIndexError::new_err(value.to_string())
            }
        }
    };
}

pub(crate) use impl_index_err;

macro_rules! impl_pyreflow_err {
    ($t:ident) => {
        impl From<$t> for pyo3::PyErr {
            fn from(value: $t) -> Self {
                crate::python::exceptions::PyreflowException::new_err(value.to_string())
            }
        }
    };
}

pub(crate) use impl_pyreflow_err;

macro_rules! impl_try_from_py {
    ($t:ident, $inner:ident) => {
        impl<'py> pyo3::FromPyObject<'py> for $t {
            fn extract_bound(ob: &pyo3::Bound<'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
                let x: $inner = pyo3::prelude::PyAnyMethods::extract(ob)?;
                let y = x.try_into()?;
                Ok(y)
            }
        }
    };
}

pub(crate) use impl_try_from_py;

// Convert string to rust type using FromStr trait; useful for traits which
// may wrap String but have some custom validation for its contents.
macro_rules! impl_from_py_via_fromstr {
    ($t:ident) => {
        impl<'py> pyo3::conversion::FromPyObject<'py> for $t {
            fn extract_bound(ob: &pyo3::Bound<'py, pyo3::types::PyAny>) -> pyo3::PyResult<Self> {
                let x: String = pyo3::prelude::PyAnyMethods::extract(ob)?;
                let ret = x.parse()?;
                Ok(ret)
            }
        }
    };
}

pub(crate) use impl_from_py_via_fromstr;

// Convert rust type to python type using its Display trait
macro_rules! impl_to_py_via_display {
    ($t:ident) => {
        impl<'py> pyo3::conversion::IntoPyObject<'py> for $t {
            type Target = pyo3::types::PyString;
            type Output = pyo3::Bound<'py, Self::Target>;
            type Error = std::convert::Infallible;

            fn into_pyobject(
                self,
                py: pyo3::marker::Python<'py>,
            ) -> Result<Self::Output, Self::Error> {
                self.to_string().into_pyobject(py)
            }
        }
    };
}

pub(crate) use impl_to_py_via_display;

// macro_rules! impl_str_to_from_py {
//     ($t:ident) => {
//         impl_str_from_py!($t);
//         impl_str_to_py!($t);
//     };
// }

// pub(crate) use impl_str_to_from_py;
