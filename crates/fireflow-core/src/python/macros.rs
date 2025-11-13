macro_rules! impl_pyreflow_err {
    ($e:ident, $t:path) => {
        impl From<$t> for pyo3::PyErr {
            fn from(value: $t) -> Self {
                crate::python::exceptions::$e::new_err(value.to_string())
            }
        }
    };
}

pub(crate) use impl_pyreflow_err;
