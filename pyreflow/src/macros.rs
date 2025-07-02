macro_rules! py_wrap {
    ($v:vis$pytype:ident, $rstype:path, $name:expr) => {
        #[pyclass(name = $name)]
        #[derive(Clone)]
        #[repr(transparent)]
        $v struct $pytype($rstype);

        impl From<$rstype> for $pytype {
            fn from(value: $rstype) -> Self {
                Self(value)
            }
        }

        impl From<$pytype> for $rstype {
            fn from(value: $pytype) -> Self {
                value.0
            }
        }
    };
}

pub(crate) use py_wrap;

macro_rules! py_eq {
    ($pytype:ident) => {
        impl PartialEq for $pytype {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }

        impl Eq for $pytype {}

        #[pymethods]
        impl $pytype {
            fn __eq__(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }
    };
}

pub(crate) use py_eq;

macro_rules! py_ord {
    ($pytype:ident) => {
        impl PartialEq for $pytype {
            fn eq(&self, other: &Self) -> bool {
                self.0 == other.0
            }
        }

        impl Eq for $pytype {}

        impl Ord for $pytype {
            fn cmp(&self, other: &Self) -> Ordering {
                self.0.cmp(&other.0)
            }
        }

        impl PartialOrd for $pytype {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.0.cmp(&other.0))
            }
        }

        #[pymethods]
        impl $pytype {
            fn __richcmp__(&self, other: &Self, op: CompareOp) -> bool {
                op.matches(self.0.cmp(&other.0))
            }
        }
    };
}

pub(crate) use py_ord;

macro_rules! py_disp {
    ($pytype:ident) => {
        impl std::fmt::Display for $pytype {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
                self.0.fmt(f)
            }
        }

        #[pymethods]
        impl $pytype {
            fn __repr__(&self) -> String {
                let name = <$pytype as pyo3::type_object::PyTypeInfo>::NAME;
                format!("{}({})", name, self.0.to_string())
            }
        }
    };
}

pub(crate) use py_disp;

macro_rules! py_enum {
    ($pytype:ident, $rstype:ident, $([$var:ident, $method:ident]),*) => {
        #[pymethods]
        impl $pytype {
            $(
                #[allow(non_snake_case)]
                #[classattr]
                fn $method() -> Self {
                    $rstype::$var.into()
                }
            )*

        }
    };
}

pub(crate) use py_enum;

macro_rules! py_parse {
    ($pytype:ident) => {
        #[pymethods]
        impl $pytype {
            #[classmethod]
            fn from_raw_string(_: &Bound<'_, PyType>, s: String) -> PyResult<Self> {
                s.parse()
                    .map(Self)
                    .map_err(|e| PyreflowException::new_err(e.to_string()))
            }
        }
    };
}

pub(crate) use py_parse;
