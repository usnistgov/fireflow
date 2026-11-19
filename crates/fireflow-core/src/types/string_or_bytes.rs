use crate::api::{TruncatedBytes, TruncatedNEBytes, TruncatedNEString, TruncatedString};

use nonempty::{NESlice, NEString, NEVec};

use derive_more::{AsRef, Display};

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use fireflow_core_proc::FromInnerPyObject;

/// A either a UTF-8 string or a non-UTF-8 byte sequence.
#[derive(Clone, Display, Debug, PartialEq, AsRef)]
#[as_ref([u8])]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct StringOrBytes(Utf8OrBytes<TruncatedString, TruncatedBytes>);

/// A either a UTF-8 string or a non-UTF-8 byte sequence (both non-empty).
#[derive(Clone, Display, Debug, PartialEq, AsRef)]
#[as_ref([u8])]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct NEStringOrBytes(Utf8OrBytes<TruncatedNEString, TruncatedNEBytes>);

#[derive(Clone, Display, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
enum Utf8OrBytes<U, B> {
    /// Valid Utf8
    Utf8(U),
    /// Not Utf8
    Bytes(B),
}

impl Default for StringOrBytes {
    fn default() -> Self {
        Self(Utf8OrBytes::Utf8(TruncatedString::default()))
    }
}

impl From<Vec<u8>> for StringOrBytes {
    fn from(value: Vec<u8>) -> Self {
        match String::from_utf8(value) {
            Ok(s) => Self(Utf8OrBytes::Utf8(TruncatedString(s))),
            Err(e) => Self(Utf8OrBytes::Bytes(TruncatedBytes(e.into_bytes()))),
        }
    }
}

impl From<NEStringOrBytes> for StringOrBytes {
    fn from(value: NEStringOrBytes) -> Self {
        match value.0 {
            Utf8OrBytes::Bytes(x) => Self(Utf8OrBytes::Bytes(x.into())),
            Utf8OrBytes::Utf8(x) => Self(Utf8OrBytes::Utf8(x.into())),
        }
    }
}

impl<'a> From<&'a NESlice<u8>> for NEStringOrBytes {
    fn from(value: &'a NESlice<u8>) -> Self {
        Self::from(value.to_ne_vec())
    }
}

impl From<NEVec<u8>> for NEStringOrBytes {
    fn from(value: NEVec<u8>) -> Self {
        match NEString::from_utf8(value) {
            Ok(s) => Self(Utf8OrBytes::Utf8(TruncatedNEString(s))),
            Err(e) => Self(Utf8OrBytes::Bytes(TruncatedNEBytes::from(e.into_bytes()))),
        }
    }
}

impl<U, B> AsRef<[u8]> for Utf8OrBytes<U, B>
where
    U: AsRef<[u8]>,
    B: AsRef<[u8]>,
{
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl StringOrBytes {
    pub(crate) fn into_ne(self) -> Option<NEStringOrBytes> {
        let ret = match self.0 {
            Utf8OrBytes::Bytes(x) => x.into_ne().map(Utf8OrBytes::Bytes),
            Utf8OrBytes::Utf8(x) => x.into_ne().map(Utf8OrBytes::Utf8),
        };
        ret.map(NEStringOrBytes)
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }
}

impl<U, B> Utf8OrBytes<U, B> {
    fn as_bytes(&self) -> &[u8]
    where
        U: AsRef<[u8]>,
        B: AsRef<[u8]>,
    {
        match self {
            Self::Bytes(x) => x.as_ref(),
            Self::Utf8(x) => x.as_ref(),
        }
    }

    fn len(&self) -> usize
    where
        U: AsRef<[u8]>,
        B: AsRef<[u8]>,
    {
        self.as_bytes().len()
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{NEStringOrBytes, StringOrBytes, Utf8OrBytes};

    use pyo3::IntoPyObjectExt as _;
    use pyo3::prelude::*;

    impl<'py, U, B> FromPyObject<'_, 'py> for Utf8OrBytes<U, B>
    where
        for<'a> U: FromPyObject<'a, 'py>,
        for<'a> B: FromPyObject<'a, 'py, Error = PyErr>,
    {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            // NOTE order matters, try to parse as string first and fall back to
            // non-string. This assumes the inner types follow this convention.
            if let Ok(u) = obj.extract() {
                Ok(Self::Utf8(u))
            } else {
                obj.extract::<B>().map(Self::Bytes)
            }
        }
    }

    impl<'py> IntoPyObject<'py> for StringOrBytes {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self.0 {
                Utf8OrBytes::Utf8(x) => x.into_bound_py_any(py),
                Utf8OrBytes::Bytes(x) => x.into_bound_py_any(py),
            }
        }
    }

    impl<'py> IntoPyObject<'py> for NEStringOrBytes {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self.0 {
                Utf8OrBytes::Utf8(x) => x.into_bound_py_any(py),
                Utf8OrBytes::Bytes(x) => x.into_bound_py_any(py),
            }
        }
    }
}
