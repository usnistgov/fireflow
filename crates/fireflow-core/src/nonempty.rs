//! A specialized version of `NonEmpty`

use derive_more::{From, Into};
use nonempty_collections::NEVec;

#[cfg(feature = "serde")]
use serde::Serialize;

// A wrapper to bestow supernatural powers to "regular" non-empty. I may also
// make my own version of this so this makes that a bit easier if I end up
// deciding in favor.
#[derive(Into, From, PartialEq, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "serde", serde(bound = "T: Serialize + Clone"))]
#[into(Vec<T>, NEVec<T>)]
pub struct FcsNEVec<T>(pub NEVec<T>);

#[cfg(feature = "python")]
mod python {
    use fireflow_types::python::InvalidKeywordValueError;

    use super::FcsNEVec;

    use nonempty_collections::NEVec;
    use pyo3::prelude::*;

    // NOTE this is only used for keywords that cannot be an empty list
    impl<'py, T> FromPyObject<'_, 'py> for FcsNEVec<T>
    where
        T: FromPyObjectOwned<'py>,
    {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let xs: Vec<T> = obj.extract()?;
            if let Ok(ys) = NEVec::try_from(xs) {
                Ok(ys.into())
            } else {
                Err(InvalidKeywordValueError::new_err("list must not be empty"))
            }
        }
    }

    impl<'py, T> IntoPyObject<'py> for FcsNEVec<T>
    where
        T: IntoPyObject<'py>,
    {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            Vec::from(self.0).into_pyobject(py)
        }
    }
}
