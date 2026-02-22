//! A specialized version of `NonEmpty`

use derive_more::{From, Into};
use nonempty_collections::NEVec;

// A wrapper to bestow supernatural powers to "regular" non-empty. I may also
// make my own version of this so this makes that a bit easier if I end up
// deciding in favor.
#[derive(Into, From, PartialEq, Clone, Debug)]
pub struct FCSNonEmpty<T>(pub NEVec<T>);

#[cfg(feature = "serde")]
mod serialize {
    use super::FCSNonEmpty;
    use serde::{Serialize, ser::SerializeSeq as _};

    impl<I: Serialize> Serialize for FCSNonEmpty<I> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut seq = serializer.serialize_seq(Some(usize::from(self.0.len())))?;
            for e in &self.0 {
                seq.serialize_element(e)?;
            }
            seq.end()
        }
    }
}

#[cfg(feature = "python")]
mod python {
    use fireflow_types::python::InvalidKeywordValueError;

    use super::FCSNonEmpty;

    use nonempty_collections::NEVec;
    use pyo3::prelude::*;
    use pyo3::types::PyList;

    // NOTE this is only used for keywords that cannot be an empty list
    impl<'py, T> FromPyObject<'py> for FCSNonEmpty<T>
    where
        T: FromPyObject<'py>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let xs: Vec<T> = ob.extract()?;
            if let Ok(ys) = NEVec::try_from(xs) {
                Ok(ys.into())
            } else {
                Err(InvalidKeywordValueError::new_err("list must not be empty"))
            }
        }
    }

    impl<'py, T> IntoPyObject<'py> for FCSNonEmpty<T>
    where
        T: IntoPyObject<'py>,
    {
        type Target = PyList;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            PyList::new(py, Vec::from(self.0))
        }
    }
}
