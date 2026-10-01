use crate::keys::AnyKey;

use nonempty::{IntoIteratorExt as _, NEVec, NonEmptyIterator as _};

use derive_more::{AsRef, Display, From};
use hashbrown::HashMap;
use hashbrown::hash_map::{IntoIter, Iter};
use itertools::Itertools as _;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    pyo3::prelude::*,
};

/// A map of pseudo(std) to std  key pairs.
///
/// The main use case for this is to rename keys.
///
/// This will be validated such that no pair has matching source and
/// destination.
#[derive(Clone, Debug, Default, AsRef, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct KeyPairs(HashMap<AnyKey, AnyKey>);

impl KeyPairs {
    fn iter(&self) -> Iter<'_, AnyKey, AnyKey> {
        (&self.0).into_iter()
    }
}

impl IntoIterator for KeyPairs {
    type Item = (AnyKey, AnyKey);
    type IntoIter = IntoIter<AnyKey, AnyKey>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a KeyPairs {
    type Item = (&'a AnyKey, &'a AnyKey);
    type IntoIter = Iter<'a, AnyKey, AnyKey>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl TryFrom<HashMap<AnyKey, AnyKey>> for KeyPairs {
    type Error = KeyPairsError;

    fn try_from(value: HashMap<AnyKey, AnyKey>) -> Result<Self, Self::Error> {
        if let Some(ne) = value.values().duplicates().try_into_nonempty_iter() {
            return Err(KeyNonUniqueError(ne.cloned().collect()).into());
        }
        if let Some(ne) = value
            .iter()
            .filter(|(k, v)| k == v)
            .map(|(k, _)| k.clone())
            .try_into_nonempty_iter()
        {
            Err(KeyMatchingKeyValueError(ne.collect()).into())
        } else {
            Ok(Self(value))
        }
    }
}

/// Error when building [`KeyPairs`] from configuration
#[derive(Error, Display, Debug, PartialEq, Clone, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum KeyPairsError {
    Matching(KeyMatchingKeyValueError),
    NonUnique(KeyNonUniqueError),
}

/// Error when key and value in [`KeyPairs`] matches
#[derive(Error, Debug, PartialEq, Clone)]
#[error("the following keys are paired with themselves: {}", .0.iter().join(","))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct KeyMatchingKeyValueError(NEVec<AnyKey>);

/// Error when values in [`KeyPairs`] are not unique
#[derive(Error, Debug, PartialEq, Clone)]
#[error("the following values are not unique: {}", .0.iter().join(","))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct KeyNonUniqueError(NEVec<AnyKey>);

#[cfg(feature = "python")]
mod python {
    use crate::keys::AnyKey;

    use super::KeyPairs;

    use hashbrown::HashMap;
    use pyo3::prelude::*;

    impl<'py> FromPyObject<'_, 'py> for KeyPairs {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let xs: HashMap<AnyKey, AnyKey> = obj.extract()?;
            let ret = xs.try_into()?;
            Ok(ret)
        }
    }
}
