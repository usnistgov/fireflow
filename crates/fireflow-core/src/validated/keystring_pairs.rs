use crate::validated::keys::KeyString;

use derive_more::{AsRef, Display, From};
use hashbrown::{HashMap, hash_map::IntoIter};
use itertools::Itertools as _;
use nonempty_collections::{IntoIteratorExt as _, NEVec, NonEmptyIterator as _};
use thiserror::Error;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    pyo3::prelude::*,
};

/// A map of [`KeyString`]/[`KeyString`] pairs.
///
/// The main use case for this is to rename keys.
///
/// This will be validated such that no pair has matching source and
/// destination.
#[derive(Clone, Debug, Default, AsRef, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct KeyStringPairs(HashMap<KeyString, KeyString>);

impl IntoIterator for KeyStringPairs {
    type Item = (KeyString, KeyString);
    type IntoIter = IntoIter<KeyString, KeyString>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl TryFrom<HashMap<KeyString, KeyString>> for KeyStringPairs {
    type Error = KeyStringPairsError;

    fn try_from(value: HashMap<KeyString, KeyString>) -> Result<Self, Self::Error> {
        if let Some(ne) = value.values().duplicates().try_into_nonempty_iter() {
            return Err(KeyStringNonUniqueError(ne.cloned().collect()).into());
        }
        let mut names = vec![];
        for (k, v) in &value {
            if k == v {
                names.push(k.clone());
            }
        }
        if let Ok(ns) = NEVec::try_from(names) {
            Err(KeyStringMatchingKeyValueError(ns).into())
        } else {
            Ok(Self(value))
        }
    }
}

/// Error when building [`KeyStringPairs`] from configuration
#[derive(Error, Display, Debug, PartialEq, Clone, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum KeyStringPairsError {
    Matching(KeyStringMatchingKeyValueError),
    NonUnique(KeyStringNonUniqueError),
}

/// Error when key and value in [`KeyStringPairs`] matches
#[derive(Error, Debug, PartialEq, Clone)]
#[error("the following keys are paired with themselves: {}", .0.iter().join(","))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(fireflow_types::python::ConfigError))]
pub struct KeyStringMatchingKeyValueError(NEVec<KeyString>);

/// Error when values in [`KeyStringPairs`] are not unique
#[derive(Error, Debug, PartialEq, Clone)]
#[error("the following value are not unique: {}", .0.iter().join(","))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(fireflow_types::python::ConfigError))]
pub struct KeyStringNonUniqueError(NEVec<KeyString>);

#[cfg(feature = "python")]
mod python {
    use super::KeyStringPairs;
    use crate::validated::keys::KeyString;

    use hashbrown::HashMap;
    use pyo3::prelude::*;

    impl<'py> FromPyObject<'_, 'py> for KeyStringPairs {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let xs: HashMap<KeyString, KeyString> = obj.extract()?;
            let ret = xs.try_into()?;
            Ok(ret)
        }
    }
}
