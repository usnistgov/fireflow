use crate::validated::keys::KeyString;

use derive_more::AsRef;
use hashbrown::{HashMap, hash_map::IntoIter};
use itertools::Itertools as _;
use nonempty_collections::NEVec;
use thiserror::Error;

#[cfg(feature = "python")]
use {fireflow_core_proc::DisplayAsPyErr, pyo3::prelude::*};

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

// TODO also ensure that destination keys are all unique so we never get
// collisions
impl TryFrom<HashMap<KeyString, KeyString>> for KeyStringPairs {
    type Error = KeyStringPairsError;

    fn try_from(value: HashMap<KeyString, KeyString>) -> Result<Self, Self::Error> {
        let mut names = vec![];
        for (k, v) in &value {
            if k == v {
                names.push(k.clone());
            }
        }
        if let Ok(ns) = NEVec::try_from(names) {
            Err(KeyStringPairsError(ns))
        } else {
            Ok(Self(value))
        }
    }
}

/// Error when building [`KeyStringPairs`] from configuration
#[derive(Error, Debug, PartialEq, Clone)]
#[error("the following keys are paired with themselves: {}", .0.iter().join(","))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(fireflow_types::python::ConfigError))]
pub struct KeyStringPairsError(NEVec<KeyString>);

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
