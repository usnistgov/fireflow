use crate::std_key::{AnyStdKey, DollarAnyStdKey, DollarStdKey, PseudoStdKey, StdKey};

use nonempty::{IntoIteratorExt as _, NEVec, NonEmptyIterator as _};

use derive_more::{AsRef, Display, From, Into};
use hashbrown::{HashMap, hash_map::IntoIter};
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
pub struct KeyStringPairs(HashMap<DollarAnyStdKey, DollarStdKey>);

/// A map of std to std  key pairs.
///
/// Keys are validated not to collide with each other.
#[derive(Clone, Debug, Default, AsRef, PartialEq, Into)]
pub struct StdKeyStringPairs(Vec<(StdKey, StdKey)>);

/// A map of pseudo(std) to std  key pairs.
///
/// Keys are validated not to collide with each other.
#[derive(Clone, Debug, Default, AsRef, PartialEq, Into)]
pub struct PseudoStdKeyStringPairs<'a>(Vec<(&'a PseudoStdKey, StdKey)>);

impl IntoIterator for KeyStringPairs {
    type Item = (DollarAnyStdKey, DollarStdKey);
    type IntoIter = IntoIter<DollarAnyStdKey, DollarStdKey>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl TryFrom<HashMap<DollarAnyStdKey, DollarStdKey>> for KeyStringPairs {
    type Error = KeyStringPairsError;

    fn try_from(value: HashMap<DollarAnyStdKey, DollarStdKey>) -> Result<Self, Self::Error> {
        if let Some(ne) = value.values().duplicates().try_into_nonempty_iter() {
            return Err(KeyStringNonUniqueError(ne.cloned().collect()).into());
        }
        let mut names = vec![];
        for (k, v) in &value {
            if let AnyStdKey::Real(rk) = k.0
                && rk == v.0
            {
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

impl KeyStringPairs {
    // TODO this doesn't need to take ownership, I could use references downstream
    #[must_use]
    pub fn split(&self) -> (StdKeyStringPairs, PseudoStdKeyStringPairs<'_>) {
        let mut std = vec![];
        let mut pstd = vec![];
        for (k0, k1) in self.as_ref() {
            match &k0.0 {
                AnyStdKey::Real(k) => std.push((*k, k1.0)),
                AnyStdKey::Pseudo(k) => pstd.push((k, k1.0)),
            }
        }
        (StdKeyStringPairs(std), PseudoStdKeyStringPairs(pstd))
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
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct KeyStringMatchingKeyValueError(NEVec<DollarAnyStdKey>);

/// Error when values in [`KeyStringPairs`] are not unique
#[derive(Error, Debug, PartialEq, Clone)]
#[error("the following value are not unique: {}", .0.iter().join(","))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct KeyStringNonUniqueError(NEVec<DollarStdKey>);

#[cfg(feature = "python")]
mod python {
    use crate::std_key::{DollarAnyStdKey, DollarStdKey};

    use super::KeyStringPairs;

    use hashbrown::HashMap;
    use pyo3::prelude::*;

    impl<'py> FromPyObject<'_, 'py> for KeyStringPairs {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let xs: HashMap<DollarAnyStdKey, DollarStdKey> = obj.extract()?;
            let ret = xs.try_into()?;
            Ok(ret)
        }
    }
}
