use crate::validated::keys::KeyString;

use derive_more::AsRef;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use std::collections::HashMap;
use thiserror::Error;

#[cfg(feature = "python")]
use {fireflow_core_proc::DisplayAsPyErr, pyo3::prelude::*};

/// A map of [`KeyString`]/[`KeyString`] pairs.
///
/// The main use case for this is to rename keys.
///
/// This will be validated such that no pair has matching source and
/// destination.
#[derive(Clone, Debug, Default, AsRef)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct KeyStringPairs(HashMap<KeyString, KeyString>);

impl TryFrom<HashMap<KeyString, KeyString>> for KeyStringPairs {
    type Error = KeyStringPairsError;

    fn try_from(value: HashMap<KeyString, KeyString>) -> Result<Self, Self::Error> {
        let mut names = vec![];
        for (k, v) in &value {
            if k == v {
                names.push(k.clone());
            }
        }
        if let Some(ns) = NonEmpty::from_vec(names) {
            Err(KeyStringPairsError(ns))
        } else {
            Ok(Self(value))
        }
    }
}

/// Error when building [`KeyStringPairs`] from configuration
#[derive(Error, Debug)]
#[error("the following keys are paired with themselves: {}", .0.iter().join(","))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(fireflow_types::python::ConfigError))]
pub struct KeyStringPairsError(NonEmpty<KeyString>);

#[cfg(feature = "python")]
mod python {
    use super::KeyStringPairs;
    use crate::validated::keys::KeyString;

    use pyo3::prelude::*;
    use std::collections::HashMap;

    impl<'py> FromPyObject<'py> for KeyStringPairs {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let xs: HashMap<KeyString, KeyString> = ob.extract()?;
            let ret = xs.try_into()?;
            Ok(ret)
        }
    }
}
