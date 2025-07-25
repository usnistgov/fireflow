use crate::text::index::MeasIndex;

use derive_more::{AsRef, Display, FromStr, Into};
use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// The value for the $PnN key (all versions).
///
/// This cannot contain commas.
#[derive(Clone, Eq, PartialEq, Hash, Debug, AsRef, Display, Into)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[as_ref(str)]
pub struct Shortname(String);

/// A prefix that can be made into a shortname by appending an index
///
/// This cannot contain commas.
#[derive(Clone, Eq, PartialEq, Hash, AsRef, Display, FromStr)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyObject))]
#[as_ref(str)]
pub struct ShortnamePrefix(Shortname);

impl Shortname {
    pub fn new_unchecked<T: AsRef<str>>(s: T) -> Self {
        Shortname(s.as_ref().to_owned())
    }
}

impl FromStr for Shortname {
    type Err = ShortnameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains(',') {
            Err(ShortnameError(s.to_string()))
        } else {
            Ok(Shortname(s.to_string()))
        }
    }
}

impl ShortnamePrefix {
    pub fn as_indexed(&self, i: MeasIndex) -> Shortname {
        Shortname(format!("{}{i}", self))
    }
}

impl Default for ShortnamePrefix {
    fn default() -> Self {
        Self(Shortname("P".into()))
    }
}

pub struct ShortnameError(String);

impl fmt::Display for ShortnameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "commas are not allowed in name '{}'", self.0)
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{Shortname, ShortnameError};
    use crate::python::macros::{impl_from_py_via_fromstr, impl_value_err};

    impl_from_py_via_fromstr!(Shortname);
    impl_value_err!(ShortnameError);
}
