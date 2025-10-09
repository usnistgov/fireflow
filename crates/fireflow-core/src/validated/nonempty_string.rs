use derive_more::{Display, Into};
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[derive(Clone, PartialEq, Eq, Default, Display, Into)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct NonEmptyString(String);

impl FromStr for NonEmptyString {
    type Err = NonEmptyStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Err(NonEmptyStringError)
        } else {
            Ok(Self(s.to_owned()))
        }
    }
}

#[derive(Error, Debug)]
#[error("string cannot be empty")]
pub struct NonEmptyStringError;

#[cfg(feature = "python")]
mod python {
    use super::{NonEmptyString, NonEmptyStringError};
    use crate::python::macros::{impl_from_py_via_fromstr, impl_value_err};

    impl_value_err!(NonEmptyStringError);
    impl_from_py_via_fromstr!(NonEmptyString);
}
