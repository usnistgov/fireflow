use derive_more::{AsRef, Display, Into};
use thiserror::Error;

use std::hash::Hash;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{DisplayAsPyErr, FromPyString},
    pyo3::prelude::*,
};

/// A static string which can never be empty.
pub type NEStrConst = NEStr<'static>;

/// A string which can never be empty.
#[derive(Clone, Copy, Display)]
pub struct NEStr<'a>(&'a str);

/// A string which can never be empty.
#[derive(Clone, PartialEq, Eq, Hash, Default, Display, Into, Debug, AsRef)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyString))]
#[as_ref(str)]
pub struct NEString(String);

impl<'a> NEStr<'a> {
    #[must_use]
    pub const fn as_str(&self) -> &'a str {
        self.0
    }

    #[must_use]
    pub fn to_owned(&self) -> NEString {
        NEString(self.0.to_owned())
    }
}

#[macro_export]
macro_rules! ne_str {
    ($s:expr) => {{
        const _: () = assert!(!$s.is_empty(), "String cannot be empty");
        // SAFETY: we just checked
        unsafe { $crate::nonempty_string::NEStr::new_unchecked($s) }
    }};
}

impl NEStrConst {
    #[must_use]
    #[allow(clippy::missing_safety_doc)]
    pub const unsafe fn new_unchecked(s: &'static str) -> Self {
        Self(s)
    }
}

impl NEString {
    pub fn push(&mut self, c: char) {
        self.0.push(c);
    }

    pub fn push_str(&mut self, s: &str) {
        self.0.push_str(s);
    }
}

impl FromStr for NEString {
    type Err = NonEmptyStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            Err(NonEmptyStringError)
        } else {
            Ok(Self(s.to_owned()))
        }
    }
}

/// Error when parsing [`NonEmptyString`] from empty [`String`]
#[derive(Error, Debug)]
#[error("string cannot be empty")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
pub struct NonEmptyStringError;
