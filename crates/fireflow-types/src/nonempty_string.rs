use derive_more::{AsRef, Display, Into};
use thiserror::Error;

use std::borrow::Borrow;
use std::hash::Hash;
use std::ptr::from_ref;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{DisplayAsPyErr, FromPyString},
    pyo3::prelude::*,
};

/// A string slice which can never be empty.
#[derive(AsRef, Display)]
#[repr(transparent)]
pub struct NEStr(str);

/// A string which can never be empty.
#[derive(Clone, PartialEq, Eq, Hash, Default, Display, Into, Debug, AsRef)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyString))]
#[as_ref(str)]
pub struct NEString(String);

impl Borrow<NEStr> for NEString {
    fn borrow(&self) -> &NEStr {
        str_to_ne_unchecked(self.0.as_str())
    }
}

impl ToOwned for NEStr {
    type Owned = NEString;
    fn to_owned(&self) -> Self::Owned {
        NEString(self.0.to_string())
    }
}

#[macro_export]
macro_rules! ne_str {
    ($s:expr) => {{
        const RET: Option<&$crate::nonempty_string::NEStr> =
            $crate::nonempty_string::NEStr::try_new($s);
        RET.expect("String cannot be empty")
        // This move is ripped off from ByteStr::from_bytes, except that we use
        // a macro here to ensure that such str's can only be made at compile
        // time so that the non-empty property can be checked. After checking,
        // double cast to the wrapper type and return a reference to it.
        // const _: () = assert!(!$s.is_empty(), "String cannot be empty");
        // let p = std::ptr::from_ref($s);
        // // SAFETY: `NEStr` is a transparent wrapper around `str`, so we can turn
        // // a reference to the wrapped type into a reference to the wrapper type.
        // unsafe {
        //     #[allow(clippy::as_conversions)]
        //     &*(p as *const $crate::nonempty_string::NEStr)
        // }
    }};
}

impl NEString {
    pub fn push(&mut self, c: char) {
        self.0.push(c);
    }

    pub fn push_str(&mut self, s: &str) {
        self.0.push_str(s);
    }
}

impl NEStr {
    #[must_use]
    pub const fn try_new(s: &str) -> Option<&Self> {
        if s.is_empty() {
            None
        } else {
            Some(str_to_ne_unchecked(s))
        }
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

const fn str_to_ne_unchecked(s: &str) -> &NEStr {
    // Ripped off from ByteStr::from_bytes
    let p: *const str = from_ref(s);
    // SAFETY: NEStr and str have same layout
    unsafe {
        #[allow(clippy::as_conversions)]
        &*(p as *const NEStr)
    }
}
