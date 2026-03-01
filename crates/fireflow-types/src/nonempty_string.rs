use derive_more::{AsRef, Display, Into};
use nonempty_collections::{
    IntoNonEmptyIterator, NESlice, NEVec,
    iter::{FromNonEmptyIterator, NonEmptyIterator as _},
};
use thiserror::Error;

use std::hash::Hash;
use std::ptr::from_ref;
use std::str::{FromStr, Utf8Error};
use std::{borrow::Borrow, num::NonZeroUsize};

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
        NEStr::new_unchecked(self.0.as_str())
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
    }};
}

impl FromNonEmptyIterator<char> for NEString {
    fn from_nonempty_iter<I>(iter: I) -> Self
    where
        I: IntoNonEmptyIterator<Item = char>,
    {
        let (x0, xs) = iter.into_nonempty_iter().next();
        let mut s = String::from(x0);
        s.extend(xs);
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

    #[must_use]
    pub fn len(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.0.len()).unwrap()
    }

    /// Like [`String::from_utf8_unchecked`] but requires a [`NEVec<u8>`].
    ///
    /// # Safety
    ///
    /// The user must ensure bytes are valid UTF-8.
    #[must_use]
    pub unsafe fn from_utf8_unchecked(bytes: NEVec<u8>) -> Self {
        // SAFETY: unsafe function
        let ret = unsafe { String::from_utf8_unchecked(bytes.into()) };
        Self(ret)
    }
}

impl NEStr {
    #[must_use]
    pub const fn try_new(s: &str) -> Option<&Self> {
        if s.is_empty() {
            None
        } else {
            Some(Self::new_unchecked(s))
        }
    }

    pub fn from_utf8<'a>(bytes: &'a NESlice<u8>) -> Result<&'a Self, Utf8Error> {
        Ok(Self::new_unchecked(str::from_utf8(bytes.as_ref())?))
    }

    #[must_use]
    pub fn len(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.0.len()).unwrap()
    }

    const fn new_unchecked(s: &str) -> &Self {
        // Ripped off from ByteStr::from_bytes
        let p: *const str = from_ref(s);
        // SAFETY: NEStr and str have same layout
        unsafe {
            #[allow(clippy::as_conversions)]
            &*(p as *const Self)
        }
    }
}

impl TryFrom<String> for NEString {
    type Error = NonEmptyStringError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            Err(NonEmptyStringError)
        } else {
            Ok(Self(value))
        }
    }
}

impl FromStr for NEString {
    type Err = NonEmptyStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s.to_owned())
    }
}

/// Error when parsing [`NonEmptyString`] from empty [`String`]
#[derive(Error, Debug)]
#[error("string cannot be empty")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
pub struct NonEmptyStringError;
