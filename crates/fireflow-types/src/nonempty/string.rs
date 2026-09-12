use super::{FromNonEmptyIterator, IntoNonEmptyIterator, NESlice, NEStr, NEVec, NonEmptyIterator};

use derive_more::{AsRef, Display, Into};
use thiserror::Error;

use std::{
    fmt,
    hash::Hash,
    str::{FromStr, Utf8Error},
    {borrow::Borrow, num::NonZeroUsize},
};

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{DisplayAsPyErr, FromPyString},
    pyo3::prelude::*,
};

/// A string which can never be empty.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Display, Into, Debug, AsRef)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyString))]
#[as_ref(str)]
pub struct NEString(String);

impl AsRef<NEStr> for NEString {
    fn as_ref(&self) -> &NEStr {
        NEStr::new_unchecked(self.0.as_ref())
    }
}

/// Like a [`FromUtf8Error`] but for non-empty strings.
#[derive(Into)]
pub struct FromNEUtf8Error {
    bytes: NEVec<u8>,
    error: Utf8Error,
}

/// Error when parsing [`NonEmptyString`] from empty [`String`]
#[derive(Error, Debug, PartialEq, Clone)]
#[error("string cannot be empty")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
pub struct NonEmptyStringError;

impl Borrow<NEStr> for NEString {
    fn borrow(&self) -> &NEStr {
        NEStr::new_unchecked(self.0.as_str())
    }
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

impl fmt::Write for NEString {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.0.push_str(s);
        Ok(())
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
    pub const fn len(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.0.len()).unwrap()
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.as_ref()
    }

    #[must_use]
    pub fn as_ne_str(&self) -> &NEStr {
        NEStr::new_unchecked(self.as_ref())
    }

    #[must_use]
    pub fn as_ne_bytes(&self) -> &NESlice<u8> {
        self.as_ne_str().as_ne_bytes()
    }

    pub fn parse<F: FromStr>(&self) -> Result<F, <F as FromStr>::Err> {
        self.0.parse()
    }

    /// Like [`String::from_utf8`] but requires a [`NEVec<u8>`].
    pub fn from_utf8(bytes: NEVec<u8>) -> Result<Self, FromNEUtf8Error> {
        let s = String::from_utf8(bytes.into()).map_err(|e| {
            let error = e.utf8_error();
            FromNEUtf8Error {
                bytes: NEVec::try_from_vec(e.into_bytes()).unwrap(),
                error,
            }
        })?;
        Ok(Self(s))
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

impl FromNEUtf8Error {
    #[must_use]
    pub fn into_bytes(self) -> NEVec<u8> {
        self.bytes
    }
}

impl From<&NEStr> for NEString {
    fn from(value: &NEStr) -> Self {
        Self(value.as_str().to_owned())
    }
}

impl From<char> for NEString {
    fn from(value: char) -> Self {
        Self(String::from(value))
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

#[cfg(feature = "testutil")]
mod testutil {
    use super::NEString;
    use proptest::prelude::*;

    impl Arbitrary for NEString {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;
        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            "\\PC+".prop_map(|s| s.parse().unwrap()).boxed()
        }
    }
}
