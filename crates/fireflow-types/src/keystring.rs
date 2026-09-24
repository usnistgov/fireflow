use nonempty::{
    IntoNonEmptyIterator as _, NESlice, NEStr, NEString, NonEmptyIterator as _, ToDisplayNE,
};

use derive_more::{AsRef, Display};
use thiserror::Error;
use unicase::Ascii;

use std::borrow::Borrow;
use std::hash::Hash;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString},
};

/// The internal string for a non-standard key (standard or nonstandard).
///
/// Must be non-empty and contain only ASCII characters. Comparisons will be
/// case-insensitive.
#[derive(Clone, Debug, AsRef, Display, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[as_ref(str)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub struct KeyString(Ascii<NEString>);

impl From<KeyString> for NEString {
    fn from(value: KeyString) -> Self {
        value.0.into_inner()
    }
}

impl Borrow<str> for KeyString {
    fn borrow(&self) -> &str {
        self.as_ref()
    }
}

/// Error when parsing [`KeyString`] from string
#[derive(PartialEq, Debug, Error, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub enum NEAsciiStringError {
    #[error("{0}")]
    Ascii(AsciiStringError),
    #[error("key string must not be empty")]
    Empty,
}

/// Error when parsing [`KeyString`] from string
#[derive(PartialEq, Debug, Error, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
#[error("string should only have printable ASCII characters, found '{0}'")]
pub struct AsciiStringError(pub NEString);

impl<'a> ToDisplayNE<'a> for KeyString {
    type NE = &'a NEString;
    fn to_ne(&'a self) -> Self::NE {
        &self.0
    }
}

impl AsRef<NEStr> for KeyString {
    fn as_ref(&self) -> &NEStr {
        (*self.0).as_ref()
    }
}

impl FromStr for KeyString {
    type Err = NEAsciiStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        to_keystring(s).map(|ne| Self(Ascii::new(ne.to_owned())))
    }
}

impl TryFrom<NEString> for KeyString {
    type Error = AsciiStringError;
    fn try_from(value: NEString) -> Result<Self, Self::Error> {
        if is_printable_ascii(value.as_str().as_bytes()) {
            Ok(Self(Ascii::new(value)))
        } else {
            Err(AsciiStringError(value))
        }
    }
}

impl TryFrom<&NEStr> for KeyString {
    type Error = AsciiStringError;
    fn try_from(value: &NEStr) -> Result<Self, Self::Error> {
        if is_printable_ascii(value.as_str().as_bytes()) {
            Ok(Self(Ascii::new(value.to_owned())))
        } else {
            Err(AsciiStringError(value.to_owned()))
        }
    }
}

impl KeyString {
    fn new_unchecked(s: NEString) -> Self {
        Self(Ascii::new(s))
    }

    pub fn disambiguate(&mut self) {
        self.0.push('_');
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    #[must_use]
    pub fn as_ne_str(&self) -> &NEStr {
        self.0.as_ne_str()
    }

    pub(crate) fn from_bytes_maybe(xs: &NESlice<u8>, single_byte: bool) -> Option<Self> {
        if single_byte {
            let ne = xs.into_nonempty_iter().copied().map(char::from).collect();
            Some(Self::new_unchecked(ne))
        } else if is_printable_ascii(xs.as_ref()) {
            // SAFETY: we just checked that the bytes are only ASCII chars
            Some(unsafe { Self::from_bytes(xs) })
        } else {
            None
        }
    }

    /// Make new keystring from slice of bytes known not to be empty.
    ///
    /// # Safety
    ///
    /// Caller must guarantee that bytes are valid UTF-8 characters.
    unsafe fn from_bytes(xs: &NESlice<u8>) -> Self {
        let ne = xs.nonempty_iter().copied().collect();
        // SAFETY: this function is marked unsafe since the caller must check
        Self::new_unchecked(unsafe { NEString::from_utf8_unchecked(ne) })
    }
}

#[cfg(feature = "serde")]
impl Serialize for KeyString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        AsRef::<str>::as_ref(self).serialize(serializer)
    }
}

pub(crate) fn to_keystring(s: &str) -> Result<&NEStr, NEAsciiStringError> {
    if let Some(ne) = NEStr::try_new(s) {
        if is_printable_ascii(ne.as_ne_bytes().as_ref()) {
            Ok(ne)
        } else {
            Err(NEAsciiStringError::Ascii(AsciiStringError(ne.to_owned())))
        }
    } else {
        Err(NEAsciiStringError::Empty)
    }
}

pub(crate) fn is_printable_ascii(xs: &[u8]) -> bool {
    xs.iter().all(|x| 32 <= *x && *x <= 126)
}
