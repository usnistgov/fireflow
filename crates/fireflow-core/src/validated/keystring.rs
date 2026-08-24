use fireflow_types::nonempty_string::{NEStr, NEString, ToDisplayNE};

use derive_more::{AsRef, Display};
use nonempty_collections::{IntoNonEmptyIterator as _, NESlice, iter::NonEmptyIterator as _};
use thiserror::Error;
use unicase::Ascii;

use std::hash::Hash;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString},
    fireflow_types::python as py,
};

/// The internal string for a key (standard or nonstandard).
///
/// Must be non-empty and contain only ASCII characters. Comparisons will be
/// case-insensitive.
#[derive(Clone, Debug, AsRef, Display, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
#[as_ref(str)]
pub struct KeyString(Ascii<NEString>);

/// Error when parsing [`KeyString`] from string
#[derive(PartialEq, Debug, Error, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub enum AsciiStringError {
    #[error("string should only have ASCII characters, found '{0}'")]
    Ascii(String),
    #[error("key string must not be empty")]
    Empty,
}

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
    type Err = AsciiStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(ne) = s.parse::<NEString>() {
            if is_printable_ascii(s.as_ref()) {
                Ok(Self(Ascii::new(ne)))
            } else {
                Err(AsciiStringError::Ascii(s.into()))
            }
        } else {
            Err(AsciiStringError::Empty)
        }
    }
}

impl KeyString {
    fn new_unchecked(s: NEString) -> Self {
        Self(Ascii::new(s))
    }

    pub(crate) fn disambiguate(&mut self) {
        self.0.push('_');
    }

    pub(crate) fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub(crate) fn as_ne_str(&self) -> &NEStr {
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

    pub(crate) unsafe fn from_bytes(xs: &NESlice<u8>) -> Self {
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

fn is_printable_ascii(xs: &[u8]) -> bool {
    xs.iter().all(|x| 32 <= *x && *x <= 126)
}
