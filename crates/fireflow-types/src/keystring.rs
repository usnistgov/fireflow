use crate::std_key::RawStdKey;
use nonempty::{DisplayableNE as _, NESlice, NEStr, NEString, ToDisplayNE};

use derive_more::{AsRef, Display, From};
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
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromPyString, IntoPyString},
};

/// The internal string for a non-standard key.
///
/// Comparisons are case-insensitive.
///
/// The following properties are upheld for the internal value:
/// * it will be non-empty.
/// * it will only include ASCII bytes 32-126 (printable characters).
/// * it will not have any character sequences that are also standard keys
///   (ie it can never be `"P1N"` or `"OP"`).
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
#[derive(From, PartialEq, Display, Debug, Error, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NEKeyStringError {
    Inner(KeyStringError),
    Empty(EmptyKeyStringError),
}

/// Error when converting [`KeyString`] from non-empty string.
#[derive(From, PartialEq, Display, Debug, Error, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum KeyStringError {
    Std(InvalidStdKeyError),
    Ascii(PrintableAsciiStringError),
}

/// Error when parsing [`KeyString`] from string
#[derive(PartialEq, Debug, Error, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub enum NEAsciiStringError {
    #[error("{0}")]
    Ascii(PrintableAsciiStringError),
    #[error("key string must not be empty")]
    Empty,
}

#[derive(PartialEq, Debug, Error, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
#[error("string should not be a valid standard key, found '{0}'")]
pub struct InvalidStdKeyError(pub RawStdKey);

#[derive(PartialEq, Debug, Error, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
#[error("string should only have printable ASCII characters, found '{0}'")]
pub struct PrintableAsciiStringError(pub NEString);

#[derive(PartialEq, Debug, Error, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
#[error("string should not be empty")]
pub struct EmptyKeyStringError;

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
    type Err = NEKeyStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(ne) = NEStr::try_new(s) {
            Ok(ne.try_into()?)
        } else {
            Err(EmptyKeyStringError.into())
        }
    }
}

impl TryFrom<NEString> for KeyString {
    type Error = KeyStringError;
    fn try_from(value: NEString) -> Result<Self, Self::Error> {
        if let Some(k) = RawStdKey::from_ne_str(value.as_ne_str()) {
            Err(InvalidStdKeyError(k).into())
        } else {
            Self::from_bytes(value.as_ne_str().as_ne_bytes())
                .ok_or(PrintableAsciiStringError(value).into())
        }
    }
}

impl TryFrom<&NEStr> for KeyString {
    type Error = KeyStringError;
    fn try_from(value: &NEStr) -> Result<Self, Self::Error> {
        if let Some(k) = RawStdKey::from_ne_str(value) {
            Err(InvalidStdKeyError(k).into())
        } else {
            Self::from_bytes(value.as_ne_bytes())
                .ok_or_else(|| PrintableAsciiStringError(value.to_owned()).into())
        }
    }
}

impl KeyString {
    #[must_use]
    pub fn from_std_key(sk: &RawStdKey) -> Self {
        let mut s = match sk {
            RawStdKey::Root(k) => k.as_ne_str().to_owned(),
            RawStdKey::Meas(k) => k.as_ne_string(),
            RawStdKey::Gate(k) => k.as_ne_string(),
            RawStdKey::Region(k) => k.as_ne_string(),
            RawStdKey::Dfc(k) => k.as_ne_string(),
            RawStdKey::CsvFlag(k) => k.as_ne_string(),
        };
        // No key in the FCS standard ends with a '_', so this will never
        // produce an internally inconsistent keystring
        s.push(DISAMBIGUATION_CHAR);
        Self::new_unchecked(s)
    }

    pub fn disambiguate(&mut self) {
        self.0.push(DISAMBIGUATION_CHAR);
    }

    fn new_unchecked(s: NEString) -> Self {
        Self(Ascii::new(s))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    #[must_use]
    pub fn as_ne_str(&self) -> &NEStr {
        self.0.as_ne_str()
    }

    pub(crate) fn from_bytes(xs: &NESlice<u8>) -> Option<Self> {
        is_printable_ascii(xs.as_ref()).then(|| {
            // SAFETY: we just checked that the bytes are only ASCII chars
            unsafe { Self::from_bytes_unchecked(xs) }
        })
    }

    /// Make new keystring from slice of bytes known not to be empty.
    ///
    /// # Safety
    ///
    /// Caller must guarantee that bytes are valid UTF-8 characters.
    unsafe fn from_bytes_unchecked(xs: &NESlice<u8>) -> Self {
        // SAFETY: this function is marked unsafe since the caller must check
        Self::new_unchecked(unsafe { NEString::from_utf8_unchecked(xs.to_ne_vec()) })
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

const DISAMBIGUATION_CHAR: char = '_';
