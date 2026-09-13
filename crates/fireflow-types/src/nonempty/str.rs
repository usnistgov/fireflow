use super::{HasNELen, slice::NESlice, string::NEString};

use derive_more::{AsRef, Display};

use std::{
    num::NonZeroUsize,
    ptr::from_ref,
    str::{FromStr, Utf8Error},
};

#[cfg(feature = "serde")]
use serde::Serialize;

/// A string slice which can never be empty.
#[derive(AsRef, Display, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[repr(transparent)]
pub struct NEStr(str);

/// Create a static non-empty string.
#[macro_export]
macro_rules! ne_str {
    ($s:expr) => {{
        const _: () = assert!(!$s.is_empty(), "string cannot be empty");
        $crate::nonempty::NEStr::try_new($s).unwrap()
    }};
}

impl PartialEq<NEString> for NEStr {
    fn eq(&self, other: &NEString) -> bool {
        self == other.as_ne_str()
    }
}

impl PartialEq<NEString> for &NEStr {
    fn eq(&self, other: &NEString) -> bool {
        *self == other
    }
}

impl AsRef<Self> for NEStr {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl ToOwned for NEStr {
    type Owned = NEString;
    fn to_owned(&self) -> Self::Owned {
        NEString::try_from(self.0.to_string()).unwrap()
    }
}

impl PartialEq<str> for NEStr {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl HasNELen for NEStr {
    fn ne_len(&self) -> NonZeroUsize {
        self.len()
    }
}

impl HasNELen for &NEStr {
    fn ne_len(&self) -> NonZeroUsize {
        self.len()
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

    pub fn parse<F: FromStr>(&self) -> Result<F, <F as FromStr>::Err> {
        self.as_str().parse()
    }

    pub fn from_utf8(bytes: &NESlice<u8>) -> Result<&Self, Utf8Error> {
        Ok(Self::new_unchecked(str::from_utf8(bytes.as_ref())?))
    }

    /// # Safety
    ///
    /// Caller must check that string is UTF8.
    #[must_use]
    pub unsafe fn from_utf8_unchecked(bytes: &NESlice<u8>) -> &Self {
        // SAFETY: function is unsafe
        let s = unsafe { str::from_utf8_unchecked(bytes.as_ref()) };
        Self::new_unchecked(s)
    }

    /// # Safety
    ///
    /// Caller must check that string is UTF8.
    #[must_use]
    pub unsafe fn try_from_utf8_unchecked(bytes: &[u8]) -> Option<&Self> {
        // SAFETY: function is unsafe
        let s = unsafe { str::from_utf8_unchecked(bytes) };
        Self::try_new(s)
    }

    #[must_use]
    pub const fn as_ne_bytes(&self) -> &NESlice<u8> {
        NESlice::new_unchecked(self.as_str().as_bytes())
    }

    #[must_use]
    pub const fn len(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.0.len()).unwrap()
    }

    #[must_use]
    pub const fn as_str(&self) -> &str {
        let p: *const Self = from_ref(self);
        // SAFETY: NEStr and str have same layout
        unsafe {
            #[allow(clippy::as_conversions)]
            &*(p as *const str)
        }
    }

    pub(crate) const fn new_unchecked(s: &str) -> &Self {
        let p: *const str = from_ref(s);
        // SAFETY: NEStr and str have same layout
        unsafe {
            #[allow(clippy::as_conversions)]
            &*(p as *const Self)
        }
    }

    /// Return first character of string.
    #[must_use]
    pub fn first(&self) -> char {
        self.0.chars().next().unwrap()
    }

    /// Return last character of string.
    #[must_use]
    pub fn last(&self) -> char {
        self.0.chars().next_back().unwrap()
    }

    /// Trim whitespace from start and end of string.
    ///
    /// This will strip 0x09, 0x0A, 0x0B, 0x0C, 0x0D, and 0x20.
    #[must_use]
    pub fn trim_ascii(&self) -> &str {
        self.trim_ascii_start().trim_ascii_end()
    }

    /// Trim whitespace from start and end of string.
    ///
    /// This will strip 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20 and 0xA0.
    #[must_use]
    pub fn trim_latin1(&self) -> &[u8] {
        self.as_ne_bytes().trim_latin1()
    }

    /// Trim whitespace from start of string.
    ///
    /// This will strip 0x09, 0x0A, 0x0B, 0x0C, 0x0D, and 0x20.
    #[must_use]
    pub fn trim_ascii_start(&self) -> &str {
        let bytes = self.as_ne_bytes().trim_ascii_start();
        // SAFETY: trimming ASCII bytes from start won't break UTF8
        unsafe { str::from_utf8_unchecked(bytes) }
    }

    /// Trim whitespace from end of string.
    ///
    /// This will strip 0x09, 0x0A, 0x0B, 0x0C, 0x0D, and 0x20.
    #[must_use]
    pub fn trim_ascii_end(&self) -> &str {
        let bytes = self.as_ne_bytes().trim_ascii_end();
        // SAFETY: trimming ASCII bytes from end won't break UTF8
        unsafe { str::from_utf8_unchecked(bytes) }
    }

    /// Trim whitespace from start of string.
    ///
    /// This will strip 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20, and 0xA0.
    #[must_use]
    pub fn trim_latin1_start(&self) -> &str {
        let bytes = self.as_ne_bytes().trim_latin1_start();
        // SAFETY: trimming ASCII bytes or 0xA0 from start won't break UTF8
        unsafe { str::from_utf8_unchecked(bytes) }
    }

    /// Trim whitespace from end of string.
    ///
    /// This will strip 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x20, and 0xA0.
    #[must_use]
    pub fn trim_latin1_end(&self) -> &[u8] {
        self.as_ne_bytes().trim_latin1_end()
    }
}

#[cfg(feature = "python")]
mod python {
    use super::NEStr;

    use pyo3::{exceptions::PyValueError, prelude::*, types::PyString};

    use std::convert::Infallible;

    impl<'a, 'py> FromPyObject<'a, 'py> for &'a NEStr {
        type Error = PyErr;
        fn extract(obj: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
            let s = obj.extract::<&'a str>()?;
            NEStr::try_new(s).ok_or(PyValueError::new_err("string must not be empty"))
        }
    }

    impl<'py> IntoPyObject<'py> for &'_ NEStr {
        type Target = PyString;
        type Output = Bound<'py, Self::Target>;
        type Error = Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.as_str().into_pyobject(py)
        }
    }
}
