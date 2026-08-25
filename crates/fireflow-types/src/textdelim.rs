use crate::nonempty_string::{NEStr, NEString};

use ambassador::delegatable_trait;
use derive_more::{Display, FromStr, Into};
use derive_new::new;
use thiserror::Error;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{DisplayAsPyErr, TryFromPyObject},
};

/// Valid chars that can be used for the TEXT delimiter when writing.
///
/// While the standards permit any ASCII character 1-127 to be used as the
/// delimiter, only control chars make sense to support, which equates to
/// characters 1 through 31.
///
/// For instance, setting the delimiter to '0' would break any keywords (many of
/// which are required) that store only '0', since this would need to be
/// escaped, but this is not allowed at the start or end of a keyword.
#[derive(Clone, Copy, Display, Into, FromStr)]
#[cfg_attr(feature = "python", derive(TryFromPyObject))]
#[into(u8, char)]
#[display("{}", char::from(self.0))]
pub struct TEXTDelim(u8);

#[delegatable_trait]
pub trait HasDelim {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError>;
}

/// Error when a token in TEXT cannot be written because of the delimiter.
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error(
    "token '{value}' could not be written because it {reason}",
    reason = if self.all_delim {
        "is entirely delimiter characters"
    } else {
        "contains a delimiter at the start or end"
    }
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::WriteFCSError))]
pub struct DelimCollisionError {
    all_delim: bool,
    value: NEString,
}

impl<T: AsRef<NEStr>> HasDelim for T {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        self.as_ref().has_delim(d)
    }
}

impl HasDelim for NEStr {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        let c = char::from(d);
        let ret = |all_delim: bool| Some(DelimCollisionError::new(all_delim, self.to_owned()));
        if self.as_str().chars().all(|x| x == c) {
            ret(true)
        } else if self.as_str().starts_with(c) || self.as_str().ends_with(c) {
            ret(false)
        } else {
            None
        }
    }
}

impl Default for TEXTDelim {
    fn default() -> Self {
        Self(30) // record separator
    }
}

impl TryFrom<u8> for TEXTDelim {
    type Error = TEXTDelimError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if (1..32).contains(&value) {
            Ok(Self(value))
        } else {
            Err(TEXTDelimError(value))
        }
    }
}

/// Error when creating [`TEXTDelim`]
#[derive(Debug, Error)]
#[error("delimiter should be char between 1 and 31 inclusive, got {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct TEXTDelimError(u8);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u8_to_delim() {
        assert!(TEXTDelim::try_from(1_u8).is_ok());
        assert!(TEXTDelim::try_from(31_u8).is_ok());
        assert!(TEXTDelim::try_from(0_u8).is_err());
        assert!(TEXTDelim::try_from(32_u8).is_err());
    }
}
