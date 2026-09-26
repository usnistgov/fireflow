pub mod keystring;
pub mod nonstd;
pub mod raw_std;

use crate::keys::keystring::{
    EmptyKeyStringError, KeyString, KeyStringError, PrintableAsciiStringError,
};
use crate::keys::nonstd::{
    DollarWrap, DollarWrapError, NoDollarWrapError, NonStdKey, SingleDollarPrefixError,
};

use nonempty::{NESlice, NEVec, ToDisplayNE, ambassador_impl_ToDisplayNE, nev};

use ambassador::Delegate;
use derive_more::{Display, From};
use nonstd::STD_PREFIX;
use raw_std::{KeyNotStdError, RawStdKey};
use thiserror::Error;

use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use fireflow_core_proc::{AllIntoPyErr, FromPyString, IntoPyString};

/// A key from the *TEXT* segment of an FCS file.
#[derive(Clone, From, PartialEq, Eq, Hash, Debug, Display)]
#[cfg_attr(feature = "python", derive(IntoPyString, FromPyString))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyKey {
    Std(StdKey),
    PseudoNonStd(PseudoNonStdKey),
    PseudoStd(PseudoStdKey),
    NonStd(NonStdKey),
}

/// A standard key which starts with a '$'.
///
/// The '$' is not stored internally. However using [`FromStr`] and [`Display`]
/// will parse/prepend a '$' during conversion.
pub type StdKey = DollarStdKey<true>;

/// A standard key which does not start with a '$'.
pub type PseudoNonStdKey = DollarStdKey<false>;

/// A non-standard key which starts with a '$'.
///
/// The '$' is not stored internally. However using [`FromStr`] and [`Display`]
/// will parse/prepend a '$' during conversion.
///
/// The string inside may start with '$'. For example, the value '$xya' would
/// actually represent the key '$xyz'.
#[derive(Clone, Debug, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyString, FromPyString))]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
// NOTE this can be pub because it has no additional restrictions on top of
// what KeyString already has.
pub struct PseudoStdKey(pub DollarKeyString<true>);

impl PseudoStdKey {
    /// Convert a pseudo-standard key into a nonstandard key.
    ///
    /// This has the effect of stripping the '$' from the key's string
    /// representation. For instance, '$xxx' will become 'xxx'.
    ///
    /// However, keys like '$$xxx' (which is stored as '$xxx' internally)
    /// will be converted to '$xxx' which is still a pseudo-standard key. This
    /// is inconsistent, hence the optional return.
    ///
    /// The alternative is to strip the '$', but this leaves the possibility of
    /// stripping the entire string which would still return an option.
    pub fn demote(self) -> Result<NonStdKey, Self> {
        self.0.0.try_into().map_err(DollarWrap).map_err(Self)
    }
}

pub type DollarStdKey<const HAS_PRE: bool> = DollarWrap<HAS_PRE, RawStdKey>;

pub type DollarKeyString<const HAS_PRE: bool> = DollarWrap<HAS_PRE, KeyString>;

pub type StdKeyError = DollarWrapError<KeyNotStdError>;

pub type PseudoNonStdKeyError = NoDollarWrapError<KeyNotStdError>;

pub type PseudoStdKeyError = DollarWrapError<KeyStringError>;

#[derive(From, PartialEq, Display, Debug, Error, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AnyKeyError {
    Ascii(PrintableAsciiStringError),
    Empty(EmptyKeyStringError),
    SingleDollar(SingleDollarPrefixError),
}

impl FromStr for AnyKey {
    type Err = AnyKeyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<PseudoStdKey>() {
            Ok(k) => Ok(Self::PseudoStd(k)),
            Err(e) => match e {
                DollarWrapError::Empty(e0) => Err(e0.into()),
                DollarWrapError::SingleDollar(e0) => Err(e0.into()),
                DollarWrapError::Inner(e0) => match e0 {
                    KeyStringError::Std(k) => Ok(Self::Std(DollarWrap(k.0))),
                    KeyStringError::Ascii(e1) => Err(e1.into()),
                },
                DollarWrapError::Prefix(e0) => match e0.try_nonstd() {
                    Ok(k) => Ok(Self::NonStd(k)),
                    Err(e1) => match e1 {
                        KeyStringError::Std(e2) => Ok(Self::PseudoNonStd(DollarWrap(e2.0))),
                        KeyStringError::Ascii(e2) => Err(e2.into()),
                    },
                },
            },
        }
    }
}

impl FromStr for StdKey {
    type Err = StdKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_dollar_str(s)
    }
}

impl FromStr for PseudoNonStdKey {
    type Err = PseudoNonStdKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_no_dollar_str(s)
    }
}

impl FromStr for PseudoStdKey {
    type Err = PseudoStdKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        DollarWrap::from_dollar_str(s).map(Self)
    }
}

// Implement methods on AnyKey

impl AnyKey {
    pub fn from_bytes(bytes: &NESlice<u8>) -> Result<Self, NEVec<u8>> {
        // TODO we may wish to distinguish an error between non-ASCII and only a
        // '$' keyword
        if let Some((&STD_PREFIX, rest)) = bytes.as_ref().split_first() {
            if let Some(ne) = NESlice::try_from_slice(rest) {
                if let Some(sk) = RawStdKey::from_bytes(ne) {
                    Ok(Self::Std(DollarWrap(sk)))
                } else if let Some(k) = KeyString::from_bytes(ne) {
                    Ok(Self::PseudoStd(PseudoStdKey(DollarWrap(k))))
                } else {
                    Err(bytes.to_ne_vec())
                }
            } else {
                Err(nev![STD_PREFIX])
            }
        } else if let Some(sk) = RawStdKey::from_bytes(bytes) {
            Ok(Self::PseudoNonStd(DollarWrap(sk)))
        } else if let Some(k) = KeyString::from_bytes(bytes) {
            match k.try_into() {
                Ok(k0) => Ok(Self::NonStd(k0)),
                Err(k0) => Ok(Self::PseudoStd(PseudoStdKey(DollarWrap(k0)))),
            }
        } else {
            Err(bytes.to_ne_vec())
        }
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{PseudoNonStdKey, StdKey};

    use pyo3::prelude::*;
    use pyo3::types::PyString;

    use std::convert::Infallible;

    macro_rules! impl_to_from_str {
        ($t:ident) => {
            impl<'py> FromPyObject<'_, 'py> for $t {
                type Error = PyErr;
                fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
                    Ok(obj.extract::<&str>()?.parse()?)
                }
            }

            impl<'py> IntoPyObject<'py> for $t {
                type Target = PyString;
                type Output = Bound<'py, PyString>;
                type Error = Infallible;

                fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
                    self.to_string().into_pyobject(py)
                }
            }
        };
    }

    impl_to_from_str!(StdKey);
    impl_to_from_str!(PseudoNonStdKey);
}
