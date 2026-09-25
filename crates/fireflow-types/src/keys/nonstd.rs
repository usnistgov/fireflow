use crate::keys::keystring::{EmptyKeyStringError, KeyString, KeyStringError};
use crate::keys::{DollarKeyString, RawStdKey};

use nonempty::{NEConcat, NEStr, NEString, ToDisplayNE, ambassador_impl_ToDisplayNE};

use ambassador::Delegate;
use derive_more::{AsRef, Display, From};
use thiserror::Error;

use std::borrow::Borrow;
use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromPyString, IntoPyString},
    pyo3::prelude::*,
};

/// A non-standard key which does not start with a '$'.
///
/// The internal value is guaranteed to not start with '$' in order to
/// distinguish from [`crate::keys::PseudoStdKey`].
#[derive(Clone, Debug, Display, PartialEq, Eq, Hash, PartialOrd, Ord, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyString, FromPyString))]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct NonStdKey(DollarKeyString<false>);

/// A wrapper for types that may or may not be prefixed with '$'.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Default, AsRef,
)]
#[display("{}{_0}", if STD { char::from(STD_PREFIX).into() } else { String::new() })]
#[display(bound(T: fmt::Display))]
pub struct DollarWrap<const STD: bool, T>(pub T);

#[derive(PartialEq, Display, Debug, Error, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(E: Into<PyErr>))]
pub enum NoDollarWrapError<E> {
    Inner(E),
    Prefix(DollarPrefixError),
    Empty(EmptyKeyStringError),
}

#[derive(PartialEq, Display, Debug, Error, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(E: Into<PyErr>))]
pub enum DollarWrapError<E> {
    Inner(E),
    SingleDollar(SingleDollarPrefixError),
    Prefix(NoDollarPrefixError),
    Empty(EmptyKeyStringError),
}

/// Error when parsing key that should not start with a '$' but actually does.
#[derive(PartialEq, Debug, Error, Clone)]
#[error("key must not start with '$'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct DollarPrefixError(NEString);

/// Error when parsing a standard key which is just '$'
#[derive(PartialEq, Debug, Error, Clone)]
#[error("key was just a '$' character")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct SingleDollarPrefixError;

/// Error when parsing key that should start with a '$' but does not.
#[derive(PartialEq, Debug, Error, Clone)]
#[error(
    "key must start with '$', got {}",
    char::from(*self.0.as_ne_bytes().first())
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
// NOTE private internals since we use outside this mod to efficiently parse all
// key types in a chain. This already has a guarantee that the NEString inside
// does not start with '$'.
pub struct NoDollarPrefixError(NEString);

/// The prefix byte for a standard or pseudostandard keyword (a '$').
pub const STD_PREFIX: u8 = 36;

pub type NonStdKeyError = NoDollarWrapError<KeyStringError>;

impl<'a, T: ToDisplayNE<'a>> ToDisplayNE<'a> for DollarWrap<true, T> {
    type NE = NEConcat<char, T::NE>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat::new(char::from(STD_PREFIX), self.0.to_ne())
    }
}

impl<'a, T: ToDisplayNE<'a>> ToDisplayNE<'a> for DollarWrap<false, T> {
    type NE = T::NE;
    fn to_ne(&'a self) -> Self::NE {
        self.0.to_ne()
    }
}

impl AsRef<NEStr> for NonStdKey {
    fn as_ref(&self) -> &NEStr {
        self.0.0.as_ne_str()
    }
}

impl From<RawStdKey> for NonStdKey {
    fn from(value: RawStdKey) -> Self {
        (&value).into()
    }
}

impl From<&RawStdKey> for NonStdKey {
    fn from(value: &RawStdKey) -> Self {
        Self(DollarWrap(KeyString::from_std_key(value)))
    }
}

impl FromStr for NonStdKey {
    type Err = NonStdKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        DollarWrap::from_no_dollar_str(s).map(Self)
    }
}

impl TryFrom<KeyString> for NonStdKey {
    type Error = KeyString;
    fn try_from(value: KeyString) -> Result<Self, Self::Error> {
        if *value.as_ne_str().as_ne_bytes().first() == STD_PREFIX {
            Err(value)
        } else {
            Ok(Self(DollarWrap(value)))
        }
    }
}

impl<const HAS_PRE: bool, T> Borrow<T> for DollarWrap<HAS_PRE, T> {
    fn borrow(&self) -> &T {
        &self.0
    }
}

impl NonStdKey {
    pub(crate) fn disambiguate(&mut self) {
        self.0.0.disambiguate();
    }
}

impl NoDollarPrefixError {
    pub(crate) fn try_nonstd(self) -> Result<NonStdKey, KeyStringError> {
        KeyString::try_from(self.0).map(DollarWrap).map(NonStdKey)
    }
}

impl<T> DollarWrap<true, T> {
    pub(crate) fn from_dollar_str<'a>(
        s: &'a str,
    ) -> Result<Self, DollarWrapError<<&'a NEStr as TryInto<T>>::Error>>
    where
        &'a NEStr: TryInto<T>,
    {
        if let Some(ne) = NEStr::try_new(s) {
            let (b0, bs) = ne.as_ne_bytes().split_first();
            let ss = str::from_utf8(bs).expect("stripping ASCII prefix should not break UTF8");
            if *b0 == STD_PREFIX {
                if let Some(ne0) = NEStr::try_new(ss) {
                    ne0.try_into().map_err(DollarWrapError::Inner).map(Self)
                } else {
                    Err(DollarWrapError::SingleDollar(SingleDollarPrefixError))
                }
            } else {
                let e = NoDollarPrefixError(ne.to_owned());
                Err(DollarWrapError::Prefix(e))
            }
        } else {
            Err(DollarWrapError::Empty(EmptyKeyStringError))
        }
    }
}

impl<T> DollarWrap<false, T> {
    pub(crate) fn from_no_dollar_str<'a>(
        s: &'a str,
    ) -> Result<Self, NoDollarWrapError<<&'a NEStr as TryInto<T>>::Error>>
    where
        &'a NEStr: TryInto<T>,
    {
        if let Some(ne) = NEStr::try_new(s) {
            if *ne.as_ne_bytes().first() == STD_PREFIX {
                Err(NoDollarWrapError::Prefix(DollarPrefixError(ne.to_owned())))
            } else {
                ne.try_into().map_err(NoDollarWrapError::Inner).map(Self)
            }
        } else {
            Err(NoDollarWrapError::Empty(EmptyKeyStringError))
        }
    }
}

#[cfg(feature = "serde")]
impl<const STD: bool, T: fmt::Display> Serialize for DollarWrap<STD, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}
