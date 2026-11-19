use fireflow_types::config::DEDUP_PNN_SEP;
use fireflow_types::index::MeasIndex;
use nonempty::{
    NEStr, NEString, NonEmptyStringError, ToDisplayNE, ambassador_impl_ToDisplayNE, ne_str,
};

use ambassador::Delegate;
use derive_more::{AsRef, Display, From, FromStr, Into};
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromPyString},
    pyo3::prelude::*,
};

#[cfg(test)]
use proptest_derive::Arbitrary;

/// The value for the $PnN key (all versions).
///
/// This cannot contain commas or be empty.
#[derive(Clone, Eq, PartialEq, Hash, Debug, AsRef, Display, Into, Delegate, FromStr)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyString))]
#[cfg_attr(test, derive(Arbitrary))]
#[as_ref(str, NEStr)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct Shortname(pub NoCommaNEString);

/// A non-empty string without any commas.
#[derive(Clone, Eq, PartialEq, Hash, Debug, AsRef, Display, Into, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyString))]
#[as_ref(str, NEStr)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct NoCommaNEString(NEString);

/// Error when parsing [`Shortname`] from string
#[derive(Debug, Error, From, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(
    feature = "python",
    pyerr(fireflow_types::python::ParseKeywordValueError)
)]
pub enum NoCommaNEStringError {
    #[error("commas are not allowed in string '{0}'")]
    Commas(String),
    #[error("{0}")]
    Empty(NonEmptyStringError),
}

impl Shortname {
    pub(crate) fn from_ne_unchecked(s: NEString) -> Self {
        Self(NoCommaNEString::from_ne_unchecked(s))
    }

    pub(crate) fn new_unchecked<T: AsRef<str>>(s: T) -> Self {
        Self(NoCommaNEString::new_unchecked(s))
    }

    pub(crate) fn increment(&self, i: usize) -> Self {
        Self(self.0.increment(i))
    }
}

impl NoCommaNEString {
    fn from_ne_unchecked(s: NEString) -> Self {
        assert!(
            !s.as_str().contains(','),
            "shortname has at least one comma"
        );
        Self(s)
    }

    fn new_unchecked<T: AsRef<str>>(s: T) -> Self {
        Self(s.as_ref().parse().unwrap())
    }

    fn increment(&self, i: usize) -> Self {
        let mut n = self.clone();
        n.0.push(DEDUP_PNN_SEP);
        n.0.push_str(&i.to_string());
        n
    }
}

impl FromStr for NoCommaNEString {
    type Err = NoCommaNEStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains(',') {
            Err(NoCommaNEStringError::Commas(s.into()))
        } else {
            Ok(Self(s.parse::<NEString>()?))
        }
    }
}

impl From<MeasIndex> for Shortname {
    fn from(value: MeasIndex) -> Self {
        let mut ret = ne_str!("P").to_owned();
        ret.push_str(&value.to_string());
        Self(NoCommaNEString(ret))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use assert_matches::assert_matches;
    use proptest::prelude::*;

    impl Arbitrary for NoCommaNEString {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;
        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            "[^,]{1,40}"
                .prop_map(|s| s.parse::<Self>().unwrap())
                .boxed()
        }
    }

    proptest! {
        #[test]
        fn str_to_shortname(s in "[^,]{1,40}") {
            assert!(s.parse::<NoCommaNEString>().is_ok());
        }
    }

    #[test]
    fn str_to_shortname_invalid() {
        assert_matches!(
            "Thunderfist,Chronicles".parse::<NoCommaNEString>(),
            Err(NoCommaNEStringError::Commas(_))
        );
        assert_matches!(
            "".parse::<NoCommaNEString>(),
            Err(NoCommaNEStringError::Empty(_))
        );
    }
}
