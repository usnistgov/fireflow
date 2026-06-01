use crate::text::index::MeasIndex;

use fireflow_types::ne_str;
use fireflow_types::nonempty_string::{NEStr, NEString};
use fireflow_types::{
    config::DEDUP_PNN_SEP,
    nonempty_string::{NonEmptyStringError, ToDisplayNE, ambassador_impl_ToDisplayNE},
};

use ambassador::Delegate;
use derive_more::{AsRef, Display, From, Into};
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromPyString},
    pyo3::prelude::*,
};

/// The value for the $PnN key (all versions).
///
/// This cannot contain commas or be empty.
#[derive(Clone, Eq, PartialEq, Hash, Debug, AsRef, Display, Into, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyString))]
#[as_ref(str, NEStr)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct Shortname(NEString);

impl Shortname {
    pub(crate) fn new_unchecked<T: AsRef<str>>(s: T) -> Self {
        let ss: &str = s.as_ref();
        assert!(!ss.contains(','), "shortname has at least one comma");
        let ne = ss.parse().unwrap();
        Self(ne)
    }

    pub(crate) fn increment(&self, i: usize) -> Self {
        let mut n = self.clone();
        n.0.push(DEDUP_PNN_SEP);
        n.0.push_str(&i.to_string());
        n
    }
}

impl FromStr for Shortname {
    type Err = ShortnameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains(',') {
            Err(ShortnameError::Commas(s.into()))
        } else {
            Ok(Self(s.parse::<NEString>()?))
        }
    }
}

impl From<MeasIndex> for Shortname {
    fn from(value: MeasIndex) -> Self {
        let mut ret = ne_str!("P").to_owned();
        ret.push_str(&value.to_string());
        Self(ret)
    }
}

/// Error when parsing [`Shortname`] from string
#[derive(Debug, Error, From)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(
    feature = "python",
    pyerr(fireflow_types::python::ParseKeywordValueError)
)]
pub enum ShortnameError {
    #[error("commas are not allowed in name '{0}'")]
    Commas(String),
    #[error("{0}")]
    Empty(NonEmptyStringError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;

    #[test]
    fn str_to_shortname() {
        assert!("Thunderfist Chronicles".parse::<Shortname>().is_ok());
        assert_matches!(
            "Thunderfist,Chronicles".parse::<Shortname>(),
            Err(ShortnameError::Commas(_))
        );
        assert_matches!("".parse::<Shortname>(), Err(ShortnameError::Empty(_)));
    }
}
