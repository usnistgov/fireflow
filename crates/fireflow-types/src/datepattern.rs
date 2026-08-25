use derive_more::{AsRef, Display};
use thiserror::Error;

use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString};

/// A [`String`] that matches a date.
///
/// This is a configuration value to be used when parsing a date using
/// [`NaiveDate::parse_from_str`](chrono::NaiveDate::parse_from_str).
#[derive(Clone, Debug, AsRef, PartialEq, Display)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct DatePattern(String);

impl FromStr for DatePattern {
    type Err = DatePatternError;

    fn from_str(s: &str) -> Result<Self, DatePatternError> {
        let count_spec = |spec: &'static str| s.match_indices(spec).count();
        #[allow(non_snake_case)]
        let nY = count_spec("%Y");
        let ny = count_spec("%y");
        let nm = count_spec("%m");
        let nb = count_spec("%b");
        #[allow(non_snake_case)]
        let nB = count_spec("%B");
        let nd = count_spec("%d");
        let ne = count_spec("%e");
        let y = matches!((nY, ny), (1, 0) | (0, 1));
        let m = matches!((nm, nb, nB), (1, 0, 0) | (0, 1, 0) | (0, 0, 1));
        let d = matches!((nd, ne), (1, 0) | (0, 1));
        if y && m && d {
            Ok(Self(s.into()))
        } else {
            Err(DatePatternError(s.into()))
        }
    }
}
/// Error when parsing [`DatePattern`] from string
#[derive(Debug, Error)]
#[error(
    "date pattern must contain specifier for year (%y or %Y), \
     month (%m, %b, or %B), and day (%d or %e), got {0}"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct DatePatternError(String);

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // test every ymd permutation, all of which should be valid
    proptest! {
        #[test]
        fn str_to_pattern(
            s in "([^%]*%[yY][^%]*%[mbB][^%]*%[de][^%]*|\
                   [^%]*%[yY][^%]*%[de][^%]*%[mbB][^%]*|\
                   [^%]*%[de][^%]*%[mbB][^%]*%[yY][^%]*|\
                   [^%]*%[de][^%]*%[yY][^%]*%[mbB][^%]*|\
                   [^%]*%[mbB][^%]*%[de][^%]*%[yY][^%]*|\
                   [^%]*%[mbB][^%]*%[yY][^%]*%[de][^%]*)"
        ) {
            assert!(s.parse::<DatePattern>().is_ok());
        }
    }

    #[test]
    fn str_to_pattern_invalid() {
        assert!("%y%y%m%d".parse::<DatePattern>().is_err());
        assert!("%m%d".parse::<DatePattern>().is_err());
    }

    // known patterns in FCS files that should work
    #[test]
    fn str_to_pattern_known_fcs() {
        assert!("%Y-%b-%d".parse::<DatePattern>().is_ok());
        assert!("%d-%b-%Y".parse::<DatePattern>().is_ok());
    }
}
