use chrono::format::strftime::StrftimeItems;
use chrono::format::{Fixed, Item, Numeric};
use derive_more::{AsRef, Display};
use thiserror::Error;

use std::str::FromStr;

#[cfg(feature = "python")]
use fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString};

/// A [`String`] that matches a date.
///
/// This is a configuration value to be used when parsing a date using
/// [`NaiveDate::parse_from_str`](chrono::NaiveDate::parse_from_str).
#[derive(Clone, Debug, AsRef, Display)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub struct DatePattern(String);

impl FromStr for DatePattern {
    type Err = DatePatternError;

    fn from_str(s: &str) -> Result<Self, DatePatternError> {
        let mut year = 0_usize;
        let mut month = 0_usize;
        let mut day = 0_usize;
        let mut invalid = 0_usize;

        let pat = StrftimeItems::new(s)
            .parse()
            .map_err(|_| DatePatternError(s.to_owned()))?;

        for item in &pat {
            match item {
                Item::Numeric(y, _) => match y {
                    Numeric::Day => day += 1,
                    Numeric::Month => month += 1,
                    Numeric::Year | Numeric::YearMod100 => year += 1,
                    Numeric::Internal(_) => debug_assert!(false, "this should never happen"),
                    _ => invalid += 1,
                },
                Item::Fixed(y) => match y {
                    Fixed::LongMonthName | Fixed::ShortMonthName => month += 1,
                    Fixed::Internal(_) => debug_assert!(false, "this should never happen"),
                    _ => invalid += 1,
                },
                Item::OwnedLiteral(_) | Item::OwnedSpace(_) | Item::Literal(_) | Item::Space(_) => {
                }
                // No errors because we parsed above.
                Item::Error => {
                    debug_assert!(false, "this should never happen");
                }
            }
        }

        if year == 1 && month == 1 && day == 1 && invalid == 0 {
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
#[cfg_attr(feature = "python", pyerr(fireflow_types::python::ConfigError))]
pub struct DatePatternError(String);

// TODO property tests would likely be useful here
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_to_pattern() {
        assert!("%y%m%d".parse::<DatePattern>().is_ok());
        assert!("%yrandom%mmorerandom%d".parse::<DatePattern>().is_ok(),);
        assert!("%y%y%m%d".parse::<DatePattern>().is_err());
        assert!("%m%d".parse::<DatePattern>().is_err());
        assert!("%H%y%m%d".parse::<DatePattern>().is_err());
    }
}
