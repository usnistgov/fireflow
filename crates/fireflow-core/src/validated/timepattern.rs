use fireflow_types::config::{BASE60_SECOND_SPEC, BASE100_SECOND_SPEC};

use chrono::format::strftime::StrftimeItems;
use chrono::format::{Fixed, Item, Numeric, Parsed, parse};
use chrono::{NaiveTime, ParseError, Timelike as _};
use derive_more::{AsRef, Display, From};
use derive_new::new;
use thiserror::Error;

use std::str::FromStr;

#[cfg(feature = "python")]
use fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString};

/// A [`String`] that matches a time.
///
/// To be used when parsing time using [`NaiveTime::parse_from_str`].
///
/// This will contain all the formatting specificers native to chrono which
/// encode for time (hours, minutes, seconds, less than seconds). Additionally,
/// it will include two new identifiers for 60th seconds (`"%!"`) centiseconds
/// (`"%@"`) which are present in FCS 3.0 and FCS 3.1+ respectively. These are
/// incompatible with any other sub-second identifiers. Since chrono cannot
/// process these natively, these identifiers will be substituted with
/// nanosecond fraction (`"%f"`) and converted after parsing.
#[derive(Clone, Debug, AsRef, Display)]
#[display("{original}")]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub struct TimePattern {
    #[as_ref([Item<'static>])]
    pat: Vec<Item<'static>>,
    #[as_ref(str)]
    original: String,
    fraction: FractionType,
}

#[derive(Clone, Debug)]
enum FractionType {
    Native,
    Sexagesimal,
    Centisecond,
}

impl TimePattern {
    pub(crate) fn parse_str(&self, s: &str) -> Result<NaiveTime, ParseWithTimePatternError> {
        let go = || {
            let mut p = Parsed::new();
            parse(&mut p, s, self.pat.iter())?;
            let t = p.to_naive_time()?;
            match &self.fraction {
                FractionType::Native => Ok(t),
                FractionType::Centisecond => {
                    // "nanoseconds" are actually centiseconds, so make sure they
                    // don't exceed 99 and then convert to real nanoseconds
                    let c = t.nanosecond();
                    if c > 99 {
                        Err(InnerPatternError::ExceededCenti)
                    } else {
                        Ok(t.with_nanosecond(c * 10_000_000).unwrap())
                    }
                }
                FractionType::Sexagesimal => {
                    // "nanoseconds" are actually 1/60 seconds, so make sure they
                    // don't exceed 59 and then convert to real nanoseconds
                    let c = t.nanosecond();
                    if c > 59 {
                        Err(InnerPatternError::ExceededSexa)
                    } else {
                        Ok(t.with_nanosecond(c * 1_000_000_000 / 60).unwrap())
                    }
                }
            }
        };
        go().map_err(|e| ParseWithTimePatternError::new(e, self.original.clone()))
    }
}

impl FromStr for TimePattern {
    type Err = TimePatternError;

    fn from_str(s: &str) -> Result<Self, TimePatternError> {
        let mut hour24 = 0_usize;
        let mut hour12 = 0_usize;
        let mut am_pm = 0_usize;
        let mut minute = 0_usize;
        let mut second = 0_usize;
        let mut frac_second = 0_usize;
        let mut invalid = 0_usize;
        let mut fraction = FractionType::Native;

        // Parse the entire pattern string to components. Don't error if an
        // invalid pattern is found because we might replace it later.
        let mut pat: Vec<_> = StrftimeItems::new_lenient(s).parse_to_owned().unwrap();

        // Iterate through components, replacing instances of sexagesimal and
        // centisecond patterns with fractional patterns that will be parsed
        // specially later. Also track how many of each thing we encounter.
        for item in &mut pat {
            match item {
                Item::OwnedLiteral(x) => {
                    let y = x.as_ref();
                    if y == BASE60_SECOND_SPEC {
                        frac_second += 1;
                        fraction = FractionType::Sexagesimal;
                        *item = Item::Fixed(Fixed::Nanosecond);
                    } else if y == BASE100_SECOND_SPEC {
                        frac_second += 1;
                        fraction = FractionType::Centisecond;
                        *item = Item::Fixed(Fixed::Nanosecond);
                    } else if y.starts_with('%') {
                        invalid += 1;
                    }
                }
                Item::OwnedSpace(_) => (),
                Item::Numeric(y, _) => match y {
                    Numeric::Hour => hour24 += 1,
                    Numeric::Hour12 => hour12 += 1,
                    Numeric::Minute => minute += 1,
                    Numeric::Second => second += 1,
                    Numeric::Nanosecond => frac_second += 1,
                    Numeric::Internal(_) => debug_assert!(false, "this should never happen"),
                    _ => invalid += 1,
                },
                Item::Fixed(y) => match y {
                    Fixed::LowerAmPm | Fixed::UpperAmPm => am_pm += 1,
                    Fixed::Nanosecond
                    | Fixed::Nanosecond3
                    | Fixed::Nanosecond6
                    | Fixed::Nanosecond9 => frac_second += 1,
                    Fixed::Internal(_) => debug_assert!(false, "this should never happen"),
                    _ => invalid += 1,
                },
                // No errors because we used lenient above. Only owned values
                // because we converted to owned above.
                Item::Error | Item::Literal(_) | Item::Space(_) => {
                    debug_assert!(false, "this should never happen");
                }
            }
        }

        // Ensure we have at least hours and minutes, seconds and frac seconds
        // are expendable
        if (hour24 == 1 || (hour12 == 1 && am_pm == 1))
            && minute == 1
            && second < 2
            && frac_second < 2
            && invalid == 0
        {
            Ok(Self {
                pat,
                original: s.into(),
                fraction,
            })
        } else {
            Err(TimePatternError(s.into()))
        }
    }
}

/// Error when parsing [`TimePattern`] from string for configuration
#[derive(Debug, Error)]
#[error(
    "time pattern must contain specifier for hour (%H/%k for 24 hours \
     or %I/%l with %p/%P for 12 hours), minute (%M), second (%S), and \
     optionally sub-second (%f, %3f, %6f, %9f, %.f, %.3f, %.6f, %.9f, \
     {BASE60_SECOND_SPEC}, or {BASE100_SECOND_SPEC}) where '{BASE60_SECOND_SPEC}' \
     corresponds to 1/60th seconds and '{BASE100_SECOND_SPEC}' corresponds to \
     centiseconds; got {0}"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(fireflow_types::python::ConfigError))]
pub struct TimePatternError(String);

/// Error when parsing [`NaiveTime`] from string using [`TimePattern`]
#[derive(Debug, Error, new)]
#[error("{inner} with pattern '{pattern}'")]
pub struct ParseWithTimePatternError {
    inner: InnerPatternError,
    pattern: String,
}

#[derive(From, Debug, Display)]
enum InnerPatternError {
    #[display("{_0}")]
    Native(ParseError),
    #[display("centiseconds exceeded 99")]
    ExceededCenti,
    #[display("1/60th fraction seconds exceeded 60")]
    ExceededSexa,
}

// TODO property tests would likely be useful here
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_to_pattern() {
        assert!("%H:%M:%S".parse::<TimePattern>().is_ok());
        assert!("%H::::::::%M:::::::%S".parse::<TimePattern>().is_ok());
        assert!("%H%H:%M:%S".parse::<TimePattern>().is_err());
        assert!("%H:%M".parse::<TimePattern>().is_ok());
    }
}
