use fireflow_types::config::{BASE60_SECOND_SPEC, BASE100_SECOND_SPEC};

use chrono::{NaiveTime, ParseError, Timelike as _};
use derive_more::{AsRef, Display, From};
use derive_new::new;
use thiserror::Error;

use std::fmt;
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
#[derive(Clone, Debug, AsRef)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub struct TimePattern {
    #[as_ref(str)]
    pat: String,
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
            let t = NaiveTime::parse_from_str(s, self.pat.as_str())?;
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
        go().map_err(|e| ParseWithTimePatternError::new(e, self.pat.clone()))
    }
}

impl FromStr for TimePattern {
    type Err = TimePatternError;

    fn from_str(s: &str) -> Result<Self, TimePatternError> {
        let has_spec = |spec: &'static str| {
            let n = s.match_indices(spec).count();
            if n > 1 {
                Err(TimePatternError(s.into()))
            } else {
                Ok(n == 1)
            }
        };
        // hours (24)
        #[allow(non_snake_case)]
        let nH = has_spec("%H")?;
        let nk = has_spec("%k")?;
        // hours (12)
        #[allow(non_snake_case)]
        let nI = has_spec("%I")?;
        let nl = has_spec("%l")?;
        #[allow(non_snake_case)]
        let nP = has_spec("%P")?;
        let np = has_spec("%p")?;
        // minutes
        #[allow(non_snake_case)]
        let nM = has_spec("%M")?;
        // seconds
        #[allow(non_snake_case)]
        let nS = has_spec("%S")?;
        // fractions of second (native)
        let nf = has_spec("%f")?;
        let n_3_f = has_spec("%3f")?;
        let n_6_f = has_spec("%6f")?;
        let n_9_f = has_spec("%9f")?;
        let n_f = has_spec("%.f")?;
        let n_d_3_f = has_spec("%.3f")?;
        let n_d_6_f = has_spec("%.6f")?;
        let n_d_9_f = has_spec("%.9f")?;
        // fractions of second (non-native)
        let nsexa = has_spec(BASE60_SECOND_SPEC)?;
        let ncenti = has_spec(BASE100_SECOND_SPEC)?;
        // check hour specs
        let h = match (nH, nk, nI, nl, nP, np) {
            // if 24 hour, allow only one and exclude 12 hour
            #[allow(non_snake_case)]
            (x_nH, x_nk, false, false, false, false) => x_nH != x_nk,
            // if 12 hour, include one number and am/pm spec and exclude 24 hour
            #[allow(non_snake_case)]
            (false, false, x_nI, x_n_l, x_nP, x_n_p) => (x_nI != x_n_l) && (x_nP != x_n_p),
            _ => false,
        };
        // only zero or one fractional patterns allowed
        let n_frac: u8 = [
            nf, n_3_f, n_6_f, n_9_f, n_f, n_d_3_f, n_d_6_f, n_d_9_f, nsexa, ncenti,
        ]
        .map(u8::from)
        .iter()
        .sum();
        if h && nM && nS && n_frac < 2 {
            let (pat, fraction) = if nsexa {
                (
                    s.replace(BASE60_SECOND_SPEC, "%f"),
                    FractionType::Sexagesimal,
                )
            } else if ncenti {
                (
                    s.replace(BASE100_SECOND_SPEC, "%f"),
                    FractionType::Centisecond,
                )
            } else {
                (s.into(), FractionType::Native)
            };
            Ok(Self { pat, fraction })
        } else {
            Err(TimePatternError(s.into()))
        }
    }
}

// TODO test that this perfectly mirrors FromStr
impl fmt::Display for TimePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self.fraction {
            FractionType::Native => self.pat.clone(),
            FractionType::Sexagesimal => self.pat.replace("%f", BASE60_SECOND_SPEC),
            FractionType::Centisecond => self.pat.replace("%f", BASE100_SECOND_SPEC),
        };
        write!(f, "{s}")
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
        assert!("%H:%M".parse::<TimePattern>().is_err());
    }
}
