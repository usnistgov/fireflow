use fireflow_types::config::{BASE60_SECOND_SPEC, BASE100_SECOND_SPEC};

use chrono::{NaiveTime, ParseError, Timelike as _};
use derive_more::{AsRef, Display, From};
use derive_new::new;
use thiserror::Error;

use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

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
#[derive(Clone, Debug, AsRef, Display, PartialEq, new)]
#[display("{original}")]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[new(visibility(""))]
pub struct TimePattern {
    #[cfg_attr(feature = "serde", serde(skip))]
    #[as_ref(str)]
    pat: String,
    original: String,
    #[cfg_attr(feature = "serde", serde(skip))]
    fraction: FractionType,
}

#[derive(Clone, Debug, PartialEq)]
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
        go().map_err(|e| ParseWithTimePatternError::new(e, self.original.clone()))
    }
}

impl FromStr for TimePattern {
    type Err = TimePatternError;

    #[allow(non_snake_case)]
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
        let n_H = has_spec("%H")?;
        let n_k = has_spec("%k")?;
        // hours (12)
        let n_I = has_spec("%I")?;
        let n_l = has_spec("%l")?;
        let n_P = has_spec("%P")?;
        let n_p = has_spec("%p")?;
        // minutes
        let n_M = has_spec("%M")?;
        // seconds
        let n_S = has_spec("%S")?;
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
        let h = match (n_H, n_k, n_I, n_l, n_P, n_p) {
            // if 24 hour, allow only one and exclude 12 hour
            (x_nH, x_nk, false, false, false, false) => x_nH != x_nk,
            // if 12 hour, include one number and am/pm spec and exclude 24 hour
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
        if h && n_M && n_S && n_frac < 2 {
            let (pat, fraction) = if nsexa {
                let rep = s.replace(BASE60_SECOND_SPEC, "%f");
                (rep, FractionType::Sexagesimal)
            } else if ncenti {
                let rep = s.replace(BASE100_SECOND_SPEC, "%f");
                (rep, FractionType::Centisecond)
            } else {
                (s.into(), FractionType::Native)
            };
            Ok(Self::new(pat, s.to_owned(), fraction))
        } else {
            Err(TimePatternError(s.into()))
        }
    }
}

/// Error when parsing [`TimePattern`] from string for configuration
#[derive(Debug, Error, PartialEq, Clone)]
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
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error("{inner} with pattern '{pattern}'")]
pub struct ParseWithTimePatternError {
    inner: InnerPatternError,
    pattern: String,
}

#[derive(From, Debug, Display, PartialEq, Clone)]
enum InnerPatternError {
    #[display("{_0}")]
    Native(ParseError),
    #[display("centiseconds exceeded 99")]
    ExceededCenti,
    #[display("1/60th fraction seconds exceeded 60")]
    ExceededSexa,
}

#[cfg(test)]
mod tests {
    use super::*;

    use itertools::Itertools as _;
    use proptest::prelude::*;
    use proptest::sugar::NamedArguments;
    use proptest::test_runner::TestRunner;

    use std::iter::once;

    const H24_PAT: &str = "%[Hk]";
    const H12_PAT: &str = "%[Il]";
    const AMPM_PAT: &str = "%[Pp]";
    const M_PAT: &str = "%M";
    const S_PAT: &str = "%S";
    const F_PAT: &str = "(%\\.?[369]?f|%@|%!)";

    const PATS_24: [&str; 4] = [H24_PAT, M_PAT, S_PAT, F_PAT];
    const PATS_12: [&str; 5] = [H12_PAT, AMPM_PAT, M_PAT, S_PAT, F_PAT];
    const PATS_24_NO_FRAC: [&str; 3] = [H24_PAT, M_PAT, S_PAT];
    const PATS_12_NO_FRAC: [&str; 4] = [H12_PAT, AMPM_PAT, M_PAT, S_PAT];

    fn pat_regex(subpats: &[&str], n: usize) -> String {
        let inner = subpats
            .iter()
            .permutations(n)
            .map(|ps| {
                once("")
                    .chain(ps.into_iter().copied())
                    .chain(once(""))
                    .join("[^%]*")
            })
            .join("|");
        format!("({inner})")
    }

    fn test_permutations(subpats: &[&str], n: usize) {
        let mut runner = TestRunner::default();
        let pats = pat_regex(subpats, n);
        let r = pats.as_str().prop_map(|v| NamedArguments("s", v));
        let res = runner.run(&r, |NamedArguments(_, s)| {
            assert!(s.parse::<TimePattern>().is_ok());
            Ok(())
        });
        match res {
            Ok(()) => (),
            Err(e) => panic!("{e}\n{runner}"),
        }
    }

    #[test]
    fn str_to_pattern24() {
        test_permutations(&PATS_24, 4);
    }

    #[test]
    fn str_to_pattern12() {
        test_permutations(&PATS_12, 5);
    }

    #[test]
    fn str_to_pattern24_nofrac() {
        test_permutations(&PATS_24_NO_FRAC, 3);
    }

    #[test]
    fn str_to_pattern12_nofrac() {
        test_permutations(&PATS_12_NO_FRAC, 4);
    }

    #[test]
    fn str_to_pattern_invalid() {
        assert!("%H%H:%M:%S".parse::<TimePattern>().is_err());
        assert!("%H:%M".parse::<TimePattern>().is_err());
    }

    // known patterns FCS files that should work
    #[test]
    fn str_to_pattern_known_fcs() {
        assert!("%H:%M:%S:%3f".parse::<TimePattern>().is_ok());
        assert!("%H:%M:%S:%@".parse::<TimePattern>().is_ok());
        assert!("%H:%M:%S:%!".parse::<TimePattern>().is_ok());
    }
}
