use chrono::{NaiveTime, ParseError, Timelike};
use derive_more::{AsRef, From};
use std::str::FromStr;
use thiserror::Error;

/// A String that matches a time.
///
/// To be used when parsing time using [`NaiveTime::parse_from_str`].
///
/// This will contain all the formatting specificers native to chrono which
/// encode for time (hours, minutes, seconds, less than seconds). Additionally,
/// it will include two new identifiers for 60th seconds (%!) centiseconds (%@)
/// which are present in FCS 3.0 and FCS 3.1+ respectively. These are
/// incompatible with any other sub-second identifiers. Since chrono cannot
/// process these natively, these identifiers will be substituted with
/// nanosecond fraction (%f) and converted after parsing.
#[derive(Clone, Debug, AsRef)]
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
        let t = NaiveTime::parse_from_str(s, self.pat.as_str())?;
        match &self.fraction {
            FractionType::Native => Ok(t),
            FractionType::Centisecond => {
                // "nanoseconds" are actually centiseconds, so make sure they
                // don't exceed 99 and then convert to real nanoseconds
                let c = t.nanosecond();
                if c > 99 {
                    Err(ParseWithTimePatternError::ExceededCenti)
                } else {
                    Ok(t.with_nanosecond(c * 10_000_000).unwrap())
                }
            }
            FractionType::Sexagesimal => {
                // "nanoseconds" are actually 1/60 seconds, so make sure they
                // don't exceed 59 and then convert to real nanoseconds
                let c = t.nanosecond();
                if c > 59 {
                    Err(ParseWithTimePatternError::ExceededSexa)
                } else {
                    Ok(t.with_nanosecond(c * 1_000_000_000 / 60).unwrap())
                }
            }
        }
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
        let nsexa = has_spec("%!")?;
        let ncenti = has_spec("%@")?;
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
                (s.replace("%!", "%f"), FractionType::Sexagesimal)
            } else if ncenti {
                (s.replace("%@", "%f"), FractionType::Centisecond)
            } else {
                (s.into(), FractionType::Native)
            };
            Ok(Self { pat, fraction })
        } else {
            Err(TimePatternError(s.into()))
        }
    }
}

#[derive(Debug, Error)]
#[error(
    "time pattern must contain specifier for hour (%H/%k for 24 hours \
     or %I/%l with %p/%P for 12 hours), minute (%M), second (%S), and \
     optionally sub-second (%f, %3f, %6f, %9f, %.f, %.3f, %.6f, %.9f, \
     %!, or %@) where '%!' corresponds to 1/60th seconds and '%@' \
     corresponds to centiseconds; got {0}"
)]
pub struct TimePatternError(String);

#[derive(From, Debug, Error)]
pub enum ParseWithTimePatternError {
    #[error("{0}")]
    Native(ParseError),
    #[error("centiseconds exceeded 99")]
    ExceededCenti,
    #[error("1/60th fraction seconds exceeded 60")]
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

#[cfg(feature = "python")]
mod python {
    use super::{TimePattern, TimePatternError};
    use crate::python::macros::{impl_from_py_via_fromstr, impl_value_err};

    impl_from_py_via_fromstr!(TimePattern);
    impl_value_err!(TimePatternError);
}
