use super::keys::KeyOrStringPatterns;

use regex::Regex;
use thiserror::Error;

/// Pattern to match a sed-like substitution operation.
#[derive(Clone)]
pub struct SubPattern {
    from: Regex,
    to: String,
    global: bool,
}

pub type SubPatterns = KeyOrStringPatterns<SubPattern>;

impl SubPattern {
    pub fn try_new(from: Regex, to: String, global: bool) -> Result<Self, SubPatternError> {
        // Verify that all references in 'to' match captures in 'from'. For
        // sanity, only consider bracketed references such as '${666}' and not
        // '$666' since the latter is ambiguous given that valid reference
        // characters may come after.
        //
        // To do this, look for all capture references and 'blank' them in 'to'.
        // If 'to' is valid, it should have no more references (ie no unescaped
        // '$' characters).
        //
        // ASSUME We can get away using raw bytes to access 'to' in the blanking
        // step since we know that the only characters that should match will be
        // ASCII characters.
        let mut tmp = to.clone();
        let mut key;
        let mut blank_match = |k: &str| {
            let xs: Vec<_> = tmp.match_indices(k).map(|x| x.0).collect();
            for i0 in xs {
                // Check for dollar signs in front of this reference. If number
                // is odd, one of them escapes this one which makes the match
                // not a real reference.
                let preceeding_dollar = &tmp.as_bytes()[0..i0]
                    .iter()
                    .rev()
                    .take_while(|&&c| c == b'$')
                    .count();
                if preceeding_dollar & 1 == 0 {
                    unsafe { tmp.as_bytes_mut()[i0] = 32 }
                }
            }
        };
        for n in from.capture_names().flatten() {
            key = format!("${{{n}}}");
            blank_match(key.as_str());
        }
        for i in 0..from.captures_len() {
            key = format!("${{{i}}}");
            blank_match(key.as_str());
        }
        let mut ndollar = 0;
        for &c in tmp.as_bytes() {
            if c == b'$' {
                ndollar += 1;
            } else if ndollar & 1 == 1 {
                break;
            } else {
                ndollar = 0;
            }
        }
        if ndollar & 1 == 1 {
            return Err(SubPatternError { from, to });
        }
        Ok(Self { from, to, global })
    }

    pub(crate) fn sub(&self, value: &str) -> String {
        let s = if self.global {
            self.from.replace_all(value, &self.to)
        } else {
            self.from.replace(value, &self.to)
        };
        s.into_owned()
    }
}

#[derive(Debug, Error)]
#[error("References in '{to}' to not match capture patterns in '{from}'")]
pub struct SubPatternError {
    from: Regex,
    to: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sub_pattern_nocap() {
        let r = Regex::new("a").unwrap();
        assert!(SubPattern::try_new(r.clone(), "b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "$$b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "$$b$$".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${0}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${1}b".into(), true).is_err());
        assert!(SubPattern::try_new(r.clone(), "$b".into(), true).is_err());
        assert!(SubPattern::try_new(r.clone(), "$$$b".into(), true).is_err());
    }

    #[test]
    fn test_sub_pattern_icap1() {
        let r = Regex::new("b(a)").unwrap();
        assert!(SubPattern::try_new(r.clone(), "b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "$$b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${0}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${1}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${2}b".into(), true).is_err());
        assert!(SubPattern::try_new(r.clone(), "${x}b".into(), true).is_err());
    }

    #[test]
    fn test_sub_pattern_ncap1() {
        let r = Regex::new("b(?<x>a)").unwrap();
        assert!(SubPattern::try_new(r.clone(), "b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "$$b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${0}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${1}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${x}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${2}b".into(), true).is_err());
    }

    #[test]
    fn test_sub_pattern_cap2() {
        let r = Regex::new("baaaaaa(?<x>a)waaaaaaa([42]+)").unwrap();
        assert!(SubPattern::try_new(r.clone(), "b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "$$b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${0}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${1}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${2}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${x}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${x}b${0}${1}".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${y}b".into(), true).is_err());
    }
}

#[cfg(feature = "python")]
mod python {
    use crate::python::macros::impl_value_err;

    use super::{SubPattern, SubPatternError, SubPatterns};

    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use regex::Regex;

    impl_value_err!(SubPatternError);

    impl<'py> FromPyObject<'py> for SubPattern {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (r, to, global): (String, String, bool) = ob.extract()?;
            let from = r
                .parse::<Regex>()
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            Ok(Self::try_new(from, to, global)?)
        }
    }

    type _SubPattern = Vec<(String, SubPattern)>;

    // pass subpatterns via config as a tuple like ({String, (...)}, {String, (...)})
    // where the first member is literal strings and the second is regex patterns
    impl<'py> FromPyObject<'py> for SubPatterns {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (lits, pats): (_SubPattern, _SubPattern) = ob.extract()?;
            let mut ret = SubPatterns::try_from_literals(lits)?;
            // this is just a regexp error
            let ps = SubPatterns::try_from_patterns(pats)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            ret.extend(ps);
            Ok(ret)
        }
    }

    // impl_value_err!(KeyStringPairsError);
}
