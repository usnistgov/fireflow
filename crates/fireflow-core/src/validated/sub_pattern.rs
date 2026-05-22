use derive_more::Display;
use regex::Regex;
use thiserror::Error;

#[cfg(feature = "python")]
use fireflow_core_proc::DisplayAsPyErr;

/// Pattern to match a string and apply a sed-like substitution operation.
#[derive(Clone, Debug, Display)]
#[display("s/{from}/{to}{g}", g = if self.global { "/g" } else { "" })]
pub struct SubPattern {
    from: Regex,
    to: String,
    global: bool,
}

impl PartialEq for SubPattern {
    fn eq(&self, other: &Self) -> bool {
        self.from.as_str() == other.from.as_str()
            && self.to == other.to
            && self.global == other.global
    }
}

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
        let mut tmp = to.as_bytes().to_vec();
        let mut key;
        let mut blank_match = |k: &str| {
            let xs: Vec<_> = tmp
                .windows(k.len())
                .enumerate()
                .filter_map(|(i, x)| (x == k.as_bytes()).then_some(i))
                .collect();
            for i0 in xs {
                // Check for dollar signs in front of this reference. If number
                // is odd, one of them escapes this one which makes the match
                // not a real reference.
                let preceeding_dollar =
                    &tmp[0..i0].iter().rev().take_while(|&&c| c == b'$').count();
                if preceeding_dollar & 1 == 0 {
                    tmp[i0] = b' ';
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
        let mut ndollar: u8 = 0;
        for c in tmp {
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

/// Error when parsing [`SubPattern`] from string for configuration
#[derive(Debug, Error)]
#[error("References in '{to}' to not match capture patterns in '{from}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(fireflow_types::python::ConfigError))]
pub struct SubPatternError {
    from: Regex,
    to: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sub_pattern_nocap() {
        let r = Regex::new("a").unwrap();
        assert!(SubPattern::try_new(r.clone(), "b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "$$b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "$$b$$".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${0}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${1}b".into(), true).is_err());
        assert!(SubPattern::try_new(r.clone(), "$b".into(), true).is_err());
        assert!(SubPattern::try_new(r, "$$$b".into(), true).is_err());
    }

    #[test]
    fn sub_pattern_icap1() {
        let r = Regex::new("b(a)").unwrap();
        assert!(SubPattern::try_new(r.clone(), "b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "$$b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${0}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${1}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${2}b".into(), true).is_err());
        assert!(SubPattern::try_new(r, "${x}b".into(), true).is_err());
    }

    #[test]
    fn sub_pattern_ncap1() {
        let r = Regex::new("b(?<x>a)").unwrap();
        assert!(SubPattern::try_new(r.clone(), "b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "$$b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${0}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${1}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${x}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r, "${2}b".into(), true).is_err());
    }

    #[test]
    fn sub_pattern_cap2() {
        let r = Regex::new("baaaaaa(?<x>a)waaaaaaa([42]+)").unwrap();
        assert!(SubPattern::try_new(r.clone(), "b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "$$b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${0}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${1}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${2}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${x}b".into(), true).is_ok());
        assert!(SubPattern::try_new(r.clone(), "${x}b${0}${1}".into(), true).is_ok());
        assert!(SubPattern::try_new(r, "${y}b".into(), true).is_err());
    }
}

#[cfg(feature = "python")]
mod python {
    use super::SubPattern;

    use fireflow_types::python::ConfigError;

    use pyo3::prelude::*;
    use pyo3::types::PyTuple;
    use regex::Regex;

    // this is like (str, str, bool)
    impl<'py> FromPyObject<'_, 'py> for SubPattern {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let (r, to, global): (String, String, bool) = obj.extract()?;
            let from = r
                .parse::<Regex>()
                .map_err(|e| ConfigError::new_err(e.to_string()))?;
            Ok(Self::try_new(from, to, global)?)
        }
    }

    impl<'py> IntoPyObject<'py> for SubPattern {
        type Target = PyTuple;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.from.as_str(), self.to, self.global).into_pyobject(py)
        }
    }
}
