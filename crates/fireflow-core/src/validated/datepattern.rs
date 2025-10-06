use derive_more::{AsRef, Display};
use std::str::FromStr;
use thiserror::Error;

/// A String that matches a date.
///
/// To be used when parsing date using [`NaiveDate::parse_from_str`].
#[derive(Clone, Debug, AsRef, Display)]
#[as_ref(str)]
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
            Ok(DatePattern(s.to_string()))
        } else {
            Err(DatePatternError(s.to_string()))
        }
    }
}

#[derive(Debug, Error)]
#[error(
    "date pattern must contain specifier for year (%y or %Y), \
     month (%m, %b, or %B), and day (%d or %e), got {0}"
)]
pub struct DatePatternError(String);

// TODO property tests would likely be useful here
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_str_to_pattern() {
        assert!("%y%m%d".parse::<DatePattern>().is_ok());
        assert!("%yrandom%mmorerandom%d".parse::<DatePattern>().is_ok(),);
        assert!("%y%y%m%d".parse::<DatePattern>().is_err());
        assert!("%m%d".parse::<DatePattern>().is_err());
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{DatePattern, DatePatternError};
    use crate::python::macros::{impl_from_py_via_fromstr, impl_value_err};

    impl_from_py_via_fromstr!(DatePattern);
    impl_value_err!(DatePatternError);
}
