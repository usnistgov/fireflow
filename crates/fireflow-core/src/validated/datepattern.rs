use derive_more::{AsRef, Display};
use std::fmt;
use std::str::FromStr;

/// A String that matches a date.
///
/// To be used when parsing date using ['NaiveDate::parse_from_str'].
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

#[derive(Debug)]
pub struct DatePatternError(String);

impl fmt::Display for DatePatternError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "date pattern must contain specifier for year (%y or %Y), \
                     month (%m, %b, or %B), and day (%d or %e), got {}",
            self.0
        )
    }
}
