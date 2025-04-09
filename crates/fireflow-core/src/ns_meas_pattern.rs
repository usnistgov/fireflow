use std::fmt;
use std::str::FromStr;

/// A String that matches a non-standard measurement keyword.
///
/// This will have exactly one '%n' and not start with a '$'.
#[derive(Clone)]
pub struct NonStdMeasPattern(String);

impl AsRef<str> for NonStdMeasPattern {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl FromStr for NonStdMeasPattern {
    type Err = NonStdMeasPatternError;

    fn from_str(s: &str) -> Result<Self, NonStdMeasPatternError> {
        if s.starts_with("$") || s.match_indices("%n").count() != 1 {
            Err(NonStdMeasPatternError(s.to_string()))
        } else {
            Ok(NonStdMeasPattern(s.to_string()))
        }
    }
}

pub struct NonStdMeasPatternError(String);

impl fmt::Display for NonStdMeasPatternError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "Non standard measurement pattern must not \
             start with '$' and should have one '%n', found '{}'",
            self.0
        )
    }
}
