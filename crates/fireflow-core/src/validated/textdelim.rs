use derive_more::Into;
use std::fmt;

/// The delimiter used when writing TEXT
#[derive(Clone, Copy, Into)]
pub struct TEXTDelim(u8);

impl Default for TEXTDelim {
    fn default() -> TEXTDelim {
        TEXTDelim(30) // record separator
    }
}

impl TryFrom<u8> for TEXTDelim {
    type Error = TEXTDelimError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if (1..127).contains(&value) {
            Ok(Self(value))
        } else {
            Err(TEXTDelimError(value))
        }
    }
}

pub struct TEXTDelimError(u8);

impl fmt::Display for TEXTDelimError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "delimiter should be char b/t 1 and 126, got {}", self.0)
    }
}

#[cfg(feature = "python")]
mod python {
    use crate::python::macros::{impl_try_from_py, impl_value_err};

    use super::{TEXTDelim, TEXTDelimError};

    impl_value_err!(TEXTDelimError);
    impl_try_from_py!(TEXTDelim, u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u8_to_delim() {
        assert_eq!(TEXTDelim::try_from(1_u8).is_ok(), true);
        assert_eq!(TEXTDelim::try_from(126_u8).is_ok(), true);
        assert_eq!(TEXTDelim::try_from(0_u8).is_ok(), false);
        assert_eq!(TEXTDelim::try_from(127_u8).is_ok(), false);
    }
}
