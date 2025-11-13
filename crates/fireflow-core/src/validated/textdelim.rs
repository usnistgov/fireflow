use derive_more::Into;
use thiserror::Error;

#[cfg(feature = "python")]
use fireflow_core_proc::IntoBuiltinPyErr;

/// The delimiter used when writing TEXT
#[derive(Clone, Copy, Into)]
pub struct TEXTDelim(u8);

impl Default for TEXTDelim {
    fn default() -> Self {
        Self(30) // record separator
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

#[derive(Debug, Error)]
#[error("delimiter should be char b/t 1 and 126, got {0}")]
#[cfg_attr(feature = "python", derive(IntoBuiltinPyErr), pyerr(PyValueError))]
pub struct TEXTDelimError(u8);

#[cfg(feature = "python")]
mod python {
    use crate::python::macros::impl_try_from_py;

    impl_try_from_py!(super::TEXTDelim, u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u8_to_delim() {
        assert!(TEXTDelim::try_from(1_u8).is_ok());
        assert!(TEXTDelim::try_from(126_u8).is_ok());
        assert!(TEXTDelim::try_from(0_u8).is_err());
        assert!(TEXTDelim::try_from(127_u8).is_err());
    }
}
