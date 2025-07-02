use derive_more::Into;
use std::fmt;

/// Width to use when parsing OTHER segments.
///
/// Must be integer between 1 and 20.
#[derive(Clone, Copy, Into)]
pub struct OtherWidth(u8);

impl Default for OtherWidth {
    fn default() -> OtherWidth {
        OtherWidth(8)
    }
}

impl TryFrom<u8> for OtherWidth {
    type Error = OtherWidthError;

    fn try_from(x: u8) -> Result<Self, Self::Error> {
        if (1..=20).contains(&x) {
            Ok(Self(x))
        } else {
            Err(OtherWidthError(x))
        }
    }
}

#[derive(Debug)]
pub struct OtherWidthError(u8);

impl fmt::Display for OtherWidthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "OTHER width should be integer b/t 1 and 20, got {}",
            self.0
        )
    }
}
