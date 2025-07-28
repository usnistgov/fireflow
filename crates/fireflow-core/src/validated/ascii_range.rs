//! Types representing $PnR/$PnB keys for an Ascii column.

use crate::error::{DeferredExt, DeferredResult, ResultExt};
use crate::text::byteord::{Width, WidthToCharsError};
use crate::text::keywords::{IntRangeError, Range};

use derive_more::{Display, From, Into};
use std::fmt;
use std::num::{NonZero, NonZeroU8};

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// The type of an ASCII column in all versions
///
/// Fields are private to guarantee they are always in sync.
#[derive(PartialEq, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AsciiRange {
    /// The maximum value of the ASCII column
    value: u64,

    /// Number of chars used to express this range.
    ///
    /// Must be able to hold ['value'] in ASCII digits but can be greater.
    chars: Chars,
}

/// The number of chars for an ASCII measurement
///
/// Must be an integer between 1 and 20.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Hash, Display, Into)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[into(NonZeroU8, u8)]
pub struct Chars(NonZeroU8);

/// Width to use when parsing OTHER segments.
///
/// Must be an integer between 1 and 20.
#[derive(Clone, Copy, Into, From)]
#[cfg_attr(feature = "python", derive(FromPyObject))]
#[into(u8, Chars)]
pub struct OtherWidth(pub Chars);

const MAX_CHARS: u8 = 20;

impl TryFrom<Range> for Chars {
    type Error = IntRangeError<u64>;

    fn try_from(value: Range) -> Result<Self, Self::Error> {
        u64::try_from(value).map(Chars::from_u64)
    }
}

impl From<u64> for AsciiRange {
    fn from(value: u64) -> Self {
        let chars = Chars::from_u64(value);
        Self { value, chars }
    }
}

impl From<&AsciiRange> for Range {
    fn from(value: &AsciiRange) -> Self {
        value.value.into()
    }
}

impl AsciiRange {
    pub fn try_new(value: u64, chars: Chars) -> Result<Self, NotEnoughCharsError> {
        let needed = Chars::from_u64(value);
        if chars < needed {
            Err(NotEnoughCharsError { value, chars })
        } else {
            Ok(Self { value, chars })
        }
    }

    // /// Make new AsciiRange from a float or integer.
    // ///
    // /// The number of chars will be automatically selected as the minimum
    // /// required to express the range.
    // pub(crate) fn from_range(range: FloatOrInt, notrunc: bool) -> BiTentative<Self, IntRangeError> {
    //     range.as_uint::<u64>(notrunc).map(AsciiRange::from)
    // }

    /// Make new AsciiRange from $PnB and $PnR values.
    ///
    /// Will return an error if $PnB is too small to hold $PnR.
    pub(crate) fn from_width_and_range(
        width: Width,
        range: Range,
        notrunc: bool,
    ) -> DeferredResult<Self, IntRangeError<()>, NewAsciiRangeError> {
        Chars::try_from(width)
            .into_deferred()
            .def_and_maybe(|chars| {
                range
                    .into_uint(notrunc)
                    .inner_into()
                    .and_maybe(|value| Self::try_new(value, chars).into_deferred())
            })
    }

    pub(crate) fn chars(&self) -> Chars {
        self.chars
    }

    pub(crate) fn value(&self) -> u64 {
        self.value
    }
}

impl Chars {
    /// Return number of chars needed to express the given u64.
    pub(crate) fn from_u64(x: u64) -> Self {
        // ASSUME the max possible value is 20 thus will always fit in u8
        Chars(
            x.checked_ilog10()
                .map(|y| y as u8)
                .and_then(|y| NonZero::new(y + 1))
                .unwrap_or(NonZeroU8::MIN),
        )
    }
}

impl TryFrom<u8> for Chars {
    type Error = CharsError;
    /// Return the number of chars represented by this if 20 or less.
    ///
    /// 20 is the maximum number of digits representable by an unsigned integer,
    /// which is the numeric type used to back ASCII data.
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match NonZeroU8::try_from(value) {
            Ok(x) => x.try_into(),
            _ => Err(CharsError(value)),
        }
    }
}

impl TryFrom<NonZeroU8> for Chars {
    type Error = CharsError;
    /// Return the number of chars represented by this if 20 or less.
    ///
    /// 20 is the maximum number of digits representable by an unsigned integer,
    /// which is the numeric type used to back ASCII data.
    fn try_from(value: NonZeroU8) -> Result<Self, Self::Error> {
        if u8::from(value) <= MAX_CHARS {
            Ok(Self(value))
        } else {
            Err(CharsError(u8::from(value)))
        }
    }
}

impl Default for OtherWidth {
    fn default() -> OtherWidth {
        OtherWidth(Chars(NonZeroU8::new(8).unwrap()))
    }
}

impl TryFrom<u8> for OtherWidth {
    type Error = OtherWidthError;

    fn try_from(x: u8) -> Result<Self, Self::Error> {
        Chars::try_from(x)
            .map_err(|e| OtherWidthError(e.0))
            .map(Self)
    }
}

pub struct CharsError(u8);

impl fmt::Display for CharsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "bits must be <= 20 to be used as number of characters, got {}",
            self.0
        )
    }
}

#[derive(Display, From)]
pub enum NewAsciiRangeError {
    New(NotEnoughCharsError),
    Width(WidthToCharsError),
    Range(IntRangeError<()>),
}

pub struct NotEnoughCharsError {
    chars: Chars,
    value: u64,
}

impl fmt::Display for NotEnoughCharsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "not enough chars to hold {}, got {}",
            self.value, self.chars
        )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u8_to_chars() {
        assert_eq!(Chars::try_from(1_u8).is_ok(), true);
        assert_eq!(Chars::try_from(0_u8).is_ok(), false);
        assert_eq!(Chars::try_from(20_u8).is_ok(), true);
        assert_eq!(Chars::try_from(21_u8).is_ok(), false);
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{Chars, CharsError};
    use crate::python::macros::impl_value_err;
    use pyo3::prelude::*;

    impl<'py> FromPyObject<'py> for Chars {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let x: u8 = ob.extract()?;
            let ret = Chars::try_from(x)?;
            Ok(ret)
        }
    }

    impl_value_err!(CharsError);
}
