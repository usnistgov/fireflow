//! Types representing $PnR/$PnB keys for an Ascii column.

use crate::error::{BiTentative, DeferredExt, DeferredResult, ResultExt};
use crate::macros::{
    enum_from, enum_from_disp, match_many_to_one, newtype_disp, newtype_from_outer,
};
use crate::text::byteord::{Width, WidthToCharsError};
use crate::text::float_or_int::{FloatOrInt, IntRangeError};
use crate::text::keywords::Range;

use serde::Serialize;
use std::fmt;

/// The type of an ASCII column in all versions
///
/// Fields are private to guarantee they are always in sync.
#[derive(PartialEq, Clone, Copy, Serialize)]
pub struct AsciiRange {
    /// The maximum value of the ASCII column
    value: u64,

    /// Number of chars used to express this range.
    ///
    /// Must be able to hold ['value'] in ASCII digits but can be greater.
    chars: Chars,
}

/// The number of chars or an ASCII measurement
#[derive(Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct Chars(u8);

const MAX_CHARS: u8 = 20;

newtype_disp!(Chars);
newtype_from_outer!(Chars, u8);

impl From<u64> for AsciiRange {
    fn from(value: u64) -> Self {
        let chars = Chars::from_u64(value);
        Self { value, chars }
    }
}

impl AsciiRange {
    pub(crate) fn try_new(value: u64, chars: Chars) -> Result<Self, NewAsciiRangeError> {
        let needed = Chars::from_u64(value);
        if chars < needed {
            Err(NewAsciiRangeError { value, chars })
        } else {
            Ok(Self { value, chars })
        }
    }

    /// Make new AsciiRange from a float or integer.
    ///
    /// The number of chars will be automatically selected as the minimum
    /// required to express the range.
    pub(crate) fn from_range(range: Range, notrunc: bool) -> BiTentative<Self, IntRangeError> {
        range.0.as_uint(notrunc).map(AsciiRange::from)
    }

    /// Make new AsciiRange from $PnB and $PnR values.
    ///
    /// Will return an error if $PnB is too small to hold $PnR.
    pub(crate) fn from_width_and_range(
        width: Width,
        range: FloatOrInt,
        notrunc: bool,
    ) -> DeferredResult<Self, IntRangeError, AsciiRangeFromWidthRangeError> {
        Chars::try_from(width)
            .into_deferred()
            .def_and_maybe(|chars| {
                range
                    .as_uint(notrunc)
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
    pub(crate) fn max() -> Self {
        Self(MAX_CHARS)
    }

    /// Return number of chars needed to express the given u64.
    pub(crate) fn from_u64(x: u64) -> Self {
        // ASSUME the max possible value is 20 thus will always fit in u8
        Chars(x.checked_ilog10().map(|y| y + 1).unwrap_or(1) as u8)
    }
}

impl TryFrom<u8> for Chars {
    type Error = CharsError;
    /// Return the number of chars represented by this if 20 or less.
    ///
    /// 20 is the maximum number of digits representable by an unsigned integer,
    /// which is the numeric type used to back ASCII data.
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if !(1..=MAX_CHARS).contains(&value) {
            Err(CharsError(value))
        } else {
            Ok(Self(value))
        }
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

enum_from_disp!(
    pub AsciiRangeFromWidthRangeError,
    [New, NewAsciiRangeError],
    [Width, WidthToCharsError],
    [Range, IntRangeError]
);

pub struct NewAsciiRangeError {
    chars: Chars,
    value: u64,
}

impl fmt::Display for NewAsciiRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "not enough chars to hold {}, got {}",
            self.value, self.chars
        )
    }
}
