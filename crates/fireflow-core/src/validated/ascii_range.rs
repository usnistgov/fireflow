//! Types representing $PnR/$PnB keys for an Ascii column.

use crate::config::DisallowRangeTrunc;
use crate::data::{IndexedError, IndexedRangeToAsciiError, RangeToAsciiError};
use crate::logging::{ResultExt as _, WarningsAndErrorsResult};
use crate::text::byteord::WidthToFixedError;
use crate::text::index::MeasIndex;
use crate::text::keywords::{Range, RangeToIntError, Width};
use crate::validated::keys::IndexedKey as _;

use derive_more::{Display, From, Into};
use std::fmt;
use std::num::{NonZero, NonZeroU8};
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr};

/// The type of an ASCII column in all versions
///
/// This represents the value of $PnB and $PnR for one measurement.
///
/// Fields are private to guarantee they are always in sync.
#[derive(PartialEq, Clone, Copy, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AsciiRange {
    /// The maximum value of the ASCII column
    value: u64,

    /// Number of chars used to express this range.
    ///
    /// Must be able to hold `value` in ASCII digits but can be greater.
    chars: Chars,
}

/// Width to use when parsing OTHER segments.
///
/// Must be an integer between 1 and 20.
#[derive(Clone, Copy, Into, From)]
#[into(u8, Chars)]
pub struct OtherWidth(Chars);

/// The number of chars for an ASCII measurement
///
/// Must be an integer between 1 and 20.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Hash, Display, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[into(NonZeroU8, u8)]
pub(crate) struct Chars(NonZeroU8);

const MAX_CHARS: u8 = 20;

impl TryFrom<Range> for Chars {
    type Error = RangeToIntError<u64>;

    fn try_from(value: Range) -> Result<Self, Self::Error> {
        u64::try_from(value).map(Self::from_u64)
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
    pub(crate) fn try_new_from_chars(
        value: u64,
        chars: Chars,
    ) -> Result<Self, NotEnoughCharsError> {
        let needed = Chars::from_u64(value);
        if chars < needed {
            Err(NotEnoughCharsError { chars, value })
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
        i: MeasIndex,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<Self, (), IndexedRangeToAsciiError, AsciiRangeFromKeywordsError>
    {
        let rng_res = range
            .into_uint()
            .map_errors(RangeToAsciiError::from)
            .map_errors(|e| IndexedError::new(i, e))
            .map_errors(IndexedRangeToAsciiError)
            .nowarn_into_switchable(flag)
            .switchable_into_commutative()
            .map_errors(AsciiRangeFromKeywordsError::from)
            .into_semigroup();
        let chars_res = Chars::try_from(width)
            .map_err(|e| IndexedError::new(i, e))
            .map_err(IndexedWidthToCharsError)
            .map_err(AsciiRangeFromKeywordsError::from)
            .into_log();
        rng_res
            .zip_commutative(chars_res)
            .and_then_commutative(|(rng, chars)| {
                Self::try_new_from_chars(rng, chars)
                    .map_err(|e| IndexedError::new(i, e))
                    .map_err(IndexedNotEnoughCharsError)
                    .map_err(AsciiRangeFromKeywordsError::from)
                    .into_log()
            })
    }

    pub(crate) fn chars(&self) -> Chars {
        self.chars
    }

    #[must_use]
    pub fn value(&self) -> u64 {
        self.value
    }
}

impl Chars {
    /// Return number of chars needed to express the given u64.
    pub(crate) fn from_u64(x: u64) -> Self {
        // ASSUME the max possible value is 20 thus will always fit in u8
        Self(
            x.checked_ilog10()
                .map(|y| u8::try_from(y).unwrap())
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
    fn default() -> Self {
        Self(Chars(NonZeroU8::new(8).unwrap()))
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

/// Error when creating `AsciiRange` ($PnB and $PnR for one index)
#[derive(From, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum AsciiRangeFromKeywordsError {
    New(IndexedNotEnoughCharsError),
    Width(IndexedWidthToCharsError),
    Range(IndexedRangeToAsciiError),
}

/// Error when $PnB could not be converted to number of characters
#[derive(From, Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::InvalidKeywordValueError))]
pub struct IndexedWidthToCharsError(IndexedError<WidthToFixedError<CharsError>>);

impl fmt::Display for IndexedWidthToCharsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let k = Width::std(self.0.index);
        match &self.0.error {
            WidthToFixedError::Fixed(e) => {
                write!(f, "could not convert {k} to chars because {e}")
            }
            WidthToFixedError::Variable(_) => {
                write!(f, "{k} is variable ('*') when fixed is needed")
            }
        }
    }
}

/// Error when $PnR exceeds number of characters allowed by $PnB.
#[derive(Debug, Error)]
#[error(
    "{pnr} ({r}) is longer than {b} digits allowed by {pnb}",
    pnr = Range::std(_0.index),
    pnb = Width::std(_0.index),
    r = _0.error.value,
    b = _0.error.chars,
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct IndexedNotEnoughCharsError(IndexedError<NotEnoughCharsError>);

/// Error when creating `OtherWidth` for configuration struct
#[derive(Debug, Error)]
#[error("OTHER width should be integer b/t 1 and 20, got {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct OtherWidthError(u8);

/// Error when $PnR exceeds number of characters allowed by $PnB.
///
/// This is not meant for external use since it is more useful when index is
/// provided as context.
#[derive(Debug)]
pub(crate) struct NotEnoughCharsError {
    chars: Chars,
    value: u64,
}

/// Error when converting $PnB to number of characters.
///
/// This is a helper type meant to be used in making more specific errors.
#[derive(Debug, Display)]
#[display("bits must be <= 20 to be used as number of characters, got {_0}")]
pub(crate) struct CharsError(u8);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u8_to_chars() {
        assert!(Chars::try_from(1_u8).is_ok());
        assert!(Chars::try_from(0_u8).is_err());
        assert!(Chars::try_from(20_u8).is_ok());
        assert!(Chars::try_from(21_u8).is_err());
    }
}

#[cfg(feature = "python")]
mod python {
    use super::OtherWidth;
    use pyo3::prelude::*;

    impl<'py> FromPyObject<'py> for OtherWidth {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let x: u8 = ob.extract()?;
            let y = x.try_into()?;
            Ok(y)
        }
    }
}
