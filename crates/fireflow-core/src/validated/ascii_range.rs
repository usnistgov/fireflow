//! Types representing $PnR/$PnB keys for an Ascii column.

use crate::config::DisallowRangeTrunc;
use crate::data::{
    ColumnSchemaFromTextRange as _, ConvertedRange, IndexedError, IndexedRangeToAsciiError,
};
use crate::logging::{ResultExt as _, WarningsAndErrorsResult};
use crate::text::byteord::WidthToFixedError;
use crate::text::index::MeasIndex;
use crate::text::keywords::{RangeToIntError, TextRange, Width};
use crate::validated::keys::IndexedKey as _;

use derive_more::{Display, From, Into};
use derive_new::new;
use thiserror::Error;

use std::fmt;
use std::num::{NonZero, NonZeroU8, NonZeroUsize};

#[cfg(feature = "serde")]
use serde::Serialize;

use super::unaligned::DstIndex;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject},
    fireflow_types::python as py,
    pyo3::prelude::*,
};

/// The type of an ASCII column in all versions where width is fixed.
///
/// This represents the value of $PnB and $PnR for one measurement.
///
/// Fields are private to guarantee they are always in sync.
#[derive(PartialEq, Clone, Copy, Debug, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[new(visibility = "")]
pub struct FixedAsciiRange {
    /// The maximum value of the ASCII column
    value: AsciiRangeValue,

    /// Number of chars used to express this range.
    ///
    /// Must be able to hold `value` in ASCII digits but can be greater.
    chars: Chars,
}

/// Wrapper type for $PnR for delimited ASCII columns.
///
/// This is like [`FixedAsciiRange`] except it doesn't include width (ie $PnB).
#[derive(Clone, Copy, PartialEq, Into, From)]
#[into(u64, AsciiRangeValue)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject, IntoPyObject))]
pub struct DelimAsciiRange(pub AsciiRangeValue);

/// Integer value for [`TextRange`] for an ASCII measurement
#[derive(PartialEq, Clone, Copy, Debug, Display, Into)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject, IntoPyObject))]
pub struct AsciiRangeValue(pub u64);

/// Width to use when parsing OTHER segments.
///
/// Must be an integer between 8 and 20.
#[derive(Clone, Copy, Into, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
#[into(NonZeroU8, u8, NonZeroUsize)]
pub struct OtherWidth(NonZeroU8);

/// The number of chars for an ASCII measurement
///
/// Must be an integer between 1 and 20.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Hash, Display, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[into(NonZeroU8, u8)]
pub(crate) struct Chars(NonZeroU8);

pub(crate) const MAX_CHARS: NonZeroU8 = NonZeroU8::new(20).unwrap();

pub(crate) const MIN_OTHER_WIDTH: NonZeroU8 = NonZeroU8::new(8).unwrap();

impl TryFrom<TextRange> for Chars {
    type Error = RangeToIntError<u64>;

    fn try_from(value: TextRange) -> Result<Self, Self::Error> {
        u64::try_from(value).map(Self::from_u64)
    }
}

impl From<AsciiRangeValue> for FixedAsciiRange {
    fn from(value: AsciiRangeValue) -> Self {
        let chars = Chars::from_u64(value.0);
        Self::new(value, chars)
    }
}

impl From<DelimAsciiRange> for FixedAsciiRange {
    fn from(value: DelimAsciiRange) -> Self {
        value.0.into()
    }
}

impl From<FixedAsciiRange> for DelimAsciiRange {
    fn from(value: FixedAsciiRange) -> Self {
        value.value.into()
    }
}

impl From<FixedAsciiRange> for TextRange {
    fn from(value: FixedAsciiRange) -> Self {
        value.value.0.into()
    }
}

impl From<DelimAsciiRange> for TextRange {
    fn from(value: DelimAsciiRange) -> Self {
        value.0.0.into()
    }
}

impl From<&FixedAsciiRange> for TextRange {
    fn from(value: &FixedAsciiRange) -> Self {
        value.value.0.into()
    }
}

impl From<&DelimAsciiRange> for TextRange {
    fn from(value: &DelimAsciiRange) -> Self {
        value.0.0.into()
    }
}

impl FixedAsciiRange {
    #[must_use]
    pub fn value(&self) -> AsciiRangeValue {
        self.value
    }

    pub(crate) fn try_new_from_chars(
        value: AsciiRangeValue,
        chars: Chars,
    ) -> Result<Self, NotEnoughCharsError> {
        let needed = Chars::from_u64(value.0);
        if chars < needed {
            Err(NotEnoughCharsError { chars, value })
        } else {
            Ok(Self::new(value, chars))
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
        range: TextRange,
        i: MeasIndex,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<
        ConvertedRange<Self>,
        (),
        IndexedRangeToAsciiError,
        AsciiRangeFromKeywordsError,
    > {
        let rng_res = Self::from_range_indexed(range, i, flag);
        let chars_res = Chars::try_from(width)
            .map_err(|e| IndexedError::new(i, e))
            .map_err(IndexedWidthToCharsError)
            .map_err(AsciiRangeFromKeywordsError::from)
            .into_log();
        rng_res
            .zip_commutative(chars_res)
            .and_then_commutative(|(cr, chars)| {
                Self::try_new_from_chars(cr.native.value, chars)
                    .map_err(|e| IndexedError::new(i, e))
                    .map_err(IndexedNotEnoughCharsError)
                    .map_err(AsciiRangeFromKeywordsError::from)
                    .into_log()
                    .map_ok_value(|ar| ConvertedRange::new(ar, cr.non_truncated))
            })
    }

    pub(crate) fn from_range_indexed(
        range: TextRange,
        i: MeasIndex,
        flag: DisallowRangeTrunc,
    ) -> WarningsAndErrorsResult<
        ConvertedRange<Self>,
        ConvertedRange<Self>,
        IndexedRangeToAsciiError,
        AsciiRangeFromKeywordsError,
    > {
        Self::from_range(range, flag)
            .map_switchable_errors(|e| IndexedError::new(i, e))
            .map_switchable_errors(IndexedRangeToAsciiError)
            .switchable_into_commutative()
            .map_errors(AsciiRangeFromKeywordsError::from)
            .into_semigroup()
    }

    pub(crate) fn chars(&self) -> Chars {
        self.chars
    }

    pub(crate) fn as_slice_unchecked(&self, value: u64, dst: &mut [u8], dst_index: &DstIndex) {
        let i = dst_index.0;
        let width = usize::from(u8::from(self.chars()));
        let str_value = value.to_string();
        assert!(i + width <= dst.len(), "new value will overflow");
        assert!(str_value.len() <= width, "ASCII value will be truncated");
        let n_zero = width - str_value.len();
        for d in &mut dst[i..i + n_zero] {
            *d = b'0';
        }
        dst[i + n_zero..i + width].copy_from_slice(str_value.as_bytes());
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
        if value <= MAX_CHARS {
            Ok(Self(value))
        } else {
            Err(CharsError(u8::from(value)))
        }
    }
}

impl Default for OtherWidth {
    fn default() -> Self {
        const N: NonZeroU8 = NonZeroU8::new(8).unwrap();
        Self(N)
    }
}

impl TryFrom<u8> for OtherWidth {
    type Error = OtherWidthError;

    fn try_from(x: u8) -> Result<Self, Self::Error> {
        if let Some(n) = NonZeroU8::new(x)
            && (MIN_OTHER_WIDTH..=MAX_CHARS).contains(&n)
        {
            Ok(Self(n))
        } else {
            Err(OtherWidthError(x))
        }
    }
}

/// Error when creating [`FixedAsciiRange`] ($PnB and $PnR for one index)
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
#[cfg_attr(feature = "python", pyerr(py::InvalidKeywordValueError))]
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
    pnr = TextRange::std(_0.index),
    pnb = Width::std(_0.index),
    r = _0.error.value,
    b = _0.error.chars,
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct IndexedNotEnoughCharsError(IndexedError<NotEnoughCharsError>);

/// Error when creating [`OtherWidth`] for configuration struct
#[derive(Debug, Error)]
#[error("OTHER width should be integer b/t {MIN_OTHER_WIDTH} and {MAX_CHARS}, got {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct OtherWidthError(u8);

/// Error when $PnR exceeds number of characters allowed by $PnB.
///
/// This is not meant for external use since it is more useful when index is
/// provided as context.
#[derive(Debug)]
pub(crate) struct NotEnoughCharsError {
    chars: Chars,
    value: AsciiRangeValue,
}

/// Error when converting $PnB to number of characters.
///
/// This is a helper type meant to be used in making more specific errors.
#[derive(Debug, Display)]
#[display("bits must be <= {MAX_CHARS} to be used as number of characters, got {_0}")]
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
    use super::{AsciiRangeValue, FixedAsciiRange, OtherWidth};

    use pyo3::{prelude::*, types::PyInt};

    use std::convert::Infallible;

    impl<'py> FromPyObject<'_, 'py> for OtherWidth {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let x: u8 = obj.extract()?;
            let y = x.try_into()?;
            Ok(y)
        }
    }

    impl<'py> FromPyObject<'_, 'py> for FixedAsciiRange {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            Ok(obj.extract::<AsciiRangeValue>()?.into())
        }
    }

    impl<'py> IntoPyObject<'py> for FixedAsciiRange {
        type Target = PyInt;
        type Output = Bound<'py, PyInt>;
        type Error = Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.value().into_pyobject(py)
        }
    }
}
