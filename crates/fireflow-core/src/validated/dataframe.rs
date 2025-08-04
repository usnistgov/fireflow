use crate::macros::match_many_to_one;
use crate::text::index::BoundaryIndexError;
use crate::validated::ascii_range::Chars;

use derive_more::{Display, From};
use polars_arrow::array::{Array, PrimitiveArray};
use polars_arrow::buffer::Buffer;
use polars_arrow::datatypes::ArrowDataType;
use std::any::type_name;
use std::fmt;
use std::iter;
use std::slice::Iter;

/// A dataframe without NULL and only types that make sense for FCS files.
#[derive(Clone, Default, PartialEq)]
pub struct FCSDataFrame {
    columns: Vec<AnyFCSColumn>,
    nrows: usize,
}

/// Any valid column from an FCS dataframe
#[derive(Clone, From)]
pub enum AnyFCSColumn {
    U08(U08Column),
    U16(U16Column),
    U32(U32Column),
    U64(U64Column),
    F32(F32Column),
    F64(F64Column),
}

#[derive(Clone, PartialEq)]
pub struct FCSColumn<T>(pub Buffer<T>);

pub type U08Column = FCSColumn<u8>;
pub type U16Column = FCSColumn<u16>;
pub type U32Column = FCSColumn<u32>;
pub type U64Column = FCSColumn<u64>;
pub type F32Column = FCSColumn<f32>;
pub type F64Column = FCSColumn<f64>;

impl PartialEq for AnyFCSColumn {
    /// Test for numeric equality between two columns.
    ///
    /// This will attempt to convert b/t datatypes when testing equality; for
    /// example, a `1` / `1.0` will be equal regardless of datatype because
    /// it can be losslessly converted between all possible types for a column
    /// (u8-64 and f32/f64).
    fn eq(&self, other: &AnyFCSColumn) -> bool {
        fn go<From, To>(xs: &FCSColumn<From>, ys: &FCSColumn<To>) -> bool
        where
            To: NumCast<From> + FCSDataType + PartialEq,
            From: FCSDataType,
        {
            From::as_col_iter::<To>(xs)
                .zip(ys.0.iter())
                .all(|(x, y)| x.lossy.is_none() && &x.new == y)
        }

        match (self, other) {
            (Self::U08(xs), Self::U08(ys)) => xs == ys,
            (Self::U08(xs), Self::U16(ys)) => go(xs, ys),
            (Self::U08(xs), Self::U32(ys)) => go(xs, ys),
            (Self::U08(xs), Self::U64(ys)) => go(xs, ys),
            (Self::U08(xs), Self::F32(ys)) => go(xs, ys),
            (Self::U08(xs), Self::F64(ys)) => go(xs, ys),

            (Self::U16(xs), Self::U08(ys)) => go(xs, ys),
            (Self::U16(xs), Self::U16(ys)) => xs == ys,
            (Self::U16(xs), Self::U32(ys)) => go(xs, ys),
            (Self::U16(xs), Self::U64(ys)) => go(xs, ys),
            (Self::U16(xs), Self::F32(ys)) => go(xs, ys),
            (Self::U16(xs), Self::F64(ys)) => go(xs, ys),

            (Self::U32(xs), Self::U08(ys)) => go(xs, ys),
            (Self::U32(xs), Self::U16(ys)) => go(xs, ys),
            (Self::U32(xs), Self::U32(ys)) => xs == ys,
            (Self::U32(xs), Self::U64(ys)) => go(xs, ys),
            (Self::U32(xs), Self::F32(ys)) => go(xs, ys),
            (Self::U32(xs), Self::F64(ys)) => go(xs, ys),

            (Self::U64(xs), Self::U08(ys)) => go(xs, ys),
            (Self::U64(xs), Self::U16(ys)) => go(xs, ys),
            (Self::U64(xs), Self::U32(ys)) => go(xs, ys),
            (Self::U64(xs), Self::U64(ys)) => xs == ys,
            (Self::U64(xs), Self::F32(ys)) => go(xs, ys),
            (Self::U64(xs), Self::F64(ys)) => go(xs, ys),

            (Self::F32(xs), Self::U08(ys)) => go(xs, ys),
            (Self::F32(xs), Self::U16(ys)) => go(xs, ys),
            (Self::F32(xs), Self::U32(ys)) => go(xs, ys),
            (Self::F32(xs), Self::U64(ys)) => go(xs, ys),
            (Self::F32(xs), Self::F32(ys)) => xs == ys,
            (Self::F32(xs), Self::F64(ys)) => go(xs, ys),

            (Self::F64(xs), Self::U08(ys)) => go(xs, ys),
            (Self::F64(xs), Self::U16(ys)) => go(xs, ys),
            (Self::F64(xs), Self::U32(ys)) => go(xs, ys),
            (Self::F64(xs), Self::U64(ys)) => go(xs, ys),
            (Self::F64(xs), Self::F32(ys)) => go(xs, ys),
            (Self::F64(xs), Self::F64(ys)) => xs == ys,
        }
    }
}

impl<T> From<Vec<T>> for FCSColumn<T> {
    fn from(value: Vec<T>) -> Self {
        FCSColumn(value.into())
    }
}

impl AnyFCSColumn {
    pub fn len(&self) -> usize {
        match_many_to_one!(self, AnyFCSColumn, [U08, U16, U32, U64, F32, F64], x, {
            x.0.len()
        })
    }

    pub(crate) fn check_writer<E, F, ToType>(&self, f: F) -> Result<(), LossError<E>>
    where
        F: Fn(ToType) -> Option<E>,
        ToType: AllFCSCast,
    {
        match_many_to_one!(self, Self, [U08, U16, U32, U64, F32, F64], xs, {
            FCSDataType::check_writer(xs, f)
        })
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Convert number at index to string
    pub fn pos_to_string(&self, i: usize) -> String {
        match_many_to_one!(self, AnyFCSColumn, [U08, U16, U32, U64, F32, F64], x, {
            x.0[i].to_string()
        })
    }

    /// The number of bytes occupied by the column if written as ASCII
    pub fn ascii_nbytes(&self) -> u32 {
        match self {
            Self::U08(xs) => u8::as_col_iter::<u64>(xs).map(cast_nbytes).sum(),
            Self::U16(xs) => u16::as_col_iter::<u64>(xs).map(cast_nbytes).sum(),
            Self::U32(xs) => u32::as_col_iter::<u64>(xs).map(cast_nbytes).sum(),
            Self::U64(xs) => u64::as_col_iter::<u64>(xs).map(cast_nbytes).sum(),
            Self::F32(xs) => f32::as_col_iter::<u64>(xs).map(cast_nbytes).sum(),
            Self::F64(xs) => f64::as_col_iter::<u64>(xs).map(cast_nbytes).sum(),
        }
    }

    pub fn as_array(&self) -> Box<dyn Array> {
        match self.clone() {
            Self::U08(xs) => Box::new(PrimitiveArray::new(ArrowDataType::UInt8, xs.0, None)),
            Self::U16(xs) => Box::new(PrimitiveArray::new(ArrowDataType::UInt16, xs.0, None)),
            Self::U32(xs) => Box::new(PrimitiveArray::new(ArrowDataType::UInt32, xs.0, None)),
            Self::U64(xs) => Box::new(PrimitiveArray::new(ArrowDataType::UInt64, xs.0, None)),
            Self::F32(xs) => Box::new(PrimitiveArray::new(ArrowDataType::Float32, xs.0, None)),
            Self::F64(xs) => Box::new(PrimitiveArray::new(ArrowDataType::Float64, xs.0, None)),
        }
    }
}

#[derive(Debug)]
pub struct NewDataframeError;

impl fmt::Display for NewDataframeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "column lengths to not match")
    }
}

pub struct ColumnLengthError {
    df_len: usize,
    col_len: usize,
}

#[derive(From, Display)]
pub enum InsertColumnError {
    Index(BoundaryIndexError),
    Column(ColumnLengthError),
}

impl fmt::Display for ColumnLengthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "column length ({}) is different from number of rows in dataframe ({})",
            self.col_len, self.df_len
        )
    }
}

impl FCSDataFrame {
    pub fn try_new(columns: Vec<AnyFCSColumn>) -> Result<Self, NewDataframeError> {
        if let Some(nrows) = columns.first().map(|c| c.len()) {
            if columns.iter().all(|c| c.len() == nrows) {
                Ok(Self { columns, nrows })
            } else {
                Err(NewDataframeError)
            }
        } else {
            Ok(Self::default())
        }
    }

    pub fn new1(column: AnyFCSColumn) -> Self {
        Self {
            nrows: column.len(),
            columns: vec![column],
        }
    }

    pub fn clear(&mut self) {
        self.columns = Vec::default();
        self.nrows = 0;
    }

    pub fn iter_columns(&self) -> Iter<'_, AnyFCSColumn> {
        self.columns.iter()
    }

    pub fn nrows(&self) -> usize {
        if self.is_empty() {
            0
        } else {
            self.nrows
        }
    }

    pub fn ncols(&self) -> usize {
        self.columns.len()
    }

    pub fn size(&self) -> u64 {
        (self.ncols() * self.nrows()) as u64
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ncols() == 0
    }

    pub(crate) fn drop_in_place(&mut self, i: usize) -> Option<AnyFCSColumn> {
        if i > self.columns.len() {
            None
        } else {
            Some(self.columns.remove(i))
        }
    }

    pub(crate) fn push_column(&mut self, col: AnyFCSColumn) -> Result<(), ColumnLengthError> {
        if self.is_empty() {
            *self = Self::new1(col);
            return Ok(());
        }
        let df_len = self.nrows();
        let col_len = col.len();
        if col_len == df_len {
            self.columns.push(col);
            Ok(())
        } else {
            Err(ColumnLengthError { df_len, col_len })
        }
    }

    // will panic if index is out of bounds
    pub(crate) fn insert_column_nocheck(
        &mut self,
        i: usize,
        col: AnyFCSColumn,
    ) -> Result<(), ColumnLengthError> {
        // don't use Self::new1 here since we want to panic if i is out of
        // bounds
        if self.is_empty() {
            self.nrows = col.len();
            self.columns.insert(i, col);
            return Ok(());
        }
        let df_len = self.nrows();
        let col_len = col.len();
        if col_len == df_len {
            self.columns.insert(i, col);
            Ok(())
        } else {
            Err(ColumnLengthError { df_len, col_len })
        }
    }

    // pub(crate) fn insert_column(
    //     &mut self,
    //     i: usize,
    //     col: AnyFCSColumn,
    // ) -> Result<(), InsertColumnError> {
    //     let ncol = self.columns.len();
    //     let df_len = self.nrows();
    //     let col_len = col.len();
    //     if i > ncol {
    //         // TODO this error is more general than just named_vec
    //         Err(BoundaryIndexError {
    //             index: i.into(),
    //             len: ncol,
    //         }
    //         .into())
    //     } else if col_len != df_len {
    //         Err(ColumnLengthError { df_len, col_len }.into())
    //     } else {
    //         self.columns.insert(i, col);
    //         Ok(())
    //     }
    // }

    /// Return number of bytes this will occupy if written as delimited ASCII
    pub(crate) fn ascii_nbytes(&self) -> u64 {
        let n = self.size();
        if n == 0 {
            return 0;
        }
        let ndelim = n - 1;
        let ndigits: u32 = self.iter_columns().map(|c| c.ascii_nbytes()).sum();
        u64::from(ndigits) + ndelim
    }
}

pub(crate) type FCSColIter<'a, FromType, ToType> =
    iter::Map<iter::Copied<Iter<'a, FromType>>, fn(FromType) -> CastResult<ToType>>;

pub(crate) trait FCSDataType
where
    Self: Sized,
    Self: Copy,
    [Self]: ToOwned,
{
    /// Return iterator for column, converting to native type on the fly.
    fn as_col_iter<ToType>(c: &FCSColumn<Self>) -> FCSColIter<'_, Self, ToType>
    where
        ToType: NumCast<Self>,
    {
        Self::iter_native(c).map(ToType::from_truncated)
    }

    /// Try to convert column to native type, and return error on failure.
    ///
    /// This is separate from returning the iterator itself because if we can't
    /// tolerate any loss, the only way to find with only the iterator it is
    /// while we are using it to write a file, which opens the possibility of a
    /// partially-written file (not good). Therefore we need to check this
    /// before returning the iterator at all, which ironically can only be found
    /// by iterating the entire vector once.
    ///
    /// This only applies to the case where we want to crash if any loss will
    /// occur. If we only wish to warn the user and use lossy conversion
    /// anyways, this only requires one iteration since the iterator itself will
    /// return a ['CastResult'] which carries a flag if loss occurred.
    fn check_writer<E, F: Fn(ToType) -> Option<E>, ToType: NumCast<Self>>(
        c: &FCSColumn<Self>,
        f: F,
    ) -> Result<(), LossError<E>> {
        for x in Self::as_col_iter::<ToType>(c) {
            x.resolve()?;
            if let Some(err) = f(x.new) {
                return Err(LossError::Other(err));
            }
        }
        Ok(())
    }

    fn iter_native(c: &FCSColumn<Self>) -> iter::Copied<Iter<'_, Self>> {
        c.0.iter().copied()
    }
}

#[derive(From, Clone, Copy)]
pub enum LossError<E> {
    #[from]
    Cast(CastError),
    Other(E),
}

#[derive(Clone, Copy)]
pub struct CastError {
    from: &'static str,
    to: &'static str,
}

impl fmt::Display for CastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "data loss occurred when converting from {} to {}",
            self.from, self.to
        )
    }
}

impl<E> fmt::Display for LossError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Cast(e) => e.fmt(f),
            Self::Other(e) => e.fmt(f),
        }
    }
}

impl FCSDataType for u8 {}
impl FCSDataType for u16 {}
impl FCSDataType for u32 {}
impl FCSDataType for u64 {}
impl FCSDataType for f32 {}
impl FCSDataType for f64 {}

#[cfg_attr(test, derive(Debug, PartialEq))]
pub(crate) struct CastResult<T> {
    pub(crate) new: T,
    pub(crate) lossy: Option<&'static str>,
}

impl<T> CastResult<T> {
    fn new<FromT>(new: T, has_loss: bool) -> Self {
        let lossy = if has_loss {
            Some(type_name::<FromT>())
        } else {
            None
        };
        Self { new, lossy }
    }

    pub(crate) fn as_err(&self) -> Option<CastError> {
        self.lossy.map(|from| {
            let to = type_name::<T>();
            CastError { from, to }
        })
    }

    pub(crate) fn resolve(&self) -> Result<(), CastError> {
        self.as_err().map_or(Ok(()), Err)
    }
}

pub(crate) trait NumCast<T>: Sized {
    fn from_truncated(x: T) -> CastResult<Self>;
}

macro_rules! impl_cast_noloss {
    ($from:ident, $to:ident) => {
        impl NumCast<$from> for $to {
            fn from_truncated(x: $from) -> CastResult<Self> {
                CastResult {
                    new: x.into(),
                    lossy: None,
                }
            }
        }
    };
}

macro_rules! impl_cast_int_lossy {
    ($from:ident, $to:ident) => {
        impl NumCast<$from> for $to {
            fn from_truncated(x: $from) -> CastResult<Self> {
                let has_loss = $to::try_from(x).is_err();
                let new = if has_loss { $to::MAX } else { x as $to };
                CastResult::new::<$from>(new, has_loss)
            }
        }
    };
}

macro_rules! impl_cast_float_to_int_lossy {
    ($from:ident, $to:ident) => {
        impl NumCast<$from> for $to {
            fn from_truncated(x: $from) -> CastResult<Self> {
                let has_loss = x.is_nan()
                    || x.is_infinite()
                    || x.is_sign_negative()
                    || x.floor() != x
                    || x > $to::MAX as $from;
                CastResult::new::<$from>(x as $to, has_loss)
            }
        }
    };
}

macro_rules! impl_cast_int_to_float_lossy {
    ($from:ident, $to:ident) => {
        impl NumCast<$from> for $to {
            fn from_truncated(x: $from) -> CastResult<Self> {
                let new = x as $to;
                let old = new as $from;
                CastResult::new::<$from>(new, old != x)
            }
        }
    };
}

impl_cast_noloss!(u8, u8);
impl_cast_noloss!(u8, u16);
impl_cast_noloss!(u8, u32);
impl_cast_noloss!(u8, u64);
impl_cast_noloss!(u8, f32);
impl_cast_noloss!(u8, f64);

impl_cast_int_lossy!(u16, u8);
impl_cast_noloss!(u16, u16);
impl_cast_noloss!(u16, u32);
impl_cast_noloss!(u16, u64);
impl_cast_noloss!(u16, f32);
impl_cast_noloss!(u16, f64);

impl_cast_int_lossy!(u32, u8);
impl_cast_int_lossy!(u32, u16);
impl_cast_noloss!(u32, u32);
impl_cast_noloss!(u32, u64);
impl_cast_int_to_float_lossy!(u32, f32);
impl_cast_noloss!(u32, f64);

impl_cast_int_lossy!(u64, u8);
impl_cast_int_lossy!(u64, u16);
impl_cast_int_lossy!(u64, u32);
impl_cast_noloss!(u64, u64);
impl_cast_int_to_float_lossy!(u64, f32);
impl_cast_int_to_float_lossy!(u64, f64);

impl_cast_float_to_int_lossy!(f32, u8);
impl_cast_float_to_int_lossy!(f32, u16);
impl_cast_float_to_int_lossy!(f32, u32);
impl_cast_float_to_int_lossy!(f32, u64);
impl_cast_noloss!(f32, f32);
// this will always be lossless, see
// https://doc.rust-lang.org/reference/expressions/operator-expr.html#r-expr.as.numeric.float-widening
impl_cast_noloss!(f32, f64);

impl_cast_float_to_int_lossy!(f64, u8);
impl_cast_float_to_int_lossy!(f64, u16);
impl_cast_float_to_int_lossy!(f64, u32);
impl_cast_float_to_int_lossy!(f64, u64);

// TODO there are plenty of cases where this isn't lossy, but it's not clear
// where the line should be drawn
impl NumCast<f64> for f32 {
    fn from_truncated(x: f64) -> CastResult<Self> {
        let new = x as f32;
        let old = new as f64;
        CastResult::new::<f64>(new, old != x)
    }
}

impl_cast_noloss!(f64, f64);

pub(crate) fn cast_nbytes(x: CastResult<u64>) -> u32 {
    u8::from(Chars::from_u64(x.new)).into()
}

pub(crate) trait AllFCSCast:
    NumCast<u8> + NumCast<u16> + NumCast<u32> + NumCast<u64> + NumCast<f32> + NumCast<f64>
{
}

impl<T> AllFCSCast for T where
    T: NumCast<u8> + NumCast<u16> + NumCast<u32> + NumCast<u64> + NumCast<f32> + NumCast<f64>
{
}

// TODO this seems like a good place for property testing
// (https://github.com/proptest-rs/proptest)
#[cfg(test)]
mod tests {
    use core::f32;

    use super::*;

    // only test lossy cases, assume the others will simply noop

    #[test]
    fn test_u16_to_u8() {
        assert_eq!(u8::from_truncated(1_u16).lossy, false);
        assert_eq!(u8::from_truncated(256_u16), CastResult::new(255, true));
    }

    #[test]
    fn test_u32_to_u8() {
        assert_eq!(u8::from_truncated(1_u32).lossy, false);
        assert_eq!(u8::from_truncated(256_u32), CastResult::new(255, true));
    }

    #[test]
    fn test_u64_to_u8() {
        assert_eq!(u8::from_truncated(1_u64).lossy, false);
        assert_eq!(u8::from_truncated(256_u64), CastResult::new(255, true));
    }

    #[test]
    fn test_u32_to_u16() {
        assert_eq!(u16::from_truncated(1_u32).lossy, false);
        assert_eq!(u16::from_truncated(65536_u32), CastResult::new(65535, true));
    }

    #[test]
    fn test_u64_to_u16() {
        assert_eq!(u16::from_truncated(1_u64).lossy, false);
        assert_eq!(u16::from_truncated(65536_u64), CastResult::new(65535, true));
    }

    #[test]
    fn test_u64_to_u32() {
        assert_eq!(u32::from_truncated(1_u64).lossy, false);
        assert_eq!(
            u32::from_truncated(4294967296_u64),
            CastResult::new(4294967295, true)
        );
    }

    // uint should map exactly to f32 if less than 2^24, above this it will
    // start rounding to nearest even number (and beyond as we get higher)

    #[test]
    fn test_u32_to_f32() {
        assert_eq!(f32::from_truncated(1_u32), CastResult::new(1.0, false));
        assert_eq!(
            f32::from_truncated(16777216_u32),
            CastResult::new(16777216.0, false)
        );
        assert_eq!(
            f32::from_truncated(16777217_u32),
            CastResult::new(16777216.0, true)
        );
        assert_eq!(
            f32::from_truncated(16777218_u32),
            CastResult::new(16777218.0, false)
        );
    }

    #[test]
    fn test_u64_to_f32() {
        assert_eq!(f32::from_truncated(1_u64), CastResult::new(1.0, false));
        assert_eq!(
            f32::from_truncated(16777216_u64),
            CastResult::new(16777216.0, false)
        );
        assert_eq!(
            f32::from_truncated(16777217_u64),
            CastResult::new(16777216.0, true)
        );
        assert_eq!(
            f32::from_truncated(16777218_u64),
            CastResult::new(16777218.0, false)
        );
    }

    // uint should map exactly to f64 if less than 2^53, above this it will
    // start rounding to nearest even number (and beyond as we get higher)

    #[test]
    fn test_u64_to_f64() {
        assert_eq!(f64::from_truncated(1_u64), CastResult::new(1.0, false));
        assert_eq!(
            f64::from_truncated(9007199254740992_u64),
            CastResult::new(9007199254740992.0, false)
        );
        assert_eq!(
            f64::from_truncated(9007199254740993_u64),
            CastResult::new(9007199254740992.0, true)
        );
        assert_eq!(
            f64::from_truncated(9007199254740994_u64),
            CastResult::new(9007199254740994.0, false)
        );
    }

    macro_rules! test_float_to_int {
        ($float:ident, $int:ident) => {
            let zero: $float = 0.0;
            let nonzero: $float = 1.5;
            let neg: $float = -1.0;

            assert_eq!($int::from_truncated(zero), CastResult::new(0, false));
            assert_eq!(
                $int::from_truncated($int::MAX as $float),
                CastResult::new($int::MAX, false)
            );
            assert_eq!($int::from_truncated(nonzero), CastResult::new(1, true));
            assert_eq!($int::from_truncated(neg), CastResult::new(0, true));
            assert_eq!($int::from_truncated($float::NAN), CastResult::new(0, true));
            assert_eq!(
                $int::from_truncated($float::NEG_INFINITY),
                CastResult::new(0, true)
            );
            assert_eq!(
                $int::from_truncated($float::INFINITY),
                CastResult::new($int::MAX, true)
            );
        };
    }

    #[test]
    fn test_f32_to_u8() {
        test_float_to_int!(f32, u8);
    }

    #[test]
    fn test_f32_to_u16() {
        test_float_to_int!(f32, u16);
    }

    #[test]
    fn test_f32_to_u32() {
        test_float_to_int!(f32, u32);
    }

    #[test]
    fn test_f32_to_u64() {
        test_float_to_int!(f32, u64);
    }

    #[test]
    fn test_f64_to_u8() {
        test_float_to_int!(f64, u8);
    }

    #[test]
    fn test_f64_to_u16() {
        test_float_to_int!(f64, u16);
    }

    #[test]
    fn test_f64_to_u32() {
        test_float_to_int!(f64, u32);
    }

    #[test]
    fn test_f64_to_u64() {
        test_float_to_int!(f64, u64);
    }

    #[test]
    fn test_f64_to_f32() {
        // this should obviously pass
        assert_eq!(f32::from_truncated(0.0_f64), CastResult::new(0.0, false));
        // this is the upper limit of ints that an f32 can represent exactly,
        // going above this will start to induce rounding errors that don't
        // happen in f64
        assert_eq!(
            f32::from_truncated(16777216.0_f64),
            CastResult::new(16777216.0, false)
        );
        assert_eq!(
            f32::from_truncated(16777217.0_f64),
            CastResult::new(16777216.0, true)
        );
        // this is a decimal that can be represented perfectly in both
        assert_eq!(f32::from_truncated(0.5_f64), CastResult::new(0.5, false));
        // this is a repeating decimal which will have different representations
        // in f32 and f64, thus it will be lossy
        assert_eq!(f32::from_truncated(0.2_f64), CastResult::new(0.2, true));
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{AnyFCSColumn, FCSColumn, FCSDataFrame};
    use crate::python::macros::impl_value_err;

    use polars::prelude::*;
    use polars_arrow::array::PrimitiveArray;
    use pyo3::prelude::*;
    use pyo3_polars::{PyDataFrame, PySeries};
    use std::fmt;

    impl<'py> IntoPyObject<'py> for FCSDataFrame {
        type Target = PyAny;
        type Output = Bound<'py, PyAny>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            let columns = self
                .iter_columns()
                .enumerate()
                .map(|(i, c)| {
                    Series::from_arrow(PlSmallStr::from(format!("X{i}")), c.as_array())
                        .unwrap()
                        .into()
                })
                .collect();
            // ASSUME this will not fail because all columns should have unique
            // names and the same length
            PyDataFrame(DataFrame::new(columns).unwrap()).into_pyobject(py)
        }
    }

    impl<'py> FromPyObject<'py> for AnyFCSColumn {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let ser: PySeries = ob.extract()?;
            let ret = series_to_fcs(ser.0)?;
            Ok(ret)
        }
    }

    fn series_to_fcs(ser: Series) -> Result<AnyFCSColumn, SeriesToColumnError> {
        fn column_to_buf<T>(ser: Series) -> Result<AnyFCSColumn, SeriesToColumnError>
        where
            T: NumericNative,
            AnyFCSColumn: From<FCSColumn<T>>,
        {
            if ser.null_count() > 0 {
                Err(SeriesToColumnError::HasNull(ser.name().clone()))
            } else {
                let buf = ser.into_chunks()[0]
                    .as_any()
                    .downcast_ref::<PrimitiveArray<T>>()
                    .unwrap()
                    .values()
                    .clone();
                Ok(FCSColumn(buf).into())
            }
        }

        match ser.dtype() {
            DataType::UInt8 => column_to_buf::<u8>(ser),
            DataType::UInt16 => column_to_buf::<u16>(ser),
            DataType::UInt32 => column_to_buf::<u32>(ser),
            DataType::UInt64 => column_to_buf::<u64>(ser),
            DataType::Float32 => column_to_buf::<f32>(ser),
            DataType::Float64 => column_to_buf::<f64>(ser),
            t => Err(SeriesToColumnError::InvalidDatatype(
                ser.name().clone(),
                t.clone(),
            )),
        }
    }

    pub enum SeriesToColumnError {
        InvalidDatatype(PlSmallStr, DataType),
        HasNull(PlSmallStr),
    }

    impl fmt::Display for SeriesToColumnError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
            match self {
                Self::InvalidDatatype(n, t) => write!(
                    f,
                    "Datatype must be u8/16/32/64 or f32/64, got {t} for series '{n}'"
                ),
                Self::HasNull(n) => {
                    write!(f, "Series {n} contains null values which are not allowed")
                }
            }
        }
    }

    impl_value_err!(SeriesToColumnError);
}
