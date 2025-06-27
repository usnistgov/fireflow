use crate::macros::{enum_from, enum_from_disp, match_many_to_one};
use crate::text::index::BoundaryIndexError;
use crate::validated::ascii_range::Chars;

use polars_arrow::array::{Array, PrimitiveArray};
use polars_arrow::buffer::Buffer;
use polars_arrow::datatypes::ArrowDataType;
use std::any::type_name;
use std::fmt;
use std::iter;
use std::slice::Iter;

/// A dataframe without NULL and only types that make sense for FCS files.
#[derive(Clone, Default)]
pub struct FCSDataFrame {
    columns: Vec<AnyFCSColumn>,
    nrows: usize,
}

enum_from!(
    /// Any valid column from an FCS dataframe
    #[derive(Clone)]
    pub AnyFCSColumn,
    [U08, U08Column],
    [U16, U16Column],
    [U32, U32Column],
    [U64, U64Column],
    [F32, F32Column],
    [F64, F64Column]
);

#[derive(Clone)]
pub struct FCSColumn<T>(pub Buffer<T>);

impl<T> From<Vec<T>> for FCSColumn<T> {
    fn from(value: Vec<T>) -> Self {
        FCSColumn(value.into())
    }
}

pub type U08Column = FCSColumn<u8>;
pub type U16Column = FCSColumn<u16>;
pub type U32Column = FCSColumn<u32>;
pub type U64Column = FCSColumn<u64>;
pub type F32Column = FCSColumn<f32>;
pub type F64Column = FCSColumn<f64>;

impl AnyFCSColumn {
    pub fn len(&self) -> usize {
        match_many_to_one!(self, AnyFCSColumn, [U08, U16, U32, U64, F32, F64], x, {
            x.0.len()
        })
    }

    pub fn check_writer<E, F, ToType>(&self, f: F) -> Result<(), LossError<E>>
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

enum_from_disp!(
    pub InsertColumnError,
    [Index, BoundaryIndexError],
    [Column, ColumnLengthError]
);

impl fmt::Display for ColumnLengthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "column length ({}) is different from number of rows in dataframe ({})",
            self.df_len, self.col_len
        )
    }
}

impl FCSDataFrame {
    pub(crate) fn try_new(columns: Vec<AnyFCSColumn>) -> Result<Self, NewDataframeError> {
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
        let df_len = self.nrows();
        let col_len = col.len();
        if col_len != df_len {
            Err(ColumnLengthError { df_len, col_len })
        } else {
            self.columns.insert(i, col);
            Ok(())
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
            if x.lossy {
                let d = CastError {
                    from: type_name::<Self>(),
                    to: type_name::<ToType>(),
                };
                return Err(LossError::Cast(d));
            }
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

pub enum LossError<E> {
    Cast(CastError),
    Other(E),
}

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

pub(crate) struct CastResult<T> {
    pub(crate) new: T,
    pub(crate) lossy: bool,
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
                    lossy: false,
                }
            }
        }
    };
}

macro_rules! impl_cast_int_lossy {
    ($from:ident, $to:ident) => {
        impl NumCast<$from> for $to {
            fn from_truncated(x: $from) -> CastResult<Self> {
                CastResult {
                    new: x as $to,
                    lossy: $to::try_from(x).is_err(),
                }
            }
        }
    };
}

macro_rules! impl_cast_float_to_int_lossy {
    ($from:ident, $to:ident) => {
        impl NumCast<$from> for $to {
            fn from_truncated(x: $from) -> CastResult<Self> {
                CastResult {
                    new: x as $to,
                    lossy: x.is_nan()
                        || x.is_infinite()
                        || x.is_sign_negative()
                        || x.floor() != x
                        || x > $to::MAX as $from,
                }
            }
        }
    };
}

macro_rules! impl_cast_int_to_float_lossy {
    ($from:ident, $to:ident, $bits:expr) => {
        impl NumCast<$from> for $to {
            fn from_truncated(x: $from) -> CastResult<Self> {
                CastResult {
                    new: x as $to,
                    lossy: x > 2 ^ $bits,
                }
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
impl_cast_int_to_float_lossy!(u32, f32, 24);
impl_cast_noloss!(u32, f64);

impl_cast_int_lossy!(u64, u8);
impl_cast_int_lossy!(u64, u16);
impl_cast_int_lossy!(u64, u32);
impl_cast_noloss!(u64, u64);
impl_cast_int_to_float_lossy!(u64, f32, 24);
impl_cast_int_to_float_lossy!(u64, f64, 53);

impl_cast_float_to_int_lossy!(f32, u8);
impl_cast_float_to_int_lossy!(f32, u16);
impl_cast_float_to_int_lossy!(f32, u32);
impl_cast_float_to_int_lossy!(f32, u64);
impl_cast_noloss!(f32, f32);
impl_cast_noloss!(f32, f64);

impl_cast_float_to_int_lossy!(f64, u8);
impl_cast_float_to_int_lossy!(f64, u16);
impl_cast_float_to_int_lossy!(f64, u32);
impl_cast_float_to_int_lossy!(f64, u64);

impl NumCast<f64> for f32 {
    fn from_truncated(x: f64) -> CastResult<Self> {
        CastResult {
            new: x as f32,
            lossy: true,
        }
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
