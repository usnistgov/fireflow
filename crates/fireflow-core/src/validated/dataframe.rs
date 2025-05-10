use crate::data::ColumnWriter;

use polars::prelude::*;
use std::iter;

/// A dataframe without NULL and only types that make sense for FCS files.
pub struct FCSDataFrame(DataFrame);

/// Any valid column from an FCS dataframe
pub enum AnyFCSColumn<'a> {
    U08(U08Column<'a>),
    U16(U16Column<'a>),
    U32(U32Column<'a>),
    U64(U64Column<'a>),
    F32(F32Column<'a>),
    F64(F64Column<'a>),
}

pub struct FCSColumn<'a, T: PolarsNumericType>(&'a ChunkedArray<T>);

// TODO the data is behind an Arc right? so cloning these and taking ownership
// will be cheap?
pub type U08Column<'a> = FCSColumn<'a, UInt8Type>;
pub type U16Column<'a> = FCSColumn<'a, UInt16Type>;
pub type U32Column<'a> = FCSColumn<'a, UInt32Type>;
pub type U64Column<'a> = FCSColumn<'a, UInt64Type>;
pub type F32Column<'a> = FCSColumn<'a, Float32Type>;
pub type F64Column<'a> = FCSColumn<'a, Float64Type>;

// newtype_from_outer!(FCSDataFrame, DataFrame);

impl AsRef<DataFrame> for FCSDataFrame {
    fn as_ref(&self) -> &DataFrame {
        &self.0
    }
}

// impl Deref<FCSDataFrame> for DataFrame {
//     fn deref(&self) -> &FCSDataFrame {
//         &FCSDataFrame(&self)
//     }
// }

// macro_rules! newtype_from_column {
//     ($($col:ident),*) => {
//         $(
//             impl<'a> From<$col<'a>> for &'a Column {
//                 fn from(value: $col<'a>) -> Self {
//                     value.0
//                 }
//             }

//             impl AsRef<Column> for $col<'_> {
//                 fn as_ref(&self) ->  &Column {
//                     self.0
//                 }
//             }
//         )*
//     };
// }

// newtype_from_column!(U08Column, U16Column, U32Column, U64Column, F32Column, F64Column);

impl TryFrom<DataFrame> for FCSDataFrame {
    type Error = ();
    fn try_from(value: DataFrame) -> Result<Self, Self::Error> {
        if value.get_columns().iter().all(|c| {
            matches!(
                c.dtype(),
                DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float32
                    | DataType::Float64
            ) && !c.has_nulls()
        }) {
            Ok(FCSDataFrame(value))
        } else {
            Err(())
        }
    }
}

impl FCSDataFrame {
    pub(crate) fn columns(&self) -> Vec<AnyFCSColumn> {
        self.0
            .get_columns()
            .iter()
            .map(|c| match c.dtype() {
                DataType::UInt8 => AnyFCSColumn::U08(FCSColumn(c.u8().unwrap())),
                DataType::UInt16 => AnyFCSColumn::U16(FCSColumn(c.u16().unwrap())),
                DataType::UInt32 => AnyFCSColumn::U32(FCSColumn(c.u32().unwrap())),
                DataType::UInt64 => AnyFCSColumn::U64(FCSColumn(c.u64().unwrap())),
                DataType::Float32 => AnyFCSColumn::F32(FCSColumn(c.f32().unwrap())),
                DataType::Float64 => AnyFCSColumn::F64(FCSColumn(c.f64().unwrap())),
                _ => unreachable!(),
            })
            .collect()
    }

    /// Return number of bytes this will occupy if written as delimited ASCII
    pub(crate) fn ascii_nchars(&self) -> usize {
        let n = self.0.size();
        if n == 0 {
            return 0;
        }
        let ndelim = n - 1;
        let ndigits: u32 = self
            .0
            .get_columns()
            .iter()
            .map(|c| match c.dtype() {
                DataType::UInt8 => c
                    .u8()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|x| x.checked_ilog10().unwrap_or(0) + 1)
                    .sum(),
                DataType::UInt16 => c
                    .u16()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|x| x.checked_ilog10().unwrap_or(0) + 1)
                    .sum(),
                DataType::UInt32 => c
                    .u32()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|x| x.checked_ilog10().unwrap_or(0) + 1)
                    .sum(),
                DataType::UInt64 => c
                    .u64()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|x| x.checked_ilog10().unwrap_or(0) + 1)
                    .sum(),
                DataType::Float32 => c
                    .f32()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|x| (x as u64).checked_ilog10().unwrap_or(0) + 1)
                    .sum(),
                DataType::Float64 => c
                    .f64()
                    .unwrap()
                    .into_no_null_iter()
                    .map(|x| (x as u64).checked_ilog10().unwrap_or(0) + 1)
                    .sum(),
                // this should never happen
                _ => 0,
            })
            .sum();
        (ndigits as usize) + ndelim
    }
}

type FCSColIterInner<'a, T> = iter::Flatten<Box<dyn PolarsIterator<Item = Option<T>> + 'a>>;

pub(crate) type FCSColIter<'a, FromType, ToType> =
    iter::Map<FCSColIterInner<'a, FromType>, fn(FromType) -> CastResult<ToType>>;

pub(crate) trait PolarsFCSType
where
    Self: PolarsNumericType,
{
    fn iter_native(c: FCSColumn<'_, Self>) -> FCSColIterInner<'_, Self::Native>;

    fn into_writer<ToType, S>(
        c: FCSColumn<'_, Self>,
        s: S,
    ) -> ColumnWriter<'_, Self::Native, ToType, S>
    where
        ToType: NumCast<Self::Native>,
    {
        ColumnWriter {
            data: Self::iter_native(c).map(ToType::from_truncated),
            size: s,
        }
    }
}

macro_rules! impl_col_iter {
    ($pltype:ident) => {
        impl PolarsFCSType for $pltype {
            fn iter_native(c: FCSColumn<'_, Self>) -> FCSColIterInner<'_, Self::Native> {
                c.0.into_iter().flatten()
            }
        }
    };
}

impl_col_iter!(UInt8Type);
impl_col_iter!(UInt16Type);
impl_col_iter!(UInt32Type);
impl_col_iter!(UInt64Type);
impl_col_iter!(Float32Type);
impl_col_iter!(Float64Type);

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

// macro_rules! impl_numcast {
//     ($($numtype:ty),*) => {
//         impl_numcast!($($numtype,)* @inner $($numtype),*);
//     };

//     ($first:ty, $($rest:ty),+, @inner $($to:ty),*) => {
//         impl_numcast!($first, @inner $($to),+);
//         impl_numcast!($($rest,)+ @inner $($to),+);
//     };

//     ($from:ty, @inner $($to:ty),*) => {
//         $(
//             impl NumCast<$from> for $to {
//                 fn from_truncated(x: $from) -> CastResult<Self> {
//                     CastResult {
//                         lossy: false,
//                         new: x as $to,
//                     }
//                 }
//             }
//         )*
//     };
// }

// impl_numcast!(u8, u16, u32, u64, f32, f64);
