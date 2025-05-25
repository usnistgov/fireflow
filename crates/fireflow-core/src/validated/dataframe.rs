use crate::data::ColumnWriter;
use crate::macros::match_many_to_one;

use polars_arrow::array::{Array, PrimitiveArray};
use polars_arrow::buffer::Buffer;
use polars_arrow::datatypes::ArrowDataType;
use std::iter;
use std::slice::Iter;

/// A dataframe without NULL and only types that make sense for FCS files.
#[derive(Clone, Default)]
pub struct FCSDataFrame {
    columns: Vec<AnyFCSColumn>,
    nrows: usize,
}

/// Any valid column from an FCS dataframe
#[derive(Clone)]
pub enum AnyFCSColumn {
    U08(U08Column),
    U16(U16Column),
    U32(U32Column),
    U64(U64Column),
    F32(F32Column),
    F64(F64Column),
}

#[derive(Clone)]
pub struct FCSColumn<T>(pub Buffer<T>);

impl<T> From<Vec<T>> for FCSColumn<T>
// where
//     [T]: ToOwned,
//     T: Clone,
{
    fn from(value: Vec<T>) -> Self {
        FCSColumn(value.into())
    }
}

macro_rules! anycolumn_from {
    ($inner:ident, $var:ident) => {
        impl From<$inner> for AnyFCSColumn {
            fn from(value: $inner) -> Self {
                AnyFCSColumn::$var(value)
            }
        }
    };
}

anycolumn_from!(U08Column, U08);
anycolumn_from!(U16Column, U16);
anycolumn_from!(U32Column, U32);
anycolumn_from!(U64Column, U64);
anycolumn_from!(F32Column, F32);
anycolumn_from!(F64Column, F64);

// impl<'a> From<U08Column<'a>> for AnyFCSColumn<'a> {
//     fn from(value: U08Column<'a>) -> Self {
//         AnyFCSColumn::U08(value)
//     }
// }

// TODO the data is behind an Arc right? so cloning these and taking ownership
// will be cheap?
pub type U08Column = FCSColumn<u8>;
pub type U16Column = FCSColumn<u16>;
pub type U32Column = FCSColumn<u32>;
pub type U64Column = FCSColumn<u64>;
pub type F32Column = FCSColumn<f32>;
pub type F64Column = FCSColumn<f64>;

// newtype_from_outer!(FCSDataFrame, DataFrame);

// impl AsRef<DataFrame> for FCSDataFrame {
//     fn as_ref(&self) -> &DataFrame {
//         &self.0
//     }
// }

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

// impl TryFrom<DataFrame> for FCSDataFrame {
//     type Error = ();
//     fn try_from(value: DataFrame) -> Result<Self, Self::Error> {
//         if value.get_columns().iter().all(|c| {
//             matches!(
//                 c.dtype(),
//                 DataType::UInt8
//                     | DataType::UInt16
//                     | DataType::UInt32
//                     | DataType::UInt64
//                     | DataType::Float32
//                     | DataType::Float64
//             ) && !c.has_nulls()
//         }) {
//             Ok(FCSDataFrame(value))
//         } else {
//             Err(())
//         }
//     }
// }

impl AnyFCSColumn {
    pub fn len(&self) -> usize {
        match_many_to_one!(self, AnyFCSColumn, [U08, U16, U32, U64, F32, F64], x, {
            x.0.len()
        })
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn pos_to_string(&self, i: usize) -> String {
        match_many_to_one!(self, AnyFCSColumn, [U08, U16, U32, U64, F32, F64], x, {
            x.0[i].to_string()
        })
    }

    pub fn as_array(&self) -> Box<dyn Array> {
        match self.clone() {
            AnyFCSColumn::U08(xs) => {
                Box::new(PrimitiveArray::new(ArrowDataType::UInt8, xs.0, None))
            }
            AnyFCSColumn::U16(xs) => {
                Box::new(PrimitiveArray::new(ArrowDataType::UInt16, xs.0, None))
            }
            AnyFCSColumn::U32(xs) => {
                Box::new(PrimitiveArray::new(ArrowDataType::UInt32, xs.0, None))
            }
            AnyFCSColumn::U64(xs) => {
                Box::new(PrimitiveArray::new(ArrowDataType::UInt64, xs.0, None))
            }
            AnyFCSColumn::F32(xs) => {
                Box::new(PrimitiveArray::new(ArrowDataType::Float32, xs.0, None))
            }
            AnyFCSColumn::F64(xs) => {
                Box::new(PrimitiveArray::new(ArrowDataType::Float64, xs.0, None))
            }
        }
    }
}

impl FCSDataFrame {
    pub(crate) fn try_new(columns: Vec<AnyFCSColumn>) -> Option<Self> {
        if let Some(nrows) = columns.first().map(|c| c.len()) {
            if columns.iter().all(|c| c.len() == nrows) {
                Some(Self { columns, nrows })
            } else {
                None
            }
        } else {
            Some(Self::default())
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
        self.nrows
    }

    pub fn ncols(&self) -> usize {
        self.columns.len()
    }

    pub fn size(&self) -> usize {
        self.ncols() * self.nrows()
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

    // TODO return real error message from here
    pub(crate) fn push_column(&mut self, col: AnyFCSColumn) -> Option<()> {
        if col.len() == self.nrows() || self.is_empty() {
            self.columns.push(col);
            Some(())
        } else {
            None
        }
    }

    // TODO return real error message from here
    pub(crate) fn insert_column(&mut self, i: usize, col: AnyFCSColumn) -> Option<()> {
        if (col.len() == self.nrows() || self.is_empty()) && i > self.columns.len() {
            self.columns.insert(i, col);
            Some(())
        } else {
            None
        }
    }

    // /// Return number of bytes this will occupy if written as delimited ASCII
    pub(crate) fn ascii_nchars(&self) -> usize {
        let n = self.size();
        if n == 0 {
            return 0;
        }
        let ndelim = n - 1;
        let ndigits: u32 = self
            .iter_columns()
            .map(|c| match c {
                AnyFCSColumn::U08(xs) => {
                    xs.0.iter()
                        .map(|x| x.checked_ilog10().unwrap_or(0) + 1)
                        .sum::<u32>()
                }
                AnyFCSColumn::U16(xs) => {
                    xs.0.iter()
                        .map(|x| x.checked_ilog10().unwrap_or(0) + 1)
                        .sum()
                }
                AnyFCSColumn::U32(xs) => {
                    xs.0.iter()
                        .map(|x| x.checked_ilog10().unwrap_or(0) + 1)
                        .sum()
                }
                AnyFCSColumn::U64(xs) => {
                    xs.0.iter()
                        .map(|x| x.checked_ilog10().unwrap_or(0) + 1)
                        .sum()
                }
                AnyFCSColumn::F32(xs) => {
                    xs.0.iter()
                        .map(|x| (*x as u64).checked_ilog10().unwrap_or(0) + 1)
                        .sum()
                }
                AnyFCSColumn::F64(xs) => {
                    xs.0.iter()
                        .map(|x| (*x as u64).checked_ilog10().unwrap_or(0) + 1)
                        .sum()
                }
            })
            .sum();
        (ndigits as usize) + ndelim
    }
}

// type FCSColIterInner<'a, T> = iter::Flatten<Box<dyn PolarsIterator<Item = Option<T>> + 'a>>;

pub(crate) type FCSColIter<'a, FromType, ToType> =
    iter::Map<iter::Copied<Iter<'a, FromType>>, fn(FromType) -> CastResult<ToType>>;

pub(crate) trait FCSDataType
where
    Self: Sized,
    Self: Copy,
    [Self]: ToOwned,
{
    fn iter_native(c: &FCSColumn<Self>) -> iter::Copied<Iter<'_, Self>> {
        c.0.iter().copied()
    }

    fn iter_converted<ToType>(c: &FCSColumn<Self>) -> FCSColIter<'_, Self, ToType>
    where
        ToType: NumCast<Self>,
    {
        Self::iter_native(c).map(ToType::from_truncated)
    }

    /// Convert column to an iterator with possibly lossy conversion
    ///
    /// Iterate through the column and check if loss will occur, if so return
    /// err. On success, return an iterator which will yield a converted value
    /// with a flag indicating if loss occurred when converting. This way we
    /// can also warn user if loss occurred.
    ///
    /// The error/warning split is such because if we can't tolerate any loss,
    /// the only way to find it is while we are using the iterator to write a
    /// file, which opens the possibility of a partially-written file (not
    /// good). Therefore we need to check this before making the iterator at
    /// all, which ironically can only be found by iterating the entire vector
    /// once. However, if we only want to warn the user, we don't need this
    /// extra scan step and can simply log lossy values when writing.
    fn into_writer<E, F, S, T>(
        c: &FCSColumn<Self>,
        s: S,
        check: bool,
        f: F,
    ) -> Result<ColumnWriter<'_, Self, T, S>, E>
    where
        E: Default,
        F: Fn(T) -> Option<E>,
        T: NumCast<Self>,
    {
        if check {
            for x in Self::iter_converted::<T>(c) {
                if x.lossy {
                    return Err(E::default());
                }
                if let Some(err) = f(x.new) {
                    return Err(err);
                }
            }
        }
        Ok(ColumnWriter {
            data: Self::iter_converted(c),
            size: s,
        })
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
