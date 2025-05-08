use polars::prelude::*;

/// A dataframe without NULL and only types that make sense for FCS files.
pub(crate) struct FCSDataFrame(DataFrame);

/// Any valid column from an FCS dataframe
pub enum AnyFCSColumn<'a> {
    U08(U08Column<'a>),
    U16(U16Column<'a>),
    U32(U32Column<'a>),
    U64(U64Column<'a>),
    F32(F32Column<'a>),
    F64(F64Column<'a>),
}

// TODO the data is behind an Arc right? so cloning these and taking ownership
// will be cheap?
pub struct U08Column<'a>(&'a Column);
pub struct U16Column<'a>(&'a Column);
pub struct U32Column<'a>(&'a Column);
pub struct U64Column<'a>(&'a Column);
pub struct F32Column<'a>(&'a Column);
pub struct F64Column<'a>(&'a Column);

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

macro_rules! newtype_from_column {
    ($($col:ident),*) => {
        $(
            impl<'a> From<$col<'a>> for &'a Column {
                fn from(value: $col<'a>) -> Self {
                    value.0
                }
            }

            impl AsRef<Column> for $col<'_> {
                fn as_ref(&self) ->  &Column {
                    self.0
                }
            }
        )*
    };
}

newtype_from_column!(U08Column, U16Column, U32Column, U64Column, F32Column, F64Column);

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
                DataType::UInt8 => AnyFCSColumn::U08(U08Column(c)),
                DataType::UInt16 => AnyFCSColumn::U16(U16Column(c)),
                DataType::UInt32 => AnyFCSColumn::U32(U32Column(c)),
                DataType::UInt64 => AnyFCSColumn::U64(U64Column(c)),
                DataType::Float32 => AnyFCSColumn::F32(F32Column(c)),
                DataType::Float64 => AnyFCSColumn::F64(F64Column(c)),
                _ => unreachable!(),
            })
            .collect()
    }

    /// Return number of bytes this will occupy if written as delimited ASCII
    pub(crate) fn ascii_nchars(&self) -> u32 {
        let n = self.0.size() as u32;
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
        ndigits + ndelim
    }
}

// trait NumColumn {
//     type T;

//     fn ascii_nbytes() -> u32;
// }
