use fireflow_core::data::{
    AnyNullBitmask, AnyOrderedLayout, DataLayout2_0, DataLayout3_0, DataLayout3_1, DataLayout3_2,
    EndianLayoutOps, FloatRange, LayoutOps, NonMixedEndianLayout, NullMixedType, OrderedLayoutOps,
};
use fireflow_core::text::byteord::{Endian, SizedByteOrd, VecToSizedError};
use fireflow_core::text::float_decimal::{FloatDecimal, HasFloatBounds};
use fireflow_core::text::keywords::AlphaNumType;
use fireflow_core::validated::ascii_range::{AsciiRange, Chars};
use fireflow_core::validated::bitmask::Bitmask;
use pyo3::conversion::FromPyObjectBound;
use pyo3::exceptions::PyValueError;

use crate::class::{PyAlphaNumType, PyreflowException};

use super::macros::py_wrap;

use bigdecimal::{BigDecimal, ParseBigDecimalError};
use derive_more::{Display, From, Into};
use nonempty::NonEmpty;
use pyo3::prelude::*;
use pyo3::types::PyType;

// All layouts
py_wrap!(pub(crate) PyDataLayout2_0, DataLayout2_0, "DataLayout2_0");
py_wrap!(pub(crate) PyDataLayout3_0, DataLayout3_0, "DataLayout3_0");
py_wrap!(pub(crate) PyDataLayout3_1, DataLayout3_1, "DataLayout3_1");
py_wrap!(pub(crate) PyDataLayout3_2, DataLayout3_2, "DataLayout3_2");

macro_rules! common_methods {
    ($t:ident) => {
        #[pymethods]
        impl $t {
            /// Return the widths of each column (ie the $PnB keyword).
            ///
            /// This will be a list of integers equal to the number of columns
            /// or an empty list if the layout is delimited Ascii (in which case
            /// it has no column widths).
            #[getter]
            fn widths(&self) -> Vec<u8> {
                self.0.widths().into_iter().map(u8::from).collect()
            }

            /// Return a list of ranges for each column.
            ///
            /// The elements of the list will be either a float or int and
            /// will depend on the underlying layout structure.
            #[getter]
            fn ranges(&self) -> Vec<BigDecimal> {
                self.0.ranges().into_iter().map(|r| r.0).collect()
            }
        }
    };
}

common_methods!(PyDataLayout2_0);
common_methods!(PyDataLayout3_0);
common_methods!(PyDataLayout3_1);
common_methods!(PyDataLayout3_2);

macro_rules! byte_order_methods {
    ($t:ident) => {
        #[pymethods]
        impl $t {
            #[getter]
            /// Return the byte order of the layout.
            ///
            /// If ASCII, return empty list. Otherwise return the byte layout
            /// as a zero-indexed list of integers.
            fn byte_order(&self) -> Vec<u8> {
                (self.0)
                    .0
                    .byte_order()
                    .map(|b| b.as_vec())
                    .unwrap_or_default()
            }
        }
    };
}

byte_order_methods!(PyDataLayout2_0);
byte_order_methods!(PyDataLayout3_0);

macro_rules! endianness_methods {
    ($t:ident) => {
        #[pymethods]
        impl $t {
            #[getter]
            /// Return the byte order of the layout.
            ///
            /// If layout is ASCII or has mixed byte order, return None.
            /// If big endian, return true, or false for little endian.
            fn endianness(&self) -> Option<bool> {
                (self.0).0.endianness().map(|e| (e == Endian::Big))
            }
        }
    };
}

endianness_methods!(PyDataLayout2_0);
endianness_methods!(PyDataLayout3_0);
endianness_methods!(PyDataLayout3_1);

macro_rules! datatype_getter {
    ($t:ident) => {
        #[pymethods]
        impl $t {
            #[getter]
            /// Return the datatype of the layout.
            fn datatype(&self) -> PyAlphaNumType {
                (self.0).0.datatype().into()
            }
        }
    };
}

datatype_getter!(PyDataLayout2_0);
datatype_getter!(PyDataLayout3_0);
datatype_getter!(PyDataLayout3_1);

// ascii layouts for all versions
macro_rules! new_ascii_methods {
    ($t:ident, $wrap:path, $subwrap:ident) => {
        #[pymethods]
        impl $t {
            #[classmethod]
            fn new_ascii_delim(_: &Bound<'_, PyType>, ranges: PyNonEmpty<u64>) -> Self {
                $wrap($subwrap::new_ascii_delim(ranges.0)).into()
            }

            #[classmethod]
            fn new_ascii_fixed_u64(_: &Bound<'_, PyType>, ranges: PyNonEmpty<u64>) -> Self {
                let rs = ranges.0.map(AsciiRange::from);
                $wrap($subwrap::new_ascii_fixed(rs)).into()
            }

            #[classmethod]
            fn new_ascii_fixed_pairs(
                _: &Bound<'_, PyType>,
                ranges: PyNonEmpty<(u64, u8)>,
            ) -> PyResult<Self> {
                // TODO clean these types up
                let ys = ranges
                    .0
                    .try_map(|(x, c)| Chars::try_from(c).map(|y| (x, y)))
                    .map_err(|e| PyreflowException::new_err(e.to_string()))?;
                let rs = ys
                    .try_map(|(x, c)| AsciiRange::try_new(x, c))
                    .map_err(|e| PyreflowException::new_err(e.to_string()))?;
                Ok($wrap($subwrap::new_ascii_fixed(rs)).into())
            }
        }
    };
}

new_ascii_methods!(PyDataLayout2_0, DataLayout2_0, AnyOrderedLayout);
new_ascii_methods!(PyDataLayout3_0, DataLayout3_0, AnyOrderedLayout);
new_ascii_methods!(PyDataLayout3_1, DataLayout3_1, NonMixedEndianLayout);
new_ascii_methods!(
    PyDataLayout3_2,
    DataLayout3_2::NonMixed,
    NonMixedEndianLayout
);

// integer layouts for 2.0/3.0
macro_rules! new_uint_2_0 {
    ($t:ident, $wrap:path, $fn_ordered:ident, $fn_endian:ident, $uint:ident, $size:expr) => {
        #[pymethods]
        impl $t {
            #[classmethod]
            /// Make a new layout for $size-byte Uints with a given byte order.
            fn $fn_ordered(
                _: &Bound<'_, PyType>,
                ranges: PyNonEmpty<PyBitmask<$uint, $size>>,
                byteord: PySizedByteOrd<$size>,
            ) -> Self {
                $wrap(AnyOrderedLayout::new_uint(ranges.0.map(|r| r.0), byteord.0)).into()
            }

            /// Make a new layout for $size-byte Uints with a given endian-ness.
            #[classmethod]
            fn $fn_endian(
                _: &Bound<'_, PyType>,
                ranges: PyNonEmpty<PyBitmask<$uint, $size>>,
                is_big: bool,
            ) -> Self {
                let b = SizedByteOrd::Endian(Endian::is_big(is_big));
                $wrap(AnyOrderedLayout::new_uint(ranges.0.map(|r| r.0), b)).into()
            }
        }
    };
}

macro_rules! new_ordered_uint_methods {
    ($t:ident, $wrap:path) => {
        new_uint_2_0!($t, $wrap, new_uint08_ordered, new_uint08_endian, u8, 1);
        new_uint_2_0!($t, $wrap, new_uint16_ordered, new_uint16_endian, u16, 2);
        new_uint_2_0!($t, $wrap, new_uint24_ordered, new_uint24_endian, u32, 3);
        new_uint_2_0!($t, $wrap, new_uint32_ordered, new_uint32_endian, u32, 4);
        new_uint_2_0!($t, $wrap, new_uint40_ordered, new_uint40_endian, u64, 5);
        new_uint_2_0!($t, $wrap, new_uint48_ordered, new_uint48_endian, u64, 6);
        new_uint_2_0!($t, $wrap, new_uint56_ordered, new_uint56_endian, u64, 7);
        new_uint_2_0!($t, $wrap, new_uint64_ordered, new_uint64_endian, u64, 8);
    };
}

new_ordered_uint_methods!(PyDataLayout2_0, DataLayout2_0);
new_ordered_uint_methods!(PyDataLayout3_0, DataLayout3_0);

// float layouts for 2.0/3.0
macro_rules! new_float_2_0 {
    ($t:ident, $wrap:path, $fn_ordered:ident, $fn_endian:ident, $num:ident, $size:expr, $subfn:ident) => {
        #[pymethods]
        impl $t {
            #[classmethod]
            /// Make a new $num layout with a given byte order.
            fn $fn_ordered(
                _: &Bound<'_, PyType>,
                ranges: PyNonEmpty<PyFloatRange<$num, $size>>,
                byteord: PySizedByteOrd<$size>,
            ) -> Self {
                $wrap(AnyOrderedLayout::$subfn(ranges.0.map(|r| r.0), byteord.0)).into()
            }

            #[classmethod]
            /// Make a new $num layout with a given endian-ness.
            fn $fn_endian(
                _: &Bound<'_, PyType>,
                ranges: PyNonEmpty<PyFloatRange<$num, $size>>,
                is_big: bool,
            ) -> Self {
                let e = SizedByteOrd::Endian(Endian::is_big(is_big));
                $wrap(AnyOrderedLayout::$subfn(ranges.0.map(|r| r.0), e)).into()
            }
        }
    };
}

new_float_2_0!(
    PyDataLayout2_0,
    DataLayout2_0,
    new_f32_ordered,
    new_f32_endian,
    f32,
    4,
    new_f32
);
new_float_2_0!(
    PyDataLayout2_0,
    DataLayout2_0,
    new_f64_ordered,
    new_f64_endian,
    f64,
    8,
    new_f64
);
new_float_2_0!(
    PyDataLayout3_0,
    DataLayout3_0,
    new_f32_ordered,
    new_f32_endian,
    f32,
    4,
    new_f32
);
new_float_2_0!(
    PyDataLayout3_0,
    DataLayout3_0,
    new_f64_ordered,
    new_f64_endian,
    f64,
    8,
    new_f64
);

// float layouts for 3.1/3.2
macro_rules! new_float_3_1 {
    ($t:ident, $wrap:path, $fn:ident, $num:ident, $size:expr) => {
        #[pymethods]
        impl $t {
            #[classmethod]
            /// Make a new $num layout with a given endian-ness.
            fn $fn(
                _: &Bound<'_, PyType>,
                ranges: PyNonEmpty<PyFloatRange<$num, $size>>,
                is_big: bool,
            ) -> PyResult<Self> {
                let e = Endian::is_big(is_big);
                Ok($wrap(NonMixedEndianLayout::$fn(ranges.0.map(|r| r.0), e)).into())
            }
        }
    };
}

new_float_3_1!(PyDataLayout3_1, DataLayout3_1, new_f32, f32, 4);
new_float_3_1!(PyDataLayout3_1, DataLayout3_1, new_f64, f64, 8);
new_float_3_1!(PyDataLayout3_2, DataLayout3_2::NonMixed, new_f32, f32, 4);
new_float_3_1!(PyDataLayout3_2, DataLayout3_2::NonMixed, new_f64, f64, 8);

// uint layout for 3.1/3.2
macro_rules! new_uint_3_1 {
    ($t:ident, $wrap:path) => {
        #[pymethods]
        impl $t {
            #[classmethod]
            /// Make a new Uint layout with a given endian-ness.
            ///
            /// Width of each column (in bytes) will depend in the input range.
            fn new_uint(_: &Bound<'_, PyType>, ranges: PyNonEmpty<u64>, is_big: bool) -> Self {
                let e = Endian::is_big(is_big);
                let rs = ranges.0.map(AnyNullBitmask::from_u64);
                $wrap(NonMixedEndianLayout::new_uint(rs, e)).into()
            }
        }
    };
}

new_uint_3_1!(PyDataLayout3_1, DataLayout3_1);
new_uint_3_1!(PyDataLayout3_2, DataLayout3_2::NonMixed);

#[pymethods]
impl PyDataLayout3_2 {
    #[classmethod]
    /// Make a new mixed layout with a given endian-ness.
    ///
    /// Columns must be specified as pairs like (flag, value) where 'flag'
    /// is one of "A", "I", "F", or "D" corresponding to Ascii, Integer, Float,
    /// or Double datatypes. The 'value' field should be an integer for "A" or
    /// "I" and a float for "F" or "D".
    fn new_mixed(_: &Bound<'_, PyType>, ranges: PyNonEmpty<PyMixedType>, is_big: bool) -> Self {
        let e = Endian::is_big(is_big);
        DataLayout3_2::new_mixed(ranges.0.map(|r| r.0), e).into()
    }

    #[getter]
    /// Return true if layout has more than one datatype.
    fn is_mixed(&self) -> bool {
        matches!(self.0, DataLayout3_2::Mixed(_))
    }

    #[getter]
    /// Return list of datatypes for each column.
    ///
    /// If not a mixed layout, datatypes will be identical.
    fn datatypes(&self) -> Vec<PyAlphaNumType> {
        Vec::from(self.0.datatypes())
            .into_iter()
            .map(|x| x.into())
            .collect()
    }

    // TODO not DRY, but didn't feel like making the above macro more flexible
    #[getter]
    /// Return the byte order of the layout.
    ///
    /// If layout is ASCII or has mixed byte order, return None.
    /// If big endian, return true, or false for little endian.
    fn endianness(&self) -> Option<bool> {
        (self.0).endianness().map(|e| (e == Endian::Big))
    }
}

#[derive(From, Into)]
struct PyBitmask<T, const LEN: usize>(Bitmask<T, LEN>);

impl<'py, T, const LEN: usize> FromPyObject<'py> for PyBitmask<T, LEN>
where
    for<'a> T: FromPyObjectBound<'a, 'py>,
    T: num_traits::PrimInt + std::fmt::Display,
{
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let x = ob.extract::<T>()?;
        let (b, trunc) = Bitmask::from_native(x);
        if trunc {
            let e = format!("could not make {LEN}-byte bitmask from {x}");
            Err(PyreflowException::new_err(e))
        } else {
            Ok(Self(b))
        }
    }
}

#[derive(From, Into)]
struct PyFloatRange<T, const LEN: usize>(FloatRange<T, LEN>);

impl<'py, T, const LEN: usize> FromPyObject<'py> for PyFloatRange<T, LEN>
where
    for<'a> T: FromPyObjectBound<'a, 'py>,
    T: HasFloatBounds,
    FloatDecimal<T>: TryFrom<T>,
    <FloatDecimal<T> as TryFrom<T>>::Error: std::fmt::Display,
{
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let x = ob.extract::<T>()?;
        FloatDecimal::try_from(x)
            .map(|r| Self(FloatRange::new(r)))
            .map_err(|e| PyreflowException::new_err(e.to_string()))
    }
}

struct PyMixedType(NullMixedType);

pub(crate) struct PyNonEmpty<T>(pub(crate) NonEmpty<T>);

impl<'py, T: FromPyObject<'py>> FromPyObject<'py> for PyNonEmpty<T> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let xs: Vec<_> = ob.extract()?;
        NonEmpty::from_vec(xs)
            .ok_or(PyValueError::new_err("list must not be empty"))
            .map(Self)
    }
}

impl<'py> FromPyObject<'py> for PyMixedType {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let (datatype, value): (PyAlphaNumType, Bound<'py, PyAny>) = ob.extract()?;
        match datatype.0 {
            AlphaNumType::Single => {
                let x = value.extract::<f32>()?;
                let y = FloatDecimal::try_from(x).map_err(PyParseBigDecimalError)?;
                Ok(Self(FloatRange::new(y).into()))
            }
            AlphaNumType::Double => {
                let x = value.extract::<f64>()?;
                let y = FloatDecimal::try_from(x).map_err(PyParseBigDecimalError)?;
                Ok(Self(FloatRange::new(y).into()))
            }
            AlphaNumType::Integer => {
                let x = value.extract()?;
                Ok(Self(AnyNullBitmask::from_u64(x).into()))
            }
            AlphaNumType::Ascii => {
                let x = value.extract::<u64>()?;
                Ok(Self(AsciiRange::from(x).into()))
            }
        }
    }
}

struct PySizedByteOrd<const LEN: usize>(SizedByteOrd<LEN>);

impl<'py, const LEN: usize> FromPyObject<'py> for PySizedByteOrd<LEN>
where
    SizedByteOrd<LEN>: TryFrom<Vec<u8>, Error = VecToSizedError>,
{
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let xs: Vec<u8> = ob.extract()?;
        let ret = SizedByteOrd::<LEN>::try_from(xs).map_err(PyVecToSizedError)?;
        Ok(Self(ret))
    }
}

#[derive(Display, From)]
struct PyParseBigDecimalError(ParseBigDecimalError);

impl From<PyParseBigDecimalError> for PyErr {
    fn from(value: PyParseBigDecimalError) -> Self {
        PyreflowException::new_err(value.to_string())
    }
}

#[derive(Display, From)]
struct PyVecToSizedError(VecToSizedError);

impl From<PyVecToSizedError> for PyErr {
    fn from(value: PyVecToSizedError) -> Self {
        PyreflowException::new_err(value.to_string())
    }
}
