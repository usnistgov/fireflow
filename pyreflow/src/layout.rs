use fireflow_core::text::byteord::SizedByteOrd;
use fireflow_core::validated::ascii_range::{AsciiRange, Chars};
use fireflow_core::validated::bitmask::Bitmask;
use fireflow_core::{
    data::{AnyOrderedLayout, DataLayout2_0, DataLayout3_0, FloatRange},
    text::float_or_int::NonNanFloat,
};

use crate::class::PyreflowException;

use super::macros::{py_disp, py_enum, py_eq, py_ord, py_parse, py_wrap};

use nonempty::NonEmpty;
use pyo3::prelude::*;
use pyo3::types::PyType;

py_wrap!(PyDataLayout2_0, DataLayout2_0, "DataLayout2_0");
py_wrap!(PyDataLayout3_0, DataLayout3_0, "DataLayout3_0");

macro_rules! new_ascii_methods {
    ($t:ident, $wrap:path) => {
        #[pymethods]
        impl $t {
            #[classmethod]
            fn new_ascii_delim(_: &Bound<'_, PyType>, ranges: Vec<u64>) -> PyResult<Self> {
                let rs = vec_to_ne(ranges)?;
                Ok($wrap(AnyOrderedLayout::new_ascii_delim(rs)).into())
            }

            #[classmethod]
            fn new_ascii_fixed_u64(_: &Bound<'_, PyType>, ranges: Vec<u64>) -> PyResult<Self> {
                let xs = vec_to_ne(ranges)?;
                let rs = xs.map(AsciiRange::from);
                Ok($wrap(AnyOrderedLayout::new_ascii_fixed(rs)).into())
            }

            #[classmethod]
            fn new_ascii_fixed_pairs(
                _: &Bound<'_, PyType>,
                ranges: Vec<(u64, u8)>,
            ) -> PyResult<Self> {
                let xs = vec_to_ne(ranges)?;
                let ys = xs
                    .try_map(|(x, c)| Chars::try_from(c).map(|y| (x, y)))
                    .map_err(|e| PyreflowException::new_err(e.to_string()))?;
                let rs = ys
                    .try_map(|(x, c)| AsciiRange::try_new(x, c))
                    .map_err(|e| PyreflowException::new_err(e.to_string()))?;
                Ok($wrap(AnyOrderedLayout::new_ascii_fixed(rs)).into())
            }
        }
    };
}

new_ascii_methods!(PyDataLayout2_0, DataLayout2_0);
new_ascii_methods!(PyDataLayout3_0, DataLayout3_0);

macro_rules! new_ordered_uint {
    ($t:ident, $wrap:path, $fn_name:ident, $uint:ident, $size:expr) => {
        #[pymethods]
        impl $t {
            #[classmethod]
            fn $fn_name(
                _: &Bound<'_, PyType>,
                ranges: Vec<$uint>,
                byteord: Vec<u8>,
            ) -> PyResult<Self> {
                let rs = vec_to_bitmasks::<$uint, $size>(ranges)?;
                let b = vec_to_byteord(byteord)?;
                Ok($wrap(AnyOrderedLayout::new_uint(rs, b)).into())
            }
        }
    };
}

macro_rules! new_ordered_uint_methods {
    ($t:ident, $wrap:path) => {
        new_ordered_uint!($t, $wrap, new_uint08, u8, 1);
        new_ordered_uint!($t, $wrap, new_uint16, u16, 2);
        new_ordered_uint!($t, $wrap, new_uint24, u32, 3);
        new_ordered_uint!($t, $wrap, new_uint32, u32, 4);
        new_ordered_uint!($t, $wrap, new_uint40, u64, 5);
        new_ordered_uint!($t, $wrap, new_uint48, u64, 6);
        new_ordered_uint!($t, $wrap, new_uint56, u64, 7);
        new_ordered_uint!($t, $wrap, new_uint64, u64, 8);
    };
}

new_ordered_uint_methods!(PyDataLayout2_0, DataLayout2_0);
new_ordered_uint_methods!(PyDataLayout3_0, DataLayout3_0);

#[pymethods]
impl PyDataLayout2_0 {
    #[classmethod]
    fn new_f32(_: &Bound<'_, PyType>, ranges: Vec<f32>, byteord: Vec<u8>) -> PyResult<Self> {
        let rs = vec_to_float_ranges(ranges)?;
        let b = vec_to_byteord(byteord)?;
        Ok(DataLayout2_0(AnyOrderedLayout::new_f32(rs, b)).into())
    }

    #[classmethod]
    fn new_f64(_: &Bound<'_, PyType>, ranges: Vec<f64>, byteord: Vec<u8>) -> PyResult<Self> {
        let rs = vec_to_float_ranges(ranges)?;
        let b = vec_to_byteord(byteord)?;
        Ok(DataLayout2_0(AnyOrderedLayout::new_f64(rs, b)).into())
    }
}

fn vec_to_float_ranges<T, const LEN: usize>(xs: Vec<T>) -> PyResult<NonEmpty<FloatRange<T, LEN>>>
where
    NonNanFloat<T>: TryFrom<T>,
    <NonNanFloat<T> as TryFrom<T>>::Error: std::fmt::Display,
{
    let ns = vec_to_ne(xs)?;
    ns.try_map(|r| NonNanFloat::try_from(r).map(|range| FloatRange { range }))
        .map_err(|e| PyreflowException::new_err(e.to_string()))
}

fn vec_to_bitmasks<T, const LEN: usize>(xs: Vec<T>) -> PyResult<NonEmpty<Bitmask<T, LEN>>>
where
    T: Copy + std::fmt::Display + num_traits::PrimInt,
{
    let ns = vec_to_ne(xs)?;
    ns.try_map(|x| {
        let (y, trunc) = Bitmask::from_native(x);
        if trunc {
            let e = format!("could not make {LEN}-byte bitmask from {x}");
            Err(PyreflowException::new_err(e))
        } else {
            Ok(y)
        }
    })
}

fn vec_to_ne<X>(xs: Vec<X>) -> PyResult<NonEmpty<X>> {
    NonEmpty::from_vec(xs).ok_or(PyreflowException::new_err(
        "list must not be empty".to_string(),
    ))
}

fn vec_to_byteord<const LEN: usize>(xs: Vec<u8>) -> PyResult<SizedByteOrd<LEN>>
where
    SizedByteOrd<LEN>: TryFrom<Vec<u8>>,
    <SizedByteOrd<LEN> as TryFrom<Vec<u8>>>::Error: std::fmt::Display,
{
    SizedByteOrd::<LEN>::try_from(xs).map_err(|e| PyreflowException::new_err(e.to_string()))
}
