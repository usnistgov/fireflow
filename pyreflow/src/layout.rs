use fireflow_core::data::{
    self, AnyAsciiLayout, AnyNullBitmask, AnyOrderedLayout, AnyOrderedUintLayout, DataLayout2_0,
    DataLayout3_0, DataLayout3_1, DataLayout3_2, EndianLayout, F32Range, F64Range, FixedLayout,
    FloatRange, KnownTot, LayoutOps, NoMeasDatatype, NonMixedEndianLayout, NullMixedType,
    OrderedLayout, OrderedLayoutOps,
};
use fireflow_core::text::byteord::{Endian, SizedByteOrd, VecToSizedError};
use fireflow_core::text::float_decimal::{FloatDecimal, HasFloatBounds};
use fireflow_core::text::keywords::AlphaNumType;
use fireflow_core::validated::ascii_range::AsciiRange;
use fireflow_core::validated::bitmask::{self, Bitmask};
use pyo3::conversion::FromPyObjectBound;
use pyo3::exceptions::PyValueError;

use crate::class::{PyAlphaNumType, PyreflowException};

use bigdecimal::{BigDecimal, ParseBigDecimalError};
use derive_more::{Display, From, Into};
use nonempty::NonEmpty;
use pyo3::prelude::*;
use pyo3::types::PyType;

#[derive(FromPyObject, IntoPyObject)]
pub(crate) enum PyOrderedLayout {
    AsciiFixed(PyAsciiFixedLayout),
    AsciiDelim(PyAsciiDelimLayout),
    Uint08(PyOrderedUint08Layout),
    Uint16(PyOrderedUint16Layout),
    Uint24(PyOrderedUint24Layout),
    Uint32(PyOrderedUint32Layout),
    Uint40(PyOrderedUint40Layout),
    Uint48(PyOrderedUint48Layout),
    Uint56(PyOrderedUint56Layout),
    Uint64(PyOrderedUint64Layout),
    F32(PyOrderedF32Layout),
    F64(PyOrderedF64Layout),
}

#[derive(FromPyObject, IntoPyObject, From)]
pub(crate) enum PyNonMixedLayout {
    #[from(
        PyAsciiFixedLayout,
        data::FixedAsciiLayout<KnownTot, NoMeasDatatype, false>
    )]
    AsciiFixed(PyAsciiFixedLayout),

    #[from(
        PyAsciiDelimLayout,
        data::DelimAsciiLayout<KnownTot, NoMeasDatatype, false>
    )]
    AsciiDelim(PyAsciiDelimLayout),

    #[from(PyEndianUintLayout, EndianLayout<AnyNullBitmask, NoMeasDatatype>)]
    Uint(PyEndianUintLayout),

    #[from(PyEndianF32Layout, EndianLayout<F32Range, NoMeasDatatype>)]
    F32(PyEndianF32Layout),

    #[from(PyEndianF64Layout, EndianLayout<F64Range, NoMeasDatatype>)]
    F64(PyEndianF64Layout),
}

#[derive(FromPyObject, IntoPyObject, From)]
pub(crate) enum PyLayout3_2 {
    NonMixed(PyNonMixedLayout),
    Mixed(PyMixedLayout),
}

#[derive(Clone, From, Into)]
#[pyclass(name = "AsciiFixedLayout")]
#[into(AnyAsciiLayout<KnownTot, NoMeasDatatype, false>)]
pub(crate) struct PyAsciiFixedLayout(data::FixedAsciiLayout<KnownTot, NoMeasDatatype, false>);

#[derive(Clone, From, Into)]
#[pyclass(name = "AsciiDelimLayout")]
#[into(AnyAsciiLayout<KnownTot, NoMeasDatatype, false>)]
pub(crate) struct PyAsciiDelimLayout(data::DelimAsciiLayout<KnownTot, NoMeasDatatype, false>);

#[derive(Clone, From, Into)]
#[pyclass(name = "OrderedUint08Layout")]
#[into(AnyOrderedUintLayout<KnownTot>)]
pub(crate) struct PyOrderedUint08Layout(OrderedLayout<bitmask::Bitmask08, KnownTot>);

#[derive(Clone, From, Into)]
#[pyclass(name = "OrderedUint16Layout")]
#[into(AnyOrderedUintLayout<KnownTot>)]
pub(crate) struct PyOrderedUint16Layout(OrderedLayout<bitmask::Bitmask16, KnownTot>);

#[derive(Clone, From, Into)]
#[pyclass(name = "OrderedUint24Layout")]
#[into(AnyOrderedUintLayout<KnownTot>)]
pub(crate) struct PyOrderedUint24Layout(OrderedLayout<bitmask::Bitmask24, KnownTot>);

#[derive(Clone, From, Into)]
#[pyclass(name = "OrderedUint32Layout")]
#[into(AnyOrderedUintLayout<KnownTot>)]
pub(crate) struct PyOrderedUint32Layout(OrderedLayout<bitmask::Bitmask32, KnownTot>);

#[derive(Clone, From, Into)]
#[pyclass(name = "OrderedUint40Layout")]
#[into(AnyOrderedUintLayout<KnownTot>)]
pub(crate) struct PyOrderedUint40Layout(OrderedLayout<bitmask::Bitmask40, KnownTot>);

#[derive(Clone, From, Into)]
#[pyclass(name = "OrderedUint48Layout")]
#[into(AnyOrderedUintLayout<KnownTot>)]
pub(crate) struct PyOrderedUint48Layout(OrderedLayout<bitmask::Bitmask48, KnownTot>);

#[derive(Clone, From, Into)]
#[pyclass(name = "OrderedUint56Layout")]
#[into(AnyOrderedUintLayout<KnownTot>)]
pub(crate) struct PyOrderedUint56Layout(OrderedLayout<bitmask::Bitmask56, KnownTot>);

#[derive(Clone, From, Into)]
#[pyclass(name = "OrderedUint64Layout")]
#[into(AnyOrderedUintLayout<KnownTot>)]
pub(crate) struct PyOrderedUint64Layout(OrderedLayout<bitmask::Bitmask64, KnownTot>);

#[derive(Clone, From, Into)]
#[pyclass(name = "OrderedF32Layout")]
#[into(AnyOrderedLayout<KnownTot>)]
pub(crate) struct PyOrderedF32Layout(OrderedLayout<F32Range, KnownTot>);

#[derive(Clone, From, Into)]
#[pyclass(name = "OrderedF64Layout")]
#[into(AnyOrderedLayout<KnownTot>)]
pub(crate) struct PyOrderedF64Layout(OrderedLayout<F64Range, KnownTot>);

#[derive(Clone, From, Into)]
#[pyclass(name = "EndianF32Layout")]
pub(crate) struct PyEndianF32Layout(EndianLayout<F32Range, NoMeasDatatype>);

#[derive(Clone, From, Into)]
#[pyclass(name = "EndianF64Layout")]
pub(crate) struct PyEndianF64Layout(EndianLayout<F64Range, NoMeasDatatype>);

#[derive(Clone, From, Into)]
#[pyclass(name = "EndianUintLayout")]
pub(crate) struct PyEndianUintLayout(EndianLayout<AnyNullBitmask, NoMeasDatatype>);

#[derive(Clone, From, Into)]
#[pyclass(name = "MixedLayout")]
pub(crate) struct PyMixedLayout(data::MixedLayout);

impl From<PyOrderedLayout> for DataLayout2_0 {
    fn from(value: PyOrderedLayout) -> Self {
        Self(AnyOrderedLayout::from(value).phantom_into())
    }
}

impl From<PyOrderedLayout> for DataLayout3_0 {
    fn from(value: PyOrderedLayout) -> Self {
        Self(AnyOrderedLayout::from(value))
    }
}

impl From<PyNonMixedLayout> for DataLayout3_1 {
    fn from(value: PyNonMixedLayout) -> Self {
        Self(NonMixedEndianLayout::from(value))
    }
}

impl From<PyLayout3_2> for DataLayout3_2 {
    fn from(value: PyLayout3_2) -> Self {
        match value {
            PyLayout3_2::Mixed(x) => Self::Mixed(x.into()),
            PyLayout3_2::NonMixed(x) => {
                Self::NonMixed(NonMixedEndianLayout::from(x).phantom_into())
            }
        }
    }
}

impl From<DataLayout2_0> for PyOrderedLayout {
    fn from(value: DataLayout2_0) -> Self {
        value.0.phantom_into().into()
    }
}

impl From<DataLayout3_0> for PyOrderedLayout {
    fn from(value: DataLayout3_0) -> Self {
        value.0.into()
    }
}

impl From<DataLayout3_1> for PyNonMixedLayout {
    fn from(value: DataLayout3_1) -> Self {
        value.0.into()
    }
}

impl From<DataLayout3_2> for PyLayout3_2 {
    fn from(value: DataLayout3_2) -> Self {
        match value {
            DataLayout3_2::Mixed(x) => Self::Mixed(x.into()),
            DataLayout3_2::NonMixed(x) => Self::NonMixed(x.phantom_into().into()),
        }
    }
}

impl From<PyOrderedLayout> for AnyOrderedLayout<KnownTot> {
    fn from(value: PyOrderedLayout) -> Self {
        match value {
            PyOrderedLayout::AsciiFixed(x) => AnyAsciiLayout::from(x).phantom_into().into(),
            PyOrderedLayout::AsciiDelim(x) => AnyAsciiLayout::from(x).phantom_into().into(),
            PyOrderedLayout::Uint08(x) => AnyOrderedUintLayout::from(x).into(),
            PyOrderedLayout::Uint16(x) => AnyOrderedUintLayout::from(x).into(),
            PyOrderedLayout::Uint24(x) => AnyOrderedUintLayout::from(x).into(),
            PyOrderedLayout::Uint32(x) => AnyOrderedUintLayout::from(x).into(),
            PyOrderedLayout::Uint40(x) => AnyOrderedUintLayout::from(x).into(),
            PyOrderedLayout::Uint48(x) => AnyOrderedUintLayout::from(x).into(),
            PyOrderedLayout::Uint56(x) => AnyOrderedUintLayout::from(x).into(),
            PyOrderedLayout::Uint64(x) => AnyOrderedUintLayout::from(x).into(),
            PyOrderedLayout::F32(x) => x.into(),
            PyOrderedLayout::F64(x) => x.into(),
        }
    }
}

impl From<AnyOrderedLayout<KnownTot>> for PyOrderedLayout {
    fn from(value: AnyOrderedLayout<KnownTot>) -> Self {
        match value {
            AnyOrderedLayout::Ascii(x) => match x.phantom_into() {
                AnyAsciiLayout::Delimited(y) => Self::AsciiDelim(y.into()),
                AnyAsciiLayout::Fixed(y) => Self::AsciiFixed(y.into()),
            },
            AnyOrderedLayout::Integer(x) => match x {
                AnyOrderedUintLayout::Uint08(y) => Self::Uint08(y.into()),
                AnyOrderedUintLayout::Uint16(y) => Self::Uint16(y.into()),
                AnyOrderedUintLayout::Uint24(y) => Self::Uint24(y.into()),
                AnyOrderedUintLayout::Uint32(y) => Self::Uint32(y.into()),
                AnyOrderedUintLayout::Uint40(y) => Self::Uint40(y.into()),
                AnyOrderedUintLayout::Uint48(y) => Self::Uint48(y.into()),
                AnyOrderedUintLayout::Uint56(y) => Self::Uint56(y.into()),
                AnyOrderedUintLayout::Uint64(y) => Self::Uint64(y.into()),
            },
            AnyOrderedLayout::F32(x) => Self::F32(x.into()),
            AnyOrderedLayout::F64(x) => Self::F64(x.into()),
        }
    }
}

impl From<NonMixedEndianLayout<NoMeasDatatype>> for PyNonMixedLayout {
    fn from(value: NonMixedEndianLayout<NoMeasDatatype>) -> Self {
        match value {
            NonMixedEndianLayout::Ascii(x) => match x {
                data::AnyAsciiLayout::Fixed(y) => y.into(),
                data::AnyAsciiLayout::Delimited(y) => y.into(),
            },
            NonMixedEndianLayout::Integer(x) => x.into(),
            NonMixedEndianLayout::F32(x) => x.into(),
            NonMixedEndianLayout::F64(x) => x.into(),
        }
    }
}

impl From<PyNonMixedLayout> for NonMixedEndianLayout<NoMeasDatatype> {
    fn from(value: PyNonMixedLayout) -> Self {
        match value {
            PyNonMixedLayout::AsciiFixed(x) => Self::Ascii(x.0.into()),
            PyNonMixedLayout::AsciiDelim(x) => Self::Ascii(x.0.into()),
            PyNonMixedLayout::Uint(x) => Self::Integer(x.into()),
            PyNonMixedLayout::F32(x) => Self::F32(x.into()),
            PyNonMixedLayout::F64(x) => Self::F64(x.into()),
        }
    }
}

macro_rules! common_methods {
    ($($t:ident),*) => {
        $(
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

                /// Return the datatype.
                fn datatype(&self) -> PyAlphaNumType {
                    self.0.datatype().into()
                }

                /// Return a list of datatypes corresponding to each column.
                fn datatypes(&self) -> Vec<PyAlphaNumType> {
                    self.0.datatypes().map(|d| d.into()).into()
                }
            }
        )*
    };
}

common_methods!(
    PyAsciiFixedLayout,
    PyAsciiDelimLayout,
    PyOrderedUint08Layout,
    PyOrderedUint16Layout,
    PyOrderedUint24Layout,
    PyOrderedUint32Layout,
    PyOrderedUint40Layout,
    PyOrderedUint48Layout,
    PyOrderedUint56Layout,
    PyOrderedUint64Layout,
    PyOrderedF32Layout,
    PyOrderedF64Layout,
    PyEndianF32Layout,
    PyEndianF64Layout,
    PyEndianUintLayout,
    PyMixedLayout
);

macro_rules! byte_order_methods {
    ($($t:ident),*) => {
        $(
            #[pymethods]
            impl $t {
                #[getter]
                /// Return the byte order of the layout.
                fn byte_order(&self) -> Vec<u8> {
                    self.0.byte_order().as_vec()
                }

                #[getter]
                /// Return the endianness if applicable.
                ///
                /// Return true for big endian, false for little endian, and
                /// None if byte order is mixed.
                fn endianness(&self) -> Option<bool> {
                    self.0.endianness().map(|x| x == Endian::Big)
                }

            }
        )*
    };
}

byte_order_methods!(
    PyOrderedUint08Layout,
    PyOrderedUint16Layout,
    PyOrderedUint24Layout,
    PyOrderedUint32Layout,
    PyOrderedUint40Layout,
    PyOrderedUint48Layout,
    PyOrderedUint56Layout,
    PyOrderedUint64Layout,
    PyOrderedF32Layout,
    PyOrderedF64Layout
);

macro_rules! endianness_methods {
    ($($t:ident),*) => {
        $(
            #[pymethods]
            impl $t {
                #[getter]
                /// Return true if big endian, false otherwise.
                fn endianness(&self) -> bool {
                    *self.0.as_ref() == Endian::Big
                }
            }
        )*
    };
}

endianness_methods!(
    PyEndianF32Layout,
    PyEndianF64Layout,
    PyEndianUintLayout,
    PyMixedLayout
);

#[pymethods]
impl PyAsciiDelimLayout {
    #[new]
    fn new(ranges: PyNonEmpty<u64>) -> Self {
        data::DelimAsciiLayout::new(ranges.0).into()
    }
}

#[pymethods]
impl PyAsciiFixedLayout {
    #[new]
    fn new(ranges: PyNonEmpty<u64>) -> Self {
        FixedLayout::new_ascii_u64(ranges.0).into()
    }

    // TODO make a constructor that takes char/range pairs
    // #[classmethod]
    // fn from_pairs(ranges: PyNonEmpty<u64>) -> Self {
    //     FixedLayout::new(columns, NoByteOrd)
    // }

    //             #[classmethod]
    //             fn new_ascii_fixed_pairs(
    //                 _: &Bound<'_, PyType>,
    //                 ranges: PyNonEmpty<(u64, u8)>,
    //             ) -> PyResult<Self> {
    //                 // TODO clean these types up
    //                 let ys = ranges
    //                     .0
    //                     .try_map(|(x, c)| Chars::try_from(c).map(|y| (x, y)))
    //                     .map_err(|e| PyreflowException::new_err(e.to_string()))?;
    //                 let rs = ys
    //                     .try_map(|(x, c)| AsciiRange::try_new(x, c))
    //                     .map_err(|e| PyreflowException::new_err(e.to_string()))?;
    //                 Ok($wrap($subwrap::new_ascii_fixed(rs)).into())
    //             }
}

macro_rules! new_ordered_uint {
    ($t:ident, $uint:ident, $size:expr) => {
        #[pymethods]
        impl $t {
            /// Make a new layout for $size-byte Uints with a given endian-ness.
            #[new]
            fn new(ranges: PyNonEmpty<PyBitmask<$uint, $size>>, is_big: bool) -> Self {
                let rs = ranges.0.map(|r| r.0);
                FixedLayout::new_endian_uint(rs, Endian::is_big(is_big)).into()
            }

            #[classmethod]
            /// Make a new layout for $size-byte Uints with a given byte order.
            fn new_ordered(
                _: &Bound<'_, PyType>,
                ranges: PyNonEmpty<PyBitmask<$uint, $size>>,
                byteord: PySizedByteOrd<$size>,
            ) -> Self {
                FixedLayout::new(ranges.0.map(|r| r.0), byteord.0).into()
            }
        }
    };
}

new_ordered_uint!(PyOrderedUint08Layout, u8, 1);
new_ordered_uint!(PyOrderedUint16Layout, u16, 2);
new_ordered_uint!(PyOrderedUint24Layout, u32, 3);
new_ordered_uint!(PyOrderedUint32Layout, u32, 4);
new_ordered_uint!(PyOrderedUint40Layout, u64, 5);
new_ordered_uint!(PyOrderedUint48Layout, u64, 6);
new_ordered_uint!(PyOrderedUint56Layout, u64, 7);
new_ordered_uint!(PyOrderedUint64Layout, u64, 8);

macro_rules! new_ordered_float {
    ($t:ident, $num:ident, $size:expr) => {
        #[pymethods]
        impl $t {
            /// Make a new $num layout with a given endian-ness.
            #[new]
            fn new(ranges: PyNonEmpty<PyFloatRange<$num, $size>>, is_big: bool) -> Self {
                let rs = ranges.0.map(|r| r.0);
                FixedLayout::new_endian_float(rs, Endian::is_big(is_big)).into()
            }

            #[classmethod]
            /// Make a new $num layout with a given byte order.
            fn new_ordered(
                _: &Bound<'_, PyType>,
                ranges: PyNonEmpty<PyFloatRange<$num, $size>>,
                byteord: PySizedByteOrd<$size>,
            ) -> Self {
                FixedLayout::new(ranges.0.map(|r| r.0), byteord.0).into()
            }
        }
    };
}

new_ordered_float!(PyOrderedF32Layout, f32, 4);
new_ordered_float!(PyOrderedF64Layout, f64, 8);

// float layouts for 3.1/3.2
macro_rules! new_endian_float {
    ($t:ident, $num:ident, $size:expr) => {
        #[pymethods]
        impl $t {
            #[new]
            /// Make a new $num layout with a given endian-ness.
            fn new(ranges: PyNonEmpty<PyFloatRange<$num, $size>>, is_big: bool) -> Self {
                let e = Endian::is_big(is_big);
                FixedLayout::new(ranges.0.map(|r| r.0), e).into()
            }
        }
    };
}

new_endian_float!(PyEndianF32Layout, f32, 4);
new_endian_float!(PyEndianF64Layout, f64, 8);

#[pymethods]
impl PyEndianUintLayout {
    /// Make a new Uint layout with a given endian-ness.
    ///
    /// Width of each column (in bytes) will depend in the input range.
    #[new]
    fn new(ranges: PyNonEmpty<u64>, is_big: bool) -> Self {
        let e = Endian::is_big(is_big);
        let rs = ranges.0.map(AnyNullBitmask::from_u64);
        FixedLayout::new(rs, e).into()
    }
}

#[pymethods]
impl PyMixedLayout {
    #[new]
    /// Make a new mixed layout with a given endian-ness.
    ///
    /// Columns must be specified as pairs like (flag, value) where 'flag'
    /// is one of "A", "I", "F", or "D" corresponding to Ascii, Integer, Float,
    /// or Double datatypes. The 'value' field should be an integer for "A" or
    /// "I" and a float for "F" or "D".
    fn new_mixed(ranges: PyNonEmpty<PyMixedType>, is_big: bool) -> Self {
        let e = Endian::is_big(is_big);
        FixedLayout::new(ranges.0.map(|r| r.0), e).into()
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

pub(crate) struct PyNonEmpty<T>(pub(crate) NonEmpty<T>);

impl<'py, T: FromPyObject<'py>> FromPyObject<'py> for PyNonEmpty<T> {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let xs: Vec<_> = ob.extract()?;
        NonEmpty::from_vec(xs)
            .ok_or(PyValueError::new_err("list must not be empty"))
            .map(Self)
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
