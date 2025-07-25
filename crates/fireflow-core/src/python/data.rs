use crate::data::{
    self, AnyAsciiLayout, AnyNullBitmask, AnyOrderedLayout, AnyOrderedUintLayout, DataLayout2_0,
    DataLayout3_0, DataLayout3_1, DataLayout3_2, EndianLayout, F32Range, F64Range, FixedLayout,
    FloatRange, KnownTot, LayoutOps, NoMeasDatatype, NonMixedEndianLayout, NullMixedType,
    OrderedLayout, OrderedLayoutOps,
};
use crate::text::byteord::{Endian, SizedByteOrd};
use crate::text::keywords::{AlphaNumType, Range};
use crate::validated::bitmask as bm;

use pyo3::exceptions::PyValueError;

use derive_more::{From, Into};
use nonempty::NonEmpty;
use pyo3::prelude::*;
use pyo3::types::PyType;
use std::num::NonZeroU8;

#[derive(FromPyObject, IntoPyObject)]
pub enum PyOrderedLayout {
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
pub enum PyNonMixedLayout {
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
pub enum PyLayout3_2 {
    NonMixed(PyNonMixedLayout),
    Mixed(PyMixedLayout),
}

macro_rules! py_wrap {
    ($pytype:ident, $rstype:path, $name:expr) => {
        #[derive(Clone, From, Into)]
        #[pyclass(name = $name)]
        pub struct $pytype($rstype);
    };
}

type AsciiDelim = data::FixedAsciiLayout<KnownTot, NoMeasDatatype, false>;
py_wrap!(PyAsciiFixedLayout, AsciiDelim, "AsciiFixedLayout");

type AsciiFixed = data::DelimAsciiLayout<KnownTot, NoMeasDatatype, false>;
py_wrap!(PyAsciiDelimLayout, AsciiFixed, "AsciiDelimLayout");

py_wrap!(PyOrderedUint08Layout, OrderedLayout<bm::Bitmask08, KnownTot>, "OrderedUint08Layout");
py_wrap!(PyOrderedUint16Layout, OrderedLayout<bm::Bitmask16, KnownTot>, "OrderedUint16Layout");
py_wrap!(PyOrderedUint24Layout, OrderedLayout<bm::Bitmask24, KnownTot>, "OrderedUint24Layout");
py_wrap!(PyOrderedUint32Layout, OrderedLayout<bm::Bitmask32, KnownTot>, "OrderedUint32Layout");
py_wrap!(PyOrderedUint40Layout, OrderedLayout<bm::Bitmask40, KnownTot>, "OrderedUint40Layout");
py_wrap!(PyOrderedUint48Layout, OrderedLayout<bm::Bitmask48, KnownTot>, "OrderedUint48Layout");
py_wrap!(PyOrderedUint56Layout, OrderedLayout<bm::Bitmask56, KnownTot>, "OrderedUint56Layout");
py_wrap!(PyOrderedUint64Layout, OrderedLayout<bm::Bitmask64, KnownTot>, "OrderedUint64Layout");

py_wrap!(PyOrderedF32Layout, OrderedLayout<F32Range, KnownTot>, "OrderedF32Layout");
py_wrap!(PyOrderedF64Layout, OrderedLayout<F64Range, KnownTot>, "OrderedF64Layout");

py_wrap!(PyEndianF32Layout, EndianLayout<F32Range, NoMeasDatatype>, "EndianF32Layout");
py_wrap!(PyEndianF64Layout, EndianLayout<F64Range, NoMeasDatatype>, "EndianF64Layout");

py_wrap!(PyEndianUintLayout, EndianLayout<AnyNullBitmask, NoMeasDatatype>, "EndianUintLayout");

py_wrap!(PyMixedLayout, data::MixedLayout, "MixedLayout");

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
            PyOrderedLayout::AsciiFixed(x) => AnyAsciiLayout::from(x.0).phantom_into().into(),
            PyOrderedLayout::AsciiDelim(x) => AnyAsciiLayout::from(x.0).phantom_into().into(),
            PyOrderedLayout::Uint08(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::Uint16(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::Uint24(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::Uint32(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::Uint40(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::Uint48(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::Uint56(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::Uint64(x) => AnyOrderedUintLayout::from(x.0).into(),
            PyOrderedLayout::F32(x) => x.0.into(),
            PyOrderedLayout::F64(x) => x.0.into(),
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
            fn ranges(&self) -> Vec<Range> {
                self.0.ranges().into()
            }

            /// Return the datatype.
            fn datatype(&self) -> AlphaNumType {
                self.0.datatype().into()
            }

            /// Return a list of datatypes corresponding to each column.
            fn datatypes(&self) -> Vec<AlphaNumType> {
                self.0.datatypes().map(|d| d.into()).into()
            }
        }
    };
}

common_methods!(PyAsciiFixedLayout);
common_methods!(PyAsciiDelimLayout);
common_methods!(PyOrderedUint08Layout);
common_methods!(PyOrderedUint16Layout);
common_methods!(PyOrderedUint24Layout);
common_methods!(PyOrderedUint32Layout);
common_methods!(PyOrderedUint40Layout);
common_methods!(PyOrderedUint48Layout);
common_methods!(PyOrderedUint56Layout);
common_methods!(PyOrderedUint64Layout);
common_methods!(PyOrderedF32Layout);
common_methods!(PyOrderedF64Layout);
common_methods!(PyEndianF32Layout);
common_methods!(PyEndianF64Layout);
common_methods!(PyEndianUintLayout);
common_methods!(PyMixedLayout);

macro_rules! byte_order_methods {
    ($t:ident) => {
        #[pymethods]
        impl $t {
            #[getter]
            /// Return the byte order of the layout.
            fn byte_order(&self) -> Vec<NonZeroU8> {
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
    };
}

byte_order_methods!(PyOrderedUint08Layout);
byte_order_methods!(PyOrderedUint16Layout);
byte_order_methods!(PyOrderedUint24Layout);
byte_order_methods!(PyOrderedUint32Layout);
byte_order_methods!(PyOrderedUint40Layout);
byte_order_methods!(PyOrderedUint48Layout);
byte_order_methods!(PyOrderedUint56Layout);
byte_order_methods!(PyOrderedUint64Layout);
byte_order_methods!(PyOrderedF32Layout);
byte_order_methods!(PyOrderedF64Layout);

macro_rules! endianness_methods {
    ($t:ident) => {
        #[pymethods]
        impl $t {
            #[getter]
            /// Return true if big endian, false otherwise.
            fn endianness(&self) -> bool {
                *self.0.as_ref() == Endian::Big
            }
        }
    };
}

endianness_methods!(PyEndianF32Layout);
endianness_methods!(PyEndianF64Layout);
endianness_methods!(PyEndianUintLayout);
endianness_methods!(PyMixedLayout);

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
            fn new(ranges: PyNonEmpty<bm::Bitmask<$uint, $size>>, is_big: bool) -> Self {
                FixedLayout::new_endian_uint(ranges.0, is_big.into()).into()
            }

            #[classmethod]
            /// Make a new layout for $size-byte Uints with a given byte order.
            fn new_ordered(
                _: &Bound<'_, PyType>,
                ranges: PyNonEmpty<bm::Bitmask<$uint, $size>>,
                byteord: SizedByteOrd<$size>,
            ) -> Self {
                FixedLayout::new(ranges.0, byteord).into()
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
            fn new(ranges: PyNonEmpty<FloatRange<$num, $size>>, is_big: bool) -> Self {
                FixedLayout::new_endian_float(ranges.0, is_big.into()).into()
            }

            #[classmethod]
            /// Make a new $num layout with a given byte order.
            fn new_ordered(
                _: &Bound<'_, PyType>,
                ranges: PyNonEmpty<FloatRange<$num, $size>>,
                byteord: SizedByteOrd<$size>,
            ) -> Self {
                FixedLayout::new(ranges.0, byteord).into()
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
            fn new(ranges: PyNonEmpty<FloatRange<$num, $size>>, is_big: bool) -> Self {
                FixedLayout::new(ranges.0, is_big.into()).into()
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
        let rs = ranges.0.map(AnyNullBitmask::from_u64);
        FixedLayout::new(rs, is_big.into()).into()
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
    fn new_mixed(ranges: PyNonEmpty<NullMixedType>, is_big: bool) -> Self {
        FixedLayout::new(ranges.0, is_big.into()).into()
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
