use crate::match_many_to_one;
use crate::text::keywords::{ByteOrd2_0, ByteOrd3_1, Width};
use crate::text::lookup::ReqMetarootKey;
use crate::validated::ascii_range::{Chars, CharsError};

use fireflow_types::ne_str;
use fireflow_types::nonempty_string::{NEDelim, NEStr, ToDisplayNE};

use derive_more::{AsRef, Display, From, Into};
use derive_new::new;
use nonempty_collections::{
    FromNonEmptyIterator, IntoNonEmptyIterator, NEVec, NonEmptyIterator as _,
};
use num_enum::{IntoPrimitive, TryFromPrimitive, TryFromPrimitiveError};
use thiserror::Error;

use std::num::{NonZeroU8, ParseIntError};
use std::str::FromStr;
use std::{array, fmt};

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
    pyo3::prelude::*,
};

/// Byte order with known size in bytes.
///
/// This is only meant to store arrays of a given length. Arrays are guaranteed
/// to include all digits 0-LEN in any order.
///
/// This needs to be fully generic to get around limitations with const generics
/// and trait bounds (ie the num_traits crate specifies an array rather than a
/// length to be used in an array)
#[derive(PartialEq, Eq, Hash, Copy, Clone, Debug, AsRef)]
pub struct ArrayByteOrd_<A>(A);

pub type ArrayByteOrd<const LEN: usize> = ArrayByteOrd_<[u8; LEN]>;

/// Endianness (big or little)
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Endian {
    Big,
    #[default]
    Little,
}

impl ToDisplayNE<'_> for Endian {
    type NE = &'static NEStr;
    fn to_ne(&self) -> Self::NE {
        match self {
            Self::Big => ne_str!("4,3,2,1"),
            Self::Little => ne_str!("1,2,3,4"),
        }
    }
}

/// Marker type representing lack of byte order.
///
/// This is used in ASCII layouts, for which $BYTEORD is meaningless.
#[derive(Clone, Copy, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct NoByteOrd<const ORD: bool>;

pub type NoByteOrd2_0 = NoByteOrd<true>;

pub type NoByteOrd3_1 = NoByteOrd<false>;

impl<const ORD: bool> From<NoByteOrd<ORD>> for Endian {
    fn from(_: NoByteOrd<ORD>) -> Self {
        Self::default()
    }
}

impl From<NoByteOrd2_0> for NoByteOrd3_1 {
    fn from(_: NoByteOrd2_0) -> Self {
        Self
    }
}

impl From<NoByteOrd3_1> for NoByteOrd2_0 {
    fn from(_: NoByteOrd3_1) -> Self {
        Self
    }
}

/// Any byte order that can be used in a 2.0/3.0 layout.
///
/// Meant for arguments to functions. Must be validated after consumed.
#[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
pub enum AnyByteOrder {
    Endian(Endian),
    Ordered(Vec<NonZeroU8>),
}

impl Default for AnyByteOrder {
    fn default() -> Self {
        Self::Endian(Endian::default())
    }
}

impl<const LEN: usize> From<ArrayByteOrd<LEN>> for AnyByteOrder
where
    ArrayByteOrd<LEN>: Into<[NonZeroU8; LEN]>,
{
    fn from(value: ArrayByteOrd<LEN>) -> Self {
        if let Some(e) = value.as_endian() {
            Self::Endian(e)
        } else {
            let arr: [NonZeroU8; LEN] = value.into();
            Self::Ordered(arr.to_vec())
        }
    }
}

impl<const LEN: usize> TryFrom<AnyByteOrder> for ArrayByteOrd<LEN>
where
    Endian: Into<Self>,
    Vec<NonZeroU8>: TryInto<Self, Error = VecToSizedError>,
{
    type Error = VecToSizedError;
    fn try_from(value: AnyByteOrder) -> Result<Self, Self::Error> {
        match value {
            AnyByteOrder::Endian(e) => Ok(e.into()),
            AnyByteOrder::Ordered(xs) => xs.try_into(),
        }
    }
}

/// The number of bytes for a numeric measurement
#[derive(Into, Debug, Display)]
#[into(u8, NonZeroU8, PrivBitsOrChars)]
pub struct Bytes(pub(crate) PrivBytes);

/// The number of bytes for a numeric measurement; used for method arguments.
pub struct ArgBytes(pub(crate) PrivBytes);

impl Default for ArgBytes {
    fn default() -> Self {
        Self(PrivBytes::B4)
    }
}

impl TryFrom<u8> for ArgBytes {
    type Error = NewArgBytesError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Ok(Self(PrivBytes::try_from(value)?))
    }
}

/// Error when making new [`ArgBytes`] from [`u8`].
#[derive(Debug, Error, From)]
#[error("must be integer 1-8")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct NewArgBytesError(TryFromPrimitiveError<PrivBytes>);

/// Private version of `Bytes`
#[derive(Clone, Copy, PartialEq, Eq, Hash, TryFromPrimitive, IntoPrimitive, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[repr(u8)]
#[display("{}", u8::from(*self))]
pub(crate) enum PrivBytes {
    B1 = 1,
    B2,
    B3,
    B4,
    B5,
    B6,
    B7,
    B8,
}

/// The value of $PnB if it is fixed.
///
/// Subsequent operations can be used to use it as "bytes" or "characters"
/// depending on what is needed by the column.
#[derive(Clone, Copy, PartialEq, Eq, Hash, From, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[into(NonZeroU8, u8)]
pub struct BitsOrChars(pub(crate) PrivBitsOrChars);

impl<'a> ToDisplayNE<'a> for BitsOrChars {
    type NE = NonZeroU8;
    fn to_ne(&'a self) -> Self::NE {
        self.0.0
    }
}

/// Internal version of `BitsOrChars`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, From, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[from(Chars)]
#[into(NonZeroU8, u8)]
pub(crate) struct PrivBitsOrChars(NonZeroU8);

/// Relate types corresponding to keywords to those storing byte layout.
pub(crate) trait HasByteOrd: Sized {
    type ByteOrd: From<Self> + ReqMetarootKey;
}

impl HasByteOrd for NoByteOrd2_0 {
    type ByteOrd = ByteOrd2_0;
}

impl HasByteOrd for NoByteOrd3_1 {
    type ByteOrd = ByteOrd3_1;
}

impl HasByteOrd for Endian {
    type ByteOrd = ByteOrd3_1;
}

impl PrivBytes {
    /// Return number of bytes needed to express the given u64.
    pub(crate) fn from_u64(x: u64) -> Self {
        // find position of most-significant non-zero byte
        x.to_le_bytes()
            .iter()
            .rposition(|i| *i > 0)
            .and_then(|i| u8::try_from(i + 1).ok())
            .and_then(|i| Self::try_from(i).ok())
            .unwrap_or(Self::B1)
    }
}

impl<const LEN: usize> TryFrom<ArrayByteOrd<LEN>> for Endian {
    type Error = OrderedToEndianError;
    fn try_from(value: ArrayByteOrd<LEN>) -> Result<Self, Self::Error> {
        assert!(value.is_valid_order(), "invalid byte order");
        match value.as_endian() {
            Some(e) => Ok(e),
            None => Err(OrderedToEndianError),
        }
    }
}

impl<const LEN: usize> TryFrom<Vec<NonZeroU8>> for ArrayByteOrd<LEN>
where
    [NonZeroU8; LEN]: TryInto<Self, Error = NewByteOrdError>,
{
    type Error = VecToSizedError;
    fn try_from(value: Vec<NonZeroU8>) -> Result<Self, Self::Error> {
        let xs: [NonZeroU8; LEN] = value.try_into().map_err(|ys: Vec<_>| VecToArrayError {
            vec_len: ys.len(),
            req_len: LEN,
        })?;
        let ret = xs.try_into()?;
        Ok(ret)
    }
}

impl<const LEN: usize> TryFrom<[NonZeroU8; LEN]> for ArrayByteOrd<LEN> {
    type Error = NewByteOrdError;
    fn try_from(xs: [NonZeroU8; LEN]) -> Result<Self, Self::Error> {
        let new = Self(xs.map(|x| u8::from(x) - 1));
        if new.is_valid_order() {
            Ok(new)
        } else {
            Err(NewByteOrdError(LEN))
        }
    }
}

impl<const LEN: usize> From<ArrayByteOrd<LEN>> for [NonZeroU8; LEN] {
    fn from(value: ArrayByteOrd<LEN>) -> Self {
        assert!(value.is_valid_order(), "invalid byte order");
        value.0.map(|x| NonZeroU8::MIN.saturating_add(x))
    }
}

impl<'a, const LEN: usize> ToDisplayNE<'a> for ArrayByteOrd<LEN>
where
    [NonZeroU8; LEN]: IntoNonEmptyIterator,
    NEVec<NonZeroU8>: FromNonEmptyIterator<<[NonZeroU8; LEN] as IntoIterator>::Item>,
{
    type NE = NEDelim<NEVec<NonZeroU8>>;
    fn to_ne(&'a self) -> Self::NE {
        assert!(self.is_valid_order(), "invalid byte order");
        let xs = <[NonZeroU8; LEN]>::from(*self);
        NEDelim::new(',', xs.into_nonempty_iter().collect())
    }
}

impl<const LEN: usize> Default for ArrayByteOrd<LEN>
where
    Endian: Into<Self>,
{
    fn default() -> Self {
        Endian::Little.into()
    }
}

impl<const LEN: usize> From<Endian> for ArrayByteOrd<LEN> {
    fn from(value: Endian) -> Self {
        let mut arr = array::from_fn(|i| u8::try_from(i).unwrap());
        if value == Endian::Big {
            arr.reverse();
        }
        Self(arr)
    }
}

macro_rules! byteord_from_sized {
    ($len:expr, $var:ident, $bytes:ident) => {
        impl TryFrom<ByteOrd2_0> for ArrayByteOrd<$len> {
            type Error = ByteOrdToSizedError;
            fn try_from(value: ByteOrd2_0) -> Result<Self, Self::Error> {
                if let ByteOrd2_0::$var(sized) = value {
                    Ok(sized)
                } else {
                    Err(ByteOrdToSizedError::new(value.nbytes(), $len))
                }
            }
        }

        impl HasByteOrd for ArrayByteOrd<$len> {
            type ByteOrd = ByteOrd2_0;
        }
    };
}

byteord_from_sized!(1, O1, B1);
byteord_from_sized!(2, O2, B2);
byteord_from_sized!(3, O3, B3);
byteord_from_sized!(4, O4, B4);
byteord_from_sized!(5, O5, B5);
byteord_from_sized!(6, O6, B6);
byteord_from_sized!(7, O7, B7);
byteord_from_sized!(8, O8, B8);

impl<const LEN: usize> ArrayByteOrd<LEN> {
    /// Convert to [`Endian`] if possible.
    pub fn as_endian(&self) -> Option<Endian> {
        assert!(self.is_valid_order(), "invalid byte order");
        let mut it = self.0.iter().copied().map(usize::from);
        if it.by_ref().enumerate().all(|(i, x)| i == x) {
            Some(Endian::Little)
        } else if it.rev().enumerate().all(|(i, x)| i == x) {
            Some(Endian::Big)
        } else {
            None
        }
    }

    /// Check if this array is a valid byte order.
    ///
    /// Array is valid if it contains all numbers 0 to LEN - 1 exactly once in
    /// any order.
    fn is_valid_order(&self) -> bool {
        let mut flags = [false; LEN];
        // Try to subtract one from each number. While doing so, track which
        // numbers were seen by setting flags in an array where each index
        // corresponds to the number we wish to see. If all are true, then each
        // number is present.
        for x in &self.0 {
            let i = usize::from(*x);
            if i < LEN {
                flags[i] = true;
            }
        }
        flags.iter().all(|x| *x)
    }
}

#[cfg(feature = "serde")]
impl<const LEN: usize> Serialize for ArrayByteOrd<LEN> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        Self: Into<[NonZeroU8; LEN]>,
        S: serde::Serializer,
    {
        assert!(self.is_valid_order(), "invalid byte order");
        let xs: [NonZeroU8; LEN] = (*self).into();
        xs.to_vec().serialize(serializer)
    }
}

#[allow(clippy::many_single_char_names)]
impl TryFrom<&[NonZeroU8]> for ByteOrd2_0 {
    type Error = NewByteOrdError;
    fn try_from(xs: &[NonZeroU8]) -> Result<Self, Self::Error> {
        match xs {
            &[a] => [a].try_into().map(Self::O1),
            &[a, b] => [a, b].try_into().map(Self::O2),
            &[a, b, c] => [a, b, c].try_into().map(Self::O3),
            &[a, b, c, d] => [a, b, c, d].try_into().map(Self::O4),
            &[a, b, c, d, e] => [a, b, c, d, e].try_into().map(Self::O5),
            &[a, b, c, d, e, f] => [a, b, c, d, e, f].try_into().map(Self::O6),
            &[a, b, c, d, e, f, g] => [a, b, c, d, e, f, g].try_into().map(Self::O7),
            &[a, b, c, d, e, f, g, h] => [a, b, c, d, e, f, g, h].try_into().map(Self::O8),
            ys => Err(NewByteOrdError(ys.len())),
        }
    }
}

impl From<bool> for Endian {
    fn from(value: bool) -> Self {
        if value { Self::Big } else { Self::Little }
    }
}

impl TryFrom<Width> for Chars {
    type Error = WidthToFixedError<CharsError>;
    fn try_from(value: Width) -> Result<Self, Self::Error> {
        let fixed = PrivBitsOrChars::try_from(value)?;
        fixed.try_into().map_err(WidthToFixedError::Fixed)
    }
}

impl TryFrom<Width> for PrivBytes {
    type Error = WidthToFixedError<WidthToBytesError>;
    fn try_from(value: Width) -> Result<Self, Self::Error> {
        let fixed = PrivBitsOrChars::try_from(value)?;
        fixed.try_into().map_err(WidthToFixedError::Fixed)
    }
}

impl TryFrom<Width> for PrivBitsOrChars {
    type Error = VariableWidthError;
    fn try_from(value: Width) -> Result<Self, Self::Error> {
        if let Width::Fixed(x) = value {
            Ok(x.0)
        } else {
            Err(VariableWidthError)
        }
    }
}

impl TryFrom<PrivBitsOrChars> for Chars {
    type Error = CharsError;
    fn try_from(value: PrivBitsOrChars) -> Result<Self, Self::Error> {
        NonZeroU8::from(value).try_into()
    }
}

impl TryFrom<PrivBitsOrChars> for PrivBytes {
    type Error = WidthToBytesError;
    /// Return number of bytes represented by this.
    ///
    /// Return error if bits is not divisible by 8 and within [1,64].
    fn try_from(value: PrivBitsOrChars) -> Result<Self, Self::Error> {
        let x = u8::from(value.0);
        if x.trailing_zeros() >= 3 {
            return (x >> 3).try_into().or(Err(WidthToBytesError(x)));
        }
        Err(WidthToBytesError(x))
    }
}

impl From<PrivBytes> for NonZeroU8 {
    fn from(value: PrivBytes) -> Self {
        // ASSUME this will never fail because Bytes is 1-8
        Self::new(u8::from(value)).unwrap()
    }
}

impl From<PrivBytes> for PrivBitsOrChars {
    fn from(value: PrivBytes) -> Self {
        // ASSUME this will never fail because Bytes is 1-8
        Self(NonZeroU8::new(u8::from(value) * 8).unwrap())
    }
}

impl From<Option<NonZeroU8>> for Width {
    fn from(value: Option<NonZeroU8>) -> Self {
        value.map_or(Self::Variable, |x| {
            Self::Fixed(BitsOrChars(PrivBitsOrChars(x)))
        })
    }
}

impl From<Width> for Option<NonZeroU8> {
    fn from(value: Width) -> Self {
        match value {
            Width::Variable => None,
            Width::Fixed(x) => Some(x.0.0),
        }
    }
}

impl TryFrom<Width> for NonZeroU8 {
    type Error = ();
    fn try_from(value: Width) -> Result<Self, Self::Error> {
        if let Width::Fixed(x) = value {
            Ok(x.0.0)
        } else {
            Err(())
        }
    }
}

impl TryFrom<ByteOrd2_0> for Endian {
    type Error = OrderedToEndianError;

    fn try_from(value: ByteOrd2_0) -> Result<Self, Self::Error> {
        match_many_to_one!(value, ByteOrd2_0, [O1, O2, O3, O4, O5, O6, O7, O8], x, {
            x.try_into()
        })
    }
}

impl FromStr for Endian {
    type Err = NewEndianError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "1,2,3,4" => Ok(Self::Little),
            "4,3,2,1" => Ok(Self::Big),
            _ => Err(NewEndianError),
        }
    }
}

impl FromStr for Width {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "*" => Ok(Self::Variable),
            _ => s
                .parse::<NonZeroU8>()
                .map(|x| Self::Fixed(BitsOrChars(PrivBitsOrChars(x)))),
        }
    }
}

/// Error when making a new byte order of some size from a sequence of digits.
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::InvalidKeywordValueError))]
pub struct NewByteOrdError(usize);

impl fmt::Display for NewByteOrdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if self.0 == 0 {
            write!(f, "byte order not be empty")
        } else {
            write!(f, "byte order must include 1-{} uniquely", self.0)
        }
    }
}

/// Error when parsing Endian from string
#[derive(Debug, Error)]
#[error("endian must be either 1,2,3,4 or 4,3,2,1")]
pub struct NewEndianError;

/// Error when converting $BYTEORD from 2.0/3.0 to 3.1/3.2
#[derive(Debug, Error)]
#[error("byte order is not monotonic")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct OrderedToEndianError;

/// Error when coercing $BYTEORD to a fixed size for use in parsing a layout
#[derive(Debug, Error, new)]
#[error("$BYTEORD is {bytes} bytes long, expected {length}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct ByteOrdToSizedError {
    bytes: PrivBytes,
    length: usize,
}

/// Error when converting $PnB to a fixed value.
///
/// This is a helper type meant to construct more specific errors, namely those
/// for converting $PnB to bytes (numeric layouts) and chars (ASCII layouts).
#[derive(Debug, From)]
pub(crate) enum WidthToFixedError<X> {
    #[from]
    Variable(VariableWidthError),
    Fixed(X),
}

/// Dummy type to indicate that $PnB is variable width ('*')
#[derive(Debug)]
pub(crate) struct VariableWidthError;

/// Error when converting $PnB (in bits) to bytes.
#[derive(Debug, Display)]
#[display("bits must be multiple of 8 and between 8 and 64, got {_0}")]
pub(crate) struct WidthToBytesError(u8);

/// Error when converting [`Vec<NonzeroU8>`] to [`ArrayByteOrd`].
#[derive(From, Error, Display, Debug)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum VecToSizedError {
    Vec(VecToArrayError),
    New(NewByteOrdError),
}

/// Error when converting [`Vec<NonzeroU8>`] to [`ArrayByteOrd`] when former is wrong size.
#[derive(Debug, Error)]
#[error("could not convert vector to array, was {vec_len} long, needed {req_len}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::InvalidKeywordValueError))]
pub struct VecToArrayError {
    vec_len: usize,
    req_len: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn str_to_width() {
        assert_eq!("*".parse::<Width>(), Ok(Width::Variable));
        assert!("1".parse::<Width>().is_ok(),);
        assert!("255".parse::<Width>().is_ok());
        assert!("0".parse::<Width>().is_err());
        assert!("256".parse::<Width>().is_err());
    }

    #[test]
    fn str_to_width_as_bytes() {
        assert!(PrivBytes::try_from("8".parse::<Width>().unwrap()).is_ok());
        assert!(PrivBytes::try_from("16".parse::<Width>().unwrap()).is_ok());
        assert!(PrivBytes::try_from("64".parse::<Width>().unwrap()).is_ok());
        assert!(PrivBytes::try_from("7".parse::<Width>().unwrap()).is_err());
        assert!(PrivBytes::try_from("63".parse::<Width>().unwrap()).is_err());
        assert!(PrivBytes::try_from("65".parse::<Width>().unwrap()).is_err());
        assert!(PrivBytes::try_from("72".parse::<Width>().unwrap()).is_err(),);
    }

    #[test]
    fn bytes_from_u64() {
        assert_eq!(PrivBytes::B1, PrivBytes::from_u64(0));
        assert_eq!(PrivBytes::B1, PrivBytes::from_u64(0x00FF));
        assert_eq!(PrivBytes::B2, PrivBytes::from_u64(0x0100));
        assert_eq!(PrivBytes::B2, PrivBytes::from_u64(0xFFFF));
        assert_eq!(PrivBytes::B3, PrivBytes::from_u64(0x0001_0000));
        assert_eq!(PrivBytes::B8, PrivBytes::from_u64(0xFFFF_FFFF_FFFF_FFFF));
    }

    #[test]
    fn valid_order() {
        assert!(ArrayByteOrd_([0, 1, 2, 3]).is_valid_order());
        assert!(ArrayByteOrd_([3, 2, 1, 0]).is_valid_order());
        assert!(ArrayByteOrd_([2, 3, 0, 1]).is_valid_order());
    }

    #[test]
    fn invalid_order() {
        assert!(!ArrayByteOrd_([1, 2, 3, 4]).is_valid_order());
        assert!(!ArrayByteOrd_([0, 0, 1, 1]).is_valid_order());
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{ArgBytes, Endian, NewArgBytesError};

    use fireflow_types::keywords::{BYTEORD_BIG, BYTEORD_LITTLE};
    use fireflow_types::python::InvalidKeywordValueError;

    use pyo3::types::PyInt;
    use pyo3::{prelude::*, types::PyString};

    use std::convert::Infallible;

    // This is just a python integer 1-8. Thus far this is only used when
    // making data layouts.
    impl<'py> FromPyObject<'py> for ArgBytes {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let x = ob.extract::<u8>()?;
            Ok(Self(x.try_into().map_err(NewArgBytesError)?))
        }
    }

    impl<'py> IntoPyObject<'py> for ArgBytes {
        type Target = PyInt;
        type Output = Bound<'py, Self::Target>;
        type Error = Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            u8::from(self.0).into_pyobject(py)
        }
    }

    // on the python side, represent big and little endian with string literals
    // "big" and "little" (to avoid using a boolean for which the direction
    // of meaning is not obvious)
    impl<'py> FromPyObject<'py> for Endian {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let xs = ob.extract::<String>()?;
            match xs.as_str() {
                BYTEORD_BIG => Ok(Self::Big),
                BYTEORD_LITTLE => Ok(Self::Little),
                _ => {
                    let msg = format!("must be '{BYTEORD_BIG}' or '{BYTEORD_LITTLE}'");
                    Err(InvalidKeywordValueError::new_err(msg))
                }
            }
        }
    }

    impl<'py> IntoPyObject<'py> for Endian {
        type Target = PyString;
        type Output = Bound<'py, PyString>;
        type Error = Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Big => BYTEORD_BIG,
                Self::Little => BYTEORD_LITTLE,
            }
            .into_pyobject(py)
        }
    }
}
