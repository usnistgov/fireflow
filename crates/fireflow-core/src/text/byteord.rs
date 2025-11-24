use crate::macros::match_many_to_one;
use crate::text::keywords::{ByteOrd2_0, ByteOrd3_1, Width};
use crate::validated::ascii_range::{Chars, CharsError};

use derive_more::{Display, From, Into};
use derive_new::new;
use itertools::Itertools as _;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::fmt;
use std::num::NonZeroU8;
use std::num::ParseIntError;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use fireflow_core_proc::DisplayAsPyErr;

use super::lookup::ReqMetarootKey;

/// Byte order with known size in bytes
#[derive(PartialEq, Eq, Hash, Copy, Clone, From, Debug)]
pub enum SizedByteOrd<const LEN: usize> {
    /// Either big or little endian
    #[from]
    Endian(Endian),

    /// The byte order if mixed (not monotonically increasing/decreasing)
    Order([u8; LEN]),
}

/// Endianness (big or little)
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Endian {
    #[display("4,3,2,1")]
    Big,
    #[default]
    #[display("1,2,3,4")]
    Little,
}

/// Marker type representing lack of byte order.
///
/// This is used in ASCII layouts, for which $BYTEORD is meaningless.
#[derive(Clone, Copy, Default, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct NoByteOrd<const ORD: bool>;

pub type NoByteOrd2_0 = NoByteOrd<true>;

pub type NoByteOrd3_1 = NoByteOrd<false>;

/// The number of bytes for a numeric measurement
#[derive(Into, Debug, Display)]
#[into(u8, NonZeroU8, PrivBitsOrChars)]
pub struct Bytes(pub(crate) PrivBytes);

/// Private version of `Bytes`
#[derive(Clone, Copy, PartialEq, Eq, Hash, TryFromPrimitive, IntoPrimitive, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[repr(u8)]
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
#[derive(Clone, Copy, PartialEq, Eq, Hash, From, Into, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[into(NonZeroU8, u8)]
pub struct BitsOrChars(pub(crate) PrivBitsOrChars);

/// Internal version of `BitsOrChars`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, From, Into, Debug, Display)]
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

macro_rules! byteord_from_sized {
    ($len:expr, $var:ident, $bytes:ident) => {
        impl TryFrom<SizedByteOrd<$len>> for Endian {
            type Error = OrderedToEndianError;
            fn try_from(value: SizedByteOrd<$len>) -> Result<Self, Self::Error> {
                match value {
                    SizedByteOrd::Endian(x) => Ok(x),
                    SizedByteOrd::Order(_) => Err(OrderedToEndianError),
                }
            }
        }

        impl TryFrom<ByteOrd2_0> for SizedByteOrd<$len> {
            type Error = ByteOrdToSizedError;
            fn try_from(value: ByteOrd2_0) -> Result<Self, Self::Error> {
                if let ByteOrd2_0::$var(sized) = value {
                    Ok(sized)
                } else {
                    Err(ByteOrdToSizedError::new(value.nbytes(), $len))
                }
            }
        }

        /// Convert array of length $len to byte order.
        ///
        /// Correct array will be from the set of {1..$len} and each number
        /// will only appear once in any order.
        impl TryFrom<[NonZeroU8; $len]> for SizedByteOrd<$len> {
            type Error = NewByteOrdError;
            fn try_from(xs: [NonZeroU8; $len]) -> Result<Self, Self::Error> {
                let mut flags = [false; $len];
                // Try to subtract one from each number. While doing so, track
                // which numbers were seen by setting flags in an array where
                // each index corresponds to the number we wish to see. If all
                // are true, then each number is present.
                let ys = xs.map(|x| {
                    let y = u8::from(x) - 1;
                    if y < $len {
                        flags[usize::from(y)] = true;
                    }
                    y
                });
                if flags.iter().all(|x| *x) {
                    let mut it = ys.iter().copied().map(usize::from);
                    let ret = if it.by_ref().enumerate().all(|(i, x)| i == x) {
                        Self::Endian(Endian::Little)
                    } else if it.rev().enumerate().all(|(i, x)| i == x) {
                        Self::Endian(Endian::Big)
                    } else {
                        // something else (mixed)
                        Self::Order(ys)
                    };
                    Ok(ret)
                } else {
                    Err(NewByteOrdError($len))
                }
            }
        }

        impl From<SizedByteOrd<$len>> for [NonZeroU8; $len] {
            fn from(value: SizedByteOrd<$len>) -> [NonZeroU8; $len] {
                let arr = match value {
                    SizedByteOrd::Endian(e) => {
                        // ASSUME this will never fail because we will only
                        // call this for ints 1-8
                        let mut o = std::array::from_fn(|i| u8::try_from(i).unwrap());
                        if e == Endian::Big {
                            o.reverse();
                        };
                        o
                    }
                    SizedByteOrd::Order(o) => o,
                };
                arr.map(|x| NonZeroU8::MIN.saturating_add(x))
            }
        }

        impl SizedByteOrd<$len> {
            pub(crate) fn nbytes() -> PrivBytes {
                PrivBytes::$bytes
            }
        }

        impl HasByteOrd for SizedByteOrd<$len> {
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

#[cfg(feature = "serde")]
impl<const LEN: usize> Serialize for SizedByteOrd<LEN> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Endian(e) => serializer.serialize_newtype_variant("SizedByteOrd", 0, "Endian", e),
            Self::Order(o) => {
                serializer.serialize_newtype_variant("SizedByteOrd", 1, "Order", &o[..])
            }
        }
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

impl<const LEN: usize> Default for SizedByteOrd<LEN> {
    fn default() -> Self {
        Self::Endian(Endian::default())
    }
}

impl SizedByteOrd<2> {
    #[must_use]
    pub fn endian(&self) -> Endian {
        let [x, y] = (*self).into();
        (y > x).into()
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
        // ASSUME this will never fail
        Self::new(u8::from(value)).unwrap()
    }
}

impl From<PrivBytes> for PrivBitsOrChars {
    fn from(value: PrivBytes) -> Self {
        // ASSUME this will never fail
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

// TODO add option to remove spaces around commas if they exist
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

impl<const LEN: usize> fmt::Display for SizedByteOrd<LEN>
where
    [NonZeroU8; LEN]: From<Self>,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", <[NonZeroU8; LEN]>::from(*self).iter().join(","))
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

impl fmt::Display for PrivBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        u8::from(*self).fmt(f)
    }
}

/// Error when making a new byte order of some size from a sequence of digits.
#[derive(Debug, Error)]
#[error("byte order must include 1-{0} uniquely")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::InvalidKeywordValueError))]
pub struct NewByteOrdError(usize);

/// Error when parsing Endian from string
#[derive(Debug, Error)]
#[error("endian must be either 1,2,3,4 or 4,3,2,1")]
pub struct NewEndianError;

/// Error when converting $BYTEORD from 2.0/3.0 to 3.1/3.2
#[derive(Debug, Error)]
#[error("byte order is not monotonic")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConversionError))]
pub struct OrderedToEndianError;

/// Error when coercing $BYTEORD to a fixed size for use in parsing a layout
#[derive(Debug, Error, new)]
#[error("$BYTEORD is {bytes} bytes long, expected {length}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
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
}

#[cfg(feature = "python")]
mod python {
    use crate::python::InvalidKeywordValueError;

    use super::{Endian, NewByteOrdError, SizedByteOrd};

    use fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr};

    use derive_more::{Display, From};
    use pyo3::{IntoPyObjectExt as _, prelude::*, types::PyString};
    use std::convert::Infallible;
    use std::num::NonZeroU8;
    use thiserror::Error;

    #[derive(From, Display)]
    #[cfg_attr(feature = "python", derive(AllIntoPyErr))]
    pub enum VecToSizedError {
        Vec(VecToArrayError),
        New(NewByteOrdError),
    }

    #[derive(Debug, Error)]
    #[error("could not convert vector to array, was {vec_len} long, needed {req_len}")]
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(crate::python::InvalidKeywordValueError))]
    pub struct VecToArrayError {
        vec_len: usize,
        req_len: usize,
    }

    macro_rules! impl_vec_to_sized {
        ($len:expr) => {
            impl TryFrom<Vec<NonZeroU8>> for SizedByteOrd<$len> {
                type Error = VecToSizedError;
                fn try_from(value: Vec<NonZeroU8>) -> Result<Self, Self::Error> {
                    let xs: [NonZeroU8; $len] =
                        value.try_into().map_err(|ys: Vec<_>| VecToArrayError {
                            vec_len: ys.len(),
                            req_len: $len,
                        })?;
                    let ret = xs.try_into()?;
                    Ok(ret)
                }
            }
        };
    }

    impl_vec_to_sized!(1);
    impl_vec_to_sized!(2);
    impl_vec_to_sized!(3);
    impl_vec_to_sized!(4);
    impl_vec_to_sized!(5);
    impl_vec_to_sized!(6);
    impl_vec_to_sized!(7);
    impl_vec_to_sized!(8);

    // on the python side, represent big and little endian with string literals
    // "big" and "little" (to avoid using a boolean for which the direction
    // of meaning is not obvious)
    impl<'py> FromPyObject<'py> for Endian {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let xs = ob.extract::<String>()?;
            match xs.as_str() {
                "big" => Ok(Self::Big),
                "little" => Ok(Self::Little),
                _ => Err(InvalidKeywordValueError::new_err(
                    "must be \"big\" or \"little\"",
                )),
            }
        }
    }

    impl<'py> IntoPyObject<'py> for Endian {
        type Target = PyString;
        type Output = Bound<'py, PyString>;
        type Error = Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Big => "big",
                Self::Little => "little",
            }
            .into_pyobject(py)
        }
    }

    // for mixed byte, order use literals "big" and "little" like above and also
    // check for appropriate lists which represent mixed order
    impl<'py, const LEN: usize> FromPyObject<'py> for SizedByteOrd<LEN>
    where
        Self: TryFrom<Vec<NonZeroU8>, Error = VecToSizedError>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let err =
                || InvalidKeywordValueError::new_err("must be \"little\", \"big\", or a list");
            if let Ok(s) = ob.extract::<String>() {
                match s.as_str() {
                    "little" => Ok(Endian::Little),
                    "big" => Ok(Endian::Big),
                    _ => Err(err()),
                }
                .map(Self::from)
            } else if let Ok(xs) = ob.extract::<Vec<NonZeroU8>>() {
                Ok(Self::try_from(xs)?)
            } else {
                Err(err())
            }
        }
    }

    impl<'py, const LEN: usize> IntoPyObject<'py> for SizedByteOrd<LEN> {
        type Target = PyAny;
        type Output = Bound<'py, PyAny>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Endian(Endian::Big) => "big".into_bound_py_any(py),
                Self::Endian(Endian::Little) => "little".into_bound_py_any(py),
                // use u32 here since Vec<u8> converts to bytes in python
                Self::Order(xs) => xs
                    .into_iter()
                    .map(u32::from)
                    .collect::<Vec<_>>()
                    .into_pyobject(py),
            }
        }
    }
}
