use crate::macros::newtype_from_outer;

use itertools::Itertools;
use serde::Serialize;
use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

/// Endianness
///
/// This is also stored in the $BYTEORD key in 3.1+
#[derive(Clone, Copy, Serialize, PartialEq, Eq, Hash)]
pub enum Endian {
    Big,
    Little,
}

/// The byte order as shown in the $BYTEORD field in 2.0 and 3.0
///
/// This can be either 1,2,3,4 (little endian), 4,3,2,1 (big endian), or some
/// sequence representing byte order. For 2.0 and 3.0, this sequence is
/// technically allowed to vary in length in the case of $DATATYPE=I since
/// integers do not necessarily need to be 32 or 64-bit.
#[derive(Clone, Serialize)]
pub enum ByteOrd {
    // TODO this should also be applied to things like 1,2,3 or 5,4,3,2,1, which
    // are big/little endian but not "traditional" byte widths.
    Endian(Endian),
    // TODO use lehmer encoding for this
    Mixed(MixedOrder),
}

/// The value for the $PnB key (all versions)
///
/// The $PnB key actually stores bits. However, this library only supports
/// widths that are multiples of 8 (ie bytes) for now. Therefore, this key
/// actually stores the number of bytes indicated by $PnB.
///
/// This may also be '*' which means "delimited ASCII" which is only valid when
/// $DATATYPE=A.
#[derive(Clone, Copy, Serialize)]
pub enum Width {
    Fixed(BitsOrChars),
    Variable,
}

/// The number of bytes for a numeric measurement
#[derive(Clone, Copy, Serialize, PartialEq)]
pub struct Bytes(u8);

/// The number of chars or an ASCII measurement
#[derive(Clone, Copy, Serialize, PartialEq)]
pub struct Chars(u8);

#[derive(Clone, Copy, Serialize)]
pub struct BitsOrChars(u8);

#[derive(PartialEq, Eq, Hash, Copy, Clone)]
pub enum SizedByteOrd<const LEN: usize> {
    Endian(Endian),
    Order([u8; LEN]),
}

#[derive(Clone, Serialize)]
pub struct MixedOrder(Vec<u8>);

impl ByteOrd {
    pub fn new(xs: Vec<u8>) -> Option<Self> {
        match xs[..] {
            [1, 2, 3, 4] => Some(ByteOrd::Endian(Endian::Little)),
            [4, 3, 2, 1] => Some(ByteOrd::Endian(Endian::Big)),
            _ => {
                let n = xs.len();
                if xs.iter().unique().count() != n
                    || !(1..8).contains(&n)
                    || xs.iter().min().is_some_and(|x| *x != 1)
                    || xs.iter().max().is_some_and(|x| usize::from(*x) != n)
                {
                    None
                } else {
                    Some(ByteOrd::Mixed(MixedOrder(
                        xs.iter().map(|x| x - 1).collect(),
                    )))
                }
            }
        }
    }

    pub fn nbytes(&self) -> Bytes {
        match self {
            ByteOrd::Endian(_) => Bytes(4),
            // ASSUME this will always be 1-8 elements
            ByteOrd::Mixed(xs) => Bytes(xs.0.len() as u8),
        }
    }

    // TODO this is basically tryfrom
    pub fn as_sized<const LEN: usize>(&self) -> Result<SizedByteOrd<LEN>, String> {
        match self {
            ByteOrd::Endian(e) => Ok(SizedByteOrd::Endian(*e)),
            ByteOrd::Mixed(v) => v.0[..]
                .try_into()
                .map(|order: [u8; LEN]| SizedByteOrd::Order(order))
                .or(Err(format!(
                    "$BYTEORD is mixed but length is {} and not {LEN}",
                    v.0.len()
                ))),
        }
    }
}

impl Endian {
    pub fn is_big(x: bool) -> Self {
        if x {
            Endian::Big
        } else {
            Endian::Little
        }
    }
}

impl Bytes {
    pub fn try_new(x: u8) -> Option<Self> {
        if 0 < x && x <= 64 {
            Some(Self(x))
        } else {
            None
        }
    }
}

impl Chars {
    pub fn try_new(x: u8) -> Option<Self> {
        if 0 < x && x <= 20 {
            Some(Self(x))
        } else {
            None
        }
    }
}

impl Width {
    pub fn new_f32() -> Self {
        Width::Fixed(BitsOrChars(32))
    }

    pub fn new_f64() -> Self {
        Width::Fixed(BitsOrChars(64))
    }
}

impl BitsOrChars {
    /// Return the number of chars represented by this if 20 or less.
    ///
    /// We can only check if the inner value is <=64 since we don't know at
    /// parse time if this is for an ASCII column or a numeric column, so
    /// need to check that the number is less then 20, which is the maximum
    /// number of digits that can be stored in a u64.
    pub fn chars(&self) -> Option<Chars> {
        if self.0 > 20 {
            None
        } else {
            Some(Chars(self.0))
        }
    }

    /// Return number of bytes represented by this.
    ///
    /// Return None if not divisible by 8. This should only return a number
    /// in [1, 8] because we check that Self is within [1,64]
    pub fn bytes(&self) -> Option<Bytes> {
        if self.0 % 8 == 0 {
            Some(Bytes(self.0 / 8))
        } else {
            None
        }
    }

    pub fn inner(&self) -> u8 {
        self.0
    }
}

impl FromStr for Endian {
    type Err = EndianError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "1,2,3,4" => Ok(Endian::Little),
            "4,3,2,1" => Ok(Endian::Big),
            _ => Err(EndianError),
        }
    }
}

impl fmt::Display for Endian {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let x = match self {
            Endian::Big => "4,3,2,1",
            Endian::Little => "1,2,3,4",
        };
        write!(f, "{x}")
    }
}

impl FromStr for ByteOrd {
    type Err = ParseByteOrdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse() {
            Ok(e) => Ok(ByteOrd::Endian(e)),
            _ => {
                let (pass, fail): (Vec<_>, Vec<_>) =
                    s.split(",").map(|x| x.parse::<u8>()).partition_result();
                if fail.is_empty() {
                    ByteOrd::new(pass).ok_or(ParseByteOrdError::InvalidOrder)
                } else {
                    Err(ParseByteOrdError::InvalidNumbers)
                }
            }
        }
    }
}

impl fmt::Display for ByteOrd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ByteOrd::Endian(e) => write!(f, "{}", e),
            ByteOrd::Mixed(xs) => write!(f, "{}", xs.0.iter().join(",")),
        }
    }
}

pub struct EndianError;

impl fmt::Display for EndianError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Endian must be either 1,2,3,4 or 4,3,2,1")
    }
}

pub enum ParseByteOrdError {
    InvalidOrder,
    InvalidNumbers,
}

impl fmt::Display for ParseByteOrdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ParseByteOrdError::InvalidNumbers => write!(f, "Could not parse numbers in byte order"),
            ParseByteOrdError::InvalidOrder => write!(f, "Byte order must include 1-n uniquely"),
        }
    }
}

newtype_from_outer!(Bytes, u8);
newtype_from_outer!(Chars, u8);
newtype_from_outer!(BitsOrChars, u8);

impl From<Chars> for Width {
    fn from(value: Chars) -> Self {
        Width::Fixed(BitsOrChars(value.0))
    }
}

impl From<Bytes> for Width {
    fn from(value: Bytes) -> Self {
        Width::Fixed(BitsOrChars(value.0 * 8))
    }
}

impl TryFrom<Width> for u8 {
    type Error = ();
    fn try_from(value: Width) -> Result<Self, Self::Error> {
        match value {
            Width::Fixed(x) => Ok(x.0),
            _ => Err(()),
        }
    }
}

impl FromStr for Width {
    type Err = ParseBitsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "*" => Ok(Width::Variable),
            _ => s.parse::<u8>().map_err(ParseBitsError::Int).and_then(|x| {
                if x < 1 || 64 < x {
                    Err(ParseBitsError::Inner(BitsError))
                } else {
                    Ok(Width::Fixed(BitsOrChars(x)))
                }
            }),
        }
    }
}

impl fmt::Display for Width {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Width::Fixed(x) => write!(f, "{}", x.0),
            Width::Variable => write!(f, "*"),
        }
    }
}

pub enum ParseBitsError {
    Int(ParseIntError),
    Inner(BitsError),
}

pub struct BitsError;

impl fmt::Display for ParseBitsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ParseBitsError::Int(i) => write!(f, "{}", i),
            ParseBitsError::Inner(i) => i.fmt(f),
        }
    }
}

impl fmt::Display for BitsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "bits must be between 8 and 64")
    }
}
