use crate::macros::{newtype_disp, newtype_from_outer};

use super::keywords::AlphaNumType;

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
/// This must be a set of unique integers in {1, N} where N is the length of the
/// vector and ranged [1, 8]. It will actually be stored as 1 less than the
/// input sequence, which will reflect the 0-indexed operation of Rust arrays.
#[derive(Clone, Serialize)]
pub struct ByteOrd(Vec<u8>);

/// The value for the $PnB key (all versions)
///
/// The $PnB key actually stores bits. However, this library only supports
/// widths that are multiples of 8 (ie bytes) for now. Therefore, this key
/// actually stores the number of bytes indicated by $PnB.
///
/// This may also be '*' which means "delimited ASCII" which is only valid when
/// $DATATYPE=A.
#[derive(Clone, Copy, Serialize, PartialEq, Eq, Hash)]
pub enum Width {
    Fixed(BitsOrChars),
    Variable,
}

/// The number of bytes for a numeric measurement
#[derive(Clone, Copy, Serialize, PartialEq, Eq, Hash)]
pub struct Bytes(u8);

/// The number of chars or an ASCII measurement
#[derive(Clone, Copy, Serialize, PartialEq, Eq, Hash)]
pub struct Chars(u8);

#[derive(Clone, Copy, Serialize, PartialEq, Eq, Hash)]
pub struct BitsOrChars(u8);

#[derive(PartialEq, Eq, Hash, Copy, Clone)]
pub enum SizedByteOrd<const LEN: usize> {
    Endian(Endian),
    Order([u8; LEN]),
}

#[derive(Clone, Serialize)]
pub struct MixedOrder(Vec<u8>);

impl ByteOrd {
    // TODO this is just try_from
    pub fn try_new(xs: Vec<u8>) -> Result<Self, NewByteOrdError> {
        let n = xs.len();
        if xs.iter().unique().count() == n
            && !(1..=8).contains(&n)
            && xs.iter().min().is_some_and(|x| *x == 1)
            && xs.iter().max().is_some_and(|x| usize::from(*x) == n)
        {
            Ok(ByteOrd(xs.iter().map(|x| x - 1).collect()))
        } else {
            Err(NewByteOrdError)
        }
    }

    pub fn new_little4() -> Self {
        ByteOrd((0..4).collect())
    }

    // ASSUME this will always be 1-8 elements
    pub fn nbytes(&self) -> Bytes {
        Bytes(self.0.len() as u8)
    }

    pub fn as_endian(&self) -> Option<Endian> {
        let mut it = self.0.iter().map(|x| usize::from(*x));
        if it.by_ref().enumerate().all(|(i, x)| i == x) {
            Some(Endian::Little)
        } else if it.rev().enumerate().all(|(i, x)| i == x) {
            Some(Endian::Big)
        } else {
            None
        }
    }

    pub fn as_sized<const LEN: usize>(&self) -> Result<SizedByteOrd<LEN>, ByteOrdToSizedError> {
        let xs = &self.0;
        let n = xs.len();
        if n != LEN {
            Err(ByteOrdToSizedError {
                bytes: n,
                length: LEN,
            })
        } else if let Some(e) = self.as_endian() {
            Ok(SizedByteOrd::Endian(e))
        } else {
            Ok(SizedByteOrd::Order(xs[..].try_into().unwrap()))
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

    pub fn as_bytord(&self, n: Bytes) -> ByteOrd {
        let it = 0..(u8::from(n));
        let xs = match self {
            Endian::Big => it.rev().collect(),
            Endian::Little => it.collect(),
        };
        ByteOrd(xs)
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

    pub(crate) fn as_fixed(&self) -> Option<BitsOrChars> {
        match self {
            Width::Fixed(x) => Some(*x),
            _ => None,
        }
    }

    pub(crate) fn as_chars(&self) -> Result<Chars, WidthToCharsError> {
        let fixed = self.as_fixed().ok_or(WidthToFixedError::Variable)?;
        fixed.chars().map_err(WidthToFixedError::Fixed)
    }

    pub(crate) fn as_bytes(&self) -> Result<Bytes, WidthToBytesError> {
        let fixed = self.as_fixed().ok_or(WidthToFixedError::Variable)?;
        fixed.bytes().map_err(WidthToFixedError::Fixed)
    }

    /// Given a list of widths and a type, return the byte-width for a matrix.
    ///
    /// That is, only return Some if the widths are all the same and they
    /// match the given type.
    ///
    /// If type is Ascii, automatically return 4 bytes,
    /// which is an arbitrary default since Ascii data does not care about
    /// $BYTEORD.
    ///
    /// If type is Integer, returned number can be 1-8.
    ///
    /// If type is Float or Double, return number must be 4 or 8 respectively.
    pub(crate) fn matrix_bytes(ms: &[Self], t: AlphaNumType) -> Option<Bytes> {
        // TODO handle errors better here?
        let sizes: Vec<_> = ms
            .iter()
            .map(|w| w.as_fixed().and_then(|x| x.bytes().ok()))
            .unique()
            .collect();
        match t {
            AlphaNumType::Ascii => Some(Bytes(4)),
            AlphaNumType::Integer => match sizes[..] {
                [Some(bytes)] => Some(bytes),
                _ => None,
            },
            AlphaNumType::Single => match sizes[..] {
                [Some(bytes)] => {
                    if u8::from(bytes) == 4 {
                        Some(bytes)
                    } else {
                        None
                    }
                }
                _ => None,
            },
            AlphaNumType::Double => match sizes[..] {
                [Some(bytes)] => {
                    if u8::from(bytes) == 8 {
                        Some(bytes)
                    } else {
                        None
                    }
                }
                _ => None,
            },
        }
    }
}

impl BitsOrChars {
    /// Return the number of chars represented by this if 20 or less.
    ///
    /// We can only check if the inner value is <=64 since we don't know at
    /// parse time if this is for an ASCII column or a numeric column, so
    /// need to check that the number is less then 20, which is the maximum
    /// number of digits that can be stored in a u64.
    pub fn chars(&self) -> Result<Chars, CharsError> {
        if self.0 > 20 {
            Err(CharsError(self.0))
        } else {
            Ok(Chars(self.0))
        }
    }

    /// Return number of bytes represented by this.
    ///
    /// Return None if not divisible by 8. This should only return a number
    /// in [1, 8] because we check that Self is within [1,64]
    pub fn bytes(&self) -> Result<Bytes, BytesError> {
        if self.0 % 8 == 0 {
            Ok(Bytes(self.0 / 8))
        } else {
            Err(BytesError(self.0))
        }
    }

    pub fn inner(&self) -> u8 {
        self.0
    }
}

impl FromStr for Endian {
    type Err = NewEndianError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "1,2,3,4" => Ok(Endian::Little),
            "4,3,2,1" => Ok(Endian::Big),
            _ => Err(NewEndianError),
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
        let (pass, fail): (Vec<_>, Vec<_>) =
            s.split(",").map(|x| x.parse::<u8>()).partition_result();
        if fail.is_empty() {
            ByteOrd::try_new(pass).map_err(ParseByteOrdError::Order)
        } else {
            Err(ParseByteOrdError::Format)
        }
    }
}

impl fmt::Display for ByteOrd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.iter().join(","))
    }
}

newtype_disp!(Bytes);
newtype_disp!(Chars);
newtype_from_outer!(Bytes, u8);
newtype_from_outer!(Chars, u8);
newtype_from_outer!(BitsOrChars, u8);

impl From<Option<u8>> for Width {
    fn from(value: Option<u8>) -> Self {
        value
            .map(|x| Width::Fixed(BitsOrChars(x)))
            .unwrap_or(Width::Variable)
    }
}

impl From<Width> for Option<u8> {
    fn from(value: Width) -> Self {
        match value {
            Width::Variable => None,
            Width::Fixed(x) => Some(x.0),
        }
    }
}

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
                // TODO this isn't necessary, we can check this when converting
                // to bytes or chars later
                if !(1..=64).contains(&x) {
                    Err(ParseBitsError::Inner(BitsError(x)))
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

impl<const LEN: usize> From<Endian> for SizedByteOrd<LEN> {
    fn from(value: Endian) -> Self {
        SizedByteOrd::Endian(value)
    }
}

impl TryFrom<ByteOrd> for Endian {
    type Error = EndianToByteOrdError;

    fn try_from(value: ByteOrd) -> Result<Self, Self::Error> {
        value.as_endian().ok_or(EndianToByteOrdError)
    }
}

pub enum ParseBitsError {
    Int(ParseIntError),
    Inner(BitsError),
}

pub struct BitsError(u8);

pub enum ParseByteOrdError {
    Order(NewByteOrdError),
    Format,
}

pub struct NewByteOrdError;

pub struct NewEndianError;

pub struct CharsError(u8);

pub struct BytesError(u8);

pub struct EndianToByteOrdError;

pub struct ByteOrdToEndianError(Vec<u8>);

pub struct ByteOrdToSizedError {
    bytes: usize,
    length: usize,
}

pub type WidthToCharsError = WidthToFixedError<CharsError>;

pub type WidthToBytesError = WidthToFixedError<BytesError>;

pub enum WidthToFixedError<X> {
    Variable,
    Fixed(X),
}

impl fmt::Display for ParseByteOrdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ParseByteOrdError::Format => write!(f, "Could not parse numbers in byte order"),
            ParseByteOrdError::Order(x) => x.fmt(f),
        }
    }
}

impl fmt::Display for NewByteOrdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Byte order must include 1-n uniquely")
    }
}

impl fmt::Display for ByteOrdToEndianError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "Byte order must be in order (ascending or descending) to convert \
             to endian, got {}",
            self.0.iter().join(",")
        )
    }
}

impl fmt::Display for EndianToByteOrdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "could not convert ByteOrd, must be either '1,2,3,4' or '4,3,2,1'",
        )
    }
}

impl fmt::Display for ByteOrdToSizedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "$BYTEORD is {} bytes, expected {}",
            self.bytes, self.length
        )
    }
}

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
        write!(f, "bits must be between 1 and 64, got {}", self.0)
    }
}

impl fmt::Display for CharsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "bits must be <= 20 to use as number of characters, got {}",
            self.0
        )
    }
}

impl fmt::Display for BytesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "bits must be multiple of 8 and between 8 and 64 \
             to used as byte width, got {}",
            self.0
        )
    }
}

impl fmt::Display for NewEndianError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Endian must be either 1,2,3,4 or 4,3,2,1")
    }
}

impl<E> fmt::Display for WidthToFixedError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Variable => write!(f, "width is variable were fixed is needed"),
            Self::Fixed(e) => write!(f, "error when converting fixed bits: {e}"),
        }
    }
}
