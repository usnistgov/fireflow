use crate::data::{MultiWidthsError, WrongFloatWidth};
use crate::error::*;
use crate::macros::{
    enum_from, enum_from_disp, match_many_to_one, newtype_disp, newtype_from_outer,
};

use super::keywords::AlphaNumType;

use itertools::Itertools;
use nonempty::NonEmpty;
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

/// The value of $PnB if it is fixed.
///
/// Subsequent operations can be used to use it as "bytes" or "characters"
/// depending on what is needed by the column.
#[derive(Clone, Copy, Serialize, PartialEq, Eq, Hash)]
pub struct BitsOrChars(u8);

/// $BYTEORD (ordered) with known size in bytes
#[derive(PartialEq, Eq, Hash, Copy, Clone)]
pub enum SizedByteOrd<const LEN: usize> {
    Endian(Endian),
    Order([u8; LEN]),
}

/// $BYTEORD (endian) with known size in bytes
#[derive(PartialEq, Eq, Hash, Copy, Clone, Serialize)]
pub struct SizedEndian<const LEN: usize>(pub Endian);

impl<const LEN: usize> From<SizedEndian<LEN>> for SizedByteOrd<LEN> {
    fn from(value: SizedEndian<LEN>) -> Self {
        SizedByteOrd::Endian(value.0)
    }
}

impl<const LEN: usize> TryFrom<SizedByteOrd<LEN>> for SizedEndian<LEN> {
    type Error = OrderedToEndianError;
    fn try_from(value: SizedByteOrd<LEN>) -> Result<Self, Self::Error> {
        match value {
            SizedByteOrd::Endian(x) => Ok(Self(x)),
            SizedByteOrd::Order(_) => Err(OrderedToEndianError),
        }
    }
}

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

impl TryFrom<Vec<u8>> for ByteOrd {
    type Error = NewByteOrdError;
    fn try_from(xs: Vec<u8>) -> Result<Self, Self::Error> {
        let n = xs.len();
        if xs.iter().unique().count() == n
            && (1..=8).contains(&n)
            && xs.iter().min().is_some_and(|x| *x == 1)
            && xs.iter().max().is_some_and(|x| usize::from(*x) == n)
        {
            Ok(ByteOrd(xs.iter().map(|x| x - 1).collect()))
        } else {
            Err(NewByteOrdError(n))
        }
    }
}

impl ByteOrd {
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

    pub fn as_sized_byteord<const LEN: usize>(
        &self,
    ) -> Result<SizedByteOrd<LEN>, ByteOrdToSizedError> {
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

    pub fn as_sized_endian<const LEN: usize>(
        &self,
    ) -> Result<SizedEndian<LEN>, ByteOrdToSizedEndianError> {
        self.as_sized_byteord().map_or_else(
            |e| Err(e.into()),
            |x: SizedByteOrd<LEN>| match x {
                SizedByteOrd::Endian(e) => Ok(SizedEndian(e)),
                _ => Err(OrderedToEndianError.into()),
            },
        )
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

impl TryFrom<Width> for Chars {
    type Error = WidthToCharsError;
    fn try_from(value: Width) -> Result<Self, Self::Error> {
        let fixed = BitsOrChars::try_from(value)
            .ok()
            .ok_or(WidthToFixedError::Variable)?;
        fixed.try_into().map_err(WidthToFixedError::Fixed)
    }
}

impl TryFrom<Width> for Bytes {
    type Error = WidthToBytesError;
    fn try_from(value: Width) -> Result<Self, Self::Error> {
        let fixed = BitsOrChars::try_from(value)
            .ok()
            .ok_or(WidthToFixedError::Variable)?;
        fixed.try_into().map_err(WidthToFixedError::Fixed)
    }
}

impl TryFrom<Width> for BitsOrChars {
    type Error = ();
    fn try_from(value: Width) -> Result<Self, Self::Error> {
        match value {
            Width::Fixed(x) => Ok(x),
            _ => Err(()),
        }
    }
}

impl TryFrom<BitsOrChars> for Chars {
    type Error = CharsError;
    /// Return the number of chars represented by this if 20 or less.
    ///
    /// 20 is the maximum number of digits representable by an unsigned integer,
    /// which is the numeric type used to back ASCII data.
    fn try_from(value: BitsOrChars) -> Result<Self, Self::Error> {
        let x = value.0;
        if !(1..=20).contains(&x) {
            Err(CharsError(x))
        } else {
            Ok(Chars(x))
        }
    }
}

impl TryFrom<BitsOrChars> for Bytes {
    type Error = BytesError;
    /// Return number of bytes represented by this.
    ///
    /// Return error if bits is not divisible by 8 and within [1,64].
    fn try_from(value: BitsOrChars) -> Result<Self, Self::Error> {
        let x = value.0;
        if (1..=64).contains(&x) && x % 8 == 0 {
            Ok(Bytes(x / 8))
        } else {
            Err(BytesError(x))
        }
    }
}

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

impl Width {
    pub fn new_f32() -> Self {
        Width::Fixed(BitsOrChars(32))
    }

    pub fn new_f64() -> Self {
        Width::Fixed(BitsOrChars(64))
    }

    /// Given a list of widths and a type, return the byte-width for a matrix.
    ///
    /// That is, only return Ok if the widths are all the same and they
    /// match the given type.
    ///
    /// If type is Ascii, automatically return 4 bytes,
    /// which is an arbitrary default since Ascii data does not care about
    /// $BYTEORD.
    ///
    /// If type is Integer, returned number can be 1-8.
    ///
    /// If type is Float or Double, return number must be 4 or 8 respectively.
    pub(crate) fn matrix_bytes(
        widths: &[Self],
        t: AlphaNumType,
    ) -> DeferredResult<Bytes, WidthToBytesError, SingleWidthError> {
        if let Some(ws) = NonEmpty::collect(widths.iter().copied()) {
            let bs = ne_map_results(ws, Bytes::try_from).mult_to_deferred();

            let go = |sizes: NonEmpty<_>, expected: usize| {
                if sizes.tail.is_empty() {
                    let bytes = sizes.head;
                    if usize::from(u8::from(bytes)) == expected {
                        Ok(bytes)
                    } else {
                        Err(WrongFloatWidth {
                            width: bytes,
                            expected,
                        }
                        .into())
                    }
                } else {
                    Err(MultiWidthsError(sizes).into())
                }
            };

            match t {
                AlphaNumType::Ascii => {
                    let bytes = Bytes(4);
                    let ret = bs.map_or_else(|e| e.unfail_with(bytes), |_| Tentative::new1(bytes));
                    Ok(ret)
                }
                AlphaNumType::Integer => bs.def_and_then(|sizes| {
                    if sizes.tail.is_empty() {
                        Ok(sizes.head)
                    } else {
                        Err(MultiWidthsError(sizes).into())
                    }
                }),
                AlphaNumType::Single => bs.def_and_then(|sizes| go(sizes, 4)),
                AlphaNumType::Double => bs.def_and_then(|sizes| go(sizes, 8)),
            }
        } else {
            Err(DeferredFailure::new1(EmptyWidthError.into()))
        }
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
            ByteOrd::try_from(pass).map_err(ParseByteOrdError::Order)
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

impl FromStr for Width {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "*" => Ok(Width::Variable),
            _ => s.parse::<u8>().map(|x| Width::Fixed(BitsOrChars(x))),
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

newtype_disp!(Bytes);
newtype_disp!(Chars);
newtype_from_outer!(Bytes, u8);
newtype_from_outer!(Chars, u8);
newtype_from_outer!(BitsOrChars, u8);

pub struct BitsError(u8);

pub enum ParseByteOrdError {
    Order(NewByteOrdError),
    Format,
}

pub struct NewByteOrdError(usize);

pub struct NewEndianError;

pub struct CharsError(u8);

pub struct BytesError(u8);

pub struct EndianToByteOrdError;

pub struct ByteOrdToEndianError(Vec<u8>);

enum_from_disp!(
    pub ByteOrdToSizedEndianError,
    [Ordered, OrderedToEndianError],
    [ToSized, ByteOrdToSizedError]
);

pub struct OrderedToEndianError;

impl fmt::Display for OrderedToEndianError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str("byte order is not monotonic")
    }
}

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
            Self::Format => write!(f, "Could not parse numbers in byte order"),
            Self::Order(x) => x.fmt(f),
        }
    }
}

impl fmt::Display for NewByteOrdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Byte order must include 1-{} uniquely", self.0)
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
             to be used as byte width, got {}",
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

enum_from_disp!(
    pub SingleWidthError,
    // TODO put this error somewhere more obvious
    [Multi, MultiWidthsError],
    [Float, WrongFloatWidth],
    [Width, WidthToBytesError],
    [Empty, EmptyWidthError]

);

pub struct EmptyWidthError;

impl fmt::Display for EmptyWidthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not determine width, no measurements available")
    }
}
