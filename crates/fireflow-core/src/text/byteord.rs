use crate::macros::match_many_to_one;
use crate::validated::ascii_range::{Chars, CharsError};

use derive_more::{Display, From, FromStr, Into};
use itertools::Itertools;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::Serialize;
use std::fmt;
use std::num::NonZeroU8;
use std::num::ParseIntError;
use std::str::FromStr;

use super::parser::ReqMetarootKey;

/// The byte order as shown in the $BYTEORD field in 2.0 and 3.0
///
/// This must be a list of integers belonging to the unordered set {1..N} where
/// N is the total number of bytes. The numbers will be stored as one less the
/// displayed integers to make array indexing easier.
#[derive(Clone, Copy, Serialize, From, Display)]
pub enum ByteOrd2_0 {
    O1(SizedByteOrd<1>),
    O2(SizedByteOrd<2>),
    O3(SizedByteOrd<3>),
    O4(SizedByteOrd<4>),
    O5(SizedByteOrd<5>),
    O6(SizedByteOrd<6>),
    O7(SizedByteOrd<7>),
    O8(SizedByteOrd<8>),
}

#[derive(Clone, Copy, Serialize, From, Display, FromStr, Default)]
pub struct ByteOrd3_1(pub Endian);

/// Endianness
///
/// This is also stored in the $BYTEORD key in 3.1+
#[derive(Clone, Copy, Serialize, PartialEq, Eq, Hash, Default)]
pub enum Endian {
    Big,
    #[default]
    Little,
}

/// Marker type representing lack of byte order.
///
/// This is used in ASCII layouts, for which $BYTEORD is meaningless.
#[derive(Clone, Copy, Serialize)]
pub struct NoByteOrd<const ORD: bool>;

pub type NoByteOrd2_0 = NoByteOrd<true>;

pub type NoByteOrd3_1 = NoByteOrd<false>;

/// The value for the $PnB key (all versions)
///
/// The $PnB key actually stores bits. However, this library only supports
/// widths that are multiples of 8 (ie bytes) for now. Therefore, this key
/// actually stores the number of bytes indicated by $PnB.
///
/// This may also be '*' which means "delimited ASCII" which is only valid when
/// $DATATYPE=A.
#[derive(Clone, Copy, Serialize, PartialEq, Eq, Hash, From)]
#[from(Chars)]
pub enum Width {
    Fixed(BitsOrChars),
    Variable,
}

/// The number of bytes for a numeric measurement
#[derive(Clone, Copy, Serialize, PartialEq, Eq, Hash, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
pub enum Bytes {
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
#[derive(Clone, Copy, Serialize, PartialEq, Eq, Hash, From, Into)]
#[from(Chars)]
#[into(NonZeroU8, u8)]
pub struct BitsOrChars(NonZeroU8);

/// $BYTEORD (ordered) with known size in bytes
#[derive(PartialEq, Eq, Hash, Copy, Clone, From)]
pub enum SizedByteOrd<const LEN: usize> {
    #[from]
    Endian(Endian),
    Order([u8; LEN]),
}

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
                    Err(ByteOrdToSizedError {
                        bytes: value.nbytes(),
                        length: $len,
                    })
                }
            }
        }

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
                        let mut o = std::array::from_fn(|i| i as u8);
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
            pub(crate) fn nbytes() -> Bytes {
                Bytes::$bytes
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

impl Bytes {
    /// Return number of bytes needed to express the given u64.
    pub(crate) fn from_u64(x: u64) -> Self {
        // find position of most-significant non-zero byte
        x.to_le_bytes()
            .iter()
            .rposition(|i| *i > 0)
            .and_then(|i| u8::try_from(i + 1).ok())
            .and_then(|i| Bytes::try_from(i).ok())
            .unwrap_or(Bytes::B1)
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

impl Default for ByteOrd2_0 {
    fn default() -> Self {
        Self::O4(SizedByteOrd::default())
    }
}

impl From<NoByteOrd<true>> for ByteOrd2_0 {
    fn from(_: NoByteOrd<true>) -> Self {
        Self::default()
    }
}

impl From<NoByteOrd<false>> for ByteOrd3_1 {
    fn from(_: NoByteOrd<false>) -> Self {
        Self::default()
    }
}

impl ByteOrd2_0 {
    pub fn nbytes(&self) -> Bytes {
        match self {
            Self::O1(_) => SizedByteOrd::<1>::nbytes(),
            Self::O2(_) => SizedByteOrd::<2>::nbytes(),
            Self::O3(_) => SizedByteOrd::<3>::nbytes(),
            Self::O4(_) => SizedByteOrd::<4>::nbytes(),
            Self::O5(_) => SizedByteOrd::<5>::nbytes(),
            Self::O6(_) => SizedByteOrd::<6>::nbytes(),
            Self::O7(_) => SizedByteOrd::<7>::nbytes(),
            Self::O8(_) => SizedByteOrd::<8>::nbytes(),
        }
    }

    pub fn as_vec(&self) -> Vec<NonZeroU8> {
        match self {
            Self::O1(x) => <[NonZeroU8; 1]>::from(*x).to_vec(),
            Self::O2(x) => <[NonZeroU8; 2]>::from(*x).to_vec(),
            Self::O3(x) => <[NonZeroU8; 3]>::from(*x).to_vec(),
            Self::O4(x) => <[NonZeroU8; 4]>::from(*x).to_vec(),
            Self::O5(x) => <[NonZeroU8; 5]>::from(*x).to_vec(),
            Self::O6(x) => <[NonZeroU8; 6]>::from(*x).to_vec(),
            Self::O7(x) => <[NonZeroU8; 7]>::from(*x).to_vec(),
            Self::O8(x) => <[NonZeroU8; 8]>::from(*x).to_vec(),
        }
    }
}

impl From<bool> for Endian {
    fn from(value: bool) -> Self {
        if value {
            Self::Big
        } else {
            Self::Little
        }
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
    fn try_from(value: BitsOrChars) -> Result<Self, Self::Error> {
        NonZeroU8::from(value).try_into()
    }
}

impl TryFrom<BitsOrChars> for Bytes {
    type Error = BytesError;
    /// Return number of bytes represented by this.
    ///
    /// Return error if bits is not divisible by 8 and within [1,64].
    fn try_from(value: BitsOrChars) -> Result<Self, Self::Error> {
        let x = u8::from(value.0);
        if (x & 0b111) == 0 {
            return (x >> 3).try_into().or(Err(BytesError(x)));
        }
        Err(BytesError(x))
    }
}

impl From<Bytes> for NonZeroU8 {
    fn from(value: Bytes) -> NonZeroU8 {
        // ASSUME this will never fail
        Self::new(u8::from(value)).unwrap()
    }
}

impl From<Bytes> for BitsOrChars {
    fn from(value: Bytes) -> BitsOrChars {
        // ASSUME this will never fail
        Self(NonZeroU8::new(u8::from(value) * 8).unwrap())
    }
}

impl From<Option<NonZeroU8>> for Width {
    fn from(value: Option<NonZeroU8>) -> Self {
        value
            .map(|x| Width::Fixed(BitsOrChars(x)))
            .unwrap_or(Width::Variable)
    }
}

impl From<Width> for Option<NonZeroU8> {
    fn from(value: Width) -> Self {
        match value {
            Width::Variable => None,
            Width::Fixed(x) => Some(x.0),
        }
    }
}

impl TryFrom<Width> for NonZeroU8 {
    type Error = ();
    fn try_from(value: Width) -> Result<Self, Self::Error> {
        match value {
            Width::Fixed(x) => Ok(x.0),
            _ => Err(()),
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

impl FromStr for ByteOrd2_0 {
    type Err = ParseByteOrdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (pass, fail): (Vec<_>, Vec<_>) = s
            .split(",")
            .map(|x| x.parse::<NonZeroU8>())
            .partition_result();
        if fail.is_empty() {
            ByteOrd2_0::try_from(&pass[..]).map_err(ParseByteOrdError::Order)
        } else {
            Err(ParseByteOrdError::Format)
        }
    }
}

impl<const LEN: usize> fmt::Display for SizedByteOrd<LEN> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Endian(e) => e.fmt(f),
            Self::Order(o) => write!(f, "{}", o.iter().map(|x| *x + 1).join(",")),
        }
    }
}

impl FromStr for Width {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "*" => Ok(Width::Variable),
            _ => s.parse::<NonZeroU8>().map(|x| Width::Fixed(BitsOrChars(x))),
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

pub struct BitsError(u8);

pub enum ParseByteOrdError {
    Order(NewByteOrdError),
    Format,
}

pub struct NewByteOrdError(usize);

pub struct NewEndianError;

pub struct BytesError(u8);

pub struct EndianToByteOrdError;

pub struct ByteOrdToEndianError(Vec<u8>);

#[derive(From, Display)]
pub enum ByteOrdToSizedEndianError {
    Ordered(OrderedToEndianError),
    ToSized(ByteOrdToSizedError),
}

pub struct OrderedToEndianError;

impl fmt::Display for OrderedToEndianError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_str("byte order is not monotonic")
    }
}

pub struct ByteOrdToSizedError {
    bytes: Bytes,
    length: usize,
}

#[derive(From, Display)]
pub enum VecToSizedError {
    Vec(VecToArrayError),
    New(NewByteOrdError),
}

pub struct VecToArrayError {
    vec_len: usize,
    req_len: usize,
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

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        u8::from(*self).fmt(f)
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

impl fmt::Display for VecToArrayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "could not convert vector to array, was {} long, needed {}",
            self.vec_len, self.req_len
        )
    }
}
