use crate::macros::{enum_from, enum_from_disp, match_many_to_one, newtype_from_outer};
use crate::validated::ascii_range::{Chars, CharsError};

use itertools::Itertools;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::Serialize;
use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

/// Endianness
///
/// This is also stored in the $BYTEORD key in 3.1+
#[derive(Clone, Copy, Serialize, PartialEq, Eq, Hash, Default)]
pub enum Endian {
    Big,
    #[default]
    Little,
}

enum_from_disp!(
    /// The byte order as shown in the $BYTEORD field in 2.0 and 3.0
    ///
    /// This must be a list of integers belonging to the unordered set {1..N} where
    /// N is the total number of bytes. The numbers will be stored as one less the
    /// displayed integers to make array indexing easier.
    #[derive(Clone, Copy, Serialize)]
    pub ByteOrd,
    [O1, SizedByteOrd<1>],
    [O2, SizedByteOrd<2>],
    [O3, SizedByteOrd<3>],
    [O4, SizedByteOrd<4>],
    [O5, SizedByteOrd<5>],
    [O6, SizedByteOrd<6>],
    [O7, SizedByteOrd<7>],
    [O8, SizedByteOrd<8>]
);

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
#[derive(Clone, Copy, Serialize, PartialEq, Eq, Hash)]
pub struct BitsOrChars(u8);

/// $BYTEORD (ordered) with known size in bytes
#[derive(PartialEq, Eq, Hash, Copy, Clone)]
pub enum SizedByteOrd<const LEN: usize> {
    Endian(Endian),
    Order([u8; LEN]),
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

        impl TryFrom<ByteOrd> for SizedByteOrd<$len> {
            type Error = ByteOrdToSizedError;
            fn try_from(value: ByteOrd) -> Result<Self, Self::Error> {
                if let ByteOrd::$var(sized) = value {
                    Ok(sized)
                } else {
                    Err(ByteOrdToSizedError {
                        bytes: value.nbytes(),
                        length: $len,
                    })
                }
            }
        }

        impl TryFrom<Vec<u8>> for SizedByteOrd<$len> {
            type Error = VecToSizedError;
            fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
                let xs: [u8; $len] = value.try_into().map_err(|ys: Vec<_>| VecToArrayError {
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
        impl TryFrom<[u8; $len]> for SizedByteOrd<$len> {
            type Error = NewByteOrdError;
            fn try_from(xs: [u8; $len]) -> Result<Self, Self::Error> {
                let mut flags = [false; $len];
                // Try to subtract one from each number. While doing so, track
                // which numbers were seen by setting flags in an array where
                // each index corresponds to the number we wish to see. If all
                // are true, then each number is present.
                let ys = xs.map(|x| {
                    if let Some(y) = x.checked_sub(1) {
                        if y < $len {
                            flags[usize::from(y)] = true;
                        }
                        y
                    } else {
                        0
                    }
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

        impl From<SizedByteOrd<$len>> for [u8; $len] {
            fn from(value: SizedByteOrd<$len>) -> [u8; $len] {
                match value {
                    SizedByteOrd::Endian(e) => {
                        let mut o = std::array::from_fn(|i| i as u8);
                        if e == Endian::Big {
                            o.reverse();
                        };
                        o
                    }
                    SizedByteOrd::Order(o) => o,
                }
            }
        }

        impl SizedByteOrd<$len> {
            pub(crate) fn nbytes() -> Bytes {
                Bytes::$bytes
            }
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

impl TryFrom<&[u8]> for ByteOrd {
    type Error = NewByteOrdError;
    fn try_from(xs: &[u8]) -> Result<Self, Self::Error> {
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

impl Default for ByteOrd {
    fn default() -> Self {
        Self::O4(SizedByteOrd::default())
    }
}

impl ByteOrd {
    // pub fn as_endian(&self) -> Option<Endian> {
    //     let mut it = self.as_slice().iter().map(|x| usize::from(*x));
    //     if it.by_ref().enumerate().all(|(i, x)| i == x) {
    //         Some(Endian::Little)
    //     } else if it.rev().enumerate().all(|(i, x)| i == x) {
    //         Some(Endian::Big)
    //     } else {
    //         None
    //     }
    // }

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

    pub fn as_vec(&self) -> Vec<u8> {
        match self {
            Self::O1(x) => <[u8; 1]>::from(*x).to_vec(),
            Self::O2(x) => <[u8; 2]>::from(*x).to_vec(),
            Self::O3(x) => <[u8; 3]>::from(*x).to_vec(),
            Self::O4(x) => <[u8; 4]>::from(*x).to_vec(),
            Self::O5(x) => <[u8; 5]>::from(*x).to_vec(),
            Self::O6(x) => <[u8; 6]>::from(*x).to_vec(),
            Self::O7(x) => <[u8; 7]>::from(*x).to_vec(),
            Self::O8(x) => <[u8; 8]>::from(*x).to_vec(),
        }
    }

    // pub fn as_sized_endian<const LEN: usize>(
    //     &self,
    // ) -> Result<SizedEndian<LEN>, ByteOrdToSizedEndianError> {
    //     self.as_sized_byteord().map_or_else(
    //         |e| Err(e.into()),
    //         |x: SizedByteOrd<LEN>| match x {
    //             SizedByteOrd::Endian(e) => Ok(SizedEndian(e)),
    //             _ => Err(OrderedToEndianError.into()),
    //         },
    //     )
    // }
}

impl Endian {
    pub fn is_big(x: bool) -> Self {
        if x {
            Endian::Big
        } else {
            Endian::Little
        }
    }

    // pub fn as_byteord(&self, n: Bytes) -> ByteOrd {
    //     let it = 0..(u8::from(n));
    //     let xs = match self {
    //         Endian::Big => it.rev().collect(),
    //         Endian::Little => it.collect(),
    //     };
    //     ByteOrd(xs)
    // }
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
        u8::from(value).try_into()
    }
}

impl From<Chars> for BitsOrChars {
    fn from(value: Chars) -> Self {
        BitsOrChars(u8::from(value))
    }
}

impl TryFrom<BitsOrChars> for Bytes {
    type Error = BytesError;
    /// Return number of bytes represented by this.
    ///
    /// Return error if bits is not divisible by 8 and within [1,64].
    fn try_from(value: BitsOrChars) -> Result<Self, Self::Error> {
        let x = value.0;
        if (x & 0b111) == 0 {
            return (x >> 3).try_into().or(Err(BytesError(x)));
        }
        Err(BytesError(x))
    }
}

impl From<Bytes> for BitsOrChars {
    fn from(value: Bytes) -> Self {
        BitsOrChars(value.into())
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
        Width::Fixed(BitsOrChars(value.into()))
    }
}

impl From<Bytes> for Width {
    fn from(value: Bytes) -> Self {
        Width::Fixed(BitsOrChars(u8::from(value) * 8))
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
    type Error = OrderedToEndianError;

    fn try_from(value: ByteOrd) -> Result<Self, Self::Error> {
        match_many_to_one!(value, ByteOrd, [O1, O2, O3, O4, O5, O6, O7, O8], x, {
            x.try_into()
        })
    }
}

impl Width {
    pub fn new_f32() -> Self {
        Width::Fixed(BitsOrChars(32))
    }

    pub fn new_f64() -> Self {
        Width::Fixed(BitsOrChars(64))
    }

    // /// Given a list of widths and a type, return the byte-width for a matrix.
    // ///
    // /// That is, only return Ok if the widths are all the same and they
    // /// match the given type.
    // ///
    // /// If type is Ascii, automatically return 4 bytes,
    // /// which is an arbitrary default since Ascii data does not care about
    // /// $BYTEORD.
    // ///
    // /// If type is Integer, returned number can be 1-8.
    // ///
    // /// If type is Float or Double, return number must be 4 or 8 respectively.
    // pub(crate) fn matrix_bytes(
    //     widths: &[Self],
    //     t: AlphaNumType,
    // ) -> DeferredResult<Bytes, WidthToBytesError, SingleWidthError> {
    //     if let Some(ws) = NonEmpty::collect(widths.iter().copied()) {
    //         let bs = ne_map_results(ws, Bytes::try_from).mult_to_deferred();

    //         let go = |sizes: NonEmpty<_>, expected: usize| {
    //             if sizes.tail.is_empty() {
    //                 let bytes = sizes.head;
    //                 if usize::from(u8::from(bytes)) == expected {
    //                     Ok(bytes)
    //                 } else {
    //                     Err(WrongFloatWidth {
    //                         width: bytes,
    //                         expected,
    //                     }
    //                     .into())
    //                 }
    //             } else {
    //                 Err(MultiWidthsError(sizes.map(|x| x.into())).into())
    //             }
    //         };

    //         match t {
    //             AlphaNumType::Ascii => {
    //                 let bytes = Bytes(4);
    //                 let ret = bs.map_or_else(|e| e.unfail_with(bytes), |_| Tentative::new1(bytes));
    //                 Ok(ret)
    //             }
    //             AlphaNumType::Integer => bs.def_and_then(|sizes| {
    //                 if sizes.tail.is_empty() {
    //                     Ok(sizes.head)
    //                 } else {
    //                     Err(MultiWidthsError(sizes.map(|x| x.into())).into())
    //                 }
    //             }),
    //             AlphaNumType::Single => bs.def_and_then(|sizes| go(sizes, 4)),
    //             AlphaNumType::Double => bs.def_and_then(|sizes| go(sizes, 8)),
    //         }
    //     } else {
    //         Err(DeferredFailure::new1(EmptyWidthError.into()))
    //     }
    // }
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
            ByteOrd::try_from(&pass[..]).map_err(ParseByteOrdError::Order)
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

newtype_from_outer!(BitsOrChars, u8);

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
    bytes: Bytes,
    length: usize,
}

enum_from_disp!(
    pub VecToSizedError,
    [Vec, VecToArrayError],
    [New, NewByteOrdError]
);

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
