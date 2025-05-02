use itertools::Itertools;
use serde::Serialize;
use std::fmt;
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
                    || n > 8
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

    pub fn nbytes(&self) -> u8 {
        match self {
            ByteOrd::Endian(_) => 4,
            ByteOrd::Mixed(xs) => xs.0.len() as u8,
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
