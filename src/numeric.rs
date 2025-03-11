use std::io;
use std::io::{BufReader, Read, Seek};

#[derive(Debug, Clone, Copy)]
pub enum Endian {
    Big,
    Little,
}

impl Endian {
    fn is_big(&self) -> bool {
        matches!(self, Endian::Big)
    }
}

pub enum Series {
    F32(Vec<f32>),
    F64(Vec<f64>),
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
}

impl Series {
    pub fn len(&self) -> usize {
        match self {
            Series::F32(x) => x.len(),
            Series::F64(x) => x.len(),
            Series::U8(x) => x.len(),
            Series::U16(x) => x.len(),
            Series::U32(x) => x.len(),
            Series::U64(x) => x.len(),
        }
    }

    pub fn format(&self, r: usize) -> String {
        match self {
            Series::F32(x) => format!("{}", x[r]),
            Series::F64(x) => format!("{}", x[r]),
            Series::U8(x) => format!("{}", x[r]),
            Series::U16(x) => format!("{}", x[r]),
            Series::U32(x) => format!("{}", x[r]),
            Series::U64(x) => format!("{}", x[r]),
        }
    }
}

pub trait IntMath: Sized {
    fn next_power_2(x: Self) -> Self;

    fn from_u64(x: u64) -> Self;
}

pub trait NumProps<const DTLEN: usize>: Sized + Copy {
    fn to_series(x: Vec<Self>) -> Series;

    fn zero() -> Self;

    fn from_big(buf: [u8; DTLEN]) -> Self;

    fn from_little(buf: [u8; DTLEN]) -> Self;

    fn read_from_big<R: Read + Seek>(h: &mut BufReader<R>) -> io::Result<Self> {
        let mut buf = [0; DTLEN];
        h.read_exact(&mut buf)?;
        Ok(Self::from_big(buf))
    }

    fn read_from_little<R: Read + Seek>(h: &mut BufReader<R>) -> io::Result<Self> {
        let mut buf = [0; DTLEN];
        h.read_exact(&mut buf)?;
        Ok(Self::from_little(buf))
    }

    fn read_from_endian<R: Read + Seek>(h: &mut BufReader<R>, endian: Endian) -> io::Result<Self> {
        if endian.is_big() {
            Self::read_from_big(h)
        } else {
            Self::read_from_little(h)
        }
    }
}

impl IntMath for u8 {
    fn next_power_2(x: Self) -> Self {
        Self::checked_next_power_of_two(x).unwrap_or(Self::MAX)
    }

    fn from_u64(x: u64) -> Self {
        x as Self
    }
}

impl IntMath for u16 {
    fn next_power_2(x: Self) -> Self {
        Self::checked_next_power_of_two(x).unwrap_or(Self::MAX)
    }

    fn from_u64(x: u64) -> Self {
        x as Self
    }
}

impl IntMath for u32 {
    fn next_power_2(x: Self) -> Self {
        Self::checked_next_power_of_two(x).unwrap_or(Self::MAX)
    }

    fn from_u64(x: u64) -> Self {
        x as Self
    }
}

impl IntMath for u64 {
    fn next_power_2(x: Self) -> Self {
        Self::checked_next_power_of_two(x).unwrap_or(Self::MAX)
    }

    fn from_u64(x: u64) -> Self {
        x as Self
    }
}

impl NumProps<1> for u8 {
    fn to_series(x: Vec<Self>) -> Series {
        Series::U8(x)
    }

    fn zero() -> Self {
        0
    }

    fn from_big(buf: [u8; 1]) -> Self {
        Self::from_be_bytes(buf)
    }

    fn from_little(buf: [u8; 1]) -> Self {
        Self::from_le_bytes(buf)
    }
}

impl NumProps<2> for u16 {
    fn to_series(x: Vec<Self>) -> Series {
        Series::U16(x)
    }

    fn zero() -> Self {
        0
    }

    fn from_big(buf: [u8; 2]) -> Self {
        u16::from_be_bytes(buf)
    }

    fn from_little(buf: [u8; 2]) -> Self {
        u16::from_le_bytes(buf)
    }
}

impl NumProps<4> for u32 {
    fn to_series(x: Vec<Self>) -> Series {
        Series::U32(x)
    }

    fn zero() -> Self {
        0
    }

    fn from_big(buf: [u8; 4]) -> Self {
        u32::from_be_bytes(buf)
    }

    fn from_little(buf: [u8; 4]) -> Self {
        u32::from_le_bytes(buf)
    }
}

impl NumProps<8> for u64 {
    fn to_series(x: Vec<Self>) -> Series {
        Series::U64(x)
    }

    fn zero() -> Self {
        0
    }

    fn from_big(buf: [u8; 8]) -> Self {
        u64::from_be_bytes(buf)
    }

    fn from_little(buf: [u8; 8]) -> Self {
        u64::from_le_bytes(buf)
    }
}

impl NumProps<4> for f32 {
    fn to_series(x: Vec<Self>) -> Series {
        Series::F32(x)
    }

    fn zero() -> Self {
        0.0
    }

    fn from_big(buf: [u8; 4]) -> Self {
        f32::from_be_bytes(buf)
    }

    fn from_little(buf: [u8; 4]) -> Self {
        f32::from_le_bytes(buf)
    }
}

impl NumProps<8> for f64 {
    fn to_series(x: Vec<Self>) -> Series {
        Series::F64(x)
    }

    fn zero() -> Self {
        0.0
    }

    fn from_big(buf: [u8; 8]) -> Self {
        f64::from_be_bytes(buf)
    }

    fn from_little(buf: [u8; 8]) -> Self {
        f64::from_le_bytes(buf)
    }
}
