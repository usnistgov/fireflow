use crate::keywords::*;
use crate::numeric::{Endian, IntMath, NumProps, Series};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use itertools::Itertools;
use regex::Regex;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fs;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::iter;
use std::num::{IntErrorKind, ParseFloatError, ParseIntError};
use std::path;
use std::str;
use std::str::FromStr;

fn format_measurement(n: &str, m: &str) -> String {
    format!("$P{}{}", n, m)
}

type ParseResult<T> = Result<T, String>;

#[derive(Debug, Clone, Serialize)]
struct FCSDateTime(DateTime<FixedOffset>);

struct FCSDateTimeError;

impl fmt::Display for FCSDateTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be formatted like 'yyyy-mm-ddThh:mm:ss[TZD]'")
    }
}

impl str::FromStr for FCSDateTime {
    type Err = FCSDateTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let formats = [
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S%.f%#z",
            "%Y-%m-%dT%H:%M:%S%.f%:z",
            "%Y-%m-%dT%H:%M:%S%.f%::z",
            "%Y-%m-%dT%H:%M:%S%.f%:::z",
        ];
        for f in formats {
            if let Ok(t) = DateTime::parse_from_str(s, f) {
                return Ok(FCSDateTime(t));
            }
        }
        Err(FCSDateTimeError)
    }
}

impl fmt::Display for FCSDateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.format("%Y-%m-%dT%H:%M:%S%.f%:z"))
    }
}

#[derive(Debug, Clone, Serialize)]
struct FCSTime(NaiveTime);

impl str::FromStr for FCSTime {
    type Err = FCSTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveTime::parse_from_str(s, "%H:%M:%S")
            .map(FCSTime)
            .or(Err(FCSTimeError))
    }
}

impl fmt::Display for FCSTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.format("%H:%M:%S"))
    }
}

struct FCSTimeError;

impl fmt::Display for FCSTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'hh:mm:ss'")
    }
}

#[derive(Debug, Clone, Serialize)]
struct FCSTime60(NaiveTime);

impl str::FromStr for FCSTime60 {
    type Err = FCSTime60Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveTime::parse_from_str(s, "%H:%M:%S")
            .or_else(|_| match s.split(":").collect::<Vec<_>>()[..] {
                [s1, s2, s3, s4] => {
                    let hh: u32 = s1.parse().or(Err(FCSTime60Error))?;
                    let mm: u32 = s2.parse().or(Err(FCSTime60Error))?;
                    let ss: u32 = s3.parse().or(Err(FCSTime60Error))?;
                    let tt: u32 = s4.parse().or(Err(FCSTime60Error))?;
                    let nn = tt * 1000000 / 60;
                    NaiveTime::from_hms_micro_opt(hh, mm, ss, nn).ok_or(FCSTime60Error)
                }
                _ => Err(FCSTime60Error),
            })
            .map(FCSTime60)
    }
}

impl fmt::Display for FCSTime60 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let base = self.0.format("%H:%M:%S");
        let cc = self.0.nanosecond() / 10000000 * 60;
        write!(f, "{}.{}", base, cc)
    }
}

struct FCSTime60Error;

impl fmt::Display for FCSTime60Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "must be like 'hh:mm:ss[:tt]' where 'tt' is in 1/60th seconds"
        )
    }
}

#[derive(Debug, Clone, Serialize)]
struct FCSTime100(NaiveTime);

impl str::FromStr for FCSTime100 {
    type Err = FCSTime100Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveTime::parse_from_str(s, "%H:%M:%S")
            .or_else(|_| {
                let re = Regex::new(r"(\d){2}:(\d){2}:(\d){2}.(\d){2}").unwrap();
                let cap = re.captures(s).ok_or(FCSTime100Error)?;
                let [s1, s2, s3, s4] = cap.extract().1;
                let hh: u32 = s1.parse().or(Err(FCSTime100Error))?;
                let mm: u32 = s2.parse().or(Err(FCSTime100Error))?;
                let ss: u32 = s3.parse().or(Err(FCSTime100Error))?;
                let tt: u32 = s4.parse().or(Err(FCSTime100Error))?;
                NaiveTime::from_hms_milli_opt(hh, mm, ss, tt * 10).ok_or(FCSTime100Error)
            })
            .map(FCSTime100)
    }
}

impl fmt::Display for FCSTime100 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let base = self.0.format("%H:%M:%S");
        let cc = self.0.nanosecond() / 10000000;
        write!(f, "{}.{}", base, cc)
    }
}

struct FCSTime100Error;

impl fmt::Display for FCSTime100Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'hh:mm:ss[.cc]'")
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
struct Offsets {
    begin: u32,
    end: u32,
}

impl Offsets {
    fn new(begin: u32, end: u32) -> Option<Offsets> {
        if begin > end {
            None
        } else {
            Some(Self { begin, end })
        }
    }

    fn adjust(&self, begin_delta: i32, end_delta: i32) -> Option<Offsets> {
        let x = i64::from(self.begin) + i64::from(begin_delta);
        let y = i64::from(self.end) + i64::from(end_delta);
        Self::new(u32::try_from(x).ok()?, u32::try_from(y).ok()?)
    }

    fn len(&self) -> u32 {
        self.end - self.begin
    }

    fn num_bytes(&self) -> u32 {
        self.len() + 1
    }

    // unset = both offsets are 0
    fn is_unset(&self) -> bool {
        self.begin == 0 && self.end == 0
    }
}

/// FCS version.
///
/// This appears as the first 6 bytes of any valid FCS file.
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, Serialize)]
enum Version {
    FCS2_0,
    FCS3_0,
    FCS3_1,
    FCS3_2,
}

impl str::FromStr for Version {
    type Err = VersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "FCS2.0" => Ok(Version::FCS2_0),
            "FCS3.0" => Ok(Version::FCS3_0),
            "FCS3.1" => Ok(Version::FCS3_1),
            "FCS3.2" => Ok(Version::FCS3_2),
            _ => Err(VersionError),
        }
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Version::FCS2_0 => write!(f, "FCS2.0"),
            Version::FCS3_0 => write!(f, "FCS3.0"),
            Version::FCS3_1 => write!(f, "FCS3.1"),
            Version::FCS3_2 => write!(f, "FCS3.2"),
        }
    }
}

struct VersionError;

/// Data contained in the FCS header.
#[derive(Debug, Clone, Serialize)]
pub struct Header {
    version: Version,
    text: Offsets,
    data: Offsets,
    analysis: Offsets,
}

/// The four allowed datatypes for FCS data.
///
/// This is shown in the $DATATYPE keyword.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
enum AlphaNumType {
    Ascii,
    Integer,
    Single,
    Double,
}

impl FromStr for AlphaNumType {
    type Err = AlphaNumTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "I" => Ok(AlphaNumType::Integer),
            "F" => Ok(AlphaNumType::Single),
            "D" => Ok(AlphaNumType::Double),
            "A" => Ok(AlphaNumType::Ascii),
            _ => Err(AlphaNumTypeError),
        }
    }
}

impl fmt::Display for AlphaNumType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            AlphaNumType::Ascii => write!(f, "A"),
            AlphaNumType::Integer => write!(f, "I"),
            AlphaNumType::Single => write!(f, "F"),
            AlphaNumType::Double => write!(f, "D"),
        }
    }
}

struct AlphaNumTypeError;

impl fmt::Display for AlphaNumTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be one of 'I', 'F', 'D', or 'A'")
    }
}

/// The three numeric data types for the $PnDATATYPE keyword in 3.2+
#[derive(Debug, Clone, Copy, Serialize)]
enum NumType {
    Integer,
    Single,
    Double,
}

impl FromStr for NumType {
    type Err = NumTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "I" => Ok(NumType::Integer),
            "F" => Ok(NumType::Single),
            "D" => Ok(NumType::Double),
            _ => Err(NumTypeError),
        }
    }
}

impl fmt::Display for NumType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            NumType::Integer => write!(f, "I"),
            NumType::Single => write!(f, "F"),
            NumType::Double => write!(f, "D"),
        }
    }
}

struct NumTypeError;

impl fmt::Display for NumTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be one of 'F', 'D', or 'A'")
    }
}

impl NumType {
    fn add_alpha(&self) -> AlphaNumType {
        match self {
            NumType::Integer => AlphaNumType::Integer,
            NumType::Single => AlphaNumType::Single,
            NumType::Double => AlphaNumType::Double,
        }
    }
}

/// A compensation matrix.
///
/// This is held in the $DFCmTOn keywords in 2.0 and $COMP in 3.0.
#[derive(Debug, Clone, Serialize)]
struct Compensation {
    /// Values in the comp matrix in row-major order. Assumed to be the
    /// same width and height as $PAR
    matrix: Vec<Vec<f32>>,
}

impl FromStr for Compensation {
    type Err = FixedSeqError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut xs = s.split(",");
        if let Some(first) = &xs.next().and_then(|x| x.parse::<usize>().ok()) {
            let n = *first;
            let nn = n * n;
            let values: Vec<_> = xs.by_ref().take(nn).collect();
            let remainder = xs.by_ref().count();
            let total = values.len() + remainder;
            if total != nn {
                Err(FixedSeqError::WrongLength {
                    expected: nn,
                    total,
                })
            } else {
                let fvalues: Vec<_> = values
                    .into_iter()
                    .filter_map(|x| x.parse::<f32>().ok())
                    .collect();
                if fvalues.len() != nn {
                    Err(FixedSeqError::BadFloat)
                } else {
                    let matrix = fvalues
                        .into_iter()
                        .chunks(n)
                        .into_iter()
                        .map(|c| c.collect())
                        .collect();
                    Ok(Compensation { matrix })
                }
            }
        } else {
            Err(FixedSeqError::BadLength)
        }
    }
}

impl fmt::Display for Compensation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = self.matrix.len();
        let xs = self.matrix.iter().map(|xs| xs.iter().join(",")).join(",");
        write!(f, "{n},{xs}")
    }
}

enum FixedSeqError {
    WrongLength { total: usize, expected: usize },
    BadLength,
    BadFloat,
}

impl fmt::Display for FixedSeqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            FixedSeqError::BadFloat => write!(f, "Float could not be parsed"),
            FixedSeqError::WrongLength { total, expected } => {
                write!(f, "Expected {expected} entries, found {total}")
            }
            FixedSeqError::BadLength => write!(f, "Could not determine length"),
        }
    }
}

/// The spillover matrix in the $SPILLOVER keyword in (3.1+)
#[derive(Debug, Clone, Serialize)]
struct Spillover {
    measurements: Vec<String>,
    /// Values in the spillover matrix in row-major order.
    matrix: Vec<Vec<f32>>,
}

impl FromStr for Spillover {
    type Err = NamedFixedSeqError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        {
            let mut xs = s.split(",");
            if let Some(first) = &xs.next().and_then(|x| x.parse::<usize>().ok()) {
                let n = *first;
                let nn = n * n;
                let expected = n + nn;
                let measurements: Vec<_> = xs.by_ref().take(n).map(String::from).collect();
                let values: Vec<_> = xs.by_ref().take(nn).collect();
                let remainder = xs.by_ref().count();
                let total = measurements.len() + values.len() + remainder;
                if total != expected {
                    Err(NamedFixedSeqError::Seq(FixedSeqError::WrongLength {
                        total,
                        expected,
                    }))
                } else if measurements.iter().unique().count() != n {
                    Err(NamedFixedSeqError::NonUnique)
                } else {
                    let fvalues: Vec<_> = values
                        .into_iter()
                        .filter_map(|x| x.parse::<f32>().ok())
                        .collect();
                    if fvalues.len() != nn {
                        Err(NamedFixedSeqError::Seq(FixedSeqError::BadFloat))
                    } else {
                        let matrix = fvalues
                            .into_iter()
                            .chunks(n)
                            .into_iter()
                            .map(|c| c.collect())
                            .collect();
                        Ok(Spillover {
                            measurements,
                            matrix,
                        })
                    }
                }
            } else {
                Err(NamedFixedSeqError::Seq(FixedSeqError::BadLength))
            }
        }
    }
}

impl fmt::Display for Spillover {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = self.measurements.len();
        let xs = self.matrix.iter().map(|ys| ys.iter().join(",")).join(",");
        write!(f, "{n},{xs}")
    }
}

enum NamedFixedSeqError {
    Seq(FixedSeqError),
    NonUnique,
}

impl fmt::Display for NamedFixedSeqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            NamedFixedSeqError::Seq(s) => write!(f, "{}", s),
            NamedFixedSeqError::NonUnique => write!(f, "Names in sequence is not unique"),
        }
    }
}

// TODO add Display trait so this can be written

impl Spillover {
    fn table(&self, delim: &str) -> Vec<String> {
        let header0 = vec![String::from("[-]")];
        let header = header0
            .into_iter()
            .chain(self.measurements.iter().map(String::from))
            .join(delim);
        let lines = vec![header];
        let rows = self.matrix.iter().map(|xs| xs.iter().join(delim));
        lines.into_iter().chain(rows).collect()
    }

    fn print_table(&self, delim: &str) {
        for e in self.table(delim) {
            println!("{}", e);
        }
    }
}

/// The byte order as shown in the $BYTEORD field in 2.0 and 3.0
///
/// This can be either 1,2,3,4 (little endian), 4,3,2,1 (big endian), or some
/// sequence representing byte order. For 2.0 and 3.0, this sequence is
/// technically allowed to vary in length in the case of $DATATYPE=I since
/// integers do not necessarily need to be 32 or 64-bit.
#[derive(Debug, Clone, Serialize)]
enum ByteOrd {
    Endian(Endian),
    Mixed(Vec<u8>),
}

pub struct EndianError;

impl fmt::Display for EndianError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Endian must be either 1,2,3,4 or 4,3,2,1")
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

enum ParseByteOrdError {
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

impl FromStr for ByteOrd {
    type Err = ParseByteOrdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse() {
            Ok(e) => Ok(ByteOrd::Endian(e)),
            _ => {
                let xs: Vec<_> = s.split(",").collect();
                let nxs = xs.len();
                let xs_num: Vec<u8> = xs.iter().filter_map(|s| s.parse().ok()).unique().collect();
                if let (Some(min), Some(max)) = (xs_num.iter().min(), xs_num.iter().max()) {
                    if *min == 1 && usize::from(*max) == nxs && xs_num.len() == nxs {
                        Ok(ByteOrd::Mixed(xs_num.iter().map(|x| x - 1).collect()))
                    } else {
                        Err(ParseByteOrdError::InvalidOrder)
                    }
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
            ByteOrd::Mixed(xs) => write!(f, "{}", xs.into_iter().join(",")),
        }
    }
}

impl ByteOrd {
    // This only makes sense for pre 3.1 integer types
    fn num_bytes(&self) -> u8 {
        match self {
            ByteOrd::Endian(_) => 4,
            ByteOrd::Mixed(xs) => xs.len() as u8,
        }
    }
}

/// The $TR field in all FCS versions.
///
/// This is formatted as 'string,f' where 'string' is a measurement name.
#[derive(Debug, Clone, Serialize)]
struct Trigger {
    measurement: String,
    threshold: u32,
}

impl FromStr for Trigger {
    type Err = TriggerError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(",").collect::<Vec<_>>()[..] {
            [p, n1] => n1
                .parse()
                .map_err(TriggerError::IntFormat)
                .map(|threshold| Trigger {
                    measurement: String::from(p),
                    threshold,
                }),
            _ => Err(TriggerError::WrongFieldNumber),
        }
    }
}

impl fmt::Display for Trigger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{},{}", self.measurement, self.threshold)
    }
}

enum TriggerError {
    WrongFieldNumber,
    IntFormat(std::num::ParseIntError),
}

impl fmt::Display for TriggerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            TriggerError::WrongFieldNumber => write!(f, "must be like 'string,f'"),
            TriggerError::IntFormat(i) => write!(f, "{}", i),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ModifiedDateTime(NaiveDateTime);

impl FromStr for ModifiedDateTime {
    type Err = ModifiedDateTimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (dt, cc) = NaiveDateTime::parse_and_remainder(s, "%d-%b-%Y %H:%M:%S")
            .or(Err(ModifiedDateTimeError))?;
        if cc.is_empty() {
            Ok(ModifiedDateTime(dt))
        } else if cc.len() == 3 && cc.starts_with(".") {
            let tt: u32 = cc[1..3].parse().or(Err(ModifiedDateTimeError))?;
            dt.with_nanosecond(tt * 10000000)
                .map(ModifiedDateTime)
                .ok_or(ModifiedDateTimeError)
        } else {
            Err(ModifiedDateTimeError)
        }
    }
}

impl fmt::Display for ModifiedDateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let dt = self.0.format("%d-%b-%Y %H:%M:%S");
        let cc = self.0.nanosecond() / 10000000;
        write!(f, "{dt}.{cc}")
    }
}

struct ModifiedDateTimeError;

impl fmt::Display for ModifiedDateTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'dd-mmm-yyyy hh:mm:ss[.cc]'")
    }
}

#[derive(Debug, Clone, Serialize)]
struct FCSDate(NaiveDate);

// the "%b" format is case-insensitive so this should work for "Jan", "JAN",
// "jan", "jaN", etc
const FCS_DATE_FORMAT: &str = "%d-%b-%Y";

impl FromStr for FCSDate {
    type Err = FCSDateError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NaiveDate::parse_from_str(s, FCS_DATE_FORMAT)
            .or(Err(FCSDateError))
            .map(FCSDate)
    }
}

impl fmt::Display for FCSDate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.format(FCS_DATE_FORMAT))
    }
}

struct FCSDateError;

impl fmt::Display for FCSDateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'dd-mmm-yyyy'")
    }
}

#[derive(Debug, Clone, Serialize)]
struct Timestamps<T> {
    btim: OptionalKw<T>,
    etim: OptionalKw<T>,
    date: OptionalKw<FCSDate>,
}

type Timestamps2_0 = Timestamps<FCSTime>;
type Timestamps3_0 = Timestamps<FCSTime60>;
type Timestamps3_1 = Timestamps<FCSTime100>;

#[derive(Debug, Clone, Serialize)]
struct Datetimes {
    begin: OptionalKw<FCSDateTime>,
    end: OptionalKw<FCSDateTime>,
}

// TODO this is super messy, see 3.2 spec for restrictions on this we may with
// to use further
#[derive(Debug, Clone, PartialEq, Serialize)]
enum Scale {
    Log { decades: f32, offset: f32 },
    Linear,
}

use Scale::*;

impl fmt::Display for Scale {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Scale::Log { decades, offset } => write!(f, "{decades},{offset}"),
            Scale::Linear => write!(f, "Lin"),
        }
    }
}

enum ScaleError {
    FloatError(ParseFloatError),
    WrongFormat,
}

impl fmt::Display for ScaleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            ScaleError::FloatError(x) => write!(f, "{}", x),
            ScaleError::WrongFormat => write!(f, "must be like 'f1,f2'"),
        }
    }
}

impl str::FromStr for Scale {
    type Err = ScaleError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(",").collect::<Vec<_>>()[..] {
            [ds, os] => {
                let f1 = ds.parse().map_err(ScaleError::FloatError)?;
                let f2 = os.parse().map_err(ScaleError::FloatError)?;
                match (f1, f2) {
                    (0.0, 0.0) => Ok(Linear),
                    (decades, offset) => Ok(Log { decades, offset }),
                }
            }
            _ => Err(ScaleError::WrongFormat),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
enum Display {
    Lin { lower: f32, upper: f32 },
    Log { offset: f32, decades: f32 },
}

enum DisplayError {
    FloatError(ParseFloatError),
    InvalidType,
    FormatError,
}

impl fmt::Display for DisplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            DisplayError::FloatError(x) => write!(f, "{}", x),
            DisplayError::InvalidType => write!(f, "Type must be either 'Logarithmic' or 'Linear'"),
            DisplayError::FormatError => write!(f, "must be like 'string,f1,f2'"),
        }
    }
}

impl str::FromStr for Display {
    type Err = DisplayError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(",").collect::<Vec<_>>()[..] {
            [which, s1, s2] => {
                let f1 = s1.parse().map_err(DisplayError::FloatError)?;
                let f2 = s2.parse().map_err(DisplayError::FloatError)?;
                match which {
                    "Linear" => Ok(Display::Lin {
                        lower: f1,
                        upper: f2,
                    }),
                    "Logarithmic" => Ok(Display::Log {
                        decades: f1,
                        offset: f2,
                    }),
                    _ => Err(DisplayError::InvalidType),
                }
            }
            _ => Err(DisplayError::FormatError),
        }
    }
}

impl fmt::Display for Display {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Display::Lin { lower, upper } => write!(f, "Linear,{lower},{upper}"),
            Display::Log { offset, decades } => write!(f, "Log,{offset},{decades}"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct Calibration3_1 {
    value: f32,
    unit: String,
}

enum CalibrationError<C> {
    Float(ParseFloatError),
    Range,
    Format(C),
}

struct CalibrationFormat3_1;
struct CalibrationFormat3_2;

impl fmt::Display for CalibrationFormat3_1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'f,string'")
    }
}

impl fmt::Display for CalibrationFormat3_2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be like 'f1,[f2],string'")
    }
}

impl<C: fmt::Display> fmt::Display for CalibrationError<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            CalibrationError::Float(x) => write!(f, "{}", x),
            CalibrationError::Range => write!(f, "must be a positive float"),
            CalibrationError::Format(x) => write!(f, "{}", x),
        }
    }
}

impl str::FromStr for Calibration3_1 {
    type Err = CalibrationError<CalibrationFormat3_1>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split(",").collect::<Vec<_>>()[..] {
            [svalue, unit] => {
                let value = svalue.parse().map_err(CalibrationError::Float)?;
                if value >= 0.0 {
                    Ok(Calibration3_1 {
                        value,
                        unit: String::from(unit),
                    })
                } else {
                    Err(CalibrationError::Range)
                }
            }
            _ => Err(CalibrationError::Format(CalibrationFormat3_1)),
        }
    }
}

impl fmt::Display for Calibration3_1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{},{}", self.value, self.unit)
    }
}

#[derive(Debug, Clone, Serialize)]
struct Calibration3_2 {
    value: f32,
    offset: f32,
    unit: String,
}

impl str::FromStr for Calibration3_2 {
    type Err = CalibrationError<CalibrationFormat3_2>;

    // TODO not dry
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (value, offset, unit) = match s.split(",").collect::<Vec<_>>()[..] {
            [svalue, unit] => {
                let f1 = svalue.parse().map_err(CalibrationError::Float)?;
                Ok((f1, 0.0, String::from(unit)))
            }
            [svalue, soffset, unit] => {
                let f1 = svalue.parse().map_err(CalibrationError::Float)?;
                let f2 = soffset.parse().map_err(CalibrationError::Float)?;
                Ok((f1, f2, String::from(unit)))
            }
            _ => Err(CalibrationError::Format(CalibrationFormat3_2)),
        }?;
        if value >= 0.0 {
            Ok(Calibration3_2 {
                value,
                offset,
                unit,
            })
        } else {
            Err(CalibrationError::Range)
        }
    }
}

impl fmt::Display for Calibration3_2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{},{},{}", self.value, self.offset, self.unit)
    }
}

#[derive(Debug, Clone, Serialize)]
enum MeasurementType {
    ForwardScatter,
    SideScatter,
    RawFluorescence,
    UnmixedFluorescence,
    Mass,
    Time,
    ElectronicVolume,
    Classification,
    Index,
    Other(String),
}

impl str::FromStr for MeasurementType {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Forward Scatter" => Ok(MeasurementType::ForwardScatter),
            "Side Scatter" => Ok(MeasurementType::SideScatter),
            "Raw Fluorescence" => Ok(MeasurementType::RawFluorescence),
            "Unmixed Fluorescence" => Ok(MeasurementType::UnmixedFluorescence),
            "Mass" => Ok(MeasurementType::Mass),
            "Time" => Ok(MeasurementType::Time),
            "Electronic Volume" => Ok(MeasurementType::ElectronicVolume),
            "Index" => Ok(MeasurementType::Index),
            "Classification" => Ok(MeasurementType::Classification),
            s => Ok(MeasurementType::Other(String::from(s))),
        }
    }
}

impl fmt::Display for MeasurementType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            MeasurementType::ForwardScatter => write!(f, "Foward Scatter"),
            MeasurementType::SideScatter => write!(f, "Side Scatter"),
            MeasurementType::RawFluorescence => write!(f, "Raw Fluorescence"),
            MeasurementType::UnmixedFluorescence => write!(f, "Unmixed Fluorescence"),
            MeasurementType::Mass => write!(f, "Mass"),
            MeasurementType::Time => write!(f, "Time"),
            MeasurementType::ElectronicVolume => write!(f, "Electronic Volume"),
            MeasurementType::Classification => write!(f, "Classification"),
            MeasurementType::Index => write!(f, "Index"),
            MeasurementType::Other(s) => write!(f, "{}", s),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
enum Feature {
    Area,
    Width,
    Height,
}

struct FeatureError;

impl fmt::Display for FeatureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be one of 'Area', 'Width', or 'Height'")
    }
}

impl str::FromStr for Feature {
    type Err = FeatureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Area" => Ok(Feature::Area),
            "Width" => Ok(Feature::Width),
            "Height" => Ok(Feature::Height),
            _ => Err(FeatureError),
        }
    }
}

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Feature::Area => write!(f, "Area"),
            Feature::Width => write!(f, "Width"),
            Feature::Height => write!(f, "Height"),
        }
    }
}

#[derive(Debug, Clone)]
enum OptionalKw<V> {
    Present(V),
    Absent,
}

use OptionalKw::*;

impl<V> OptionalKw<V> {
    fn as_ref(&self) -> OptionalKw<&V> {
        match self {
            OptionalKw::Present(x) => Present(x),
            Absent => Absent,
        }
    }

    fn into_option(self) -> Option<V> {
        match self {
            OptionalKw::Present(x) => Some(x),
            Absent => None,
        }
    }

    fn from_option(x: Option<V>) -> Self {
        x.map_or_else(|| Absent, |y| OptionalKw::Present(y))
    }
}

impl<V: fmt::Display> OptionalKw<V> {
    fn as_opt_string(&self) -> Option<String> {
        self.as_ref().into_option().map(|x| x.to_string())
    }
}

impl<T: Serialize> Serialize for OptionalKw<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.as_ref() {
            Present(x) => serializer.serialize_some(x),
            Absent => serializer.serialize_none(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct Wavelengths(Vec<u32>);

impl fmt::Display for Wavelengths {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.iter().join(","))
    }
}

impl str::FromStr for Wavelengths {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut ws = vec![];
        for x in s.split(",") {
            ws.push(x.parse()?);
        }
        Ok(Wavelengths(ws))
    }
}

#[derive(Debug, Clone, Serialize)]
struct Shortname(String);

struct ShortnameError;

impl fmt::Display for ShortnameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "commas are not allowed")
    }
}

impl fmt::Display for Shortname {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl str::FromStr for Shortname {
    type Err = ShortnameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains(',') {
            Err(ShortnameError)
        } else {
            Ok(Shortname(String::from(s)))
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct InnerMeasurement2_0 {
    scale: OptionalKw<Scale>,         // PnE
    wavelength: OptionalKw<u32>,      // PnL
    shortname: OptionalKw<Shortname>, // PnN
}

#[derive(Debug, Clone, Serialize)]
struct InnerMeasurement3_0 {
    scale: Scale,                     // PnE
    wavelength: OptionalKw<u32>,      // PnL
    shortname: OptionalKw<Shortname>, // PnN
    gain: OptionalKw<f32>,            // PnG
}

#[derive(Debug, Clone, Serialize)]
struct InnerMeasurement3_1 {
    scale: Scale,                         // PnE
    wavelengths: OptionalKw<Wavelengths>, // PnL
    shortname: Shortname,                 // PnN
    gain: OptionalKw<f32>,                // PnG
    calibration: OptionalKw<Calibration3_1>,
    display: OptionalKw<Display>,
}

#[derive(Debug, Clone, Serialize)]
struct InnerMeasurement3_2 {
    scale: Scale,                         // PnE
    wavelengths: OptionalKw<Wavelengths>, // PnL
    shortname: Shortname,                 // PnN
    gain: OptionalKw<f32>,                // PnG
    calibration: OptionalKw<Calibration3_2>,
    display: OptionalKw<Display>,
    analyte: OptionalKw<String>,
    feature: OptionalKw<Feature>,
    measurement_type: OptionalKw<MeasurementType>,
    tag: OptionalKw<String>,
    detector_name: OptionalKw<String>,
    datatype: OptionalKw<NumType>,
}

// TODO this will likely need to be a trait in 4.0
impl InnerMeasurement3_2 {
    fn get_column_type(&self, default: AlphaNumType) -> AlphaNumType {
        self.datatype
            .as_ref()
            .into_option()
            .map(|x| x.add_alpha())
            .unwrap_or(default)
    }
}

#[derive(Debug, Clone, Serialize)]
enum Bytes {
    Fixed(u8),
    Variable,
}

enum BytesError {
    Int(ParseIntError),
    Range,
    NotOctet,
}

impl fmt::Display for BytesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            BytesError::Int(i) => write!(f, "{}", i),
            BytesError::Range => write!(f, "bit widths over 64 are not supported"),
            BytesError::NotOctet => write!(f, "bit widths must be octets"),
        }
    }
}

impl FromStr for Bytes {
    type Err = BytesError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "*" => Ok(Bytes::Variable),
            _ => s.parse::<u8>().map_err(BytesError::Int).and_then(|x| {
                if x > 64 {
                    Err(BytesError::Range)
                } else if x % 8 > 1 {
                    Err(BytesError::NotOctet)
                } else {
                    Ok(Bytes::Fixed(x / 8))
                }
            }),
        }
    }
}

impl fmt::Display for Bytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Bytes::Fixed(x) => write!(f, "{}", x),
            Bytes::Variable => write!(f, "*"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
enum Range {
    // This will actually store PnR - 1; most cytometers will store this as a
    // power of 2, so in the case of a 64 bit channel this will be 2^64 which is
    // one greater than u64::MAX.
    Int(u64),
    // This stores the value of PnR as-is. Sometimes PnR is actually a float
    // for floating point measurements rather than an int.
    Float(f64),
}

enum RangeError {
    Int(ParseIntError),
    Float(ParseFloatError),
}

impl fmt::Display for RangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            RangeError::Int(x) => write!(f, "{x}"),
            RangeError::Float(x) => write!(f, "{x}"),
        }
    }
}

impl str::FromStr for Range {
    type Err = RangeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.parse::<u64>() {
            Ok(x) => Ok(Range::Int(x - 1)),
            Err(e) => match e.kind() {
                IntErrorKind::InvalidDigit => s
                    .parse::<f64>()
                    .map_or_else(|e| Err(RangeError::Float(e)), |x| Ok(Range::Float(x))),
                IntErrorKind::PosOverflow => Ok(Range::Int(u64::MAX)),
                _ => Err(RangeError::Int(e)),
            },
        }
    }
}

impl fmt::Display for Range {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Range::Int(x) => write!(f, "{x}"),
            Range::Float(x) => write!(f, "{x}"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct Measurement<X> {
    bytes: Bytes,                      // PnB
    range: Range,                      // PnR
    longname: OptionalKw<String>,      // PnS
    filter: OptionalKw<String>,        // PnF there is a loose standard for this
    power: OptionalKw<u32>,            // PnO
    detector_type: OptionalKw<String>, // PnD
    percent_emitted: OptionalKw<u32>,  // PnP (TODO deprecated in 3.2, factor out)
    detector_voltage: OptionalKw<f32>, // PnV
    nonstandard: HashMap<NonStdKey, String>,
    specific: X,
}

impl<P: VersionedMeasurement> Measurement<P> {
    fn bytes_eq(&self, b: u8) -> bool {
        match self.bytes {
            Bytes::Fixed(x) => x == b,
            _ => false,
        }
    }

    // TODO add measurement index to error message
    fn make_int_parser(&self, o: &ByteOrd, t: usize) -> Result<AnyIntColumn, Vec<String>> {
        match self.bytes {
            Bytes::Fixed(b) => make_int_parser(b, &self.range, o, t),
            _ => Err(vec![String::from("PnB is variable length")]),
        }
    }

    // TODO include nonstandard?
    fn keywords(&self, n: &str) -> Vec<(String, Option<String>)> {
        P::keywords(self, n)
    }

    fn table_header(&self) -> Vec<String> {
        vec![String::from("index")]
            .into_iter()
            .chain(P::keywords(self, "n").into_iter().map(|(k, _)| k))
            .collect()
    }

    fn table_row(&self, n: usize) -> Vec<Option<String>> {
        vec![Some(n.to_string())]
            .into_iter()
            // NOTE; the "n" is a dummy and never used
            .chain(P::keywords(self, "n").into_iter().map(|(_, v)| v))
            .collect()
    }
}

fn make_int_parser(b: u8, r: &Range, o: &ByteOrd, t: usize) -> Result<AnyIntColumn, Vec<String>> {
    match b {
        1 => u8::to_col_parser(r, o, t).map(AnyIntColumn::Uint8),
        2 => u16::to_col_parser(r, o, t).map(AnyIntColumn::Uint16),
        3 => IntFromBytes::<4, 3>::to_col_parser(r, o, t).map(AnyIntColumn::Uint24),
        4 => IntFromBytes::<4, 4>::to_col_parser(r, o, t).map(AnyIntColumn::Uint32),
        5 => IntFromBytes::<8, 5>::to_col_parser(r, o, t).map(AnyIntColumn::Uint40),
        6 => IntFromBytes::<8, 6>::to_col_parser(r, o, t).map(AnyIntColumn::Uint48),
        7 => IntFromBytes::<8, 7>::to_col_parser(r, o, t).map(AnyIntColumn::Uint56),
        8 => IntFromBytes::<8, 8>::to_col_parser(r, o, t).map(AnyIntColumn::Uint64),
        _ => Err(vec![String::from("$PnB has invalid byte length")]),
    }
}

trait Versioned {
    fn fcs_version() -> Version;
}

trait VersionedMeasurement: Sized + Versioned {
    fn build_inner(st: &mut KwState, n: u32) -> Option<Self>;

    fn measurement_name(p: &Measurement<Self>) -> Option<&str>;

    fn has_linear_scale(&self) -> bool;

    fn has_gain(&self) -> bool;

    fn lookup_measurements(st: &mut KwState, par: u32) -> Option<Vec<Measurement<Self>>> {
        let mut ps = vec![];
        let v = Self::fcs_version();
        for n in 1..(par + 1) {
            let p = Measurement {
                bytes: st.lookup_meas_bytes(n)?,
                range: st.lookup_meas_range(n)?,
                longname: st.lookup_meas_longname(n),
                filter: st.lookup_meas_filter(n),
                power: st.lookup_meas_power(n),
                detector_type: st.lookup_meas_detector_type(n),
                percent_emitted: st.lookup_meas_percent_emitted(n, v == Version::FCS3_2),
                detector_voltage: st.lookup_meas_detector_voltage(n),
                specific: Self::build_inner(st, n)?,
                nonstandard: st.lookup_meas_nonstandard(n),
            };
            ps.push(p);
        }
        let names: Vec<_> = ps
            .iter()
            .filter_map(|m| Self::measurement_name(m))
            .collect();
        if names.iter().unique().count() < names.len() {
            st.push_meta_error_str("$PnN are not all unique");
            None
        } else {
            Some(ps)
        }
    }

    fn suffixes_inner(&self) -> Vec<(&'static str, Option<String>)>;

    fn keywords(m: &Measurement<Self>, n: &str) -> Vec<(String, Option<String>)> {
        let fixed = [
            (BYTES_SFX, Some(m.bytes.to_string())),
            (RANGE_SFX, Some(m.range.to_string())),
            (LONGNAME_SFX, m.longname.as_opt_string()),
            (FILTER_SFX, m.filter.as_opt_string()),
            (POWER_SFX, m.power.as_opt_string()),
            (DET_TYPE_SFX, m.detector_type.as_opt_string()),
            (PCNT_EMT_SFX, m.percent_emitted.as_opt_string()),
            (DET_VOLT_SFX, m.detector_voltage.as_opt_string()),
        ];
        fixed
            .into_iter()
            .chain(m.specific.suffixes_inner())
            .map(|(s, v)| (format_measurement(n, s), v))
            .collect()
    }
}

type Measurement2_0 = Measurement<InnerMeasurement2_0>;
type Measurement3_0 = Measurement<InnerMeasurement3_0>;
type Measurement3_1 = Measurement<InnerMeasurement3_1>;
type Measurement3_2 = Measurement<InnerMeasurement3_2>;

impl Versioned for InnerMeasurement2_0 {
    fn fcs_version() -> Version {
        Version::FCS2_0
    }
}

impl Versioned for InnerMeasurement3_0 {
    fn fcs_version() -> Version {
        Version::FCS3_0
    }
}

impl Versioned for InnerMeasurement3_1 {
    fn fcs_version() -> Version {
        Version::FCS3_1
    }
}

impl Versioned for InnerMeasurement3_2 {
    fn fcs_version() -> Version {
        Version::FCS3_2
    }
}

impl VersionedMeasurement for InnerMeasurement2_0 {
    fn measurement_name(p: &Measurement<Self>) -> Option<&str> {
        p.specific
            .shortname
            .as_ref()
            .into_option()
            .map(|s| s.0.as_str())
    }

    fn has_linear_scale(&self) -> bool {
        self.scale
            .as_ref()
            .into_option()
            .map(|x| *x == Scale::Linear)
            .unwrap_or(false)
    }

    fn has_gain(&self) -> bool {
        false
    }

    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerMeasurement2_0> {
        Some(InnerMeasurement2_0 {
            scale: st.lookup_meas_scale_opt(n),
            shortname: st.lookup_meas_shortname_opt(n),
            wavelength: st.lookup_meas_wavelength(n),
        })
    }

    fn suffixes_inner(&self) -> Vec<(&'static str, Option<String>)> {
        [
            (SCALE_SFX, self.scale.as_opt_string()),
            (SHORTNAME_SFX, self.shortname.as_opt_string()),
            (WAVELEN_SFX, self.wavelength.as_opt_string()),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedMeasurement for InnerMeasurement3_0 {
    fn measurement_name(p: &Measurement<Self>) -> Option<&str> {
        p.specific
            .shortname
            .as_ref()
            .into_option()
            .map(|s| s.0.as_str())
    }

    fn has_linear_scale(&self) -> bool {
        self.scale == Scale::Linear
    }

    fn has_gain(&self) -> bool {
        self.gain.as_ref().into_option().is_some()
    }

    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerMeasurement3_0> {
        Some(InnerMeasurement3_0 {
            scale: st.lookup_meas_scale_req(n)?,
            shortname: st.lookup_meas_shortname_opt(n),
            wavelength: st.lookup_meas_wavelength(n),
            gain: st.lookup_meas_gain(n),
        })
    }

    fn suffixes_inner(&self) -> Vec<(&'static str, Option<String>)> {
        [
            (SCALE_SFX, Some(self.scale.to_string())),
            (SHORTNAME_SFX, self.shortname.as_opt_string()),
            (WAVELEN_SFX, self.wavelength.as_opt_string()),
            (GAIN_SFX, self.gain.as_opt_string()),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedMeasurement for InnerMeasurement3_1 {
    fn measurement_name(p: &Measurement<Self>) -> Option<&str> {
        Some(p.specific.shortname.0.as_str())
    }

    fn has_linear_scale(&self) -> bool {
        self.scale == Scale::Linear
    }

    fn has_gain(&self) -> bool {
        self.gain.as_ref().into_option().is_some()
    }

    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerMeasurement3_1> {
        Some(InnerMeasurement3_1 {
            scale: st.lookup_meas_scale_req(n)?,
            shortname: st.lookup_meas_shortname_req(n)?,
            wavelengths: st.lookup_meas_wavelengths(n),
            gain: st.lookup_meas_gain(n),
            calibration: st.lookup_meas_calibration3_1(n),
            display: st.lookup_meas_display(n),
        })
    }

    fn suffixes_inner(&self) -> Vec<(&'static str, Option<String>)> {
        [
            (SCALE_SFX, Some(self.scale.to_string())),
            (SHORTNAME_SFX, Some(self.shortname.to_string())),
            (WAVELEN_SFX, self.wavelengths.as_opt_string()),
            (GAIN_SFX, self.gain.as_opt_string()),
            (CALIBRATION_SFX, self.calibration.as_opt_string()),
            (DISPLAY_SFX, self.display.as_opt_string()),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedMeasurement for InnerMeasurement3_2 {
    fn measurement_name(p: &Measurement<Self>) -> Option<&str> {
        Some(p.specific.shortname.0.as_str())
    }

    fn has_linear_scale(&self) -> bool {
        self.scale == Scale::Linear
    }

    fn has_gain(&self) -> bool {
        self.gain.as_ref().into_option().is_some()
    }

    fn build_inner(st: &mut KwState, n: u32) -> Option<InnerMeasurement3_2> {
        Some(InnerMeasurement3_2 {
            scale: st.lookup_meas_scale_req(n)?,
            shortname: st.lookup_meas_shortname_req(n)?,
            wavelengths: st.lookup_meas_wavelengths(n),
            gain: st.lookup_meas_gain(n),
            calibration: st.lookup_meas_calibration3_2(n),
            display: st.lookup_meas_display(n),
            detector_name: st.lookup_meas_detector(n),
            tag: st.lookup_meas_tag(n),
            measurement_type: st.lookup_meas_type(n),
            feature: st.lookup_meas_feature(n),
            analyte: st.lookup_meas_analyte(n),
            datatype: st.lookup_meas_datatype(n),
        })
    }

    fn suffixes_inner(&self) -> Vec<(&'static str, Option<String>)> {
        [
            (SCALE_SFX, Some(self.scale.to_string())),
            (SHORTNAME_SFX, Some(self.shortname.to_string())),
            (WAVELEN_SFX, self.wavelengths.as_opt_string()),
            (GAIN_SFX, self.gain.as_opt_string()),
            (CALIBRATION_SFX, self.calibration.as_opt_string()),
            (DISPLAY_SFX, self.display.as_opt_string()),
            (DET_NAME_SFX, self.detector_name.as_opt_string()),
            (TAG_SFX, self.tag.as_opt_string()),
            (MEAS_TYPE_SFX, self.measurement_type.as_opt_string()),
            (FEATURE_SFX, self.feature.as_opt_string()),
            (ANALYTE_SFX, self.analyte.as_opt_string()),
            (DATATYPE_SFX, self.datatype.as_opt_string()),
        ]
        .into_iter()
        .collect()
    }
}

#[derive(Debug, Clone, Serialize)]
enum Originality {
    Original,
    NonDataModified,
    Appended,
    DataModified,
}

struct OriginalityError;

impl fmt::Display for OriginalityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "Originality must be one of 'Original', 'NonDataModified', \
                   'Appended', or 'DataModified'"
        )
    }
}

impl str::FromStr for Originality {
    type Err = OriginalityError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Original" => Ok(Originality::Original),
            "NonDataModified" => Ok(Originality::NonDataModified),
            "Appended" => Ok(Originality::Appended),
            "DataModified" => Ok(Originality::DataModified),
            _ => Err(OriginalityError),
        }
    }
}

impl fmt::Display for Originality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let x = match self {
            Originality::Appended => "Appended",
            Originality::Original => "Original",
            Originality::NonDataModified => "NonDataModified",
            Originality::DataModified => "DataModified",
        };
        write!(f, "{x}")
    }
}

#[derive(Debug, Clone, Serialize)]
struct ModificationData {
    last_modifier: OptionalKw<String>,
    last_modified: OptionalKw<ModifiedDateTime>,
    originality: OptionalKw<Originality>,
}

#[derive(Debug, Clone, Serialize)]
struct PlateData {
    plateid: OptionalKw<String>,
    platename: OptionalKw<String>,
    wellid: OptionalKw<String>,
}

#[derive(Debug, Clone, Serialize)]
struct UnstainedCenters(HashMap<String, f32>);

impl FromStr for UnstainedCenters {
    type Err = NamedFixedSeqError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut xs = s.split(",");
        if let Some(n) = xs.next().and_then(|s| s.parse().ok()) {
            let measurements: Vec<_> = xs.by_ref().take(n).map(String::from).collect();
            let values: Vec<_> = xs.by_ref().take(n).collect();
            let remainder = xs.by_ref().count();
            let total = values.len() + measurements.len() + remainder;
            let expected = 2 * n;
            if total != expected {
                let fvalues: Vec<_> = values
                    .into_iter()
                    .filter_map(|s| s.parse::<f32>().ok())
                    .collect();
                if fvalues.len() != n {
                    Err(NamedFixedSeqError::Seq(FixedSeqError::BadFloat))
                } else if measurements.iter().unique().count() != n {
                    Err(NamedFixedSeqError::NonUnique)
                } else {
                    Ok(UnstainedCenters(
                        measurements.into_iter().zip(fvalues).collect(),
                    ))
                }
            } else {
                Err(NamedFixedSeqError::Seq(FixedSeqError::WrongLength {
                    total,
                    expected,
                }))
            }
        } else {
            Err(NamedFixedSeqError::Seq(FixedSeqError::BadLength))
        }
    }
}

impl fmt::Display for UnstainedCenters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let n = self.0.len();
        let (measurements, values): (Vec<_>, Vec<_>) =
            self.0.iter().map(|(k, v)| (k.clone(), *v)).unzip();
        write!(
            f,
            "{n},{},{}",
            measurements.join(","),
            values.iter().join(",")
        )
    }
}

#[derive(Debug, Clone, Serialize)]
struct UnstainedData {
    unstainedcenters: OptionalKw<UnstainedCenters>,
    unstainedinfo: OptionalKw<String>,
}

#[derive(Debug, Clone, Serialize)]
struct CarrierData {
    carrierid: OptionalKw<String>,
    carriertype: OptionalKw<String>,
    locationid: OptionalKw<String>,
}

#[derive(Debug, Clone, Serialize)]
struct Unicode {
    page: u32,
    kws: Vec<String>,
}

enum UnicodeError {
    Empty,
    BadFormat,
}

impl fmt::Display for UnicodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            UnicodeError::Empty => write!(f, "No keywords given"),
            UnicodeError::BadFormat => write!(f, "Must be like 'n,string,[[string],...]'"),
        }
    }
}

impl FromStr for Unicode {
    type Err = UnicodeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut xs = s.split(",");
        if let Some(page) = xs.next().and_then(|s| s.parse().ok()) {
            let kws: Vec<String> = xs.map(String::from).collect();
            if kws.is_empty() {
                Err(UnicodeError::Empty)
            } else {
                Ok(Unicode { page, kws })
            }
        } else {
            Err(UnicodeError::BadFormat)
        }
    }
}

impl fmt::Display for Unicode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{},{}", self.page, self.kws.iter().join(","))
    }
}

#[derive(Debug, Clone, Serialize)]
struct SupplementalOffsets3_0 {
    analysis: Offsets,
    stext: Offsets,
}

#[derive(Debug, Clone, Serialize)]
struct SupplementalOffsets3_2 {
    analysis: OptionalKw<Offsets>,
    stext: OptionalKw<Offsets>,
}

#[derive(Debug, Clone, Serialize)]
struct InnerMetadata2_0 {
    // tot: OptionalKw<u32>,
    mode: Mode,
    byteord: ByteOrd,
    cyt: OptionalKw<String>,
    comp: OptionalKw<Compensation>,
    timestamps: Timestamps2_0, // BTIM/ETIM/DATE
}

#[derive(Debug, Clone, Serialize)]
struct InnerMetadata3_0 {
    // data: Offsets,
    // supplemental: SupplementalOffsets3_0,
    // tot: u32,
    mode: Mode,
    byteord: ByteOrd,
    timestamps: Timestamps3_0, // BTIM/ETIM/DATE
    cyt: OptionalKw<String>,
    comp: OptionalKw<Compensation>,
    cytsn: OptionalKw<String>,
    timestep: OptionalKw<f32>,
    unicode: OptionalKw<Unicode>,
}

#[derive(Debug, Clone, Serialize)]
struct InnerMetadata3_1 {
    // data: Offsets,
    // supplemental: SupplementalOffsets3_0,
    // tot: u32,
    mode: Mode,
    byteord: Endian,
    timestamps: Timestamps3_1, // BTIM/ETIM/DATE
    cyt: OptionalKw<String>,
    spillover: OptionalKw<Spillover>,
    cytsn: OptionalKw<String>,
    timestep: OptionalKw<f32>,
    modification: ModificationData,
    plate: PlateData,
    vol: OptionalKw<f32>,
}

#[derive(Debug, Clone, Serialize)]
struct InnerMetadata3_2 {
    // TODO offsets are not necessary for writing
    // data: Offsets,
    // supplemental: SupplementalOffsets3_2,
    // TODO this can be assumed from full dataframe when we have it
    // tot: u32,
    byteord: Endian,
    timestamps: Timestamps3_1, // BTIM/ETIM/DATE
    datetimes: Datetimes,      // DATETIMESTART/END
    cyt: String,
    spillover: OptionalKw<Spillover>,
    cytsn: OptionalKw<String>,
    timestep: OptionalKw<f32>,
    modification: ModificationData,
    plate: PlateData,
    vol: OptionalKw<f32>,
    carrier: CarrierData,
    unstained: UnstainedData,
    flowrate: OptionalKw<String>,
}

#[derive(Debug, Clone, Serialize)]
struct InnerReadData2_0 {
    tot: OptionalKw<u32>,
}

#[derive(Debug, Clone, Serialize)]
struct InnerReadData3_0 {
    data: Offsets,
    supplemental: SupplementalOffsets3_0,
    tot: u32,
}

// struct InnerReadData3_1 {
//     data: Offsets,
//     supplemental: SupplementalOffsets3_0,
//     tot: u32,
// }

#[derive(Debug, Clone, Serialize)]
struct InnerReadData3_2 {
    data: Offsets,
    supplemental: SupplementalOffsets3_2,
    tot: u32,
}

#[derive(Debug, Clone, Serialize)]
struct ReadData<X> {
    par: usize,
    nextdata: u32,
    specific: X,
}

trait VersionedReadData: Sized {
    fn lookup_inner(st: &mut KwState) -> Option<Self>;

    fn get_tot(&self) -> Option<u32>;

    // fn measurement_name(p: &Measurement<Self>) -> Option<&str>;

    // fn has_linear_scale(&self) -> bool;

    // fn has_gain(&self) -> bool;

    fn lookup(st: &mut KwState, par: usize) -> Option<ReadData<Self>> {
        let r = ReadData {
            par,
            nextdata: st.lookup_nextdata()?,
            specific: Self::lookup_inner(st)?,
        };
        Some(r)
    }
}

impl VersionedReadData for InnerReadData2_0 {
    fn lookup_inner(st: &mut KwState) -> Option<Self> {
        Some(InnerReadData2_0 {
            tot: st.lookup_tot_opt(),
        })
    }

    fn get_tot(&self) -> Option<u32> {
        self.tot.as_ref().into_option().copied()
    }
}

impl VersionedReadData for InnerReadData3_0 {
    fn lookup_inner(st: &mut KwState) -> Option<Self> {
        Some(InnerReadData3_0 {
            data: st.lookup_data_offsets()?,
            supplemental: st.lookup_supplemental3_0()?,
            tot: st.lookup_tot_req()?,
        })
    }

    fn get_tot(&self) -> Option<u32> {
        Some(self.tot)
    }
}

impl VersionedReadData for InnerReadData3_2 {
    fn lookup_inner(st: &mut KwState) -> Option<Self> {
        let r = InnerReadData3_2 {
            data: st.lookup_data_offsets()?,
            supplemental: st.lookup_supplemental3_2(),
            tot: st.lookup_tot_req()?,
        };
        Some(r)
    }

    fn get_tot(&self) -> Option<u32> {
        Some(self.tot)
    }
}

#[derive(Debug, Clone, Serialize)]
struct Metadata<X> {
    // TODO par is redundant when we have a full dataframe
    // TODO nextdata is not relevant for writing
    // par: u32,
    // nextdata: u32,
    datatype: AlphaNumType,
    abrt: OptionalKw<u32>,
    com: OptionalKw<String>,
    cells: OptionalKw<String>,
    exp: OptionalKw<String>,
    fil: OptionalKw<String>,
    inst: OptionalKw<String>,
    lost: OptionalKw<u32>,
    op: OptionalKw<String>,
    proj: OptionalKw<String>,
    smno: OptionalKw<String>,
    src: OptionalKw<String>,
    sys: OptionalKw<String>,
    tr: OptionalKw<Trigger>,
    specific: X,
}

impl<M: VersionedMetadata> Metadata<M> {
    fn keywords(&self, par: usize, tot: usize, len: KwLengths) -> MaybeKeywords {
        M::keywords(self, par, tot, len)
    }
}

type Metadata2_0 = Metadata<InnerMetadata2_0>;
type Metadata3_0 = Metadata<InnerMetadata3_0>;
type Metadata3_1 = Metadata<InnerMetadata3_1>;
type Metadata3_2 = Metadata<InnerMetadata3_2>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
enum Mode {
    List,
    Uncorrelated,
    Correlated,
}

impl FromStr for Mode {
    type Err = ModeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "C" => Ok(Mode::Correlated),
            "L" => Ok(Mode::List),
            "U" => Ok(Mode::Uncorrelated),
            _ => Err(ModeError),
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let x = match self {
            Mode::Correlated => "C",
            Mode::List => "L",
            Mode::Uncorrelated => "U",
        };
        write!(f, "{}", x)
    }
}

struct ModeError;

impl fmt::Display for ModeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "must be one of 'C', 'L', or 'U'")
    }
}

#[derive(Debug, Clone, Serialize)]
struct CoreText<M, P> {
    metadata: Metadata<M>,
    measurements: Vec<Measurement<P>>,
}

#[derive(Debug, Clone, Serialize)]
struct StdText<M, P, R> {
    // TODO this isn't necessary for writing and is redundant here
    data_offsets: Offsets,
    read_data: ReadData<R>,
    core: CoreText<M, P>,
}

#[derive(Debug)]
pub enum AnyStdTEXT {
    FCS2_0(Box<StdText2_0>),
    FCS3_0(Box<StdText3_0>),
    FCS3_1(Box<StdText3_1>),
    FCS3_2(Box<StdText3_2>),
}

impl AnyStdTEXT {
    pub fn print_meas_table(&self, delim: &str) {
        match self {
            AnyStdTEXT::FCS2_0(x) => x.print_meas_table(delim),
            AnyStdTEXT::FCS3_0(x) => x.print_meas_table(delim),
            AnyStdTEXT::FCS3_1(x) => x.print_meas_table(delim),
            AnyStdTEXT::FCS3_2(x) => x.print_meas_table(delim),
        }
    }

    pub fn print_spillover_table(&self, delim: &str) {
        let res = match self {
            AnyStdTEXT::FCS2_0(_) => None,
            AnyStdTEXT::FCS3_0(_) => None,
            AnyStdTEXT::FCS3_1(x) => x
                .core
                .metadata
                .specific
                .spillover
                .as_ref()
                .into_option()
                .map(|s| s.print_table(delim)),
            AnyStdTEXT::FCS3_2(x) => x
                .core
                .metadata
                .specific
                .spillover
                .as_ref()
                .into_option()
                .map(|s| s.print_table(delim)),
        };
        if res.is_none() {
            println!("None")
        }
    }
}

impl Serialize for AnyStdTEXT {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AnyStdTEXT", 2)?;
        match self {
            AnyStdTEXT::FCS2_0(x) => {
                state.serialize_field("version", &Version::FCS2_0)?;
                state.serialize_field("data", &x)?;
            }
            AnyStdTEXT::FCS3_0(x) => {
                state.serialize_field("version", &Version::FCS3_0)?;
                state.serialize_field("data", &x)?;
            }
            AnyStdTEXT::FCS3_1(x) => {
                state.serialize_field("version", &Version::FCS3_1)?;
                state.serialize_field("data", &x)?;
            }
            AnyStdTEXT::FCS3_2(x) => {
                state.serialize_field("version", &Version::FCS3_2)?;
                state.serialize_field("data", &x)?;
            }
        }
        state.end()
    }
}

#[derive(Debug)]
pub struct ParsedTEXT {
    pub header: Header,
    pub raw: RawTEXT,
    pub standard: AnyStdTEXT,
    data_parser: DataParser,
    pub nonfatal: NonFatalErrors,
}

type TEXTResult = Result<ParsedTEXT, Box<StandardErrors>>;

impl<M: VersionedMetadata> StdText<M, M::P, M::R> {
    fn get_shortnames(&self) -> Vec<&str> {
        self.core
            .measurements
            .iter()
            .filter_map(|p| M::P::measurement_name(p))
            .collect()
    }

    // TODO char should be validated somehow
    fn text_segment(&self, delim: char, data_len: usize) -> String {
        let ms: Vec<_> = self
            .core
            .measurements
            .iter()
            .enumerate()
            .flat_map(|(i, m)| m.keywords(&(i + 1).to_string()))
            .flat_map(|(k, v)| v.map(|x| (k, x)))
            .collect();
        let meas_len = ms.iter().map(|(k, v)| k.len() + v.len() + 2).sum();
        let len = KwLengths {
            data: data_len,
            measurements: meas_len,
        };
        // TODO properly populate tot/par here
        let mut meta: Vec<(String, String)> = self
            .core
            .metadata
            .keywords(0, 0, len)
            .into_iter()
            .flat_map(|(k, v)| v.map(|x| (String::from(k), x)))
            .chain(ms)
            .collect();

        meta.sort_by(|a, b| a.0.cmp(&b.0));

        let fin = meta
            .into_iter()
            .map(|(k, v)| format!("{}{}{}", k, delim, v))
            .join(&delim.to_string());
        format!("{fin}{delim}")
    }

    fn meas_table(&self, delim: &str) -> Vec<String> {
        let ms = &self.core.measurements;
        if ms.is_empty() {
            return vec![];
        }
        let header = ms[0].table_header().join(delim);
        let rows = self.core.measurements.iter().enumerate().map(|(i, m)| {
            m.table_row(i)
                .into_iter()
                .map(|v| v.unwrap_or(String::from("NA")))
                .join(delim)
        });
        vec![header].into_iter().chain(rows).collect()
    }

    fn print_meas_table(&self, delim: &str) {
        for e in self.meas_table(delim) {
            println!("{}", e);
        }
    }

    fn raw_to_std_text(header: Header, raw: RawTEXT, conf: &StdTextReader) -> TEXTResult {
        let mut st = raw.to_state(conf);
        // This will fail if a) not all required keywords pass and b) not all
        // required measurement keywords pass (according to $PAR)
        if let Some(s) = st
            .lookup_par()
            .and_then(|par| M::P::lookup_measurements(&mut st, par))
            .and_then(|ms| M::lookup_metadata(&mut st, &ms).map(|md| (ms, md)))
            .and_then(|(ms, md)| M::R::lookup(&mut st, ms.len()).map(|rd| (ms, md, rd)))
            .map(|(measurements, metadata, read_data)| StdText {
                data_offsets: header.data,
                read_data,
                core: CoreText {
                    metadata,
                    measurements,
                },
            })
        {
            M::validate(&mut st, &s);
            match M::build_data_parser(&mut st, &s) {
                Some(data_parser) => st.into_result(s, data_parser, header, raw),
                None => Err(Box::new(st.into_errors())),
            }
        } else {
            Err(Box::new(st.into_errors()))
        }
    }
}

type StdText2_0 = StdText<InnerMetadata2_0, InnerMeasurement2_0, InnerReadData2_0>;
type StdText3_0 = StdText<InnerMetadata3_0, InnerMeasurement3_0, InnerReadData3_0>;
type StdText3_1 = StdText<InnerMetadata3_1, InnerMeasurement3_1, InnerReadData3_0>;
type StdText3_2 = StdText<InnerMetadata3_2, InnerMeasurement3_2, InnerReadData3_2>;

trait OrderedFromBytes<const DTLEN: usize, const OLEN: usize>: NumProps<DTLEN> {
    fn read_from_ordered<R: Read>(h: &mut BufReader<R>, order: &[u8; OLEN]) -> io::Result<Self> {
        let mut tmp = [0; OLEN];
        let mut buf = [0; DTLEN];
        h.read_exact(&mut tmp)?;
        for (i, j) in order.iter().enumerate() {
            buf[usize::from(*j)] = tmp[i];
        }
        Ok(Self::from_little(buf))
    }
}

// TODO where to put this?
fn byteord_to_sized<const LEN: usize>(byteord: &ByteOrd) -> Result<SizedByteOrd<LEN>, String> {
    match byteord {
        ByteOrd::Endian(e) => Ok(SizedByteOrd::Endian(*e)),
        ByteOrd::Mixed(v) => v[..]
            .try_into()
            .map(|order: [u8; LEN]| SizedByteOrd::Order(order))
            .or(Err(String::from(
                "$BYTEORD is mixed but length is {v.len()} and not {INTLEN}",
            ))),
    }
}

trait IntFromBytes<const DTLEN: usize, const INTLEN: usize>:
    NumProps<DTLEN> + OrderedFromBytes<DTLEN, INTLEN> + Ord + IntMath
{
    fn byteord_to_sized(byteord: &ByteOrd) -> Result<SizedByteOrd<INTLEN>, String> {
        byteord_to_sized(byteord)
    }

    fn range_to_bitmask(range: &Range) -> Option<Self> {
        match range {
            Range::Float(_) => None,
            Range::Int(i) => Some(Self::next_power_2(Self::from_u64(*i))),
        }
    }

    fn to_col_parser(
        range: &Range,
        byteord: &ByteOrd,
        total_events: usize,
    ) -> Result<IntColumnParser<Self, INTLEN>, Vec<String>> {
        // TODO be more specific, which means we need the measurement index
        let b =
            Self::range_to_bitmask(range).ok_or(String::from("PnR is float for an integer column"));
        let s = Self::byteord_to_sized(byteord);
        let data = vec![Self::zero(); total_events];
        match (b, s) {
            (Ok(bitmask), Ok(size)) => Ok(IntColumnParser {
                bitmask,
                size,
                data,
            }),
            (Err(x), Err(y)) => Err(vec![x, y]),
            (Err(x), _) => Err(vec![x]),
            (_, Err(y)) => Err(vec![y]),
        }
    }

    fn read_int_masked<R: Read>(
        h: &mut BufReader<R>,
        byteord: &SizedByteOrd<INTLEN>,
        bitmask: Self,
    ) -> io::Result<Self> {
        Self::read_int(h, byteord).map(|x| x.min(bitmask))
    }

    fn read_int<R: Read>(h: &mut BufReader<R>, byteord: &SizedByteOrd<INTLEN>) -> io::Result<Self> {
        // This lovely code will read data that is not a power-of-two
        // bytes long. Start by reading n bytes into a vector, which can
        // take a varying size. Then copy this into the power of 2 buffer
        // and reset all the unused cells to 0. This copy has to go to one
        // or the other end of the buffer depending on endianness.
        //
        // ASSUME for u8 and u16 that these will get heavily optimized away
        // since 'order' is totally meaningless for u8 and the only two possible
        // 'orders' for u16 are big and little.
        let mut tmp = [0; INTLEN];
        let mut buf = [0; DTLEN];
        match byteord {
            SizedByteOrd::Endian(Endian::Big) => {
                let b = DTLEN - INTLEN;
                h.read_exact(&mut tmp)?;
                buf[b..].copy_from_slice(&tmp[b..]);
                Ok(Self::from_big(buf))
            }
            SizedByteOrd::Endian(Endian::Little) => {
                h.read_exact(&mut tmp)?;
                buf[..INTLEN].copy_from_slice(&tmp[..INTLEN]);
                Ok(Self::from_little(buf))
            }
            SizedByteOrd::Order(order) => Self::read_from_ordered(h, order),
        }
    }

    fn assign<R: Read>(
        h: &mut BufReader<R>,
        d: &mut IntColumnParser<Self, INTLEN>,
        row: usize,
    ) -> io::Result<()> {
        d.data[row] = Self::read_int_masked(h, &d.size, d.bitmask)?;
        Ok(())
    }
}

trait FloatFromBytes<const LEN: usize>:
    NumProps<LEN> + OrderedFromBytes<LEN, LEN> + Clone + NumProps<LEN>
{
    fn to_float_byteord(byteord: &ByteOrd) -> Result<SizedByteOrd<LEN>, String> {
        byteord_to_sized(byteord)
    }

    fn make_column_parser(endian: Endian, total_events: usize) -> FloatColumn<Self> {
        FloatColumn {
            data: vec![Self::zero(); total_events],
            endian,
        }
    }

    fn make_matrix_parser(
        byteord: &ByteOrd,
        par: usize,
        total_events: usize,
    ) -> Result<FloatParser<LEN>, String> {
        Self::to_float_byteord(byteord).map(|byteord| FloatParser {
            nrows: total_events,
            ncols: par,
            byteord,
        })
    }

    fn read_float<R: Read>(h: &mut BufReader<R>, byteord: &SizedByteOrd<LEN>) -> io::Result<Self> {
        let mut buf = [0; LEN];
        match byteord {
            SizedByteOrd::Endian(Endian::Big) => {
                h.read_exact(&mut buf)?;
                Ok(Self::from_big(buf))
            }
            SizedByteOrd::Endian(Endian::Little) => {
                h.read_exact(&mut buf)?;
                Ok(Self::from_little(buf))
            }
            SizedByteOrd::Order(order) => Self::read_from_ordered(h, order),
        }
    }

    fn assign_column<R: Read>(
        h: &mut BufReader<R>,
        column: &mut FloatColumn<Self>,
        row: usize,
    ) -> io::Result<()> {
        // TODO endian wrap thing seems unnecessary
        column.data[row] = Self::read_float(h, &SizedByteOrd::Endian(column.endian))?;
        Ok(())
    }

    fn assign_matrix<R: Read + Seek>(
        h: &mut BufReader<R>,
        d: &FloatParser<LEN>,
        column: &mut [Self],
        row: usize,
    ) -> io::Result<()> {
        column[row] = Self::read_float(h, &d.byteord)?;
        Ok(())
    }

    fn parse_matrix<R: Read + Seek>(
        h: &mut BufReader<R>,
        p: FloatParser<LEN>,
    ) -> io::Result<Vec<Series>> {
        let mut columns: Vec<_> = iter::repeat_with(|| vec![Self::zero(); p.nrows])
            .take(p.ncols)
            .collect();
        for r in 0..p.nrows {
            for c in columns.iter_mut() {
                Self::assign_matrix(h, &p, c, r)?;
            }
        }
        Ok(columns.into_iter().map(Self::into_series).collect())
    }
}

impl OrderedFromBytes<1, 1> for u8 {}
impl OrderedFromBytes<2, 2> for u16 {}
impl OrderedFromBytes<4, 3> for u32 {}
impl OrderedFromBytes<4, 4> for u32 {}
impl OrderedFromBytes<8, 5> for u64 {}
impl OrderedFromBytes<8, 6> for u64 {}
impl OrderedFromBytes<8, 7> for u64 {}
impl OrderedFromBytes<8, 8> for u64 {}
impl OrderedFromBytes<4, 4> for f32 {}
impl OrderedFromBytes<8, 8> for f64 {}

impl FloatFromBytes<4> for f32 {}
impl FloatFromBytes<8> for f64 {}

impl IntFromBytes<1, 1> for u8 {}
impl IntFromBytes<2, 2> for u16 {}
impl IntFromBytes<4, 3> for u32 {}
impl IntFromBytes<4, 4> for u32 {}
impl IntFromBytes<8, 5> for u64 {}
impl IntFromBytes<8, 6> for u64 {}
impl IntFromBytes<8, 7> for u64 {}
impl IntFromBytes<8, 8> for u64 {}

#[derive(Debug)]
struct FloatParser<const LEN: usize> {
    nrows: usize,
    ncols: usize,
    byteord: SizedByteOrd<LEN>,
}

#[derive(Debug)]
struct AsciiColumn {
    data: Vec<f64>,
    width: u8,
}

#[derive(Debug)]
struct FloatColumn<T> {
    data: Vec<T>,
    endian: Endian,
}

#[derive(Debug)]
enum MixedColumnType {
    Ascii(AsciiColumn),
    Uint(AnyIntColumn),
    Single(FloatColumn<f32>),
    Double(FloatColumn<f64>),
}

impl MixedColumnType {
    fn into_series(self) -> Series {
        match self {
            MixedColumnType::Ascii(x) => f64::into_series(x.data),
            MixedColumnType::Single(x) => f32::into_series(x.data),
            MixedColumnType::Double(x) => f64::into_series(x.data),
            MixedColumnType::Uint(x) => x.into_series(),
        }
    }
}

#[derive(Debug)]
struct MixedParser {
    nrows: usize,
    columns: Vec<MixedColumnType>,
}

#[derive(Debug)]
enum SizedByteOrd<const LEN: usize> {
    Endian(Endian),
    Order([u8; LEN]),
}

#[derive(Debug)]
struct IntColumnParser<B, const LEN: usize> {
    bitmask: B,
    size: SizedByteOrd<LEN>,
    data: Vec<B>,
}

#[derive(Debug)]
enum AnyIntColumn {
    Uint8(IntColumnParser<u8, 1>),
    Uint16(IntColumnParser<u16, 2>),
    Uint24(IntColumnParser<u32, 3>),
    Uint32(IntColumnParser<u32, 4>),
    Uint40(IntColumnParser<u64, 5>),
    Uint48(IntColumnParser<u64, 6>),
    Uint56(IntColumnParser<u64, 7>),
    Uint64(IntColumnParser<u64, 8>),
}

impl AnyIntColumn {
    fn into_series(self) -> Series {
        match self {
            AnyIntColumn::Uint8(y) => u8::into_series(y.data),
            AnyIntColumn::Uint16(y) => u16::into_series(y.data),
            AnyIntColumn::Uint24(y) => u32::into_series(y.data),
            AnyIntColumn::Uint32(y) => u32::into_series(y.data),
            AnyIntColumn::Uint40(y) => u64::into_series(y.data),
            AnyIntColumn::Uint48(y) => u64::into_series(y.data),
            AnyIntColumn::Uint56(y) => u64::into_series(y.data),
            AnyIntColumn::Uint64(y) => u64::into_series(y.data),
        }
    }

    fn assign<R: Read>(&mut self, h: &mut BufReader<R>, r: usize) -> io::Result<()> {
        match self {
            AnyIntColumn::Uint8(d) => u8::assign(h, d, r)?,
            AnyIntColumn::Uint16(d) => u16::assign(h, d, r)?,
            AnyIntColumn::Uint24(d) => u32::assign(h, d, r)?,
            AnyIntColumn::Uint32(d) => u32::assign(h, d, r)?,
            AnyIntColumn::Uint40(d) => u64::assign(h, d, r)?,
            AnyIntColumn::Uint48(d) => u64::assign(h, d, r)?,
            AnyIntColumn::Uint56(d) => u64::assign(h, d, r)?,
            AnyIntColumn::Uint64(d) => u64::assign(h, d, r)?,
        }
        Ok(())
    }
}

// Integers are complicated because in each version we need to at least deal
// with the possibility that each column has a different bitmask. In addition,
// 3.1+ allows for different widths (even though this likely is used seldom
// if ever) so each series can potentially be a different type. Finally,
// BYTEORD further complicates this because unlike floats which can only have
// widths of 4 or 8 bytes, integers can have any number of bytes up to their
// next power of 2 data type. For example, some cytometers store their values
// in 3-byte segments, which would need to be stored in u32 but are read as
// triples, which in theory could be any byte order.
//
// There may be some small optimizations we can make for the "typical" cases
// where the entire file is u32 with big/little BYTEORD and only a handful
// of different bitmasks. For now, the increased complexity of dealing with this
// is likely no worth it.
#[derive(Debug)]
struct IntParser {
    nrows: usize,
    columns: Vec<AnyIntColumn>,
}

#[derive(Debug)]
struct FixedAsciiParser {
    columns: Vec<u8>,
    nrows: usize,
}

#[derive(Debug)]
struct DelimAsciiParser {
    ncols: usize,
    nrows: Option<usize>,
    nbytes: usize,
}

#[derive(Debug)]
enum ColumnParser {
    // DATATYPE=A where all PnB = *
    DelimitedAscii(DelimAsciiParser),
    // DATATYPE=A where all PnB = number
    FixedWidthAscii(FixedAsciiParser),
    // DATATYPE=F (with no overrides in 3.2+)
    Single(FloatParser<4>),
    // DATATYPE=D (with no overrides in 3.2+)
    Double(FloatParser<8>),
    // DATATYPE=I this is complicated so see struct definition
    Int(IntParser),
    // Mixed column types (3.2+)
    Mixed(MixedParser),
}

#[derive(Debug)]
struct DataParser {
    column_parser: ColumnParser,
    begin: u64,
}

type ParsedData = Vec<Series>;

fn format_parsed_data(res: &FCSSuccess, delim: &str) -> Vec<String> {
    let shortnames = match &res.std {
        AnyStdTEXT::FCS2_0(x) => x.get_shortnames(),
        AnyStdTEXT::FCS3_0(x) => x.get_shortnames(),
        AnyStdTEXT::FCS3_1(x) => x.get_shortnames(),
        AnyStdTEXT::FCS3_2(x) => x.get_shortnames(),
    };
    if res.data.is_empty() {
        return vec![];
    }
    let mut buf = vec![];
    let mut lines = vec![];
    let nrows = res.data[0].len();
    let ncols = res.data.len();
    // ASSUME names is the same length as columns
    lines.push(shortnames.join(delim));
    for r in 0..nrows {
        buf.clear();
        for c in 0..ncols {
            buf.push(res.data[c].format(r));
        }
        lines.push(buf.join(delim));
    }
    lines
}

pub fn print_parsed_data(res: &FCSSuccess, delim: &str) {
    for x in format_parsed_data(res, delim) {
        println!("{}", x);
    }
}

fn ascii_to_float_io(buf: Vec<u8>) -> io::Result<f64> {
    String::from_utf8(buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        .and_then(|s| parse_f64_io(&s))
}

fn parse_f64_io(s: &str) -> io::Result<f64> {
    s.parse::<f64>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn read_data_delim_ascii<R: Read>(
    h: &mut BufReader<R>,
    p: DelimAsciiParser,
) -> io::Result<ParsedData> {
    let mut buf = Vec::new();
    let mut row = 0;
    let mut col = 0;
    let mut last_was_delim = false;
    // Delimiters are tab, newline, carriage return, space, or comma. Any
    // consecutive delimiter counts as one, and delimiters can be mixed.
    let is_delim = |byte| byte == 9 || byte == 10 || byte == 13 || byte == 32 || byte == 44;
    // FCS 2.0 files have an optional $TOT field, which complicates this a bit
    if let Some(nrows) = p.nrows {
        let mut data: Vec<_> = iter::repeat_with(|| vec![0.0; nrows])
            .take(p.ncols)
            .collect();
        for b in h.bytes().take(p.nbytes) {
            let byte = b?;
            // exit if we encounter more rows than expected.
            if row == nrows {
                let msg = format!("Exceeded expected number of rows: {nrows}");
                return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
            }
            if is_delim(byte) {
                if !last_was_delim {
                    last_was_delim = true;
                    // TODO this will spaz out if we end up reading more
                    // rows than expected
                    data[col][row] = ascii_to_float_io(buf.clone())?;
                    buf.clear();
                    if col == p.ncols - 1 {
                        col = 0;
                        row += 1;
                    } else {
                        col += 1;
                    }
                }
            } else {
                buf.push(byte);
            }
        }
        // The spec isn't clear if the last value should be a delim or
        // not, so flush the buffer if it has anything in it since we
        // only try to parse if we hit a delim above.
        if !buf.is_empty() {
            data[col][row] = ascii_to_float_io(buf.clone())?;
        }
        if !(col == 0 && row == nrows) {
            let msg = format!(
                "Parsing ended in column {col} and row {row}, \
                               where expected number of rows is {nrows}"
            );
            return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
        }
        Ok(data.into_iter().map(f64::into_series).collect())
    } else {
        let mut data: Vec<_> = iter::repeat_with(Vec::new).take(p.ncols).collect();
        for b in h.bytes().take(p.nbytes) {
            let byte = b?;
            // Delimiters are tab, newline, carriage return, space, or
            // comma. Any consecutive delimiter counts as one, and
            // delimiters can be mixed.
            if is_delim(byte) {
                if !last_was_delim {
                    last_was_delim = true;
                    data[col].push(ascii_to_float_io(buf.clone())?);
                    buf.clear();
                    if col == p.ncols - 1 {
                        col = 0;
                    } else {
                        col += 1;
                    }
                }
            } else {
                buf.push(byte);
            }
        }
        // The spec isn't clear if the last value should be a delim or
        // not, so flush the buffer if it has anything in it since we
        // only try to parse if we hit a delim above.
        if !buf.is_empty() {
            data[col][row] = ascii_to_float_io(buf.clone())?;
        }
        // Scream if not all columns are equal in length
        if data.iter().map(|c| c.len()).unique().count() > 1 {
            let msg = "Not all columns are equal length";
            return Err(io::Error::new(io::ErrorKind::InvalidData, msg));
        }
        Ok(data.into_iter().map(f64::into_series).collect())
    }
}

fn read_data_ascii_fixed<R: Read>(
    h: &mut BufReader<R>,
    parser: &FixedAsciiParser,
) -> io::Result<ParsedData> {
    let ncols = parser.columns.len();
    let mut data: Vec<_> = iter::repeat_with(|| vec![0.0; parser.nrows])
        .take(ncols)
        .collect();
    let mut buf = String::new();
    for r in 0..parser.nrows {
        for (c, width) in parser.columns.iter().enumerate() {
            buf.clear();
            h.take(u64::from(*width)).read_to_string(&mut buf)?;
            data[c][r] = parse_f64_io(&buf)?;
        }
    }
    Ok(data.into_iter().map(f64::into_series).collect())
}

fn read_data_mixed<R: Read>(h: &mut BufReader<R>, parser: MixedParser) -> io::Result<ParsedData> {
    let mut p = parser;
    let mut strbuf = String::new();
    for r in 0..p.nrows {
        for c in p.columns.iter_mut() {
            match c {
                MixedColumnType::Single(t) => f32::assign_column(h, t, r)?,
                MixedColumnType::Double(t) => f64::assign_column(h, t, r)?,
                MixedColumnType::Uint(u) => u.assign(h, r)?,
                MixedColumnType::Ascii(d) => {
                    strbuf.clear();
                    h.take(u64::from(d.width)).read_to_string(&mut strbuf)?;
                    d.data[r] = parse_f64_io(&strbuf)?;
                }
            }
        }
    }
    Ok(p.columns.into_iter().map(|c| c.into_series()).collect())
}

fn read_data_int<R: Read>(h: &mut BufReader<R>, parser: IntParser) -> io::Result<ParsedData> {
    let mut p = parser;
    for r in 0..p.nrows {
        for c in p.columns.iter_mut() {
            c.assign(h, r)?;
        }
    }
    Ok(p.columns.into_iter().map(|c| c.into_series()).collect())
}

fn read_data<R: Read + Seek>(h: &mut BufReader<R>, parser: DataParser) -> io::Result<ParsedData> {
    h.seek(SeekFrom::Start(parser.begin))?;
    match parser.column_parser {
        ColumnParser::DelimitedAscii(p) => read_data_delim_ascii(h, p),
        ColumnParser::FixedWidthAscii(p) => read_data_ascii_fixed(h, &p),
        ColumnParser::Single(p) => f32::parse_matrix(h, p),
        ColumnParser::Double(p) => f64::parse_matrix(h, p),
        ColumnParser::Mixed(p) => read_data_mixed(h, p),
        ColumnParser::Int(p) => read_data_int(h, p),
    }
}

enum EventWidth {
    Finite(Vec<u8>),
    Variable,
    Error(Vec<usize>, Vec<usize>),
}

type MaybeKeyword = (&'static str, Option<String>);

type MaybeKeywords = Vec<MaybeKeyword>;

/// Used to hold critical lengths when calculating the pad for $BEGIN/ENDDATA.
struct KwLengths {
    /// Length of the entire DATA segment when written.
    data: usize,
    /// Length of all the measurement keywords in the TEXT segment.
    ///
    /// This is computed as the sum of all string lengths of each key and
    /// value plus 2*P (number of measurements) which captures the length
    /// of the two delimiters b/t the key and value and the key and previous
    /// value.
    measurements: usize,
}

fn sum_keywords(kws: &[MaybeKeyword]) -> usize {
    kws.iter()
        .map(|(k, v)| v.as_ref().map(|y| y.len() + k.len() + 2).unwrap_or(0))
        .sum()
}

fn n_digits(x: f64) -> f64 {
    // ASSUME this is effectively only going to be used on the u32 range
    // starting at 1; keep in f64 space to minimize casts
    f64::log10(x).floor() + 1.0
}

fn compute_data_offsets(textlen: u32, datalen: u32) -> (u32, u32) {
    let d = f64::from(datalen);
    let t = f64::from(textlen);
    let mut datastart = t;
    let mut dataend = datastart + d;
    let mut ndigits_start = n_digits(datastart);
    let mut ndigits_end = n_digits(dataend);
    let mut tmp_start;
    let mut tmp_end;
    loop {
        datastart = ndigits_start + ndigits_end + t;
        dataend = datastart + d;
        tmp_start = n_digits(datastart);
        tmp_end = n_digits(dataend);
        if tmp_start == ndigits_start && tmp_end == ndigits_end {
            return (datastart as u32, dataend as u32);
        } else {
            ndigits_start = tmp_start;
            ndigits_end = tmp_end;
        }
    }
}

// ASSUME header is always this length
const HEADER_LEN: usize = 58;

// length of BEGIN/ENDDATA keywords (without values), + 4 to account for
// delimiters
const DATALEN_NO_VAL: usize = BEGINDATA.len() + ENDDATA.len() + 4;

fn make_data_offset_keywords(other_textlen: usize, datalen: usize) -> [MaybeKeyword; 2] {
    // add everything up, + 1 at the end to account for the delimiter at
    // the end of TEXT
    let textlen = HEADER_LEN + DATALEN_NO_VAL + other_textlen + 1;
    let (datastart, dataend) = compute_data_offsets(textlen as u32, datalen as u32);
    [
        (BEGINDATA, Some(datastart.to_string())),
        (ENDDATA, Some(dataend.to_string())),
    ]
}

trait VersionedMetadata: Sized {
    type P: VersionedMeasurement;
    type R: VersionedReadData;

    fn into_any_text(s: Box<StdText<Self, Self::P, Self::R>>) -> AnyStdTEXT;

    fn get_data_offsets(s: &StdText<Self, Self::P, Self::R>) -> Offsets;

    fn get_byteord(&self) -> ByteOrd;

    // fn get_tot(r: Self::R) -> Option<u32>;

    fn has_timestep(&self) -> bool;

    fn event_width(s: &StdText<Self, Self::P, Self::R>) -> EventWidth {
        let (fixed, variable_indices): (Vec<_>, Vec<_>) = s
            .core
            .measurements
            .iter()
            .enumerate()
            .map(|(i, p)| match p.bytes {
                Bytes::Fixed(b) => Ok((i, b)),
                Bytes::Variable => Err(i),
            })
            .partition_result();
        let (fixed_indices, fixed_bytes): (Vec<_>, Vec<_>) = fixed.into_iter().unzip();
        if variable_indices.is_empty() {
            EventWidth::Finite(fixed_bytes)
        } else if fixed_indices.is_empty() {
            EventWidth::Variable
        } else {
            EventWidth::Error(fixed_indices, variable_indices)
        }
    }

    fn total_events(
        st: &mut KwState,
        s: &StdText<Self, Self::P, Self::R>,
        event_width: u32,
    ) -> Option<usize> {
        let nbytes = Self::get_data_offsets(s).num_bytes();
        let remainder = nbytes % event_width;
        let res = nbytes / event_width;
        let total_events = if nbytes % event_width > 0 {
            let msg = format!(
                "Events are {event_width} bytes wide, but this does not evenly \
                 divide DATA segment which is {nbytes} bytes long \
                 (remainder of {remainder})"
            );
            if st.conf.raw.enfore_data_width_divisibility {
                st.push_meta_error(msg);
                None
            } else {
                st.push_meta_warning(msg);
                Some(res)
            }
        } else {
            Some(res)
        };
        total_events.and_then(|x| {
            if let Some(tot) = s.read_data.specific.get_tot() {
                if x != tot {
                    let msg = format!(
                        "$TOT field is {tot} but number of events \
                         that evenly fit into DATA is {x}"
                    );
                    if st.conf.raw.enfore_matching_tot {
                        st.push_meta_error(msg);
                    } else {
                        st.push_meta_warning(msg);
                    }
                }
            }
            usize::try_from(x).ok()
        })
    }

    fn build_int_parser(
        &self,
        st: &mut KwState,
        ps: &[Measurement<Self::P>],
        total_events: usize,
    ) -> Option<IntParser>;

    fn build_mixed_parser(
        &self,
        st: &mut KwState,
        ps: &[Measurement<Self::P>],
        dt: &AlphaNumType,
        total_events: usize,
    ) -> Option<Option<MixedParser>>;

    fn build_float_parser(
        &self,
        st: &mut KwState,
        is_double: bool,
        par: usize,
        total_events: usize,
        ps: &[Measurement<Self::P>],
    ) -> Option<ColumnParser> {
        let (bytes, dt) = if is_double { (8, "D") } else { (4, "F") };
        let remainder: Vec<_> = ps.iter().filter(|p| p.bytes_eq(bytes)).collect();
        if remainder.is_empty() {
            let byteord = &self.get_byteord();
            let res = if is_double {
                f64::make_matrix_parser(byteord, par, total_events).map(ColumnParser::Double)
            } else {
                f32::make_matrix_parser(byteord, par, total_events).map(ColumnParser::Single)
            };
            match res {
                Ok(x) => Some(x),
                Err(e) => {
                    st.push_meta_error(e);
                    None
                }
            }
        } else {
            for e in remainder.iter().enumerate().map(|(i, p)| {
                format!(
                    "Measurment {} uses {} bytes but DATATYPE={}",
                    i, p.bytes, dt
                )
            }) {
                st.push_meta_error(e);
            }
            None
        }
    }

    fn build_fixed_width_parser(
        st: &mut KwState,
        s: &StdText<Self, Self::P, Self::R>,
        total_events: usize,
        measurement_widths: Vec<u8>,
    ) -> Option<ColumnParser> {
        // TODO fix cast?
        let par = s.read_data.par as usize;
        let ps = &s.core.measurements;
        let dt = &s.core.metadata.datatype;
        let specific = &s.core.metadata.specific;
        if let Some(mixed) = Self::build_mixed_parser(specific, st, ps, dt, total_events) {
            mixed.map(ColumnParser::Mixed)
        } else {
            match dt {
                AlphaNumType::Single => {
                    specific.build_float_parser(st, false, par, total_events, ps)
                }
                AlphaNumType::Double => {
                    specific.build_float_parser(st, true, par, total_events, ps)
                }
                AlphaNumType::Integer => specific
                    .build_int_parser(st, ps, total_events)
                    .map(ColumnParser::Int),
                AlphaNumType::Ascii => Some(ColumnParser::FixedWidthAscii(FixedAsciiParser {
                    columns: measurement_widths,
                    nrows: total_events,
                })),
            }
        }
    }

    fn build_delim_ascii_parser(
        s: &StdText<Self, Self::P, Self::R>,
        tot: Option<usize>,
    ) -> ColumnParser {
        let nbytes = Self::get_data_offsets(s).num_bytes();
        ColumnParser::DelimitedAscii(DelimAsciiParser {
            ncols: s.read_data.par as usize,
            nrows: tot,
            nbytes: nbytes as usize,
        })
    }

    fn build_column_parser(
        st: &mut KwState,
        s: &StdText<Self, Self::P, Self::R>,
    ) -> Option<ColumnParser> {
        // In order to make a data parser, the $DATATYPE, $BYTEORD, $PnB, and
        // $PnDATATYPE (if present) all need to be a specific relationship to
        // each other, each of which corresponds to the options below.
        if s.core.metadata.datatype == AlphaNumType::Ascii
            && Self::P::fcs_version() >= Version::FCS3_1
        {
            st.push_meta_deprecated_str("$DATATYPE=A has been deprecated since FCS 3.1");
        }
        match (Self::event_width(s), s.core.metadata.datatype) {
            // Numeric/Ascii (fixed width)
            (EventWidth::Finite(measurement_widths), _) => {
                let event_width = measurement_widths.iter().map(|x| u32::from(*x)).sum();
                Self::total_events(st, s, event_width).and_then(|total_events| {
                    Self::build_fixed_width_parser(st, s, total_events, measurement_widths)
                })
            }
            // Ascii (variable width)
            (EventWidth::Variable, AlphaNumType::Ascii) => {
                let tot = s.read_data.specific.get_tot();
                Some(Self::build_delim_ascii_parser(s, tot.map(|x| x as usize)))
            }
            // nonsense...scream at user
            (EventWidth::Error(fixed, variable), _) => {
                st.push_meta_error_str("$PnBs are a mix of numeric and variable");
                for f in fixed {
                    st.push_meta_error(format!("$PnB for measurement {f} is numeric"));
                }
                for v in variable {
                    st.push_meta_error(format!("$PnB for measurement {v} is variable"));
                }
                None
            }
            (EventWidth::Variable, dt) => {
                st.push_meta_error(format!("$DATATYPE is {dt} but all $PnB are '*'"));
                None
            }
        }
    }

    fn build_data_parser(
        st: &mut KwState,
        s: &StdText<Self, Self::P, Self::R>,
    ) -> Option<DataParser> {
        Self::build_column_parser(st, s).map(|column_parser| DataParser {
            column_parser,
            begin: u64::from(Self::get_data_offsets(s).begin),
        })
    }

    fn validate_specific(st: &mut KwState, s: &StdText<Self, Self::P, Self::R>);

    // TODO make sure measurement type is correct
    fn validate_time_channel(st: &mut KwState, s: &StdText<Self, Self::P, Self::R>) {
        if let Some(time_name) = st.conf.time_shortname.as_ref() {
            if let Some(tc) =
                s.core
                    .measurements
                    .iter()
                    .find(|p| match Self::P::measurement_name(p) {
                        Some(n) => n == time_name,
                        _ => false,
                    })
            {
                if !tc.specific.has_linear_scale() {
                    st.push_meta_error_or_warning(
                        st.conf.ensure_time_linear,
                        String::from("Time channel must have linear $PnE"),
                    );
                }
                if tc.specific.has_gain() {
                    st.push_meta_error_or_warning(
                        st.conf.ensure_time_nogain,
                        String::from("Time channel must not have $PnG"),
                    );
                }
                if !s.core.metadata.specific.has_timestep() {
                    st.push_meta_error_or_warning(
                        st.conf.ensure_time_timestep,
                        String::from("$TIMESTEP must be present if time channel given"),
                    );
                }
            } else {
                st.push_meta_error_or_warning(
                    st.conf.ensure_time,
                    format!("Channel called '{time_name}' not found for time"),
                );
            }
        }
    }

    fn validate(st: &mut KwState, s: &StdText<Self, Self::P, Self::R>) {
        // ensure time channel is valid if present
        Self::validate_time_channel(st, s);

        // do any version-specific validation
        Self::validate_specific(st, s);
    }

    fn build_inner(st: &mut KwState, par: usize, names: &HashSet<&str>) -> Option<Self>;

    fn lookup_metadata(st: &mut KwState, ms: &[Measurement<Self::P>]) -> Option<Metadata<Self>> {
        let names: HashSet<_> = ms
            .iter()
            .filter_map(|m| Self::P::measurement_name(m))
            .collect();
        let par = ms.len();
        Some(Metadata {
            // par,
            // nextdata: st.lookup_nextdata()?,
            datatype: st.lookup_datatype()?,
            abrt: st.lookup_abrt(),
            com: st.lookup_com(),
            cells: st.lookup_cells(),
            exp: st.lookup_exp(),
            fil: st.lookup_fil(),
            inst: st.lookup_inst(),
            lost: st.lookup_lost(),
            op: st.lookup_op(),
            proj: st.lookup_proj(),
            smno: st.lookup_smno(),
            src: st.lookup_src(),
            sys: st.lookup_sys(),
            tr: st.lookup_trigger_checked(&names),
            specific: Self::build_inner(st, par, &names)?,
        })
    }

    fn keywords_inner(&self, other_textlen: usize, data_len: usize) -> MaybeKeywords;

    fn keywords(m: &Metadata<Self>, par: usize, tot: usize, len: KwLengths) -> MaybeKeywords {
        let fixed = [
            (PAR, Some(par.to_string())),
            (TOT, Some(tot.to_string())),
            // (NEXTDATA, Some(m.nextdata.to_string())),
            (NEXTDATA, Some("0".to_string())),
            (DATATYPE, Some(m.datatype.to_string())),
            (ABRT, m.abrt.as_opt_string()),
            (COM, m.com.as_opt_string()),
            (CELLS, m.cells.as_opt_string()),
            (EXP, m.exp.as_opt_string()),
            (FIL, m.fil.as_opt_string()),
            (INST, m.inst.as_opt_string()),
            (LOST, m.lost.as_opt_string()),
            (OP, m.op.as_opt_string()),
            (PROJ, m.proj.as_opt_string()),
            (SMNO, m.smno.as_opt_string()),
            (SRC, m.src.as_opt_string()),
            (SYS, m.sys.as_opt_string()),
            (TR, m.tr.as_opt_string()),
        ];
        let fixed_len = sum_keywords(&fixed) + len.measurements;
        fixed
            .into_iter()
            .chain(m.specific.keywords_inner(fixed_len, len.data))
            .collect()
    }
}

fn build_int_parser_2_0<P: VersionedMeasurement>(
    st: &mut KwState,
    byteord: &ByteOrd,
    ps: &[Measurement<P>],
    total_events: usize,
) -> Option<IntParser> {
    let nbytes = byteord.num_bytes();
    let remainder: Vec<_> = ps.iter().filter(|p| !p.bytes_eq(nbytes)).collect();
    if remainder.is_empty() {
        let (columns, fail): (Vec<_>, Vec<_>) = ps
            .iter()
            .map(|p| p.make_int_parser(byteord, total_events))
            .partition_result();
        let errors: Vec<_> = fail.into_iter().flatten().collect();
        if errors.is_empty() {
            Some(IntParser {
                columns,
                nrows: total_events,
            })
        } else {
            for e in errors.into_iter() {
                st.push_meta_error(e);
            }
            None
        }
    } else {
        for e in remainder.iter().enumerate().map(|(i, p)| {
            format!(
                "Measurement {} uses {} bytes when DATATYPE=I \
                         and BYTEORD implies {} bytes",
                i, p.bytes, nbytes
            )
        }) {
            st.push_meta_error(e);
        }
        None
    }
}

impl VersionedMetadata for InnerMetadata2_0 {
    type P = InnerMeasurement2_0;
    type R = InnerReadData2_0;

    fn into_any_text(t: Box<StdText2_0>) -> AnyStdTEXT {
        AnyStdTEXT::FCS2_0(t)
    }

    fn get_data_offsets(s: &StdText<Self, Self::P, Self::R>) -> Offsets {
        s.data_offsets
    }

    fn get_byteord(&self) -> ByteOrd {
        self.byteord.clone()
    }

    fn has_timestep(&self) -> bool {
        false
    }

    fn build_int_parser(
        &self,
        st: &mut KwState,
        ps: &[Measurement<Self::P>],
        total_events: usize,
    ) -> Option<IntParser> {
        build_int_parser_2_0(st, &self.byteord, ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        _: &mut KwState,
        _: &[Measurement<Self::P>],
        _: &AlphaNumType,
        _: usize,
    ) -> Option<Option<MixedParser>> {
        None
    }

    fn validate_specific(_: &mut KwState, _: &StdText<Self, Self::P, Self::R>) {}

    fn build_inner(st: &mut KwState, par: usize, _: &HashSet<&str>) -> Option<InnerMetadata2_0> {
        Some(InnerMetadata2_0 {
            // tot: st.lookup_tot_opt(),
            mode: st.lookup_mode()?,
            byteord: st.lookup_byteord()?,
            cyt: st.lookup_cyt_opt(),
            comp: st.lookup_compensation_2_0(par as usize),
            timestamps: st.lookup_timestamps2_0(),
        })
    }

    fn keywords_inner(&self, _: usize, _: usize) -> MaybeKeywords {
        [
            // (TOT, self.tot.as_opt_string()),
            (MODE, Some(self.mode.to_string())),
            (BYTEORD, Some(self.byteord.to_string())),
            (CYT, self.cyt.as_opt_string()),
            (COMP, self.comp.as_opt_string()),
            (BTIM, self.timestamps.btim.as_opt_string()),
            (ETIM, self.timestamps.etim.as_opt_string()),
            (DATE, self.timestamps.date.as_opt_string()),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedMetadata for InnerMetadata3_0 {
    type P = InnerMeasurement3_0;
    type R = InnerReadData3_0;

    fn into_any_text(t: Box<StdText3_0>) -> AnyStdTEXT {
        AnyStdTEXT::FCS3_0(t)
    }

    fn get_data_offsets(s: &StdText<Self, Self::P, Self::R>) -> Offsets {
        let header_offsets = s.data_offsets;
        if header_offsets.is_unset() {
            s.read_data.specific.data
        } else {
            header_offsets
        }
    }

    fn get_byteord(&self) -> ByteOrd {
        self.byteord.clone()
    }

    fn has_timestep(&self) -> bool {
        self.timestep.as_ref().into_option().is_some()
    }

    fn build_int_parser(
        &self,
        st: &mut KwState,
        ps: &[Measurement<Self::P>],
        total_events: usize,
    ) -> Option<IntParser> {
        build_int_parser_2_0(st, &self.byteord, ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        _: &mut KwState,
        _: &[Measurement<Self::P>],
        _: &AlphaNumType,
        _: usize,
    ) -> Option<Option<MixedParser>> {
        None
    }

    fn validate_specific(_: &mut KwState, _: &StdText<Self, Self::P, Self::R>) {}

    fn build_inner(st: &mut KwState, _: usize, _: &HashSet<&str>) -> Option<InnerMetadata3_0> {
        Some(InnerMetadata3_0 {
            // data: st.lookup_data_offsets()?,
            // supplemental: st.lookup_supplemental3_0()?,
            // tot: st.lookup_tot_req()?,
            mode: st.lookup_mode()?,
            byteord: st.lookup_byteord()?,
            cyt: st.lookup_cyt_opt(),
            comp: st.lookup_compensation_3_0(),
            timestamps: st.lookup_timestamps3_0(),
            cytsn: st.lookup_cytsn(),
            timestep: st.lookup_timestep(),
            unicode: st.lookup_unicode(),
        })
    }

    fn keywords_inner(&self, other_textlen: usize, data_len: usize) -> MaybeKeywords {
        // let anal = self.supplemental.analysis;
        // let stext = self.supplemental.stext;
        let ts = &self.timestamps;
        let kws = [
            // (BEGINANALYSIS, Some(anal.begin.to_string())),
            // (ENDANALYSIS, Some(anal.end.to_string())),
            // (BEGINSTEXT, Some(stext.begin.to_string())),
            // (ENDSTEXT, Some(stext.end.to_string())),
            // (TOT, Some(tot.to_string())),
            (MODE, Some(self.mode.to_string())),
            (BYTEORD, Some(self.byteord.to_string())),
            (CYT, self.cyt.as_opt_string()),
            (COMP, self.comp.as_opt_string()),
            (BTIM, ts.btim.as_opt_string()),
            (ETIM, ts.etim.as_opt_string()),
            (DATE, ts.date.as_opt_string()),
            (CYTSN, self.cytsn.as_opt_string()),
            (TIMESTEP, self.timestep.as_opt_string()),
            (UNICODE, self.unicode.as_opt_string()),
        ];
        let text_len = other_textlen + sum_keywords(&kws);
        make_data_offset_keywords(text_len, data_len)
            .into_iter()
            .chain(kws)
            .collect()
    }
}

fn validate_spillover(st: &mut KwState, spillover: &Spillover, names: &HashSet<&str>) {
    for m in spillover.measurements.iter() {
        if !names.contains(m.as_str()) {
            st.push_meta_error(format!(
                "$SPILLOVER refers to non-existent measurement: {m}"
            ));
        }
    }
}

impl VersionedMetadata for InnerMetadata3_1 {
    type P = InnerMeasurement3_1;
    type R = InnerReadData3_0;

    fn into_any_text(t: Box<StdText3_1>) -> AnyStdTEXT {
        AnyStdTEXT::FCS3_1(t)
    }

    fn get_data_offsets(s: &StdText<Self, Self::P, Self::R>) -> Offsets {
        let header_offsets = s.data_offsets;
        if header_offsets.is_unset() {
            s.read_data.specific.data
        } else {
            header_offsets
        }
    }

    fn get_byteord(&self) -> ByteOrd {
        ByteOrd::Endian(self.byteord)
    }

    fn has_timestep(&self) -> bool {
        self.timestep.as_ref().into_option().is_some()
    }

    fn build_int_parser(
        &self,
        st: &mut KwState,
        ps: &[Measurement<Self::P>],
        total_events: usize,
    ) -> Option<IntParser> {
        build_int_parser_2_0(st, &ByteOrd::Endian(self.byteord), ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        _: &mut KwState,
        _: &[Measurement<Self::P>],
        _: &AlphaNumType,
        _: usize,
    ) -> Option<Option<MixedParser>> {
        None
    }

    fn validate_specific(_: &mut KwState, _: &StdText<Self, Self::P, Self::R>) {}

    fn build_inner(st: &mut KwState, _: usize, names: &HashSet<&str>) -> Option<InnerMetadata3_1> {
        let mode = st.lookup_mode()?;
        if mode != Mode::List {
            st.push_meta_deprecated_str("$MODE should only be L");
        };
        Some(InnerMetadata3_1 {
            mode,
            byteord: st.lookup_endian()?,
            cyt: st.lookup_cyt_opt(),
            spillover: st.lookup_spillover_checked(names),
            timestamps: st.lookup_timestamps3_1(false),
            cytsn: st.lookup_cytsn(),
            timestep: st.lookup_timestep(),
            modification: st.lookup_modification(),
            plate: st.lookup_plate(false),
            vol: st.lookup_vol(),
        })
    }

    fn keywords_inner(&self, other_textlen: usize, data_len: usize) -> MaybeKeywords {
        // let anal = self.supplemental.analysis;
        // let stext = self.supplemental.stext;
        let mdn = &self.modification;
        let ts = &self.timestamps;
        let pl = &self.plate;
        let fixed = [
            // (BEGINANALYSIS, Some(anal.begin.to_string())),
            // (ENDANALYSIS, Some(anal.end.to_string())),
            // (BEGINSTEXT, Some(stext.begin.to_string())),
            // (ENDSTEXT, Some(stext.end.to_string())),
            // (TOT, Some(self.tot.to_string())),
            (MODE, Some(self.mode.to_string())),
            (BYTEORD, Some(self.byteord.to_string())),
            (CYT, self.cyt.as_opt_string()),
            (SPILLOVER, self.spillover.as_opt_string()),
            (BTIM, ts.btim.as_opt_string()),
            (ETIM, ts.etim.as_opt_string()),
            (DATE, ts.date.as_opt_string()),
            (CYTSN, self.cytsn.as_opt_string()),
            (TIMESTEP, self.timestep.as_opt_string()),
            (LAST_MODIFIER, mdn.last_modifier.as_opt_string()),
            (LAST_MODIFIED, mdn.last_modified.as_opt_string()),
            (ORIGINALITY, mdn.originality.as_opt_string()),
            (PLATEID, pl.plateid.as_opt_string()),
            (PLATENAME, pl.platename.as_opt_string()),
            (WELLID, pl.wellid.as_opt_string()),
            (VOL, self.vol.as_opt_string()),
        ];
        let text_len = sum_keywords(&fixed) + other_textlen;
        make_data_offset_keywords(text_len, data_len)
            .into_iter()
            .chain(fixed)
            .collect()
    }
}

impl VersionedMetadata for InnerMetadata3_2 {
    type P = InnerMeasurement3_2;
    type R = InnerReadData3_2;

    fn into_any_text(t: Box<StdText3_2>) -> AnyStdTEXT {
        AnyStdTEXT::FCS3_2(t)
    }

    // TODO not DRY
    fn get_data_offsets(s: &StdText<Self, Self::P, Self::R>) -> Offsets {
        let header_offsets = s.data_offsets;
        if header_offsets.is_unset() {
            s.read_data.specific.data
        } else {
            header_offsets
        }
    }

    fn get_byteord(&self) -> ByteOrd {
        ByteOrd::Endian(self.byteord)
    }

    fn has_timestep(&self) -> bool {
        self.timestep.as_ref().into_option().is_some()
    }

    fn build_int_parser(
        &self,
        st: &mut KwState,
        ps: &[Measurement<Self::P>],
        total_events: usize,
    ) -> Option<IntParser> {
        build_int_parser_2_0(st, &ByteOrd::Endian(self.byteord), ps, total_events)
    }

    fn build_mixed_parser(
        &self,
        st: &mut KwState,
        ps: &[Measurement<Self::P>],
        dt: &AlphaNumType,
        total_events: usize,
    ) -> Option<Option<MixedParser>> {
        let endian = self.byteord;
        // first test if we have any PnDATATYPEs defined, if no then skip this
        // data parser entirely
        if ps
            .iter()
            .filter(|p| p.specific.datatype.as_ref().into_option().is_some())
            .count()
            == 0
        {
            return None;
        }
        let (pass, fail): (Vec<_>, Vec<_>) = ps
            .iter()
            .enumerate()
            .map(|(i, p)| {
                // TODO this range thing seems not necessary
                match (
                    p.specific.get_column_type(*dt),
                    p.specific.datatype.as_ref().into_option().is_some(),
                    p.range,
                    &p.bytes,
                ) {
                    (AlphaNumType::Ascii, _, _, Bytes::Fixed(bytes)) => {
                        Ok(MixedColumnType::Ascii(AsciiColumn {
                            width: *bytes,
                            data: vec![],
                        }))
                    }
                    (AlphaNumType::Single, _, _, Bytes::Fixed(4)) => Ok(MixedColumnType::Single(
                        f32::make_column_parser(endian, total_events),
                    )),
                    (AlphaNumType::Double, _, _, Bytes::Fixed(8)) => Ok(MixedColumnType::Double(
                        f64::make_column_parser(endian, total_events),
                    )),
                    (AlphaNumType::Integer, _, r, Bytes::Fixed(bytes)) => {
                        make_int_parser(*bytes, &r, &ByteOrd::Endian(self.byteord), total_events)
                            .map(MixedColumnType::Uint)
                    }
                    (dt, overridden, _, bytes) => {
                        let sdt = if overridden { "PnDATATYPE" } else { "DATATYPE" };
                        Err(vec![format!(
                            "{}={} but PnB={} for measurement {}",
                            sdt, dt, bytes, i
                        )])
                    }
                }
            })
            .partition_result();
        if fail.is_empty() {
            Some(Some(MixedParser {
                nrows: total_events,
                columns: pass,
            }))
        } else {
            for e in fail.into_iter().flatten() {
                st.push_meta_error(e);
            }
            None
        }
    }

    fn validate_specific(st: &mut KwState, s: &StdText<Self, Self::P, Self::R>) {
        let spec = &s.core.metadata.specific;
        // check that BEGINDATETIME is before ENDDATETIME
        if let (OptionalKw::Present(begin), OptionalKw::Present(end)) =
            (&spec.datetimes.begin, &spec.datetimes.end)
        {
            if end.0 < begin.0 {
                st.push_meta_warning_str("$BEGINDATETIME is after $ENDDATETIME");
            }
        }
    }

    fn build_inner(st: &mut KwState, _: usize, names: &HashSet<&str>) -> Option<InnerMetadata3_2> {
        // Only L is allowed as of 3.2, so pull the value and check it if
        // given. The only thing we care about here is that the value is not
        // invalid, since we don't need to use it anywhere.
        let _ = st.lookup_mode3_2();
        Some(InnerMetadata3_2 {
            // data: st.lookup_data_offsets()?,
            // supplemental: st.lookup_supplemental3_2(),
            // tot: st.lookup_tot_req()?,
            byteord: st.lookup_endian()?,
            cyt: st.lookup_cyt_req()?,
            spillover: st.lookup_spillover_checked(names),
            timestamps: st.lookup_timestamps3_1(true),
            cytsn: st.lookup_cytsn(),
            timestep: st.lookup_timestep(),
            modification: st.lookup_modification(),
            plate: st.lookup_plate(true),
            vol: st.lookup_vol(),
            carrier: st.lookup_carrier(),
            datetimes: st.lookup_datetimes(),
            unstained: st.lookup_unstained(names),
            flowrate: st.lookup_flowrate(),
        })
    }

    fn keywords_inner(&self, other_textlen: usize, data_len: usize) -> MaybeKeywords {
        // let anal = self.supplemental.analysis.as_ref().into_option();
        // let stext = self.supplemental.stext.as_ref().into_option();
        let mdn = &self.modification;
        let ts = &self.timestamps;
        let pl = &self.plate;
        let car = &self.carrier;
        let dt = &self.datetimes;
        let us = &self.unstained;
        let fixed = [
            // (BEGINDATA, Some(self.data.begin.to_string())),
            // (ENDDATA, Some(self.data.end.to_string())),
            // (BEGINANALYSIS, anal.map(|x| x.begin.to_string())),
            // (ENDANALYSIS, anal.map(|x| x.begin.to_string())),
            // (BEGINSTEXT, stext.map(|x| x.begin.to_string())),
            // (ENDSTEXT, stext.map(|x| x.end.to_string())),
            // (TOT, Some(self.tot.to_string())),
            (BYTEORD, Some(self.byteord.to_string())),
            (CYT, Some(self.cyt.to_string())),
            (SPILLOVER, self.spillover.as_opt_string()),
            (BTIM, ts.btim.as_opt_string()),
            (ETIM, ts.etim.as_opt_string()),
            (DATE, ts.date.as_opt_string()),
            (CYTSN, self.cytsn.as_opt_string()),
            (TIMESTEP, self.timestep.as_opt_string()),
            (LAST_MODIFIER, mdn.last_modifier.as_opt_string()),
            (LAST_MODIFIED, mdn.last_modified.as_opt_string()),
            (ORIGINALITY, mdn.originality.as_opt_string()),
            (PLATEID, pl.plateid.as_opt_string()),
            (PLATENAME, pl.platename.as_opt_string()),
            (WELLID, pl.wellid.as_opt_string()),
            (VOL, self.vol.as_opt_string()),
            (CARRIERID, car.carrierid.as_opt_string()),
            (CARRIERTYPE, car.carriertype.as_opt_string()),
            (LOCATIONID, car.locationid.as_opt_string()),
            (BEGINDATETIME, dt.begin.as_opt_string()),
            (ENDDATETIME, dt.end.as_opt_string()),
            (UNSTAINEDCENTERS, us.unstainedcenters.as_opt_string()),
            (UNSTAINEDINFO, us.unstainedinfo.as_opt_string()),
            (FLOWRATE, self.flowrate.as_opt_string()),
        ];
        let text_len = sum_keywords(&fixed) + other_textlen;
        make_data_offset_keywords(text_len, data_len)
            .into_iter()
            .chain(fixed)
            .collect()
    }
}

fn parse_raw_text(header: Header, raw: RawTEXT, conf: &StdTextReader) -> TEXTResult {
    match header.version {
        Version::FCS2_0 => StdText2_0::raw_to_std_text(header, raw, conf),
        Version::FCS3_0 => StdText3_0::raw_to_std_text(header, raw, conf),
        Version::FCS3_1 => StdText3_1::raw_to_std_text(header, raw, conf),
        Version::FCS3_2 => StdText3_2::raw_to_std_text(header, raw, conf),
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug, Serialize)]
struct StdKey(String);

impl StdKey {
    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug, Serialize)]
struct NonStdKey(String);

impl NonStdKey {
    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Eq, PartialEq)]
enum ValueStatus {
    Raw,
    Error(String),
    Warning(String),
    Used,
}

struct KwValue {
    value: String,
    status: ValueStatus,
}

// all hail the almighty state monad :D

struct KwState<'a> {
    raw_standard_keywords: HashMap<StdKey, KwValue>,
    raw_nonstandard_keywords: HashMap<NonStdKey, String>,
    missing_keywords: Vec<StdKey>,
    deprecated_keys: Vec<StdKey>,
    deprecated_features: Vec<String>,
    meta_errors: Vec<String>,
    meta_warnings: Vec<String>,
    conf: &'a StdTextReader,
}

#[derive(Debug, Clone)]
struct KeyError {
    key: StdKey,
    value: String,
    msg: String,
}

#[derive(Debug, Clone, Serialize)]
struct KeyWarning {
    key: StdKey,
    value: String,
    msg: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct NonFatalErrors {
    deprecated_keys: Vec<StdKey>,
    deprecated_features: Vec<String>,
    meta_warnings: Vec<String>,
    deviant_keywords: HashMap<StdKey, String>,
    nonstandard_keywords: HashMap<NonStdKey, String>,
    keyword_warnings: Vec<KeyWarning>,
}

impl NonFatalErrors {
    fn has_error(&self, conf: &StdTextReader) -> bool {
        (!self.deviant_keywords.is_empty() && conf.disallow_deviant)
            || (!self.deprecated_features.is_empty() && conf.disallow_deprecated)
            || (!self.deprecated_keys.is_empty() && conf.disallow_deprecated)
            || (!self.meta_warnings.is_empty() && conf.warnings_are_errors)
            || (!self.keyword_warnings.is_empty() && conf.warnings_are_errors)
            || (!self.nonstandard_keywords.is_empty() && conf.disallow_nonstandard)
    }

    fn into_critical(self, conf: &StdTextReader) -> NonFatalErrors {
        NonFatalErrors {
            deviant_keywords: if conf.disallow_deviant {
                self.deviant_keywords
            } else {
                HashMap::new()
            },
            deprecated_features: if conf.disallow_deprecated {
                self.deprecated_features
            } else {
                vec![]
            },
            deprecated_keys: if conf.disallow_deprecated {
                self.deprecated_keys
            } else {
                vec![]
            },
            meta_warnings: if conf.warnings_are_errors {
                self.meta_warnings
            } else {
                vec![]
            },
            keyword_warnings: if conf.warnings_are_errors {
                self.keyword_warnings
            } else {
                vec![]
            },
            nonstandard_keywords: if conf.disallow_nonstandard {
                self.nonstandard_keywords
            } else {
                HashMap::new()
            },
        }
    }

    fn into_lines(self) -> Vec<String> {
        let depkeys = self
            .deprecated_keys
            .into_iter()
            .map(|k| format!("{} has been deprecated", k.0));
        let devkeys = self
            .deviant_keywords
            .into_keys()
            .map(|k| format!("{} starts with a '$' but is not a standard keyword", k.0));
        let nskeys = self
            .nonstandard_keywords
            .into_keys()
            .map(|k| format!("{} is a nonstandard keyword", k.0));
        let kwarnings = self.keyword_warnings.into_iter().map(|e| {
            format!(
                "Potential issue for {}. Warning was '{}'. Value was '{}'.",
                e.key.0, e.msg, e.value
            )
        });
        depkeys
            .chain(devkeys)
            .chain(nskeys)
            .chain(kwarnings)
            .chain(self.meta_warnings)
            .chain(self.deprecated_features)
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct StandardErrors {
    /// Required keywords that are missing
    missing_keywords: Vec<StdKey>,

    /// Errors that pertain to one keyword value
    value_errors: Vec<KeyError>,

    /// Errors involving multiple keywords, like PnB not matching DATATYPE
    meta_errors: Vec<String>,

    nonfatal: NonFatalErrors,
}

impl StandardErrors {
    fn into_lines(self) -> Vec<String> {
        let ks = self
            .missing_keywords
            .into_iter()
            .map(|s| format!("Required keyword is missing: {}", s.0));
        let vs = self.value_errors.into_iter().map(|e| {
            format!(
                "Could not get value for {}. Error was '{}'. Value was '{}'.",
                e.key.0, e.msg, e.value
            )
        });
        let nfs = self.nonfatal.into_lines();
        ks.chain(vs).chain(nfs).chain(self.meta_errors).collect()
    }

    pub fn print(self) {
        for e in self.into_lines() {
            eprintln!("ERROR: {e}");
        }
    }
}

impl KwState<'_> {
    // TODO not DRY (although will likely need HKTs)
    fn lookup_required_fun<V, F>(&mut self, k: &str, f: F, dep: bool) -> Option<V>
    where
        F: FnOnce(&str) -> ParseResult<V>,
    {
        let sk = StdKey(String::from(k));
        match self.raw_standard_keywords.get_mut(&sk) {
            Some(v) => match v.status {
                ValueStatus::Raw => {
                    let (s, r) = f(&v.value).map_or_else(
                        |e| (ValueStatus::Error(e), None),
                        |x| (ValueStatus::Used, Some(x)),
                    );
                    if dep {
                        self.deprecated_keys.push(sk);
                    }
                    v.status = s;
                    r
                }
                _ => None,
            },
            None => {
                self.missing_keywords.push(StdKey(String::from(k)));
                None
            }
        }
    }

    fn lookup_optional_fun<V, F>(&mut self, k: &str, f: F, dep: bool) -> OptionalKw<V>
    where
        F: FnOnce(&str) -> ParseResult<V>,
    {
        let sk = StdKey(String::from(k));
        match self.raw_standard_keywords.get_mut(&sk) {
            Some(v) => match v.status {
                ValueStatus::Raw => {
                    let (s, r) = f(&v.value).map_or_else(
                        |w| (ValueStatus::Warning(w), Absent),
                        |x| (ValueStatus::Used, OptionalKw::Present(x)),
                    );
                    if dep {
                        self.deprecated_keys.push(sk);
                    }
                    v.status = s;
                    r
                }
                _ => Absent,
            },
            None => Absent,
        }
    }

    fn lookup_required<V: FromStr>(&mut self, k: &str, dep: bool) -> Option<V>
    where
        <V as FromStr>::Err: fmt::Display,
    {
        self.lookup_required_fun(k, |s| s.parse().map_err(|e| format!("{}", e)), dep)
    }

    fn lookup_optional<V: FromStr>(&mut self, k: &str, dep: bool) -> OptionalKw<V>
    where
        <V as FromStr>::Err: fmt::Display,
    {
        self.lookup_optional_fun(k, |s| s.parse().map_err(|e| format!("{}", e)), dep)
    }

    // TODO these need to be checked with the delta taken into account
    fn build_offsets(&mut self, begin: u32, end: u32, which: &'static str) -> Option<Offsets> {
        Offsets::new(begin, end).or_else(|| {
            let msg = format!("Could not make {} offset: begin > end", which);
            self.meta_errors.push(msg);
            None
        })
    }

    // metadata

    fn lookup_begindata(&mut self) -> Option<u32> {
        self.lookup_required(BEGINDATA, false)
    }

    fn lookup_enddata(&mut self) -> Option<u32> {
        self.lookup_required(ENDDATA, false)
    }

    fn lookup_data_offsets(&mut self) -> Option<Offsets> {
        let begin = self.lookup_begindata()?;
        let end = self.lookup_enddata()?;
        self.build_offsets(begin, end, "DATA")
    }

    fn lookup_stext_offsets(&mut self) -> Option<Offsets> {
        let beginstext = self.lookup_required(BEGINSTEXT, false)?;
        let endstext = self.lookup_required(ENDSTEXT, false)?;
        self.build_offsets(beginstext, endstext, "STEXT")
    }

    fn lookup_analysis_offsets(&mut self) -> Option<Offsets> {
        let beginstext = self.lookup_required(BEGINANALYSIS, false)?;
        let endstext = self.lookup_required(ENDANALYSIS, false)?;
        self.build_offsets(beginstext, endstext, "ANALYSIS")
    }

    fn lookup_supplemental3_0(&mut self) -> Option<SupplementalOffsets3_0> {
        let stext = self.lookup_stext_offsets()?;
        let analysis = self.lookup_analysis_offsets()?;
        Some(SupplementalOffsets3_0 { stext, analysis })
    }

    fn lookup_supplemental3_2(&mut self) -> SupplementalOffsets3_2 {
        let stext = OptionalKw::from_option(self.lookup_stext_offsets());
        let analysis = OptionalKw::from_option(self.lookup_analysis_offsets());
        SupplementalOffsets3_2 { stext, analysis }
    }

    fn lookup_byteord(&mut self) -> Option<ByteOrd> {
        self.lookup_required(BYTEORD, false)
    }

    fn lookup_endian(&mut self) -> Option<Endian> {
        self.lookup_required(BYTEORD, false)
    }

    fn lookup_datatype(&mut self) -> Option<AlphaNumType> {
        self.lookup_required(DATATYPE, false)
    }

    fn lookup_mode(&mut self) -> Option<Mode> {
        self.lookup_required(MODE, false)
    }

    // TODO this needs to be a new type to enforce the 3.2 value
    fn lookup_mode3_2(&mut self) -> OptionalKw<Mode> {
        self.lookup_optional(MODE, true)
    }

    fn lookup_nextdata(&mut self) -> Option<u32> {
        self.lookup_required(NEXTDATA, false)
    }

    fn lookup_par(&mut self) -> Option<u32> {
        self.lookup_required(PAR, false)
    }

    fn lookup_tot_req(&mut self) -> Option<u32> {
        self.lookup_required(TOT, false)
    }

    fn lookup_tot_opt(&mut self) -> OptionalKw<u32> {
        self.lookup_optional(TOT, false)
    }

    fn lookup_cyt_req(&mut self) -> Option<String> {
        self.lookup_required(CYT, false)
    }

    fn lookup_cyt_opt(&mut self) -> OptionalKw<String> {
        self.lookup_optional(CYT, false)
    }

    fn lookup_abrt(&mut self) -> OptionalKw<u32> {
        self.lookup_optional(ABRT, false)
    }

    fn lookup_cells(&mut self) -> OptionalKw<String> {
        self.lookup_optional(CELLS, false)
    }

    fn lookup_com(&mut self) -> OptionalKw<String> {
        self.lookup_optional(COM, false)
    }

    fn lookup_exp(&mut self) -> OptionalKw<String> {
        self.lookup_optional(EXP, false)
    }

    fn lookup_fil(&mut self) -> OptionalKw<String> {
        self.lookup_optional(FIL, false)
    }

    fn lookup_inst(&mut self) -> OptionalKw<String> {
        self.lookup_optional(INST, false)
    }

    fn lookup_lost(&mut self) -> OptionalKw<u32> {
        self.lookup_optional(LOST, false)
    }

    fn lookup_op(&mut self) -> OptionalKw<String> {
        self.lookup_optional(OP, false)
    }

    fn lookup_proj(&mut self) -> OptionalKw<String> {
        self.lookup_optional(PROJ, false)
    }

    fn lookup_smno(&mut self) -> OptionalKw<String> {
        self.lookup_optional(SMNO, false)
    }

    fn lookup_src(&mut self) -> OptionalKw<String> {
        self.lookup_optional(SRC, false)
    }

    fn lookup_sys(&mut self) -> OptionalKw<String> {
        self.lookup_optional(SYS, false)
    }

    fn lookup_trigger(&mut self) -> OptionalKw<Trigger> {
        self.lookup_optional(TR, false)
    }

    fn lookup_trigger_checked(&mut self, names: &HashSet<&str>) -> OptionalKw<Trigger> {
        if let Present(tr) = self.lookup_trigger() {
            let p = tr.measurement.as_str();
            if names.contains(p) {
                self.push_meta_warning(format!(
                    "$TRIGGER refers to non-existent measurements '{p}'",
                ));
                Absent
            } else {
                Present(tr)
            }
        } else {
            Absent
        }
    }

    fn lookup_cytsn(&mut self) -> OptionalKw<String> {
        self.lookup_optional(CYTSN, false)
    }

    fn lookup_timestep(&mut self) -> OptionalKw<f32> {
        self.lookup_optional(TIMESTEP, false)
    }

    fn lookup_vol(&mut self) -> OptionalKw<f32> {
        self.lookup_optional(VOL, false)
    }

    fn lookup_flowrate(&mut self) -> OptionalKw<String> {
        self.lookup_optional(FLOWRATE, false)
    }

    fn lookup_unicode(&mut self) -> OptionalKw<Unicode> {
        // TODO actually verify that these are real keywords, although this
        // doesn't matter too much since we are going to parse TEXT as utf8
        // anyways since we can, so this keywords isn't that useful.
        self.lookup_optional(UNICODE, false)
    }

    fn lookup_plateid(&mut self, dep: bool) -> OptionalKw<String> {
        self.lookup_optional(PLATEID, dep)
    }

    fn lookup_platename(&mut self, dep: bool) -> OptionalKw<String> {
        self.lookup_optional(PLATENAME, dep)
    }

    fn lookup_wellid(&mut self, dep: bool) -> OptionalKw<String> {
        self.lookup_optional(WELLID, dep)
    }

    fn lookup_unstainedinfo(&mut self) -> OptionalKw<String> {
        self.lookup_optional(UNSTAINEDINFO, false)
    }

    fn lookup_unstainedcenters(&mut self) -> OptionalKw<UnstainedCenters> {
        self.lookup_optional(UNSTAINEDCENTERS, false)
    }

    fn lookup_unstainedcenters_checked(
        &mut self,
        names: &HashSet<&str>,
    ) -> OptionalKw<UnstainedCenters> {
        if let Present(u) = self.lookup_unstainedcenters() {
            let noexist: Vec<_> = u.0.keys().filter(|m| !names.contains(m.as_str())).collect();
            if !noexist.is_empty() {
                let msg = format!(
                    "$UNSTAINEDCENTERS refers to non-existent measurements: {}",
                    noexist.iter().join(","),
                );
                self.push_meta_error(msg);
                Absent
            } else {
                Present(u)
            }
        } else {
            Absent
        }
    }

    fn lookup_last_modifier(&mut self) -> OptionalKw<String> {
        self.lookup_optional(LAST_MODIFIER, false)
    }

    // TODO wrap this in a newtype
    fn lookup_last_modified(&mut self) -> OptionalKw<ModifiedDateTime> {
        self.lookup_optional(LAST_MODIFIED, false)
    }

    fn lookup_originality(&mut self) -> OptionalKw<Originality> {
        self.lookup_optional(ORIGINALITY, false)
    }

    fn lookup_carrierid(&mut self) -> OptionalKw<String> {
        self.lookup_optional(CARRIERID, false)
    }

    fn lookup_carriertype(&mut self) -> OptionalKw<String> {
        self.lookup_optional(CARRIERTYPE, false)
    }

    fn lookup_locationid(&mut self) -> OptionalKw<String> {
        self.lookup_optional(LOCATIONID, false)
    }

    fn lookup_begindatetime(&mut self) -> OptionalKw<FCSDateTime> {
        self.lookup_optional(BEGINDATETIME, false)
    }

    fn lookup_enddatetime(&mut self) -> OptionalKw<FCSDateTime> {
        self.lookup_optional(ENDDATETIME, false)
    }

    fn lookup_date(&mut self, dep: bool) -> OptionalKw<FCSDate> {
        self.lookup_optional(DATE, dep)
    }

    fn lookup_btim(&mut self) -> OptionalKw<FCSTime> {
        self.lookup_optional(BTIM, false)
    }

    fn lookup_etim(&mut self) -> OptionalKw<FCSTime> {
        self.lookup_optional(ETIM, false)
    }

    fn lookup_btim60(&mut self) -> OptionalKw<FCSTime60> {
        self.lookup_optional(BTIM, false)
    }

    fn lookup_etim60(&mut self) -> OptionalKw<FCSTime60> {
        self.lookup_optional(ETIM, false)
    }

    fn lookup_btim100(&mut self, dep: bool) -> OptionalKw<FCSTime100> {
        self.lookup_optional(BTIM, dep)
    }

    fn lookup_etim100(&mut self, dep: bool) -> OptionalKw<FCSTime100> {
        self.lookup_optional(ETIM, dep)
    }

    fn lookup_timestamps2_0(&mut self) -> Timestamps2_0 {
        Timestamps2_0 {
            btim: self.lookup_btim(),
            etim: self.lookup_etim(),
            date: self.lookup_date(false),
        }
    }

    fn lookup_timestamps3_0(&mut self) -> Timestamps3_0 {
        Timestamps3_0 {
            btim: self.lookup_btim60(),
            etim: self.lookup_etim60(),
            date: self.lookup_date(false),
        }
    }

    fn lookup_timestamps3_1(&mut self, dep: bool) -> Timestamps3_1 {
        Timestamps3_1 {
            btim: self.lookup_btim100(dep),
            etim: self.lookup_etim100(dep),
            date: self.lookup_date(dep),
        }
    }

    fn lookup_datetimes(&mut self) -> Datetimes {
        Datetimes {
            begin: self.lookup_begindatetime(),
            end: self.lookup_enddatetime(),
        }
    }

    fn lookup_modification(&mut self) -> ModificationData {
        ModificationData {
            last_modifier: self.lookup_last_modifier(),
            last_modified: self.lookup_last_modified(),
            originality: self.lookup_originality(),
        }
    }

    fn lookup_plate(&mut self, dep: bool) -> PlateData {
        PlateData {
            wellid: self.lookup_plateid(dep),
            platename: self.lookup_platename(dep),
            plateid: self.lookup_wellid(dep),
        }
    }

    fn lookup_carrier(&mut self) -> CarrierData {
        CarrierData {
            locationid: self.lookup_locationid(),
            carrierid: self.lookup_carrierid(),
            carriertype: self.lookup_carriertype(),
        }
    }

    fn lookup_unstained(&mut self, names: &HashSet<&str>) -> UnstainedData {
        UnstainedData {
            unstainedcenters: self.lookup_unstainedcenters_checked(names),
            unstainedinfo: self.lookup_unstainedinfo(),
        }
    }

    fn lookup_compensation_2_0(&mut self, par: usize) -> OptionalKw<Compensation> {
        let mut matrix: Vec<_> = iter::repeat_with(|| vec![0.0; par]).take(par).collect();
        // column = src channel
        // row = target channel
        // These are "flipped" in 2.0, where "column" goes TO the "row"
        let mut any_error = false;
        for r in 0..par {
            for c in 0..par {
                let m = format!("DFC{c}TO{r}");
                if let Present(x) = self.lookup_optional(m.as_str(), false) {
                    matrix[r][c] = x;
                } else {
                    any_error = true;
                }
            }
        }
        if any_error {
            Absent
        } else {
            Present(Compensation { matrix })
        }
    }

    fn lookup_compensation_3_0(&mut self) -> OptionalKw<Compensation> {
        self.lookup_optional(COMP, false)
    }

    fn lookup_spillover(&mut self) -> OptionalKw<Spillover> {
        self.lookup_optional(SPILLOVER, false)
    }

    // TODO this is basically the same as unstained centers
    fn lookup_spillover_checked(&mut self, names: &HashSet<&str>) -> OptionalKw<Spillover> {
        if let Present(s) = self.lookup_spillover() {
            let noexist: Vec<_> = s
                .measurements
                .iter()
                .filter(|m| !names.contains(m.as_str()))
                .collect();
            if !noexist.is_empty() {
                let msg = format!(
                    "$SPILLOVER refers to non-existent measurements: {}",
                    noexist.iter().join(", ")
                );
                self.push_meta_error(msg);
            }

            Present(s)
        } else {
            Absent
        }
    }

    // measurements

    fn lookup_meas_req<V: FromStr>(&mut self, m: &'static str, n: u32, dep: bool) -> Option<V>
    where
        <V as FromStr>::Err: fmt::Display,
    {
        self.lookup_required(&format_measurement(&n.to_string(), m), dep)
    }

    fn lookup_meas_opt<V: FromStr>(&mut self, m: &'static str, n: u32, dep: bool) -> OptionalKw<V>
    where
        <V as FromStr>::Err: fmt::Display,
    {
        self.lookup_optional(&format_measurement(&n.to_string(), m), dep)
    }

    // this reads the PnB field which has "bits" in it, but as far as I know
    // nobody is using anything other than evenly-spaced bytes
    fn lookup_meas_bytes(&mut self, n: u32) -> Option<Bytes> {
        self.lookup_meas_req(BYTES_SFX, n, false)
    }

    fn lookup_meas_range(&mut self, n: u32) -> Option<Range> {
        self.lookup_meas_req(RANGE_SFX, n, false)
    }

    fn lookup_meas_wavelength(&mut self, n: u32) -> OptionalKw<u32> {
        self.lookup_meas_opt(WAVELEN_SFX, n, false)
    }

    fn lookup_meas_power(&mut self, n: u32) -> OptionalKw<u32> {
        self.lookup_meas_opt(POWER_SFX, n, false)
    }

    fn lookup_meas_detector_type(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_meas_opt(DET_TYPE_SFX, n, false)
    }

    fn lookup_meas_shortname_req(&mut self, n: u32) -> Option<Shortname> {
        self.lookup_meas_req(SHORTNAME_SFX, n, false)
    }

    fn lookup_meas_shortname_opt(&mut self, n: u32) -> OptionalKw<Shortname> {
        self.lookup_meas_opt(SHORTNAME_SFX, n, false)
    }

    fn lookup_meas_longname(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_meas_opt(LONGNAME_SFX, n, false)
    }

    fn lookup_meas_filter(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_meas_opt(FILTER_SFX, n, false)
    }

    fn lookup_meas_percent_emitted(&mut self, n: u32, dep: bool) -> OptionalKw<u32> {
        self.lookup_meas_opt(PCNT_EMT_SFX, n, dep)
    }

    fn lookup_meas_detector_voltage(&mut self, n: u32) -> OptionalKw<f32> {
        self.lookup_meas_opt(DET_VOLT_SFX, n, false)
    }

    fn lookup_meas_detector(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_meas_opt(DET_NAME_SFX, n, false)
    }

    fn lookup_meas_tag(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_meas_opt(TAG_SFX, n, false)
    }

    fn lookup_meas_analyte(&mut self, n: u32) -> OptionalKw<String> {
        self.lookup_meas_opt(ANALYTE_SFX, n, false)
    }

    fn lookup_meas_gain(&mut self, n: u32) -> OptionalKw<f32> {
        self.lookup_meas_opt(GAIN_SFX, n, false)
    }

    fn lookup_meas_scale_req(&mut self, n: u32) -> Option<Scale> {
        self.lookup_meas_req(SCALE_SFX, n, false)
    }

    fn lookup_meas_scale_opt(&mut self, n: u32) -> OptionalKw<Scale> {
        self.lookup_meas_opt(SCALE_SFX, n, false)
    }

    fn lookup_meas_calibration3_1(&mut self, n: u32) -> OptionalKw<Calibration3_1> {
        self.lookup_meas_opt(CALIBRATION_SFX, n, false)
    }

    fn lookup_meas_calibration3_2(&mut self, n: u32) -> OptionalKw<Calibration3_2> {
        self.lookup_meas_opt(CALIBRATION_SFX, n, false)
    }

    // for 3.1+ PnL measurements, which can have multiple wavelengths
    fn lookup_meas_wavelengths(&mut self, n: u32) -> OptionalKw<Wavelengths> {
        self.lookup_meas_opt(WAVELEN_SFX, n, false)
    }

    fn lookup_meas_display(&mut self, n: u32) -> OptionalKw<Display> {
        self.lookup_meas_opt(DISPLAY_SFX, n, false)
    }

    fn lookup_meas_datatype(&mut self, n: u32) -> OptionalKw<NumType> {
        self.lookup_meas_opt(DATATYPE_SFX, n, false)
    }

    fn lookup_meas_type(&mut self, n: u32) -> OptionalKw<MeasurementType> {
        self.lookup_meas_opt(DET_TYPE_SFX, n, false)
    }

    fn lookup_meas_feature(&mut self, n: u32) -> OptionalKw<Feature> {
        self.lookup_meas_opt(FEATURE_SFX, n, false)
    }

    /// Find nonstandard keys that a specific for a given measurement
    fn lookup_meas_nonstandard(&mut self, n: u32) -> HashMap<NonStdKey, String> {
        let mut ns = HashMap::new();
        // ASSUME the pattern does not start with "$" and has a %n which will be
        // subbed for the measurement index. The pattern will then be turned
        // into a legit rust regular expression, which may fail depending on
        // what %n does, so check it each time.
        if let Some(p) = &self.conf.nonstandard_measurement_pattern {
            let rep = p.replace("%n", n.to_string().as_str());
            if let Ok(pattern) = Regex::new(rep.as_str()) {
                for (k, v) in self.raw_nonstandard_keywords.iter() {
                    if pattern.is_match(k.as_str()) {
                        ns.insert(k.clone(), v.clone());
                    }
                }
            } else {
                self.push_meta_warning(format!(
                    "Could not make regular expression using \
                     pattern '{rep}' for measurement {n}"
                ));
            }
        }
        // TODO it seems like there should be a more efficient way to do this,
        // but the only ways I can think of involve taking ownership of the
        // keywords and then moving matching key/vals into a new hashlist.
        for k in ns.keys() {
            self.raw_nonstandard_keywords.remove(k);
        }
        ns
    }

    fn push_meta_error_str(&mut self, msg: &str) {
        self.push_meta_error(String::from(msg));
    }

    fn push_meta_error(&mut self, msg: String) {
        self.meta_errors.push(msg);
    }

    fn push_meta_warning_str(&mut self, msg: &str) {
        self.push_meta_warning(String::from(msg));
    }

    fn push_meta_warning(&mut self, msg: String) {
        self.meta_warnings.push(msg);
    }

    fn push_meta_deprecated_str(&mut self, msg: &str) {
        self.deprecated_features.push(String::from(msg));
    }

    fn push_meta_error_or_warning(&mut self, is_error: bool, msg: String) {
        if is_error {
            self.meta_errors.push(msg);
        } else {
            self.meta_warnings.push(msg);
        }
    }

    fn split_keywords(
        kws: HashMap<StdKey, KwValue>,
        deprecated_keys: Vec<StdKey>,
        deprecated_features: Vec<String>,
        meta_warnings: Vec<String>,
        nonstandard_keywords: HashMap<NonStdKey, String>,
    ) -> (Vec<KeyError>, NonFatalErrors) {
        let mut deviant_keywords = HashMap::new();
        let mut keyword_warnings = Vec::new();
        let mut value_errors = Vec::new();
        for (key, v) in kws {
            match v.status {
                ValueStatus::Raw => {
                    deviant_keywords.insert(key, v.value);
                }
                ValueStatus::Warning(msg) => keyword_warnings.push(KeyWarning {
                    msg,
                    key,
                    value: v.value,
                }),
                ValueStatus::Error(msg) => value_errors.push(KeyError {
                    msg,
                    key,
                    value: v.value,
                }),
                ValueStatus::Used => (),
            }
        }
        let nfe = NonFatalErrors {
            deviant_keywords,
            keyword_warnings,
            nonstandard_keywords,
            deprecated_keys,
            deprecated_features,
            meta_warnings,
        };
        (value_errors, nfe)
    }

    fn into_result<M: VersionedMetadata>(
        self,
        standard: StdText<M, <M as VersionedMetadata>::P, <M as VersionedMetadata>::R>,
        data_parser: DataParser,
        header: Header,
        raw: RawTEXT,
    ) -> TEXTResult {
        let (value_errors, nonfatal) = Self::split_keywords(
            self.raw_standard_keywords,
            self.deprecated_keys,
            self.deprecated_features,
            self.meta_warnings,
            self.raw_nonstandard_keywords,
        );
        let any_crit = !self.missing_keywords.is_empty()
            || !self.meta_errors.is_empty()
            || !value_errors.is_empty();
        if any_crit || nonfatal.has_error(self.conf) {
            // TODO this doesn't include nonstandard measurements, which is
            // probably fine, because if the user didn't want to include them
            // in the ns measurement field they wouldn't have used that param
            // anyways, in which case we probably need to call them something
            // different (like "upgradable")
            Err(Box::new(StandardErrors {
                missing_keywords: self.missing_keywords,
                value_errors,
                meta_errors: self.meta_errors,
                nonfatal: nonfatal.into_critical(self.conf),
            }))
        } else {
            Ok(ParsedTEXT {
                standard: M::into_any_text(Box::new(standard)),
                header,
                raw,
                nonfatal,
                data_parser,
            })
        }
    }

    fn into_errors(self) -> StandardErrors {
        let (value_errors, nonfatal) = Self::split_keywords(
            self.raw_standard_keywords,
            self.deprecated_keys,
            self.deprecated_features,
            self.meta_warnings,
            self.raw_nonstandard_keywords,
        );
        StandardErrors {
            missing_keywords: self.missing_keywords,
            value_errors,
            meta_errors: self.meta_errors,
            nonfatal: nonfatal.into_critical(self.conf),
        }
    }
}

fn parse_header_offset(s: &str, allow_blank: bool) -> Option<u32> {
    if allow_blank && s.trim().is_empty() {
        return Some(0);
    }
    let re = Regex::new(r" *(\d+)").unwrap();
    re.captures(s).map(|c| {
        let [i] = c.extract().1;
        i.parse().unwrap()
    })
}

fn parse_bounds(s0: &str, s1: &str, allow_blank: bool) -> Result<Offsets, &'static str> {
    if let (Some(begin), Some(end)) = (
        parse_header_offset(s0, allow_blank),
        parse_header_offset(s1, allow_blank),
    ) {
        if begin > end {
            Err("beginning is greater than end")
        } else {
            Ok(Offsets { begin, end })
        }
    } else if allow_blank {
        Err("could not make bounds from integers/blanks")
    } else {
        Err("could not make bounds from integers")
    }
}

const hre: &str = r"(.{6})    (.{8})(.{8})(.{8})(.{8})(.{8})(.{8})";

// TODO this error could be better
fn parse_header(s: &str) -> Result<Header, &'static str> {
    let re = Regex::new(hre).unwrap();
    re.captures(s)
        .and_then(|c| {
            let [v, t0, t1, d0, d1, a0, a1] = c.extract().1;
            if let (Ok(version), Ok(text), Ok(data), Ok(analysis)) = (
                v.parse(),
                parse_bounds(t0, t1, false),
                parse_bounds(d0, d1, false),
                parse_bounds(a0, a1, true),
            ) {
                Some(Header {
                    version,
                    text,
                    data,
                    analysis,
                })
            } else {
                None
            }
        })
        .ok_or("malformed header")
}

fn read_header<R: Read>(h: &mut BufReader<R>) -> io::Result<Header> {
    let mut verbuf = [0; 58];
    h.read_exact(&mut verbuf)?;
    if let Ok(hs) = str::from_utf8(&verbuf) {
        parse_header(hs).map_err(io::Error::other)
    } else {
        Err(io::Error::other("header sequence is not valid text"))
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RawTEXT {
    delimiter: u8,
    standard_keywords: HashMap<StdKey, String>,
    nonstandard_keywords: HashMap<NonStdKey, String>,
    warnings: Vec<String>,
}

impl RawTEXT {
    fn to_state<'a>(&self, conf: &'a StdTextReader) -> KwState<'a> {
        let mut raw_standard_keywords = HashMap::new();
        for (k, v) in self.standard_keywords.iter() {
            raw_standard_keywords.insert(
                k.clone(),
                KwValue {
                    value: v.clone(),
                    status: ValueStatus::Raw,
                },
            );
        }
        KwState {
            raw_standard_keywords,
            raw_nonstandard_keywords: self.nonstandard_keywords.clone(),
            deprecated_keys: vec![],
            deprecated_features: vec![],
            missing_keywords: vec![],
            meta_errors: vec![],
            meta_warnings: vec![],
            conf,
        }
    }
}

struct FCSFile<M, P> {
    keywords: CoreText<M, P>,
    data: ParsedData,
}

type FCSFile2_0 = FCSFile<InnerMeasurement2_0, InnerMeasurement2_0>;
type FCSFile3_0 = FCSFile<InnerMeasurement3_0, InnerMeasurement3_0>;
type FCSFile3_1 = FCSFile<InnerMeasurement3_1, InnerMeasurement3_1>;
type FCSFile3_2 = FCSFile<InnerMeasurement3_2, InnerMeasurement3_2>;

pub struct FCSSuccess {
    pub header: Header,
    pub raw: RawTEXT,
    pub std: AnyStdTEXT,
    pub data: ParsedData,
}

fn split_raw_text(xs: &[u8], conf: &RawTextReader) -> Result<RawTEXT, String> {
    // this needs the entire TEXT segment to be loaded in memory, which should
    // be fine since the max number of bytes is ~100M given the upper limit
    // imposed by the header
    //
    // 1. bounded by textstart/end
    // 2. first character is delimiter (and so is the last, I think...)
    // 3. two delimiters in a row shall be replaced by one delimiter
    // 4. no value shall be blank, so (3) implies that delimiters can be present in
    //    keys or values
    // 5. there should be an even number of delimited fields (obviously)
    // 6. delimiters should not be at the start or end of word; for instance, if
    //    we encountered XXX (where X is delim) it wouldn't be clear if the first
    //    or third X is the boundary and if the other two belong to the previous or
    //    or next keyword. Implication: keywords can only appear once in a row or
    //    an even number of times in a row.
    let mut keywords: HashMap<_, _> = HashMap::new();
    let mut warnings = vec![];
    let textlen = xs.len();
    let mut warn_or_err2 = |is_error, err, warn| {
        if is_error || conf.warnings_are_errors {
            Err(err)
        } else {
            warnings.push(warn);
            Ok(())
        }
    };

    // First character is the delimiter
    let delimiter: u8 = xs[0];
    let str_delim = String::from_utf8(vec![delimiter]).or(Err(String::from(
        "First character of TEXT segment is not a valid UTF-8 byte, parsing failed",
    )))?;

    // Check that the delim is valid; this is technically only written in the
    // spec for 3.1+ but for older versions this should still be true since
    // these were ASCII-everywhere
    if !(1..=126).contains(&delimiter) {
        warn_or_err2(
            conf.force_ascii_delim,
            String::from("delimiter must be an ASCII character 1-126, got {str_delim}"),
            String::from("delimiter should be an ASCII character 1-126, got {str_delim}"),
        )?
    }

    // Read from the 2nd character to all but the last character and record
    // delimiter positions.
    let delim_positions: Vec<_> = xs
        .iter()
        .enumerate()
        .filter_map(|(i, c)| if *c == delimiter { Some(i) } else { None })
        .collect();

    let raw_boundaries = delim_positions
        // We force-added the first and last delimiter in the TEXT segment, so
        // this is guaranteed to have at least two elements (one pair)
        .windows(2)
        .filter_map(|x| match x {
            [a, b] => Some((*a, b - a)),
            _ => None,
        });

    // Compute word boundaries depending on if we want to "escape" delims or
    // not. Technically all versions of the standard allow double delimiters to
    // be used in a word to represented a single delimiter. However, this means
    // we also can't have blank values. Many FCS files unfortunately use blank
    // values, so we need to be able to toggle this behavior.
    let boundaries = if conf.no_delim_escape {
        raw_boundaries.collect()
    } else {
        // Remove "escaped" delimiters from position vector. Because we disallow
        // blank values and also disallow delimiters at the start/end of words,
        // this implies that we should only see delimiters by themselves or in a
        // consecutive sequence whose length is even.
        let mut filtered_boundaries = vec![];
        for (key, chunk) in raw_boundaries.chunk_by(|(_, x)| *x).into_iter() {
            if key == 1 {
                if chunk.count() % 2 == 1 {
                    return Err(format!("delimiter '{str_delim}' found at word boundary"));
                }
            } else {
                for x in chunk {
                    filtered_boundaries.push(x);
                }
            }
        }

        // If all went well in the previous step, we should have the following:
        // 1. at least one boundary
        // 2. first entry coincides with start of TEXT
        // 3. last entry coincides with end of TEXT
        if let (Some((x0, _)), Some((xf, len))) =
            (filtered_boundaries.first(), filtered_boundaries.last())
        {
            if *x0 > 0 {
                return Err(format!("first key starts with a delim '{str_delim}'"));
            }
            if *xf + len < textlen - 1 {
                return Err(format!("final value ends with a delim '{str_delim}'",));
            }
        } else {
            return Err(String::from("no boundaries found, cannot parse keywords"));
        }
        filtered_boundaries
    };

    // Check that the last char is also a delim, if not file probably sketchy
    // ASSUME this will not fail since we have at least one delim by definition
    if !delim_positions.last().unwrap() == xs.len() - 1 {
        warn_or_err2(
            conf.enforce_final_delim,
            format!("last TEXT char is not {str_delim}"),
            format!("last TEXT char is not delim {str_delim}, parsing may have failed"),
        )?
    }

    let delim2 = [delimiter, delimiter];
    let delim1 = [delimiter];
    // ASSUME these won't fail as we checked the delimiter is an ASCII character
    let escape_from = str::from_utf8(&delim2).unwrap();
    let escape_to = str::from_utf8(&delim1).unwrap();

    let final_boundaries: Vec<_> = boundaries
        .into_iter()
        .map(|(a, b)| (a + 1, a + b))
        .collect();

    for chunk in final_boundaries.chunks(2) {
        if let [(ki, kf), (vi, vf)] = *chunk {
            if let (Ok(k), Ok(v)) = (str::from_utf8(&xs[ki..kf]), str::from_utf8(&xs[vi..vf])) {
                let kupper = k.to_uppercase();
                // test if keyword is ascii
                if !kupper.is_ascii() {
                    warn_or_err2(
                        conf.enfore_keyword_ascii,
                        String::from("keywords must be ASCII"),
                        String::from("keywords should be ASCII"),
                    )?
                }
                // if delimiters were escaped, replace them here
                let res = if conf.no_delim_escape {
                    // Test for empty values if we don't allow delim escaping;
                    // anything empty will either drop or produce an error
                    // depending on user settings
                    if v.is_empty() {
                        let msg = format!("key {kupper} has a blank value");
                        warn_or_err2(
                            conf.enforce_nonempty,
                            msg.clone(),
                            format!("{}, dropping", msg),
                        )?;
                        None
                    } else {
                        keywords.insert(kupper.clone(), v.to_string())
                    }
                } else {
                    let krep = kupper.replace(escape_from, escape_to);
                    let rrep = v.replace(escape_from, escape_to);
                    keywords.insert(krep, rrep)
                };
                // test if the key was inserted already
                if res.is_some() {
                    let msg = format!("key {kupper} is found more than once");
                    warn_or_err2(conf.enforce_unique, msg.clone(), msg)?
                }
            } else {
                let msg = String::from("invalid UTF-8 byte encountered when parsing TEXT");
                warn_or_err2(conf.error_on_invalid_utf8, msg.clone(), msg)?
            }
        } else {
            warn_or_err2(
                conf.enforce_even,
                String::from("number of words is not even"),
                String::from("number of words is not even, parse may have failed"),
            )?
        }
    }

    // TODO filter keywords based on pattern somewhere here

    repair_keywords(&mut keywords, conf);

    let (std, nstd): (Vec<_>, Vec<_>) = keywords
        .into_iter()
        .collect::<Vec<_>>()
        .into_iter()
        .partition(|(k, _)| k.starts_with("$"));

    Ok(RawTEXT {
        delimiter,
        standard_keywords: std.into_iter().map(|(k, v)| (StdKey(k), v)).collect(),
        nonstandard_keywords: nstd.into_iter().map(|(k, v)| (NonStdKey(k), v)).collect(),
        warnings,
    })
}

fn repair_keywords(kws: &mut HashMap<String, String>, conf: &RawTextReader) {
    for (key, v) in kws.iter_mut() {
        let k = key.as_str();
        if (k == "$BEGINDATA"
            || k == "$ENDDATA"
            || k == "$BEGINSTEXT"
            || k == "$ENDSTEXT"
            || k == "$BEGINANALYSIS"
            || k == "$ENDANALYSIS")
            && conf.repair_offset_spaces
        {
            let len = v.len();
            let trimmed = v.trim_start();
            let newlen = trimmed.len();
            *v = ("0").repeat(len - newlen) + trimmed
        } else if k == "$DATE" {
            if let Some(pattern) = &conf.date_pattern {
                if let Ok(d) = NaiveDate::parse_from_str(v, pattern.as_str()) {
                    *v = format!("{}", FCSDate(d))
                }
            }
        }
    }
}

fn read_raw_text<R: Read + Seek>(
    h: &mut BufReader<R>,
    header: &Header,
    conf: &RawTextReader,
) -> io::Result<RawTEXT> {
    if let Some(adjusted) = header.text.adjust(conf.starttext_delta, conf.endtext_delta) {
        let begin = u64::from(adjusted.begin);
        let nbytes = u64::from(adjusted.num_bytes());
        let mut buf = vec![];

        h.seek(SeekFrom::Start(begin))?;
        h.take(nbytes).read_to_end(&mut buf)?;
        split_raw_text(&buf, conf).map_err(io::Error::other)
    } else {
        Err(io::Error::other(
            "Adjusted begin is after adjusted end for TEXT",
        ))
    }
}

/// Instructions for reading the TEXT segment as raw key/value pairs.
#[derive(Default, Clone)]
pub struct RawTextReader {
    /// Will adjust the offset of the start of the TEXT segment by `offset + n`.
    pub starttext_delta: i32,

    /// Will adjust the offset of the end of the TEXT segment by `offset + n`.
    pub endtext_delta: i32,

    /// If true, all raw text parsing warnings will be considered fatal errors
    /// which will halt the parsing routine.
    pub warnings_are_errors: bool,

    /// Will treat every delimiter as a literal delimiter rather than "escaping"
    /// double delimiters
    pub no_delim_escape: bool,

    /// If true, only ASCII characters 1-126 will be allowed for the delimiter
    pub force_ascii_delim: bool,

    /// If true, throw an error if the last byte of the TEXT segment is not
    /// a delimiter.
    pub enforce_final_delim: bool,

    /// If true, throw an error if any key in the TEXT segment is not unique
    pub enforce_unique: bool,

    /// If true, throw an error if the number or words in the TEXT segment is
    /// not an even number (ie there is a key with no value)
    pub enforce_even: bool,

    /// If true, throw an error if we encounter a key with a blank value.
    /// Only relevant if [`no_delim_escape`] is also true.
    pub enforce_nonempty: bool,

    /// If true, throw an error if the parser encounters a bad UTF-8 byte when
    /// creating the key/value list. If false, merely drop the bad pair.
    pub error_on_invalid_utf8: bool,

    /// If true, throw error when encoutering keyword with non-ASCII characters
    pub enfore_keyword_ascii: bool,

    /// If true, throw error when total event width does not evenly divide
    /// the DATA segment. Meaningless for delimited ASCII data.
    pub enfore_data_width_divisibility: bool,

    /// If true, throw error if the total number of events as computed by
    /// dividing DATA segment length event width doesn't match $TOT. Does
    /// nothing if $TOT not given, which may be the case in version 2.0.
    pub enfore_matching_tot: bool,

    /// If true, replace leading spaces in offset keywords with 0.
    ///
    ///These often need to be padded to make the DATA segment appear at a
    /// predictable offset. Many machines/programs will pad with spaces despite
    /// the spec requiring that all numeric fields be entirely numeric
    /// character.
    pub repair_offset_spaces: bool,

    /// If supplied, will be used as an alternative pattern when parsing $DATE.
    ///
    /// It should have specifiers for year, month, and day as outlined in
    /// https://docs.rs/chrono/latest/chrono/format/strftime/index.html. If not
    /// supplied, $DATE will be parsed according to the standard pattern which
    /// is '%d-%b-%Y'.
    pub date_pattern: Option<String>,
    // TODO add keyword and value overrides, something like a list of patterns
    // that can be used to alter each keyword
    // TODO allow lambda function to be supplied which will alter the kv list
}

/// Instructions for reading the TEXT segment in a standardized structure.
#[derive(Default, Clone)]
pub struct StdTextReader {
    pub raw: RawTextReader,

    /// If true, all metadata standardization warnings will be considered fatal
    /// errors which will halt the parsing routine.
    pub warnings_are_errors: bool,

    /// If given, will be the $PnN used to identify the time channel. Means
    /// nothing for 2.0.
    ///
    /// Will be used for the [`ensure_time*`] options below. If not given, skip
    /// time channel checking entirely.
    pub time_shortname: Option<String>,

    /// If true, will ensure that time channel is present
    pub ensure_time: bool,

    /// If true, will ensure TIMESTEP is present if time channel is also
    /// present.
    pub ensure_time_timestep: bool,

    /// If true, will ensure PnE is 0,0 for time channel.
    pub ensure_time_linear: bool,

    /// If true, will ensure PnG is absent for time channel.
    pub ensure_time_nogain: bool,

    /// If true, throw an error if TEXT includes any keywords that start with
    /// "$" which are not standard.
    pub disallow_deviant: bool,

    /// If true, throw an error if TEXT includes any deprecated features
    pub disallow_deprecated: bool,

    /// If true, throw an error if TEXT includes any keywords that do not
    /// start with "$".
    pub disallow_nonstandard: bool,

    /// If supplied, this pattern will be used to group "nonstandard" keywords
    /// with matching measurements.
    ///
    /// Usually this will be something like '^P%n.+' where '%n' will be
    /// substituted with the measurement index before using it as a regular
    /// expression to match keywords. It should not start with a "$".
    ///
    /// This will matching something like 'P7FOO' which would be 'FOO' for
    /// measurement 7. This might be useful in the future when this code offers
    /// "upgrade" routines since these are often used to represent future
    /// keywords in an older version where the newer version cannot be used for
    /// some reason.
    pub nonstandard_measurement_pattern: Option<String>,
    // TODO add repair stuff
}

/// Instructions for reading the DATA segment.
#[derive(Default)]
pub struct DataReader {
    /// Will adjust the offset of the start of the TEXT segment by `offset + n`.
    datastart_delta: u32,
    /// Will adjust the offset of the end of the TEXT segment by `offset + n`.
    dataend_delta: u32,
}

/// Instructions for reading an FCS file.
#[derive(Default)]
pub struct Reader {
    pub text: StdTextReader,
    pub data: DataReader,
}

type FCSResult = Result<FCSSuccess, Box<StandardErrors>>;

/// Return header in an FCS file.
///
/// The header contains the version and offsets for the TEXT, DATA, and ANALYSIS
/// segments, all of which are present in fixed byte offset segments. This
/// function will fail and return an error if the file does not follow this
/// structure. Will also check that the begin and end segments are not reversed.
///
/// Depending on the version, all of these except the TEXT offsets might be 0
/// which indicates they are actually stored in TEXT due to size limitations.
pub fn read_fcs_header(p: &path::PathBuf) -> io::Result<Header> {
    let file = fs::File::options().read(true).open(p)?;
    let mut reader = BufReader::new(file);
    read_header(&mut reader)
}

/// Return header and raw key/value metadata pairs in an FCS file.
///
/// First will parse the header according to [`read_fcs_header`]. If this fails
/// an error will be returned.
///
/// Next will use the offset information in the header to parse the TEXT segment
/// for key/value pairs. On success will return these pairs as-is using Strings
/// in a HashMap. No other processing will be performed.
pub fn read_fcs_raw_text(p: &path::PathBuf, conf: &Reader) -> io::Result<(Header, RawTEXT)> {
    let file = fs::File::options().read(true).open(p)?;
    let mut reader = BufReader::new(file);
    let header = read_header(&mut reader)?;
    let raw = read_raw_text(&mut reader, &header, &conf.text.raw)?;
    Ok((header, raw))
}

/// Return header and standardized metadata in an FCS file.
///
/// Begins by parsing header and raw keywords according to [`read_fcs_raw_text`]
/// and will return error if this function fails.
///
/// Next, all keywords in the TEXT segment will be validated to conform to the
/// FCS standard indicated in the header and returned in a struct storing each
/// key/value pair in a standardized manner. This will halt and return any
/// errors encountered during this process.
pub fn read_fcs_text(p: &path::PathBuf, conf: &Reader) -> io::Result<TEXTResult> {
    let (header, raw) = read_fcs_raw_text(p, conf)?;
    Ok(parse_raw_text(header, raw, &conf.text))
}

// fn read_fcs_text_2_0(p: path::PathBuf, conf: StdTextReader) -> TEXTResult<TEXT2_0>;
// fn read_fcs_text_3_0(p: path::PathBuf, conf: StdTextReader) -> TEXTResult<TEXT3_0>;
// fn read_fcs_text_3_1(p: path::PathBuf, conf: StdTextReader) -> TEXTResult<TEXT3_1>;
// fn read_fcs_text_3_2(p: path::PathBuf, conf: StdTextReader) -> TEXTResult<TEXT3_2>;

/// Return header, structured metadata, and data in an FCS file.
///
/// Begins by parsing header and raw keywords according to [`read_fcs_text`]
/// and will return error if this function fails.
///
/// Next, the DATA segment will be parsed according to the metadata present
/// in TEXT.
///
/// On success will return all three of the above segments along with any
/// non-critical warnings.
///
/// The [`conf`] argument can be used to control the behavior of each reading
/// step, including the repair of non-conforming files.
pub fn read_fcs_file(p: &path::PathBuf, conf: &Reader) -> io::Result<FCSResult> {
    let file = fs::File::options().read(true).open(p)?;
    let mut reader = BufReader::new(file);
    let header = read_header(&mut reader)?;
    let raw = read_raw_text(&mut reader, &header, &conf.text.raw)?;
    // TODO useless clone?
    match parse_raw_text(header, raw, &conf.text) {
        Ok(std) => {
            let data = read_data(&mut reader, std.data_parser).unwrap();
            Ok(Ok(FCSSuccess {
                header: std.header,
                raw: std.raw,
                std: std.standard,
                data,
            }))
        }
        Err(e) => Ok(Err(e)),
    }
}

// fn read_fcs_file_2_0(p: path::PathBuf, conf: Reader) -> FCSResult<TEXT2_0>;
// fn read_fcs_file_3_0(p: path::PathBuf, conf: Reader) -> FCSResult<TEXT3_0>;
// fn read_fcs_file_3_1(p: path::PathBuf, conf: Reader) -> FCSResult<TEXT3_1>;
// fn read_fcs_file_3_2(p: path::PathBuf, conf: Reader) -> FCSResult<TEXT3_2>;

// /// Return header, raw metadata, and data in an FCS file.
// ///
// /// In contrast to [`read_fcs_file`], this will return the keywords as a flat
// /// list of key/value pairs. Only the bare minimum of these will be read in
// /// order to determine how to parse the DATA segment (including $DATATYPE,
// /// $BYTEORD, etc). No other checks will be performed to ensure the metadata
// /// conforms to the FCS standard version indicated in the header.
// ///
// /// This might be useful for applications where one does not necessarily need
// /// the strict structure of the standardized metadata, or if one does not care
// /// too much about the degree to which the metadata conforms to standard.
// ///
// /// Other than this, behavior is identical to [`read_fcs_file`],
// pub fn read_fcs_raw_file(p: path::PathBuf, conf: Reader) -> io::Result<FCSResult<()>> {
//     let file = fs::File::options().read(true).open(p)?;
//     let mut reader = BufReader::new(file);
//     let header = read_header(&mut reader)?;
//     let raw = read_raw_text(&mut reader, &header, &conf.text.raw)?;
//     // TODO need to modify this so it doesn't do the crazy version checking
//     // stuff we don't actually want in this case
//     match parse_raw_text(header.clone(), raw.clone(), &conf.text) {
//         Ok(std) => {
//             let data = read_data(&mut reader, std.data_parser).unwrap();
//             Ok(Ok(FCSSuccess {
//                 header,
//                 raw,
//                 std: (),
//                 data,
//             }))
//         }
//         Err(e) => Ok(Err(e)),
//     }
// }
