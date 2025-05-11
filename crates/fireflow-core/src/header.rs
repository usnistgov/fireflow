use crate::config::{HeaderConfig, OffsetCorrection};
use crate::error::*;
use crate::segment::*;

use serde::Serialize;
use std::fmt;
use std::io::{BufReader, Read};
use std::str;

/// The length of the HEADER.
///
/// This should always be the same. This also assumes that there are no OTHER
/// segments (which for now are not supported).
pub const HEADER_LEN: usize = A1_END;

const VERSION_END: usize = 6;
const SPACE_END: usize = VERSION_END + 4;
const T0_END: usize = SPACE_END + 8;
const T1_END: usize = T0_END + 8;
const D0_END: usize = T1_END + 8;
const D1_END: usize = D0_END + 8;
const A0_END: usize = D1_END + 8;
const A1_END: usize = A0_END + 8;

/// All FCS versions this library supports.
///
/// This appears as the first 6 bytes of any valid FCS file.
#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Serialize)]
pub enum Version {
    FCS2_0,
    FCS3_0,
    FCS3_1,
    FCS3_2,
}

pub struct VersionError;

/// Output from parsing the FCS header.
///
/// Includes version and the three main segments (TEXT, DATA, ANALYSIS). For
/// now, OTHER segments are ignored. This may change in the future. Segments may
/// or may not be adjusted using configuration parameters to correct for errors.
///
/// Only valid segments are to be put in this struct (ie begin <= end).
#[derive(Debug, Clone, Serialize)]
pub struct Header {
    pub version: Version,
    pub text: Segment,
    pub data: Segment,
    pub analysis: Segment,
}

pub fn h_read_header<R: Read>(h: &mut BufReader<R>, conf: &HeaderConfig) -> ImpureResult<Header> {
    let mut verbuf = [0; HEADER_LEN];
    h.read_exact(&mut verbuf)?;
    if let Ok(hs) = str::from_utf8(&verbuf) {
        let succ = parse_header(hs, conf)?;
        Ok(succ)
    } else {
        Err(Failure::new("HEADER is not valid text".to_string()))?
    }
}

fn parse_header_offset(s: &str, allow_blank: bool) -> Option<u32> {
    let trimmed = s.trim_start();
    if allow_blank && trimmed.is_empty() {
        return Some(0);
    }
    trimmed.parse().ok()
}

fn parse_bounds(
    s0: &str,
    s1: &str,
    allow_blank: bool,
    id: SegmentId,
    corr: OffsetCorrection,
) -> PureMaybe<Segment> {
    let parse_one = |s, which| {
        PureMaybe::from_result_1(
            parse_header_offset(s, allow_blank).ok_or(format!(
                "could not parse {which} offset for {id} segment; value was '{s}'"
            )),
            PureErrorLevel::Error,
        )
    };
    let begin_res = parse_one(s0, "begin");
    let end_res = parse_one(s1, "end");
    begin_res
        .combine(end_res, |b, e| (b, e))
        .and_then(|(b, e)| {
            if let (Some(begin), Some(end)) = (b, e) {
                PureMaybe::from_result_1(
                    Segment::try_new(begin, end, corr, id),
                    PureErrorLevel::Error,
                )
            } else {
                PureMaybe::empty()
            }
        })
}

fn parse_header(s: &str, conf: &HeaderConfig) -> PureResult<Header> {
    let v = &s[0..VERSION_END];
    let spaces = &s[VERSION_END..SPACE_END];
    let t0 = &s[SPACE_END..T0_END];
    let t1 = &s[T0_END..T1_END];
    let d0 = &s[T1_END..D0_END];
    let d1 = &s[D0_END..D1_END];
    let a0 = &s[D1_END..A0_END];
    let a1 = &s[A0_END..A1_END];
    let vers_succ = PureMaybe::from_result_1(
        v.parse::<Version>().map_err(|e| e.to_string()),
        PureErrorLevel::Error,
    );
    let text_succ = parse_bounds(t0, t1, false, SegmentId::PrimaryText, conf.text);
    let data_succ = parse_bounds(d0, d1, false, SegmentId::Data, conf.data);
    let anal_succ = parse_bounds(a0, a1, true, SegmentId::Analysis, conf.analysis);
    let mut succ = vers_succ.combine4(text_succ, data_succ, anal_succ, |ver, t, d, a| {
        if let (Some(version), Some(text), Some(data), Some(analysis)) = (ver, t, d, a) {
            Some(Header {
                version: conf.version_override.unwrap_or(version),
                text,
                data,
                analysis,
            })
        } else {
            None
        }
    });
    if !spaces.chars().all(|x| x == ' ') {
        succ.push_error("version must be followed by 4 spaces".into());
    }
    PureMaybe::into_result(succ, "could not parse HEADER fields".to_string())
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

impl fmt::Display for VersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not parse FCS Version")
    }
}
