use crate::config::HeaderConfig;
use crate::error::*;
use crate::macros::{enum_from, enum_from_disp, match_many_to_one};
use crate::segment::*;

use nonempty::NonEmpty;
use serde::Serialize;
use std::fmt;
use std::io::{BufReader, Read};
use std::num::ParseIntError;
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
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Serialize)]
pub enum Version {
    FCS2_0,
    FCS3_0,
    FCS3_1,
    FCS3_2,
}

/// The three segments from the HEADER
#[derive(Clone, Serialize)]
pub struct HeaderSegments {
    pub text: PrimaryTextSegment,
    pub data: HeaderDataSegment,
    pub analysis: HeaderAnalysisSegment,
    // TODO add OTHER
}

/// Output from parsing the FCS header.
///
/// Includes version and the three main segments (TEXT, DATA, ANALYSIS). For
/// now, OTHER segments are ignored. This may change in the future. Segments may
/// or may not be adjusted using configuration parameters to correct for errors.
///
/// Only valid segments are to be put in this struct (ie begin <= end).
#[derive(Clone, Serialize)]
pub struct Header {
    pub version: Version,
    pub segments: HeaderSegments,
}

pub fn h_read_header<R: Read>(
    h: &mut BufReader<R>,
    conf: &HeaderConfig,
) -> MultiResult<Header, ImpureError<HeaderError>> {
    let mut verbuf = [0; HEADER_LEN];
    h.read_exact(&mut verbuf).into_mult()?;
    if verbuf.is_ascii() {
        let hs = unsafe { str::from_utf8_unchecked(&verbuf) };
        parse_header(hs, conf).map_err(|es| es.map(ImpureError::Pure))
    } else {
        Err(NonEmpty::new(ImpureError::Pure(HeaderError::NotAscii)))
    }
}

fn parse_header(s: &str, conf: &HeaderConfig) -> MultiResult<Header, HeaderError> {
    let v = &s[0..VERSION_END];
    let spaces = &s[VERSION_END..SPACE_END];
    let t0 = &s[SPACE_END..T0_END];
    let t1 = &s[T0_END..T1_END];
    let d0 = &s[T1_END..D0_END];
    let d1 = &s[D0_END..D1_END];
    let a0 = &s[D1_END..A0_END];
    let a1 = &s[A0_END..A1_END];
    let vers_res = v
        .parse::<Version>()
        .map_err(HeaderError::Version)
        .into_mult();
    let space_res = if !spaces.chars().all(|x| x == ' ') {
        Err(NonEmpty::new(HeaderError::Space))
    } else {
        Ok(())
    };
    let text_res = parse_segment(t0, t1, false, conf.text);
    let data_res = parse_segment(d0, d1, false, conf.data);
    let anal_res = parse_segment(a0, a1, true, conf.analysis);
    vers_res
        .mult_zip3(space_res, text_res)
        .mult_zip3(data_res, anal_res)
        .map(|((version, _, text), data, analysis)| Header {
            version: conf.version_override.unwrap_or(version),
            segments: HeaderSegments {
                text,
                data,
                analysis,
            },
        })
}

fn parse_header_offset<I: HasRegion>(
    s: &str,
    allow_blank: bool,
    is_begin: bool,
) -> Result<u32, ParseOffsetError> {
    let trimmed = s.trim_start();
    if allow_blank && trimmed.is_empty() {
        return Ok(0);
    }
    trimmed.parse().map_err(|error| ParseOffsetError {
        error,
        is_begin,
        location: I::REGION,
        source: s.to_string(),
    })
}

fn parse_segment<I: HasRegion>(
    s0: &str,
    s1: &str,
    allow_blank: bool,
    corr: OffsetCorrection<I, SegmentFromHeader>,
) -> MultiResult<SpecificSegment<I, SegmentFromHeader>, HeaderError> {
    let parse_one = |s, is_begin| {
        parse_header_offset::<I>(s, allow_blank, is_begin).map_err(HeaderSegmentError::Parse)
    };
    let begin_res = parse_one(s0, true);
    let end_res = parse_one(s1, false);
    begin_res
        .zip(end_res)
        .and_then(|(begin, end)| {
            SpecificSegment::try_new(begin, end, corr, SegmentFromHeader)
                .map_err(HeaderSegmentError::Segment)
                .map_err(NonEmpty::new)
        })
        .map_err(|es| es.map(HeaderError::Segment))
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

pub enum HeaderError {
    Segment(HeaderSegmentError),
    Space,
    Version(VersionError),
    NotAscii,
}

impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            HeaderError::Segment(x) => x.fmt(f),
            HeaderError::Version(x) => x.fmt(f),
            HeaderError::Space => write!(f, "version must be followed by 4 spaces"),
            HeaderError::NotAscii => write!(f, "HEADER must be ASCII"),
        }
    }
}

enum_from_disp!(
    pub HeaderSegmentError,
    [Segment, SegmentError],
    [Parse, ParseOffsetError]
);

pub struct ParseOffsetError {
    error: ParseIntError,
    is_begin: bool,
    location: &'static str,
    source: String,
}

impl fmt::Display for ParseOffsetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let which = if self.is_begin { "begin" } else { "end" };
        write!(
            f,
            "parse error for {which} offset in {} segment from source '{}': {}",
            self.location, self.source, self.error
        )
    }
}

pub struct VersionError;

impl fmt::Display for VersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not parse FCS Version")
    }
}
