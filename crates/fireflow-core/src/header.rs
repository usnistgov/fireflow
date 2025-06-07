use crate::config::HeaderConfig;
use crate::error::*;
use crate::segment::*;
use crate::text::keywords::*;
use crate::validated::standard::*;

use nonempty::NonEmpty;
use serde::Serialize;
use std::fmt;
use std::io::{BufReader, Read};
use std::iter::repeat;
use std::str;

/// The length of the HEADER.
///
/// This should always be the same. This also assumes that there are no OTHER
/// segments (which for now are not supported).
pub const HEADER_LEN: u8 = A1_END;

const VERSION_END: u8 = 6;
const SPACE_END: u8 = VERSION_END + 4;
const T0_END: u8 = SPACE_END + 8;
const T1_END: u8 = T0_END + 8;
const D0_END: u8 = T1_END + 8;
const D1_END: u8 = D0_END + 8;
const A0_END: u8 = D1_END + 8;
const A1_END: u8 = A0_END + 8;

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
    pub other: Vec<OtherSegment>,
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
    let mut verbuf = [0; HEADER_LEN as usize];
    h.read_exact(&mut verbuf).into_mult()?;
    if verbuf.is_ascii() {
        let hs = unsafe { str::from_utf8_unchecked(&verbuf) };
        parse_header(hs, conf)
            .mult_map_errors(ImpureError::Pure)
            .and_then(|(version, text, data, analysis)| {
                h_read_other_segments(h, text.inner.begin(), &conf.other[..]).map(|other| Header {
                    version,
                    segments: HeaderSegments {
                        text,
                        data,
                        analysis,
                        other,
                    },
                })
            })
    } else {
        Err(NonEmpty::new(ImpureError::Pure(HeaderError::NotAscii)))
    }
}

fn parse_header(
    s: &str,
    conf: &HeaderConfig,
) -> MultiResult<
    (
        Version,
        PrimaryTextSegment,
        HeaderDataSegment,
        HeaderAnalysisSegment,
    ),
    HeaderError,
> {
    let v = &s[0..VERSION_END.into()];
    let spaces = &s[usize::from(VERSION_END)..SPACE_END.into()];
    let t0 = &s[usize::from(SPACE_END)..T0_END.into()];
    let t1 = &s[usize::from(T0_END)..T1_END.into()];
    let d0 = &s[usize::from(T1_END)..D0_END.into()];
    let d1 = &s[usize::from(D0_END)..D1_END.into()];
    let a0 = &s[usize::from(D1_END)..A0_END.into()];
    let a1 = &s[usize::from(A0_END)..A1_END.into()];
    let vers_res = v
        .parse::<Version>()
        .map_err(HeaderError::Version)
        .into_mult();
    let space_res = if !spaces.chars().all(|x| x == ' ') {
        Err(NonEmpty::new(HeaderError::Space))
    } else {
        Ok(())
    };
    let text_res = PrimaryTextSegment::parse(t0, t1, false, conf.text);
    let data_res = HeaderDataSegment::parse(d0, d1, false, conf.data);
    let anal_res = HeaderAnalysisSegment::parse(a0, a1, true, conf.analysis);
    let offset_res = text_res
        .mult_zip3(data_res, anal_res)
        .mult_map_errors(HeaderError::Segment);
    vers_res
        .mult_zip3(space_res, offset_res)
        .map(|(version, _, (text, data, analysis))| {
            (
                conf.version_override.unwrap_or(version),
                text,
                data,
                analysis,
            )
        })
}

fn h_read_other_segments<R: Read>(
    h: &mut BufReader<R>,
    text_begin: u64,
    corrs: &[OffsetCorrection<OtherSegmentId, SegmentFromHeader>],
) -> MultiResult<Vec<OtherSegment>, ImpureError<HeaderError>> {
    // ASSUME this won't fail because we checked that each offset is greater
    // than this
    let n = text_begin - u64::from(HEADER_LEN);
    // if less than 16 bytes (the width of two offsets) then nothing to do
    if n < 16 {
        return Ok(vec![]);
    }
    let mut buf = vec![];
    // ASSUME cursor is at HEADER_LEN
    h.take(u64::from(n)).read_to_end(&mut buf).into_mult()?;
    if buf.is_ascii() {
        let mut xs = unsafe { str::from_utf8_unchecked(&buf) };
        // if all spaces, nothing to do
        if xs.chars().all(|x| x == ' ') {
            return Ok(vec![]);
        }

        // chop the bytes into 8-byte chunks which hopefully have the offsets
        let mut raw_offsets = vec![];
        while let Some((left, right, rest)) = xs.split_at_checked(8).and_then(|(left, rest0)| {
            rest0
                .split_at_checked(8)
                .map(|(right, rest1)| (left, right, rest1))
        }) {
            raw_offsets.push((left, right));
            xs = rest;
        }

        let padded_corrs = corrs
            .iter()
            .copied()
            .chain(repeat(OffsetCorrection::default()))
            .take(raw_offsets.len());

        raw_offsets
            .into_iter()
            .zip(padded_corrs)
            .map(|((left, right), corr)| OtherSegment::parse(left, right, false, corr))
            .gather()
            .map_err(NonEmpty::flatten)
            .mult_map_errors(HeaderError::Segment)
            .mult_map_errors(ImpureError::Pure)
    } else {
        Err(NonEmpty::new(ImpureError::Pure(HeaderError::OtherNotAscii)))
    }
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
    OtherNotAscii,
}

impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            HeaderError::Segment(x) => x.fmt(f),
            HeaderError::Version(x) => x.fmt(f),
            HeaderError::Space => write!(f, "version must be followed by 4 spaces"),
            HeaderError::NotAscii => write!(f, "HEADER must be ASCII"),
            HeaderError::OtherNotAscii => write!(f, "OTHER offsets must be ASCII"),
        }
    }
}

// enum_from_disp!(
//     pub HeaderSegmentError,
//     [Segment, SegmentError],
//     [Parse, ParseOffsetError]
// );

// pub struct ParseOffsetError {
//     error: ParseIntError,
//     is_begin: bool,
//     location: &'static str,
//     source: String,
// }

// impl fmt::Display for ParseOffsetError {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
//         let which = if self.is_begin { "begin" } else { "end" };
//         write!(
//             f,
//             "parse error for {which} offset in {} segment from source '{}': {}",
//             self.location, self.source, self.error
//         )
//     }
// }

pub struct VersionError;

impl fmt::Display for VersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not parse FCS Version")
    }
}

/// HEADER and TEXT offsets
pub struct OffsetFormatResult {
    pub header: HeaderSegments,

    /// The offset TEXT keywords and their values.
    ///
    /// For 2.0 this will only contain $NEXTDATA. For 3.0+, this will contain,
    /// (BEGIN|END)(STEXT|ANALYSIS|DATA).
    pub stext: Option<SupplementalTextSegment>,
    pub data: Option<TEXTDataSegment>,
    pub analysis: Option<TEXTAnalysisSegment>,

    /// The offset where the next data segment can start.
    ///
    /// If beyond 99,999,999 bytes, this will be zero.
    pub real_nextdata: Nextdata,
}

/// Create HEADER+TEXT+OTHER offsets for FCS 2.0
pub fn make_data_offset_keywords_2_0(
    nooffset_text_len: u64,
    data_len: u64,
    analysis_len: u64,
    other_lens: Vec<u64>,
) -> Option<OffsetFormatResult> {
    let (other_segs, other_header_len, other_segments_len) = other_segments(other_lens);
    // +1 at end accounts for first delimiter
    let begin_text = u64::from(HEADER_LEN) + other_header_len + other_segments_len + 1;
    let text_len = nextdata_len() + nooffset_text_len;

    let text_seg = PrimaryTextSegment::new_with_len(begin_text, text_len);
    let data_seg = HeaderDataSegment::new_with_len(text_seg.inner.next(), data_len);
    let anal_seg = HeaderAnalysisSegment::new_with_len(data_seg.inner.next(), analysis_len);
    let nextdata = anal_seg.inner.next().try_into().ok().unwrap_or_default();

    Some(OffsetFormatResult {
        header: HeaderSegments {
            text: text_seg,
            data: data_seg,
            analysis: anal_seg,
            other: other_segs,
        },
        stext: None,
        data: None,
        analysis: None,
        real_nextdata: Nextdata(nextdata),
    })
}

/// Create HEADER+TEXT+OTHER offsets for FCS 3.0
///
/// Order in which this is expected to be written is HEADER, OTHER(s), TEXT,
/// STEXT, DATA, ANALYSIS.
pub fn make_data_offset_keywords_3_0(
    nooffset_req_text_len: u64,
    opt_text_len: u64,
    data_len: u64,
    analysis_len: u64,
    other_lens: Vec<u64>,
) -> Option<OffsetFormatResult> {
    let (other_segs, other_header_len, other_segments_len) = other_segments(other_lens);
    // +1 at end accounts for first delimiter
    let begin_prim_text = u64::from(HEADER_LEN) + other_header_len + other_segments_len + 1;

    // // Compute the length of (S)TEXT
    let nosupp_text_len = offsets_len() + nooffset_req_text_len;
    let all_text_len = opt_text_len + nosupp_text_len;

    let (prim_text_seg, supp_text_seg) =
        if begin_prim_text + all_text_len <= u64::from(MAX_HEADER_OFFSET) {
            // all of TEXT can fit in in HEADER, so no need for Supp TEXT
            let p = PrimaryTextSegment::new_with_len(begin_prim_text, all_text_len);
            let s = SupplementalTextSegment::default();
            (p, s)
        } else if begin_prim_text + nosupp_text_len <= u64::from(MAX_HEADER_OFFSET) {
            // otherwise make Supp TEXT
            let p = PrimaryTextSegment::new_with_len(begin_prim_text, nosupp_text_len);
            let s = SupplementalTextSegment::new_with_len(p.inner.next(), opt_text_len);
            (p, s)
        } else {
            // If HEADER+TEXT(required)+OTHER exceeds 99,999,999 bytes, we are stuck
            return None;
        };

    let begin_data = prim_text_seg.inner.next() + supp_text_seg.inner.len();

    let data_seg = TEXTDataSegment::new_with_len(begin_data, data_len);
    let anal_seg = TEXTAnalysisSegment::new_with_len(data_seg.inner.next(), analysis_len);

    let h_anal_seg =
        HeaderAnalysisSegment::new_with_len(anal_seg.inner.begin(), anal_seg.inner.len());
    let h_data_seg = HeaderDataSegment::new_with_len(data_seg.inner.begin(), data_seg.inner.len());

    let nextdata = anal_seg.inner.next().try_into().ok().unwrap_or_default();

    Some(OffsetFormatResult {
        header: HeaderSegments {
            text: prim_text_seg,
            analysis: h_anal_seg,
            data: h_data_seg,
            other: other_segs,
        },
        stext: Some(supp_text_seg),
        data: Some(data_seg),
        analysis: Some(anal_seg),
        real_nextdata: Nextdata(nextdata),
    })
}

fn other_segments(other_lens: Vec<u64>) -> (Vec<OtherSegment>, u64, u64) {
    let os: Vec<_> = other_lens
        .into_iter()
        .filter(|x| *x > 0)
        .scan(HEADER_LEN.into(), |begin, length| {
            let ret = OtherSegment::new_with_len(*begin, length);
            *begin = *begin + length;
            Some(ret)
        })
        .collect();
    let total_length = os.iter().map(|s| s.inner.len()).sum();
    let header_length = (os.len() as u64) * 16;
    (os, header_length, total_length)
}

// /// Compute length occupied by OTHER segments.
// ///
// /// This includes the length of the segments themselves plus the length
// /// added to the back of the HEADER to encode their offsets.
// fn other_segment_lengths(other_lens: Vec<usize>) -> (usize, usize) {
//     (other_lens.len() * 16, other_lens.iter().sum::<usize>())
// }

/// Create offset keyword pairs for HEADER
///
/// Returns two right-aligned, space-padded numbers exactly 8 bytes long as one
/// contiguous string.
pub fn offset_header_string(begin: u64, end: u64) -> String {
    let nbytes = end - begin + 1;
    let (b, e) = if end <= u64::from(MAX_HEADER_OFFSET) && nbytes > 0 {
        (begin, end)
    } else {
        (0, 0)
    };
    format!("{:0>8}{:0>8}", b, e)
}

/// Compute $NEXTDATA offset and format the keyword pair.
///
/// Returns something like (12345678, ["$NEXTDATA", "12345678"])
pub fn offset_nextdata_string(nextdata: u64) -> (u64, (String, String)) {
    let n = if nextdata > u64::from(MAX_HEADER_OFFSET) {
        0
    } else {
        nextdata
    };
    let s = format_zero_padded(n, NEXTDATA_VAL_LEN);
    (n, (Nextdata::std().to_string(), s))
}

/// Compute the number of digits for a number.
///
/// Assume number is greater than 0 and in decimal radix.
fn n_digits(x: u64) -> u64 {
    // TODO cast?
    let n = u64::ilog10(x) as u64;
    if 10 ^ n == x {
        n
    } else {
        n + 1
    }
}

/// Format a digit with left-padded zeros to a given length
pub(crate) fn format_zero_padded(x: u64, width: u64) -> String {
    format!("{}{}", ("0").repeat((width - n_digits(x)) as usize), x)
}

/// Length of the $NEXTDATA offset length.
///
/// This value has a maximum of 99,999,999, and as such the length of this
/// number is always 8 bytes.
const NEXTDATA_VAL_LEN: u64 = 8;

/// Length of $(BEGIN/END)(STEXT/ANALYSIS/DATA) offset length.
///
/// This was chosen on the basis that the maximum file size is 2^64, and thus
/// the maximum offset is the number of digits in 2^64, which is 20. This will
/// "waste" very little space in TEXT and will make computing the TEXT width
/// much easier.
pub(crate) const OFFSET_VAL_LEN: u64 = 20;

/// The maximum value that may be stored in a HEADER offset.
pub(crate) const MAX_HEADER_OFFSET: u32 = 99_999_999;

/// Number of bytes consumed by $NEXTDATA keyword + value + delimiters
fn nextdata_len() -> u64 {
    Nextdata::len() + NEXTDATA_VAL_LEN + 2
}

/// The number of bytes each offset is expected to take.
///
/// These are the length of each keyword + 2 since there should be two
/// delimiters counting toward its byte real estate.
fn data_len() -> u64 {
    Begindata::len() + Enddata::len() + OFFSET_VAL_LEN * 2 + 4
}

fn analysis_len() -> u64 {
    Beginanalysis::len() + Endanalysis::len() + OFFSET_VAL_LEN * 2 + 4
}

fn supp_text_len() -> u64 {
    Beginstext::len() + Endstext::len() + OFFSET_VAL_LEN * 2 + 4
}

/// The total number of bytes offset keywords are expected to take.
///
/// This only applies to 3.0+ since 2.0 only has NEXTDATA.
fn offsets_len() -> u64 {
    data_len() + analysis_len() + supp_text_len() + nextdata_len()
}
