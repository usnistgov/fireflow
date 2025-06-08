use crate::config::HeaderConfig;
use crate::error::*;
use crate::segment::*;
use crate::text::keywords::*;
use crate::validated::ascii_uint::*;
use crate::validated::standard::*;

use nonempty::NonEmpty;
use serde::Serialize;
use std::fmt;
use std::io;
use std::io::{BufReader, BufWriter, Read, Write};
use std::iter::repeat;
use std::str;

/// The length of the HEADER.
///
/// This should always be the same. This also assumes that there are no OTHER
/// segments (which for now are not supported).
pub const HEADER_LEN: u8 = 58;

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

impl HeaderSegments {
    pub(crate) fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        version: Version,
    ) -> io::Result<()> {
        for s in [
            version.to_string(),
            self.text.header_string(),
            self.data.header_string(),
            self.analysis.header_string(),
        ]
        .into_iter()
        .chain(self.other.iter().map(|x| x.header_string()))
        {
            h.write_all(s.as_bytes())?;
        }
        Ok(())
    }
}

/// Output from parsing the FCS header.
///
/// Includes version and the three main segments (TEXT, DATA, ANALYSIS) plus
/// any OTHER segments after the first 58 bytes.
///
/// Only valid segments are to be put in this struct (ie begin <= end).
#[derive(Clone, Serialize)]
pub struct Header {
    pub version: Version,
    pub segments: HeaderSegments,
}

impl Header {
    pub fn h_read<R: Read>(
        h: &mut BufReader<R>,
        conf: &HeaderConfig,
    ) -> MultiResult<Self, ImpureError<HeaderError>> {
        h_read_standard_header(h, conf).and_then(|(version, text, data, analysis)| {
            [
                text.inner.try_coords(),
                data.inner.try_coords(),
                analysis.inner.try_coords(),
            ]
            .iter()
            .flatten()
            .map(|(x, _)| x)
            .min()
            .map_or(Ok(vec![]), |earliest_begin| {
                h_read_other_segments(h, *earliest_begin, &conf.other[..])
            })
            .map(|other| Self {
                version,
                segments: HeaderSegments {
                    text,
                    data,
                    analysis,
                    other,
                },
            })
            .and_then(|hdr| {
                hdr.validate()
                    .map_err(HeaderError::Validation)
                    .map_err(ImpureError::Pure)
                    .into_mult()?;
                Ok(hdr)
            })
        })
    }

    /// Return number of bytes required to encode HEADER
    fn nbytes(&self) -> u64 {
        u64::from(HEADER_LEN) + (self.segments.other.len() as u64) * 16
    }

    fn validate(&self) -> Result<(), HeaderValidationError> {
        let s = &self.segments;
        // ensure segments don't overlap
        let xs: Vec<_> = s
            .other
            .iter()
            .copied()
            .map(|x| x.inner)
            .chain([s.text.inner, s.data.inner, s.analysis.inner])
            .collect();
        if Segment::any_overlap(&xs[..]) {
            return Err(HeaderValidationError::Overlap);
        }

        // ensure segments don't start within header
        let n = self.nbytes();
        if xs
            .iter()
            .flat_map(|x| x.as_nonempty())
            .any(|x| x.as_u64().coords().0 < n)
        {
            return Err(HeaderValidationError::InHeader);
        }

        Ok(())
    }
}

fn h_read_standard_header<R: Read>(
    h: &mut BufReader<R>,
    conf: &HeaderConfig,
) -> MultiResult<
    (
        Version,
        PrimaryTextSegment,
        HeaderDataSegment,
        HeaderAnalysisSegment,
    ),
    ImpureError<HeaderError>,
> {
    let vers_res = Version::h_read(h)
        .map_err(NonEmpty::new)
        .mult_map_errors(|e| e.map_inner(HeaderError::Version));
    let space_res = h_read_spaces(h).map_err(NonEmpty::new);
    let text_res = PrimaryTextSegment::h_read_offsets(h, false, conf.text);
    let data_res = HeaderDataSegment::h_read_offsets(h, false, conf.data);
    let anal_res = HeaderAnalysisSegment::h_read_offsets(h, true, conf.analysis);
    let offset_res = text_res
        .mult_zip3(data_res, anal_res)
        .mult_map_errors(|e| e.map_inner(HeaderError::Segment));
    vers_res
        .mult_zip3(space_res, offset_res)
        .map(|(version, (), (text, data, analysis))| {
            (
                conf.version_override.unwrap_or(version),
                text,
                data,
                analysis,
            )
        })
}

fn h_read_spaces<R: Read>(h: &mut BufReader<R>) -> Result<(), ImpureError<HeaderError>> {
    let mut buf = [0_u8; 4];
    h.read_exact(&mut buf)?;
    if buf.iter().all(|x| *x == 32) {
        Ok(())
    } else {
        Err(ImpureError::Pure(HeaderError::Space))
    }
}

fn h_read_other_segments<R: Read>(
    h: &mut BufReader<R>,
    text_begin: Uint8Digit,
    corrs: &[OffsetCorrection<OtherSegmentId, SegmentFromHeader>],
) -> MultiResult<Vec<OtherSegment>, ImpureError<HeaderError>> {
    // ASSUME this won't fail because we checked that each offset is greater
    // than this
    let n = u64::from(text_begin) - u64::from(HEADER_LEN);
    let mut buf0 = [0_u8; 8];
    let mut buf1 = [0_u8; 8];
    let n_segs = (n / 16) as usize;

    corrs
        .iter()
        .copied()
        .chain(repeat(OffsetCorrection::default()))
        .take(n_segs)
        .map(|corr| {
            h.read_exact(&mut buf0).into_mult()?;
            h.read_exact(&mut buf1).into_mult()?;
            // If any regions are entirely blank, just ignore them
            if buf0.iter().chain(buf1.iter()).all(|x| *x == 32) {
                Ok(None)
            } else {
                OtherSegment::parse(&buf0, &buf1, false, corr)
                    .map(Some)
                    .mult_map_errors(HeaderError::Segment)
                    .mult_map_errors(ImpureError::Pure)
            }
        })
        .gather()
        .map_err(NonEmpty::flatten)
        .map(|os| os.into_iter().flatten().collect())
}

impl Version {
    fn h_read<R: Read>(h: &mut BufReader<R>) -> Result<Self, ImpureError<VersionError>> {
        let mut buf = [0; 6];
        h.read_exact(&mut buf)?;
        if buf.is_ascii() {
            let s = unsafe { str::from_utf8_unchecked(&buf) };
            s.parse().map_err(ImpureError::Pure)
        } else {
            Err(ImpureError::Pure(VersionError))
        }
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
    Validation(HeaderValidationError),
}

impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Segment(x) => x.fmt(f),
            Self::Version(x) => x.fmt(f),
            Self::Validation(x) => x.fmt(f),
            Self::Space => f.write_str("version must be followed by 4 spaces"),
        }
    }
}

pub enum HeaderValidationError {
    Overlap,
    InHeader,
}

impl fmt::Display for HeaderValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Overlap => f.write_str("segment(s) in HEADER overlap"),
            Self::InHeader => f.write_str("segment(s) start within HEADER"),
        }
    }
}

pub struct VersionError;

impl fmt::Display for VersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "could not parse FCS Version")
    }
}

pub(crate) struct HeaderKeywordsToWrite {
    pub(crate) header: HeaderSegments,
    pub(crate) primary: KeywordsWriter,
    pub(crate) supplemental: KeywordsWriter,
    // TODO do something useful with this
    pub(crate) _nextdata: Nextdata,
}

#[derive(Default)]
pub(crate) struct KeywordsWriter(pub Vec<(String, String)>);

impl KeywordsWriter {
    pub(crate) fn h_write<W: Write>(&self, h: &mut BufWriter<W>, delim: u8) -> io::Result<()> {
        h.write_all(&[delim])?; // write first delim
        for s in self.0.iter().flat_map(|(k, v)| [k, v]) {
            h.write_all(s.as_bytes())?;
            h.write_all(&[delim])?;
        }
        Ok(())
    }
}

/// Create HEADER+TEXT+OTHER offsets for FCS 2.0
pub(crate) fn make_data_offset_keywords_2_0(
    req: Vec<(String, String)>,
    opt: Vec<(String, String)>,
    data_len: u64,
    analysis_len: u64,
    other_lens: Vec<u64>,
) -> Result<HeaderKeywordsToWrite, Uint8DigitOverflow> {
    let (other_segs, other_header_len, other_segments_len) = other_segments(other_lens)?;

    let text_begin: Uint8Digit =
        (u64::from(HEADER_LEN) + other_header_len + other_segments_len).try_into()?;
    // +1 at end accounts for first delimiter
    let text_len =
        raw_keywords_length(&req[..]) + raw_keywords_length(&opt[..]) + nextdata_len() + 1;
    let text_seg = PrimaryTextSegment::try_new_with_len(text_begin, text_len)?;

    let data_begin = text_seg
        .inner
        .try_next_byte()
        .map_or(Ok(text_begin), |x| x.try_into())?;
    let data_seg = HeaderDataSegment::try_new_with_len(data_begin, data_len)?;

    let analysis_begin = data_seg
        .inner
        .try_next_byte()
        .map_or(Ok(text_begin), |x| x.try_into())?;
    let analysis_seg = HeaderAnalysisSegment::try_new_with_len(analysis_begin, analysis_len)?;

    let nextdata = Nextdata(Uint8Char(
        analysis_seg
            .inner
            .try_next_byte()
            .map_or(Ok(analysis_begin), |x| x.try_into())
            .ok()
            .unwrap_or_default(),
    ));

    let header = HeaderSegments {
        text: text_seg,
        data: data_seg,
        analysis: analysis_seg,
        other: other_segs,
    };

    let primary = KeywordsWriter(
        [nextdata.pair()]
            .into_iter()
            .chain(req)
            .chain(opt)
            .collect(),
    );

    Ok(HeaderKeywordsToWrite {
        header,
        primary,
        supplemental: KeywordsWriter::default(),
        _nextdata: nextdata,
    })
}

/// Create HEADER+TEXT+OTHER offsets for FCS 3.0
///
/// Order in which this is expected to be written is HEADER, OTHER(s), TEXT,
/// STEXT, DATA, ANALYSIS.
pub(crate) fn make_data_offset_keywords_3_0(
    req: Vec<(String, String)>,
    opt: Vec<(String, String)>,
    data_len: u64,
    analysis_len: u64,
    other_lens: Vec<u64>,
) -> Result<HeaderKeywordsToWrite, Uint8DigitOverflow> {
    let (other_segs, other_header_len, other_segments_len) = other_segments(other_lens)?;
    let prim_text_begin: Uint8Digit =
        (u64::from(HEADER_LEN) + other_header_len + other_segments_len).try_into()?;

    let nooffset_req_text_len = raw_keywords_length(&req[..]);
    let opt_text_len = raw_keywords_length(&opt[..]);
    // +1 accounts for first delimiter
    let nosupp_text_len = offsets_len() + nooffset_req_text_len + 1;
    let supp_text_len = opt_text_len + 1;
    let all_text_len = opt_text_len + nosupp_text_len;

    // include STEXT only if the optional keywords don't fit within the first
    // 99,999,999 bytes
    let (prim_text_seg, supp_text_seg) =
        PrimaryTextSegment::try_new_with_len(prim_text_begin, all_text_len)
            .map(|p| (p, SupplementalTextSegment::default()))
            .or(
                PrimaryTextSegment::try_new_with_len(prim_text_begin, nosupp_text_len).map(|p| {
                    let supp_text_begin = p
                        .inner
                        .try_next_byte()
                        .map_or(u64::from(prim_text_begin).into(), |x| x.into());
                    let s = SupplementalTextSegment::new_with_len(supp_text_begin, supp_text_len);
                    (p, s)
                }),
            )?;

    let data_begin = supp_text_seg
        .inner
        .try_next_byte()
        .or(prim_text_seg.inner.try_next_byte())
        .unwrap_or(u64::from(prim_text_begin))
        .into();

    let data_seg = TEXTDataSegment::new_with_len(data_begin, data_len);

    let analysis_begin = data_seg
        .inner
        .try_next_byte()
        .map(|x| x.into())
        .unwrap_or(data_begin);
    let analysis_seg = TEXTAnalysisSegment::new_with_len(analysis_begin, analysis_len);

    let h_analysis_seg = analysis_seg.as_header();
    let h_data_seg = data_seg.as_header();

    let nextdata = Nextdata(Uint8Char(
        analysis_seg
            .inner
            .try_next_byte()
            .unwrap_or(u64::from(analysis_begin))
            .try_into()
            .ok()
            .unwrap_or_default(),
    ));

    // NOTE in 3.2 *DATA and *SDATA are technically optional, but it is much
    // easier just to include them in the "required" stuff regardless.
    let all_req = supp_text_seg
        .keywords()
        .into_iter()
        .chain(data_seg.keywords())
        .chain(analysis_seg.keywords())
        .chain([nextdata.pair()])
        .chain(req);

    let (primary, supplemental) = if supp_text_seg.inner.is_empty() {
        (all_req.chain(opt).collect(), vec![])
    } else {
        (all_req.collect(), opt)
    };

    let header = HeaderSegments {
        text: prim_text_seg,
        analysis: h_analysis_seg,
        data: h_data_seg,
        other: other_segs,
    };

    Ok(HeaderKeywordsToWrite {
        header,
        primary: KeywordsWriter(primary),
        supplemental: KeywordsWriter(supplemental),
        _nextdata: nextdata,
    })
}

fn raw_keywords_length(ks: &[(String, String)]) -> u64 {
    ks.iter().map(|(k, v)| k.len() + v.len() + 2).sum::<usize>() as u64
}

fn other_segments(
    other_lens: Vec<u64>,
) -> Result<(Vec<OtherSegment>, u64, u64), Uint8DigitOverflow> {
    let mut os = vec![];
    let mut begin: Uint8Digit = HEADER_LEN.into();
    for length in other_lens {
        let seg = OtherSegment::try_new_with_len(begin, length)?;
        begin = (u64::from(begin) + length).try_into()?;
        os.push(seg);
    }
    let total_length = os.iter().map(|s| s.inner.len()).sum();
    let header_length = (os.len() as u64) * 16;
    Ok((os, header_length, total_length))
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
