use crate::config::{HeaderConfigInner, ReadState};
use crate::error::*;
use crate::segment::*;
use crate::text::keywords::*;
use crate::text::parser::*;
use crate::validated::ascii_uint::*;
use crate::validated::keys::*;

use derive_more::{Display, From};
use itertools::Itertools;
use nonempty::NonEmpty;
use std::fmt;
use std::io;
use std::io::{BufReader, BufWriter, Read, Write};
use std::iter::repeat;
use std::str;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// The length of the HEADER.
///
/// This should always be the same. This also assumes that there are no OTHER
/// segments (which for now are not supported).
pub const HEADER_LEN: u8 = 58;

/// All FCS versions this library supports.
///
/// This appears as the first 6 bytes of any valid FCS file.
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Version {
    FCS2_0,
    FCS3_0,
    FCS3_1,
    FCS3_2,
}

macro_rules! impl_version {
    ($name:ident, $var:ident) => {
        #[derive(Clone, Copy, Eq, PartialEq)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        pub struct $name;

        impl From<$name> for Version {
            fn from(_: $name) -> Self {
                Self::$var
            }
        }
    };
}

impl_version!(Version2_0, FCS2_0);
impl_version!(Version3_0, FCS3_0);
impl_version!(Version3_1, FCS3_1);
impl_version!(Version3_2, FCS3_2);

/// The three segments from the HEADER
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
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
        // ASSUME this is a total of 58 bytes long (sans OTHER)
        for s in [
            version.to_string(),           // 6 bytes
            "    ".to_string(),            // 4 bytes
            self.text.header_string(),     // 16 bytes
            self.data.header_string(),     // 16 bytes
            self.analysis.header_string(), // 16 bytes
        ]
        .into_iter()
        // TODO the other segments will each be 20 chars wide and padded with 0,
        // which is probably overkill
        .chain(self.other.iter().map(|x| x.header_string()))
        {
            h.write_all(s.as_bytes())?;
        }
        Ok(())
    }

    /// Check if TEXT segment starts within HEADER
    pub(crate) fn contains_text_segment<I>(&self, s: TEXTSegment<I>) -> Result<(), InHeaderError>
    where
        I: HasRegion,
    {
        s.try_as_generic()
            .map_or(Ok(()), |q| self.contains_segment(q))
    }

    /// Check if TEXT segment overlaps with any in HEADER.
    ///
    /// Assume HEADER itself has no overlapping segments.
    pub(crate) fn overlaps_with<I>(&self, s: TEXTSegment<I>) -> MultiResult<(), SegmentOverlapError>
    where
        I: HasRegion,
    {
        if let Some(q) = s.try_as_generic() {
            self.as_generics().map(|x| x.overlaps(&q)).gather().void()
        } else {
            Ok(())
        }
    }

    /// Ensure HEADER segments don't overlap and start after HEADER itself
    fn validate(&self) -> MultiResult<(), HeaderValidationError> {
        let x = self.overlapping_segments().mult_errors_into();
        let y = self.contains_header_segments().mult_errors_into();
        x.mult_zip(y).void()
    }

    fn contains_header_segments(&self) -> MultiResult<(), InHeaderError> {
        let t = self.contains_header_segment(self.text);
        let d = self.contains_header_segment(self.data);
        let a = self.contains_header_segment(self.analysis);
        let os = self
            .other
            .iter()
            .copied()
            .map(|o| self.contains_header_segment(o))
            .gather();
        t.zip3(d, a).mult_zip(os).void()
    }

    fn contains_header_segment<I, S, T>(
        &self,
        s: SpecificSegment<I, S, T>,
    ) -> Result<(), InHeaderError>
    where
        I: HasRegion,
        S: HasSource,
        T: Into<u64> + Copy,
    {
        s.try_as_generic()
            .map_or(Ok(()), |q| self.contains_segment(q))
    }

    fn contains_segment(&self, s: GenericSegment) -> Result<(), InHeaderError> {
        if s.begin < self.nbytes() {
            Err(InHeaderError(s))
        } else {
            Ok(())
        }
    }

    fn overlapping_segments(&self) -> MultiResult<(), SegmentOverlapError> {
        GenericSegment::find_overlaps(self.as_generics().collect())
    }

    /// Return number of bytes required to encode HEADER
    fn nbytes(&self) -> u64 {
        u64::from(HEADER_LEN) + (self.other.len() as u64) * 16
    }

    fn as_generics(&self) -> impl Iterator<Item = GenericSegment> {
        self.other
            .iter()
            .copied()
            .map(|x| x.try_as_generic())
            .chain([
                self.text.try_as_generic(),
                self.data.try_as_generic(),
                self.analysis.try_as_generic(),
            ])
            .flatten()
    }
}

/// Output from parsing the FCS header.
///
/// Includes version and the three main segments (TEXT, DATA, ANALYSIS) plus
/// any OTHER segments after the first 58 bytes.
///
/// Only valid segments are to be put in this struct (ie begin <= end).
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct Header {
    pub version: Version,
    pub segments: HeaderSegments,
}

impl Header {
    pub fn h_read<C, R>(
        h: &mut BufReader<R>,
        st: &ReadState<C>,
    ) -> MultiResult<Self, ImpureError<HeaderError>>
    where
        C: AsRef<HeaderConfigInner>,
        R: Read,
    {
        h_read_required_header(h, st).and_then(|(version, text, data, analysis)| {
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
                h_read_other_segments(h, *earliest_begin, st)
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
                hdr.segments
                    .validate()
                    .mult_map_errors(Box::new)
                    .mult_map_errors(HeaderError::Validation)
                    .mult_map_errors(ImpureError::Pure)?;
                Ok(hdr)
            })
        })
    }
}

fn h_read_required_header<C, R>(
    h: &mut BufReader<R>,
    st: &ReadState<C>,
) -> MultiResult<
    (
        Version,
        PrimaryTextSegment,
        HeaderDataSegment,
        HeaderAnalysisSegment,
    ),
    ImpureError<HeaderError>,
>
where
    R: Read,
    C: AsRef<HeaderConfigInner>,
{
    let conf = &st.conf.as_ref();
    let vers_res = Version::h_read(h)
        .map_err(NonEmpty::new)
        .mult_map_errors(|e| e.map_inner(HeaderError::Version));
    let space_res = h_read_spaces(h).map_err(NonEmpty::new);
    let text_res = h_read_primary_segment(h, false, conf.text_correction, st);
    let data_res = h_read_primary_segment(h, true, conf.data_correction, st);
    let anal_res = h_read_primary_segment(h, true, conf.analysis_correction, st);
    let offset_res = text_res
        .mult_zip3(data_res, anal_res)
        .mult_map_errors(|e| e.map_inner(HeaderError::Segment));
    vers_res
        .mult_zip3(space_res, offset_res)
        .map(|(version, (), (text, data, analysis))| (version, text, data, analysis))
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

fn h_read_primary_segment<C, R, I>(
    h: &mut BufReader<R>,
    allow_blank: bool,
    corr: HeaderCorrection<I>,
    st: &ReadState<C>,
) -> MultiResult<HeaderSegment<I>, ImpureError<HeaderSegmentError>>
where
    R: Read,
    C: AsRef<HeaderConfigInner>,
    I: HasRegion + Copy,
{
    let conf = st.conf.as_ref();
    let seg_conf = NewSegmentConfig {
        corr,
        file_len: st.file_len.try_into().ok(),
        truncate_offsets: conf.truncate_offsets,
    };
    HeaderSegment::<I>::h_read_offsets(
        h,
        allow_blank,
        conf.allow_negative,
        conf.squish_offsets,
        &seg_conf,
    )
}

fn h_read_other_segments<C, R>(
    h: &mut BufReader<R>,
    text_begin: UintSpacePad8,
    st: &ReadState<C>,
) -> MultiResult<Vec<OtherSegment>, ImpureError<HeaderError>>
where
    R: Read,
    C: AsRef<HeaderConfigInner>,
{
    // ASSUME this won't fail because we checked that each offset is greater
    // than this
    let conf = st.conf.as_ref();
    let n = u64::from(text_begin) - u64::from(HEADER_LEN);
    let w = u8::from(conf.other_width);
    let mut buf0 = vec![];
    let mut buf1 = vec![];
    let n_segs = (n / (u64::from(w) * 2)) as usize;

    conf.other_corrections
        .iter()
        .copied()
        .chain(repeat(OffsetCorrection::default()))
        .take(conf.max_other.map(|x| x.min(n_segs)).unwrap_or(n_segs))
        .map(|corr| {
            buf0.clear();
            buf1.clear();
            h.take(u64::from(w)).read_to_end(&mut buf0).into_mult()?;
            h.take(u64::from(w)).read_to_end(&mut buf1).into_mult()?;
            let seg_conf = NewSegmentConfig {
                corr,
                file_len: Some(st.file_len.into()),
                truncate_offsets: conf.truncate_offsets,
            };
            // If any regions are entirely blank, just ignore them
            if buf0.iter().chain(buf1.iter()).all(|x| *x == 32) {
                Ok(None)
            } else {
                OtherSegment::parse(&buf0, &buf1, conf.allow_negative, &seg_conf)
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
            Err(ImpureError::Pure(VersionError(buf.to_vec())))
        }
    }

    pub fn short(&self) -> &'static str {
        match self {
            Self::FCS2_0 => "2.0",
            Self::FCS3_0 => "3.0",
            Self::FCS3_1 => "3.1",
            Self::FCS3_2 => "3.2",
        }
    }

    pub fn short_underscore(&self) -> &'static str {
        match self {
            Self::FCS2_0 => "2_0",
            Self::FCS3_0 => "3_0",
            Self::FCS3_1 => "3_1",
            Self::FCS3_2 => "3_2",
        }
    }

    pub fn from_short(s: &str) -> Option<Self> {
        match s {
            "2.0" => Some(Self::FCS2_0),
            "3.0" => Some(Self::FCS3_0),
            "3.1" => Some(Self::FCS3_1),
            "3.2" => Some(Self::FCS3_2),
            _ => None,
        }
    }

    pub fn from_short_underscore(s: &str) -> Option<Self> {
        match s {
            "2_0" => Some(Self::FCS2_0),
            "3_0" => Some(Self::FCS3_0),
            "3_1" => Some(Self::FCS3_1),
            "3_2" => Some(Self::FCS3_2),
            _ => None,
        }
    }
}

impl str::FromStr for Version {
    type Err = VersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "FCS2.0" => Ok(Self::FCS2_0),
            "FCS3.0" => Ok(Self::FCS3_0),
            "FCS3.1" => Ok(Self::FCS3_1),
            "FCS3.2" => Ok(Self::FCS3_2),
            _ => Err(VersionError(s.as_bytes().to_vec())),
        }
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            Version::FCS2_0 => "FCS2.0",
            Version::FCS3_0 => "FCS3.0",
            Version::FCS3_1 => "FCS3.1",
            Version::FCS3_2 => "FCS3.2",
        };
        f.write_str(s)
    }
}

pub enum HeaderError {
    Segment(HeaderSegmentError),
    Space,
    Version(VersionError),
    Validation(Box<HeaderValidationError>),
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

#[derive(From, Display)]
pub enum HeaderValidationError {
    Overlap(SegmentOverlapError),
    InHeader(InHeaderError),
}

pub struct InHeaderError(GenericSegment);

impl fmt::Display for InHeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{} is within HEADER region", self.0)
    }
}

#[derive(Debug)]
pub struct VersionError(Vec<u8>);

impl fmt::Display for VersionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if let Ok(s) = str::from_utf8(&self.0) {
            write!(f, "'{s}' is not a valid or supported FCS version")
        } else {
            write!(
                f,
                "could not read FCS version, got bytes [{}]",
                self.0.iter().join(",")
            )
        }
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
    has_nextdata: bool,
) -> Result<HeaderKeywordsToWrite, Uint8DigitOverflow> {
    let other_header_len = other_header_len(&other_lens[..]);

    let text_begin: UintSpacePad8 = (u64::from(HEADER_LEN) + other_header_len).try_into()?;
    // +1 at end accounts for first delimiter
    let text_len =
        raw_keywords_length(&req[..]) + raw_keywords_length(&opt[..]) + nextdata_len() + 1;
    let text_seg = PrimaryTextSegment::try_new_with_len(text_begin, text_len)?;

    let other_begin = text_seg
        .inner
        .try_next_byte()
        .map_or(Ok(text_begin), |x| u64::from(x).try_into())?;
    let (other_segs, data_begin) = other_segments(other_begin.into(), &other_lens[..]);

    let data_seg = HeaderDataSegment::try_new_with_len(data_begin.try_into()?, data_len)?;

    let analysis_begin = data_seg
        .inner
        .try_next_byte()
        .map_or(Ok(text_begin), |x| u64::from(x).try_into())?;
    let analysis_seg = HeaderAnalysisSegment::try_new_with_len(analysis_begin, analysis_len)?;

    let nextdata = Nextdata(if !has_nextdata {
        UintZeroPad20(0)
    } else {
        UintZeroPad20(
            analysis_seg
                .inner
                .try_next_byte()
                .map(u64::from)
                .unwrap_or(u64::from(analysis_begin)),
        )
    });

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
    has_nextdata: bool,
) -> Result<HeaderKeywordsToWrite, Uint8DigitOverflow> {
    let other_header_len = other_header_len(&other_lens[..]);
    let prim_text_begin: UintSpacePad8 = (u64::from(HEADER_LEN) + other_header_len).try_into()?;

    let nooffset_req_text_len = raw_keywords_length(&req[..]);
    let opt_text_len = raw_keywords_length(&opt[..]);
    // +1 accounts for first delimiter
    let nosupp_text_len = offsets_len() + nooffset_req_text_len + 1;
    let supp_text_len = opt_text_len + 1;
    let all_text_len = opt_text_len + nosupp_text_len;

    let make_text_seg = |len| {
        PrimaryTextSegment::try_new_with_len(prim_text_begin, len).map(|seg| {
            let other_begin = seg
                .inner
                .try_next_byte()
                .map(u64::from)
                .unwrap_or(u64::from(prim_text_begin));
            (seg, other_begin)
        })
    };

    // include STEXT only if the optional keywords don't fit within the first
    // 99,999,999 bytes
    let prim_text_res = make_text_seg(all_text_len);
    let (prim_text_seg, other_segs, supp_text_seg, data_begin) = match prim_text_res {
        Ok((prim_text_seg, other_begin)) => {
            let (other_segs, next_begin) = other_segments(other_begin, &other_lens[..]);
            (
                prim_text_seg,
                other_segs,
                SupplementalTextSegment::default(),
                next_begin,
            )
        }
        Err(_) => {
            let (prim_text_seg, other_begin) = make_text_seg(nosupp_text_len)?;
            let (other_segs, supp_text_begin) = other_segments(other_begin, &other_lens[..]);
            let supp_text_seg =
                SupplementalTextSegment::new_with_len(supp_text_begin.into(), supp_text_len);
            let data_begin = supp_text_seg
                .inner
                .try_next_byte()
                .map(u64::from)
                .unwrap_or(supp_text_begin);
            (prim_text_seg, other_segs, supp_text_seg, data_begin)
        }
    };

    let data_seg = TEXTDataSegment::new_with_len(data_begin.into(), data_len);

    let analysis_begin = data_seg
        .inner
        .try_next_byte()
        .map(u64::from)
        .unwrap_or(data_begin);
    let analysis_seg = TEXTAnalysisSegment::new_with_len(analysis_begin.into(), analysis_len);

    let h_analysis_seg = analysis_seg.as_header();
    let h_data_seg = data_seg.as_header();

    let nextdata = Nextdata(if !has_nextdata {
        UintZeroPad20(0)
    } else {
        UintZeroPad20(
            analysis_seg
                .inner
                .try_next_byte()
                .map(u64::from)
                .unwrap_or(analysis_begin),
        )
    });

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

fn other_header_len(other_lens: &[u64]) -> u64 {
    // each segment offset pair has two digits that are 20 bytes long
    (other_lens.len() as u64) * 20 * 2
}

fn other_segments(begin: u64, other_lens: &[u64]) -> (Vec<OtherSegment>, u64) {
    let ret: Vec<_> = other_lens
        .iter()
        .scan(begin, |b, &length| {
            let ret = OtherSegment::new_with_len(begin.into(), length);
            *b += length;
            Some(ret)
        })
        .collect();
    let next = ret
        .iter()
        .flat_map(|x| x.inner.try_next_byte())
        .last()
        .map(|x| x.into())
        .unwrap_or(begin);
    (ret, next)
}

/// Length of $(BEGIN/END)(STEXT/ANALYSIS/DATA) and $NEXTDATA offset length.
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
    Nextdata::len() + OFFSET_VAL_LEN + 2
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

#[cfg(feature = "python")]
mod python {
    use super::{Version, VersionError};
    use crate::python::macros::{impl_from_py_via_fromstr, impl_to_py_via_display, impl_value_err};

    impl_to_py_via_display!(Version);
    impl_from_py_via_fromstr!(Version);
    impl_value_err!(VersionError);
}
