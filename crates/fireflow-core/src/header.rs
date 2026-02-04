//! Reading and writing the HEADER segment

use crate::config::{
    AppendableFlag, ConfigFlag as _, DatasetOffset, OverlapCorrectionLimit, ReadHeaderInnerConfig,
    ReadOffsetConfig, ReadState, SelectVersionStrategy, VersionOverride,
};
use crate::core::Other;
use crate::logging::{
    ErrorsResult, IOAnonErrorGroup, IOErrorGroup, IOGroupResult, LogResult, ResultExt as _,
    WarningsAndIOGroupResult, io_to_log, split_io,
};
use crate::segment::{
    GenericSegment, GuessOtherWidthError, HasRegion, HasSource, HeaderAnalysisSegment,
    HeaderDataSegment, HeaderSegment, HeaderSegmentError, IsDataOrAnalysis, OtherSegment,
    OtherSegment20, PrimaryTextSegment, Segment, SegmentOverlapError, SupplementalTextSegment,
    TEXTAnalysisSegment, TEXTDataSegment, TEXTSegment, UncorrectedSegment,
};
use crate::text::keywords::{
    Beginanalysis, Begindata, Beginstext, Endanalysis, Enddata, Endstext, KeywordOptimizer,
    KeywordVersionScore, Nextdata, Par,
};
use crate::text::lookup::ReqMetarootKey as _;
use crate::validated::ascii_range::OtherWidth;
use crate::validated::ascii_uint::{
    HeaderString, Uint8DigitOverflowError, UintSpacePad20, UintZeroPad20,
};
use crate::validated::keys::{Key as _, StdKeywords};
use crate::validated::textdelim::TEXTDelim;

use type_families::{impl_functor_once, impl_kind1};

use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use num_traits::identities::Zero;
use thiserror::Error;

use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::iter::once;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromPyString, IntoPyString},
};

/// The length of the HEADER.
///
/// This should always be the same. This also assumes that there are no OTHER
/// segments (which for now are not supported).
pub const HEADER_LEN: u8 = 58;

/// All FCS versions this library supports.
///
/// This appears as the first 6 bytes of any valid FCS file.
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Debug, Display, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyString, FromPyString))]
pub enum Version {
    #[display("FCS2.0")]
    FCS2_0,
    #[display("FCS3.0")]
    FCS3_0,
    #[display("FCS3.1")]
    FCS3_1,
    #[display("FCS3.2")]
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

/// The segments from the HEADER
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct HeaderSegments<O> {
    pub text: PrimaryTextSegment,
    pub data: HeaderDataSegment,
    pub analysis: HeaderAnalysisSegment,
    pub other: O,
}

impl_kind1!(pub HeaderSegmentsFamily, HeaderSegments);

impl_functor_once!(
    HeaderSegments,
    self,
    mut f,
    HeaderSegments::new(self.text, self.data, self.analysis, f(self.other))
);

pub type ParsedHeaderSegments = HeaderSegments<ParsedOtherSegments>;
pub type WriteHeaderSegments<T> = HeaderSegments<WriteOtherSegments<T>>;

pub type ParsedOtherSegments = Option<(NonEmpty<OtherSegment<UintSpacePad20>>, OtherWidth)>;
pub type WriteOtherSegments<T> = Vec<OtherSegment<T>>;

/// The uncorrected segments from the HEADER
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct UncorrectedHeaderSegments {
    pub text: UncorrectedSegment,
    pub data: UncorrectedSegment,
    pub analysis: UncorrectedSegment,
    pub other: Vec<UncorrectedSegment>,
}

/// Keyword scores for all versions generated when guessing version
///
/// Each score should sum to the same number.
pub type KeywordVersionScores = (
    KeywordVersionScore,
    KeywordVersionScore,
    KeywordVersionScore,
    KeywordVersionScore,
);

/// Any mutable reference to segment from HEADER.
pub(crate) enum AnyHeaderSegmentMut<'a> {
    Text(&'a mut PrimaryTextSegment),
    Data(&'a mut HeaderDataSegment),
    Analysis(&'a mut HeaderAnalysisSegment),
    Other(&'a mut OtherSegment20),
}

impl<T> WriteHeaderSegments<T> {
    pub(crate) fn h_write<W: Write>(&self, h: &mut BufWriter<W>, version: Version) -> io::Result<()>
    where
        T: HeaderString + Zero,
    {
        let towrite = [
            version.to_string(),           // 6 bytes
            "    ".into(),                 // 4 bytes
            self.text.header_string(),     // 16 bytes
            self.data.header_string(),     // 16 bytes
            self.analysis.header_string(), // 16 bytes
        ];
        debug_assert!(
            towrite.iter().join("").len() == 58,
            "HEADER (without OTHER) should be 58 bytes"
        );
        for s in towrite
            .into_iter()
            .chain(self.other.iter().map(Segment::header_string))
        {
            h.write_all(s.as_bytes())?;
        }
        Ok(())
    }
}

impl ParsedHeaderSegments {
    /// Ensure that TEXT segment does not start in HEADER and does not overlap.
    pub(crate) fn validate_supp_text<I>(
        &mut self,
        s: &mut TEXTSegment<I>,
        limit: OverlapCorrectionLimit,
    ) -> impl Iterator<Item = SegmentValidationError>
    where
        I: HasRegion,
    {
        let contains = self.contains_segment(s).map(SegmentValidationError::from);
        let hs = self.as_mut_nonempty_segments();
        let overlaps = Self::fix_text_overlap(hs, s, limit)
            .into_iter()
            .map(SegmentValidationError::from);
        contains.into_iter().chain(overlaps)
    }

    pub(crate) fn validate_text_data_or_analysis<I>(
        &mut self,
        s: &mut TEXTSegment<I>,
        limit: OverlapCorrectionLimit,
    ) -> impl Iterator<Item = SegmentValidationError>
    where
        I: HasRegion + IsDataOrAnalysis,
    {
        let contains = self.contains_segment(s).map(SegmentValidationError::from);
        let hs = self.as_mut_nonempty_segments_filtered::<I>();
        let overlaps = Self::fix_text_overlap(hs, s, limit)
            .into_iter()
            .map(SegmentValidationError::from);
        contains.into_iter().chain(overlaps)
    }

    pub(crate) fn fix_text_overlap<'a, I>(
        xs: impl IntoIterator<Item = (AnyHeaderSegmentMut<'a>, GenericSegment)>,
        s: &mut TEXTSegment<I>,
        limit: OverlapCorrectionLimit,
    ) -> Vec<SegmentOverlapError>
    where
        I: HasRegion,
    {
        if let Some(txt_seg) = s.try_as_generic() {
            let mut errors = vec![];
            let mut it = xs.into_iter();
            debug_assert!(
                it.by_ref().is_sorted_by_key(|x| x.1.as_pair()),
                "not sorted"
            );
            let mut hdr_pair = None;
            // Skip all HEADER segments that come before TEXT seg
            while let p @ Some((_, hdr_seg)) = it.next() {
                hdr_pair = p;
                if txt_seg.begin <= hdr_seg.end {
                    break;
                }
            }
            // The next HEADER segment has an end offset that starts at or after
            // the TEXT begin offset, and thus may overlap the beginning of
            // TEXT, be totally within TEXT, or overlap the ending of TEXT.
            if let Some((mut hdr_ref, hdr_seg)) = hdr_pair {
                if hdr_seg.begin < txt_seg.begin {
                    // HEADER starts before TEXT. Check if the HEADER segment is
                    // the TEXT segment itself. If so, throw error regardless
                    // since we already read it at this point and thus should
                    // not alter it. If not, truncate if within the limit.
                    let overlap = hdr_seg.get_tail_overlap(&txt_seg);
                    if overlap <= limit.0 && !matches!(hdr_ref, AnyHeaderSegmentMut::Text(_)) {
                        hdr_ref.truncate(overlap);
                    } else if overlap > 0 {
                        errors.push(SegmentOverlapError::new(hdr_seg, txt_seg));
                    }
                } else {
                    // HEADER begins within TEXT or after. Truncate TEXT if
                    // within limit or throw error. In former case, return early
                    // since we know that no more HEADER segments can overlap.
                    let overlap = txt_seg.get_tail_overlap(&hdr_seg);
                    if overlap <= limit.0 {
                        s.truncate(overlap);
                        return vec![];
                    }
                    errors.push(SegmentOverlapError::new(hdr_seg, txt_seg));
                }
            }
            // All the remaining HEADER segments should now begin within TEXT or
            // after.
            for (_, hdr_seg) in it {
                let overlap = txt_seg.get_tail_overlap(&hdr_seg);
                // If no overlaps, we can assume there are no more overlaps
                // since the HEADER offsets are sorted. Break early to save
                // time.
                if overlap == 0 {
                    break;
                }
                // If overlap within limit and we have not encountered an error
                // yet, truncate TEXT and return early without error. Otherwise
                // push error.
                if overlap <= limit.0 && errors.is_empty() {
                    s.truncate(overlap);
                    return vec![];
                }
                errors.push(SegmentOverlapError::new(hdr_seg, txt_seg));
            }
            errors
        } else {
            vec![]
        }
    }

    // TODO if we don't have TEXT, we can have ANALYSIS but not DATA
    /// Ensure HEADER segments don't overlap and start after HEADER itself
    fn validate(
        &mut self,
        limit: OverlapCorrectionLimit,
    ) -> impl Iterator<Item = SegmentValidationError> {
        let overlap_errors = self.find_or_fix_header_overlaps(limit);
        self.contains_header_segments()
            .map(SegmentValidationError::from)
            .chain(overlap_errors.into_iter().map(SegmentValidationError::from))
    }

    fn contains_header_segments(&self) -> impl Iterator<Item = InHeaderError> {
        let t = self.contains_segment(&self.text);
        let d = self.contains_segment(&self.data);
        let a = self.contains_segment(&self.analysis);
        let os = self.as_others().map(|o| self.contains_segment(o));
        [t, d, a].into_iter().chain(os).flatten()
    }

    fn contains_segment<I, S, T0>(&self, s: &Segment<I, S, T0>) -> Option<InHeaderError>
    where
        I: HasRegion,
        S: HasSource,
        T0: Into<u64> + Copy,
    {
        let q = s.try_as_generic()?;
        (q.begin < self.nbytes()).then_some(InHeaderError(q))
    }

    /// Return number of bytes required to encode HEADER (including OTHER)
    pub(crate) fn nbytes(&self) -> u64 {
        u64::from(HEADER_LEN) + self.other_offset_nbytes()
    }

    fn as_mut_segments(&mut self) -> impl Iterator<Item = AnyHeaderSegmentMut<'_>> {
        self.other
            .iter_mut()
            .flat_map(|(os, _)| os.iter_mut())
            .map(AnyHeaderSegmentMut::Other)
            .chain([
                AnyHeaderSegmentMut::Text(&mut self.text),
                AnyHeaderSegmentMut::Data(&mut self.data),
                AnyHeaderSegmentMut::Analysis(&mut self.analysis),
            ])
    }

    fn as_mut_nonempty_segments(
        &mut self,
    ) -> impl Iterator<Item = (AnyHeaderSegmentMut<'_>, GenericSegment)> {
        self.as_mut_segments()
            .filter_map(|x| x.try_as_generic().map(|y| (x, y)))
            .sorted_by_key(|x| x.1.as_pair())
    }

    fn as_mut_nonempty_segments_filtered<I>(
        &mut self,
    ) -> impl Iterator<Item = (AnyHeaderSegmentMut<'_>, GenericSegment)>
    where
        I: IsDataOrAnalysis,
    {
        self.as_mut_nonempty_segments().filter(|(k, _)| {
            !matches!(
                (k, I::IS_DATA),
                (AnyHeaderSegmentMut::Data(_), true) | (AnyHeaderSegmentMut::Analysis(_), false)
            )
        })
    }

    fn find_or_fix_header_overlaps(
        &mut self,
        limit: OverlapCorrectionLimit,
    ) -> Vec<SegmentOverlapError> {
        let mut pairs: Vec<_> = self.as_mut_nonempty_segments().collect();
        debug_assert!(pairs.is_sorted_by_key(|x| x.1.as_pair()), "not sorted");
        let mut errors = vec![];
        let mut remainder = &mut pairs[..];
        while let Some(((ref0, seg0), rest)) = remainder.split_first_mut() {
            for (_, seg1) in rest {
                let overlap = seg0.get_tail_overlap(seg1);
                if overlap <= limit.0 {
                    // TODO throw warning here if we want
                    ref0.truncate(overlap);
                    // break early because any offset after this one is
                    // guaranteed to be after the new truncated ending due
                    // to sorting
                    break;
                }
                errors.push(SegmentOverlapError::new(*seg0, *seg1));
            }
            if !remainder.is_empty() {
                remainder = &mut remainder[1..];
            }
        }
        errors
    }

    pub(crate) fn as_others(&self) -> impl Iterator<Item = &OtherSegment<UintSpacePad20>> {
        self.other.iter().flat_map(|(os, _)| os.iter())
    }

    fn other_offset_nbytes(&self) -> u64 {
        self.other.as_ref().map_or(0, |(os, width)| {
            let n = u64::try_from(os.len()).expect("usize overflow");
            n * u64::from(u8::from(*width))
        })
    }
}

impl AnyHeaderSegmentMut<'_> {
    fn try_as_generic(&self) -> Option<GenericSegment> {
        match self {
            Self::Analysis(x) => x.try_as_generic(),
            Self::Data(x) => x.try_as_generic(),
            Self::Text(x) => x.try_as_generic(),
            Self::Other(x) => x.try_as_generic(),
        }
    }

    fn truncate(&mut self, n: u64) {
        match self {
            Self::Analysis(x) => x.truncate(n),
            Self::Data(x) => x.truncate(n),
            Self::Text(x) => x.truncate(n),
            Self::Other(x) => x.truncate(n),
        }
    }
}

/// Output from parsing the FCS header.
///
/// Includes version and the three main segments (TEXT, DATA, ANALYSIS) plus
/// any OTHER segments after the first 58 bytes.
///
/// Only valid segments are to be put in this struct (ie begin <= end).
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Header {
    pub version: Version,
    pub segments: ParsedHeaderSegments,
    pub uncorrected_segments: UncorrectedHeaderSegments,
}

impl Header {
    pub fn h_read<C, R>(
        h: &mut BufReader<R>,
        st: &ReadState<C>,
    ) -> WarningsAndIOGroupResult<Self, GuessOtherWidthError, HeaderError, ()>
    where
        C: AsRef<ReadHeaderInnerConfig> + AsRef<ReadOffsetConfig>,
        R: Read + Seek,
    {
        let oconf: &ReadOffsetConfig = st.conf.as_ref();
        io_to_log!(h.seek(SeekFrom::Start(st.dataset_offset.0)));
        let req = io_to_log!(h_read_required_header(h, st));
        let (text, text_raw) = req.text;
        let (data, data_raw) = req.data;
        let (analysis, analysis_raw) = req.analysis;
        let coords = [text.try_coords(), data.try_coords(), analysis.try_coords()];
        let min_coord = coords.iter().flatten().map(|x| x.0).min();
        let other_res = if let Some(m) = min_coord {
            OtherSegment20::h_read_others(h, m, st)
        } else {
            LogResult::new_ok(None)
        };
        other_res
            .map_ok_value(|other| {
                let (os, os_raw) = if let Some((os, w)) = other {
                    let (parsed, raw) = os.into_iter().unzip();
                    (Some((NonEmpty::from_vec(parsed).unwrap(), w)), raw)
                } else {
                    (None, vec![])
                };
                let ss = ParsedHeaderSegments::new(text, data, analysis, os);
                let us = UncorrectedHeaderSegments::new(text_raw, data_raw, analysis_raw, os_raw);
                Self::new(req.version, ss, us)
            })
            .map_pure_errors(HeaderError::from)
            .and_then_commutative(|mut hdr| {
                let es = hdr
                    .segments
                    .validate(oconf.overlap_correction_limit)
                    .map(HeaderError::from);
                ErrorsResult::new_err_from_iter(es, ())
                    .set_ok_value(hdr)
                    .nowarn_into_warn()
                    .group()
                    .map_error(IOErrorGroup::Pure)
            })
    }
}

#[derive(new)]
struct ReqHeader {
    version: Version,
    text: (PrimaryTextSegment, UncorrectedSegment),
    data: (HeaderDataSegment, UncorrectedSegment),
    analysis: (HeaderAnalysisSegment, UncorrectedSegment),
}

fn h_read_required_header<C, R>(
    h: &mut BufReader<R>,
    st: &ReadState<C>,
) -> IOGroupResult<ReqHeader, HeaderError, ()>
where
    R: Read + Seek,
    C: AsRef<ReadHeaderInnerConfig> + AsRef<ReadOffsetConfig>,
{
    let conf: &ReadHeaderInnerConfig = st.conf.as_ref();
    let text_cor = conf.text_correction;
    let data_cor = conf.data_correction;
    let anal_cor = conf.analysis_correction;

    let vers_res = split_io!(Version::h_read(h, st))
        .ungroup()
        .map_errors(HeaderError::from);
    let space_res = split_io!(h_read_spaces(h, st))
        .ungroup()
        .map_errors(HeaderError::from);

    let (version, ()) = vers_res
        .zip_commutative(space_res)
        .group()
        .resolve_nowarn()
        .map_err(IOErrorGroup::Pure)?;

    let text_res = HeaderSegment::h_read_primary(h, true, text_cor, version, st);
    let data_res = HeaderSegment::h_read_primary(h, false, data_cor, version, st);
    let anal_res = HeaderSegment::h_read_primary(h, false, anal_cor, version, st);

    let pure_text_res = split_io!(text_res).ungroup();
    let pure_data_res = split_io!(data_res).ungroup();
    let pure_anal_res = split_io!(anal_res).ungroup();

    pure_text_res
        .zip3_commutative(pure_data_res, pure_anal_res)
        .map_errors(HeaderError::from)
        .group()
        .resolve_nowarn()
        .map_err(IOErrorGroup::Pure)
        .map(|(t, d, a)| ReqHeader::new(version, t, d, a))
}

fn h_read_spaces<R, C>(
    h: &mut BufReader<R>,
    st: &ReadState<C>,
) -> Result<(), IOAnonErrorGroup<HeaderSpacesError>>
where
    R: Read + Seek,
{
    let remaining = st.remaining_bytes(h)?;
    if remaining < 4 {
        let e = HeaderSpacesNoBytesError(remaining).into();
        return Err(IOAnonErrorGroup::new_pure_one(e));
    }
    let mut buf = [0_u8; 4];
    h.read_exact(&mut buf)?;
    if buf.iter().all(|x| *x == 32) {
        Ok(())
    } else {
        Err(IOAnonErrorGroup::new_pure_one(
            HeaderSpacesFormatError.into(),
        ))
    }
}

impl Version {
    fn h_read<R, C>(
        h: &mut BufReader<R>,
        st: &ReadState<C>,
    ) -> IOGroupResult<Self, VersionError, ()>
    where
        R: Read + Seek,
    {
        let remaining = st.remaining_bytes(h)?;
        if remaining < 6 {
            let e = VersionNoBytesError(remaining).into();
            return Err(IOAnonErrorGroup::new_pure_one(e));
        }
        let mut buf = [0; 6];
        h.read_exact(&mut buf)?;
        if buf.is_ascii() {
            // SAFETY: we just checked that all bytes are ASCII
            let s = unsafe { str::from_utf8_unchecked(&buf) };
            s.parse()
                .map_err(VersionError::from)
                .map_err(IOErrorGroup::new_pure_one)
        } else {
            let e = VersionNonUtf8Error(buf.to_vec());
            Err(IOErrorGroup::new_pure_one(e.into()))
        }
    }

    pub(crate) fn autodetect(
        self,
        kws: &StdKeywords,
        ver_override: Option<&VersionOverride>,
    ) -> Result<(Self, Option<KeywordVersionScores>), GuessVersionError> {
        let vs = [Self::FCS2_0, Self::FCS3_0, Self::FCS3_1, Self::FCS3_2];
        match ver_override {
            None => Ok((self, None)),
            Some(VersionOverride::Force(v)) => Ok((*v, None)),
            Some(VersionOverride::AutoDetect(strat)) => {
                let rank =
                    |(v0, s0): &(Self, KeywordVersionScore),
                     (v1, s1): &(Self, KeywordVersionScore)| match strat {
                        SelectVersionStrategy::Earliest => v1.cmp(v0),
                        SelectVersionStrategy::Latest => v0.cmp(v1),
                        SelectVersionStrategy::Loose => s1.good_opt.cmp(&s0.good_opt),
                        SelectVersionStrategy::Strict => s0.good_opt.cmp(&s1.good_opt),
                    };
                if let Ok(par) = Par::get_metaroot_req(kws) {
                    let mut opt = KeywordOptimizer::default();
                    for (k, v) in kws {
                        opt.classify_keyword(k, v);
                    }
                    let scores = vs.map(|v| (v, opt.get_score(v, par)));
                    let ret_scores = || Some(scores.clone().map(|(_, s)| s).into());
                    if let Some(xs) =
                        NonEmpty::collect(scores.iter().filter(|(_, s)| s.is_passing(false)))
                    {
                        // Found at least one version that doesn't require dropping,
                        // rank by strategy to select
                        Ok((xs.maximum_by(|&x, &y| rank(x, y)).0, ret_scores()))
                    } else if let Some(xs) =
                        NonEmpty::collect(scores.iter().filter(|(_, s)| s.is_passing(true)))
                    {
                        // No versions found that can be satisfied without dropping
                        // keywords, find versions with dropping and rank using
                        // strategy.
                        let ret = xs.maximum_by(|&x, &y| {
                            if x.1.drop == y.1.drop {
                                rank(x, y)
                            } else {
                                y.1.drop.cmp(&x.1.drop)
                            }
                        });
                        Ok((ret.0, ret_scores()))
                    } else {
                        // No versions found that have valid keywords available,
                        // return error
                        Err(GuessVersionError::AllInvalid)
                    }
                } else {
                    Err(GuessVersionError::NoPar)
                }
            }
        }
    }
}

impl FromStr for Version {
    type Err = VersionFormatError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "FCS2.0" => Ok(Self::FCS2_0),
            "FCS3.0" => Ok(Self::FCS3_0),
            "FCS3.1" => Ok(Self::FCS3_1),
            "FCS3.2" => Ok(Self::FCS3_2),
            _ => Err(VersionFormatError(s.to_owned())),
        }
    }
}

/// Error when parsing HEADER segment
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum HeaderError {
    Segment(HeaderSegmentError),
    Version(VersionError),
    Validation(SegmentValidationError),
    Space(HeaderSpacesError),
}

/// Error when parsing spaces after FCS version
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum HeaderSpacesError {
    Format(HeaderSpacesFormatError),
    Bytes(HeaderSpacesNoBytesError),
}

/// Error when version is not follow by proper number of spaces in HEADER
#[derive(Debug, Error)]
#[error("version must be followed by 4 spaces")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
pub struct HeaderSpacesFormatError;

/// Error when spaces could not be read because not enough bytes were present
#[derive(Debug, Error)]
#[error("needed 4 bytes to read spaces after FCS version, got {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
pub struct HeaderSpacesNoBytesError(u64);

/// Error when validating segments in HEADER
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SegmentValidationError {
    Overlap(SegmentOverlapError),
    InHeader(InHeaderError),
}

/// Error when a non-empty segment occurs within the first 58 bytes of the file.
#[derive(Debug, Error)]
#[error("{0} is within HEADER region")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
pub struct InHeaderError(GenericSegment);

/// Error when validating segments in HEADER
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum VersionError {
    Format(VersionFormatError),
    NonUtf8(VersionNonUtf8Error),
    Bytes(VersionNoBytesError),
}

/// Error when parsing FCS version
#[derive(Debug, Error)]
#[error("'{0}' is not a valid or supported FCS version")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
pub struct VersionFormatError(String);

/// Error when parsing FCS version
#[derive(Debug, Error)]
#[error("invalid bytes found when parsing version: {}", self.0.iter().join(","))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
pub struct VersionNonUtf8Error(Vec<u8>);

/// Error when not enough bytes to parse version
#[derive(Debug, Error)]
#[error("needed 6 bytes to parse FCS version, got {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
pub struct VersionNoBytesError(u64);

/// Error when trying to guess FCS version from keywords
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub enum GuessVersionError {
    // TODO should also say a bit more on why this is the case
    #[error("no FCS versions could be guessed from keywords")]
    AllInvalid,
    #[error("$PAR could not be found and thus FCS version could not be detected")]
    NoPar,
}

#[derive(new)]
pub(crate) struct HeaderKeywordsToWrite<T> {
    pub(crate) header: WriteHeaderSegments<T>,
    pub(crate) primary: KeywordsWriter,
    pub(crate) supplemental: KeywordsWriter,
    pub(crate) nextdata: Nextdata,
}

impl<T> HeaderKeywordsToWrite<T> {
    /// Create HEADER+TEXT+OTHER offsets for FCS 2.0
    pub(crate) fn new_2_0(
        req: Vec<(String, String)>,
        opt: Vec<(String, String)>,
        data_len: u64,
        analysis_len: u64,
        other_lens: &[u64],
        has_nextdata: AppendableFlag,
    ) -> Result<Self, Uint8DigitOverflowError>
    where
        T: TryFrom<u64, Error = Uint8DigitOverflowError> + HeaderString,
    {
        let text_begin = Self::header_len(other_lens.len(), T::WIDTH);
        let dso = DatasetOffset(0);

        // +1 at end accounts for first delimiter
        let text_len: u64 =
            flat_keywords_length(&req[..]) + flat_keywords_length(&opt[..]) + nextdata_len() + 1;
        let text_seg = PrimaryTextSegment::try_new_with_len(text_begin, text_len, dso)?;

        let other_begin = text_seg.try_next_byte().map_or(text_begin, u64::from);
        let (other_segs, data_begin) = Self::other_segments(other_begin, other_lens, dso)?;

        let data_seg = HeaderDataSegment::try_new_with_len(data_begin, data_len, dso)?;

        let analysis_begin = data_seg.try_next_byte().map_or(data_begin, u64::from);
        let analysis_seg =
            HeaderAnalysisSegment::try_new_with_len(analysis_begin, analysis_len, dso)?;

        let nextdata = Nextdata(if has_nextdata.is_set() {
            let n = analysis_seg
                .try_next_byte()
                .map_or(analysis_begin, u64::from);
            UintZeroPad20(n)
        } else {
            UintZeroPad20(0)
        });

        let header = HeaderSegments {
            text: text_seg,
            data: data_seg,
            analysis: analysis_seg,
            other: other_segs,
        };

        let primary = KeywordsWriter(once(nextdata.pair()).chain(req).chain(opt).collect());

        Ok(Self::new(
            header,
            primary,
            KeywordsWriter::default(),
            nextdata,
        ))
    }

    /// Create HEADER+TEXT+OTHER offsets for FCS 3.0
    ///
    /// Order in which this is expected to be written is HEADER, OTHER(s), TEXT,
    /// STEXT, DATA, ANALYSIS.
    pub(crate) fn new_3_0(
        req: Vec<(String, String)>,
        opt: Vec<(String, String)>,
        data_len: u64,
        analysis_len: u64,
        other_lens: &[u64],
        has_nextdata: AppendableFlag,
    ) -> Result<Self, Uint8DigitOverflowError>
    where
        T: TryFrom<u64, Error = Uint8DigitOverflowError> + HeaderString,
    {
        let dso = DatasetOffset(0);
        let prim_text_begin = Self::header_len(other_lens.len(), T::WIDTH);

        let nooffset_req_text_len = flat_keywords_length(&req[..]);
        let opt_text_len = flat_keywords_length(&opt[..]);
        // +1 accounts for first delimiter
        let nosupp_text_len = offsets_len() + nooffset_req_text_len + 1;
        let supp_text_len = opt_text_len + 1;
        let all_text_len = opt_text_len + nosupp_text_len;

        let make_text_seg = |len| {
            PrimaryTextSegment::try_new_with_len(prim_text_begin, len, dso).map(|seg| {
                let other_begin = seg.try_next_byte().map_or(prim_text_begin, u64::from);
                (seg, other_begin)
            })
        };

        // include STEXT only if the optional keywords don't fit within the first
        // 99,999,999 bytes
        let prim_text_res = make_text_seg(all_text_len);
        let (prim_text_seg, other_segs, supp_text_seg, data_begin) =
            if let Ok((prim_text_seg, other_begin)) = prim_text_res {
                let (other_segs, next_begin) = Self::other_segments(other_begin, other_lens, dso)?;
                (
                    prim_text_seg,
                    other_segs,
                    SupplementalTextSegment::default(),
                    next_begin,
                )
            } else {
                let (prim_text_seg, other_begin) = make_text_seg(nosupp_text_len)?;
                let (other_segs, supp_text_begin) =
                    Self::other_segments(other_begin, other_lens, dso)?;
                let supp_text_seg =
                    SupplementalTextSegment::new_with_len(supp_text_begin, supp_text_len, dso);
                let data_begin = supp_text_seg
                    .try_next_byte()
                    .map_or(supp_text_begin, u64::from);
                (prim_text_seg, other_segs, supp_text_seg, data_begin)
            };

        let data_seg = TEXTDataSegment::new_with_len(data_begin, data_len, dso);

        let analysis_begin = data_seg.try_next_byte().map_or(data_begin, u64::from);
        let analysis_seg = TEXTAnalysisSegment::new_with_len(analysis_begin, analysis_len, dso);

        let h_analysis_seg = analysis_seg.as_header();
        let h_data_seg = data_seg.as_header();

        let nextdata = Nextdata(if has_nextdata.is_set() {
            let n = analysis_seg
                .try_next_byte()
                .map_or(analysis_begin, u64::from);
            UintZeroPad20(n)
        } else {
            UintZeroPad20(0)
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

        let (primary, supplemental) = if supp_text_seg.is_empty() {
            (all_req.chain(opt).collect(), vec![])
        } else {
            (all_req.collect(), opt)
        };

        let header = HeaderSegments::new(prim_text_seg, h_data_seg, h_analysis_seg, other_segs);

        Ok(Self::new(
            header,
            KeywordsWriter(primary),
            KeywordsWriter(supplemental),
            nextdata,
        ))
    }

    pub(crate) fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        version: Version,
        delim: TEXTDelim,
        other_segs: &[Other],
    ) -> io::Result<()>
    where
        T: Zero + HeaderString,
    {
        // write HEADER
        self.header.h_write(h, version)?;

        // write primary TEXT
        self.primary.h_write(h, delim.into())?;

        // write OTHER
        for o in other_segs {
            h.write_all(&o.0)?;
        }

        // write supplemental TEXT
        if !self.supplemental.0.is_empty() {
            self.supplemental.h_write(h, delim.into())?;
        }
        Ok(())
    }

    fn header_len(other_n: usize, w: u8) -> u64
    where
        T: HeaderString,
    {
        let n = u64::try_from(other_n).unwrap();
        let o = n * u64::from(w) * 2;
        u64::from(HEADER_LEN) + o
    }

    #[allow(clippy::type_complexity)]
    fn other_segments(
        begin: u64,
        other_lens: &[u64],
        offset: DatasetOffset,
    ) -> Result<(Vec<OtherSegment<T>>, u64), <T as TryFrom<u64>>::Error>
    where
        T: Copy + TryFrom<u64> + Into<u64>,
    {
        let ret = other_lens
            .iter()
            .scan(begin, |b, &length| {
                let s = OtherSegment::try_new_with_len(*b, length, offset);
                *b += length;
                Some(s)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let next = ret
            .iter()
            .filter_map(Segment::try_next_byte)
            .last()
            .map_or(begin, Into::into);
        Ok((ret, next))
    }
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

fn flat_keywords_length(ks: &[(String, String)]) -> u64 {
    let n = ks.iter().map(|(k, v)| k.len() + v.len() + 2).sum::<usize>();
    u64::try_from(n).expect("length of TEXT exceeds 2^64")
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
