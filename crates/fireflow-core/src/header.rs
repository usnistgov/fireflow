//! Reading and writing the HEADER segment

use crate::config::{
    AppendableFlag, ConfigFlag as _, DatasetOffset, ReadHeaderInnerConfig, ReadState,
    SelectVersionStrategy, VersionOverride,
};
use crate::core::Other;
use crate::logging::{
    DeferredErrors, DeferredIter as _, IOAnonErrorGroup, IOErrorGroup, IOGroupResult, LogResult,
    ResultExt, split_io,
};
use crate::segment::{
    GenericSegment, HasRegion, HasSource, HeaderAnalysisSegment, HeaderDataSegment, HeaderSegment,
    HeaderSegmentError, OtherSegment, OtherSegment20, PrimaryTextSegment, Segment,
    SegmentOverlapError, SupplementalTextSegment, TEXTAnalysisSegment, TEXTDataSegment,
    TEXTSegment,
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

use type_families::ApplyOnce as _;

use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use num_traits::identities::Zero;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::iter::once;
use std::str::FromStr;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromPyString, IntoPyString},
    pyo3::prelude::*,
};

/// The length of the HEADER.
///
/// This should always be the same. This also assumes that there are no OTHER
/// segments (which for now are not supported).
pub const HEADER_LEN: u8 = 58;

/// All FCS versions this library supports.
///
/// This appears as the first 6 bytes of any valid FCS file.
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Debug, Display)]
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

/// The three segments from the HEADER
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct HeaderSegments<T> {
    pub text: PrimaryTextSegment,
    pub data: HeaderDataSegment,
    pub analysis: HeaderAnalysisSegment,
    pub other: Vec<OtherSegment<T>>,
}

impl<T> HeaderSegments<T> {
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

    /// Ensure that TEXT segment does not start in HEADER and does not overlap.
    pub(crate) fn validate_text<I>(
        &self,
        s: &TEXTSegment<I>,
        w: OtherWidth,
    ) -> DeferredErrors<Option<GenericSegment>, HeaderValidationError>
    where
        I: HasRegion,
        T: HeaderString,
    {
        let contains_res = self
            .contains_text_segment(s, w)
            .map_err(HeaderValidationError::from)
            .into_deferred_nowarn();
        let overlap_res = self
            .overlaps_with(s)
            .map_errors(HeaderValidationError::from);
        contains_res.lift_f2_once(overlap_res, |q, _| q)
    }

    /// Check if TEXT segment starts within HEADER
    pub(crate) fn contains_text_segment<I>(
        &self,
        s: &TEXTSegment<I>,
        w: OtherWidth,
    ) -> Result<Option<GenericSegment>, InHeaderError>
    where
        I: HasRegion,
        T: HeaderString,
    {
        s.try_as_generic()
            .map_or(Ok(None), |q| self.contains_segment(q, w).map(Some))
    }

    /// Check if TEXT segment overlaps with any in HEADER.
    ///
    /// Assume HEADER itself has no overlapping segments.
    pub(crate) fn overlaps_with<I>(
        &self,
        s: &TEXTSegment<I>,
    ) -> DeferredErrors<Option<GenericSegment>, SegmentOverlapError>
    where
        I: HasRegion,
        T: Copy + Into<u64>,
    {
        if let Some(q) = s.try_as_generic() {
            self.as_generics()
                .map(|x| x.overlaps(&q).into_log())
                .sequence_def()
                .set_deferred_value(Some(q))
        } else {
            LogResult::new_ok(None)
        }
    }

    /// Ensure HEADER segments don't overlap and start after HEADER itself
    fn validate(&self, w: OtherWidth) -> DeferredErrors<(), HeaderValidationError>
    where
        T: Copy + Into<u64> + HeaderString,
    {
        let x = self
            .overlapping_segments()
            .map_errors(HeaderValidationError::from);
        let y = self
            .contains_header_segments(w)
            .map_errors(HeaderValidationError::from);
        x.lift_f2_once(y, |(), ()| ())
    }

    fn contains_header_segments(&self, w: OtherWidth) -> DeferredErrors<(), InHeaderError>
    where
        T: Copy + Into<u64> + HeaderString,
    {
        let t = self.contains_header_segment(&self.text, w);
        let d = self.contains_header_segment(&self.data, w);
        let a = self.contains_header_segment(&self.analysis, w);
        let os = self
            .other
            .iter()
            .map(|o| self.contains_header_segment(o, w));
        [t, d, a]
            .into_iter()
            .chain(os)
            .map(ResultExt::into_deferred_nowarn)
            .sequence_def_void()
    }

    fn contains_header_segment<I, S, T0>(
        &self,
        s: &Segment<I, S, T0>,
        w: OtherWidth,
    ) -> Result<Option<GenericSegment>, InHeaderError>
    where
        T: HeaderString,
        I: HasRegion,
        S: HasSource,
        T0: Into<u64> + Copy,
    {
        s.try_as_generic()
            .map_or(Ok(None), |q| self.contains_segment(q, w).map(Some))
    }

    fn contains_segment(
        &self,
        s: GenericSegment,
        w: OtherWidth,
    ) -> Result<GenericSegment, InHeaderError>
    where
        T: HeaderString,
    {
        if s.begin < self.nbytes(w) {
            Err(InHeaderError(s))
        } else {
            Ok(s)
        }
    }

    fn overlapping_segments(&self) -> DeferredErrors<(), SegmentOverlapError>
    where
        T: Copy + Into<u64>,
    {
        GenericSegment::find_overlaps(self.as_generics().collect())
    }

    /// Return number of bytes required to encode HEADER
    fn nbytes(&self, w: OtherWidth) -> u64
    where
        T: HeaderString,
    {
        HeaderKeywordsToWrite::<T>::header_len(self.other.len(), u8::from(w))
    }

    fn as_generics(&self) -> impl Iterator<Item = GenericSegment>
    where
        T: Copy + Into<u64>,
    {
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
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct Header {
    pub version: Version,
    pub segments: HeaderSegments<UintSpacePad20>,
}

impl Header {
    pub fn h_read<C, R>(
        h: &mut BufReader<R>,
        st: &ReadState<C>,
    ) -> IOGroupResult<Self, HeaderError, ()>
    where
        C: AsRef<ReadHeaderInnerConfig>,
        R: Read + Seek,
    {
        h.seek(SeekFrom::Start(st.dataset_offset.0))?;
        let (version, text, data, analysis) = h_read_required_header(h, st)?;
        let coords = [text.try_coords(), data.try_coords(), analysis.try_coords()];
        let min_coord = coords.iter().flatten().map(|x| x.0).min();
        let other_res = if let Some(m) = min_coord {
            split_io!(OtherSegment20::h_read_others(h, m, st))
        } else {
            Ok(vec![])
        };
        let conf: &ReadHeaderInnerConfig = st.conf.as_ref();
        other_res
            .map(|other| Self::new(version, HeaderSegments::new(text, data, analysis, other)))
            .ungroup()
            .map_errors(HeaderError::from)
            .and_then_commutative(|hdr| {
                hdr.segments
                    .validate(conf.other_width)
                    .map_errors(HeaderError::from)
                    .map_ok_value(|()| hdr)
            })
            .group()
            .resolve_nowarn()
            .map_err(IOErrorGroup::Pure)
    }
}

fn h_read_required_header<C, R>(
    h: &mut BufReader<R>,
    st: &ReadState<C>,
) -> IOGroupResult<
    (
        Version,
        PrimaryTextSegment,
        HeaderDataSegment,
        HeaderAnalysisSegment,
    ),
    HeaderError,
    (),
>
where
    R: Read + Seek,
    C: AsRef<ReadHeaderInnerConfig>,
{
    let conf = &st.conf.as_ref();
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
        .map(|(t, d, a)| (version, t, d, a))
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
    ) -> Result<Self, GuessVersionError> {
        let vs = [Self::FCS2_0, Self::FCS3_0, Self::FCS3_1, Self::FCS3_2];
        match ver_override {
            None => Ok(self),
            Some(VersionOverride::Force(v)) => Ok(*v),
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
                    let scores: Vec<_> = vs.iter().map(|&v| (v, opt.get_score(v, par))).collect();
                    if let Some(xs) =
                        NonEmpty::collect(scores.iter().filter(|(_, s)| s.is_passing(false)))
                    {
                        // Found at least one version that doesn't require dropping,
                        // rank by strategy to select
                        Ok(xs.maximum_by(|&x, &y| rank(x, y)).0)
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
                        Ok(ret.0)
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
    Validation(HeaderValidationError),
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
pub enum HeaderValidationError {
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
    pub(crate) header: HeaderSegments<T>,
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

#[cfg(feature = "python")]
mod python {
    use super::{HeaderSegments, UintSpacePad20};

    use pyo3::prelude::*;
    use pyo3::types::PyDict;

    impl<'py> IntoPyObject<'py> for HeaderSegments<UintSpacePad20> {
        type Target = PyDict;
        type Output = Bound<'py, PyDict>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            let dict = PyDict::new(py);
            dict.set_item("text", self.text.into_pyobject(py)?)?;
            dict.set_item("data", self.data.into_pyobject(py)?)?;
            dict.set_item("analysis", self.analysis.into_pyobject(py)?)?;
            dict.set_item("other", self.other.into_pyobject(py)?)?;
            Ok(dict)
        }
    }
}
