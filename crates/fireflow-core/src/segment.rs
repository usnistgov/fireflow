//! Reading and writing offsets in an FCS file

use crate::config::{
    AllowPseudoempty, ConfigFlag, DatasetOffset, FileLen, IgnoreTEXTAnalysisOffsets,
    IgnoreTEXTDataOffsets, OverlapCorrectionLimit, ProcessKeywordFailure, ProcessOptionalFailure,
    ReadDataKeywordsConfig, ReadHeaderInnerConfig, ReadOffsetConfig, ReadState,
    TruncateOffsetLimit,
};
use crate::header::{
    HEADER_LEN, HeaderSegments, ParsedHeaderSegments, ParsedOtherSegments, SegmentValidationError,
    Version,
};
use crate::logging::{
    CommutativeResultIter as _, ErrorsResult, IOErrorGroup, LogResult, ResultExt as _,
    SwitchableErrorsResult, WarningsAndErrorsResult, WarningsAndIOGroupResult, io_to_log,
};
use crate::text::keywords::{Beginanalysis, Begindata, Beginstext, Endanalysis, Enddata, Endstext};
use crate::text::lookup::{
    OptMetarootKey, Optional, ParseKeyError, ReqKeyErrorInner, ReqMetarootKey,
};
use crate::validated::ascii_range::{MAX_CHARS, OtherWidth};
use crate::validated::ascii_uint::{
    HeaderString, ParseFixedUintError, UintSpacePad8, UintSpacePad20, UintZeroPad20,
};
use crate::validated::keys::{Key, StdKeywords, StringOrBytes};

use type_families::{Functor as _, impl_functor, impl_kind1};

use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use num_traits::identities::Zero;
use thiserror::Error;

use std::fmt::{self, Debug};
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::iter::{self, once, repeat};
use std::marker::PhantomData;
use std::num::{NonZeroU64, ParseIntError};
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr};

/// Denotes a correction for a segment
#[derive(Default, Clone, Copy, new)]
pub struct OffsetCorrection<I, S> {
    begin: i32,
    end: i32,
    _id: PhantomData<I>,
    _src: PhantomData<S>,
}

pub type Segment<I, S, T> = OffsetSegment<I, S, T, DatasetOffset>;

/// A segment that is specific to a region in the FCS file.
#[derive(Clone, Copy, new, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[new(visibility = "")]
pub struct OffsetSegment<I, S, T, O> {
    inner: InnerSegment<T, O>,
    _id: PhantomData<I>,
    _src: PhantomData<S>,
}

/// Segment offsets as read straight from the file with no corrections.
///
/// Useful for diagnostics.
#[derive(Clone, Copy, PartialEq, Display, Debug, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("({begin}, {end})")]
pub struct UncorrectedSegment {
    pub begin: i128,
    pub end: i128,
}

/// A non-empty segment that still has regional/src data but is type-agnostic.
///
/// Useful for bulk operations on lots of segments at once that wouldn't work
/// if they segments were all different types.
#[derive(Clone, Copy, Debug, Display, new)]
#[display("segment for {region} from {src} with coords ({begin}, {end})")]
pub(crate) struct GenericSegment {
    pub(crate) begin: u64,
    pub(crate) end: u64,
    pub(crate) region: AnyRegion,
    pub(crate) src: AnySrc,
}

impl GenericSegment {
    pub(crate) fn as_pair(&self) -> (u64, u64) {
        (self.begin, self.end)
    }

    pub(crate) fn get_tail_overlap(&self, other: &Self) -> Option<u64> {
        (self.end + 1).checked_sub(other.begin)
    }
}

#[derive(Clone, Copy, Debug, Display)]
pub(crate) enum AnySrc {
    #[display("HEADER")]
    Header,
    #[display("TEXT")]
    Text,
}

#[derive(Clone, Copy, Debug, Display)]
pub(crate) enum AnyRegion {
    #[display("ANALYSIS")]
    Analysis,
    #[display("DATA")]
    Data,
    #[display("TEXT")]
    Text,
    #[display("STEXT")]
    Stext,
    #[display("OTHER")]
    Other,
}

/// Denotes [`Segment`] came from HEADER
#[derive(Default, Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct SegmentFromHeader;

/// Denotes [`Segment`] came from TEXT
#[derive(Default, Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct SegmentFromTEXT;

/// Denotes [`Segment`] came from either TEXT or HEADER
#[derive(Clone, Copy, PartialEq)]
pub struct SegmentFromAnywhere;

/// Denotes [`Segment`] pertains to primary TEXT
#[derive(Default, Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct PrimaryTextSegmentId;

/// Denotes [`Segment`] pertains to supplemental TEXT
#[derive(Default, Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct SupplementalTextSegmentId;

/// Denotes [`Segment`] pertains to DATA
#[derive(Default, Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct DataSegmentId;

/// Denotes [`Segment`] pertains to ANALYSIS
#[derive(Default, Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AnalysisSegmentId;

/// Denotes [`Segment`] pertains to OTHER (indexed from 0)
#[derive(Default, Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct OtherSegmentId;

/// Configuration for making a new [`Segment`]
#[derive(new)]
pub struct NewSegmentConfig<I, S> {
    corr: OffsetCorrection<I, S>,
    file_len: FileLen,
    dataset_offset: DatasetOffset,
    allow_pseudoempty: AllowPseudoempty,
    truncate_offset_limit: TruncateOffsetLimit,
}

impl<I, S> NewSegmentConfig<I, S> {
    fn from_read_config<C>(corr: OffsetCorrection<I, S>, st: &ReadState<C>) -> Self
    where
        C: AsRef<ReadOffsetConfig>,
    {
        let oconf = st.conf.as_ref();
        Self::new(
            corr,
            st.file_len,
            st.dataset_offset,
            oconf.allow_pseudoempty,
            oconf.truncate_offset_limit,
        )
    }
}

pub type PrimaryTextSegment = Segment<PrimaryTextSegmentId, SegmentFromHeader, UintSpacePad8>;
pub type SupplementalTextSegment =
    Segment<SupplementalTextSegmentId, SegmentFromTEXT, UintZeroPad20>;

type DataSegment<S, T> = Segment<DataSegmentId, S, T>;
pub type HeaderDataSegment = DataSegment<SegmentFromHeader, UintSpacePad8>;
pub type TEXTDataSegment = DataSegment<SegmentFromTEXT, UintZeroPad20>;

type AnalysisSegment<S, T> = Segment<AnalysisSegmentId, S, T>;
pub type HeaderAnalysisSegment = AnalysisSegment<SegmentFromHeader, UintSpacePad8>;
pub type TEXTAnalysisSegment = AnalysisSegment<SegmentFromTEXT, UintZeroPad20>;

pub type HeaderSegment<I> = Segment<I, SegmentFromHeader, UintSpacePad8>;
pub type TEXTSegment<I> = Segment<I, SegmentFromTEXT, UintZeroPad20>;
pub type AnySegment<I> = Segment<I, SegmentFromAnywhere, u64>;

pub type HeaderCorrection<I> = OffsetCorrection<I, SegmentFromHeader>;
pub type TEXTCorrection<I> = OffsetCorrection<I, SegmentFromTEXT>;

pub type AnyDataSegment = DataSegment<SegmentFromAnywhere, u64>;
pub type AnyAnalysisSegment = AnalysisSegment<SegmentFromAnywhere, u64>;

pub type OtherSegment<T> = Segment<OtherSegmentId, SegmentFromHeader, T>;
pub type OtherSegment8 = OtherSegment<UintSpacePad20>;
pub type OtherSegment20 = OtherSegment<UintSpacePad20>;

pub(crate) type ReqSegResult<T> = WarningsAndErrorsResult<
    (AnySegment<T>, Option<UncorrectedSegment>),
    (),
    ReqSegmentWithDefaultWarning<T>,
    ReqSegmentWithDefaultError<T>,
>;

pub(crate) type OptSegRes<T> = WarningsAndErrorsResult<
    (AnySegment<T>, Option<UncorrectedSegment>),
    (),
    OptSegmentWithDefaultWarning<T>,
    OptSegmentWithDefaultWarning<T>,
>;

pub type ReqSegmentWithDefaultWarning<T> =
    ReqSegmentWithDefaultWarning_<T, <T as KeyedSegment>::B, <T as KeyedSegment>::E>;

pub type ReqSegmentWithDefaultError<T> =
    ReqSegmentWithDefaultErrorInner<T, <T as KeyedSegment>::B, <T as KeyedSegment>::E>;

pub type OptSegmentWithDefaultWarning<T> =
    OptSegmentWithDefaultWarningInner<T, <T as KeyedSegment>::B, <T as KeyedSegment>::E>;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
enum InnerSegment<T, O> {
    NonEmpty(NonEmptySegment<T, O>),
    #[default]
    Empty,
}

/// An offset as shown in an FCS file.
#[derive(Debug, Clone, Copy, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
struct NonEmptySegment<T, O> {
    /// First coordinate (zero indexed)
    begin: T,

    /// Second coordinate pointing at the last byte of the segment.
    ///
    /// Note that length of segment is `end` - `begin` + 1
    end: T,

    /// The absolute position of the segment in the FCS file.
    ///
    /// `begin` and `end` are relative to this number. This will be the sum of
    /// all $NEXTDATA values for all previous datasets relative to the dataset
    /// in which this segment belongs (which implies it will be zero for the
    /// first dataset)
    dataset_offset: O,
}

/// Helper struct to bundle all but the DATA and ANALYSIS segments from TEXT
#[derive(new)]
pub struct NonDataSegments {
    pub(crate) header: ParsedHeaderSegments,
    pub(crate) supp: Option<SupplementalTextSegment>,
    pub(crate) uncorr_data: UncorrectedSegment,
    pub(crate) uncorr_analysis: UncorrectedSegment,
}

pub(crate) enum OneOrTwo<X> {
    One(X),
    Two(X, X),
}

impl_kind1!(pub(crate) OneOrTwoFamily, OneOrTwo);

impl_functor!(
    OneOrTwo,
    self,
    mut f,
    match self {
        Self::One(x) => OneOrTwo::One(f(x)),
        Self::Two(x, y) => OneOrTwo::Two(f(x), f(y)),
    }
);

impl<X> IntoIterator for OneOrTwo<X> {
    type Item = X;
    type IntoIter = iter::Chain<iter::Once<X>, <Option<X> as IntoIterator>::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        let (x, y) = self.split();
        once(x).chain(y)
    }
}

impl<X> OneOrTwo<X> {
    pub(crate) fn split(self) -> (X, Option<X>) {
        match self {
            Self::One(x) => (x, None),
            Self::Two(x, y) => (x, Some(y)),
        }
    }

    fn from_results<A, B>(x: Result<A, X>, y: Result<B, X>) -> Result<(A, B), Self> {
        match (x, y) {
            (Ok(a), Ok(b)) => Ok((a, b)),
            (Err(a), Ok(_)) => Err(Self::One(a)),
            (Ok(_), Err(b)) => Err(Self::One(b)),
            (Err(a), Err(b)) => Err(Self::Two(a, b)),
        }
    }
}

impl NonDataSegments {
    pub(crate) fn new_no_text(
        data: HeaderDataSegment,
        analyis: HeaderAnalysisSegment,
        other: ParsedOtherSegments,
        uncorr_data: UncorrectedSegment,
        uncorr_analysis: UncorrectedSegment,
    ) -> Self {
        let hdr = HeaderSegments::new(PrimaryTextSegment::default(), data, analyis, other);
        Self::new(hdr, None, uncorr_data, uncorr_analysis)
    }

    /// Ensure this segment does not overlap with other segments.
    ///
    /// Specifically check that no other segment (except its analogue in HEADER
    /// if non-empty) overlaps with this one. Also ensure that that these
    /// segments don't overlap with HEADER itself.
    fn validate<I>(
        &mut self,
        s: &mut TEXTSegment<I>,
        limit: OverlapCorrectionLimit,
    ) -> Vec<SegmentValidationError>
    where
        I: HasRegion + IsDataOrAnalysis,
    {
        if let Some(this_seg) = s.try_as_generic() {
            // Check for overlap with STEXT segment. This segment should not be
            // modified since it has already been read. Therefore, only change
            // the offsets of the new segment if its ending offset is within
            // STEXT.
            let stxt_error = self.supp.as_ref().and_then(|supp| {
                let stxt_seg = supp.try_as_generic()?;
                let overlap = this_seg.get_tail_overlap(&stxt_seg)?;
                if overlap <= limit.0 {
                    s.truncate(overlap);
                    None
                } else {
                    let e = SegmentOverlapError::new(this_seg, stxt_seg);
                    Some(SegmentValidationError::from(e))
                }
            });
            // Check for any errors between this segment and HEADER segments,
            // modifying as necessary and as overlap limit permits.
            self.header
                .validate_text_data_or_analysis(s, limit)
                .chain(stxt_error)
                .collect()
        } else {
            vec![]
        }
    }
}

pub(crate) trait HasSegmentPair: Sized {
    fn corrected_segment(segs: &NonDataSegments) -> HeaderSegment<Self>;
    fn uncorrected_segment(segs: &NonDataSegments) -> UncorrectedSegment;

    fn segment_pair(segs: &NonDataSegments) -> (HeaderSegment<Self>, UncorrectedSegment) {
        (
            Self::corrected_segment(segs),
            Self::uncorrected_segment(segs),
        )
    }
}

/// Operations to obtain optional segment from TEXT keywords
pub trait KeyedSegment: Sized + Copy {
    type B: Key + Into<UintZeroPad20> + FromStr<Err = ParseIntError>;
    type E: Key + Into<UintZeroPad20> + FromStr<Err = ParseIntError>;
}

/// Operations to obtain optional segment from TEXT keywords
pub(crate) trait KeyedSegmentInner: KeyedSegment + HasRegion {
    #[allow(clippy::type_complexity)]
    #[allow(clippy::result_large_err)]
    fn pair_to_segment<C>(
        x0: Self::B,
        x1: Self::E,
        corr: TEXTCorrection<Self>,
        st: &ReadState<C>,
    ) -> Result<
        (
            Segment<Self, SegmentFromTEXT, UintZeroPad20>,
            UncorrectedSegment,
        ),
        SegmentError,
    >
    where
        C: AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        let y0 = i128::from(x0);
        let y1 = i128::from(x1);
        let new_conf = NewSegmentConfig::from_read_config(corr, st);
        let raw = UncorrectedSegment::new(y0, y1);
        Segment::try_new(y0, y1, &new_conf).map(|x| (x, raw))
    }
}

/// Operations to obtain required segment from TEXT keywords
pub(crate) trait KeyedReqSegment: KeyedSegmentInner
where
    Self::B: ReqMetarootKey,
    Self::E: ReqMetarootKey,
{
    #[allow(clippy::type_complexity)]
    #[allow(clippy::result_large_err)]
    fn with_req_pair<C>(
        pair: ReqPair<Self::B, Self::E>,
        corr: TEXTCorrection<Self>,
        st: &ReadState<C>,
    ) -> Result<
        (
            Segment<Self, SegmentFromTEXT, UintZeroPad20>,
            UncorrectedSegment,
        ),
        OneOrTwo<ReqSegmentError<Self::B, Self::E>>,
    >
    where
        C: AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        match pair {
            Ok((x0, x1)) => Self::pair_to_segment(x0, x1, corr, st)
                .map_err(ReqSegmentError::Segment)
                .map_err(OneOrTwo::One),
            Err(e) => Err(e.fmap(ReqSegmentError::Key)),
        }
    }

    fn get_req_pair(kws: &StdKeywords) -> ReqPair<Self::B, Self::E> {
        let x0 = Self::B::get_metaroot_req(kws).map_err(ReqSegmentKeyError::Begin);
        let x1 = Self::E::get_metaroot_req(kws).map_err(ReqSegmentKeyError::End);
        OneOrTwo::from_results(x0, x1)
    }

    fn remove_req_pair(kws: &mut StdKeywords) -> ReqPair<Self::B, Self::E> {
        let x0 = Self::B::remove_metaroot_req(kws).map_err(ReqSegmentKeyError::Begin);
        let x1 = Self::E::remove_metaroot_req(kws).map_err(ReqSegmentKeyError::End);
        OneOrTwo::from_results(x0, x1)
    }
}

/// Operations to obtain required segment from TEXT keywords with a default segment
pub(crate) trait KeyedReqSegmentWithDefault: KeyedReqSegment + HasRegion
where
    Self::B: ReqMetarootKey,
    Self::E: ReqMetarootKey,
{
    type IgnoreFlag: ConfigFlag;
    type OtherDataId: HasRegion;

    fn get_req_or<C>(
        kws: &StdKeywords,
        segs: &mut NonDataSegments,
        ignore: Self::IgnoreFlag,
        corr: TEXTCorrection<Self>,
        st: &ReadState<C>,
    ) -> ReqSegResult<Self>
    where
        Self: HasSegmentPair + IsDataOrAnalysis,
        Self::OtherDataId: HasSegmentPair,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        if ignore.is_set() {
            let default = Self::corrected_segment(segs);
            LogResult::new_ok((default.into_any(), None))
        } else {
            Self::with_req_pair_default(Self::get_req_pair(kws), segs, corr, st)
        }
    }

    fn remove_req_or<C>(
        kws: &mut StdKeywords,
        segs: &mut NonDataSegments,
        ignore: Self::IgnoreFlag,
        corr: TEXTCorrection<Self>,
        st: &ReadState<C>,
    ) -> ReqSegResult<Self>
    where
        Self: HasSegmentPair + IsDataOrAnalysis,
        Self::OtherDataId: HasSegmentPair,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        // if we want to totally ignore the TEXT offsets, just blindly remove
        // them so we don't trigger any pseudostandard false positives later and
        // return the default segment
        if ignore.is_set() {
            let _ = Self::remove_req_pair(kws);
            let default = Self::corrected_segment(segs);
            LogResult::new_ok((default.into_any(), None))
        } else {
            Self::with_req_pair_default(Self::remove_req_pair(kws), segs, corr, st)
        }
    }

    fn with_req_pair_default<C>(
        pair: ReqPair<Self::B, Self::E>,
        segs: &mut NonDataSegments,
        corr: TEXTCorrection<Self>,
        st: &ReadState<C>,
    ) -> ReqSegResult<Self>
    where
        Self: HasSegmentPair + IsDataOrAnalysis,
        Self::OtherDataId: HasSegmentPair,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        let dconf: &ReadDataKeywordsConfig = st.conf.as_ref();
        let oconf: &ReadOffsetConfig = st.conf.as_ref();
        let (header_seg, uncorr_hdr) = Self::segment_pair(segs);
        let header_pair = (header_seg.into_any(), None);
        let mismatch_flag = dconf.allow_header_text_offset_mismatch;
        let missing_flag = dconf.allow_missing_required_offsets;
        let limit = oconf.overlap_correction_limit;

        let default_warning = || {
            let w = ReqSegmentWithDefaultWarning::from(SegmentDefaultWarning::default());
            LogResult::new_ok(header_pair).set_commutative_warnings(vec![w])
        };

        let mut pair_to_text = |uncorr_txt: UncorrectedSegment, mismatch_warn| {
            let seg_conf = NewSegmentConfig::from_read_config(corr, st);
            let seg_res = Segment::try_new(uncorr_txt.begin, uncorr_txt.end, &seg_conf)
                .map_err(ReqSegmentError::Segment);
            match seg_res {
                Ok(mut text_seg) => {
                    let es = segs.validate(&mut text_seg, limit);
                    let mut res =
                        SwitchableErrorsResult::new_switchable_iter3((), (), es, missing_flag)
                            .map_switchable_errors(ReqSegmentWithDefaultErrorInner::from)
                            .switchable_into_commutative()
                            .map_commutative_warnings(ReqSegmentWithDefaultWarning::from)
                            .set_ok_value((text_seg.into_any(), Some(uncorr_txt)));
                    res.extend_commutative_warnings(mismatch_warn);
                    res
                }
                Err(e) => default_warning().extend_warnings_or_errors3(
                    Some(ReqSegmentWithDefaultErrorInner::from(e)),
                    |_| (),
                    |()| (),
                    ReqSegmentWithDefaultWarning::Error,
                    |x| x,
                    missing_flag,
                ),
            }
        };

        let mut choose = |uncorr_txt| {
            if header_seg.is_empty() {
                // HEADER is empty, ignore the mismatch and get TEXT offsets
                // without mismatch warning
                pair_to_text(uncorr_txt, None)
            } else if let Some((choose_header, do_warn)) = mismatch_flag.is_warning() {
                // Not an error, choose offset and optionally throw warning
                let e = SegmentMismatchError::new(uncorr_hdr, uncorr_txt, Some(choose_header));
                let w = do_warn
                    .then_some(e)
                    .map(ReqSegmentWithDefaultErrorInner::from)
                    .map(ReqSegmentWithDefaultWarning::from);
                if choose_header {
                    // We choose HEADER, return it possibly with warning
                    let ws = w.into_iter().collect::<Vec<_>>();
                    LogResult::new_ok(header_pair).set_commutative_warnings(ws)
                } else {
                    // We choose TEXT, convert offsets to segment, validate, and
                    // possibly attach warning for mismatch
                    pair_to_text(uncorr_txt, w)
                }
            } else {
                // Error for mismatch, don't bother processing offsets
                let e = SegmentMismatchError::new(uncorr_hdr, uncorr_txt, None);
                WarningsAndErrorsResult::new_err(e).map_errors(ReqSegmentWithDefaultError::from)
            }
        };

        match pair {
            // TEXT offsets found, compare with HEADER
            Ok((x0, x1)) => {
                let uncorr_txt = UncorrectedSegment::new(i128::from(x0), i128::from(x1));
                if uncorr_txt == uncorr_hdr {
                    // Uncorrected offsets are identical, not a mismatch
                    LogResult::new_ok(header_pair)
                } else {
                    // Offsets not identical, choose one
                    choose(uncorr_txt)
                }
            }
            // TEXT offsets not found, throw error or warning depending on
            // if we want to enforce required offsets
            Err(es) => {
                let es0 = es
                    .fmap(ReqSegmentError::Key)
                    .fmap(ReqSegmentWithDefaultErrorInner::from);
                default_warning().extend_warnings_or_errors3(
                    es0,
                    |_| (),
                    |()| (),
                    ReqSegmentWithDefaultWarning::Error,
                    |e| e,
                    missing_flag,
                )
            }
        }
    }
}

/// Operations to obtain optional segment from TEXT keywords
pub(crate) trait KeyedOptSegment: KeyedSegmentInner
where
    Self::B: OptMetarootKey + Optional<Outer = Option<Self::B>>,
    Self::E: OptMetarootKey + Optional<Outer = Option<Self::E>>,
{
    #[allow(clippy::result_large_err)]
    #[allow(clippy::type_complexity)]
    fn with_opt_pair<C>(
        pair: OptPair<Self::B, Self::E>,
        corr: TEXTCorrection<Self>,
        st: &ReadState<C>,
    ) -> Result<
        Option<(
            Segment<Self, SegmentFromTEXT, UintZeroPad20>,
            UncorrectedSegment,
        )>,
        OneOrTwo<OptSegmentError<Self::B, Self::E>>,
    >
    where
        C: AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        match pair {
            Ok(maybe) => maybe
                .map(|(x0, x1)| Self::pair_to_segment(x0, x1, corr, st))
                .transpose()
                .map_err(OptSegmentError::Segment)
                .map_err(OneOrTwo::One),
            Err(e) => Err(e.fmap(OptSegmentError::Key)),
        }
    }

    fn get_opt_pair(kws: &StdKeywords) -> OptPair<Self::B, Self::E> {
        let x0 = Self::B::get_root_opt(kws).map_err(OptSegmentKeyError::Begin);
        let x1 = Self::E::get_root_opt(kws).map_err(OptSegmentKeyError::End);
        OneOrTwo::from_results(x0, x1).map(|(x, y)| x.zip(y))
    }

    fn remove_opt_pair(kws: &mut StdKeywords) -> OptPair<Self::B, Self::E> {
        // TODO these should process optional keywords the same as everything else
        let x0 = Self::B::remove_root_opt(kws).map_err(OptSegmentKeyError::Begin);
        let x1 = Self::E::remove_root_opt(kws).map_err(OptSegmentKeyError::End);
        OneOrTwo::from_results(x0, x1).map(|(x, y)| x.zip(y))
    }
}

/// Operations to obtain optional segment from TEXT keywords with a default segment
pub(crate) trait KeyedOptSegmentWithDefault: KeyedOptSegment + HasRegion
where
    Self::B: OptMetarootKey + Optional<Outer = Option<Self::B>>,
    Self::E: OptMetarootKey + Optional<Outer = Option<Self::E>>,
{
    type IgnoreFlag: ConfigFlag;
    type OtherDataId: HasRegion;

    fn get_opt_or<C>(
        kws: &StdKeywords,
        segs: &mut NonDataSegments,
        ignore: Self::IgnoreFlag,
        corr: TEXTCorrection<Self>,
        st: &ReadState<C>,
    ) -> OptSegRes<Self>
    where
        Self: HasSegmentPair + IsDataOrAnalysis,
        Self::OtherDataId: HasSegmentPair,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        if ignore.is_set() {
            let default = Self::corrected_segment(segs);
            LogResult::new_ok((default.into_any(), None))
        } else {
            let pair = Self::get_opt_pair(kws);
            Self::with_opt_pair_default(pair, segs, corr, st)
        }
    }

    fn remove_opt_or<C>(
        kws: &mut StdKeywords,
        segs: &mut NonDataSegments,
        ignore: Self::IgnoreFlag,
        corr: TEXTCorrection<Self>,
        st: &ReadState<C>,
    ) -> OptSegRes<Self>
    where
        Self: HasSegmentPair + IsDataOrAnalysis,
        Self::OtherDataId: HasSegmentPair,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        if ignore.is_set() {
            let default = Self::corrected_segment(segs);
            let _ = Self::remove_opt_pair(kws);
            LogResult::new_ok((default.into_any(), None))
        } else {
            let pair = Self::remove_opt_pair(kws);
            Self::with_opt_pair_default(pair, segs, corr, st)
        }
    }

    fn with_opt_pair_default<C>(
        pair: OptPair<Self::B, Self::E>,
        segs: &mut NonDataSegments,
        corr: TEXTCorrection<Self>,
        st: &ReadState<C>,
    ) -> OptSegRes<Self>
    where
        Self: HasSegmentPair + IsDataOrAnalysis,
        Self::OtherDataId: HasSegmentPair,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        let dconf: &ReadDataKeywordsConfig = st.conf.as_ref();
        let oconf: &ReadOffsetConfig = st.conf.as_ref();
        let (header_seg, uncorr_hdr) = Self::segment_pair(segs);
        let header_pair = (header_seg.into_any(), None);
        // TODO configure this
        let drop_flag = ProcessOptionalFailure(ProcessKeywordFailure::Drop);
        let mismatch_flag = dconf.allow_header_text_offset_mismatch;
        let limit = oconf.overlap_correction_limit;

        let mut pair_to_text = |uncorr_txt: UncorrectedSegment, mismatch_warn| {
            let seg_conf = NewSegmentConfig::from_read_config(corr, st);
            let seg_res = Segment::try_new(uncorr_txt.begin, uncorr_txt.end, &seg_conf)
                .map_err(OptSegmentError::Segment);
            match seg_res {
                Ok(mut text_seg) => {
                    let es = segs.validate(&mut text_seg, limit);
                    let mut res =
                        SwitchableErrorsResult::new_switchable_iter((), (), es, drop_flag)
                            .map_switchable_errors(OptSegmentWithDefaultWarning::from)
                            .switchable_into_commutative()
                            .set_ok_value((text_seg.into_any(), Some(uncorr_txt)));
                    res.extend_commutative_warnings(mismatch_warn);
                    res
                }
                Err(e) => SwitchableErrorsResult::new_deferred_switchable((), e, drop_flag)
                    .map_switchable_errors(OptSegmentWithDefaultWarning::from)
                    .switchable_into_commutative()
                    .set_ok_value(header_pair),
            }
        };

        let mut choose = |uncorr_txt| {
            if header_seg.is_empty() {
                // HEADER is empty, ignore the mismatch and get TEXT offsets
                // without mismatch warning
                pair_to_text(uncorr_txt, None)
            } else if let Some((choose_header, do_warn)) = mismatch_flag.is_warning() {
                // Not an error, figure out which segment we want
                let me = SegmentMismatchError::new(uncorr_hdr, uncorr_txt, Some(choose_header));
                let w = do_warn
                    .then_some(me)
                    .map(OptSegmentWithDefaultWarning::from);
                if choose_header {
                    // We choose HEADER, return it possibly with warning
                    let ws = w.into_iter().collect::<Vec<_>>();
                    LogResult::new_ok(header_pair).set_commutative_warnings(ws)
                } else {
                    // We choose TEXT, create new TEXT segment from pairs,
                    // validate it, and possibly attach a warning
                    pair_to_text(uncorr_txt, w)
                }
            } else {
                // Error, don't bother with any segment processing
                let e = SegmentMismatchError::new(uncorr_hdr, uncorr_txt, None);
                WarningsAndErrorsResult::new_err(e).map_errors(OptSegmentWithDefaultWarning::from)
            }
        };

        match pair {
            // No TEXT segment found, but no errors either, just use HEADER
            Ok(None) => LogResult::new_ok(header_pair),
            // TEXT offsets found without errors, compare with HEADER
            Ok(Some((x0, x1))) => {
                let uncorr_txt = UncorrectedSegment::new(i128::from(x0), i128::from(x1));
                if uncorr_txt == uncorr_hdr {
                    // Uncorrected HEADER and TEXT are identical, just use HEADER
                    LogResult::new_ok(header_pair)
                } else {
                    // Segments are mismatched, figure out what to do
                    choose(uncorr_txt)
                }
            }
            // TEXT pairs found with errors, use HEADER
            Err(es) => {
                let (e0, e1) = es.split();
                SwitchableErrorsResult::new_deferred_switchable((), e0, drop_flag)
                    .extend_deferred_switchable_errors(e1)
                    .set_ok_value(header_pair)
                    .map_switchable_errors(OptSegmentError::Key)
                    .map_switchable_errors(OptSegmentWithDefaultWarningInner::from)
                    .switchable_into_commutative()
            }
        }
    }
}

type ReqPair<B, E> = Result<(B, E), OneOrTwo<ReqSegmentKeyError<B, E>>>;

type OptPair<B, E> = Result<Option<(B, E)>, OneOrTwo<OptSegmentKeyError<B, E>>>;

/// Denotes that a type comes from a specific part of the FCS file
pub(crate) trait HasSource {
    const SRC: AnySrc;
}

/// Denotes that a type pertains to a region of the FCS file
pub(crate) trait HasRegion {
    const REGION: AnyRegion;
}

/// Denotes that a type pertains to a region of the FCS file
pub(crate) trait IsDataOrAnalysis {
    const IS_DATA: bool;
}

impl HasSegmentPair for DataSegmentId {
    fn corrected_segment(segs: &NonDataSegments) -> HeaderSegment<Self> {
        segs.header.data
    }

    fn uncorrected_segment(segs: &NonDataSegments) -> UncorrectedSegment {
        segs.uncorr_data
    }
}

impl HasSegmentPair for AnalysisSegmentId {
    fn corrected_segment(segs: &NonDataSegments) -> HeaderSegment<Self> {
        segs.header.analysis
    }

    fn uncorrected_segment(segs: &NonDataSegments) -> UncorrectedSegment {
        segs.uncorr_analysis
    }
}

impl KeyedSegment for AnalysisSegmentId {
    type B = Beginanalysis;
    type E = Endanalysis;
}

impl KeyedSegment for DataSegmentId {
    type B = Begindata;
    type E = Enddata;
}

impl KeyedSegment for SupplementalTextSegmentId {
    type B = Beginstext;
    type E = Endstext;
}

impl KeyedSegmentInner for AnalysisSegmentId {}
impl KeyedSegmentInner for DataSegmentId {}
impl KeyedSegmentInner for SupplementalTextSegmentId {}

impl KeyedReqSegment for AnalysisSegmentId {}
impl KeyedReqSegment for DataSegmentId {}
impl KeyedReqSegment for SupplementalTextSegmentId {}

impl KeyedOptSegment for AnalysisSegmentId {}
impl KeyedOptSegment for SupplementalTextSegmentId {}

impl KeyedReqSegmentWithDefault for AnalysisSegmentId {
    type IgnoreFlag = IgnoreTEXTAnalysisOffsets;
    type OtherDataId = DataSegmentId;
}

impl KeyedOptSegmentWithDefault for AnalysisSegmentId {
    type IgnoreFlag = IgnoreTEXTAnalysisOffsets;
    type OtherDataId = DataSegmentId;
}

impl KeyedReqSegmentWithDefault for DataSegmentId {
    type IgnoreFlag = IgnoreTEXTDataOffsets;
    type OtherDataId = AnalysisSegmentId;
}

impl HasSource for SegmentFromHeader {
    const SRC: AnySrc = AnySrc::Header;
}

impl HasSource for SegmentFromTEXT {
    const SRC: AnySrc = AnySrc::Text;
}

impl HasRegion for AnalysisSegmentId {
    const REGION: AnyRegion = AnyRegion::Analysis;
}

impl HasRegion for DataSegmentId {
    const REGION: AnyRegion = AnyRegion::Data;
}

impl HasRegion for SupplementalTextSegmentId {
    const REGION: AnyRegion = AnyRegion::Stext;
}

impl HasRegion for PrimaryTextSegmentId {
    const REGION: AnyRegion = AnyRegion::Text;
}

impl HasRegion for OtherSegmentId {
    const REGION: AnyRegion = AnyRegion::Other;
}

impl IsDataOrAnalysis for AnalysisSegmentId {
    const IS_DATA: bool = false;
}

impl IsDataOrAnalysis for DataSegmentId {
    const IS_DATA: bool = true;
}

impl<I, S> From<(i32, i32)> for OffsetCorrection<I, S> {
    fn from(value: (i32, i32)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl<I, S> From<(Option<i32>, Option<i32>)> for OffsetCorrection<I, S> {
    fn from(value: (Option<i32>, Option<i32>)) -> Self {
        Self::from((value.0.unwrap_or_default(), value.1.unwrap_or_default()))
    }
}

impl<I, S, T> Default for Segment<I, S, T> {
    fn default() -> Self {
        Self::new(InnerSegment::Empty)
    }
}

impl<I, S, T> Segment<I, S, T> {
    pub(crate) fn into_any(self) -> AnySegment<I>
    where
        T: Into<u64> + Copy,
    {
        Segment::new(self.inner.as_u64())
    }

    /// Return the first and last byte with offset or `None` if empty
    pub(crate) fn try_coords(&self) -> Option<(T, T, DatasetOffset)>
    where
        T: Copy,
    {
        self.inner.try_as_nonempty().map(|x| {
            let (a, b) = x.coords();
            (a, b, x.dataset_offset)
        })
    }

    /// Return the first and last byte with offset or `None` if empty
    pub(crate) fn try_abs_coords(&self) -> Option<(u64, u64)>
    where
        T: Copy + Into<u64>,
    {
        self.try_coords().map(|(a, b, o)| {
            let x = u64::from(o);
            (a.into() + x, b.into() + x)
        })
    }

    /// Subtract n bytes off the end of this offset
    pub(crate) fn truncate(&mut self, n: u64)
    where
        T: TryFrom<u64> + Copy,
        T::Error: Debug,
        u64: From<T>,
    {
        self.inner.truncate(n);
    }

    /// Read bytes within this segment
    pub(crate) fn h_read_contents<R>(
        &self,
        h: &mut BufReader<R>,
        buf: &mut Vec<u8>,
    ) -> io::Result<()>
    where
        R: Read + Seek,
        T: Into<u64> + Copy,
    {
        match self.inner {
            InnerSegment::Empty => Ok(()),
            InnerSegment::NonEmpty(s) => {
                let begin = s.begin.into();
                let end = begin + s.dataset_offset.0;
                let nbytes = u64::from(s.nbytes());

                #[cfg(debug_assertions)]
                {
                    let current_pos = h.stream_position()?;
                    let file_size = h.seek(SeekFrom::End(0))?;
                    h.seek(SeekFrom::Start(current_pos))?;
                    assert!(end < file_size, "end of segment exceeds file");
                }

                h.seek(SeekFrom::Start(end))?;
                h.take(nbytes).read_to_end(buf)?;
                Ok(())
            }
        }
    }

    /// Return true if segment has 0 bytes
    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Return the number of bytes in this segment
    pub(crate) fn len(&self) -> u64
    where
        T: Copy + Into<u64>,
    {
        // NOTE In FCS a 0,0 means "empty" but this also means one byte
        // according to the spec's on definitions. The first number points to
        // the first byte in a segment, and the second number points to the last
        // byte, therefore 0,0 means "0 is both the first and last byte, which
        // also means there is one byte".
        self.inner
            .try_as_nonempty()
            .map_or(0, |s| u64::from(s.nbytes()))
    }

    /// Return byte after end of segment if applicable
    pub(crate) fn try_next_byte(&self) -> Option<NonZeroU64>
    where
        T: Copy + Into<u64>,
    {
        self.inner.try_as_nonempty().map(|x| x.next_byte())
    }

    /// Convert offsets to u64
    #[cfg(feature = "python")]
    pub(crate) fn as_u64(&self) -> Segment<I, S, u64>
    where
        T: Into<u64> + Copy,
    {
        Segment::new(self.inner.as_u64())
    }

    pub(crate) fn try_new_with_len(
        begin: u64,
        length: u64,
        offset: DatasetOffset,
    ) -> Result<Self, <T as TryFrom<u64>>::Error>
    where
        T: TryFrom<u64> + Copy,
    {
        let s = if length == 0 {
            InnerSegment::default()
        } else {
            let end = (begin + length - 1).try_into()?;
            InnerSegment::NonEmpty(NonEmptySegment::new(begin.try_into()?, end, offset))
        };
        Ok(Self::new(s))
    }

    pub(crate) fn new_with_len(begin: u64, length: u64, offset: DatasetOffset) -> Self
    where
        T: From<u64> + Copy,
    {
        let inner = if length == 0 {
            InnerSegment::default()
        } else {
            let end = (begin + length - 1).into();
            InnerSegment::NonEmpty(NonEmptySegment::new(begin.into(), end, offset))
        };
        Self::new(inner)
    }

    pub(crate) fn try_as_generic(&self) -> Option<GenericSegment>
    where
        I: HasRegion,
        S: HasSource,
        T: Copy + Into<u64>,
    {
        self.inner.try_as_nonempty().map(|x| {
            let (begin, end) = x.as_u64().coords();
            GenericSegment::new(begin, end, I::REGION, S::SRC)
        })
    }

    fn try_new(begin: i128, end: i128, conf: &NewSegmentConfig<I, S>) -> Result<Self, SegmentError>
    where
        I: HasRegion,
        S: HasSource,
        T: TryFrom<i128>,
    {
        InnerSegment::try_new::<I, S>(begin, end, conf).map(Self::new)
    }
}

impl<I> TEXTSegment<I> {
    /// Convert TEXT segment to HEADER segment.
    ///
    /// If offsets are too big, return an empty segment.
    pub(crate) fn as_header(&self) -> HeaderSegment<I> {
        let inner = self
            .try_coords()
            .map_or(InnerSegment::default(), |(b, e, o)| {
                let br = u64::from(b).try_into();
                let er = u64::from(e).try_into();
                if let (Ok(begin), Ok(end)) = (br, er) {
                    InnerSegment::NonEmpty(NonEmptySegment::new(begin, end, o))
                } else {
                    InnerSegment::default()
                }
            });
        Segment::new(inner)
    }
}

impl<I, T> Segment<I, SegmentFromHeader, T> {
    pub(crate) fn header_string(&self) -> String
    where
        T: Zero + HeaderString,
    {
        let (b, e) = self
            .try_coords()
            .map_or((T::zero(), T::zero()), |(b, e, _)| (b, e));
        let mut s = String::new();
        s.push_str(&b.header_string());
        s.push_str(&e.header_string());
        s
    }
}

impl<I: Copy> HeaderSegment<I> {
    pub(crate) fn h_read_primary<C, R>(
        h: &mut BufReader<R>,
        is_text: bool,
        corr: HeaderCorrection<I>,
        version: Version,
        st: &ReadState<C>,
    ) -> Result<(Self, UncorrectedSegment), IOErrorGroup<HeaderSegmentError, ()>>
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderInnerConfig> + AsRef<ReadOffsetConfig>,
        I: HasRegion + Copy,
    {
        let hconf: &ReadHeaderInnerConfig = st.conf.as_ref();
        let seg_conf = NewSegmentConfig::from_read_config(corr, st);

        let mut buf0 = [0_u8; 8];
        let mut buf1 = [0_u8; 8];

        let remaining = st.remaining_bytes(h)?;

        if remaining < 16 {
            let pos = h.stream_position()?;
            let e = OffsetsNoBytesError::new(pos, remaining, 16, I::REGION, AnySrc::Header);
            return Err(IOErrorGroup::new_pure_one(e.into()));
        }

        h.read_exact(&mut buf0)?;
        h.read_exact(&mut buf1)?;

        let parse_one = |bs, is_begin| {
            // TEXT segment should never be blank
            let allow_blank = !is_text;
            UintSpacePad8::from_bytes(bs, allow_blank).map_err(|error| {
                let src = StringOrBytes::from(bs.to_vec());
                ParseOffsetError::new(error, is_begin, I::REGION, src).into()
            })
        };

        let begin_res = parse_one(buf0, true).into_nowarn();
        let end_res = parse_one(buf1, false).into_nowarn();
        begin_res
            .zip_commutative(end_res)
            .and_then_commutative(|(begin, end)| {
                // TEXT segment is not squishable
                let allow_squish = !is_text;
                let squish = hconf.squish_offsets.is_set() && allow_squish;
                let raw = UncorrectedSegment::new(begin, end);
                Self::try_new_squish(begin, end, squish, version, &seg_conf)
                    .map(|x| (x, raw))
                    .map_err(HeaderSegmentError::from)
                    .into_log()
            })
            .group()
            .resolve_nowarn()
            .map_err(IOErrorGroup::Pure)
    }

    // pub(crate) fn unless(
    //     self,
    //     other: TEXTSegment<I>,
    // ) -> (AnySegment<I>, Option<SegmentMismatchWarning<I>>) {
    //     if other.inner.as_u64() != self.inner.as_u64() && !self.inner.is_empty() {
    //         let e = SegmentMismatchWarning::new(self, other);
    //         (self.into_any(), Some(e))
    //     } else {
    //         (Segment::new(other.inner.as_u64()), None)
    //     }
    // }

    fn try_new_squish(
        begin: i128,
        end: i128,
        squish_offsets: bool,
        version: Version,
        conf: &NewSegmentConfig<I, SegmentFromHeader>,
    ) -> Result<Self, SegmentError>
    where
        I: HasRegion,
    {
        // never run on 2.0 since offset "squishing" only applies to HEADER
        // offsets that overflow and necessitate TEXT offsets, which don't exist
        // in 2.0
        let (b, e) = if version > Version::FCS2_0 && squish_offsets && end == 0 && begin > 0 {
            (0, 0)
        } else {
            (begin, end)
        };
        Self::try_new(b, e, conf)
    }
}

impl OtherSegment20 {
    // TODO this won't deal with offsets like 0,-1, which hopefully aren't too
    // common.
    #[allow(clippy::type_complexity)]
    pub(crate) fn h_read_others<C, R>(
        h: &mut BufReader<R>,
        first_seg_begin: UintSpacePad8,
        st: &ReadState<C>,
    ) -> WarningsAndIOGroupResult<
        Option<(NonEmpty<(Self, UncorrectedSegment)>, OtherWidth)>,
        GuessOtherWidthError,
        HeaderSegmentError,
        (),
    >
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderInnerConfig> + AsRef<ReadOffsetConfig>,
    {
        let hconf: &ReadHeaderInnerConfig = st.conf.as_ref();

        // Get maximum length of OTHER offset region according to first required
        // offset. If zero, exit early.
        let Ok(max_other_len): Result<NonZeroU64, _> = u64::from(first_seg_begin)
            .checked_sub(u64::from(HEADER_LEN))
            .expect("minimal offset is less than 58")
            .try_into()
        else {
            return LogResult::new_ok(None);
        };

        // Get max desired number of segments; If zero, exit early.
        let Ok(max_other) = hconf
            .max_other
            .map(|x| u64::try_from(x).expect("usize overflow"))
            .map(NonZeroU64::try_from)
            .transpose()
        else {
            return LogResult::new_ok(None);
        };

        // Check that we have enough bytes left to read the offsets.
        let remaining = io_to_log!(st.remaining_bytes(h));
        if remaining < u64::from(max_other_len) {
            // ASSUME this will always be at byte 58 (that's what the error says)
            let e = OtherOffsetsNoBytesError::new(remaining, max_other_len);
            return LogResult::new_err(IOErrorGroup::new_pure_one(e.into()));
        }

        // Read the offsets.
        //
        // TODO (minor optimization opportunity) This will take all bytes
        // between offset 58 and the next required offset (ie from
        // TEXT/DATA/ANALYSIS in HEADER). In %99.9999 of case, the first segment
        // will be one of these three. However, it is theoretically possible
        // that this region has both the OTHER offsets and the OTHER segments
        // themselves. This is technically standards compliant since OTHER
        // segments only need to be within the first 99,999,999 bytes as of 3.2
        // (in earlier versions this was even less restricted since they did not
        // specify a width). In these cases, reading bytes like this will
        // result in the OTHER segments themselves being read twice (here they
        // be read and ignored).
        let mut buf = vec![];
        io_to_log!(h.take(u64::from(max_other_len)).read_to_end(&mut buf));

        // Only consider bytes which are spaces, nulls, or digits.
        let n_valid_bytes = buf
            .iter()
            .take_while(|&&x| x == 0 || x == 32 || (48..=57).contains(&x))
            .count();
        let valid_buf = &buf[0..n_valid_bytes];

        // Exit early if there are only spaces, nulls, or zero
        if valid_buf.iter().all(|&x| x == 0 || x == 32 || x == 48) {
            return LogResult::new_ok(None);
        }

        // Guess offset width if desired.
        let width_res = if let Some(guess) = hconf.guess_other_width.into_tri_flag() {
            match Self::guess_other_width(valid_buf, max_other) {
                Ok(w) => WarningsAndErrorsResult::new_ok(w),
                Err(e) => {
                    let w = hconf.other_width;
                    LogResult::new_switchable3(w, (), e, guess).switchable_into_commutative()
                }
            }
        } else {
            WarningsAndErrorsResult::new_ok(hconf.other_width)
        };

        width_res
            .map_errors(HeaderSegmentError::from)
            .and_then_commutative(|width| {
                let n_valid = valid_buf.len();
                let w = u8::from(width);
                let n_segs = n_valid / (usize::from(w) * 2);

                let mut results = vec![];

                let corrs = hconf
                    .other_corrections
                    .iter()
                    .copied()
                    .chain(repeat(OffsetCorrection::default()))
                    .take(hconf.max_other.map_or(n_segs, |x| x.min(n_segs)));

                for (i, corr) in corrs.enumerate() {
                    let seg_conf = NewSegmentConfig::from_read_config(corr, st);
                    let uw = usize::from(w);
                    let i0 = 2 * i * uw;
                    let i1 = ((2 * i) + 1) * uw;
                    let i2 = ((2 * i) + 2) * uw;
                    let buf0 = &buf[i0..i1];
                    let buf1 = &buf[i1..i2];

                    // If any regions are entirely blank or zero, just ignore them
                    if !buf0.iter().chain(buf1.iter()).all(|&x| x == 32 || x == 48) {
                        let r = Self::parse_other(buf0, buf1, &seg_conf);
                        results.push(r);
                    }
                }

                results
                    .into_iter()
                    .sequence_commutative()
                    .nowarn_into_warn()
                    .map_ok_value(|xs| Some((NonEmpty::from_vec(xs).unwrap(), width)))
            })
            .group()
            .map_error(IOErrorGroup::Pure)
    }

    fn parse_other(
        bs0: &[u8],
        bs1: &[u8],
        conf: &NewSegmentConfig<OtherSegmentId, SegmentFromHeader>,
    ) -> ErrorsResult<(Self, UncorrectedSegment), (), HeaderSegmentError> {
        let parse_one = |bs: &[u8], is_begin| {
            UintSpacePad20::from_bytes(bs).map_err(|error| {
                let src = StringOrBytes::from(bs.to_vec());
                ParseOffsetError::new(error, is_begin, OtherSegmentId::REGION, src).into()
            })
        };

        let begin_res = parse_one(bs0, true).into_nowarn();
        let end_res = parse_one(bs1, false).into_nowarn();
        begin_res
            .zip_commutative(end_res)
            .and_then_commutative(|(begin, end)| {
                let raw = UncorrectedSegment::new(begin, end);
                Self::try_new(begin, end, conf)
                    .map(|x| (x, raw))
                    .map_err(HeaderSegmentError::from)
                    .into_log()
            })
    }

    fn guess_other_width(
        xs: &[u8],
        max_other: Option<NonZeroU64>,
    ) -> Result<OtherWidth, GuessOtherWidthError> {
        const MIN_WIDTH: u8 = 8;
        let is_null = |x: u8| x == 0 || x == 32;
        debug_assert!(
            xs.iter().all(|x| is_null(*x) || (48..58).contains(x)),
            "stream must be all one of null, space, or a digit"
        );
        debug_assert!(!xs.is_empty(), "stream must be non-empty");

        // Indices where chars changed (false = null->digit, true = digit->null)
        let mut digit_starts: Vec<usize> = vec![];
        let mut digit_ends: Vec<usize> = vec![];

        // Iterate through all possible widths and test if the width is
        // compatible with the bytestring.
        let go = |w| {
            digit_starts.clear();
            digit_ends.clear();

            // Limit bytes if limit for maximum segment number is given.
            let these_bytes = if let Some(n) = max_other {
                let i = usize::try_from(u64::from(n)).expect("u64 overflow") * usize::from(w) * 2;
                &xs[0..i]
            } else {
                xs
            };

            // Get boundaries of "digit streams" which are contiguous streams of
            // digit characters separated by at least one space or null char.
            // The boundaries will be constructed as intervals like (start, end)
            // where start and end are the indices of the start and end of the
            // stream.
            let mut it = these_bytes.iter();
            let mut prev_was_null = is_null(*it.by_ref().next().unwrap());
            // If first char is digit, push start boundary to balance the ends
            if !prev_was_null {
                digit_starts.push(0);
            }
            for (&x, i) in it.zip(1..) {
                let this_is_null = is_null(x);
                if prev_was_null != this_is_null {
                    if this_is_null {
                        digit_ends.push(i);
                    } else {
                        digit_starts.push(i);
                    }
                }
                prev_was_null = this_is_null;
            }
            // If previous was a digit, add a boundary to the end
            if !prev_was_null {
                digit_ends.push(these_bytes.len());
            }
            let final_digit_position = digit_ends.iter().copied().last().unwrap_or_default();
            debug_assert!(digit_starts.len() == digit_ends.len(), "start != end");
            let digit_intervals: Vec<_> = digit_starts
                .iter()
                .copied()
                .zip(digit_ends.iter().copied())
                .collect();

            // Compute number of segments that fit into digits. Use the last
            // found digit as the end of the bytes to be considered. If segment
            // number is odd, this width is not valid since offsets come in
            // pairs.
            let ww = usize::from(w);
            let n_segs = final_digit_position / ww;
            if n_segs & 1 == 1 {
                return None;
            }

            // Match intervals of digits computed by positions of digit bytes
            // themselves with offset boundaries as defined by the width.
            //
            // Criteria for passing width
            // - the right position of all digit streams should correspond to
            //   an offset boundary
            // - all offset boundaries should be in a digit stream
            let mut seg_ends = (0..n_segs).map(|x| (x + 1) * ww);
            let mut cur_end = seg_ends.by_ref().next();
            for (a, b) in &digit_intervals {
                if let Some(s) = cur_end {
                    if &s == b {
                        // offset end and digit end are equal, this digit stream
                        // is satisfied
                        cur_end = seg_ends.by_ref().next();
                        continue;
                    } else if a < &s && &s < b {
                        // offset end is in digit stream, which is allowed but
                        // we still need to match the current digit stream's
                        // ending offset. Advance until we either find a match
                        // (pass) or we overshoot (fail)
                        while cur_end.is_some_and(|s0| &s0 < b) {
                            cur_end = seg_ends.by_ref().next();
                        }
                        if cur_end.is_some_and(|s0| &s0 == b) {
                            cur_end = seg_ends.by_ref().next();
                            continue;
                        }
                        return None;
                    }
                    // offset end is before the start of digit stream, invalid
                    return None;
                }
                // we ran out of segment ends, this digit stream is not
                // matched which is a fail
                return None;
            }
            Some(w)
        };
        let candidates = (MIN_WIDTH..MAX_CHARS).filter_map(go);

        // TODO for now we are assuming that checking digit boundaries is good
        // enough to figure out what the offset width should be. We could also
        // parse the offsets to check that the digits make sense, and also
        // check for overlaps. This is obviously much more complex. This
        // would only be necessary in the case of ties where multiple widths
        // are valid. In theory, ties are most likely for widths 8, 9, and 10
        // which could be mistaken instead of 16, 18, and 20 respectively. There
        // may be other edge cases as well.
        //
        // Example of a tie: '   11111   22222' could either be 1,1111 and
        // 2,2222 or 11111,22222 (width is 4 or 8 respectively)
        if let Some(ws) = NonEmpty::collect(candidates) {
            if ws.tail.is_empty() {
                Ok(OtherWidth::try_from(ws.head).unwrap())
            } else {
                Err(GuessOtherWidthError::MultiWidth(ws))
            }
        } else {
            Err(GuessOtherWidthError::NoWidth)
        }
    }
}

impl<I> TEXTSegment<I> {
    pub(crate) fn keywords(&self) -> [(String, String); 2]
    where
        I: KeyedReqSegment,
        I::B: Into<UintZeroPad20>
            + From<UintZeroPad20>
            + ReqMetarootKey
            + FromStr<Err = ParseIntError>
            + fmt::Display,
        I::E: Into<UintZeroPad20>
            + From<UintZeroPad20>
            + ReqMetarootKey
            + FromStr<Err = ParseIntError>
            + fmt::Display,
    {
        let i = self.inner;
        let (b, e) = match i {
            InnerSegment::Empty => (UintZeroPad20::zero(), UintZeroPad20::zero()),
            InnerSegment::NonEmpty(x) => (x.begin, x.end),
        };
        [
            ReqMetarootKey::pair(&I::B::from(b)),
            ReqMetarootKey::pair(&I::E::from(e)),
        ]
    }
}

impl<T> InnerSegment<T, DatasetOffset> {
    fn try_new<I: HasRegion, S: HasSource>(
        begin: i128,
        end: i128,
        conf: &NewSegmentConfig<I, S>,
    ) -> Result<Self, SegmentError>
    where
        T: TryFrom<i128>,
    {
        let corr = &conf.corr;
        let err = |kind| {
            let o = conf.dataset_offset;
            let c = (corr.begin, corr.end);
            SegmentError::new((begin, end), c, o, kind, I::REGION, S::SRC)
        };

        let new_begin = begin + i128::from(corr.begin);
        let new_end = end + i128::from(corr.end);

        if new_begin == new_end + 1 && conf.allow_pseudoempty.is_set() {
            // Check if this offset is pseudoempty
            // TODO possibly throw warning if this happens
            return Ok(Self::Empty);
        } else if new_begin == 0 && new_end == 0 {
            // Check if this offset if empty
            return Ok(Self::Empty);
        } else if new_begin > new_end {
            // Check if begin is greater than end
            return Err(err(SegmentErrorKind::Inverted));
        } else if new_begin < i128::from(HEADER_LEN) {
            // Check if segment overlaps with HEADER (sans OTHER segments).
            return Err(err(SegmentErrorKind::InHeader));
        }

        let dso = i128::from(conf.dataset_offset.0);
        let fl = i128::from(conf.file_len.0);
        debug_assert!(dso <= fl, "dataset offset exceeds file length");

        // put offset in absolute coordinates to check for
        // truncation
        let abs_begin = dso + new_begin;
        let abs_end = dso + new_end;

        let (b, e) = if let Some(overflow_end) = (abs_end + 1).checked_sub(fl) {
            // Check by how much the final offset exceeds EOF (if anything)
            let trunc_limit = i128::from(conf.truncate_offset_limit.0);
            if overflow_end > trunc_limit {
                // If the extra bytes are more than what is allowed, throw error
                // depending on if the beginning offset is also over EOF.
                let kind = if fl < abs_begin {
                    SegmentErrorKind::BeginEOF(conf.file_len)
                } else {
                    SegmentErrorKind::Truncated(conf.file_len)
                };
                return Err(err(kind));
            } else if fl < abs_begin {
                // If begin is also beyond the file length, return empty
                // segment. We can do this because this block only runs if the
                // ending offset is within the truncation limit, and the entire
                // segment is within the truncation limit is begin is also
                // beyond EOF.
                return Ok(Self::Empty);
            }
            // Otherwise, the segment is partially truncated, so adjust the
            // final offset. The maximum offset is one less the file length.
            let max_end = fl.saturating_sub(1);
            let trunc_end = abs_end.min(max_end);
            // Put the truncated ending offset back into relative coordinates.
            let rel_trunc_end = trunc_end
                .checked_sub(dso)
                .expect("truncated end is bigger than dataset offset");
            (new_begin, rel_trunc_end)
        } else {
            // If we make it to this block, we know that the segment is entirely
            // within the file. Don't bother with truncation at all.
            (new_begin, new_end)
        };

        match (T::try_from(b), T::try_from(e)) {
            (Ok(b0), Ok(e0)) => {
                let seg = NonEmptySegment::new(b0, e0, conf.dataset_offset);
                Ok(Self::NonEmpty(seg))
            }
            (_, _) => Err(err(SegmentErrorKind::Range)),
        }
    }
}

impl<T, O> InnerSegment<T, O> {
    fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    fn as_u64(&self) -> InnerSegment<u64, O>
    where
        T: Into<u64> + Copy,
        O: Copy,
    {
        match self {
            Self::Empty => InnerSegment::Empty,
            Self::NonEmpty(x) => InnerSegment::NonEmpty(x.as_u64()),
        }
    }

    fn try_as_nonempty(&self) -> Option<NonEmptySegment<T, O>>
    where
        T: Copy,
        O: Copy,
    {
        match self {
            Self::Empty => None,
            Self::NonEmpty(x) => Some(*x),
        }
    }

    /// Subtract n bytes off the end of this offset
    pub(crate) fn truncate(&mut self, n: u64)
    where
        T: TryFrom<u64> + Copy,
        T::Error: Debug,
        u64: From<T>,
    {
        if let Self::NonEmpty(x) = self {
            x.end = T::try_from(u64::from(x.end).saturating_sub(n))
                .expect("smaller T should convert from u64");
        }
    }
}

impl<T, O> NonEmptySegment<T, O> {
    /// Return the number of bytes in this segment
    fn nbytes(&self) -> NonZeroU64
    where
        T: Into<u64> + Copy,
    {
        NonZeroU64::MIN.saturating_add(self.end.into() - self.begin.into())
    }

    /// Return the first and last byte or this segment
    fn coords(&self) -> (T, T)
    where
        T: Copy,
    {
        (self.begin, self.end)
    }

    /// Return the next byte after this segment
    fn next_byte(&self) -> NonZeroU64
    where
        T: Into<u64> + Copy,
    {
        NonZeroU64::MIN.checked_add(self.end.into()).unwrap()
    }

    fn as_u64(&self) -> NonEmptySegment<u64, O>
    where
        T: Into<u64> + Copy,
        O: Copy,
    {
        NonEmptySegment::new(self.begin.into(), self.end.into(), self.dataset_offset)
    }
}

/// Error when parsing or creating required segment offsets from TEXT
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(B: Key), bound(E: Key))]
pub enum ReqSegmentError<B, E> {
    Key(ReqSegmentKeyError<B, E>),
    Segment(SegmentError),
}

/// Error when parsing required segment offsets from TEXT
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(B: Key), bound(E: Key))]
pub enum ReqSegmentKeyError<B, E> {
    Begin(ReqKeyErrorInner<ParseIntError, B, ()>),
    End(ReqKeyErrorInner<ParseIntError, E, ()>),
}

/// Error when parsing optional segment offsets from TEXT
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(B: Key), bound(E: Key))]
pub enum OptSegmentError<B, E> {
    Key(OptSegmentKeyError<B, E>),
    Segment(SegmentError),
}

/// Error when parsing or creating optional segment offsets from TEXT
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(B: Key), bound(E: Key))]
pub enum OptSegmentKeyError<B, E> {
    Begin(ParseKeyError<ParseIntError, B, ()>),
    End(ParseKeyError<ParseIntError, E, ()>),
}

/// Error when parsing a segment from HEADER
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum HeaderSegmentError {
    New(SegmentError),
    Parse(ParseOffsetError),
    SegmentBytes(OffsetsNoBytesError),
    OtherBytes(OtherOffsetsNoBytesError),
    Guess(GuessOtherWidthError),
}

/// Error when there are not enough bytes in file to read offsets
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
#[error(
    "needed {required} bytes to parse {location} offset from {src} at byte \
     {position}, only {remaining} bytes left in file"
)]
pub struct OffsetsNoBytesError {
    position: u64,
    remaining: u64,
    required: u64,
    location: AnyRegion,
    src: AnySrc,
}

/// Error when there are not enough bytes in file to read OTHER segments
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
#[error(
    "needed {required} bytes to parse OTHER offsets at byte 58
     only {remaining} bytes left in file"
)]
pub struct OtherOffsetsNoBytesError {
    remaining: u64,
    required: NonZeroU64,
}

/// Error when creating a new segment
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
pub struct SegmentError {
    coords: (i128, i128),
    correction: (i32, i32),
    dataset_offset: DatasetOffset,
    kind: SegmentErrorKind,
    location: AnyRegion,
    src: AnySrc,
}

#[derive(Debug)]
enum SegmentErrorKind {
    Range,
    Inverted,
    BeginEOF(FileLen),
    InHeader,
    Truncated(FileLen),
}

impl fmt::Display for SegmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let (x0, x1) = self.coords;
        let (c0, c1) = self.correction;
        let kind_text = match &self.kind {
            SegmentErrorKind::Range => "Offset out of range".into(),
            SegmentErrorKind::Inverted => "Begin after end".into(),
            SegmentErrorKind::BeginEOF(size) => format!("Begin exceeds file size ({size} bytes)"),
            SegmentErrorKind::InHeader => "Begins within HEADER (first 58 bytes)".into(),
            SegmentErrorKind::Truncated(size) => {
                format!("Segment exceeds file size ({size} bytes)")
            }
        };
        write!(
            f,
            "{kind_text} for {} segment from {}; \
             coords=({x0}, {x1}), correction=({c0}, {c1}), offset={}",
            self.location, self.src, self.dataset_offset
        )
    }
}

/// Error when one segment overlaps with another
#[derive(Debug, Error, new)]
#[error("{seg0} overlaps with {seg1}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
pub struct SegmentOverlapError {
    seg0: GenericSegment,
    seg1: GenericSegment,
}

/// Error when parsing the offset for a segment
#[derive(Debug, Error, new)]
#[error(
    "parse error for {which} offset in {location} segment from source '{src:?}': {error}",
    which = if self.is_begin { "begin" } else { "end" },
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
pub struct ParseOffsetError {
    error: ParseFixedUintError,
    is_begin: bool,
    location: AnyRegion,
    src: StringOrBytes,
}

/// Error when TEXT offsets are overridden using corresponding offsets from HEADER
#[derive(Debug, Error, Display)]
#[display(bound(I: HasRegion))]
#[display(
    "could not obtain {} segment offset from TEXT, using offsets from HEADER",
    I::REGION
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
#[cfg_attr(feature = "python", bound(I: HasRegion))]
pub struct SegmentDefaultWarning<I>(PhantomData<I>);

impl<I> Default for SegmentDefaultWarning<I> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

/// Error when segments from TEXT and HEADER do not match
#[derive(Debug, Error, Display, new)]
#[display(bound(I: HasRegion))]
#[display(
    "segments differ in HEADER {header} and TEXT {text} for {}{}",
    I::REGION,
    self.use_header.map_or("", |x| if x { ", using former" } else { ", using latter" })
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
#[cfg_attr(feature = "python", bound(I: HasRegion))]
pub struct SegmentMismatchError<I> {
    header: UncorrectedSegment,
    text: UncorrectedSegment,
    use_header: Option<bool>,
    _region: PhantomData<I>,
}

/// Error when parsing required segments from TEXT when HEADER is allowed to override
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(I: HasRegion), bound(B: Key), bound(E: Key))]
pub enum ReqSegmentWithDefaultErrorInner<I, B, E> {
    Req(ReqSegmentError<B, E>),
    Mismatch(SegmentMismatchError<I>),
    Validation(SegmentValidationError),
}

/// Warning when parsing required segments from TEXT when HEADER is allowed to override
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(I: HasRegion), bound(B: Key), bound(E: Key))]
pub enum ReqSegmentWithDefaultWarning_<I, B, E> {
    Error(ReqSegmentWithDefaultErrorInner<I, B, E>),
    Default(SegmentDefaultWarning<I>),
}

/// Warning when parsing optional segments from TEXT when HEADER is allowed to override
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(I: HasRegion), bound(B: Key), bound(E: Key))]
pub enum OptSegmentWithDefaultWarningInner<I, B, E> {
    Opt(OptSegmentError<B, E>),
    Mismatch(SegmentMismatchError<I>),
    Validation(SegmentValidationError),
}

// /// Error when segment with TEXT offsets overlaps with HEADER or another segment
// #[derive(From, Display, Debug, Error)]
// #[cfg_attr(feature = "python", derive(AllIntoPyErr))]
// #[cfg_attr(feature = "python", bound(I: HasRegion))]
// pub enum TEXTSegmentOverlapError<I> {
//     Header(TEXTSegmentInHeaderError<I>),
//     OtherSeg(SegmentOverlapError),
// }

// /// Error when segment from TEXT begins in HEADER
// #[derive(Debug, Display, Error, new)]
// #[display(bound(I: HasRegion))]
// #[display(
//     "begin offset of {} segment is {begin} which starts within \
//      HEADER which is {header_len} bytes long",
//     I::REGION
// )]
// #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
// #[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
// #[cfg_attr(feature = "python", bound(I: HasRegion))]
// pub struct TEXTSegmentInHeaderError<I> {
//     begin: u64,
//     header_len: u64,
//     _loc: PhantomData<I>,
// }

/// Error when segment with TEXT offsets overlaps with HEADER or another segment
#[derive(Debug, Error, PartialEq)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
pub enum GuessOtherWidthError {
    #[error("No width for OTHER offsets could be found.")]
    NoWidth,
    #[error("Multiple possible widths for OTHER offsets: {}", _0.iter().join(","))]
    MultiWidth(NonEmpty<u8>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn other_width_2x8() {
        let s = b"       0       0";
        assert_eq!(
            OtherSegment20::guess_other_width(s, None).map(u8::from),
            Ok(8)
        );
    }

    #[test]
    fn other_width_4x8() {
        let s = b"       0       0    2112   90125";
        assert_eq!(
            OtherSegment20::guess_other_width(s, None).map(u8::from),
            Ok(8)
        );
    }

    #[test]
    fn other_width_4x8_hidden() {
        let s = b"       010000000       1       2";
        assert_eq!(
            OtherSegment20::guess_other_width(s, None).map(u8::from),
            Ok(8)
        );
    }

    #[test]
    fn other_width_4x8_spaceballs() {
        // random space after than should be ignored
        let s = b"       0       0       0   12345              ";
        assert_eq!(
            OtherSegment20::guess_other_width(s, None).map(u8::from),
            Ok(8)
        );
    }

    #[test]
    fn other_width_uneven() {
        // 8 then 9
        let s = b"       0        0";
        assert!(OtherSegment20::guess_other_width(s, None).is_err());
    }

    #[test]
    fn other_width_nobound() {
        // this can either be 8 or 16
        let s = b"00000000000000000000000000000000";
        assert!(OtherSegment20::guess_other_width(s, None).is_err());
    }
}

#[cfg(feature = "serde")]
mod serialize {
    use super::InnerSegment;

    use serde::ser::{Serialize, SerializeStruct as _, Serializer};

    impl<T: Serialize, O: Serialize> Serialize for InnerSegment<T, O> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match self {
                Self::NonEmpty(s) => s.serialize(serializer),
                Self::Empty => {
                    let mut state = serializer.serialize_struct("EmptySegment", 2)?;
                    state.serialize_field("start", "0")?;
                    state.serialize_field("end", "0")?;
                    state.end()
                }
            }
        }
    }
}

#[cfg(feature = "python")]
mod python {
    use crate::config::DatasetOffset;
    use crate::python::ConfigError;

    use super::{InnerSegment, NonEmptySegment, Segment, UncorrectedSegment, Zero};

    use pyo3::prelude::*;
    use pyo3::types::PyTuple;

    // TODO this shouldn't be necessary. The only reason this is required for
    // the python interface is because the output classes which have segments
    // in them also have constructors for the sake of completion. These segments
    // can't be used anywhere so there is no point in validating them, but this
    // implies we should have yet another type just for "read-only output"
    // segments
    impl<'py, I, S, T> FromPyObject<'py> for Segment<I, S, T>
    where
        T: FromPyObject<'py> + Zero + Ord,
        u64: From<T>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (begin, end): (T, T) = ob.extract()?;
            let ret = if begin > end {
                // Use ConfigError because these offsets will be supplied to
                // functions which "configure" a reader to look in a certain
                // location for something (a stretch, but that's the closest we
                // have now)
                Err(ConfigError::new_err("offset begin is greater than end"))
            } else if begin == T::zero() && end == T::zero() {
                Ok(InnerSegment::Empty)
            } else {
                // NOTE use zero for offset since all segments from Python-land
                // will be consider relative to current dataset (ie just like
                // they are in an FCS file)
                let dso = DatasetOffset(0);
                let ret = InnerSegment::NonEmpty(NonEmptySegment::new(begin, end, dso));
                Ok(ret)
            };
            ret.map(Self::new)
        }
    }

    impl<'py, I, S, T> IntoPyObject<'py> for Segment<I, S, T>
    where
        T: Copy,
        u64: From<T>,
    {
        type Target = PyTuple;
        type Output = Bound<'py, <(u64, u64) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.as_u64()
                .try_coords()
                .map_or((0, 0), |(b, e, _)| (b, e))
                .into_pyobject(py)
        }
    }

    impl<'py> FromPyObject<'py> for UncorrectedSegment {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (begin, end): (i128, i128) = ob.extract()?;
            Ok(Self::new(begin, end))
        }
    }

    impl<'py> IntoPyObject<'py> for UncorrectedSegment {
        type Target = PyTuple;
        type Output = Bound<'py, <(i128, i128) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.begin, self.end).into_pyobject(py)
        }
    }
}
