//! Reading and writing offsets in an FCS file

use crate::config::{
    AllowHeaderTEXTOffsetMismatch, AllowMissingRequiredOffsets, AllowNegative,
    AllowOptionalDropping, ConfigFlag, DatasetOffset, FileLen, IgnoreTEXTAnalysisOffsets,
    IgnoreTEXTDataOffsets, ReadDataKeywordsConfig, ReadHeaderInnerConfig, ReadState,
    TruncateOffsets,
};
use crate::header::{HEADER_LEN, Version};
use crate::logging::{
    CommutativeResultIter as _, DeferredErrors, DeferredWarningsAndErrors, ErrorsResult,
    IOErrorGroup, LogResult, ResultExt as _, SwitchableErrorsResult, WarningsAndErrorsResult,
};
use crate::text::keywords::{Beginanalysis, Begindata, Beginstext, Endanalysis, Enddata, Endstext};
use crate::text::lookup::{
    OptMetarootKey, Optional, ParseKeyError, ReqKeyErrorInner, ReqMetarootKey,
};
use crate::validated::ascii_uint::{
    HeaderString, ParseFixedUintError, UintSpacePad8, UintSpacePad20, UintZeroPad20,
};
use crate::validated::keys::{Key, StdKeywords};

use type_families::ApplyOnce as _;

use derive_more::{AsRef, Display, From};
use derive_new::new;
use nonempty::NonEmpty;
use num_traits::identities::{One, Zero};
use num_traits::ops::checked::CheckedSub;
use thiserror::Error;

use std::any::type_name;
use std::fmt::{self, Debug};
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::iter::repeat;
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
pub type RelativeSegment<I> = OffsetSegment<I, SegmentFromHeader, u64, ()>;

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

/// A non-empty segment that still has regional/src data but is type-agnostic.
///
/// Useful for bulk operations on lots of segments at once that wouldn't work
/// if they segments were all different types.
#[derive(Clone, Debug, Display, new)]
#[display("segment for {region} from {src} with coords ({begin}, {end})")]
pub(crate) struct GenericSegment {
    pub(crate) begin: u64,
    pub(crate) end: u64,
    pub(crate) region: AnyRegion,
    pub(crate) src: AnySrc,
}

#[derive(Clone, Debug, Display)]
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
    truncate_offsets: TruncateOffsets,
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
    AnySegment<T>,
    (),
    ReqSegmentWithDefaultWarning<T>,
    ReqSegmentWithDefaultError<T>,
>;

pub(crate) type OptSegTentative<T> = DeferredWarningsAndErrors<
    AnySegment<T>,
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

/// Helper struct to bundle all but the DATA and ANALYSIS segments
#[derive(new, AsRef)]
pub struct NonDataSegments<'a> {
    pub(crate) text: PrimaryTextSegment,
    #[as_ref(HeaderDataSegment)]
    pub(crate) data: HeaderDataSegment,
    #[as_ref(HeaderAnalysisSegment)]
    pub(crate) analysis: HeaderAnalysisSegment,
    pub(crate) other: &'a [OtherSegment20],
    pub(crate) supp: Option<SupplementalTextSegment>,
}

impl NonDataSegments<'_> {
    /// Ensure this segment does not overlap with other segments.
    ///
    /// Specifically check that no other segment (except its analogue in HEADER
    /// if non-empty) overlaps with this one. Also ensure that that these
    /// segments don't overlap with HEADER itself.
    fn validate<I, O>(&self, s: &TEXTSegment<I>) -> DeferredErrors<(), TEXTSegmentOverlapError<I>>
    where
        I: HasRegion,
        O: HasRegion,
        Self: AsRef<HeaderSegment<O>>,
    {
        if let Some(this_seg) = s.try_as_generic() {
            let hdr_len = u64::try_from(self.other.len()).unwrap() + u64::from(HEADER_LEN);
            let in_hdr_err = (this_seg.begin < hdr_len)
                .then_some(TEXTSegmentInHeaderError::new(this_seg.begin, hdr_len))
                .map(TEXTSegmentOverlapError::from);
            let text = self.text.try_as_generic();
            let not_this_seg = self.as_ref().try_as_generic();
            let supp = self.supp.as_ref().and_then(Segment::try_as_generic);
            let es = self
                .other
                .iter()
                .map(Segment::try_as_generic)
                .chain([text, not_this_seg, supp])
                .flatten()
                .filter_map(|hdr_seg| hdr_seg.overlaps(&this_seg).err())
                .map(TEXTSegmentOverlapError::from)
                .chain(in_hdr_err);
            DeferredErrors::new_err_from_iter(es, ())
        } else {
            LogResult::new_ok(())
        }
    }
}

/// Operations to obtain optional segment from TEXT keywords
pub trait KeyedSegment: Sized + Copy {
    type B: Key + Into<UintZeroPad20> + FromStr<Err = ParseIntError>;
    type E: Key + Into<UintZeroPad20> + FromStr<Err = ParseIntError>;

    fn segment_conf<C>(st: &ReadState<C>) -> NewSegmentConfig<Self, SegmentFromTEXT>
    where
        C: AsRef<TEXTCorrection<Self>> + AsRef<TruncateOffsets>,
    {
        let correction: &TEXTCorrection<Self> = st.conf.as_ref();
        let truncate: &TruncateOffsets = st.conf.as_ref();
        NewSegmentConfig::new(*correction, st.file_len, st.dataset_offset, *truncate)
    }
}

/// Operations to obtain required segment from TEXT keywords
pub(crate) trait KeyedReqSegment: KeyedSegment + HasRegion
where
    Self::B: ReqMetarootKey,
    Self::E: ReqMetarootKey,
{
    #[allow(clippy::type_complexity)]
    fn with_req_pair<C>(
        pair: ReqPair<Self::B, Self::E>,
        st: &ReadState<C>,
    ) -> Result<
        Segment<Self, SegmentFromTEXT, UintZeroPad20>,
        (
            ReqSegmentError<Self::B, Self::E>,
            Option<ReqSegmentError<Self::B, Self::E>>,
        ),
    >
    where
        C: AsRef<TruncateOffsets> + AsRef<TEXTCorrection<Self>>,
    {
        match pair {
            (Ok(x0), Ok(x1)) => {
                let new_conf = Self::segment_conf(st);
                Segment::try_new(x0, x1, &new_conf)
                    .map_err(ReqSegmentError::Segment)
                    .map_err(|e| (e, None))
            }
            (Err(e), Ok(_)) => Err((ReqSegmentError::BeginKey(e), None)),
            (Ok(_), Err(e)) => Err((ReqSegmentError::EndKey(e), None)),
            (Err(e0), Err(e1)) => Err((
                ReqSegmentError::BeginKey(e0),
                Some(ReqSegmentError::EndKey(e1)),
            )),
        }
    }

    fn get_req_pair(kws: &StdKeywords) -> ReqPair<Self::B, Self::E> {
        let x0 = Self::B::get_metaroot_req(kws);
        let x1 = Self::E::get_metaroot_req(kws);
        (x0, x1)
    }

    fn remove_req_pair(kws: &mut StdKeywords) -> ReqPair<Self::B, Self::E> {
        let x0 = Self::B::remove_metaroot_req(kws);
        let x1 = Self::E::remove_metaroot_req(kws);
        (x0, x1)
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

    fn get_req_or<'a, C>(
        kws: &StdKeywords,
        segs: &NonDataSegments<'a>,
        st: &ReadState<C>,
    ) -> ReqSegResult<Self>
    where
        NonDataSegments<'a>: AsRef<HeaderSegment<Self>> + AsRef<HeaderSegment<Self::OtherDataId>>,
        C: AsRef<ReadDataKeywordsConfig>,
        ReadDataKeywordsConfig: AsRef<TEXTCorrection<Self>> + AsRef<Self::IgnoreFlag>,
    {
        let ignore_flag: &Self::IgnoreFlag = st.conf.as_ref().as_ref();
        if ignore_flag.is_set() {
            let default: &HeaderSegment<Self> = segs.as_ref();
            LogResult::new_ok(default.into_any())
        } else {
            let inner_st = st.as_innner_ref::<ReadDataKeywordsConfig>();
            Self::with_req_pair_default(Self::get_req_pair(kws), segs, &inner_st)
        }
    }

    fn remove_req_or<'a, C>(
        kws: &mut StdKeywords,
        segs: &NonDataSegments<'a>,
        st: &ReadState<C>,
    ) -> ReqSegResult<Self>
    where
        NonDataSegments<'a>: AsRef<HeaderSegment<Self>> + AsRef<HeaderSegment<Self::OtherDataId>>,
        C: AsRef<ReadDataKeywordsConfig>,
        ReadDataKeywordsConfig: AsRef<TEXTCorrection<Self>> + AsRef<Self::IgnoreFlag>,
    {
        // if we want to totally ignore the TEXT offsets, just blindly remove
        // them so we don't trigger any pseudostandard false positives later and
        // return the default segment
        let ignore_flag: &Self::IgnoreFlag = st.conf.as_ref().as_ref();
        if ignore_flag.is_set() {
            let _ = Self::remove_req_pair(kws);
            let default: &HeaderSegment<Self> = segs.as_ref();
            LogResult::new_ok(default.into_any())
        } else {
            let inner_st = st.as_innner_ref::<ReadDataKeywordsConfig>();
            Self::with_req_pair_default(Self::remove_req_pair(kws), segs, &inner_st)
        }
    }

    fn with_req_pair_default<'a, C>(
        pair: ReqPair<Self::B, Self::E>,
        segs: &NonDataSegments<'a>,
        st: &ReadState<C>,
    ) -> ReqSegResult<Self>
    where
        NonDataSegments<'a>: AsRef<HeaderSegment<Self>> + AsRef<HeaderSegment<Self::OtherDataId>>,
        C: AsRef<AllowHeaderTEXTOffsetMismatch>
            + AsRef<AllowMissingRequiredOffsets>
            + AsRef<TruncateOffsets>
            + AsRef<TEXTCorrection<Self>>,
    {
        let default: &HeaderSegment<Self> = segs.as_ref();
        let header_seg = default.into_any();
        let mismatch_flag: &AllowHeaderTEXTOffsetMismatch = st.conf.as_ref();
        let missing_flag: &AllowMissingRequiredOffsets = st.conf.as_ref();

        match Self::with_req_pair(pair, st) {
            Ok(text_seg) => {
                let val_res = segs
                    .validate::<_, Self::OtherDataId>(&text_seg)
                    .nowarn_into_switchable(*missing_flag)
                    .map_switchable_errors(ReqSegmentWithDefaultErrorInner::from)
                    .switchable_into_commutative();
                let (seg, warn) = default.unless(text_seg);
                let mismatch_res =
                    SwitchableErrorsResult::new_switchable_maybe(seg, (), warn, *mismatch_flag)
                        .map_switchable_errors(ReqSegmentWithDefaultErrorInner::from)
                        .switchable_into_commutative();
                val_res
                    .zip_commutative(mismatch_res)
                    .map_commutative_warnings(ReqSegmentWithDefaultWarning_::from)
                    .map_ok_value(|((), ret)| ret)
            }
            Err((e0, e1)) => {
                let mut res = SwitchableErrorsResult::new_switchable((), (), e0, *missing_flag)
                    .extend_deferred_switchable_errors(e1)
                    .map_switchable_errors(ReqSegmentWithDefaultErrorInner::from)
                    .switchable_into_commutative()
                    .map_commutative_warnings(ReqSegmentWithDefaultWarning_::from)
                    .set_ok_value(header_seg);
                let w = SegmentDefaultWarning::default().into();
                res.eval_warning(|_| Some(w));
                res
            }
        }
    }
}

/// Operations to obtain optional segment from TEXT keywords
pub(crate) trait KeyedOptSegment: KeyedSegment + HasRegion
where
    Self::B: OptMetarootKey + Optional<Outer = Option<Self::B>>,
    Self::E: OptMetarootKey + Optional<Outer = Option<Self::E>>,
{
    #[allow(clippy::type_complexity)]
    fn with_opt_pair<C>(
        pair: OptPair<Self::B, Self::E>,
        st: &ReadState<C>,
    ) -> Result<
        Option<Segment<Self, SegmentFromTEXT, UintZeroPad20>>,
        (
            OptSegmentError<Self::B, Self::E>,
            Option<OptSegmentError<Self::B, Self::E>>,
        ),
    >
    where
        C: AsRef<TruncateOffsets> + AsRef<TEXTCorrection<Self>>,
    {
        match pair {
            (Ok(x0), Ok(x1)) => {
                let new_conf = Self::segment_conf(st);
                x0.zip(x1)
                    .map(|(y0, y1)| Segment::try_new(y0, y1, &new_conf))
                    .transpose()
                    .map_err(OptSegmentError::Segment)
                    .map_err(|e| (e, None))
            }
            (Err(e), Ok(_)) => Err((OptSegmentError::BeginKey(e), None)),
            (Ok(_), Err(e)) => Err((OptSegmentError::EndKey(e), None)),
            (Err(e0), Err(e1)) => Err((
                OptSegmentError::BeginKey(e0),
                Some(OptSegmentError::EndKey(e1)),
            )),
        }
    }

    fn get_opt_pair(kws: &StdKeywords) -> OptPair<Self::B, Self::E> {
        let x0 = Self::B::get_root_opt(kws);
        let x1 = Self::E::get_root_opt(kws);
        (x0, x1)
    }

    fn remove_opt_pair(kws: &mut StdKeywords) -> OptPair<Self::B, Self::E> {
        let x0 = Self::B::remove_root_opt(kws);
        let x1 = Self::E::remove_root_opt(kws);
        (x0, x1)
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

    fn get_opt_or<'a, C>(
        kws: &StdKeywords,
        segs: &NonDataSegments<'a>,
        st: &ReadState<C>,
    ) -> OptSegTentative<Self>
    where
        NonDataSegments<'a>: AsRef<HeaderSegment<Self>> + AsRef<HeaderSegment<Self::OtherDataId>>,
        C: AsRef<ReadDataKeywordsConfig>,
        ReadDataKeywordsConfig: AsRef<TEXTCorrection<Self>> + AsRef<Self::IgnoreFlag>,
    {
        let ignore_flag: &Self::IgnoreFlag = st.conf.as_ref().as_ref();
        if ignore_flag.is_set() {
            let default: &HeaderSegment<Self> = segs.as_ref();
            LogResult::new_ok(default.into_any())
        } else {
            let inner_st = st.as_innner_ref::<ReadDataKeywordsConfig>();
            let pair = Self::get_opt_pair(kws);
            Self::with_opt_pair_default(pair, segs, &inner_st)
        }
    }

    fn remove_opt_or<'a, C>(
        kws: &mut StdKeywords,
        segs: &NonDataSegments<'a>,
        st: &ReadState<C>,
    ) -> OptSegTentative<Self>
    where
        NonDataSegments<'a>: AsRef<HeaderSegment<Self>> + AsRef<HeaderSegment<Self::OtherDataId>>,
        C: AsRef<ReadDataKeywordsConfig>,
        ReadDataKeywordsConfig: AsRef<TEXTCorrection<Self>> + AsRef<Self::IgnoreFlag>,
    {
        let ignore_flag: &Self::IgnoreFlag = st.conf.as_ref().as_ref();
        if ignore_flag.is_set() {
            let default: &HeaderSegment<Self> = segs.as_ref();
            let _ = Self::remove_opt_pair(kws);
            LogResult::new_ok(default.into_any())
        } else {
            let inner_st = st.as_innner_ref::<ReadDataKeywordsConfig>();
            let pair = Self::remove_opt_pair(kws);
            Self::with_opt_pair_default(pair, segs, &inner_st)
        }
    }

    fn with_opt_pair_default<'a, C>(
        pair: OptPair<Self::B, Self::E>,
        segs: &NonDataSegments<'a>,
        st: &ReadState<C>,
    ) -> OptSegTentative<Self>
    where
        NonDataSegments<'a>: AsRef<HeaderSegment<Self>> + AsRef<HeaderSegment<Self::OtherDataId>>,
        C: AsRef<AllowHeaderTEXTOffsetMismatch>
            + AsRef<TruncateOffsets>
            + AsRef<TEXTCorrection<Self>>,
    {
        let default: &HeaderSegment<Self> = segs.as_ref();
        let header_seg = default.into_any();
        // TODO configure this
        let drop_flag = AllowOptionalDropping(true);
        let mismatch_flag: &AllowHeaderTEXTOffsetMismatch = st.conf.as_ref();

        match Self::with_opt_pair(pair, st) {
            Ok(ts) => match ts {
                None => LogResult::new_ok(header_seg),
                Some(text_seg) => {
                    let val_res = segs
                        .validate::<_, Self::OtherDataId>(&text_seg)
                        .nowarn_into_switchable(drop_flag)
                        .map_switchable_errors(OptSegmentWithDefaultWarning::from)
                        .switchable_into_commutative();
                    let (seg, warn) = default.unless(text_seg);
                    let mismatch_res = SwitchableErrorsResult::new_deferred_switchable_maybe(
                        seg,
                        warn,
                        *mismatch_flag,
                    )
                    .map_switchable_errors(OptSegmentWithDefaultWarning::from)
                    .switchable_into_commutative();
                    val_res.lift_f2_once(mismatch_res, |(), ret| ret)
                }
            },
            Err((e0, e1)) => {
                SwitchableErrorsResult::new_deferred_switchable(header_seg, e0, drop_flag)
                    .extend_deferred_switchable_errors(e1)
                    .map_switchable_errors(OptSegmentError::from)
                    .map_switchable_errors(OptSegmentWithDefaultWarningInner::from)
                    .switchable_into_commutative()
            }
        }
    }
}

type ReqPair<B, E> = (
    Result<B, ReqKeyErrorInner<ParseIntError, B, ()>>,
    Result<E, ReqKeyErrorInner<ParseIntError, E, ()>>,
);

type OptPair<B, E> = (
    Result<Option<B>, ParseKeyError<ParseIntError, B, ()>>,
    Result<Option<E>, ParseKeyError<ParseIntError, E, ()>>,
);

/// Denotes that a type comes from a specific part of the FCS file
pub(crate) trait HasSource {
    const SRC: AnySrc;
}

/// Denotes that a type pertains to a region of the FCS file
pub(crate) trait HasRegion {
    const REGION: AnyRegion;
}

impl KeyedSegment for AnalysisSegmentId {
    type B = Beginanalysis;
    type E = Endanalysis;
}

impl KeyedReqSegment for AnalysisSegmentId {}

impl KeyedReqSegmentWithDefault for AnalysisSegmentId {
    type IgnoreFlag = IgnoreTEXTAnalysisOffsets;
    type OtherDataId = DataSegmentId;
}

impl KeyedOptSegment for AnalysisSegmentId {}

impl KeyedOptSegmentWithDefault for AnalysisSegmentId {
    type IgnoreFlag = IgnoreTEXTAnalysisOffsets;
    type OtherDataId = DataSegmentId;
}

impl KeyedSegment for DataSegmentId {
    type B = Begindata;
    type E = Enddata;
}

impl KeyedReqSegment for DataSegmentId {}

impl KeyedReqSegmentWithDefault for DataSegmentId {
    type IgnoreFlag = IgnoreTEXTDataOffsets;
    type OtherDataId = AnalysisSegmentId;
}

impl KeyedSegment for SupplementalTextSegmentId {
    type B = Beginstext;
    type E = Endstext;
}

impl KeyedReqSegment for SupplementalTextSegmentId {}

impl KeyedOptSegment for SupplementalTextSegmentId {}

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

impl<I> Default for RelativeSegment<I> {
    fn default() -> Self {
        Self::new(InnerSegment::Empty)
    }
}

impl<I, S, T> Default for Segment<I, S, T> {
    fn default() -> Self {
        Self::new(InnerSegment::Empty)
    }
}

impl<I, S, T> Segment<I, S, T> {
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

    fn fmt_pair(&self) -> String
    where
        T: Default + Copy + fmt::Display,
    {
        let (b, e) = self
            .try_coords()
            .map_or((T::default(), T::default()), |(a, b, _)| (a, b));
        format!("{b},{e}")
    }

    fn try_new(
        begin: impl Into<T>,
        end: impl Into<T>,
        conf: &NewSegmentConfig<I, S>,
    ) -> Result<Self, SegmentError>
    where
        I: HasRegion,
        S: HasSource,
        T: Zero
            + One
            + CheckedSub
            + Into<u64>
            + Into<i128>
            + TryFrom<i128>
            + Ord
            + Copy
            + TryFrom<u64>,
        u64: From<T>,
        <T as TryFrom<u64>>::Error: Debug,
    {
        InnerSegment::try_new::<I, S>(begin.into(), end.into(), conf).map(Self::new)
    }
}

impl<I> RelativeSegment<I> {
    #[cfg(feature = "python")]
    fn try_new_relative(begin: u64, end: u64) -> Result<Self, RelativeSegmentError>
    where
        I: HasRegion,
    {
        let ret = if begin > end {
            return Err(RelativeSegmentError(I::REGION));
        } else if begin == 0 && end == 0 {
            InnerSegment::Empty
        } else {
            InnerSegment::NonEmpty(NonEmptySegment::new(begin, end, ()))
        };
        Ok(Self::new(ret))
    }

    pub(crate) fn relative_to_abs<T>(
        self,
        dso: DatasetOffset,
        fl: FileLen,
    ) -> Result<Segment<I, SegmentFromHeader, T>, RelativeToAbsSegmentError>
    where
        I: HasRegion,
        T: TryFrom<u64>,
    {
        let ret = match self.inner {
            InnerSegment::Empty => InnerSegment::Empty,
            InnerSegment::NonEmpty(s) => {
                debug_assert!(s.begin <= s.end, "begin is not before end");
                if s.end + u64::from(dso) >= u64::from(fl) {
                    let b = s.begin;
                    let e = s.end;
                    return Err(RelativeFileLenError::new(I::REGION, b, e, dso, fl).into());
                }
                let err = RelativeToAbsSegmentError::Conversion(type_name::<T>());
                let b = s.begin.try_into().map_err(|_| err)?;
                let e = s.end.try_into().map_err(|_| err)?;
                let seg = NonEmptySegment::new(b, e, dso);
                InnerSegment::NonEmpty(seg)
            }
        };
        Ok(Segment::new(ret))
    }
}

impl GenericSegment {
    pub(crate) fn overlaps(&self, other: &Self) -> Result<(), SegmentOverlapError> {
        if (self.begin < other.begin && self.end < other.begin)
            || (other.begin < self.begin && other.end < self.begin)
        {
            Ok(())
        } else {
            Err(SegmentOverlapError {
                seg0: self.clone(),
                seg1: other.clone(),
            })
        }
    }

    // TODO add tests for this
    pub(crate) fn find_overlaps(mut xs: Vec<Self>) -> DeferredErrors<(), SegmentOverlapError> {
        xs.sort_by_key(|x| x.begin);
        if let Some(ys) = NonEmpty::from_vec(xs) {
            let mut prev = ys.head;
            let mut errors = vec![];
            // NOTE this won't find all overlaps since it won't check if a given
            // segment's end is after the beginning of segments 2 or more ahead,
            // but at least an error will be throw for all that are 1 away which
            // should be good enough to let the user fix the problem
            for z in ys.tail {
                if z.begin <= prev.end {
                    errors.push(SegmentOverlapError {
                        seg0: prev,
                        seg1: z.clone(),
                    });
                    prev = z;
                }
            }
            LogResult::new_err_from_iter(errors, ())
        } else {
            LogResult::new_ok(())
        }
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
    ) -> Result<Self, IOErrorGroup<HeaderSegmentError, ()>>
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderInnerConfig>,
        I: HasRegion + Copy,
    {
        let conf = st.conf.as_ref();
        let dso = st.dataset_offset;
        let seg_conf = NewSegmentConfig::new(corr, st.file_len, dso, conf.truncate_offsets);

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
            UintSpacePad8::from_bytes(bs, allow_blank, conf.allow_negative).map_err(|error| {
                ParseOffsetError::new(error, is_begin, I::REGION, bs.to_vec()).into()
            })
        };

        let begin_res = parse_one(buf0, true).into_nowarn();
        let end_res = parse_one(buf1, false).into_nowarn();
        begin_res
            .zip_commutative(end_res)
            .and_then_commutative(|(begin, end)| {
                // TEXT segment is not squishable
                let allow_squish = !is_text;
                let squish = conf.squish_offsets.is_set() && allow_squish;
                Self::try_new_squish(begin, end, squish, version, &seg_conf)
                    .map_err(HeaderSegmentError::from)
                    .into_log()
            })
            .group()
            .resolve_nowarn()
            .map_err(IOErrorGroup::Pure)
    }

    pub(crate) fn unless(
        self,
        other: TEXTSegment<I>,
    ) -> (AnySegment<I>, Option<SegmentMismatchWarning<I>>) {
        if other.inner.as_u64() != self.inner.as_u64() && !self.inner.is_empty() {
            let e = SegmentMismatchWarning {
                header: self,
                text: other,
            };
            (self.into_any(), Some(e))
        } else {
            (Segment::new(other.inner.as_u64()), None)
        }
    }

    pub(crate) fn into_any(self) -> AnySegment<I> {
        Segment::new(self.inner.as_u64())
    }

    fn try_new_squish(
        begin: UintSpacePad8,
        end: UintSpacePad8,
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
        let (b, e) = if version > Version::FCS2_0
            && squish_offsets
            && end == UintSpacePad8::zero()
            && begin > end
        {
            (UintSpacePad8::zero(), UintSpacePad8::zero())
        } else {
            (begin, end)
        };
        Self::try_new(b, e, conf)
    }
}

impl OtherSegment20 {
    pub(crate) fn h_read_others<C, R>(
        h: &mut BufReader<R>,
        text_begin: UintSpacePad8,
        st: &ReadState<C>,
    ) -> Result<Vec<Self>, IOErrorGroup<HeaderSegmentError, ()>>
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderInnerConfig>,
    {
        let conf = st.conf.as_ref();
        let n = u64::from(text_begin)
            .checked_sub(u64::from(HEADER_LEN))
            .expect("TEXT begin is less than 58");
        let w = u8::from(conf.other_width);
        let total_width = u64::from(w) * 2;
        let mut buf0 = vec![];
        let mut buf1 = vec![];
        let n_segs = usize::try_from(n / (u64::from(w) * 2)).expect("usize overflow");

        let mut results = vec![];

        let corrs = conf
            .other_corrections
            .iter()
            .copied()
            .chain(repeat(OffsetCorrection::default()))
            .take(conf.max_other.map_or(n_segs, |x| x.min(n_segs)));

        for corr in corrs {
            let seg_conf =
                NewSegmentConfig::new(corr, st.file_len, st.dataset_offset, conf.truncate_offsets);
            buf0.clear();
            buf1.clear();

            let remaining = st.remaining_bytes(h)?;

            if remaining < total_width {
                let pos = h.stream_position()?;
                let e = OffsetsNoBytesError::new(
                    pos,
                    remaining,
                    total_width,
                    AnyRegion::Other,
                    AnySrc::Header,
                );
                return Err(IOErrorGroup::new_pure_one(e.into()));
            }

            h.take(u64::from(w)).read_to_end(&mut buf0)?;
            h.take(u64::from(w)).read_to_end(&mut buf1)?;
            // If any regions are entirely blank, just ignore them
            if !buf0.iter().chain(buf1.iter()).all(|x| *x == 32) {
                let r = Self::parse_other(&buf0, &buf1, conf.allow_negative, &seg_conf);
                results.push(r);
            }
        }

        results
            .into_iter()
            .sequence_commutative()
            .group()
            .resolve_nowarn()
            .map_err(IOErrorGroup::Pure)
    }

    fn parse_other(
        bs0: &[u8],
        bs1: &[u8],
        allow_negative: AllowNegative,
        conf: &NewSegmentConfig<OtherSegmentId, SegmentFromHeader>,
    ) -> ErrorsResult<Self, (), HeaderSegmentError> {
        let parse_one = |bs: &[u8], is_begin| {
            UintSpacePad20::from_bytes(bs, allow_negative).map_err(|error| {
                ParseOffsetError::new(error, is_begin, OtherSegmentId::REGION, bs.to_vec()).into()
            })
        };

        let begin_res = parse_one(bs0, true).into_nowarn();
        let end_res = parse_one(bs1, false).into_nowarn();
        begin_res
            .zip_commutative(end_res)
            .and_then_commutative(|(begin, end)| {
                Self::try_new(begin, end, conf)
                    .map_err(HeaderSegmentError::from)
                    .into_log()
            })
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
        begin: T,
        end: T,
        conf: &NewSegmentConfig<I, S>,
    ) -> Result<Self, SegmentError>
    where
        T: Zero + One + CheckedSub + Into<i128> + TryFrom<i128> + Ord + Copy + TryFrom<u64>,
        u64: From<T>,
        <T as TryFrom<u64>>::Error: Debug,
    {
        let corr = &conf.corr;
        let x = Into::<i128>::into(begin) + i128::from(corr.begin);
        let y = Into::<i128>::into(end) + i128::from(corr.end);
        let err = |kind| {
            SegmentError::new(
                (u64::from(begin), u64::from(end)),
                (corr.begin, corr.end),
                conf.dataset_offset,
                kind,
                I::REGION,
                S::SRC,
            )
        };
        match (T::try_from(x), T::try_from(y)) {
            (Ok(new_begin), Ok(new_end)) => {
                if new_begin > new_end {
                    Err(err(SegmentErrorKind::Inverted))
                } else if new_begin == T::zero() && new_end == T::zero() {
                    Ok(Self::Empty)
                } else {
                    let dso = conf.dataset_offset.0;
                    let fl = conf.file_len.0;
                    debug_assert!(dso <= fl, "dataset offset exceeds file length");
                    // put offset in absolute coordinates to check for
                    // truncation
                    let abs_begin = dso + u64::from(new_begin);
                    let abs_end = dso + u64::from(new_end);
                    // the maximum coordinate the ending offset can have is
                    // one less the file length (since the end is the last byte
                    // of the offset rather than the next byte)
                    let max_end = fl.saturating_sub(1);
                    // begin should never exceed file length
                    if fl < abs_begin {
                        return Err(err(SegmentErrorKind::BeginEOF(conf.file_len)));
                    }
                    // end can only be greater then end if we allow it, in which
                    // case it must be truncated
                    if abs_end >= fl && !conf.truncate_offsets.is_set() {
                        return Err(err(SegmentErrorKind::Truncated(conf.file_len)));
                    }
                    let trunc_end = abs_end.min(max_end);
                    // put the (possibly truncated) ending offset back into
                    // relative coordinates.
                    let rel_trunc_end = T::try_from(trunc_end - dso)
                        .expect("could not convert absolute to relative offset");
                    let seg = NonEmptySegment::new(new_begin, rel_trunc_end, conf.dataset_offset);
                    Ok(Self::NonEmpty(seg))
                }
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

/// Error when parsing required segment offsets from TEXT
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(B: Key), bound(E: Key))]
pub enum ReqSegmentError<B, E> {
    BeginKey(ReqKeyErrorInner<ParseIntError, B, ()>),
    EndKey(ReqKeyErrorInner<ParseIntError, E, ()>),
    Segment(SegmentError),
}

/// Error when parsing optional segment offsets from TEXT
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(B: Key), bound(E: Key))]
pub enum OptSegmentError<B, E> {
    BeginKey(ParseKeyError<ParseIntError, B, ()>),
    EndKey(ParseKeyError<ParseIntError, E, ()>),
    Segment(SegmentError),
}

/// Error when parsing a segment from HEADER
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum HeaderSegmentError {
    New(SegmentError),
    Parse(ParseOffsetError),
    Bytes(OffsetsNoBytesError),
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

#[derive(From, Debug, Error, Clone, Copy)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub enum RelativeToAbsSegmentError {
    #[error("{0}")]
    Len(RelativeFileLenError),
    #[error("Could not convert u64 to {0}")]
    Conversion(&'static str),
}

/// Error when converting relative segment to absolute segment
#[derive(Debug, Error, Clone, Copy, new)]
#[error("{region} segment ({begin}, {end}) with offset {offset} exceeds length of file {len}")]
pub struct RelativeFileLenError {
    region: AnyRegion,
    begin: u64,
    end: u64,
    offset: DatasetOffset,
    len: FileLen,
}

/// Error when creating a new relative segment
#[derive(Debug, Error)]
#[error("Begin is after end for supplied {0} offset")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConfigError))]
pub struct RelativeSegmentError(AnyRegion);

/// Error when creating a new segment
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
pub struct SegmentError {
    coords: (u64, u64),
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
    // InHeader,
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
            // SegmentErrorKind::InHeader => "Begins within HEADER".into(),
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
#[derive(Debug, Error)]
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
    src: Vec<u8>,
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
#[derive(Debug, Error, Display)]
#[display(bound(I: HasRegion))]
#[display(
    "segments differ in HEADER ({}) and TEXT ({}) for {}, using TEXT",
    header.as_u64().fmt_pair(),
    text.as_u64().fmt_pair(),
    I::REGION,
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
#[cfg_attr(feature = "python", bound(I: HasRegion))]
pub struct SegmentMismatchWarning<I> {
    header: HeaderSegment<I>,
    text: TEXTSegment<I>,
}

/// Error when parsing required segments from TEXT when HEADER is allowed to override
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(I: HasRegion), bound(B: Key), bound(E: Key))]
pub enum ReqSegmentWithDefaultErrorInner<I, B, E> {
    Req(ReqSegmentError<B, E>),
    Mismatch(SegmentMismatchWarning<I>),
    Validation(TEXTSegmentOverlapError<I>),
}

/// Warning when parsing required segments from TEXT when HEADER is allowed to override
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(I: HasRegion), bound(B: Key), bound(E: Key))]
pub enum ReqSegmentWithDefaultWarning_<I, B, E> {
    Error(ReqSegmentWithDefaultErrorInner<I, B, E>),
    Lookup(SegmentDefaultWarning<I>),
}

/// Warning when parsing optional segments from TEXT when HEADER is allowed to override
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(I: HasRegion), bound(B: Key), bound(E: Key))]
pub enum OptSegmentWithDefaultWarningInner<I, B, E> {
    Opt(OptSegmentError<B, E>),
    Mismatch(SegmentMismatchWarning<I>),
    Validation(TEXTSegmentOverlapError<I>),
}

/// Error when segment with TEXT offsets overlaps with HEADER or another segment
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(I: HasRegion))]
pub enum TEXTSegmentOverlapError<I> {
    Header(TEXTSegmentInHeaderError<I>),
    OtherSeg(SegmentOverlapError),
}

/// Error when segment from TEXT begins in HEADER
#[derive(Debug, Display, Error, new)]
#[display(bound(I: HasRegion))]
#[display(
    "begin offset of {} segment is {begin} which starts within \
     HEADER which is {header_len} bytes long",
    I::REGION
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
#[cfg_attr(feature = "python", bound(I: HasRegion))]
pub struct TEXTSegmentInHeaderError<I> {
    begin: u64,
    header_len: u64,
    _loc: PhantomData<I>,
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

    use super::{HasRegion, InnerSegment, NonEmptySegment, RelativeSegment, Segment, Zero};

    use pyo3::prelude::*;
    use pyo3::types::PyTuple;

    // segments will be returned as tuples like (u32, u32) reflecting their
    // exact representation in an FCS file
    impl<'py, I: HasRegion> FromPyObject<'py> for RelativeSegment<I> {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (begin, end): (u64, u64) = ob.extract()?;
            Ok(Self::try_new_relative(begin, end)?)
        }
    }

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
}
