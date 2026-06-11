//! Reading and writing offsets in an FCS file

use crate::api::HeaderAndSuppOffsets;
use crate::config::{
    AllowPseudoempty, ConfigFlag, DatasetOffset, DatasetOverflowLimit, DummyTriFlag, FileLen,
    IgnoreTEXTAnalysisOffsets, IgnoreTEXTDataOffsets, ProcessOptionalFailure,
    ReadDataKeywordsConfig, ReadHeaderInnerConfig, ReadOffsetConfig, ReadState,
};
use crate::core::{MismatchedTEXTOffsetOrigin, TEXTOffsetsOrigin};
use crate::fixed_vec::OneOrTwo;
use crate::logging::{
    CommutativeResultIter as _, ErrorsResult, IOErrorGroup, LogResult, ResultExt as _,
    SwitchableErrorsResult, WarningsAndErrorsResult, WarningsAndIOGroupResult, io_to_log,
};
use crate::text::keywords::{
    Beginanalysis, Begindata, Beginstext, Endanalysis, Enddata, Endstext, Nextdata,
};
use crate::text::lookup::{
    MissingKeyError, OptMetarootKey, Optional, ParseKeyError, ReqKeyErrorInner, ReqMetarootKey,
};
use crate::validated::ascii_range::{MAX_CHARS, MIN_OTHER_WIDTH, OtherWidth};
use crate::validated::ascii_uint::{
    ParseFixedUintError, UintSpacePad8, UintSpacePad20, UintZeroPad20,
};
use crate::validated::header_offsets::{HEADER_LEN, TextToHeaderOrSuppOffsetsValidationError};
use crate::validated::keys::{
    AsStdKey as _, Key, NEStringOrBytes, SpecificKey, StdKeywords, TruncatedNEString,
};

use fireflow_types::config::ProcessKeywordFailure;
use fireflow_types::keywords::Version;
use fireflow_types::nonempty_string::NESliceExt as _;

use type_families::{
    BifunctorOnce, Functor as _, FunctorOnce as _, Sibling2, impl_functor_once, impl_kind1,
    impl_kind2,
};

use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty_collections::{
    IntoIteratorExt as _, NESlice, NEVec, NonEmptyArrayExt as _,
    iter::{NonEmptyIterator as _, once},
};
use thiserror::Error;

use std::fmt::{self, Debug};
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::iter::repeat;
use std::marker::PhantomData;
use std::mem;
use std::num::{NonZeroU64, NonZeroUsize, ParseIntError};
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
};

/// Denotes a correction for a segment offset pair
#[derive(Default, Clone, Copy, new)]
pub struct OffsetsCorrection<I, S> {
    begin: i32,
    end: i32,
    _id: PhantomData<I>,
    _src: PhantomData<S>,
}

/// An offset pair that corresponds to a specific byte sequence in the file.
#[derive(new, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[new(visibility = "")]
pub struct Offsets<I, S> {
    inner: InnerOffsets,
    _id: PhantomData<I>,
    _src: PhantomData<S>,
}

impl<I, S> Clone for Offsets<I, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<I, S> Copy for Offsets<I, S> {}

/// Segment offsets as read straight from the file with no corrections.
///
/// Useful for diagnostics.
#[derive(Clone, Copy, PartialEq, Display, Debug, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("({begin}, {end})")]
pub struct OriginalOffsets {
    pub begin: i128,
    pub end: i128,
}

/// A segment offset that exceeds the end of the dataset.
///
/// "The end" can be defined either by $NEXTDATA or EOF, whichever is lower.
#[derive(Clone, Copy, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct OffsetsOverflow<N, const IS_EOF: bool> {
    /// The offsets with a name.
    pub offsets: NamedOffsets<N>,
    /// Amount by which the end of the segment offsets exceeds EOF/$NEXTDATA.
    pub overflow: NonZeroU64,
}

pub type HeaderOffsetsEOFOverflow = OffsetsOverflow<HeaderOffsetsName, true>;
pub type SuppOffsetsEOFOverflow = OffsetsOverflow<SuppTextOffsetsName, true>;
pub type TextOffsetsEOFOverflow = OffsetsOverflow<TextOffsetsName, true>;

pub type HeaderOffsetsNextdataOverflow = OffsetsOverflow<HeaderOffsetsName, false>;
pub type TextOffsetsNextdataOverflow = OffsetsOverflow<TextOffsetsName, false>;
pub type SuppOffsetsNextdataOverflow = OffsetsOverflow<SuppTextOffsetsName, false>;

/// Two offsets from HEADER which overlap.
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct OversetsOverlap<N0, N1> {
    /// First offsets
    pub offsets0: NamedOffsets<N0>,
    /// Second offsets
    pub offsets1: NamedOffsets<N1>,
    /// Amount of overlap between two segments
    pub overlap: NonZeroU64,
}

impl_kind2!(pub OffsetOverlapFamily, OversetsOverlap);

impl<A, B> BifunctorOnce<A, B> for OversetsOverlap<A, B> {
    fn first_once<F: FnOnce(A) -> C, C>(self, f: F) -> Sibling2<Self, C, B> {
        OversetsOverlap::new(self.offsets0.fmap_once(f), self.offsets1, self.overlap)
    }

    fn second_once<F: FnOnce(B) -> C, C>(self, f: F) -> Sibling2<Self, A, C> {
        OversetsOverlap::new(self.offsets0, self.offsets1.fmap_once(f), self.overlap)
    }
}

pub type HeaderToHeaderOffsetsOverlap = OversetsOverlap<HeaderOffsetsName, HeaderOffsetsName>;
pub type TextToHeaderOffsetsOverlap = OversetsOverlap<TextOffsetsName, HeaderOffsetsName>;
pub type SuppToHeaderOffsetsOverlap = OversetsOverlap<SuppTextOffsetsName, HeaderOffsetsName>;
pub type TextToHeaderOrSuppOffsetsOverlap =
    OversetsOverlap<TextOffsetsName, HeaderOrSuppOffsetsName>;

/// Segment offsets which have a name to identify them
///
/// Used when processing and diagnosing overlaps.
///
/// Note that `begin` and `end` may or may not match the original values of
/// the offsets as read from the file because some segments can go through
/// multiple rounds of overlap corrections. These values reflect the state of
/// offsets immediately before the overlap correction.
#[derive(Clone, Copy, Debug, new, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct NamedOffsets<N> {
    name: N,
    begin: u64,
    length: NonZeroU64,
}

impl_kind1!(pub NamedOffsetsFamily, NamedOffsets);

impl_functor_once!(
    NamedOffsets,
    self,
    mut f,
    NamedOffsets::new(f(self.name), self.begin, self.length)
);

impl<N> NamedOffsets<N> {
    //     pub(crate) fn as_pair(&self) -> (u64, u64) {
    //         (self.begin, self.end())
    //     }

    pub(crate) fn end(&self) -> u64 {
        self.begin + self.length.get() - 1
    }

    //     pub(crate) fn get_tail_offset_overlap<N0>(
    //         &self,
    //         other: &NamedOffsets<N0>,
    //     ) -> Option<NonZeroU64> {
    //         NonZeroU64::new((self.end() + 1).saturating_sub(other.begin))
    //     }

    //     pub(crate) fn get_tail_nextdata_overlap(&self, n: Nextdata) -> Option<NonZeroU64> {
    //         let nn = u64::from(n.0);
    //         if nn == 0 {
    //             None
    //         } else {
    //             NonZeroU64::new((self.end() + 1).saturating_sub(nn))
    //         }
    //     }
}

/// Error when a non-empty offset pair occurs within the first 58 bytes of the file.
#[derive(Debug, Error, PartialEq, Clone, Display)]
#[display(
    "{} segment offsets ({}, {}) is within HEADER region",
    self.0.name,
    self.0.begin,
    self.0.end()
)]
#[display(bound(N: fmt::Display))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
#[cfg_attr(feature = "python", bound(N: fmt::Display))]
pub struct InHeaderError<N>(pub NamedOffsets<N>);

/// Error when segment offsets exceed $NEXTDATA.
#[derive(Debug, Error, new, PartialEq, Clone, Display)]
#[display(
    "{} segment offsets ({}, {}) exceeds $NEXTDATA ({})",
    self.offsets.name,
    self.offsets.begin,
    self.offsets.end(),
    u64::from(self.nextdata)
)]
#[display(bound(N: fmt::Display))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
#[cfg_attr(feature = "python", bound(N: fmt::Display))]
pub struct NextdataOffsetsError<N> {
    nextdata: Nextdata,
    offsets: NamedOffsets<N>,
}

#[derive(Clone, Copy, Debug, Display, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum HeaderOffsetsName {
    #[display("Primary TEXT")]
    Text,
    #[display("DATA")]
    Data,
    #[display("ANALYSIS")]
    Analysis,
    #[display("OTHER-{_0}")]
    Other(usize),
}

#[derive(Clone, Copy, Debug, Display, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum HeaderOrSuppOffsetsName {
    #[display("Primary TEXT")]
    PrimaryText,
    #[display("Supplemental TEXT")]
    SuppText,
    #[display("DATA")]
    Data,
    #[display("ANALYSIS")]
    Analysis,
    #[display("OTHER-{_0}")]
    Other(usize),
}

impl From<HeaderOffsetsName> for HeaderOrSuppOffsetsName {
    fn from(value: HeaderOffsetsName) -> Self {
        match value {
            HeaderOffsetsName::Analysis => Self::Analysis,
            HeaderOffsetsName::Text => Self::PrimaryText,
            HeaderOffsetsName::Data => Self::Data,
            HeaderOffsetsName::Other(i) => Self::Other(i),
        }
    }
}

impl From<SuppTextOffsetsName> for HeaderOrSuppOffsetsName {
    fn from(_: SuppTextOffsetsName) -> Self {
        Self::SuppText
    }
}

#[derive(Clone, Copy, Debug, Display, PartialEq)]
#[display("Supplemental TEXT")]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct SuppTextOffsetsName;

#[derive(Clone, Copy, Debug, Display, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum TextOffsetsName {
    #[display("DATA")]
    Data,
    #[display("ANALYSIS")]
    Analysis,
}

#[derive(Clone, Copy, Debug, Display, PartialEq)]
pub(crate) enum AnySrc {
    #[display("HEADER")]
    Header,
    #[display("TEXT")]
    Text,
}

#[derive(Clone, Copy, Debug, Display, PartialEq)]
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

/// Denotes [`Offsets`] came from HEADER
#[derive(Default, Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct OffsetsFromHeader;

/// Denotes [`Offsets`] came from TEXT
#[derive(Default, Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct OffsetsFromTEXT;

/// Denotes [`Offsets`] came from either TEXT or HEADER
#[derive(Clone, Copy, PartialEq)]
pub struct OffsetsFromAnywhere;

/// Denotes [`Offsets`] pertains to primary TEXT
#[derive(Default, Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct PrimaryTextSegmentId;

/// Denotes [`Offsets`] pertains to supplemental TEXT
#[derive(Default, Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct SupplementalTextSegmentId;

/// Denotes [`Offsets`] pertains to DATA
#[derive(Default, Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct DataSegmentId;

/// Denotes [`Offsets`] pertains to ANALYSIS
#[derive(Default, Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AnalysisSegmentId;

/// Denotes [`Offsets`] pertains to OTHER
#[derive(Default, Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct OtherSegmentId;

/// A [`Offsets`] pertains to OTHER with its index in the HEADER.
#[derive(Debug, Clone, Copy, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct IndexedOtherOffsets {
    pub index: usize,
    pub seg: OtherOffsets20,
}

/// Configuration for making a new [`Offsets`]
#[derive(new)]
pub struct NewOffsetsConfig<I, S> {
    corr: OffsetsCorrection<I, S>,
    file_len: FileLen,
    dataset_offset: DatasetOffset,
    allow_pseudoempty: AllowPseudoempty,
    truncate_offset_limit: DatasetOverflowLimit,
}

impl<I, S> NewOffsetsConfig<I, S> {
    fn from_read_config<C>(corr: OffsetsCorrection<I, S>, st: &ReadState<C>) -> Self
    where
        C: AsRef<ReadOffsetConfig>,
    {
        let oconf = st.conf.as_ref();
        Self::new(
            corr,
            st.file_len,
            st.dataset_offset,
            oconf.allow_pseudoempty,
            oconf.dataset_overflow_limit,
        )
    }
}

pub type PrimaryTextOffsets = Offsets<PrimaryTextSegmentId, OffsetsFromHeader>;
pub type SupplementalTextOffsets = Offsets<SupplementalTextSegmentId, OffsetsFromTEXT>;

type DataOffsets<S> = Offsets<DataSegmentId, S>;
pub type HeaderDataOffsets = DataOffsets<OffsetsFromHeader>;
pub type TEXTDataOffsets = DataOffsets<OffsetsFromTEXT>;

type AnalysisOffsets<S> = Offsets<AnalysisSegmentId, S>;
pub type HeaderAnalysisOffsets = AnalysisOffsets<OffsetsFromHeader>;
pub type TEXTAnalysisOffsets = AnalysisOffsets<OffsetsFromTEXT>;

pub type HeaderOffsets<I> = Offsets<I, OffsetsFromHeader>;
pub type TEXTOffsets<I> = Offsets<I, OffsetsFromTEXT>;
pub type AnyOffsets<I> = Offsets<I, OffsetsFromAnywhere>;

pub type HeaderCorrection<I> = OffsetsCorrection<I, OffsetsFromHeader>;
pub type TEXTCorrection<I> = OffsetsCorrection<I, OffsetsFromTEXT>;

pub type AnyDataOffsets = DataOffsets<OffsetsFromAnywhere>;
pub type AnyAnalysisOffsets = AnalysisOffsets<OffsetsFromAnywhere>;

pub type OtherOffsets20 = Offsets<OtherSegmentId, OffsetsFromHeader>;

pub(crate) type ReqSegResult<I> = WarningsAndErrorsResult<
    HeaderOrTextOffsets<I>,
    (),
    ReqOffsetsWithDefaultWarning<I>,
    ReqOffsetsWithDefaultError<I>,
>;

pub(crate) type OptSegRes<I> = WarningsAndErrorsResult<
    HeaderOrTextOffsets<I>,
    (),
    OptOffsetsWithDefaultWarning<I>,
    OptOffsetsWithDefaultWarning<I>,
>;

pub type ReqOffsetsWithDefaultWarning<T> =
    ReqOffsetsWithDefaultWarning_<T, <T as KeyedOffsets>::B, <T as KeyedOffsets>::E>;

pub type ReqOffsetsWithDefaultError<T> =
    ReqOffsetsWithDefaultErrorInner<T, <T as KeyedOffsets>::B, <T as KeyedOffsets>::E>;

pub type OptOffsetsWithDefaultWarning<T> =
    OptOffsetsWithDefaultWarningInner<T, <T as KeyedOffsets>::B, <T as KeyedOffsets>::E>;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
enum InnerOffsets {
    NonEmpty(NonEmptyOffsetsInner),
    #[default]
    Empty,
}

/// A mutable reference to offsets which are guaranteed to be non-empty.
///
/// This needs to wrap the entire struct since one of the operations this will
/// permit will be truncating offsets, possibly down to empty.
pub struct NonEmptyOffsets<I, S>(Offsets<I, S>);

/// A mutable reference to offsets which are guaranteed to be non-empty.
///
/// This needs to wrap the entire struct since one of the operations this will
/// permit will be truncating offsets, possibly down to empty.
pub struct NonEmptyOffsetsMut<'a, I, S>(&'a mut Offsets<I, S>);

pub(crate) type AnyNonEmptyDataOffsets<'a> =
    NonEmptyOffsetsMut<'a, DataSegmentId, OffsetsFromAnywhere>;

/// An offset as shown in an FCS file.
#[derive(Debug, Clone, Copy, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
struct NonEmptyOffsetsInner {
    /// First coordinate (zero indexed)
    begin: u64,

    /// Length of segment in bytes, possibly after truncation.
    length: NonZeroU64,

    /// The length of the original segment as written in the FCS file.
    ///
    /// This is only used to check if a truncation of a given length can be
    /// done. Truncation can be performed in multiple passes for different
    /// reasons, but it makes sense to have one global limit to how much a
    /// segment can be truncated. Keeping the original ending offset will allow
    /// us to track how much a segment has been truncated across multiple
    /// truncations.
    ///
    /// Note that only the second offset can be truncated, so we don't need to
    /// have one of these for the first offset.
    ///
    /// Also note, this technically will open an inconsistency for any case that
    /// allows offsets to be created from scratch (for instance, when reading
    /// TEXT and DATA separately and serially using separate functions). Since
    /// this involves passing offsets "outside" the read workflow, we can alter
    /// them however we want. This means we can "reset" this counter which will
    /// allow more truncation than otherwise. This is quite minor compared to
    /// the fact that the user can wholesale modify the offsets to their
    /// choosing (not that they should). This is mentioned here since the
    /// behavior is not necessarily expected and could be confusing.
    original_length: NonZeroU64,

    /// The absolute position of the segment in the FCS file.
    ///
    /// `begin` is relative to this number. This will be the sum of all
    /// $NEXTDATA values for all previous datasets relative to the dataset in
    /// which this segment belongs (which implies it will be zero for the first
    /// dataset)
    dataset_offset: DatasetOffset,
}

/// A valid ASCII char in an OTHER segment.
#[derive(Clone, Copy, PartialEq)]
enum CharType {
    /// Minus sign ('-')
    Minus,
    /// space or \0
    Null,
    /// Any ASCII digit 0-9
    Digit,
    /// Anything else
    Other,
}

impl From<u8> for CharType {
    fn from(value: u8) -> Self {
        if value == 0 || value == 32 {
            Self::Null
        } else if value == 45 {
            Self::Minus
        } else if (48..=57).contains(&value) {
            Self::Digit
        } else {
            Self::Other
        }
    }
}

impl CharType {
    fn is_digit_or_minus(self) -> bool {
        matches!(self, Self::Digit | Self::Minus)
    }
}

/// A type which has a beginning and end offset that defines a byte segment.
///
/// For a given pair X,Y, X is the offset of the first byte, and Y is the offset
/// immediately after the last byte.
pub(crate) trait IsOffsetPair {
    /// The first position
    fn begin(&self) -> u64;

    /// The second position
    fn end(&self) -> u64;

    /// First and second position
    fn slice_pair(&self) -> (u64, u64) {
        (self.begin(), self.end())
    }

    /// The length of the slice
    fn nbytes(&self) -> u64 {
        self.end() - self.begin()
    }

    /// Get the overlap between two offset pairs.
    ///
    /// Will panic if first `other` offset is less than first offset of `self`.
    fn tail_overlap_pair<P>(&self, other: &P) -> Option<NonZeroU64>
    where
        P: IsOffsetPair,
    {
        assert!(
            self.begin() <= other.begin(),
            "other pair must start after this pair"
        );
        NonZeroU64::new((self.end()).saturating_sub(other.begin()))
    }

    /// Get the overlap between this pair and another offset.
    ///
    /// Will panic if `other` offset is less than first offset of `self`.
    fn tail_overlap_offset(&self, other: u64) -> Option<NonZeroU64> {
        assert!(
            self.begin() <= other,
            "other ({other}) must start after this pair ({}, {})",
            self.begin(),
            self.end()
        );
        NonZeroU64::new((self.end()).saturating_sub(other))
    }
}

impl<I, S> IsOffsetPair for NonEmptyOffsets<I, S> {
    fn begin(&self) -> u64 {
        self.inner().begin()
    }

    fn end(&self) -> u64 {
        self.inner().end()
    }
}

impl<I, S> IsOffsetPair for NonEmptyOffsetsMut<'_, I, S> {
    fn begin(&self) -> u64 {
        self.inner().begin()
    }

    fn end(&self) -> u64 {
        self.inner().end()
    }
}

impl IsOffsetPair for NonEmptyOffsetsInner {
    fn begin(&self) -> u64 {
        self.begin
    }

    fn end(&self) -> u64 {
        self.begin + self.length.get()
    }
}

pub(crate) trait HasOffsetPair: Sized {
    fn final_offsets(segs: &HeaderAndSuppOffsets) -> HeaderOffsets<Self>;
    fn original_offsets(segs: &HeaderAndSuppOffsets) -> OriginalOffsets;

    fn offset_pair(segs: &HeaderAndSuppOffsets) -> (HeaderOffsets<Self>, OriginalOffsets) {
        (Self::final_offsets(segs), Self::original_offsets(segs))
    }
}

/// Segment offsets which can either be from HEADER or TEXT.
///
/// This only applies to ANALYSIS and DATA.
#[derive(Clone)]
pub(crate) enum HeaderOrTextOffsets<I> {
    /// Offsets are from HEADER.
    ///
    /// Include offsets and reason for choosing it.
    Header(HeaderOffsets<I>, ChoseHeaderReason),

    /// Offsets are from TEXT
    Text {
        seg: TEXTOffsets<I>,
        origin: MismatchedTEXTOffsetOrigin,
    },
}

/// Encodes the reason why offsets were taken from HEADER and not TEXT.
#[derive(Clone, Copy)]
pub(crate) enum ChoseHeaderReason {
    /// TEXT is empty (or possibly totally absent if optional)
    Empty,
    /// TEXT is ignored
    Ignored(Option<OriginalOffsets>),
    /// TEXT is required but could not be parsed.
    Unparsed,
    /// TEXT is required but was numerically malformed.
    Malformed(OriginalOffsets),
    /// TEXT matches HEADER, HEADER chosen arbitrarily
    Match,
    /// TEXT mismatches HEADER, HEADER chosen
    Mismatch(OriginalOffsets),
}

impl<I> HeaderOrTextOffsets<I> {
    pub(crate) fn into_any(self) -> (AnyOffsets<I>, TEXTOffsetsOrigin) {
        match self {
            Self::Header(seg, reason) => {
                let anyseg = seg.into_any();
                let origin = match reason {
                    ChoseHeaderReason::Empty => TEXTOffsetsOrigin::EmptyTEXT,
                    ChoseHeaderReason::Ignored(uncorr) => TEXTOffsetsOrigin::Ignored(uncorr),
                    ChoseHeaderReason::Unparsed => TEXTOffsetsOrigin::Unparsed,
                    ChoseHeaderReason::Malformed(uncorr) => TEXTOffsetsOrigin::Malformed(uncorr),
                    ChoseHeaderReason::Match => TEXTOffsetsOrigin::Match,
                    ChoseHeaderReason::Mismatch(uncorr) => {
                        TEXTOffsetsOrigin::MismatchHeader(uncorr)
                    }
                };
                (anyseg, origin)
            }
            Self::Text { seg, origin } => (seg.into_any(), TEXTOffsetsOrigin::MismatchTEXT(origin)),
        }
    }
}

/// Result from parsing a pair of strings into a segment offset pair
pub(crate) enum PairResult<T, E> {
    Valid(Offsets<T, OffsetsFromTEXT>, OriginalOffsets),
    Malformed(OriginalOffsets, SegmentOffsetError),
    Unparsed(OneOrTwo<E>),
}

/// Operations to obtain optional segment from TEXT keywords
pub trait KeyedOffsets: Sized + Copy {
    type B: Key + Into<UintZeroPad20> + FromStr<Err = ParseIntError>;
    type E: Key + Into<UintZeroPad20> + FromStr<Err = ParseIntError>;
}

/// Operations to obtain optional segment from TEXT keywords
pub(crate) trait KeyedSegmentInner: KeyedOffsets + HasRegion {
    #[allow(clippy::type_complexity)]
    #[allow(clippy::result_large_err)]
    fn pair_to_segment<C>(
        x0: i128,
        x1: i128,
        corr: TEXTCorrection<Self>,
        st: &ReadState<C>,
    ) -> (
        Result<Offsets<Self, OffsetsFromTEXT>, SegmentOffsetError>,
        OriginalOffsets,
    )
    where
        C: AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        let new_conf = NewOffsetsConfig::from_read_config(corr, st);
        let raw = OriginalOffsets::new(x0, x1);
        (Offsets::try_new(x0, x1, &new_conf), raw)
    }
}

macro_rules! lookup_req {
    ($kws:ident, $fun:ident) => {{
        let k = SpecificKey::default();
        match $kws.$fun(&k.as_std_key()) {
            Some(v) => v
                .parse::<i128>()
                .map_err(|e| ParseKeyError::new(e, k.into(), TruncatedNEString(v.to_owned())))
                .map_err(ReqKeyErrorInner::from),
            None => Err(ReqKeyErrorInner::from(MissingKeyError(k.into()))),
        }
    }};
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
    ) -> PairResult<Self, ReqSegmentKeyError<Self::B, Self::E>>
    where
        C: AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        match pair {
            Ok((x0, x1)) => {
                let (res, raw) = Self::pair_to_segment(x0, x1, corr, st);
                match res {
                    Ok(final_pair) => PairResult::Valid(final_pair, raw),
                    Err(e) => PairResult::Malformed(raw, e),
                }
            }
            Err(e) => PairResult::Unparsed(e),
        }
    }

    fn get_req_pair(kws: &StdKeywords) -> ReqPair<Self::B, Self::E> {
        let x0 = Self::get_req::<Self::B>(kws).map_err(ReqSegmentKeyError::Begin);
        let x1 = Self::get_req::<Self::E>(kws).map_err(ReqSegmentKeyError::End);
        OneOrTwo::from_results(x0, x1)
    }

    fn remove_req_pair(kws: &mut StdKeywords) -> ReqPair<Self::B, Self::E> {
        let x0 = Self::remove_req::<Self::B>(kws).map_err(ReqSegmentKeyError::Begin);
        let x1 = Self::remove_req::<Self::E>(kws).map_err(ReqSegmentKeyError::End);
        OneOrTwo::from_results(x0, x1)
    }

    fn get_req<K>(kws: &StdKeywords) -> Result<i128, ReqKeyErrorInner<ParseIntError, K, ()>>
    where
        K: Key,
    {
        lookup_req!(kws, get)
    }

    fn remove_req<K>(kws: &mut StdKeywords) -> Result<i128, ReqKeyErrorInner<ParseIntError, K, ()>>
    where
        K: Key,
    {
        lookup_req!(kws, remove)
    }
}

/// Operations to obtain required segment from TEXT keywords with a default segment
pub(crate) trait KeyedReqSegmentWithDefault
where
    Self: KeyedReqSegment + HasRegion + AreNamedOffsets<TextOffsetsName, Params = ()>,
    Self::B: ReqMetarootKey,
    Self::E: ReqMetarootKey,
{
    type IgnoreFlag: ConfigFlag;
    type OtherDataId: HasRegion;

    fn get_req_or<C>(
        kws: &StdKeywords,
        segs: &mut HeaderAndSuppOffsets,
        ignore: Self::IgnoreFlag,
        corr: TEXTCorrection<Self>,
        st: &ReadState<C>,
    ) -> ReqSegResult<Self>
    where
        Self: HasOffsetPair + IsDataOrAnalysis,
        Self::OtherDataId: HasOffsetPair,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        Self::with_req_pair_default(Self::get_req_pair(kws), segs, corr, ignore, st)
    }

    fn remove_req_or<C>(
        kws: &mut StdKeywords,
        segs: &mut HeaderAndSuppOffsets,
        ignore: Self::IgnoreFlag,
        corr: TEXTCorrection<Self>,
        st: &ReadState<C>,
    ) -> ReqSegResult<Self>
    where
        Self: HasOffsetPair + IsDataOrAnalysis,
        Self::OtherDataId: HasOffsetPair,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        Self::with_req_pair_default(Self::remove_req_pair(kws), segs, corr, ignore, st)
    }

    #[allow(clippy::too_many_lines)]
    fn with_req_pair_default<C>(
        pair: ReqPair<Self::B, Self::E>,
        segs: &mut HeaderAndSuppOffsets,
        corr: TEXTCorrection<Self>,
        ignore: Self::IgnoreFlag,
        st: &ReadState<C>,
    ) -> ReqSegResult<Self>
    where
        Self: HasOffsetPair + IsDataOrAnalysis,
        Self::OtherDataId: HasOffsetPair,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        let dconf: &ReadDataKeywordsConfig = st.conf.as_ref();
        let oconf: &ReadOffsetConfig = st.conf.as_ref();
        let (header_seg, uncorr_hdr) = Self::offset_pair(segs);
        let header_pair = |reason| HeaderOrTextOffsets::Header(header_seg, reason);
        let mismatch_flag = dconf.allow_header_text_offset_mismatch;
        let missing_flag = dconf.allow_missing_required_offsets;
        let overflow_limit = oconf.dataset_overflow_limit;
        let overlap_limit = oconf.overlap_correction_limit;

        let text_missing = |es: Vec<ReqOffsetsWithDefaultError<Self>>| {
            let hpair = header_pair(ChoseHeaderReason::Unparsed);
            let mut res = LogResult::new_switchable_iter3(hpair, (), es, missing_flag)
                .switchable_into_commutative()
                .map_commutative_warnings(ReqOffsetsWithDefaultWarning::from);
            let w = ReqOffsetsWithDefaultWarning::from(SegmentOffsetsDefaultWarning::default());
            res.eval_warning(|_| Some(w));
            res
        };

        let mut pair_to_text = |txt_orig: OriginalOffsets, mismatch_warn, header_is_empty| {
            // mismatch_warn and header_is_empty need to be independent because we
            // may or may not throw a warning if a mismatch actually happened
            let offsets_conf = NewOffsetsConfig::from_read_config(corr, st);
            let offsets_res = Offsets::try_new(txt_orig.begin, txt_orig.end, &offsets_conf)
                .map_err(ReqOffsetsError::Segment);
            match offsets_res {
                Ok(mut offsets) => {
                    let eof_overflow = offsets.as_nonempty().and_then(|x| x.eof_overflow(()));
                    let nd_res = segs
                        .nextdata
                        .map_or(Ok(None), |nd| {
                            nd.validate_text_offset(&mut offsets, overflow_limit)
                        })
                        .map_err(ReqOffsetsWithDefaultErrorInner::from)
                        .into_deferred_switchable3(missing_flag)
                        .switchable_into_commutative();
                    let val_res = segs
                        .validate_text_offsets(&mut offsets, overlap_limit)
                        .map_errors(ReqOffsetsWithDefaultErrorInner::from)
                        .nowarn_into_switchable3(missing_flag)
                        .switchable_into_commutative();
                    let mut res = nd_res
                        .zip_commutative(val_res)
                        .map_commutative_warnings(ReqOffsetsWithDefaultWarning::from)
                        .map_ok_value(|(nd_overflow, offset_overlaps)| {
                            let origin = MismatchedTEXTOffsetOrigin::new(
                                header_is_empty,
                                txt_orig,
                                offset_overlaps,
                                eof_overflow,
                                nd_overflow,
                            );
                            HeaderOrTextOffsets::Text {
                                seg: offsets,
                                origin,
                            }
                        });
                    res.extend_commutative_warnings(mismatch_warn);
                    res
                }
                Err(e) => text_missing(vec![ReqOffsetsWithDefaultErrorInner::from(e)]),
            }
        };

        let mut mismatch_choose = |uncorr_txt| {
            if header_seg.is_empty() {
                // HEADER is empty, ignore the mismatch and get TEXT offsets
                // without mismatch warning
                pair_to_text(uncorr_txt, None, false)
            } else if let Some((choose_header, do_warn)) = mismatch_flag.is_warning() {
                // Not an error, choose offset and optionally throw warning
                let e = OffsetsMismatchError::new(uncorr_hdr, uncorr_txt, Some(choose_header));
                let w = do_warn
                    .then_some(e)
                    .map(ReqOffsetsWithDefaultErrorInner::from)
                    .map(ReqOffsetsWithDefaultWarning::from);
                if choose_header {
                    // We choose HEADER, return it possibly with warning
                    let ws = w.into_iter().collect::<Vec<_>>();
                    LogResult::new_ok(header_pair(ChoseHeaderReason::Mismatch(uncorr_txt)))
                        .set_commutative_warnings(ws)
                } else {
                    // We choose TEXT, convert offsets to segment, validate, and
                    // possibly attach warning for mismatch
                    pair_to_text(uncorr_txt, w, true)
                }
            } else {
                // Error for mismatch, don't bother processing offsets
                let e = OffsetsMismatchError::new(uncorr_hdr, uncorr_txt, None);
                WarningsAndErrorsResult::new_err(e).map_errors(ReqOffsetsWithDefaultError::from)
            }
        };

        match pair {
            // TEXT offsets found, compare with HEADER
            Ok((x0, x1)) => {
                let uncorr_txt = OriginalOffsets::new(x0, x1);
                if ignore.is_set() {
                    // If ignore is set, return immediately with uncorrected offsets
                    LogResult::new_ok(header_pair(ChoseHeaderReason::Ignored(Some(uncorr_txt))))
                } else if uncorr_txt == uncorr_hdr {
                    // Uncorrected offsets are identical, not a mismatch.
                    LogResult::new_ok(header_pair(ChoseHeaderReason::Match))
                } else {
                    // Offsets not identical, choose one
                    mismatch_choose(uncorr_txt)
                }
            }
            // TEXT offsets not found, throw error or warning depending on
            // if we want to enforce required offsets
            Err(es) => {
                if ignore.is_set() {
                    // If ignore is set, bypass errors and return nothing
                    LogResult::new_ok(header_pair(ChoseHeaderReason::Ignored(None)))
                } else {
                    // Otherwise return all the errors to make user a better and
                    // more enlightened person
                    let es0 = es
                        .fmap(ReqOffsetsError::Key)
                        .fmap(ReqOffsetsWithDefaultErrorInner::from)
                        .into_iter()
                        .collect();
                    text_missing(es0)
                }
            }
        }
    }
}

macro_rules! lookup_opt {
    ($kws:ident, $fun:ident) => {{
        let k = SpecificKey::default();
        $kws.$fun(&k.as_std_key())
            .map(|v| {
                v.parse::<i128>()
                    .map_err(|e| ParseKeyError::new(e, k.into(), TruncatedNEString(v.to_owned())))
            })
            .transpose()
    }};
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
    ) -> Option<PairResult<Self, OptSegmentKeyError<Self::B, Self::E>>>
    where
        C: AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        match pair {
            Ok(None) => None,
            Ok(Some((x0, x1))) => {
                let (res, raw) = Self::pair_to_segment(x0, x1, corr, st);
                match res {
                    Ok(final_pair) => Some(PairResult::Valid(final_pair, raw)),
                    Err(e) => Some(PairResult::Malformed(raw, e)),
                }
            }
            Err(e) => Some(PairResult::Unparsed(e)),
        }
    }

    fn get_opt_pair(kws: &StdKeywords) -> OptPair<Self::B, Self::E> {
        let x0 = Self::get_opt::<Self::B>(kws).map_err(OptSegmentKeyError::Begin);
        let x1 = Self::get_opt::<Self::E>(kws).map_err(OptSegmentKeyError::End);
        OneOrTwo::from_results(x0, x1).map(|(x, y)| x.zip(y))
    }

    fn remove_opt_pair(kws: &mut StdKeywords) -> OptPair<Self::B, Self::E> {
        // TODO these should process optional keywords the same as everything else
        let x0 = Self::remove_opt::<Self::B>(kws).map_err(OptSegmentKeyError::Begin);
        let x1 = Self::remove_opt::<Self::E>(kws).map_err(OptSegmentKeyError::End);
        OneOrTwo::from_results(x0, x1).map(|(x, y)| x.zip(y))
    }

    fn get_opt<K>(kws: &StdKeywords) -> Result<Option<i128>, ParseKeyError<ParseIntError, K, ()>>
    where
        K: Key,
    {
        lookup_opt!(kws, get)
    }

    fn remove_opt<K>(
        kws: &mut StdKeywords,
    ) -> Result<Option<i128>, ParseKeyError<ParseIntError, K, ()>>
    where
        K: Key,
    {
        lookup_opt!(kws, remove)
    }
}

/// Operations to obtain optional segment from TEXT keywords with a default segment
pub(crate) trait KeyedOptSegmentWithDefault
where
    Self: KeyedOptSegment + HasRegion + AreNamedOffsets<TextOffsetsName, Params = ()>,
    Self::B: OptMetarootKey + Optional<Outer = Option<Self::B>>,
    Self::E: OptMetarootKey + Optional<Outer = Option<Self::E>>,
{
    type IgnoreFlag: ConfigFlag;
    type OtherDataId: HasRegion;

    fn get_opt_or<C>(
        kws: &StdKeywords,
        segs: &mut HeaderAndSuppOffsets,
        ignore: Self::IgnoreFlag,
        corr: TEXTCorrection<Self>,
        st: &ReadState<C>,
    ) -> OptSegRes<Self>
    where
        Self: HasOffsetPair + IsDataOrAnalysis,
        Self::OtherDataId: HasOffsetPair,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        let pair = Self::get_opt_pair(kws);
        Self::with_opt_pair_default(pair, segs, corr, ignore, st)
    }

    fn remove_opt_or<C>(
        kws: &mut StdKeywords,
        segs: &mut HeaderAndSuppOffsets,
        ignore: Self::IgnoreFlag,
        corr: TEXTCorrection<Self>,
        st: &ReadState<C>,
    ) -> OptSegRes<Self>
    where
        Self: HasOffsetPair + IsDataOrAnalysis,
        Self::OtherDataId: HasOffsetPair,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        let pair = Self::remove_opt_pair(kws);
        Self::with_opt_pair_default(pair, segs, corr, ignore, st)
    }

    fn with_opt_pair_default<C>(
        pair: OptPair<Self::B, Self::E>,
        segs: &mut HeaderAndSuppOffsets,
        corr: TEXTCorrection<Self>,
        ignore: Self::IgnoreFlag,
        st: &ReadState<C>,
    ) -> OptSegRes<Self>
    where
        Self: HasOffsetPair + IsDataOrAnalysis,
        Self::OtherDataId: HasOffsetPair,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadOffsetConfig>,
        i128: From<Self::B> + From<Self::E>,
        Self::B: Copy,
        Self::E: Copy,
    {
        let dconf: &ReadDataKeywordsConfig = st.conf.as_ref();
        let oconf: &ReadOffsetConfig = st.conf.as_ref();
        let (header_seg, uncorr_hdr) = Self::offset_pair(segs);
        let header_pair = |reason| HeaderOrTextOffsets::Header(header_seg, reason);
        // TODO configure this
        let drop_flag = ProcessOptionalFailure(ProcessKeywordFailure::DropWarn);
        let mismatch_flag = dconf.allow_header_text_offset_mismatch;
        let overflow_limit = oconf.dataset_overflow_limit;
        let overlap_limit = oconf.overlap_correction_limit;

        let mut pair_to_text = |txt_orig: OriginalOffsets, mismatch_warn, header_is_empty| {
            // mismatch_warn and header_is_empty need to be independent because we
            // may or may not throw a warning if a mismatch actually happened
            let offsets_conf = NewOffsetsConfig::from_read_config(corr, st);
            let offsets_res = Offsets::try_new(txt_orig.begin, txt_orig.end, &offsets_conf)
                .map_err(OptOffsetsError::Segment);
            match offsets_res {
                Ok(mut offsets) => {
                    let eof_overflow = offsets.as_nonempty().and_then(|x| x.eof_overflow(()));
                    let nd_res = segs
                        .nextdata
                        .map_or(Ok(None), |nd| {
                            nd.validate_text_offset(&mut offsets, overflow_limit)
                        })
                        .map_err(OptOffsetsWithDefaultWarning::from)
                        .into_deferred_switchable(drop_flag)
                        .switchable_into_commutative();
                    let val_res = segs
                        .validate_text_offsets(&mut offsets, overlap_limit)
                        .nowarn_into_switchable(drop_flag)
                        .map_switchable_errors(OptOffsetsWithDefaultWarning::from)
                        .switchable_into_commutative();
                    let mut res = nd_res.zip_commutative(val_res).map_ok_value(
                        |(nd_overflow, offset_overlaps)| {
                            let origin = MismatchedTEXTOffsetOrigin::new(
                                header_is_empty,
                                txt_orig,
                                offset_overlaps,
                                eof_overflow,
                                nd_overflow,
                            );
                            HeaderOrTextOffsets::Text {
                                seg: offsets,
                                origin,
                            }
                        },
                    );
                    res.extend_commutative_warnings(mismatch_warn);
                    res
                }
                Err(e) => SwitchableErrorsResult::new_deferred_switchable((), e, drop_flag)
                    .map_switchable_errors(OptOffsetsWithDefaultWarning::from)
                    .switchable_into_commutative()
                    .set_ok_value(header_pair(ChoseHeaderReason::Malformed(txt_orig))),
            }
        };

        let mut choose = |uncorr_txt| {
            if header_seg.is_empty() {
                // HEADER is empty, ignore the mismatch and get TEXT offsets
                // without mismatch warning
                pair_to_text(uncorr_txt, None, false)
            } else if let Some((choose_header, do_warn)) = mismatch_flag.is_warning() {
                // Not an error, figure out which segment we want
                let me = OffsetsMismatchError::new(uncorr_hdr, uncorr_txt, Some(choose_header));
                let w = do_warn
                    .then_some(me)
                    .map(OptOffsetsWithDefaultWarning::from);
                if choose_header {
                    // We choose HEADER, return it possibly with warning
                    let ws = w.into_iter().collect::<Vec<_>>();
                    let hpair = header_pair(ChoseHeaderReason::Mismatch(uncorr_txt));
                    LogResult::new_ok(hpair).set_commutative_warnings(ws)
                } else {
                    // We choose TEXT, create new TEXT segment from pairs,
                    // validate it, and possibly attach a warning
                    pair_to_text(uncorr_txt, w, true)
                }
            } else {
                // Error, don't bother with any segment processing
                let e = OffsetsMismatchError::new(uncorr_hdr, uncorr_txt, None);
                WarningsAndErrorsResult::new_err(e).map_errors(OptOffsetsWithDefaultWarning::from)
            }
        };

        match pair {
            // No TEXT segment found, but no errors either, just use HEADER
            Ok(None) => LogResult::new_ok(header_pair(ChoseHeaderReason::Empty)),
            // TEXT offsets found without errors, compare with HEADER
            Ok(Some((x0, x1))) => {
                let uncorr_txt = OriginalOffsets::new(x0, x1);
                if ignore.is_set() {
                    // Ignore is set, return uncorrected offsets immediately
                    LogResult::new_ok(header_pair(ChoseHeaderReason::Ignored(Some(uncorr_txt))))
                } else if uncorr_txt == uncorr_hdr {
                    // Uncorrected HEADER and TEXT are identical, just use HEADER
                    LogResult::new_ok(header_pair(ChoseHeaderReason::Match))
                } else {
                    // Segments are mismatched, figure out what to do
                    choose(uncorr_txt)
                }
            }
            // TEXT pairs found with errors, use HEADER
            Err(es) => {
                if ignore.is_set() {
                    // Ignore is set, bypass errors
                    LogResult::new_ok(header_pair(ChoseHeaderReason::Ignored(None)))
                } else {
                    // Otherwise throw lots of errors so user will have more
                    // information to contemplate their life's decisions.
                    let (e0, e1) = es.split();
                    let hpair = header_pair(ChoseHeaderReason::Unparsed);
                    SwitchableErrorsResult::new_deferred_switchable((), e0, drop_flag)
                        .extend_deferred_switchable_errors(e1)
                        .set_ok_value(hpair)
                        .map_switchable_errors(OptOffsetsError::Key)
                        .map_switchable_errors(OptOffsetsWithDefaultWarningInner::from)
                        .switchable_into_commutative()
                }
            }
        }
    }
}

type ReqPair<B, E> = Result<(i128, i128), OneOrTwo<ReqSegmentKeyError<B, E>>>;

type OptPair<B, E> = Result<Option<(i128, i128)>, OneOrTwo<OptSegmentKeyError<B, E>>>;

/// Denotes that a type comes from a specific part of the FCS file
pub(crate) trait HasSource {
    const SRC: AnySrc;
}

/// Denotes that a type pertains to a region of the FCS file
pub(crate) trait HasRegion {
    const REGION: AnyRegion;
}

/// A type which has a segment name.
pub(crate) trait AreNamedOffsets<N> {
    type Params;

    fn segname(args: Self::Params) -> N;
}

/// Denotes that a type pertains to a region of the FCS file
pub(crate) trait IsDataOrAnalysis {
    const IS_DATA: bool;
}

impl HasOffsetPair for DataSegmentId {
    fn final_offsets(segs: &HeaderAndSuppOffsets) -> HeaderOffsets<Self> {
        *AsRef::<HeaderOffsets<Self>>::as_ref(&segs.header.final_offsets)
    }

    fn original_offsets(segs: &HeaderAndSuppOffsets) -> OriginalOffsets {
        segs.header.original_offsets.data
    }
}

impl HasOffsetPair for AnalysisSegmentId {
    fn final_offsets(segs: &HeaderAndSuppOffsets) -> HeaderOffsets<Self> {
        *AsRef::<HeaderOffsets<Self>>::as_ref(&segs.header.final_offsets)
    }

    fn original_offsets(segs: &HeaderAndSuppOffsets) -> OriginalOffsets {
        segs.header.original_offsets.analysis
    }
}

impl KeyedOffsets for AnalysisSegmentId {
    type B = Beginanalysis;
    type E = Endanalysis;
}

impl KeyedOffsets for DataSegmentId {
    type B = Begindata;
    type E = Enddata;
}

impl KeyedOffsets for SupplementalTextSegmentId {
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

impl HasSource for OffsetsFromHeader {
    const SRC: AnySrc = AnySrc::Header;
}

impl HasSource for OffsetsFromTEXT {
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

impl AreNamedOffsets<HeaderOffsetsName> for PrimaryTextSegmentId {
    type Params = ();

    fn segname((): Self::Params) -> HeaderOffsetsName {
        HeaderOffsetsName::Text
    }
}

impl AreNamedOffsets<HeaderOffsetsName> for AnalysisSegmentId {
    type Params = ();

    fn segname((): Self::Params) -> HeaderOffsetsName {
        HeaderOffsetsName::Analysis
    }
}

impl AreNamedOffsets<HeaderOffsetsName> for DataSegmentId {
    type Params = ();

    fn segname((): Self::Params) -> HeaderOffsetsName {
        HeaderOffsetsName::Data
    }
}

impl AreNamedOffsets<HeaderOffsetsName> for OtherSegmentId {
    type Params = usize;

    fn segname(args: Self::Params) -> HeaderOffsetsName {
        HeaderOffsetsName::Other(args)
    }
}

impl AreNamedOffsets<SuppTextOffsetsName> for SupplementalTextSegmentId {
    type Params = ();

    fn segname((): Self::Params) -> SuppTextOffsetsName {
        SuppTextOffsetsName
    }
}

impl AreNamedOffsets<TextOffsetsName> for AnalysisSegmentId {
    type Params = ();

    fn segname((): Self::Params) -> TextOffsetsName {
        TextOffsetsName::Analysis
    }
}

impl AreNamedOffsets<TextOffsetsName> for DataSegmentId {
    type Params = ();

    fn segname((): Self::Params) -> TextOffsetsName {
        TextOffsetsName::Data
    }
}

impl IsDataOrAnalysis for AnalysisSegmentId {
    const IS_DATA: bool = false;
}

impl IsDataOrAnalysis for DataSegmentId {
    const IS_DATA: bool = true;
}

impl<I, S> From<(i32, i32)> for OffsetsCorrection<I, S> {
    fn from(value: (i32, i32)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl<I, S> From<(Option<i32>, Option<i32>)> for OffsetsCorrection<I, S> {
    fn from(value: (Option<i32>, Option<i32>)) -> Self {
        Self::from((value.0.unwrap_or_default(), value.1.unwrap_or_default()))
    }
}

impl<I, S> Default for Offsets<I, S> {
    fn default() -> Self {
        Self::new(InnerOffsets::Empty)
    }
}

impl<I, S> Offsets<I, S> {
    pub(crate) fn into_any(self) -> AnyOffsets<I> {
        Offsets::new(self.inner)
    }

    /// Return the first and last byte with offset or `0,0` if empty.
    #[cfg(feature = "python")]
    pub(crate) fn fcs_offset_pair(&self) -> (u64, u64) {
        if let InnerOffsets::NonEmpty(ne) = self.inner {
            ne.fcs_offset_pair()
        } else {
            (0, 0)
        }
    }

    /// Read bytes within this segment
    pub(crate) fn h_read_contents<R>(
        &self,
        h: &mut BufReader<R>,
        buf: &mut Vec<u8>,
    ) -> io::Result<()>
    where
        R: Read + Seek,
    {
        match self.inner {
            InnerOffsets::Empty => Ok(()),
            InnerOffsets::NonEmpty(s) => s.h_read_contents(h, buf),
        }
    }

    /// Return true if segment has 0 bytes
    pub(crate) fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Return the number of bytes in this segment
    pub(crate) fn nbytes(&self) -> u64 {
        // NOTE In FCS a 0,0 means "empty" but this also means one byte
        // according to the spec's own definitions. The first number points to
        // the first byte in a segment, and the second number points to the last
        // byte, therefore 0,0 means "0 is both the first and last byte, which
        // also means there is one byte".
        self.as_nonempty().map_or(0, |s| s.nbytes())
    }

    fn try_new(
        begin: i128,
        end: i128,
        conf: &NewOffsetsConfig<I, S>,
    ) -> Result<Self, SegmentOffsetError>
    where
        I: HasRegion,
        S: HasSource,
    {
        InnerOffsets::try_new::<I, S>(begin, end, conf).map(Self::new)
    }

    pub(crate) fn as_nonempty(&self) -> Option<NonEmptyOffsets<I, S>> {
        matches!(self.inner, InnerOffsets::NonEmpty(_)).then_some(NonEmptyOffsets(*self))
    }

    pub(crate) fn as_nonempty_mut(&mut self) -> Option<NonEmptyOffsetsMut<'_, I, S>> {
        matches!(self.inner, InnerOffsets::NonEmpty(_)).then_some(NonEmptyOffsetsMut(self))
    }
}

impl<I, S> NonEmptyOffsets<I, S> {
    fn inner(&self) -> &NonEmptyOffsetsInner {
        let InnerOffsets::NonEmpty(ne) = &self.0.inner else {
            panic!("offsets should always be non-empty in this struct")
        };
        ne
    }

    pub(crate) fn as_named<N>(&self, args: I::Params) -> NamedOffsets<N>
    where
        I: AreNamedOffsets<N>,
    {
        let inner = self.inner();
        NamedOffsets::new(I::segname(args), inner.begin, inner.length)
    }

    /// Project this non-empty offset pair to an overflow error if applicable.
    ///
    /// This will simply take the difference between the original length and the
    /// current length and convert it to an overflow error. It is only meant to
    /// be used for EOF overflow since this corresponds to when the offsets were
    /// first created and the file length was checked.
    pub(crate) fn eof_overflow<N>(&self, args: I::Params) -> Option<OffsetsOverflow<N, true>>
    where
        I: AreNamedOffsets<N>,
    {
        let n = self.as_named(args);
        let l = NonZeroU64::new(self.inner().truncated_len())?;
        Some(OffsetsOverflow::new(n, l))
    }
}

impl<I, S> NonEmptyOffsetsMut<'_, I, S> {
    pub(crate) fn begin_abs(&self) -> u64 {
        self.begin() + self.inner().dataset_offset.0
    }

    pub(crate) fn as_named<N>(&self, args: I::Params) -> NamedOffsets<N>
    where
        I: AreNamedOffsets<N>,
    {
        let inner = self.inner();
        NamedOffsets::new(I::segname(args), inner.begin, inner.length)
    }

    /// Return `true` if end offset can be truncated.
    ///
    /// Specifically, only return `true` if amount to be truncated + amount
    /// already truncated is less than/equal to the lesser of `limit` or the
    /// number of bytes of the offset pair.
    fn over_truncation_limit(&self, offset: u64, limit: u64) -> bool {
        if let Some(n) = self.tail_overlap_offset(offset) {
            let to_truncate = self.inner().truncated_len() + n.get();
            let trunc_limit = limit.min(self.inner().length.get());
            to_truncate > trunc_limit
        } else {
            false
        }
    }

    /// Filter items that cannot be truncated and truncate the first that can.
    ///
    /// Items are assumed to be offset-like object. The sequence of items must
    /// be sorted. `f_begin` is a function that must return the beginning offset
    /// of the item.
    #[allow(clippy::unused_peekable, reason = "false positive")]
    pub(crate) fn filter_and_truncate<F, X>(
        self,
        limit: u64,
        mut f_begin: F,
        xs: impl IntoIterator<Item = X>,
    ) -> (Vec<X>, Option<(X, NonZeroU64)>)
    where
        F: FnMut(&X) -> u64,
    {
        let mut it = xs.into_iter().peekable();
        // Automatically convert any offset pair that cannot be truncated
        // because they exceed the allowed limit.
        let exceed_limit = it
            .peeking_take_while(|x| self.over_truncation_limit(f_begin(x), limit))
            .collect();
        // If there is one more offset, truncate it if necessary. We just
        // removed all the prior offsets which cannot be truncated, so
        // truncation in this case should not fail if it is needed. Regardless,
        // this is the last offset pair we need to consider because truncation
        // will decrease the end of `self` such that the remaining pairs no
        // longer overlap, or they don't overlap to begin with. Either way, no
        // more to do.
        let last_res = if let Some(x) = it.next() {
            match self.tail_overlap_offset_and_truncate(f_begin(&x), limit) {
                // If no overlaps, we can assume there are no more overlaps
                // since the HEADER offsets are sorted. Break early.
                TruncateOffsetResult::NoOverlap(_) => None,
                // If overlap within limit and we have not encountered an
                // error yet, truncate TEXT and return early without error.
                // Otherwise push error.
                TruncateOffsetResult::Truncated(overlap) => Some((x, overlap)),
                TruncateOffsetResult::LimitExceeded(_, _) => {
                    panic!("offset should be truncatable")
                }
            }
        } else {
            None
        };
        (exceed_limit, last_res)
    }

    pub(crate) fn tail_overlap_pair_and_truncate<P>(
        self,
        other: &P,
        limit: u64,
    ) -> TruncateOffsetResult<Self>
    where
        P: IsOffsetPair,
    {
        self.tail_overlap_offset_and_truncate(other.begin(), limit)
    }

    pub(crate) fn tail_overlap_offset_and_truncate(
        self,
        other: u64,
        limit: u64,
    ) -> TruncateOffsetResult<Self> {
        if let Some(overlap) = self.tail_overlap_offset(other) {
            match self.truncate(overlap.get(), limit) {
                Ok(_) => TruncateOffsetResult::Truncated(overlap),
                Err(old) => TruncateOffsetResult::LimitExceeded(overlap, old),
            }
        } else {
            TruncateOffsetResult::NoOverlap(self)
        }
    }

    /// Subtract n bytes off the end of this offset
    ///
    /// Ensure that the truncated length won't be more than `limit`.
    ///
    /// Return new length if successful, otherwise `self` unchanged.
    pub(crate) fn truncate(self, n: u64, limit: u64) -> Result<u64, Self> {
        let InnerOffsets::NonEmpty(mut ne) = mem::take(&mut self.0.inner) else {
            panic!("offsets should always be non-empty in this struct")
        };
        let to_truncate = (ne.truncated_len() + n).min(ne.length.get());
        if to_truncate <= limit
            && let Some(diff) = ne.length.get().checked_sub(n)
        {
            let new_length = if let Some(new_length) = NonZeroU64::new(diff) {
                // Amount of truncation is within limit and new length is > 0,
                // set new length
                ne.length = new_length;
                self.0.inner = InnerOffsets::NonEmpty(ne);
                new_length.get()
            } else {
                // Entire length was truncated and offset is now empty
                0
            };
            return Ok(new_length);
        }
        // Amount to truncate is larger than offset pair length or exceeds
        // allowed limit, return unchanged
        self.0.inner = InnerOffsets::NonEmpty(ne);
        Err(self)
    }

    fn inner(&self) -> &NonEmptyOffsetsInner {
        let InnerOffsets::NonEmpty(ne) = &self.0.inner else {
            panic!("offsets should always be non-empty in this struct")
        };
        ne
    }
}

pub(crate) enum TruncateOffsetResult<T> {
    NoOverlap(T),
    Truncated(NonZeroU64),
    LimitExceeded(NonZeroU64, T),
}

impl_kind1!(pub(crate) TruncateOffsetResultFamily, TruncateOffsetResult);

impl_functor_once!(
    TruncateOffsetResult,
    self,
    mut f,
    match self {
        Self::NoOverlap(x) => TruncateOffsetResult::NoOverlap(f(x)),
        Self::Truncated(x) => TruncateOffsetResult::Truncated(x),
        Self::LimitExceeded(x, y) => TruncateOffsetResult::LimitExceeded(x, f(y)),
    }
);

impl<I: Copy> HeaderOffsets<I> {
    pub(crate) fn h_read_primary<C, R>(
        h: &mut BufReader<R>,
        is_text: bool,
        corr: HeaderCorrection<I>,
        version: Version,
        st: &ReadState<C>,
    ) -> Result<(Self, OriginalOffsets), IOErrorGroup<HeaderSegmentError, ()>>
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderInnerConfig> + AsRef<ReadOffsetConfig>,
        I: HasRegion + Copy,
    {
        let hconf: &ReadHeaderInnerConfig = st.conf.as_ref();
        let seg_conf = NewOffsetsConfig::from_read_config(corr, st);

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
                let src = NEStringOrBytes::from(bs.into_nonempty_vec());
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
                let raw = OriginalOffsets::new(begin, end);
                Self::try_new_squish(begin, end, squish, version, &seg_conf)
                    .map(|x| (x, raw))
                    .map_err(HeaderSegmentError::from)
                    .into_log()
            })
            .group()
            .resolve_nowarn()
            .map_err(IOErrorGroup::Pure)
    }

    fn try_new_squish(
        begin: i128,
        end: i128,
        squish_offsets: bool,
        version: Version,
        conf: &NewOffsetsConfig<I, OffsetsFromHeader>,
    ) -> Result<Self, SegmentOffsetError>
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

impl OtherOffsets20 {
    #[allow(clippy::type_complexity)]
    pub(crate) fn h_read_others<C, R>(
        h: &mut BufReader<R>,
        first_seg_begin: u64,
        st: &ReadState<C>,
    ) -> WarningsAndIOGroupResult<
        Option<(NEVec<(IndexedOtherOffsets, OriginalOffsets)>, OtherWidth)>,
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
        let Ok(max_other_len): Result<NonZeroU64, _> = first_seg_begin
            .checked_sub(u64::from(HEADER_LEN))
            .expect("minimal offset greater than 58")
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
        // between offset 58 and the next *required* offset (ie from
        // TEXT/DATA/ANALYSIS in HEADER). In %99.9999 of case, the first segment
        // will be one of these three. However, it is theoretically possible
        // that this region has both the OTHER offsets and the OTHER segments
        // themselves. This is technically standards compliant since OTHER
        // segments only need to be within the first 99,999,999 bytes as of 3.2
        // (in earlier versions this was even less restricted since they did not
        // specify a width). In these cases, reading bytes like this will
        // result in the OTHER segments themselves being read twice (here they
        // will be read and ignored).
        let mut buf = vec![];
        io_to_log!(h.take(u64::from(max_other_len)).read_to_end(&mut buf));

        // Only consider bytes which are spaces, nulls, minus sign, or digits
        // where a minus sign must always immediately precede a digit
        let mut n_valid_bytes = 0;
        let mut prev_was_minus = false;
        for &c in &buf {
            let t = CharType::from(c);
            if prev_was_minus && t != CharType::Digit {
                // Char is not a digit following a minus sign, decrement by one
                // byte before breaking loop since the previous minus sign does
                // not go with a digit. ASSUME this will not underflow because
                // the previous char cannot be minus on the first iteration.
                n_valid_bytes -= 1;
                break;
            }
            match t {
                CharType::Minus => prev_was_minus = true,
                CharType::Null | CharType::Digit => prev_was_minus = false,
                CharType::Other => break,
            }
            n_valid_bytes += 1;
        }

        // Exit early if there are no valid chars or all bytes are null or space
        let valid_buf = if let Some(ne) = NESlice::try_from_slice(&buf[0..n_valid_bytes])
            && !(ne.iter().all(|&x| x == 0) | ne.iter().all(|&x| x == 32))
        {
            ne
        } else {
            return LogResult::new_ok(None);
        };

        // Guess offset width if desired.
        let guess_maybe = DummyTriFlag::from_guess_other_width(hconf.guess_other_width);
        let width_res = if let Some(guess) = guess_maybe {
            match Self::guess_other_width(&valid_buf, max_other) {
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
                let corrs = hconf
                    .other_corrections
                    .iter()
                    .copied()
                    .chain(repeat(OffsetsCorrection::default()));
                let limit = hconf.max_other.unwrap_or(usize::MAX);
                valid_buf
                    .nonempty_chunks(width.into())
                    .into_iter()
                    .tuples()
                    .zip(corrs)
                    .take(limit)
                    .enumerate()
                    .filter_map(|(i, ((buf0, buf1), corr))| {
                        let seg_conf = NewOffsetsConfig::from_read_config(corr, st);
                        let all_are = |c| buf0.iter().chain(buf1.iter()).all(|&x| x == c);
                        (!(all_are(0) || all_are(32) || all_are(48))).then(|| {
                            Self::parse_other(&buf0, &buf1, &seg_conf)
                                .map_ok_value(|(s, d)| (IndexedOtherOffsets::new(i, s), d))
                        })
                    })
                    .sequence_commutative()
                    .nowarn_into_warn()
                    .map_ok_value(|xs| NEVec::try_from_vec(xs).map(|ys| (ys, width)))
            })
            .group()
            .map_error(IOErrorGroup::Pure)
    }

    fn parse_other(
        bs0: &NESlice<'_, u8>,
        bs1: &NESlice<'_, u8>,
        conf: &NewOffsetsConfig<OtherSegmentId, OffsetsFromHeader>,
    ) -> ErrorsResult<(Self, OriginalOffsets), (), HeaderSegmentError> {
        let parse_one = |bs: &NESlice<'_, u8>, is_begin| {
            UintSpacePad20::from_bytes(bs.as_ref()).map_err(|error| {
                let src = NEStringOrBytes::from(bs.to_ne_vec());
                ParseOffsetError::new(error, is_begin, OtherSegmentId::REGION, src).into()
            })
        };

        let begin_res = parse_one(bs0, true).into_nowarn();
        let end_res = parse_one(bs1, false).into_nowarn();
        begin_res
            .zip_commutative(end_res)
            .and_then_commutative(|(begin, end)| {
                let raw = OriginalOffsets::new(begin, end);
                Self::try_new(begin, end, conf)
                    .map(|x| (x, raw))
                    .map_err(HeaderSegmentError::from)
                    .into_log()
            })
    }

    #[allow(clippy::too_many_lines)]
    fn guess_other_width(
        xs: &NESlice<'_, u8>,
        max_other: Option<NonZeroU64>,
    ) -> Result<OtherWidth, GuessOtherWidthError> {
        #[cfg(debug_assertions)]
        {
            let cs: NEVec<_> = xs.nonempty_iter().copied().map(CharType::from).collect();
            assert!(
                cs.iter().all(|&x| x != CharType::Other),
                "stream must be all one of null, space, minus sign, or a digit"
            );
            assert!(
                !cs.iter()
                    .tuple_windows()
                    .any(|(&prev, &this)| prev == CharType::Minus && this != CharType::Digit),
                "stream has minus sign which is not followed by digit"
            );
            assert!(cs.last() != &CharType::Minus, "stream ends with minus sign");
        }

        // Indices where chars changed (false = null->digit, true = digit->null)
        let mut digit_starts: Vec<usize> = vec![];
        let mut digit_ends: Vec<usize> = vec![];

        // Iterate through all possible widths and test if the width is
        // compatible with the bytestring.
        let mut go = |w: OtherWidth| {
            digit_starts.clear();
            digit_ends.clear();

            // Limit bytes if limit for maximum segment number is given.
            let total_bytes = if let Some(n) = max_other {
                const N: NonZeroUsize = NonZeroUsize::new(2).unwrap();
                NonZeroUsize::try_from(n)
                    .expect("overflow")
                    .checked_mul(NonZeroUsize::from(w))
                    .expect("overflow")
                    .checked_mul(N)
                    .expect("overflow")
            } else {
                xs.len()
            };

            // Get boundaries of "digit streams" which are contiguous streams of
            // digit characters separated by at least one space or null char
            // which may or may not have a minus sign in front. The boundaries
            // will be constructed as intervals like (start, end) where start
            // and end are the indices of the start and end of the stream.
            let (x0, rest) = xs.nonempty_iter().take(total_bytes).next();
            let mut prev_char_type = CharType::from(*x0);
            // If first char is digit or minus, push start boundary to balance the ends
            if prev_char_type.is_digit_or_minus() {
                digit_starts.push(0);
            }
            for (i, &x) in rest.enumerate() {
                let this_char_type = CharType::from(x);
                if prev_char_type != this_char_type {
                    if this_char_type == CharType::Null {
                        digit_ends.push(i + 1);
                    } else if prev_char_type == CharType::Null {
                        digit_starts.push(i);
                    }
                }
                prev_char_type = this_char_type;
            }
            if prev_char_type == CharType::Digit {
                // If previous was a digit, add a boundary to the end
                digit_ends.push(usize::from(total_bytes));
            } else if prev_char_type == CharType::Minus {
                // If previous was a minus, the last char in the last digit is
                // a minus for this width, which is invalid.
                return false;
            }
            let final_digit_position = digit_ends.iter().copied().last().unwrap_or_default();
            debug_assert!(digit_starts.len() == digit_ends.len(), "start != end");

            // Compute number of segments that fit into digits. Use the last
            // found digit as the end of the bytes to be considered. If segment
            // number is odd, this width is not valid since offsets come in
            // pairs.
            let ww = usize::from(NonZeroUsize::from(w));
            let n_segs = final_digit_position / ww;
            if n_segs & 1 == 1 {
                return false;
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
            let digit_intervals = digit_starts.iter().copied().zip(digit_ends.iter().copied());
            for (a, b) in digit_intervals {
                if let Some(s) = cur_end {
                    if s == b {
                        // offset end and digit end are equal, this digit stream
                        // is satisfied
                        cur_end = seg_ends.by_ref().next();
                        continue;
                    } else if a < s && s < b {
                        // offset end is in digit stream, which is allowed but
                        // we still need to match the current digit stream's
                        // ending offset. Advance until we either find a match
                        // (pass) or we overshoot (fail)
                        while cur_end.is_some_and(|s0| s0 < b) {
                            cur_end = seg_ends.by_ref().next();
                        }
                        if cur_end.is_some_and(|s0| s0 == b) {
                            cur_end = seg_ends.by_ref().next();
                            continue;
                        }
                        return false;
                    }
                    // offset end is before the start of digit stream, invalid
                    return false;
                }
                // we ran out of segment ends, this digit stream is not
                // matched which is a fail
                return false;
            }
            true
        };
        // TODO use NZU8 directly (this is not yet stable)
        let candidates = (MIN_OTHER_WIDTH.get()..=MAX_CHARS.get())
            .filter_map(|w| OtherWidth::try_from(w).ok())
            .filter(|&w| go(w));

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
        if let Some(ne) = candidates.try_into_nonempty_iter() {
            let (w0, mut ws) = ne.next();
            if ws.by_ref().peek().is_none() {
                Ok(w0)
            } else {
                let mw = once(w0).chain(ws).collect();
                Err(GuessOtherWidthError::MultiWidth(mw))
            }
        } else {
            Err(GuessOtherWidthError::NoWidth)
        }
    }
}

impl InnerOffsets {
    fn try_new<I: HasRegion, S: HasSource>(
        begin: i128,
        end: i128,
        conf: &NewOffsetsConfig<I, S>,
    ) -> Result<Self, SegmentOffsetError> {
        let corr = &conf.corr;
        let err = |kind| {
            let o = conf.dataset_offset;
            let c = (corr.begin, corr.end);
            SegmentOffsetError::new((begin, end), c, o, kind, I::REGION, S::SRC)
        };

        let corrected_begin = begin + i128::from(corr.begin);
        let corrected_end = end + i128::from(corr.end);

        if corrected_begin == corrected_end + 1 && conf.allow_pseudoempty.is_set() {
            // Check if this offset is pseudoempty
            // TODO possibly throw warning if this happens
            return Ok(Self::Empty);
        } else if corrected_begin == 0 && corrected_end == 0 {
            // Return empty if both offsets are zero
            return Ok(Self::Empty);
        } else if corrected_begin < i128::from(HEADER_LEN) {
            // Check if segment overlaps with HEADER (sans OTHER segments) which
            // is automatically invalid since the HEADER has a minimum length
            return Err(err(SegmentOffsetErrorKind::InHeader));
        } else if corrected_begin > corrected_end {
            // Return error if ending offset is greater than beginning offset
            return Err(err(SegmentOffsetErrorKind::Inverted));
        }

        // At this point, we know:
        // - each offset must be >= HEADER_LEN (58 bytes)
        // - begin offset should be <= the end offset (which means segment is at
        //   least one byte long)

        let new_length = corrected_end
            .checked_sub(corrected_begin)
            .expect("end should be greater than end")
            .try_into()
            .ok()
            .and_then(|n| NonZeroU64::MIN.checked_add(n))
            .expect("offset length should be within u64");

        let new_begin = u64::try_from(corrected_begin).expect("offset begin exceeded u64");

        let dso = conf.dataset_offset.0;
        let fl = conf.file_len.0;
        assert!(dso <= fl, "dataset offset exceeds file length");

        // put offset in absolute coordinates to check for
        // truncation
        //
        // TODO it would be marginally better to return an error rather than
        // panic here since we could exceed u64 if the user simply supplies a
        // large dataset offset, which is much more likely than encountering a
        // file that is ~4EB
        let abs_new_begin = dso.checked_add(new_begin).expect("abs begin exceeded u64");

        let truncated_length = if let Some(overflow) = abs_new_begin
            .checked_add(new_length.get())
            .expect("abs end exceeded u64")
            .checked_sub(fl)
        {
            // Check by how much the final offset exceeds EOF (if anything)
            let trunc_limit = conf.truncate_offset_limit.0;
            if overflow > trunc_limit {
                return Err(err(SegmentOffsetErrorKind::Truncated(conf.file_len)));
            } else if let Some(l) = new_length.get().checked_sub(overflow) {
                if let Some(truncated_length) = NonZeroU64::new(l) {
                    // length - overflow is greater than one, return new length
                    truncated_length
                } else {
                    // length - overflow is exactly zero, in which case this
                    // offset is empty after truncation
                    return Ok(Self::Empty);
                }
            } else {
                // length - overflow is less than zero, which is an error
                // because the first offset cannot move
                return Err(err(SegmentOffsetErrorKind::BeginEOF(conf.file_len)));
            }
        } else {
            // If no overlap, return original length
            new_length
        };
        let ne =
            NonEmptyOffsetsInner::new(new_begin, truncated_length, new_length, conf.dataset_offset);
        Ok(Self::NonEmpty(ne))
    }

    fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }
}

impl NonEmptyOffsetsInner {
    /// Return the number of bytes in this segment
    fn nbytes(&self) -> NonZeroU64 {
        self.length
    }

    /// Return the first and last byte or this segment
    #[cfg(feature = "python")]
    fn fcs_offset_pair(&self) -> (u64, u64) {
        (self.begin, self.begin + self.length.get() - 1)
    }

    pub(crate) fn truncated_len(&self) -> u64 {
        let o = self.original_length.get();
        let l = self.length.get();
        o.checked_sub(l)
            .unwrap_or_else(|| panic!("original length ({o}) should be >= length ({l})"))
    }

    /// Read bytes within this segment
    pub(crate) fn h_read_contents<R>(
        &self,
        h: &mut BufReader<R>,
        buf: &mut Vec<u8>,
    ) -> io::Result<()>
    where
        R: Read + Seek,
    {
        let absolute_begin = self.begin + self.dataset_offset.0;
        let nbytes = self.nbytes().get();

        #[cfg(debug_assertions)]
        {
            let end = absolute_begin + nbytes;
            let file_size = h.seek(SeekFrom::End(0))?;
            h.seek(SeekFrom::Start(absolute_begin))?;
            assert!(
                end <= file_size,
                "end of segment ({end}) exceeds file ({file_size})"
            );
        }

        h.seek(SeekFrom::Start(absolute_begin))?;
        h.take(nbytes).read_to_end(buf)?;
        Ok(())
    }
}

/// Error when parsing or creating required segment offsets from TEXT
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(B: Key), bound(E: Key))]
pub enum ReqOffsetsError<B, E> {
    Key(ReqSegmentKeyError<B, E>),
    Segment(SegmentOffsetError),
}

impl<B, E> Clone for ReqOffsetsError<B, E> {
    fn clone(&self) -> Self {
        match self {
            Self::Key(x) => Self::Key(x.clone()),
            Self::Segment(x) => Self::Segment(x.clone()),
        }
    }
}

impl<B, E> PartialEq for ReqOffsetsError<B, E> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Key(a), Self::Key(b)) => a == b,
            (Self::Segment(a), Self::Segment(b)) => a == b,
            _ => false,
        }
    }
}

/// Error when parsing required segment offsets from TEXT
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(B: Key), bound(E: Key))]
pub enum ReqSegmentKeyError<B, E> {
    Begin(ReqKeyErrorInner<ParseIntError, B, ()>),
    End(ReqKeyErrorInner<ParseIntError, E, ()>),
}

impl<B, E> Clone for ReqSegmentKeyError<B, E> {
    fn clone(&self) -> Self {
        match self {
            Self::Begin(x) => Self::Begin(x.clone()),
            Self::End(x) => Self::End(x.clone()),
        }
    }
}

impl<B, E> PartialEq for ReqSegmentKeyError<B, E> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Begin(a), Self::Begin(b)) => a == b,
            (Self::End(a), Self::End(b)) => a == b,
            _ => false,
        }
    }
}

/// Error when parsing optional segment offsets from TEXT
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(B: Key), bound(E: Key))]
pub enum OptOffsetsError<B, E> {
    Key(OptSegmentKeyError<B, E>),
    Segment(SegmentOffsetError),
}

impl<B, E> Clone for OptOffsetsError<B, E> {
    fn clone(&self) -> Self {
        match self {
            Self::Key(x) => Self::Key(x.clone()),
            Self::Segment(x) => Self::Segment(x.clone()),
        }
    }
}

impl<B, E> PartialEq for OptOffsetsError<B, E> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Key(a), Self::Key(b)) => a == b,
            (Self::Segment(a), Self::Segment(b)) => a == b,
            _ => false,
        }
    }
}

/// Error when parsing or creating optional segment offsets from TEXT
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(B: Key), bound(E: Key))]
pub enum OptSegmentKeyError<B, E> {
    Begin(ParseKeyError<ParseIntError, B, ()>),
    End(ParseKeyError<ParseIntError, E, ()>),
}

impl<B, E> Clone for OptSegmentKeyError<B, E> {
    fn clone(&self) -> Self {
        match self {
            Self::Begin(x) => Self::Begin(x.clone()),
            Self::End(x) => Self::End(x.clone()),
        }
    }
}

impl<B, E> PartialEq for OptSegmentKeyError<B, E> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Begin(a), Self::Begin(b)) => a == b,
            (Self::End(a), Self::End(b)) => a == b,
            _ => false,
        }
    }
}

/// Error when parsing a segment from HEADER
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum HeaderSegmentError {
    New(SegmentOffsetError),
    Parse(ParseOffsetError),
    SegmentBytes(OffsetsNoBytesError),
    OtherBytes(OtherOffsetsNoBytesError),
    Guess(GuessOtherWidthError),
}

/// Error when there are not enough bytes in file to read offsets
#[derive(Debug, Error, new, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
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
#[derive(Debug, Error, new, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
#[error(
    "needed {required} bytes to parse OTHER offsets at byte 58
     only {remaining} bytes left in file"
)]
pub struct OtherOffsetsNoBytesError {
    remaining: u64,
    required: NonZeroU64,
}

/// Error when creating a new segment
#[derive(Debug, Error, new, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct SegmentOffsetError {
    coords: (i128, i128),
    correction: (i32, i32),
    dataset_offset: DatasetOffset,
    kind: SegmentOffsetErrorKind,
    location: AnyRegion,
    src: AnySrc,
}

#[derive(Debug, PartialEq, Clone)]
enum SegmentOffsetErrorKind {
    Inverted,
    BeginEOF(FileLen),
    InHeader,
    Truncated(FileLen),
}

impl fmt::Display for SegmentOffsetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let (x0, x1) = self.coords;
        let (c0, c1) = self.correction;
        let kind_text = match &self.kind {
            SegmentOffsetErrorKind::Inverted => "Begin after end".into(),
            SegmentOffsetErrorKind::BeginEOF(size) => {
                format!("Begin exceeds file size ({size} bytes)")
            }
            SegmentOffsetErrorKind::InHeader => "Begins within HEADER (first 58 bytes)".into(),
            SegmentOffsetErrorKind::Truncated(size) => {
                format!("Segment exceeds file size ({size} bytes)")
            }
        };
        write!(
            f,
            "{kind_text} for {} offsets from {}; \
             coords=({x0}, {x1}), correction=({c0}, {c1}), offset={}",
            self.location, self.src, self.dataset_offset
        )
    }
}

/// Error when one offset pair overlaps with another
#[derive(Debug, Error, new, PartialEq, Clone, Display)]
#[display(
    "{} segment offsets ({}, {}) overlaps with {} segment offsets ({}, {})",
    self.seg0.name,
    self.seg0.begin,
    self.seg0.end(),
    self.seg1.name,
    self.seg1.begin,
    self.seg1.end(),
)]
#[display(bound(N0: fmt::Display, N1: fmt::Display))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
#[cfg_attr(feature = "python", bound(N0: fmt::Display))]
#[cfg_attr(feature = "python", bound(N1: fmt::Display))]
#[new(visibilty(""))]
pub struct OffsetPairsOverlapError<N0, N1> {
    seg0: NamedOffsets<N0>,
    seg1: NamedOffsets<N1>,
}

impl_kind2!(pub OffsetPairsOverlapErrorFamily, OffsetPairsOverlapError);

impl<A, B> BifunctorOnce<A, B> for OffsetPairsOverlapError<A, B> {
    fn first_once<F: FnOnce(A) -> C, C>(self, f: F) -> Sibling2<Self, C, B> {
        OffsetPairsOverlapError::new(self.seg0.fmap_once(f), self.seg1)
    }

    fn second_once<F: FnOnce(B) -> C, C>(self, f: F) -> Sibling2<Self, A, C> {
        OffsetPairsOverlapError::new(self.seg0, self.seg1.fmap_once(f))
    }
}

pub type HeaderOffsetPairOverlapOverlapError =
    OffsetPairsOverlapError<HeaderOffsetsName, HeaderOffsetsName>;
pub type HeaderOffsetPairsOverlapError =
    OffsetPairsOverlapError<TextOffsetsName, HeaderOffsetsName>;

/// Error when parsing a segment offset from bytes
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error(
    "parse error for {which} offset for {location} segment from source '{src}': {error}",
    which = if self.is_begin { "begin" } else { "end" },
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct ParseOffsetError {
    error: ParseFixedUintError,
    is_begin: bool,
    location: AnyRegion,
    src: NEStringOrBytes,
}

/// Error when TEXT offsets are overridden using corresponding offsets from HEADER
#[derive(Debug, Error, Display)]
#[display(bound(I: HasRegion))]
#[display(
    "could not obtain {} segment offset from TEXT, using offsets from HEADER",
    I::REGION
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
#[cfg_attr(feature = "python", bound(I: HasRegion))]
pub struct SegmentOffsetsDefaultWarning<I>(PhantomData<I>);

impl<I> Clone for SegmentOffsetsDefaultWarning<I> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl<I> PartialEq for SegmentOffsetsDefaultWarning<I> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl<I> Default for SegmentOffsetsDefaultWarning<I> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

/// Error when offsets from TEXT and HEADER do not match
#[derive(Debug, Error, Display, new)]
#[display(bound(I: HasRegion))]
#[display(
    "offsets differ in HEADER {header} and TEXT {text} for {}{}",
    I::REGION,
    self.use_header.map_or("", |x| if x { ", using former" } else { ", using latter" })
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
#[cfg_attr(feature = "python", bound(I: HasRegion))]
pub struct OffsetsMismatchError<I> {
    header: OriginalOffsets,
    text: OriginalOffsets,
    use_header: Option<bool>,
    _region: PhantomData<I>,
}

impl<I> Clone for OffsetsMismatchError<I> {
    fn clone(&self) -> Self {
        Self::new(self.header, self.text, self.use_header)
    }
}

impl<I> PartialEq for OffsetsMismatchError<I> {
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header
            && self.text == other.text
            && self.use_header == other.use_header
    }
}

/// Error when parsing required offsets from TEXT when HEADER is allowed to override
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(I: HasRegion), bound(B: Key), bound(E: Key))]
pub enum ReqOffsetsWithDefaultErrorInner<I, B, E> {
    Req(ReqOffsetsError<B, E>),
    Mismatch(OffsetsMismatchError<I>),
    Validation(TextToHeaderOrSuppOffsetsValidationError),
    Nextdata(NextdataOffsetsError<TextOffsetsName>),
}

impl<I, B, E> Clone for ReqOffsetsWithDefaultErrorInner<I, B, E> {
    fn clone(&self) -> Self {
        match self {
            Self::Req(x) => Self::Req(x.clone()),
            Self::Mismatch(x) => Self::Mismatch(x.clone()),
            Self::Validation(x) => Self::Validation(x.clone()),
            Self::Nextdata(x) => Self::Nextdata(x.clone()),
        }
    }
}

impl<I, B, E> PartialEq for ReqOffsetsWithDefaultErrorInner<I, B, E> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Req(a), Self::Req(b)) => a == b,
            (Self::Mismatch(a), Self::Mismatch(b)) => a == b,
            (Self::Validation(a), Self::Validation(b)) => a == b,
            (Self::Nextdata(a), Self::Nextdata(b)) => a == b,
            _ => false,
        }
    }
}

/// Warning when parsing required offsets from TEXT when HEADER is allowed to override
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(I: HasRegion), bound(B: Key), bound(E: Key))]
pub enum ReqOffsetsWithDefaultWarning_<I, B, E> {
    Error(ReqOffsetsWithDefaultErrorInner<I, B, E>),
    Default(SegmentOffsetsDefaultWarning<I>),
}

impl<I, B, E> Clone for ReqOffsetsWithDefaultWarning_<I, B, E> {
    fn clone(&self) -> Self {
        match self {
            Self::Error(x) => Self::Error(x.clone()),
            Self::Default(x) => Self::Default(x.clone()),
        }
    }
}

impl<I, B, E> PartialEq for ReqOffsetsWithDefaultWarning_<I, B, E> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Error(a), Self::Error(b)) => a == b,
            (Self::Default(a), Self::Default(b)) => a == b,
            _ => false,
        }
    }
}

/// Warning when parsing optional offsets from TEXT when HEADER is allowed to override
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(I: HasRegion), bound(B: Key), bound(E: Key))]
pub enum OptOffsetsWithDefaultWarningInner<I, B, E> {
    Opt(OptOffsetsError<B, E>),
    Mismatch(OffsetsMismatchError<I>),
    Validation(TextToHeaderOrSuppOffsetsValidationError),
    Nextdata(NextdataOffsetsError<TextOffsetsName>),
}

impl<I, B, E> Clone for OptOffsetsWithDefaultWarningInner<I, B, E> {
    fn clone(&self) -> Self {
        match self {
            Self::Opt(x) => Self::Opt(x.clone()),
            Self::Mismatch(x) => Self::Mismatch(x.clone()),
            Self::Validation(x) => Self::Validation(x.clone()),
            Self::Nextdata(x) => Self::Nextdata(x.clone()),
        }
    }
}

impl<I, B, E> PartialEq for OptOffsetsWithDefaultWarningInner<I, B, E> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Opt(a), Self::Opt(b)) => a == b,
            (Self::Mismatch(a), Self::Mismatch(b)) => a == b,
            (Self::Validation(a), Self::Validation(b)) => a == b,
            (Self::Nextdata(a), Self::Nextdata(b)) => a == b,
            _ => false,
        }
    }
}

/// Error when width of OTHER offsets could not be guessed.
#[derive(Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub enum GuessOtherWidthError {
    #[error("No width for OTHER offsets could be found.")]
    NoWidth,
    #[error("Multiple possible widths for OTHER offsets: {}", _0.iter().join(","))]
    MultiWidth(NEVec<OtherWidth>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn other_width_2x8() {
        let s = NESlice::try_from_slice(b"       0       0").unwrap();
        assert_eq!(
            OtherOffsets20::guess_other_width(&s, None).map(u8::from),
            Ok(8)
        );
    }

    #[test]
    fn other_width_2x8_minus() {
        let s = NESlice::try_from_slice(b"       0      -1").unwrap();
        assert_eq!(
            OtherOffsets20::guess_other_width(&s, None).map(u8::from),
            Ok(8)
        );
    }

    #[test]
    fn other_width_2x8_big_minus() {
        let s = NESlice::try_from_slice(b"       0-1000000").unwrap();
        assert_eq!(
            OtherOffsets20::guess_other_width(&s, None).map(u8::from),
            Ok(8)
        );
    }

    #[test]
    fn other_width_2x8_first_minus() {
        let s = NESlice::try_from_slice(b"-1000000       0").unwrap();
        assert_eq!(
            OtherOffsets20::guess_other_width(&s, None).map(u8::from),
            Ok(8)
        );
    }

    #[test]
    fn other_width_4x8() {
        let s = NESlice::try_from_slice(b"       0       0    2112   90125").unwrap();
        assert_eq!(
            OtherOffsets20::guess_other_width(&s, None).map(u8::from),
            Ok(8)
        );
    }

    #[test]
    fn other_width_4x8_minus() {
        let s = NESlice::try_from_slice(b"       0       0    2112  -90125").unwrap();
        assert_eq!(
            OtherOffsets20::guess_other_width(&s, None).map(u8::from),
            Ok(8)
        );
    }

    #[test]
    fn other_width_4x8_hidden() {
        let s = NESlice::try_from_slice(b"       010000000       1       2").unwrap();
        assert_eq!(
            OtherOffsets20::guess_other_width(&s, None).map(u8::from),
            Ok(8)
        );
    }

    #[test]
    fn other_width_4x8_spaceballs() {
        // random space after than should be ignored
        let s = NESlice::try_from_slice(b"       0       0       0   12345              ").unwrap();
        assert_eq!(
            OtherOffsets20::guess_other_width(&s, None).map(u8::from),
            Ok(8)
        );
    }

    #[test]
    fn other_width_uneven() {
        // 8 then 9
        let s = NESlice::try_from_slice(b"       0        0").unwrap();
        assert!(OtherOffsets20::guess_other_width(&s, None).is_err());
    }

    #[test]
    fn other_width_nobound() {
        // this can either be 8 or 16
        let s = NESlice::try_from_slice(b"00000000000000000000000000000000").unwrap();
        assert!(OtherOffsets20::guess_other_width(&s, None).is_err());
    }
}

#[cfg(feature = "serde")]
mod serialize {
    use super::InnerOffsets;

    use serde::ser::{Serialize, SerializeStruct as _, Serializer};

    impl Serialize for InnerOffsets {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match self {
                Self::NonEmpty(s) => s.serialize(serializer),
                Self::Empty => {
                    let mut state = serializer.serialize_struct("EmptySegment", 2)?;
                    state.serialize_field("start", &0_u8)?;
                    state.serialize_field("end", &0_u8)?;
                    state.end()
                }
            }
        }
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{
        HeaderOffsetsName, HeaderOrSuppOffsetsName, IndexedOtherOffsets, InnerOffsets,
        NamedOffsets, NonEmptyOffsetsInner, Offsets, OffsetsCorrection, OriginalOffsets,
        OtherOffsets20, SuppTextOffsetsName, TextOffsetsName,
    };

    use crate::config::DatasetOffset;

    use fireflow_types::python as py;

    use pyo3::exceptions::PyValueError;
    use pyo3::types::{PyString, PyTuple};
    use pyo3::{IntoPyObjectExt as _, prelude::*};

    use std::convert::Infallible;
    use std::num::NonZeroU64;

    // offset corrections will be tuples like (int, int)
    impl<'py, I, S> FromPyObject<'_, 'py> for OffsetsCorrection<I, S> {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let t: (i32, i32) = obj.extract()?;
            Ok(Self::from(t))
        }
    }

    impl<'py, I, S> IntoPyObject<'py> for OffsetsCorrection<I, S> {
        type Target = PyTuple;
        type Output = Bound<'py, <(u64, u64) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.begin, self.end).into_pyobject(py)
        }
    }

    // offsets will be tuples like (int, int)
    impl<'a, 'py, I, S> FromPyObject<'a, 'py> for Offsets<I, S> {
        type Error = PyErr;
        fn extract(obj: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
            let (begin, end): (u64, u64) = obj.extract()?;
            let ret = if begin == 0 && end == 0 {
                InnerOffsets::Empty
            } else if let Some(length) = end.checked_sub(begin).and_then(NonZeroU64::new) {
                // NOTE use zero for offset since all segments from Python-land
                // will be considered relative to current dataset (ie just like
                // they are in an FCS file)
                let dso = DatasetOffset(0);
                InnerOffsets::NonEmpty(NonEmptyOffsetsInner::new(begin, length, length, dso))
            } else {
                // Use ConfigError because these offsets will be supplied to
                // functions which "configure" a reader to look in a certain
                // location for something (a stretch, but that's the closest we
                // have now)
                return Err(py::ConfigError::new_err(
                    "offsets must both be zero or the first must be less than the second",
                ));
            };
            Ok(Self::new(ret))
        }
    }

    impl<'py, I, S> IntoPyObject<'py> for Offsets<I, S> {
        type Target = PyTuple;
        type Output = Bound<'py, <(u64, u64) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.fcs_offset_pair().into_pyobject(py)
        }
    }

    // indexed OTHER segments are like regular segments except they have the
    // index in front, like (int, (int, int))
    impl<'py> FromPyObject<'_, 'py> for IndexedOtherOffsets {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let (index, seg): (usize, OtherOffsets20) = obj.extract()?;
            Ok(Self::new(index, seg))
        }
    }

    impl<'py> IntoPyObject<'py> for IndexedOtherOffsets {
        type Target = PyTuple;
        type Output = Bound<'py, <(usize, (i128, i128)) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.index, self.seg).into_pyobject(py)
        }
    }

    // uncorrected segments are just like segments, ie (int, int)
    //
    // differentiating them will be determined by context
    impl<'py> FromPyObject<'_, 'py> for OriginalOffsets {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let (begin, end): (i128, i128) = obj.extract()?;
            Ok(Self::new(begin, end))
        }
    }

    impl<'py> IntoPyObject<'py> for OriginalOffsets {
        type Target = PyTuple;
        type Output = Bound<'py, <(i128, i128) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.begin, self.end).into_pyobject(py)
        }
    }

    impl<'py> FromPyObject<'_, 'py> for HeaderOffsetsName {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Ok(x) = obj.extract::<usize>() {
                return Ok(Self::Other(x));
            } else if let Ok(s) = obj.extract::<String>() {
                let n = s.as_str();
                if n == py::SEGMENT_NAME_TEXT.as_str() {
                    return Ok(Self::Text);
                }
                if n == py::SEGMENT_NAME_DATA.as_str() {
                    return Ok(Self::Data);
                }
                if n == py::SEGMENT_NAME_ANALYSIS.as_str() {
                    return Ok(Self::Analysis);
                }
            }
            Err(PyValueError::new_err(format!(
                "Must be an integer for OTHER index or one of {}, {}, or {}",
                py::SEGMENT_NAME_TEXT,
                py::SEGMENT_NAME_DATA,
                py::SEGMENT_NAME_ANALYSIS
            )))
        }
    }

    impl<'py> IntoPyObject<'py> for HeaderOffsetsName {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Other(x) => x.into_bound_py_any(py),
                Self::Text => py::SEGMENT_NAME_TEXT.as_str().into_bound_py_any(py),
                Self::Data => py::SEGMENT_NAME_DATA.as_str().into_bound_py_any(py),
                Self::Analysis => py::SEGMENT_NAME_ANALYSIS.as_str().into_bound_py_any(py),
            }
        }
    }

    impl<'py> FromPyObject<'_, 'py> for HeaderOrSuppOffsetsName {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Ok(x) = obj.extract::<usize>() {
                return Ok(Self::Other(x));
            } else if let Ok(s) = obj.extract::<String>() {
                let n = s.as_str();
                if n == py::SEGMENT_NAME_TEXT.as_str() {
                    return Ok(Self::PrimaryText);
                }
                if n == py::SEGMENT_NAME_STEXT.as_str() {
                    return Ok(Self::SuppText);
                }
                if n == py::SEGMENT_NAME_DATA.as_str() {
                    return Ok(Self::Data);
                }
                if n == py::SEGMENT_NAME_ANALYSIS.as_str() {
                    return Ok(Self::Analysis);
                }
            }
            Err(PyValueError::new_err(format!(
                "Must be an integer for OTHER index or one of {}, {}, {}, or {}",
                py::SEGMENT_NAME_TEXT,
                py::SEGMENT_NAME_STEXT,
                py::SEGMENT_NAME_DATA,
                py::SEGMENT_NAME_ANALYSIS
            )))
        }
    }

    impl<'py> IntoPyObject<'py> for HeaderOrSuppOffsetsName {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Other(x) => x.into_bound_py_any(py),
                Self::PrimaryText => py::SEGMENT_NAME_TEXT.as_str().into_bound_py_any(py),
                Self::SuppText => py::SEGMENT_NAME_STEXT.as_str().into_bound_py_any(py),
                Self::Data => py::SEGMENT_NAME_DATA.as_str().into_bound_py_any(py),
                Self::Analysis => py::SEGMENT_NAME_ANALYSIS.as_str().into_bound_py_any(py),
            }
        }
    }

    impl<'py> FromPyObject<'_, 'py> for TextOffsetsName {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Ok(s) = obj.extract::<String>() {
                let n = s.as_str();
                if n == py::SEGMENT_NAME_DATA.as_str() {
                    return Ok(Self::Data);
                }
                if n == py::SEGMENT_NAME_ANALYSIS.as_str() {
                    return Ok(Self::Analysis);
                }
            }
            Err(PyValueError::new_err(format!(
                "Must be one of {} or {}",
                py::SEGMENT_NAME_DATA,
                py::SEGMENT_NAME_ANALYSIS
            )))
        }
    }

    impl<'py> IntoPyObject<'py> for TextOffsetsName {
        type Target = PyString;
        type Output = Bound<'py, Self::Target>;
        type Error = Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Data => py::SEGMENT_NAME_DATA.as_str().into_pyobject(py),
                Self::Analysis => py::SEGMENT_NAME_ANALYSIS.as_str().into_pyobject(py),
            }
        }
    }

    impl<'py> FromPyObject<'_, 'py> for SuppTextOffsetsName {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Ok(s) = obj.extract::<String>()
                && s.as_str() == py::SEGMENT_NAME_STEXT.as_str()
            {
                Ok(Self)
            } else {
                Err(PyValueError::new_err(format!(
                    "Must be {}",
                    py::SEGMENT_NAME_STEXT
                )))
            }
        }
    }

    impl<'py> IntoPyObject<'py> for SuppTextOffsetsName {
        type Target = PyString;
        type Output = Bound<'py, Self::Target>;
        type Error = Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            py::SEGMENT_NAME_STEXT.as_str().into_pyobject(py)
        }
    }

    // named segments are just like segments, ie (<name>, int, int) where the
    // type of <name> depends on context
    impl<'py, N> FromPyObject<'_, 'py> for NamedOffsets<N>
    where
        for<'a> N: FromPyObject<'a, 'py>,
    {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let (name, begin, end) = obj.extract::<(N, u64, u64)>()?;
            if let Some(length) = begin.checked_sub(end).and_then(NonZeroU64::new) {
                Ok(Self::new(name, begin, length))
            } else {
                Err(PyValueError::new_err("begin must be less than end"))
            }
        }
    }

    impl<'py, N> IntoPyObject<'py> for NamedOffsets<N>
    where
        N: IntoPyObject<'py>,
    {
        type Target = PyTuple;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            let end = self.end();
            (self.name, self.begin, end).into_pyobject(py)
        }
    }
}
