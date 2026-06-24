use crate::config::{OverlapCorrectionLimit, ReadOffsetConfig};
use crate::core::{DatasetOffsets, OthersReader, TEXTOffsetsOrigin};
use crate::logging::{DeferredErrors, ErrorGroup, ErrorsResult, LogResult};
use crate::macros::def_summary;
use crate::segment::{
    AnalysisSegmentId, DataSegmentId, OffsetsFromHeader, OtherSegmentId, PrimaryTextSegmentId,
    read::{
        AreNamedOffsets, DatasetOverflowError, HasOneName, HasRegion, HasSource,
        HeaderAnalysisOffsets, HeaderDataOffsets, HeaderOffsetPairOverlapOverlapError,
        HeaderOffsetsName, HeaderOffsetsOverflow, HeaderOrSuppOffsetsName,
        HeaderToHeaderOffsetsOverlap, InHeaderError, IndexedOtherOffsets, IsDataOrAnalysis,
        IsOffsetPair, NonEmptyOffsetsMut, OffsetPairsOverlapError, Offsets, OffsetsOverflow,
        OffsetsOverlap, OverflowResult, OverlapResult, PrimaryTextOffsets, SuppTextOffsetsName,
        SuppToHeaderOffsetsOverlap, SupplementalTextOffsets, TEXTOffsets, TextOffsetsName,
        TextToHeaderOffsetsOverlap,
    },
};
use crate::validated::ascii_range::OtherWidth;
use crate::validated::read_state::{DatasetOffset, FileLen, HeaderReadState, TEXTReadState};

use type_families::BifunctorOnce as _;

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty_collections::{NESlice, NEVec};
use thiserror::Error;

use std::iter;
use std::num::NonZeroU64;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    crate::nonempty::FcsNEVec,
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
    nonempty_collections::{IntoNonEmptyIterator as _, NonEmptyIterator as _},
    std::fmt,
};

/// The segment offsets as read from HEADER.
///
/// These are validated such that no segment will be overlapping with another.
#[derive(Clone, PartialEq, AsRef, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[new(visibility = "")]
pub struct FinalHeaderOffsets {
    #[as_ref(PrimaryTextOffsets)]
    text: PrimaryTextOffsets,
    #[as_ref(HeaderDataOffsets)]
    data: HeaderDataOffsets,
    #[as_ref(HeaderAnalysisOffsets)]
    analysis: HeaderAnalysisOffsets,
    #[as_ref(FinalOtherOffsets)]
    other: FinalOtherOffsets,
}

pub(crate) type FinalOtherOffsets = Option<(NEVec<IndexedOtherOffsets>, OtherWidth)>;

#[cfg(feature = "python")]
pub type PyFinalOtherOffsets = Option<(FcsNEVec<IndexedOtherOffsets>, OtherWidth)>;

impl FinalHeaderOffsets {
    /// Return primary TEXT offsets
    #[must_use]
    pub fn text(&self) -> PrimaryTextOffsets {
        self.text
    }

    /// Return DATA offsets
    #[must_use]
    pub fn data(&self) -> HeaderDataOffsets {
        self.data
    }

    /// Return ANALYSIS offsets
    #[must_use]
    pub fn analysis(&self) -> HeaderAnalysisOffsets {
        self.analysis
    }

    /// Return parsed OTHER offsets data
    #[must_use]
    pub fn other(&self) -> Option<(NESlice<'_, IndexedOtherOffsets>, OtherWidth)> {
        self.other
            .as_ref()
            .map(|(xs, w)| (xs.as_nonempty_slice(), *w))
    }

    /// Return parsed OTHER offsets data as slice
    #[must_use]
    pub fn other_ref(&self) -> &[IndexedOtherOffsets] {
        if let Some((offsets, _)) = self.other.as_ref() {
            offsets.as_ref()
        } else {
            &[]
        }
    }

    /// Return parsed OTHER offsets data
    #[cfg(feature = "python")]
    #[must_use]
    pub fn py_other(&self) -> PyFinalOtherOffsets {
        let (ws, w) = self.other()?;
        Some((FcsNEVec(ws.into_nonempty_iter().copied().collect()), w))
    }

    /// Make new collection of HEADER offsets.
    ///
    /// Will throw errors for overlapping offsets.
    pub fn try_new(
        text: PrimaryTextOffsets,
        data: HeaderDataOffsets,
        analysis: HeaderAnalysisOffsets,
        os: FinalOtherOffsets,
    ) -> Result<(Self, Vec<HeaderToHeaderOffsetsOverlap>), OffsetsValidationErrors> {
        // set limit to zero so that any overlap causes an error
        Self::try_new_with_limit(text, data, analysis, os, 0.into())
            .group()
            .resolve_nowarn()
    }

    /// Make new collection of HEADER offsets.
    ///
    /// Will try to fix overlaps subject to limit or will throw error.
    ///
    /// With throw error if any offsets overlaps with HEADER itself.
    #[must_use]
    pub(crate) fn try_new_with_limit(
        text: PrimaryTextOffsets,
        data: HeaderDataOffsets,
        analysis: HeaderAnalysisOffsets,
        os: FinalOtherOffsets,
        limit: OverlapCorrectionLimit,
    ) -> ErrorsResult<(Self, Vec<HeaderToHeaderOffsetsOverlap>), (), HeaderOffsetsValidationError>
    {
        let mut ret = Self::new(text, data, analysis, os);
        ret.validate(limit).map_ok_value(|overlaps| (ret, overlaps))
    }

    /// Return DATA and ANALYSIS offsets in struct.
    ///
    /// The returned struct encodes DATA and ANALYSIS offsets that should
    /// actually be used for reading these segments. Since this takes no inputs,
    /// TEXT offsets cannot be included, therefore it should only be called in
    /// 2.0 code which does not include TEXT segmetns.
    pub(crate) fn as_dataset_offsets_2_0(&self) -> DatasetOffsets {
        let d = self.data.into_any();
        let a = self.analysis.into_any();
        DatasetOffsets::new(
            d,
            a,
            TEXTOffsetsOrigin::EmptyTEXT,
            TEXTOffsetsOrigin::EmptyTEXT,
            None,
        )
    }

    /// Ensure supp TEXT does not overlap other offsets or HEADER.
    ///
    /// This will either truncate offsets here (the HEADER) or truncate the
    /// supplied offset (from TEXT), limit permitting.
    pub(crate) fn validate_supp_text(
        &mut self,
        s: &mut SupplementalTextOffsets,
        limit: OverlapCorrectionLimit,
    ) -> DeferredErrors<Vec<SuppToHeaderOffsetsOverlap>, SuppToHeaderOffsetsValidationError> {
        let contains = self
            .contains_offsets(s, ())
            .map(OffsetsValidationError::from);
        let hs = self.as_mut_nonempty_offsets();
        Self::fix_offsets_from_text_overlap(hs, s, limit)
            .map_errors(OffsetsValidationError::from)
            .extend_errors(contains, |v| v)
    }

    /// Ensure DATA/ANALYSIS from TEXT do not overlap other offsets or HEADER.
    ///
    /// This will either truncate offsets here (the HEADER) or truncate the
    /// supplied offset (from TEXT), limit permitting.
    pub(crate) fn validate_text_data_or_analysis<I>(
        &mut self,
        s: &mut TEXTOffsets<I>,
        limit: OverlapCorrectionLimit,
    ) -> DeferredErrors<Vec<TextToHeaderOffsetsOverlap>, TextToHeaderOffsetsValidationError>
    where
        I: HasRegion + AreNamedOffsets<TextOffsetsName, Params = ()> + IsDataOrAnalysis,
    {
        let contains = self
            .contains_offsets(s, ())
            .map(OffsetsValidationError::from);
        let hs = self.as_mut_nonempty_offsets_no_data_analysis::<I>();
        Self::fix_offsets_from_text_overlap(hs, s, limit)
            .map_errors(OffsetsValidationError::from)
            .extend_errors(contains, |v| v)
    }

    /// Return number of bytes required to encode HEADER (including OTHER)
    pub(crate) fn nbytes(&self) -> u64 {
        u64::from(HEADER_LEN) + self.other_offset_nbytes()
    }

    /// Return reader for OTHER segments.
    pub(crate) fn others_reader(&self) -> OthersReader {
        OthersReader::new(self.as_others().copied().collect())
    }

    /// Truncate primary TEXT offsets that exceed EOF.
    ///
    /// Return the amount that was truncated (possibly none) on success, or
    /// return error if truncation would exceed limit set by user.
    pub(crate) fn try_truncate_primary_text<C>(
        &mut self,
        st: &HeaderReadState<C>,
    ) -> Result<u64, PrimaryTEXTOverflowError>
    where
        C: AsRef<ReadOffsetConfig>,
    {
        let local_file_len = st.local_file_len();
        let conf: &ReadOffsetConfig = st.conf().as_ref();
        let limit = conf.dataset_overflow_limit;
        if let Some(ne) = self.text.as_nonempty_mut() {
            if let Some(res) = ne.tail_overlap_offset_and_truncate(local_file_len, limit.0, ()) {
                if res.truncated {
                    Ok(res.overflow.get())
                } else {
                    let e = PrimaryTEXTOverflowError::new(
                        res.offsets.begin,
                        res.offsets.length,
                        st.dataset_offset(),
                        st.file_len(),
                        res.overflow,
                    );
                    Err(e)
                }
            } else {
                Ok(0)
            }
        } else {
            Ok(0)
        }
    }

    /// Truncate non-primary TEXT offsets that exceed EOF or $NEXTDATA.
    ///
    /// Return amount truncated for each offset pair or error if truncation
    /// amount is beyond limit.
    pub(crate) fn try_truncate_non_primary_text<C>(
        &mut self,
        st: &TEXTReadState<C>,
    ) -> ErrorsResult<Vec<HeaderOffsetsOverflow>, (), DatasetOverflowError<HeaderOffsetsName>>
    where
        C: AsRef<ReadOffsetConfig>,
    {
        let bounds = st.dataset_bounds();
        let dataset_len = bounds.len.0;
        let conf: &ReadOffsetConfig = st.conf().as_ref();
        let limit = conf.dataset_overflow_limit;
        let mut overlaps = vec![];
        let mut errors = vec![];
        for ne in self.as_mut_nonempty_offsets_no_text() {
            if let Some(res) = ne.tail_overlap_offset_and_truncate(dataset_len, limit.0) {
                let o = OffsetsOverflow::new(
                    res.offsets,
                    res.overflow,
                    dataset_len,
                    bounds.from_nextdata,
                );
                if res.truncated {
                    overlaps.push(o);
                } else {
                    errors.push(DatasetOverflowError(o));
                }
            }
        }
        LogResult::new_from_err_iter(errors, overlaps, ())
    }

    /// Set OTHER offsets at index to empty.
    ///
    /// Will panic if out of bounds.
    pub(crate) fn remove_other(&mut self, i: usize) {
        if let Some((xs, _)) = self.other.as_mut() {
            xs[i].offsets = Offsets::default();
        }
    }

    fn as_others(&self) -> impl Iterator<Item = &IndexedOtherOffsets> {
        self.other.iter().flat_map(|(os, _)| os.iter())
    }

    fn fix_offsets_from_text_overlap<'a, I, N>(
        xs: impl IntoIterator<Item = AnyHeaderOffsetsMut<'a>>,
        offsets: &mut TEXTOffsets<I>,
        limit: OverlapCorrectionLimit,
    ) -> DeferredErrors<
        Vec<OffsetsOverlap<N, HeaderOffsetsName>>,
        OffsetPairsOverlapError<N, HeaderOffsetsName>,
    >
    where
        N: Copy,
        I: HasRegion + AreNamedOffsets<N, Params = ()>,
    {
        // ASSUME incoming iterator is sorted (no debug assert since this would
        // consume iterator)
        let mut it = xs.into_iter().peekable();
        let mut errors = vec![];
        let mut overlaps = vec![];

        let err = |txt_named, hdr_named, overlap| {
            OffsetPairsOverlapError(OffsetsOverlap::new(txt_named, hdr_named, overlap))
        };

        if let Some(txt_ne) = offsets.as_nonempty_mut() {
            let mut last_hdr_ne = None;
            // Skip all HEADER offsets that come before TEXT offsets
            for hdr_ne in it.by_ref() {
                let end = hdr_ne.end();
                last_hdr_ne = Some(hdr_ne);
                if txt_ne.begin() < end {
                    break;
                }
            }
            // The next HEADER offset pair has an end offset that starts at or
            // after the TEXT begin offset, and thus may overlap the beginning
            // of TEXT, be totally within TEXT, or overlap the ending of TEXT.
            if let Some(hdr_ne) = last_hdr_ne {
                if hdr_ne.begin() < txt_ne.begin() {
                    let txt_named = txt_ne.as_named1();
                    // HEADER starts before TEXT. Check if the HEADER offsets
                    // pair is for TEXT itself. If so, throw error regardless
                    // since we already read it at this point and thus should
                    // not alter it. If not, truncate if within the limit.
                    if let Some(overlap_len) = hdr_ne.tail_overlap_pair(&txt_ne)
                        && matches!(hdr_ne, AnyHeaderOffsetsMut::Text(_))
                    {
                        errors.push(err(txt_named, hdr_ne.as_named1(), overlap_len));
                    } else if let Some(res) =
                        hdr_ne.tail_overlap_pair_and_truncate(&txt_ne, limit.0)
                    {
                        let o = res.overlap.flip();
                        if res.truncated {
                            overlaps.push(o);
                        } else {
                            errors.push(OffsetPairsOverlapError(o));
                        }
                    }
                } else {
                    // HEADER begins within TEXT or after. Truncate TEXT if
                    // within limit or throw error. In former case, return early
                    // since we know that no more HEADER offsets can overlap.
                    if let Some(res) = txt_ne.tail_overlap_pair_and_truncate(&hdr_ne, limit.0, ()) {
                        if res.truncated {
                            overlaps.push(res.overlap);
                            return LogResult::new_ok(overlaps);
                        }
                        errors.push(OffsetPairsOverlapError(res.overlap));
                    }
                }
            }
        } else {
            return LogResult::new_ok(vec![]);
        }

        // All the remaining HEADER offset pairs should now begin within TEXT or
        // after. Try to get TEXT offsets as non-empty again since we may have
        // truncated it down to empty in the above code.
        if let Some(txt_ne) = offsets.as_nonempty_mut() {
            let exceeded = peekable_map_while(it.by_ref(), |x| {
                let n = txt_ne.exceeds_limit(x.begin(), limit.0)?;
                let overlap = OffsetsOverlap::new(txt_ne.as_named1(), x.as_named1(), n);
                Some(OffsetPairsOverlapError(overlap))
            });
            errors.extend(exceeded);
            if let Some(res) = it
                .next()
                .and_then(|x| txt_ne.tail_overlap_pair_and_truncate(&x, limit.0, ()))
            {
                assert!(res.truncated, "final offset should be truncatable");
                overlaps.push(res.overlap);
            }
        }
        LogResult::new_from_err_iter(errors, (), ()).set_deferred_value(overlaps)
    }

    /// Ensure HEADER offset pairs don't overlap and start after HEADER itself
    ///
    /// HEADER overlap are always fatal since the start of an offset pair cannot
    /// be changed. Overlaps with other offset pairs may be non-fatal if the
    /// overlap is smaller than the correction limit.
    fn validate(
        &mut self,
        limit: OverlapCorrectionLimit,
    ) -> ErrorsResult<Vec<HeaderToHeaderOffsetsOverlap>, (), HeaderOffsetsValidationError> {
        self.find_or_fix_header_overlaps(limit)
            .map_errors(OffsetsValidationError::from)
            .extend_errors(
                self.contains_header_offsets()
                    .map(OffsetsValidationError::from),
                |_| (),
            )
    }

    fn contains_header_offsets(&self) -> impl Iterator<Item = InHeaderError<HeaderOffsetsName>> {
        let t = self.contains_offsets(&self.text, ());
        let d = self.contains_offsets(&self.data, ());
        let a = self.contains_offsets(&self.analysis, ());
        let os = self
            .as_others()
            .enumerate()
            .map(|(i, o)| self.contains_offsets(&o.offsets, i));
        [t, d, a].into_iter().chain(os).flatten()
    }

    fn contains_offsets<I, S, N>(
        &self,
        s: &Offsets<I, S>,
        args: I::Params,
    ) -> Option<InHeaderError<N>>
    where
        I: HasRegion + AreNamedOffsets<N>,
        S: HasSource,
    {
        let q = s.as_nonempty()?;
        (q.begin() < self.nbytes()).then_some(InHeaderError(q.as_named(args)))
    }

    fn as_mut_nonempty_offsets(&mut self) -> impl Iterator<Item = AnyHeaderOffsetsMut<'_>> {
        let req = [
            self.text.as_nonempty_mut().map(AnyHeaderOffsetsMut::Text),
            self.data.as_nonempty_mut().map(AnyHeaderOffsetsMut::Data),
            self.analysis
                .as_nonempty_mut()
                .map(AnyHeaderOffsetsMut::Analysis),
        ];
        self.other
            .iter_mut()
            .flat_map(|(os, _)| os.iter_mut())
            .filter_map(|x| {
                x.offsets
                    .as_nonempty_mut()
                    .map(|y| AnyHeaderOffsetsMut::Other(y, x.index))
            })
            .chain(req.into_iter().flatten())
            .sorted_by_key(IsOffsetPair::slice_pair)
    }

    fn as_mut_nonempty_offsets_no_text(&mut self) -> impl Iterator<Item = AnyHeaderOffsetsMut<'_>> {
        self.as_mut_nonempty_offsets()
            .filter(|k| !matches!(k, AnyHeaderOffsetsMut::Text(_)))
    }

    fn as_mut_nonempty_offsets_no_data_analysis<I>(
        &mut self,
    ) -> impl Iterator<Item = AnyHeaderOffsetsMut<'_>>
    where
        I: IsDataOrAnalysis,
    {
        self.as_mut_nonempty_offsets().filter(|k| {
            !matches!(
                (k, I::IS_DATA),
                (AnyHeaderOffsetsMut::Data(_), true) | (AnyHeaderOffsetsMut::Analysis(_), false)
            )
        })
    }

    fn find_or_fix_header_overlaps(
        &mut self,
        limit: OverlapCorrectionLimit,
    ) -> ErrorsResult<Vec<HeaderToHeaderOffsetsOverlap>, (), HeaderOffsetPairOverlapOverlapError>
    {
        let pairs: Vec<_> = self.as_mut_nonempty_offsets().collect();
        debug_assert!(
            pairs.is_sorted_by_key(IsOffsetPair::slice_pair),
            "not sorted"
        );
        let tmp: Vec<_> = pairs.iter().map(|x| (x.begin(), x.as_named1())).collect();
        let mut errors = vec![];
        let mut fixed = vec![];
        let plen = pairs.len().saturating_sub(1);
        for (i, p) in pairs.into_iter().enumerate().take(plen) {
            let mut ts = tmp[i + 1..].iter().copied().peekable();
            let exceeded = peekable_map_while(ts.by_ref(), |(begin, named)| {
                let n = p.exceeds_limit(*begin, limit.0)?;
                let overlap = OffsetsOverlap::new(p.as_named1(), *named, n);
                Some(OffsetPairsOverlapError(overlap))
            });
            errors.extend(exceeded);
            if let Some(res) = ts
                .next()
                .and_then(|(_, named)| p.tail_overlap_pair_and_truncate(&named, limit.0))
            {
                assert!(res.truncated, "final offset should be truncatable");
                fixed.push(res.overlap);
            }
        }
        LogResult::new_from_err_iter(errors, fixed, ())
    }

    fn other_offset_nbytes(&self) -> u64 {
        self.other.as_ref().map_or(0, |(os, width)| {
            let n = u64::try_from(usize::from(os.len())).expect("usize overflow");
            n * u64::from(u8::from(*width))
        })
    }

    pub(crate) fn ptext_other_max_end_offset(&self) -> u64 {
        // Don't include ANALYSIS or DATA here because this will be called in
        // order to find the CRC, in which case we need to take the TEXT
        // versions of these offsets into account which may more may not be
        // different.
        let text_end = self
            .text
            .as_nonempty()
            .expect("TEXT should not be empty")
            .end();
        let other_max = self
            .other
            .as_ref()
            .into_iter()
            .flat_map(|(o, _)| o.iter())
            .filter_map(|o| o.offsets.as_nonempty())
            .map(|o| o.end())
            .max();
        other_max.map_or(text_end, |o| o.max(text_end))
    }
}

/// Any mutable reference to offsets from HEADER.
enum AnyHeaderOffsetsMut<'a> {
    Text(NonEmptyOffsetsMut<'a, PrimaryTextSegmentId, OffsetsFromHeader>),
    Data(NonEmptyOffsetsMut<'a, DataSegmentId, OffsetsFromHeader>),
    Analysis(NonEmptyOffsetsMut<'a, AnalysisSegmentId, OffsetsFromHeader>),
    Other(
        NonEmptyOffsetsMut<'a, OtherSegmentId, OffsetsFromHeader>,
        usize,
    ),
}

impl IsOffsetPair for AnyHeaderOffsetsMut<'_> {
    fn begin(&self) -> u64 {
        match self {
            Self::Text(s) => s.begin(),
            Self::Data(s) => s.begin(),
            Self::Analysis(s) => s.begin(),
            Self::Other(s, _) => s.begin(),
        }
    }

    fn end(&self) -> u64 {
        match self {
            Self::Text(s) => s.end(),
            Self::Data(s) => s.end(),
            Self::Analysis(s) => s.end(),
            Self::Other(s, _) => s.end(),
        }
    }
}

impl HasOneName<HeaderOffsetsName> for AnyHeaderOffsetsMut<'_> {
    fn segname1(&self) -> HeaderOffsetsName {
        match self {
            Self::Text(s) => s.segname(()),
            Self::Data(s) => s.segname(()),
            Self::Analysis(s) => s.segname(()),
            Self::Other(s, i) => s.segname(*i),
        }
    }
}

impl AnyHeaderOffsetsMut<'_> {
    fn exceeds_limit(&self, offset: u64, limit: u64) -> Option<NonZeroU64> {
        match self {
            Self::Text(s) => s.exceeds_limit(offset, limit),
            Self::Data(s) => s.exceeds_limit(offset, limit),
            Self::Analysis(s) => s.exceeds_limit(offset, limit),
            Self::Other(s, _) => s.exceeds_limit(offset, limit),
        }
    }

    fn tail_overlap_pair_and_truncate<P, N>(
        self,
        other: &P,
        limit: u64,
    ) -> Option<OverlapResult<HeaderOffsetsName, N>>
    where
        P: IsOffsetPair + HasOneName<N>,
    {
        match self {
            Self::Text(s) => s.tail_overlap_pair_and_truncate(other, limit, ()),
            Self::Data(s) => s.tail_overlap_pair_and_truncate(other, limit, ()),
            Self::Analysis(s) => s.tail_overlap_pair_and_truncate(other, limit, ()),
            Self::Other(s, i) => s.tail_overlap_pair_and_truncate(other, limit, i),
        }
    }

    fn tail_overlap_offset_and_truncate(
        self,
        other: u64,
        limit: u64,
    ) -> Option<OverflowResult<HeaderOffsetsName>> {
        match self {
            Self::Text(s) => s.tail_overlap_offset_and_truncate(other, limit, ()),
            Self::Data(s) => s.tail_overlap_offset_and_truncate(other, limit, ()),
            Self::Analysis(s) => s.tail_overlap_offset_and_truncate(other, limit, ()),
            Self::Other(s, i) => s.tail_overlap_offset_and_truncate(other, limit, i),
        }
    }
}

/// Error when segment offsets exceed $NEXTDATA.
///
/// This is special and different from [`OffsetsOverflow`] since primary TEXT
/// can only be validated against EOF and not both EOF and $NEXTDATA; the latter
/// doesn't exist until primary TEXT is read.
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error(
    "primary TEXT offsets offsets (begin = {begin}, length = {length}) exceed \
     file length {file_len} by {overflow} bytes given dataset offset of \
     {dataset_offset}"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct PrimaryTEXTOverflowError {
    begin: u64,
    length: u64,
    dataset_offset: DatasetOffset,
    file_len: FileLen,
    overflow: NonZeroU64,
}

/// Error when validating offsets in HEADER
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(N0: fmt::Display))]
#[cfg_attr(feature = "python", bound(N1: fmt::Display))]
pub enum OffsetsValidationError<N0, N1> {
    Overlap(OffsetPairsOverlapError<N0, N1>),
    InHeader(InHeaderError<N0>),
}

impl<N0, N1> OffsetsValidationError<N0, N1> {
    pub(crate) fn into2<N1f>(self) -> OffsetsValidationError<N0, N1f>
    where
        N1: Into<N1f>,
    {
        match self {
            Self::Overlap(e) => OffsetsValidationError::Overlap(e.second_into_once()),
            Self::InHeader(e) => OffsetsValidationError::InHeader(e),
        }
    }
}

pub type HeaderOffsetsValidationError =
    OffsetsValidationError<HeaderOffsetsName, HeaderOffsetsName>;

pub type TextToTextOffsetsValidationError =
    OffsetsValidationError<TextOffsetsName, TextOffsetsName>;

pub type SuppToHeaderOffsetsValidationError =
    OffsetsValidationError<SuppTextOffsetsName, HeaderOffsetsName>;

pub type TextToSuppOffsetsValidationError =
    OffsetsValidationError<TextOffsetsName, SuppTextOffsetsName>;

pub type TextToHeaderOrSuppOffsetsValidationError =
    OffsetsValidationError<TextOffsetsName, HeaderOrSuppOffsetsName>;

pub type TextToHeaderOffsetsValidationError =
    OffsetsValidationError<TextOffsetsName, HeaderOffsetsName>;

pub type OffsetsValidationErrors = ErrorGroup<
    OffsetsValidationError<HeaderOffsetsName, HeaderOffsetsName>,
    OffsetsValidationSummary,
>;

def_summary!(
    pub OffsetsValidationSummary,
    "Error when making new HEADER offsets"
);

fn peekable_map_while<I, B, F>(iter: &mut iter::Peekable<I>, mut f: F) -> impl Iterator<Item = B>
where
    I: Iterator,
    F: FnMut(&I::Item) -> Option<B>,
{
    iter::from_fn(move || {
        let mapped = f(iter.peek()?)?;
        iter.next();
        Some(mapped)
    })
}

/// The length of the HEADER without OTHER segments.
pub(crate) const HEADER_LEN: u8 = 58;
