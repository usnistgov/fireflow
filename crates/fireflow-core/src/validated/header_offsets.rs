use crate::config::OverlapCorrectionLimit;
use crate::core::{DatasetOffsets, OthersReader, TEXTOffsetsOrigin};
use crate::logging::{DeferredErrors, ErrorGroup, ErrorsResult, LogResult};
use crate::macros::def_summary;
use crate::segment::{
    AreNamedOffsets, HasRegion, HasSource, HeaderAnalysisOffsets, HeaderDataOffsets,
    HeaderOffsetPairOverlapOverlapError, HeaderOffsetsName, HeaderOffsetsOverflow,
    HeaderOrSuppOffsetsName, HeaderToHeaderOffsetsOverlap, IndexedOtherOffsets, IsDataOrAnalysis,
    NamedOffsets, OffsetPairsOverlapError, Offsets, OffsetsOverflow, OversetsOverlap,
    PrimaryTextOffsets, SuppTextOffsetsName, SuppToHeaderOffsetsOverlap, SupplementalTextOffsets,
    TEXTOffsets, TextOffsetsName, TextToHeaderOffsetsOverlap,
};
use crate::text::keywords::Nextdata;
use crate::validated::ascii_range::OtherWidth;

use type_families::BifunctorOnce as _;

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty_collections::{NESlice, NEVec};
use thiserror::Error;

use std::fmt;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    crate::nonempty::FcsNEVec,
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
    nonempty_collections::{IntoNonEmptyIterator as _, NonEmptyIterator as _},
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
    #[as_ref(ParsedOtherOffsets)]
    other: ParsedOtherOffsets,
}

pub(crate) type ParsedOtherOffsets = Option<(NEVec<IndexedOtherOffsets>, OtherWidth)>;

#[cfg(feature = "python")]
pub type PyParsedOtherOffsets = Option<(FcsNEVec<IndexedOtherOffsets>, OtherWidth)>;

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

    /// Return parsed OTHER offsets data
    #[cfg(feature = "python")]
    #[must_use]
    pub fn py_other(&self) -> PyParsedOtherOffsets {
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
        os: ParsedOtherOffsets,
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
        os: ParsedOtherOffsets,
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
        Self::fix_text_overlap(hs, s, limit)
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
        let hs = self.as_mut_nonempty_offsets_filtered::<I>();
        Self::fix_text_overlap(hs, s, limit)
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

    /// Fix offsets that exceed $NEXTDATA or return error if this fails.
    pub(crate) fn validate_nextdata(
        &mut self,
        n: Nextdata,
        limit: OverlapCorrectionLimit,
    ) -> ErrorsResult<Vec<HeaderOffsetsOverflow>, (), NextdataOffsetsError<HeaderOffsetsName>> {
        let mut overlaps = vec![];
        let mut errors = vec![];
        for (mut r, s) in self.as_mut_nonempty_offsets() {
            if let Some(overlap) = s.get_tail_nextdata_overlap(n) {
                if r.truncate(overlap.get(), limit.0).is_some() {
                    overlaps.push(OffsetsOverflow::new(s, overlap));
                } else {
                    errors.push(NextdataOffsetsError::new(n, s));
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
            xs[i].seg = Offsets::default();
        }
    }

    fn as_others(&self) -> impl Iterator<Item = &IndexedOtherOffsets> {
        self.other.iter().flat_map(|(os, _)| os.iter())
    }

    fn fix_text_overlap<'a, I, N>(
        xs: impl IntoIterator<Item = (AnyHeaderOffsetsMut<'a>, NamedOffsets<HeaderOffsetsName>)>,
        s: &mut TEXTOffsets<I>,
        limit: OverlapCorrectionLimit,
    ) -> DeferredErrors<
        Vec<OversetsOverlap<N, HeaderOffsetsName>>,
        OffsetPairsOverlapError<N, HeaderOffsetsName>,
    >
    where
        N: Copy,
        I: HasRegion + AreNamedOffsets<N, Params = ()>,
    {
        // ASSUME incoming iterator is sorted (no debug assert since this would
        // consume iterator)
        if let Some(txt_seg) = s.try_as_named(()) {
            let err = |hdr_seg| OffsetPairsOverlapError::new(txt_seg, hdr_seg);
            let t2h_overlap = |hdr_seg: NamedOffsets<HeaderOffsetsName>, overlap| {
                OversetsOverlap::new(txt_seg, hdr_seg, overlap)
            };
            let mut errors = vec![];
            let mut overlaps = vec![];
            let mut it = xs.into_iter();
            let mut hdr_pair = None;
            // Skip all HEADER offsets that come before TEXT seg
            while let p @ Some((_, hdr_seg)) = it.next() {
                hdr_pair = p;
                if txt_seg.begin <= hdr_seg.end {
                    break;
                }
            }
            // The next HEADER offset pair has an end offset that starts at or
            // after the TEXT begin offset, and thus may overlap the beginning
            // of TEXT, be totally within TEXT, or overlap the ending of TEXT.
            if let Some((mut hdr_ref, hdr_seg)) = hdr_pair {
                if hdr_seg.begin < txt_seg.begin {
                    // HEADER starts before TEXT. Check if the HEADER offsets
                    // pair is for TEXT itself. If so, throw error regardless
                    // since we already read it at this point and thus should
                    // not alter it. If not, truncate if within the limit.
                    if let Some(overlap) = hdr_seg.get_tail_offset_overlap(&txt_seg) {
                        if hdr_ref.truncate(overlap.get(), limit.0).is_some()
                            && !matches!(hdr_ref, AnyHeaderOffsetsMut::Text(_))
                        {
                            overlaps.push(t2h_overlap(hdr_seg, overlap));
                        } else {
                            errors.push(err(hdr_seg));
                        }
                    }
                } else {
                    // HEADER begins within TEXT or after. Truncate TEXT if
                    // within limit or throw error. In former case, return early
                    // since we know that no more HEADER offsets can overlap.
                    if let Some(overlap) = txt_seg.get_tail_offset_overlap(&hdr_seg) {
                        overlaps.push(t2h_overlap(hdr_seg, overlap));
                        if s.truncate(overlap.get(), limit.0).is_some() {
                            return LogResult::new_ok(overlaps);
                        }
                        errors.push(err(hdr_seg));
                    }
                }
            }
            // All the remaining HEADER offset pairs should now begin within
            // TEXT or after.
            for (_, hdr_seg) in it {
                if let Some(overlap) = txt_seg.get_tail_offset_overlap(&hdr_seg) {
                    // If overlap within limit and we have not encountered an
                    // error yet, truncate TEXT and return early without error.
                    // Otherwise push error.
                    if s.truncate(overlap.get(), limit.0).is_some() {
                        overlaps.push(t2h_overlap(hdr_seg, overlap));
                        if errors.is_empty() {
                            return LogResult::new_ok(overlaps);
                        }
                    }
                    errors.push(err(hdr_seg));
                } else {
                    // If no overlaps, we can assume there are no more overlaps
                    // since the HEADER offsets are sorted. Break early to save
                    // time.
                    break;
                }
            }
            LogResult::new_from_err_iter(errors, (), ()).set_deferred_value(overlaps)
        } else {
            LogResult::new_ok(vec![])
        }
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
            .map(|(i, o)| self.contains_offsets(&o.seg, i));
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
        let q = s.try_as_named(args)?;
        (q.begin < self.nbytes()).then_some(InHeaderError(q))
    }

    fn as_mut_offsets(&mut self) -> impl Iterator<Item = AnyHeaderOffsetsMut<'_>> {
        self.other
            .iter_mut()
            .flat_map(|(os, _)| os.iter_mut())
            .map(AnyHeaderOffsetsMut::Other)
            .chain([
                AnyHeaderOffsetsMut::Text(&mut self.text),
                AnyHeaderOffsetsMut::Data(&mut self.data),
                AnyHeaderOffsetsMut::Analysis(&mut self.analysis),
            ])
    }

    fn as_mut_nonempty_offsets(
        &mut self,
    ) -> impl Iterator<Item = (AnyHeaderOffsetsMut<'_>, NamedOffsets<HeaderOffsetsName>)> {
        self.as_mut_offsets()
            .filter_map(|x| x.try_as_named().map(|y| (x, y)))
            .sorted_by_key(|x| x.1.as_pair())
    }

    fn as_mut_nonempty_offsets_filtered<I>(
        &mut self,
    ) -> impl Iterator<Item = (AnyHeaderOffsetsMut<'_>, NamedOffsets<HeaderOffsetsName>)>
    where
        I: IsDataOrAnalysis,
    {
        self.as_mut_nonempty_offsets().filter(|(k, _)| {
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
        let mut pairs: Vec<_> = self.as_mut_nonempty_offsets().collect();
        debug_assert!(pairs.is_sorted_by_key(|x| x.1.as_pair()), "not sorted");
        let mut errors = vec![];
        let mut remainder = &mut pairs[..];
        let mut fixed = vec![];
        while let Some(((ref0, seg0), rest)) = remainder.split_first_mut() {
            for (_, seg1) in rest {
                if let Some(overlap) = seg0.get_tail_offset_overlap(seg1) {
                    if ref0.truncate(overlap.get(), limit.0).is_some() {
                        let overlap_ret = HeaderToHeaderOffsetsOverlap::new(*seg0, *seg1, overlap);
                        fixed.push(overlap_ret);
                        // break early because any offset after this one is
                        // guaranteed to be after the new truncated ending due
                        // to sorting
                        break;
                    }
                    errors.push(OffsetPairsOverlapError::new(*seg0, *seg1));
                }
            }
            if !remainder.is_empty() {
                remainder = &mut remainder[1..];
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
}

/// Any mutable reference to offsets from HEADER.
enum AnyHeaderOffsetsMut<'a> {
    Text(&'a mut PrimaryTextOffsets),
    Data(&'a mut HeaderDataOffsets),
    Analysis(&'a mut HeaderAnalysisOffsets),
    Other(&'a mut IndexedOtherOffsets),
}

impl AnyHeaderOffsetsMut<'_> {
    fn try_as_named(&self) -> Option<NamedOffsets<HeaderOffsetsName>> {
        match self {
            Self::Text(s) => s.try_as_named(()),
            Self::Data(s) => s.try_as_named(()),
            Self::Analysis(s) => s.try_as_named(()),
            Self::Other(s) => s.seg.try_as_named(s.index),
        }
    }

    fn truncate(&mut self, n: u64, limit: u64) -> Option<u64> {
        match self {
            Self::Text(s) => s.truncate(n, limit),
            Self::Data(s) => s.truncate(n, limit),
            Self::Analysis(s) => s.truncate(n, limit),
            Self::Other(s) => s.seg.truncate(n, limit),
        }
    }
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

/// Error when a non-empty offset pair occurs within the first 58 bytes of the file.
#[derive(Debug, Error, PartialEq, Clone, Display)]
#[display(
    "{} segment offsets ({}, {}) is within HEADER region",
    self.0.name,
    self.0.begin,
    self.0.end
)]
#[display(bound(N: fmt::Display))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
#[cfg_attr(feature = "python", bound(N: fmt::Display))]
pub struct InHeaderError<N>(NamedOffsets<N>);

/// Error when segment offsets exceed $NEXTDATA.
#[derive(Debug, Error, new, PartialEq, Clone, Display)]
#[display(
    "{} segment offsets ({}, {}) exceeds $NEXTDATA ({})",
    self.offsets.name,
    self.offsets.begin,
    self.offsets.end,
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

/// The length of the HEADER without OTHER segments.
pub(crate) const HEADER_LEN: u8 = 58;
