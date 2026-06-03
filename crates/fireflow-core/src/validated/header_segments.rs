use crate::config::OverlapCorrectionLimit;
use crate::core::{DatasetSegments, OthersReader};
use crate::logging::{ErrorGroup, ErrorsResult};
use crate::macros::def_summary;
use crate::segment::{
    GenericSegment, HasRegion, HasSegmentName, HasSource, HeaderAnalysisSegment, HeaderDataSegment,
    IndexedOtherSegment, IsDataOrAnalysis, PrimaryTextSegment, Segment, SegmentFromTEXT,
    SegmentOverlapError, TEXTSegment,
};
use crate::text::keywords::Nextdata;
use crate::validated::ascii_range::OtherWidth;

use derive_more::{AsRef, Display, From};
use derive_new::new;
use fireflow_types::keywords::TEXTOffsetOrigin;
use itertools::Itertools as _;
use nonempty_collections::{NESlice, NEVec};
use thiserror::Error;

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
pub struct ParsedHeaderSegments {
    #[as_ref(PrimaryTextSegment)]
    text: PrimaryTextSegment,
    #[as_ref(HeaderDataSegment)]
    data: HeaderDataSegment,
    #[as_ref(HeaderAnalysisSegment)]
    analysis: HeaderAnalysisSegment,
    #[as_ref(ParsedOtherSegments)]
    other: ParsedOtherSegments,
}

pub(crate) type ParsedOtherSegments = Option<(NEVec<IndexedOtherSegment>, OtherWidth)>;

#[cfg(feature = "python")]
pub type PyParsedOtherSegments = Option<(FcsNEVec<IndexedOtherSegment>, OtherWidth)>;

impl ParsedHeaderSegments {
    /// Return primary TEXT segment
    #[must_use]
    pub fn text(&self) -> PrimaryTextSegment {
        self.text
    }

    /// Return DATA segment
    #[must_use]
    pub fn data(&self) -> HeaderDataSegment {
        self.data
    }

    /// Return DATA segment
    #[must_use]
    pub fn analysis(&self) -> HeaderAnalysisSegment {
        self.analysis
    }

    /// Return parsed OTHER segment data
    #[must_use]
    pub fn other(&self) -> Option<(NESlice<'_, IndexedOtherSegment>, OtherWidth)> {
        self.other
            .as_ref()
            .map(|(xs, w)| (xs.as_nonempty_slice(), *w))
    }

    /// Return parsed OTHER segment data
    #[cfg(feature = "python")]
    #[must_use]
    pub fn py_other(&self) -> PyParsedOtherSegments {
        let (ws, w) = self.other()?;
        Some((FcsNEVec(ws.into_nonempty_iter().copied().collect()), w))
    }

    /// Make new collection of HEADER segments.
    ///
    /// Will throw errors for overlapping segments.
    pub fn try_new(
        text: PrimaryTextSegment,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        os: ParsedOtherSegments,
    ) -> Result<Self, SegmentValidationErrors> {
        // set limit to zero so that any overlap causes an error
        Self::try_new_with_limit(text, data, analysis, os, 0.into())
            .group()
            .resolve_nowarn()
    }

    /// Make new collection of HEADER segments.
    ///
    /// Will try to fix overlaps subject to limit or will throw error.
    ///
    /// With throw error if any segment overlaps with HEADER itself.
    #[must_use]
    pub(crate) fn try_new_with_limit(
        text: PrimaryTextSegment,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        os: ParsedOtherSegments,
        limit: OverlapCorrectionLimit,
    ) -> ErrorsResult<Self, (), SegmentValidationError> {
        let mut ret = Self::new(text, data, analysis, os);
        let es = ret.validate(limit);
        ErrorsResult::new_from_err_iter(es, (), ()).set_ok_value(ret)
    }

    /// Return DATA and ANALYSIS segments in struct.
    ///
    /// The returned struct encodes DATA and ANALYSIS segments that should
    /// actually be used for reading these segments. Since this takes no inputs,
    /// TEXT segments cannot be included, therefore it should only be called in
    /// 2.0 code which does not include TEXT segmetns.
    pub(crate) fn as_dataset_segments_2_0(&self) -> DatasetSegments {
        let d = self.data.into_any();
        let a = self.analysis.into_any();
        DatasetSegments::new(
            d,
            a,
            TEXTOffsetOrigin::EmptyTEXT,
            TEXTOffsetOrigin::EmptyTEXT,
            None,
            None,
        )
    }

    /// Ensure supp TEXT does not overlap other offsets or HEADER.
    ///
    /// This will either truncate offsets here (the HEADER) or truncate the
    /// supplied offset (from TEXT), limit permitting.
    pub(crate) fn validate_supp_text<I>(
        &mut self,
        s: &mut TEXTSegment<I>,
        limit: OverlapCorrectionLimit,
    ) -> impl Iterator<Item = SegmentValidationError>
    where
        I: HasRegion + HasSegmentName<SegmentFromTEXT, Params = ()>,
    {
        let contains = self
            .contains_segment(s, ())
            .map(SegmentValidationError::from);
        let hs = self.as_mut_nonempty_segments();
        let overlaps = Self::fix_text_overlap(hs, s, limit)
            .into_iter()
            .map(SegmentValidationError::from);
        contains.into_iter().chain(overlaps)
    }

    /// Ensure DATA/ANALYSIS from TEXT do not overlap other offsets or HEADER.
    ///
    /// This will either truncate offsets here (the HEADER) or truncate the
    /// supplied offset (from TEXT), limit permitting.
    pub(crate) fn validate_text_data_or_analysis<I>(
        &mut self,
        s: &mut TEXTSegment<I>,
        limit: OverlapCorrectionLimit,
    ) -> impl Iterator<Item = SegmentValidationError>
    where
        I: HasRegion + HasSegmentName<SegmentFromTEXT, Params = ()> + IsDataOrAnalysis,
    {
        let contains = self
            .contains_segment(s, ())
            .map(SegmentValidationError::from);
        let hs = self.as_mut_nonempty_segments_filtered::<I>();
        let overlaps = Self::fix_text_overlap(hs, s, limit)
            .into_iter()
            .map(SegmentValidationError::from);
        contains.into_iter().chain(overlaps)
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
    ) -> Vec<NextdataOffsetsError> {
        let nn = u64::from(n);
        if nn == 0 {
            vec![]
        } else {
            let mut errors = vec![];
            for (mut r, s) in self.as_mut_nonempty_segments() {
                let overlap = (s.end + 1).saturating_sub(nn);
                if overlap <= limit.0 {
                    r.truncate(overlap);
                } else {
                    errors.push(NextdataOffsetsError::new(n, s));
                }
            }
            errors
        }
    }

    /// Set OTHER segment at index to empty.
    ///
    /// Will panic if out of bounds.
    pub(crate) fn remove_other(&mut self, i: usize) {
        if let Some((xs, _)) = self.other.as_mut() {
            xs[i].seg = Segment::default();
        }
    }

    fn as_others(&self) -> impl Iterator<Item = &IndexedOtherSegment> {
        self.other.iter().flat_map(|(os, _)| os.iter())
    }

    fn fix_text_overlap<'a, I>(
        xs: impl IntoIterator<Item = (AnyHeaderSegmentMut<'a>, GenericSegment)>,
        s: &mut TEXTSegment<I>,
        limit: OverlapCorrectionLimit,
    ) -> Vec<SegmentOverlapError>
    where
        I: HasRegion + HasSegmentName<SegmentFromTEXT, Params = ()>,
    {
        // ASSUME incoming iterator is sorted (no debug assert since this would
        // consume iterator)
        if let Some(txt_seg) = s.try_as_generic(()) {
            let err = |hdr_seg| SegmentOverlapError::new(hdr_seg, txt_seg);
            let mut errors = vec![];
            let mut it = xs.into_iter();
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
                        errors.push(err(hdr_seg));
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
                    errors.push(err(hdr_seg));
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
                errors.push(err(hdr_seg));
            }
            errors
        } else {
            vec![]
        }
    }

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
        let t = self.contains_segment(&self.text, ());
        let d = self.contains_segment(&self.data, ());
        let a = self.contains_segment(&self.analysis, ());
        let os = self
            .as_others()
            .enumerate()
            .map(|(i, o)| self.contains_segment(&o.seg, i));
        [t, d, a].into_iter().chain(os).flatten()
    }

    fn contains_segment<I, S, T0>(
        &self,
        s: &Segment<I, S, T0>,
        args: I::Params,
    ) -> Option<InHeaderError>
    where
        I: HasRegion + HasSegmentName<S>,
        S: HasSource,
        T0: Into<u64> + Copy,
    {
        let q = s.try_as_generic(args)?;
        (q.begin < self.nbytes()).then_some(InHeaderError(q))
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

    fn other_offset_nbytes(&self) -> u64 {
        self.other.as_ref().map_or(0, |(os, width)| {
            let n = u64::try_from(usize::from(os.len())).expect("usize overflow");
            n * u64::from(u8::from(*width))
        })
    }
}

/// Any mutable reference to segment from HEADER.
enum AnyHeaderSegmentMut<'a> {
    Text(&'a mut PrimaryTextSegment),
    Data(&'a mut HeaderDataSegment),
    Analysis(&'a mut HeaderAnalysisSegment),
    Other(&'a mut IndexedOtherSegment),
}

impl AnyHeaderSegmentMut<'_> {
    fn try_as_generic(&self) -> Option<GenericSegment> {
        match self {
            Self::Text(s) => s.try_as_generic(()),
            Self::Data(s) => s.try_as_generic(()),
            Self::Analysis(s) => s.try_as_generic(()),
            Self::Other(s) => s.seg.try_as_generic(s.index),
        }
    }

    fn truncate(&mut self, n: u64) {
        match self {
            Self::Text(s) => s.truncate(n),
            Self::Data(s) => s.truncate(n),
            Self::Analysis(s) => s.truncate(n),
            Self::Other(s) => s.seg.truncate(n),
        }
    }
}

/// Error when validating segments in HEADER
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SegmentValidationError {
    Overlap(SegmentOverlapError),
    InHeader(InHeaderError),
}

pub type SegmentValidationErrors = ErrorGroup<SegmentValidationError, SegmentValidationSummary>;

def_summary!(
    pub SegmentValidationSummary,
    "Error when making new HEADER segments"
);

/// Error when a non-empty segment occurs within the first 58 bytes of the file.
#[derive(Debug, Error, PartialEq, Clone)]
#[error("{0} is within HEADER region")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct InHeaderError(GenericSegment);

/// Error when segment offsets exceed $NEXTDATA.
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error("{offsets} exceeds $NEXTDATA ({})", u64::from(self.nextdata))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct NextdataOffsetsError {
    nextdata: Nextdata,
    offsets: GenericSegment,
}

/// The length of the HEADER without OTHER segments.
pub(crate) const HEADER_LEN: u8 = 58;
