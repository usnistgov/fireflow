use crate::config::OverlapCorrectionLimit;
use crate::core::{DatasetSegments, OthersReader};
use crate::logging::{ErrorGroup, ErrorsResult};
use crate::macros::{def_summary, match_many_to_one};
use crate::segment::{
    GenericSegment, HasRegion, HasSource, HeaderAnalysisSegment, HeaderDataSegment,
    IsDataOrAnalysis, OtherSegment20, PrimaryTextSegment, Segment, SegmentOverlapError,
    TEXTSegment, UncorrectedSegment,
};
use crate::text::keywords::Nextdata;
use crate::validated::ascii_range::OtherWidth;

use derive_more::{AsRef, Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr};

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

pub(crate) type ParsedOtherSegments = Option<(NonEmpty<OtherSegment20>, OtherWidth)>;

impl ParsedHeaderSegments {
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

    /// Mae new collection of HEADER segments.
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
        ErrorsResult::new_err_from_iter(es, ()).set_ok_value(ret)
    }

    /// Return DATA and ANALYSIS segments in struct.
    pub(crate) fn as_dataset_segments(
        &self,
        data_uncorr: Option<UncorrectedSegment>,
        analysis_uncorr: Option<UncorrectedSegment>,
    ) -> DatasetSegments {
        let d = self.data.into_any();
        let a = self.analysis.into_any();
        DatasetSegments::new(d, a, data_uncorr, analysis_uncorr)
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
        I: HasRegion,
    {
        let contains = self.contains_segment(s).map(SegmentValidationError::from);
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
        I: HasRegion + IsDataOrAnalysis,
    {
        let contains = self.contains_segment(s).map(SegmentValidationError::from);
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

    fn as_others(&self) -> impl Iterator<Item = &OtherSegment20> {
        self.other.iter().flat_map(|(os, _)| os.iter())
    }

    fn fix_text_overlap<'a, I>(
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
            let n = u64::try_from(os.len()).expect("usize overflow");
            n * u64::from(u8::from(*width))
        })
    }
}

/// Any mutable reference to segment from HEADER.
enum AnyHeaderSegmentMut<'a> {
    Text(&'a mut PrimaryTextSegment),
    Data(&'a mut HeaderDataSegment),
    Analysis(&'a mut HeaderAnalysisSegment),
    Other(&'a mut OtherSegment20),
}

impl AnyHeaderSegmentMut<'_> {
    fn try_as_generic(&self) -> Option<GenericSegment> {
        match_many_to_one!(self, Self, [Analysis, Data, Text, Other], x, {
            x.try_as_generic()
        })
    }

    fn truncate(&mut self, n: u64) {
        match_many_to_one!(self, Self, [Analysis, Data, Text, Other], x, {
            x.truncate(n);
        });
    }
}

/// Error when validating segments in HEADER
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SegmentValidationError {
    Overlap(SegmentOverlapError),
    InHeader(InHeaderError),
}

pub type SegmentValidationErrors = ErrorGroup<SegmentValidationError, SegmentValidationSummary>;

def_summary!(
    SegmentValidationSummary,
    "Error when making new HEADER segments"
);

/// Error when a non-empty segment occurs within the first 58 bytes of the file.
#[derive(Debug, Error)]
#[error("{0} is within HEADER region")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
pub struct InHeaderError(GenericSegment);

/// Error when segment offsets exceed $NEXTDATA.
#[derive(Debug, Error, new)]
#[error("{offsets} exceed $NEXTDATA ({})", u64::from(self.nextdata))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FileLayoutError))]
pub struct NextdataOffsetsError {
    nextdata: Nextdata,
    offsets: GenericSegment,
}

/// The length of the HEADER without OTHER segments.
pub(crate) const HEADER_LEN: u8 = 58;
