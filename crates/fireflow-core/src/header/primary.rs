use crate::header::final_offsets::HEADER_LEN;
use crate::logging::ErrorsResult;
use crate::segment::read::{
    AreNamedOffsets, HasRegion, HasSource, HeaderAnalysisOffsets, HeaderDataOffsets,
    HeaderOffsetsName, IsOffsetPair as _, NamedOffsets, Offsets, PrimaryTextOffsets,
};

use derive_more::Display;
use thiserror::Error;

use std::fmt;

#[cfg(feature = "python")]
use {fireflow_core_proc::DisplayAsPyErr, fireflow_types::python as py};

/// The primary segment offsets as read from HEADER.
///
/// These are validated such that no segment overlaps with (primary) HEADER
/// itself (only the first 58 bytes). These will be checked later once the OTHER
/// segments are parsed in which case we will know the actual length of HEADER.
pub(crate) struct PrimaryHeaderOffsets {
    text: PrimaryTextOffsets,
    data: HeaderDataOffsets,
    analysis: HeaderAnalysisOffsets,
}

/// Error when a non-empty offset pair occurs within the primary HEADER region.
#[derive(Debug, Error, PartialEq, Clone, Display)]
#[display(
    "{} segment offsets with begin offset of {} is within first {HEADER_LEN} bytes of dataset.",
    self.0.name,
    self.0.begin,
)]
#[display(bound(N: fmt::Display))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
#[cfg_attr(feature = "python", bound(N: fmt::Display))]
pub struct InPrimaryHeaderError<N>(pub NamedOffsets<N>);

impl PrimaryHeaderOffsets {
    /// Make new collection of HEADER offsets.
    ///
    /// Will try to fix overlaps subject to limit or will throw error.
    ///
    /// With throw error if any offsets overlaps with HEADER itself.
    #[must_use]
    pub(crate) fn try_new(
        text: PrimaryTextOffsets,
        data: HeaderDataOffsets,
        analysis: HeaderAnalysisOffsets,
    ) -> ErrorsResult<Self, Self, InPrimaryHeaderError<HeaderOffsetsName>> {
        let ret = Self {
            text,
            data,
            analysis,
        };
        let errors = ret.contains_header_offsets();
        ErrorsResult::new_from_err_iter(errors, (), ()).set_deferred_value(ret)
    }

    pub(crate) fn split(self) -> (PrimaryTextOffsets, HeaderDataOffsets, HeaderAnalysisOffsets) {
        (self.text, self.data, self.analysis)
    }

    pub(crate) fn min_coord(&self) -> Option<u64> {
        [
            self.text.as_nonempty().map(|x| x.begin()),
            self.data.as_nonempty().map(|x| x.begin()),
            self.analysis.as_nonempty().map(|x| x.begin()),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    fn contains_header_offsets(
        &self,
    ) -> impl Iterator<Item = InPrimaryHeaderError<HeaderOffsetsName>> {
        let t = Self::contains_offsets(&self.text);
        let d = Self::contains_offsets(&self.data);
        let a = Self::contains_offsets(&self.analysis);
        [t, d, a].into_iter().flatten()
    }

    fn contains_offsets<I, S>(s: &Offsets<I, S>) -> Option<InPrimaryHeaderError<HeaderOffsetsName>>
    where
        I: HasRegion + AreNamedOffsets<HeaderOffsetsName, Params = ()>,
        S: HasSource,
    {
        let q = s.as_nonempty()?;
        (q.begin() < u64::from(HEADER_LEN)).then_some(InPrimaryHeaderError(q.as_named(())))
    }
}
