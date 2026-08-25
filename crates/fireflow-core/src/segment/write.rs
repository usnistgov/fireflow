//! Types and methods to deal with offsets when writing FCS files.

use super::KeyedOffsets;
use crate::text::keyword_enum::{Keyword0FromValue as _, OffsetKeyword, SplitKeyword0};
use crate::validated::ascii_uint::{UintSpacePad8, UintZeroPad20};

use fireflow_types::segment::{
    AnalysisSegmentId, DataSegmentId, OffsetsFromHeader, OffsetsFromTEXT, OtherSegmentId,
    PrimaryTextSegmentId, SupplementalTextSegmentId,
};

use derive_more::Display;
use derive_new::new;
use num_traits::identities::Zero;

use std::marker::PhantomData;

/// An offset pair corresponding to a specific byte sequence that is to be written.
#[derive(Clone, Copy, Display, new)]
// ASSUME the display trait for the inner type will render with the
// proper number of characters
#[display("{begin}{end}")]
#[new(visibility = "")]
pub struct OffsetsToWrite<I, S, T> {
    begin: T,
    end: T,
    _id: PhantomData<I>,
    _src: PhantomData<S>,
}

impl<I, S, T: Zero> Default for OffsetsToWrite<I, S, T> {
    fn default() -> Self {
        Self::new(T::zero(), T::zero())
    }
}

pub type PrimaryTextOffsetsToWrite =
    OffsetsToWrite<PrimaryTextSegmentId, OffsetsFromHeader, UintSpacePad8>;
pub type SupplementalTextOffsetsToWrite =
    OffsetsToWrite<SupplementalTextSegmentId, OffsetsFromTEXT, UintZeroPad20>;

type DataOffsetsToWrite<S, T> = OffsetsToWrite<DataSegmentId, S, T>;
pub type HeaderDataOffsetsToWrite = DataOffsetsToWrite<OffsetsFromHeader, UintSpacePad8>;
pub type TEXTDataOffsetsToWrite = DataOffsetsToWrite<OffsetsFromTEXT, UintZeroPad20>;

type AnalysisOffsetsToWrite<S, T> = OffsetsToWrite<AnalysisSegmentId, S, T>;
pub type HeaderAnalysisOffsetsToWrite = AnalysisOffsetsToWrite<OffsetsFromHeader, UintSpacePad8>;
pub type TEXTAnalysisOffsetsToWrite = AnalysisOffsetsToWrite<OffsetsFromTEXT, UintZeroPad20>;

pub type HeaderOffsetsToWrite<I> = OffsetsToWrite<I, OffsetsFromHeader, UintSpacePad8>;
pub type TEXTOffsetsToWrite<I> = OffsetsToWrite<I, OffsetsFromTEXT, UintZeroPad20>;
pub type OtherOffsetsToWrite<T> = OffsetsToWrite<OtherSegmentId, OffsetsFromHeader, T>;

impl<I, S, T> OffsetsToWrite<I, S, T> {
    /// Return true if segment has 0 bytes
    pub fn is_empty(&self) -> bool
    where
        T: Zero,
    {
        self.begin.is_zero() && self.end.is_zero()
    }

    /// Return byte after end of segment if applicable
    pub fn try_next_byte(&self) -> Option<u64>
    where
        T: Copy + Into<u64> + Zero,
    {
        (!self.is_empty()).then(|| self.end.into() + 1)
    }

    pub fn try_new_with_len(begin: u64, length: u64) -> Result<Self, <u64 as TryInto<T>>::Error>
    where
        u64: TryInto<T>,
        T: Zero,
    {
        if length == 0 {
            Ok(Self::default())
        } else {
            Ok(Self::new(
                begin.try_into()?,
                (begin + length - 1).try_into()?,
            ))
        }
    }

    pub fn new_with_len(begin: u64, length: u64) -> Self
    where
        u64: Into<T>,
        T: Zero,
    {
        if length == 0 {
            Self::default()
        } else {
            Self::new(begin.into(), (begin + length - 1).into())
        }
    }
}

impl<I> TEXTOffsetsToWrite<I> {
    /// Convert TEXT segment to HEADER segment.
    ///
    /// If offsets are too big, return an empty segment.
    pub fn as_header(&self) -> HeaderOffsetsToWrite<I> {
        let br = u64::from(self.begin).try_into();
        let er = u64::from(self.end).try_into();
        if let (Ok(begin), Ok(end)) = (br, er) {
            OffsetsToWrite::new(begin, end)
        } else {
            OffsetsToWrite::default()
        }
    }

    pub fn keywords(&self) -> [OffsetKeyword; 2]
    where
        I: KeyedOffsets,
        I::B: From<UintZeroPad20>,
        I::E: From<UintZeroPad20>,
        OffsetKeyword: From<SplitKeyword0<I::B>> + From<SplitKeyword0<I::E>>,
    {
        [
            OffsetKeyword::from_value(I::B::from(self.begin)),
            OffsetKeyword::from_value(I::E::from(self.end)),
        ]
    }
}
