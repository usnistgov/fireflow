use crate::config::OffsetCorrection;
use crate::macros::{enum_from, enum_from_disp, match_many_to_one};

use serde::Serialize;
use std::fmt;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};

/// A segment in an FCS file which is denoted by a pair of offsets
#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub struct Segment {
    begin: u32,
    pseudo_length: u32,
}

/// A segment that is specific to a region in the FCS file.
#[derive(Clone, Copy, Serialize)]
pub struct SpecificSegment<I, S> {
    pub inner: Segment,
    id: I,
    src: S,
}

enum_from!(
    /// Denotes a segment came from either HEADER or TEXT
    #[derive(Clone, Copy)]
    pub SegmentFromAnywhere,
    [Header, SegmentFromHeader],
    [TEXT, SegmentFromTEXT]
);

/// Denotes a segment came from either HEADER
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SegmentFromHeader;

/// Denotes a segment came from either TEXT
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SegmentFromTEXT;

enum_from_disp!(
    /// Denotes the segment pertains to any region in the FCS file
    pub SegmentId,
    [PrimaryText, PrimaryTextSegmentId],
    [SupplementalText, SupplementalTextSegmentId],
    [Analysis, AnalysisSegmentId],
    [Data, DataSegmentId]
);

/// Denotes the segment pertains to primary TEXT
#[derive(Debug, Clone, Copy, Serialize)]
pub struct PrimaryTextSegmentId;

/// Denotes the segment pertains to supplemental TEXT
#[derive(Debug, Clone, Copy, Serialize)]
pub struct SupplementalTextSegmentId;

/// Denotes the segment pertains to DATA
#[derive(Debug, Clone, Copy, Serialize)]
pub struct DataSegmentId;

/// Denotes the segment pertains to ANALYSIS
#[derive(Debug, Clone, Copy, Serialize)]
pub struct AnalysisSegmentId;

pub type PrimaryTextSegment = SpecificSegment<PrimaryTextSegmentId, SegmentFromHeader>;
pub type SupplementalTextSegment = SpecificSegment<SupplementalTextSegmentId, SegmentFromTEXT>;

type DataSegment<S> = SpecificSegment<DataSegmentId, S>;
pub type AnyDataSegment = DataSegment<SegmentFromAnywhere>;
pub type HeaderDataSegment = DataSegment<SegmentFromHeader>;
pub type TEXTDataSegment = DataSegment<SegmentFromTEXT>;

type AnalysisSegment<S> = SpecificSegment<AnalysisSegmentId, S>;
pub type AnyAnalysisSegment = AnalysisSegment<SegmentFromAnywhere>;
pub type HeaderAnalysisSegment = AnalysisSegment<SegmentFromHeader>;
pub type TEXTAnalysisSegment = AnalysisSegment<SegmentFromTEXT>;

impl<I, S> SpecificSegment<I, S> {
    pub fn try_new(
        begin: u32,
        end: u32,
        corr: OffsetCorrection,
        id: I,
        src: S,
    ) -> Result<Self, SegmentError>
    where
        I: Into<SegmentId> + Copy + Clone,
    {
        Segment::try_new(begin, end, corr, id).map(|inner| Self { inner, id, src })
    }

    pub fn into_any(self) -> SpecificSegment<I, SegmentFromAnywhere>
    where
        SegmentFromAnywhere: From<S>,
    {
        SpecificSegment {
            inner: self.inner,
            id: self.id,
            src: self.src.into(),
        }
    }

    pub fn id(&self) -> &I {
        &self.id
    }
}

impl Segment {
    /// Make new segment and check bounds to ensure validity
    ///
    /// Will return error explaining why bounds were invalid if failed.
    ///
    /// Begin and End are treated as they are in an FCS file, where Begin points
    /// to first byte and End points to last byte. As such, the only way to
    /// make a zero-length segment is to have (b, b-1) since the real ending
    /// *offset* will be one after End.
    ///
    /// As a consequence of the above, "unset segments" given as (0,0) are
    /// actually 1 byte long. There is no way to represent a zero-length segment
    /// starting at 0 unless we use signed ints.
    pub fn try_new<I: Into<SegmentId>>(
        begin: u32,
        end: u32,
        corr: OffsetCorrection,
        id: I,
    ) -> Result<Self, SegmentError> {
        let x = i64::from(begin) + i64::from(corr.begin);
        let y = i64::from(end) + i64::from(corr.end);
        let err = |kind| {
            Err(SegmentError {
                begin,
                end,
                corr,
                kind,
                id: id.into(),
            })
        };
        match (u32::try_from(x), u32::try_from(y)) {
            (Ok(new_begin), Ok(new_end)) => {
                if new_begin > new_end {
                    err(SegmentErrorKind::Inverted)
                } else {
                    Ok(Self::new_unchecked(new_begin, new_end))
                }
            }
            (_, _) => err(SegmentErrorKind::Range),
        }
    }

    pub fn h_read<R: Read + Seek>(
        &self,
        h: &mut BufReader<R>,
        buf: &mut Vec<u8>,
    ) -> io::Result<()> {
        let begin = u64::from(self.begin);
        let nbytes = u64::from(self.len());

        h.seek(SeekFrom::Start(begin))?;
        h.take(nbytes).read_to_end(buf)?;
        Ok(())
    }

    pub fn try_adjust(self, corr: OffsetCorrection, id: SegmentId) -> Result<Self, SegmentError> {
        Self::try_new(self.begin, self.end(), corr, id)
    }

    pub fn len(&self) -> u32 {
        // NOTE In FCS a 0,0 means "empty" but this also means one byte
        // according to the spec's on definitions. The first number points to
        // the first byte in a segment, and the second number points to the last
        // byte, therefore 0,0 means "0 is both the first and last byte, which
        // also means there is one byte".
        if self.is_empty() {
            0
        } else {
            self.pseudo_length + 1
        }
    }

    pub fn is_empty(&self) -> bool {
        self.begin == 0 && self.pseudo_length == 0
    }

    pub fn begin(&self) -> u32 {
        self.begin
    }

    pub fn end(&self) -> u32 {
        self.begin + self.pseudo_length
    }

    pub fn fmt_pair(&self) -> String {
        format!("{},{}", self.begin(), self.end())
    }

    fn new_unchecked(begin: u32, end: u32) -> Segment {
        Segment {
            begin,
            pseudo_length: end - begin,
        }
    }
}

impl fmt::Display for PrimaryTextSegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "TEXT")
    }
}

impl fmt::Display for SupplementalTextSegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "STEXT")
    }
}

impl fmt::Display for AnalysisSegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "ANALYSIS")
    }
}

impl fmt::Display for DataSegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "DATA")
    }
}

#[derive(Debug)]
pub enum SegmentErrorKind {
    Range,
    Inverted,
}

pub struct SegmentError {
    begin: u32,
    end: u32,
    corr: OffsetCorrection,
    kind: SegmentErrorKind,
    id: SegmentId,
}

impl fmt::Display for SegmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let offset_text = |x, delta| {
            if delta == 0 {
                format!("{}", x)
            } else {
                format!("{} ({}))", x, delta)
            }
        };
        let begin_text = offset_text(self.begin, self.corr.begin);
        let end_text = offset_text(self.end, self.corr.end);
        let kind_text = match &self.kind {
            SegmentErrorKind::Range => "Offset out of range",
            SegmentErrorKind::Inverted => "Begin after end",
        };
        write!(
            f,
            "{kind_text} for {} segment; begin={begin_text}, end={end_text}",
            self.id,
        )
    }
}
