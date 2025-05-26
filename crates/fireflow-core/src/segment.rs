use crate::config::OffsetCorrection;

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
    pub fn try_new(
        begin: u32,
        end: u32,
        corr: OffsetCorrection,
        id: SegmentId,
    ) -> Result<Segment, SegmentError> {
        let x = i64::from(begin) + i64::from(corr.begin);
        let y = i64::from(end) + i64::from(corr.end);
        let err = |kind| {
            Err(SegmentError {
                begin,
                end,
                corr,
                kind,
                id,
            })
        };
        match (u32::try_from(x), u32::try_from(y)) {
            (Ok(new_begin), Ok(new_end)) => {
                if new_begin > new_end {
                    err(SegmentErrorKind::Inverted)
                } else {
                    Ok(Segment::new_unchecked(new_begin, new_end))
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

    pub fn try_adjust(
        self,
        corr: OffsetCorrection,
        id: SegmentId,
    ) -> Result<Segment, SegmentError> {
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

/// The kind of segment in an FCS file.
#[derive(Debug, Clone, Copy)]
pub enum SegmentId {
    PrimaryText,
    SupplementalText,
    Analysis,
    Data,
    // TODO add Other (which will be indexed I think)
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

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let x = match self {
            SegmentId::PrimaryText => "TEXT",
            SegmentId::SupplementalText => "STEXT",
            SegmentId::Analysis => "ANALYSIS",
            SegmentId::Data => "DATA",
        };
        write!(f, "{x}")
    }
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
