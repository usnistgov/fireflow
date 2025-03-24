use serde::Serialize;
use std::fmt;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};

/// A segment in an FCS file which is denoted by a pair of offsets
#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub struct Segment {
    begin: u32,
    length: u32,
}

/// The kind of segment in an FCS file.
#[derive(Debug)]
pub enum SegmentId {
    PrimaryText,
    SupplementalText,
    Analysis,
    Data,
    // TODO add Other (which will be indexed I think)
}

#[derive(Debug)]
enum SegmentErrorKind {
    Range,
    Inverted,
}

#[derive(Debug)]
struct SegmentError {
    begin: u32,
    end: u32,
    begin_delta: i32,
    end_delta: i32,
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
        let begin_text = offset_text(self.begin, self.begin_delta);
        let end_text = offset_text(self.end, self.end_delta);
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

impl Segment {
    pub fn try_new(begin: u32, end: u32, id: SegmentId) -> Result<Segment, String> {
        Self::try_new_adjusted(begin, end, 0, 0, id)
    }

    pub fn try_new_adjusted(
        begin: u32,
        end: u32,
        begin_delta: i32,
        end_delta: i32,
        id: SegmentId,
    ) -> Result<Segment, String> {
        let x = i64::from(begin) + i64::from(begin_delta);
        let y = i64::from(end) + i64::from(end_delta);
        let err = |kind| {
            Err(SegmentError {
                begin,
                end,
                begin_delta,
                end_delta,
                kind,
                id,
            }
            .to_string())
        };
        match (u32::try_from(x), u32::try_from(y)) {
            (Ok(new_begin), Ok(new_end)) => {
                if new_begin > new_end {
                    err(SegmentErrorKind::Inverted)
                } else {
                    Ok(Segment {
                        begin: new_begin,
                        length: new_begin - new_end,
                    })
                }
            }
            (_, _) => err(SegmentErrorKind::Range),
        }
    }

    pub fn is_unset(&self) -> bool {
        self.begin == 0 && self.length == 0
    }

    pub fn read<R: Read + Seek>(&self, h: &mut BufReader<R>, buf: &mut Vec<u8>) -> io::Result<()> {
        let begin = u64::from(self.begin);
        let nbytes = u64::from(self.nbytes());

        h.seek(SeekFrom::Start(begin))?;
        h.take(nbytes).read_to_end(buf)?;
        Ok(())
    }

    pub fn try_adjust(
        self,
        begin_delta: i32,
        end_delta: i32,
        id: SegmentId,
    ) -> Result<Segment, String> {
        Self::try_new_adjusted(self.begin, self.end(), begin_delta, end_delta, id)
    }

    pub fn nbytes(&self) -> u32 {
        self.length + 1
    }

    fn end(&self) -> u32 {
        self.begin + self.length
    }
}
