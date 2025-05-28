use crate::config::OffsetCorrection;
use crate::error::*;
use crate::macros::{enum_from, enum_from_disp, match_many_to_one};
use crate::text::keywords::*;
use crate::validated::standard::*;

use serde::Serialize;
use std::fmt;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::num::ParseIntError;
use std::str::FromStr;

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
    _id: PhantomData<I>,
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

/// Intermediate store for TEXT keywords that might be parsed as a segment
pub struct SegmentKeywords<I> {
    pub begin: Option<String>,
    pub end: Option<String>,
    _id: PhantomData<I>,
}

pub(crate) trait TEXTSegment
where
    Self: Sized,
    Self: SegmentHasLocation,
    Self::B: Into<u32>,
    Self::E: Into<u32>,
    Self::B: Key,
    Self::E: Key,
    Self::B: FromStr<Err = ParseIntError>,
    Self::E: FromStr<Err = ParseIntError>,
    <Self::B as FromStr>::Err: fmt::Display,
    <Self::E as FromStr>::Err: fmt::Display,
{
    type B;
    type E;

    fn lookup(kws: &mut StdKeywords) -> SegmentKeywords<Self> {
        SegmentKeywords {
            begin: kws.remove(&Self::B::std()),
            end: kws.remove(&Self::E::std()),
            _id: PhantomData,
        }
    }

    fn lookup_cloned(kws: &StdKeywords) -> SegmentKeywords<Self> {
        SegmentKeywords {
            begin: kws.get(&Self::B::std()).cloned(),
            end: kws.get(&Self::E::std()).cloned(),
            _id: PhantomData,
        }
    }

    fn req(
        mut kws: SegmentKeywords<Self>,
        corr: OffsetCorrection,
    ) -> MultiResult<SpecificSegment<Self, SegmentFromTEXT>, ReqSegmentError>
    where
        Self::B: ReqMetaKey,
        Self::E: ReqMetaKey,
    {
        let x0 = parse_req::<Self::B>(&mut kws.begin).map_err(|e| e.into());
        let x1 = parse_req::<Self::E>(&mut kws.end).map_err(|e| e.into());
        x0.zip(x1).and_then(|(y0, y1)| {
            SpecificSegment::try_new(y0.into(), y1.into(), corr, SegmentFromTEXT).into_mult()
        })
    }

    fn opt(
        mut kws: SegmentKeywords<Self>,
        corr: OffsetCorrection,
    ) -> MultiResult<Option<SpecificSegment<Self, SegmentFromTEXT>>, OptSegmentError>
    where
        Self::B: OptMetaKey,
        Self::E: OptMetaKey,
    {
        let x0 = parse_opt::<Self::B>(&mut kws.begin).map_err(|e| e.into());
        let x1 = parse_opt::<Self::E>(&mut kws.end).map_err(|e| e.into());
        x0.zip(x1).and_then(|(y0, y1)| {
            y0.zip(y1)
                .map(|(z0, z1)| {
                    SpecificSegment::try_new(z0.into(), z1.into(), corr, SegmentFromTEXT)
                        .into_mult()
                })
                .transpose()
        })
    }
}

impl TEXTSegment for AnalysisSegmentId {
    type B = Beginanalysis;
    type E = Endanalysis;
}

impl TEXTSegment for DataSegmentId {
    type B = Begindata;
    type E = Enddata;
}

impl TEXTSegment for SupplementalTextSegmentId {
    type B = Beginstext;
    type E = Endstext;
}

pub trait SegmentHasLocation {
    const NAME: &'static str;
}

impl SegmentHasLocation for AnalysisSegmentId {
    const NAME: &'static str = "ANALYSIS";
}

impl SegmentHasLocation for DataSegmentId {
    const NAME: &'static str = "DATA";
}

impl SegmentHasLocation for SupplementalTextSegmentId {
    const NAME: &'static str = "STEXT";
}

impl SegmentHasLocation for PrimaryTextSegmentId {
    const NAME: &'static str = "TEXT";
}

enum_from_disp!(
    pub ReqSegmentError,
    [Key, ReqKeyError<ParseIntError>],
    [Segment, SegmentError]
);

enum_from_disp!(
    pub OptSegmentError,
    [Key, ParseKeyError<ParseIntError>],
    [Segment, SegmentError]
);

impl<I, S> SpecificSegment<I, S> {
    pub fn try_new(
        begin: u32,
        end: u32,
        corr: OffsetCorrection,
        src: S,
    ) -> Result<Self, SegmentError>
    where
        I: SegmentHasLocation,
    {
        Segment::try_new::<I>(begin, end, corr).map(|inner| Self {
            inner,
            _id: PhantomData,
            src,
        })
    }

    pub fn into_any(self) -> SpecificSegment<I, SegmentFromAnywhere>
    where
        SegmentFromAnywhere: From<S>,
    {
        SpecificSegment {
            inner: self.inner,
            _id: PhantomData,
            src: self.src.into(),
        }
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
    pub fn try_new<I: SegmentHasLocation>(
        begin: u32,
        end: u32,
        corr: OffsetCorrection,
    ) -> Result<Self, SegmentError> {
        let x = i64::from(begin) + i64::from(corr.begin);
        let y = i64::from(end) + i64::from(corr.end);
        let err = |kind| {
            Err(SegmentError {
                begin,
                end,
                corr,
                kind,
                location: I::NAME,
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

    pub fn try_adjust<I: SegmentHasLocation>(
        self,
        corr: OffsetCorrection,
    ) -> Result<Self, SegmentError> {
        Self::try_new::<I>(self.begin, self.end(), corr)
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
    location: &'static str,
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
            self.location,
        )
    }
}
