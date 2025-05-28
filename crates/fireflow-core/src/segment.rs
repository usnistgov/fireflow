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
    // this isn't phantom because some functions return segments from either
    // HEADER or TEXT and we want to allow either, which requires an enum
    src: S,
}

/// Denotes a correction for a segment
#[derive(Default, Clone, Copy)]
pub struct OffsetCorrection<I, S> {
    pub begin: i32,
    pub end: i32,
    _id: PhantomData<I>,
    _src: PhantomData<S>,
}

// enum_from!(
//     /// Denotes a segment came from either HEADER or TEXT
//     #[derive(Clone, Copy)]
//     pub SegmentFromAnywhere,
//     [Header, SegmentFromHeader],
//     [TEXT, SegmentFromTEXT]
// );

/// Denotes a segment came from either HEADER
#[derive(Default, Debug, Clone, Copy, Serialize)]
pub struct SegmentFromHeader;

/// Denotes a segment came from either TEXT
#[derive(Default, Debug, Clone, Copy, Serialize)]
pub struct SegmentFromTEXT;

/// Denotes the segment pertains to primary TEXT
#[derive(Default, Debug, Clone, Copy, Serialize)]
pub struct PrimaryTextSegmentId;

/// Denotes the segment pertains to supplemental TEXT
#[derive(Default, Debug, Clone, Copy, Serialize)]
pub struct SupplementalTextSegmentId;

/// Denotes the segment pertains to DATA
#[derive(Default, Debug, Clone, Copy, Serialize)]
pub struct DataSegmentId;

/// Denotes the segment pertains to ANALYSIS
#[derive(Default, Debug, Clone, Copy, Serialize)]
pub struct AnalysisSegmentId;

/// Intermediate store for TEXT keywords that might be parsed as a segment
pub struct SegmentKeywords<I> {
    pub begin: Option<String>,
    pub end: Option<String>,
    _id: PhantomData<I>,
}

pub type PrimaryTextSegment = SpecificSegment<PrimaryTextSegmentId, SegmentFromHeader>;
pub type SupplementalTextSegment = SpecificSegment<SupplementalTextSegmentId, SegmentFromTEXT>;

type DataSegment<S> = SpecificSegment<DataSegmentId, S>;
pub type HeaderDataSegment = DataSegment<SegmentFromHeader>;
pub type TEXTDataSegment = DataSegment<SegmentFromTEXT>;

type AnalysisSegment<S> = SpecificSegment<AnalysisSegmentId, S>;
pub type HeaderAnalysisSegment = AnalysisSegment<SegmentFromHeader>;
pub type TEXTAnalysisSegment = AnalysisSegment<SegmentFromTEXT>;

pub type HeaderSegment<I> = SpecificSegment<I, SegmentFromHeader>;
pub type TEXTSegment<I> = SpecificSegment<I, SegmentFromTEXT>;
pub type AnySegment<I> = SpecificSegment<I, SegmentFromAnywhere>;

#[derive(Clone, Copy)]
pub struct SegmentFromAnywhere;

pub type AnyDataSegment = DataSegment<SegmentFromAnywhere>;
pub type AnyAnalysisSegment = AnalysisSegment<SegmentFromAnywhere>;

/// Operations to obtain segment from TEXT keywords
pub(crate) trait LookupSegment
where
    Self: Sized,
    Self: HasRegion,
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

    fn remove_req<W>(
        kws: &mut StdKeywords,
        corr: OffsetCorrection<Self, SegmentFromTEXT>,
    ) -> DeferredResult<TEXTSegment<Self>, W, ReqSegmentError>
    where
        Self::B: ReqMetaKey,
        Self::E: ReqMetaKey,
    {
        let x0 = Self::B::remove_meta_req(kws).map_err(|e| e.into());
        let x1 = Self::E::remove_meta_req(kws).map_err(|e| e.into());
        x0.zip(x1)
            .and_then(|(y0, y1)| {
                SpecificSegment::try_new(y0.into(), y1.into(), corr, SegmentFromTEXT)
                    .into_mult::<ReqSegmentError>()
            })
            .into_deferred1()
    }

    fn remove_opt<E>(
        kws: &mut StdKeywords,
        corr: OffsetCorrection<Self, SegmentFromTEXT>,
    ) -> Tentative<Option<TEXTSegment<Self>>, OptSegmentError, E>
    where
        Self::B: OptMetaKey,
        Self::E: OptMetaKey,
    {
        let x0 = Self::B::remove_meta_opt(kws).map_err(|e| e.into());
        let x1 = Self::E::remove_meta_opt(kws).map_err(|e| e.into());
        // TODO this unwrap thing isn't totally necessary
        x0.zip(x1)
            .and_then(|(y0, y1)| {
                y0.0.zip(y1.0)
                    .map(|(z0, z1)| {
                        SpecificSegment::try_new(z0.into(), z1.into(), corr, SegmentFromTEXT)
                            .into_mult()
                    })
                    .transpose()
            })
            .map_or_else(
                |ws| Tentative::new(None.into(), ws.into(), vec![]),
                Tentative::new1,
            )
    }

    fn req(
        mut kws: SegmentKeywords<Self>,
        corr: OffsetCorrection<Self, SegmentFromTEXT>,
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
        corr: OffsetCorrection<Self, SegmentFromTEXT>,
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

/// Denotes that a type comes from a specific part of the FCS file
pub trait HasSource {
    const SRC: &'static str;
}

/// Denotes that a type pertains to a region of the FCS file
pub trait HasRegion {
    const REGION: &'static str;
}

impl LookupSegment for AnalysisSegmentId {
    type B = Beginanalysis;
    type E = Endanalysis;
}

impl LookupSegment for DataSegmentId {
    type B = Begindata;
    type E = Enddata;
}

impl LookupSegment for SupplementalTextSegmentId {
    type B = Beginstext;
    type E = Endstext;
}

impl HasSource for SegmentFromHeader {
    const SRC: &'static str = "HEADER";
}

impl HasSource for SegmentFromTEXT {
    const SRC: &'static str = "TEXT";
}

impl HasRegion for AnalysisSegmentId {
    const REGION: &'static str = "ANALYSIS";
}

impl HasRegion for DataSegmentId {
    const REGION: &'static str = "DATA";
}

impl HasRegion for SupplementalTextSegmentId {
    const REGION: &'static str = "STEXT";
}

impl HasRegion for PrimaryTextSegmentId {
    const REGION: &'static str = "TEXT";
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

impl<I, S> OffsetCorrection<I, S> {
    pub fn new(begin: i32, end: i32) -> Self {
        Self {
            begin,
            end,
            _id: PhantomData,
            _src: PhantomData,
        }
    }
}

impl<I, S> SpecificSegment<I, S> {
    pub fn try_new(
        begin: u32,
        end: u32,
        corr: OffsetCorrection<I, S>,
        src: S,
    ) -> Result<Self, SegmentError>
    where
        I: HasRegion,
        S: HasSource,
    {
        Segment::try_new::<I, S>(begin, end, corr).map(|inner| Self {
            inner,
            _id: PhantomData,
            src,
        })
    }
}

impl<I: Copy> HeaderSegment<I> {
    pub fn or(
        self,
        other: TEXTSegment<I>,
    ) -> Result<AnySegment<I>, (AnySegment<I>, SegmentMismatchWarning<I>)> {
        if other.inner != self.inner && !self.inner.is_empty() {
            Err((
                self.into_any(),
                SegmentMismatchWarning {
                    header: self,
                    text: other,
                },
            ))
        } else {
            Ok(SpecificSegment {
                inner: other.inner,
                _id: PhantomData,
                src: SegmentFromAnywhere,
            })
        }
    }

    pub fn into_any(self) -> AnySegment<I> {
        SpecificSegment {
            inner: self.inner,
            _id: PhantomData,
            src: SegmentFromAnywhere,
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
    pub fn try_new<I: HasRegion, S: HasSource>(
        begin: u32,
        end: u32,
        corr: OffsetCorrection<I, S>,
    ) -> Result<Self, SegmentError> {
        let x = i64::from(begin) + i64::from(corr.begin);
        let y = i64::from(end) + i64::from(corr.end);
        let err = |kind| {
            Err(SegmentError {
                begin,
                end,
                corr_begin: corr.begin,
                corr_end: corr.end,
                kind,
                location: I::REGION,
                src: S::SRC,
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

    pub fn try_adjust<I, S>(self, corr: OffsetCorrection<I, S>) -> Result<Self, SegmentError>
    where
        I: HasRegion,
        S: HasSource,
    {
        Self::try_new::<I, S>(self.begin, self.end(), corr)
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
    corr_begin: i32,
    corr_end: i32,
    kind: SegmentErrorKind,
    location: &'static str,
    src: &'static str,
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
        let begin_text = offset_text(self.begin, self.corr_begin);
        let end_text = offset_text(self.end, self.corr_end);
        let kind_text = match &self.kind {
            SegmentErrorKind::Range => "Offset out of range",
            SegmentErrorKind::Inverted => "Begin after end",
        };
        write!(
            f,
            "{kind_text} for {} segment from {}; begin={begin_text}, end={end_text}",
            self.location, self.src,
        )
    }
}

pub struct SegmentDefaultWarning<I>(PhantomData<I>);

impl<I> Default for SegmentDefaultWarning<I> {
    fn default() -> Self {
        SegmentDefaultWarning(PhantomData)
    }
}

impl<I> fmt::Display for SegmentDefaultWarning<I>
where
    I: HasRegion,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "could not obtain {} segment offset from TEXT, \
             using offsets from HEADER",
            I::REGION,
        )
    }
}

pub struct SegmentMismatchWarning<S> {
    header: SpecificSegment<S, SegmentFromHeader>,
    text: SpecificSegment<S, SegmentFromTEXT>,
}

impl<I> fmt::Display for SegmentMismatchWarning<I>
where
    I: HasRegion,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "segments differ in HEADER ({}) and TEXT ({}) for {}, using TEXT",
            self.header.inner.fmt_pair(),
            self.text.inner.fmt_pair(),
            I::REGION,
        )
    }
}
