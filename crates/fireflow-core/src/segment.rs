use crate::error::*;
use crate::macros::{enum_from, enum_from_disp, match_many_to_one};
use crate::text::keywords::*;
use crate::validated::ascii_uint::*;
use crate::validated::standard::*;

use itertools::Itertools;
use nonempty::NonEmpty;
use serde::Serialize;
use std::fmt;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::num::ParseIntError;
use std::str::FromStr;

/// A segment in an FCS file which is denoted by a pair of offsets
#[derive(Debug, Clone, Copy, Serialize, PartialEq, Default)]
pub enum Segment<T> {
    NonEmpty(NonEmptySegment<T>),
    #[default]
    Empty,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq)]
pub struct NonEmptySegment<T> {
    begin: T,
    end: T,
}

/// A segment that is specific to a region in the FCS file.
#[derive(Clone, Copy, Serialize, Default)]
pub struct SpecificSegment<I, S, T> {
    pub inner: Segment<T>,
    _id: PhantomData<I>,
    _src: PhantomData<S>,
}

/// Denotes a correction for a segment
#[derive(Default, Clone, Copy)]
pub struct OffsetCorrection<I, S> {
    pub begin: i32,
    pub end: i32,
    _id: PhantomData<I>,
    _src: PhantomData<S>,
}

/// Denotes a segment came from HEADER
#[derive(Default, Debug, Clone, Copy, Serialize)]
pub struct SegmentFromHeader;

/// Denotes a segment came from TEXT
#[derive(Default, Debug, Clone, Copy, Serialize)]
pub struct SegmentFromTEXT;

/// Denotes a segment came from either TEXT or HEADER
#[derive(Clone, Copy)]
pub struct SegmentFromAnywhere;

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

/// Denotes the segment pertains to OTHER (indexed from 0)
#[derive(Default, Debug, Clone, Copy, Serialize)]
pub struct OtherSegmentId;

pub type PrimaryTextSegment = SpecificSegment<PrimaryTextSegmentId, SegmentFromHeader, Uint8Digit>;
pub type SupplementalTextSegment =
    SpecificSegment<SupplementalTextSegmentId, SegmentFromTEXT, Uint20Char>;

type DataSegment<S, T> = SpecificSegment<DataSegmentId, S, T>;
pub type HeaderDataSegment = DataSegment<SegmentFromHeader, Uint8Digit>;
pub type TEXTDataSegment = DataSegment<SegmentFromTEXT, Uint20Char>;

type AnalysisSegment<S, T> = SpecificSegment<AnalysisSegmentId, S, T>;
pub type HeaderAnalysisSegment = AnalysisSegment<SegmentFromHeader, Uint8Digit>;
pub type TEXTAnalysisSegment = AnalysisSegment<SegmentFromTEXT, Uint20Char>;

pub type HeaderSegment<I> = SpecificSegment<I, SegmentFromHeader, Uint8Digit>;
pub type TEXTSegment<I> = SpecificSegment<I, SegmentFromTEXT, Uint20Char>;
pub type AnySegment<I> = SpecificSegment<I, SegmentFromAnywhere, u64>;

pub type HeaderCorrection<I> = OffsetCorrection<I, SegmentFromHeader>;
pub type TEXTCorrection<I> = OffsetCorrection<I, SegmentFromTEXT>;

pub type AnyDataSegment = DataSegment<SegmentFromAnywhere, u64>;
pub type AnyAnalysisSegment = AnalysisSegment<SegmentFromAnywhere, u64>;

pub type OtherSegment = SpecificSegment<OtherSegmentId, SegmentFromHeader, Uint8Digit>;

pub(crate) type ReqSegResult<T> =
    DeferredResult<AnySegment<T>, ReqSegmentWithDefaultWarning<T>, ReqSegmentWithDefaultError<T>>;

pub(crate) type OptSegTentative<T> =
    Tentative<AnySegment<T>, OptSegmentWithDefaultWarning<T>, SegmentMismatchWarning<T>>;

/// Operations to obtain optional segment from TEXT keywords
pub(crate) trait KeyedSegment
where
    Self: Sized,
    Self::B: Key,
    Self::E: Key,
{
    type B;
    type E;
}

/// Operations to obtain required segment from TEXT keywords
pub(crate) trait KeyedReqSegment
where
    Self: KeyedSegment,
    Self: HasRegion,
    Self::B: Into<Uint20Char>,
    Self::E: Into<Uint20Char>,
    Self::B: ReqMetaKey,
    Self::E: ReqMetaKey,
    Self::B: FromStr<Err = ParseIntError>,
    Self::E: FromStr<Err = ParseIntError>,
{
    fn get_or(
        kws: &StdKeywords,
        corr: TEXTCorrection<Self>,
        default: HeaderSegment<Self>,
        enforce_match: bool,
        enforce_lookup: bool,
    ) -> ReqSegResult<Self>
    where
        Self: Copy,
    {
        let res = Self::get(kws, corr).def_map_errors(ReqSegmentWithDefaultError::Req);
        Self::default_or(res, default, enforce_lookup, enforce_match)
    }

    fn get<W>(
        kws: &StdKeywords,
        corr: TEXTCorrection<Self>,
    ) -> DeferredResult<TEXTSegment<Self>, W, ReqSegmentError> {
        Self::get_mult(kws, corr).mult_to_deferred()
    }

    fn get_mult(
        kws: &StdKeywords,
        corr: TEXTCorrection<Self>,
    ) -> MultiResult<TEXTSegment<Self>, ReqSegmentError> {
        Self::get_pair(kws)
            .map_err(|es| es.map(|e| e.into()))
            .and_then(|(y0, y1)| {
                SpecificSegment::try_new(y0.into(), y1.into(), corr).into_mult::<ReqSegmentError>()
            })
    }

    fn remove_or(
        kws: &mut StdKeywords,
        corr: TEXTCorrection<Self>,
        default: HeaderSegment<Self>,
        enforce_match: bool,
        enforce_lookup: bool,
    ) -> ReqSegResult<Self>
    where
        Self: Copy,
    {
        let res = Self::remove(kws, corr).def_map_errors(ReqSegmentWithDefaultError::Req);
        Self::default_or(res, default, enforce_lookup, enforce_match)
    }

    fn remove<W>(
        kws: &mut StdKeywords,
        corr: TEXTCorrection<Self>,
    ) -> DeferredResult<TEXTSegment<Self>, W, ReqSegmentError> {
        Self::remove_mult(kws, corr).mult_to_deferred()
    }

    fn remove_mult(
        kws: &mut StdKeywords,
        corr: TEXTCorrection<Self>,
    ) -> MultiResult<TEXTSegment<Self>, ReqSegmentError> {
        Self::remove_pair(kws)
            .map_err(|es| es.map(|e| e.into()))
            .and_then(|(y0, y1)| {
                SpecificSegment::try_new(y0.into(), y1.into(), corr).into_mult::<ReqSegmentError>()
            })
    }

    fn default_or(
        res: DeferredResult<
            TEXTSegment<Self>,
            ReqSegmentWithDefaultWarning<Self>,
            ReqSegmentWithDefaultError<Self>,
        >,
        default: HeaderSegment<Self>,
        enforce_lookup: bool,
        enforce_match: bool,
    ) -> ReqSegResult<Self>
    where
        Self: Copy,
    {
        res.map_or_else(
            |f| {
                if enforce_lookup {
                    Err(f)
                } else {
                    let mut tnt = f.unfail_with(default.into_any());
                    tnt.push_warning(SegmentDefaultWarning::default().into());
                    Ok(tnt)
                }
            },
            |tnt| {
                Ok(tnt.and_tentatively(|other| {
                    default.unless(other).map_or_else(
                        |(s, w)| Tentative::new_either(s, vec![w], enforce_match),
                        Tentative::new1,
                    )
                }))
            },
        )
    }

    fn get_pair(kws: &StdKeywords) -> MultiResult<(Self::B, Self::E), ReqKeyError<ParseIntError>> {
        let x0 = Self::B::get_meta_req(kws);
        let x1 = Self::E::get_meta_req(kws);
        x0.zip(x1)
    }

    fn remove_pair(
        kws: &mut StdKeywords,
    ) -> MultiResult<(Self::B, Self::E), ReqKeyError<ParseIntError>> {
        let x0 = Self::B::remove_meta_req(kws);
        let x1 = Self::E::remove_meta_req(kws);
        x0.zip(x1)
    }
}

/// Operations to obtain optional segment from TEXT keywords
pub(crate) trait KeyedOptSegment
where
    Self: KeyedSegment,
    Self: HasRegion,
    Self::B: Into<Uint20Char>,
    Self::E: Into<Uint20Char>,
    Self::B: OptMetaKey,
    Self::E: OptMetaKey,
    Self::B: FromStr<Err = ParseIntError>,
    Self::E: FromStr<Err = ParseIntError>,
{
    fn get_or(
        kws: &StdKeywords,
        corr: TEXTCorrection<Self>,
        default: HeaderSegment<Self>,
        enforce: bool,
    ) -> OptSegTentative<Self>
    where
        Self: Copy,
        Self::B: OptMetaKey,
        Self::E: OptMetaKey,
    {
        let res = Self::get(kws, corr).map_warnings(OptSegmentWithDefaultWarning::Opt);
        Self::default_or(res, default, enforce)
    }

    fn get<E>(
        kws: &StdKeywords,
        corr: TEXTCorrection<Self>,
    ) -> Tentative<Option<TEXTSegment<Self>>, OptSegmentError, E> {
        Self::get_pair(kws)
            .map_err(|es| es.map(|e| e.into()))
            .and_then(|x| {
                x.map(|(z0, z1)| SpecificSegment::try_new(z0.into(), z1.into(), corr).into_mult())
                    .transpose()
            })
            .map_or_else(
                |ws| Tentative::new(None, ws.into(), vec![]),
                Tentative::new1,
            )
    }

    fn remove_or(
        kws: &mut StdKeywords,
        corr: TEXTCorrection<Self>,
        default: HeaderSegment<Self>,
        enforce: bool,
    ) -> OptSegTentative<Self>
    where
        Self: Copy,
    {
        let res = Self::remove(kws, corr).map_warnings(OptSegmentWithDefaultWarning::Opt);
        Self::default_or(res, default, enforce)
    }

    fn remove<E>(
        kws: &mut StdKeywords,
        corr: TEXTCorrection<Self>,
    ) -> Tentative<Option<TEXTSegment<Self>>, OptSegmentError, E> {
        Self::remove_pair(kws)
            .map_err(|es| es.map(|e| e.into()))
            .and_then(|x| {
                x.map(|(z0, z1)| SpecificSegment::try_new(z0.into(), z1.into(), corr).into_mult())
                    .transpose()
            })
            .map_or_else(
                |ws| Tentative::new(None, ws.into(), vec![]),
                Tentative::new1,
            )
    }

    fn default_or(
        res: Tentative<
            Option<TEXTSegment<Self>>,
            OptSegmentWithDefaultWarning<Self>,
            SegmentMismatchWarning<Self>,
        >,
        default: HeaderSegment<Self>,
        enforce: bool,
    ) -> OptSegTentative<Self>
    where
        Self: Copy,
    {
        res.and_tentatively(|other| {
            other.map_or(Tentative::new1(default.into_any()), |o| {
                default.unless(o).map_or_else(
                    |(s, w)| Tentative::new_either(s, vec![w], enforce),
                    Tentative::new1,
                )
            })
        })
    }

    #[allow(clippy::type_complexity)]
    fn get_pair(
        kws: &StdKeywords,
    ) -> MultiResult<Option<(Self::B, Self::E)>, ParseKeyError<ParseIntError>> {
        let x0 = Self::B::get_meta_opt(kws).map(|x| x.0);
        let x1 = Self::E::get_meta_opt(kws).map(|x| x.0);
        x0.zip(x1).map(|(x, y)| x.zip(y))
    }

    #[allow(clippy::type_complexity)]
    fn remove_pair(
        kws: &mut StdKeywords,
    ) -> MultiResult<Option<(Self::B, Self::E)>, ParseKeyError<ParseIntError>> {
        let x0 = Self::B::remove_meta_opt(kws).map(|x| x.0);
        let x1 = Self::E::remove_meta_opt(kws).map(|x| x.0);
        x0.zip(x1).map(|(x, y)| x.zip(y))
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

impl KeyedSegment for AnalysisSegmentId {
    type B = Beginanalysis;
    type E = Endanalysis;
}

impl KeyedReqSegment for AnalysisSegmentId {}

impl KeyedOptSegment for AnalysisSegmentId {}

impl KeyedSegment for DataSegmentId {
    type B = Begindata;
    type E = Enddata;
}

impl KeyedReqSegment for DataSegmentId {}

impl KeyedSegment for SupplementalTextSegmentId {
    type B = Beginstext;
    type E = Endstext;
}

impl KeyedReqSegment for SupplementalTextSegmentId {}

impl KeyedOptSegment for SupplementalTextSegmentId {}

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

impl HasRegion for OtherSegmentId {
    const REGION: &'static str = "OTHER";
}

enum_from_disp!(
    pub ReqSegmentError,
    [Key, ReqKeyError<ParseIntError>],
    [Segment, SegmentError<Uint20Char>]
);

enum_from_disp!(
    pub OptSegmentError,
    [Key, ParseKeyError<ParseIntError>],
    [Segment, SegmentError<Uint20Char>]
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

impl<I, S, T> SpecificSegment<I, S, T> {
    pub fn try_new(begin: T, end: T, corr: OffsetCorrection<I, S>) -> Result<Self, SegmentError<T>>
    where
        I: HasRegion,
        S: HasSource,
        T: Default,
        T: Into<u64>,
        T: Into<i128>,
        T: TryFrom<i128>,
        T: PartialOrd,
        T: Copy,
    {
        Segment::try_new::<I, S>(begin, end, corr).map(|inner| Self {
            inner,
            _id: PhantomData,
            _src: PhantomData,
        })
    }

    pub(crate) fn try_new_with_len(
        begin: T,
        length: u64,
    ) -> Result<Self, <T as TryFrom<u64>>::Error>
    where
        T: Into<u64>,
        T: TryFrom<u64>,
        T: Copy,
    {
        if length == 0 {
            Ok(Segment::default())
        } else {
            (begin.into() + length - 1)
                .try_into()
                .map(|end| Segment::NonEmpty(NonEmptySegment::new_unchecked(begin, end)))
        }
        .map(|inner| Self {
            inner,
            _id: PhantomData,
            _src: PhantomData,
        })
    }
}

impl<I> TEXTSegment<I> {
    pub(crate) fn new_with_len(begin: Uint20Char, length: u64) -> Self {
        let inner = if length == 0 {
            Segment::default()
        } else {
            let end = Uint20Char::from(u64::from(begin) + length - 1);
            Segment::NonEmpty(NonEmptySegment::new_unchecked(begin, end))
        };
        Self {
            inner,
            _id: PhantomData,
            _src: PhantomData,
        }
    }

    /// Convert TEXT segment to HEADER segment.
    ///
    /// If offsets are too big, return an empty segment.
    pub(crate) fn as_header(&self) -> HeaderSegment<I> {
        let inner = self
            .inner
            .try_coords()
            .map_or(Segment::default(), |(b, e)| {
                let br = u64::from(b).try_into();
                let er = u64::from(e).try_into();
                if let (Ok(begin), Ok(end)) = (br, er) {
                    Segment::NonEmpty(NonEmptySegment::new_unchecked(begin, end))
                } else {
                    Segment::default()
                }
            });
        SpecificSegment {
            inner,
            _id: PhantomData,
            _src: PhantomData,
        }
    }
}

impl<I: Copy> HeaderSegment<I> {
    pub(crate) fn h_read_offsets<R: Read>(
        h: &mut BufReader<R>,
        allow_blank: bool,
        corr: OffsetCorrection<I, SegmentFromHeader>,
    ) -> MultiResult<Self, ImpureError<HeaderSegmentError>>
    where
        I: HasRegion,
    {
        let mut buf0 = [0_u8; 8];
        let mut buf1 = [0_u8; 8];
        h.read_exact(&mut buf0).into_mult()?;
        h.read_exact(&mut buf1).into_mult()?;
        Self::parse(&buf0, &buf1, allow_blank, corr).mult_map_errors(ImpureError::Pure)
    }

    pub(crate) fn parse(
        bs0: &[u8; 8],
        bs1: &[u8; 8],
        allow_blank: bool,
        corr: OffsetCorrection<I, SegmentFromHeader>,
    ) -> MultiResult<Self, HeaderSegmentError>
    where
        I: HasRegion,
    {
        let parse_one = |bs, is_begin| {
            Uint8Digit::from_bytes(bs, allow_blank).map_err(|error| ParseOffsetError {
                error,
                is_begin,
                location: I::REGION,
                source: bs.to_vec(),
            })
        };

        let begin_res = parse_one(bs0, true);
        let end_res = parse_one(bs1, false);
        begin_res
            .zip(end_res)
            .mult_errors_into()
            .and_then(|(begin, end)| SpecificSegment::try_new(begin, end, corr).into_mult())
    }

    /// Create offset pairs for HEADER
    ///
    /// Returns a string array like "   XXXX    YYYY".
    pub(crate) fn header_string(&self) -> String {
        let (b, e) = self
            .inner
            .try_coords()
            .unwrap_or((Uint8Digit::default(), Uint8Digit::default()));
        format!("{:>8}{:>8}", b, e)
    }

    pub(crate) fn unless(
        self,
        other: TEXTSegment<I>,
    ) -> Result<AnySegment<I>, (AnySegment<I>, SegmentMismatchWarning<I>)> {
        if other.inner.as_u64() != self.inner.as_u64() && !self.inner.is_empty() {
            Err((
                self.into_any(),
                SegmentMismatchWarning {
                    header: self,
                    text: other,
                },
            ))
        } else {
            Ok(SpecificSegment {
                inner: other.inner.as_u64(),
                _id: PhantomData,
                _src: PhantomData,
            })
        }
    }

    pub(crate) fn into_any(self) -> AnySegment<I> {
        SpecificSegment {
            inner: self.inner.as_u64(),
            _id: PhantomData,
            _src: PhantomData,
        }
    }
}

impl<I> TEXTSegment<I> {
    pub(crate) fn keywords(&self) -> [(String, String); 2]
    where
        I: KeyedReqSegment,
        I::B: Into<Uint20Char>,
        I::E: Into<Uint20Char>,
        I::B: From<Uint20Char>,
        I::E: From<Uint20Char>,
        I::B: ReqMetaKey,
        I::E: ReqMetaKey,
        I::B: FromStr<Err = ParseIntError>,
        I::E: FromStr<Err = ParseIntError>,
    {
        let i = self.inner;
        let (b, e) = match i {
            Segment::Empty => (Uint20Char::default(), Uint20Char::default()),
            Segment::NonEmpty(x) => (x.begin, x.end),
        };
        [
            ReqMetaKey::pair(&I::B::from(b)),
            ReqMetaKey::pair(&I::E::from(e)),
        ]
    }
}

impl<T> Segment<T> {
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
        begin: T,
        end: T,
        corr: OffsetCorrection<I, S>,
    ) -> Result<Self, SegmentError<T>>
    where
        T: Default,
        T: Into<i128>,
        T: TryFrom<i128>,
        T: PartialOrd,
        T: Copy,
    {
        let x = Into::<i128>::into(begin) + i128::from(corr.begin);
        let y = Into::<i128>::into(end) + i128::from(corr.end);
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
        match (T::try_from(x), T::try_from(y)) {
            (Ok(new_begin), Ok(new_end)) => {
                if new_begin > new_end {
                    err(SegmentErrorKind::Inverted)
                } else if new_begin == T::default() && new_end == T::default() {
                    Ok(Self::Empty)
                } else {
                    Ok(Self::NonEmpty(NonEmptySegment::new_unchecked(
                        new_begin, new_end,
                    )))
                }
            }
            (_, _) => err(SegmentErrorKind::Range),
        }
    }

    pub fn h_read_contents<R: Read + Seek>(
        &self,
        h: &mut BufReader<R>,
        buf: &mut Vec<u8>,
    ) -> io::Result<()>
    where
        T: Into<u64>,
        T: Copy,
    {
        match self {
            Self::Empty => Ok(()),
            Self::NonEmpty(s) => {
                let begin = s.begin.into();
                let nbytes = s.nbytes();

                h.seek(SeekFrom::Start(begin))?;
                h.take(nbytes).read_to_end(buf)?;
                Ok(())
            }
        }
    }

    pub fn try_adjust<I, S>(self, corr: OffsetCorrection<I, S>) -> Result<Self, SegmentError<T>>
    where
        I: HasRegion,
        S: HasSource,
        T: Copy,
        T: Default,
        T: TryFrom<i128>,
        T: Into<i128>,
        T: PartialOrd,
    {
        let (b, e) = match self {
            Self::Empty => (T::default(), T::default()),
            Self::NonEmpty(s) => s.coords(),
        };
        Self::try_new::<I, S>(b, e, corr)
    }

    /// Return the first and last byte if applicable
    pub fn try_coords(&self) -> Option<(T, T)>
    where
        T: Copy,
    {
        self.as_nonempty().map(|x| x.coords())
    }

    /// Return byte after end of segment if applicable
    pub fn try_next_byte(&self) -> Option<u64>
    where
        T: Copy,
        T: Into<u64>,
    {
        self.as_nonempty().map(|x| x.next_byte())
    }

    /// Return the number of bytes in this segment
    pub fn len(&self) -> u64
    where
        T: Copy,
        T: Into<u64>,
    {
        // NOTE In FCS a 0,0 means "empty" but this also means one byte
        // according to the spec's on definitions. The first number points to
        // the first byte in a segment, and the second number points to the last
        // byte, therefore 0,0 means "0 is both the first and last byte, which
        // also means there is one byte".
        self.as_nonempty().map_or(0, |s| s.nbytes())
    }

    /// Return true if segment has 0 bytes
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    pub fn fmt_pair(&self) -> String
    where
        T: Default,
        T: Copy,
        T: fmt::Display,
    {
        let (b, e) = self.try_coords().unwrap_or((T::default(), T::default()));
        format!("{},{}", b, e)
    }

    pub fn as_u64(&self) -> Segment<u64>
    where
        T: Into<u64>,
        T: Copy,
    {
        match self {
            Self::Empty => Segment::Empty,
            Self::NonEmpty(x) => Segment::NonEmpty(x.as_u64()),
        }
    }

    pub fn as_nonempty(&self) -> Option<NonEmptySegment<T>>
    where
        T: Copy,
    {
        match self {
            Self::Empty => None,
            Self::NonEmpty(x) => Some(*x),
        }
    }

    pub fn any_overlap(xs: &[Self]) -> bool
    where
        T: Ord,
        T: Copy,
    {
        let mut ys: Vec<_> = xs.iter().flat_map(|x| x.as_nonempty()).collect();
        ys.sort_by_key(|x| x.begin);
        if let Some(zs) = NonEmpty::from_vec(ys) {
            let mut prev_end = zs.head.end;
            for z in zs.tail {
                if z.begin > prev_end {
                    prev_end = z.end;
                } else {
                    return true;
                }
            }
        }
        false
    }
}

impl<T> NonEmptySegment<T> {
    /// Return the number of bytes in this segment
    pub fn nbytes(&self) -> u64
    where
        T: Into<u64>,
        T: Copy,
    {
        self.end.into() - self.begin.into() + 1
    }

    /// Return the first and last byte or this segment
    pub fn coords(&self) -> (T, T)
    where
        T: Copy,
    {
        (self.begin, self.end)
    }

    /// Return the next byte after this segment
    pub fn next_byte(&self) -> u64
    where
        T: Into<u64>,
        T: Copy,
    {
        // TODO technically this should return option since it isn't guaranteed
        // that the next byte won't wrap
        self.begin.into() + self.nbytes()
    }

    fn new_unchecked(begin: T, end: T) -> Self {
        Self { begin, end }
    }

    pub fn as_u64(&self) -> NonEmptySegment<u64>
    where
        T: Into<u64>,
        T: Copy,
    {
        NonEmptySegment {
            begin: self.begin.into(),
            end: self.end.into(),
        }
    }
}

enum_from_disp!(
    pub HeaderSegmentError,
    [Segment, SegmentError<Uint8Digit>],
    [Parse, ParseOffsetError]
);

pub struct SegmentError<T> {
    begin: T,
    end: T,
    corr_begin: i32,
    corr_end: i32,
    kind: SegmentErrorKind,
    location: &'static str,
    src: &'static str,
}

#[derive(Debug)]
pub enum SegmentErrorKind {
    Range,
    Inverted,
    InHeader,
}

pub struct ParseOffsetError {
    pub(crate) error: ParseFixedUint8CharError,
    pub(crate) is_begin: bool,
    pub(crate) location: &'static str,
    pub(crate) source: Vec<u8>,
}

impl fmt::Display for ParseOffsetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let which = if self.is_begin { "begin" } else { "end" };
        write!(
            f,
            "parse error for {which} offset in {} segment from source '{}': {}",
            self.location,
            self.source.iter().join(","),
            self.error
        )
    }
}

impl<T> fmt::Display for SegmentError<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let offset_text = |x, delta| {
            if delta == 0 {
                format!("{}", x)
            } else {
                format!("{} ({}))", x, delta)
            }
        };
        let begin_text = offset_text(&self.begin, self.corr_begin);
        let end_text = offset_text(&self.end, self.corr_end);
        let kind_text = match &self.kind {
            SegmentErrorKind::Range => "Offset out of range",
            SegmentErrorKind::Inverted => "Begin after end",
            SegmentErrorKind::InHeader => "Begins within HEADER",
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
    header: HeaderSegment<S>,
    text: TEXTSegment<S>,
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

pub enum ReqSegmentWithDefaultError<I> {
    Req(ReqSegmentError),
    Mismatch(SegmentMismatchWarning<I>),
}

impl<I> From<SegmentMismatchWarning<I>> for ReqSegmentWithDefaultError<I> {
    fn from(value: SegmentMismatchWarning<I>) -> Self {
        Self::Mismatch(value)
    }
}

pub enum ReqSegmentWithDefaultWarning<I> {
    Mismatch(SegmentMismatchWarning<I>),
    Lookup(SegmentDefaultWarning<I>),
}

impl<I> fmt::Display for ReqSegmentWithDefaultError<I>
where
    I: HasRegion,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Mismatch(e) => e.fmt(f),
            Self::Req(e) => e.fmt(f),
        }
    }
}

impl<I> fmt::Display for ReqSegmentWithDefaultWarning<I>
where
    I: HasRegion,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Mismatch(e) => e.fmt(f),
            Self::Lookup(e) => e.fmt(f),
        }
    }
}

impl<I> From<SegmentMismatchWarning<I>> for ReqSegmentWithDefaultWarning<I> {
    fn from(value: SegmentMismatchWarning<I>) -> Self {
        Self::Mismatch(value)
    }
}

impl<I> From<SegmentDefaultWarning<I>> for ReqSegmentWithDefaultWarning<I> {
    fn from(value: SegmentDefaultWarning<I>) -> Self {
        Self::Lookup(value)
    }
}

pub enum OptSegmentWithDefaultWarning<I> {
    Opt(OptSegmentError),
    Mismatch(SegmentMismatchWarning<I>),
}

impl<I> From<SegmentMismatchWarning<I>> for OptSegmentWithDefaultWarning<I> {
    fn from(value: SegmentMismatchWarning<I>) -> Self {
        Self::Mismatch(value)
    }
}

impl<I> fmt::Display for OptSegmentWithDefaultWarning<I>
where
    I: HasRegion,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Mismatch(e) => e.fmt(f),
            Self::Opt(e) => e.fmt(f),
        }
    }
}
