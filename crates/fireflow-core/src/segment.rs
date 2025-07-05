use crate::config::{HeaderConfig, ReadState, StdTextReadConfig};
use crate::error::*;
use crate::text::keywords::*;
use crate::text::parser::*;
use crate::validated::ascii_uint::*;
use crate::validated::keys::*;

use derive_more::{Display, From};
use itertools::Itertools;
use nonempty::NonEmpty;
use num_traits::identities::{One, Zero};
use num_traits::ops::checked::CheckedSub;
use serde::Serialize;
use std::fmt;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::num::ParseIntError;
use std::str;
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
#[derive(Clone, Copy, Serialize)]
pub struct SpecificSegment<I, S, T> {
    pub inner: Segment<T>,
    _id: PhantomData<I>,
    _src: PhantomData<S>,
}

/// A non-empty segment that still has regional/src data but is type-agnostic.
///
/// Useful for bulk operations on lots of segments at once that wouldn't work
/// if they segments were all different types.
#[derive(Clone)]
pub struct GenericSegment {
    pub begin: u64,
    pub end: u64,
    pub region: &'static str,
    pub src: &'static str,
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

pub type OtherSegment = SpecificSegment<OtherSegmentId, SegmentFromHeader, Uint20Char>;

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
    Self::B: ReqMetarootKey,
    Self::E: ReqMetarootKey,
    Self::B: FromStr<Err = ParseIntError>,
    Self::E: FromStr<Err = ParseIntError>,
{
    fn get_or(
        kws: &StdKeywords,
        corr: TEXTCorrection<Self>,
        default: HeaderSegment<Self>,
        force_default: bool,
        st: &ReadState<StdTextReadConfig>,
    ) -> ReqSegResult<Self>
    where
        Self: Copy,
    {
        if force_default {
            Ok(Tentative::new1(default.into_any()))
        } else {
            let res = Self::get(kws, corr, &st.map_inner(|c| &c.raw.header))
                .def_map_errors(ReqSegmentWithDefaultError::Req);
            Self::default_or(res, default, st.conf)
        }
    }

    fn get<W>(
        kws: &StdKeywords,
        corr: TEXTCorrection<Self>,
        st: &ReadState<HeaderConfig>,
    ) -> DeferredResult<TEXTSegment<Self>, W, ReqSegmentError> {
        Self::get_mult(kws, corr, st).mult_to_deferred()
    }

    fn get_mult(
        kws: &StdKeywords,
        corr: TEXTCorrection<Self>,
        st: &ReadState<HeaderConfig>,
    ) -> MultiResult<TEXTSegment<Self>, ReqSegmentError> {
        Self::get_pair(kws)
            .map_err(|es| es.map(|e| e.into()))
            .and_then(|(y0, y1)| {
                SpecificSegment::try_new(
                    y0.into(),
                    y1.into(),
                    corr,
                    Some(st.file_len.into()),
                    st.conf.truncate_offsets,
                )
                .into_mult::<ReqSegmentError>()
            })
    }

    fn remove_or(
        kws: &mut StdKeywords,
        corr: TEXTCorrection<Self>,
        default: HeaderSegment<Self>,
        force_default: bool,
        st: &ReadState<StdTextReadConfig>,
    ) -> ReqSegResult<Self>
    where
        Self: Copy,
    {
        // if we want to totally ignore the TEXT offsets, just blindly remove
        // them so we don't trigger any pseudostandard false positives later and
        // return the default segment
        if force_default {
            let _ = Self::remove_pair(kws);
            Ok(Tentative::new1(default.into_any()))
        } else {
            let res = Self::remove(kws, corr, &st.map_inner(|c| &c.raw.header))
                .def_map_errors(ReqSegmentWithDefaultError::Req);
            Self::default_or(res, default, st.conf)
        }
    }

    fn remove<W>(
        kws: &mut StdKeywords,
        corr: TEXTCorrection<Self>,
        st: &ReadState<HeaderConfig>,
    ) -> DeferredResult<TEXTSegment<Self>, W, ReqSegmentError> {
        Self::remove_mult(kws, corr, st).mult_to_deferred()
    }

    fn remove_mult(
        kws: &mut StdKeywords,
        corr: TEXTCorrection<Self>,
        st: &ReadState<HeaderConfig>,
    ) -> MultiResult<TEXTSegment<Self>, ReqSegmentError> {
        Self::remove_pair(kws)
            .map_err(|es| es.map(|e| e.into()))
            .and_then(|(y0, y1)| {
                SpecificSegment::try_new(
                    y0.into(),
                    y1.into(),
                    corr,
                    Some(st.file_len.into()),
                    st.conf.truncate_offsets,
                )
                .into_mult::<ReqSegmentError>()
            })
    }

    fn default_or(
        res: DeferredResult<
            TEXTSegment<Self>,
            ReqSegmentWithDefaultWarning<Self>,
            ReqSegmentWithDefaultError<Self>,
        >,
        default: HeaderSegment<Self>,
        conf: &StdTextReadConfig,
    ) -> ReqSegResult<Self>
    where
        Self: Copy,
    {
        let allow_mismatch = conf.allow_header_text_offset_mismatch;
        let allow_missing = conf.allow_missing_required_offsets;
        res.map_or_else(
            |f| {
                if allow_missing {
                    let mut tnt = f.unfail_with(default.into_any());
                    tnt.push_warning(SegmentDefaultWarning::default().into());
                    Ok(tnt)
                } else {
                    Err(f)
                }
            },
            |tnt| {
                Ok(tnt.and_tentatively(|other| {
                    default.unless(other).map_or_else(
                        |(s, w)| Tentative::new_either(s, vec![w], !allow_mismatch),
                        Tentative::new1,
                    )
                }))
            },
        )
    }

    fn get_pair(kws: &StdKeywords) -> MultiResult<(Self::B, Self::E), ReqKeyError<ParseIntError>> {
        let x0 = Self::B::get_metaroot_req(kws);
        let x1 = Self::E::get_metaroot_req(kws);
        x0.zip(x1)
    }

    fn remove_pair(
        kws: &mut StdKeywords,
    ) -> MultiResult<(Self::B, Self::E), ReqKeyError<ParseIntError>> {
        let x0 = Self::B::remove_metaroot_req(kws);
        let x1 = Self::E::remove_metaroot_req(kws);
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
    Self::B: OptMetarootKey,
    Self::E: OptMetarootKey,
    Self::B: FromStr<Err = ParseIntError>,
    Self::E: FromStr<Err = ParseIntError>,
{
    fn get_or(
        kws: &StdKeywords,
        corr: TEXTCorrection<Self>,
        default: HeaderSegment<Self>,
        force_default: bool,
        st: &ReadState<StdTextReadConfig>,
    ) -> OptSegTentative<Self>
    where
        Self: Copy,
        Self::B: OptMetarootKey,
        Self::E: OptMetarootKey,
    {
        if force_default {
            Tentative::new1(default.into_any())
        } else {
            let res = Self::get(kws, corr, &st.map_inner(|c| &c.raw.header))
                .map_warnings(OptSegmentWithDefaultWarning::Opt);
            Self::default_or(res, default, st.conf)
        }
    }

    fn get<E>(
        kws: &StdKeywords,
        corr: TEXTCorrection<Self>,
        st: &ReadState<HeaderConfig>,
    ) -> Tentative<Option<TEXTSegment<Self>>, OptSegmentError, E> {
        Self::get_pair(kws)
            .map_err(|es| es.map(|e| e.into()))
            .and_then(|x| {
                x.map(|(z0, z1)| {
                    SpecificSegment::try_new(
                        z0.into(),
                        z1.into(),
                        corr,
                        Some(st.file_len.into()),
                        st.conf.truncate_offsets,
                    )
                    .into_mult()
                })
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
        force_default: bool,
        st: &ReadState<StdTextReadConfig>,
    ) -> OptSegTentative<Self>
    where
        Self: Copy,
    {
        if force_default {
            let _ = Self::remove_pair(kws);
            Tentative::new1(default.into_any())
        } else {
            let res = Self::remove(kws, corr, &st.map_inner(|c| &c.raw.header))
                .map_warnings(OptSegmentWithDefaultWarning::Opt);
            Self::default_or(res, default, st.conf)
        }
    }

    fn remove<E>(
        kws: &mut StdKeywords,
        corr: TEXTCorrection<Self>,
        st: &ReadState<HeaderConfig>,
    ) -> Tentative<Option<TEXTSegment<Self>>, OptSegmentError, E> {
        Self::remove_pair(kws)
            .map_err(|es| es.map(|e| e.into()))
            .and_then(|x| {
                x.map(|(z0, z1)| {
                    SpecificSegment::try_new(
                        z0.into(),
                        z1.into(),
                        corr,
                        Some(st.file_len.into()),
                        st.conf.truncate_offsets,
                    )
                    .into_mult()
                })
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
        conf: &StdTextReadConfig,
    ) -> OptSegTentative<Self>
    where
        Self: Copy,
    {
        let allow_mismatch = conf.allow_header_text_offset_mismatch;
        res.and_tentatively(|other| {
            other.map_or(Tentative::new1(default.into_any()), |o| {
                default.unless(o).map_or_else(
                    |(s, w)| Tentative::new_either(s, vec![w], !allow_mismatch),
                    Tentative::new1,
                )
            })
        })
    }

    #[allow(clippy::type_complexity)]
    fn get_pair(
        kws: &StdKeywords,
    ) -> MultiResult<Option<(Self::B, Self::E)>, ParseKeyError<ParseIntError>> {
        let x0 = Self::B::get_metaroot_opt(kws).map(|x| x.0);
        let x1 = Self::E::get_metaroot_opt(kws).map(|x| x.0);
        x0.zip(x1).map(|(x, y)| x.zip(y))
    }

    #[allow(clippy::type_complexity)]
    fn remove_pair(
        kws: &mut StdKeywords,
    ) -> MultiResult<Option<(Self::B, Self::E)>, ParseKeyError<ParseIntError>> {
        let x0 = Self::B::remove_metaroot_opt(kws).map(|x| x.0);
        let x1 = Self::E::remove_metaroot_opt(kws).map(|x| x.0);
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

#[derive(From, Display)]
pub enum ReqSegmentError {
    Key(ReqKeyError<ParseIntError>),
    Segment(SegmentError<Uint20Char>),
}

#[derive(From, Display)]
pub enum OptSegmentError {
    Key(ParseKeyError<ParseIntError>),
    Segment(SegmentError<Uint20Char>),
}

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

impl<I, S> From<(i32, i32)> for OffsetCorrection<I, S> {
    fn from(value: (i32, i32)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl<I, S, T> Default for SpecificSegment<I, S, T> {
    fn default() -> Self {
        Self {
            inner: Segment::Empty,
            _id: PhantomData,
            _src: PhantomData,
        }
    }
}

impl<I, S, T> SpecificSegment<I, S, T> {
    pub fn try_new(
        begin: T,
        end: T,
        corr: OffsetCorrection<I, S>,
        file_len: Option<T>,
        force_truncate: bool,
    ) -> Result<Self, SegmentError<T>>
    where
        I: HasRegion,
        S: HasSource,
        T: Zero + One + CheckedSub + Into<u64> + Into<i128> + TryFrom<i128> + Ord + Copy,
        u64: From<T>,
    {
        Segment::try_new::<I, S>(begin, end, corr, file_len, force_truncate).map(|inner| Self {
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

    pub fn try_as_generic(&self) -> Option<GenericSegment>
    where
        I: HasRegion,
        S: HasSource,
        T: Copy,
        T: Into<u64>,
    {
        self.inner.try_as_nonempty().map(|x| {
            let (begin, end) = x.as_u64().coords();
            GenericSegment {
                begin,
                end,
                src: S::SRC,
                region: I::REGION,
            }
        })
    }
}

impl GenericSegment {
    pub fn overlaps(&self, other: &GenericSegment) -> Result<(), SegmentOverlapError> {
        if (self.begin < other.begin && self.end < other.begin)
            || (other.begin < self.begin && other.end < self.begin)
        {
            Ok(())
        } else {
            Err(SegmentOverlapError {
                seg0: self.clone(),
                seg1: other.clone(),
            })
        }
    }

    pub fn find_overlaps(mut xs: Vec<Self>) -> MultiResult<(), SegmentOverlapError> {
        xs.sort_by_key(|x| x.begin);
        if let Some(ys) = NonEmpty::from_vec(xs) {
            let mut prev = ys.head;
            let mut errors = vec![];
            // NOTE this won't find all overlaps since it won't check if a given
            // segment's end is after the beginning of segments 2 or more ahead,
            // but at least an error will be throw for all that are 1 away which
            // should be good enough to let the user fix the problem
            for z in ys.tail {
                if z.begin <= prev.begin {
                    errors.push(SegmentOverlapError {
                        seg0: prev,
                        seg1: z.clone(),
                    });
                    prev = z;
                }
            }
            NonEmpty::from_vec(errors).map(Err).unwrap_or(Ok(()))
        } else {
            Ok(())
        }
    }
}

impl<I, S> SpecificSegment<I, S, Uint20Char> {
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
}

impl<I> TEXTSegment<I> {
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
        st: &ReadState<HeaderConfig>,
        corr: OffsetCorrection<I, SegmentFromHeader>,
    ) -> MultiResult<Self, ImpureError<HeaderSegmentError>>
    where
        I: HasRegion,
    {
        let mut buf0 = [0_u8; 8];
        let mut buf1 = [0_u8; 8];
        h.read_exact(&mut buf0).into_mult()?;
        h.read_exact(&mut buf1).into_mult()?;
        Self::parse(&buf0, &buf1, allow_blank, corr, st).mult_map_errors(ImpureError::Pure)
    }

    pub(crate) fn parse(
        bs0: &[u8; 8],
        bs1: &[u8; 8],
        allow_blank: bool,
        corr: OffsetCorrection<I, SegmentFromHeader>,
        st: &ReadState<HeaderConfig>,
    ) -> MultiResult<Self, HeaderSegmentError>
    where
        I: HasRegion,
    {
        let parse_one = |bs, is_begin| {
            Uint8Digit::from_bytes(bs, allow_blank, st.conf.allow_negative).map_err(|error| {
                ParseOffsetError {
                    error,
                    is_begin,
                    location: I::REGION,
                    source: bs.to_vec(),
                }
            })
        };

        let begin_res = parse_one(bs0, true);
        let end_res = parse_one(bs1, false);
        begin_res
            .zip(end_res)
            .mult_errors_into()
            .and_then(|(begin, end)| {
                SpecificSegment::try_new_squish(begin, end, corr, st).into_mult()
            })
    }

    /// Create offset pairs for HEADER
    ///
    /// Returns a string array like "   XXXX    YYYY".
    pub(crate) fn header_string(&self) -> String {
        let (b, e) = self
            .inner
            .try_coords()
            .unwrap_or((Uint8Digit::zero(), Uint8Digit::zero()));
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

    fn try_new_squish(
        begin: Uint8Digit,
        end: Uint8Digit,
        corr: OffsetCorrection<I, SegmentFromHeader>,
        st: &ReadState<HeaderConfig>,
    ) -> Result<Self, SegmentError<Uint8Digit>>
    where
        I: HasRegion,
    {
        // TODO this might produce really weird errors if run on a 2.0
        // file, so in those cases, this should never be true
        let conf = &st.conf;
        let (b, e) = if conf.squish_offsets && end == Uint8Digit::zero() && begin > end {
            (Uint8Digit::zero(), Uint8Digit::zero())
        } else {
            (begin, end)
        };
        let file_len = st.file_len.try_into().ok();
        SpecificSegment::try_new(b, e, corr, file_len, conf.truncate_offsets)
    }
}

impl OtherSegment {
    pub(crate) fn parse(
        bs0: &[u8],
        bs1: &[u8],
        allow_negative: bool,
        corr: OffsetCorrection<OtherSegmentId, SegmentFromHeader>,
        st: &ReadState<HeaderConfig>,
    ) -> MultiResult<Self, HeaderSegmentError> {
        let parse_one = |bs: &[u8], is_begin| {
            ascii_str_from_bytes(bs)
                .map_err(ParseFixedUintError::NotAscii)
                .and_then(|s| {
                    let x = s
                        .trim_start()
                        .parse::<i32>()
                        .map_err(ParseFixedUintError::Int)?;
                    if x < 0 {
                        if allow_negative {
                            Ok(Uint20Char::zero())
                        } else {
                            Err(ParseFixedUintError::Negative(NegativeOffsetError(x)))
                        }
                    } else {
                        // ASSUME this will never fail because we checked the
                        // sign above
                        Ok(Uint20Char(x as u64))
                    }
                })
                .map_err(|error| ParseOffsetError {
                    error,
                    is_begin,
                    location: OtherSegmentId::REGION,
                    source: bs.to_vec(),
                })
        };

        let begin_res = parse_one(bs0, true);
        let end_res = parse_one(bs1, false);
        let file_len = Some(st.file_len.into());
        begin_res
            .zip(end_res)
            .mult_errors_into()
            .and_then(|(begin, end)| {
                SpecificSegment::try_new(begin, end, corr, file_len, st.conf.truncate_offsets)
                    .into_mult()
            })
    }

    pub(crate) fn header_string(&self) -> String {
        let (b, e) = self
            .inner
            .try_coords()
            .unwrap_or((Uint20Char::zero(), Uint20Char::zero()));
        let mut s = String::new();
        s.push_str(&b.to_string());
        s.push_str(&e.to_string());
        s
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
        I::B: ReqMetarootKey,
        I::E: ReqMetarootKey,
        I::B: FromStr<Err = ParseIntError>,
        I::E: FromStr<Err = ParseIntError>,
    {
        let i = self.inner;
        let (b, e) = match i {
            Segment::Empty => (Uint20Char::zero(), Uint20Char::zero()),
            Segment::NonEmpty(x) => (x.begin, x.end),
        };
        [
            ReqMetarootKey::pair(&I::B::from(b)),
            ReqMetarootKey::pair(&I::E::from(e)),
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
        file_len: Option<T>,
        force_truncate: bool,
    ) -> Result<Self, SegmentError<T>>
    where
        T: Zero + One + CheckedSub + Into<i128> + TryFrom<i128> + Ord + Copy,
        u64: From<T>,
    {
        let x = Into::<i128>::into(begin) + i128::from(corr.begin);
        let y = Into::<i128>::into(end) + i128::from(corr.end);
        let err = |kind| SegmentError {
            begin,
            end,
            corr_begin: corr.begin,
            corr_end: corr.end,
            kind,
            location: I::REGION,
            src: S::SRC,
        };
        match (T::try_from(x), T::try_from(y)) {
            (Ok(new_begin), Ok(new_end)) => {
                if new_begin > new_end {
                    Err(err(SegmentErrorKind::Inverted))
                } else if new_begin == T::zero() && new_end == T::zero() {
                    Ok(Self::Empty)
                } else {
                    // file length is optional because it might exceed the max
                    // of whatever type is used in this segment, in which case
                    // truncation is impossible.
                    if let Some(fl) = file_len {
                        if new_end >= fl && !force_truncate {
                            Err(err(SegmentErrorKind::Truncated(u64::from(fl))))
                        } else {
                            Ok(new_end.min(fl.checked_sub(&T::one()).unwrap_or(T::zero())))
                        }
                    } else {
                        Ok(new_end)
                    }
                    .map(|e| Self::NonEmpty(NonEmptySegment::new_unchecked(new_begin, e)))
                }
            }
            (_, _) => Err(err(SegmentErrorKind::Range)),
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

    pub fn try_adjust<I, S>(
        self,
        corr: OffsetCorrection<I, S>,
        file_len: Option<T>,
        force_truncate: bool,
    ) -> Result<Self, SegmentError<T>>
    where
        S: HasSource,
        I: HasRegion,
        T: Copy + Zero + One + CheckedSub + TryFrom<i128> + Into<i128> + Ord,
        u64: From<T>,
    {
        let (b, e) = match self {
            Self::Empty => (T::zero(), T::zero()),
            Self::NonEmpty(s) => s.coords(),
        };
        Self::try_new::<I, S>(b, e, corr, file_len, force_truncate)
    }

    /// Return the first and last byte if applicable
    pub fn try_coords(&self) -> Option<(T, T)>
    where
        T: Copy,
    {
        self.try_as_nonempty().map(|x| x.coords())
    }

    /// Return byte after end of segment if applicable
    pub fn try_next_byte(&self) -> Option<u64>
    where
        T: Copy,
        T: Into<u64>,
    {
        self.try_as_nonempty().map(|x| x.next_byte())
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
        self.try_as_nonempty().map_or(0, |s| s.nbytes())
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

    pub fn try_as_nonempty(&self) -> Option<NonEmptySegment<T>>
    where
        T: Copy,
    {
        match self {
            Self::Empty => None,
            Self::NonEmpty(x) => Some(*x),
        }
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

#[derive(From, Display)]
pub enum HeaderSegmentError {
    Standard(SegmentError<Uint8Digit>),
    Other(SegmentError<Uint20Char>),
    Parse(ParseOffsetError),
}

pub struct SegmentError<T> {
    begin: T,
    end: T,
    corr_begin: i32,
    corr_end: i32,
    kind: SegmentErrorKind,
    location: &'static str,
    src: &'static str,
}

pub struct SegmentOverlapError {
    seg0: GenericSegment,
    seg1: GenericSegment,
}

impl fmt::Display for SegmentOverlapError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{} overlaps with {}", self.seg0, self.seg1)
    }
}

impl fmt::Display for GenericSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "segment for {} from {} with coords ({}, {})",
            self.region, self.src, self.begin, self.end
        )
    }
}

#[derive(Debug)]
pub enum SegmentErrorKind {
    Range,
    Inverted,
    InHeader,
    Truncated(u64),
}

pub struct ParseOffsetError {
    pub(crate) error: ParseFixedUintError,
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
            SegmentErrorKind::Range => "Offset out of range".to_string(),
            SegmentErrorKind::Inverted => "Begin after end".to_string(),
            SegmentErrorKind::InHeader => "Begins within HEADER".to_string(),
            SegmentErrorKind::Truncated(size) => {
                format!("Segment exceeds file size ({size} bytes)")
            }
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
            self.header.inner.as_u64().fmt_pair(),
            self.text.inner.as_u64().fmt_pair(),
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
