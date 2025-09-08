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
use std::fmt;
use std::io;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::num::ParseIntError;
use std::str;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

/// A segment that is specific to a region in the FCS file.
#[derive(Clone, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct SpecificSegment<I, S, T> {
    pub inner: Segment<T>,
    _id: PhantomData<I>,
    _src: PhantomData<S>,
}

/// A segment in an FCS file which is denoted by a pair of offsets
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum Segment<T> {
    NonEmpty(NonEmptySegment<T>),
    #[default]
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct NonEmptySegment<T> {
    begin: T,
    end: T,
}

/// Denotes a correction for a segment
#[derive(Default, Clone, Copy)]
pub struct OffsetCorrection<I, S> {
    pub begin: i32,
    pub end: i32,
    _id: PhantomData<I>,
    _src: PhantomData<S>,
}

/// A non-empty segment that still has regional/src data but is type-agnostic.
///
/// Useful for bulk operations on lots of segments at once that wouldn't work
/// if they segments were all different types.
#[derive(Clone)]
pub(crate) struct GenericSegment {
    pub(crate) begin: u64,
    pub(crate) end: u64,
    pub(crate) region: &'static str,
    pub(crate) src: &'static str,
}

/// Denotes a segment came from HEADER
#[derive(Default, Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct SegmentFromHeader;

/// Denotes a segment came from TEXT
#[derive(Default, Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct SegmentFromTEXT;

/// Denotes a segment came from either TEXT or HEADER
#[derive(Clone, Copy)]
pub struct SegmentFromAnywhere;

/// Denotes the segment pertains to primary TEXT
#[derive(Default, Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct PrimaryTextSegmentId;

/// Denotes the segment pertains to supplemental TEXT
#[derive(Default, Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct SupplementalTextSegmentId;

/// Denotes the segment pertains to DATA
#[derive(Default, Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct DataSegmentId;

/// Denotes the segment pertains to ANALYSIS
#[derive(Default, Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AnalysisSegmentId;

/// Denotes the segment pertains to OTHER (indexed from 0)
#[derive(Default, Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct OtherSegmentId;

pub type PrimaryTextSegment = SpecificSegment<PrimaryTextSegmentId, SegmentFromHeader, UintSpacePad8>;
pub type SupplementalTextSegment =
    SpecificSegment<SupplementalTextSegmentId, SegmentFromTEXT, UintZeroPad20>;

type DataSegment<S, T> = SpecificSegment<DataSegmentId, S, T>;
pub type HeaderDataSegment = DataSegment<SegmentFromHeader, UintSpacePad8>;
pub type TEXTDataSegment = DataSegment<SegmentFromTEXT, UintZeroPad20>;

type AnalysisSegment<S, T> = SpecificSegment<AnalysisSegmentId, S, T>;
pub type HeaderAnalysisSegment = AnalysisSegment<SegmentFromHeader, UintSpacePad8>;
pub type TEXTAnalysisSegment = AnalysisSegment<SegmentFromTEXT, UintZeroPad20>;

pub type HeaderSegment<I> = SpecificSegment<I, SegmentFromHeader, UintSpacePad8>;
pub type TEXTSegment<I> = SpecificSegment<I, SegmentFromTEXT, UintZeroPad20>;
pub type AnySegment<I> = SpecificSegment<I, SegmentFromAnywhere, u64>;

pub type HeaderCorrection<I> = OffsetCorrection<I, SegmentFromHeader>;
pub type TEXTCorrection<I> = OffsetCorrection<I, SegmentFromTEXT>;

pub type AnyDataSegment = DataSegment<SegmentFromAnywhere, u64>;
pub type AnyAnalysisSegment = AnalysisSegment<SegmentFromAnywhere, u64>;

pub type OtherSegment = SpecificSegment<OtherSegmentId, SegmentFromHeader, UintZeroPad20>;

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
    Self: KeyedSegment + HasRegion,
    Self::B: Into<UintZeroPad20> + ReqMetarootKey + FromStr<Err = ParseIntError>,
    Self::E: Into<UintZeroPad20> + ReqMetarootKey + FromStr<Err = ParseIntError>,
{
    fn get_or(
        kws: &StdKeywords,
        default: HeaderSegment<Self>,
        force_default: bool,
        allow_mismatch: bool,
        allow_missing: bool,
        conf: &NewSegmentConfig<UintZeroPad20, Self, SegmentFromTEXT>,
    ) -> ReqSegResult<Self>
    where
        Self: Copy,
    {
        if force_default {
            Ok(Tentative::new1(default.into_any()))
        } else {
            let res = Self::get(kws, conf).def_map_errors(ReqSegmentWithDefaultError::Req);
            Self::default_or(res, default, allow_mismatch, allow_missing)
        }
    }

    fn get<W>(
        kws: &StdKeywords,
        conf: &NewSegmentConfig<UintZeroPad20, Self, SegmentFromTEXT>,
    ) -> DeferredResult<TEXTSegment<Self>, W, ReqSegmentError> {
        Self::get_mult(kws, conf).mult_to_deferred()
    }

    fn get_mult(
        kws: &StdKeywords,
        conf: &NewSegmentConfig<UintZeroPad20, Self, SegmentFromTEXT>,
    ) -> MultiResult<TEXTSegment<Self>, ReqSegmentError> {
        Self::get_pair(kws)
            .map_err(|es| es.map(|e| e.into()))
            .and_then(|(y0, y1)| {
                SpecificSegment::try_new(y0.into(), y1.into(), conf).into_mult::<ReqSegmentError>()
            })
    }

    fn remove_or(
        kws: &mut StdKeywords,
        default: HeaderSegment<Self>,
        force_default: bool,
        allow_mismatch: bool,
        allow_missing: bool,
        conf: &NewSegmentConfig<UintZeroPad20, Self, SegmentFromTEXT>,
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
            let res = Self::remove(kws, conf).def_map_errors(ReqSegmentWithDefaultError::Req);
            Self::default_or(res, default, allow_mismatch, allow_missing)
        }
    }

    fn remove<W>(
        kws: &mut StdKeywords,
        conf: &NewSegmentConfig<UintZeroPad20, Self, SegmentFromTEXT>,
    ) -> DeferredResult<TEXTSegment<Self>, W, ReqSegmentError> {
        Self::remove_mult(kws, conf).mult_to_deferred()
    }

    fn remove_mult(
        kws: &mut StdKeywords,
        conf: &NewSegmentConfig<UintZeroPad20, Self, SegmentFromTEXT>,
    ) -> MultiResult<TEXTSegment<Self>, ReqSegmentError> {
        Self::remove_pair(kws)
            .map_err(|es| es.map(|e| e.into()))
            .and_then(|(y0, y1)| {
                SpecificSegment::try_new(y0.into(), y1.into(), conf).into_mult::<ReqSegmentError>()
            })
    }

    fn default_or(
        res: DeferredResult<
            TEXTSegment<Self>,
            ReqSegmentWithDefaultWarning<Self>,
            ReqSegmentWithDefaultError<Self>,
        >,
        default: HeaderSegment<Self>,
        allow_mismatch: bool,
        allow_missing: bool,
    ) -> ReqSegResult<Self>
    where
        Self: Copy,
    {
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
    Self: KeyedSegment + HasRegion,
    Self::B: Into<UintZeroPad20> + OptMetarootKey + FromStr<Err = ParseIntError>,
    Self::E: Into<UintZeroPad20> + OptMetarootKey + FromStr<Err = ParseIntError>,
{
    fn get_or(
        kws: &StdKeywords,
        default: HeaderSegment<Self>,
        force_default: bool,
        allow_mismatch: bool,
        conf: &NewSegmentConfig<UintZeroPad20, Self, SegmentFromTEXT>,
    ) -> OptSegTentative<Self>
    where
        Self: Copy,
        Self::B: OptMetarootKey,
        Self::E: OptMetarootKey,
    {
        if force_default {
            Tentative::new1(default.into_any())
        } else {
            let res = Self::get(kws, conf).map_warnings(OptSegmentWithDefaultWarning::Opt);
            Self::default_or(res, default, allow_mismatch)
        }
    }

    fn get<E>(
        kws: &StdKeywords,
        conf: &NewSegmentConfig<UintZeroPad20, Self, SegmentFromTEXT>,
    ) -> Tentative<Option<TEXTSegment<Self>>, OptSegmentError, E> {
        Self::get_pair(kws)
            .map_err(|es| es.map(|e| e.into()))
            .and_then(|x| {
                x.map(|(z0, z1)| SpecificSegment::try_new(z0.into(), z1.into(), conf).into_mult())
                    .transpose()
            })
            .map_or_else(
                |ws| Tentative::new(None, ws.into(), vec![]),
                Tentative::new1,
            )
    }

    fn remove_or(
        kws: &mut StdKeywords,
        default: HeaderSegment<Self>,
        force_default: bool,
        allow_mismatch: bool,
        conf: &NewSegmentConfig<UintZeroPad20, Self, SegmentFromTEXT>,
    ) -> OptSegTentative<Self>
    where
        Self: Copy,
    {
        if force_default {
            let _ = Self::remove_pair(kws);
            Tentative::new1(default.into_any())
        } else {
            let res = Self::remove(kws, conf).map_warnings(OptSegmentWithDefaultWarning::Opt);
            Self::default_or(res, default, allow_mismatch)
        }
    }

    fn remove<E>(
        kws: &mut StdKeywords,
        conf: &NewSegmentConfig<UintZeroPad20, Self, SegmentFromTEXT>,
    ) -> Tentative<Option<TEXTSegment<Self>>, OptSegmentError, E> {
        Self::remove_pair(kws)
            .map_err(|es| es.map(|e| e.into()))
            .and_then(|x| {
                x.map(|(z0, z1)| SpecificSegment::try_new(z0.into(), z1.into(), conf).into_mult())
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
        allow_mismatch: bool,
    ) -> OptSegTentative<Self>
    where
        Self: Copy,
    {
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
pub(crate) trait HasSource {
    const SRC: &'static str;
}

/// Denotes that a type pertains to a region of the FCS file
pub(crate) trait HasRegion {
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
    Segment(SegmentError<UintZeroPad20>),
}

#[derive(From, Display)]
pub enum OptSegmentError {
    Key(ParseKeyError<ParseIntError>),
    Segment(SegmentError<UintZeroPad20>),
}

impl<I, S> OffsetCorrection<I, S> {
    fn new(begin: i32, end: i32) -> Self {
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

impl<I, S> From<(Option<i32>, Option<i32>)> for OffsetCorrection<I, S> {
    fn from(value: (Option<i32>, Option<i32>)) -> Self {
        Self::from((value.0.unwrap_or_default(), value.1.unwrap_or_default()))
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
    fn try_new(begin: T, end: T, conf: &NewSegmentConfig<T, I, S>) -> Result<Self, SegmentError<T>>
    where
        I: HasRegion,
        S: HasSource,
        T: Zero + One + CheckedSub + Into<u64> + Into<i128> + TryFrom<i128> + Ord + Copy,
        u64: From<T>,
    {
        Segment::try_new::<I, S>(begin, end, conf).map(|inner| Self {
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
        T: Into<u64> + TryFrom<u64> + Copy,
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

    // TODO this is just tryfrom
    pub(crate) fn try_as_generic(&self) -> Option<GenericSegment>
    where
        I: HasRegion,
        S: HasSource,
        T: Copy + Into<u64>,
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
    pub(crate) fn overlaps(&self, other: &GenericSegment) -> Result<(), SegmentOverlapError> {
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

    pub(crate) fn find_overlaps(mut xs: Vec<Self>) -> MultiResult<(), SegmentOverlapError> {
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

impl<I, S> SpecificSegment<I, S, UintZeroPad20> {
    pub(crate) fn new_with_len(begin: UintZeroPad20, length: u64) -> Self {
        let inner = if length == 0 {
            Segment::default()
        } else {
            let end = UintZeroPad20::from(u64::from(begin) + length - 1);
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
        allow_negative: bool,
        squish_offsets: bool,
        conf: &NewSegmentConfig<UintSpacePad8, I, SegmentFromHeader>,
    ) -> MultiResult<Self, ImpureError<HeaderSegmentError>>
    where
        I: HasRegion,
    {
        let mut buf0 = [0_u8; 8];
        let mut buf1 = [0_u8; 8];
        h.read_exact(&mut buf0).into_mult()?;
        h.read_exact(&mut buf1).into_mult()?;
        Self::parse(
            &buf0,
            &buf1,
            allow_blank,
            allow_negative,
            squish_offsets,
            conf,
        )
        .mult_map_errors(ImpureError::Pure)
    }

    pub(crate) fn parse(
        bs0: &[u8; 8],
        bs1: &[u8; 8],
        allow_blank: bool,
        allow_negative: bool,
        squish_offsets: bool,
        conf: &NewSegmentConfig<UintSpacePad8, I, SegmentFromHeader>,
    ) -> MultiResult<Self, HeaderSegmentError>
    where
        I: HasRegion,
    {
        let parse_one = |bs, is_begin| {
            UintSpacePad8::from_bytes(bs, allow_blank, allow_negative).map_err(|error| {
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
                SpecificSegment::try_new_squish(begin, end, squish_offsets, conf).into_mult()
            })
    }

    /// Create offset pairs for HEADER
    ///
    /// Returns a string array like "   XXXX    YYYY".
    pub(crate) fn header_string(&self) -> String {
        let (b, e) = self
            .inner
            .try_coords()
            .unwrap_or((UintSpacePad8::zero(), UintSpacePad8::zero()));
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
        begin: UintSpacePad8,
        end: UintSpacePad8,
        squish_offsets: bool,
        conf: &NewSegmentConfig<UintSpacePad8, I, SegmentFromHeader>,
    ) -> Result<Self, SegmentError<UintSpacePad8>>
    where
        I: HasRegion,
    {
        // TODO this might produce really weird errors if run on a 2.0
        // file, so in those cases, this should never be true
        let (b, e) = if squish_offsets && end == UintSpacePad8::zero() && begin > end {
            (UintSpacePad8::zero(), UintSpacePad8::zero())
        } else {
            (begin, end)
        };
        SpecificSegment::try_new(b, e, conf)
    }
}

impl OtherSegment {
    pub(crate) fn parse(
        bs0: &[u8],
        bs1: &[u8],
        allow_negative: bool,
        conf: &NewSegmentConfig<UintZeroPad20, OtherSegmentId, SegmentFromHeader>,
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
                            Ok(UintZeroPad20::zero())
                        } else {
                            Err(ParseFixedUintError::Negative(NegativeOffsetError(x)))
                        }
                    } else {
                        // ASSUME this will never fail because we checked the
                        // sign above
                        Ok(UintZeroPad20(x as u64))
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
        begin_res
            .zip(end_res)
            .mult_errors_into()
            .and_then(|(begin, end)| SpecificSegment::try_new(begin, end, conf).into_mult())
    }

    pub(crate) fn header_string(&self) -> String {
        let (b, e) = self
            .inner
            .try_coords()
            .unwrap_or((UintZeroPad20::zero(), UintZeroPad20::zero()));
        let mut s = String::new();
        s.push_str(&b.to_space_padded_string());
        s.push_str(&e.to_space_padded_string());
        s
    }
}

impl<I> TEXTSegment<I> {
    pub(crate) fn keywords(&self) -> [(String, String); 2]
    where
        I: KeyedReqSegment,
        I::B: Into<UintZeroPad20>
            + From<UintZeroPad20>
            + ReqMetarootKey
            + FromStr<Err = ParseIntError>
            + fmt::Display,
        I::E: Into<UintZeroPad20>
            + From<UintZeroPad20>
            + ReqMetarootKey
            + FromStr<Err = ParseIntError>
            + fmt::Display,
    {
        let i = self.inner;
        let (b, e) = match i {
            Segment::Empty => (UintZeroPad20::zero(), UintZeroPad20::zero()),
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
    pub(crate) fn try_new<I: HasRegion, S: HasSource>(
        begin: T,
        end: T,
        conf: &NewSegmentConfig<T, I, S>,
    ) -> Result<Self, SegmentError<T>>
    where
        T: Zero + One + CheckedSub + Into<i128> + TryFrom<i128> + Ord + Copy,
        u64: From<T>,
    {
        let corr = &conf.corr;
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
                    if let Some(fl) = conf.file_len {
                        if new_end >= fl && !conf.truncate_offsets {
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

    pub(crate) fn h_read_contents<R: Read + Seek>(
        &self,
        h: &mut BufReader<R>,
        buf: &mut Vec<u8>,
    ) -> io::Result<()>
    where
        T: Into<u64> + Copy,
    {
        match self {
            Self::Empty => Ok(()),
            Self::NonEmpty(s) => {
                let begin = s.begin.into();
                let nbytes = u64::from(s.nbytes());

                h.seek(SeekFrom::Start(begin))?;
                h.take(nbytes).read_to_end(buf)?;
                Ok(())
            }
        }
    }

    /// Return the first and last byte if applicable
    pub fn try_coords(&self) -> Option<(T, T)>
    where
        T: Copy,
    {
        self.try_as_nonempty().map(|x| x.coords())
    }

    /// Return byte after end of segment if applicable
    pub(crate) fn try_next_byte(&self) -> Option<NonZeroU64>
    where
        T: Copy + Into<u64>,
    {
        self.try_as_nonempty().map(|x| x.next_byte())
    }

    /// Return the number of bytes in this segment
    pub(crate) fn len(&self) -> u64
    where
        T: Copy + Into<u64>,
    {
        // NOTE In FCS a 0,0 means "empty" but this also means one byte
        // according to the spec's on definitions. The first number points to
        // the first byte in a segment, and the second number points to the last
        // byte, therefore 0,0 means "0 is both the first and last byte, which
        // also means there is one byte".
        self.try_as_nonempty().map_or(0, |s| u64::from(s.nbytes()))
    }

    /// Return true if segment has 0 bytes
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }

    fn fmt_pair(&self) -> String
    where
        T: Default + Copy + fmt::Display,
    {
        let (b, e) = self.try_coords().unwrap_or((T::default(), T::default()));
        format!("{},{}", b, e)
    }

    pub fn as_u64(&self) -> Segment<u64>
    where
        T: Into<u64> + Copy,
    {
        match self {
            Self::Empty => Segment::Empty,
            Self::NonEmpty(x) => Segment::NonEmpty(x.as_u64()),
        }
    }

    // TODO this is just tryfrom
    fn try_as_nonempty(&self) -> Option<NonEmptySegment<T>>
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
    pub fn nbytes(&self) -> NonZeroU64
    where
        T: Into<u64> + Copy,
    {
        NonZeroU64::MIN.saturating_add(self.end.into() - self.begin.into())
    }

    /// Return the first and last byte or this segment
    pub fn coords(&self) -> (T, T)
    where
        T: Copy,
    {
        (self.begin, self.end)
    }

    /// Return the next byte after this segment
    pub fn next_byte(&self) -> NonZeroU64
    where
        T: Into<u64> + Copy,
    {
        // TODO technically this should return option since it isn't guaranteed
        // that the next byte won't wrap
        NonZeroU64::MIN.saturating_add(self.end.into())
    }

    fn new_unchecked(begin: T, end: T) -> Self {
        Self { begin, end }
    }

    pub fn as_u64(&self) -> NonEmptySegment<u64>
    where
        T: Into<u64> + Copy,
    {
        NonEmptySegment {
            begin: self.begin.into(),
            end: self.end.into(),
        }
    }
}

#[derive(From, Display)]
pub enum HeaderSegmentError {
    Standard(SegmentError<UintSpacePad8>),
    Other(SegmentError<UintZeroPad20>),
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

#[derive(Default)]
pub(crate) struct NewSegmentConfig<T, I, S> {
    pub(crate) corr: OffsetCorrection<I, S>,
    pub(crate) file_len: Option<T>,
    pub(crate) truncate_offsets: bool,
}

#[cfg(feature = "serde")]
mod serialize {
    use super::*;

    use serde::ser::{Serialize, SerializeStruct, Serializer};

    impl<T: Serialize> Serialize for Segment<T> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match self {
                Self::NonEmpty(s) => s.serialize(serializer),
                Self::Empty => {
                    let mut state = serializer.serialize_struct("EmptySegment", 2)?;
                    state.serialize_field("start", "0")?;
                    state.serialize_field("end", "0")?;
                    state.end()
                }
            }
        }
    }
}

#[cfg(feature = "python")]
mod python {
    use super::*;
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use pyo3::types::PyTuple;

    // segments will be returned as tuples like (u32, u32) reflecting their
    // exact representation in an FCS file
    impl<'py, T> FromPyObject<'py> for Segment<T>
    where
        T: FromPyObject<'py> + Zero + Ord,
        u64: From<T>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (begin, end): (T, T) = ob.extract()?;
            if begin > end {
                Err(PyValueError::new_err("offset begin is greater than end"))
            } else if begin == T::zero() && end == T::zero() {
                Ok(Self::Empty)
            } else {
                Ok(Self::NonEmpty(NonEmptySegment::new_unchecked(begin, end)))
            }
        }
    }

    impl<'py, T> IntoPyObject<'py> for Segment<T>
    where
        T: Copy,
        u64: From<T>,
    {
        type Target = PyTuple;
        type Output = Bound<'py, <(u64, u64) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.as_u64()
                .try_coords()
                .unwrap_or((0, 0))
                .into_pyobject(py)
        }
    }

    // pyo3 apparently can't deal with phantomdata, this is basically just
    // converting the inner segment which already has this trait
    impl<'py, I, S, T> FromPyObject<'py> for SpecificSegment<I, S, T>
    where
        Segment<T>: FromPyObject<'py>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            Ok(Self {
                inner: ob.extract()?,
                _id: PhantomData,
                _src: PhantomData,
            })
        }
    }

    impl<'py, I, S, T> IntoPyObject<'py> for SpecificSegment<I, S, T>
    where
        T: Copy,
        u64: From<T>,
        Segment<T>: IntoPyObject<'py>,
    {
        type Target = <Segment<T> as IntoPyObject<'py>>::Target;
        type Output = <Segment<T> as IntoPyObject<'py>>::Output;
        type Error = <Segment<T> as IntoPyObject<'py>>::Error;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.inner.into_pyobject(py)
        }
    }
}
