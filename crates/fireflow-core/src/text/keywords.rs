use crate::config::{
    ConfigFlag as _, DummyTriFlag, OverlapCorrectionLimit, ReadDataKeywordsConfig,
    ReadHeaderAndTEXTConfig, ReadStdKeywordsConfig, TriErrorFlag as _, TrimIntraValueWhitespace,
};
use crate::core::Key0LossError;
use crate::logging::{
    DeferredError, DeferredSwitchableErrors, DeferredWarningAndError, LogResult, ResultExt as _,
};
use crate::macros::impl_newtype_try_from;
use crate::segment::{HasRegion, TEXTSegment};
use crate::text::byteord::{
    BitsOrChars, Endian, NewByteOrdError, NoByteOrd, PrivBytes, SizedByteOrd,
};
use crate::text::compensation::{Compensation, NewCompError};
use crate::text::datetimes::{BeginDateTime, EndDateTime};
use crate::text::float_decimal::{DecimalToFloatError, FloatDecimal, HasFloatBounds};
use crate::text::index::{GateIndex, MeasIndex, RegionIndex};
use crate::text::lookup::{
    FromStrDelim, FromStrWith, OptIndexedKey, OptIndexedKeyError, OptMetarootKey, Optional,
    ParseKeyError, ReqIndexedKey, ReqKeyError, ReqMetarootKey, Required, impl_from_str_with_delim,
};
use crate::text::named_vec::{NameMapping, NamedSet, NamedSetMembership};
use crate::text::optional::{CheckMaybe, OptionalZST};
use crate::text::ranged_float::{NonNegFloat, PositiveFloat, RangedFloatError};
use crate::text::relational::{
    ExistingNamedLinkError, KeyToIndexLinkError, KeyToNameLinkError, LinkName,
    OpticalNamedLinkError, OpticalNamesToRemove, RemovedIndexLink, RemovedNamedLink,
    TemporalNamedLinkError,
};
use crate::text::spillover::Spillover;
use crate::text::timestamps::{Btim, Etim, FCSDate, FCSTime, FCSTime60, FCSTime100, Xtim};
use crate::validated::ascii_range::AsciiRangeValue;
use crate::validated::ascii_uint::UintZeroPad20;
use crate::validated::bitmask::BitmaskValue;
use crate::validated::header_segments::NextdataOffsetsError;
use crate::validated::keys::{
    AnyKey, BiIndex, BiIndexedKey, DKey0, DKey1, DKey2, DollarKey, IndexedKey, Key0, Key1, Key2,
    NonStdKey, NonStdKeywords, PrefixSuffix, SpecificKey, StdKeywords, TruncatedString,
    VersionedKey,
};
use crate::validated::keys::{AsStdKey, NonStdKeywordsExt as _, StdKey};
use crate::validated::shortname::Shortname;
use crate::validated::textdelim::{
    DelimCollisionError, HasDelim, TEXTDelim, ambassador_impl_HasDelim,
};

use nonempty_collections::{NEMap, NESlice};
use type_families::{BifunctorOnce, FunctorOnce as _, impl_functor, impl_kind1};

use fireflow_types::config::{ForceLinearScale, TemporalOpticalKey, TruncateEventValues};
use fireflow_types::keywords::{
    self as tk, MeasKeywordClass, RootKeywordClass, Version, VersionMembership,
};
use fireflow_types::nonempty_string::{
    DisplayNE as _, DisplayableNE as _, NEAlt, NEConcat, NEConcat3, NEConcat5, NEDelim,
    NESliceExt as _, NEStr, NEString, ToDisplayNE, ToNE, ambassador_impl_ToDisplayNE,
};
use fireflow_types::{impl_str_enum, impl_str_enum_kw, ne_str};

use ambassador::{Delegate, delegatable_trait};
use bigdecimal::{BigDecimal, ParseBigDecimalError};
use chrono::{NaiveDateTime, NaiveTime, Timelike as _};
use derive_more::{Add, AsMut, AsRef, Display, From, FromStr, Into, Sub};
use derive_new::new;
use itertools::Itertools as _;
use nalgebra::DMatrix;
use nonempty_collections::{
    IntoIteratorExt as _, NEVec,
    iter::{IntoNonEmptyIterator as _, NonEmptyIterator as _, once},
};
use num_traits::PrimInt;
use num_traits::cast::ToPrimitive as _;
use num_traits::identities::{One as _, Zero as _};
use thiserror::Error;
use unicase::Ascii;

use std::collections::HashMap;
use std::fmt::{self, Write as _};
use std::mem::take;
use std::num::{NonZeroU8, NonZeroU32, NonZeroUsize, ParseFloatError, ParseIntError};
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

use super::index::IndexFromOne;
use super::lookup::{
    DiagnosedKeyword, FromStrWithResult, ReqKeyErrorInner, Trimmed, TrimmedKeyword,
};

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{
        AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject, FromPyString, IntoPyNEString,
    },
    fireflow_types::python as py,
    pyo3::prelude::*,
};

#[derive(new)]
pub(crate) struct Escaped<T> {
    delim: TEXTDelim,
    inner: T,
}

impl<T> Escaped<T> {
    pub(crate) fn write_str(&self, buf: &mut String)
    where
        Self: fmt::Display,
    {
        write!(buf, "{self}").expect("str write should be infallible");
    }
}

impl<T: DisplayEscaped + ?Sized> fmt::Display for Escaped<&T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt_escaped(self.delim, f)
    }
}

#[delegatable_trait]
trait DisplayEscaped {
    fn fmt_escaped(&self, delim: TEXTDelim, f: &mut fmt::Formatter<'_>) -> fmt::Result;
}

struct EscapedFormatter<'a, 'b> {
    delim: TEXTDelim,
    inner: &'a mut fmt::Formatter<'b>,
}

impl EscapedFormatter<'_, '_> {
    fn write_with_delim<V>(&mut self, v: &V, escape: bool) -> fmt::Result
    where
        V: ?Sized + for<'a> ToDisplayNE<'a>,
    {
        let delim = self.delim;
        let w = v.as_displayable();
        if escape {
            write!(self, "{w}")?;
            write!(self.inner, "{delim}")
        } else {
            write!(self.inner, "{w}{delim}")
        }
    }
}

impl fmt::Write for EscapedFormatter<'_, '_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let d = self.delim;
        // Check if delim is in str before trying to escape it. This is
        // a massive optimization since encoding and decoding to chars
        // on the fly is extremely expensive as opposed to checking if
        // any single byte in the string is equal to some value.
        if s.contains(char::from(d)) {
            for c in s.bytes() {
                if c == u8::from(d) {
                    // if delimiter found, write it twice
                    write!(self.inner, "{x}{x}", x = self.delim)?;
                } else {
                    // otherwise write non-delim once
                    self.inner.write_char(char::from(c))?;
                }
            }
        } else {
            self.inner.write_str(s)?;
        }
        Ok(())
    }
}

impl<K, I, V> DisplayEscaped for SplitKeyword<DollarKey<K, I>, V>
where
    for<'a> DollarKey<K, I>: ToDisplayNE<'a>,
    for<'a> V: ToDisplayNE<'a>,
{
    fn fmt_escaped(&self, delim: TEXTDelim, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut xf = EscapedFormatter { delim, inner: f };
        // ASSUME standard keys don't need to be escaped because the delim
        // character is 0-31 which never appears in the standard keys
        xf.write_with_delim(&self.key, false)?;
        xf.write_with_delim(&self.value, true)?;
        Ok(())
    }
}

impl DisplayEscaped for NonStdKeyword<'_> {
    fn fmt_escaped(&self, delim: TEXTDelim, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut xf = EscapedFormatter { delim, inner: f };
        xf.write_with_delim(self.key, true)?;
        xf.write_with_delim(&self.value, true)?;
        Ok(())
    }
}

#[derive(Clone, From, Delegate)]
#[delegate(DisplayEscaped)]
pub(crate) enum OffsetKeyword {
    Nextdata(SplitKeyword0<Nextdata>),
    Begindata(SplitKeyword0<Begindata>),
    Enddata(SplitKeyword0<Enddata>),
    Beginanalysis(SplitKeyword0<Beginanalysis>),
    Endanalysis(SplitKeyword0<Endanalysis>),
    Beginstext(SplitKeyword0<Beginstext>),
    Endstext(SplitKeyword0<Endstext>),
}

#[derive(Clone, From, Delegate)]
#[delegate(HasDelim)]
#[delegate(DisplayEscaped)]
pub(crate) enum AnyKeyword<'a> {
    Req(ReqKeyword<'a>),
    Opt(OptKeyword<'a>),
}

#[derive(Clone, From, Delegate)]
#[delegate(HasDelim)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
pub(crate) enum ReqKeyword<'a> {
    Root(ReqRootKeyword<'a>),
    Meas(ReqMeasKeyword<'a>),
}

#[derive(Clone, From, Delegate)]
#[delegate(HasDelim)]
#[delegate(AsKeywordPair)]
#[delegate(DisplayEscaped)]
pub(crate) enum OptKeyword<'a> {
    Root(StdOrNonStdOptRootKeyword<'a>),
    Meas(StdOrNonStdOptMeasKeyword<'a>),
}

#[derive(Clone, From, Delegate)]
#[delegate(HasDelim)]
#[delegate(AsKeywordPair)]
#[delegate(DisplayEscaped)]
pub(crate) enum StdOrNonStdOptRootKeyword<'a> {
    Std(OptRootKeyword<'a>),
    NonStd(NonStdKeyword<'a>),
}

#[derive(Clone, From, Delegate)]
#[delegate(HasDelim)]
#[delegate(AsKeywordPair)]
#[delegate(DisplayEscaped)]
pub(crate) enum StdOrNonStdOptMeasKeyword<'a> {
    Std(OptMeasKeyword<'a>),
    NonStd(NonStdKeyword<'a>),
}

// TODO this shouldn't need to be pub
#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
pub enum ReqRootKeyword<'a> {
    ByteOrd2_0(SplitKeyword0<ByteOrd2_0>),
    ByteOrd3_1(SplitKeyword0<ByteOrd3_1>),
    Par(SplitKeyword0<Par>),
    Tot(SplitKeyword0<Tot>),
    Datatype(SplitKeyword0<AlphaNumType>),
    Mode(SplitKeyword0<Mode>),
    Cyt(RefKeyword0<'a, Cyt3_2>),
}

pub(crate) type NonStdKeyword<'a> = SplitKeyword<&'a NonStdKey, &'a NEStr>;

#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
pub enum OptRootKeyword<'a> {
    GateMeas(GateMeasKeyword<'a>),
    GateRegion(RegionKeyword<'a>),
    Dfc(SplitKeyword2<Dfc>),
    UnstainedCenters(SplitKeyword<DKey0<UnstainedCenters>, NEUnstainedCenters>),
    Timestep(SplitKeyword0<Timestep>),
    CSMode(SplitKeyword0<CSMode>),
    CSVFlag(SplitKeyword1<CSVFlag>),
    CSVBits(NonZeroU32Keyword0<CSVBits>),
    CSTot(NonZeroU32Keyword0<CSTot>),
    Btim2_0(SplitKeyword0<Btim2_0>),
    Btim3_0(SplitKeyword0<Btim3_0>),
    Btim3_1(SplitKeyword0<Btim3_1>),
    Etim2_0(SplitKeyword0<Etim2_0>),
    Etim3_0(SplitKeyword0<Etim3_0>),
    Etim3_1(SplitKeyword0<Etim3_1>),
    Date(SplitKeyword0<FCSDate>),
    Begindatetime(SplitKeyword0<BeginDateTime>),
    Enddatetime(SplitKeyword0<EndDateTime>),
    Gate(SplitKeyword0<Gate>),
    Gating(RefKeyword0<'a, Gating>),
    Comp(RefKeyword0<'a, Compensation3_0>),
    Unicode(RefKeyword0<'a, Unicode>),
    Abrt(SplitKeyword0<Abrt>),
    Lost(SplitKeyword0<Lost>),
    Tr(RefKeyword0<'a, Trigger>),
    Vol(SplitKeyword0<Vol>),
    LastModified(SplitKeyword0<LastModified>),
    Originality(SplitKeyword0<Originality>),
    Mode3_2(SplitKeyword0<Mode3_2>),
    Spillover(RefKeyword0<'a, Spillover>),
    Cyt(NEStringKeyword0<'a, Cyt>),
    Cytsn(NEStringKeyword0<'a, Cytsn>),
    Com(NEStringKeyword0<'a, Com>),
    Cells(NEStringKeyword0<'a, Cells>),
    Exp(NEStringKeyword0<'a, Exp>),
    Fil(NEStringKeyword0<'a, Fil>),
    Inst(NEStringKeyword0<'a, Inst>),
    Op(NEStringKeyword0<'a, Op>),
    Proj(NEStringKeyword0<'a, Proj>),
    Smno(NEStringKeyword0<'a, Smno>),
    Src(NEStringKeyword0<'a, Src>),
    Sys(NEStringKeyword0<'a, Sys>),
    Flowrate(NEStringKeyword0<'a, Flowrate>),
    LastModifier(NEStringKeyword0<'a, LastModifier>),
    UnstainedInfo(NEStringKeyword0<'a, UnstainedInfo>),
    Carrierid(NEStringKeyword0<'a, Carrierid>),
    Carriertype(NEStringKeyword0<'a, Carriertype>),
    Locationid(NEStringKeyword0<'a, Locationid>),
    Plateid(NEStringKeyword0<'a, Plateid>),
    Platename(NEStringKeyword0<'a, Platename>),
    Wellid(NEStringKeyword0<'a, Wellid>),
}

#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
pub enum ReqMeasKeyword<'a> {
    Shortname(RefKeyword1<'a, Shortname>),
    Scale(SplitKeyword1<Scale>),
    TemporalScale3_0(SplitKeyword1<TemporalScale3_0>),
    Width(SplitKeyword1<Width>),
    Range(SplitKeyword1<Range>),
}

#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
pub enum OptMeasKeyword<'a> {
    Longname(NEStringKeyword1<'a, Longname>),
    Filter(NEStringKeyword1<'a, Filter>),
    DetectorType(NEStringKeyword1<'a, DetectorType>),
    DetectorName(NEStringKeyword1<'a, DetectorName>),
    Tag(NEStringKeyword1<'a, Tag>),
    Analyte(NEStringKeyword1<'a, Analyte>),
    OpticalType(NEStringKeyword1<'a, OpticalType>),
    TemporalType(OptZSTKeyword1<TemporalType, TemporalTypeInner>),
    TemporalScale2_0(OptZSTKeyword1<TemporalScale2_0, TemporalScaleInner>),
    Wavelengths(SplitKeyword<DKey1<Wavelengths>, NEWavelengths<'a>>),
    Shortname(RefKeyword1<'a, Shortname>),
    NumType(SplitKeyword1<NumType>),
    Scale(SplitKeyword1<Scale>),
    Power(SplitKeyword1<Power>),
    PercentEmitted(SplitKeyword1<PercentEmitted>),
    DetectorVoltage(SplitKeyword1<DetectorVoltage>),
    Gain(SplitKeyword1<Gain>),
    Wavelength(SplitKeyword1<Wavelength>),
    Display(SplitKeyword1<Display>),
    Feature(RefKeyword1<'a, Feature>),
    Calibration3_1(RefKeyword1<'a, Calibration3_1>),
    Calibration3_2(RefKeyword1<'a, Calibration3_2>),
    PeakBin(SplitKeyword1<PeakBin>),
    PeakIndex(SplitKeyword1<PeakIndex>),
}

#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
pub enum GateMeasKeyword<'a> {
    Scale(SplitKeyword1<GateScale>),
    Shortname(RefKeyword1<'a, GateShortname>),
    PercentEmitted(SplitKeyword1<GatePercentEmitted>),
    Range(RefKeyword1<'a, GateRange>),
    DetectorVoltage(SplitKeyword1<GateDetectorVoltage>),
    Filter(NEStringKeyword1<'a, GateFilter>),
    Longname(NEStringKeyword1<'a, GateLongname>),
    DetectorType(NEStringKeyword1<'a, GateDetectorType>),
}

#[derive(Clone, From, Delegate)]
#[delegate(AsStdKeywordPair)]
#[delegate(DisplayEscaped)]
pub enum RegionKeyword<'a> {
    GateIndex2_0(SplitKeyword1<RegionGateIndex<GateIndex>>),
    GateIndex3_0(SplitKeyword1<RegionGateIndex<MeasOrGateIndex>>),
    GateIndex3_2(SplitKeyword1<RegionGateIndex<PrefixedMeasIndex>>),
    Window(RegionWindowSplitKeyword<'a>),
}

#[derive(Clone, new)]
pub struct SplitKeyword<K, V> {
    pub(crate) key: K,
    pub(crate) value: V,
}

pub type SplitKeyword0<T> = SplitKeyword<DKey0<T>, T>;
pub type SplitKeyword1<T> = SplitKeyword<DKey1<T>, T>;
pub type SplitKeyword2<T> = SplitKeyword<DKey2<T>, T>;

pub type RefKeyword0<'a, T> = SplitKeyword<DKey0<T>, &'a T>;
pub type RefKeyword1<'a, T> = SplitKeyword<DKey1<T>, &'a T>;

pub type OptZSTKeyword1<K, T> = SplitKeyword<DKey1<K>, T>;

pub type NEStringKeyword0<'a, T> = NEStringKeyword<'a, DKey0<T>>;
pub type NEStringKeyword1<'a, T> = NEStringKeyword<'a, DKey1<T>>;

pub type NonZeroU32Keyword0<T> = NonZeroU32Keyword<DKey0<T>>;

pub type NEStringKeyword<'a, K> = SplitKeyword<K, &'a NEStr>;
pub type NonZeroU32Keyword<K> = SplitKeyword<K, NonZeroU32>;

pub type RegionWindowSplitKeyword<'a> = SplitKeyword<DKey1<RegionWindow>, RegionWindowRef<'a>>;

impl<T> SplitKeyword0<T> {
    pub(crate) fn from_value0(value: T) -> Self {
        Self::new(DKey0::<T>::default(), value)
    }
}

impl<T> SplitKeyword1<T> {
    pub(crate) fn from_value1(value: T, i: impl Into<IndexFromOne>) -> Self {
        Self::new(DKey1::<T>::new_i1(i.into()), value)
    }
}

impl<'a, T> RefKeyword0<'a, T> {
    pub(crate) fn from_ref0(value: &'a T) -> Self {
        Self::new(DKey0::<T>::default(), value)
    }
}

impl<'a, T> RefKeyword1<'a, T> {
    pub(crate) fn from_ref1(value: &'a T, i: impl Into<IndexFromOne>) -> Self {
        Self::new(DKey1::<T>::new_i1(i.into()), value)
    }
}

impl<'a, T> NEStringKeyword0<'a, T> {
    pub(crate) fn try_new_ne_str0(kw: &'a T) -> Option<Self>
    where
        T: AsRef<str>,
    {
        let value = NEStr::try_new(kw.as_ref())?;
        Some(Self::new(DKey0::<T>::default(), value))
    }
}

impl<'a, T> NEStringKeyword1<'a, T> {
    pub(crate) fn try_new_ne_str1(kw: &'a T, i: impl Into<IndexFromOne>) -> Option<Self>
    where
        T: AsRef<str>,
    {
        let value = NEStr::try_new(kw.as_ref())?;
        Some(Self::new(DKey1::<T>::new_i1(i.into()), value))
    }
}

impl<T> NonZeroU32Keyword0<T> {
    pub(crate) fn try_new_nz_u32(kw: &T) -> Option<Self>
    where
        T: AsRef<u32>,
    {
        let value = NonZeroU32::new(*kw.as_ref())?;
        Some(Self::new(DKey0::<T>::default(), value))
    }
}

impl<'a> OptRootKeyword<'a> {
    pub(crate) fn from_u32<T>(x: &T) -> Option<Self>
    where
        T: AsRef<u32>,
        Self: From<NonZeroU32Keyword0<T>>,
    {
        NonZeroU32Keyword0::try_new_nz_u32(x).map(Self::from)
    }

    pub(crate) fn from_unstainedcenters(x: &'a UnstainedCenters) -> Option<Self> {
        Some(Self::from(SplitKeyword::new(DKey0::default(), x.try_ne()?)))
    }
}

impl<'a> OptMeasKeyword<'a> {
    pub(crate) fn from_wavelengths(x: &'a Wavelengths, i: MeasIndex) -> Option<Self> {
        let ret = SplitKeyword::new(DKey1::new_i1(i), x.try_ne()?);
        Some(Self::from(ret))
    }

    pub(crate) fn from_opt_zst<T, Z>(x: T, i: MeasIndex) -> Option<Self>
    where
        Z: Copy,
        T: AsRef<Option<Z>>,
        Self: From<OptZSTKeyword1<T, Z>>,
    {
        let y: &Option<Z> = x.as_ref();
        let z = y.as_ref().copied()?;
        let ret = SplitKeyword::new(DKey1::<T>::new_i1(i), z);
        Some(Self::from(ret))
    }
}

pub(crate) trait Keyword0FromValue<'a> {
    fn from_value<T>(x: T) -> Self
    where
        Self: From<SplitKeyword0<T>>,
    {
        Self::from(SplitKeyword0::from_value0(x))
    }

    fn from_ref<T>(x: &'a T) -> Self
    where
        Self: From<RefKeyword0<'a, T>>,
    {
        Self::from(RefKeyword0::from_ref0(x))
    }

    fn from_str<T>(x: &'a T) -> Option<Self>
    where
        T: AsRef<str>,
        Self: From<NEStringKeyword0<'a, T>>,
    {
        NEStringKeyword0::try_new_ne_str0(x).map(Self::from)
    }
}

pub(crate) trait Keyword1FromValue<'a> {
    fn from_value<T>(x: T, i: impl Into<IndexFromOne>) -> Self
    where
        Self: From<SplitKeyword1<T>>,
    {
        Self::from(SplitKeyword1::from_value1(x, i))
    }

    fn from_ref<T>(x: &'a T, i: impl Into<IndexFromOne>) -> Self
    where
        Self: From<RefKeyword1<'a, T>>,
    {
        Self::from(RefKeyword1::from_ref1(x, i))
    }

    fn from_str<T>(x: &'a T, i: impl Into<IndexFromOne>) -> Option<Self>
    where
        T: AsRef<str>,
        Self: From<NEStringKeyword1<'a, T>>,
    {
        NEStringKeyword1::try_new_ne_str1(x, i).map(Self::from)
    }
}

impl Keyword0FromValue<'_> for OffsetKeyword {}
impl<'a> Keyword0FromValue<'a> for ReqRootKeyword<'a> {}
impl<'a> Keyword0FromValue<'a> for OptRootKeyword<'a> {}

impl<'a> Keyword1FromValue<'a> for ReqMeasKeyword<'a> {}
impl<'a> Keyword1FromValue<'a> for OptMeasKeyword<'a> {}
impl<'a> Keyword1FromValue<'a> for GateMeasKeyword<'a> {}
impl Keyword1FromValue<'_> for RegionKeyword<'_> {}

#[delegatable_trait]
pub(crate) trait AsStdKeywordPair {
    fn as_std_key_pair(&self) -> (StdKey, NEString);
}

#[delegatable_trait]
pub(crate) trait AsKeywordPair {
    fn as_key_pair(&self) -> (AnyKey, NEString);

    fn as_str_pair(&self) -> (NEString, NEString) {
        let (k, v) = self.as_key_pair();
        (ToNE(k).to_ne_string(), v)
    }
}

impl<K, V> AsStdKeywordPair for SplitKeyword<K, V>
where
    K: AsStdKey,
    for<'a> V: ToDisplayNE<'a>,
{
    fn as_std_key_pair(&self) -> (StdKey, NEString) {
        (self.key.as_std_key(), ToNE(&self.value).to_ne_string())
    }
}

impl<T: AsStdKeywordPair> AsKeywordPair for T {
    fn as_key_pair(&self) -> (AnyKey, NEString) {
        let (k, v) = self.as_std_key_pair();
        (k.into(), v)
    }
}

impl AsKeywordPair for NonStdKeyword<'_> {
    fn as_key_pair(&self) -> (AnyKey, NEString) {
        (self.key.clone().into(), ToNE(&self.value).to_ne_string())
    }
}

impl<I, V: HasDelim> HasDelim for SplitKeyword<DollarKey<V, I>, V> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        self.value.has_delim(d)
    }
}

impl<I, V: HasDelim> HasDelim for SplitKeyword<DollarKey<V, I>, &V> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        self.value.has_delim(d)
    }
}

impl HasDelim for ReqRootKeyword<'_> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        if let Self::Cyt(x) = self {
            x.has_delim(d)
        } else {
            None
        }
    }
}

impl HasDelim for ReqMeasKeyword<'_> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        if let Self::Shortname(x) = self {
            x.has_delim(d)
        } else {
            None
        }
    }
}

impl HasDelim for NonStdKeyword<'_> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        self.key.has_delim(d).or(self.value.has_delim(d))
    }
}

impl HasDelim for OptRootKeyword<'_> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        match self {
            Self::GateMeas(x) => x.has_delim(d),
            Self::Unicode(x) => x.has_delim(d),
            Self::Tr(x) => x.has_delim(d),
            Self::Spillover(x) => x.has_delim(d),
            Self::Cyt(x) => x.value.has_delim(d),
            Self::Cytsn(x) => x.value.has_delim(d),
            Self::Com(x) => x.value.has_delim(d),
            Self::Cells(x) => x.value.has_delim(d),
            Self::Exp(x) => x.value.has_delim(d),
            Self::Fil(x) => x.value.has_delim(d),
            Self::Inst(x) => x.value.has_delim(d),
            Self::Op(x) => x.value.has_delim(d),
            Self::Proj(x) => x.value.has_delim(d),
            Self::Smno(x) => x.value.has_delim(d),
            Self::Src(x) => x.value.has_delim(d),
            Self::Sys(x) => x.value.has_delim(d),
            Self::Flowrate(x) => x.value.has_delim(d),
            Self::LastModifier(x) => x.value.has_delim(d),
            Self::UnstainedInfo(x) => x.value.has_delim(d),
            Self::Carrierid(x) => x.value.has_delim(d),
            Self::Carriertype(x) => x.value.has_delim(d),
            Self::Locationid(x) => x.value.has_delim(d),
            Self::Plateid(x) => x.value.has_delim(d),
            Self::Platename(x) => x.value.has_delim(d),
            Self::Wellid(x) => x.value.has_delim(d),
            _ => None,
        }
    }
}

impl HasDelim for OptMeasKeyword<'_> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        match self {
            Self::Shortname(x) => x.has_delim(d),
            Self::Feature(x) => x.has_delim(d),
            Self::Calibration3_1(x) => x.has_delim(d),
            Self::Calibration3_2(x) => x.has_delim(d),
            Self::Longname(x) => x.value.has_delim(d),
            Self::Filter(x) => x.value.has_delim(d),
            Self::DetectorType(x) => x.value.has_delim(d),
            Self::DetectorName(x) => x.value.has_delim(d),
            Self::Tag(x) => x.value.has_delim(d),
            Self::Analyte(x) => x.value.has_delim(d),
            Self::OpticalType(x) => x.value.has_delim(d),
            _ => None,
        }
    }
}

impl HasDelim for GateMeasKeyword<'_> {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        match self {
            Self::Filter(x) => x.value.has_delim(d),
            Self::Longname(x) => x.value.has_delim(d),
            Self::DetectorType(x) => x.value.has_delim(d),
            Self::Shortname(x) => x.value.has_delim(d),
            _ => None,
        }
    }
}

/// Value for $NEXTDATA (all versions)
#[derive(From, Into, FromStr, Debug, Clone, Copy, PartialEq, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[into(u64, UintZeroPad20)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct Nextdata(pub UintZeroPad20);

impl Nextdata {
    // TODO unlike all other keyword lookup ops this won't demote a bad key on
    // failure since it is read-only. Not sure how to fix this without
    // destroying many other things
    pub(crate) fn lookup_ro(
        kws: &StdKeywords,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> DeferredWarningAndError<Option<Self>, ReadNextdataError, ReadNextdataError> {
        let k = SpecificKey::default();
        if let Some(is_err) = conf.allow_missing_nextdata.is_error() {
            let res = Self::get_req_with(kws, k, (), conf).map(|x| Some(x.native));
            if is_err {
                res.into_log().set_err_value(None)
            } else {
                LogResult::Succ(res.into_succ())
            }
        } else {
            let ret = kws
                .get(&k.as_std_key())
                .and_then(|v| Self::from_str_with(v, (), conf).ok())
                .map(|x| x.native);
            LogResult::new_ok(ret)
        }
    }

    pub(crate) fn validate_text_offset<I>(
        self,
        s: &mut TEXTSegment<I>,
        limit: OverlapCorrectionLimit,
    ) -> Option<NextdataOffsetsError>
    where
        I: HasRegion,
    {
        let q = s.try_as_generic()?;
        let n = u64::from(self);
        let overlap = (q.end + 1).saturating_sub(n);
        if n == 0 {
            None
        } else if overlap <= limit.0 {
            s.truncate(overlap);
            None
        } else {
            Some(NextdataOffsetsError::new(self, q))
        }
    }
}

impl FromStrWith for Nextdata {
    type Err = ParseNextdataError;
    type Payload<'a> = ();
    type Diagnostic = ();
    type Config = ReadHeaderAndTEXTConfig;

    fn from_str_with(s: &str, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
        let corr = i128::from(conf.nextdata_correction);
        let x = s.parse::<i128>()?;
        let y = x.saturating_add(corr);
        if y < 0 {
            Err(ParseNextdataError::from(NegativeNextdataError(x)))
        } else {
            let out = u64::try_from(y).unwrap_or(u64::MAX);
            Ok(DiagnosedKeyword::new1(Self(UintZeroPad20(out))))
        }
    }
}

pub type ReadNextdataError = ReqKeyErrorInner<ParseNextdataError, Nextdata, ()>;

/// Error when parsing [`Nextdata`] from [`String`]
#[derive(Debug, Display, From, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ParseNextdataError {
    Int(ParseIntError),
    Negative(NegativeNextdataError),
}

/// Error when $NEXTDATA is negative
#[derive(Debug, Error)]
#[error("$NEXTDATA value is negative ({0})")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NegativeNextdataError(i128);

/// The value for the $PnE key (all versions).
///
/// Format is assumed to be 'f1,f2'
#[derive(Clone, Copy, PartialEq, Debug, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Scale {
    /// Linear scale (ie '0,0')
    #[default]
    Linear,

    /// Log scale, where both numbers are positive
    Log(LogScale),
}

impl ToDisplayNE<'_> for Scale {
    type NE = NEAlt<&'static NEStr, ToNE<LogScale>>;
    fn to_ne(&self) -> Self::NE {
        match self {
            Self::Linear => NEAlt::Left(ne_str!("0,0")),
            Self::Log(x) => NEAlt::Right(ToNE(*x)),
        }
    }
}

/// Diagnostic data from parsing $PnE
#[derive(Clone, PartialEq, From)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum OpticalScaleFix {
    /// Was forced to be linear (which overrides everything else)
    Forced(String),
    /// Fixes shared with $Gn* keywords
    Inner(ScaleFix),
}

impl Default for OpticalScaleFix {
    fn default() -> Self {
        Self::Inner(ScaleFix::default())
    }
}

/// Diagnostic data from parsing $PnE or $GmE
#[derive(Default, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum ScaleFix {
    /// Nothing happened
    #[default]
    None,
    /// Whitespace was trimmed
    Trimmed(String),
    /// Zero log offset was corrected
    LogFixed(String),
    /// Trimmed and zero log offset was corrected
    TrimmedLogFixed(String),
}

#[derive(Clone, Copy, PartialEq, Debug, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct LogScale {
    pub decades: PositiveFloat,
    pub offset: PositiveFloat,
}

impl<'a> ToDisplayNE<'a> for LogScale {
    type NE = NEDelim<[ToNE<PositiveFloat>; 2]>;
    fn to_ne(&'a self) -> Self::NE {
        NEDelim::new(',', [ToNE(self.decades), ToNE(self.offset)])
    }
}

impl Scale {
    pub fn try_new_log(decades: f32, offset: f32) -> Result<Self, LogRangeError> {
        (decades, offset).try_into().map(Self::Log)
    }

    fn parse_fix_maybe(
        s: &str,
        conf: &ReadStdKeywordsConfig,
    ) -> Result<DiagnosedKeyword<Self, ScaleFix>, ScaleError> {
        let go = |x: TrimmedKeyword<_>| {
            let d = x.trimmed.map(ScaleFix::Trimmed).unwrap_or_default();
            DiagnosedKeyword::new(x.native, d)
        };
        let res = Self::from_str_delim(s, conf.trim_intra_value_whitespace);
        if conf.fix_log_scale_offsets.is_set() {
            match res {
                Ok(x) => Ok(go(x)),
                Err(e) => {
                    if let ScaleError::LogRange(le) = e {
                        le.try_fix_offset()
                            .map(Self::Log)
                            .map(|x| {
                                // TODO there is no way to tell if the
                                // previous value was trimmed
                                let d = ScaleFix::LogFixed(s.to_owned());
                                DiagnosedKeyword::new(x, d)
                            })
                            .map_err(ScaleError::LogRange)
                    } else {
                        Err(e)
                    }
                }
            }
        } else {
            res.map(go)
        }
    }
}

impl TryFrom<(f32, f32)> for LogScale {
    type Error = LogRangeError;

    fn try_from(value: (f32, f32)) -> Result<Self, Self::Error> {
        let (d0, o0) = value;
        if let (Ok(decades), Ok(offset)) =
            (PositiveFloat::try_from(d0), PositiveFloat::try_from(o0))
        {
            Ok(Self::new(decades, offset))
        } else {
            Err(LogRangeError::new(d0, o0))
        }
    }
}

impl FromStrWith for Scale {
    type Err = ScaleError;
    type Payload<'a> = AlphaNumType;
    type Diagnostic = OpticalScaleFix;
    type Config = ReadStdKeywordsConfig;

    fn from_str_with(s: &str, dt: AlphaNumType, conf: &Self::Config) -> FromStrWithResult<Self> {
        if (matches!(conf.force_linear_scale, ForceLinearScale::AllNonInt)
            && !matches!(dt, AlphaNumType::Integer))
            || matches!(conf.force_linear_scale, ForceLinearScale::All)
        {
            let d = OpticalScaleFix::Forced(s.to_owned());
            Ok(DiagnosedKeyword::new(Self::Linear, d))
        } else {
            Self::parse_fix_maybe(s, conf).map(BifunctorOnce::second_into_once)
        }
    }
}

impl FromStrDelim for Scale {
    type Err = ScaleError;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        let x0 = iter.next();
        let x1 = iter.next();
        let x2 = iter.next();
        match (x0, x1, x2) {
            (Some(ds), Some(os), None) => {
                let f1 = ds.parse().map_err(ScaleError::FloatError)?;
                let f2 = os.parse().map_err(ScaleError::FloatError)?;
                match (f1, f2) {
                    (0.0, 0.0) => Ok(Self::Linear),
                    (decades, offset) => {
                        Self::try_new_log(decades, offset).map_err(ScaleError::LogRange)
                    }
                }
            }
            _ => Err(ScaleError::WrongFormat),
        }
    }
}

/// Error when parsing [`Scale`] from string
#[derive(Debug, Error)]
pub enum ScaleError {
    #[error("{0}")]
    FloatError(ParseFloatError),
    #[error("{0}")]
    LogRange(LogRangeError),
    #[error("must be like 'f1,f2'")]
    WrongFormat,
}

/// Error when parsing [`Scale`] as log from string
#[derive(Debug, Error, new)]
#[error("decades/offset must both be positive, got '{decades},{offset}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::InvalidKeywordValueError))]
pub struct LogRangeError {
    decades: f32,
    offset: f32,
}

impl LogRangeError {
    /// Try to 'fix' log scales which are 'X,0' where X is positive.
    ///
    /// The 'recommended' way to fix these is to make the 0 and 1, which is
    /// what this does. This is a heuristic hack to get some files to work
    /// which didn't write $PnE correctly.
    pub(crate) fn try_fix_offset(self) -> Result<LogScale, Self> {
        if self.offset.is_zero()
            && let Ok(decades) = PositiveFloat::try_from(self.decades)
        {
            return Ok(LogScale::new(decades, PositiveFloat::one()));
        }
        Err(self)
    }
}

/// The value of the $PnG keyword
#[derive(Clone, Copy, PartialEq, From, FromStr, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct Gain(pub PositiveFloat);

impl Gain {
    pub(crate) fn lookup_temporal_3_0<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> DeferredSwitchableErrors<Option<Self>, DummyTriFlag, LookupTemporalGainError>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let ignore = &AsRef::<ReadStdKeywordsConfig>::as_ref(conf).ignore_time_optical_keys;
        let drop_flag = AsRef::<ReadDataKeywordsConfig>::as_ref(conf)
            .process_optional_failure
            .as_triflag();
        if ignore.0.contains(&TemporalOpticalKey::Gain) {
            nonstd.transfer_demoted(std, Self::std(i));
            LogResult::new_switchable_ok(None, drop_flag)
        } else {
            Self::remove_or_drop_meas_opt(std, nonstd, i, conf.as_ref())
                .map_switchable_errors(LookupTemporalGainError::from)
                .into_semigroup()
                .eval_deferred_switchable_error3(|gain| {
                    (!gain.is_none_or(|g| g.0.is_one())).then_some(TemporalGainError(i).into())
                })
        }
    }
}

/// Error when lookup up [`Gain`] from keyword pairs
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupTemporalGainError {
    Parse(OptIndexedKeyError<Gain>),
    HasGain(TemporalGainError),
}

/// Error when time measurement has [`Gain`] ($PnG)
#[derive(Debug, Error)]
#[error("{} must be 1.0 or not set for temporal measurement", Gain::std(self.0))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct TemporalGainError(MeasIndex);

/// The value of the $TIMESTEP keyword
#[derive(Clone, Copy, PartialEq, From, FromStr, Into, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[into(f32, PositiveFloat)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct Timestep(pub PositiveFloat);

impl_newtype_try_from!(Timestep, PositiveFloat, f32, RangedFloatError);

impl Default for Timestep {
    fn default() -> Self {
        Self(PositiveFloat::one())
    }
}

impl Timestep {
    pub(crate) fn loss_error(self) -> Option<Key0LossError<Self>> {
        (!self.0.is_one()).then_some(Key0LossError::default())
    }

    pub(crate) fn lookup(
        std: &mut StdKeywords,
        conf: &ReadStdKeywordsConfig,
    ) -> Result<DiagnosedKeyword<Self, TimestepAdded>, ReqKeyError<Self>> {
        match Self::remove_metaroot_req(std) {
            Ok(x) => Ok(DiagnosedKeyword::new(x, false)),
            Err(e) => conf
                .add_missing_timestep
                .map_or(Err(e), |x| Ok(DiagnosedKeyword::new(x, true))),
        }
    }
}

pub(crate) type TimestepAdded = bool;

/// The value of the $TR field (all versions)
///
/// This is formatted as 'string,f' where 'string' is a measurement name.
#[derive(Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Trigger {
    /// The measurement name (assumed to match a '$PnN' value).
    pub measurement: Shortname,

    /// The threshold of the trigger.
    pub threshold: u32,
}

impl<'a> ToDisplayNE<'a> for Trigger {
    type NE = NEConcat3<ToNE<&'a Shortname>, char, u32>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat::new(ToNE(&self.measurement), ',').append(self.threshold)
    }
}

impl HasDelim for Trigger {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        self.measurement.has_delim(d)
    }
}

impl Trigger {
    pub(crate) fn reassign(&mut self, mapping: &NameMapping) {
        if let Some(new) = mapping.get(&self.measurement) {
            self.measurement = (*new).clone();
        }
    }

    pub(crate) fn existing_link_error(
        &self,
        names: &OpticalNamesToRemove<'_>,
    ) -> Option<ExistingNamedLinkError<Self, ()>> {
        let m = &self.measurement;
        (names.as_ref().contains(m))
            .then(|| ExistingNamedLinkError::new(Key0::default(), NEVec::new(m.clone())))
    }

    pub(crate) fn invalid_link_error(
        &self,
        names: &NamedSet<'_>,
    ) -> Option<KeyToNameLinkError<Self>> {
        let m = &self.measurement;
        match names.membership(m) {
            NamedSetMembership::None => {
                Some(OpticalNamedLinkError::new_i0(NEVec::new(m.clone())).into())
            }
            NamedSetMembership::Center => Some(TemporalNamedLinkError::new_i0(m.clone()).into()),
            NamedSetMembership::NonCenter => None,
        }
    }

    pub(crate) fn remove_invalid_links(
        src: &mut Option<Self>,
        names: &NamedSet<'_>,
    ) -> Option<RemovedNamedLink<Self>> {
        let go = |tr: &Self| {
            let m = &tr.measurement;
            match names.membership(m) {
                NamedSetMembership::None => Some(LinkName::Both(NEVec::new(m.clone()), None)),
                NamedSetMembership::Center => Some(LinkName::Temporal(m.clone())),
                NamedSetMembership::NonCenter => None,
            }
        };
        RemovedNamedLink::remove_invalid_link(src, go)
    }
}

impl FromStrDelim for Trigger {
    type Err = TriggerError;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        let x0 = iter.next();
        let x1 = iter.next();
        let x2 = iter.next();
        match (x0, x1, x2) {
            (Some(p), Some(n1), None) => {
                n1.parse()
                    .map_err(TriggerError::IntFormat)
                    .map(|threshold| Self {
                        measurement: Shortname::new_unchecked(p),
                        threshold,
                    })
            }
            _ => Err(TriggerError::WrongFieldNumber),
        }
    }
}

impl_from_str_with_delim!(Trigger, TriggerError);

/// Error when parsing [`Trigger`] from string
#[derive(Debug, Error)]
pub enum TriggerError {
    #[error("must be like 'string,f'")]
    WrongFieldNumber,
    #[error("{0}")]
    IntFormat(ParseIntError),
}

impl_str_enum_kw!(
    /// The values used for the $MODE key (up to 3.1)
    #[derive(PartialEq, Eq, Default, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize))]
    #[cfg_attr(feature = "python", derive(FromPyString, IntoPyNEString))]
    pub Mode,
    /// Error when parsing [`Mode`] from string
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
    pub ModeError,
    #[default]
    List         => ne_str!("L"),
    Uncorrelated => ne_str!("U"),
    Correlated   => ne_str!("C")
);

/// The value for the $MODE key, which can only contain 'L' (3.2)
#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyNEString))]
pub struct Mode3_2;

impl ToDisplayNE<'_> for Mode3_2 {
    type NE = &'static NEStr;
    fn to_ne(&self) -> Self::NE {
        ne_str!("L")
    }
}

impl FromStr for Mode3_2 {
    type Err = Mode3_2Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "L" => Ok(Self),
            _ => Err(Mode3_2Error),
        }
    }
}

impl TryFrom<Mode> for Mode3_2 {
    type Error = ModeUpgradeError;

    fn try_from(value: Mode) -> Result<Self, Self::Error> {
        match value {
            Mode::List => Ok(Self),
            _ => Err(ModeUpgradeError),
        }
    }
}

/// Error when parsing [`Mode3_2`]
#[derive(Debug, Error)]
#[error("can only be 'L'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
pub struct Mode3_2Error;

/// Error when converting [`Mode`] to [`Mode3_2`]
#[derive(Debug, Error)]
#[error("$MODE must be 'L'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct ModeUpgradeError;

/// The value for the $PnD key (3.1+)
#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Display {
    /// Linear display (value like `"Linear,<lower>,<upper>"`)
    Lin { lower: f32, upper: f32 },

    /// Logarithmic display (value like `"Logarithmic,<decades>,<offset>"`)
    Log {
        decades: PositiveFloat,
        offset: PositiveFloat,
    },
}

impl ToDisplayNE<'_> for Display {
    type NE = NEConcat5<&'static NEStr, char, f32, char, f32>;
    fn to_ne(&self) -> Self::NE {
        let (m, x, y) = match self {
            Self::Lin { lower, upper } => (ne_str!("Linear"), *lower, *upper),
            Self::Log { offset, decades } => (
                ne_str!("Logarithmic"),
                f32::from(*decades),
                f32::from(*offset),
            ),
        };
        NEConcat::new(m, ',').append(x).append(',').append(y)
    }
}

impl FromStrDelim for Display {
    type Err = DisplayError;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        let x0 = iter.next();
        let x1 = iter.next();
        let x2 = iter.next();
        let x3 = iter.next();
        match (x0, x1, x2, x3) {
            (Some(which), Some(s1), Some(s2), None) => {
                let f1 = s1.parse().map_err(DisplayError::FloatError)?;
                let f2 = s2.parse().map_err(DisplayError::FloatError)?;
                match which {
                    "Linear" => {
                        if f1 > f2 {
                            Err(DisplayError::Linear(f1, f2))
                        } else {
                            Ok(Self::Lin {
                                lower: f1,
                                upper: f2,
                            })
                        }
                    }
                    "Logarithmic" => match (f1.try_into(), f2.try_into()) {
                        (Ok(decades), Ok(offset)) => Ok(Self::Log { decades, offset }),
                        _ => Err(DisplayError::Log(f1, f2)),
                    },
                    _ => Err(DisplayError::InvalidType),
                }
            }
            _ => Err(DisplayError::FormatError),
        }
    }
}

impl_from_str_with_delim!(Display, DisplayError);

/// Error when parsing [`enum@Display`] from string
#[derive(Debug, Error)]
pub enum DisplayError {
    #[error("{0}")]
    FloatError(ParseFloatError),
    #[error("Type must be either 'Logarithmic' or 'Linear'")]
    InvalidType,
    #[error("must be like 'string,f1,f2'")]
    FormatError,
    #[error("linear bounds out of order, got 'Linear,{0},{1}'")]
    Linear(f32, f32),
    #[error("log must only use positive floats, got 'Logarithmic,{0},{1}'")]
    Log(f32, f32),
}

impl_str_enum_kw!(
    /// The three values for the $PnDATATYPE keyword (3.2+)
    #[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize))]
    #[cfg_attr(feature = "python", derive(FromPyString, IntoPyNEString))]
    pub NumType,
    /// Error when parsing [`NumType`] from string
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
    pub NumTypeError,
    Integer => ne_str!("I"),
    Float   => ne_str!("F"),
    Double  => ne_str!("D")
);

/// The $BYTEORD field in FCS 2.0 and 3.0
///
/// This must be a list of integers belonging to the unordered set {1..N} where
/// N is the total number of bytes. The numbers will be stored as one less the
/// displayed integers to make array indexing easier.
#[derive(Clone, Copy, From, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub enum ByteOrd2_0 {
    O1(SizedByteOrd<1>),
    O2(SizedByteOrd<2>),
    O3(SizedByteOrd<3>),
    O4(SizedByteOrd<4>),
    O5(SizedByteOrd<5>),
    O6(SizedByteOrd<6>),
    O7(SizedByteOrd<7>),
    O8(SizedByteOrd<8>),
}

impl FromStr for ByteOrd2_0 {
    type Err = ParseByteOrdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (pass, fail): (Vec<_>, Vec<_>) =
            s.split(',').map(str::parse::<NonZeroU8>).partition_result();
        if fail.is_empty() {
            Self::try_from(&pass[..]).map_err(ParseByteOrdError::Order)
        } else {
            Err(ParseByteOrdError::Digit(ByteordDigitError))
        }
    }
}

/// Error when parsing [`ByteOrd2_0`] from string
#[derive(From, Debug, Display, Error)]
pub enum ParseByteOrdError {
    Order(NewByteOrdError),
    Digit(ByteordDigitError),
}

/// Error when [`ByteOrd2_0`] has invalid digit(s)
#[derive(Debug, Error)]
#[error("could not parse digits from byte order")]
pub struct ByteordDigitError;

impl Default for ByteOrd2_0 {
    fn default() -> Self {
        // Default $BYTEORD for FCS 2.0 is simply 32-bit little endian
        Self::O4(SizedByteOrd::default())
    }
}

impl From<NoByteOrd<true>> for ByteOrd2_0 {
    fn from(_: NoByteOrd<true>) -> Self {
        Self::default()
    }
}

impl ByteOrd2_0 {
    #[must_use]
    pub(crate) fn nbytes(&self) -> PrivBytes {
        match self {
            Self::O1(_) => SizedByteOrd::<1>::nbytes(),
            Self::O2(_) => SizedByteOrd::<2>::nbytes(),
            Self::O3(_) => SizedByteOrd::<3>::nbytes(),
            Self::O4(_) => SizedByteOrd::<4>::nbytes(),
            Self::O5(_) => SizedByteOrd::<5>::nbytes(),
            Self::O6(_) => SizedByteOrd::<6>::nbytes(),
            Self::O7(_) => SizedByteOrd::<7>::nbytes(),
            Self::O8(_) => SizedByteOrd::<8>::nbytes(),
        }
    }

    #[cfg(feature = "python")]
    fn to_vec(self) -> Vec<NonZeroU8> {
        match self {
            Self::O1(x) => <[NonZeroU8; 1]>::from(x).to_vec(),
            Self::O2(x) => <[NonZeroU8; 2]>::from(x).to_vec(),
            Self::O3(x) => <[NonZeroU8; 3]>::from(x).to_vec(),
            Self::O4(x) => <[NonZeroU8; 4]>::from(x).to_vec(),
            Self::O5(x) => <[NonZeroU8; 5]>::from(x).to_vec(),
            Self::O6(x) => <[NonZeroU8; 6]>::from(x).to_vec(),
            Self::O7(x) => <[NonZeroU8; 7]>::from(x).to_vec(),
            Self::O8(x) => <[NonZeroU8; 8]>::from(x).to_vec(),
        }
    }

    fn is_endian(&self) -> bool {
        matches!(
            self,
            Self::O1(SizedByteOrd::Endian(_))
                | Self::O2(SizedByteOrd::Endian(_))
                | Self::O3(SizedByteOrd::Endian(_))
                | Self::O4(SizedByteOrd::Endian(_))
                | Self::O5(SizedByteOrd::Endian(_))
                | Self::O6(SizedByteOrd::Endian(_))
                | Self::O7(SizedByteOrd::Endian(_))
                | Self::O8(SizedByteOrd::Endian(_))
        )
    }
}

/// The $BYTEORD field in FCS 3.1 and 3.2
#[derive(Clone, Copy, From, FromStr, Default, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct ByteOrd3_1(pub Endian);

impl From<NoByteOrd<false>> for ByteOrd3_1 {
    fn from(_: NoByteOrd<false>) -> Self {
        Self::default()
    }
}

impl_str_enum_kw!(
    /// The four allowed values for the $DATATYPE keyword.
    #[derive(Eq, PartialEq, PartialOrd, Ord, Hash, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize))]
    #[cfg_attr(feature = "python", derive(FromPyString, IntoPyNEString))]
    pub AlphaNumType,
    /// Error when parsing [`AlphaNumType`] from string
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
    pub AlphaNumTypeError,
    Ascii   => ne_str!("A"),
    Integer => ne_str!("I"),
    Float   => ne_str!("F"),
    Double  => ne_str!("D")
);

impl AlphaNumType {
    pub(crate) fn matches_truncation(self, trunc: TruncateEventValues) -> bool {
        matches!(
            (trunc, self),
            (TruncateEventValues::IntOnly, Self::Integer) | (TruncateEventValues::All, _)
        )
    }
}

impl From<NumType> for AlphaNumType {
    fn from(value: NumType) -> Self {
        match value {
            NumType::Integer => Self::Integer,
            NumType::Float => Self::Float,
            NumType::Double => Self::Double,
        }
    }
}

impl TryFrom<AlphaNumType> for NumType {
    type Error = ();
    fn try_from(value: AlphaNumType) -> Result<Self, Self::Error> {
        match value {
            AlphaNumType::Integer => Ok(Self::Integer),
            AlphaNumType::Float => Ok(Self::Float),
            AlphaNumType::Double => Ok(Self::Double),
            AlphaNumType::Ascii => Err(()),
        }
    }
}

/// The value of the $PnE key for temporal measurements (all versions)
///
/// This can only be linear (0,0)
#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub struct TemporalScaleInner;

impl ToDisplayNE<'_> for TemporalScaleInner {
    type NE = &'static NEStr;
    fn to_ne(&self) -> Self::NE {
        ne_str!("0,0")
    }
}

#[derive(Default, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum TemporalScaleFix {
    #[default]
    None,
    Forced(String),
    Trimmed(String),
}

#[derive(From, Clone, PartialEq)]
#[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyMeasScaleFix {
    Optical(OpticalScaleFix),
    Temporal(TemporalScaleFix),
}

impl FromStrDelim for TemporalScaleInner {
    type Err = TemporalScaleError;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        let x0 = iter.next();
        let x1 = iter.next();
        let x2 = iter.next();
        if let (Some(y0), Some(y1), None) = (x0, x1, x2)
            && (y0.parse::<f32>(), y1.parse::<f32>()) == (Ok(0.0), Ok(0.0))
        {
            return Ok(Self);
        }
        Err(TemporalScaleError)
    }
}

impl_from_str_with_delim!(TemporalScaleInner, TemporalScaleError);

/// The value of the $PnE key for temporal measurements (3.0+)
#[derive(Clone, PartialEq, Debug, Default, Delegate)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct TemporalScale3_0(pub TemporalScaleInner);

impl FromStrWith for TemporalScale3_0 {
    type Err = TemporalScaleError;
    type Payload<'a> = ();
    type Diagnostic = TemporalScaleFix;
    type Config = ReadStdKeywordsConfig;

    fn from_str_with(s: &str, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
        if conf.force_linear_scale.time_selected() {
            let d = TemporalScaleFix::Forced(s.to_owned());
            Ok(DiagnosedKeyword::new(Self(TemporalScaleInner), d))
        } else {
            let flag = conf.trim_intra_value_whitespace;
            TemporalScaleInner::from_str_delim(s, flag).map(|x| {
                let d = x.trimmed.map(TemporalScaleFix::Trimmed).unwrap_or_default();
                DiagnosedKeyword::new(Self(x.native), d)
            })
        }
    }
}

// impl TemporalScale3_0 {
//     pub(crate) fn lookup(
//         kws: &mut StdKeywords,
//         i: MeasIndex,
//         nonstd: &mut NonStdKeywords,
//         conf: &ReadStdKeywordsConfig,
//     ) -> Result<(), ReqIndexedStKeyError<Self>> {
//         if conf.force_linear_scale.time_selected() {
//             nonstd.transfer_demoted(kws, TemporalScale2_0::std(i));
//             Ok(())
//         } else {
//             Self::remove_meas_req_with(kws, i, (), conf).map(|_| ())
//         }
//     }
// }

// impl KeywordPairMaybe for TemporalScale3_0 {
//     type Inner = Self;
// }

/// Error when parsing [`TemporalScaleInner`] from string
#[derive(Debug, Error)]
#[error("time measurement must have linear scaling")]
pub struct TemporalScaleError;

/// The value for the $PnCALIBRATION key (3.1 only)
///
/// This should be formatted like "`<value>,<unit>`"
#[derive(Clone, PartialEq, Debug, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Calibration3_1 {
    pub slope: PositiveFloat,
    pub unit: NEString,
}

impl<'a> ToDisplayNE<'a> for Calibration3_1 {
    type NE = NEConcat3<ToNE<PositiveFloat>, char, &'a NEString>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat::new(ToNE(self.slope), ',').append(&self.unit)
    }
}

impl HasDelim for Calibration3_1 {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        self.unit.has_delim(d)
    }
}

impl FromStrDelim for Calibration3_1 {
    type Err = CalibrationError<CalibrationFormat3_1>;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        let x0 = iter.next();
        let x1 = iter.next();
        let x2 = iter.next();
        match (x0, x1, x2) {
            (Some(value), Some(unit), None) => {
                let slope = value.parse().map_err(CalibrationError::Range)?;
                if let Ok(u) = unit.parse() {
                    Ok(Self::new(slope, u))
                } else {
                    Err(CalibrationError::EmptyUnit(EmptyCalibrationUnitError))
                }
            }
            _ => Err(CalibrationError::Format(CalibrationFormat3_1)),
        }
    }
}

impl_from_str_with_delim!(Calibration3_1, CalibrationError<CalibrationFormat3_1>);

/// Error when parsing [`Calibration3_1`] from string
#[derive(Debug, Error)]
#[error("must be like 'slope,unit'")]
pub struct CalibrationFormat3_1;

/// Error when calibration type has an empty unit string.
#[derive(Debug, Error)]
#[error("unit cannot be an empty string")]
pub struct EmptyCalibrationUnitError;

#[derive(Debug, Display, Error)]
pub enum CalibrationError<C> {
    Float(ParseFloatError),
    Range(RangedFloatError),
    EmptyUnit(EmptyCalibrationUnitError),
    Format(C),
}

impl From<Calibration3_1> for Calibration3_2 {
    fn from(value: Calibration3_1) -> Self {
        Self::new(value.slope, 0.0, value.unit)
    }
}

/// The value for the $PnCALIBRATION key (3.2+)
///
/// This should be formatted like `"<value>,[<offset>,]<unit>"` and differs from
/// 3.1 with the optional inclusion of `offset` (assumed 0 if not included).
#[derive(Clone, PartialEq, Debug, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Calibration3_2 {
    pub slope: PositiveFloat,
    pub offset: f32,
    pub unit: NEString,
}

impl<'a> ToDisplayNE<'a> for Calibration3_2 {
    type NE = NEConcat5<ToNE<PositiveFloat>, char, f32, char, &'a NEString>;
    fn to_ne(&'a self) -> Self::NE {
        // NOTE offset will always be written even if it is zero
        NEConcat::new(ToNE(self.slope), ',')
            .append(self.offset)
            .append(',')
            .append(&self.unit)
    }
}

impl HasDelim for Calibration3_2 {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        self.unit.has_delim(d)
    }
}

impl FromStrDelim for Calibration3_2 {
    type Err = CalibrationError<CalibrationFormat3_2>;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        let x0 = iter.next();
        let x1 = iter.next();
        let x2 = iter.next();
        let x3 = iter.next();
        let (slope_str, offset, unit_str) = match (x0, x1, x2, x3) {
            (Some(slope), Some(unit), None, None) => Ok((slope, 0.0, unit)),
            (Some(slope), Some(soffset), Some(unit), None) => {
                let f2 = soffset.parse().map_err(CalibrationError::Float)?;
                Ok((slope, f2, unit))
            }
            _ => Err(CalibrationError::Format(CalibrationFormat3_2)),
        }?;
        let slope = slope_str.parse().map_err(CalibrationError::Range)?;
        if let Ok(u) = unit_str.parse() {
            Ok(Self::new(slope, offset, u))
        } else {
            Err(CalibrationError::EmptyUnit(EmptyCalibrationUnitError))
        }
    }
}

impl_from_str_with_delim!(Calibration3_2, CalibrationError<CalibrationFormat3_2>);

/// Error when parsing [`Calibration3_2`] from string
#[derive(Debug, Error)]
#[error("must be like 'slope,[offset],unit'")]
pub struct CalibrationFormat3_2;

impl Calibration3_2 {
    pub(crate) fn into_3_1(
        self,
        i: MeasIndex,
    ) -> DeferredError<Calibration3_1, CalibrationLossError> {
        let ret = Calibration3_1::new(self.slope, self.unit);
        let e = (!self.offset.is_zero()).then_some(CalibrationLossError(i, self.offset));
        DeferredError::new_deferred_maybe(ret, e)
    }
}

/// Error when converting [`Calibration3_2`] to [`Calibration3_1`]
///
/// Loss will occur if the offset is specified, which is not applicable to FCS
/// 3.1
#[derive(Debug, Error)]
#[error(
    "{k} has offset {o} which will be lost upon conversion",
    k = Calibration3_2::std(self.0),
    o = self.1,
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct CalibrationLossError(MeasIndex, f32);

/// The value for the $PnL key (2.0/3.0).
#[derive(Clone, Copy, From, FromStr, Into, PartialEq, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[into(f32, PositiveFloat)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct Wavelength(pub PositiveFloat);

impl_newtype_try_from!(Wavelength, PositiveFloat, f32, RangedFloatError);

impl From<Wavelength> for Wavelengths {
    fn from(value: Wavelength) -> Self {
        Self(vec![value.0])
    }
}

/// The value for the $PnL key (3.1).
///
/// Starting in 3.1 this is a vector rather than a scaler.
#[derive(Clone, From, PartialEq, Debug, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct Wavelengths(pub Vec<PositiveFloat>);

#[derive(Clone)]
pub struct NEWavelengths<'a>(pub(crate) NESlice<'a, PositiveFloat>);

impl<'a> ToDisplayNE<'a> for NEWavelengths<'_> {
    type NE = NEDelim<NESlice<'a, ToNE<PositiveFloat>>>;
    fn to_ne(&'a self) -> Self::NE {
        let xs = ToNE::on_inner_slice(self.0.by_ref());
        NEDelim::new(',', xs)
    }
}

impl CheckMaybe for Wavelengths {
    type Inner = Self;
}

impl From<Wavelengths> for Vec<f32> {
    fn from(value: Wavelengths) -> Self {
        value.0.into_iter().map(Into::into).collect()
    }
}

impl FromStrDelim for Wavelengths {
    type Err = WavelengthsError;
    const DELIM: char = ',';

    fn from_iter<'a>(iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        let xs = iter
            .try_into_nonempty_iter()
            .ok_or(WavelengthsError::Empty)?;
        let ys = xs
            .into_iter()
            .map(|x| x.parse().map_err(WavelengthsError::Num))
            .collect::<Result<_, _>>()?;
        Ok(Self(ys))
    }
}

impl_from_str_with_delim!(Wavelengths, WavelengthsError);

impl Wavelengths {
    pub(crate) fn try_ne(&self) -> Option<NEWavelengths<'_>> {
        NESlice::try_from_slice(&self.0[..]).map(NEWavelengths)
    }

    pub(crate) fn into_wavelength(
        self,
        i: MeasIndex,
    ) -> DeferredError<Option<Wavelength>, WavelengthsLossError> {
        NEVec::try_from_vec(self.0).map_or(LogResult::new_ok(None), |ws| {
            let n = ws.len();
            let k = Key1::new_i1(i);
            let e = WavelengthsLossError(k, n);
            let wl = Some(Wavelength(ws.into_nonempty_iter().next().0));
            LogResult::new_deferred_if(usize::from(n) == 1, wl, e)
        })
    }
}

/// Error when converting [`Wavelengths`] (3.1/3.2) to [`Wavelength`] (2.0/3.0)
///
/// Loss may occur in this case because $PnL in later versions allows multiple
/// numbers and earlier versions only allow one.
#[derive(Debug, Error)]
#[error(
    "{0} is {1} elements long and will \
     be reduced to first upon conversion"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct WavelengthsLossError(Key1<Wavelengths>, NonZeroUsize);

/// Error when parsing [`Wavelengths`] from string
#[derive(Debug, Error)]
pub enum WavelengthsError {
    #[error("{0}")]
    Num(RangedFloatError),
    #[error("list must not be empty")]
    Empty,
}

/// A datetime as used in the $LAST_MODIFIED key (3.1+ only)
///
/// Inner value is private to ensure it always gets parsed/printed using the
/// correct format
#[derive(Clone, Copy, From, Into, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct LastModified(pub NaiveDateTime);

impl<'a> ToDisplayNE<'a> for LastModified {
    type NE = NEString;
    fn to_ne(&'a self) -> Self::NE {
        let mut s = NEString::try_from(self.0.format(DATETIME_FMT).to_string())
            .expect("format should be non-empty");
        let cc = format!("{:02}", self.0.nanosecond() / 10_000_000);
        s.push('.');
        s.push_str(cc.as_str());
        s
    }
}

impl FromStrWith for LastModified {
    type Err = LastModifiedError;
    type Payload<'a> = ();
    type Diagnostic = ();
    type Config = ReadStdKeywordsConfig;

    fn from_str_with(s: &str, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
        if let Some(pat) = conf.last_modified_pattern.as_ref() {
            return NaiveDateTime::parse_from_str(s, pat.as_str())
                .map(Self)
                .map(DiagnosedKeyword::new1)
                .map_err(|_| LastModifiedError::AltFormat(pat.to_owned()));
        }
        let mut it = s.split('.');
        let (t, cc) = match (it.by_ref().next(), it.by_ref().next(), it.next()) {
            (Some(t), None, None) => (t, ""),
            (Some(t), Some(cc), None) => (t, cc),
            _ => return Err(LastModifiedError::Format),
        };
        NaiveDateTime::parse_from_str(t, DATETIME_FMT)
            .or(Err(LastModifiedError::Format))
            .and_then(|dt| {
                if cc.is_empty() {
                    Ok(dt)
                } else {
                    let tt = cc.parse::<u32>().or(Err(LastModifiedError::Format))?;
                    if tt > 100 {
                        Err(LastModifiedError::Format)
                    } else {
                        dt.with_nanosecond(tt * 10_000_000)
                            .ok_or(LastModifiedError::Format)
                    }
                }
            })
            .map(Self)
            .map(DiagnosedKeyword::new1)
    }
}

/// Error when parsing [`LastModified`] from string
#[derive(Debug, Error)]
pub enum LastModifiedError {
    #[error("could not parse with format string '{0}'")]
    AltFormat(String),
    #[error("must be like 'dd-mmm-yyyy hh:mm:ss[.cc]'")]
    Format,
}

impl_str_enum_kw!(
    /// The value for the $ORIGINALITY key (3.1+)
    #[derive(PartialEq, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize))]
    #[cfg_attr(feature = "python", derive(FromPyString, IntoPyNEString))]
    pub Originality,
    /// Error when parsing [`Originality`] from string
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
    pub OriginalityError,
    Original        => ne_str!("Original"),
    NonDataModified => ne_str!("NonDataModified"),
    Appended        => ne_str!("Appended"),
    DataModified    => ne_str!("DataModified")
);

/// The value of the $COMP keyword (3.0 only)
#[derive(Clone, From, Into, AsRef, PartialEq, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[as_ref(DMatrix<f32>, Compensation)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct Compensation3_0(pub Compensation);

impl FromStrWith for Compensation3_0 {
    type Err = ParseCompError;
    type Payload<'a> = ();
    type Diagnostic = Trimmed;
    type Config = ReadStdKeywordsConfig;

    fn from_str_with(s: &str, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
        Self::from_str_delim(s, conf.trim_intra_value_whitespace).map(TrimmedKeyword::lift)
    }
}

impl FromStrDelim for Compensation3_0 {
    type Err = ParseCompError;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        if let Some(first) = iter.next().and_then(|x| x.parse::<usize>().ok()) {
            let n = first;
            let nn = n * n;
            let values = iter
                .by_ref()
                .take(nn)
                .map(str::parse::<f32>)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| ParseCompError::BadFloat)?;
            let remainder = iter.by_ref().count();
            let total = values.len() + remainder;
            if total == nn {
                let matrix = DMatrix::from_row_iterator(n, n, values);
                Ok(Compensation::try_from(matrix).map(Self)?)
            } else {
                Err(ParseCompError::WrongLength {
                    expected: nn,
                    total,
                })
            }
        } else {
            Err(ParseCompError::BadLength)
        }
    }
}

impl Compensation3_0 {
    pub(crate) fn invalid_link_errors(&self, par: Par) -> Option<KeyToIndexLinkError<Self>> {
        let m: &DMatrix<_> = self.as_ref();
        (par.0..m.nrows())
            .map(MeasIndex::from)
            .try_into_nonempty_iter()
            .map(|js| KeyToIndexLinkError::new_i0(js.collect()))
    }

    pub(crate) fn remove_invalid_link(
        src: &mut Option<Self>,
        par: Par,
    ) -> Option<RemovedIndexLink<Self>> {
        let go = |c: &Self| {
            let m: &DMatrix<_> = c.as_ref();
            (par.0..m.nrows()).map(MeasIndex::from)
        };
        RemovedIndexLink::remove_invalid_link(src, go)
    }
}

/// Error when parsing [`Compensation3_0`] from string
#[derive(Debug, Error)]
pub enum ParseCompError {
    #[error("Expected {expected} entries, found {total}")]
    WrongLength { total: usize, expected: usize },
    #[error("Could not determine length")]
    BadLength,
    #[error("Float could not be parsed")]
    BadFloat,
    #[error("{0}")]
    New(#[from] NewCompError),
}

/// The value of the $UNICODE key (3.0 only)
///
/// Formatted like `"codepage,[keys]"`. This key is not actually used for
/// anything in this library and is present to be complete. The original purpose
/// was to indicate keywords which supported UTF-8, but these days it is hard to
/// write a library that does NOT support UTF-8 ;)
#[derive(Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Unicode {
    pub page: u32,
    pub kws: Vec<NEString>,
}

impl<'a> ToDisplayNE<'a> for Unicode {
    type NE = NEAlt<u32, NEConcat3<u32, char, NEDelim<NESlice<'a, NEString>>>>;
    fn to_ne(&'a self) -> Self::NE {
        if let Some(kws) = NESlice::try_from_slice(&self.kws[..]) {
            NEAlt::Right(NEConcat::new(self.page, ',').append(NEDelim::new(',', kws)))
        } else {
            NEAlt::Left(self.page)
        }
    }
}

impl HasDelim for Unicode {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        self.kws.iter().find_map(|x| x.has_delim(d))
    }
}

impl FromStrDelim for Unicode {
    type Err = UnicodeError;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        if let Some(page) = iter.next().and_then(|x| x.parse().ok()) {
            let kws = iter
                .map(str::parse)
                .collect::<Result<Vec<NEString>, _>>()
                .map_err(|_| UnicodeError::EmptyKws)?;
            if kws.is_empty() {
                Err(UnicodeError::Empty)
            } else {
                Ok(Self { page, kws })
            }
        } else {
            Err(UnicodeError::BadFormat)
        }
    }
}

impl_from_str_with_delim!(Unicode, UnicodeError);

/// Error when parsing [`Unicode`] from string
#[derive(Debug, Error)]
pub enum UnicodeError {
    #[error("No keywords given")]
    Empty,
    #[error("Must be like 'n,string,[[string],...]'")]
    BadFormat,
    #[error("At least one keyword is an empty string")]
    EmptyKws,
}

/// The value of the $PnTYPE key in optical channels (3.2+)
#[derive(Clone, PartialEq, Debug, Default, AsRef)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyString))]
#[as_ref(str)]
pub struct OpticalType(String);

/// Error when parsing [`OpticalType`] from string
#[derive(Debug, Error)]
#[error("$PnTYPE for time measurement shall not be 'Time' if given")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
pub struct OpticalTypeError;

impl FromStr for OpticalType {
    type Err = OpticalTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == TIME.as_ref() {
            Err(OpticalTypeError)
        } else {
            Ok(Self(s.to_owned()))
        }
    }
}

/// The value of the $PnTYPE key in temporal channels (3.2+)
#[derive(Clone, Copy, PartialEq, Debug, Default)]
pub struct TemporalTypeInner;

// TODO combine with the other ZST in macro
impl ToDisplayNE<'_> for TemporalTypeInner {
    type NE = &'static NEStr;
    fn to_ne(&self) -> Self::NE {
        TIME
    }
}

impl FromStr for TemporalTypeInner {
    type Err = TemporalTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == TIME.as_ref() {
            Ok(Self)
        } else {
            Err(TemporalTypeError)
        }
    }
}

/// Error when parsing [`TemporalType`] from string
#[derive(Debug, Error)]
#[error("$PnTYPE for time measurement shall be 'Time' if given")]
pub struct TemporalTypeError;

/// The value of the $PnFEATURE key (3.2+)
#[derive(Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyNEString))]
pub enum Feature {
    Optical(OpticalFeature),
    Other(NEString),
}

impl<'a> ToDisplayNE<'a> for Feature {
    type NE = NEAlt<ToNE<OpticalFeature>, &'a NEString>;
    fn to_ne(&'a self) -> Self::NE {
        match self {
            Self::Optical(x) => NEAlt::Left(ToNE(*x)),
            Self::Other(x) => NEAlt::Right(x),
        }
    }
}

impl HasDelim for Feature {
    fn has_delim(&self, d: TEXTDelim) -> Option<DelimCollisionError> {
        if let Self::Other(x) = self {
            x.has_delim(d)
        } else {
            None
        }
    }
}

#[cfg(feature = "python")]
impl FromStr for Feature {
    type Err = FeatureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let conf = ReadStdKeywordsConfig {
            allow_other_feature: true.into(),
            ..ReadStdKeywordsConfig::default()
        };
        // throw away diagnostic flag here since this is only for python
        // conversion
        Self::from_str_with(s, (), &conf).map(|x| x.native)
    }
}

impl FromStrWith for Feature {
    type Err = FeatureError;
    type Payload<'a> = ();
    type Diagnostic = bool;
    type Config = ReadStdKeywordsConfig;

    fn from_str_with(s: &str, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
        match s.parse::<OpticalFeature>() {
            Ok(f) => Ok(DiagnosedKeyword::new(Self::Optical(f), false)),
            Err(e) => {
                if conf.allow_other_feature.is_set() {
                    let out = Self::Other(s.parse().map_err(|_| FeatureError::Other)?);
                    Ok(DiagnosedKeyword::new(out, true))
                } else {
                    Err(FeatureError::Optical(e))
                }
            }
        }
    }
}

// TODO this does too much, we only need this inside another enum, and the
// error struct is useless
impl_str_enum_kw!(
    /// The value of the $PnFEATURE key when restricted to area/width/height (3.2+)
    #[derive(PartialEq, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize))]
    #[cfg_attr(feature = "python", derive(FromPyString, IntoPyNEString))]
    pub OpticalFeature,
    /// Error when parsing [`Feature`] (optical only)
    #[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
    #[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
    pub OpticalFeatureError,
    Area   => ne_str!("Area"),
    Width  => ne_str!("Width"),
    Height => ne_str!("Height")
);

/// Error when parsing [`Feature`]
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
pub enum FeatureError {
    // TODO this is misleading
    #[error("{0}")]
    Optical(OpticalFeatureError),
    #[error("non-area/width/height feature must not be empty")]
    Other,
}

/// The value of the $RnI key (all versions)
#[derive(Clone, Copy, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum RegionGateIndex<I> {
    Univariate(I),
    Bivariate(IndexPair<I>),
}

impl<'a, I> ToDisplayNE<'a> for RegionGateIndex<I>
where
    for<'b> I: ToDisplayNE<'b> + Copy,
{
    type NE = NEAlt<ToNE<I>, ToNE<IndexPair<I>>>;
    fn to_ne(&'a self) -> Self::NE {
        match self {
            Self::Univariate(x) => NEAlt::Left(ToNE(*x)),
            Self::Bivariate(x) => NEAlt::Right(ToNE(*x)),
        }
    }
}

/// The two indices of a bivariate gate
#[derive(Clone, Copy, PartialEq, Debug, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct IndexPair<I> {
    pub x: I,
    pub y: I,
}

impl<'a, I> ToDisplayNE<'a> for IndexPair<I>
where
    for<'b> I: ToDisplayNE<'b> + Copy,
{
    type NE = NEDelim<[ToNE<I>; 2]>;
    fn to_ne(&'a self) -> Self::NE {
        NEDelim::new(',', [self.x, self.y].map(ToNE))
    }
}

impl_kind1!(pub IndexPairFamily, IndexPair);
impl_functor!(IndexPair, self, mut f, IndexPair::new(f(self.x), f(self.y)));

impl<I> IndexPair<I> {
    pub(crate) fn try_map<F, J, E>(self, mut f: F) -> Result<IndexPair<J>, E>
    where
        F: FnMut(I, I) -> Result<(J, J), E>,
    {
        let (x, y) = f(self.x, self.y)?;
        Ok(IndexPair { x, y })
    }
}

impl<I: FromStr> FromStrWith for RegionGateIndex<I> {
    type Err = RegionGateIndexError<<I as FromStr>::Err>;
    type Payload<'a> = ();
    type Diagnostic = Trimmed;
    type Config = ReadStdKeywordsConfig;

    fn from_str_with(s: &str, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
        Self::from_str_delim(s, conf.trim_intra_value_whitespace).map(TrimmedKeyword::lift)
    }
}

impl<I: FromStr> FromStrDelim for RegionGateIndex<I> {
    type Err = RegionGateIndexError<<I as FromStr>::Err>;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        let x0 = iter.next();
        let x1 = iter.next();
        let x2 = iter.next();
        match (x0, x1, x2) {
            (Some(x), None, None) => x
                .parse()
                .map(RegionGateIndex::Univariate)
                .map_err(RegionGateIndexError::Int),
            (Some(x), Some(y), None) => x
                .parse()
                .and_then(|a| y.parse().map(|b| Self::Bivariate(IndexPair { x: a, y: b })))
                .map_err(RegionGateIndexError::Int),
            _ => Err(RegionGateIndexError::Format),
        }
    }
}

/// Error when parsing [`RegionGateIndex<I>`] from string
#[derive(Debug, Error)]
pub enum RegionGateIndexError<E> {
    #[error("{0}")]
    Int(E),
    #[error("must be either a single value 'x' or a pair 'x,y'")]
    Format,
}

/// Index which can either refer to a gate ($Gn*) or a measurement ($Pn*)
#[derive(Clone, Copy, From, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyNEString))]
pub enum MeasOrGateIndex {
    Meas(MeasIndex),
    Gate(GateIndex),
}

impl<'a> ToDisplayNE<'a> for MeasOrGateIndex {
    type NE = NEConcat<char, ToNE<IndexFromOne>>;
    fn to_ne(&'a self) -> Self::NE {
        let (p, n) = match self {
            Self::Meas(x) => ('P', x.0),
            Self::Gate(x) => ('G', x.0),
        };
        NEConcat::new(p, ToNE(n))
    }
}

impl FromStr for MeasOrGateIndex {
    type Err = MeasOrGateIndexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((prefix, rest)) = s.split_at_checked(1) {
            match prefix {
                "P" => rest
                    .parse::<MeasIndex>()
                    .map(Into::into)
                    .map_err(MeasOrGateIndexError::Int),
                "G" => rest
                    .parse::<GateIndex>()
                    .map(Into::into)
                    .map_err(MeasOrGateIndexError::Int),
                _ => Err(MeasOrGateIndexError::Format),
            }
        } else {
            Err(MeasOrGateIndexError::Format)
        }
    }
}

/// Error when parsing [`RegionGateIndex<MeasOrGateIndex>`] from string (3.0/3.1)
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
pub enum MeasOrGateIndexError {
    #[error("{0}")]
    Int(ParseIntError),
    #[error("must be prefixed with either 'P' or 'G'")]
    Format,
}

/// Index for $RnI (3.2)
///
/// This is just a measurement index with 'P' in front of it
#[derive(Clone, Copy, From, PartialEq, Into, AsMut, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[from(MeasIndex, usize)]
#[into(MeasIndex, usize)]
pub struct PrefixedMeasIndex(pub MeasIndex);

impl<'a> ToDisplayNE<'a> for PrefixedMeasIndex {
    type NE = NEConcat<char, ToNE<MeasIndex>>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat::new('P', ToNE(self.0))
    }
}

impl FromStr for PrefixedMeasIndex {
    type Err = PrefixedMeasIndexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some((prefix, rest)) = s.split_at_checked(1) {
            match prefix {
                "P" => rest.parse().map_err(PrefixedMeasIndexError::Int).map(Self),
                _ => Err(PrefixedMeasIndexError::Format),
            }
        } else {
            Err(PrefixedMeasIndexError::Format)
        }
    }
}

/// Error when parsing [`RegionGateIndex<PrefixedMeasIndexError>`] from string (3.2)
#[derive(Debug, Error)]
pub enum PrefixedMeasIndexError {
    #[error("{0}")]
    Int(ParseIntError),
    #[error("must be prefixed with 'P'")]
    Format,
}

/// The value of the $RnW key (3.0-3.2)
///
/// This is meant to be used internally to construct a higher-level abstraction
/// over the gating keywords.
#[derive(Clone, Debug, PartialEq)]
pub enum RegionWindow {
    Univariate(UniGate),
    Bivariate(NEVec<Vertex>),
}

impl<'a> ToDisplayNE<'a> for RegionWindow {
    type NE = NEAlt<ToNE<&'a UniGate>, NEDelim<NESlice<'a, ToNE<Vertex>>>>;
    fn to_ne(&'a self) -> Self::NE {
        match self {
            Self::Univariate(x) => NEAlt::Left(ToNE(x)),
            Self::Bivariate(x) => {
                let xs = ToNE::on_inner_slice(x.as_nonempty_slice());
                NEAlt::Right(NEDelim::new(';', xs))
            }
        }
    }
}

/// A reference to the contents of [`RegionWindow`].
///
/// This is necessary since internally these values are separate and cannot
/// be borrowed using [`RegionWindow`].
#[derive(Clone)]
pub enum RegionWindowRef<'a> {
    Univariate(&'a UniGate),
    Bivariate(NESlice<'a, Vertex>),
}

impl<'a> ToDisplayNE<'a> for RegionWindowRef<'_> {
    type NE = NEAlt<ToNE<&'a UniGate>, NESlice<'a, ToNE<Vertex>>>;
    fn to_ne(&'a self) -> Self::NE {
        match self {
            Self::Univariate(x) => NEAlt::Left(ToNE(x)),
            Self::Bivariate(x) => {
                let xs = ToNE::on_inner_slice(x.by_ref());
                NEAlt::Right(xs)
            }
        }
    }
}

/// A vertex on a polygon gate
#[derive(Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Vertex {
    pub x: BigDecimal,
    pub y: BigDecimal,
}

impl<'a> ToDisplayNE<'a> for Vertex {
    type NE = NEConcat3<&'a BigDecimal, char, &'a BigDecimal>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat::new(&self.x, ',').append(&self.y)
    }
}

/// A gate on one dimension with lower and upper bound
#[derive(Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct UniGate {
    pub lower: BigDecimal,
    pub upper: BigDecimal,
}

impl<'a> ToDisplayNE<'a> for UniGate {
    type NE = NEConcat3<&'a BigDecimal, char, &'a BigDecimal>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat::new(&self.lower, ',').append(&self.upper)
    }
}

impl FromStrDelim for RegionWindow {
    type Err = RegionWindowError;
    const DELIM: char = ';';

    fn from_str_delim(
        s: &str,
        trim_whitespace: TrimIntraValueWhitespace,
    ) -> Result<TrimmedKeyword<Self>, Self::Err> {
        let it = s.split(Self::DELIM);
        if trim_whitespace.is_set() {
            let mut was_trimmed = false;
            Self::from_iter_inner(
                s,
                it.map(|x| {
                    let y = str::trim(x);
                    was_trimmed = was_trimmed || y.len() < x.len();
                    y
                }),
                trim_whitespace,
            )
            .map(|x| {
                let d = (x.trimmed.is_some() || was_trimmed).then(|| s.to_owned());
                TrimmedKeyword::new(x.native, d)
            })
        } else {
            Self::from_iter_inner(s, it, false.into())
        }
    }

    // TODO this function should never be used, it normally is supposed to be
    // called by Self::from_str_delim but it is overridden above to get the
    // nested behavior to work
    #[allow(clippy::unimplemented)]
    fn from_iter<'a>(_: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        unimplemented!()
    }
}

impl_from_str_with_delim!(RegionWindow, RegionWindowError);

impl RegionWindow {
    fn from_iter_inner<'a>(
        original: &str,
        ss: impl Iterator<Item = &'a str>,
        trim_whitespace: TrimIntraValueWhitespace,
    ) -> Result<TrimmedKeyword<Self>, RegionWindowError> {
        let mut it = ss.peekable();
        if let Some(head) = it.next() {
            if it.by_ref().peek().is_none() {
                UniGate::from_str_delim(head, trim_whitespace)
                    .map(|x| x.fmap_once(RegionWindow::Univariate))
            } else {
                let mut was_trimmed = false;
                let ys = once(head)
                    .chain(it)
                    .map(|x| {
                        let y = Vertex::from_str_delim(x, trim_whitespace)?;
                        was_trimmed = was_trimmed || y.trimmed.is_some();
                        Ok(y.native)
                    })
                    .collect::<Result<_, _>>()?;
                let d = was_trimmed.then(|| original.to_owned());
                Ok(TrimmedKeyword::new(Self::Bivariate(ys), d))
            }
        } else {
            // this will happen if the input string is empty
            Err(RegionWindowError::Format)
        }
    }
}

impl FromStrDelim for UniGate {
    type Err = RegionWindowError;
    const DELIM: char = ',';

    fn from_iter<'a>(iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        parse_pair(iter).map(|(lower, upper)| Self { lower, upper })
    }
}

impl FromStrDelim for Vertex {
    type Err = RegionWindowError;
    const DELIM: char = ',';

    fn from_iter<'a>(iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        parse_pair(iter).map(|(x, y)| Self { x, y })
    }
}

fn parse_pair<'a>(
    mut ss: impl Iterator<Item = &'a str>,
) -> Result<(BigDecimal, BigDecimal), RegionWindowError> {
    let x0 = ss.next();
    let x1 = ss.next();
    let x2 = ss.next();
    match (x0, x1, x2) {
        (Some(a), Some(b), None) => a
            .parse()
            .and_then(|x| b.parse().map(|y| (x, y)))
            .map_err(RegionWindowError::Num),
        _ => Err(RegionWindowError::Format),
    }
}

/// Error when parsing [`RegionWindow`] from string
#[derive(Debug, Error)]
pub enum RegionWindowError {
    #[error("{0}")]
    Num(ParseBigDecimalError),
    #[error("must be a string like 'f1,f2;[f3,f4;...]'")]
    Format,
}

/// The value of the $GATING key (3.0-3.2)
#[derive(Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyNEString))]
pub enum Gating {
    Region(RegionIndex),
    Not(Box<Self>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl<'a> ToDisplayNE<'a> for Gating {
    type NE = NEAlt<
        NEAlt<NEConcat<char, ToNE<RegionIndex>>, NEConcat3<&'static NEStr, &'a Box<Self>, char>>,
        NEConcat5<char, &'a Box<Self>, &'static NEStr, &'a Box<Self>, char>,
    >;
    fn to_ne(&'a self) -> Self::NE {
        let conj = |x, middle, y| {
            let ret = NEConcat::new('(', x).append(middle).append(y).append(')');
            NEAlt::Right(ret)
        };
        match self {
            Self::Region(x) => {
                let ret = NEConcat::new('R', ToNE(*x));
                NEAlt::Left(NEAlt::Left(ret))
            }
            Self::Not(x) => {
                let ret = NEConcat::new(ne_str!("(NOT "), x).append(')');
                NEAlt::Left(NEAlt::Right(ret))
            }
            Self::And(x, y) => conj(x, ne_str!(" AND "), y),
            Self::Or(x, y) => conj(x, ne_str!(" OR "), y),
        }
    }
}

impl Gating {
    pub(crate) fn region_indices(&self) -> NEVec<RegionIndex> {
        let mut xs = match self {
            Self::Region(x) => NEVec::new(*x),
            Self::Not(x) => Self::region_indices(x),
            Self::And(x, y) | Self::Or(x, y) => {
                let mut acc = Self::region_indices(x);
                acc.extend(Self::region_indices(y));
                acc
            }
        };
        xs.dedup();
        xs
    }
}

impl FromStr for Gating {
    type Err = GatingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_ascii() {
            let mut it = tokenize_gating(s);
            match_tokens(&mut it, 0)
        } else {
            Err(GatingError::NonAscii)
        }
    }
}

fn match_tokens(
    rest: &mut impl Iterator<Item = GatingToken>,
    depth: u32,
) -> Result<Gating, GatingError> {
    if let Some(this) = rest.next() {
        match this {
            GatingToken::LParen => match_tokens_new_expr(rest, depth + 1),
            GatingToken::Not => {
                let inner = match_tokens_new_expr(rest, depth)?;
                let new = Gating::Not(Box::new(inner));
                match_tokens_extend_expr(new, rest, depth)
            }
            GatingToken::Region(r) => {
                let new = Gating::Region(r);
                match_tokens_extend_expr(new, rest, depth)
            }
            _ => Err(GatingError::InvalidExprToken),
        }
    } else {
        Err(GatingError::Empty)
    }
}

/// Start a new expression if next token is valid.
///
/// This inclues:
/// - (blabla...
/// - NOT blabla...
/// - RX blabla...
fn match_tokens_new_expr(
    rest: &mut impl Iterator<Item = GatingToken>,
    depth: u32,
) -> Result<Gating, GatingError> {
    if let Some(this) = rest.next() {
        match this {
            GatingToken::LParen => {
                let inner = match_tokens_new_expr(rest, depth + 1)?;
                match_tokens_extend_expr(inner, rest, depth + 1)
            }
            GatingToken::Not => {
                let inner = match_tokens_new_expr(rest, depth)?;
                Ok(Gating::Not(Box::new(inner)))
            }
            GatingToken::Region(r) => Ok(Gating::Region(r)),
            _ => Err(GatingError::InvalidExprToken),
        }
    } else {
        Err(GatingError::ExpectedExpr)
    }
}

/// Extend current expression
fn match_tokens_extend_expr(
    acc: Gating,
    rest: &mut impl Iterator<Item = GatingToken>,
    depth: u32,
) -> Result<Gating, GatingError> {
    if let Some(this) = rest.next() {
        match this {
            GatingToken::And => {
                let right = match_tokens_new_expr(rest, depth)?;
                let new = Gating::And(Box::new(acc), Box::new(right));
                match_tokens_extend_expr(new, rest, depth)
            }
            GatingToken::Or => {
                let right = match_tokens_new_expr(rest, depth)?;
                let new = Gating::Or(Box::new(acc), Box::new(right));
                match_tokens_extend_expr(new, rest, depth)
            }
            GatingToken::RParen => {
                if depth > 0 {
                    match_tokens_extend_expr(acc, rest, depth - 1)
                } else {
                    Err(GatingError::ExtraParen)
                }
            }
            _ => Err(GatingError::InvalidOpToken),
        }
    } else if depth == 0 {
        Ok(acc)
    } else {
        Err(GatingError::MissingParen)
    }
}

fn tokenize_gating(s: &str) -> impl Iterator<Item = GatingToken> {
    s.split(['.', ' ']).filter(|x| !x.is_empty()).flat_map(|x| {
        x.split('(').flat_map(|y| {
            if y.is_empty() {
                vec![GatingToken::LParen]
            } else {
                y.split(')')
                    .map(|z| {
                        if z.is_empty() {
                            GatingToken::RParen
                        } else {
                            match z {
                                "NOT" => GatingToken::Not,
                                "AND" => GatingToken::And,
                                "OR" => GatingToken::Or,
                                _ => match z.split_at(1) {
                                    ("R", rest) => {
                                        rest.parse().map_or(GatingToken::Other, GatingToken::Region)
                                    }
                                    _ => GatingToken::Other,
                                },
                            }
                        }
                    })
                    .collect()
            }
        })
    })
}

#[derive(Debug)]
enum GatingToken {
    RParen,
    LParen,
    Region(RegionIndex),
    And,
    Or,
    Not,
    Other,
}

/// Error when parsing [`Gating`] from string
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
pub enum GatingError {
    #[error("gating string is empty")]
    Empty,
    #[error("expected expression which evaluates to a region")]
    ExpectedExpr,
    #[error("must be like 'f,string'")]
    InvalidOpToken,
    #[error("expected 'AND', 'OR', or ')'")]
    InvalidExprToken,
    #[error("extra ')' encountered")]
    ExtraParen,
    #[error("must be like 'f,string'")]
    MissingParen,
    #[error("gating contains invalid bytes")]
    NonAscii,
}

/// The value for the $PnB key (all versions)
///
/// The $PnB key actually stores bits. However, this library only supports
/// widths that are multiples of 8 (ie bytes). Therefore, this key actually
/// stores the number of bytes indicated by $PnB.
///
/// This may also be '*' which means "delimited ASCII" which is only valid when
/// $DATATYPE=A.
#[derive(Clone, Copy, PartialEq, Eq, Hash, From, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[from(Chars)]
pub enum Width {
    Fixed(BitsOrChars),
    Variable,
}

impl ToDisplayNE<'_> for Width {
    type NE = NEAlt<ToNE<BitsOrChars>, &'static NEStr>;
    fn to_ne(&self) -> Self::NE {
        match self {
            Self::Fixed(x) => NEAlt::Left(ToNE(*x)),
            Self::Variable => NEAlt::Right(ne_str!("*")),
        }
    }
}

/// The value of the $PnR key.
#[derive(Clone, From, FromStr, Add, Sub, PartialEq, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[from(u8, u16, u32, u64, BigDecimal)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct Range(pub BigDecimal);

impl Range {
    pub(crate) fn into_uint<T>(self) -> DeferredError<BitmaskValue<T>, RangeToIntError<()>>
    where
        T: TryFrom<Self, Error = RangeToIntError<T>> + PrimInt,
    {
        (self - Self::from(1_u8))
            .into_uint_inner()
            .map_deferred_value(BitmaskValue)
    }

    pub(crate) fn into_ascii_uint(self) -> DeferredError<AsciiRangeValue, RangeToIntError<()>> {
        self.into_uint_inner::<u64>()
            .map_deferred_value(AsciiRangeValue)
    }

    fn into_uint_inner<T>(self) -> DeferredError<T, RangeToIntError<()>>
    where
        T: TryFrom<Self, Error = RangeToIntError<T>> + PrimInt,
    {
        let (b, err) = self.try_into().map_or_else(
            |e: RangeToIntError<T>| match e.error_kind {
                RangeToIntErrorKind::Overrange => (T::max_value(), Some(e.void())),
                RangeToIntErrorKind::Underrange => (T::zero(), Some(e.void())),
                RangeToIntErrorKind::PrecisionLoss(y) => (y, Some(e.void())),
            },
            |x| (x, None),
        );
        LogResult::new_deferred_maybe(b, err)
    }

    pub(crate) fn into_float<T>(self) -> DeferredError<FloatDecimal<T>, DecimalToFloatError>
    where
        FloatDecimal<T>: TryFrom<BigDecimal, Error = DecimalToFloatError>,
        T: HasFloatBounds,
    {
        let (x, err) = FloatDecimal::try_from(self.0).map_or_else(
            |e| {
                let m = if e.over {
                    T::max_decimal()
                } else {
                    T::min_decimal()
                };
                (m, Some(e))
            },
            |x| (x, None),
        );
        LogResult::new_deferred_maybe(x, err)
    }
}

macro_rules! try_from_range_int {
    ($inttype:ident, $to:ident, $ut:ident) => {
        impl TryFrom<Range> for $inttype {
            type Error = RangeToIntError<$inttype>;

            fn try_from(value: Range) -> Result<Self, Self::Error> {
                let x = &value.0;
                let err = |error_kind| RangeToIntError {
                    dest_type: UintType::$ut,
                    src_value: x.clone(),
                    error_kind,
                };
                if let Some(y) = x.$to() {
                    if x.fractional_digit_count() <= 0 {
                        Ok(y)
                    } else {
                        Err(err(RangeToIntErrorKind::PrecisionLoss(y)))
                    }
                } else {
                    if BigDecimal::from($inttype::MAX) < *x {
                        Err(err(RangeToIntErrorKind::Overrange))
                    } else {
                        Err(err(RangeToIntErrorKind::Underrange))
                    }
                }
            }
        }
    };
}

try_from_range_int!(u8, to_u8, U8);
try_from_range_int!(u16, to_u16, U16);
try_from_range_int!(u32, to_u32, U32);
try_from_range_int!(u64, to_u64, U64);

/// Error when converting [`Range`] to integer.
///
/// This is a helper type to make more specific errors and not meant for
/// external use.
#[derive(Debug)]
pub struct RangeToIntError<T> {
    pub(crate) dest_type: UintType,
    pub(crate) src_value: BigDecimal,
    pub(crate) error_kind: RangeToIntErrorKind<T>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum UintType {
    U8,
    U16,
    U32,
    U64,
}

impl From<UintType> for PrivBytes {
    fn from(value: UintType) -> Self {
        match value {
            UintType::U8 => Self::B1,
            UintType::U16 => Self::B2,
            UintType::U32 => Self::B4,
            UintType::U64 => Self::B8,
        }
    }
}

#[derive(Debug)]
pub(crate) enum RangeToIntErrorKind<T> {
    Overrange,
    Underrange,
    PrecisionLoss(T),
}

impl<T> RangeToIntError<T> {
    pub(crate) fn void(self) -> RangeToIntError<()> {
        RangeToIntError {
            dest_type: self.dest_type,
            src_value: self.src_value,
            error_kind: match self.error_kind {
                RangeToIntErrorKind::Overrange => RangeToIntErrorKind::Overrange,
                RangeToIntErrorKind::Underrange => RangeToIntErrorKind::Underrange,
                RangeToIntErrorKind::PrecisionLoss(_) => RangeToIntErrorKind::PrecisionLoss(()),
            },
        }
    }
}

impl TryFrom<f32> for Range {
    type Error = ParseBigDecimalError;
    fn try_from(value: f32) -> Result<Self, Self::Error> {
        value.try_into().map(Self)
    }
}

impl TryFrom<f64> for Range {
    type Error = ParseBigDecimalError;
    fn try_from(value: f64) -> Result<Self, Self::Error> {
        value.try_into().map(Self)
    }
}

/// The value of the $GmN key
#[derive(Clone, From, FromStr, PartialEq, Debug, AsRef, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[as_ref(str)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct GateShortname(pub Shortname);

/// The value of the $GmR key
#[derive(Clone, From, FromStr, PartialEq, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[from(u64)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct GateRange(pub Range);

macro_rules! impl_non_neg_float {
    ($(#[$meta:meta])* $t:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, From, FromStr, Into, PartialEq, Debug, Delegate)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        #[into(NonNegFloat, f32)]
        #[delegate(ToDisplayNE<'a>, generics = "'a")]
        pub struct $t(pub NonNegFloat);

        impl_newtype_try_from!($t, NonNegFloat, f32, RangedFloatError);
    };
}

impl_non_neg_float! {
    /// The value of the $VOL key.
    Vol
}

impl_non_neg_float! {
    /// The value of the $PnO key.
    Power
}

impl_non_neg_float! {
    /// The value of the $PnP key.
    PercentEmitted
}

impl_non_neg_float! {
    /// The value of the $PnV key.
    DetectorVoltage
}

impl_non_neg_float! {
    /// The value of the $GmV key.
    GateDetectorVoltage
}

impl_non_neg_float! {
    /// The value of the $GmP key.
    GatePercentEmitted
}

/// The value of the $GmE key
#[derive(Clone, Copy, PartialEq, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct GateScale(pub Scale);

impl FromStrWith for GateScale {
    type Err = ScaleError;
    type Payload<'a> = ();
    type Diagnostic = ScaleFix;
    type Config = ReadStdKeywordsConfig;

    fn from_str_with(s: &str, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
        // use the same fix we use for PnE here
        Scale::parse_fix_maybe(s, conf).map(|x| x.first_once(Self))
    }
}

/// The value of the $CYT key (3.2).
///
/// This is not a normal string because it is required in 3.2 and thus cannot
/// be empty.
#[derive(Clone, FromStr, PartialEq, Into, Debug, AsRef, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[as_ref(str)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct Cyt3_2(pub NEString);

impl From<Cyt3_2> for Cyt {
    fn from(value: Cyt3_2) -> Self {
        Self(value.0.into())
    }
}

impl TryFrom<Cyt> for Cyt3_2 {
    type Error = NoCytError;

    fn try_from(value: Cyt) -> Result<Self, Self::Error> {
        (value.0).parse().map_err(|_| NoCytError)
    }
}

/// Error when parsing [`Cyt3_2`] from string
#[derive(Debug, Error)]
#[error("$CYT is missing")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct NoCytError;

/// The value for the $UNSTAINEDCENTERS key (3.2+)
#[derive(Clone, Into, PartialEq, Debug, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct UnstainedCenters(pub HashMap<Shortname, f32>);

#[derive(Clone)]
pub struct NEUnstainedCenters(pub(crate) NEMap<Shortname, f32>);

impl<'a> ToDisplayNE<'a> for NEUnstainedCenters {
    type NE = NEConcat5<
        NonZeroUsize,
        char,
        NEDelim<NEVec<ToNE<&'a Shortname>>>,
        char,
        NEDelim<NEVec<f32>>,
    >;
    fn to_ne(&'a self) -> Self::NE {
        let n = self.0.len();
        let ks = NEDelim::new(',', self.0.keys().map(ToNE).collect());
        let vs = NEDelim::new(',', self.0.values().copied().collect());
        NEConcat::new(n, ',').append(ks).append(',').append(vs)
    }
}

/// Error when parsing [`UnstainedCenters`] from string
#[derive(Debug, Error)]
pub enum ParseUnstainedCenterError {
    #[error("Names are not unique")]
    NonUnique,
    #[error("Expected {expected} values, found {total}")]
    BadLength { total: usize, expected: usize },
    #[error("Could not parse N")]
    BadN,
    #[error("Error parsing float value(s)")]
    BadFloat,
}

impl UnstainedCenters {
    pub(crate) fn try_ne(&self) -> Option<NEUnstainedCenters> {
        NEMap::try_from_map(self.0.clone()).map(NEUnstainedCenters)
    }

    pub(crate) fn reassign(&mut self, mapping: &NameMapping) {
        // keys can't be mutated in place so need to rebuild the hashmap with
        // new keys from the mapping
        let new: HashMap<_, _> = self
            .0
            .iter()
            .map(|(k, v)| {
                (
                    mapping.get(k).map(|x| (*x).clone()).unwrap_or(k.clone()),
                    *v,
                )
            })
            .collect();
        self.0 = new;
    }

    /// Return error if any about-to-removed names are in unstained center names
    pub(crate) fn existing_link_error(
        &self,
        names: &OpticalNamesToRemove<'_>,
    ) -> Option<ExistingNamedLinkError<Self, ()>> {
        self.0
            .keys()
            .filter(|n| names.as_ref().contains(n))
            .cloned()
            .try_into_nonempty_iter()
            .map(|js| ExistingNamedLinkError::new(Key0::default(), js.collect()))
    }

    /// Return error if any names in matrix are not in measurement vector
    pub(crate) fn invalid_link_error(
        &self,
        names: &NamedSet<'_>,
    ) -> impl Iterator<Item = KeyToNameLinkError<Self>> {
        names.invalid_link_errors(self.0.keys())
    }

    /// Remove $UNSTAINEDCENTERS if any names in array are not in measurement vector
    pub(crate) fn remove_invalid_links(
        &mut self,
        names: &NamedSet<'_>,
    ) -> Option<RemovedNamedLink<Self>> {
        let ln = names.error_link_name(self.0.keys());
        ln.map(|x| RemovedNamedLink::new(take(self), x))
    }
}

impl FromStrDelim for UnstainedCenters {
    type Err = ParseUnstainedCenterError;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        // NOTE the standard does not say if this is allowed to be empty or not
        // (ie the string "0") so do not enforce here. However, if empty we will
        // not save the keyword when writing the file.
        if let Some(n) = iter.next().and_then(|x| x.parse().ok()) {
            // This should be safe since we are splitting by commas
            let measurements: Vec<_> = iter
                .by_ref()
                .take(n)
                .map(Shortname::new_unchecked)
                .collect();
            if measurements.iter().unique().count() < measurements.len() {
                return Err(ParseUnstainedCenterError::NonUnique);
            }
            let values: Vec<_> = iter
                .by_ref()
                .take(n)
                .map(str::parse::<f32>)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| ParseUnstainedCenterError::BadFloat)?;
            let remainder = iter.by_ref().count();
            let total = values.len() + measurements.len() + remainder;
            let expected = 2 * n;
            if total == expected {
                let ys = measurements.into_iter().zip(values).collect();
                Ok(Self(ys))
            } else {
                Err(ParseUnstainedCenterError::BadLength { total, expected })
            }
        } else {
            Err(ParseUnstainedCenterError::BadN)
        }
    }
}

impl_from_str_with_delim!(UnstainedCenters, ParseUnstainedCenterError);

impl CheckMaybe for UnstainedCenters {
    type Inner = Self;
}

/// Leftover standard keyword after parsing
#[derive(Clone, new, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct ExtraStdKeywords {
    pub pseudostandard: StdKeywords,
    pub hyper_par: StdKeywords,
    pub hyper_gate: StdKeywords,
    pub other_version: StdKeywords,
    pub timestep: Option<String>,
}

pub(crate) enum ExtraKeywordClass {
    Version(NEVec<Version>),
    HyperPar,
    HyperGate,
    Pseudostandard,
    UnusedTimestep,
}

#[derive(new)]
pub(crate) struct ExtraKeywordOutput {
    pub(crate) pseudo: Vec<PseudostandardError>,
    pub(crate) hyper_par: Vec<HyperParError>,
    pub(crate) hyper_gate: Vec<HyperGateError>,
    pub(crate) other_version: Vec<KeywordOtherVersionError>,
}

impl ExtraStdKeywords {
    /// Classify unused keyword based on all known FCS versions
    ///
    /// Will not try to match $PAR since we can assume this function will never
    /// get called if $PAR is not parsed properly. Will also not match
    /// $NEXTDATA, $BEGINSTEXT, or $ENDSTEXT since these should have already
    /// been processed when parsing TEXT itself.
    fn classify_kws(
        key: &StdKey,
        current_version: Version,
        par: Par,
        gate: Gate,
    ) -> Option<ExtraKeywordClass> {
        let if_invalid_version = |vs: VersionMembership| {
            (!vs.contains_version(current_version))
                .then(|| ExtraKeywordClass::Version(vs.versions()))
        };
        let if_hyperpar = |i: usize, vs: VersionMembership| {
            if i >= par.0 {
                Some(ExtraKeywordClass::HyperPar)
            } else {
                if_invalid_version(vs)
            }
        };
        match AnyKeywordClass::classify_keyword(key) {
            AnyKeywordClass::Root(c) => {
                let m = c.membership();
                if m.contains_version(current_version) {
                    matches!(c, RootKeywordClass::Timestep)
                        .then_some(ExtraKeywordClass::UnusedTimestep)
                } else {
                    Some(ExtraKeywordClass::Version(m.versions()))
                }
            }
            AnyKeywordClass::Meas(i, c) => if_hyperpar(i.into(), c.membership()),
            AnyKeywordClass::Peak(i) => if_hyperpar(i.into(), PKN_VERS),
            AnyKeywordClass::CSVFlag(i) => if_hyperpar(i.into(), CSV_VERS),
            AnyKeywordClass::Dfc(x, y) => {
                if usize::from(x) >= par.0 || usize::from(y) >= par.0 {
                    Some(ExtraKeywordClass::HyperPar)
                } else {
                    if_invalid_version(Dfc::VERS)
                }
            }
            AnyKeywordClass::GateOptLE3_1(i) => {
                (usize::from(i) >= gate.0).then_some(ExtraKeywordClass::HyperGate)
            }
            AnyKeywordClass::RegionIndex | AnyKeywordClass::RegionWindow => None,
            AnyKeywordClass::NonStandard => Some(ExtraKeywordClass::Pseudostandard),
        }
    }

    pub(crate) fn split_keywords(
        kws: StdKeywords,
        current_version: Version,
        par: Par,
        gate: Gate,
    ) -> (Self, ExtraKeywordOutput) {
        let mut pseudo = HashMap::new();
        let mut hyper_par = HashMap::new();
        let mut hyper_gate = HashMap::new();
        let mut other_version = HashMap::new();
        let mut pseudo_es = vec![];
        let mut hyper_par_es = vec![];
        let mut hyper_gate_es = vec![];
        let mut other_version_es = vec![];
        let mut timestep = None;
        for (k, v) in kws {
            if let Some(m) = Self::classify_kws(&k, current_version, par, gate) {
                match m {
                    ExtraKeywordClass::HyperPar => {
                        hyper_par_es.push(HyperParError::new(par, k.clone()));
                        hyper_par.insert(k, v);
                    }
                    ExtraKeywordClass::HyperGate => {
                        hyper_gate_es.push(HyperGateError::new(gate, k.clone()));
                        hyper_gate.insert(k, v);
                    }
                    ExtraKeywordClass::Version(vs) => {
                        let e = KeywordOtherVersionError::new(k.clone(), current_version, vs);
                        other_version_es.push(e);
                        other_version.insert(k, v);
                    }
                    ExtraKeywordClass::Pseudostandard => {
                        pseudo_es.push(PseudostandardError(k.clone()));
                        pseudo.insert(k, v);
                    }
                    ExtraKeywordClass::UnusedTimestep => {
                        timestep = Some(v);
                    }
                }
            }
        }
        let ret = Self::new(pseudo, hyper_par, hyper_gate, other_version, timestep);
        let out = ExtraKeywordOutput::new(pseudo_es, hyper_par_es, hyper_gate_es, other_version_es);
        (ret, out)
    }
}

/// Error denoting that pseudostandard keyword was found.
#[derive(Debug, Error)]
#[error("pseudostandard keyword found: {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ExtraKeywordError))]
pub struct PseudostandardError(pub StdKey);

/// Error denoting that measurement keyword within standard but above $PAR was found
#[derive(Debug, Error, new)]
#[error("measurement keyword is part of standard but outside $PAR ({par}): {key}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ExtraKeywordError))]
pub struct HyperParError {
    pub par: Par,
    pub key: StdKey,
}

/// Error denoting that gating keyword within standard but above $GATE was found
#[derive(Debug, Error, new)]
#[error("gating keyword is part of standard but outside $GATE ({gate}): {key}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ExtraKeywordError))]
pub struct HyperGateError {
    pub gate: Gate,
    pub key: StdKey,
}

/// Error denoting that keyword from different version was found
#[derive(Debug, Error, new)]
#[error(
    "keyword is not compatible with {current} but is compatible with {os}: {key}",
    os = self.others.iter().join(", ")
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ExtraKeywordError))]
pub struct KeywordOtherVersionError {
    pub key: StdKey,
    pub current: Version,
    pub others: NEVec<Version>,
}

/// Error denoting that $TIMESTEP was unused and possibly should have been
#[derive(Debug, Error)]
#[error("$TIMESTEP found, this may indicate a time measurement exists but was not identified")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ExtraKeywordError))]
pub struct TimestepFoundError;

macro_rules! newtype_string {
    ($t:ident) => {
        #[derive(Clone, FromStr, From, Into, PartialEq, Debug, Default, AsRef)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
        #[as_ref(str)]
        pub struct $t(pub String);

        impl CheckMaybe for $t {
            type Inner = Self;
        }
    };
}

macro_rules! newtype_int {
    ($t:ident, $type:ty) => {
        #[derive(
            Clone,
            Copy,
            Display,
            FromStr,
            From,
            Into,
            PartialEq,
            PartialOrd,
            Eq,
            Ord,
            Debug,
            Delegate,
        )]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        #[delegate(ToDisplayNE<'a>, generics = "'a")]
        pub struct $t(pub $type);
    };
}

// TODO refactor
macro_rules! impl_display_maybe_self {
    ($t:ident) => {
        impl CheckMaybe for $t {
            type Inner = Self;
        }
    };
}

macro_rules! newtype_opt_u32 {
    ($t:ident) => {
        #[derive(Clone, Copy, Default, PartialEq, Eq, FromStr, Debug, AsRef)]
        #[as_ref(u32)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        pub struct $t(pub u32);

        impl_display_maybe_self!($t);
    };
}

macro_rules! newtype_opt_bool {
    ($t:ident, $inner:ident) => {
        #[derive(Clone, Copy, PartialEq, Debug, Default, From, Into, AsRef)]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[from(bool)]
        #[into(bool)]
        #[as_ref(Option<$inner>)]
        pub struct $t(pub OptionalZST<$inner>);

        impl_display_maybe_self!($t);
    };
}

macro_rules! impl_versioned_key {
    ($t:ident, $m:expr) => {
        impl crate::validated::keys::VersionedKey for $t {
            const VERS: fireflow_types::keywords::VersionMembership = $m;
        }
    };
}

macro_rules! kw_meta {
    ($t:ident, $k:expr, $m:expr) => {
        impl_versioned_key!($t, $m);
        impl crate::validated::keys::Key for $t {
            const C: &'static NEStr = ne_str!($k);
        }
    };
}

macro_rules! kw_meas {
    ($t:ident, $sfx:expr, $m:expr) => {
        impl_versioned_key!($t, $m);
        impl crate::validated::keys::IndexedKey for $t {
            const C: PrefixSuffix = PrefixSuffix::Both(MEAS_KW_PREFIX, ne_str!($sfx));
        }
    };
}

macro_rules! kw_meta_string {
    ($t:ident, $k:expr, $m:expr) => {
        kw_meta!($t, $k, $m);
        newtype_string!($t);
    };
}

macro_rules! kw_meta_int {
    ($t:ident, $type:ident, $k:expr, $m:expr) => {
        kw_meta!($t, $k, $m);
        newtype_int!($t, $type);
    };
}

macro_rules! kw_meas_string {
    ($t:ident, $sfx:expr, $m:expr) => {
        newtype_string!($t);
        kw_meas!($t, $sfx, $m);
    };
}

macro_rules! req_meta {
    ($t:ident) => {
        impl Required for $t {}
        impl ReqMetarootKey for $t {}
    };
}

macro_rules! opt_meta {
    ($t:ident, $outer:path) => {
        impl Optional for $t {
            type Outer = $outer;
        }
        impl OptMetarootKey for $t {}
    };
}

macro_rules! req_meas {
    ($t:ident) => {
        impl Required for $t {}
        impl ReqIndexedKey for $t {}
    };
}

macro_rules! opt_meas {
    ($t:ident, $outer:path) => {
        impl Optional for $t {
            type Outer = $outer;
        }
        impl OptIndexedKey for $t {}
    };
}

macro_rules! kw_req_meta {
    ($t:ident, $k:expr, $m:expr) => {
        kw_meta!($t, $k, $m);
        req_meta!($t);
    };
}

macro_rules! kw_opt_meta {
    ($t:ident, $k:expr, $m:expr, $outer:path) => {
        kw_meta!($t, $k, $m);
        opt_meta!($t, $outer);
    };
}

macro_rules! kw_req_meas {
    ($t:ident, $sfx:expr, $m:expr) => {
        kw_meas!($t, $sfx, $m);
        req_meas!($t);
    };
}

macro_rules! kw_opt_meas {
    ($t:ident, $sfx:expr, $m:expr, $outer:path) => {
        kw_meas!($t, $sfx, $m);
        opt_meas!($t, $outer);
    };
}

macro_rules! kw_opt_root_string {
    ($t:ident, $k:expr, $m:expr) => {
        kw_meta_string!($t, $k, $m);
        opt_meta!($t, Self);
    };
}

macro_rules! kw_opt_meas_string {
    ($t:ident, $sfx:expr, $m:expr) => {
        kw_meas_string!($t, $sfx, $m);
        opt_meas!($t, Self);
    };
}

macro_rules! kw_req_root_int {
    ($t:ident, $type:ident, $k:expr, $m:expr) => {
        kw_meta_int!($t, $type, $k, $m);
        req_meta!($t);
    };
}

macro_rules! kw_opt_root_int {
    ($t:ident, $type:ident, $k:expr, $m:expr) => {
        kw_meta_int!($t, $type, $k, $m);
        opt_meta!($t, Option<Self>);
    };
}

macro_rules! kw_time {
    ($outer:ident, $wrap:ident, $inner:ident, $err:ident, $key:expr, $ver:expr) => {
        type $outer = $wrap<$inner>;

        kw_opt_meta!($outer, $key, $ver, Option<Self>);

        impl From<NaiveTime> for $outer {
            fn from(value: NaiveTime) -> Self {
                Xtim($inner(value))
            }
        }
    };
}

macro_rules! kw_opt_gate {
    ($t:ident, $sfx:expr, $outer:path) => {
        impl_versioned_key!($t, fireflow_types::keywords::VersionMembership::All);
        impl IndexedKey for $t {
            const C: PrefixSuffix = PrefixSuffix::Both(GATE_KW_PREFIX, ne_str!($sfx));
        }
        opt_meas!($t, $outer);
    };
}

macro_rules! kw_opt_gate_other {
    ($t:ident, $sfx:expr) => {
        kw_opt_gate!($t, $sfx, Option<Self>);
    };
}

macro_rules! kw_opt_gate_string {
    ($t:ident, $sfx:expr) => {
        newtype_string!($t);
        kw_opt_gate!($t, $sfx, Self);
    };
}

macro_rules! meas_opt_zst {
    ($t:ident, $sym:expr, $m:expr, $inner:ident) => {
        newtype_opt_bool!($t, $inner);
        kw_opt_meas!($t, $sym, $m, Self);
    };
}

macro_rules! kw_opt_meta_opt_u32 {
    ($t:ident, $k:expr, $m:expr) => {
        newtype_opt_u32!($t);
        kw_opt_meta!($t, $k, $m, Self);
    };
}

// all versions
kw_req_meta!(AlphaNumType, tk::DATATYPE_KW, tk::DATATYPE_VERS);
kw_opt_root_int!(Abrt, u32, tk::ABRT_KW, tk::ABRT_VERS);
kw_opt_root_string!(Cytsn, tk::CYTSN_KW, tk::CYTSN_VERS);
kw_opt_root_string!(Com, tk::COM_KW, tk::COM_VERS);
kw_opt_root_string!(Cells, tk::CELLS_KW, tk::CELLS_VERS);
kw_opt_meta!(FCSDate, tk::DATE_KW, tk::DATE_VERS, Option<Self>);
kw_opt_root_string!(Exp, tk::EXP_KW, tk::EXP_VERS);
kw_opt_root_string!(Fil, tk::FIL_KW, tk::FIL_VERS);
kw_opt_root_string!(Inst, tk::INST_KW, tk::INST_VERS);
kw_opt_root_int!(Lost, u32, tk::LOST_KW, tk::LOST_VERS);
kw_opt_root_string!(Op, tk::OP_KW, tk::OP_VERS);
kw_req_root_int!(Par, usize, tk::PAR_KW, tk::PAR_VERS);
kw_opt_root_string!(Proj, tk::PROJ_KW, tk::PROJ_VERS);
kw_opt_root_string!(Smno, tk::SMNO_KW, tk::SMNO_VERS);
kw_opt_root_string!(Src, tk::SRC_KW, tk::SRC_VERS);
kw_opt_root_string!(Sys, tk::SYS_KW, tk::SYS_VERS);
kw_opt_meta!(Trigger, tk::TR_KW, tk::TR_VERS, Option<Self>);

// time for 2.0
kw_time!(
    Btim2_0,
    Btim,
    FCSTime,
    FCSTimeError,
    tk::BTIM_KW,
    tk::BTIM_VERS
);
kw_time!(
    Etim2_0,
    Etim,
    FCSTime,
    FCSTimeError,
    tk::ETIM_KW,
    tk::ETIM_VERS
);

// time for 3.0
kw_time!(
    Btim3_0,
    Btim,
    FCSTime60,
    FCSTime60Error,
    tk::BTIM_KW,
    tk::BTIM_VERS
);
kw_time!(
    Etim3_0,
    Etim,
    FCSTime60,
    FCSTime60Error,
    tk::ETIM_KW,
    tk::ETIM_VERS
);

// time for 3.1-3.2
kw_time!(
    Btim3_1,
    Btim,
    FCSTime100,
    FCSTime100Error,
    tk::BTIM_KW,
    tk::BTIM_VERS
);
kw_time!(
    Etim3_1,
    Etim,
    FCSTime100,
    FCSTime100Error,
    tk::ETIM_KW,
    tk::ETIM_VERS
);

// 3.0 only
kw_opt_meta!(Compensation3_0, tk::COMP_KW, tk::COMP_VERS, Option<Self>);
kw_opt_meta!(Unicode, tk::UNICODE_KW, tk::UNICODE_VERS, Option<Self>);

// for 3.0+
kw_req_meta!(Timestep, tk::TIMESTEP_KW, tk::TIMESTEP_VERS);

// for 3.1+
kw_opt_root_string!(LastModifier, tk::LAST_MODIFIER_KW, tk::LAST_MODIFIER_VERS);
kw_opt_meta!(
    Originality,
    tk::ORIGINALITY_KW,
    tk::ORIGINALITY_VERS,
    Option<Self>
);
kw_opt_meta!(
    LastModified,
    tk::LAST_MODIFIED_KW,
    tk::LAST_MODIFIED_VERS,
    Option<Self>
);

kw_opt_root_string!(Plateid, tk::PLATEID_KW, tk::PLATEID_VERS);
kw_opt_root_string!(Platename, tk::PLATENAME_KW, tk::PLATENAME_VERS);
kw_opt_root_string!(Wellid, tk::WELLID_KW, tk::WELLID_VERS);

kw_opt_meta!(
    Spillover,
    tk::SPILLOVER_KW,
    tk::SPILLOVER_VERS,
    Option<Self>
);

kw_opt_meta!(Vol, tk::VOL_KW, tk::VOL_VERS, Option<Self>);

// for 3.2+
kw_opt_root_string!(Carrierid, tk::CARRIERID_KW, tk::CARRIERID_VERS);
kw_opt_root_string!(Carriertype, tk::CARRIERTYPE_KW, tk::CARRIERTYPE_VERS);
kw_opt_root_string!(Locationid, tk::LOCATIONID_KW, tk::LOCATIONID_VERS);

kw_opt_meta!(
    BeginDateTime,
    tk::BEGINDATETIME_KW,
    tk::BEGINDATETIME_VERS,
    Option<Self>
);
kw_opt_meta!(
    EndDateTime,
    tk::ENDDATETIME_KW,
    tk::ENDDATETIME_VERS,
    Option<Self>
);
kw_opt_meta!(
    UnstainedCenters,
    tk::UNSTAINEDCENTERS_KW,
    tk::UNSTAINEDCENTERS_VERS,
    Self
);

kw_opt_root_string!(UnstainedInfo, tk::UNSTAINEDINFO_KW, tk::UNSTAINEDINFO_VERS);

kw_opt_root_string!(Flowrate, tk::FLOWRATE_KW, tk::FLOWRATE_VERS);

// version-specific
kw_opt_root_int!(Tot, usize, tk::TOT_KW, tk::TOT_VERS); // optional in 2.0
req_meta!(Tot); // required in 3.0+

kw_req_meta!(Mode, tk::MODE_KW, tk::MODE_VERS); // for 2.0-3.1
kw_opt_meta!(Mode3_2, tk::MODE_KW, tk::MODE_VERS, Option<Self>); // for 3.2+

kw_opt_root_string!(Cyt, tk::CYT_KW, tk::CYT_VERS); // optional for 2.0-3.1
kw_req_meta!(Cyt3_2, tk::CYT_KW, tk::CYT_VERS); // required for 3.2+

kw_req_meta!(ByteOrd2_0, tk::BYTEORD_KW, tk::BYTEORD_VERS); // 2.0/3.0
kw_req_meta!(ByteOrd3_1, tk::BYTEORD_KW, tk::BYTEORD_VERS); // 3.1+

// all versions
kw_req_meas!(Width, tk::WIDTH_KW_SUFFIX, tk::PNB_VERS);
kw_opt_meas_string!(Filter, tk::FILTER_KW_SUFFIX, tk::PNF_VERS);
kw_opt_meas!(Power, tk::POWER_KW_SUFFIX, tk::PNO_VERS, Option<Self>);
kw_opt_meas!(
    PercentEmitted,
    tk::PERCENT_EMITTED_KW_SUFFIX,
    tk::PNP_VERS,
    Option<Self>
);
kw_req_meas!(Range, tk::RANGE_KW_SUFFIX, tk::PNR_VERS);
kw_opt_meas_string!(Longname, tk::LONGNAME_KW_SUFFIX, tk::PNL_VERS);
kw_opt_meas_string!(DetectorType, tk::DET_TYPE_KW_SUFFIX, tk::PNT_VERS);
kw_opt_meas!(
    DetectorVoltage,
    tk::DET_VOLTAGE_KW_SUFFIX,
    tk::PNV_VERS,
    Option<Self>
);

// 3.0+
kw_opt_meas!(Gain, tk::GAIN_KW_SUFFIX, tk::PNG_VERS, Option<Self>);

// 3.1+
kw_opt_meas!(Display, tk::DISPLAY_KW_SUFFIX, tk::PND_VERS, Option<Self>);

// 3.2+
kw_opt_meas!(
    Feature,
    tk::FEATURE_KW_SUFFIX,
    tk::PNFEATURE_VERS,
    Option<Self>
);
meas_opt_zst!(
    TemporalType,
    tk::TYPE_KW_SUFFIX,
    tk::PNTYPE_VERS,
    TemporalTypeInner
);

impl FromStr for TemporalType {
    type Err = TemporalTypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<TemporalTypeInner>()
            .map(Some)
            .map(OptionalZST::from)
            .map(Self)
    }
}

kw_opt_meas!(
    NumType,
    tk::DATATYPE_KW_SUFFIX,
    tk::PNDATATYPE_VERS,
    Option<Self>
);
kw_opt_meas_string!(Analyte, tk::ANALYTE_KW_SUFFIX, tk::PNANALYTE_VERS);
kw_opt_meas_string!(Tag, tk::TAG_KW_SUFFIX, tk::PNTAG_VERS);
kw_opt_meas_string!(DetectorName, tk::DET_NAME_KW_SUFFIX, tk::PNDET_VERS);

impl_display_maybe_self!(OpticalType);
kw_opt_meas!(OpticalType, tk::TYPE_KW_SUFFIX, tk::PNTYPE_VERS, Self);

// version specific
kw_opt_meas!(
    Shortname,
    tk::SHORTNAME_KW_SUFFIX,
    tk::PNN_VERS,
    Option<Self>
); // optional for 2.0/3.0
req_meas!(Shortname); // required for 3.1+

kw_opt_meas!(Scale, tk::SCALE_KW_SUFFIX, tk::PNS_VERS, Option<Self>); // optional for 2.0
req_meas!(Scale); // required for 3.0+

meas_opt_zst!(
    TemporalScale2_0,
    tk::SCALE_KW_SUFFIX,
    tk::PNS_VERS,
    TemporalScaleInner
); // optional for 2.0

impl FromStrWith for TemporalScale2_0 {
    type Err = TemporalScaleError;
    type Payload<'a> = ();
    type Diagnostic = TemporalScaleFix;
    type Config = ReadStdKeywordsConfig;

    fn from_str_with(s: &str, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
        let go = |x| Self(OptionalZST(Some(x)));
        if conf.force_linear_scale.time_selected() {
            let d = TemporalScaleFix::Forced(s.to_owned());
            Ok(DiagnosedKeyword::new(go(TemporalScaleInner), d))
        } else {
            let flag = conf.trim_intra_value_whitespace;
            TemporalScaleInner::from_str_delim(s, flag).map(|x| {
                let d = x.trimmed.map(TemporalScaleFix::Trimmed).unwrap_or_default();
                DiagnosedKeyword::new(go(x.native), d)
            })
        }
    }
}

// required for 3.0+
kw_req_meas!(TemporalScale3_0, tk::SCALE_KW_SUFFIX, tk::PNS_VERS);

// scaler in 2.0/3.0
kw_opt_meas!(
    Wavelength,
    tk::WAVELENGTH_KW_SUFFIX,
    tk::PNL_VERS,
    Option<Self>
);

// vector in 3.1+
kw_opt_meas!(Wavelengths, tk::WAVELENGTH_KW_SUFFIX, tk::PNL_VERS, Self);

// 3.1 doesn't have offset
kw_opt_meas!(
    Calibration3_1,
    tk::CALIBRATION_KW_SUFFIX,
    tk::PNCALIBRATION_VERS,
    Option<Self>
);

// 3.2+ includes offset
kw_opt_meas!(
    Calibration3_2,
    tk::CALIBRATION_KW_SUFFIX,
    tk::PNCALIBRATION_VERS,
    Option<Self>
);

// 2.0 compensation matrix
#[derive(Clone, Copy, Debug, FromStr, Default, Into, Delegate)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct Dfc(pub f32);

impl_versioned_key!(Dfc, VersionMembership::One(Version::FCS2_0));

impl BiIndexedKey for Dfc {
    const PREFIX: &'static NEStr = ne_str!("DFC");
    const MIDDLE: &'static NEStr = ne_str!("TO");
}

impl Dfc {
    pub(crate) fn lookup(
        kws: &mut StdKeywords,
        k: Key2<Self>,
    ) -> Result<Option<Self>, LookupDfcError> {
        kws.remove(&k.as_std_key()).map_or(Ok(None), |v| {
            v.parse::<Self>()
                .map_err(|e| ParseKeyError::new(e, k, TruncatedString(v.clone())))
                .map(Some)
        })
    }
}

pub type LookupDfcError = ParseKeyError<ParseFloatError, Dfc, BiIndex>;

// 3.0/3.1 subsets
kw_opt_root_int!(CSMode, usize, tk::CSMODE_KW, tk::CSMODE_VERS);

kw_opt_meta_opt_u32!(CSTot, tk::CSTOT_KW, tk::CSTOT_VERS);
kw_opt_meta_opt_u32!(CSVBits, tk::CSVBITS_KW, tk::CSVBITS_VERS);

// $CSVnFLAG (3.0/3.1)
newtype_int!(CSVFlag, u32);
opt_meas!(CSVFlag, Option<Self>);

const CSV_VERS: VersionMembership = VersionMembership::Two([Version::FCS3_0, Version::FCS3_1]);

impl VersionedKey for CSVFlag {
    const VERS: VersionMembership = CSV_VERS;
}

// TODO use macro for this
impl IndexedKey for CSVFlag {
    const C: PrefixSuffix = PrefixSuffix::Both(ne_str!("CSV"), ne_str!("FLAG"));
}

// $PKn (2.0-3.1)
const PKN_VERS: VersionMembership =
    VersionMembership::Three([Version::FCS2_0, Version::FCS3_0, Version::FCS3_1]);

newtype_int!(PeakBin, u32);
opt_meas!(PeakBin, Option<Self>);

impl VersionedKey for PeakBin {
    const VERS: VersionMembership = PKN_VERS;
}

impl IndexedKey for PeakBin {
    const C: PrefixSuffix = PrefixSuffix::Prefix(ne_str!("PK"));
}

// $PKNn (2.0-3.1)
newtype_int!(PeakIndex, MeasIndex);
opt_meas!(PeakIndex, Option<Self>);

// TODO make macro for both of these
impl VersionedKey for PeakIndex {
    const VERS: VersionMembership = PKN_VERS;
}

impl IndexedKey for PeakIndex {
    const C: PrefixSuffix = PrefixSuffix::Prefix(ne_str!("PKN"));
}

// 2.0-3.1 gating parameters
kw_opt_root_int!(Gate, usize, tk::GATE_KW, tk::GATE_VERS);

kw_opt_gate_other!(GateScale, tk::SCALE_KW_SUFFIX);
kw_opt_gate_string!(GateFilter, tk::FILTER_KW_SUFFIX);
kw_opt_gate_other!(GatePercentEmitted, tk::PERCENT_EMITTED_KW_SUFFIX);
kw_opt_gate_other!(GateRange, tk::RANGE_KW_SUFFIX);
kw_opt_gate_other!(GateShortname, tk::SHORTNAME_KW_SUFFIX);
kw_opt_gate_string!(GateLongname, tk::LONGNAME_KW_SUFFIX);
kw_opt_gate_string!(GateDetectorType, tk::DET_TYPE_KW_SUFFIX);
kw_opt_gate_other!(GateDetectorVoltage, tk::DET_VOLTAGE_KW_SUFFIX);
kw_opt_meta!(Gating, tk::GATING_KW, tk::GATING_VERS, Option<Self>);

const REGION_VERS: VersionMembership = VersionMembership::All;

impl VersionedKey for RegionWindow {
    const VERS: VersionMembership = REGION_VERS;
}

impl IndexedKey for RegionWindow {
    const C: PrefixSuffix = PrefixSuffix::Both(REGION_KW_PREFIX, REGION_WINDOW_KW_SUFFIX);
}

opt_meas!(RegionWindow, Option<Self>);

impl<I> VersionedKey for RegionGateIndex<I> {
    const VERS: VersionMembership = REGION_VERS;
}

impl<I> IndexedKey for RegionGateIndex<I> {
    const C: PrefixSuffix = PrefixSuffix::Both(REGION_KW_PREFIX, REGION_INDEX_KW_SUFFIX);
}

impl<I> Optional for RegionGateIndex<I> {
    type Outer = Option<Self>;
}
impl<I> OptIndexedKey for RegionGateIndex<I> {}

// offsets for all versions
kw_req_meta!(Nextdata, tk::NEXTDATA_KW, tk::NEXTDATA_VERS);
opt_meta!(Nextdata, Option<Self>);

macro_rules! kw_offset {
    ($(#[$attr:meta])* $t:ident, $key:expr, $m:expr) => {
        $(#[$attr])*
        #[derive(From, Into, FromStr, Debug, Clone, Copy, Delegate)]
        #[delegate(ToDisplayNE<'a>, generics = "'a")]
        #[into(u64, i128, UintZeroPad20)]
        pub struct $t(pub UintZeroPad20);

        kw_req_meta!($t, $key, $m);
    };
}

kw_offset!(
    /// Value for $BEGINANALYSIS key (3.0-3.2)
    Beginanalysis,
    tk::BEGINANALYSIS_KW,
    tk::BEGINANALYSIS_VERS
);
kw_offset!(
    /// Value for $BEGINDATA key (3.0-3.2)
    Begindata,
    tk::BEGINDATA_KW,
    tk::BEGINDATA_VERS
);
kw_offset!(
    /// Value for $BEGINSTEXT key (3.0-3.2)
    Beginstext,
    tk::BEGINSTEXT_KW,
    tk::BEGINSTEXT_VERS
);
kw_offset!(
    /// Value for $ENDANALYSIS key (3.0-3.2)
    Endanalysis,
    tk::ENDANALYSIS_KW,
    tk::ENDANALYSIS_VERS
);
kw_offset!(
    /// Value for $ENDDATA key (3.0-3.2)
    Enddata,
    tk::ENDDATA_KW,
    tk::ENDDATA_VERS
);
kw_offset!(
    /// Value for $ENDSTEXT (3.0-3.2)
    Endstext,
    tk::ENDSTEXT_KW,
    tk::ENDSTEXT_VERS
);

opt_meta!(Beginanalysis, Option<Self>);
opt_meta!(Endanalysis, Option<Self>);
opt_meta!(Beginstext, Option<Self>);
opt_meta!(Endstext, Option<Self>);

/// Score generated when guessing version from keywords.
#[derive(Default, PartialEq, Clone, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct KeywordVersionScore {
    /// Number of required keywords expected to be in this version and found.
    ///
    /// This is for documentation only.
    pub good_req: usize,
    /// Number of optional keywords expected to be in this version and found.
    ///
    /// This is for documentation only.
    pub good_opt: usize,
    /// Number of keywords (opt or req) that must be dropped for this version.
    ///
    /// Smaller is better when comparing versions.
    pub drop: usize,
    /// Number of optional keywords that are missing in this version.
    ///
    /// This is for documentation only.
    pub missing_opt: usize,
    /// Number of required keywords that are missing in this version.
    ///
    /// If this number is non-zero, the version will be considered impossible
    /// for the given set of keywords.
    pub missing_req: usize,
    /// Number of keywords that are expected to be missing for this version.
    ///
    /// This is for documentation only.
    pub missing_absent: usize,
}

impl KeywordVersionScore {
    pub(crate) fn is_passing(&self, allow_drop: bool) -> bool {
        (self.missing_req == 0) && (self.drop == 0 || (self.drop > 0 && allow_drop))
    }
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Default)]
pub(crate) struct KeywordOptimizer {
    /// Number of keywords not counted elsewhere here
    n_any: usize,
    /// Number of optional keywords found that will be dropped if less then 3.0
    n_opt_min3_0: usize,
    /// Number of optional keywords found that will be dropped if less then 3.1
    n_opt_min3_1: usize,
    /// Number of optional keywords found that will be dropped if less then 3.2
    n_opt_min3_2: usize,
    /// Number of optional keywords found that will be dropped if greater than 3.1
    n_opt_max3_1: usize,
    /// Number of optional keywords found that will be dropped if not 2.0
    n_opt_eq2_0: usize,
    /// Number of optional keywords found that will be dropped if not 3.0
    n_opt_eq3_0: usize,
    /// Number of optional keywords found that will be dropped if not 3.2
    n_opt_eq3_2: usize,
    /// Number of optional keywords found that will be dropped if not 3.0/3.1
    n_opt_eq3_0or3_1: usize,
    /// Number of $PnN found
    n_pnn: usize,
    /// Number of $PnE found
    n_pne: usize,
    /// If $CYT was found
    found_cyt: bool,
    /// If $TOT was found
    found_tot: bool,
    /// If $BEGINDATA found
    found_begindata: bool,
    /// If $BEGINANALYSIS found
    found_beginanalysis: bool,
    /// If $BEGINSTEXT found
    found_beginstext: bool,
    /// If $ENDDATA found
    found_enddata: bool,
    /// If $ENDANALYSIS found
    found_endanalysis: bool,
    /// If $ENDSTEXT found
    found_endstext: bool,
    /// If $BYTEORD is not either '1,2,3,4' or '4,3,2,1'
    non_endian_byteord: bool,
    /// Value (or not) of $MODE
    mode_value: ModeValue,
}

impl KeywordOptimizer {
    #[allow(clippy::too_many_lines)]
    pub(crate) fn get_score(&self, version: Version, par: Par) -> KeywordVersionScore {
        let mut score = KeywordVersionScore::default();

        // these can be any version, so automatically count them as good
        score.good_opt += self.n_any;

        // count keywords as dropped if the version is not in range
        macro_rules! comp_drop_maybe {
            ($comp:expr, $field:ident) => {
                if $comp {
                    score.good_opt += self.$field;
                } else {
                    score.drop += self.$field;
                }
            };
        }
        comp_drop_maybe!(version >= Version::FCS3_0, n_opt_min3_0);
        comp_drop_maybe!(version >= Version::FCS3_1, n_opt_min3_1);
        comp_drop_maybe!(version >= Version::FCS3_2, n_opt_min3_2);
        comp_drop_maybe!(version <= Version::FCS3_1, n_opt_max3_1);
        comp_drop_maybe!(version == Version::FCS2_0, n_opt_eq2_0);
        comp_drop_maybe!(version == Version::FCS3_0, n_opt_eq3_0);
        comp_drop_maybe!(version == Version::FCS3_2, n_opt_eq3_2);
        comp_drop_maybe!(
            version == Version::FCS3_0 || version == Version::FCS3_1,
            n_opt_eq3_0or3_1
        );

        // $PnN became required in version 3.1, so count any missing $PnN as
        // impossible in these later versions
        // ASSUME n_pnn will always be less than $PAR
        let missing_names = par.0.saturating_sub(self.n_pnn);
        if version >= Version::FCS3_1 {
            score.missing_req += missing_names;
            score.good_req += self.n_pnn;
        } else {
            score.missing_opt += missing_names;
            score.good_opt += self.n_pnn;
        }

        // $PnE are the same as $PnN except for version 3.0
        let missing_scales = par.0.saturating_sub(self.n_pne);
        if version >= Version::FCS3_0 {
            score.missing_req += missing_scales;
            score.good_req += self.n_pnn;
        } else {
            score.missing_opt += missing_scales;
            score.good_opt += self.n_pnn;
        }

        // $CYT became required in version 3.2, so mark as impossible for this
        // version if not found
        match (version == Version::FCS3_2, self.found_cyt) {
            (true, true) => score.good_req += 1,
            (true, false) => score.missing_req += 1,
            (false, true) => score.good_opt += 1,
            (false, false) => score.missing_opt += 1,
        }

        // $TOT became required in version 3.0
        match (version >= Version::FCS3_0, self.found_tot) {
            (true, true) => score.good_req += 1,
            (true, false) => score.missing_req += 1,
            (false, true) => score.good_opt += 1,
            (false, false) => score.missing_opt += 1,
        }

        // $(BEGIN/END)(STEXT/ANALYSIS) were not in 2.0 and required in 3.0+
        let go_req_offsets = |s: &mut KeywordVersionScore, found: bool| {
            if version == Version::FCS2_0 {
                if found {
                    s.drop += 1;
                } else {
                    s.missing_absent += 1;
                }
            } else if found {
                s.good_req += 1;
            } else {
                s.missing_req += 1;
            }
        };

        go_req_offsets(&mut score, self.found_begindata);
        go_req_offsets(&mut score, self.found_enddata);

        // $(BEGIN/END)(STEXT/ANALYSIS) were not in 2.0, required in 3.0/3.1, and
        // optional in 3.2
        let go_opt_offsets = |s: &mut KeywordVersionScore, found: bool| match version {
            Version::FCS2_0 => {
                if found {
                    s.drop += 1;
                } else {
                    s.missing_absent += 1;
                }
            }
            Version::FCS3_0 | Version::FCS3_1 => {
                if found {
                    s.good_req += 1;
                } else {
                    s.missing_req += 1;
                }
            }
            Version::FCS3_2 => {
                if found {
                    s.good_opt += 1;
                } else {
                    s.missing_opt += 1;
                }
            }
        };

        go_opt_offsets(&mut score, self.found_beginanalysis);
        go_opt_offsets(&mut score, self.found_beginstext);
        go_opt_offsets(&mut score, self.found_endanalysis);
        go_opt_offsets(&mut score, self.found_endstext);

        // $BYTEORD must only be big or little endian in 3.1+
        if version >= Version::FCS3_1 && self.non_endian_byteord {
            score.missing_req += 1;
        } else {
            score.good_req += 1;
        }

        // $MODE can only be U or C in 3.1 or less, and can only be missing
        // in 3.2
        match (version == Version::FCS3_2, self.mode_value) {
            (true, ModeValue::List) => score.good_opt += 1,
            (true, ModeValue::Other) => score.drop += 1,
            (true, ModeValue::Missing) => score.missing_opt += 1,
            (false, ModeValue::Missing) => score.missing_req += 1,
            (false, ModeValue::Other | ModeValue::List) => score.good_req += 1,
        }

        score
    }

    pub(crate) fn classify_keyword(&mut self, key: &StdKey, value: &str) {
        match AnyKeywordClass::classify_keyword(key) {
            AnyKeywordClass::Root(r) => match r {
                RootKeywordClass::Beginanalysis => self.found_beginanalysis = true,
                RootKeywordClass::Beginstext => self.found_beginstext = true,
                RootKeywordClass::Begindata => self.found_begindata = true,
                RootKeywordClass::Endanalysis => self.found_endanalysis = true,
                RootKeywordClass::Endstext => self.found_endstext = true,
                RootKeywordClass::Enddata => self.found_enddata = true,
                RootKeywordClass::Cyt => self.found_cyt = true,
                RootKeywordClass::Tot => self.found_tot = true,
                RootKeywordClass::Mode => {
                    let m = value
                        .parse::<Mode>()
                        .map(|m| match m {
                            Mode::List => ModeValue::List,
                            _ => ModeValue::Other,
                        })
                        .unwrap_or(ModeValue::Missing);
                    self.mode_value = m;
                }
                RootKeywordClass::Byteord => {
                    if let Ok(res) = value.parse::<ByteOrd2_0>() {
                        self.non_endian_byteord = !res.is_endian();
                    }
                }
                RootKeywordClass::Timestep => {
                    self.n_opt_min3_0 += 1;
                }
                RootKeywordClass::OptGE3_1 => {
                    self.n_opt_min3_1 += 1;
                }
                RootKeywordClass::OptGE3_2 => {
                    self.n_opt_min3_2 += 1;
                }
                RootKeywordClass::OptEQ3_0or3_1 => {
                    self.n_opt_eq3_0or3_1 += 1;
                }
                RootKeywordClass::OptLE3_1 => {
                    self.n_opt_max3_1 += 1;
                }
                RootKeywordClass::OptEQ3_0 => self.n_opt_eq3_0 += 1,
                RootKeywordClass::OptAny => self.n_any += 1,
            },
            AnyKeywordClass::Meas(_, r) => match r {
                MeasKeywordClass::OptGE3_0 => {
                    self.n_opt_min3_0 += 1;
                }
                MeasKeywordClass::OptGE3_1 => {
                    self.n_opt_min3_1 += 1;
                }
                MeasKeywordClass::OptGE3_2 => {
                    self.n_opt_min3_2 += 1;
                }
                MeasKeywordClass::Scale => self.n_pne += 1,
                MeasKeywordClass::Shortname => self.n_pnn += 1,
                MeasKeywordClass::Wavelength => {
                    // TODO what to do on failure?
                    if let Ok(w) = Wavelengths::from_str_delim(value, true.into()) {
                        if w.native.0.len() > 1 {
                            self.n_opt_min3_1 += 1;
                        } else {
                            self.n_any += 1;
                        }
                    }
                }
                MeasKeywordClass::OptAny => self.n_any += 1,
            },
            AnyKeywordClass::Peak(_) => {
                self.n_opt_max3_1 += 1;
            }
            AnyKeywordClass::CSVFlag(_) => {
                self.n_opt_eq3_0or3_1 += 1;
            }
            AnyKeywordClass::Dfc(_, _) => self.n_opt_eq2_0 += 1,
            AnyKeywordClass::GateOptLE3_1(_) => self.n_opt_max3_1 += 1,
            AnyKeywordClass::RegionWindow => self.n_any += 1,
            AnyKeywordClass::RegionIndex => {
                if RegionGateIndex::<GateIndex>::from_str_delim(value, true.into()).is_ok() {
                    self.n_opt_eq2_0 += 1;
                } else if RegionGateIndex::<MeasOrGateIndex>::from_str_delim(value, true.into())
                    .is_ok()
                {
                    self.n_opt_eq3_0or3_1 += 1;
                } else if RegionGateIndex::<PrefixedMeasIndex>::from_str_delim(value, true.into())
                    .is_ok()
                {
                    self.n_opt_eq3_2 += 1;
                }
            }
            AnyKeywordClass::NonStandard => (),
        }
    }
}

#[derive(Clone, Copy, Default)]
enum ModeValue {
    #[default]
    Missing,
    List,
    Other,
}

enum AnyKeywordClass {
    Root(RootKeywordClass),
    Meas(MeasIndex, MeasKeywordClass),
    CSVFlag(MeasIndex),
    Peak(MeasIndex),
    Dfc(MeasIndex, MeasIndex),
    GateOptLE3_1(GateIndex),
    RegionIndex,
    RegionWindow,
    NonStandard,
}

impl AnyKeywordClass {
    fn classify_keyword(key: &StdKey) -> Self {
        fn split_index_and_suffix(xs: &str) -> Option<(usize, &str)> {
            let mut index = 0_usize;
            let mut it = xs.as_bytes().iter();
            // read first character, only continue if a digit 1-9 (no leading
            // zeros)
            if let Some(x) = it.by_ref().next()
                && (49..58).contains(x)
            {
                index += usize::from(*x) - 48;
                let mut k = 1;
                for y in it.take_while(|&&z| (48..58).contains(&z)) {
                    index = 10 * index + (usize::from(*y) - 48);
                    k += 1;
                }
                debug_assert!(index > 0, "index should be greater than 0 here");
                Some((index - 1, xs.split_at(k).1))
            } else {
                None
            }
        }

        fn starts_with_icase<'a>(haystack: &'a str, prefix: &str) -> Option<&'a str> {
            let n = prefix.len();
            if n > haystack.len() {
                None
            } else {
                let (x, y) = haystack.split_at(n);
                x.eq_ignore_ascii_case(prefix).then_some(y)
            }
        }

        let s = key.as_ascii_str();
        let ss: &str = key.as_ref();

        debug_assert!(s.is_ascii(), "key is not ASCII");

        if let Some(rc) = tk::KW_MAP.get(&s) {
            Self::Root(*rc)
        } else if let Some(rest) = starts_with_icase(ss, "P") {
            // $Pn* keywords or $PKn or $PKNn
            if let Some((index, suffix)) =
                starts_with_icase(rest, "KN").and_then(|r| split_index_and_suffix(r))
                && suffix.is_empty()
            {
                // $PKNn
                Self::Peak(index.into())
            } else if let Some((index, suffix)) =
                starts_with_icase(rest, "K").and_then(|r| split_index_and_suffix(r))
                && suffix.is_empty()
            {
                // $PKn
                Self::Peak(index.into())
            } else if let Some((index, suffix)) = split_index_and_suffix(rest) {
                // $Pn*
                let j = index.into();
                if let Some(vc) = tk::MEAS_SUFFIX_MAP.get(&Ascii::new(suffix)) {
                    Self::Meas(j, *vc)
                } else {
                    Self::NonStandard
                }
            } else {
                Self::NonStandard
            }
        } else if let Some((index, suffix)) =
            starts_with_icase(ss, "G").and_then(|r| split_index_and_suffix(r))
            && tk::GATE_SUFFIX_SET.contains(&Ascii::new(suffix))
        {
            // $Gn* keywords
            Self::GateOptLE3_1(index.into())
        } else if let Some((_, suffix)) =
            starts_with_icase(ss, "R").and_then(|r| split_index_and_suffix(r))
        {
            // $Rn* keywords
            if REGION_INDEX_KW_SUFFIX.as_ref().eq_ignore_ascii_case(suffix) {
                Self::RegionIndex
            } else if REGION_WINDOW_KW_SUFFIX
                .as_ref()
                .eq_ignore_ascii_case(suffix)
            {
                Self::RegionWindow
            } else {
                Self::NonStandard
            }
        } else if let Some((index, suffix)) =
            starts_with_icase(ss, "CSV").and_then(|r| split_index_and_suffix(r))
            && suffix.eq_ignore_ascii_case("FLAG")
        {
            // $CSVnFLAG
            Self::CSVFlag(index.into())
        } else if let Some((i0, i1, suffix)) = starts_with_icase(ss, "DFC")
            .and_then(|r| split_index_and_suffix(r))
            .and_then(|(index, suffix)| starts_with_icase(suffix, "TO").map(|r| (index, r)))
            .and_then(|(i0, r)| split_index_and_suffix(r).map(|(i1, rr)| (i0, i1, rr)))
            && suffix.is_empty()
        {
            // $DFCmTOn
            Self::Dfc(i0.into(), i1.into())
        } else {
            Self::NonStandard
        }
    }
}

pub(crate) const MEAS_KW_PREFIX: &NEStr = ne_str!("P");
pub(crate) const GATE_KW_PREFIX: &NEStr = ne_str!("G");
pub(crate) const REGION_KW_PREFIX: &NEStr = ne_str!("R");

pub(crate) const REGION_INDEX_KW_SUFFIX: &NEStr = ne_str!("I");
pub(crate) const REGION_WINDOW_KW_SUFFIX: &NEStr = ne_str!("W");

const TIME: &NEStr = ne_str!("Time");
const DATETIME_FMT: &str = "%d-%b-%Y %H:%M:%S";

#[cfg(test)]
mod tests {
    use fireflow_types::nonempty_string::DisplayNE as _;

    use super::*;
    use crate::test::*;

    #[test]
    fn tr() {
        let conf = ReadStdKeywordsConfig::default();
        assert_from_to_str_with::<Trigger>("Wooden Leg Pt 3,456", (), &conf);
        assert!(Trigger::from_str_with("x,x", (), &conf).is_err());
        assert!(Trigger::from_str_with("x,0.0", (), &conf).is_err());
        assert!(Trigger::from_str_with("x", (), &conf).is_err());
        assert!(Trigger::from_str_with("x,x,x", (), &conf).is_err());
    }

    #[test]
    fn tr_commas() {
        let v = "Wookie Leg Pt 3, 666";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(Trigger::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<Trigger>(v, "Wookie Leg Pt 3,666", (), &conf);
    }

    #[test]
    fn mode() {
        assert_from_to_str::<Mode>("C");
        assert_from_to_str::<Mode>("L");
        assert_from_to_str::<Mode>("U");
        assert!(Mode::from_str("X").is_err());
    }

    #[test]
    fn mode_3_2() {
        assert_from_to_str::<Mode3_2>("L");
        assert!(Mode3_2::from_str("C").is_err());
        assert!(Mode3_2::from_str("U").is_err());
    }

    #[test]
    fn pnd() {
        let conf = ReadStdKeywordsConfig::default();
        assert_from_to_str_with::<Display>("Linear,0,1", (), &conf);
        assert_from_to_str_with::<Display>("Logarithmic,1,1", (), &conf);
        assert_from_to_str_with::<Display>("Logarithmic,1,0.1", (), &conf);
        assert!(Display::from_str_with("LIN,0,1", (), &conf).is_err());
        assert!(Display::from_str_with("LOG,1,1", (), &conf).is_err());
        assert!(Display::from_str_with("Logicle,0,1,2,3", (), &conf).is_err());
    }

    #[test]
    fn pnd_commas() {
        let v = "Linear, 0 , 1";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(Display::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<Display>(v, "Linear,0,1", (), &conf);
    }

    #[test]
    fn datatype() {
        assert_from_to_str::<NumType>("I");
        assert_from_to_str::<NumType>("F");
        assert_from_to_str::<NumType>("D");
        assert!(NumType::from_str("A").is_err());
    }

    #[test]
    fn pndatetype() {
        assert_from_to_str::<AlphaNumType>("I");
        assert_from_to_str::<AlphaNumType>("F");
        assert_from_to_str::<AlphaNumType>("D");
        assert_from_to_str::<AlphaNumType>("A");
        assert!(AlphaNumType::from_str("X").is_err());
    }

    #[test]
    fn pncalibration_3_1() {
        let conf = ReadStdKeywordsConfig::default();
        assert_from_to_str_with::<Calibration3_1>("0.1,cubic imperial lightyears", (), &conf);
        assert!(Calibration3_1::from_str_with("x", (), &conf).is_err());
        assert!(Calibration3_1::from_str_with("x,x", (), &conf).is_err());
        assert!(Calibration3_1::from_str_with("x,0.1", (), &conf).is_err());
    }

    #[test]
    fn pncalibration_3_1_commas() {
        let mut conf = ReadStdKeywordsConfig::default();
        let v = "1000 , yodabytes";
        assert!(Calibration3_1::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<Calibration3_1>(v, "1000,yodabytes", (), &conf);
    }

    #[test]
    fn pncalibration_3_2() {
        let conf = ReadStdKeywordsConfig::default();
        assert_from_to_str_with::<Calibration3_2>("1.1,3.5813,progressive metal albums", (), &conf);
        assert_from_to_str_with::<Calibration3_2>("1.61,0,quartic slugs", (), &conf);
        assert!(Calibration3_2::from_str_with("x", (), &conf).is_err());
        assert!(Calibration3_2::from_str_with("x,x", (), &conf).is_err());
        assert!(Calibration3_2::from_str_with("x,0.1", (), &conf).is_err());
        assert!(Calibration3_2::from_str_with("0.1,x,x", (), &conf).is_err());
    }

    #[test]
    fn pncalibration_3_2_commas() {
        let mut conf = ReadStdKeywordsConfig::default();
        let v = "1, 0.2, nanobytes";
        assert!(Calibration3_2::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<Calibration3_2>(v, "1,0.2,nanobytes", (), &conf);
    }

    #[test]
    fn pnl_3_1() {
        let conf = ReadStdKeywordsConfig::default();
        let go = |v: &str| {
            let w = Wavelengths::from_str_with(v, (), &conf).unwrap().native;
            let w_str = w.try_ne().unwrap().to_ne().to_ne_string();
            assert_eq!(w_str.as_ref(), v);
        };
        go("0.5");
        go("0.5,2");
        assert!(Wavelengths::from_str_with("x", (), &conf).is_err());
    }

    #[test]
    fn pnl_3_1_commas() {
        let mut conf = ReadStdKeywordsConfig::default();
        let v = "1, 2";
        assert!(Wavelengths::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        let w = Wavelengths::from_str_with(v, (), &conf).unwrap().native;
        let w_str = w.try_ne().unwrap().to_ne().to_ne_string();
        assert_eq!(w_str.as_ne_str(), ne_str!("1,2"));
    }

    #[test]
    fn last_modified() {
        let mut conf = ReadStdKeywordsConfig::default();
        assert_from_to_str_with::<LastModified>("01-Jan-2112 00:00:00.01", (), &conf);
        assert_from_to_str_almost_with::<LastModified>(
            "01-Jan-2112 00:00:00",
            "01-Jan-2112 00:00:00.00",
            (),
            &conf,
        );
        let v = "01-Jan-2112 00:00";
        assert!(LastModified::from_str_with(v, (), &conf).is_err());
        conf.last_modified_pattern = Some("%d-%b-%Y %H:%M".into());
        assert_from_to_str_almost_with::<LastModified>(v, "01-Jan-2112 00:00:00.00", (), &conf);
    }

    #[test]
    fn originality() {
        assert_from_to_str::<Originality>("Original");
        assert_from_to_str::<Originality>("NonDataModified");
        assert_from_to_str::<Originality>("Appended");
        assert_from_to_str::<Originality>("DataModified");
        assert!(Originality::from_str("x").is_err());
    }

    #[test]
    fn unicode() {
        let conf = ReadStdKeywordsConfig::default();
        assert_from_to_str_with::<Unicode>("42,$BYTEORD", (), &conf);
        // we don't actually check that the keyword is valid, likely nobody
        // will notice ;)
        assert_from_to_str_with::<Unicode>("42,$40DOLLARBILL", (), &conf);
        assert!(Unicode::from_str_with("42", (), &conf).is_err());
    }

    #[test]
    fn unicode_commas() {
        let v = "50 ,something tour";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(Unicode::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<Unicode>(v, "50,something tour", (), &conf);
    }

    #[test]
    fn pntype_optical() {
        // this can basically be everything, even though only a few values make sense
        let go = |v| {
            let t = OpticalType::from_str(v).unwrap();
            let k = OptMeasKeyword::from_str(&t, MeasIndex::from(0)).unwrap();
            assert!(k.as_std_key_pair().1.as_ref() == v);
        };
        go("Forward Scatter");
        go("Side Scatter");
        go("Raw Fluorescence");
        go("Unmixed Fluorescence");
        go("Mass");
        go("Electronic Volume");
        go("Index");
        go("Classification");
        go("Spongebob");
    }

    #[test]
    fn pntype_time() {
        let t = TemporalType::from_str("Time").unwrap();
        let k = OptMeasKeyword::from_opt_zst(t, MeasIndex::from(0)).unwrap();
        assert!(k.as_std_key_pair().1.as_ref() == "Time");
        assert!(TemporalType::from_str("Space").is_err());
    }

    #[test]
    fn pnfeature() {
        let mut conf = ReadStdKeywordsConfig::default();
        assert_from_to_str_with::<Feature>("Area", (), &conf);
        assert_from_to_str_with::<Feature>("Width", (), &conf);
        assert_from_to_str_with::<Feature>("Height", (), &conf);
        assert!(Feature::from_str_with("Volume", (), &conf).is_err());
        conf.allow_other_feature = true.into();
        assert_from_to_str_with::<Feature>("Volume", (), &conf);
    }

    #[test]
    fn rni_2_0() {
        let conf = ReadStdKeywordsConfig::default();
        assert_from_to_str_with::<RegionGateIndex<GateIndex>>("1", (), &conf);
        assert_from_to_str_with::<RegionGateIndex<GateIndex>>("1,2", (), &conf);
        assert!(RegionGateIndex::<GateIndex>::from_str_with("x", (), &conf).is_err());
        assert!(RegionGateIndex::<GateIndex>::from_str_with("1,2,3", (), &conf).is_err());
    }

    #[test]
    fn rni_2_0_commas() {
        let v = "1, 2";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(RegionGateIndex::<GateIndex>::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<RegionGateIndex<GateIndex>>(v, "1,2", (), &conf);
    }

    #[test]
    fn rni_3_0() {
        let conf = ReadStdKeywordsConfig::default();
        assert_from_to_str_with::<RegionGateIndex<MeasOrGateIndex>>("P1", (), &conf);
        assert_from_to_str_with::<RegionGateIndex<MeasOrGateIndex>>("P1,P2", (), &conf);
        assert_from_to_str_with::<RegionGateIndex<MeasOrGateIndex>>("G1", (), &conf);
        assert_from_to_str_with::<RegionGateIndex<MeasOrGateIndex>>("G1,G2", (), &conf);
        assert!(RegionGateIndex::<MeasOrGateIndex>::from_str_with("x", (), &conf).is_err());
        assert!(RegionGateIndex::<MeasOrGateIndex>::from_str_with("P1,G2,P3", (), &conf).is_err());
    }

    #[test]
    fn rni_3_0_commas() {
        let v = "P1, G2";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(RegionGateIndex::<MeasOrGateIndex>::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<RegionGateIndex<MeasOrGateIndex>>(v, "P1,G2", (), &conf);
    }

    #[test]
    fn rni_3_2() {
        let conf = ReadStdKeywordsConfig::default();
        assert_from_to_str_with::<RegionGateIndex<PrefixedMeasIndex>>("P1", (), &conf);
        assert_from_to_str_with::<RegionGateIndex<PrefixedMeasIndex>>("P1,P2", (), &conf);
        assert!(RegionGateIndex::<PrefixedMeasIndex>::from_str_with("x", (), &conf).is_err());
        assert!(
            RegionGateIndex::<PrefixedMeasIndex>::from_str_with("P1,P2,P3", (), &conf).is_err()
        );
    }

    #[test]
    fn rni_3_2_commas() {
        let v = "P1, P2";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(RegionGateIndex::<PrefixedMeasIndex>::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<RegionGateIndex<PrefixedMeasIndex>>(v, "P1,P2", (), &conf);
    }

    #[test]
    fn rnw() {
        let conf = ReadStdKeywordsConfig::default();
        assert_from_to_str_with::<RegionWindow>("1,1", (), &conf);
        assert_from_to_str_with::<RegionWindow>("1,1;2,3;5,8;13,21", (), &conf);
        assert!(RegionWindow::from_str_with("1", (), &conf).is_err());
        assert!(RegionWindow::from_str_with("1,1,1", (), &conf).is_err());
        assert!(RegionWindow::from_str_with("1;1", (), &conf).is_err());
        assert!(RegionWindow::from_str_with("1,1,1;1,1,1", (), &conf).is_err());
    }

    #[test]
    fn rnw_commas() {
        let v = "1, 1 ; 2, 2";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(RegionWindow::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<RegionWindow>(v, "1,1;2,2", (), &conf);
    }

    #[test]
    fn gating() {
        assert_from_to_str::<Gating>("R1");
        assert_from_to_str_almost::<Gating>("R1 AND (R2.OR.R3)", "(R1 AND (R2 OR R3))");
        assert_from_to_str::<Gating>("((NOT R1) AND R2)");
        assert!(Gating::from_str("NAND R1").is_err());
    }

    #[test]
    fn unstained_centers() {
        let conf = ReadStdKeywordsConfig::default();
        let v = "1,X,0";
        let t = UnstainedCenters::from_str_with(v, (), &conf).unwrap();
        let s = t.native.try_ne().unwrap().to_ne().to_ne_string();
        assert_eq!(s.as_ref(), v);
    }

    #[test]
    fn unstained_centers_commas() {
        let v = "1, X , 0";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(UnstainedCenters::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        let t = UnstainedCenters::from_str_with(v, (), &conf).unwrap();
        let s = t.native.try_ne().unwrap().to_ne().to_ne_string();
        assert_eq!(s.as_ref(), "1,X,0");
    }

    #[test]
    fn unstained_centers_wrong_len() {
        let conf = ReadStdKeywordsConfig::default();
        assert!(UnstainedCenters::from_str_with("2,X,0", (), &conf).is_err());
    }

    #[test]
    fn unstained_centers_nonunique() {
        let conf = ReadStdKeywordsConfig::default();
        assert!(UnstainedCenters::from_str_with("3,Y,Y,Z,0,0,0", (), &conf).is_err());
    }

    #[test]
    fn str_compensation() {
        let conf = ReadStdKeywordsConfig::default();
        assert_from_to_str_with::<Compensation3_0>("2,0,0,0,0", (), &conf);
        assert_from_to_str_with::<Compensation3_0>("3,0,0,0,0,0,0,0,0,0", (), &conf);
        assert_from_to_str_with::<Compensation3_0>("2,1.1,1,0,-1.5", (), &conf);
    }

    #[test]
    fn str_compensation_too_small() {
        let conf = ReadStdKeywordsConfig::default();
        assert!(Compensation3_0::from_str_with("1,0", (), &conf).is_err());
    }

    #[test]
    fn str_compensation_mismatch() {
        let conf = ReadStdKeywordsConfig::default();
        assert!(Compensation3_0::from_str_with("2,0,0,0", (), &conf).is_err());
    }

    #[test]
    fn str_compensation_badfloats() {
        let conf = ReadStdKeywordsConfig::default();
        assert!(Compensation3_0::from_str_with("2,zero,0,coconut", (), &conf).is_err());
    }

    #[test]
    fn str_compensation_commas() {
        let v = "2, 0, 0, 0, 0";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(Compensation3_0::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<Compensation3_0>(v, "2,0,0,0,0", (), &conf);
    }

    #[test]
    fn str_to_byteord_valid() {
        assert_from_to_str::<ByteOrd2_0>("1");
        assert_from_to_str::<ByteOrd2_0>("1,2,3");
        assert_from_to_str::<ByteOrd2_0>("1,2,3,4");
        assert_from_to_str::<ByteOrd2_0>("4,3,2,1");
        assert_from_to_str::<ByteOrd2_0>("3,4,2,1");
        assert_from_to_str::<ByteOrd2_0>("1,2,3,4,5,6,7,8");
    }

    #[test]
    fn str_to_byteord_tolong() {
        assert!("1,2,3,4,5,6,7,8,9".parse::<ByteOrd2_0>().is_err());
    }

    #[test]
    fn str_to_byteord_bad_digits() {
        assert!("0".parse::<ByteOrd2_0>().is_err());
        assert!("2".parse::<ByteOrd2_0>().is_err());
    }

    #[test]
    fn str_to_byteord_skipped() {
        assert!("1,3".parse::<ByteOrd2_0>().is_err());
    }

    #[test]
    fn str_to_byteord_repeat() {
        assert!("1,1".parse::<ByteOrd2_0>().is_err());
    }

    #[test]
    fn str_to_byteord_garbage() {
        assert!("fortytwo".parse::<ByteOrd2_0>().is_err());
        assert!("".parse::<ByteOrd2_0>().is_err());
        assert!("one,two,three".parse::<ByteOrd2_0>().is_err());
    }

    #[test]
    fn str_to_endian() {
        assert!("1,2,3,4".parse::<ByteOrd3_1>().is_ok());
        assert!("4,3,2,1".parse::<ByteOrd3_1>().is_ok());
        assert!("1,2,3".parse::<ByteOrd3_1>().is_err());
        assert!("5,4,3,2,1".parse::<ByteOrd3_1>().is_err());
    }

    #[test]
    fn scale() {
        let conf = ReadStdKeywordsConfig::default();
        let dt = AlphaNumType::Integer;
        assert_from_to_str_with::<Scale>("0,0", dt, &conf);
        assert_from_to_str_with::<Scale>("4.5,0.01", dt, &conf);
    }

    #[test]
    fn scale_zero_log() {
        let v = "4.5,0";
        let mut conf = ReadStdKeywordsConfig::default();
        let dt = AlphaNumType::Integer;
        assert!(Scale::from_str_with(v, dt, &conf).is_err());
        conf.fix_log_scale_offsets = true.into();
        assert_from_to_str_almost_with::<Scale>(v, "4.5,1", dt, &conf);
    }

    #[test]
    fn scale_force_linear() {
        let v = "1,1";
        let mut conf = ReadStdKeywordsConfig::default();
        let dt = AlphaNumType::Float;
        assert_from_to_str_almost_with::<Scale>(v, "1,1", dt, &conf);
        conf.force_linear_scale = ForceLinearScale::AllNonInt;
        assert_from_to_str_almost_with::<Scale>(v, "0,0", dt, &conf);
    }

    #[test]
    fn scale_force_linear_int() {
        let v = "1,1";
        let mut conf = ReadStdKeywordsConfig::default();
        let dt = AlphaNumType::Integer;
        assert_from_to_str_almost_with::<Scale>(v, "1,1", dt, &conf);
        conf.force_linear_scale = ForceLinearScale::AllNonInt;
        assert_from_to_str_almost_with::<Scale>(v, "1,1", dt, &conf);
        conf.force_linear_scale = ForceLinearScale::All;
        assert_from_to_str_almost_with::<Scale>(v, "0,0", dt, &conf);
    }

    #[test]
    fn scale_commas() {
        let v = "0, 0";
        let mut conf = ReadStdKeywordsConfig::default();
        let dt = AlphaNumType::Integer;
        assert!(Scale::from_str_with(v, dt, &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<Scale>(v, "0,0", dt, &conf);
    }

    #[test]
    fn tmp_scale2() {
        let conf = ReadStdKeywordsConfig::default();
        // no display, so just check parse
        assert!(TemporalScale2_0::from_str_with("0,0", (), &conf).is_ok());
        assert!(TemporalScale2_0::from_str_with("1,1", (), &conf).is_err());
    }

    #[test]
    fn tmp_scale2_commas() {
        let v = "0, 0";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(TemporalScale2_0::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert!(TemporalScale2_0::from_str_with(v, (), &conf).is_ok());
    }

    #[test]
    fn tmp_scale3() {
        let conf = ReadStdKeywordsConfig::default();
        assert_from_to_str_with::<TemporalScale3_0>("0,0", (), &conf);
        assert!(TemporalScale3_0::from_str_with("1,1", (), &conf).is_err());
    }

    #[test]
    fn tmp_scale3_commas() {
        let v = "0, 0";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(TemporalScale3_0::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<TemporalScale3_0>(v, "0,0", (), &conf);
    }

    #[test]
    fn gate_scale() {
        let conf = ReadStdKeywordsConfig::default();
        assert_from_to_str_with::<GateScale>("0,0", (), &conf);
        assert_from_to_str_with::<GateScale>("4.5,0.01", (), &conf);
    }

    #[test]
    fn gate_scale_zero_log() {
        let v = "4.5,0";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(GateScale::from_str_with(v, (), &conf).is_err());
        conf.fix_log_scale_offsets = true.into();
        assert_from_to_str_almost_with::<GateScale>(v, "4.5,1", (), &conf);
    }

    #[test]
    fn gate_scale_commas() {
        let v = "0, 0";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(GateScale::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<GateScale>(v, "0,0", (), &conf);
    }
}

#[cfg(feature = "python")]
mod python {
    use crate::text::keywords::ScaleFix;
    use crate::text::ranged_float::PositiveFloat;
    use crate::validated::shortname::Shortname;

    use super::{
        ByteOrd2_0, Calibration3_1, Calibration3_2, Display, IndexPair, OpticalScaleFix, Scale,
        TemporalScaleFix, Trigger, UniGate, Unicode, Vertex,
    };

    use fireflow_types::keywords::{
        SCALE_DIAGNOSTIC_FORCED, SCALE_DIAGNOSTIC_LOG, SCALE_DIAGNOSTIC_TRIMMED,
        SCALE_DIAGNOSTIC_TRIMMED_LOG, TEMPORAL_SCALE_DIAGNOSTIC_FORCED,
        TEMPORAL_SCALE_DIAGNOSTIC_TRIMMED,
    };
    use fireflow_types::nonempty_string::NEString;
    use pyo3::conversion::IntoPyObjectExt as _;
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use pyo3::types::PyTuple;
    use std::num::NonZeroU8;

    // TOOD this is not a well-defined conversion since "big" and "little"
    // should be usable as well. This won't work for this because there is no
    // way to know a priori what the length of byteord should be for big/little,
    // but that means we should just use a different type altogether than
    // specifying the byteord in config

    // $BYTEORD is a list of integers
    impl<'py> FromPyObject<'py> for ByteOrd2_0 {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let xs: Vec<NonZeroU8> = ob.extract()?;
            let ret = Self::try_from(&xs[..])?;
            Ok(ret)
        }
    }

    impl<'py> IntoPyObject<'py> for ByteOrd2_0 {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            let xs: Vec<_> = self
                .to_vec()
                .into_iter()
                .map(u8::from)
                .map(u32::from)
                .collect();
            xs.into_pyobject(py)
        }
    }

    // $PnE (2.0) as either () or (f32, f32) tuples in python
    impl<'py> FromPyObject<'py> for Scale {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            if ob.is_instance_of::<PyTuple>() && ob.len()? == 0 {
                Ok(Self::Linear)
            } else {
                let (decades, offset): (f32, f32) = ob.extract()?;
                let ret = Self::try_new_log(decades, offset)?;
                Ok(ret)
            }
        }
    }

    impl<'py> IntoPyObject<'py> for Scale {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Linear => Ok(PyTuple::empty(py).into_any()),
                Self::Log(l) => (f32::from(l.decades), f32::from(l.offset)).into_bound_py_any(py),
            }
        }
    }

    // $PnCALIBRATION (3.1) as (f32, String) tuple in python
    impl<'py> FromPyObject<'py> for Calibration3_1 {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (slope, unit): (PositiveFloat, NEString) = ob.extract()?;
            Ok(Self { slope, unit })
        }
    }

    impl<'py> IntoPyObject<'py> for Calibration3_1 {
        type Target = PyTuple;
        type Output = Bound<'py, <(PositiveFloat, NEString) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.slope, self.unit).into_pyobject(py)
        }
    }

    // $PnCALIBRATION (3.2) as (f32, f32, String) tuple in python
    impl<'py> FromPyObject<'py> for Calibration3_2 {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (slope, offset, unit): (PositiveFloat, f32, NEString) = ob.extract()?;
            Ok(Self {
                slope,
                offset,
                unit,
            })
        }
    }

    impl<'py> IntoPyObject<'py> for Calibration3_2 {
        type Target = PyTuple;
        type Output = Bound<'py, <(PositiveFloat, f32, NEString) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.slope, self.offset, self.unit).into_pyobject(py)
        }
    }

    // $UNICODE (3.0) as a tuple like (f32, [String]) in python
    impl<'py> FromPyObject<'py> for Unicode {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (page, kws): (u32, Vec<NEString>) = ob.extract()?;
            Ok(Self { page, kws })
        }
    }

    impl<'py> IntoPyObject<'py> for Unicode {
        type Target = PyTuple;
        type Output = Bound<'py, <(u32, Vec<NEString>) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.page, self.kws).into_pyobject(py)
        }
    }

    // $PnD (3.1+) as a tuple like (bool, f32, f32) in python where 'bool' is true
    // if linear
    impl<'py> FromPyObject<'py> for Display {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (is_log, x0, x1): (bool, f32, f32) = ob.extract()?;
            let ret = if is_log {
                Self::Log {
                    offset: x0.try_into()?,
                    decades: x1.try_into()?,
                }
            } else {
                Self::Lin {
                    lower: x0,
                    upper: x1,
                }
            };
            Ok(ret)
        }
    }

    impl<'py> IntoPyObject<'py> for Display {
        type Target = PyTuple;
        type Output = Bound<'py, <(bool, f32, f32) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            let ret = match self {
                Self::Lin { lower, upper } => (false, lower, upper),
                Self::Log { offset, decades } => (true, offset.into(), decades.into()),
            };
            ret.into_pyobject(py)
        }
    }

    // $TR as a tuple like (String, u32) in python
    impl<'py> FromPyObject<'py> for Trigger {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (measurement, threshold): (Shortname, u32) = ob.extract()?;
            Ok(Self {
                measurement,
                threshold,
            })
        }
    }

    impl<'py> IntoPyObject<'py> for Trigger {
        type Target = PyTuple;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.measurement, self.threshold).into_pyobject(py)
        }
    }

    // unigate (for univariate gating regions) is a tuple pair of floats
    impl<'py> FromPyObject<'py> for UniGate {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (lower, upper) = ob.extract()?;
            Ok(Self { lower, upper })
        }
    }

    impl<'py> IntoPyObject<'py> for UniGate {
        type Target = PyTuple;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.lower, self.upper).into_pyobject(py)
        }
    }

    // vertex (for bivariate gating regions) is a tuple pair of floats
    impl<'py> FromPyObject<'py> for Vertex {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (x, y) = ob.extract()?;
            Ok(Self { x, y })
        }
    }

    impl<'py> IntoPyObject<'py> for Vertex {
        type Target = PyTuple;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.x, self.y).into_pyobject(py)
        }
    }

    // index pairs are like python tuple pairs
    impl<'py, I> FromPyObject<'py> for IndexPair<I>
    where
        I: FromPyObject<'py>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (x, y) = ob.extract()?;
            Ok(Self { x, y })
        }
    }

    impl<'py, I> IntoPyObject<'py> for IndexPair<I>
    where
        I: IntoPyObject<'py>,
    {
        type Target = PyTuple;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.x, self.y).into_pyobject(py)
        }
    }

    impl<'py> FromPyObject<'py> for ScaleFix {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            if let Some((x, y)) = ob.extract::<Option<(String, String)>>()? {
                match y.as_str() {
                    SCALE_DIAGNOSTIC_LOG => Ok(Self::LogFixed(x)),
                    SCALE_DIAGNOSTIC_TRIMMED => Ok(Self::Trimmed(x)),
                    SCALE_DIAGNOSTIC_TRIMMED_LOG => Ok(Self::TrimmedLogFixed(x)),
                    _ => Err(PyValueError::new_err(format!(
                        "second string must be '{SCALE_DIAGNOSTIC_LOG}', \
                         '{SCALE_DIAGNOSTIC_TRIMMED}', or \
                         '{SCALE_DIAGNOSTIC_TRIMMED_LOG}'",
                    ))),
                }
            } else {
                Ok(Self::None)
            }
        }
    }

    impl<'py> FromPyObject<'py> for OpticalScaleFix {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            if let Some((x, y)) = ob.extract::<Option<(String, String)>>()? {
                match y.as_str() {
                    SCALE_DIAGNOSTIC_FORCED => Ok(Self::Forced(x)),
                    SCALE_DIAGNOSTIC_LOG => Ok(ScaleFix::LogFixed(x).into()),
                    SCALE_DIAGNOSTIC_TRIMMED => Ok(ScaleFix::Trimmed(x).into()),
                    SCALE_DIAGNOSTIC_TRIMMED_LOG => Ok(ScaleFix::TrimmedLogFixed(x).into()),
                    _ => Err(PyValueError::new_err(format!(
                        "second string must be '{SCALE_DIAGNOSTIC_FORCED}', \
                         '{SCALE_DIAGNOSTIC_LOG}', '{SCALE_DIAGNOSTIC_TRIMMED}', \
                         or '{SCALE_DIAGNOSTIC_TRIMMED_LOG}'",
                    ))),
                }
            } else {
                Ok(ScaleFix::None.into())
            }
        }
    }

    impl<'py> FromPyObject<'py> for TemporalScaleFix {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            if let Some((x, y)) = ob.extract::<Option<(String, String)>>()? {
                match y.as_str() {
                    TEMPORAL_SCALE_DIAGNOSTIC_FORCED => Ok(Self::Forced(x)),
                    TEMPORAL_SCALE_DIAGNOSTIC_TRIMMED => Ok(Self::Trimmed(x)),
                    _ => Err(PyValueError::new_err(format!(
                        "second string must be '{TEMPORAL_SCALE_DIAGNOSTIC_FORCED}' \
                         or '{TEMPORAL_SCALE_DIAGNOSTIC_TRIMMED}'"
                    ))),
                }
            } else {
                Ok(Self::None)
            }
        }
    }

    impl<'py> IntoPyObject<'py> for ScaleFix {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            let ret = match self {
                Self::None => None,
                Self::LogFixed(x) => Some((x, SCALE_DIAGNOSTIC_LOG)),
                Self::Trimmed(x) => Some((x, SCALE_DIAGNOSTIC_TRIMMED)),
                Self::TrimmedLogFixed(x) => Some((x, SCALE_DIAGNOSTIC_TRIMMED_LOG)),
            };
            ret.into_bound_py_any(py)
        }
    }

    impl<'py> IntoPyObject<'py> for OpticalScaleFix {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            let ret = match self {
                Self::Forced(x) => Some((x, SCALE_DIAGNOSTIC_FORCED)),
                Self::Inner(ScaleFix::None) => None,
                Self::Inner(ScaleFix::LogFixed(x)) => Some((x, SCALE_DIAGNOSTIC_LOG)),
                Self::Inner(ScaleFix::Trimmed(x)) => Some((x, SCALE_DIAGNOSTIC_TRIMMED)),
                Self::Inner(ScaleFix::TrimmedLogFixed(x)) => {
                    Some((x, SCALE_DIAGNOSTIC_TRIMMED_LOG))
                }
            };
            ret.into_bound_py_any(py)
        }
    }

    impl<'py> IntoPyObject<'py> for TemporalScaleFix {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            let ret = match self {
                Self::None => None,
                Self::Forced(x) => Some((x, TEMPORAL_SCALE_DIAGNOSTIC_FORCED)),
                Self::Trimmed(x) => Some((x, TEMPORAL_SCALE_DIAGNOSTIC_TRIMMED)),
            };
            ret.into_bound_py_any(py)
        }
    }
}
