use crate::config::{
    ConfigFlag as _, DummyTriFlag, EvaledReadDataKeywordsConfig, EvaledReadStdKeywordsConfig,
    ProcessOptionalFailure, ReadHeaderAndTEXTConfig, TriErrorFlag as _, TrimIntraValueWhitespace,
};
use crate::logging::{
    DeferredError, DeferredSwitchableErrors, LogResult, ResultExt as _, WarningAndErrorResult,
};
use crate::macros::impl_newtype_try_from;
use crate::segment::read::{IsOffsetPair as _, PrimaryTextOffsets};
use crate::text::byteord::{
    ArrayByteOrd, BitsOrChars, Endian, NewByteOrdError, NoByteOrd, PrivBytes,
};
use crate::text::datetimes::{BeginDateTime, EndDateTime};
use crate::text::index::{GateIndex, IndexFromOne, MeasIndex, RegionIndex};
use crate::text::keyword_enum::AsStdKeywordPair as _;
use crate::text::lookup::{
    Diagnosed, FromStrDelim, FromStrWith, FromStrWithResult, OptIndexedKey, OptIndexedKeyError,
    OptMetarootKey, Optional, ParseKeyError, ReqIndexedKey, ReqKeyError, ReqKeyErrorInner,
    ReqMetarootKey, Required, Trimmed, impl_from_str_with_delim,
};
use crate::text::named_vec::{NameMapping, NamedSet, NamedSetMembership};
use crate::text::optional::OptionalZST;
use crate::text::ranged_float::{NonNegFloat, PositiveFloat, RangedFloatError};
use crate::text::relational::{
    BiIndexedKeyToIndexLinkError, ExistingNamedLinkError, KeyToIndexLinkError, KeyToNameLinkError,
    LinkName, OpticalNamedLinkError, OpticalNamesToRemove, RemovedIndexLink, RemovedNamedLink,
    TemporalNamedLinkError,
};
use crate::text::spillover::Spillover;
use crate::text::timestamps::{Btim, Etim, FCSDate, FCSTime, FCSTime60, FCSTime100, Xtim};
use crate::validated::ascii_range::AsciiRangeValue;
use crate::validated::ascii_uint::UintZeroPad20;
use crate::validated::bitmask::BitmaskValue;
use crate::validated::compensation::{Compensation, NewCompError};
use crate::validated::finite_float::{DecimalToFloatError, FiniteFloat};
use crate::validated::keys::{
    AsStdKey as _, BiIndex, BiIndexedKey, DKey0, DKey2, DollarKey, IndexedKey, Key1, Key2,
    NonStdKeywordsExt as _, PrefixSuffix, SpecificKey, StdKey, StdKeywords, StdOptKeyword,
    TruncatedNEString, ValidKeywords, VersionedKey,
};
use crate::validated::read_state::{FileLen, HeaderReadState, TEXTReadState};
use crate::validated::shortname::Shortname;
use crate::validated::textdelim::{DelimCollisionError, HasDelim, TEXTDelim};
use crate::validated::unaligned::{U24, U40, U48, U56};

use type_families::{BifunctorOnce, impl_functor, impl_kind1};

use fireflow_types::config::{ForceLinearScale, TemporalOpticalKey};
use fireflow_types::keywords::{
    self as tk, MeasKeywordClass, OpticalFeature, OpticalFeatureError, RootKeywordClass, Version,
    VersionMembership,
};
use fireflow_types::nonempty_string::{
    DisplayableNE as _, NEAlt, NEConcat, NEConcat3, NEConcat5, NEDelim, NESliceExt as _, NEStr,
    NEString, ToDisplayNE, ToNE, ambassador_impl_ToDisplayNE,
};
use fireflow_types::{impl_str_enum, impl_str_enum_kw, ne_str};

use ambassador::Delegate;
use bigdecimal::{BigDecimal, ParseBigDecimalError, Signed as _};
use chrono::{NaiveDateTime, NaiveTime, Timelike as _};
use derive_more::{Add, AsMut, AsRef, Display, From, FromStr, Into, Sub};
use derive_new::new;
use hashbrown::HashMap;
use itertools::Itertools as _;
use ndarray::Array2;
use nonempty_collections::{
    IntoIteratorExt as _, IntoNonEmptyIterator as _, NEMap, NESlice, NEVec, NonEmptyArrayExt as _,
    NonEmptyIterator as _, iter::once,
};
use num_traits::{Bounded, One as _, ToPrimitive as _, Zero as _};
use thiserror::Error;
use unicase::Ascii;

use std::collections::HashSet;
use std::mem::take;
use std::num::{NonZeroU8, NonZeroUsize, ParseFloatError, ParseIntError};
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

use super::keyword_enum::{OptRootKeyword, SplitKeyword, SplitKeyword2};
use super::relational::{
    Comp2_0Missing, ExistingIndexedLinkError, RemovedComp2_0Cell, RemovedLink,
};

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{
        AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject, FromPyString, IntoPyNEString,
    },
    fireflow_types::python as py,
    pyo3::prelude::*,
};

#[cfg(test)]
use proptest_derive::Arbitrary;

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
    pub(crate) fn lookup_ro<C>(
        kws: &StdKeywords,
        primary_text: &PrimaryTextOffsets,
        st: HeaderReadState<C>,
    ) -> WarningAndErrorResult<
        (Option<Self>, TEXTReadState<C>),
        (),
        ReadNextdataError,
        LookupNextdataError,
    >
    where
        C: AsRef<ReadHeaderAndTEXTConfig>,
    {
        Self::lookup_ro_inner(kws, st.conf().as_ref())
            .map_errors(LookupNextdataError::from)
            .and_then_nowarn_commutative(|nextdata| {
                // If $NEXTDATA exists (almost all the time) validate that it is
                // a) less than the length of the FCS file from which it was
                // read and b) beyond the end of the TEXT segment from which it
                // was read.
                let res = if let Some(nd) = nextdata {
                    let n = u64::from(nd);
                    let f = st.file_len();
                    if n == 0 {
                        Ok(st.into_last_dataset())
                    } else if let Some(ptext_end) = primary_text.as_nonempty().map(|t| t.end())
                        // TODO this should always be some since we know that
                        // the TEXT segment is non-empty (otherwise how did we
                        // get $NEXTDATA?)
                        && n < ptext_end
                    {
                        let e = NextdataInPrimaryError(nd, ptext_end);
                        Err(LookupNextdataError::PrimaryTEXT(e))
                    } else if n >= u64::from(f) {
                        let e = NextdataEOFError(nd, f);
                        Err(LookupNextdataError::FileLength(e))
                    } else {
                        Ok(st.with_nextdata(nd))
                    }
                } else {
                    Ok(st.into_last_dataset())
                };
                res.map(|txt_st| (nextdata, txt_st)).into_nowarn1()
            })
    }

    pub(crate) fn lookup_ro_inner(
        kws: &StdKeywords,
        conf: &ReadHeaderAndTEXTConfig,
    ) -> WarningAndErrorResult<Option<Self>, (), ReadNextdataError, ReadNextdataError> {
        let k = SpecificKey::default();
        if let Some(is_err) = conf.allow_missing_nextdata.is_error() {
            let res = Self::get_req_with(kws, k, (), conf).map(|x| Some(x.inner));
            if is_err {
                res.into_log()
            } else {
                LogResult::Succ(res.into_succ())
            }
        } else {
            let ret = kws
                .get(&k.as_std_key())
                .and_then(|v| Self::from_str_with(v.as_ne_str(), (), conf).ok())
                .map(|x| x.inner);
            LogResult::new_ok(ret)
        }
    }
}

impl FromStrWith for Nextdata {
    type Err = ParseNextdataError;
    type Payload<'a> = ();
    type Diagnostic = ();
    type Config = ReadHeaderAndTEXTConfig;

    fn from_str_with(s: &NEStr, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
        let corr = i128::from(conf.nextdata_correction);
        let x = s.parse::<i128>()?;
        let y = x.saturating_add(corr);
        if y < 0 {
            Err(ParseNextdataError::from(NegativeNextdataError(x)))
        } else {
            let out = u64::try_from(y).unwrap_or(u64::MAX);
            Ok(Diagnosed::new1(Self(UintZeroPad20(out))))
        }
    }
}

/// Error when parsing or validating [`Nextdata`].
#[derive(Debug, Display, From, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupNextdataError {
    Parse(ReadNextdataError),
    FileLength(NextdataEOFError),
    PrimaryTEXT(NextdataInPrimaryError),
}

pub type ReadNextdataError = ReqKeyErrorInner<ParseNextdataError, Nextdata, ()>;

/// Error when parsing [`Nextdata`] from [`String`]
#[derive(Debug, Display, From, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ParseNextdataError {
    Int(ParseIntError),
    Negative(NegativeNextdataError),
}

/// Error when $NEXTDATA exceeds EOF.
#[derive(Debug, Error, PartialEq, Clone)]
#[error(
    "$NEXTDATA ({}) exceeds file length ({})",
    self.0.as_displayable(),
    self.1
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NextdataEOFError(Nextdata, FileLen);

/// Error when $NEXTDATA exceeds EOF.
#[derive(Debug, Error, PartialEq, Clone)]
#[error(
    "$NEXTDATA ({}) occurs before primary TEXT ends (offset = {})",
    self.0.as_displayable(),
    self.1
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NextdataInPrimaryError(Nextdata, u64);

/// Error when $NEXTDATA is negative
#[derive(Debug, Error, PartialEq, Clone)]
#[error("$NEXTDATA value is negative ({0})")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct NegativeNextdataError(i128);

/// The value for the $PnE key (all versions).
///
/// Format is assumed to be 'f1,f2'
#[derive(Clone, Copy, PartialEq, Debug, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(test, derive(Arbitrary))]
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

/// Fixes that were required in order to make $PnE parsable for optical channel.
#[derive(Clone, PartialEq, From, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum OpticalScaleFix {
    /// $PnE was non-linear and needed to be linear in order to be standardized.
    Forced(NEString),
    /// Fixes shared with $Gn* keywords
    Inner(ScaleFix),
}

impl Default for OpticalScaleFix {
    fn default() -> Self {
        Self::Inner(ScaleFix::default())
    }
}

/// Diagnostic data from parsing $PnE or $GmE
#[derive(Default, Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum ScaleFix {
    /// Nothing happened
    #[default]
    None,
    /// Whitespace was trimmed
    Trimmed(NEString),
    /// Zero log offset was corrected
    LogFixed(NEString),
    /// Trimmed and zero log offset was corrected
    TrimmedLogFixed(NEString),
}

#[derive(Clone, Copy, PartialEq, Debug, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(test, derive(Arbitrary))]
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
        s: &NEStr,
        conf: &EvaledReadStdKeywordsConfig,
    ) -> Result<Diagnosed<Self, ScaleFix>, ScaleError> {
        let (res, trimmed) = Self::from_str_delim(s, conf.trim_intra_value_whitespace);
        let go = |x, t: Trimmed| {
            let d = t.map(ScaleFix::Trimmed).unwrap_or_default();
            Diagnosed::new(x, d)
        };
        if conf.fix_log_scale_offsets.is_set() {
            match res {
                Ok(x) => Ok(go(x, trimmed)),
                Err(e) => {
                    if let ScaleError::LogRange(le) = e {
                        le.try_fix_offset()
                            .map(Self::Log)
                            .map(|x| {
                                let d = trimmed.map_or(
                                    ScaleFix::LogFixed(s.to_owned()),
                                    ScaleFix::TrimmedLogFixed,
                                );
                                Diagnosed::new(x, d)
                            })
                            .map_err(ScaleError::LogRange)
                    } else {
                        Err(e)
                    }
                }
            }
        } else {
            res.map(|x| go(x, trimmed))
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
    type Config = EvaledReadStdKeywordsConfig;

    fn from_str_with(s: &NEStr, dt: AlphaNumType, conf: &Self::Config) -> FromStrWithResult<Self> {
        let can_force = (matches!(conf.force_linear_scale, ForceLinearScale::AllNonInt)
            && !matches!(dt, AlphaNumType::Integer))
            || matches!(conf.force_linear_scale, ForceLinearScale::All);
        let do_force = || {
            let d = OpticalScaleFix::Forced(s.to_owned());
            Diagnosed::new(Self::Linear, d)
        };

        match Self::parse_fix_maybe(s, conf).map(BifunctorOnce::second_into_once) {
            Ok(diag) => {
                let ret = if diag.inner != Self::Linear && can_force {
                    do_force()
                } else {
                    diag
                };
                Ok(ret)
            }
            Err(e) => {
                if can_force {
                    Ok(do_force())
                } else {
                    Err(e)
                }
            }
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
#[derive(Debug, Error, PartialEq, Clone)]
pub enum ScaleError {
    #[error("{0}")]
    FloatError(ParseFloatError),
    #[error("{0}")]
    LogRange(LogRangeError),
    #[error("must be like 'f1,f2'")]
    WrongFormat,
}

/// Error when parsing [`Scale`] as log from string
#[derive(Debug, Error, new, PartialEq, Clone, Copy)]
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
#[cfg_attr(test, derive(Arbitrary))]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct Gain(pub PositiveFloat);

impl Gain {
    pub(crate) fn lookup_temporal_3_0<C>(
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> DeferredSwitchableErrors<Option<Self>, DummyTriFlag, LookupTemporalGainError>
    where
        C: AsRef<EvaledReadDataKeywordsConfig> + AsRef<EvaledReadStdKeywordsConfig>,
    {
        let ignore = &AsRef::<EvaledReadStdKeywordsConfig>::as_ref(conf).ignore_time_optical_keys;
        let drop_flag = AsRef::<EvaledReadDataKeywordsConfig>::as_ref(conf)
            .process_optional_failure
            .as_triflag();
        if ignore.0.contains(&TemporalOpticalKey::Gain) {
            kws.transfer_demoted(Self::std(i));
            LogResult::new_switchable_ok(None, drop_flag)
        } else {
            Self::remove_or_drop_meas_opt(kws, dropped, i, conf.as_ref())
                .map_switchable_errors(LookupTemporalGainError::from)
                .into_semigroup()
                .eval_deferred_switchable_error3(|gain| {
                    (!gain.is_none_or(|g| g.0.is_one())).then_some(TemporalGainError(i).into())
                })
        }
    }
}

/// Error when lookup up [`Gain`] from keyword pairs
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupTemporalGainError {
    Parse(OptIndexedKeyError<Gain>),
    HasGain(TemporalGainError),
}

/// Error when time measurement has [`Gain`] ($PnG)
#[derive(Debug, Error, PartialEq, Clone)]
#[error("{} must be 1.0 or not set for temporal measurement", Gain::std(self.0))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct TemporalGainError(MeasIndex);

/// The value of the $TIMESTEP keyword
#[derive(Clone, Copy, PartialEq, From, FromStr, Into, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[cfg_attr(test, derive(Arbitrary))]
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
    pub(crate) fn lookup(
        std: &mut StdKeywords,
        conf: &EvaledReadStdKeywordsConfig,
    ) -> Result<Diagnosed<Self, TimestepAdded>, ReqKeyError<Self>> {
        match Self::remove_metaroot_req(std) {
            Ok(x) => Ok(Diagnosed::new(x, false)),
            Err(e) => conf
                .add_missing_timestep
                .map_or(Err(e), |x| Ok(Diagnosed::new(x, true))),
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
            .then(|| ExistingNamedLinkError::new(DKey0::default(), NEVec::new(m.clone())))
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
#[derive(Debug, Error, PartialEq, Eq, Clone)]
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
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error("can only be 'L'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
pub struct Mode3_2Error;

/// Error when converting [`Mode`] to [`Mode3_2`]
#[derive(Debug, Error, PartialEq, Clone)]
#[error("$MODE must be 'L'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct ModeUpgradeError;

/// The value for the $PnD key (3.1+)
#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(test, derive(Arbitrary))]
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
#[derive(Debug, Error, PartialEq, Clone)]
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
#[derive(Clone, Copy, From, Debug, Delegate, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub enum ByteOrd2_0 {
    O1(ArrayByteOrd<1>),
    O2(ArrayByteOrd<2>),
    O3(ArrayByteOrd<3>),
    O4(ArrayByteOrd<4>),
    O5(ArrayByteOrd<5>),
    O6(ArrayByteOrd<6>),
    O7(ArrayByteOrd<7>),
    O8(ArrayByteOrd<8>),
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
#[derive(From, Debug, Display, Error, PartialEq, Eq, Copy, Clone)]
pub enum ParseByteOrdError {
    Order(NewByteOrdError),
    Digit(ByteordDigitError),
}

/// Error when [`ByteOrd2_0`] has invalid digit(s)
#[derive(Debug, Error, PartialEq, Eq, Clone, Copy)]
#[error("could not parse digits from byte order")]
pub struct ByteordDigitError;

impl Default for ByteOrd2_0 {
    fn default() -> Self {
        // Default $BYTEORD for FCS 2.0 is simply 32-bit little endian
        Self::O4(ArrayByteOrd::default())
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
            Self::O1(_) => PrivBytes::B1,
            Self::O2(_) => PrivBytes::B2,
            Self::O3(_) => PrivBytes::B3,
            Self::O4(_) => PrivBytes::B4,
            Self::O5(_) => PrivBytes::B5,
            Self::O6(_) => PrivBytes::B6,
            Self::O7(_) => PrivBytes::B7,
            Self::O8(_) => PrivBytes::B8,
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

    pub(crate) fn from_endian(endian: Endian, bytes: PrivBytes) -> Self {
        match bytes {
            PrivBytes::B1 => Self::O1(endian.into()),
            PrivBytes::B2 => Self::O2(endian.into()),
            PrivBytes::B3 => Self::O3(endian.into()),
            PrivBytes::B4 => Self::O4(endian.into()),
            PrivBytes::B5 => Self::O5(endian.into()),
            PrivBytes::B6 => Self::O6(endian.into()),
            PrivBytes::B7 => Self::O7(endian.into()),
            PrivBytes::B8 => Self::O8(endian.into()),
        }
    }

    fn is_endian(&self) -> bool {
        Endian::try_from(*self).is_ok()
    }
}

/// The $BYTEORD field in FCS 3.1 and 3.2
#[derive(Clone, Copy, From, FromStr, Default, Debug, Delegate, PartialEq, Eq)]
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
#[cfg_attr(test, derive(Arbitrary))]
pub struct TemporalScaleInner;

impl ToDisplayNE<'_> for TemporalScaleInner {
    type NE = &'static NEStr;
    fn to_ne(&self) -> Self::NE {
        ne_str!("0,0")
    }
}

/// Fixes that were required in order to make $PnE parsable for temporal channel.
#[derive(Default, Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum TemporalScaleFix {
    /// $PnE had no problems
    #[default]
    None,
    /// $PnE was not linear and needed to be forced as linear to be parsed
    Forced(NEString),
    /// $PnE needed to be trimmed to be parsed
    Trimmed(NEString),
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
            && let (Ok(x), Ok(y)) = (y0.parse::<f32>(), y1.parse::<f32>())
        {
            if x.is_zero() && y.is_zero() {
                Ok(Self)
            } else {
                Err(TemporalScaleError::NonLinear)
            }
        } else {
            Err(TemporalScaleError::Format)
        }
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
    type Config = EvaledReadStdKeywordsConfig;

    fn from_str_with(s: &NEStr, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
        let flag = conf.trim_intra_value_whitespace;
        let (res, trimmed) = TemporalScaleInner::from_str_delim(s, flag);
        match res {
            Ok(x) => {
                let d = trimmed.map(TemporalScaleFix::Trimmed).unwrap_or_default();
                Ok(Diagnosed::new(Self(x), d))
            }
            Err(e) => {
                if conf.force_linear_scale.time_selected() {
                    let d = TemporalScaleFix::Forced(s.to_owned());
                    Ok(Diagnosed::new(Self(TemporalScaleInner), d))
                } else {
                    Err(e)
                }
            }
        }
    }
}

/// Error when parsing [`TemporalScaleInner`] from string
#[derive(Debug, Error, PartialEq, Eq, Clone, Copy)]
pub enum TemporalScaleError {
    #[error("time measurement must have linear scaling")]
    NonLinear,
    #[error("invalid format")]
    Format,
}

/// The value for the $PnCALIBRATION key (3.1 only)
///
/// This should be formatted like "`<value>,<unit>`"
#[derive(Clone, PartialEq, Debug, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(test, derive(Arbitrary))]
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
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error("must be like 'slope,unit'")]
pub struct CalibrationFormat3_1;

/// Error when calibration type has an empty unit string.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[error("unit cannot be an empty string")]
pub struct EmptyCalibrationUnitError;

#[derive(Debug, Display, Error, PartialEq, Clone)]
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
#[cfg_attr(test, derive(Arbitrary))]
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
#[derive(Debug, Error, PartialEq, Eq, Copy, Clone)]
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
#[derive(Debug, Error, PartialEq, Clone)]
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
#[cfg_attr(test, derive(Arbitrary))]
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
#[derive(Debug, Error, PartialEq, Clone)]
#[error(
    "{0} is {1} elements long and will \
     be reduced to first upon conversion"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConversionError))]
pub struct WavelengthsLossError(Key1<Wavelengths>, NonZeroUsize);

/// Error when parsing [`Wavelengths`] from string
#[derive(Debug, Error, PartialEq, Clone)]
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
    type Diagnostic = Option<String>;
    type Config = EvaledReadStdKeywordsConfig;

    fn from_str_with(s: &NEStr, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
        if let Some(pat) = conf.last_modified_pattern.as_ref() {
            return NaiveDateTime::parse_from_str(s.as_ref(), pat.as_str())
                .map(Self)
                .map(|x| Diagnosed::new(x, Some(pat.clone())))
                .map_err(|_| LastModifiedError::AltFormat(pat.to_owned()));
        }
        let mut it = s.as_ref().split('.');
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
            .map(|x| Diagnosed::new(x, None))
    }
}

/// Error when parsing [`LastModified`] from string
#[derive(Debug, Error, PartialEq, Eq, Clone)]
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

/// The aggregated values of the $DFCiTOj keywords (2.0 only)
#[derive(Clone, From, Into, AsRef, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[as_ref(Array2<f32>, Compensation)]
pub struct Compensation2_0(pub Compensation);

impl<'a> ToDisplayNE<'a> for Compensation {
    type NE = NEConcat3<NonZeroUsize, char, NEDelim<NEVec<f32>>>;
    fn to_ne(&'a self) -> Self::NE {
        let xs = self.row_major_ne_vec();
        NEConcat::new(self.dim(), ',').append(NEDelim::new(',', xs))
    }
}

/// The value of one $DFCmTOn keyword.
#[derive(Clone)]
pub struct DfcKeyword {
    pub(crate) row: MeasIndex,
    pub(crate) col: MeasIndex,
    pub(crate) value: Dfc,
}

impl Compensation2_0 {
    pub(crate) fn lookup(
        kws: &mut ValidKeywords,
        dropped: &mut StdKeywords,
        par: Par,
        conf: &EvaledReadDataKeywordsConfig,
    ) -> DeferredSwitchableErrors<Option<Self>, ProcessOptionalFailure, LookupComp2_0Error> {
        // column = src measurement
        // row = target measurement
        // These are "flipped" in 2.0, where "column" goes TO the "row"
        let n = par.0;
        let flag = conf.process_optional_failure;
        let (xs, warnings): (Vec<_>, Vec<_>) = (0..n)
            .cartesian_product(0..n)
            .map(|(r, c)| {
                let k = SpecificKey::new_i2(c, r);
                match Dfc::lookup(kws, k, dropped, flag) {
                    Ok(x) => (x, None),
                    Err(w) => (None, Some(LookupComp2_0Error::Dfc(w))),
                }
            })
            .unzip();
        let res = if xs.iter().all(Option::is_none) || xs.is_empty() {
            LogResult::new_switchable_ok(None, flag)
        } else {
            let ys = xs
                .into_iter()
                .map(Option::unwrap_or_default)
                .map(f32::from)
                .collect();
            let matrix = Array2::from_shape_vec((n, n), ys).expect("shape is checked above");
            Compensation::try_from(matrix)
                .map(|x| Some(Self(x)))
                .map_err(|(e, m)| {
                    // Return non-zero keywords to non-standard list on failure
                    // if desired
                    let failed_kws = m
                        .iter()
                        .enumerate()
                        .filter(|(_, value)| !value.is_zero())
                        .map(|(i, &value)| {
                            let ncols = m.ncols();
                            let row = i / ncols;
                            let col = i % ncols;
                            let k = DKey2::new_i2(col, row);
                            SplitKeyword2::new(k, Dfc(value))
                        });
                    match flag.is_demote_or_drop() {
                        Some(true) => {
                            for k in failed_kws {
                                let sk = StdOptKeyword::from(OptRootKeyword::Dfc(k));
                                kws.nonstd.insert_demoted_keyword(sk);
                            }
                        }
                        Some(false) => {
                            for k in failed_kws {
                                k.insert_unique(dropped);
                            }
                        }
                        None => (),
                    }
                    LookupComp2_0Error::Matrix(e)
                })
                .into_deferred_switchable(flag)
        };
        res.extend_deferred_switchable_errors(warnings.into_iter().flatten())
    }

    // TODO this awkward, if all the entries are zero then we will be saving
    // lots of zeros as keywords. The best way to handle this in order to keep
    // read/write operations isomorphic is to ignore comp matrices (and
    // spillover matrices) that are entirely zero since they clearly are
    // meaningless.
    pub fn non_zero_indices(&self) -> impl Iterator<Item = DfcKeyword> {
        let m = self.0.matrix();
        m.iter().enumerate().map(|(i, &value)| {
            let n = m.ncols();
            let row = i / n;
            let col = i % n;
            DfcKeyword {
                col: col.into(),
                row: row.into(),
                value: Dfc(value),
            }
        })
    }

    pub(crate) fn invalid_link_errors(
        &self,
        par: &Par,
    ) -> impl Iterator<Item = BiIndexedKeyToIndexLinkError<Dfc>> {
        // If $PAR is 1 or matrix is smaller than $PAR, use a cutoff of zero
        // since the entire matrix must be removed.
        self.non_zero_indices().filter_map(|kw| {
            // TODO throw error if temporal measurement is anything other than ID
            let n = self.0.matrix().nrows();
            let bad_matrix = n < par.0 || par.0 < 2;
            let cutoff = if bad_matrix { 0 } else { par.0 };
            let k = DKey2::new_i2(kw.col, kw.row);
            let r = (usize::from(kw.row) >= cutoff).then_some(kw.row);
            let c = (usize::from(kw.col) >= cutoff).then_some(kw.col);
            [r, c]
                .into_iter()
                .flatten()
                .try_into_nonempty_iter()
                .map(|js| BiIndexedKeyToIndexLinkError::new(js.collect(), k))
        })
    }

    // NOTE this shouldn't do anything for a freshly made comp matrix since
    // the DFCmTOn lookups are bound by $PAR, so it impossible for the matrix
    // to be greater than $PAR. This will fire whenever we assign an external
    // matrix to the Core data struct.
    pub(crate) fn remove_invalid_link(src: &mut Option<Self>, par: Par) -> Option<RemovedLink> {
        // TODO throw error if temporal measurement is anything other than ID
        let c = src.as_mut()?;
        let n = c.0.matrix().nrows();
        // If $PAR is 1 or matrix is smaller than $PAR, use a cutoff of zero
        // since the entire matrix must be removed.
        let cutoff = if n < par.0 || par.0 < 2 { 0 } else { par.0 };
        // Scan through matrix and pull out all cells in rows/columns greater
        // or equal to cutoff and whose value is not zero. These are the keywords
        // to return.
        let es = c.non_zero_indices().filter_map(|kw| {
            let which = match (usize::from(kw.row) >= cutoff, usize::from(kw.col) >= cutoff) {
                (true, true) => Some(Comp2_0Missing::Both),
                (true, false) => Some(Comp2_0Missing::Row),
                (false, true) => Some(Comp2_0Missing::Col),
                (false, false) => None,
            };
            let k = DollarKey::new_i2(kw.row, kw.col);
            which.map(|b| RemovedComp2_0Cell::new(SplitKeyword::new(k, kw.value), b))
        });
        let ret = es
            .try_into_nonempty_iter()
            .map(|js| RemovedLink::Comp2_0(js.collect()));
        // Truncate the matrix down to $PAR.
        *src = c.0.square_view(par.0).map(Self);
        ret
    }

    pub(crate) fn existing_links(
        &self,
    ) -> impl Iterator<Item = ExistingIndexedLinkError<Dfc, BiIndex>> {
        self.non_zero_indices().map(|kw| {
            let xs = [kw.col.into(), kw.row.into()].into_nonempty_vec();
            ExistingIndexedLinkError::new(DKey2::new_i2(kw.col, kw.row), xs)
        })
    }

    // pub(crate) fn loss_errors(&self) -> impl Iterator<Item = Key2LossError<Dfc>> {
    //     self.non_zero_indices()
    //         .map(|kw| KeyLossError(DKey2::new_i2(kw.col, kw.row)))
    // }
}

/// Error when parsing $DFCiTOj keywords for compensation matrix (2.0)
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum LookupComp2_0Error {
    Dfc(LookupDfcError),
    Matrix(NewCompError),
}

/// The value of the $COMP keyword (3.0 only)
#[derive(Clone, From, Into, AsRef, PartialEq, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[as_ref(Array2<f32>, Compensation)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct Compensation3_0(pub Compensation);

impl FromStrWith for Compensation3_0 {
    type Err = ParseCompError;
    type Payload<'a> = ();
    type Diagnostic = Trimmed;
    type Config = EvaledReadStdKeywordsConfig;

    fn from_str_with(s: &NEStr, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
        Self::from_str_delim_diagnosed(s, conf.trim_intra_value_whitespace)
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
                let matrix =
                    Array2::from_shape_vec((n, n), values).expect("shape is checked above");
                Ok(Compensation::try_from(matrix)
                    .map(Self)
                    .map_err(|(e, _)| e)?)
            } else {
                Err(ParseCompError::WrongLength {
                    expected: nn,
                    found: total,
                })
            }
        } else {
            Err(ParseCompError::BadLength)
        }
    }
}

impl Compensation3_0 {
    pub(crate) fn invalid_link_errors(&self, par: Par) -> Option<KeyToIndexLinkError<Self>> {
        let m: &Array2<_> = self.as_ref();
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
            let m: &Array2<_> = c.as_ref();
            (par.0..m.nrows()).map(MeasIndex::from)
        };
        RemovedIndexLink::remove_invalid_link(src, go)
    }
}

/// Error when parsing [`Compensation3_0`] from string
#[derive(Debug, Error, PartialEq, Eq, Copy, Clone)]
pub enum ParseCompError {
    #[error("Expected {expected} entries, found {found}")]
    WrongLength { found: usize, expected: usize },
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
#[derive(Debug, Error, PartialEq, Eq, Clone)]
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
#[cfg_attr(test, derive(Arbitrary))]
#[as_ref(str)]
pub struct OpticalType(String);

/// Error when parsing [`OpticalType`] from string
#[derive(Debug, Error, PartialEq, Clone)]
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
#[cfg_attr(test, derive(Arbitrary))]
pub struct TemporalTypeInner;

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
#[derive(Debug, Error, PartialEq, Eq, Clone, Copy)]
#[error("$PnTYPE for time measurement shall be 'Time' if given")]
pub struct TemporalTypeError;

/// The value of the $PnFEATURE key (3.2+)
#[derive(Clone, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyNEString))]
#[cfg_attr(test, derive(Arbitrary))]
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
        let conf = EvaledReadStdKeywordsConfig {
            allow_other_feature: true.into(),
            ..EvaledReadStdKeywordsConfig::default()
        };
        // throw away diagnostic flag here since this is only for python
        // conversion
        if let Some(ne) = NEStr::try_new(s) {
            Ok(Self::from_str_with(ne, (), &conf).map(|x| x.inner)?)
        } else {
            Err(FeatureError::Other)
        }
    }
}

impl FromStrWith for Feature {
    type Err = OpticalFeatureError;
    type Payload<'a> = ();
    type Diagnostic = bool;
    type Config = EvaledReadStdKeywordsConfig;

    fn from_str_with(s: &NEStr, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
        match s.parse::<OpticalFeature>() {
            Ok(f) => Ok(Diagnosed::new(Self::Optical(f), false)),
            Err(e) => {
                if conf.allow_other_feature.is_set() {
                    let out = Self::Other(s.to_owned());
                    Ok(Diagnosed::new(out, true))
                } else {
                    Err(e)
                }
            }
        }
    }
}

/// Error when parsing [`Feature`]
#[derive(Debug, Error, From, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
pub enum FeatureError {
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

pub type RegionGateIndex2_0 = RegionGateIndex<GateIndex>;
pub type RegionGateIndex3_0 = RegionGateIndex<MeasOrGateIndex>;
pub type RegionGateIndex3_2 = RegionGateIndex<PrefixedMeasIndex>;

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
    type Config = EvaledReadStdKeywordsConfig;

    fn from_str_with(s: &NEStr, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
        Self::from_str_delim_diagnosed(s, conf.trim_intra_value_whitespace)
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
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum RegionGateIndexError<E> {
    #[error("{0}")]
    Int(E),
    #[error("must be either a single value 'x' or a pair 'x,y'")]
    Format,
}

/// Index which can either refer to a gate ($Gn*) or a measurement ($Pn*)
#[derive(Clone, Copy, From, PartialEq, Eq, Debug)]
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
#[derive(Debug, Error, PartialEq, Eq, Clone)]
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
#[derive(Debug, Error, PartialEq, Eq, Clone)]
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

impl FromStrWith for RegionWindow {
    type Err = RegionWindowError;
    type Payload<'a> = ();
    type Diagnostic = Trimmed;
    type Config = EvaledReadStdKeywordsConfig;

    fn from_str_with(
        s: &NEStr,
        (): Self::Payload<'_>,
        conf: &Self::Config,
    ) -> FromStrWithResult<Self> {
        let it = s.as_ref().split(';');
        let flag = conf.trim_intra_value_whitespace;
        if flag.is_set() {
            let mut was_trimmed = false;
            Self::from_iter_inner(
                s,
                it.map(|x| {
                    let y = str::trim(x);
                    was_trimmed = was_trimmed || y.len() < x.len();
                    y
                }),
                flag,
            )
            .map(|x| {
                let d = (x.diagnostic.is_some() || was_trimmed).then(|| s.to_owned());
                Diagnosed::new(x.inner, d)
            })
        } else {
            Self::from_iter_inner(s, it, false.into())
        }
    }
}

impl RegionWindow {
    fn from_iter_inner<'a>(
        original: &NEStr,
        ss: impl Iterator<Item = &'a str>,
        trim_whitespace: TrimIntraValueWhitespace,
    ) -> Result<Diagnosed<Self, Trimmed>, RegionWindowError> {
        let mut it = ss.peekable();
        if let Some(head) = it.next() {
            let ne_head = NEStr::try_new(head).ok_or(RegionWindowError::Format)?;
            if it.by_ref().peek().is_none() {
                let (res, trimmed) = UniGate::from_str_delim(ne_head, trim_whitespace);
                res.map(RegionWindow::Univariate)
                    .map(|x| Diagnosed::new(x, trimmed))
            } else {
                let mut was_trimmed = false;
                let ys = once(head)
                    .chain(it)
                    .map(|x| {
                        let ne = NEStr::try_new(x).ok_or(RegionWindowError::Format)?;
                        let (res, trimmed) = Vertex::from_str_delim(ne, trim_whitespace);
                        was_trimmed = was_trimmed || trimmed.is_some();
                        res
                    })
                    .collect::<Result<_, _>>()?;
                let d = was_trimmed.then(|| original.to_owned());
                Ok(Diagnosed::new(Self::Bivariate(ys), d))
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
#[derive(Debug, Error, PartialEq, Clone)]
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
        GatingToken::from_str(s)
    }
}

/// A parsed atom of the $GATING keyword
#[derive(Debug, Display, PartialEq, Eq, Clone, Copy)]
pub enum GatingToken {
    #[display(")")]
    RParen,
    #[display("(")]
    LParen,
    #[display("R{_0}")]
    Region(RegionIndex),
    #[display("AND")]
    And,
    #[display("OR")]
    Or,
    #[display("NOT")]
    Not,
}

impl GatingToken {
    fn from_str(s: &str) -> Result<Gating, GatingError> {
        if s.is_ascii() {
            match Self::tokenize_str(s) {
                Ok(ts) => Self::match_expression(&ts[..]),
                Err(bad) => Err(GatingError::BadToken(bad)),
            }
        } else {
            Err(GatingError::NonAscii)
        }
    }

    fn tokenize_str(s: &str) -> Result<Vec<Self>, String> {
        let mut acc = vec![];
        for x in s.split(['.', ' ']).filter(|x| !x.is_empty()) {
            for y in x.split('(') {
                if y.is_empty() {
                    acc.push(Self::LParen);
                } else {
                    for z in y.split(')') {
                        if z.is_empty() {
                            acc.push(Self::RParen);
                        } else {
                            match z {
                                "NOT" => acc.push(Self::Not),
                                "AND" => acc.push(Self::And),
                                "OR" => acc.push(Self::Or),
                                other => {
                                    if let ("R", rest) = z.split_at(1)
                                        && let Ok(r) = rest.parse()
                                    {
                                        acc.push(Self::Region(r));
                                    } else {
                                        return Err(other.to_owned());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(acc)
    }

    fn match_expression(tokens: &[Self]) -> Result<Gating, GatingError> {
        if let Some((t, ts)) = tokens.split_first() {
            match t {
                Self::LParen => {
                    if let Some(i) = ts.iter().rposition(|x| matches!(x, Self::RParen)) {
                        let inner = Self::match_expression(&ts[..i])?;
                        Self::extend_expression(inner, &ts[i + 1..])
                    } else {
                        Err(GatingError::MissingParen)
                    }
                }
                Self::Not => Ok(Gating::Not(Box::new(Self::match_expression(ts)?))),
                Self::Region(r) => Self::extend_expression(Gating::Region(*r), ts),
                e => Err(GatingError::InvalidExprToken(*e)),
            }
        } else {
            Err(GatingError::EmptyExpr)
        }
    }

    fn extend_expression(new: Gating, rest: &[Self]) -> Result<Gating, GatingError> {
        if let Some((t, ts)) = rest.split_first() {
            let is_and = match t {
                Self::And => true,
                Self::Or => false,
                e => return Err(GatingError::InvalidBinaryToken(*e)),
            };
            let right = Box::new(Self::match_expression(ts)?);
            let left = Box::new(new);
            if is_and {
                Ok(Gating::And(left, right))
            } else {
                Ok(Gating::Or(left, right))
            }
        } else {
            Ok(new)
        }
    }
}

/// Error when parsing [`Gating`] from string
#[derive(Debug, Error, PartialEq, Eq, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
pub enum GatingError {
    #[error("no more tokens to create expression")]
    EmptyExpr,
    #[error("expected 'AND' or 'OR', found {0}")]
    InvalidBinaryToken(GatingToken),
    #[error("expected 'NOT' or '(', or a region, found {0}")]
    InvalidExprToken(GatingToken),
    #[error("missing ')'")]
    MissingParen,
    #[error("gating contains invalid bytes")]
    NonAscii,
    #[error("invalid token found: {0}")]
    BadToken(String),
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
pub struct TextRange(pub BigDecimal);

macro_rules! impl_from_unaligned {
    ($t:ident) => {
        impl From<$t> for TextRange {
            fn from(value: $t) -> Self {
                u64::from(value).into()
            }
        }
    };
}

impl_from_unaligned!(U24);
impl_from_unaligned!(U40);
impl_from_unaligned!(U48);
impl_from_unaligned!(U56);

impl TextRange {
    pub(crate) fn into_uint<T>(self) -> DeferredError<BitmaskValue<T>, RangeToIntError<()>>
    where
        T: TryFrom<Self, Error = RangeToIntError<T>> + Bounded + Copy,
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
        T: TryFrom<Self, Error = RangeToIntError<T>> + Bounded + Copy,
    {
        let (b, err) = self.try_into().map_or_else(
            |e: RangeToIntError<T>| match e.error_kind {
                RangeToIntErrorKind::Overrange => (T::max_value(), Some(e.void())),
                RangeToIntErrorKind::Underrange => (T::min_value(), Some(e.void())),
                RangeToIntErrorKind::PrecisionLoss(y) => (y, Some(e.void())),
            },
            |x| (x, None),
        );
        LogResult::new_deferred_maybe(b, err)
    }

    pub(crate) fn into_float<T>(self) -> DeferredError<FiniteFloat<T>, DecimalToFloatError>
    where
        BigDecimal: TryInto<FiniteFloat<T>, Error = DecimalToFloatError>,
        FiniteFloat<T>: Bounded,
    {
        let (x, err) = self.0.try_into().map_or_else(
            |e| {
                let m = if e.over() {
                    FiniteFloat::<T>::max_value()
                } else {
                    FiniteFloat::<T>::min_value()
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
        impl TryFrom<TextRange> for $inttype {
            type Error = RangeToIntError<$inttype>;

            fn try_from(value: TextRange) -> Result<Self, Self::Error> {
                let x = &value.0;
                let err = |error_kind| RangeToIntError {
                    dest_type: PrivBytes::$ut,
                    src_value: x.clone(),
                    error_kind,
                };
                if let Some(y) = x.$to().and_then(|y| y.try_into().ok()) {
                    if x.fractional_digit_count() <= 0 {
                        Ok(y)
                    } else {
                        Err(err(RangeToIntErrorKind::PrecisionLoss(y)))
                    }
                } else {
                    if x.is_negative() {
                        Err(err(RangeToIntErrorKind::Underrange))
                    } else {
                        Err(err(RangeToIntErrorKind::Overrange))
                    }
                }
            }
        }
    };
}

try_from_range_int!(u8, to_u8, B1);
try_from_range_int!(u16, to_u16, B2);
try_from_range_int!(U24, to_u32, B3);
try_from_range_int!(u32, to_u32, B4);
try_from_range_int!(U40, to_u64, B5);
try_from_range_int!(U48, to_u64, B6);
try_from_range_int!(U56, to_u64, B7);
try_from_range_int!(u64, to_u64, B8);

/// Error when converting [`TextRange`] to integer.
///
/// This is a helper type to make more specific errors and not meant for
/// external use.
#[derive(Debug)]
pub struct RangeToIntError<T> {
    pub(crate) dest_type: PrivBytes,
    pub(crate) src_value: BigDecimal,
    pub(crate) error_kind: RangeToIntErrorKind<T>,
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

impl TryFrom<f32> for TextRange {
    type Error = ParseBigDecimalError;
    fn try_from(value: f32) -> Result<Self, Self::Error> {
        value.try_into().map(Self)
    }
}

impl TryFrom<f64> for TextRange {
    type Error = ParseBigDecimalError;
    fn try_from(value: f64) -> Result<Self, Self::Error> {
        value.try_into().map(Self)
    }
}

/// The value of the $GmN key
#[derive(Clone, From, FromStr, PartialEq, Debug, AsRef, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[as_ref(str, NEStr)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct GateShortname(pub Shortname);

/// The value of the $GmR key
#[derive(Clone, From, FromStr, PartialEq, Debug, Delegate)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[from(u64)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct GateRange(pub TextRange);

macro_rules! impl_non_neg_float {
    ($(#[$meta:meta])* $t:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, From, FromStr, Into, PartialEq, Debug, Delegate)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        #[cfg_attr(test, derive(Arbitrary))]
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
    type Config = EvaledReadStdKeywordsConfig;

    fn from_str_with(s: &NEStr, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
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
#[as_ref(str, NEStr)]
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
#[derive(Debug, Error, PartialEq, Clone)]
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
#[derive(Debug, Error, PartialEq, Eq, Copy, Clone)]
pub enum ParseUnstainedCenterError {
    #[error("Names are not unique")]
    NonUnique,
    #[error("Expected {expected} values, found {found}")]
    BadLength { found: usize, expected: usize },
    #[error("Could not parse N")]
    BadN,
    #[error("Error parsing float value(s)")]
    BadFloat,
}

impl UnstainedCenters {
    pub(crate) fn try_ne(&self) -> Option<NEUnstainedCenters> {
        // NOTE NEMap use std::HashMap internally, so this is actually
        // converting to a different hashmap type
        NEMap::try_from_map(self.0.clone().into_iter().collect()).map(NEUnstainedCenters)
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
            .map(|js| ExistingNamedLinkError::new(DKey0::default(), js.collect()))
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
                Err(ParseUnstainedCenterError::BadLength {
                    found: total,
                    expected,
                })
            }
        } else {
            Err(ParseUnstainedCenterError::BadN)
        }
    }
}

impl_from_str_with_delim!(UnstainedCenters, ParseUnstainedCenterError);

/// Leftover standard keyword after parsing
#[derive(Clone, new, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct ExtraStdKeywords {
    pub pseudostandard: StdKeywords,
    pub hyper_par: StdKeywords,
    pub hyper_gate: StdKeywords,
    pub other_version: StdKeywords,
    pub timestep: Option<NEString>,
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
#[derive(Debug, Error, PartialEq, Clone)]
#[error("pseudostandard keyword found: {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ExtraKeywordError))]
pub struct PseudostandardError(pub StdKey);

/// Error denoting that measurement keyword within standard but above $PAR was found
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error("measurement keyword is part of standard but outside $PAR ({par}): {key}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ExtraKeywordError))]
pub struct HyperParError {
    pub par: Par,
    pub key: StdKey,
}

/// Error denoting that gating keyword within standard but above $GATE was found
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error("gating keyword is part of standard but outside $GATE ({gate}): {key}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ExtraKeywordError))]
pub struct HyperGateError {
    pub gate: Gate,
    pub key: StdKey,
}

/// Error denoting that keyword from different version was found
#[derive(Debug, Error, new, PartialEq, Clone)]
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
#[derive(Debug, Error, PartialEq, Clone)]
#[error("$TIMESTEP found, this may indicate a time measurement exists but was not identified")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ExtraKeywordError))]
pub struct TimestepFoundError;

macro_rules! newtype_string {
    ($t:ident) => {
        #[derive(Clone, FromStr, From, Into, PartialEq, Debug, Default, AsRef)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
        #[cfg_attr(test, derive(Arbitrary))]
        #[as_ref(str)]
        pub struct $t(pub String);
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
        #[cfg_attr(test, derive(Arbitrary))]
        #[delegate(ToDisplayNE<'a>, generics = "'a")]
        pub struct $t(pub $type);
    };
}

macro_rules! newtype_opt_u32 {
    ($t:ident) => {
        #[derive(Clone, Copy, Default, PartialEq, Eq, FromStr, Debug, AsRef)]
        #[as_ref(u32)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        pub struct $t(pub u32);
    };
}

macro_rules! newtype_opt_bool {
    ($t:ident, $inner:ident) => {
        #[derive(Clone, Copy, PartialEq, Debug, Default, From, Into, AsRef)]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(test, derive(Arbitrary))]
        #[from(bool)]
        #[into(bool)]
        #[as_ref(Option<$inner>)]
        pub struct $t(pub OptionalZST<$inner>);
    };
}

macro_rules! impl_versioned_key {
    ($t:path, $m:expr) => {
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
        pub(crate) type $outer = $wrap<$inner>;

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
kw_req_meas!(TextRange, tk::RANGE_KW_SUFFIX, tk::PNR_VERS);
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
    type Config = EvaledReadStdKeywordsConfig;

    fn from_str_with(s: &NEStr, (): (), conf: &Self::Config) -> FromStrWithResult<Self> {
        let go = |x| Self(OptionalZST(Some(x)));
        let flag = conf.trim_intra_value_whitespace;
        let (res, trimmed) = TemporalScaleInner::from_str_delim(s, flag);
        match res {
            Ok(x) => {
                let d = trimmed.map(TemporalScaleFix::Trimmed).unwrap_or_default();
                Ok(Diagnosed::new(go(x), d))
            }
            Err(e) => {
                if conf.force_linear_scale.time_selected() {
                    let d = TemporalScaleFix::Forced(s.to_owned());
                    Ok(Diagnosed::new(go(TemporalScaleInner), d))
                } else {
                    Err(e)
                }
            }
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
#[derive(Clone, Copy, Debug, FromStr, Default, Into, Delegate, PartialEq)]
#[delegate(ToDisplayNE<'a>, generics = "'a")]
pub struct Dfc(pub f32);

impl_versioned_key!(Dfc, VersionMembership::One(Version::FCS2_0));

impl BiIndexedKey for Dfc {
    const PREFIX: &'static NEStr = ne_str!("DFC");
    const MIDDLE: &'static NEStr = ne_str!("TO");
}

impl Dfc {
    pub(crate) fn lookup(
        kws: &mut ValidKeywords,
        k: Key2<Self>,
        dropped: &mut StdKeywords,
        flag: ProcessOptionalFailure,
    ) -> Result<Option<Self>, LookupDfcError> {
        kws.std
            .remove(&k.as_std_key())
            .map_or(Ok(None), |v| {
                v.parse::<Self>()
                    .map_err(|e| ParseKeyError::new(e, k.into(), TruncatedNEString(v.clone())))
                    .map(Some)
            })
            .inspect_err(|e| match flag.is_demote_or_drop() {
                Some(true) => kws.nonstd.insert_demoted(k.as_std_key(), e.value.0.clone()),
                Some(false) => {
                    let out = dropped.insert(k.as_std_key(), e.value.0.clone());
                    assert!(out.is_none(), "key was already dropped, {}", k.as_std_key());
                }
                None => (),
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

impl VersionedKey for RegionWindow {
    const VERS: VersionMembership = VersionMembership::All;
}

impl IndexedKey for RegionWindow {
    const C: PrefixSuffix = PrefixSuffix::Both(REGION_KW_PREFIX, REGION_WINDOW_KW_SUFFIX);
}

opt_meas!(RegionWindow, Option<Self>);

const REGION_INDEX_PRE_SUF: PrefixSuffix =
    PrefixSuffix::Both(REGION_KW_PREFIX, REGION_INDEX_KW_SUFFIX);

macro_rules! impl_region_index {
    ($t:path, $m:expr) => {
        impl_versioned_key!($t, $m);
        impl crate::validated::keys::IndexedKey for $t {
            const C: PrefixSuffix = REGION_INDEX_PRE_SUF;
        }
        impl Optional for $t {
            type Outer = Option<Self>;
        }
        impl OptIndexedKey for $t {}
    };
}

impl_region_index!(RegionGateIndex2_0, VersionMembership::One(Version::FCS2_0));
impl_region_index!(
    RegionGateIndex3_0,
    VersionMembership::Two([Version::FCS3_0, Version::FCS3_1])
);
impl_region_index!(RegionGateIndex3_2, VersionMembership::One(Version::FCS3_2));

// dummy to help print stuff
impl_versioned_key!(RegionGateIndex<()>, VersionMembership::All);
impl IndexedKey for RegionGateIndex<()> {
    const C: PrefixSuffix = REGION_INDEX_PRE_SUF;
}

// offsets for all versions
kw_req_meta!(Nextdata, tk::NEXTDATA_KW, tk::NEXTDATA_VERS);
opt_meta!(Nextdata, Option<Self>);

// TODO this won't allow pseudoempty TEXT offsets like 0,-1 which might happen
// in real files and there is a config to fix if encountered
macro_rules! kw_offset {
    ($(#[$attr:meta])* $t:ident, $key:expr, $m:expr) => {
        $(#[$attr])*
        #[derive(From, Into, FromStr, Debug, Clone, Copy, Delegate, PartialEq)]
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

    /// The $PnB values are incompatible with this version.
    ///
    /// This will only be `true` if version is 2.0 and 3.0 and the $PnB values
    /// contain multiple widths across them.
    pub incompatible_widths: bool,
}

impl KeywordVersionScore {
    pub(crate) fn is_passing(&self, allow_drop: bool) -> bool {
        (self.missing_req == 0)
            && (self.drop == 0 || (self.drop > 0 && allow_drop))
            && !self.incompatible_widths
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

    /// Number of $DFCnTOm keywords found
    n_dfc: usize,

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

    /// Number of unique $PnB values seen.
    ///
    /// `None` means "not a valid width". which could mean it was "*", "0", a
    /// number larger than 64, or something else. All should be rare.
    ///
    /// Can be used to eliminate entire versions since 2.0 and 3.0 only allow
    /// one width.
    // TODO there may be one nasty edge case with this where the user decides to
    // override the widths in the config and the file is 2.0/3.0 and has
    // multiple widths (which presumably are wrong because why else would the
    // user override them?). In that case the file will be forced to 3.1 or 3.2
    // and then the width override will be applied. In other words, the override
    // and the version guessing logic don't talk. This case should be extremely
    // rare if not non-existent, since it isn't clear who would be making
    // 2.0/3.0 files with multiple widths (which in turn are incorrect) in the
    // first place.
    widths: HashSet<Width>,
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

        // $DFCmTOn were only in 2.0 and replaced in 3.0 with $COMP. Since only
        // one $DFCmTOn keywords is needed to make a comp matrix (or
        // equivalently, many $DFCmTOn map to one $COMP value) treat any
        // $DFCmTOn keywords as one keyword. Without this, 2.0 configurations
        // with $DFCmTOn keyword will likely win the optimization simply because
        // these usually come in large collections which will skew the scores.
        if version == Version::FCS2_0 {
            score.good_opt += usize::from(self.n_dfc > 0);
        } else {
            score.drop += usize::from(self.n_dfc > 0);
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

        // $PnB are incompatible if there are 2 or more widths and version is
        // 2.0 or 3.0.
        score.incompatible_widths = version < Version::FCS3_1 && self.widths.len() > 1;

        score
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn classify_keyword(&mut self, key: &StdKey, value: &NEStr, par: Par) {
        let p = par.0;
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
                        .map_or(ModeValue::Missing, |m| match m {
                            Mode::List => ModeValue::List,
                            _ => ModeValue::Other,
                        });
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
            AnyKeywordClass::Meas(i, r) => {
                if usize::from(i) < p {
                    match r {
                        MeasKeywordClass::OptGE3_0 => {
                            self.n_opt_min3_0 += 1;
                        }
                        MeasKeywordClass::OptGE3_1 => {
                            self.n_opt_min3_1 += 1;
                        }
                        MeasKeywordClass::OptGE3_2 => {
                            self.n_opt_min3_2 += 1;
                        }
                        MeasKeywordClass::Width => {
                            if let Ok(width) = Width::from_str(value.as_str()) {
                                self.widths.insert(width);
                            }
                            self.n_any += 1;
                        }
                        MeasKeywordClass::Scale => self.n_pne += 1,
                        MeasKeywordClass::Shortname => self.n_pnn += 1,
                        MeasKeywordClass::Wavelength => {
                            // if this fails, do nothing since we would end up
                            // dropping this keyword for any version
                            if let Ok(w) = Wavelengths::from_str_delim(value, true.into()).0 {
                                if w.0.len() > 1 {
                                    self.n_opt_min3_1 += 1;
                                } else {
                                    self.n_any += 1;
                                }
                            }
                        }
                        MeasKeywordClass::OptAny => self.n_any += 1,
                    }
                } else {
                    self.n_any += 1;
                }
            }
            AnyKeywordClass::Peak(i) => {
                if usize::from(i) < p {
                    self.n_opt_max3_1 += 1;
                } else {
                    self.n_any += 1;
                }
            }

            AnyKeywordClass::CSVFlag(i) => {
                if usize::from(i) < p {
                    self.n_opt_eq3_0or3_1 += 1;
                } else {
                    self.n_any += 1;
                }
            }
            AnyKeywordClass::Dfc(i, j) => {
                if usize::from(i) < p && usize::from(j) < p {
                    self.n_dfc += 1;
                } else {
                    self.n_any += 1;
                }
            }
            AnyKeywordClass::GateOptLE3_1(i) => {
                if usize::from(i) < p {
                    self.n_opt_max3_1 += 1;
                } else {
                    self.n_any += 1;
                }
            }
            AnyKeywordClass::RegionWindow => self.n_any += 1,
            AnyKeywordClass::RegionIndex => {
                if RegionGateIndex2_0::from_str_delim(value, true.into())
                    .0
                    .is_ok()
                {
                    self.n_opt_eq2_0 += 1;
                } else if RegionGateIndex3_0::from_str_delim(value, true.into())
                    .0
                    .is_ok()
                {
                    self.n_opt_eq3_0or3_1 += 1;
                } else if RegionGateIndex3_2::from_str_delim(value, true.into())
                    .0
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
    use super::*;

    use crate::test::*;
    use crate::text::{
        byteord::NewEndianError,
        keyword_enum::{self as kr, Keyword1FromValue as _},
    };

    use fireflow_types::nonempty_string::DisplayNE as _;

    use assert_matches::assert_matches;
    use proptest::prelude::*;

    // don't derive this since it is better for debug output if this is shorter,
    // and it isn't necessary for it to be anything more than 0-2 elements; this
    // is also more realistic in terms of the values that will really be seen in
    // the wild
    impl Arbitrary for Wavelengths {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;
        fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
            prop::collection::vec(any::<PositiveFloat>(), 0..2)
                .prop_map(Wavelengths)
                .boxed()
        }
    }

    #[test]
    fn tr() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_from_to_str_with::<Trigger>(ne_str!("Wooden Leg Pt 3,456"), (), &conf);
        let go = |s| Trigger::from_str_with(s, (), &conf);
        assert_matches!(go(ne_str!("x,x")), Err(TriggerError::IntFormat(_)));
        assert_matches!(go(ne_str!("x,0.0")), Err(TriggerError::IntFormat(_)));
        assert_eq!(go(ne_str!("x")), Err(TriggerError::WrongFieldNumber));
        assert_eq!(go(ne_str!("x,x,x")), Err(TriggerError::WrongFieldNumber));
    }

    #[test]
    fn tr_commas() {
        let v = ne_str!("Wookie Leg Pt 3, 666");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_matches!(
            Trigger::from_str_with(v, (), &conf),
            Err(TriggerError::IntFormat(_))
        );
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<Trigger>(v, "Wookie Leg Pt 3,666", (), &conf);
    }

    #[test]
    fn mode() {
        assert_from_to_str::<Mode>("C");
        assert_from_to_str::<Mode>("L");
        assert_from_to_str::<Mode>("U");
        assert_matches!(Mode::from_str("X"), Err(ModeError(_)));
    }

    #[test]
    fn mode_3_2() {
        assert_from_to_str::<Mode3_2>("L");
        assert_eq!(Mode3_2::from_str("C"), Err(Mode3_2Error));
        assert_eq!(Mode3_2::from_str("U"), Err(Mode3_2Error));
    }

    #[test]
    fn pnd() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_from_to_str_with::<Display>(ne_str!("Linear,0,1"), (), &conf);
        assert_from_to_str_with::<Display>(ne_str!("Logarithmic,1,1"), (), &conf);
        assert_from_to_str_with::<Display>(ne_str!("Logarithmic,1,0.1"), (), &conf);

        macro_rules! go {
            ($s:expr) => {
                Display::from_str_with(ne_str!($s), (), &conf)
            };
        }

        assert_matches!(go!("Linear,x,x"), Err(DisplayError::FloatError(_)));
        assert_eq!(go!("LIN,0,1"), Err(DisplayError::InvalidType));
        assert_eq!(go!("LOG,1,1"), Err(DisplayError::InvalidType));
        assert_eq!(go!("Logicle,0,1,2,3"), Err(DisplayError::FormatError));
        assert_eq!(go!("Linear,1.0,-1.0"), Err(DisplayError::Linear(1.0, -1.0)));
        assert_eq!(
            go!("Logarithmic,-1.0,1.0"),
            Err(DisplayError::Log(-1.0, 1.0))
        );
    }

    #[test]
    fn pnd_commas() {
        let v = ne_str!("Linear, 0 , 1");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_matches!(
            Display::from_str_with(v, (), &conf),
            Err(DisplayError::FloatError(_))
        );
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<Display>(v, "Linear,0,1", (), &conf);
    }

    #[test]
    fn datatype() {
        assert_from_to_str::<NumType>("I");
        assert_from_to_str::<NumType>("F");
        assert_from_to_str::<NumType>("D");
        assert_matches!(NumType::from_str("A"), Err(NumTypeError(_)));
    }

    #[test]
    fn pndatetype() {
        assert_from_to_str::<AlphaNumType>("I");
        assert_from_to_str::<AlphaNumType>("F");
        assert_from_to_str::<AlphaNumType>("D");
        assert_from_to_str::<AlphaNumType>("A");
        assert_matches!(AlphaNumType::from_str("X"), Err(AlphaNumTypeError(_)));
    }

    #[test]
    fn pncalibration_3_1() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_from_to_str_with::<Calibration3_1>(ne_str!("0.1,imperial lightyears"), (), &conf);
        macro_rules! go {
            ($s:expr) => {
                Calibration3_1::from_str_with(ne_str!($s), (), &conf)
            };
        }
        assert_eq!(
            go!("x"),
            Err(CalibrationError::Format(CalibrationFormat3_1))
        );
        assert_matches!(
            go!("x,x"),
            Err(CalibrationError::Range(RangedFloatError::Parse(_)))
        );
        assert_matches!(
            go!("x,0.1"),
            Err(CalibrationError::Range(RangedFloatError::Parse(_)))
        );
        assert_eq!(
            go!("0.1,"),
            Err(CalibrationError::EmptyUnit(EmptyCalibrationUnitError))
        );
    }

    #[test]
    fn pncalibration_3_1_commas() {
        let mut conf = EvaledReadStdKeywordsConfig::default();
        let v = ne_str!("1000 , yodabytes");
        assert_matches!(
            Calibration3_1::from_str_with(v, (), &conf),
            Err(CalibrationError::Range(RangedFloatError::Parse(_)))
        );
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<Calibration3_1>(v, "1000,yodabytes", (), &conf);
    }

    #[test]
    fn pncalibration_3_2() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_from_to_str_with::<Calibration3_2>(ne_str!("1.1,3.5813,prog albums"), (), &conf);
        assert_from_to_str_with::<Calibration3_2>(ne_str!("1.61,0,quartic slugs"), (), &conf);

        macro_rules! go {
            ($s:expr) => {
                Calibration3_2::from_str_with(ne_str!($s), (), &conf)
            };
        }

        assert_eq!(
            go!("x"),
            Err(CalibrationError::Format(CalibrationFormat3_2))
        );
        assert_matches!(
            go!("x,x"),
            Err(CalibrationError::Range(RangedFloatError::Parse(_)))
        );
        assert_matches!(
            go!("x,0.1"),
            Err(CalibrationError::Range(RangedFloatError::Parse(_)))
        );
        assert_matches!(go!("0.1,x,x"), Err(CalibrationError::Float(_)));
        assert_eq!(
            go!("0.1,1.0,"),
            Err(CalibrationError::EmptyUnit(EmptyCalibrationUnitError))
        );
    }

    #[test]
    fn pncalibration_3_2_commas() {
        let mut conf = EvaledReadStdKeywordsConfig::default();
        let v = ne_str!("1, 0.2, nanobytes");
        assert_matches!(
            Calibration3_2::from_str_with(v, (), &conf),
            Err(CalibrationError::Float(_))
        );
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<Calibration3_2>(v, "1,0.2,nanobytes", (), &conf);
    }

    #[test]
    fn pnl_3_1() {
        let conf = EvaledReadStdKeywordsConfig::default();
        let go = |v: &NEStr| {
            let w = Wavelengths::from_str_with(v, (), &conf).unwrap().inner;
            let w_str = w.try_ne().unwrap().to_ne().to_ne_string();
            assert_eq!(w_str.as_ne_str(), v);
        };
        go(ne_str!("0.5"));
        go(ne_str!("0.5,2"));

        macro_rules! go_err {
            ($s:expr) => {
                Wavelengths::from_str_with(ne_str!($s), (), &conf)
            };
        }
        assert_matches!(
            go_err!("x"),
            Err(WavelengthsError::Num(RangedFloatError::Parse(_)))
        );
        assert_matches!(
            go_err!("j,"),
            Err(WavelengthsError::Num(RangedFloatError::Parse(_)))
        );
    }

    #[test]
    fn pnl_3_1_commas() {
        let mut conf = EvaledReadStdKeywordsConfig::default();
        let v = ne_str!("1, 2");
        assert_matches!(
            Wavelengths::from_str_with(v, (), &conf),
            Err(WavelengthsError::Num(RangedFloatError::Parse(_)))
        );
        conf.trim_intra_value_whitespace = true.into();
        let w = Wavelengths::from_str_with(v, (), &conf).unwrap().inner;
        let w_str = w.try_ne().unwrap().to_ne().to_ne_string();
        assert_eq!(w_str.as_ne_str(), ne_str!("1,2"));
    }

    #[test]
    fn last_modified() {
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_from_to_str_with::<LastModified>(ne_str!("01-Jan-2112 00:00:00.01"), (), &conf);
        assert_from_to_str_almost_with::<LastModified>(
            ne_str!("01-Jan-2112 00:00:00"),
            "01-Jan-2112 00:00:00.00",
            (),
            &conf,
        );
        let v = ne_str!("01-Jan-2112 00:00");
        assert_eq!(
            LastModified::from_str_with(v, (), &conf),
            Err(LastModifiedError::Format)
        );
        conf.last_modified_pattern = Some("%d_%b_%Y_%H_%M".into());
        assert_matches!(
            LastModified::from_str_with(v, (), &conf),
            Err(LastModifiedError::AltFormat(_))
        );
        conf.last_modified_pattern = Some("%d-%b-%Y %H:%M".into());
        assert_from_to_str_almost_with::<LastModified>(v, "01-Jan-2112 00:00:00.00", (), &conf);
    }

    #[test]
    fn originality() {
        assert_from_to_str::<Originality>("Original");
        assert_from_to_str::<Originality>("NonDataModified");
        assert_from_to_str::<Originality>("Appended");
        assert_from_to_str::<Originality>("DataModified");
        assert_matches!(Originality::from_str("x"), Err(OriginalityError(_)));
    }

    #[test]
    fn unicode() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_from_to_str_with::<Unicode>(ne_str!("42,$BYTEORD"), (), &conf);
        // we don't actually check that the keyword is valid, likely nobody
        // will notice ;)
        assert_from_to_str_with::<Unicode>(ne_str!("42,$40DOLLARBILL"), (), &conf);
        macro_rules! go {
            ($s:expr) => {
                Unicode::from_str_with(ne_str!($s), (), &conf)
            };
        }
        assert_eq!(go!("42"), Err(UnicodeError::Empty));
        assert_eq!(go!("x"), Err(UnicodeError::BadFormat));
        assert_eq!(go!("666,"), Err(UnicodeError::EmptyKws));
    }

    #[test]
    fn unicode_commas() {
        let v = ne_str!("50 ,something tour");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_eq!(
            Unicode::from_str_with(v, (), &conf),
            Err(UnicodeError::BadFormat)
        );
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<Unicode>(v, "50,something tour", (), &conf);
    }

    #[test]
    fn pntype_optical() {
        // this can basically be anything, even though only a few values make sense
        let go = |v| {
            let t = OpticalType::from_str(v).unwrap();
            let k = kr::OptOpticalKeyword::from_str(&t, MeasIndex::from(0)).unwrap();
            assert!(k.as_std_key_pair().1.as_str() == v);
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
        let k = kr::OptTemporalKeyword::from_opt_zst(t, MeasIndex::from(0)).unwrap();
        assert!(k.as_std_key_pair().1.as_str() == "Time");
        assert_eq!(TemporalType::from_str("Space"), Err(TemporalTypeError));
    }

    #[test]
    fn pnfeature() {
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_from_to_str_with::<Feature>(ne_str!("Area"), (), &conf);
        assert_from_to_str_with::<Feature>(ne_str!("Width"), (), &conf);
        assert_from_to_str_with::<Feature>(ne_str!("Height"), (), &conf);
        assert_matches!(
            Feature::from_str_with(ne_str!("Volume"), (), &conf),
            Err(OpticalFeatureError(_))
        );
        conf.allow_other_feature = true.into();
        assert_from_to_str_with::<Feature>(ne_str!("Volume"), (), &conf);
    }

    #[test]
    fn rni_2_0() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_from_to_str_with::<RegionGateIndex2_0>(ne_str!("1"), (), &conf);
        assert_from_to_str_with::<RegionGateIndex2_0>(ne_str!("1,2"), (), &conf);
        macro_rules! go {
            ($s:expr) => {
                RegionGateIndex2_0::from_str_with(ne_str!($s), (), &conf)
            };
        }
        assert_matches!(go!("x"), Err(RegionGateIndexError::Int(_)));
        assert_eq!(go!("1,2,3"), Err(RegionGateIndexError::Format));
    }

    #[test]
    fn rni_2_0_commas() {
        let v = ne_str!("1, 2");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_matches!(
            RegionGateIndex2_0::from_str_with(v, (), &conf),
            Err(RegionGateIndexError::Int(_))
        );
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<RegionGateIndex2_0>(v, "1,2", (), &conf);
    }

    #[test]
    fn rni_3_0() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_from_to_str_with::<RegionGateIndex3_0>(ne_str!("P1"), (), &conf);
        assert_from_to_str_with::<RegionGateIndex3_0>(ne_str!("P1,P2"), (), &conf);
        assert_from_to_str_with::<RegionGateIndex3_0>(ne_str!("G1"), (), &conf);
        assert_from_to_str_with::<RegionGateIndex3_0>(ne_str!("G1,G2"), (), &conf);
        macro_rules! go {
            ($s:expr) => {
                RegionGateIndex3_0::from_str_with(ne_str!($s), (), &conf)
            };
        }
        assert_eq!(
            go!("x"),
            Err(RegionGateIndexError::Int(MeasOrGateIndexError::Format))
        );
        assert_matches!(
            go!("Px"),
            Err(RegionGateIndexError::Int(MeasOrGateIndexError::Int(_)))
        );
        assert_eq!(go!("P1,G2,P3"), Err(RegionGateIndexError::Format));
    }

    #[test]
    fn rni_3_0_commas() {
        let v = ne_str!("P1, G2");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_matches!(
            RegionGateIndex3_0::from_str_with(v, (), &conf),
            Err(RegionGateIndexError::Int(MeasOrGateIndexError::Format))
        );
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<RegionGateIndex3_0>(v, "P1,G2", (), &conf);
    }

    #[test]
    fn rni_3_2() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_from_to_str_with::<RegionGateIndex3_2>(ne_str!("P1"), (), &conf);
        assert_from_to_str_with::<RegionGateIndex3_2>(ne_str!("P1,P2"), (), &conf);

        macro_rules! go {
            ($s:expr) => {
                RegionGateIndex3_2::from_str_with(ne_str!($s), (), &conf)
            };
        }
        assert_eq!(
            go!("G1"),
            Err(RegionGateIndexError::Int(PrefixedMeasIndexError::Format))
        );
        assert_matches!(
            go!("Px"),
            Err(RegionGateIndexError::Int(PrefixedMeasIndexError::Int(_)))
        );
        assert_eq!(go!("P1,G2,P3"), Err(RegionGateIndexError::Format));
    }

    #[test]
    fn rni_3_2_commas() {
        let v = ne_str!("P1, P2");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_matches!(
            RegionGateIndex3_2::from_str_with(v, (), &conf),
            Err(RegionGateIndexError::Int(PrefixedMeasIndexError::Format))
        );
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<RegionGateIndex3_2>(v, "P1,P2", (), &conf);
    }

    #[test]
    fn rnw() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_from_to_str_with::<RegionWindow>(ne_str!("1,1"), (), &conf);
        assert_from_to_str_with::<RegionWindow>(ne_str!("1,1;2,3;5,8;13,21"), (), &conf);
        macro_rules! go {
            ($s:expr) => {
                RegionWindow::from_str_with(ne_str!($s), (), &conf)
            };
        }
        assert_eq!(go!("1"), Err(RegionWindowError::Format));
        assert_eq!(go!("1,1,1"), Err(RegionWindowError::Format));
        assert_eq!(go!("1;1"), Err(RegionWindowError::Format));
        assert_eq!(go!("1,1,1;1,1,1"), Err(RegionWindowError::Format));
        assert_matches!(go!("1,1;1,x"), Err(RegionWindowError::Num(_)));
    }

    #[test]
    fn rnw_commas() {
        let v = ne_str!("1, 1 ; 2, 2");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_matches!(
            RegionWindow::from_str_with(v, (), &conf),
            Err(RegionWindowError::Num(_))
        );
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<RegionWindow>(v, "1,1;2,2", (), &conf);
    }

    #[test]
    fn gating() {
        assert_from_to_str::<Gating>("R1");
        assert_from_to_str_almost::<Gating>("R1 AND (R2.OR.R3)", "(R1 AND (R2 OR R3))");
        assert_from_to_str::<Gating>("((NOT R1) AND R2)");

        assert_eq!(Gating::from_str(""), Err(GatingError::EmptyExpr));
        assert_eq!(
            Gating::from_str("NAND R1"),
            Err(GatingError::BadToken("NAND".into()))
        );
        assert_eq!(Gating::from_str("(NOT R1"), Err(GatingError::MissingParen));
        assert_eq!(
            Gating::from_str("NOT R1)"),
            Err(GatingError::InvalidBinaryToken(GatingToken::RParen))
        );
        assert_eq!(
            Gating::from_str("AND R1)"),
            Err(GatingError::InvalidExprToken(GatingToken::And))
        );
        assert_eq!(Gating::from_str("R1 AND"), Err(GatingError::EmptyExpr));
    }

    #[test]
    fn unstained_centers() {
        let conf = EvaledReadStdKeywordsConfig::default();
        let v = ne_str!("1,X,0");
        let t = UnstainedCenters::from_str_with(v, (), &conf).unwrap();
        let s = t.inner.try_ne().unwrap().to_ne().to_ne_string();
        assert_eq!(s.as_ne_str(), v);
    }

    #[test]
    fn unstained_centers_commas() {
        let v = ne_str!("1, X , 0");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_eq!(
            UnstainedCenters::from_str_with(v, (), &conf),
            Err(ParseUnstainedCenterError::BadFloat)
        );
        conf.trim_intra_value_whitespace = true.into();
        let t = UnstainedCenters::from_str_with(v, (), &conf).unwrap();
        let s = t.inner.try_ne().unwrap().to_ne().to_ne_string();
        assert_eq!(s.as_str(), "1,X,0");
    }

    #[test]
    fn unstained_centers_wrong_len() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_eq!(
            UnstainedCenters::from_str_with(ne_str!("2,X,0"), (), &conf),
            Err(ParseUnstainedCenterError::BadLength {
                found: 2,
                expected: 4
            })
        );
    }

    #[test]
    fn unstained_centers_nonunique() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_eq!(
            UnstainedCenters::from_str_with(ne_str!("3,Y,Y,Z,0,0,0"), (), &conf),
            Err(ParseUnstainedCenterError::NonUnique),
        );
    }

    #[test]
    fn unstained_centers_badn() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_eq!(
            UnstainedCenters::from_str_with(ne_str!("impoppy,X,Y,0,0"), (), &conf),
            Err(ParseUnstainedCenterError::BadN),
        );
    }

    #[test]
    fn unstained_centers_badfloat() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_eq!(
            UnstainedCenters::from_str_with(ne_str!("1,X,impoppy"), (), &conf),
            Err(ParseUnstainedCenterError::BadFloat),
        );
    }

    #[test]
    fn str_compensation() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_from_to_str_with::<Compensation3_0>(ne_str!("2,0,0,0,0"), (), &conf);
        assert_from_to_str_with::<Compensation3_0>(ne_str!("3,0,0,0,0,0,0,0,0,0"), (), &conf);
        assert_from_to_str_with::<Compensation3_0>(ne_str!("2,1.1,1,0,-1.5"), (), &conf);
    }

    #[test]
    fn str_compensation_too_small() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_eq!(
            Compensation3_0::from_str_with(ne_str!("1,0"), (), &conf),
            Err(ParseCompError::New(NewCompError::TooSmall))
        );
    }

    #[test]
    fn str_compensation_mismatch() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_eq!(
            Compensation3_0::from_str_with(ne_str!("2,0,0,0"), (), &conf),
            Err(ParseCompError::WrongLength {
                found: 3,
                expected: 4
            })
        );
    }

    #[test]
    fn str_compensation_badfloats() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_eq!(
            Compensation3_0::from_str_with(ne_str!("2,straberina,0,coconick"), (), &conf),
            Err(ParseCompError::BadFloat)
        );
    }

    #[test]
    fn str_compensation_badn() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_eq!(
            Compensation3_0::from_str_with(ne_str!("inf,0,0,0,0"), (), &conf),
            Err(ParseCompError::BadLength)
        );
    }

    #[test]
    fn str_compensation_commas() {
        let v = ne_str!("2, 0, 0, 0, 0");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_eq!(
            Compensation3_0::from_str_with(v, (), &conf),
            Err(ParseCompError::BadFloat)
        );
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
        assert_eq!(
            "1,2,3,4,5,6,7,8,9".parse::<ByteOrd2_0>(),
            Err(ParseByteOrdError::Order(NewByteOrdError(9)))
        );
    }

    #[test]
    fn str_to_byteord_bad_digits() {
        assert_eq!(
            "0".parse::<ByteOrd2_0>(),
            Err(ParseByteOrdError::Digit(ByteordDigitError))
        );
        assert_eq!(
            "2".parse::<ByteOrd2_0>(),
            Err(ParseByteOrdError::Order(NewByteOrdError(1)))
        );
    }

    #[test]
    fn str_to_byteord_skipped() {
        assert_eq!(
            "1,3".parse::<ByteOrd2_0>(),
            Err(ParseByteOrdError::Order(NewByteOrdError(2)))
        );
    }

    #[test]
    fn str_to_byteord_repeat() {
        assert_eq!(
            "1,1".parse::<ByteOrd2_0>(),
            Err(ParseByteOrdError::Order(NewByteOrdError(2)))
        );
    }

    #[test]
    fn str_to_byteord_garbage() {
        assert_eq!(
            "fortytwo".parse::<ByteOrd2_0>(),
            Err(ParseByteOrdError::Digit(ByteordDigitError))
        );
        assert_eq!(
            "".parse::<ByteOrd2_0>(),
            Err(ParseByteOrdError::Digit(ByteordDigitError))
        );
        assert_eq!(
            "one,two,three".parse::<ByteOrd2_0>(),
            Err(ParseByteOrdError::Digit(ByteordDigitError))
        );
    }

    #[test]
    fn str_to_endian() {
        assert_eq!(
            "1,2,3,4".parse::<ByteOrd3_1>(),
            Ok(ByteOrd3_1(Endian::Little))
        );
        assert_eq!("4,3,2,1".parse::<ByteOrd3_1>(), Ok(ByteOrd3_1(Endian::Big)));
        assert_eq!("1,2,3".parse::<ByteOrd3_1>(), Err(NewEndianError));
        assert_eq!("5,4,3,2,1".parse::<ByteOrd3_1>(), Err(NewEndianError));
    }

    #[test]
    fn scale() {
        let conf = EvaledReadStdKeywordsConfig::default();
        let dt = AlphaNumType::Integer;
        assert_from_to_str_with::<Scale>(ne_str!("0,0"), dt, &conf);
        assert_from_to_str_with::<Scale>(ne_str!("4.5,0.01"), dt, &conf);
    }

    #[test]
    fn scale_zero_log() {
        let v = ne_str!("4.5,0");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        let dt = AlphaNumType::Integer;
        assert_eq!(
            Scale::from_str_with(v, dt, &conf),
            Err(ScaleError::LogRange(LogRangeError::new(4.5, 0.0)))
        );
        conf.fix_log_scale_offsets = true.into();
        assert_from_to_str_almost_with::<Scale>(v, "4.5,1", dt, &conf);
    }

    #[test]
    fn scale_force_linear() {
        let v = ne_str!("1,1");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        let dt = AlphaNumType::Float;
        assert_from_to_str_almost_with::<Scale>(v, "1,1", dt, &conf);
        conf.force_linear_scale = ForceLinearScale::AllNonInt;
        assert_from_to_str_almost_with::<Scale>(v, "0,0", dt, &conf);
    }

    #[test]
    fn scale_force_linear_int() {
        let v = ne_str!("1,1");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        let dt = AlphaNumType::Integer;
        assert_from_to_str_almost_with::<Scale>(v, "1,1", dt, &conf);
        conf.force_linear_scale = ForceLinearScale::AllNonInt;
        assert_from_to_str_almost_with::<Scale>(v, "1,1", dt, &conf);
        conf.force_linear_scale = ForceLinearScale::All;
        assert_from_to_str_almost_with::<Scale>(v, "0,0", dt, &conf);
    }

    #[test]
    fn scale_commas() {
        let v = ne_str!("0, 0");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        let dt = AlphaNumType::Integer;
        assert_matches!(
            Scale::from_str_with(v, dt, &conf),
            Err(ScaleError::FloatError(_))
        );
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<Scale>(v, "0,0", dt, &conf);
    }

    #[test]
    fn tmp_scale2() {
        let conf = EvaledReadStdKeywordsConfig::default();
        // no display, so just check parse
        assert!(TemporalScale2_0::from_str_with(ne_str!("0,0"), (), &conf).is_ok());
        assert_eq!(
            TemporalScale2_0::from_str_with(ne_str!("1,1"), (), &conf),
            Err(TemporalScaleError::NonLinear),
        );
    }

    #[test]
    fn tmp_scale2_commas() {
        let v = ne_str!("0, 0");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_eq!(
            TemporalScale2_0::from_str_with(v, (), &conf),
            Err(TemporalScaleError::Format),
        );
        conf.trim_intra_value_whitespace = true.into();
        assert!(TemporalScale2_0::from_str_with(v, (), &conf).is_ok());
    }

    #[test]
    fn tmp_scale3() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_from_to_str_with::<TemporalScale3_0>(ne_str!("0,0"), (), &conf);
        assert_eq!(
            TemporalScale3_0::from_str_with(ne_str!("1,1"), (), &conf),
            Err(TemporalScaleError::NonLinear),
        );
    }

    #[test]
    fn tmp_scale3_commas() {
        let v = ne_str!("0, 0");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_eq!(
            TemporalScale3_0::from_str_with(v, (), &conf),
            Err(TemporalScaleError::Format),
        );
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<TemporalScale3_0>(v, "0,0", (), &conf);
    }

    #[test]
    fn gate_scale() {
        let conf = EvaledReadStdKeywordsConfig::default();
        assert_from_to_str_with::<GateScale>(ne_str!("0,0"), (), &conf);
        assert_from_to_str_with::<GateScale>(ne_str!("4.5,0.01"), (), &conf);
    }

    #[test]
    fn gate_scale_zero_log() {
        let v = ne_str!("4.5,0");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_eq!(
            GateScale::from_str_with(v, (), &conf),
            Err(ScaleError::LogRange(LogRangeError::new(4.5, 0.0)))
        );
        conf.fix_log_scale_offsets = true.into();
        assert_from_to_str_almost_with::<GateScale>(v, "4.5,1", (), &conf);
    }

    #[test]
    fn gate_scale_commas() {
        let v = ne_str!("0, 0");
        let mut conf = EvaledReadStdKeywordsConfig::default();
        assert_matches!(
            GateScale::from_str_with(v, (), &conf),
            Err(ScaleError::FloatError(_))
        );
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
    impl<'py> FromPyObject<'_, 'py> for ByteOrd2_0 {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let xs: Vec<NonZeroU8> = obj.extract()?;
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
    impl<'py> FromPyObject<'_, 'py> for Scale {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if obj.is_instance_of::<PyTuple>() && obj.len()? == 0 {
                Ok(Self::Linear)
            } else {
                let (decades, offset): (f32, f32) = obj.extract()?;
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
    impl<'py> FromPyObject<'_, 'py> for Calibration3_1 {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let (slope, unit): (PositiveFloat, NEString) = obj.extract()?;
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
    impl<'py> FromPyObject<'_, 'py> for Calibration3_2 {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let (slope, offset, unit): (PositiveFloat, f32, NEString) = obj.extract()?;
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
    impl<'py> FromPyObject<'_, 'py> for Unicode {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let (page, kws): (u32, Vec<NEString>) = obj.extract()?;
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
    impl<'py> FromPyObject<'_, 'py> for Display {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let (is_log, x0, x1): (bool, f32, f32) = obj.extract()?;
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
    impl<'py> FromPyObject<'_, 'py> for Trigger {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let (measurement, threshold): (Shortname, u32) = obj.extract()?;
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
    impl<'py> FromPyObject<'_, 'py> for UniGate {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let (lower, upper) = obj.extract()?;
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
    impl<'py> FromPyObject<'_, 'py> for Vertex {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let (x, y) = obj.extract()?;
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
    impl<'a, 'py, I> FromPyObject<'a, 'py> for IndexPair<I>
    where
        I: FromPyObject<'a, 'py>,
    {
        type Error = PyErr;
        fn extract(obj: Borrowed<'a, 'py, PyAny>) -> Result<Self, Self::Error> {
            let (x, y) = obj.extract()?;
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

    impl<'py> FromPyObject<'_, 'py> for ScaleFix {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Some((x, y)) = obj.extract::<Option<(NEString, NEString)>>()? {
                match y.as_ref() {
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

    impl<'py> FromPyObject<'_, 'py> for OpticalScaleFix {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Some((x, y)) = obj.extract::<Option<(NEString, NEString)>>()? {
                match y.as_ref() {
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

    impl<'py> FromPyObject<'_, 'py> for TemporalScaleFix {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Some((x, y)) = obj.extract::<Option<(NEString, NEString)>>()? {
                match y.as_ref() {
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
