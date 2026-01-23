use crate::config::{
    ConfigFlag as _, ForceLinearScale, ProcessOptionalFailure, ReadDataKeywordsConfig,
    ReadStdKeywordsConfig, TemporalOpticalKey, TrimIntraValueWhitespace,
};
use crate::core::UnitaryKeyLossError;
use crate::header::Version;
use crate::logging::{
    DeferredError, DeferredSwitchableErrors, LogResult, ResultExt as _, WarningAndErrorResult,
};
use crate::macros::impl_newtype_try_from;
use crate::nonempty::FCSNonEmpty;
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
use crate::text::optional::{
    CheckMaybe, DisplayMaybe, KeywordPairMaybe, OptionalInt, OptionalString, OptionalZST,
};
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
use crate::validated::keys::{
    AnyKey as _, BiIndex, BiIndexedKey, IndexedKey, Key, Key0, Key1, Key2, NonStdKeywords,
    StdKeywords,
};
use crate::validated::keys::{NonStdKeywordsExt as _, StdKey};
use crate::validated::nonempty_string::NonEmptyString;
use crate::validated::shortname::Shortname;

use type_families::{BifunctorOnce as _, FunctorOnce as _, impl_functor, impl_kind1};

use bigdecimal::{BigDecimal, ParseBigDecimalError};
use chrono::{NaiveDateTime, NaiveTime, Timelike as _};
use derive_more::{Add, AsMut, AsRef, Display, From, FromStr, Into, Sub};
use derive_new::new;
use itertools::Itertools as _;
use nalgebra::DMatrix;
use nonempty::NonEmpty;
use num_traits::PrimInt;
use num_traits::cast::ToPrimitive as _;
use num_traits::identities::{One as _, Zero as _};
use std::collections::HashMap;
use std::fmt;
use std::mem::take;
use std::num::{NonZeroU8, ParseFloatError, ParseIntError};
use std::str::FromStr;
use thiserror::Error;
use unicase::Ascii;

#[cfg(feature = "serde")]
use serde::Serialize;

use super::lookup::{DiagnosedKeyword, FromStrWithResult, Trimmed, TrimmedKeyword};

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{
        AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject, FromPyString, IntoPyString,
    },
    pyo3::prelude::*,
};

// The string primitives for almost all keywords are compiled in a build script
// as string constants and included here. This is done in order to put these
// strings into a pre-compiled hash table which will be used for version
// autodetection and sorting through unused keywords efficiently.
include!(concat!(env!("OUT_DIR"), "/kw_map.rs"));

/// Data structure to classify root (non-indexed) keywords.
///
/// For optional keywords this simply records the version in which a given
/// keyword is valid. Some specific keywords ($CYT, $TOT, etc) are explicitly
/// encoded since they are optional or required (or missing entirely) depending
/// on version. $BYTEORD is included because a non-endian value implies 2.0/3.0.
/// $MODE is included because its value and optionality is different between 3.1
/// and 3.2
#[derive(Clone, Copy)]
pub(crate) enum RootKeywordClass {
    OptAny,
    OptGE3_1,
    OptGE3_2,
    OptEQ3_0or3_1,
    OptEQ3_0,
    OptLE3_1,
    Mode,
    Cyt,
    Tot,
    Timestep,
    Byteord,
    Begindata,
    Enddata,
    Beginanalysis,
    Endanalysis,
    Beginstext,
    Endstext,
}

pub(crate) enum MeasKeywordClass {
    OptAny,
    OptGE3_0,
    OptGE3_1,
    OptGE3_2,
    Scale,
    Shortname,
    Wavelength,
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

#[derive(Clone, Copy, Default)]
enum ModeValue {
    #[default]
    Missing,
    List,
    Other,
}

#[derive(Default, PartialEq)]
pub(crate) struct KeywordVersionScore {
    /// Number of required keywords expected to be in this version and found
    pub(crate) good_req: usize,
    /// Number of optional keywords expected to be in this version and found
    pub(crate) good_opt: usize,
    /// Number of keywords (opt or req) that must be dropped for this version
    pub(crate) drop: usize,
    /// Number of optional keywords that are missing in this version
    pub(crate) missing_opt: usize,
    /// Number of required keywords that are missing in this version
    pub(crate) missing_req: usize,
    /// Number of expected keywords that are not present in this version
    pub(crate) missing_absent: usize,
}

impl KeywordVersionScore {
    pub(crate) fn is_passing(&self, allow_drop: bool) -> bool {
        (self.missing_req == 0) && (self.drop == 0 || (self.drop > 0 && allow_drop))
    }
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
                    // TODO if this fails we should just bug out immediately since
                    // this is required
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
                    // TODO ditto Mode
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
            AnyKeywordClass::MeasOptGE3_0(_) => {
                self.n_opt_min3_0 += 1;
            }
            AnyKeywordClass::MeasOptGE3_1(_) => {
                self.n_opt_min3_1 += 1;
            }
            AnyKeywordClass::MeasOptGE3_2(_) => {
                self.n_opt_min3_2 += 1;
            }
            AnyKeywordClass::MeasOptEq3_0or3_1(_) => {
                self.n_opt_eq3_0or3_1 += 1;
            }
            AnyKeywordClass::Scale(_) => self.n_pne += 1,
            AnyKeywordClass::Shortname(_) => self.n_pnn += 1,
            AnyKeywordClass::Wavelength(_) => {
                // TODO what to do on failure?
                if let Ok(w) = Wavelengths::from_str_delim(value, true.into()) {
                    if w.native.0.len() > 1 {
                        self.n_opt_min3_1 += 1;
                    } else {
                        self.n_any += 1;
                    }
                }
            }
            AnyKeywordClass::Dfc(_, _) => self.n_opt_eq2_0 += 1,
            AnyKeywordClass::GateOptLE3_1(_) => self.n_opt_max3_1 += 1,
            AnyKeywordClass::MeasAny(_) | AnyKeywordClass::RegionWindow => self.n_any += 1,
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

enum AnyKeywordClass {
    Root(RootKeywordClass),
    MeasAny(MeasIndex),
    MeasOptGE3_0(MeasIndex),
    MeasOptGE3_1(MeasIndex),
    MeasOptGE3_2(MeasIndex),
    MeasOptEq3_0or3_1(MeasIndex),
    Shortname(MeasIndex),
    Scale(MeasIndex),
    Wavelength(MeasIndex),
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

        if let Some(rc) = KW_MAP.get(&s) {
            Self::Root(*rc)
        } else if let Some(rest) = starts_with_icase(ss, "P") {
            // $Pn* keywords or $PKn or $PKNn
            if let Some((index, suffix)) =
                starts_with_icase(rest, "KN").and_then(|r| split_index_and_suffix(r))
                && suffix.is_empty()
            {
                // $PKNn
                Self::MeasOptGE3_1(index.into())
            } else if let Some((index, suffix)) =
                starts_with_icase(rest, "K").and_then(|r| split_index_and_suffix(r))
                && suffix.is_empty()
            {
                // $PKn
                Self::MeasOptGE3_1(index.into())
            } else if let Some((index, suffix)) = split_index_and_suffix(rest) {
                // $Pn*
                let j = index.into();
                if let Some(vc) = MEAS_SUFFIX_MAP.get(&Ascii::new(suffix)) {
                    match vc {
                        MeasKeywordClass::OptAny => Self::MeasAny(j),
                        MeasKeywordClass::OptGE3_0 => Self::MeasOptGE3_0(j),
                        MeasKeywordClass::OptGE3_1 => Self::MeasOptGE3_1(j),
                        MeasKeywordClass::OptGE3_2 => Self::MeasOptGE3_2(j),
                        MeasKeywordClass::Shortname => Self::Shortname(j),
                        MeasKeywordClass::Scale => Self::Scale(j),
                        MeasKeywordClass::Wavelength => Self::Wavelength(j),
                    }
                } else {
                    Self::NonStandard
                }
            } else {
                Self::NonStandard
            }
        } else if let Some((index, suffix)) =
            starts_with_icase(ss, "G").and_then(|r| split_index_and_suffix(r))
            && GATE_SUFFIX_SET.contains(&Ascii::new(suffix))
        {
            // $Gn* keywords
            Self::GateOptLE3_1(index.into())
        } else if let Some((_, suffix)) =
            starts_with_icase(ss, "R").and_then(|r| split_index_and_suffix(r))
        {
            // $Rn* keywords
            if RegionGateIndex::<()>::SUFFIX.eq_ignore_ascii_case(suffix) {
                Self::RegionIndex
            } else if RegionWindow::SUFFIX.eq_ignore_ascii_case(suffix) {
                Self::RegionWindow
            } else {
                Self::NonStandard
            }
        } else if let Some((index, suffix)) =
            starts_with_icase(ss, "CSV").and_then(|r| split_index_and_suffix(r))
            && suffix.eq_ignore_ascii_case("FLAG")
        {
            // $CSVnFLAG
            Self::MeasOptEq3_0or3_1(index.into())
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

pub(crate) const MEAS_KW_PREFIX: &str = "P";
pub(crate) const GATE_KW_PREFIX: &str = "G";
pub(crate) const REGION_KW_PREFIX: &str = "R";

pub(crate) const REGION_INDEX_KW_SUFFIX: &str = "I";
pub(crate) const REGION_WINDOW_KW_SUFFIX: &str = "W";

/// Value for $NEXTDATA (all versions)
#[derive(From, Into, FromStr, Display, Debug, Clone, Copy)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct Nextdata(pub UintZeroPad20);

/// The value for the $PnE key (all versions).
///
/// Format is assumed to be 'f1,f2'
#[derive(Clone, Copy, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Scale {
    /// Linear scale (ie '0,0')
    #[display("0,0")]
    Linear,

    /// Log scale, where both numbers are positive
    #[display("{_0}")]
    Log(LogScale),
}

/// Diagnostic data from parsing $PnE
#[derive(Default, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum ScaleDiagnostic {
    /// Nothing happend
    #[default]
    None,
    /// Was forced to be linear (which overrides everything else)
    Forced(String),
    /// Whitespace was trimmed
    Trimmed(String),
    /// Zero log offset was corrected
    LogFixed(String),
    /// Trimmed and zero log offset was corrected
    TrimmedLogFixed(String),
}

#[derive(Clone, Copy, PartialEq, Debug, Display, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{decades},{offset}")]
pub struct LogScale {
    pub decades: PositiveFloat,
    pub offset: PositiveFloat,
}

impl Scale {
    pub fn try_new_log(decades: f32, offset: f32) -> Result<Self, LogRangeError> {
        (decades, offset).try_into().map(Self::Log)
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
    type Payload<'a> = ();
    type Diagnostic = ScaleDiagnostic;

    fn from_str_with(s: &str, (): (), conf: &ReadStdKeywordsConfig) -> FromStrWithResult<Self> {
        let go = |x: TrimmedKeyword<_>| {
            let d = x.trimmed.map(ScaleDiagnostic::Trimmed).unwrap_or_default();
            DiagnosedKeyword::new(x.native, d)
        };
        if matches!(conf.force_linear_scale, ForceLinearScale::All) {
            let d = ScaleDiagnostic::Forced(s.to_owned());
            Ok(DiagnosedKeyword::new(Self::Linear, d))
        } else {
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
                                    let d = ScaleDiagnostic::LogFixed(s.to_owned());
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
#[cfg_attr(feature = "python", pyerr(crate::python::InvalidKeywordValueError))]
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
#[derive(Clone, Copy, PartialEq, From, Display, FromStr, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Gain(pub PositiveFloat);

impl Gain {
    pub(crate) fn lookup_temporal_3_0<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        i: MeasIndex,
        conf: &C,
    ) -> DeferredSwitchableErrors<Option<Self>, ProcessOptionalFailure, LookupTemporalGainError>
    where
        C: AsRef<ReadDataKeywordsConfig> + AsRef<ReadStdKeywordsConfig>,
    {
        let ignore = &AsRef::<ReadStdKeywordsConfig>::as_ref(conf).ignore_time_optical_keys;
        let drop_flag = AsRef::<ReadDataKeywordsConfig>::as_ref(conf).process_optional_failure;
        if ignore.contains(&TemporalOpticalKey::Gain) {
            nonstd.transfer_demoted(std, Self::std(i));
            LogResult::new_switchable_ok(None, drop_flag)
        } else {
            Self::remove_or_drop_meas_opt(std, nonstd, i, conf.as_ref())
                .map_switchable_errors(LookupTemporalGainError::from)
                .into_semigroup()
                .eval_deferred_switchable_error(|gain| {
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
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct TemporalGainError(MeasIndex);

/// The value of the $TIMESTEP keyword
#[derive(Clone, Copy, PartialEq, From, Display, FromStr, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[into(f32, PositiveFloat)]
pub struct Timestep(pub PositiveFloat);

impl_newtype_try_from!(Timestep, PositiveFloat, f32, RangedFloatError);

impl Default for Timestep {
    fn default() -> Self {
        Self(PositiveFloat::one())
    }
}

impl Timestep {
    pub(crate) fn loss_error(self) -> Option<UnitaryKeyLossError<Self>> {
        (!self.0.is_one()).then_some(UnitaryKeyLossError::default())
    }
}

/// The value of the $VOL keyword
#[derive(Clone, Copy, From, Display, FromStr, Into, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[into(NonNegFloat, f32)]
pub struct Vol(pub NonNegFloat);

impl_newtype_try_from!(Vol, NonNegFloat, f32, RangedFloatError);

/// The value of the $TR field (all versions)
///
/// This is formatted as 'string,f' where 'string' is a measurement name.
#[derive(Clone, PartialEq, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{measurement},{threshold}")]
pub struct Trigger {
    /// The measurement name (assumed to match a '$PnN' value).
    pub measurement: Shortname,

    /// The threshold of the trigger.
    pub threshold: u32,
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
            .then(|| ExistingNamedLinkError::new(Key0::default(), NonEmpty::new(m.clone())))
    }

    pub(crate) fn invalid_link_error(
        &self,
        names: &NamedSet<'_>,
    ) -> Option<KeyToNameLinkError<Self>> {
        let m = &self.measurement;
        match names.membership(m) {
            NamedSetMembership::None => {
                Some(OpticalNamedLinkError::new_i0(NonEmpty::new(m.clone())).into())
            }
            NamedSetMembership::Center => Some(TemporalNamedLinkError::new_i0(m.clone()).into()),
            NamedSetMembership::NonCenter => None,
        }
    }

    pub(crate) fn remove_invalid_links(
        src: &mut Option<Self>,
        names: &NamedSet<'_>,
    ) -> Option<RemovedNamedLink<Self>> {
        let tr = src.as_ref()?;
        let m = &tr.measurement;
        let ln = match names.membership(m) {
            NamedSetMembership::None => Some(LinkName::Both(NonEmpty::new(m.clone()), None)),
            NamedSetMembership::Center => Some(LinkName::Temporal(m.clone())),
            NamedSetMembership::NonCenter => None,
        };
        // ASSUME this won't fail since we filter out None above with ?
        ln.map(|n| RemovedNamedLink::new(take(src).unwrap(), n))
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

/// The values used for the $MODE key (up to 3.1)
#[derive(Clone, PartialEq, Eq, Default, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum Mode {
    #[default]
    #[display("L")]
    List,
    #[display("U")]
    Uncorrelated,
    #[display("C")]
    Correlated,
}

/// Error when [`Mode`] has a deprecated value (FCS 3.1)
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FCSDeprecatedError))]
pub enum DeprecatedModeWarning {
    #[error("$MODE=C is deprecated")]
    ModeCorrelated,
    #[error("$MODE=U is deprecated")]
    ModeUncorrelated,
}

/// Error when parsing [`Mode`] from string
#[derive(Debug, Error)]
#[error("must be one of 'C', 'L', or 'U'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub struct ModeError;

impl FromStr for Mode {
    type Err = ModeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "C" => Ok(Self::Correlated),
            "L" => Ok(Self::List),
            "U" => Ok(Self::Uncorrelated),
            _ => Err(ModeError),
        }
    }
}

/// The value for the $MODE key, which can only contain 'L' (3.2)
#[derive(Clone, PartialEq, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
#[display("L")]
pub struct Mode3_2;

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
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub struct Mode3_2Error;

/// Error when converting [`Mode`] to [`Mode3_2`]
#[derive(Debug, Error)]
#[error("$MODE must be 'L'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConversionError))]
pub struct ModeUpgradeError;

/// The value for the $PnD key (3.1+)
#[derive(Clone, Copy, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum Display {
    /// Linear display (value like `"Linear,<lower>,<upper>"`)
    #[display("Linear,{lower},{upper}")]
    Lin { lower: f32, upper: f32 },

    /// Logarithmic display (value like `"Logarithmic,<offset>,<decades>"`)
    #[display("Logarithmic,{decades},{offset}")]
    Log {
        offset: PositiveFloat,
        decades: PositiveFloat,
    },
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

/// The three values for the $PnDATATYPE keyword (3.2+)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum NumType {
    #[display("I")]
    Integer,
    #[display("F")]
    Float,
    #[display("D")]
    Double,
}

impl FromStr for NumType {
    type Err = NumTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "I" => Ok(Self::Integer),
            "F" => Ok(Self::Float),
            "D" => Ok(Self::Double),
            _ => Err(NumTypeError),
        }
    }
}

/// Error when parsing [`NumType`] from string
#[derive(Debug, Error)]
#[error("must be one of 'F', 'D', or 'A'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub struct NumTypeError;

/// The $BYTEORD field in FCS 2.0 and 3.0
///
/// This must be a list of integers belonging to the unordered set {1..N} where
/// N is the total number of bytes. The numbers will be stored as one less the
/// displayed integers to make array indexing easier.
#[derive(Clone, Copy, From, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
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
#[derive(Clone, Copy, From, Display, FromStr, Default, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ByteOrd3_1(pub Endian);

impl From<NoByteOrd<false>> for ByteOrd3_1 {
    fn from(_: NoByteOrd<false>) -> Self {
        Self::default()
    }
}

/// The four allowed values for the $DATATYPE keyword.
#[derive(Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Hash, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum AlphaNumType {
    #[display("A")]
    Ascii,
    #[display("I")]
    Integer,
    #[display("F")]
    Float,
    #[display("D")]
    Double,
}

macro_rules! check_ascii {
    ($res:expr) => {
        if let Ok(dt) = $res
            && dt == Self::Ascii
        {
            let w = Some(DeprecatedDatatypeWarning);
            $res.into_log().set_commutative_warnings(w)
        } else {
            $res.into_log()
        }
    };
}

pub(crate) type LookupDatatypeResult<T> =
    WarningAndErrorResult<T, (), DeprecatedDatatypeWarning, ReqKeyError<T>>;

impl AlphaNumType {
    pub(crate) fn get_req_check_ascii(kws: &StdKeywords) -> LookupDatatypeResult<Self> {
        let res = Self::get_metaroot_req(kws);
        check_ascii!(res)
    }

    pub(crate) fn remove_req_check_ascii(kws: &mut StdKeywords) -> LookupDatatypeResult<Self> {
        let res = Self::remove_metaroot_req(kws);
        check_ascii!(res)
    }
}

impl FromStr for AlphaNumType {
    type Err = AlphaNumTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "I" => Ok(Self::Integer),
            "F" => Ok(Self::Float),
            "D" => Ok(Self::Double),
            "A" => Ok(Self::Ascii),
            _ => Err(AlphaNumTypeError),
        }
    }
}

/// Error when [`AlphaNumType`] is ASCII which is deprecated in 3.1 and 3.2
#[derive(Debug, Error)]
#[error("$DATATYPE=A is deprecated")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::FCSDeprecatedError))]
pub struct DeprecatedDatatypeWarning;

/// Error when parsing [`AlphaNumType`] from string
#[derive(Debug, Error)]
#[error("must be one of 'I', 'F', 'D', or 'A'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub struct AlphaNumTypeError;

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
#[derive(Clone, PartialEq, Display, Debug, Default)]
#[display("0,0")]
pub struct TemporalScaleInner;

#[derive(Default, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum TemporalScaleDiagnostic {
    #[default]
    None,
    Forced(String),
    Trimmed(String),
}

#[derive(From, Clone, PartialEq)]
#[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum AnyScaleDiagnostic {
    Optical(ScaleDiagnostic),
    Temporal(TemporalScaleDiagnostic),
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
#[derive(Clone, PartialEq, Display, Debug, Default)]
pub struct TemporalScale3_0(pub TemporalScaleInner);

impl FromStrWith for TemporalScale3_0 {
    type Err = TemporalScaleError;
    type Payload<'a> = ();
    type Diagnostic = TemporalScaleDiagnostic;

    fn from_str_with(s: &str, (): (), conf: &ReadStdKeywordsConfig) -> FromStrWithResult<Self> {
        if conf.force_linear_scale.time_selected() {
            let d = TemporalScaleDiagnostic::Forced(s.to_owned());
            Ok(DiagnosedKeyword::new(Self(TemporalScaleInner), d))
        } else {
            let flag = conf.trim_intra_value_whitespace;
            TemporalScaleInner::from_str_delim(s, flag).map(|x| {
                let d = x
                    .trimmed
                    .map(TemporalScaleDiagnostic::Trimmed)
                    .unwrap_or_default();
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

impl DisplayMaybe for TemporalScale3_0 {
    fn display_maybe(&self) -> Option<String> {
        Some(self.0.to_string())
    }
}

impl KeywordPairMaybe for TemporalScale3_0 {
    type Inner = Self;
}

/// Error when parsing [`TemporalScaleInner`] from string
#[derive(Debug, Error)]
#[error("time measurement must have linear scaling")]
pub struct TemporalScaleError;

/// The value for the $PnCALIBRATION key (3.1 only)
///
/// This should be formatted like "`<value>,<unit>`"
#[derive(Clone, PartialEq, Debug, Display, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{slope},{unit}")]
pub struct Calibration3_1 {
    pub slope: PositiveFloat,
    pub unit: String,
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
                Ok(Self::new(slope, String::from(unit)))
            }
            _ => Err(CalibrationError::Format(CalibrationFormat3_1)),
        }
    }
}

impl_from_str_with_delim!(Calibration3_1, CalibrationError<CalibrationFormat3_1>);

/// Error when parsing [`Calibration3_1`] from string
#[derive(Debug, Error)]
#[error("must be like 'f,string'")]
pub struct CalibrationFormat3_1;

#[derive(Debug, Display, Error)]
pub enum CalibrationError<C> {
    Float(ParseFloatError),
    Range(RangedFloatError),
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
#[derive(Clone, PartialEq, Debug, Display, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{slope},{offset},{unit}")]
pub struct Calibration3_2 {
    pub slope: PositiveFloat,
    pub offset: f32,
    pub unit: String,
}

impl FromStrDelim for Calibration3_2 {
    type Err = CalibrationError<CalibrationFormat3_2>;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        let x0 = iter.next();
        let x1 = iter.next();
        let x2 = iter.next();
        let x3 = iter.next();
        let (slope, offset, unit) = match (x0, x1, x2, x3) {
            (Some(slope), Some(unit), None, None) => Ok((slope, 0.0, unit)),
            (Some(slope), Some(soffset), Some(unit), None) => {
                let f2 = soffset.parse().map_err(CalibrationError::Float)?;
                Ok((slope, f2, unit))
            }
            _ => Err(CalibrationError::Format(CalibrationFormat3_2)),
        }?;
        Ok(Self::new(
            slope.parse().map_err(CalibrationError::Range)?,
            offset,
            unit.into(),
        ))
    }
}

impl_from_str_with_delim!(Calibration3_2, CalibrationError<CalibrationFormat3_2>);

/// Error when parsing [`Calibration3_2`] from string
#[derive(Debug, Error)]
#[error("must be like 'f1,[f2],string'")]
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
#[cfg_attr(feature = "python", pyerr(crate::python::ConversionError))]
pub struct CalibrationLossError(MeasIndex, f32);

/// The value for the $PnL key (2.0/3.0).
#[derive(Clone, Copy, From, FromStr, Display, Into, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[into(f32, PositiveFloat)]
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

impl DisplayMaybe for Wavelengths {
    fn display_maybe(&self) -> Option<String> {
        if self.0.is_empty() {
            None
        } else {
            Some(self.0.iter().join(","))
        }
    }
}

impl KeywordPairMaybe for Wavelengths {
    type Inner = Self;
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
        let xs = NonEmpty::collect(iter).ok_or(WavelengthsError::Empty)?;
        let ys = xs.try_map(|x| x.parse().map_err(WavelengthsError::Num))?;
        Ok(Self(ys.into()))
    }
}

impl_from_str_with_delim!(Wavelengths, WavelengthsError);

impl Wavelengths {
    pub(crate) fn into_wavelength(
        self,
        i: MeasIndex,
    ) -> DeferredError<Option<Wavelength>, WavelengthsLossError> {
        NonEmpty::from_vec(self.0).map_or(LogResult::new_ok(None), |ws| {
            let n = ws.len();
            let k = Key1::new_i1(i.into());
            let e = WavelengthsLossError(k, n);
            LogResult::new_deferred_if(n == 1, Some(Wavelength(ws.head)), e)
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
#[cfg_attr(feature = "python", pyerr(crate::python::ConversionError))]
pub struct WavelengthsLossError(Key1<Wavelengths>, usize);

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
#[derive(Clone, Copy, From, Into, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[display("{}.{:02}", _0.format(DATETIME_FMT), _0.nanosecond() / 10_000_000)]
pub struct LastModified(pub NaiveDateTime);

const DATETIME_FMT: &str = "%d-%b-%Y %H:%M:%S";

impl FromStrWith for LastModified {
    type Err = LastModifiedError;
    type Payload<'a> = ();
    type Diagnostic = ();

    fn from_str_with(s: &str, (): (), conf: &ReadStdKeywordsConfig) -> FromStrWithResult<Self> {
        if let Some(pat) = conf.last_modified_pattern.as_ref() {
            return NaiveDateTime::parse_from_str(s, pat.as_str())
                .map(Self)
                .map(DiagnosedKeyword::new1)
                .map_err(|_| LastModifiedError::AltFormat(pat.to_owned()));
        }
        let (t, cc) = match &s.split('.').collect::<Vec<_>>()[..] {
            [t] => (*t, ""),
            [t, cc] => (*t, *cc),
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

/// The value for the $ORIGINALITY key (3.1+)
#[derive(Clone, Copy, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum Originality {
    #[display("Original")]
    Original,
    #[display("NonDataModified")]
    NonDataModified,
    #[display("Appended")]
    Appended,
    #[display("DataModified")]
    DataModified,
}

impl FromStr for Originality {
    type Err = OriginalityError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Original" => Ok(Self::Original),
            "NonDataModified" => Ok(Self::NonDataModified),
            "Appended" => Ok(Self::Appended),
            "DataModified" => Ok(Self::DataModified),
            _ => Err(OriginalityError),
        }
    }
}

/// Error when parsing [`Originality`] from string
#[derive(Debug, Error)]
#[error("must be one of 'Original', 'NonDataModified', 'Appended', or 'DataModified'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub struct OriginalityError;

/// The value of the $COMP keyword (3.0 only)
#[derive(Clone, From, Into, Display, AsRef, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
#[as_ref(DMatrix<f32>, Compensation)]
pub struct Compensation3_0(pub Compensation);

impl FromStrWith for Compensation3_0 {
    type Err = ParseCompError;
    type Payload<'a> = ();
    type Diagnostic = Trimmed;

    fn from_str_with(s: &str, (): (), conf: &ReadStdKeywordsConfig) -> FromStrWithResult<Self> {
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
        let js = (par.0..m.nrows()).map(MeasIndex::from);
        NonEmpty::collect(js).map(KeyToIndexLinkError::new_i0)
    }

    pub(crate) fn remove_invalid_link(
        src: &mut Option<Self>,
        par: Par,
    ) -> Option<RemovedIndexLink<Self>> {
        let c = src.as_ref()?;
        let m: &DMatrix<_> = c.as_ref();
        let js = (par.0..m.nrows()).map(MeasIndex::from);
        NonEmpty::collect(js).map(|xs| {
            // ASSUME this won't fail because we filter with ? above
            let v = take(src).unwrap();
            RemovedIndexLink::new(v, xs)
        })
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
#[derive(Clone, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{page},{}", kws.iter().join(","))]
pub struct Unicode {
    pub page: u32,
    pub kws: Vec<String>,
}

impl FromStrDelim for Unicode {
    type Err = UnicodeError;
    const DELIM: char = ',';

    fn from_iter<'a>(mut iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err> {
        if let Some(page) = iter.next().and_then(|x| x.parse().ok()) {
            let kws: Vec<String> = iter.map(String::from).collect();
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
}

/// The value of the $PnTYPE key in optical channels (3.2+)
#[derive(Clone, PartialEq, Debug, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyString))]
pub struct OpticalType(OptionalString);

/// Error when parsing [`OpticalType`] from string
#[derive(Debug, Error)]
#[error("$PnTYPE for time measurement shall not be 'Time' if given")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub struct OpticalTypeError;

const TIME: &str = "Time";

impl FromStr for OpticalType {
    type Err = OpticalTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            TIME => Err(OpticalTypeError),
            _ => Ok(Self(s.to_owned().into())),
        }
    }
}

/// The value of the $PnTYPE key in temporal channels (3.2+)
#[derive(Clone, PartialEq, Debug, Display, Default)]
#[display("{}", TIME)]
pub struct TemporalTypeInner;

impl FromStr for TemporalTypeInner {
    type Err = TemporalTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            TIME => Ok(Self),
            _ => Err(TemporalTypeError),
        }
    }
}

/// Error when parsing [`TemporalType`] from string
#[derive(Debug, Error)]
#[error("$PnTYPE for time measurement shall be 'Time' if given")]
pub struct TemporalTypeError;

/// The value of the $PnFEATURE key (3.2+)
#[derive(Clone, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum Feature {
    #[display("{_0}")]
    Optical(OpticalFeature),
    #[display("{_0}")]
    Other(NonEmptyString),
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

    fn from_str_with(s: &str, (): (), conf: &ReadStdKeywordsConfig) -> FromStrWithResult<Self> {
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

/// The value of the $PnFEATURE key when restricted to area/width/height (3.2+)
#[derive(Clone, Copy, PartialEq, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum OpticalFeature {
    #[display("{}", AREA)]
    Area,
    #[display("{}", WIDTH)]
    Width,
    #[display("{}", HEIGHT)]
    Height,
}

impl FromStr for OpticalFeature {
    type Err = OpticalFeatureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            AREA => Ok(Self::Area),
            WIDTH => Ok(Self::Width),
            HEIGHT => Ok(Self::Height),
            _ => Err(OpticalFeatureError),
        }
    }
}

const AREA: &str = "Area";
const WIDTH: &str = "Width";
const HEIGHT: &str = "Height";

/// Error when parsing [`Feature`] (optical only)
#[derive(Debug, Error)]
#[error("must be one of 'Area', 'Width', or 'Height'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub struct OpticalFeatureError;

/// Error when parsing [`Feature`]
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub enum FeatureError {
    #[error("{0}")]
    Optical(OpticalFeatureError),
    #[error("non-area/width/height feature must not be empty")]
    Other,
}

/// The value of the $RnI key (all versions)
#[derive(Clone, Copy, Display, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum RegionGateIndex<I> {
    Univariate(I),
    Bivariate(IndexPair<I>),
}

/// The two indices of a bivariate gate
#[derive(Clone, Copy, PartialEq, Display, Debug, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{x},{y}")]
pub struct IndexPair<I> {
    pub x: I,
    pub y: I,
}

impl_kind1!(IndexPairFamily, IndexPair);
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

    fn from_str_with(s: &str, (): (), conf: &ReadStdKeywordsConfig) -> FromStrWithResult<Self> {
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
#[derive(Clone, Copy, From, PartialEq, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum MeasOrGateIndex {
    #[display("P{_0}")]
    Meas(MeasIndex),
    #[display("G{_0}")]
    Gate(GateIndex),
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
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
pub enum MeasOrGateIndexError {
    #[error("{0}")]
    Int(ParseIntError),
    #[error("must be prefixed with either 'P' or 'G'")]
    Format,
}

/// Index for $RnI (3.2)
///
/// This is just a measurement index with 'P' in front of it
#[derive(Clone, Copy, From, PartialEq, Into, AsMut, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[from(MeasIndex, usize)]
#[into(MeasIndex, usize)]
#[display("P{_0}")]
pub struct PrefixedMeasIndex(pub MeasIndex);

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
#[derive(Display, Debug, PartialEq)]
pub enum RegionWindow {
    #[display("{_0}")]
    Univariate(UniGate),
    #[display("{}", _0.iter().join(";"))]
    Bivariate(NonEmpty<Vertex>),
}

/// A vertex on a polygon gate
#[derive(Clone, PartialEq, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{x},{y}")]
pub struct Vertex {
    pub x: BigDecimal,
    pub y: BigDecimal,
}

/// A gate on one dimension with lower and upper bound
#[derive(Clone, PartialEq, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("{lower},{upper}")]
pub struct UniGate {
    pub lower: BigDecimal,
    pub upper: BigDecimal,
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
        if let Some(xs) = NonEmpty::collect(ss) {
            if xs.tail.is_empty() {
                UniGate::from_str_delim(xs.head, trim_whitespace)
                    .map(|x| x.fmap_once(RegionWindow::Univariate))
            } else {
                let mut was_trimmed = false;
                let ys = xs.try_map(|x| Vertex::from_str_delim(x, trim_whitespace))?;
                let zs = ys.map(|x| {
                    was_trimmed = was_trimmed || x.trimmed.is_some();
                    x.native
                });
                let d = was_trimmed.then(|| original.to_owned());
                Ok(TrimmedKeyword::new(Self::Bivariate(zs), d))
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
#[derive(Clone, PartialEq, Display, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
pub enum Gating {
    #[display("R{_0}")]
    Region(RegionIndex),
    #[display("(NOT {_0})")]
    Not(Box<Self>),
    #[display("({_0} AND {_1})")]
    And(Box<Self>, Box<Self>),
    #[display("({_0} OR {_1})")]
    Or(Box<Self>, Box<Self>),
}

impl Gating {
    pub(crate) fn region_indices(&self) -> NonEmpty<RegionIndex> {
        let xs = match self {
            Self::Region(x) => NonEmpty::new(*x),
            Self::Not(x) => Self::region_indices(x),
            Self::And(x, y) | Self::Or(x, y) => {
                let mut acc = Self::region_indices(x);
                acc.extend(Self::region_indices(y));
                acc
            }
        };
        FCSNonEmpty::from(xs).unique().0
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
#[cfg_attr(feature = "python", pyerr(crate::python::ParseKeywordValueError))]
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
#[derive(Clone, Copy, PartialEq, Eq, Hash, From, Debug, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[from(Chars)]
pub enum Width {
    #[display("{_0}")]
    Fixed(BitsOrChars),
    #[display("*")]
    Variable,
}

/// The value of the $PnR key.
#[derive(Clone, From, Display, FromStr, Add, Sub, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[from(u8, u16, u32, u64, BigDecimal)]
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
#[derive(Clone, From, Display, FromStr, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct GateShortname(pub Shortname);

/// The value of the $GmR key
#[derive(Clone, From, Display, FromStr, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
#[from(u64)]
pub struct GateRange(pub Range);

macro_rules! impl_non_neg_float {
    ($(#[$meta:meta])* $t:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, From, Display, FromStr, Into, PartialEq, Debug)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        #[into(NonNegFloat, f32)]
        pub struct $t(pub NonNegFloat);

        impl_newtype_try_from!($t, NonNegFloat, f32, RangedFloatError);
    };
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
#[derive(Clone, Copy, Display, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct GateScale(pub Scale);

// use the same fix we use for PnE here
impl FromStrWith for GateScale {
    type Err = ScaleError;
    type Payload<'a> = ();
    type Diagnostic = ScaleDiagnostic;

    fn from_str_with(s: &str, data: (), conf: &ReadStdKeywordsConfig) -> FromStrWithResult<Self> {
        Scale::from_str_with(s, data, conf).map(|x| x.first_once(Self))
    }
}

/// The value of the $CYT key (3.2).
///
/// This is not a normal string because it is required in 3.2 and thus cannot
/// be empty.
#[derive(Clone, Display, FromStr, PartialEq, Into, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct Cyt3_2(pub NonEmptyString);

impl From<Cyt3_2> for Cyt {
    fn from(value: Cyt3_2) -> Self {
        Self(OptionalString(value.0.into()))
    }
}

impl TryFrom<Cyt> for Cyt3_2 {
    type Error = NoCytError;

    fn try_from(value: Cyt) -> Result<Self, Self::Error> {
        (value.0).0.parse().map_err(|_| NoCytError)
    }
}

/// Error when parsing [`Cyt3_2`] from string
#[derive(Debug, Error)]
#[error("$CYT is missing")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ConversionError))]
pub struct NoCytError;

/// The value for the $UNSTAINEDCENTERS key (3.2+)
#[derive(Clone, Into, PartialEq, Debug, Default)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
pub struct UnstainedCenters(pub HashMap<Shortname, f32>);

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
        let ns = self
            .0
            .keys()
            .filter(|n| names.as_ref().contains(n))
            .cloned();
        NonEmpty::collect(ns).map(|js| ExistingNamedLinkError::new(Key0::default(), js))
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

impl DisplayMaybe for UnstainedCenters {
    fn display_maybe(&self) -> Option<String> {
        if self.0.is_empty() {
            None
        } else {
            let n = self.0.len();
            let k = self.0.keys().join(",");
            let v = self.0.values().join(",");
            Some(format!("{n},{k},{v}"))
        }
    }
}

impl KeywordPairMaybe for UnstainedCenters {
    type Inner = Self;
}

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
    VersionEQ(Version),
    VersionLE(Version),
    VersionGE(Version),
    Version3_0or3_1,
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
        let minimal_version = |v| (current_version < v).then_some(ExtraKeywordClass::VersionGE(v));
        let maximal_version = |v| (v < current_version).then_some(ExtraKeywordClass::VersionLE(v));
        let eq_version = |v| (current_version != v).then_some(ExtraKeywordClass::VersionEQ(v));

        let maximal_indexed_version = |v, i: MeasIndex| {
            if usize::from(i) >= par.0 {
                Some(ExtraKeywordClass::HyperPar)
            } else {
                maximal_version(v)
            }
        };

        match AnyKeywordClass::classify_keyword(key) {
            AnyKeywordClass::Root(r) => match r {
                RootKeywordClass::Beginanalysis
                | RootKeywordClass::Beginstext
                | RootKeywordClass::Begindata
                | RootKeywordClass::Endanalysis
                | RootKeywordClass::Endstext
                | RootKeywordClass::Enddata => minimal_version(Version::FCS3_0),
                RootKeywordClass::Timestep => {
                    if current_version < Version::FCS3_0 {
                        Some(ExtraKeywordClass::VersionGE(Version::FCS3_0))
                    } else {
                        Some(ExtraKeywordClass::UnusedTimestep)
                    }
                }
                RootKeywordClass::OptGE3_1 => minimal_version(Version::FCS3_1),
                RootKeywordClass::OptGE3_2 => minimal_version(Version::FCS3_2),
                RootKeywordClass::OptEQ3_0or3_1 => (current_version == Version::FCS3_0
                    || current_version == Version::FCS3_1)
                    .then_some(ExtraKeywordClass::Version3_0or3_1),
                RootKeywordClass::OptEQ3_0 => eq_version(Version::FCS3_0),
                RootKeywordClass::OptLE3_1 => maximal_version(Version::FCS3_1),
                _ => None,
            },
            AnyKeywordClass::MeasOptGE3_0(i) => maximal_indexed_version(Version::FCS3_0, i),
            AnyKeywordClass::MeasOptGE3_1(i) => maximal_indexed_version(Version::FCS3_1, i),
            AnyKeywordClass::MeasOptGE3_2(i) => maximal_indexed_version(Version::FCS3_2, i),
            AnyKeywordClass::MeasOptEq3_0or3_1(i) => {
                if usize::from(i) >= par.0 {
                    Some(ExtraKeywordClass::HyperPar)
                } else if current_version == Version::FCS3_0 || current_version == Version::FCS3_1 {
                    Some(ExtraKeywordClass::Version3_0or3_1)
                } else {
                    None
                }
            }
            AnyKeywordClass::GateOptLE3_1(i) => {
                (usize::from(i) >= gate.0).then_some(ExtraKeywordClass::HyperGate)
            }
            AnyKeywordClass::Dfc(x, y) => {
                if usize::from(x) >= par.0 || usize::from(y) >= par.0 {
                    Some(ExtraKeywordClass::HyperPar)
                } else {
                    eq_version(Version::FCS2_0)
                }
            }
            AnyKeywordClass::Scale(i)
            | AnyKeywordClass::Shortname(i)
            | AnyKeywordClass::Wavelength(i)
            | AnyKeywordClass::MeasAny(i) => {
                (usize::from(i) >= par.0).then_some(ExtraKeywordClass::HyperPar)
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
        let all_versions = [
            Version::FCS2_0,
            Version::FCS3_0,
            Version::FCS3_1,
            Version::FCS3_2,
        ];
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
            macro_rules! go_version {
                ($vs:expr) => {
                    let e = KeywordOtherVersionError::new(k.clone(), current_version, $vs);
                    other_version_es.push(e);
                    other_version.insert(k, v);
                };
            }
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
                    ExtraKeywordClass::VersionEQ(ver) => {
                        let vs = NonEmpty::new(ver);
                        go_version!(vs);
                    }
                    ExtraKeywordClass::VersionLE(ver) => {
                        let mut vs = NonEmpty::new(ver);
                        vs.extend(all_versions.iter().filter(|&&x| x < ver).copied());
                        go_version!(vs);
                    }
                    ExtraKeywordClass::VersionGE(ver) => {
                        let mut vs = NonEmpty::new(ver);
                        vs.extend(all_versions.iter().filter(|&&x| x > ver).copied());
                        go_version!(vs);
                    }
                    ExtraKeywordClass::Version3_0or3_1 => {
                        let vs = NonEmpty::from((Version::FCS3_0, vec![Version::FCS3_1]));
                        go_version!(vs);
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
#[cfg_attr(feature = "python", pyerr(crate::python::ExtraKeywordError))]
pub struct PseudostandardError(pub StdKey);

/// Error denoting that measurement keyword within standard but above $PAR was found
#[derive(Debug, Error, new)]
#[error("measurement keyword is part of standard but outside $PAR ({par}): {key}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ExtraKeywordError))]
pub struct HyperParError {
    pub par: Par,
    pub key: StdKey,
}

/// Error denoting that gating keyword within standard but above $GATE was found
#[derive(Debug, Error, new)]
#[error("gating keyword is part of standard but outside $GATE ({gate}): {key}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ExtraKeywordError))]
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
#[cfg_attr(feature = "python", pyerr(crate::python::ExtraKeywordError))]
pub struct KeywordOtherVersionError {
    pub key: StdKey,
    pub current: Version,
    pub others: NonEmpty<Version>,
}

/// Error denoting that $TIMESTEP was unused and possibly should have been
#[derive(Debug, Error)]
#[error("$TIMESTEP found, this may indicate a time measurement exists but was not identified")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::ExtraKeywordError))]
pub struct TimestepFoundError;

macro_rules! newtype_string {
    ($t:ident) => {
        #[derive(Clone, FromStr, From, Into, PartialEq, Debug, Default, AsRef)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
        #[as_ref(str)]
        pub struct $t(pub OptionalString);

        impl DisplayMaybe for $t {
            fn display_maybe(&self) -> Option<String> {
                self.0.display_maybe()
            }
        }

        impl KeywordPairMaybe for $t {
            type Inner = Self;
        }

        impl CheckMaybe for $t {
            type Inner = Self;
        }
    };
}

macro_rules! newtype_int {
    ($t:ident, $type:ty) => {
        #[derive(
            Clone, Copy, Display, FromStr, From, Into, PartialEq, PartialOrd, Eq, Ord, Debug,
        )]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        pub struct $t(pub $type);
    };
}

macro_rules! impl_display_maybe_self {
    ($t:ident) => {
        impl DisplayMaybe for $t {
            fn display_maybe(&self) -> Option<String> {
                self.0.display_maybe()
            }
        }

        impl CheckMaybe for $t {
            type Inner = Self;
        }

        impl KeywordPairMaybe for $t {
            type Inner = Self;
        }
    };
}

macro_rules! newtype_opt_int {
    ($t:ident, $inner:ident) => {
        #[derive(Clone, Default, PartialEq, Eq, FromStr, Debug)]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        pub struct $t(pub OptionalInt<$inner>);

        impl_display_maybe_self!($t);
    };
}

macro_rules! newtype_opt_bool {
    ($t:ident, $inner:ident) => {
        #[derive(Clone, PartialEq, Debug, Default, From, Into)]
        #[cfg_attr(feature = "python", derive(IntoPyObject, FromInnerPyObject))]
        #[cfg_attr(feature = "serde", derive(Serialize))]
        #[from(bool)]
        #[into(bool)]
        pub struct $t(pub OptionalZST<$inner>);

        impl_display_maybe_self!($t);
    };
}

macro_rules! kw_meta {
    ($t:ident, $k:expr) => {
        impl Key for $t {
            const C: &'static str = $k;
        }
    };
}

macro_rules! kw_meas {
    ($t:ident, $sfx:expr) => {
        impl IndexedKey for $t {
            const PREFIX: &'static str = MEAS_KW_PREFIX;
            const SUFFIX: &'static str = $sfx;
        }
    };
}

macro_rules! kw_meta_string {
    ($t:ident, $kw:expr) => {
        newtype_string!($t);

        impl Key for $t {
            const C: &'static str = $kw;
        }
    };
}

macro_rules! kw_meta_int {
    ($t:ident, $type:ident, $kw:expr) => {
        newtype_int!($t, $type);

        impl Key for $t {
            const C: &'static str = $kw;
        }
    };
}

macro_rules! kw_meas_string {
    ($t:ident, $sfx:expr) => {
        newtype_string!($t);
        kw_meas!($t, $sfx);
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
    ($t:ident, $sfx:expr) => {
        kw_meta!($t, $sfx);
        req_meta!($t);
    };
}

macro_rules! kw_opt_meta {
    ($t:ident, $sfx:expr, $outer:path) => {
        kw_meta!($t, $sfx);
        opt_meta!($t, $outer);
    };
}

macro_rules! kw_req_meas {
    ($t:ident, $sfx:expr) => {
        kw_meas!($t, $sfx);
        req_meas!($t);
    };
}

macro_rules! kw_opt_meas {
    ($t:ident, $sfx:expr, $outer:path) => {
        kw_meas!($t, $sfx);
        opt_meas!($t, $outer);
    };
}

macro_rules! kw_opt_meta_string {
    ($t:ident, $sfx:expr) => {
        kw_meta_string!($t, $sfx);
        opt_meta!($t, Self);
    };
}

macro_rules! kw_opt_meas_string {
    ($t:ident, $sfx:expr) => {
        kw_meas_string!($t, $sfx);
        opt_meas!($t, Self);
    };
}

macro_rules! kw_req_meta_int {
    ($t:ident, $type:ident, $sfx:expr) => {
        kw_meta_int!($t, $type, $sfx);
        req_meta!($t);
    };
}

macro_rules! kw_opt_meta_int {
    ($t:ident, $type:ident, $sfx:expr) => {
        kw_meta_int!($t, $type, $sfx);
        opt_meta!($t, Option<Self>);
    };
}

macro_rules! kw_time {
    ($outer:ident, $wrap:ident, $inner:ident, $err:ident, $key:expr) => {
        type $outer = $wrap<$inner>;

        kw_opt_meta!($outer, $key, Option<Self>);

        impl From<NaiveTime> for $outer {
            fn from(value: NaiveTime) -> Self {
                Xtim($inner(value))
            }
        }
    };
}

macro_rules! kw_opt_gate {
    ($t:ident, $sfx:expr, $outer:path) => {
        impl IndexedKey for $t {
            const PREFIX: &'static str = GATE_KW_PREFIX;
            const SUFFIX: &'static str = $sfx;
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

macro_rules! kw_opt_region {
    ($t:ident, $sfx:expr) => {
        impl IndexedKey for $t {
            const PREFIX: &'static str = REGION_KW_PREFIX;
            const SUFFIX: &'static str = $sfx;
        }
        opt_meas!($t, Option<Self>);
    };
}

macro_rules! meas_opt_zst {
    ($t:ident, $sym:expr, $inner:ident) => {
        newtype_opt_bool!($t, $inner);
        kw_opt_meas!($t, $sym, Self);
    };
}

macro_rules! kw_opt_meta_opt_int {
    ($t:ident, $inner:ident, $sym:expr) => {
        newtype_opt_int!($t, $inner);
        kw_opt_meta!($t, $sym, Self);
    };
}

// all versions
kw_req_meta!(AlphaNumType, DATATYPE_KW);
kw_opt_meta_int!(Abrt, u32, ABRT_KW);
kw_opt_meta_string!(Cytsn, CYTSN_KW);
kw_opt_meta_string!(Com, COM_KW);
kw_opt_meta_string!(Cells, CELLS_KW);
kw_opt_meta!(FCSDate, DATE_KW, Option<Self>);
kw_opt_meta_string!(Exp, EXP_KW);
kw_opt_meta_string!(Fil, FIL_KW);
kw_opt_meta_string!(Inst, INST_KW);
kw_opt_meta_int!(Lost, u32, LOST_KW);
kw_opt_meta_string!(Op, OP_KW);
kw_req_meta_int!(Par, usize, PAR_KW);
kw_opt_meta_string!(Proj, PROJ_KW);
kw_opt_meta_string!(Smno, SMNO_KW);
kw_opt_meta_string!(Src, SRC_KW);
kw_opt_meta_string!(Sys, SYS_KW);
kw_opt_meta!(Trigger, TR_KW, Option<Self>);

// time for 2.0
kw_time!(Btim2_0, Btim, FCSTime, FCSTimeError, BTIM_KW);
kw_time!(Etim2_0, Etim, FCSTime, FCSTimeError, ETIM_KW);

// time for 3.0
kw_time!(Btim3_0, Btim, FCSTime60, FCSTime60Error, BTIM_KW);
kw_time!(Etim3_0, Etim, FCSTime60, FCSTime60Error, ETIM_KW);

// time for 3.1-3.2
kw_time!(Btim3_1, Btim, FCSTime100, FCSTime100Error, BTIM_KW);
kw_time!(Etim3_1, Etim, FCSTime100, FCSTime100Error, ETIM_KW);

// 3.0 only
kw_opt_meta!(Compensation3_0, COMP_KW, Option<Self>);
kw_opt_meta!(Unicode, UNICODE_KW, Option<Self>);

// for 3.0+
kw_req_meta!(Timestep, TIMESTEP_KW);

// for 3.1+
kw_opt_meta_string!(LastModifier, LAST_MODIFIER_KW);
kw_opt_meta!(Originality, ORIGINALITY_KW, Option<Self>);
kw_opt_meta!(LastModified, LAST_MODIFIED_KW, Option<Self>);

kw_opt_meta_string!(Plateid, PLATEID_KW);
kw_opt_meta_string!(Platename, PLATENAME_KW);
kw_opt_meta_string!(Wellid, WELLID_KW);

kw_opt_meta!(Spillover, SPILLOVER_KW, Option<Self>);

kw_opt_meta!(Vol, VOL_KW, Option<Self>);

// for 3.2+
kw_opt_meta_string!(Carrierid, CARRIERID_KW);
kw_opt_meta_string!(Carriertype, CARRIERTYPE_KW);
kw_opt_meta_string!(Locationid, LOCATIONID_KW);

kw_opt_meta!(BeginDateTime, BEGINDATETIME_KW, Option<Self>);
kw_opt_meta!(EndDateTime, ENDDATETIME_KW, Option<Self>);
kw_opt_meta!(UnstainedCenters, UNSTAINEDCENTERS_KW, Self);

kw_opt_meta_string!(UnstainedInfo, UNSTAINEDINFO_KW);

kw_opt_meta_string!(Flowrate, FLOWRATE_KW);

// version-specific
kw_opt_meta_int!(Tot, usize, TOT_KW); // optional in 2.0
req_meta!(Tot); // required in 3.0+

kw_req_meta!(Mode, MODE_KW); // for 2.0-3.1
kw_opt_meta!(Mode3_2, MODE_KW, Option<Self>); // for 3.2+

kw_opt_meta_string!(Cyt, CYT_KW); // optional for 2.0-3.1
kw_req_meta!(Cyt3_2, CYT_KW); // required for 3.2+

kw_req_meta!(ByteOrd2_0, BYTEORD_KW); // 2.0/3.0
kw_req_meta!(ByteOrd3_1, BYTEORD_KW); // 3.1+

// all versions
kw_req_meas!(Width, WIDTH_KW_SUFFIX);
kw_opt_meas_string!(Filter, FILTER_KW_SUFFIX);
kw_opt_meas!(Power, POWER_KW_SUFFIX, Option<Self>);
kw_opt_meas!(PercentEmitted, PERCENT_EMITTED_KW_SUFFIX, Option<Self>);
kw_req_meas!(Range, RANGE_KW_SUFFIX);
kw_opt_meas_string!(Longname, LONGNAME_KW_SUFFIX);
kw_opt_meas_string!(DetectorType, DET_TYPE_KW_SUFFIX);
kw_opt_meas!(DetectorVoltage, DET_VOLTAGE_KW_SUFFIX, Option<Self>);

// 3.0+
kw_opt_meas!(Gain, GAIN_KW_SUFFIX, Option<Self>);

// 3.1+
kw_opt_meas!(Display, DISPLAY_KW_SUFFIX, Option<Self>);

// 3.2+
kw_opt_meas!(Feature, FEATURE_KW_SUFFIX, Option<Self>);
meas_opt_zst!(TemporalType, TYPE_KW_SUFFIX, TemporalTypeInner);

impl FromStr for TemporalType {
    type Err = TemporalTypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<TemporalTypeInner>()
            .map(Some)
            .map(OptionalZST::from)
            .map(Self)
    }
}

kw_opt_meas!(NumType, DATATYPE_KW_SUFFIX, Option<Self>);
kw_opt_meas_string!(Analyte, ANALYTE_KW_SUFFIX);
kw_opt_meas_string!(Tag, TAG_KW_SUFFIX);
kw_opt_meas_string!(DetectorName, DET_NAME_KW_SUFFIX);

impl_display_maybe_self!(OpticalType);
kw_opt_meas!(OpticalType, TYPE_KW_SUFFIX, Self);

// version specific
kw_opt_meas!(Shortname, SHORTNAME_KW_SUFFIX, Option<Self>); // optional for 2.0/3.0
req_meas!(Shortname); // required for 3.1+

kw_opt_meas!(Scale, SCALE_KW_SUFFIX, Option<Self>); // optional for 2.0
req_meas!(Scale); // required for 3.0+

meas_opt_zst!(TemporalScale2_0, SCALE_KW_SUFFIX, TemporalScaleInner); // optional for 2.0

impl FromStrWith for TemporalScale2_0 {
    type Err = TemporalScaleError;
    type Payload<'a> = ();
    type Diagnostic = TemporalScaleDiagnostic;

    fn from_str_with(s: &str, (): (), conf: &ReadStdKeywordsConfig) -> FromStrWithResult<Self> {
        let go = |x| Self(OptionalZST(Some(x)));
        if conf.force_linear_scale.time_selected() {
            let d = TemporalScaleDiagnostic::Forced(s.to_owned());
            Ok(DiagnosedKeyword::new(go(TemporalScaleInner), d))
        } else {
            let flag = conf.trim_intra_value_whitespace;
            TemporalScaleInner::from_str_delim(s, flag).map(|x| {
                let d = x
                    .trimmed
                    .map(TemporalScaleDiagnostic::Trimmed)
                    .unwrap_or_default();
                DiagnosedKeyword::new(go(x.native), d)
            })
        }
    }
}

kw_req_meas!(TemporalScale3_0, SCALE_KW_SUFFIX); // required for 3.0+

kw_opt_meas!(Wavelength, WAVELENGTH_KW_SUFFIX, Option<Self>); // scaler in 2.0/3.0
kw_opt_meas!(Wavelengths, WAVELENGTH_KW_SUFFIX, Self); // vector in 3.1+

kw_opt_meas!(Calibration3_1, CALIBRATION_KW_SUFFIX, Option<Self>); // 3.1 doesn't have offset
kw_opt_meas!(Calibration3_2, CALIBRATION_KW_SUFFIX, Option<Self>); // 3.2+ includes offset

// 2.0 compensation matrix
#[derive(Debug)]
pub struct Dfc;

impl BiIndexedKey for Dfc {
    const PREFIX: &'static str = "DFC";
    const MIDDLE: &'static str = "TO";
    const SUFFIX: &'static str = "";
}

impl Dfc {
    pub(crate) fn lookup(
        kws: &mut StdKeywords,
        k: Key2<Self>,
    ) -> Result<Option<f32>, LookupDfcError> {
        kws.remove(&k.as_std()).map_or(Ok(None), |v| {
            v.parse::<f32>()
                .map_err(|e| ParseKeyError::new(e, k, v.clone()))
                .map(Some)
        })
    }
}

pub type LookupDfcError = ParseKeyError<ParseFloatError, Dfc, BiIndex>;

// 3.0/3.1 subsets
kw_opt_meta_int!(CSMode, usize, CSMODE_KW);

kw_opt_meta_opt_int!(CSTot, u32, CSTOT_KW);
kw_opt_meta_opt_int!(CSVBits, u32, CSVBITS_KW);

// $CSVnFLAG (3.0/3.1)
newtype_int!(CSVFlag, u32);
opt_meas!(CSVFlag, Option<Self>);

impl IndexedKey for CSVFlag {
    const PREFIX: &'static str = "CSV";
    const SUFFIX: &'static str = "FLAG";
}

// $PKn (2.0-3.1)
newtype_int!(PeakBin, u32);
opt_meas!(PeakBin, Option<Self>);

impl IndexedKey for PeakBin {
    const PREFIX: &'static str = "PK";
    const SUFFIX: &'static str = "";
}

// $PKNn (2.0-3.1)
newtype_int!(PeakIndex, MeasIndex);
opt_meas!(PeakIndex, Option<Self>);

impl IndexedKey for PeakIndex {
    const PREFIX: &'static str = "PKN";
    const SUFFIX: &'static str = "";
}

// 2.0-3.1 gating parameters
kw_opt_meta_int!(Gate, usize, GATE_KW);

kw_opt_gate_other!(GateScale, SCALE_KW_SUFFIX);
kw_opt_gate_string!(GateFilter, FILTER_KW_SUFFIX);
kw_opt_gate_other!(GatePercentEmitted, PERCENT_EMITTED_KW_SUFFIX);
kw_opt_gate_other!(GateRange, RANGE_KW_SUFFIX);
kw_opt_gate_other!(GateShortname, SHORTNAME_KW_SUFFIX);
kw_opt_gate_string!(GateLongname, LONGNAME_KW_SUFFIX);
kw_opt_gate_string!(GateDetectorType, DET_TYPE_KW_SUFFIX);
kw_opt_gate_other!(GateDetectorVoltage, DET_VOLTAGE_KW_SUFFIX);
kw_opt_meta!(Gating, GATING_KW, Option<Self>);

kw_opt_region!(RegionWindow, REGION_WINDOW_KW_SUFFIX);

impl<I> IndexedKey for RegionGateIndex<I> {
    const PREFIX: &'static str = REGION_KW_PREFIX;
    const SUFFIX: &'static str = REGION_INDEX_KW_SUFFIX;
}

impl<I> Optional for RegionGateIndex<I> {
    type Outer = Option<Self>;
}
impl<I> OptIndexedKey for RegionGateIndex<I> where I: fmt::Display + FromStr {}

// offsets for all versions
kw_req_meta!(Nextdata, NEXTDATA_KW);
opt_meta!(Nextdata, Option<Self>);

macro_rules! kw_offset {
    ($(#[$attr:meta])* $t:ident, $key:expr) => {
        $(#[$attr])*
        #[derive(Display, From, Into, FromStr, Debug, Clone, Copy)]
        #[into(u64, i128, UintZeroPad20)]
        pub struct $t(pub UintZeroPad20);

        kw_req_meta!($t, $key);
    };
}

kw_offset!(
    /// Value for $BEGINANALYSIS key (3.0-3.2)
    Beginanalysis,
    BEGINANALYSIS_KW
);
kw_offset!(
    /// Value for $BEGINDATA key (3.0-3.2)
    Begindata,
    BEGINDATA_KW
);
kw_offset!(
    /// Value for $BEGINSTEXT key (3.0-3.2)
    Beginstext,
    BEGINSTEXT_KW
);
kw_offset!(
    /// Value for $ENDANALYSIS key (3.0-3.2)
    Endanalysis,
    ENDANALYSIS_KW
);
kw_offset!(
    /// Value for $ENDDATA key (3.0-3.2)
    Enddata,
    ENDDATA_KW
);
kw_offset!(
    /// Value for $ENDSTEXT (3.0-3.2)
    Endstext,
    ENDSTEXT_KW
);

opt_meta!(Beginanalysis, Option<Self>);
opt_meta!(Endanalysis, Option<Self>);
opt_meta!(Beginstext, Option<Self>);
opt_meta!(Endstext, Option<Self>);

#[cfg(test)]
mod tests {
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
        assert_from_to_str_maybe_with::<Wavelengths>("0.5", (), &conf);
        assert_from_to_str_maybe_with::<Wavelengths>("0.5,2", (), &conf);
        assert!(Wavelengths::from_str_with("x", (), &conf).is_err());
    }

    #[test]
    fn pnl_3_1_commas() {
        let mut conf = ReadStdKeywordsConfig::default();
        let v = "1, 2";
        assert!(Wavelengths::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_eq!(
            Wavelengths::from_str_with(v, (), &conf)
                .unwrap()
                .native
                .display_maybe(),
            Some("1,2".into())
        );
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
        assert_from_to_str_maybe::<OpticalType>("Forward Scatter");
        assert_from_to_str_maybe::<OpticalType>("Side Scatter");
        assert_from_to_str_maybe::<OpticalType>("Raw Fluorescence");
        assert_from_to_str_maybe::<OpticalType>("Unmixed Fluorescence");
        assert_from_to_str_maybe::<OpticalType>("Mass");
        assert_from_to_str_maybe::<OpticalType>("Electronic Volume");
        assert_from_to_str_maybe::<OpticalType>("Index");
        assert_from_to_str_maybe::<OpticalType>("Classification");
        assert_from_to_str_maybe::<OpticalType>("Spongebob");
    }

    #[test]
    fn pntype_time() {
        assert_from_to_str_maybe::<TemporalType>("Time");
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
        assert_from_to_str_maybe_with::<UnstainedCenters>("1,X,0", (), &conf);
    }

    #[test]
    fn unstained_centers_commas() {
        let v = "1, X , 0";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(UnstainedCenters::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_eq!(
            UnstainedCenters::from_str_with(v, (), &conf)
                .unwrap()
                .native
                .display_maybe(),
            Some("1,X,0".into())
        );
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
        assert_from_to_str::<ByteOrd2_0>("1,2,3,4");
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
        assert_from_to_str_with::<Scale>("0,0", (), &conf);
        assert_from_to_str_with::<Scale>("4.5,0.01", (), &conf);
    }

    #[test]
    fn scale_zero_log() {
        let v = "4.5,0";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(Scale::from_str_with(v, (), &conf).is_err());
        conf.fix_log_scale_offsets = true.into();
        assert_from_to_str_almost_with::<Scale>(v, "4.5,1", (), &conf);
    }

    #[test]
    fn scale_commas() {
        let v = "0, 0";
        let mut conf = ReadStdKeywordsConfig::default();
        assert!(Scale::from_str_with(v, (), &conf).is_err());
        conf.trim_intra_value_whitespace = true.into();
        assert_from_to_str_almost_with::<Scale>(v, "0,0", (), &conf);
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
    use crate::text::ranged_float::PositiveFloat;
    use crate::validated::shortname::Shortname;

    use super::{
        ByteOrd2_0, Calibration3_1, Calibration3_2, Display, IndexPair, Scale, ScaleDiagnostic,
        TemporalScaleDiagnostic, Trigger, UniGate, Unicode, Vertex,
    };

    use pyo3::conversion::IntoPyObjectExt as _;
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use pyo3::types::PyTuple;
    use std::num::NonZeroU8;

    // $BYTEORD is a list of integers
    impl<'py> FromPyObject<'py> for ByteOrd2_0 {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let xs: Vec<NonZeroU8> = ob.extract()?;
            let ret = Self::try_from(&xs[..])?;
            Ok(ret)
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
            let (slope, unit): (PositiveFloat, String) = ob.extract()?;
            Ok(Self { slope, unit })
        }
    }

    impl<'py> IntoPyObject<'py> for Calibration3_1 {
        type Target = PyTuple;
        type Output = Bound<'py, <(PositiveFloat, String) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.slope, self.unit).into_pyobject(py)
        }
    }

    // $PnCALIBRATION (3.2) as (f32, f32, String) tuple in python
    impl<'py> FromPyObject<'py> for Calibration3_2 {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (slope, offset, unit): (PositiveFloat, f32, String) = ob.extract()?;
            Ok(Self {
                slope,
                offset,
                unit,
            })
        }
    }

    impl<'py> IntoPyObject<'py> for Calibration3_2 {
        type Target = PyTuple;
        type Output = Bound<'py, <(PositiveFloat, f32, String) as IntoPyObject<'py>>::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (self.slope, self.offset, self.unit).into_pyobject(py)
        }
    }

    // $UNICODE (3.0) as a tuple like (f32, [String]) in python
    impl<'py> FromPyObject<'py> for Unicode {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            let (page, kws): (u32, Vec<String>) = ob.extract()?;
            Ok(Self { page, kws })
        }
    }

    impl<'py> IntoPyObject<'py> for Unicode {
        type Target = PyTuple;
        type Output = Bound<'py, <(u32, Vec<String>) as IntoPyObject<'py>>::Target>;
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

    impl<'py> FromPyObject<'py> for ScaleDiagnostic {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            if let Some((x, y)) = ob.extract::<Option<(String, String)>>()? {
                match y.as_str() {
                    "forced" => Ok(Self::Forced(x)),
                    "log" => Ok(Self::LogFixed(x)),
                    "trimmed" => Ok(Self::Trimmed(x)),
                    "trimmed_log" => Ok(Self::TrimmedLogFixed(x)),
                    _ => Err(PyValueError::new_err(
                        "second string must be 'forced', 'log', 'trimmed', \
                         or 'trimmed_log'",
                    )),
                }
            } else {
                Ok(Self::None)
            }
        }
    }

    impl<'py> FromPyObject<'py> for TemporalScaleDiagnostic {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            if let Some((x, y)) = ob.extract::<Option<(String, String)>>()? {
                match y.as_str() {
                    "forced" => Ok(Self::Forced(x)),
                    "trimmed" => Ok(Self::Trimmed(x)),
                    _ => Err(PyValueError::new_err(
                        "second string must be 'forced' or 'trimmed'",
                    )),
                }
            } else {
                Ok(Self::None)
            }
        }
    }

    impl<'py> IntoPyObject<'py> for ScaleDiagnostic {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            let ret = match self {
                Self::None => None,
                Self::Forced(x) => Some((x, "forced")),
                Self::LogFixed(x) => Some((x, "log")),
                Self::Trimmed(x) => Some((x, "trimmed")),
                Self::TrimmedLogFixed(x) => Some((x, "trimmed_log")),
            };
            ret.into_bound_py_any(py)
        }
    }

    impl<'py> IntoPyObject<'py> for TemporalScaleDiagnostic {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            let ret = match self {
                Self::None => None,
                Self::Forced(x) => Some((x, "forced")),
                Self::Trimmed(x) => Some((x, "trimmed")),
            };
            ret.into_bound_py_any(py)
        }
    }
}
