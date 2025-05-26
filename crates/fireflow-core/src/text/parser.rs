use crate::core::{CarrierData, ModificationData, PlateData, UnstainedData};
use crate::error::*;
use crate::macros::{enum_from, enum_from_disp, match_many_to_one};
use crate::validated::nonstandard::*;
use crate::validated::pattern::*;
use crate::validated::shortname::*;
use crate::validated::standard::*;

use super::byteord::*;
use super::compensation::*;
use super::datetimes::*;
use super::keywords::*;
use super::modified_date_time::*;
use super::named_vec::*;
use super::optionalkw::*;
use super::range::*;
use super::ranged_float::RangedFloatError;
use super::scale::*;
use super::spillover::*;
use super::timestamps::*;
use super::unstainedcenters::*;

use nalgebra::DMatrix;
use std::convert::Infallible;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;

pub(crate) type LookupResult<V> = DeferredResult<V, ParseKeysWarning, ParseKeysError>;

pub(crate) type LookupTentative<V> = Tentative<V, ParseKeysWarning, ParseKeysError>;

pub(crate) fn lookup_meta_req<V>(kws: &mut StdKeywords) -> LookupResult<V>
where
    V: ReqMetaKey,
    V: FromStr,
    <V as FromStr>::Err: fmt::Display,
    ParseReqKeyError: From<ReqKeyError<<V as FromStr>::Err>>,
{
    V::remove_meta_req(kws)
        .map_err(ParseReqKeyError::from)
        .map_err(Box::new)
        .into_deferred0()
}

pub(crate) fn lookup_meas_req<V>(kws: &mut StdKeywords, n: MeasIdx) -> LookupResult<V>
where
    V: ReqMeasKey,
    V: FromStr,
    <V as FromStr>::Err: fmt::Display,
    ParseReqKeyError: From<ReqKeyError<<V as FromStr>::Err>>,
{
    V::remove_meas_req(kws, n)
        .map_err(ParseReqKeyError::from)
        .map_err(Box::new)
        .into_deferred0()
}

pub(crate) fn lookup_meta_opt<V>(kws: &mut StdKeywords, dep: bool) -> LookupTentative<OptionalKw<V>>
where
    V: OptMetaKey,
    V: FromStr,
    <V as FromStr>::Err: fmt::Display,
    ParseOptKeyWarning: From<ParseKeyError<<V as FromStr>::Err>>,
{
    let mut x = process_opt(V::remove_meta_opt(kws));
    if dep {
        x.push_warning(DepKeyWarning(V::std()).into());
    }
    x
}

pub(crate) fn lookup_meas_opt<V>(
    kws: &mut StdKeywords,
    n: MeasIdx,
    dep: bool,
) -> LookupTentative<OptionalKw<V>>
where
    V: OptMeasKey,
    V: FromStr,
    <V as FromStr>::Err: fmt::Display,
    ParseOptKeyWarning: From<ParseKeyError<<V as FromStr>::Err>>,
{
    let mut x = process_opt(V::remove_meas_opt(kws, n));
    if dep {
        x.push_warning(DepKeyWarning(V::std(n)).into());
    }
    x
}

pub(crate) fn lookup_timestamps<T>(
    kws: &mut StdKeywords,
    dep: bool,
) -> LookupTentative<Timestamps<T>>
where
    T: PartialOrd,
    T: Copy,
    Btim<T>: OptMetaKey,
    <Btim<T> as FromStr>::Err: fmt::Display,
    Etim<T>: OptMetaKey,
    <Etim<T> as FromStr>::Err: fmt::Display,
    ParseOptKeyWarning: From<ParseKeyError<<Btim<T> as FromStr>::Err>>,
    ParseOptKeyWarning: From<ParseKeyError<<Etim<T> as FromStr>::Err>>,
{
    let b = lookup_meta_opt(kws, dep);
    let e = lookup_meta_opt(kws, dep);
    let d = lookup_meta_opt(kws, dep);
    b.zip3(e, d).and_tentatively(|(btim, etim, date)| {
        Timestamps::new(btim, etim, date)
            .map(Tentative::new1)
            .unwrap_or_else(|w| {
                let ow = ParseKeysWarning::OtherWarning(w.into());
                Tentative::new(Timestamps::default(), vec![ow], vec![])
            })
    })
}

pub(crate) fn lookup_datetimes(kws: &mut StdKeywords) -> LookupTentative<Datetimes> {
    let b = lookup_meta_opt(kws, false);
    let e = lookup_meta_opt(kws, false);
    b.zip(e).and_tentatively(|(begin, end)| {
        Datetimes::try_new(begin, end)
            .map(Tentative::new1)
            .unwrap_or_else(|w| {
                let ow = ParseKeysWarning::OtherWarning(w.into());
                Tentative::new(Datetimes::default(), vec![ow], vec![])
            })
    })
}

pub(crate) fn lookup_modification(kws: &mut StdKeywords) -> LookupTentative<ModificationData> {
    let lmr = lookup_meta_opt(kws, false);
    let lmd = lookup_meta_opt(kws, false);
    let ori = lookup_meta_opt(kws, false);
    lmr.zip3(lmd, ori).map(
        |(last_modifier, last_modified, originality)| ModificationData {
            last_modifier,
            last_modified,
            originality,
        },
    )
}

pub(crate) fn lookup_plate(kws: &mut StdKeywords, dep: bool) -> LookupTentative<PlateData> {
    let w = lookup_meta_opt(kws, dep);
    let n = lookup_meta_opt(kws, dep);
    let i = lookup_meta_opt(kws, dep);
    w.zip3(n, i).map(|(wellid, platename, plateid)| PlateData {
        wellid,
        platename,
        plateid,
    })
}

pub(crate) fn lookup_carrier(kws: &mut StdKeywords) -> LookupTentative<CarrierData> {
    let l = lookup_meta_opt(kws, false);
    let i = lookup_meta_opt(kws, false);
    let t = lookup_meta_opt(kws, false);
    l.zip3(i, t)
        .map(|(locationid, carrierid, carriertype)| CarrierData {
            locationid,
            carrierid,
            carriertype,
        })
}

pub(crate) fn lookup_unstained(kws: &mut StdKeywords) -> LookupTentative<UnstainedData> {
    let c = lookup_meta_opt(kws, false);
    let i = lookup_meta_opt(kws, false);
    c.zip(i)
        .map(|(centers, info)| UnstainedData::new_unchecked(centers, info))
}

pub(crate) fn lookup_compensation_2_0(
    kws: &mut StdKeywords,
    par: Par,
) -> LookupTentative<OptionalKw<Compensation>> {
    // column = src measurement
    // row = target measurement
    // These are "flipped" in 2.0, where "column" goes TO the "row"
    let n = par.0;
    let mut matrix = DMatrix::<f32>::identity(n, n);
    let mut warnings = vec![];
    for r in 0..n {
        for c in 0..n {
            let k = Dfc::std(c, r);
            match lookup_dfc(kws, &k) {
                Ok(Some(x)) => {
                    matrix[(r, c)] = x;
                }
                Ok(None) => (),
                Err(w) => warnings.push(ParseKeysWarning::OptKey(w.into())),
            }
        }
    }
    if warnings.is_empty() {
        Compensation::try_new(matrix).map_or_else(
            |w| {
                Tentative::new(
                    None.into(),
                    vec![ParseKeysWarning::OptKey(w.into())],
                    vec![],
                )
            },
            |x| Tentative::new1(Some(x).into()),
        )
    } else {
        Tentative::new(None.into(), warnings, vec![])
    }
}

pub(crate) fn lookup_dfc(
    kws: &mut StdKeywords,
    k: &StdKey,
) -> Result<Option<f32>, ParseKeyError<ParseFloatError>> {
    kws.remove(k).map_or(Ok(None), |v| {
        v.parse::<f32>()
            .map_err(|e| ParseKeyError {
                error: e,
                key: k.clone(),
                value: v.clone(),
            })
            .map(Some)
    })
}

pub(crate) fn lookup_temporal_gain_3_0(
    kws: &mut StdKeywords,
    i: MeasIdx,
) -> LookupTentative<OptionalKw<Gain>> {
    let mut tnt_gain = lookup_meas_opt::<Gain>(kws, i, false);
    tnt_gain.eval_error(|gain| {
        if gain.0.is_some() {
            Some(ParseKeysError::Other(TemporalError::HasGain.into()))
        } else {
            None
        }
    });
    tnt_gain
}

pub(crate) fn lookup_temporal_scale_3_0(kws: &mut StdKeywords, i: MeasIdx) -> LookupResult<Scale> {
    let mut res = lookup_meas_req(kws, i);
    res.eval_error(|scale| {
        if *scale != Scale::Linear {
            Some(ParseKeysError::Other(TemporalError::NonLinear.into()))
        } else {
            None
        }
    });
    res
}

fn process_opt<V>(
    res: Result<OptionalKw<V>, ParseKeyError<<V as FromStr>::Err>>,
) -> Tentative<OptionalKw<V>, ParseKeysWarning, ParseKeysError>
where
    V: FromStr,
    ParseOptKeyWarning: From<ParseKeyError<<V as FromStr>::Err>>,
{
    res.map_or_else(
        |e| {
            Tentative::new(
                None.into(),
                vec![ParseKeysWarning::OptKey(e.into())],
                vec![],
            )
        },
        Tentative::new1,
    )
}

enum_from_disp!(
    pub ParseKeysError,
    [ReqKey,     Box<ParseReqKeyError>],
    [Other,      ParseOtherError],
    [Deprecated, DepKeyWarning],
    [Linked,     LinkedNameError]
);

enum_from_disp!(
    pub LookupMeasWarning,
    [Parse, ParseKeysWarning],
    [Pattern, NonStdMeasRegexError]
);

enum_from_disp!(
    pub ParseKeysWarning,
    [OptKey,       ParseOptKeyWarning],
    [OtherWarning, ParseOtherWarning],
    [DepKey,   DepKeyWarning],
    [OtherDep, DepFeatureWarning]
);

enum_from_disp!(
    pub ParseReqKeyError,
    [Width,          ReqKeyError<ParseBitsError>],
    [Range,          ReqKeyError<ParseRangeError>],
    [AlphaNumType,   ReqKeyError<AlphaNumTypeError>],
    [String,         ReqKeyError<Infallible>],
    [Int,            ReqKeyError<ParseIntError>],
    [Scale,          ReqKeyError<ScaleError>],
    [ReqRangedFloat, ReqKeyError<RangedFloatError>],
    [Mode,           ReqKeyError<ModeError>],
    [ByteOrd,        ReqKeyError<ParseByteOrdError>],
    [Endian,         ReqKeyError<NewEndianError>],
    [ReqShortname,   ReqKeyError<ShortnameError>]
);

enum_from_disp!(
    pub ParseOptKeyWarning,
    [NumType,          ParseKeyError<NumTypeError>],
    [Trigger,          ParseKeyError<TriggerError>],
    [OptScale,         ParseKeyError<ScaleError>],
    [OptFloat,         ParseKeyError<ParseFloatError>],
    [OptRangedFloat,   ParseKeyError<RangedFloatError>],
    [Feature,          ParseKeyError<FeatureError>],
    [Wavelengths,      ParseKeyError<WavelengthsError>],
    [Calibration3_1,   ParseKeyError<CalibrationError<CalibrationFormat3_1>>],
    [Calibration3_2,   ParseKeyError<CalibrationError<CalibrationFormat3_2>>],
    [OptInt,           ParseKeyError<ParseIntError>],
    [OptString,        ParseKeyError<Infallible>],
    [FCSDate,          ParseKeyError<FCSDateError>],
    [FCSTime,          ParseKeyError<FCSTimeError>],
    [FCSTime60,        ParseKeyError<FCSTime60Error>],
    [FCSTime100,       ParseKeyError<FCSTime100Error>],
    [FCSDateTime,      ParseKeyError<FCSDateTimeError>],
    [ModifiedDateTime, ParseKeyError<ModifiedDateTimeError>],
    [Originality,      ParseKeyError<OriginalityError>],
    [UnstainedCenter,  ParseKeyError<ParseUnstainedCenterError>],
    [Mode3_2,          ParseKeyError<Mode3_2Error>],
    [TemporalType,     ParseKeyError<TemporalTypeError>],
    [OpticalType,      ParseKeyError<OpticalTypeError>],
    [OptShortname,     ParseKeyError<ShortnameError>],
    [Display,          ParseKeyError<DisplayError>],
    [Unicode,          ParseKeyError<UnicodeError>],
    [Spillover,        ParseKeyError<ParseSpilloverError>],
    [Compensation,     ParseKeyError<ParseCompError>],
    [CompShape,        NewCompError]
);

enum_from_disp!(
    pub ParseOtherError,
    [Temporal, TemporalError],
    [NamedVec, NewNamedVecError],
    [MissingTime, MissingTime]
);

pub struct MissingTime(pub TimePattern);

pub enum TemporalError {
    NonLinear,
    HasGain,
}

enum_from_disp!(
    pub ParseOtherWarning,
    [Timestamp, InvalidTimestamps],
    [Datetime, InvalidDatetimes]
);

pub struct DepKeyWarning(pub StdKey);

pub enum DepFeatureWarning {
    DatatypeASCII,
    ModeCorrelated,
    ModeUncorrelated,
}

impl fmt::Display for DepFeatureWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            Self::DatatypeASCII => "$DATATYPE=A is deprecated",
            Self::ModeCorrelated => "$MODE=C is deprecated",
            Self::ModeUncorrelated => "$MODE=U is deprecated",
        };
        write!(f, "{s}")
    }
}

impl fmt::Display for DepKeyWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "deprecated key: {}", self.0)
    }
}

impl fmt::Display for MissingTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Could not find time measurement matching {}", self.0)
    }
}

impl fmt::Display for TemporalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        // TODO include meas idx here
        match self {
            TemporalError::NonLinear => write!(f, "$PnE must be '0,0'"),
            TemporalError::HasGain => write!(f, "$PnG must not be set"),
        }
    }
}

pub enum CoreTEXTFailure {
    NoPar(ReqKeyError<ParseIntError>),
    Keywords,
    Linked,
}
