use crate::core::*;
use crate::error::*;
use crate::macros::{enum_from, enum_from_disp, match_many_to_one};
use crate::validated::nonstandard::*;
use crate::validated::pattern::*;
use crate::validated::shortname::*;
use crate::validated::standard::*;

use super::byteord::*;
use super::compensation::*;
use super::datetimes::*;
use super::float_or_int::*;
use super::index::*;
use super::keywords::*;
use super::named_vec::*;
use super::optionalkw::*;
use super::ranged_float::RangedFloatError;
use super::scale::*;
use super::spillover::*;
use super::timestamps::*;
use super::unstainedcenters::*;

use nalgebra::DMatrix;
use nonempty::NonEmpty;
use std::convert::Infallible;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;

pub(crate) type LookupResult<V> = DeferredResult<V, ParseKeysWarning, ParseKeysError>;

pub(crate) type LookupTentative<V, E> = Tentative<V, ParseKeysWarning, E>;

pub(crate) fn lookup_meta_req<V>(kws: &mut StdKeywords) -> LookupResult<V>
where
    V: ReqMetaKey,
    ParseReqKeyError: From<ReqKeyError<<V as FromStr>::Err>>,
{
    V::remove_meta_req(kws)
        .map_err(ParseReqKeyError::from)
        .map_err(Box::new)
        .into_deferred()
}

pub(crate) fn lookup_indexed_req<V>(kws: &mut StdKeywords, n: IndexFromOne) -> LookupResult<V>
where
    V: ReqMeasKey,
    ParseReqKeyError: From<ReqKeyError<<V as FromStr>::Err>>,
{
    V::remove_meas_req(kws, n)
        .map_err(ParseReqKeyError::from)
        .map_err(Box::new)
        .into_deferred()
}

pub(crate) fn lookup_meta_opt<V, E>(
    kws: &mut StdKeywords,
    dep: bool,
) -> LookupTentative<OptionalKw<V>, E>
where
    V: OptMetaKey,
    V: FromStr,
    ParseOptKeyWarning: From<ParseKeyError<<V as FromStr>::Err>>,
{
    let mut x = process_opt(V::remove_meta_opt(kws));
    // TODO toggle
    if dep {
        x.push_warning(DepKeyWarning(V::std()).into());
    }
    x
}

pub(crate) fn lookup_indexed_opt<V, E>(
    kws: &mut StdKeywords,
    n: IndexFromOne,
    dep: bool,
) -> LookupTentative<OptionalKw<V>, E>
where
    V: OptMeasKey,
    ParseOptKeyWarning: From<ParseKeyError<<V as FromStr>::Err>>,
{
    let mut x = process_opt(V::remove_meas_opt(kws, n));
    if dep {
        x.push_warning(DepKeyWarning(V::std(n)).into());
    }
    x
}

pub(crate) fn lookup_timestamps<T, E>(
    kws: &mut StdKeywords,
    dep: bool,
) -> LookupTentative<Timestamps<T>, E>
where
    T: PartialOrd,
    T: Copy,
    Btim<T>: OptMetaKey,
    Etim<T>: OptMetaKey,
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

pub(crate) fn lookup_datetimes<E>(kws: &mut StdKeywords) -> LookupTentative<Datetimes, E> {
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

pub(crate) fn lookup_modification<E>(
    kws: &mut StdKeywords,
) -> LookupTentative<ModificationData, E> {
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

pub(crate) fn lookup_plate<E>(kws: &mut StdKeywords, dep: bool) -> LookupTentative<PlateData, E> {
    let w = lookup_meta_opt(kws, dep);
    let n = lookup_meta_opt(kws, dep);
    let i = lookup_meta_opt(kws, dep);
    w.zip3(n, i).map(|(wellid, platename, plateid)| PlateData {
        wellid,
        platename,
        plateid,
    })
}

pub(crate) fn lookup_carrier<E>(kws: &mut StdKeywords) -> LookupTentative<CarrierData, E> {
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

pub(crate) fn lookup_unstained<E>(kws: &mut StdKeywords) -> LookupTentative<UnstainedData, E> {
    let c = lookup_meta_opt(kws, false);
    let i = lookup_meta_opt(kws, false);
    c.zip(i)
        .map(|(centers, info)| UnstainedData::new_unchecked(centers, info))
}

pub(crate) fn lookup_compensation_2_0<E>(
    kws: &mut StdKeywords,
    par: Par,
) -> LookupTentative<OptionalKw<Compensation>, E> {
    // column = src measurement
    // row = target measurement
    // These are "flipped" in 2.0, where "column" goes TO the "row"
    let n = par.0;
    let mut matrix = DMatrix::<f32>::identity(n, n);
    let mut warnings = vec![];
    for r in 0..n {
        for c in 0..n {
            let k = Dfc::std(c.into(), r.into());
            match lookup_dfc(kws, k) {
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
    k: StdKey,
) -> Result<Option<f32>, ParseKeyError<ParseFloatError>> {
    kws.remove(&k).map_or(Ok(None), |v| {
        v.parse::<f32>()
            .map_err(|e| ParseKeyError {
                error: e,
                key: k,
                value: v.clone(),
            })
            .map(Some)
    })
}

pub(crate) fn lookup_subset<E>(
    kws: &mut StdKeywords,
    dep: bool,
) -> LookupTentative<OptionalKw<SubsetData>, E> {
    lookup_meta_opt(kws, dep)
        .map(|x| x.0)
        .and_tentatively(|m: Option<CSMode>| {
            if let Some(n) = m {
                let it = (0..(n.0 as usize)).map(|i| {
                    lookup_indexed_opt::<CSVFlag, _>(kws, i.into(), dep).map(|x| x.0.map(|y| y.0))
                });
                Tentative::mconcat_ne(NonEmpty::collect(it).unwrap()).and_tentatively(|flags| {
                    lookup_meta_opt::<CSVBits, _>(kws, dep)
                        .map(|x| x.0.map(|y| y.0))
                        .map(|bits| Some(SubsetData { flags, bits }).into())
                })
            } else {
                Tentative::new1(None.into())
            }
        })
}

pub(crate) fn lookup_peakdata<E>(
    kws: &mut StdKeywords,
    i: MeasIndex,
    dep: bool,
) -> LookupTentative<PeakData, E> {
    let b = lookup_indexed_opt(kws, i.into(), dep);
    let s = lookup_indexed_opt(kws, i.into(), dep);
    b.zip(s).map(|(bin, size)| PeakData { bin, size })
}

pub(crate) fn lookup_applied_gates2_0<E>(
    kws: &mut StdKeywords,
) -> LookupTentative<OptionalKw<AppliedGates2_0>, E> {
    let ag = lookup_applied_gates(kws, false, |k, i| lookup_gate_region_2_0(k, i));
    let gm = lookup_gated_measurements(kws, false);
    ag.zip(gm).and_tentatively(|(x, y)| {
        if let Some((applied, gated_measurements)) = x.0.zip(y.0) {
            let ret = AppliedGates2_0 {
                gated_measurements,
                regions: applied,
            };
            match ret.check_gates() {
                Ok(_) => Tentative::new1(Some(ret).into()),
                Err(e) => {
                    let w = ParseKeysWarning::OtherWarning(e.into());
                    Tentative::new(None.into(), vec![w], vec![])
                }
            }
        } else {
            Tentative::new1(None.into())
        }
    })
}

pub(crate) fn lookup_applied_gates3_0<E>(
    kws: &mut StdKeywords,
    dep: bool,
) -> LookupTentative<OptionalKw<AppliedGates3_0>, E> {
    let ag = lookup_applied_gates(kws, false, |k, i| lookup_gate_region_3_0(k, i));
    let gm = lookup_gated_measurements(kws, dep);
    ag.zip(gm).and_tentatively(|(x, y)| {
        if let Some(applied) = x.0 {
            let ret = AppliedGates3_0 {
                gated_measurements: y.0.map(|z| z.0.into()).unwrap_or_default(),
                regions: applied,
            };
            match ret.check_gates() {
                Ok(_) => Tentative::new1(Some(ret).into()),
                Err(e) => {
                    let w = ParseKeysWarning::OtherWarning(e.into());
                    Tentative::new(None.into(), vec![w], vec![])
                }
            }
        } else {
            Tentative::new1(None.into())
        }
    })
}

pub(crate) fn lookup_applied_gates3_2<E>(
    kws: &mut StdKeywords,
) -> LookupTentative<OptionalKw<AppliedGates3_2>, E> {
    lookup_applied_gates(kws, false, |k, i| lookup_gate_region_3_2(k, i))
        .map(|x| x.map(|regions| AppliedGates3_2 { regions }))
}

pub(crate) fn lookup_applied_gates<I, E, F>(
    kws: &mut StdKeywords,
    dep: bool,
    get_region: F,
) -> LookupTentative<OptionalKw<GatingRegions<I>>, E>
where
    F: Fn(&mut StdKeywords, RegionIndex) -> LookupTentative<OptionalKw<I>, E>,
{
    lookup_meta_opt::<Gating, _>(kws, dep)
        .and_tentatively(|maybe| {
            if let Some(gating) = maybe.0 {
                let res = gating.flatten().try_map(|ri| {
                    get_region(kws, ri)
                        .map(|x| x.0.map(|y| (ri, y)))
                        .transpose()
                        .ok_or(GateRegionLinkError.into())
                });
                match res {
                    Ok(xs) => Tentative::mconcat_ne(xs)
                        .map(|regions| Some(GatingRegions { regions, gating })),
                    Err(w) => Tentative::new(None, vec![ParseKeysWarning::OtherWarning(w)], vec![]),
                }
            } else {
                Tentative::new1(None)
            }
        })
        .map(|x| x.into())
}

pub(crate) fn lookup_gated_measurements<E>(
    kws: &mut StdKeywords,
    dep: bool,
) -> LookupTentative<OptionalKw<GatedMeasurements>, E> {
    lookup_meta_opt::<Gate, E>(kws, dep).and_tentatively(|maybe| {
        if let Some(n) = maybe.0 {
            // TODO this will be nicer with NonZeroUsize
            if n.0 > 0 {
                let xs = NonEmpty::collect(
                    (0..n.0).map(|i| lookup_gated_measurement(kws, i.into(), dep)),
                )
                .unwrap();
                return Tentative::mconcat_ne(xs).map(|x| Some(GatedMeasurements(x)).into());
            }
        }
        Tentative::new1(None.into())
    })
}

pub(crate) fn lookup_gated_measurement<E>(
    kws: &mut StdKeywords,
    i: GateIndex,
    dep: bool,
) -> LookupTentative<GatedMeasurement, E> {
    let j = i.into();
    let e = lookup_indexed_opt(kws, j, dep);
    let f = lookup_indexed_opt(kws, j, dep);
    let n = lookup_indexed_opt(kws, j, dep);
    let p = lookup_indexed_opt(kws, j, dep);
    let r = lookup_indexed_opt(kws, j, dep);
    let s = lookup_indexed_opt(kws, j, dep);
    let t = lookup_indexed_opt(kws, j, dep);
    let v = lookup_indexed_opt(kws, j, dep);
    e.zip4(f, n, p).zip5(r, s, t, v).map(
        |(
            (scale, filter, shortname, percent_emitted),
            range,
            longname,
            detector_type,
            detector_voltage,
        )| {
            GatedMeasurement {
                scale,
                filter,
                shortname,
                percent_emitted,
                range,
                longname,
                detector_type,
                detector_voltage,
            }
        },
    )
}

pub(crate) fn lookup_gate_region_2_0<E>(
    kws: &mut StdKeywords,
    i: RegionIndex,
) -> LookupTentative<OptionalKw<Region2_0>, E> {
    let n = lookup_indexed_opt::<RegionGateIndex2_0, _>(kws, i.into(), false);
    let w = lookup_indexed_opt(kws, i.into(), false);
    n.zip(w)
        .and_tentatively(|(_n, _y)| {
            _n.0.zip(_y.0)
                .and_then(|(gi, win)| Region::try_new(gi.0, win).map(|x| x.inner_into()))
                .map_or_else(
                    || {
                        let warn = ParseOtherWarning::GateRegion(InvalidGateRegion).into();
                        Tentative::new(None, vec![warn], vec![])
                    },
                    |x| Tentative::new1(Some(x)),
                )
        })
        .map(|x| x.into())
}

pub(crate) fn lookup_gate_region_3_0<E>(
    kws: &mut StdKeywords,
    i: RegionIndex,
) -> LookupTentative<OptionalKw<Region3_0>, E> {
    let n = lookup_indexed_opt::<RegionGateIndex3_0, _>(kws, i.into(), false);
    let w = lookup_indexed_opt(kws, i.into(), false);
    n.zip(w)
        .and_tentatively(|(_n, _y)| {
            _n.0.zip(_y.0)
                .and_then(|(gi, win)| Region::try_new(gi.0, win))
                .map_or_else(
                    || {
                        let warn = ParseOtherWarning::GateRegion(InvalidGateRegion).into();
                        Tentative::new(None, vec![warn], vec![])
                    },
                    |x| Tentative::new1(Some(x)),
                )
        })
        .map(|x| x.into())
}

pub(crate) fn lookup_gate_region_3_2<E>(
    kws: &mut StdKeywords,
    i: RegionIndex,
) -> LookupTentative<OptionalKw<Region3_2>, E> {
    let n = lookup_indexed_opt::<RegionGateIndex3_2, _>(kws, i.into(), true);
    let w = lookup_indexed_opt(kws, i.into(), true);
    n.zip(w)
        .and_tentatively(|(_n, _y)| {
            _n.0.zip(_y.0)
                .and_then(|(gi, win)| Region::try_new(gi.0, win).map(|x| x.inner_into()))
                .map_or_else(
                    || {
                        let warn = ParseOtherWarning::GateRegion(InvalidGateRegion).into();
                        Tentative::new(None, vec![warn], vec![])
                    },
                    |x| Tentative::new1(Some(x)),
                )
        })
        .map(|x| x.into())
}

pub(crate) fn lookup_temporal_gain_3_0(
    kws: &mut StdKeywords,
    i: IndexFromOne,
) -> LookupTentative<OptionalKw<Gain>, ParseKeysError> {
    let mut tnt_gain = lookup_indexed_opt(kws, i, false);
    tnt_gain.eval_error(|gain| {
        if gain.0.is_some() {
            Some(ParseKeysError::Other(TemporalError::HasGain.into()))
        } else {
            None
        }
    });
    tnt_gain
}

pub(crate) fn lookup_temporal_scale_3_0(
    kws: &mut StdKeywords,
    i: IndexFromOne,
) -> LookupResult<Scale> {
    let mut res = lookup_indexed_req(kws, i);
    res.def_eval_error(|scale| {
        if *scale != Scale::Linear {
            Some(ParseKeysError::Other(TemporalError::NonLinear.into()))
        } else {
            None
        }
    });
    res
}

fn process_opt<V, E>(
    res: Result<OptionalKw<V>, ParseKeyError<<V as FromStr>::Err>>,
) -> Tentative<OptionalKw<V>, ParseKeysWarning, E>
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

// TODO this could be nested better
enum_from_disp!(
    pub ParseKeysError,
    [ReqKey,     Box<ParseReqKeyError>],
    [Other,      ParseOtherError],
    [Deprecated, DepKeyWarning],
    [Linked,     LinkedNameError],
    [Deviant,    DeviantError]
);

enum_from_disp!(
    pub LookupMeasWarning,
    [Parse, ParseKeysWarning],
    [Pattern, NonStdMeasRegexError],
    [Deviant, DeviantError]
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
    [FloatOrInt,     ReqKeyError<ParseFloatOrIntError>],
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
    [NumType,            ParseKeyError<NumTypeError>],
    [Trigger,            ParseKeyError<TriggerError>],
    [OptScale,           ParseKeyError<ScaleError>],
    [OptFloat,           ParseKeyError<ParseFloatError>],
    [OptRangedFloat,     ParseKeyError<RangedFloatError>],
    [Feature,            ParseKeyError<FeatureError>],
    [Wavelengths,        ParseKeyError<WavelengthsError>],
    [Calibration3_1,     ParseKeyError<CalibrationError<CalibrationFormat3_1>>],
    [Calibration3_2,     ParseKeyError<CalibrationError<CalibrationFormat3_2>>],
    [OptInt,             ParseKeyError<ParseIntError>],
    [OptString,          ParseKeyError<Infallible>],
    [FCSDate,            ParseKeyError<FCSDateError>],
    [FCSTime,            ParseKeyError<FCSTimeError>],
    [FCSTime60,          ParseKeyError<FCSTime60Error>],
    [FCSTime100,         ParseKeyError<FCSTime100Error>],
    [FCSDateTime,        ParseKeyError<FCSDateTimeError>],
    [ModifiedDateTime,   ParseKeyError<ModifiedDateTimeError>],
    [Originality,        ParseKeyError<OriginalityError>],
    [UnstainedCenter,    ParseKeyError<ParseUnstainedCenterError>],
    [Mode3_2,            ParseKeyError<Mode3_2Error>],
    [TemporalType,       ParseKeyError<TemporalTypeError>],
    [OpticalType,        ParseKeyError<OpticalTypeError>],
    [OptShortname,       ParseKeyError<ShortnameError>],
    [Display,            ParseKeyError<DisplayError>],
    [Unicode,            ParseKeyError<UnicodeError>],
    [Spillover,          ParseKeyError<ParseSpilloverError>],
    [Compensation,       ParseKeyError<ParseCompError>],
    [FloatOrInt,         ParseKeyError<ParseFloatOrIntError>],
    [GateRegionIndex2_0, ParseKeyError<RegionGateIndex2_0Error>],
    [GateRegionIndex3_0, ParseKeyError<RegionGateIndex3_0Error>],
    [GateRegionIndex3_2, ParseKeyError<RegionGateIndex3_2Error>],
    [GateRegionWindow,   ParseKeyError<GatePairError>],
    [Gating,             ParseKeyError<GatingError>],
    [CompShape,          NewCompError]
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
    [Datetime, InvalidDatetimes],
    [GateRegion, InvalidGateRegion],
    [GateRegionLink, GateRegionLinkError],
    [GateMeasLink, GateMeasurementLinkError]
);

pub struct DepKeyWarning(pub StdKey);

pub enum DepFeatureWarning {
    DatatypeASCII,
    ModeCorrelated,
    ModeUncorrelated,
}

pub struct InvalidGateRegion;

impl fmt::Display for InvalidGateRegion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "values for $RnI and $RnW must both be univariate or bivariate"
        )
    }
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

pub struct DeviantError;

impl fmt::Display for DeviantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "deviant keywords found")
    }
}
