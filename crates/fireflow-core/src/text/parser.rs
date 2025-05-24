use crate::config::StdTextReadConfig;
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
use nonempty::NonEmpty;
use std::convert::Infallible;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError};
use std::str::FromStr;

/// A structure to look up and parse keywords in the TEXT segment
///
/// Given a hash table of keywords (as String pairs) and a configuration, this
/// provides an interface to extract keywords. If found, the keyword will be
/// removed from the table and parsed to its native type (number, list, matrix,
/// etc). If lookup or parsing fails, an error/warning will be logged within the
/// state depending on if the key is required or optional.
///
/// Errors in all cases are deferred until the end of the state's lifetime, at
/// which point the errors are returned along with the result of the computation
/// or failure (if applicable).
pub(crate) struct KwParser<'a, 'b> {
    raw_keywords: &'b mut StdKeywords,
    req_errors: Vec<ParseReqKeyError>,
    opt_warnings: Vec<ParseOptKeyWarning>,
    other_errors: Vec<ParseOtherError>,
    other_warnings: Vec<ParseOtherWarning>,
    deprecated: Vec<DepKeyWarning>,
    pub(crate) conf: &'a StdTextReadConfig,
}

impl<'a, 'b> KwParser<'a, 'b> {
    /// Run a computation within a keyword lookup context which may fail.
    ///
    /// This is like 'run' except the computation may not return anything (None)
    /// in which case Err(Failure(...)) will be returned along with the reason
    /// for failure which must be given a priori.
    ///
    /// Any errors which are logged must be pushed into the state's error buffer
    /// directly, as errors are not allowed to be returned by the inner
    /// computation.
    pub(crate) fn try_run<X, F>(
        kws: &'b mut StdKeywords,
        conf: &'a StdTextReadConfig,
        f: F,
    ) -> DeferredResult<X, ParseKeysWarning, ParseKeysError>
    where
        F: FnOnce(&mut Self) -> Option<X>,
    {
        let mut st = Self::from(kws, conf);
        if let Some(value) = f(&mut st) {
            let (errors, warnings) = st.collect();
            Ok(Tentative::new(value, warnings, errors))
        } else {
            let (errors, warnings) = st.collect();
            let fail = DeferredFailure::new(
                // TODO I contradicted myself :( this entire struct is now
                // useless. Oh well, it was bound to happen
                warnings,
                NonEmpty::from_vec(errors).unwrap(),
            );
            Err(fail)
        }
    }

    pub(crate) fn lookup_meta_req<V>(&mut self) -> Option<V>
    where
        V: ReqMetaKey,
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
        ParseReqKeyError: From<ReqKeyError<<V as FromStr>::Err>>,
    {
        V::remove_meta_req(self.raw_keywords)
            .map_err(|e| self.req_errors.push(e.into()))
            .ok()
    }

    pub(crate) fn lookup_meta_opt<V>(&mut self, dep: bool) -> OptionalKw<V>
    where
        V: OptMetaKey,
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
        ParseOptKeyWarning: From<ParseKeyError<<V as FromStr>::Err>>,
    {
        let res = V::remove_meta_opt(self.raw_keywords).map_err(|e| e.into());
        self.process_opt(res, V::std(), dep)
    }

    pub(crate) fn lookup_meas_req<V>(&mut self, n: MeasIdx) -> Option<V>
    where
        V: ReqMeasKey,
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
        ParseReqKeyError: From<ReqKeyError<<V as FromStr>::Err>>,
    {
        V::remove_meas_req(self.raw_keywords, n)
            .map_err(|e| self.req_errors.push(e.into()))
            .ok()
    }

    pub(crate) fn lookup_meas_opt<V>(&mut self, n: MeasIdx, dep: bool) -> OptionalKw<V>
    where
        V: OptMeasKey,
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
        ParseOptKeyWarning: From<ParseKeyError<<V as FromStr>::Err>>,
    {
        let res = V::remove_meas_opt(self.raw_keywords, n).map_err(|e| e.into());
        self.process_opt(res, V::std(n), dep)
    }

    pub(crate) fn lookup_timestamps<T>(&mut self, dep: bool) -> Timestamps<T>
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
        Timestamps::new(
            self.lookup_meta_opt(dep),
            self.lookup_meta_opt(dep),
            self.lookup_meta_opt(dep),
        )
        .unwrap_or_else(|e| {
            self.other_warnings.push(e.into());
            Timestamps::default()
        })
    }

    pub(crate) fn lookup_datetimes(&mut self) -> Datetimes {
        let b = self.lookup_meta_opt(false);
        let e = self.lookup_meta_opt(false);
        Datetimes::new(b, e).unwrap_or_else(|w| {
            self.other_warnings.push(w.into());
            Datetimes::default()
        })
    }

    pub(crate) fn lookup_modification(&mut self) -> ModificationData {
        ModificationData {
            last_modifier: self.lookup_meta_opt(false),
            last_modified: self.lookup_meta_opt(false),
            originality: self.lookup_meta_opt(false),
        }
    }

    pub(crate) fn lookup_plate(&mut self, dep: bool) -> PlateData {
        PlateData {
            wellid: self.lookup_meta_opt(dep),
            platename: self.lookup_meta_opt(dep),
            plateid: self.lookup_meta_opt(dep),
        }
    }

    pub(crate) fn lookup_carrier(&mut self) -> CarrierData {
        CarrierData {
            locationid: self.lookup_meta_opt(false),
            carrierid: self.lookup_meta_opt(false),
            carriertype: self.lookup_meta_opt(false),
        }
    }

    pub(crate) fn lookup_unstained(&mut self) -> UnstainedData {
        UnstainedData::new_unchecked(self.lookup_meta_opt(false), self.lookup_meta_opt(false))
    }

    pub(crate) fn lookup_compensation_2_0(&mut self, par: Par) -> OptionalKw<Compensation> {
        // column = src measurement
        // row = target measurement
        // These are "flipped" in 2.0, where "column" goes TO the "row"
        let n = par.0;
        let mut any_error = false;
        let mut matrix = DMatrix::<f32>::identity(n, n);
        for r in 0..n {
            for c in 0..n {
                let k = Dfc::std(c, r);
                if let Some(x) = self.lookup_dfc(&k) {
                    matrix[(r, c)] = x;
                } else {
                    any_error = true;
                }
            }
        }
        if any_error {
            None
        } else {
            // TODO will return none if matrix is less than 2x2, which is a
            // warning
            Compensation::new(matrix)
        }
        .into()
    }

    pub(crate) fn lookup_dfc(&mut self, k: &StdKey) -> Option<f32> {
        self.raw_keywords.remove(k).and_then(|v| {
            v.parse::<f32>()
                .map_err(|e| {
                    let err = ParseKeyError {
                        error: e,
                        key: k.clone(),
                        value: v.clone(),
                    };
                    self.opt_warnings.push(err.into());
                })
                .ok()
        })
    }

    pub fn push_error<E>(&mut self, e: E)
    where
        ParseOtherError: From<E>,
    {
        self.other_errors.push(e.into())
    }

    // auxiliary functions

    // fn collect(self) -> PureErrorBuf<AnyParserError> {
    //     let rs = PureErrorBuf::from_many(
    //         self.req_errors.into_iter().map(|e| e.into()).collect(),
    //         PureErrorLevel::Error,
    //     );
    //     let ws = PureErrorBuf::from_many(
    //         self.opt_warnings
    //             .into_iter()
    //             .map(|e| e.into())
    //             .chain(self.other_warnings.into_iter().map(|e| e.into()))
    //             .collect(),
    //         PureErrorLevel::Warning,
    //     );
    //     let ds = PureErrorBuf::from_many(
    //         self.deprecated.into_iter().map(|e| e.into()).collect(),
    //         if self.conf.disallow_deprecated {
    //             PureErrorLevel::Error
    //         } else {
    //             PureErrorLevel::Warning
    //         },
    //     );
    //     PureErrorBuf::mconcat(vec![rs, ws, ds])
    // }

    // fn into_failure(self, reason: String) -> PureFailure<AnyParserError> {
    //     Failure {
    //         reason,
    //         deferred: self.collect(),
    //     }
    // }

    fn collect(self) -> (Vec<ParseKeysError>, Vec<ParseKeysWarning>) {
        let mut errors: Vec<ParseKeysError> = self
            .req_errors
            .into_iter()
            .map(|x| x.into())
            .chain(self.other_errors.into_iter().map(|x| x.into()))
            .collect();
        let mut warnings: Vec<ParseKeysWarning> = self
            .opt_warnings
            .into_iter()
            .map(|x| x.into())
            .chain(self.other_warnings.into_iter().map(|x| x.into()))
            .collect();
        let dep = self.deprecated.into_iter();
        if self.conf.disallow_deprecated {
            errors.extend(dep.map(|x| x.into()));
        } else {
            warnings.extend(dep.map(|x| x.into()));
        }
        (errors, warnings)
    }

    fn from(kws: &'b mut StdKeywords, conf: &'a StdTextReadConfig) -> Self {
        KwParser {
            raw_keywords: kws,
            req_errors: vec![],
            opt_warnings: vec![],
            other_errors: vec![],
            other_warnings: vec![],
            deprecated: vec![],
            conf,
        }
    }

    fn process_opt<V>(
        &mut self,
        res: Result<OptionalKw<V>, ParseOptKeyWarning>,
        k: StdKey,
        dep: bool,
    ) -> OptionalKw<V>
    where
        V: FromStr,
    {
        res.inspect(|_| {
            if dep {
                self.deprecated.push(DepKeyWarning(k));
            }
        })
        .map_err(|e| self.opt_warnings.push(e))
        .unwrap_or(None.into())
    }
}

enum_from_disp!(
    pub ParseKeysError,
    [ReqKey,     ParseReqKeyError],
    [Other,      ParseOtherError],
    [Deprecated, DepKeyWarning],
    [Linked,     LinkedNameError]
);

enum_from_disp!(
    pub ParseKeysWarning,
    [OptKey,       ParseOptKeyWarning],
    [OtherWarning, ParseOtherWarning],
    [Deprecated,   DepKeyWarning]
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
    [Compensation,     ParseKeyError<CompensationError>]
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
