use crate::config::*;
use crate::data::*;
use crate::error::*;
use crate::header::*;
use crate::header_text::*;
use crate::macros::match_many_to_one;
use crate::segment::*;
use crate::validated::dataframe::AnyFCSColumn;
use crate::validated::nonstandard::*;
use crate::validated::pattern::*;
use crate::validated::shortname::*;

use super::byteord::*;
use super::compensation::*;
use super::datetimes::*;
use super::keywords::*;
use super::modified_date_time::*;
use super::named_vec::*;
use super::optionalkw::*;
use super::range::*;
use super::scale::*;
use super::spillover::*;
use super::timestamps::*;
use super::unstainedcenters::*;

use chrono::Timelike;
use itertools::Itertools;
use nalgebra::DMatrix;
use nonempty::NonEmpty;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io;
use std::io::{BufWriter, Write};
use std::str::FromStr;

/// Represents the minimal data required to fully represent the TEXT keywords.
///
/// This is created by further processing ['RawTEXT'] and arranging keywords
/// in a static structure that conforms to each version's FCS standard.
///
/// This includes "measurement data" (bit width, voltage, filter, name, etc) for
/// each measurement (aka "parameter" aka "channel") and "metadata" which is all
/// non-measurement data. Both of these are "standardized" which means they are
/// structured in a manner that reflects the indicated FCS standard in the
/// HEADER. If these structures cannot be created, then an error should be
/// thrown informing the user that their keywords are not standards-compliant.
///
/// Additionally this includes keywords that do not fit into the standard
/// (deviant and nonstandard, which means keys that start with '$' or not
/// respectively).
///
/// Importantly this does NOT include the following:
/// - $TOT (inferred from summed bit width and length of DATA)
/// - $PAR (inferred from length of measurement vector)
/// - $NEXTDATA (handled elsewhere)
/// - $(BEGIN|END)(DATA|ANALYSIS|STEXT) (handled elsewhere)
///
/// These are not included because this struct will also be used to encode the
/// TEXT data when writing a new FCS file, and the keywords that are not
/// included can be computed on the fly when writing.
#[derive(Clone, Serialize)]
pub struct CoreTEXT<M, T, P, N, W> {
    /// All "non-measurement" TEXT keywords.
    ///
    /// This is specific to each FCS version, which is encoded in the generic
    /// type variable.
    pub metadata: Metadata<M>,

    /// All measurement TEXT keywords.
    ///
    /// Specifically these are denoted by "$Pn*" keywords where "n" is the index
    /// of the measurement which also corresponds to its column in the DATA
    /// segment. The order of each measurement in this vector is n - 1.
    ///
    /// This is specific to each FCS version, which is encoded in the generic
    /// type variable.
    measurements: NamedVec<N, W, TimeChannel<T>, Measurement<P>>,
}

/// Structured non-measurement metadata.
///
/// Explicit below are common to all FCS versions.
///
/// The generic type parameter allows version-specific data to be encoded.
#[derive(Clone, Serialize)]
pub struct Metadata<X> {
    /// Value of $DATATYPE
    datatype: AlphaNumType,

    /// Value of $ABRT
    pub abrt: OptionalKw<Abrt>,

    /// Value of $COM
    pub com: OptionalKw<Com>,

    /// Value of $CELLS
    pub cells: OptionalKw<Cells>,

    /// Value of $EXP
    pub exp: OptionalKw<Exp>,

    /// Value of $FIL
    pub fil: OptionalKw<Fil>,

    /// Value of $INST
    pub inst: OptionalKw<Inst>,

    /// Value of $LOST
    pub lost: OptionalKw<Lost>,

    /// Value of $OP
    pub op: OptionalKw<Op>,

    /// Value of $PROJ
    pub proj: OptionalKw<Proj>,

    /// Value of $SMNO
    pub smno: OptionalKw<Smno>,

    /// Value of $SRC
    pub src: OptionalKw<Src>,

    /// Value of $SYS
    pub sys: OptionalKw<Sys>,

    /// Value of $TR
    tr: OptionalKw<Trigger>,

    /// Version-specific data
    pub specific: X,

    /// Non-standard keywords.
    ///
    /// This will include all the keywords that do not start with '$'.
    ///
    /// Keywords which do start with '$' but are not part of the standard are
    /// considered 'deviant' and stored elsewhere since this structure will also
    /// be used to write FCS-compliant files (which do not allow nonstandard
    /// keywords starting with '$')
    pub nonstandard_keywords: NonStdKeywords,
}

/// Structured data for time keywords.
///
/// Explicit fields are common to all versions. The generic type parameter
/// allows for version-specific information to be encoded.
#[derive(Clone, Serialize)]
pub struct TimeChannel<X> {
    /// Value for $PnB
    // TODO rename this
    bytes: Width,

    /// Value for $PnR
    range: Range,

    /// Value for $PnS
    pub longname: OptionalKw<Longname>,

    /// Non standard keywords that belong to this measurement.
    ///
    /// These are found using a configurable pattern to filter matching keys.
    pub nonstandard_keywords: NonStdKeywords,

    /// Version specific data
    pub specific: X,
}

/// Structured data for measurement keywords.
///
/// Explicit fields are common to all versions. The generic type parameter
/// allows for version-specific information to be encoded.
#[derive(Clone, Serialize)]
pub struct Measurement<X> {
    /// Value for $PnB
    bytes: Width,

    /// Value for $PnR
    range: Range,

    /// Value for $PnS
    pub longname: OptionalKw<Longname>,

    /// Value for $PnF
    pub filter: OptionalKw<Filter>,

    /// Value for $PnO
    pub power: OptionalKw<Power>,

    /// Value for $PnD
    pub detector_type: OptionalKw<DetectorType>,

    /// Value for $PnP
    pub percent_emitted: OptionalKw<PercentEmitted>,

    /// Value for $PnV
    pub detector_voltage: OptionalKw<DetectorVoltage>,

    /// Non standard keywords that belong to this measurement.
    ///
    /// These are found using a configurable pattern to filter matching keys.
    pub nonstandard_keywords: NonStdKeywords,

    /// Version specific data
    pub specific: X,
}

/// Critical FCS TEXT data for any supported FCS version
#[derive(Clone)]
pub enum AnyCoreTEXT {
    FCS2_0(Box<CoreTEXT2_0>),
    FCS3_0(Box<CoreTEXT3_0>),
    FCS3_1(Box<CoreTEXT3_1>),
    FCS3_2(Box<CoreTEXT3_2>),
}

macro_rules! from_anycoretext {
    ($anyvar:ident, $coretype:ident) => {
        impl From<$coretype> for AnyCoreTEXT {
            fn from(value: $coretype) -> Self {
                Self::$anyvar(Box::new(value))
            }
        }
    };
}

from_anycoretext!(FCS2_0, CoreTEXT2_0);
from_anycoretext!(FCS3_0, CoreTEXT3_0);
from_anycoretext!(FCS3_1, CoreTEXT3_1);
from_anycoretext!(FCS3_2, CoreTEXT3_2);

impl Serialize for AnyCoreTEXT {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AnyStdTEXT", 2)?;
        match self {
            AnyCoreTEXT::FCS2_0(x) => {
                state.serialize_field("version", &Version::FCS2_0)?;
                state.serialize_field("data", &x)?;
            }
            AnyCoreTEXT::FCS3_0(x) => {
                state.serialize_field("version", &Version::FCS3_0)?;
                state.serialize_field("data", &x)?;
            }
            AnyCoreTEXT::FCS3_1(x) => {
                state.serialize_field("version", &Version::FCS3_1)?;
                state.serialize_field("data", &x)?;
            }
            AnyCoreTEXT::FCS3_2(x) => {
                state.serialize_field("version", &Version::FCS3_2)?;
                state.serialize_field("data", &x)?;
            }
        }
        state.end()
    }
}

macro_rules! match_anycoretext {
    ($self:expr, $bind:ident, $stuff:block) => {
        match_many_to_one!(
            $self,
            AnyCoreTEXT,
            [FCS2_0, FCS3_0, FCS3_1, FCS3_2],
            $bind,
            $stuff
        )
    };
}

pub(crate) use match_anycoretext;

impl AnyCoreTEXT {
    pub(crate) fn parse_raw(
        version: Version,
        kws: &mut RawKeywords,
        conf: &StdTextReadConfig,
    ) -> PureResult<Self> {
        match version {
            Version::FCS2_0 => CoreTEXT2_0::new_from_raw(kws, conf).map(|x| x.map(|y| y.into())),
            Version::FCS3_0 => CoreTEXT3_0::new_from_raw(kws, conf).map(|x| x.map(|y| y.into())),
            Version::FCS3_1 => CoreTEXT3_1::new_from_raw(kws, conf).map(|x| x.map(|y| y.into())),
            Version::FCS3_2 => CoreTEXT3_2::new_from_raw(kws, conf).map(|x| x.map(|y| y.into())),
        }
    }

    pub fn version(&self) -> Version {
        match self {
            AnyCoreTEXT::FCS2_0(_) => Version::FCS2_0,
            AnyCoreTEXT::FCS3_0(_) => Version::FCS3_0,
            AnyCoreTEXT::FCS3_1(_) => Version::FCS3_1,
            AnyCoreTEXT::FCS3_2(_) => Version::FCS3_2,
        }
    }

    pub fn text_segment(
        &self,
        tot: Tot,
        data_len: usize,
        analysis_len: usize,
    ) -> Option<Vec<String>> {
        match_anycoretext!(self, x, { x.text_segment(tot, data_len, analysis_len) })
    }

    pub fn print_meas_table(&self, delim: &str) {
        match_anycoretext!(self, x, { x.print_meas_table(delim) })
    }

    pub fn print_spillover_table(&self, delim: &str) {
        let res = match_anycoretext!(self, x, { x.metadata.specific.as_spillover() })
            .as_ref()
            .map(|s| s.print_table(delim));
        if res.is_none() {
            println!("None")
        }
    }

    // pub(crate) fn as_column_layout(&self) -> Result<ColumnLayout, Vec<String>> {
    //     match_anycoretext!(self, x, { x.as_column_layout() })
    // }

    pub(crate) fn as_data_reader(
        &self,
        kws: &mut RawKeywords,
        conf: &DataReadConfig,
        data_seg: Segment,
    ) -> PureMaybe<DataReader> {
        match_anycoretext!(self, x, { x.as_data_reader(kws, conf, data_seg) })
    }
}

impl From<AnyCoreTEXT> for AnyCoreDataset {
    fn from(value: AnyCoreTEXT) -> Self {
        match value {
            AnyCoreTEXT::FCS2_0(x) => AnyCoreDataset::FCS2_0((*x).into()),
            AnyCoreTEXT::FCS3_0(x) => AnyCoreDataset::FCS3_0((*x).into()),
            AnyCoreTEXT::FCS3_1(x) => AnyCoreDataset::FCS3_1((*x).into()),
            AnyCoreTEXT::FCS3_2(x) => AnyCoreDataset::FCS3_2((*x).into()),
        }
    }
}

/// Minimally-required FCS TEXT data for each version
pub type CoreTEXT2_0 = CoreTEXT<
    InnerMetadata2_0,
    InnerTime2_0,
    InnerMeasurement2_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type CoreTEXT3_0 = CoreTEXT<
    InnerMetadata3_0,
    InnerTime3_0,
    InnerMeasurement3_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type CoreTEXT3_1 = CoreTEXT<
    InnerMetadata3_1,
    InnerTime3_1,
    InnerMeasurement3_1,
    IdentityFamily,
    Identity<Shortname>,
>;
pub type CoreTEXT3_2 = CoreTEXT<
    InnerMetadata3_2,
    InnerTime3_2,
    InnerMeasurement3_2,
    IdentityFamily,
    Identity<Shortname>,
>;

/// Metadata fields specific to version 2.0
#[derive(Clone, Serialize)]
pub struct InnerMetadata2_0 {
    /// Value of $MODE
    pub mode: Mode,

    /// Value of $BYTEORD
    byteord: ByteOrd,

    /// Value of $CYT
    pub cyt: OptionalKw<Cyt>,

    /// Compensation matrix derived from 'DFCnTOm' key/value pairs
    comp: OptionalKw<Compensation>,

    /// Values of $BTIM/ETIM/$DATE
    pub timestamps: Timestamps2_0,
}

/// Metadata fields specific to version 3.0
#[derive(Clone, Serialize)]
pub struct InnerMetadata3_0 {
    /// Value of $MODE
    pub mode: Mode,

    /// Value of $BYTEORD
    byteord: ByteOrd,

    /// Value of $CYT
    pub cyt: OptionalKw<Cyt>,

    /// Value of $COMP
    comp: OptionalKw<Compensation>,

    /// Values of $BTIM/ETIM/$DATE
    pub timestamps: Timestamps3_0,

    /// Value of $CYTSN
    pub cytsn: OptionalKw<Cytsn>,

    /// Value of $UNICODE
    pub unicode: OptionalKw<Unicode>,
}

/// Metadata fields specific to version 3.1
#[derive(Clone, Serialize)]
pub struct InnerMetadata3_1 {
    /// Value of $MODE
    pub mode: Mode,

    /// Value of $BYTEORD
    pub byteord: Endian,

    /// Value of $CYT
    pub cyt: OptionalKw<Cyt>,

    /// Values of $BTIM/ETIM/$DATE
    pub timestamps: Timestamps3_1,

    /// Value of $CYTSN
    pub cytsn: OptionalKw<Cytsn>,

    /// Value of $SPILLOVER
    spillover: OptionalKw<Spillover>,

    /// Values of $LAST_MODIFIED/$LAST_MODIFIER/$ORIGINALITY
    pub modification: ModificationData,

    /// Values of $PLATEID/$PLATENAME/$WELLID
    pub plate: PlateData,

    /// Value of $VOL
    pub vol: OptionalKw<Vol>,
}

#[derive(Clone, Serialize)]
pub struct InnerMetadata3_2 {
    /// Value of $BYTEORD
    pub byteord: Endian,

    /// Values of $BTIM/ETIM/$DATE
    pub timestamps: Timestamps3_1,

    /// Values of $BEGINDATETIME/$ENDDATETIME
    pub datetimes: Datetimes,

    /// Value of $CYT
    pub cyt: Cyt,

    /// Value of $SPILLOVER
    spillover: OptionalKw<Spillover>,

    /// Value of $CYTSN
    pub cytsn: OptionalKw<Cytsn>,

    /// Values of $LAST_MODIFIED/$LAST_MODIFIER/$ORIGINALITY
    // TODO it makes sense to verify this isn't before the file was created
    pub modification: ModificationData,

    /// Values of $PLATEID/$PLATENAME/$WELLID
    pub plate: PlateData,

    /// Value of $VOL
    pub vol: OptionalKw<Vol>,

    /// Values of $CARRIERID/$CARRIERTYPE/$LOCATIONID
    pub carrier: CarrierData,

    /// Values of $UNSTAINEDINFO/$UNSTAINEDCENTERS
    pub unstained: UnstainedData,

    /// Value of $FLOWRATE
    pub flowrate: OptionalKw<Flowrate>,
}

/// Time channel fields specific to version 2.0
#[derive(Clone, Serialize)]
pub struct InnerTime2_0;

/// Time channel fields specific to version 3.0
#[derive(Clone, Serialize)]
pub struct InnerTime3_0 {
    /// Value for $TIMESTEP
    timestep: Timestep,
}

/// Time channel fields specific to version 3.1
#[derive(Clone, Serialize)]
pub struct InnerTime3_1 {
    /// Value for $TIMESTEP
    timestep: Timestep,

    /// Value for $PnDISPLAY
    pub display: OptionalKw<Display>,
}

/// Time channel fields specific to version 3.2
#[derive(Clone, Serialize)]
pub struct InnerTime3_2 {
    /// Value for $TIMESTEP
    timestep: Timestep,

    /// Value for $PnDISPLAY
    pub display: OptionalKw<Display>,

    /// Value for $PnDATATYPE
    pub datatype: OptionalKw<NumType>,
}

/// Measurement fields specific to version 2.0
#[derive(Clone, Serialize)]
pub struct InnerMeasurement2_0 {
    /// Value for $PnE
    scale: OptionalKw<Scale>,

    /// Value for $PnL
    pub wavelength: OptionalKw<Wavelength>,
}

/// Measurement fields specific to version 3.0
#[derive(Clone, Serialize)]
pub struct InnerMeasurement3_0 {
    /// Value for $PnE
    scale: Scale,

    /// Value for $PnL
    pub wavelength: OptionalKw<Wavelength>,

    /// Value for $PnG
    gain: OptionalKw<Gain>,
}

/// Measurement fields specific to version 3.1
#[derive(Clone, Serialize)]
pub struct InnerMeasurement3_1 {
    /// Value for $PnE
    scale: Scale,

    /// Value for $PnL
    pub wavelengths: OptionalKw<Wavelengths>,

    /// Value for $PnG
    gain: OptionalKw<Gain>,

    /// Value for $PnCALIBRATION
    pub calibration: OptionalKw<Calibration3_1>,

    /// Value for $PnDISPLAY
    pub display: OptionalKw<Display>,
}

/// Measurement fields specific to version 3.2
#[derive(Clone, Serialize)]
pub struct InnerMeasurement3_2 {
    /// Value for $PnE
    scale: Scale,

    /// Value for $PnL
    pub wavelengths: OptionalKw<Wavelengths>,

    /// Value for $PnG
    gain: OptionalKw<Gain>,

    /// Value for $PnCALIBRATION
    pub calibration: OptionalKw<Calibration3_2>,

    /// Value for $PnDISPLAY
    pub display: OptionalKw<Display>,

    /// Value for $PnANALYTE
    pub analyte: OptionalKw<Analyte>,

    /// Value for $PnFEATURE
    pub feature: OptionalKw<Feature>,

    /// Value for $PnTYPE
    measurement_type: OptionalKw<MeasurementType>,

    /// Value for $PnTAG
    pub tag: OptionalKw<Tag>,

    /// Value for $PnDET
    pub detector_name: OptionalKw<DetectorName>,

    /// Value for $PnDATATYPE
    pub datatype: OptionalKw<NumType>,
}

pub type TimeChannel2_0 = TimeChannel<InnerTime2_0>;
pub type TimeChannel3_0 = TimeChannel<InnerTime3_0>;
pub type TimeChannel3_1 = TimeChannel<InnerTime3_1>;
pub type TimeChannel3_2 = TimeChannel<InnerTime3_2>;

pub type Measurement2_0 = Measurement<InnerMeasurement2_0>;
pub type Measurement3_0 = Measurement<InnerMeasurement3_0>;
pub type Measurement3_1 = Measurement<InnerMeasurement3_1>;
pub type Measurement3_2 = Measurement<InnerMeasurement3_2>;

pub type Measurements2_0 = Measurements<OptionalKwFamily, InnerTime2_0, InnerMeasurement2_0>;
pub type Measurements3_0 = Measurements<OptionalKwFamily, InnerTime3_0, InnerMeasurement3_0>;
pub type Measurements3_1 = Measurements<IdentityFamily, InnerTime3_1, InnerMeasurement3_1>;
pub type Measurements3_2 = Measurements<IdentityFamily, InnerTime3_2, InnerMeasurement3_2>;

pub type Metadata2_0 = Metadata<InnerMetadata2_0>;
pub type Metadata3_0 = Metadata<InnerMetadata3_0>;
pub type Metadata3_1 = Metadata<InnerMetadata3_1>;
pub type Metadata3_2 = Metadata<InnerMetadata3_2>;

impl<T> TimeChannel<T>
where
    T: VersionedTime,
{
    pub fn new_common(bytes: Width, range: Range, specific: T) -> Self {
        Self {
            bytes,
            range,
            specific,
            longname: None.into(),
            nonstandard_keywords: HashMap::new(),
        }
    }

    fn lookup_time_channel(st: &mut KwParser, i: MeasIdx) -> Option<Self>
    where
        T: LookupTime,
    {
        if let (Some(bytes), Some(range), Some(specific)) = (
            st.lookup_meas_req(i),
            st.lookup_meas_req(i),
            T::lookup_specific(st, i),
        ) {
            Some(TimeChannel {
                bytes,
                range,
                longname: st.lookup_meas_opt(i, false),
                specific,
                nonstandard_keywords: st.lookup_all_meas_nonstandard(i),
            })
        } else {
            None
        }
    }

    fn convert<ToT>(self) -> TimeChannel<ToT>
    where
        ToT: From<T>,
    {
        TimeChannel {
            bytes: self.bytes,
            range: self.range,
            longname: self.longname,
            nonstandard_keywords: self.nonstandard_keywords,
            specific: self.specific.into(),
        }
    }

    fn req_meas_keywords(&self, n: MeasIdx) -> RawPairs {
        [self.bytes.pair(n), self.range.pair(n)]
            .into_iter()
            .collect()
    }

    fn req_meta_keywords(&self) -> RawPairs {
        self.specific.req_meta_keywords_inner()
    }

    fn opt_meas_keywords(&self, n: MeasIdx) -> RawPairs {
        [OptMeasKey::pair(&self.longname, n)]
            .into_iter()
            .chain(self.specific.opt_meas_keywords_inner(n))
            .flat_map(|(k, v)| v.map(|x| (k, x)))
            .collect()
    }

    fn layout_data(&self) -> ColumnLayoutData<()> {
        ColumnLayoutData {
            width: self.bytes,
            range: self.range.clone(),
            datatype: (),
        }
    }
}

impl<P> Measurement<P>
where
    P: VersionedMeasurement,
{
    // TODO renameme
    pub fn bytes(&self) -> Width {
        self.bytes
    }

    pub fn range(&self) -> &Range {
        &self.range
    }

    pub fn new_common(bytes: Width, range: Range, specific: P) -> Self {
        Self {
            bytes,
            range,
            specific,
            detector_type: None.into(),
            detector_voltage: None.into(),
            filter: None.into(),
            longname: None.into(),
            percent_emitted: None.into(),
            power: None.into(),
            nonstandard_keywords: HashMap::new(),
        }
    }

    fn layout_data(&self) -> ColumnLayoutData<()> {
        ColumnLayoutData {
            width: self.bytes(),
            range: self.range().clone(),
            datatype: (),
        }
    }

    fn try_convert<ToP: TryFrom<P, Error = MeasConvertError>>(
        self,
        n: MeasIdx,
    ) -> Result<Measurement<ToP>, String> {
        self.specific
            .try_into()
            .map_err(|e: MeasConvertError| e.fmt(n))
            .map(|specific| Measurement {
                bytes: self.bytes,
                range: self.range,
                longname: self.longname,
                detector_type: self.detector_type,
                detector_voltage: self.detector_voltage,
                filter: self.filter,
                power: self.power,
                percent_emitted: self.percent_emitted,
                nonstandard_keywords: self.nonstandard_keywords,
                specific,
            })
    }

    // fn longname(&self, n: usize) -> Longname {
    //     // TODO not DRY
    //     self.longname
    //         .0
    //         .as_ref()
    //         .cloned()
    //         .unwrap_or(Longname(format!("M{n}")))
    // }

    // fn set_longname(&mut self, n: Option<String>) {
    //     self.longname = n.map(|y| y.into()).into();
    // }

    fn lookup_measurement(st: &mut KwParser, i: MeasIdx) -> Option<Self>
    where
        P: LookupMeasurement,
    {
        let v = P::fcs_version();
        if let (Some(bytes), Some(range), Some(specific)) = (
            st.lookup_meas_req(i),
            st.lookup_meas_req(i),
            P::lookup_specific(st, i),
        ) {
            Some(Measurement {
                bytes,
                range,
                longname: st.lookup_meas_opt(i, false),
                filter: st.lookup_meas_opt(i, false),
                power: st.lookup_meas_opt(i, false),
                detector_type: st.lookup_meas_opt(i, false),
                percent_emitted: st.lookup_meas_opt(i, v == Version::FCS3_2),
                detector_voltage: st.lookup_meas_opt(i, false),
                specific,
                nonstandard_keywords: st.lookup_all_meas_nonstandard(i),
            })
        } else {
            None
        }
    }

    fn req_keywords(&self, n: MeasIdx) -> RawTriples {
        [self.bytes.triple(n), self.range.triple(n)]
            .into_iter()
            .chain(self.specific.req_suffixes_inner(n))
            .collect()
    }

    fn opt_keywords(&self, n: MeasIdx) -> RawOptTriples {
        [
            OptMeasKey::triple(&self.longname, n),
            OptMeasKey::triple(&self.filter, n),
            OptMeasKey::triple(&self.power, n),
            OptMeasKey::triple(&self.detector_type, n),
            OptMeasKey::triple(&self.percent_emitted, n),
            OptMeasKey::triple(&self.detector_voltage, n),
        ]
        .into_iter()
        .chain(self.specific.opt_suffixes_inner(n))
        .collect()
    }

    // TODO move out, this is specific to the CLI interface
    // for table
    fn table_pairs(&self) -> Vec<(String, Option<String>)> {
        // zero is a dummy and not meaningful here
        let n = 0.into();
        self.req_keywords(n)
            .into_iter()
            .map(|(t, _, v)| (t, Some(v)))
            .chain(self.opt_keywords(n).into_iter().map(|(k, _, v)| (k, v)))
            .collect()
    }

    fn table_header(&self) -> Vec<String> {
        ["index".into(), "$PnN".into()]
            .into_iter()
            .chain(self.table_pairs().into_iter().map(|(k, _)| k))
            .collect()
    }

    fn table_row(&self, i: MeasIdx, n: Option<&Shortname>) -> Vec<String> {
        let na = || "NA".into();
        [i.to_string(), n.map_or(na(), |x| x.to_string())]
            .into_iter()
            .chain(
                self.table_pairs()
                    .into_iter()
                    .map(|(_, v)| v)
                    .map(|v| v.unwrap_or(na())),
            )
            .collect()
    }

    // TODO this name is weird, this is standard+nonstandard keywords
    // after filtering out None values
    fn all_req_keywords(&self, n: MeasIdx) -> RawPairs {
        self.req_keywords(n)
            .into_iter()
            .map(|(_, k, v)| (k, v))
            .collect()
    }

    fn all_opt_keywords(&self, n: MeasIdx) -> RawPairs {
        self.opt_keywords(n)
            .into_iter()
            .filter_map(|(_, k, v)| v.map(|x| (k, x)))
            .chain(
                self.nonstandard_keywords
                    .iter()
                    .map(|(k, v)| (k.as_ref().to_string(), v.clone())),
            )
            .collect()
    }

    // fn as_column_type(
    //     &self,
    //     dt: AlphaNumType,
    //     byteord: &ByteOrd,
    // ) -> Result<Option<MixedType>, Vec<String>> {
    //     let mdt = self.specific.datatype().map(|d| d.into()).unwrap_or(dt);
    //     let rng = self.range();
    //     MixedType::try_new(*self.bytes(), mdt, byteord, rng)
    // }
}

impl<N, P, T> Measurements<N, T, P>
where
    N: MightHave,
    T: VersionedTime,
    P: VersionedMeasurement,
{
    fn layout_data(&self) -> Vec<ColumnLayoutData<()>> {
        self.iter()
            .map(|x| {
                x.1.map_or_else(|m| m.value.layout_data(), |t| t.value.layout_data())
            })
            .collect()
    }
}

impl<M> Metadata<M>
where
    M: VersionedMetadata,
{
    pub fn new(datatype: AlphaNumType, specific: M) -> Self {
        Metadata {
            datatype,
            abrt: None.into(),
            cells: None.into(),
            com: None.into(),
            exp: None.into(),
            fil: None.into(),
            inst: None.into(),
            lost: None.into(),
            op: None.into(),
            proj: None.into(),
            smno: None.into(),
            src: None.into(),
            sys: None.into(),
            tr: None.into(),
            nonstandard_keywords: HashMap::new(),
            specific,
        }
    }

    /// Show $DATATYPE
    pub fn datatype(&self) -> AlphaNumType {
        self.datatype
    }

    fn try_convert<ToM: TryFromMetadata<M>>(
        self,
        convert: SizeConvert<M::D>,
    ) -> Result<Metadata<ToM>, Vec<String>> {
        // TODO this seems silly, break struct up into common bits
        ToM::try_from_meta(self.specific, convert)
            .map_err(|es: MetaConvertErrors| es.into_iter().map(|s| s.to_string()).collect())
            .map(|specific| Metadata {
                abrt: self.abrt,
                cells: self.cells,
                com: self.com,
                datatype: self.datatype,
                exp: self.exp,
                fil: self.fil,
                inst: self.inst,
                lost: self.lost,
                op: self.op,
                proj: self.proj,
                smno: self.smno,
                sys: self.sys,
                src: self.src,
                tr: self.tr,
                nonstandard_keywords: self.nonstandard_keywords,
                specific,
            })
    }

    fn lookup_metadata(st: &mut KwParser, ms: &Measurements<M::N, M::T, M::P>) -> Option<Self>
    where
        M: LookupMetadata,
    {
        let par = Par(ms.len());
        st.lookup_meta_req()
            .zip(M::lookup_specific(st, par))
            .map(|(datatype, specific)| Metadata {
                datatype,
                abrt: st.lookup_meta_opt(false),
                com: st.lookup_meta_opt(false),
                cells: st.lookup_meta_opt(false),
                exp: st.lookup_meta_opt(false),
                fil: st.lookup_meta_opt(false),
                inst: st.lookup_meta_opt(false),
                lost: st.lookup_meta_opt(false),
                op: st.lookup_meta_opt(false),
                proj: st.lookup_meta_opt(false),
                smno: st.lookup_meta_opt(false),
                src: st.lookup_meta_opt(false),
                sys: st.lookup_meta_opt(false),
                tr: st.lookup_meta_opt(false),
                nonstandard_keywords: st.lookup_all_nonstandard(),
                specific,
            })
    }

    fn all_req_keywords(&self, par: Par) -> RawPairs {
        [par.pair(), self.datatype.pair()]
            .into_iter()
            .chain(self.specific.keywords_req_inner())
            .collect()
    }

    fn all_opt_keywords(&self) -> RawPairs {
        [
            OptMetaKey::pair(&self.abrt),
            OptMetaKey::pair(&self.com),
            OptMetaKey::pair(&self.cells),
            OptMetaKey::pair(&self.exp),
            OptMetaKey::pair(&self.fil),
            OptMetaKey::pair(&self.inst),
            OptMetaKey::pair(&self.lost),
            OptMetaKey::pair(&self.op),
            OptMetaKey::pair(&self.proj),
            OptMetaKey::pair(&self.smno),
            OptMetaKey::pair(&self.src),
            OptMetaKey::pair(&self.sys),
            OptMetaKey::pair(&self.tr),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .chain(self.specific.keywords_opt_inner())
        .map(|(k, v)| (k.to_string(), v))
        .chain(
            self.nonstandard_keywords
                .iter()
                .map(|(k, v)| (k.as_ref().to_string(), v.clone())),
        )
        .collect()
    }

    fn reassign_trigger(&mut self, mapping: &NameMapping) {
        self.tr.0.as_mut().map_or((), |tr| tr.reassign(mapping))
    }

    fn reassign_all(&mut self, mapping: &NameMapping) {
        self.reassign_trigger(mapping);
        self.specific.with_spillover(|s| {
            s.reassign(mapping);
            Ok(())
        });
        self.specific.with_unstainedcenters(|u| {
            u.reassign(mapping);
            Ok(())
        });
    }

    fn check_trigger(&self, names: &HashSet<&Shortname>) -> Result<(), String> {
        self.tr.0.as_ref().map_or(Ok(()), |tr| tr.check_link(names))
    }

    fn check_unstainedcenters(&self, names: &HashSet<&Shortname>) -> Result<(), String> {
        self.specific
            .as_unstainedcenters()
            .map_or(Ok(()), |x| x.check_link(names))
    }

    fn check_spillover(&self, names: &HashSet<&Shortname>) -> Result<(), String> {
        self.specific
            .as_spillover()
            .map_or(Ok(()), |x| x.check_link(names))
    }

    fn remove_trigger_by_name(&mut self, n: &Shortname) -> bool {
        if self.tr.as_ref_opt().is_some_and(|m| &m.measurement == n) {
            self.tr = None.into();
            true
        } else {
            false
        }
    }

    fn remove_name_index(&mut self, n: &Shortname, i: MeasIdx) {
        self.remove_trigger_by_name(n);
        let s = &mut self.specific;
        s.with_spillover(|x| x.remove_by_name(n));
        s.with_unstainedcenters(|u| u.remove(n));
        s.with_compensation(|c| c.remove_by_index(i));
    }
}

pub trait Versioned {
    fn fcs_version() -> Version;
}

pub(crate) trait LookupMetadata: Sized + VersionedMetadata {
    fn lookup_shortname(
        st: &mut KwParser,
        n: MeasIdx,
    ) -> Option<<Self::N as MightHave>::Wrapper<Shortname>>;

    fn lookup_specific(st: &mut KwParser, par: Par) -> Option<Self>;
}

pub trait TryFromMetadata<M>: Sized
where
    Self: VersionedMetadata,
    M: VersionedMetadata,
{
    fn try_from_meta(value: M, byteord: SizeConvert<M::D>) -> Result<Self, MetaConvertErrors>;
}

pub trait VersionedMetadata: Sized {
    type P: VersionedMeasurement;
    type T: VersionedTime;
    type N: MightHave;
    type L: VersionedDataLayout;
    type D;

    fn as_unstainedcenters(&self) -> Option<&UnstainedCenters>;

    fn with_unstainedcenters<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut UnstainedCenters) -> Result<X, ClearOptional>;

    fn as_spillover(&self) -> Option<&Spillover>;

    fn with_spillover<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut Spillover) -> Result<X, ClearOptional>;

    fn as_compensation(&self) -> Option<&Compensation>;

    fn with_compensation<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut Compensation) -> Result<X, ClearOptional>;

    fn timestamps_valid(&self) -> bool;

    fn datetimes_valid(&self) -> bool;

    // TODO borrow?
    fn byteord(&self) -> Self::D;

    fn keywords_req_inner(&self) -> RawPairs;

    fn keywords_opt_inner(&self) -> RawPairs;

    fn as_column_layout(
        metadata: &Metadata<Self>,
        ms: &Measurements<Self::N, Self::T, Self::P>,
    ) -> Result<Self::L, Vec<String>>;
}

pub trait VersionedMeasurement: Sized + Versioned {
    fn req_suffixes_inner(&self, n: MeasIdx) -> RawTriples;

    fn opt_suffixes_inner(&self, n: MeasIdx) -> RawOptTriples;

    fn datatype(&self) -> Option<NumType>;
}

pub(crate) trait LookupMeasurement: Sized + VersionedMeasurement {
    fn lookup_specific(st: &mut KwParser, n: MeasIdx) -> Option<Self>;
}

pub trait VersionedTime: Sized {
    fn timestep(&self) -> Option<Timestep>;

    fn datatype(&self) -> Option<NumType>;

    fn set_timestep(&mut self, ts: Timestep);

    fn req_meta_keywords_inner(&self) -> RawPairs;

    fn opt_meas_keywords_inner(&self, _: MeasIdx) -> RawOptPairs;
}

pub(crate) trait LookupTime: VersionedTime {
    fn lookup_specific(st: &mut KwParser, n: MeasIdx) -> Option<Self>;
}

pub(crate) type RawPairs = Vec<(String, String)>;
pub(crate) type RawTriples = Vec<(String, String, String)>;
pub(crate) type RawOptPairs = Vec<(String, Option<String>)>;
pub(crate) type RawOptTriples = Vec<(String, String, Option<String>)>;

/// A structure to look up and parse keywords in the TEXT segment
///
/// Given a hash table of keywords (as String pairs) and a configuration, this
/// provides an interface to extract keywords. If found, the keyword will be
/// removed from the table and parsed to its native type (number, list, matrix,
/// etc). If lookup or parsing fails, an error/warning will be logged within the
/// state depending on if the key is required or optional (among other things
/// depending on the keyword).
///
/// Errors in all cases are deferred until the end of the state's lifetime, at
/// which point the errors are returned along with the result of the computation
/// or failure (if applicable).
///
/// For Haskell zealots, this is somewhat like a pure state monad in that it has
/// a 'run' interface which takes an input and a function to act within the
/// state. The main difference is that it modifies the keywords in-place
/// and returns the error result (rather than purely returning both).
pub(crate) struct KwParser<'a, 'b> {
    raw_keywords: &'b mut RawKeywords,
    deferred: PureErrorBuf,
    conf: &'a StdTextReadConfig,
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
    fn try_run<X, F>(
        kws: &'b mut RawKeywords,
        conf: &'a StdTextReadConfig,
        reason: String,
        f: F,
    ) -> PureResult<X>
    where
        F: FnOnce(&mut Self) -> Option<X>,
    {
        let mut st = Self::from(kws, conf);
        if let Some(data) = f(&mut st) {
            Ok(PureSuccess {
                data,
                deferred: st.collect(),
            })
        } else {
            Err(st.into_failure(reason))
        }
    }

    fn lookup_timestamps<T>(&mut self, dep: bool) -> Timestamps<T>
    where
        T: PartialOrd,
        T: Copy,
        Btim<T>: OptMetaKey,
        <Btim<T> as FromStr>::Err: fmt::Display,
        Etim<T>: OptMetaKey,
        <Etim<T> as FromStr>::Err: fmt::Display,
    {
        Timestamps::new(
            self.lookup_meta_opt(dep),
            self.lookup_meta_opt(dep),
            self.lookup_meta_opt(dep),
        )
        .unwrap_or_else(|e| {
            self.deferred.push_warning(e.to_string());
            Timestamps::default()
        })
    }

    fn lookup_datetimes(&mut self) -> Datetimes {
        let b = self.lookup_meta_opt(false);
        let e = self.lookup_meta_opt(false);
        Datetimes::new(b, e).unwrap_or_else(|w| {
            self.deferred.push_warning(w.to_string());
            Datetimes::default()
        })
    }

    fn lookup_modification(&mut self) -> ModificationData {
        ModificationData {
            last_modifier: self.lookup_meta_opt(false),
            last_modified: self.lookup_meta_opt(false),
            originality: self.lookup_meta_opt(false),
        }
    }

    fn lookup_plate(&mut self, dep: bool) -> PlateData {
        PlateData {
            wellid: self.lookup_meta_opt(dep),
            platename: self.lookup_meta_opt(dep),
            plateid: self.lookup_meta_opt(dep),
        }
    }

    fn lookup_carrier(&mut self) -> CarrierData {
        CarrierData {
            locationid: self.lookup_meta_opt(false),
            carrierid: self.lookup_meta_opt(false),
            carriertype: self.lookup_meta_opt(false),
        }
    }

    fn lookup_unstained(&mut self) -> UnstainedData {
        UnstainedData {
            unstainedcenters: self.lookup_meta_opt(false),
            unstainedinfo: self.lookup_meta_opt(false),
        }
    }

    fn lookup_compensation_2_0(&mut self, par: Par) -> OptionalKw<Compensation> {
        // column = src channel
        // row = target channel
        // These are "flipped" in 2.0, where "column" goes TO the "row"
        let n = par.0;
        let mut any_error = false;
        let mut matrix = DMatrix::<f32>::identity(n, n);
        for r in 0..n {
            for c in 0..n {
                let k = format!("DFC{c}TO{r}");
                if let Some(x) = self.lookup_dfc(k.as_str()) {
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

    fn lookup_dfc(&mut self, k: &str) -> Option<f32> {
        self.raw_keywords.remove(k).and_then(|v| {
            v.parse::<f32>()
                .inspect_err(|e| {
                    let msg = format!("{e} (key={k}, value='{v}'))");
                    self.deferred.push_warning(msg);
                })
                .ok()
        })
    }

    fn lookup_all_nonstandard(&mut self) -> NonStdKeywords {
        let mut ns = HashMap::new();
        self.raw_keywords.retain(|k, v| {
            if let Ok(nk) = k.parse::<NonStdKey>() {
                ns.insert(nk, v.clone());
                false
            } else {
                true
            }
        });
        ns
    }

    // TODO I don't really need a hash table for this. It would be easier and
    // probably faster to just use a paired vector, although I would need to
    // ensure the keys are unique.
    fn lookup_all_meas_nonstandard(&mut self, n: MeasIdx) -> NonStdKeywords {
        let mut ns = HashMap::new();
        // ASSUME the pattern does not start with "$" and has a %n which will be
        // subbed for the measurement index. The pattern will then be turned
        // into a legit rust regular expression, which may fail depending on
        // what %n does, so check it each time.
        if let Some(p) = &self.conf.nonstandard_measurement_pattern {
            match p.from_index(n) {
                Ok(pattern) => self.raw_keywords.retain(|k, v| {
                    if let Some(nk) = pattern.try_match(k.as_str()) {
                        ns.insert(nk, v.clone());
                        false
                    } else {
                        true
                    }
                }),
                Err(err) => self.deferred.push_warning(err.to_string()),
            }
        }
        ns
    }

    // auxiliary functions

    fn collect(self) -> PureErrorBuf {
        self.deferred
    }

    fn into_failure(self, reason: String) -> PureFailure {
        Failure {
            reason,
            deferred: self.collect(),
        }
    }

    fn from(kws: &'b mut RawKeywords, conf: &'a StdTextReadConfig) -> Self {
        KwParser {
            raw_keywords: kws,
            deferred: PureErrorBuf::default(),
            conf,
        }
    }

    fn lookup_meta_req<V>(&mut self) -> Option<V>
    where
        V: ReqMetaKey,
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
    {
        V::remove_meta_req(self.raw_keywords)
            .map_err(|e| self.deferred.push_error(e))
            .ok()
    }

    fn lookup_meta_opt<V>(&mut self, dep: bool) -> OptionalKw<V>
    where
        V: OptMetaKey,
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
    {
        let res = V::remove_meta_opt(self.raw_keywords);
        self.process_opt(res, V::std(), dep)
    }

    fn lookup_meas_req<V>(&mut self, n: MeasIdx) -> Option<V>
    where
        V: ReqMeasKey,
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
    {
        V::remove_meas_req(self.raw_keywords, n)
            .map_err(|e| self.deferred.push_error(e))
            .ok()
    }

    fn lookup_meas_opt<V>(&mut self, n: MeasIdx, dep: bool) -> OptionalKw<V>
    where
        V: OptMeasKey,
        V: FromStr,
        <V as FromStr>::Err: fmt::Display,
    {
        let res = V::remove_meas_opt(self.raw_keywords, n);
        self.process_opt(res, V::std(n), dep)
    }

    fn process_opt<V>(
        &mut self,
        res: Result<OptionalKw<V>, String>,
        k: String,
        dep: bool,
    ) -> OptionalKw<V> {
        res.inspect(|_| {
            if dep {
                let msg = format!("deprecated key: {k}");
                self.deferred
                    .push_msg_leveled(msg, self.conf.disallow_deprecated);
            }
        })
        .map_err(|e| self.deferred.push_warning(e))
        .unwrap_or(None.into())
    }
}

// TODO generalize this
pub enum MeasConvertError {
    NoScale,
}

impl MeasConvertError {
    fn fmt(&self, n: MeasIdx) -> String {
        match self {
            MeasConvertError::NoScale => format!("$PnE not found when converting measurement {n}"),
        }
    }
}

type TryFromTimeError<T> = TryFromErrorReset<MeasToTimeErrors, T>;

pub(crate) type MeasToTimeErrors = NonEmpty<MeasToTimeError>;

pub enum MeasToTimeError {
    NonLinear,
    HasGain,
    NotTimeType,
}

impl fmt::Display for MeasToTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            MeasToTimeError::NonLinear => write!(f, "$PnE must be '0,0'"),
            MeasToTimeError::HasGain => write!(f, "$PnG must not be set"),
            MeasToTimeError::NotTimeType => write!(f, "$PnTYPE must not be set"),
        }
    }
}

pub(crate) type MetaConvertErrors = Vec<MetaConvertError>;

pub enum MetaConvertError {
    NoCyt,
    FromByteOrd(FromByteOrdError),
    FromEndian,
}

impl fmt::Display for MetaConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            MetaConvertError::NoCyt => write!(f, "$CYT is missing"),
            MetaConvertError::FromEndian => write!(
                f,
                "Could not convert Endian to ByteOrd since measurement widths \
                 are not all the same size or are different from $DATATYPE",
            ),
            MetaConvertError::FromByteOrd(e) => e.fmt(f),
        }
    }
}

impl<M, T> TryFrom<Measurement<M>> for TimeChannel<T>
where
    T: TryFrom<M, Error = TryFromTimeError<M>>,
{
    type Error = TryFromErrorReset<MeasToTimeErrors, Measurement<M>>;
    fn try_from(value: Measurement<M>) -> Result<Self, Self::Error> {
        match value.specific.try_into() {
            Ok(specific) => Ok(Self {
                bytes: value.bytes,
                range: value.range,
                longname: value.longname,
                nonstandard_keywords: value.nonstandard_keywords,
                specific,
            }),
            Err(old) => Err(TryFromErrorReset {
                error: old.error,
                value: Measurement {
                    specific: old.value,
                    ..value
                },
            }),
        }
    }
}

impl<M, T> From<TimeChannel<T>> for Measurement<M>
where
    M: From<T>,
{
    fn from(value: TimeChannel<T>) -> Self {
        Self {
            bytes: value.bytes,
            range: value.range,
            longname: value.longname,
            detector_type: None.into(),
            detector_voltage: None.into(),
            filter: None.into(),
            percent_emitted: None.into(),
            power: None.into(),
            nonstandard_keywords: value.nonstandard_keywords,
            specific: value.specific.into(),
        }
    }
}

pub struct FromByteOrdError;

impl fmt::Display for FromByteOrdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "could not convert ByteOrd, must be either '1,2,3,4' or '4,3,2,1'",
        )
    }
}

/// A bundle for $ORIGINALITY, $LAST_MODIFIER, and $LAST_MODIFIED (3.1+)
#[derive(Clone, Serialize, Default)]
pub struct ModificationData {
    pub last_modifier: OptionalKw<LastModifier>,
    pub last_modified: OptionalKw<ModifiedDateTime>,
    pub originality: OptionalKw<Originality>,
}

/// A bundle for $PLATEID, $PLATENAME, and $WELLID (3.1+)
#[derive(Clone, Serialize, Default)]
pub struct PlateData {
    pub plateid: OptionalKw<Plateid>,
    pub platename: OptionalKw<Platename>,
    pub wellid: OptionalKw<Wellid>,
}

/// A bundle for $UNSTAINEDCENTERS and $UNSTAINEDINFO (3.2+)
#[derive(Clone, Serialize, Default)]
pub struct UnstainedData {
    unstainedcenters: OptionalKw<UnstainedCenters>,
    pub unstainedinfo: OptionalKw<UnstainedInfo>,
}

/// A bundle for $CARRIERID, $CARRIERTYPE, $LOCATIONID (3.2+)
#[derive(Clone, Serialize, Default)]
pub struct CarrierData {
    pub carrierid: OptionalKw<Carrierid>,
    pub carriertype: OptionalKw<Carriertype>,
    pub locationid: OptionalKw<Locationid>,
}

pub(crate) type Measurements<N, T, P> =
    NamedVec<N, <N as MightHave>::Wrapper<Shortname>, TimeChannel<T>, Measurement<P>>;

macro_rules! non_time_get_set {
    ($get:ident, $set:ident, $ty:ident, [$($root:ident)*], $field:ident, $kw:ident) => {
        /// Get $$kw value for all non-time measurements
        pub fn $get(&self) -> Vec<(MeasIdx, Option<&$ty>)> {
            self.measurements
                .iter_non_center_values()
                .map(|(i, m)| (i, m.$($root.)*$field.as_ref_opt()))
                .collect()
        }

        /// Set $$kw value for for all non-time measurements
        pub fn $set(&mut self, xs: Vec<Option<$ty>>) -> bool {
            self.measurements.alter_non_center_values_zip(xs, |m, x| {
                m.$($root.)*$field = x.into();
            }).is_some()
        }
    };
}

macro_rules! comp_methods {
    () => {
        /// Return matrix for $COMP
        pub fn compensation(&self) -> Option<&Compensation> {
            self.metadata.specific.comp.as_ref_opt()
        }

        /// Set matrix for $COMP
        ///
        /// Return true if successfully set. Return false if matrix is either not
        /// square or rows/columns are not the same length as $PAR.
        pub fn set_compensation(&mut self, matrix: DMatrix<f32>) -> bool {
            if !matrix.is_square() && matrix.ncols() != self.par().0 {
                // TODO None here is actually an error
                self.metadata.specific.comp = Compensation::new(matrix).into();
                true
            } else {
                false
            }
        }

        /// Clear $COMP
        pub fn unset_compensation(&mut self) {
            self.metadata.specific.comp = None.into();
        }
    };
}

macro_rules! scale_get_set {
    ($t:path, $time_default:expr) => {
        /// Show $PnE for each measurement, including time
        pub fn all_scales(&self) -> Vec<$t> {
            self.measurements
                .iter()
                .map(|(_, x)| x.map_or($time_default, |p| p.value.specific.scale.into()))
                .collect()
        }

        /// Show $PnE for each measurement, not including time
        pub fn scales(&self) -> Vec<(MeasIdx, $t)> {
            self.measurements
                .iter_non_center_values()
                .map(|(i, m)| (i, m.specific.scale.into()))
                .collect()
        }

        /// Set $PnE for for all non-time measurements
        pub fn set_scales(&mut self, xs: Vec<$t>) -> bool {
            self.measurements
                .alter_non_center_values_zip(xs, |m, x| {
                    m.specific.scale = x.into();
                })
                .is_some()
        }
    };
}

pub(crate) type VersionedCoreTEXT<M> = CoreTEXT<
    M,
    <M as VersionedMetadata>::T,
    <M as VersionedMetadata>::P,
    <M as VersionedMetadata>::N,
    <<M as VersionedMetadata>::N as MightHave>::Wrapper<Shortname>,
>;

impl<M> VersionedCoreTEXT<M>
where
    M: VersionedMetadata,
    M::N: Clone,
{
    /// Write this structure to a handle.
    ///
    /// The actual bytes written will be the HEADER and TEXT, including the
    /// last delimiter.
    pub(crate) fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        tot: Tot,
        data_len: usize,
        analysis_len: usize,
        conf: &WriteConfig,
    ) -> io::Result<bool> {
        // TODO newtypes for data and analysis lenth to make them more obvious
        if let Some(ts) = self.text_segment(tot, data_len, analysis_len) {
            for t in ts {
                h.write_all(t.as_bytes())?;
                h.write_all(&[conf.delim.inner()])?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn into_dataset(
        self,
        columns: Vec<AnyFCSColumn>,
        analysis: Analysis,
    ) -> Result<VersionedCoreDataset<M>, String> {
        let mut dataset: VersionedCoreDataset<M> = self.into();
        dataset.set_data(columns)?;
        dataset.analysis = analysis;
        Ok(dataset)
    }

    /// Return HEADER+TEXT as a list of strings
    ///
    /// The first member will be a string exactly 58 bytes long which will be
    /// the HEADER. The next members will be alternating keys and values of the
    /// primary TEXT segment and supplemental if included. Each member should be
    /// separated by a delimiter when writing to a file, and a trailing
    /// delimiter should be added.
    ///
    /// Keywords will be sorted with offsets first, non-measurement keywords
    /// second in alphabetical order, then measurement keywords in alphabetical
    /// order.
    ///
    /// Return None if primary TEXT does not fit into first 99,999,999 bytes.
    fn text_segment(&self, tot: Tot, data_len: usize, analysis_len: usize) -> Option<Vec<String>> {
        self.header_and_raw_keywords(tot, data_len, analysis_len)
            .map(|(header, kws)| {
                let version = M::P::fcs_version();
                let flat: Vec<_> = kws.into_iter().flat_map(|(k, v)| [k, v]).collect();
                [format!("{version}{header}")]
                    .into_iter()
                    .chain(flat)
                    .collect()
            })
    }

    /// Return all keywords as an ordered list of pairs
    ///
    /// Thiw will only include keywords that can be directly derived from
    /// [CoreTEXT]. This means it will not include $TOT, since this depends on
    /// the DATA segment.
    pub fn raw_keywords(&self, want_req: Option<bool>, want_meta: Option<bool>) -> RawKeywords {
        let (req_meas, req_meta, _) = self.req_meta_meas_keywords();
        let (opt_meas, opt_meta, _) = self.opt_meta_meas_keywords();

        let triop = |op| match op {
            None => (true, true),
            Some(true) => (true, false),
            Some(false) => (false, true),
        };

        let keep = |xs, t1, t2| {
            if t1 && t2 {
                xs
            } else {
                vec![]
            }
        };

        let (keep_req, keep_opt) = triop(want_req);
        let (keep_meta, keep_meas) = triop(want_meta);

        keep(req_meta, keep_req, keep_meta)
            .into_iter()
            .chain(keep(req_meas, keep_req, keep_meas))
            .chain(keep(opt_meta, keep_opt, keep_meta))
            .chain(keep(opt_meas, keep_opt, keep_meas))
            .collect()
    }

    /// Get measurement name for $TR keyword
    pub fn trigger_name(&self) -> Option<&Shortname> {
        self.metadata.tr.as_ref_opt().map(|x| &x.measurement)
    }

    /// Get threshold for $TR keyword
    pub fn trigger_threshold(&self) -> Option<u32> {
        self.metadata.tr.as_ref_opt().map(|x| x.threshold)
    }

    // TODO return an error for these since returning false is less obvious
    // and doesn't impl try
    /// Set measurement name for $TR keyword.
    pub fn set_trigger_name(&mut self, n: Shortname) -> bool {
        if !self.measurement_names().contains(&n) {
            return false;
        }
        if let Some(tr) = self.metadata.tr.0.as_mut() {
            tr.measurement = n;
        } else {
            self.metadata.tr = Some(Trigger {
                measurement: n,
                threshold: 0,
            })
            .into();
        }
        true
    }

    /// Set threshold for $TR keyword
    pub fn set_trigger_threshold(&mut self, x: u32) -> bool {
        if let Some(tr) = self.metadata.tr.0.as_mut() {
            tr.threshold = x;
            true
        } else {
            false
        }
    }

    /// Remove $TR keyword
    pub fn clear_trigger(&mut self) {
        self.metadata.tr = None.into();
    }

    /// Return a list of measurement names as stored in $PnN.
    pub fn shortnames_maybe(&self) -> Vec<Option<&Shortname>> {
        self.measurements
            .iter()
            .map(|(_, x)| x.map_or_else(|t| Some(&t.key), |m| M::N::as_opt(&m.key)))
            .collect()
    }

    /// Return a list of measurement names as stored in $PnN
    ///
    /// For cases where $PnN is optional and its value is not given, this will
    /// return "Mn" where "n" is the parameter index starting at 0.
    pub fn all_shortnames(&self) -> Vec<Shortname> {
        self.measurements.iter_all_names().collect()
    }

    /// Set all $PnN keywords to list of names.
    ///
    /// The length of the names must match the number of measurements. Any
    /// keywords refering to the old names will be updated to reflect the new
    /// names. For 2.0 and 3.0 which have optional $PnN, all $PnN will end up
    /// being set.
    pub fn set_all_shortnames(
        &mut self,
        ns: Vec<Shortname>,
    ) -> Result<NameMapping, DistinctKeysError> {
        let mapping = self.measurements.set_names(ns)?;
        // TODO reassign names in dataframe
        self.metadata.reassign_all(&mapping);
        Ok(mapping)
    }

    /// Set the channel matching given name to the time channel.
    ///
    /// Return error if time channel already exists or if measurement cannot
    /// be converted to a time channel.
    pub fn set_time_channel(&mut self, n: &Shortname) -> Result<(), MeasToTimeErrors>
    where
        Measurement<M::P>: From<TimeChannel<M::T>>,
        TimeChannel<M::T>: TryFrom<Measurement<M::P>, Error = TryFromTimeError<Measurement<M::P>>>,
    {
        // This is tricky because $TIMESTEP will be filled with a dummy value
        // when TimeChannel is converted from a Measurement. This will get
        // annoying for cases where the time channel already exists and we are
        // simply "moving" it to a new channel; $TIMESTEP should follow the move
        // in this case. Therefore, get the value of $TIMESTEP (if it exists)
        // and reassign to new time channel (if it exists). The added
        // complication is that not all versions have $TIMESTEP, so consult
        // trait-level functions for this and wrap in Option.
        let ms = &mut self.measurements;
        let current_timestep = ms.as_center().and_then(|c| c.value.specific.timestep());
        match ms.set_center_by_name(n) {
            Ok(center) => {
                // Technically this is a bit more work than necessary because
                // we should know by this point if the center exists. Oh well..
                if let (Some(ts), Some(c)) = (current_timestep, ms.as_center_mut()) {
                    M::T::set_timestep(&mut c.value.specific, ts);
                }
                Ok(center)
            }
            Err(e) => Err(e),
        }
    }

    /// Convert the current time channel to a measurement
    ///
    /// Return false if there was no time channel to convert.
    pub fn unset_time_channel(&mut self) -> bool
    where
        Measurement<M::P>: From<TimeChannel<M::T>>,
    {
        self.measurements.unset_center()
    }

    /// Insert a nonstandard key/value pair for each measurement.
    ///
    /// Return a vector of elements corresponding to each measurement, where
    /// each element is the value of the inserted key if already present.
    ///
    /// This includes the time channel if present.
    pub fn insert_meas_nonstandard(
        &mut self,
        xs: Vec<(NonStdKey, String)>,
    ) -> Option<Vec<Option<String>>> {
        self.measurements.alter_values_zip(
            xs,
            |x, (k, v)| x.value.nonstandard_keywords.insert(k, v),
            |x, (k, v)| x.value.nonstandard_keywords.insert(k, v),
        )
    }

    /// Remove a key from nonstandard key/value pairs for each measurement.
    ///
    /// Return a vector with removed values for each measurement if present.
    ///
    /// This includes the time channel if present.
    pub fn remove_meas_nonstandard(&mut self, xs: Vec<&NonStdKey>) -> Option<Vec<Option<String>>> {
        self.measurements.alter_values_zip(
            xs,
            |x, k| x.value.nonstandard_keywords.remove(k),
            |x, k| x.value.nonstandard_keywords.remove(k),
        )
    }

    /// Read a key from nonstandard key/value pairs for each measurement.
    ///
    /// Return a vector with each successfully found value.
    ///
    /// This includes the time channel if present.
    // TODO generlize this to take slices
    pub fn get_meas_nonstandard(&self, ks: Vec<&NonStdKey>) -> Option<Vec<Option<&String>>> {
        let ms = &self.measurements;
        if ks.len() != ms.len() {
            None
        } else {
            let res = ms
                .iter()
                .zip(ks)
                .map(|((_, x), k)| {
                    x.map_or_else(
                        |t| t.value.nonstandard_keywords.get(k),
                        |m| m.value.nonstandard_keywords.get(k),
                    )
                })
                .collect();
            Some(res)
        }
    }

    /// Return read-only reference to measurement vector
    pub fn measurements_named_vec(&self) -> &Measurements<M::N, M::T, M::P> {
        &self.measurements
    }

    /// Apply functions to time and non-time channel values
    pub fn alter_measurements<F, G, R>(&mut self, f: F, g: G) -> Vec<R>
    where
        F: Fn(
            IndexedElement<&<M::N as MightHave>::Wrapper<Shortname>, &mut Measurement<M::P>>,
        ) -> R,
        G: Fn(IndexedElement<&Shortname, &mut TimeChannel<M::T>>) -> R,
    {
        self.measurements.alter_values(f, g)
    }

    /// Apply functions to time and non-time channel values with payload
    pub fn alter_measurements_zip<F, G, X, R>(&mut self, xs: Vec<X>, f: F, g: G) -> Option<Vec<R>>
    where
        F: Fn(
            IndexedElement<&<M::N as MightHave>::Wrapper<Shortname>, &mut Measurement<M::P>>,
            X,
        ) -> R,
        G: Fn(IndexedElement<&Shortname, &mut TimeChannel<M::T>>, X) -> R,
    {
        self.measurements.alter_values_zip(xs, f, g)
    }

    /// Return mutable reference to time channel as a name/value pair.
    pub fn as_center_mut(
        &mut self,
    ) -> Option<IndexedElement<&mut Shortname, &mut TimeChannel<M::T>>> {
        self.measurements.as_center_mut()
    }

    // TODO is a measurement the same as a time channel? does that term
    // include the time channel? hmm....
    // TODO make a better way to distinguish time and measurement (or whatever
    // we end up calling these)
    pub fn remove_measurement_by_name(
        &mut self,
        n: &Shortname,
    ) -> Result<Option<(MeasIdx, Result<Measurement<M::P>, TimeChannel<M::T>>)>, String> {
        if let Some(e) = self.measurements.remove_name(n) {
            self.metadata.remove_name_index(n, e.0);
            Ok(Some(e))
        } else {
            Ok(None)
        }
    }

    pub fn remove_measurement_by_index(
        &mut self,
        index: MeasIdx,
    ) -> Result<Option<EitherPair<M::N, Measurement<M::P>, TimeChannel<M::T>>>, String> {
        if let Some(e) = self.measurements.remove_index(index) {
            if let Ok(left) = &e {
                if let Some(n) = M::N::as_opt(&left.key) {
                    self.metadata.remove_name_index(n, index);
                }
            }
            Ok(Some(e))
        } else {
            Ok(None)
        }
    }

    pub fn push_time_channel(&mut self, n: Shortname, m: TimeChannel<M::T>) -> Result<(), String> {
        self.measurements
            .push_center(n, m)
            .map_err(|e| e.to_string())
    }

    pub fn insert_time_channel(
        &mut self,
        i: MeasIdx,
        n: Shortname,
        m: TimeChannel<M::T>,
    ) -> Result<(), String> {
        self.measurements
            .insert_center(i, n, m)
            .map_err(|e| e.to_string())
    }

    // TODO this doesn't buy much since it will ulimately be easier to use
    // the type specific wrapper in practice
    pub fn push_measurement(
        &mut self,
        n: <M::N as MightHave>::Wrapper<Shortname>,
        m: Measurement<M::P>,
    ) -> Result<Shortname, String> {
        self.measurements.push(n, m).map_err(|e| e.to_string())
    }

    pub fn insert_measurement(
        &mut self,
        i: MeasIdx,
        n: <M::N as MightHave>::Wrapper<Shortname>,
        m: Measurement<M::P>,
    ) -> Result<Shortname, String> {
        self.measurements.insert(i, n, m).map_err(|e| e.to_string())
    }

    pub fn set_measurements(
        &mut self,
        xs: RawInput<M::N, TimeChannel<M::T>, Measurement<M::P>>,
        prefix: ShortnamePrefix,
    ) -> Result<(), String> {
        if self.trigger_name().is_some() {
            return Err("$TR depends on existing measurements".into());
        }
        let m = &self.metadata;
        let s = &m.specific;
        if s.as_unstainedcenters().is_some() {
            return Err("$UNSTAINEDCENTERS depends on existing measurements".into());
        }
        if s.as_compensation().is_some() || s.as_spillover().is_some() {
            return Err("$COMP/$SPILLOVER depends on existing measurements".into());
        }
        let ms = NamedVec::new(xs, prefix)?;
        self.measurements = ms;
        Ok(())
    }

    pub fn unset_measurements(&mut self) -> Result<(), String> {
        // TODO not DRY
        if self.trigger_name().is_some() {
            return Err("$TR depends on existing measurements".into());
        }
        let m = &self.metadata;
        let s = &m.specific;
        if s.as_unstainedcenters().is_some() {
            return Err("$UNSTAINEDCENTERS depends on existing measurements".into());
        }
        if s.as_compensation().is_some() || s.as_spillover().is_some() {
            return Err("$COMP/$SPILLOVER depends on existing measurements".into());
        }
        self.measurements = NamedVec::default();
        Ok(())
    }

    fn header_and_raw_keywords(
        &self,
        tot: Tot,
        data_len: usize,
        analysis_len: usize,
    ) -> Option<(String, RawKeywords)> {
        let version = M::P::fcs_version();
        let tot_pair = (Tot::std().to_string(), tot.to_string());

        let (req_meas, req_meta, req_text_len) = self.req_meta_meas_keywords();
        let (opt_meas, opt_meta, opt_text_len) = self.opt_meta_meas_keywords();

        let offset_result = if version == Version::FCS2_0 {
            make_data_offset_keywords_2_0(req_text_len + opt_text_len, data_len, analysis_len)
        } else {
            make_data_offset_keywords_3_0(req_text_len, opt_text_len, data_len, analysis_len)
        }?;

        let mut meta: Vec<_> = req_meta
            .into_iter()
            .chain(opt_meta)
            .chain([tot_pair])
            .collect();
        let mut meas: Vec<_> = req_meas.into_iter().chain(opt_meas).collect();
        meta.sort_by(|a, b| a.0.cmp(&b.0));
        meas.sort_by(|a, b| a.0.cmp(&b.0));

        let req_opt_kws: Vec<_> = meta.into_iter().chain(meas).collect();
        Some((
            offset_result.header,
            offset_result
                .offsets
                .into_iter()
                .chain(req_opt_kws)
                .collect(),
        ))
    }

    non_time_get_set!(filters, set_filters, Filter, [], filter, PnF);

    non_time_get_set!(powers, set_powers, Power, [], power, PnO);

    non_time_get_set!(
        detector_types,
        set_detector_types,
        DetectorType,
        [],
        detector_type,
        PnD
    );

    non_time_get_set!(
        percents_emitted,
        set_percents_emitted,
        PercentEmitted,
        [],
        percent_emitted,
        PnP
    );

    non_time_get_set!(
        detector_voltages,
        set_detector_voltages,
        DetectorVoltage,
        [],
        detector_voltage,
        PnV
    );

    /// Return a list of measurement names as stored in $PnS
    ///
    /// If not given, will be replaced by "Mn" where "n" is the measurement
    /// index starting at 1.
    pub fn longnames(&self) -> Vec<Option<&Longname>> {
        self.measurements
            .iter()
            .map(|x| {
                x.1.map_or_else(
                    |t| t.value.longname.as_ref_opt(),
                    |m| m.value.longname.as_ref_opt(),
                )
            })
            .collect()
    }

    /// Set all $PnS keywords to list of names.
    ///
    /// Will return false if length of supplied list does not match length
    /// of measurements; true otherwise. Since $PnS is an optional keyword for
    /// all versions, any name in the list may be None which will blank the
    /// keyword.
    pub fn set_longnames(&mut self, ns: Vec<Option<String>>) -> bool {
        self.measurements
            .alter_values_zip(
                ns,
                |x, n| x.value.longname = n.map(Longname).into(),
                |x, n| x.value.longname = n.map(Longname).into(),
            )
            .is_some()
    }

    /// Show $PnB for each measurement
    ///
    /// Will be None if all $PnB are variable, and a vector of u8 showing the
    /// width of each measurement otherwise.
    // TODO misleading name
    pub fn bytes(&self) -> Option<Vec<u8>> {
        let xs: Vec<_> = self
            .measurements
            .iter()
            .flat_map(|(_, x)| {
                x.map_or_else(|p| p.value.bytes, |p| p.value.bytes)
                    .try_into()
                    .ok()
            })
            .collect();
        // ASSUME internal state is controlled such that no "partially variable"
        // configurations are possible
        if xs.len() != self.par().0 {
            None
        } else {
            Some(xs)
        }
    }

    /// Show $PnB for each measurement
    ///
    /// Will be None if all $PnB are variable, and a vector of u8 showing the
    /// width of each measurement otherwise.
    // TODO misleading name
    pub fn widths(&self) -> Vec<Width> {
        self.measurements
            .iter()
            .map(|(_, x)| x.map_or_else(|p| p.value.bytes, |p| p.value.bytes))
            .collect()
    }

    /// Show $PnR for each measurement
    pub fn ranges(&self) -> Vec<&Range> {
        self.measurements
            .iter()
            .map(|(_, x)| x.map_or_else(|p| &p.value.range, |p| &p.value.range))
            .collect()
    }

    pub fn par(&self) -> Par {
        Par(self.measurements.len())
    }

    // pub(crate) fn iter_width_range_pairs(&self) -> Vec<(Width, &Range)> {
    //     self.measurements
    //         .iter()
    //         .map(|(_, x)| {
    //             x.map_or_else(
    //                 |p| (p.value.bytes, &p.value.range),
    //                 |p| (p.value.bytes, &p.value.range),
    //             )
    //         })
    //         .collect()
    // }

    fn meta_meas_keywords<F, G, H, I>(
        &self,
        f_meas: F,
        f_time_meas: G,
        f_time_meta: H,
        f_meta: I,
    ) -> (RawPairs, RawPairs)
    where
        F: Fn(&Measurement<M::P>, MeasIdx) -> RawPairs,
        G: Fn(&TimeChannel<M::T>, MeasIdx) -> RawPairs,
        H: Fn(&TimeChannel<M::T>) -> RawPairs,
        I: Fn(&Metadata<M>, Par) -> RawPairs,
    {
        let meas: Vec<_> = self
            .measurements
            .iter_non_center_values()
            .flat_map(|(i, m)| f_meas(m, i))
            .collect();
        let (time_meas, time_meta) = self
            .measurements
            .as_center()
            .map_or((vec![], vec![]), |tc| {
                (f_time_meas(tc.value, tc.index), f_time_meta(tc.value))
            });
        let meta: Vec<_> = f_meta(&self.metadata, self.par()).into_iter().collect();
        let all_meas: Vec<_> = meas.into_iter().chain(time_meas).collect();
        let all_meta: Vec<_> = meta.into_iter().chain(time_meta).collect();
        (all_meta, all_meas)
    }

    fn req_meta_meas_keywords(&self) -> (RawPairs, RawPairs, usize) {
        let (meta, mut meas) = self.meta_meas_keywords(
            Measurement::all_req_keywords,
            TimeChannel::req_meas_keywords,
            TimeChannel::req_meta_keywords,
            Metadata::all_req_keywords,
        );
        if M::N::INFALLABLE {
            meas.append(&mut self.shortname_keywords());
        };
        let length = meta
            .iter()
            .chain(meas.iter())
            .map(|(k, v)| k.len() + v.len())
            .sum();
        (meta, meas, length)
    }

    fn opt_meta_meas_keywords(&self) -> (RawPairs, RawPairs, usize) {
        let (meta, mut meas) = self.meta_meas_keywords(
            Measurement::all_opt_keywords,
            TimeChannel::opt_meas_keywords,
            |_| vec![],
            |s, _| Metadata::all_opt_keywords(s),
        );
        if !M::N::INFALLABLE {
            meas.append(&mut self.shortname_keywords());
        };
        // TODO not DRY
        let length = meta
            .iter()
            .chain(meas.iter())
            .map(|(k, v)| k.len() + v.len())
            .sum();
        (meta, meas, length)
    }

    fn shortname_keywords(&self) -> RawPairs {
        // This is sometimes an optional key, but here we use ReqMeasKey
        // instance since we already pre-filter using the wrapper type internal
        // to named vector structure
        self.measurements
            .indexed_names()
            .map(|(i, n)| ReqMeasKey::pair(n, i))
            .collect()
    }

    fn meas_table(&self, delim: &str) -> Vec<String>
    where
        M::T: Clone,
        M::P: From<M::T>,
    {
        let ms = &self.measurements;
        if let Some(m0) = ms.get(0.into()).and_then(|x| x.ok()) {
            let header = m0.table_header();
            let rows = self.measurements.iter().map(|(i, r)| {
                r.map_or_else(
                    |c| Measurement::<M::P>::from(c.value.clone()).table_row(i, Some(&c.key)),
                    |nc| nc.value.table_row(i, M::N::as_opt(&nc.key)),
                )
            });
            [header]
                .into_iter()
                .chain(rows)
                .map(|r| r.join(delim))
                .collect()
        } else {
            vec![]
        }
    }

    // TOOD moveme
    pub(crate) fn print_meas_table(&self, delim: &str)
    where
        M::T: Clone,
        M::P: From<M::T>,
    {
        for e in self.meas_table(delim) {
            println!("{}", e);
        }
    }

    fn lookup_measurements(
        st: &mut KwParser,
        par: Par,
        pat: Option<&TimePattern>,
        prefix: &ShortnamePrefix,
    ) -> Option<Measurements<M::N, M::T, M::P>>
    where
        M: LookupMetadata,
        M::T: LookupTime,
        M::P: LookupMeasurement,
    {
        let ps: Vec<_> = (0..par.0)
            .flat_map(|n| {
                let i = n.into();
                let name_res = M::lookup_shortname(st, i)?;
                let key = M::N::to_res(name_res).and_then(|name| {
                    if let Some(tp) = pat {
                        if tp.0.as_inner().is_match(name.as_ref()) {
                            return Ok(name);
                        }
                    }
                    Err(M::N::into_wrapped(name))
                });
                // TODO this will make cryptic errors if the time pattern
                // happens to match more than one channel
                let res = match key {
                    Ok(name) => {
                        let t = TimeChannel::lookup_time_channel(st, i)?;
                        Err((name, t))
                    }
                    Err(k) => {
                        let m = Measurement::lookup_measurement(st, i)?;
                        Ok((k, m))
                    }
                };
                Some(res)
            })
            .collect();
        if ps.len() == par.0 {
            NamedVec::new(ps, prefix.clone()).map_or_else(
                |e| {
                    st.deferred.push_error(e);
                    None
                },
                Some,
            )
        } else {
            // ASSUME errors were capture elsewhere
            None
        }
    }

    pub(crate) fn new_from_raw(kws: &mut RawKeywords, conf: &StdTextReadConfig) -> PureResult<Self>
    where
        M: LookupMetadata,
        M::T: LookupTime,
        M::P: LookupMeasurement,
    {
        // Lookup $PAR first; everything depends on this since we need to know
        // the number of measurements to find which are used in turn for
        // validating lots of keywords in metadata. If we fail we need to bail.
        //
        // TODO this isn't entirely true; there are some things that can be
        // tested without $PAR, so if we really wanted to be aggressive we could
        // pass $PAR as an Option and only test if not None when we need it and
        // possibly bail. Might be worth it.
        let par = Failure::from_result(Par::remove_meta_req(kws))?;
        // Lookup measurements+metadata based on $PAR, which also might fail in
        // a zillion ways. If this fails we need to bail since we cannot create
        // a struct with missing fields.
        let md_fail = "could not standardize TEXT".to_string();
        let tp = conf.time.pattern.as_ref();
        let md_succ = KwParser::try_run(kws, conf, md_fail, |st| {
            if let Some(ms) = Self::lookup_measurements(st, par, tp, &conf.shortname_prefix) {
                Metadata::lookup_metadata(st, &ms).map(|metadata| CoreTEXT {
                    metadata,
                    measurements: ms,
                })
            } else {
                None
            }
        })?;
        // hooray, we win and can now make the core struct
        Ok(md_succ.and_then(|core| core.validate(&conf.time).map(|_| core)))
    }

    fn measurement_names(&self) -> HashSet<&Shortname> {
        self.measurements.indexed_names().map(|(_, x)| x).collect()
    }

    // TODO add non-kw deprecation checker

    // TODO The goal of this function is to ensure that the internal state of
    // this struct is "fully compliant". This means that anything that fails
    // the below tests needs to either be dropped, altered (if possible), or
    // fail the entire struct entirely. Then each manipulation can be assumed
    // to operate on a clean state.
    fn validate(&self, conf: &TimeConfig) -> PureSuccess<()> {
        // TODO also validate internal data configuration state, since many
        // functions assume that it is "valid"
        let mut deferred = PureErrorBuf::default();

        if let Some(pat) = conf.pattern.as_ref() {
            if conf.ensure && self.measurements.as_center().is_none() {
                let msg = format!("Could not find time channel matching {}", pat);
                deferred.push_error(msg);
            }
        }

        let names = self.measurement_names();

        if let Err(msg) = self.metadata.check_trigger(&names) {
            // TODO toggle this
            deferred.push_error(msg);
        }

        if let Err(msg) = self.metadata.check_unstainedcenters(&names) {
            // TODO toggle this
            deferred.push_error(msg);
        }

        if let Err(msg) = self.metadata.check_spillover(&names) {
            // TODO toggle this
            deferred.push_error(msg);
        }

        PureSuccess { data: (), deferred }
    }

    // TODO this is basically tryfrom
    pub fn try_convert<ToM>(self) -> PureResult<VersionedCoreTEXT<ToM>>
    where
        M::N: Clone,
        ToM: VersionedMetadata,
        ToM: TryFromMetadata<M>,
        ToM::P: VersionedMeasurement,
        ToM::T: VersionedTime,
        ToM::N: MightHave,
        ToM::N: Clone,
        ToM::P: TryFrom<M::P, Error = MeasConvertError>,
        ToM::T: From<M::T>,
        <ToM::N as MightHave>::Wrapper<Shortname>: TryFrom<<M::N as MightHave>::Wrapper<Shortname>>,
    {
        let widths = self.widths();
        let ps = self
            .measurements
            .map_non_center_values(|i, v| v.try_convert(i.into()))
            .map(|x| x.map_center_value(|y| y.value.convert()));
        let convert = SizeConvert {
            widths,
            datatype: self.metadata.datatype,
            size: self.metadata.specific.byteord(),
        };
        let m = self.metadata.try_convert(convert);
        let res = match (m, ps) {
            (Ok(metadata), Ok(old_ps)) => {
                if let Some(measurements) = old_ps.try_rewrapped() {
                    Ok(CoreTEXT {
                        metadata,
                        measurements,
                    })
                } else {
                    Err(vec!["Some $PnN are blank and could not be converted".into()])
                }
            }
            (a, b) => Err(b
                .err()
                .unwrap_or_default()
                .into_iter()
                .chain(a.err().unwrap_or_default())
                .collect()),
        };
        let msg = format!(
            "could not convert from {} to {}",
            M::P::fcs_version(),
            ToM::P::fcs_version()
        );
        PureMaybe::from_result_strs(res, PureErrorLevel::Error).into_result(msg)
    }

    fn set_data_bytes_range(&mut self, xs: Vec<(Width, Range)>) -> bool {
        self.measurements
            .alter_values_zip(
                xs,
                |x, (b, r)| {
                    x.value.bytes = b;
                    x.value.range = r;
                },
                |x, (b, r)| {
                    x.value.bytes = b;
                    x.value.range = r;
                },
            )
            .is_some()
    }

    fn set_data_delimited_inner(&mut self, xs: Vec<u64>) -> bool {
        let ys: Vec<_> = xs
            .into_iter()
            .map(|r| (Width::Variable, r.into()))
            .collect();
        self.set_data_bytes_range(ys)
    }

    fn set_to_floating_point(&mut self, is_double: bool, rs: Vec<Range>) -> bool {
        let (dt, b) = if is_double {
            (AlphaNumType::Double, Width::new_f64())
        } else {
            (AlphaNumType::Single, Width::new_f32())
        };
        let xs: Vec<_> = rs.into_iter().map(|r| (b, r)).collect();
        // ASSUME time channel will always be set to linear since we do that
        // a few lines above, so the only error/warning we need to screen is
        // for the length of the input
        let res = self.set_data_bytes_range(xs);
        if res {
            self.metadata.datatype = dt;
        }
        res
    }

    fn set_data_ascii_inner(&mut self, xs: Vec<AsciiRangeSetter>) -> bool {
        let ys: Vec<_> = xs.into_iter().map(|s| s.truncated()).collect();
        let res = self.set_data_bytes_range(ys);
        if res {
            self.metadata.datatype = AlphaNumType::Ascii;
        }
        res
    }

    pub fn set_data_integer_inner(&mut self, xs: Vec<NumRangeSetter>) -> bool {
        let ys: Vec<_> = xs.into_iter().map(|s| s.truncated()).collect();
        let res = self.set_data_bytes_range(ys);
        if res {
            self.metadata.datatype = AlphaNumType::Integer;
        }
        res
    }

    pub(crate) fn as_column_layout(&self) -> Result<M::L, Vec<String>> {
        M::as_column_layout(&self.metadata, &self.measurements)
    }

    // fn df_names(&self) -> Vec<PlSmallStr> {
    //     self.all_shortnames()
    //         .into_iter()
    //         .map(|s| s.as_ref().into())
    //         .collect()
    // }

    // fn set_df_column_names(&self, df: &mut DataFrame) -> PolarsResult<()> {
    //     df.set_column_names(self.df_names())
    // }

    pub(crate) fn as_data_reader(
        &self,
        kws: &mut RawKeywords,
        _: &DataReadConfig,
        data_seg: Segment,
    ) -> PureMaybe<DataReader> {
        let l = M::as_column_layout(&self.metadata, &self.measurements);
        PureMaybe::from_result_strs(l, PureErrorLevel::Error)
            .and_then_opt(|dl| dl.into_reader(kws, data_seg))
            .map(|x| {
                x.map(|column_reader| DataReader {
                    column_reader,
                    // TODO fix cast
                    begin: data_seg.begin() as u64,
                })
            })
    }
}

macro_rules! timestamp_methods {
    ($timetype:ident) => {
        pub fn timestamps(&self) -> &Timestamps<$timetype> {
            &self.metadata.specific.timestamps
        }

        pub fn timestamps_mut(&mut self) -> &mut Timestamps<$timetype> {
            &mut self.metadata.specific.timestamps
        }
    };
}

impl CoreTEXT2_0 {
    pub fn new(datatype: AlphaNumType, byteord: ByteOrd, mode: Mode) -> Self {
        let specific = InnerMetadata2_0::new(mode, byteord);
        let metadata = Metadata::new(datatype, specific);
        CoreTEXT {
            metadata,
            measurements: NamedVec::default(),
        }
    }

    comp_methods!();
    scale_get_set!(Option<Scale>, Some(Scale::Linear));

    /// Set all non-time $PnN keywords to list of names.
    pub fn set_measurement_shortnames_maybe(
        &mut self,
        ns: Vec<Option<Shortname>>,
    ) -> Result<NameMapping, DistinctKeysError> {
        let ks = ns.into_iter().map(|n| n.into()).collect();
        let mapping = self.measurements.set_non_center_keys(ks)?;
        self.metadata.reassign_all(&mapping);
        Ok(mapping)
    }

    /// Set data layout to be Integer for all measurements
    pub fn set_data_integer(&mut self, rs: Vec<u64>, byteord: ByteOrd) -> bool {
        let n = byteord.nbytes();
        let ys = rs
            .into_iter()
            .map(|r| RangeSetter { width: n, range: r })
            .collect();
        let res = self.set_data_integer_inner(ys);
        if res {
            self.metadata.specific.byteord = byteord;
        }
        res
    }

    /// Set data layout to be 32-bit float for all measurements.
    pub fn set_data_f32(&mut self, rs: Vec<f32>) -> bool {
        let ys: Vec<_> = rs.into_iter().map(|r| r.into()).collect();
        self.set_to_floating_point(false, ys)
    }

    /// Set data layout to be 64-bit float for all measurements.
    pub fn set_data_f64(&mut self, rs: Vec<f64>) -> bool {
        let ys: Vec<_> = rs.into_iter().map(|r| r.into()).collect();
        self.set_to_floating_point(true, ys)
    }

    /// Set data layout to be ASCII-delimited
    pub fn set_data_delimited(&mut self, xs: Vec<u64>) -> bool {
        self.set_data_delimited_inner(xs)
    }

    /// Set data layout to be ASCII-fixed for all measurements
    pub fn set_data_ascii(&mut self, xs: Vec<AsciiRangeSetter>) -> bool {
        self.set_data_ascii_inner(xs)
    }

    timestamp_methods!(FCSTime);

    non_time_get_set!(
        wavelengths,
        set_wavelengths,
        Wavelength,
        [specific],
        wavelength,
        PnL
    );
}

impl CoreTEXT3_0 {
    pub fn new(datatype: AlphaNumType, byteord: ByteOrd, mode: Mode) -> Self {
        let specific = InnerMetadata3_0::new(mode, byteord);
        let metadata = Metadata::new(datatype, specific);
        CoreTEXT {
            metadata,
            measurements: NamedVec::default(),
        }
    }

    comp_methods!();
    scale_get_set!(Scale, Scale::Linear);

    /// Set all non-time $PnN keywords to list of names.
    pub fn set_measurement_shortnames_maybe(
        &mut self,
        ns: Vec<Option<Shortname>>,
    ) -> Result<NameMapping, DistinctKeysError> {
        let ks = ns.into_iter().map(|n| n.into()).collect();
        let mapping = self.measurements.set_non_center_keys(ks)?;
        self.metadata.reassign_all(&mapping);
        Ok(mapping)
    }

    /// Set data layout to be Integer for all measurements
    // TODO not DRY
    pub fn set_data_integer(&mut self, rs: Vec<u64>, byteord: ByteOrd) -> bool {
        let n = byteord.nbytes();
        let ys = rs
            .into_iter()
            .map(|r| RangeSetter { width: n, range: r })
            .collect();
        let res = self.set_data_integer_inner(ys);
        if res {
            self.metadata.specific.byteord = byteord;
        }
        res
    }

    /// Set data layout to be 32-bit float for all measurements.
    pub fn set_data_f32(&mut self, rs: Vec<f32>) -> bool {
        let ys: Vec<_> = rs.into_iter().map(|r| r.into()).collect();
        self.set_to_floating_point(false, ys)
    }

    /// Set data layout to be 64-bit float for all measurements.
    pub fn set_data_f64(&mut self, rs: Vec<f64>) -> bool {
        let ys: Vec<_> = rs.into_iter().map(|r| r.into()).collect();
        self.set_to_floating_point(true, ys)
    }

    /// Set data layout to be ASCII-delimited
    pub fn set_data_delimited(&mut self, xs: Vec<u64>) -> bool {
        self.set_data_delimited_inner(xs)
    }

    /// Set data layout to be ASCII-fixed for all measurements
    pub fn set_data_ascii(&mut self, xs: Vec<AsciiRangeSetter>) -> bool {
        self.set_data_ascii_inner(xs)
    }

    timestamp_methods!(FCSTime60);

    non_time_get_set!(gains, set_gains, Gain, [specific], gain, PnG);

    non_time_get_set!(
        wavelengths,
        set_wavelengths,
        Wavelength,
        [specific],
        wavelength,
        PnL
    );
}

// TODO this needs to be safer
// macro_rules! spillover_methods {
//     ($($root:ident),*) => {
//         /// Show $SPILLOVER
//         pub fn spillover(&self) -> Option<&Spillover> {
//             self.$($root.)*metadata.specific.spillover.as_ref_opt()
//         }

//         /// Set names and matrix for $SPILLOVER
//         pub fn set_spillover(
//             &mut self,
//             ns: Vec<Shortname>,
//             m: DMatrix<f32>,
//         ) -> Result<(), SpilloverError> {
//             self.$($root.)*metadata.specific.spillover = Some(Spillover::new(ns, m)?).into();
//             Ok(())
//         }

//         /// Clear $SPILLOVER
//         pub fn unset_spillover(&mut self) {
//             self.$($root.)*metadata.specific.spillover = None.into();
//         }
//     };
// }

macro_rules! display_methods {
    () => {
        pub fn displays(&self) -> Vec<Option<&Display>> {
            self.measurements
                .iter()
                .map(|x| {
                    x.1.map_or_else(
                        |t| t.value.specific.display.as_ref_opt(),
                        |m| m.value.specific.display.as_ref_opt(),
                    )
                })
                .collect()
        }

        pub fn set_displays(&mut self, ns: Vec<Option<Display>>) -> bool {
            self.measurements
                .alter_values_zip(
                    ns,
                    |x, n| x.value.specific.display = n.into(),
                    |x, n| x.value.specific.display = n.into(),
                )
                .is_some()
        }
    };
}

impl CoreTEXT3_1 {
    pub fn new(datatype: AlphaNumType, is_big: bool, mode: Mode) -> Self {
        let specific = InnerMetadata3_1::new(mode, is_big);
        let metadata = Metadata::new(datatype, specific);
        CoreTEXT {
            metadata,
            measurements: NamedVec::default(),
        }
    }

    scale_get_set!(Scale, Scale::Linear);
    // spillover_methods!();

    // TODO better input type here?
    /// Set data layout to be integers for all measurements.
    pub fn set_data_integer(&mut self, xs: Vec<NumRangeSetter>) -> bool {
        self.set_data_integer_inner(xs)
    }

    /// Set data layout to be 32-bit float for all measurements.
    pub fn set_data_f32(&mut self, rs: Vec<f32>) -> bool {
        let ys: Vec<_> = rs.into_iter().map(|r| r.into()).collect();
        self.set_to_floating_point(false, ys)
    }

    /// Set data layout to be 64-bit float for all measurements.
    pub fn set_data_f64(&mut self, rs: Vec<f64>) -> bool {
        let ys: Vec<_> = rs.into_iter().map(|r| r.into()).collect();
        self.set_to_floating_point(true, ys)
    }

    /// Set data layout to be fixed-ASCII for all measurements
    pub fn set_data_ascii(&mut self, xs: Vec<AsciiRangeSetter>) -> bool {
        self.set_data_ascii_inner(xs)
    }

    /// Set data layout to be ASCII-delimited
    pub fn set_data_delimited(&mut self, xs: Vec<u64>) -> bool {
        self.set_data_delimited_inner(xs)
    }

    pub fn get_big_endian(&self) -> bool {
        self.metadata.specific.byteord == Endian::Big
    }

    pub fn set_big_endian(&mut self, is_big: bool) {
        self.metadata.specific.byteord = Endian::is_big(is_big);
    }

    timestamp_methods!(FCSTime100);

    display_methods!();

    non_time_get_set!(gains, set_gains, Gain, [specific], gain, PnG);

    non_time_get_set!(
        calibrations,
        set_calibrations,
        Calibration3_1,
        [specific],
        calibration,
        PnCALIBRATION
    );

    non_time_get_set!(
        wavelengths,
        set_wavelengths,
        Wavelengths,
        [specific],
        wavelengths,
        PnL
    );
}

impl CoreTEXT3_2 {
    pub fn new(datatype: AlphaNumType, is_big: bool, cyt: String) -> Self {
        let specific = InnerMetadata3_2::new(is_big, cyt);
        let metadata = Metadata::new(datatype, specific);
        CoreTEXT {
            metadata,
            measurements: NamedVec::default(),
        }
    }

    /// Show $UNSTAINEDCENTERS
    pub fn unstained_centers(&self) -> Option<&UnstainedCenters> {
        self.metadata
            .specific
            .unstained
            .unstainedcenters
            .as_ref_opt()
    }

    /// Insert an unstained center
    pub fn insert_unstained_center(&mut self, k: Shortname, v: f32) -> Option<f32> {
        if !self.measurement_names().contains(&k) {
            // TODO this is ambiguous, user has no idea why their value was
            // rejected
            return None;
        }
        let us = &mut self.metadata.specific.unstained;
        if let Some(u) = us.unstainedcenters.0.as_mut() {
            u.insert(k, v)
        } else {
            us.unstainedcenters = Some(UnstainedCenters::new_1(k, v)).into();
            None
        }
    }

    /// Remove an unstained center
    pub fn remove_unstained_center(&mut self, k: &Shortname) -> Option<f32> {
        let us = &mut self.metadata.specific.unstained;
        if let Some(u) = us.unstainedcenters.0.as_mut() {
            match u.remove(k) {
                Ok(ret) => ret,
                Err(_) => {
                    us.unstainedcenters = None.into();
                    None
                }
            }
        } else {
            None
        }
    }

    /// Remove all unstained center
    pub fn clear_unstained_centers(&mut self) {
        self.metadata.specific.unstained.unstainedcenters = None.into()
    }

    scale_get_set!(Scale, Scale::Linear);
    // spillover_methods!();

    /// Show datatype for all measurements
    ///
    /// This will be $PnDATATYPE if given and $DATATYPE otherwise at each
    /// measurement index
    pub fn datatypes(&self) -> Vec<AlphaNumType> {
        let dt = self.metadata.datatype;
        self.measurements
            .iter()
            .map(|(_, x)| {
                x.map_or_else(
                    |p| p.value.specific.datatype.as_ref_opt(),
                    |p| p.value.specific.datatype.as_ref_opt(),
                )
                .map(|t| (*t).into())
                .unwrap_or(dt)
            })
            .collect()
    }

    /// Set data layout to be a mix of datatypes
    pub fn set_data_mixed(&mut self, xs: Vec<MixedColumnSetter>) -> bool {
        // Figure out what $DATATYPE (the default) should be; count frequencies
        // of each type, and if ASCII is given at all, this must be $DATATYPE
        // since it can't be set to $PnDATATYPE; otherwise, use whatever is
        // most frequent.
        let dt_opt = xs
            .iter()
            .map(|y| AlphaNumType::from(*y))
            .sorted()
            .chunk_by(|x| *x)
            .into_iter()
            .map(|(key, gs)| (key, gs.count()))
            .sorted_by_key(|(_, count)| *count)
            .rev()
            .find_or_first(|(key, _)| *key == AlphaNumType::Ascii)
            .map(|(key, _)| key);
        if let Some(dt) = dt_opt {
            let go = |x: MixedColumnSetter| {
                let this_dt = x.into();
                let pndt = if dt == this_dt {
                    None
                } else {
                    // ASSUME this will never fail since we set the default type
                    // to ASCII if any ASCII are found in the input
                    Some(this_dt.try_into().unwrap())
                };
                match x {
                    MixedColumnSetter::Float(range) => (Width::new_f32(), range.into(), pndt),
                    MixedColumnSetter::Double(range) => (Width::new_f64(), range.into(), pndt),
                    MixedColumnSetter::Ascii(s) => {
                        let (b, r) = s.truncated();
                        (b, r, None)
                    }
                    MixedColumnSetter::Uint(s) => {
                        let (b, r) = s.truncated();
                        (b, r, pndt)
                    }
                }
            };
            let res = self
                .measurements
                .alter_values_zip(
                    xs,
                    |x, y| {
                        let (b, r, pndt) = go(y);
                        let m = x.value;
                        m.bytes = b;
                        m.range = r;
                        m.specific.datatype = pndt.into();
                    },
                    |x, y| {
                        let (b, r, pndt) = go(y);
                        let t = x.value;
                        t.bytes = b;
                        t.range = r;
                        t.specific.datatype = pndt.into();
                    },
                )
                .is_some();
            if res {
                self.metadata.datatype = dt;
            }
            return res;
        }
        // this will only happen if the input is empty
        false
    }

    /// Set data layout to be integer for all measurements
    pub fn set_data_integer(&mut self, xs: Vec<NumRangeSetter>) -> bool {
        let res = self.set_data_integer_inner(xs);
        if res {
            self.unset_meas_datatypes();
        }
        res
    }

    /// Set data layout to be 32-bit float for all measurements.
    pub fn set_data_f32(&mut self, rs: Vec<f32>) -> bool {
        let ys: Vec<_> = rs.into_iter().map(|r| r.into()).collect();
        self.set_to_floating_point_3_2(false, ys)
    }

    /// Set data layout to be 64-bit float for all measurements.
    pub fn set_data_f64(&mut self, rs: Vec<f64>) -> bool {
        let ys: Vec<_> = rs.into_iter().map(|r| r.into()).collect();
        self.set_to_floating_point_3_2(true, ys)
    }

    /// Set data layout to be fixed-ASCII for all measurements
    pub fn set_data_ascii(&mut self, xs: Vec<AsciiRangeSetter>) -> bool {
        let res = self.set_data_ascii_inner(xs);
        if res {
            self.unset_meas_datatypes();
        }
        res
    }

    /// Set data layout to be ASCII-delimited for all measurements
    pub fn set_data_delimited(&mut self, xs: Vec<u64>) -> bool {
        let res = self.set_data_delimited_inner(xs);
        if res {
            self.unset_meas_datatypes();
        }
        res
    }

    pub fn get_big_endian(&self) -> bool {
        self.metadata.specific.byteord == Endian::Big
    }

    pub fn set_big_endian(&mut self, is_big: bool) {
        self.metadata.specific.byteord = Endian::is_big(is_big);
    }

    timestamp_methods!(FCSTime100);

    display_methods!();

    non_time_get_set!(gains, set_gains, Gain, [specific], gain, PnG);

    non_time_get_set!(
        wavelengths,
        set_wavelengths,
        Wavelengths,
        [specific],
        wavelengths,
        PnL
    );

    non_time_get_set!(
        det_names,
        set_det_names,
        DetectorName,
        [specific],
        detector_name,
        PnDET
    );

    non_time_get_set!(
        calibrations,
        set_calibrations,
        Calibration3_2,
        [specific],
        calibration,
        PnCALIBRATION
    );

    non_time_get_set!(tags, set_tags, Tag, [specific], tag, PnTAG);

    non_time_get_set!(
        measurement_types,
        set_measurement_types,
        MeasurementType,
        [specific],
        measurement_type,
        PnTYPE
    );

    non_time_get_set!(
        features,
        set_features,
        Feature,
        [specific],
        feature,
        PnFEATURE
    );

    non_time_get_set!(
        analytes,
        set_analytes,
        Analyte,
        [specific],
        analyte,
        PnANALYTE
    );

    fn unset_meas_datatypes(&mut self) {
        self.measurements.alter_values(
            |x| {
                x.value.specific.datatype = None.into();
            },
            |x| {
                x.value.specific.datatype = None.into();
            },
        );
    }

    // TODO check that floating point types are linear
    fn set_to_floating_point_3_2(&mut self, is_double: bool, rs: Vec<Range>) -> bool {
        let res = self.set_to_floating_point(is_double, rs);
        if res {
            self.unset_meas_datatypes();
        }
        res
    }
}

#[derive(Clone, Serialize)]
pub struct OptionalKwFamily;

#[derive(Clone, Serialize)]
pub struct Identity<T>(pub T);

impl MightHave for OptionalKwFamily {
    type Wrapper<T> = OptionalKw<T>;
    const INFALLABLE: bool = false;

    fn to_res<T>(x: Self::Wrapper<T>) -> Result<T, Self::Wrapper<T>> {
        x.0.ok_or(None.into())
    }

    fn as_ref<T>(x: &Self::Wrapper<T>) -> Self::Wrapper<&T> {
        x.as_ref()
    }
}

#[derive(Clone, Serialize)]
pub struct IdentityFamily;

impl MightHave for IdentityFamily {
    type Wrapper<T> = Identity<T>;
    const INFALLABLE: bool = true;

    fn to_res<T>(x: Self::Wrapper<T>) -> Result<T, Self::Wrapper<T>> {
        Ok(x.0)
    }

    fn as_ref<T>(x: &Self::Wrapper<T>) -> Self::Wrapper<&T> {
        Identity(&x.0)
    }
}

impl<T> From<T> for Identity<T> {
    fn from(value: T) -> Self {
        Identity(value)
    }
}

impl<T> From<T> for OptionalKw<T> {
    fn from(value: T) -> Self {
        Some(value).into()
    }
}

impl<T> TryFrom<OptionalKw<T>> for Identity<T> {
    type Error = ();
    fn try_from(value: OptionalKw<T>) -> Result<Self, ()> {
        value.0.ok_or(()).map(Identity)
    }
}

// This will never really fail but is implemented for symmetry with its inverse
impl<T> TryFrom<Identity<T>> for OptionalKw<T> {
    type Error = ();
    fn try_from(value: Identity<T>) -> Result<Self, ()> {
        Ok(Some(value.0).into())
    }
}

impl From<FCSTime> for FCSTime60 {
    fn from(value: FCSTime) -> Self {
        FCSTime60(value.0)
    }
}

impl From<FCSTime> for FCSTime100 {
    fn from(value: FCSTime) -> Self {
        FCSTime100(value.0)
    }
}

impl From<FCSTime60> for FCSTime {
    fn from(value: FCSTime60) -> Self {
        // ASSUME this will never fail, we are just removing nanoseconds
        FCSTime(value.0.with_nanosecond(0).unwrap())
    }
}

impl From<FCSTime100> for FCSTime {
    fn from(value: FCSTime100) -> Self {
        // ASSUME this will never fail, we are just removing nanoseconds
        FCSTime(value.0.with_nanosecond(0).unwrap())
    }
}

impl From<FCSTime60> for FCSTime100 {
    fn from(value: FCSTime60) -> Self {
        FCSTime100(value.0)
    }
}

impl From<FCSTime100> for FCSTime60 {
    fn from(value: FCSTime100) -> Self {
        FCSTime60(value.0)
    }
}

impl TryFrom<ByteOrd> for Endian {
    type Error = FromByteOrdError;

    fn try_from(value: ByteOrd) -> Result<Self, Self::Error> {
        value.as_endian().ok_or(FromByteOrdError)
    }
}

impl From<Wavelength> for Wavelengths {
    fn from(value: Wavelength) -> Self {
        Wavelengths(NonEmpty {
            head: value.0,
            tail: vec![],
        })
    }
}

impl From<OptionalKw<Wavelengths>> for OptionalKw<Wavelength> {
    fn from(value: OptionalKw<Wavelengths>) -> Self {
        value.0.map(|x| (*x.0.first()).into()).into()
    }
}

impl From<Calibration3_1> for Calibration3_2 {
    fn from(value: Calibration3_1) -> Self {
        Calibration3_2 {
            unit: value.unit,
            offset: 0.0,
            value: value.value,
        }
    }
}

impl From<Calibration3_2> for Calibration3_1 {
    fn from(value: Calibration3_2) -> Self {
        Calibration3_1 {
            unit: value.unit,
            value: value.value,
        }
    }
}

impl TryFrom<InnerMeasurement3_0> for InnerMeasurement2_0 {
    type Error = MeasConvertError;

    fn try_from(value: InnerMeasurement3_0) -> Result<Self, Self::Error> {
        Ok(InnerMeasurement2_0 {
            scale: Some(value.scale).into(),
            wavelength: value.wavelength,
        })
    }
}

impl TryFrom<InnerMeasurement3_1> for InnerMeasurement2_0 {
    type Error = MeasConvertError;

    fn try_from(value: InnerMeasurement3_1) -> Result<Self, Self::Error> {
        Ok(InnerMeasurement2_0 {
            scale: Some(value.scale).into(),
            wavelength: value.wavelengths.into(),
        })
    }
}

impl TryFrom<InnerMeasurement3_2> for InnerMeasurement2_0 {
    type Error = MeasConvertError;

    fn try_from(value: InnerMeasurement3_2) -> Result<Self, Self::Error> {
        Ok(InnerMeasurement2_0 {
            scale: Some(value.scale).into(),
            wavelength: value.wavelengths.into(),
        })
    }
}

impl TryFrom<InnerMeasurement2_0> for InnerMeasurement3_0 {
    type Error = MeasConvertError;

    fn try_from(value: InnerMeasurement2_0) -> Result<Self, Self::Error> {
        value
            .scale
            .0
            .ok_or(MeasConvertError::NoScale)
            .map(|scale| InnerMeasurement3_0 {
                scale,
                wavelength: value.wavelength,
                gain: None.into(),
            })
    }
}

impl TryFrom<InnerMeasurement3_1> for InnerMeasurement3_0 {
    type Error = MeasConvertError;

    fn try_from(value: InnerMeasurement3_1) -> Result<Self, Self::Error> {
        Ok(InnerMeasurement3_0 {
            scale: value.scale,
            gain: value.gain,
            wavelength: value.wavelengths.into(),
        })
    }
}

impl TryFrom<InnerMeasurement3_2> for InnerMeasurement3_0 {
    type Error = MeasConvertError;

    fn try_from(value: InnerMeasurement3_2) -> Result<Self, Self::Error> {
        Ok(InnerMeasurement3_0 {
            scale: value.scale,
            gain: value.gain,
            wavelength: value.wavelengths.into(),
        })
    }
}

impl TryFrom<InnerMeasurement2_0> for InnerMeasurement3_1 {
    type Error = MeasConvertError;

    fn try_from(value: InnerMeasurement2_0) -> Result<Self, Self::Error> {
        value
            .scale
            .0
            .ok_or(MeasConvertError::NoScale)
            .map(|scale| InnerMeasurement3_1 {
                scale,
                wavelengths: value.wavelength.map(|x| x.into()),
                gain: None.into(),
                calibration: None.into(),
                display: None.into(),
            })
    }
}

impl TryFrom<InnerMeasurement3_0> for InnerMeasurement3_1 {
    type Error = MeasConvertError;

    fn try_from(value: InnerMeasurement3_0) -> Result<Self, Self::Error> {
        Ok(InnerMeasurement3_1 {
            scale: value.scale,
            gain: value.gain,
            wavelengths: None.into(),
            calibration: None.into(),
            display: None.into(),
        })
    }
}

impl TryFrom<InnerMeasurement3_2> for InnerMeasurement3_1 {
    type Error = MeasConvertError;

    fn try_from(value: InnerMeasurement3_2) -> Result<Self, Self::Error> {
        Ok(InnerMeasurement3_1 {
            scale: value.scale,
            gain: value.gain,
            wavelengths: value.wavelengths,
            calibration: value.calibration.map(|x| x.into()),
            display: value.display,
        })
    }
}

impl TryFrom<InnerMeasurement2_0> for InnerMeasurement3_2 {
    type Error = MeasConvertError;

    fn try_from(value: InnerMeasurement2_0) -> Result<Self, Self::Error> {
        value
            .scale
            .0
            .ok_or(MeasConvertError::NoScale)
            .map(|scale| InnerMeasurement3_2 {
                scale,
                wavelengths: None.into(),
                gain: None.into(),
                calibration: None.into(),
                display: None.into(),
                analyte: None.into(),
                feature: None.into(),
                tag: None.into(),
                detector_name: None.into(),
                datatype: None.into(),
                measurement_type: None.into(),
            })
    }
}

impl TryFrom<InnerMeasurement3_0> for InnerMeasurement3_2 {
    type Error = MeasConvertError;

    fn try_from(value: InnerMeasurement3_0) -> Result<Self, Self::Error> {
        Ok(InnerMeasurement3_2 {
            scale: value.scale,
            wavelengths: value.wavelength.map(|x| x.into()),
            gain: value.gain,
            calibration: None.into(),
            display: None.into(),
            analyte: None.into(),
            feature: None.into(),
            tag: None.into(),
            detector_name: None.into(),
            datatype: None.into(),
            measurement_type: None.into(),
        })
    }
}

impl TryFrom<InnerMeasurement3_1> for InnerMeasurement3_2 {
    type Error = MeasConvertError;

    fn try_from(value: InnerMeasurement3_1) -> Result<Self, Self::Error> {
        Ok(InnerMeasurement3_2 {
            scale: value.scale,
            wavelengths: value.wavelengths,
            gain: value.gain,
            calibration: value.calibration.map(|x| x.into()),
            display: value.display,
            analyte: None.into(),
            feature: None.into(),
            tag: None.into(),
            detector_name: None.into(),
            datatype: None.into(),
            measurement_type: None.into(),
        })
    }
}

type MetaConvertResult<M> = Result<M, MetaConvertErrors>;

pub struct SizeConvert<O> {
    size: O,
    datatype: AlphaNumType,
    widths: Vec<Width>,
}

type ByteOrdConvert = SizeConvert<ByteOrd>;
type EndianConvert = SizeConvert<Endian>;

impl EndianConvert {
    fn try_as_byteord(&self) -> Option<ByteOrd> {
        Width::matrix_bytes(&self.widths, self.datatype).map(|bytes| self.size.as_bytord(bytes))
    }
}

impl TryFromMetadata<InnerMetadata3_0> for InnerMetadata2_0 {
    fn try_from_meta(value: InnerMetadata3_0, _: ByteOrdConvert) -> MetaConvertResult<Self> {
        Ok(InnerMetadata2_0 {
            mode: value.mode,
            byteord: value.byteord,
            cyt: value.cyt,
            comp: value.comp,
            timestamps: value.timestamps.map(|d| d.into()),
        })
    }
}

impl TryFromMetadata<InnerMetadata3_1> for InnerMetadata2_0 {
    fn try_from_meta(value: InnerMetadata3_1, endian: EndianConvert) -> MetaConvertResult<Self> {
        let byteord = endian
            .try_as_byteord()
            .ok_or(vec![MetaConvertError::FromEndian])?;
        Ok(InnerMetadata2_0 {
            mode: value.mode,
            byteord,
            cyt: value.cyt,
            comp: None.into(),
            timestamps: value.timestamps.map(|d| d.into()),
        })
    }
}

impl TryFromMetadata<InnerMetadata3_2> for InnerMetadata2_0 {
    fn try_from_meta(value: InnerMetadata3_2, endian: EndianConvert) -> MetaConvertResult<Self> {
        let byteord = endian
            .try_as_byteord()
            .ok_or(vec![MetaConvertError::FromEndian])?;
        Ok(InnerMetadata2_0 {
            mode: Mode::List,
            byteord,
            cyt: Some(value.cyt).into(),
            comp: None.into(),
            timestamps: value.timestamps.map(|d| d.into()),
        })
    }
}

impl TryFromMetadata<InnerMetadata2_0> for InnerMetadata3_0 {
    fn try_from_meta(value: InnerMetadata2_0, _: ByteOrdConvert) -> MetaConvertResult<Self> {
        Ok(InnerMetadata3_0 {
            mode: value.mode,
            byteord: value.byteord,
            cyt: value.cyt,
            comp: value.comp,
            timestamps: value.timestamps.map(|d| d.into()),
            cytsn: None.into(),
            unicode: None.into(),
        })
    }
}

impl TryFromMetadata<InnerMetadata3_1> for InnerMetadata3_0 {
    fn try_from_meta(value: InnerMetadata3_1, endian: EndianConvert) -> MetaConvertResult<Self> {
        let byteord = endian
            .try_as_byteord()
            .ok_or(vec![MetaConvertError::FromEndian])?;
        Ok(InnerMetadata3_0 {
            mode: value.mode,
            byteord,
            cyt: value.cyt,
            cytsn: value.cytsn,
            timestamps: value.timestamps.map(|d| d.into()),
            comp: None.into(),
            unicode: None.into(),
        })
    }
}

impl TryFromMetadata<InnerMetadata3_2> for InnerMetadata3_0 {
    fn try_from_meta(value: InnerMetadata3_2, endian: EndianConvert) -> MetaConvertResult<Self> {
        let byteord = endian
            .try_as_byteord()
            .ok_or(vec![MetaConvertError::FromEndian])?;
        Ok(InnerMetadata3_0 {
            mode: Mode::List,
            byteord,
            cyt: Some(value.cyt).into(),
            cytsn: value.cytsn,
            timestamps: value.timestamps.map(|d| d.into()),
            comp: None.into(),
            unicode: None.into(),
        })
    }
}

impl TryFromMetadata<InnerMetadata2_0> for InnerMetadata3_1 {
    fn try_from_meta(value: InnerMetadata2_0, _: ByteOrdConvert) -> MetaConvertResult<Self> {
        value
            .byteord
            .try_into()
            .map_err(|e| vec![MetaConvertError::FromByteOrd(e)])
            .map(|byteord| InnerMetadata3_1 {
                mode: value.mode,
                byteord,
                cyt: value.cyt,
                timestamps: value.timestamps.map(|d| d.into()),
                cytsn: None.into(),
                spillover: None.into(),
                modification: ModificationData::default(),
                plate: PlateData::default(),
                vol: None.into(),
            })
    }
}

impl TryFromMetadata<InnerMetadata3_0> for InnerMetadata3_1 {
    fn try_from_meta(value: InnerMetadata3_0, _: ByteOrdConvert) -> MetaConvertResult<Self> {
        value
            .byteord
            .try_into()
            .map_err(|e| vec![MetaConvertError::FromByteOrd(e)])
            .map(|byteord| InnerMetadata3_1 {
                byteord,
                mode: value.mode,
                cyt: value.cyt,
                cytsn: value.cytsn,
                timestamps: value.timestamps.map(|d| d.into()),
                spillover: None.into(),
                modification: ModificationData::default(),
                plate: PlateData::default(),
                vol: None.into(),
            })
    }
}

impl TryFromMetadata<InnerMetadata3_2> for InnerMetadata3_1 {
    fn try_from_meta(value: InnerMetadata3_2, _: EndianConvert) -> MetaConvertResult<Self> {
        Ok(InnerMetadata3_1 {
            mode: Mode::List,
            byteord: value.byteord,
            cyt: Some(value.cyt).into(),
            cytsn: value.cytsn,
            timestamps: value.timestamps,
            spillover: value.spillover,
            plate: value.plate,
            modification: value.modification,
            vol: value.vol,
        })
    }
}

impl TryFromMetadata<InnerMetadata2_0> for InnerMetadata3_2 {
    fn try_from_meta(value: InnerMetadata2_0, _: ByteOrdConvert) -> MetaConvertResult<Self> {
        // TODO what happens if $MODE is not list?
        let bord = value
            .byteord
            .try_into()
            .map_err(MetaConvertError::FromByteOrd);
        let c = value.cyt.0.ok_or(MetaConvertError::NoCyt);
        match (bord, c) {
            (Ok(byteord), Ok(cyt)) => Ok(InnerMetadata3_2 {
                byteord,
                cyt,
                timestamps: value.timestamps.map(|d| d.into()),
                cytsn: None.into(),
                modification: ModificationData::default(),
                spillover: None.into(),
                plate: PlateData::default(),
                vol: None.into(),
                flowrate: None.into(),
                carrier: CarrierData::default(),
                unstained: UnstainedData::default(),
                datetimes: Datetimes::default(),
            }),
            (a, b) => Err([a.err(), b.err()].into_iter().flatten().collect()),
        }
    }
}

impl TryFromMetadata<InnerMetadata3_0> for InnerMetadata3_2 {
    fn try_from_meta(value: InnerMetadata3_0, _: ByteOrdConvert) -> MetaConvertResult<Self> {
        let bord = value
            .byteord
            .try_into()
            .map_err(MetaConvertError::FromByteOrd);
        let c = value.cyt.0.ok_or(MetaConvertError::NoCyt);
        match (bord, c) {
            (Ok(byteord), Ok(cyt)) => Ok({
                InnerMetadata3_2 {
                    byteord,
                    cyt,
                    cytsn: value.cytsn,
                    timestamps: value.timestamps.map(|d| d.into()),
                    modification: ModificationData::default(),
                    spillover: None.into(),
                    plate: PlateData::default(),
                    vol: None.into(),
                    flowrate: None.into(),
                    carrier: CarrierData::default(),
                    unstained: UnstainedData::default(),
                    datetimes: Datetimes::default(),
                }
            }),
            (a, b) => Err([a.err(), b.err()].into_iter().flatten().collect()),
        }
    }
}

impl TryFromMetadata<InnerMetadata3_1> for InnerMetadata3_2 {
    fn try_from_meta(value: InnerMetadata3_1, _: EndianConvert) -> MetaConvertResult<Self> {
        value
            .cyt
            .0
            .ok_or(vec![MetaConvertError::NoCyt])
            .map(|cyt| InnerMetadata3_2 {
                byteord: value.byteord,
                cyt,
                cytsn: value.cytsn,
                timestamps: value.timestamps,
                spillover: value.spillover,
                modification: value.modification,
                plate: value.plate,
                vol: value.vol,
                flowrate: None.into(),
                carrier: CarrierData::default(),
                unstained: UnstainedData::default(),
                datetimes: Datetimes::default(),
            })
    }
}

impl From<InnerTime3_0> for InnerTime2_0 {
    fn from(_: InnerTime3_0) -> Self {
        Self
    }
}

impl From<InnerTime3_1> for InnerTime2_0 {
    fn from(_: InnerTime3_1) -> Self {
        Self
    }
}

impl From<InnerTime3_2> for InnerTime2_0 {
    fn from(_: InnerTime3_2) -> Self {
        Self
    }
}

impl From<InnerTime2_0> for InnerTime3_0 {
    fn from(_: InnerTime2_0) -> Self {
        Self {
            timestep: Timestep::default(),
        }
    }
}

impl From<InnerTime3_1> for InnerTime3_0 {
    fn from(value: InnerTime3_1) -> Self {
        Self {
            timestep: value.timestep,
        }
    }
}

impl From<InnerTime3_2> for InnerTime3_0 {
    fn from(value: InnerTime3_2) -> Self {
        Self {
            timestep: value.timestep,
        }
    }
}

impl From<InnerTime2_0> for InnerTime3_1 {
    fn from(_: InnerTime2_0) -> Self {
        Self {
            timestep: Timestep::default(),
            display: None.into(),
        }
    }
}

impl From<InnerTime3_0> for InnerTime3_1 {
    fn from(value: InnerTime3_0) -> Self {
        Self {
            timestep: value.timestep,
            display: None.into(),
        }
    }
}

impl From<InnerTime3_2> for InnerTime3_1 {
    fn from(value: InnerTime3_2) -> Self {
        Self {
            timestep: value.timestep,
            display: value.display,
        }
    }
}

impl From<InnerTime2_0> for InnerTime3_2 {
    fn from(_: InnerTime2_0) -> Self {
        Self {
            timestep: Timestep::default(),
            display: None.into(),
            datatype: None.into(),
        }
    }
}

impl From<InnerTime3_0> for InnerTime3_2 {
    fn from(value: InnerTime3_0) -> Self {
        Self {
            timestep: value.timestep,
            display: None.into(),
            datatype: None.into(),
        }
    }
}

impl From<InnerTime3_1> for InnerTime3_2 {
    fn from(value: InnerTime3_1) -> Self {
        Self {
            timestep: value.timestep,
            display: value.display,
            datatype: None.into(),
        }
    }
}

impl From<InnerTime2_0> for InnerMeasurement2_0 {
    fn from(_: InnerTime2_0) -> Self {
        Self {
            scale: Some(Scale::Linear).into(),
            wavelength: None.into(),
        }
    }
}

impl From<InnerTime3_0> for InnerMeasurement3_0 {
    fn from(_: InnerTime3_0) -> Self {
        Self {
            scale: Scale::Linear,
            wavelength: None.into(),
            gain: None.into(),
        }
    }
}

impl From<InnerTime3_1> for InnerMeasurement3_1 {
    fn from(value: InnerTime3_1) -> Self {
        Self {
            scale: Scale::Linear,
            display: value.display,
            wavelengths: None.into(),
            gain: None.into(),
            calibration: None.into(),
        }
    }
}

impl From<InnerTime3_2> for InnerMeasurement3_2 {
    fn from(value: InnerTime3_2) -> Self {
        Self {
            scale: Scale::Linear,
            display: value.display,
            datatype: value.datatype,
            wavelengths: None.into(),
            gain: None.into(),
            calibration: None.into(),
            analyte: None.into(),
            measurement_type: None.into(),
            tag: None.into(),
            detector_name: None.into(),
            feature: None.into(),
        }
    }
}

impl TryFrom<InnerMeasurement2_0> for InnerTime2_0 {
    type Error = TryFromTimeError<InnerMeasurement2_0>;
    fn try_from(value: InnerMeasurement2_0) -> Result<Self, Self::Error> {
        if value.scale.0.as_ref().is_some_and(|s| *s == Scale::Linear) {
            Ok(Self)
        } else {
            Err(TryFromTimeError {
                error: NonEmpty {
                    head: MeasToTimeError::NonLinear,
                    tail: vec![],
                },
                value,
            })
        }
    }
}

impl TryFrom<InnerMeasurement3_0> for InnerTime3_0 {
    type Error = TryFromTimeError<InnerMeasurement3_0>;
    fn try_from(value: InnerMeasurement3_0) -> Result<Self, Self::Error> {
        let mut es = vec![];
        if value.scale != Scale::Linear {
            es.push(MeasToTimeError::NonLinear);
        }
        if value.gain.0.is_some() {
            es.push(MeasToTimeError::HasGain);
        }
        NonEmpty::from_vec(es).map_or(
            Ok(Self {
                timestep: Timestep::default(),
            }),
            |error| Err(TryFromTimeError { error, value }),
        )
    }
}

impl TryFrom<InnerMeasurement3_1> for InnerTime3_1 {
    type Error = TryFromTimeError<InnerMeasurement3_1>;
    fn try_from(value: InnerMeasurement3_1) -> Result<Self, Self::Error> {
        let mut es = vec![];
        if value.scale != Scale::Linear {
            es.push(MeasToTimeError::NonLinear);
        }
        if value.gain.0.is_some() {
            es.push(MeasToTimeError::HasGain);
        }
        match NonEmpty::from_vec(es) {
            None => Ok(Self {
                timestep: Timestep::default(),
                display: value.display,
            }),
            Some(error) => Err(TryFromTimeError { error, value }),
        }
    }
}

impl TryFrom<InnerMeasurement3_2> for InnerTime3_2 {
    type Error = TryFromTimeError<InnerMeasurement3_2>;
    fn try_from(value: InnerMeasurement3_2) -> Result<Self, Self::Error> {
        let mut es = vec![];
        if value.scale != Scale::Linear {
            es.push(MeasToTimeError::NonLinear);
        }
        if value.gain.0.is_some() {
            es.push(MeasToTimeError::HasGain);
        }
        if value.measurement_type.0.is_some() {
            es.push(MeasToTimeError::NotTimeType);
        }
        match NonEmpty::from_vec(es) {
            None => Ok(Self {
                timestep: Timestep::default(),
                display: value.display,
                datatype: value.datatype,
            }),
            Some(error) => Err(TryFromTimeError { error, value }),
        }
    }
}

impl Versioned for InnerMeasurement2_0 {
    fn fcs_version() -> Version {
        Version::FCS2_0
    }
}

impl Versioned for InnerMeasurement3_0 {
    fn fcs_version() -> Version {
        Version::FCS3_0
    }
}

impl Versioned for InnerMeasurement3_1 {
    fn fcs_version() -> Version {
        Version::FCS3_1
    }
}

impl Versioned for InnerMeasurement3_2 {
    fn fcs_version() -> Version {
        Version::FCS3_2
    }
}

impl LookupMeasurement for InnerMeasurement2_0 {
    fn lookup_specific(st: &mut KwParser, n: MeasIdx) -> Option<Self> {
        Some(Self {
            scale: st.lookup_meas_opt(n, false),
            wavelength: st.lookup_meas_opt(n, false),
        })
    }
}

impl LookupMeasurement for InnerMeasurement3_0 {
    fn lookup_specific(st: &mut KwParser, n: MeasIdx) -> Option<Self> {
        Some(Self {
            scale: st.lookup_meas_req(n)?,
            gain: st.lookup_meas_opt(n, false),
            wavelength: st.lookup_meas_opt(n, false),
        })
    }
}

impl LookupMeasurement for InnerMeasurement3_1 {
    fn lookup_specific(st: &mut KwParser, n: MeasIdx) -> Option<Self> {
        Some(Self {
            scale: st.lookup_meas_req(n)?,
            gain: st.lookup_meas_opt(n, false),
            wavelengths: st.lookup_meas_opt(n, false),
            calibration: st.lookup_meas_opt(n, false),
            display: st.lookup_meas_opt(n, false),
        })
    }
}

impl LookupMeasurement for InnerMeasurement3_2 {
    fn lookup_specific(st: &mut KwParser, i: MeasIdx) -> Option<Self> {
        let measurement_type: OptionalKw<MeasurementType> = st.lookup_meas_opt(i, false);
        if measurement_type
            .0
            .as_ref()
            .is_some_and(|x| *x == MeasurementType::Time)
        {
            let msg = "$PnTYPE for non-time channel should not be 'Time' if given".into();
            st.deferred.push_error(msg);
        }
        Some(Self {
            scale: st.lookup_meas_req(i)?,
            gain: st.lookup_meas_opt(i, false),
            wavelengths: st.lookup_meas_opt(i, false),
            calibration: st.lookup_meas_opt(i, false),
            display: st.lookup_meas_opt(i, false),
            detector_name: st.lookup_meas_opt(i, false),
            tag: st.lookup_meas_opt(i, false),
            measurement_type,
            feature: st.lookup_meas_opt(i, false),
            analyte: st.lookup_meas_opt(i, false),
            datatype: st.lookup_meas_opt(i, false),
        })
    }
}

impl LookupTime for InnerTime2_0 {
    fn lookup_specific(st: &mut KwParser, i: MeasIdx) -> Option<Self> {
        let scale: OptionalKw<Scale> = st.lookup_meas_opt(i, false);
        if scale.0.is_some_and(|x| x != Scale::Linear) {
            st.deferred
                .push_error("$PnE for time channel must be linear".into());
        }
        Some(Self)
    }
}

impl LookupTime for InnerTime3_0 {
    fn lookup_specific(st: &mut KwParser, i: MeasIdx) -> Option<Self> {
        let scale: Option<Scale> = st.lookup_meas_req(i);
        if scale.is_some_and(|x| x != Scale::Linear) {
            st.deferred
                .push_error("$PnE for time channel must be linear".into());
        }
        let gain: OptionalKw<Gain> = st.lookup_meas_opt(i, false);
        if gain.0.is_some() {
            st.deferred
                .push_error("$PnG for time channel should not be set".into());
        }
        st.lookup_meta_req().map(|timestep| Self { timestep })
    }
}

impl LookupTime for InnerTime3_1 {
    fn lookup_specific(st: &mut KwParser, i: MeasIdx) -> Option<Self> {
        // TODO not DRY
        let scale: Option<Scale> = st.lookup_meas_req(i);
        if scale.is_some_and(|x| x != Scale::Linear) {
            st.deferred
                .push_error("$PnE for time channel must be linear".into());
        }
        let gain: OptionalKw<Gain> = st.lookup_meas_opt(i, false);
        if gain.0.is_some() {
            st.deferred
                .push_error("$PnG for time channel should not be set".into());
        }
        st.lookup_meta_req().map(|timestep| Self {
            timestep,
            display: st.lookup_meas_opt(i, false),
        })
    }
}

impl LookupTime for InnerTime3_2 {
    fn lookup_specific(st: &mut KwParser, i: MeasIdx) -> Option<Self> {
        let scale: Option<Scale> = st.lookup_meas_req(i);
        if scale.is_some_and(|x| x != Scale::Linear) {
            st.deferred
                .push_error("$PnE for time channel must be linear".into());
        }
        let gain: OptionalKw<Gain> = st.lookup_meas_opt(i, false);
        if gain.0.is_some() {
            st.deferred
                .push_error("$PnG for time channel should not be set".into());
        }
        let mt: OptionalKw<MeasurementType> = st.lookup_meas_opt(i, false);
        if mt.0.is_some_and(|x| x != MeasurementType::Time) {
            st.deferred
                .push_error("$PnTYPE for time channel should be 'Time' if given".into());
        }
        st.lookup_meta_req().map(|timestep| Self {
            timestep,
            display: st.lookup_meas_opt(i, false),
            datatype: st.lookup_meas_opt(i, false),
        })
    }
}

impl VersionedMeasurement for InnerMeasurement2_0 {
    fn datatype(&self) -> Option<NumType> {
        None
    }

    fn req_suffixes_inner(&self, _: MeasIdx) -> RawTriples {
        vec![]
    }

    fn opt_suffixes_inner(&self, n: MeasIdx) -> RawOptTriples {
        [
            OptMeasKey::triple(&self.scale, n),
            OptMeasKey::triple(&self.wavelength, n),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedMeasurement for InnerMeasurement3_0 {
    fn datatype(&self) -> Option<NumType> {
        None
    }

    fn req_suffixes_inner(&self, n: MeasIdx) -> RawTriples {
        [self.scale.triple(n)].into_iter().collect()
    }

    fn opt_suffixes_inner(&self, n: MeasIdx) -> RawOptTriples {
        [
            OptMeasKey::triple(&self.wavelength, n),
            OptMeasKey::triple(&self.gain, n),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedMeasurement for InnerMeasurement3_1 {
    fn datatype(&self) -> Option<NumType> {
        None
    }

    fn req_suffixes_inner(&self, n: MeasIdx) -> RawTriples {
        [self.scale.triple(n)].into_iter().collect()
    }

    fn opt_suffixes_inner(&self, n: MeasIdx) -> RawOptTriples {
        [
            OptMeasKey::triple(&self.wavelengths, n),
            OptMeasKey::triple(&self.gain, n),
            OptMeasKey::triple(&self.calibration, n),
            OptMeasKey::triple(&self.display, n),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedMeasurement for InnerMeasurement3_2 {
    fn datatype(&self) -> Option<NumType> {
        self.datatype.0.as_ref().copied()
    }

    fn req_suffixes_inner(&self, n: MeasIdx) -> RawTriples {
        [self.scale.triple(n)].into_iter().collect()
    }

    fn opt_suffixes_inner(&self, n: MeasIdx) -> RawOptTriples {
        [
            OptMeasKey::triple(&self.wavelengths, n),
            OptMeasKey::triple(&self.gain, n),
            OptMeasKey::triple(&self.calibration, n),
            OptMeasKey::triple(&self.display, n),
            OptMeasKey::triple(&self.detector_name, n),
            OptMeasKey::triple(&self.tag, n),
            OptMeasKey::triple(&self.measurement_type, n),
            OptMeasKey::triple(&self.feature, n),
            OptMeasKey::triple(&self.analyte, n),
            OptMeasKey::triple(&self.datatype, n),
        ]
        .into_iter()
        .collect()
    }
}

impl VersionedTime for InnerTime2_0 {
    fn timestep(&self) -> Option<Timestep> {
        None
    }

    fn datatype(&self) -> Option<NumType> {
        None
    }

    fn set_timestep(&mut self, _: Timestep) {}

    fn req_meta_keywords_inner(&self) -> RawPairs {
        vec![]
    }

    fn opt_meas_keywords_inner(&self, _: MeasIdx) -> RawOptPairs {
        vec![]
    }
}

impl VersionedTime for InnerTime3_0 {
    fn timestep(&self) -> Option<Timestep> {
        Some(self.timestep)
    }

    fn datatype(&self) -> Option<NumType> {
        None
    }

    fn set_timestep(&mut self, ts: Timestep) {
        self.timestep = ts;
    }

    fn req_meta_keywords_inner(&self) -> RawPairs {
        [ReqMetaKey::pair(&self.timestep)].into_iter().collect()
    }

    fn opt_meas_keywords_inner(&self, _: MeasIdx) -> RawOptPairs {
        vec![]
    }
}

impl VersionedTime for InnerTime3_1 {
    fn timestep(&self) -> Option<Timestep> {
        Some(self.timestep)
    }

    fn datatype(&self) -> Option<NumType> {
        None
    }

    fn set_timestep(&mut self, ts: Timestep) {
        self.timestep = ts;
    }

    fn req_meta_keywords_inner(&self) -> RawPairs {
        [ReqMetaKey::pair(&self.timestep)].into_iter().collect()
    }

    fn opt_meas_keywords_inner(&self, n: MeasIdx) -> RawOptPairs {
        [OptMeasKey::pair(&self.display, n)].into_iter().collect()
    }
}

impl VersionedTime for InnerTime3_2 {
    fn timestep(&self) -> Option<Timestep> {
        Some(self.timestep)
    }

    fn datatype(&self) -> Option<NumType> {
        self.datatype.0
    }

    fn set_timestep(&mut self, ts: Timestep) {
        self.timestep = ts;
    }

    fn req_meta_keywords_inner(&self) -> RawPairs {
        [ReqMetaKey::pair(&self.timestep)].into_iter().collect()
    }

    fn opt_meas_keywords_inner(&self, n: MeasIdx) -> RawOptPairs {
        [
            OptMeasKey::pair(&self.display, n),
            OptMeasKey::pair(&self.datatype, n),
        ]
        .into_iter()
        .collect()
    }
}

#[derive(Clone, Copy)]
pub enum MixedColumnSetter {
    Float(f32),
    Double(f64),
    Ascii(AsciiRangeSetter),
    Uint(NumRangeSetter),
}

impl From<MixedColumnSetter> for AlphaNumType {
    fn from(value: MixedColumnSetter) -> Self {
        match value {
            MixedColumnSetter::Float(_) => AlphaNumType::Single,
            MixedColumnSetter::Double(_) => AlphaNumType::Double,
            MixedColumnSetter::Ascii(_) => AlphaNumType::Ascii,
            MixedColumnSetter::Uint(_) => AlphaNumType::Integer,
        }
    }
}

#[derive(Clone, Copy)]
pub struct RangeSetter<B> {
    pub width: B,
    pub range: u64,
}

pub type AsciiRangeSetter = RangeSetter<Chars>;
pub type NumRangeSetter = RangeSetter<Bytes>;

impl NumRangeSetter {
    fn truncated(&self) -> (Width, Range) {
        (
            self.width.into(),
            2_u64
                .pow(u8::from(self.width).into())
                .min(self.range)
                .into(),
        )
    }
}

impl AsciiRangeSetter {
    fn truncated(&self) -> (Width, Range) {
        (
            self.width.into(),
            10_u64
                .pow(u8::from(self.width).into())
                .min(self.range)
                .into(),
        )
    }
}

type Timestamps2_0 = Timestamps<FCSTime>;
type Timestamps3_0 = Timestamps<FCSTime60>;
type Timestamps3_1 = Timestamps<FCSTime100>;

impl LookupMetadata for InnerMetadata2_0 {
    fn lookup_shortname(
        st: &mut KwParser,
        n: MeasIdx,
    ) -> Option<<Self::N as MightHave>::Wrapper<Shortname>> {
        Some(st.lookup_meas_opt(n, false))
    }

    fn lookup_specific(st: &mut KwParser, par: Par) -> Option<InnerMetadata2_0> {
        let maybe_mode = st.lookup_meta_req();
        let maybe_byteord = st.lookup_meta_req();
        if let (Some(mode), Some(byteord)) = (maybe_mode, maybe_byteord) {
            Some(InnerMetadata2_0 {
                mode,
                byteord,
                cyt: st.lookup_meta_opt(false),
                comp: st.lookup_compensation_2_0(par),
                timestamps: st.lookup_timestamps(false),
            })
        } else {
            None
        }
    }
}

impl VersionedMetadata for InnerMetadata2_0 {
    type P = InnerMeasurement2_0;
    type T = InnerTime2_0;
    type N = OptionalKwFamily;
    type L = DataLayout2_0;
    type D = ByteOrd;

    fn byteord(&self) -> ByteOrd {
        self.byteord.clone()
    }

    fn as_unstainedcenters(&self) -> Option<&UnstainedCenters> {
        None
    }

    fn with_unstainedcenters<F, X>(&mut self, _: F) -> Option<X>
    where
        F: Fn(&mut UnstainedCenters) -> Result<X, ClearOptional>,
    {
        None
    }

    fn as_spillover(&self) -> Option<&Spillover> {
        None
    }

    fn with_spillover<F, X>(&mut self, _: F) -> Option<X>
    where
        F: Fn(&mut Spillover) -> Result<X, ClearOptional>,
    {
        None
    }

    fn as_compensation(&self) -> Option<&Compensation> {
        self.comp.as_ref_opt()
    }

    fn with_compensation<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut Compensation) -> Result<X, ClearOptional>,
    {
        self.comp.mut_or_unset(f)
    }

    fn timestamps_valid(&self) -> bool {
        self.timestamps.valid()
    }

    fn datetimes_valid(&self) -> bool {
        true
    }

    fn keywords_req_inner(&self) -> RawPairs {
        [self.mode.pair(), self.byteord.pair()]
            .into_iter()
            .collect()
    }

    fn keywords_opt_inner(&self) -> RawPairs {
        [
            OptMetaKey::pair(&self.cyt),
            OptMetaKey::pair(&self.comp),
            OptMetaKey::pair(&self.timestamps.btim()),
            OptMetaKey::pair(&self.timestamps.etim()),
            OptMetaKey::pair(&self.timestamps.date()),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .collect()
    }

    fn as_column_layout(
        metadata: &Metadata<Self>,
        ms: &Measurements<Self::N, Self::T, Self::P>,
    ) -> Result<Self::L, Vec<String>> {
        Self::L::try_new(
            metadata.datatype,
            metadata.specific.byteord.clone(),
            ms.layout_data(),
        )
    }
}

impl LookupMetadata for InnerMetadata3_0 {
    fn lookup_shortname(
        st: &mut KwParser,
        n: MeasIdx,
    ) -> Option<<Self::N as MightHave>::Wrapper<Shortname>> {
        Some(st.lookup_meas_opt(n, false))
    }

    fn lookup_specific(st: &mut KwParser, _: Par) -> Option<InnerMetadata3_0> {
        let maybe_mode = st.lookup_meta_req();
        let maybe_byteord = st.lookup_meta_req();
        if let (Some(mode), Some(byteord)) = (maybe_mode, maybe_byteord) {
            Some(InnerMetadata3_0 {
                mode,
                byteord,
                cyt: st.lookup_meta_opt(false),
                comp: st.lookup_meta_opt(false),
                cytsn: st.lookup_meta_opt(false),
                unicode: st.lookup_meta_opt(false),
                timestamps: st.lookup_timestamps(false),
            })
        } else {
            None
        }
    }
}

impl VersionedMetadata for InnerMetadata3_0 {
    type P = InnerMeasurement3_0;
    type T = InnerTime3_0;
    type N = OptionalKwFamily;
    type L = DataLayout3_0;
    type D = ByteOrd;

    fn byteord(&self) -> ByteOrd {
        self.byteord.clone()
    }

    fn as_unstainedcenters(&self) -> Option<&UnstainedCenters> {
        None
    }

    fn with_unstainedcenters<F, X>(&mut self, _: F) -> Option<X>
    where
        F: Fn(&mut UnstainedCenters) -> Result<X, ClearOptional>,
    {
        None
    }

    fn as_spillover(&self) -> Option<&Spillover> {
        None
    }

    fn with_spillover<F, X>(&mut self, _: F) -> Option<X>
    where
        F: Fn(&mut Spillover) -> Result<X, ClearOptional>,
    {
        None
    }

    fn as_compensation(&self) -> Option<&Compensation> {
        self.comp.as_ref_opt()
    }

    fn with_compensation<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut Compensation) -> Result<X, ClearOptional>,
    {
        self.comp.mut_or_unset(f)
    }

    fn timestamps_valid(&self) -> bool {
        self.timestamps.valid()
    }

    fn datetimes_valid(&self) -> bool {
        true
    }

    fn keywords_req_inner(&self) -> RawPairs {
        [self.mode.pair(), self.byteord.pair()]
            .into_iter()
            .collect()
    }

    fn keywords_opt_inner(&self) -> RawPairs {
        let ts = &self.timestamps;
        [
            OptMetaKey::pair(&self.cyt),
            OptMetaKey::pair(&self.comp),
            OptMetaKey::pair(&ts.btim()),
            OptMetaKey::pair(&ts.etim()),
            OptMetaKey::pair(&ts.date()),
            OptMetaKey::pair(&self.cytsn),
            OptMetaKey::pair(&self.unicode),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .collect()
    }

    fn as_column_layout(
        metadata: &Metadata<Self>,
        ms: &Measurements<Self::N, Self::T, Self::P>,
    ) -> Result<Self::L, Vec<String>> {
        Self::L::try_new(
            metadata.datatype,
            metadata.specific.byteord.clone(),
            ms.layout_data(),
        )
    }
}

impl LookupMetadata for InnerMetadata3_1 {
    fn lookup_shortname(
        st: &mut KwParser,
        n: MeasIdx,
    ) -> Option<<Self::N as MightHave>::Wrapper<Shortname>> {
        st.lookup_meas_req(n).map(Identity)
    }

    fn lookup_specific(st: &mut KwParser, _: Par) -> Option<InnerMetadata3_1> {
        let maybe_mode = st.lookup_meta_req();
        let maybe_byteord = st.lookup_meta_req();
        if let (Some(mode), Some(byteord)) = (maybe_mode, maybe_byteord) {
            Some(InnerMetadata3_1 {
                mode,
                byteord,
                cyt: st.lookup_meta_opt(false),
                spillover: st.lookup_meta_opt(false),
                cytsn: st.lookup_meta_opt(false),
                vol: st.lookup_meta_opt(false),
                modification: st.lookup_modification(),
                timestamps: st.lookup_timestamps(false),
                plate: st.lookup_plate(false),
            })
        } else {
            None
        }
    }
}

impl VersionedMetadata for InnerMetadata3_1 {
    type P = InnerMeasurement3_1;
    type T = InnerTime3_1;
    type N = IdentityFamily;
    type L = DataLayout3_1;
    type D = Endian;

    fn byteord(&self) -> Endian {
        self.byteord
    }

    fn as_unstainedcenters(&self) -> Option<&UnstainedCenters> {
        None
    }

    fn with_unstainedcenters<F, X>(&mut self, _: F) -> Option<X>
    where
        F: Fn(&mut UnstainedCenters) -> Result<X, ClearOptional>,
    {
        None
    }

    fn as_spillover(&self) -> Option<&Spillover> {
        self.spillover.as_ref_opt()
    }

    fn with_spillover<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut Spillover) -> Result<X, ClearOptional>,
    {
        self.spillover.mut_or_unset(f)
    }

    fn as_compensation(&self) -> Option<&Compensation> {
        None
    }

    fn with_compensation<F, X>(&mut self, _: F) -> Option<X>
    where
        F: Fn(&mut Compensation) -> Result<X, ClearOptional>,
    {
        None
    }

    fn timestamps_valid(&self) -> bool {
        self.timestamps.valid()
    }

    fn datetimes_valid(&self) -> bool {
        true
    }

    fn keywords_req_inner(&self) -> RawPairs {
        [self.mode.pair(), self.byteord.pair()]
            .into_iter()
            .collect()
    }

    fn keywords_opt_inner(&self) -> RawPairs {
        let mdn = &self.modification;
        let ts = &self.timestamps;
        let pl = &self.plate;
        [
            OptMetaKey::pair(&self.cyt),
            OptMetaKey::pair(&self.spillover),
            OptMetaKey::pair(&ts.btim()),
            OptMetaKey::pair(&ts.etim()),
            OptMetaKey::pair(&ts.date()),
            OptMetaKey::pair(&self.cytsn),
            OptMetaKey::pair(&mdn.last_modifier),
            OptMetaKey::pair(&mdn.last_modified),
            OptMetaKey::pair(&mdn.originality),
            OptMetaKey::pair(&pl.plateid),
            OptMetaKey::pair(&pl.platename),
            OptMetaKey::pair(&pl.wellid),
            OptMetaKey::pair(&self.vol),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .collect()
    }

    fn as_column_layout(
        metadata: &Metadata<Self>,
        ms: &Measurements<Self::N, Self::T, Self::P>,
    ) -> Result<Self::L, Vec<String>> {
        Self::L::try_new(
            metadata.datatype,
            metadata.specific.byteord,
            ms.layout_data(),
        )
    }
}

impl LookupMetadata for InnerMetadata3_2 {
    fn lookup_shortname(
        st: &mut KwParser,
        n: MeasIdx,
    ) -> Option<<Self::N as MightHave>::Wrapper<Shortname>> {
        st.lookup_meas_req(n).map(Identity)
    }

    fn lookup_specific(st: &mut KwParser, _: Par) -> Option<InnerMetadata3_2> {
        // Only L is allowed as of 3.2, so pull the value and check it if given.
        // The only thing we care about is that the value is valid, since we
        // don't need to use it anywhere.
        let _: OptionalKw<Mode3_2> = st.lookup_meta_opt(true);
        let maybe_byteord = st.lookup_meta_req();
        let maybe_cyt = st.lookup_meta_req();
        if let (Some(byteord), Some(cyt)) = (maybe_byteord, maybe_cyt) {
            Some(InnerMetadata3_2 {
                byteord,
                cyt,
                spillover: st.lookup_meta_opt(false),
                cytsn: st.lookup_meta_opt(false),
                vol: st.lookup_meta_opt(false),
                flowrate: st.lookup_meta_opt(false),
                modification: st.lookup_modification(),
                plate: st.lookup_plate(true),
                timestamps: st.lookup_timestamps(true),
                carrier: st.lookup_carrier(),
                datetimes: st.lookup_datetimes(),
                unstained: st.lookup_unstained(),
            })
        } else {
            None
        }
    }
}

impl VersionedMetadata for InnerMetadata3_2 {
    type P = InnerMeasurement3_2;
    type T = InnerTime3_2;
    type N = IdentityFamily;
    type L = DataLayout3_2;
    type D = Endian;

    // fn lookup_tot(kws: &mut RawKeywords) -> PureMaybe<Tot> {
    //     PureMaybe::from_result_1(Tot::remove_meta_req(kws), PureErrorLevel::Error)
    // }

    fn byteord(&self) -> Endian {
        self.byteord
    }

    fn as_unstainedcenters(&self) -> Option<&UnstainedCenters> {
        self.unstained.unstainedcenters.as_ref_opt()
    }

    fn with_unstainedcenters<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut UnstainedCenters) -> Result<X, ClearOptional>,
    {
        self.unstained.unstainedcenters.mut_or_unset(f)
    }

    fn as_spillover(&self) -> Option<&Spillover> {
        self.spillover.as_ref_opt()
    }

    fn with_spillover<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut Spillover) -> Result<X, ClearOptional>,
    {
        self.spillover.mut_or_unset(f)
    }

    fn as_compensation(&self) -> Option<&Compensation> {
        None
    }

    fn with_compensation<F, X>(&mut self, _: F) -> Option<X>
    where
        F: Fn(&mut Compensation) -> Result<X, ClearOptional>,
    {
        None
    }

    fn timestamps_valid(&self) -> bool {
        self.timestamps.valid()
    }

    fn datetimes_valid(&self) -> bool {
        self.datetimes.valid()
    }

    fn keywords_req_inner(&self) -> RawPairs {
        [self.byteord.pair(), self.cyt.pair()].into_iter().collect()
    }

    fn keywords_opt_inner(&self) -> RawPairs {
        let mdn = &self.modification;
        let ts = &self.timestamps;
        let pl = &self.plate;
        let car = &self.carrier;
        let dt = &self.datetimes;
        let us = &self.unstained;
        [
            OptMetaKey::pair(&self.spillover),
            OptMetaKey::pair(&ts.btim()),
            OptMetaKey::pair(&ts.etim()),
            OptMetaKey::pair(&ts.date()),
            OptMetaKey::pair(&self.cytsn),
            OptMetaKey::pair(&mdn.last_modifier),
            OptMetaKey::pair(&mdn.last_modified),
            OptMetaKey::pair(&mdn.originality),
            OptMetaKey::pair(&pl.plateid),
            OptMetaKey::pair(&pl.platename),
            OptMetaKey::pair(&pl.wellid),
            OptMetaKey::pair(&self.vol),
            OptMetaKey::pair(&car.carrierid),
            OptMetaKey::pair(&car.carriertype),
            OptMetaKey::pair(&car.locationid),
            OptMetaKey::pair(&dt.begin()),
            OptMetaKey::pair(&dt.end()),
            OptMetaKey::pair(&us.unstainedcenters),
            OptMetaKey::pair(&us.unstainedinfo),
            OptMetaKey::pair(&self.flowrate),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .collect()
    }

    fn as_column_layout(
        metadata: &Metadata<Self>,
        ms: &Measurements<Self::N, Self::T, Self::P>,
    ) -> Result<Self::L, Vec<String>> {
        let endian = metadata.specific.byteord;
        let blank_cs = ms.layout_data();
        let cs: Vec<_> = ms
            .iter()
            .map(|x| {
                x.1.map_or_else(
                    |m| (&m.value.specific.datatype),
                    |t| (&t.value.specific.datatype),
                )
            })
            .map(|dt| dt.0.map(|d| d.into()).unwrap_or(metadata.datatype))
            .zip(blank_cs)
            .map(|(datatype, c)| ColumnLayoutData {
                width: c.width,
                range: c.range,
                datatype,
            })
            .collect();
        Self::L::try_new(metadata.datatype, endian, cs)
    }
}

impl InnerTime3_0 {
    pub(crate) fn new(timestep: Timestep) -> Self {
        Self { timestep }
    }
}

impl InnerTime3_1 {
    pub(crate) fn new(timestep: Timestep) -> Self {
        Self {
            timestep,
            display: None.into(),
        }
    }
}

impl InnerTime3_2 {
    pub(crate) fn new(timestep: Timestep) -> Self {
        Self {
            timestep,
            datatype: None.into(),
            display: None.into(),
        }
    }
}

impl InnerMeasurement2_0 {
    pub(crate) fn new() -> Self {
        Self {
            scale: None.into(),
            wavelength: None.into(),
        }
    }
}

impl InnerMeasurement3_0 {
    pub(crate) fn new(scale: Scale) -> Self {
        Self {
            scale,
            gain: None.into(),
            wavelength: None.into(),
        }
    }
}

impl InnerMeasurement3_1 {
    pub(crate) fn new(scale: Scale) -> Self {
        Self {
            scale,
            calibration: None.into(),
            display: None.into(),
            gain: None.into(),
            wavelengths: None.into(),
        }
    }
}

impl InnerMeasurement3_2 {
    pub(crate) fn new(scale: Scale) -> Self {
        Self {
            scale,
            analyte: None.into(),
            calibration: None.into(),
            datatype: None.into(),
            detector_name: None.into(),
            display: None.into(),
            feature: None.into(),
            gain: None.into(),
            measurement_type: None.into(),
            tag: None.into(),
            wavelengths: None.into(),
        }
    }
}

impl InnerMetadata2_0 {
    pub(crate) fn new(mode: Mode, byteord: ByteOrd) -> Self {
        Self {
            mode,
            byteord,
            cyt: None.into(),
            timestamps: Timestamps::default(),
            comp: None.into(),
        }
    }
}

impl InnerMetadata3_0 {
    pub(crate) fn new(mode: Mode, byteord: ByteOrd) -> Self {
        Self {
            mode,
            byteord,
            cyt: None.into(),
            timestamps: Timestamps::default(),
            cytsn: None.into(),
            comp: None.into(),
            unicode: None.into(),
        }
    }
}

impl InnerMetadata3_1 {
    pub(crate) fn new(mode: Mode, is_big: bool) -> Self {
        Self {
            mode,
            byteord: Endian::is_big(is_big),
            cyt: None.into(),
            plate: PlateData::default(),
            timestamps: Timestamps::default(),
            cytsn: None.into(),
            modification: ModificationData::default(),
            spillover: None.into(),
            vol: None.into(),
        }
    }
}

impl InnerMetadata3_2 {
    pub(crate) fn new(is_big: bool, cyt: String) -> Self {
        Self {
            byteord: Endian::is_big(is_big),
            cyt: cyt.into(),
            carrier: CarrierData::default(),
            plate: PlateData::default(),
            datetimes: Datetimes::default(),
            timestamps: Timestamps::default(),
            cytsn: None.into(),
            flowrate: None.into(),
            modification: ModificationData::default(),
            unstained: UnstainedData::default(),
            spillover: None.into(),
            vol: None.into(),
        }
    }
}

impl TimeChannel2_0 {
    pub fn new(bytes: Width, range: Range) -> Self {
        let specific = InnerTime2_0;
        TimeChannel::new_common(bytes, range, specific)
    }
}

impl TimeChannel3_0 {
    pub fn new(bytes: Width, range: Range, timestep: Timestep) -> Self {
        let specific = InnerTime3_0::new(timestep);
        TimeChannel::new_common(bytes, range, specific)
    }
}

impl TimeChannel3_1 {
    pub fn new(bytes: Width, range: Range, timestep: Timestep) -> Self {
        let specific = InnerTime3_1::new(timestep);
        TimeChannel::new_common(bytes, range, specific)
    }
}

impl TimeChannel3_2 {
    pub fn new(bytes: Width, range: Range, timestep: Timestep) -> Self {
        let specific = InnerTime3_2::new(timestep);
        TimeChannel::new_common(bytes, range, specific)
    }
}

impl Measurement2_0 {
    pub fn new(bytes: Width, range: Range) -> Self {
        let specific = InnerMeasurement2_0::new();
        Measurement::new_common(bytes, range, specific)
    }
}

impl Measurement3_0 {
    pub fn new(bytes: Width, range: Range, scale: Scale) -> Self {
        let specific = InnerMeasurement3_0::new(scale);
        Measurement::new_common(bytes, range, specific)
    }
}

impl Measurement3_1 {
    pub fn new(bytes: Width, range: Range, scale: Scale) -> Self {
        let specific = InnerMeasurement3_1::new(scale);
        Measurement::new_common(bytes, range, specific)
    }
}

impl Measurement3_2 {
    pub fn new(bytes: Width, range: Range, scale: Scale) -> Self {
        let specific = InnerMeasurement3_2::new(scale);
        Measurement::new_common(bytes, range, specific)
    }
}
