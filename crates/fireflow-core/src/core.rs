use crate::config::*;
use crate::data::*;
use crate::error::*;
use crate::header::*;
use crate::header_text::*;
use crate::macros::{enum_from, enum_from_disp, match_many_to_one, newtype_from};
use crate::segment::*;
use crate::text::byteord::*;
use crate::text::compensation::*;
use crate::text::datetimes::*;
use crate::text::float_or_int::*;
use crate::text::index::*;
use crate::text::keywords::*;
use crate::text::named_vec::*;
use crate::text::optionalkw::*;
use crate::text::parser::*;
use crate::text::scale::*;
use crate::text::spillover::*;
use crate::text::timestamps::*;
use crate::text::unstainedcenters::*;
use crate::validated::dataframe::*;
use crate::validated::nonstandard::*;
use crate::validated::pattern::*;
use crate::validated::shortname::*;
use crate::validated::standard::*;

use chrono::Timelike;
use itertools::Itertools;
use nalgebra::DMatrix;
use nonempty::NonEmpty;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::io::{BufReader, BufWriter, Read, Seek, Write};
use std::marker::PhantomData;
use std::str::FromStr;

/// Represents the minimal data required to write an FCS file.
///
/// At minimum, this contains the TEXT keywords in a version-specific structure
/// with a few exceptions (see next). It may also contain the DATA and ANALYSIS
/// segments depending on how much of the FCS file is read. These fields are
/// left generic to allow this flexibility.
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
pub struct Core<A, D, M, T, P, N, W> {
    /// The root of the metadata tree (ie "non-measurement" keywords)
    pub metaroot: Metaroot<M>,

    /// All measurement TEXT keywords.
    ///
    /// Specifically these are denoted by "$Pn*" keywords where "n" is the index
    /// of the measurement which also corresponds to its column in the DATA
    /// segment. The index of each measurement in this vector is n - 1.
    measurements: NamedVec<N, W, Temporal<T>, Optical<P>>,

    /// DATA segment (if applicable)
    data: D,

    /// ANALYSIS segment (if applicable)
    pub analysis: A,
    // TODO add CRC

    // TODO add OTHER
}

/// The ANALYSIS segment, which is just a string of bytes
#[derive(Clone)]
pub struct Analysis(pub Vec<u8>);

newtype_from!(Analysis, Vec<u8>);

/// Root of the metadata hierarchy.
///
/// Explicit fields are common to all FCS versions.
///
/// The generic type parameter allows version-specific data to be encoded.
#[derive(Clone, Serialize)]
pub struct Metaroot<X> {
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

#[derive(Clone, Serialize)]
pub struct CommonMeasurement {
    /// Value for $PnB
    pub width: Width,

    /// Value for $PnR
    pub range: Range,

    /// Value for $PnS
    pub longname: OptionalKw<Longname>,

    /// Non standard keywords that belong to this measurement.
    ///
    /// These are found using a configurable pattern to filter matching keys.
    pub nonstandard_keywords: NonStdKeywords,
}

/// Structured data for time keywords.
///
/// Explicit fields are common to all versions. The generic type parameter
/// allows for version-specific information to be encoded.
#[derive(Clone, Serialize)]
pub struct Temporal<X> {
    /// Fields shared with optical measurements
    pub common: CommonMeasurement,

    /// Version specific data
    pub specific: X,
}

/// Structured data for optical keywords.
///
/// Explicit fields are common to all versions. The generic type parameter
/// allows for version-specific information to be encoded.
#[derive(Clone, Serialize)]
pub struct Optical<X> {
    /// Fields shared with optical measurements
    pub common: CommonMeasurement,

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

    /// Version specific data
    pub specific: X,
}

/// Minimal TEXT data for any supported FCS version
#[derive(Clone)]
pub enum AnyCore<A, D> {
    FCS2_0(Box<Core2_0<A, D>>),
    FCS3_0(Box<Core3_0<A, D>>),
    FCS3_1(Box<Core3_1<A, D>>),
    FCS3_2(Box<Core3_2<A, D>>),
}

pub type AnyCoreTEXT = AnyCore<(), ()>;
pub type AnyCoreDataset = AnyCore<Analysis, FCSDataFrame>;

macro_rules! from_anycoretext {
    ($anyvar:ident, $coretype:ident) => {
        impl<A, D> From<$coretype<A, D>> for AnyCore<A, D> {
            fn from(value: $coretype<A, D>) -> Self {
                Self::$anyvar(Box::new(value))
            }
        }
    };
}

from_anycoretext!(FCS2_0, Core2_0);
from_anycoretext!(FCS3_0, Core3_0);
from_anycoretext!(FCS3_1, Core3_1);
from_anycoretext!(FCS3_2, Core3_2);

impl<A, D> Serialize for AnyCore<A, D>
where
    A: Serialize,
    D: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AnyCore", 2)?;
        match self {
            Self::FCS2_0(x) => {
                state.serialize_field("version", &Version::FCS2_0)?;
                state.serialize_field("data", &x)?;
            }
            Self::FCS3_0(x) => {
                state.serialize_field("version", &Version::FCS3_0)?;
                state.serialize_field("data", &x)?;
            }
            Self::FCS3_1(x) => {
                state.serialize_field("version", &Version::FCS3_1)?;
                state.serialize_field("data", &x)?;
            }
            Self::FCS3_2(x) => {
                state.serialize_field("version", &Version::FCS3_2)?;
                state.serialize_field("data", &x)?;
            }
        }
        state.end()
    }
}

macro_rules! match_anycore {
    ($self:expr, $bind:ident, $stuff:block) => {
        match_many_to_one!($self, Self, [FCS2_0, FCS3_0, FCS3_1, FCS3_2], $bind, $stuff)
    };
}

pub(crate) use match_anycore;

impl<A, D> AnyCore<A, D> {
    pub fn version(&self) -> Version {
        match self {
            Self::FCS2_0(_) => Version::FCS2_0,
            Self::FCS3_0(_) => Version::FCS3_0,
            Self::FCS3_1(_) => Version::FCS3_1,
            Self::FCS3_2(_) => Version::FCS3_2,
        }
    }

    pub fn shortnames(&self) -> Vec<Shortname> {
        match_anycore!(self, x, { x.all_shortnames() })
    }

    pub fn text_segment(
        &self,
        tot: Tot,
        data_len: usize,
        analysis_len: usize,
    ) -> Option<Vec<String>> {
        match_anycore!(self, x, { x.text_segment(tot, data_len, analysis_len) })
    }

    pub fn print_meas_table(&self, delim: &str) {
        match_anycore!(self, x, { x.print_meas_table(delim) })
    }

    pub fn print_spillover_table(&self, delim: &str) {
        let res = match_anycore!(self, x, { x.metaroot.specific.as_spillover() })
            .as_ref()
            .map(|s| s.print_table(delim));
        if res.is_none() {
            println!("None")
        }
    }
}

impl AnyCoreTEXT {
    pub(crate) fn parse_raw(
        version: Version,
        std: &mut StdKeywords,
        nonstd: NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> DeferredResult<Self, LookupMeasWarning, LookupKeysError> {
        match version {
            Version::FCS2_0 => {
                CoreTEXT2_0::new_text_from_raw(std, nonstd, conf).def_map_value(|x| x.into())
            }
            Version::FCS3_0 => {
                CoreTEXT3_0::new_text_from_raw(std, nonstd, conf).def_map_value(|x| x.into())
            }
            Version::FCS3_1 => {
                CoreTEXT3_1::new_text_from_raw(std, nonstd, conf).def_map_value(|x| x.into())
            }
            Version::FCS3_2 => {
                CoreTEXT3_2::new_text_from_raw(std, nonstd, conf).def_map_value(|x| x.into())
            }
        }
    }
}

impl AnyCoreDataset {
    pub fn as_data(&self) -> &FCSDataFrame {
        match_anycore!(self, x, { &x.data })
    }

    pub(crate) fn parse_raw<R: Read + Seek>(
        h: &mut BufReader<R>,
        version: Version,
        kws: &mut StdKeywords,
        nonstd: NonStdKeywords,
        data_seg: HeaderDataSegment,
        analysis_seg: HeaderAnalysisSegment,
        conf: &DataReadConfig,
    ) -> IODeferredResult<
        (Self, AnyDataSegment, AnyAnalysisSegment),
        StdDatasetFromRawWarning,
        StdDatasetFromRawError,
    > {
        match version {
            Version::FCS2_0 => {
                CoreDataset2_0::new_dataset_from_raw(h, kws, nonstd, data_seg, analysis_seg, conf)
                    .def_map_value(|(x, y, z)| (x.into(), y, z))
            }
            Version::FCS3_0 => {
                CoreDataset3_0::new_dataset_from_raw(h, kws, nonstd, data_seg, analysis_seg, conf)
                    .def_map_value(|(x, y, z)| (x.into(), y, z))
            }
            Version::FCS3_1 => {
                CoreDataset3_1::new_dataset_from_raw(h, kws, nonstd, data_seg, analysis_seg, conf)
                    .def_map_value(|(x, y, z)| (x.into(), y, z))
            }
            Version::FCS3_2 => {
                CoreDataset3_2::new_dataset_from_raw(h, kws, nonstd, data_seg, analysis_seg, conf)
                    .def_map_value(|(x, y, z)| (x.into(), y, z))
            }
        }
    }
}

/// Metaroot fields specific to version 2.0
#[derive(Clone, Serialize)]
pub struct InnerMetaroot2_0 {
    /// Value of $MODE
    pub mode: Mode,

    /// Value of $BYTEORD
    byteord: ByteOrd,

    /// Value of $CYT
    pub cyt: OptionalKw<Cyt>,

    /// Compensation matrix derived from 'DFCnTOm' key/value pairs
    comp: OptionalKw<Compensation2_0>,

    /// Values of $BTIM/ETIM/$DATE
    pub timestamps: Timestamps2_0,

    /// Values of $Gm*/$RnI/$RnW/$GATING/$GATE
    applied_gates: OptionalKw<AppliedGates2_0>,
}

/// Metaroot fields specific to version 3.0
#[derive(Clone, Serialize)]
pub struct InnerMetaroot3_0 {
    /// Value of $MODE
    pub mode: Mode,

    /// Value of $BYTEORD
    byteord: ByteOrd,

    /// Value of $CYT
    pub cyt: OptionalKw<Cyt>,

    /// Value of $COMP
    comp: OptionalKw<Compensation3_0>,

    /// Values of $BTIM/ETIM/$DATE
    pub timestamps: Timestamps3_0,

    /// Value of $CYTSN
    pub cytsn: OptionalKw<Cytsn>,

    /// Value of $UNICODE
    pub unicode: OptionalKw<Unicode>,

    /// Aggregated values for $CS* keywords
    pub subset: OptionalKw<SubsetData>,

    /// Values of $Gm*/$RnI/$RnW/$GATING/$GATE
    applied_gates: OptionalKw<AppliedGates3_0>,
}

/// Metaroot fields specific to version 3.1
#[derive(Clone, Serialize)]
pub struct InnerMetaroot3_1 {
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

    /// Aggregated values for $CS* keywords
    pub subset: OptionalKw<SubsetData>,

    /// Values of $Gm*/$RnI/$RnW/$GATING/$GATE
    applied_gates: OptionalKw<AppliedGates3_0>,
}

/// Metaroot fields specific to version 3.2
#[derive(Clone, Serialize)]
pub struct InnerMetaroot3_2 {
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

    /// Values of $RnI/$RnW/$GATING
    applied_gates: OptionalKw<AppliedGates3_2>,
}

/// Temporal measurement fields specific to version 2.0
#[derive(Clone, Serialize, Default)]
pub struct InnerTemporal2_0 {
    /// Value of $PnE
    ///
    /// Unlike subsequent versions, included here because it is optional rather
    /// than required and constant.
    pub scale: OptionalKw<TemporalScale>,

    /// Values of $Pkn/$PKNn
    pub peak: PeakData,
}

/// Temporal measurement fields specific to version 3.0
///
/// $PnE is implied as linear but not included since it only has one value
#[derive(Clone, Serialize)]
pub struct InnerTemporal3_0 {
    /// Value for $TIMESTEP
    pub timestep: Timestep,

    /// Values of $Pkn/$PKNn
    pub peak: PeakData,
}

/// Temporal measurement fields specific to version 3.1
///
/// $PnE is implied as linear but not included since it only has one value
#[derive(Clone, Serialize)]
pub struct InnerTemporal3_1 {
    /// Value for $TIMESTEP
    pub timestep: Timestep,

    /// Value for $PnDISPLAY
    pub display: OptionalKw<Display>,

    /// Values of $Pkn/$PKNn
    pub peak: PeakData,
}

/// Temporal measurement fields specific to version 3.2
///
/// $PnE is implied as linear but not included since it only has one value
#[derive(Clone, Serialize)]
pub struct InnerTemporal3_2 {
    /// Value for $TIMESTEP
    pub timestep: Timestep,

    /// Value for $PnDISPLAY
    pub display: OptionalKw<Display>,

    /// Value for $PnDATATYPE
    pub datatype: OptionalKw<NumType>,

    /// Value for $PnTYPE
    pub measurement_type: OptionalKw<TemporalType>,
}

/// Optical measurement fields specific to version 2.0
#[derive(Clone, Serialize)]
pub struct InnerOptical2_0 {
    /// Value for $PnE
    pub scale: OptionalKw<Scale>,

    /// Value for $PnL
    pub wavelength: OptionalKw<Wavelength>,

    /// Values of $Pkn/$PKNn
    pub peak: PeakData,
}

/// Optical measurement fields specific to version 3.0
#[derive(Clone, Serialize)]
pub struct InnerOptical3_0 {
    /// Value for $PnE
    pub scale: Scale,

    /// Value for $PnL
    pub wavelength: OptionalKw<Wavelength>,

    /// Value for $PnG
    pub gain: OptionalKw<Gain>,

    /// Values of $Pkn/$PKNn
    pub peak: PeakData,
}

/// Optical measurement fields specific to version 3.1
#[derive(Clone, Serialize)]
pub struct InnerOptical3_1 {
    /// Value for $PnE
    pub scale: Scale,

    /// Value for $PnL
    pub wavelengths: OptionalKw<Wavelengths>,

    /// Value for $PnG
    pub gain: OptionalKw<Gain>,

    /// Value for $PnCALIBRATION
    pub calibration: OptionalKw<Calibration3_1>,

    /// Value for $PnDISPLAY
    pub display: OptionalKw<Display>,

    /// Values of $Pkn/$PKNn
    pub peak: PeakData,
}

/// Optical measurement fields specific to version 3.2
#[derive(Clone, Serialize)]
pub struct InnerOptical3_2 {
    /// Value for $PnE
    pub scale: Scale,

    /// Value for $PnL
    pub wavelengths: OptionalKw<Wavelengths>,

    /// Value for $PnG
    pub gain: OptionalKw<Gain>,

    /// Value for $PnCALIBRATION
    pub calibration: OptionalKw<Calibration3_2>,

    /// Value for $PnDISPLAY
    pub display: OptionalKw<Display>,

    /// Value for $PnANALYTE
    pub analyte: OptionalKw<Analyte>,

    /// Value for $PnFEATURE
    pub feature: OptionalKw<Feature>,

    /// Value for $PnTYPE
    pub measurement_type: OptionalKw<OpticalType>,

    /// Value for $PnTAG
    pub tag: OptionalKw<Tag>,

    /// Value for $PnDET
    pub detector_name: OptionalKw<DetectorName>,

    /// Value for $PnDATATYPE
    pub datatype: OptionalKw<NumType>,
}

/// The values for $Gm* keywords (2.0-3.1)
#[derive(Clone, Default, Serialize)]
pub struct GatedMeasurement {
    /// Value for $GmE
    pub scale: OptionalKw<GateScale>,

    /// Value for $GmF
    pub filter: OptionalKw<GateFilter>,

    /// Value for $GmN
    ///
    /// Unlike $PnN, this is not validated to be without commas
    pub shortname: OptionalKw<GateShortname>,

    /// Value for $GmP
    pub percent_emitted: OptionalKw<GatePercentEmitted>,

    /// Value for $GmR
    pub range: OptionalKw<GateRange>,

    /// Value for $GmS
    pub longname: OptionalKw<GateLongname>,

    /// Value for $GmT
    pub detector_type: OptionalKw<GateDetectorType>,

    /// Value for $GmV
    pub detector_voltage: OptionalKw<GateDetectorVoltage>,
}

/// The $GATING/$RnI/$RnW/$Gn* keywords in a unified bundle (2.0)
///
/// Each region is assumed to point to a member of ['gated_measurements'].
#[derive(Clone, Serialize)]
pub struct AppliedGates2_0 {
    pub gated_measurements: GatedMeasurements,
    pub regions: GatingRegions<GateIndex>,
}

/// The $GATING/$RnI/$RnW/$Gn* keywords in a unified bundle (3.0-3.1)
///
/// Each region is assumed to point to a member of ['gated_measurements'] or
/// a measurement in the ['Core'] struct
#[derive(Clone, Serialize)]
pub struct AppliedGates3_0 {
    pub gated_measurements: Vec<GatedMeasurement>,
    pub regions: GatingRegions<MeasOrGateIndex>,
}

/// The $GATING/$RnI/$RnW keywords in a unified bundle (3.2)
///
/// Each region is assumed to point to a measurement in the ['Core'] struct
#[derive(Clone, Serialize)]
pub struct AppliedGates3_2 {
    pub regions: GatingRegions<PrefixedMeasIndex>,
}

/// The $GATING/$RnI/$RnW keywords in a unified bundle.
///
/// All regions in $GATING are assumed to have corresponding $RnI/$RnW keywords,
/// and each $RnI/$RnW pair is assumed to be consistent (ie both are univariate
/// or bivariate)
#[derive(Clone)]
pub struct GatingRegions<I> {
    pub gating: Gating,
    pub regions: NonEmpty<(RegionIndex, Region<I>)>,
}

/// A list of $Gn* keywords for indices 1-n.
///
/// The maximum value of 'n' implies the $GATE keyword.
#[derive(Clone)]
pub struct GatedMeasurements(pub NonEmpty<GatedMeasurement>);

/// A uni/bivariate region corresponding to an $RnI/$RnW keyword pair
#[derive(Clone, Serialize)]
pub enum Region<I> {
    Univariate(UnivariateRegion<I>),
    Bivariate(BivariateRegion<I>),
}

pub type Region2_0 = Region<GateIndex>;
pub type Region3_0 = Region<MeasOrGateIndex>;
pub type Region3_2 = Region<PrefixedMeasIndex>;

/// A univariate region corresponding to an $RnI/$RnW keyword pair
#[derive(Clone, Serialize)]
pub struct UnivariateRegion<I> {
    pub gate: UniGate,
    pub index: I,
}

/// A bivariate region corresponding to an $RnI/$RnW keyword pair
#[derive(Clone)]
pub struct BivariateRegion<I> {
    pub vertices: NonEmpty<Vertex>,
    pub x_index: I,
    pub y_index: I,
}

/// A bundle for $PKn and $PKNn (2.0-3.1)
///
/// It makes little sense to have only one of these since they both collectively
/// describe a histogram peak. This currently is not enforced since these keys
/// are likely not used much and it is easy for users to check these themselves.
#[derive(Clone, Default, Serialize)]
pub struct PeakData {
    /// Value of $Pkn
    pub bin: OptionalKw<PeakBin>,

    /// Value of $PkNn
    pub size: OptionalKw<PeakNumber>,
}

/// A bundle for $CSMODE, $CSVBITS, and $CSVnFLAG (3.0, 3.1)
///
/// These describe what is sometimes present in the ANALYSIS segment for 3.0 and
/// 3.1. In these versions, it was similar to TEXT which had key/value pairs. In
/// 3.2, these keywords were removed and the ANALYSIS segment became a free-form
/// bytestring. This library currently makes no attempt to interpret the
/// ANALYSIS segment given the CS* keywords, but may add this in the future if
/// the need arises.
#[derive(Clone, Default)]
pub struct SubsetData {
    /// Value of $CSBITS if given
    pub bits: OptionalKw<CSVBits>,

    /// Values of $CSVnFLAG if given, with length equal to $CSMODE
    pub flags: NonEmpty<OptionalKw<CSVFlag>>,
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

pub type Temporal2_0 = Temporal<InnerTemporal2_0>;
pub type Temporal3_0 = Temporal<InnerTemporal3_0>;
pub type Temporal3_1 = Temporal<InnerTemporal3_1>;
pub type Temporal3_2 = Temporal<InnerTemporal3_2>;

pub type Optical2_0 = Optical<InnerOptical2_0>;
pub type Optical3_0 = Optical<InnerOptical3_0>;
pub type Optical3_1 = Optical<InnerOptical3_1>;
pub type Optical3_2 = Optical<InnerOptical3_2>;

pub type Measurements2_0 = Measurements<OptionalKwFamily, InnerTemporal2_0, InnerOptical2_0>;
pub type Measurements3_0 = Measurements<OptionalKwFamily, InnerTemporal3_0, InnerOptical3_0>;
pub type Measurements3_1 = Measurements<IdentityFamily, InnerTemporal3_1, InnerOptical3_1>;
pub type Measurements3_2 = Measurements<IdentityFamily, InnerTemporal3_2, InnerOptical3_2>;

pub type Metaroot2_0 = Metaroot<InnerMetaroot2_0>;
pub type Metaroot3_0 = Metaroot<InnerMetaroot3_0>;
pub type Metaroot3_1 = Metaroot<InnerMetaroot3_1>;
pub type Metaroot3_2 = Metaroot<InnerMetaroot3_2>;

/// A minimal representation of the TEXT segment
pub type CoreTEXT<M, T, P, N, W> = Core<(), (), M, T, P, N, W>;

/// A minimal representation of the TEXT+DATA+ANALYSIS segments
pub type CoreDataset<M, T, P, N, W> = Core<Analysis, FCSDataFrame, M, T, P, N, W>;

pub type CoreTEXT2_0 = CoreTEXT<
    InnerMetaroot2_0,
    InnerTemporal2_0,
    InnerOptical2_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type CoreTEXT3_0 = CoreTEXT<
    InnerMetaroot3_0,
    InnerTemporal3_0,
    InnerOptical3_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type CoreTEXT3_1 = CoreTEXT<
    InnerMetaroot3_1,
    InnerTemporal3_1,
    InnerOptical3_1,
    IdentityFamily,
    Identity<Shortname>,
>;
pub type CoreTEXT3_2 = CoreTEXT<
    InnerMetaroot3_2,
    InnerTemporal3_2,
    InnerOptical3_2,
    IdentityFamily,
    Identity<Shortname>,
>;

pub type CoreDataset2_0 = CoreDataset<
    InnerMetaroot2_0,
    InnerTemporal2_0,
    InnerOptical2_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type CoreDataset3_0 = CoreDataset<
    InnerMetaroot3_0,
    InnerTemporal3_0,
    InnerOptical3_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type CoreDataset3_1 = CoreDataset<
    InnerMetaroot3_1,
    InnerTemporal3_1,
    InnerOptical3_1,
    IdentityFamily,
    Identity<Shortname>,
>;
pub type CoreDataset3_2 = CoreDataset<
    InnerMetaroot3_2,
    InnerTemporal3_2,
    InnerOptical3_2,
    IdentityFamily,
    Identity<Shortname>,
>;

pub type Core2_0<A, D> = Core<
    A,
    D,
    InnerMetaroot2_0,
    InnerTemporal2_0,
    InnerOptical2_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type Core3_0<A, D> = Core<
    A,
    D,
    InnerMetaroot3_0,
    InnerTemporal3_0,
    InnerOptical3_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type Core3_1<A, D> = Core<
    A,
    D,
    InnerMetaroot3_1,
    InnerTemporal3_1,
    InnerOptical3_1,
    IdentityFamily,
    Identity<Shortname>,
>;
pub type Core3_2<A, D> = Core<
    A,
    D,
    InnerMetaroot3_2,
    InnerTemporal3_2,
    InnerOptical3_2,
    IdentityFamily,
    Identity<Shortname>,
>;

type RawInput2_0 = RawInput<OptionalKwFamily, Temporal2_0, Optical2_0>;
type RawInput3_0 = RawInput<OptionalKwFamily, Temporal3_0, Optical3_0>;
type RawInput3_1 = RawInput<IdentityFamily, Temporal3_1, Optical3_1>;
type RawInput3_2 = RawInput<IdentityFamily, Temporal3_2, Optical3_2>;

pub trait Versioned {
    fn fcs_version() -> Version;
}

pub(crate) trait LookupMetaroot: Sized + VersionedMetaroot {
    fn lookup_shortname(
        kws: &mut StdKeywords,
        n: MeasIndex,
    ) -> LookupResult<<Self::N as MightHave>::Wrapper<Shortname>>;

    fn lookup_specific(st: &mut StdKeywords, par: Par) -> LookupResult<Self>;
}

pub trait ConvertFromMetaroot<M>: Sized
where
    Self: VersionedMetaroot,
    M: VersionedMetaroot,
{
    fn convert_from_metaroot(
        value: M,
        byteord: SizeConvert<M::D>,
        force: bool,
    ) -> MetarootConvertResult<Self>;
}

pub trait ConvertFromOptical<O>: Sized
where
    Self: VersionedOptical,
{
    fn convert_from_optical(value: O, i: MeasIndex, force: bool) -> OpticalConvertResult<Self>;
}

pub trait ConvertFromTemporal<T>: Sized
where
    Self: VersionedTemporal,
{
    fn convert_from_temporal(value: T, i: MeasIndex, force: bool)
        -> TemporalConvertTentative<Self>;
}

pub trait VersionedMetaroot: Sized {
    type O: VersionedOptical;
    type T: VersionedTemporal;
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

    fn byteord(&self) -> Self::D;

    fn keywords_req_inner(&self) -> RawPairs;

    fn keywords_opt_inner(&self) -> RawPairs;

    fn as_data_layout(
        metaroot: &Metaroot<Self>,
        ms: &Measurements<Self::N, Self::T, Self::O>,
        conf: &SharedConfig,
    ) -> DeferredResult<Self::L, NewDataLayoutWarning, NewDataLayoutError>;

    #[allow(clippy::type_complexity)]
    fn swap_optical_temporal(
        t: Temporal<Self::T>,
        o: Optical<Self::O>,
        i: MeasIndex,
        lossless: bool,
    ) -> PassthruResult<
        (Optical<Self::O>, Temporal<Self::T>),
        Box<(Temporal<Self::T>, Optical<Self::O>)>,
        SwapOpticalTemporalError,
        SwapOpticalTemporalError,
    > {
        let go = |old_t: Temporal<Self::T>, old_o: Optical<Self::O>| {
            let (so, st) = Self::swap_optical_temporal_inner(old_t.specific, old_o.specific);
            let new_o = Optical {
                common: old_t.common,
                filter: None.into(),
                power: None.into(),
                detector_type: None.into(),
                percent_emitted: None.into(),
                detector_voltage: None.into(),
                specific: so,
            };
            let new_t = Temporal {
                common: old_o.common,
                specific: st,
            };
            (new_o, new_t)
        };
        let t_res = o.specific.can_convert_to_temporal(i).mult_errors_into();
        let o_specific_res = t.specific.can_convert_to_optical(i).mult_errors_into();
        let o_common_res = check_optical_keys_transfer(&o, i).mult_errors_into();
        match t_res.mult_zip3(o_specific_res, o_common_res) {
            Ok(_) => Ok(Tentative::new1(go(t, o))),
            Err(es) => {
                if lossless {
                    Err(DeferredFailure::new(vec![], es, Box::new((t, o))))
                } else {
                    Ok(Tentative::new(go(t, o), es.into(), vec![]))
                }
            }
        }
    }

    fn swap_optical_temporal_inner(t: Self::T, p: Self::O) -> (Self::O, Self::T);
}

pub trait VersionedOptical: Sized + Versioned {
    fn req_suffixes_inner(&self, n: MeasIndex) -> RawTriples;

    fn opt_suffixes_inner(&self, n: MeasIndex) -> RawOptTriples;

    fn datatype(&self) -> Option<NumType>;

    fn can_convert_to_temporal(&self, i: MeasIndex) -> MultiResult<(), OpticalToTemporalError>;
}

pub(crate) trait LookupOptical: Sized + VersionedOptical {
    fn lookup_specific(kws: &mut StdKeywords, n: MeasIndex) -> LookupResult<Self>;
}

pub trait VersionedTemporal: Sized {
    fn timestep(&self) -> Option<Timestep>;

    fn datatype(&self) -> Option<NumType>;

    fn set_timestep(&mut self, ts: Timestep);

    fn req_meta_keywords_inner(&self) -> RawPairs;

    fn opt_meas_keywords_inner(&self, _: MeasIndex) -> RawOptPairs;

    fn can_convert_to_optical(&self, i: MeasIndex) -> MultiResult<(), TemporalToOpticalError>;
}

pub(crate) trait LookupTemporal: VersionedTemporal {
    fn lookup_specific(kws: &mut StdKeywords, n: MeasIndex) -> LookupResult<Self>;
}

pub trait TemporalFromOptical<O: VersionedOptical>: Sized {
    type TData;

    fn from_optical(
        o: Optical<O>,
        i: MeasIndex,
        d: Self::TData,
        lossless: bool,
    ) -> PassthruResult<
        Temporal<Self>,
        Box<Optical<O>>,
        OpticalToTemporalError,
        OpticalToTemporalError,
    > {
        let opt_common_res = check_optical_keys_transfer(&o, i).mult_errors_into();
        let opt_specific_res = o.specific.can_convert_to_temporal(i).mult_errors_into();
        match opt_common_res.mult_zip(opt_specific_res) {
            Ok(_) => Ok(Tentative::new1(Self::from_optical_unchecked(o, d))),
            Err(es) => {
                if lossless {
                    Err(DeferredFailure::new(vec![], es, Box::new(o)))
                } else {
                    Ok(Tentative::new(
                        Self::from_optical_unchecked(o, d),
                        es.into(),
                        vec![],
                    ))
                }
            }
        }
    }

    fn from_optical_unchecked(o: Optical<O>, d: Self::TData) -> Temporal<Self> {
        Temporal {
            common: o.common,
            specific: Self::from_optical_inner(o.specific, d),
        }
    }

    fn from_optical_inner(o: O, d: Self::TData) -> Self;
}

pub trait OpticalFromTemporal<T: VersionedTemporal>: Sized {
    type TData;

    #[allow(clippy::type_complexity)]
    fn from_temporal(
        t: Temporal<T>,
        i: MeasIndex,
        lossless: bool,
    ) -> PassthruResult<
        (Optical<Self>, Self::TData),
        Box<Temporal<T>>,
        TemporalToOpticalError,
        TemporalToOpticalError,
    > {
        match t.specific.can_convert_to_optical(i) {
            Ok(()) => Ok(Tentative::new1(Self::from_temporal_unchecked(t))),
            Err(es) => {
                if lossless {
                    Err(DeferredFailure::new(vec![], es, Box::new(t)))
                } else {
                    Ok(Tentative::new(
                        Self::from_temporal_unchecked(t),
                        es.into(),
                        vec![],
                    ))
                }
            }
        }
    }

    fn from_temporal_unchecked(t: Temporal<T>) -> (Optical<Self>, Self::TData) {
        let (specific, td) = Self::from_temporal_inner(t.specific);
        let new = Optical {
            common: t.common,
            filter: None.into(),
            power: None.into(),
            detector_type: None.into(),
            percent_emitted: None.into(),
            detector_voltage: None.into(),
            specific,
        };
        (new, td)
    }

    fn from_temporal_inner(t: T) -> (Self, Self::TData);
}

impl CommonMeasurement {
    fn new(width: Width, range: Range) -> Self {
        Self {
            width,
            range,
            nonstandard_keywords: HashMap::new(),
            longname: None.into(),
        }
    }

    fn lookup(kws: &mut StdKeywords, i: MeasIndex, nonstd: NonStdPairs) -> LookupResult<Self> {
        lookup_indexed_opt(kws, i.into(), false).and_maybe(|longname| {
            let w = lookup_indexed_req(kws, i.into());
            let r = lookup_indexed_req(kws, i.into());
            w.def_zip(r).def_map_value(|(width, range)| Self {
                width,
                range,
                longname,
                nonstandard_keywords: nonstd.into_iter().collect(),
            })
        })
    }

    fn layout_data(&self) -> ColumnLayoutData<()> {
        ColumnLayoutData {
            width: self.width,
            range: self.range,
            datatype: (),
        }
    }
}

impl<T> Temporal<T>
where
    T: VersionedTemporal,
{
    fn new_common(width: Width, range: Range, specific: T) -> Self {
        Self {
            common: CommonMeasurement::new(width, range),
            specific,
        }
    }

    fn lookup_temporal(
        kws: &mut StdKeywords,
        i: MeasIndex,
        nonstd: NonStdPairs,
    ) -> LookupResult<Self>
    where
        T: LookupTemporal,
    {
        let c = CommonMeasurement::lookup(kws, i, nonstd);
        let t = T::lookup_specific(kws, i);
        c.def_zip(t)
            .def_map_value(|(common, specific)| Temporal { common, specific })
    }

    fn convert<ToT>(self, i: MeasIndex, force: bool) -> TemporalConvertTentative<Temporal<ToT>>
    where
        ToT: ConvertFromTemporal<T>,
    {
        ToT::convert_from_temporal(self.specific, i, force).map(|specific| Temporal {
            common: self.common,
            specific,
        })
    }

    fn req_meas_keywords(&self, n: MeasIndex) -> RawPairs {
        [
            self.common.width.pair(n.into()),
            self.common.range.pair(n.into()),
        ]
        .into_iter()
        .collect()
    }

    fn req_meta_keywords(&self) -> RawPairs {
        self.specific.req_meta_keywords_inner()
    }

    fn opt_meas_keywords(&self, i: MeasIndex) -> RawPairs {
        [OptIndexedKey::pair(&self.common.longname, i.into())]
            .into_iter()
            .chain(self.specific.opt_meas_keywords_inner(i))
            .flat_map(|(k, v)| v.map(|x| (k, x)))
            .collect()
    }
}

impl<P> Optical<P>
where
    P: VersionedOptical,
{
    pub fn width(&self) -> Width {
        self.common.width
    }

    pub fn range(&self) -> &Range {
        &self.common.range
    }

    fn new_common(width: Width, range: Range, specific: P) -> Self {
        Self {
            common: CommonMeasurement::new(width, range),
            specific,
            detector_type: None.into(),
            detector_voltage: None.into(),
            filter: None.into(),
            percent_emitted: None.into(),
            power: None.into(),
        }
    }

    fn try_convert<ToP: ConvertFromOptical<P>>(
        self,
        i: MeasIndex,
        force: bool,
    ) -> OpticalConvertResult<Optical<ToP>> {
        ToP::convert_from_optical(self.specific, i, force).def_map_value(|specific| Optical {
            common: self.common,
            detector_type: self.detector_type,
            detector_voltage: self.detector_voltage,
            filter: self.filter,
            power: self.power,
            percent_emitted: self.percent_emitted,
            specific,
        })
    }

    fn lookup_optical(
        kws: &mut StdKeywords,
        i: MeasIndex,
        nonstd: NonStdPairs,
    ) -> LookupResult<Self>
    where
        P: LookupOptical,
    {
        let version = P::fcs_version();
        let f = lookup_indexed_opt(kws, i.into(), false);
        let p = lookup_indexed_opt(kws, i.into(), false);
        let d = lookup_indexed_opt(kws, i.into(), false);
        let e = lookup_indexed_opt(kws, i.into(), version == Version::FCS3_2);
        let v = lookup_indexed_opt(kws, i.into(), false);
        f.zip5(p, d, e, v).and_maybe(
            |(filter, power, detector_type, percent_emitted, detector_voltage)| {
                let c = CommonMeasurement::lookup(kws, i, nonstd);
                let s = P::lookup_specific(kws, i);
                c.def_zip(s).def_map_value(|(common, specific)| Optical {
                    common,
                    filter,
                    power,
                    detector_type,
                    percent_emitted,
                    detector_voltage,
                    specific,
                })
            },
        )
    }

    fn req_keywords(&self, i: MeasIndex) -> RawTriples {
        [
            self.common.width.triple(i.into()),
            self.common.range.triple(i.into()),
        ]
        .into_iter()
        .chain(self.specific.req_suffixes_inner(i))
        .collect()
    }

    fn opt_keywords(&self, i: MeasIndex) -> RawOptTriples {
        [
            OptIndexedKey::triple(&self.common.longname, i.into()),
            OptIndexedKey::triple(&self.filter, i.into()),
            OptIndexedKey::triple(&self.power, i.into()),
            OptIndexedKey::triple(&self.detector_type, i.into()),
            OptIndexedKey::triple(&self.percent_emitted, i.into()),
            OptIndexedKey::triple(&self.detector_voltage, i.into()),
        ]
        .into_iter()
        .chain(self.specific.opt_suffixes_inner(i))
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

    fn table_row(&self, i: MeasIndex, n: Option<&Shortname>) -> Vec<String> {
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
    fn all_req_keywords(&self, n: MeasIndex) -> RawPairs {
        self.req_keywords(n)
            .into_iter()
            .map(|(_, k, v)| (k, v))
            .collect()
    }

    fn all_opt_keywords(&self, n: MeasIndex) -> RawPairs {
        self.opt_keywords(n)
            .into_iter()
            .filter_map(|(_, k, v)| v.map(|x| (k, x)))
            .chain(
                self.common
                    .nonstandard_keywords
                    .iter()
                    .map(|(k, v)| (k.as_ref().to_string(), v.clone())),
            )
            .collect()
    }
}

impl<N, P, T> Measurements<N, T, P>
where
    N: MightHave,
    T: VersionedTemporal,
    P: VersionedOptical,
{
    fn layout_data(&self) -> Vec<ColumnLayoutData<()>> {
        self.iter_common_values()
            .map(|(_, x)| x.layout_data())
            .collect()
    }
}

impl<M> Metaroot<M>
where
    M: VersionedMetaroot,
{
    /// Make new version-specific metaroot
    pub fn new(datatype: AlphaNumType, specific: M) -> Self {
        Metaroot {
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

    fn try_convert<ToM: ConvertFromMetaroot<M>>(
        self,
        convert: SizeConvert<M::D>,
        force: bool,
    ) -> MetarootConvertResult<Metaroot<ToM>> {
        // TODO this seems silly, break struct up into common bits
        ToM::convert_from_metaroot(self.specific, convert, force).def_map_value(|specific| {
            Metaroot {
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
            }
        })
    }

    fn lookup_metaroot(
        kws: &mut StdKeywords,
        ms: &Measurements<M::N, M::T, M::O>,
        nonstd: NonStdPairs,
    ) -> LookupResult<Self>
    where
        M: LookupMetaroot,
    {
        let par = Par(ms.len());
        let a = lookup_meta_opt(kws, false);
        let ce = lookup_meta_opt(kws, false);
        let co = lookup_meta_opt(kws, false);
        let e = lookup_meta_opt(kws, false);
        let f = lookup_meta_opt(kws, false);
        let i = lookup_meta_opt(kws, false);
        let l = lookup_meta_opt(kws, false);
        let o = lookup_meta_opt(kws, false);
        let p = lookup_meta_opt(kws, false);
        let sm = lookup_meta_opt(kws, false);
        let sr = lookup_meta_opt(kws, false);
        let sy = lookup_meta_opt(kws, false);
        let t = lookup_meta_opt(kws, false);
        a.zip5(ce, co, e, f)
            .zip5(i, l, o, p)
            .zip5(sm, sr, sy, t)
            .and_maybe(
                |(((abrt, com, cells, exp, fil), inst, lost, op, proj), smno, src, sys, tr)| {
                    let mut dt = lookup_meta_req(kws);
                    let s = M::lookup_specific(kws, par);
                    dt.def_eval_warning(|datatype| {
                        if *datatype == AlphaNumType::Ascii
                            && M::O::fcs_version() >= Version::FCS3_1
                        {
                            Some(DeprecatedError::Value(DepValueWarning::DatatypeASCII).into())
                        } else {
                            None
                        }
                    });
                    dt.def_zip(s)
                        .def_map_value(|(datatype, specific)| Metaroot {
                            datatype,
                            abrt,
                            com,
                            cells,
                            exp,
                            fil,
                            inst,
                            lost,
                            op,
                            proj,
                            smno,
                            src,
                            sys,
                            tr,
                            nonstandard_keywords: nonstd.into_iter().collect(),
                            specific,
                        })
                },
            )
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

    fn check_trigger(&self, names: &HashSet<&Shortname>) -> Result<(), LinkedNameError> {
        self.tr.0.as_ref().map_or(Ok(()), |tr| tr.check_link(names))
    }

    fn check_unstainedcenters(&self, names: &HashSet<&Shortname>) -> Result<(), LinkedNameError> {
        self.specific
            .as_unstainedcenters()
            .map_or(Ok(()), |x| x.check_link(names))
    }

    fn check_spillover(&self, names: &HashSet<&Shortname>) -> Result<(), LinkedNameError> {
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

    fn remove_name_index(&mut self, n: &Shortname, i: MeasIndex) {
        self.remove_trigger_by_name(n);
        let s = &mut self.specific;
        s.with_spillover(|x| x.remove_by_name(n));
        s.with_unstainedcenters(|u| u.remove(n));
        s.with_compensation(|c| c.remove_by_index(i));
    }
}

pub(crate) type RawPairs = Vec<(String, String)>;
pub(crate) type RawTriples = Vec<(String, String, String)>;
pub(crate) type RawOptPairs = Vec<(String, Option<String>)>;
pub(crate) type RawOptTriples = Vec<(String, String, Option<String>)>;

impl<M, T> From<Optical<M>> for Temporal<T>
where
    T: From<M>,
{
    fn from(value: Optical<M>) -> Self {
        Self {
            common: value.common,
            specific: value.specific.into(),
        }
    }
}

impl<M, T> From<Temporal<T>> for Optical<M>
where
    M: From<T>,
{
    fn from(value: Temporal<T>) -> Self {
        Self {
            common: value.common,
            detector_type: None.into(),
            detector_voltage: None.into(),
            filter: None.into(),
            percent_emitted: None.into(),
            power: None.into(),
            specific: value.specific.into(),
        }
    }
}

pub(crate) type Measurements<N, T, P> =
    NamedVec<N, <N as MightHave>::Wrapper<Shortname>, Temporal<T>, Optical<P>>;

pub(crate) type VersionedCoreTEXT<M> = CoreTEXT<
    M,
    <M as VersionedMetaroot>::T,
    <M as VersionedMetaroot>::O,
    <M as VersionedMetaroot>::N,
    <<M as VersionedMetaroot>::N as MightHave>::Wrapper<Shortname>,
>;

pub(crate) type VersionedCoreDataset<M> = CoreDataset<
    M,
    <M as VersionedMetaroot>::T,
    <M as VersionedMetaroot>::O,
    <M as VersionedMetaroot>::N,
    <<M as VersionedMetaroot>::N as MightHave>::Wrapper<Shortname>,
>;

pub(crate) type VersionedCore<A, D, M> = Core<
    A,
    D,
    M,
    <M as VersionedMetaroot>::T,
    <M as VersionedMetaroot>::O,
    <M as VersionedMetaroot>::N,
    <<M as VersionedMetaroot>::N as MightHave>::Wrapper<Shortname>,
>;

pub(crate) type VersionedConvertError<N, ToN> = ConvertError<
    <<ToN as MightHave>::Wrapper<Shortname> as TryFrom<
        <N as MightHave>::Wrapper<Shortname>,
    >>::Error,
>;

macro_rules! non_time_get_set {
    ($get:ident, $set:ident, $ty:ident, [$($root:ident)*], $field:ident, $kw:ident) => {
        /// Get $$kw value for optical measurements
        pub fn $get(&self) -> Vec<(MeasIndex, Option<&$ty>)> {
            self.measurements
                .iter_non_center_values()
                .map(|(i, m)| (i, m.$($root.)*$field.as_ref_opt()))
                .collect()
        }

        /// Set $$kw value for for optical measurements
        pub fn $set(&mut self, xs: Vec<Option<$ty>>) -> Result<(), KeyLengthError> {
            self.measurements.alter_non_center_values_zip(xs, |m, x| {
                m.$($root.)*$field = x.into();
            }).map(|_| ())
        }
    };
}

impl<M, A, D> VersionedCore<A, D, M>
where
    M: VersionedMetaroot,
    M::N: Clone,
{
    /// Write this structure to a handle.
    ///
    /// The actual bytes written will be the HEADER and TEXT, including the
    /// last delimiter.
    pub(crate) fn h_write_text<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        tot: Tot,
        data_len: usize,
        analysis_len: usize,
        conf: &WriteConfig,
    ) -> Result<(), ImpureError<TEXTOverflowError>> {
        // TODO newtypes for data and analysis lenth to make them more obvious
        if let Some(ts) = self.text_segment(tot, data_len, analysis_len) {
            for t in ts {
                h.write_all(t.as_bytes())?;
                h.write_all(&[conf.delim.inner()])?;
            }
            Ok(())
        } else {
            let te = ImpureError::Pure(TEXTOverflowError);
            Err(te)
        }
    }

    pub(crate) fn try_cols_to_dataframe(
        &self,
        cols: Vec<AnyFCSColumn>,
    ) -> Result<FCSDataFrame, ColumsnToDataframeError> {
        let data_n = cols.len();
        let meas_n = self.par().0;
        if data_n != meas_n {
            return Err(MeasDataMismatchError { meas_n, data_n }.into());
        }
        let df = FCSDataFrame::try_new(cols)?;
        Ok(df)
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
            // TODO do something useful with nextdata offset (the "_" thing)
            .map(|(header, kws, _)| {
                let version = M::O::fcs_version();
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
        self.metaroot.tr.as_ref_opt().map(|x| &x.measurement)
    }

    /// Get threshold for $TR keyword
    pub fn trigger_threshold(&self) -> Option<u32> {
        self.metaroot.tr.as_ref_opt().map(|x| x.threshold)
    }

    /// Set measurement name for $TR keyword.
    ///
    /// Return true if trigger exists and was renamed, false otherwise.
    pub fn set_trigger_name(&mut self, n: Shortname) -> bool {
        if !self.measurement_names().contains(&n) {
            return false;
        }
        if let Some(tr) = self.metaroot.tr.0.as_mut() {
            tr.measurement = n;
        } else {
            self.metaroot.tr = Some(Trigger {
                measurement: n,
                threshold: 0,
            })
            .into();
        }
        true
    }

    /// Set threshold for $TR keyword
    ///
    /// Return true if trigger exists, false otherwise.
    pub fn set_trigger_threshold(&mut self, x: u32) -> bool {
        if let Some(tr) = self.metaroot.tr.0.as_mut() {
            tr.threshold = x;
            true
        } else {
            false
        }
    }

    /// Remove $TR keyword
    ///
    /// Return true if trigger existed and was removed, false if it did not
    /// already exist.
    pub fn clear_trigger(&mut self) -> bool {
        let ret = self.metaroot.tr.0.is_some();
        self.metaroot.tr = None.into();
        ret
    }

    /// Return a list of measurement names as stored in $PnN.
    pub fn shortnames_maybe(&self) -> Vec<Option<&Shortname>> {
        self.measurements
            .iter()
            .map(|(_, x)| x.both(|t| Some(&t.key), |m| M::N::as_opt(&m.key)))
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
    pub fn set_all_shortnames(&mut self, ns: Vec<Shortname>) -> Result<NameMapping, SetKeysError> {
        let mapping = self.measurements.set_names(ns)?;
        self.metaroot.reassign_all(&mapping);
        Ok(mapping)
    }

    /// Set the measurement matching given name to be the time measurement.
    pub fn set_temporal(
        &mut self,
        n: &Shortname,
        timestep: <M::T as TemporalFromOptical<M::O>>::TData,
        force: bool,
    ) -> DeferredResult<bool, SetTemporalError, SetTemporalError>
    where
        M::T: TemporalFromOptical<M::O>,
    {
        let lossless = !force;
        self.measurements.set_center_by_name(
            n,
            |i, old_o, old_t| M::swap_optical_temporal(old_o, old_t, i, lossless).def_inner_into(),
            |i, old_o| {
                <M::T as TemporalFromOptical<M::O>>::from_optical(old_o, i, timestep, lossless)
                    .def_inner_into()
            },
        )
    }

    /// Set the measurement at given index to the time measurement.
    pub fn set_temporal_at(
        &mut self,
        index: MeasIndex,
        timestep: <M::T as TemporalFromOptical<M::O>>::TData,
        force: bool,
    ) -> DeferredResult<bool, SetTemporalError, SetTemporalError>
    where
        M::T: TemporalFromOptical<M::O>,
    {
        let lossless = !force;
        self.measurements.set_center_by_index(
            index,
            |i, old_o, old_t| M::swap_optical_temporal(old_o, old_t, i, lossless).def_inner_into(),
            |i, old_o| {
                <M::T as TemporalFromOptical<M::O>>::from_optical(old_o, i, timestep, lossless)
                    .def_inner_into()
            },
        )
    }

    /// Convert time measurement to optical measurement.
    ///
    /// Return true if a time measurement existed and was converted, false
    /// otherwise.
    #[allow(clippy::type_complexity)]
    pub fn unset_temporal(
        &mut self,
        force: bool,
    ) -> Tentative<
        Option<<M::O as OpticalFromTemporal<M::T>>::TData>,
        TemporalToOpticalError,
        TemporalToOpticalError,
    >
    where
        M::O: OpticalFromTemporal<M::T>,
    {
        self.measurements.unset_center(|i, old_t| {
            <M::O as OpticalFromTemporal<M::T>>::from_temporal(old_t, i, !force)
        })
    }

    /// Insert a nonstandard key/value pair for each measurement.
    ///
    /// Return a vector of elements corresponding to each measurement, where
    /// each element is the value of the inserted key if already present.
    ///
    /// This includes the time measurement if present.
    pub fn insert_meas_nonstandard(
        &mut self,
        xs: Vec<(NonStdKey, String)>,
    ) -> Result<Vec<Option<String>>, KeyLengthError> {
        self.measurements
            .alter_common_values_zip(xs, |_, x, (k, v)| x.nonstandard_keywords.insert(k, v))
    }

    /// Remove a key from nonstandard key/value pairs for each measurement.
    ///
    /// Return a vector with removed values for each measurement if present.
    ///
    /// This includes the time measurement if present.
    pub fn remove_meas_nonstandard(
        &mut self,
        xs: Vec<&NonStdKey>,
    ) -> Result<Vec<Option<String>>, KeyLengthError> {
        self.measurements
            .alter_common_values_zip(xs, |_, x, k| x.nonstandard_keywords.remove(k))
    }

    /// Read a key from nonstandard key/value pairs for each measurement.
    ///
    /// Return a vector with each successfully found value.
    ///
    /// This includes the time measurement if present.
    pub fn get_meas_nonstandard(&self, ks: &[NonStdKey]) -> Option<Vec<Option<&String>>> {
        let ms = &self.measurements;
        if ks.len() != ms.len() {
            None
        } else {
            let res = ms
                .iter_common_values()
                .zip(ks)
                .map(|((_, x), k)| x.nonstandard_keywords.get(k))
                .collect();
            Some(res)
        }
    }

    /// Return read-only reference to measurement vector
    pub fn measurements_named_vec(&self) -> &Measurements<M::N, M::T, M::O> {
        &self.measurements
    }

    /// Replace optical measurement at index.
    ///
    /// If index points to a temporal measurement, replace it with the given
    /// optical measurement. In both cases the name is kept. Return the
    /// measurement that was replaced if the index was in bounds.
    #[allow(clippy::type_complexity)]
    pub fn replace_optical_at(
        &mut self,
        index: MeasIndex,
        m: Optical<M::O>,
    ) -> Result<Element<Temporal<M::T>, Optical<M::O>>, ElementIndexError> {
        self.measurements.replace_at(index, m)
    }

    /// Replace optical measurement with name.
    ///
    /// If name refers to a temporal measurement, replace it with the given
    /// optical measurement. Return the measurement that was replaced if the
    /// index was in bounds.
    #[allow(clippy::type_complexity)]
    pub fn replace_optical_named(
        &mut self,
        name: &Shortname,
        m: Optical<M::O>,
    ) -> Option<Element<Temporal<M::T>, Optical<M::O>>> {
        self.measurements.replace_named(name, m)
    }

    /// Replace temporal measurement at index.
    #[allow(clippy::type_complexity)]
    pub fn replace_temporal_at(
        &mut self,
        index: MeasIndex,
        m: Temporal<M::T>,
        force: bool,
    ) -> DeferredResult<
        Element<Temporal<M::T>, Optical<M::O>>,
        ReplaceTemporalError,
        ReplaceTemporalError,
    >
    where
        M::O: OpticalFromTemporal<M::T>,
    {
        self.measurements.replace_center_at(index, m, |i, old_t| {
            <M::O as OpticalFromTemporal<M::T>>::from_temporal(old_t, i, !force)
                .def_inner_into()
                .def_map_value(|(x, _)| x)
        })
    }

    /// Replace temporal measurement at index.
    #[allow(clippy::type_complexity)]
    pub fn replace_temporal_named(
        &mut self,
        name: &Shortname,
        m: Temporal<M::T>,
        force: bool,
    ) -> DeferredResult<
        Option<Element<Temporal<M::T>, Optical<M::O>>>,
        ReplaceTemporalError,
        ReplaceTemporalError,
    >
    where
        M::O: OpticalFromTemporal<M::T>,
    {
        self.measurements
            .replace_center_by_name(name, m, |i, old_t| {
                <M::O as OpticalFromTemporal<M::T>>::from_temporal(old_t, i, !force)
                    .def_inner_into()
                    .def_map_value(|(x, _)| x)
            })
    }

    /// Rename a measurement
    ///
    /// If index points to the center element and the wrapped name contains
    /// nothing, the default name will be assigned. Return error if index is
    /// out of bounds or name is not unique. Return pair of old and new name
    /// on success.
    pub fn rename_measurement(
        &mut self,
        index: MeasIndex,
        key: <M::N as MightHave>::Wrapper<Shortname>,
    ) -> Result<(Shortname, Shortname), RenameError> {
        self.measurements.rename(index, key).map(|(old, new)| {
            let mapping = [(old.clone(), new.clone())].into_iter().collect();
            self.metaroot.reassign_all(&mapping);
            (old, new)
        })
    }

    /// Rename time measurement if it exists
    pub fn rename_temporal(&mut self, name: Shortname) -> Option<Shortname> {
        self.measurements.rename_center(name)
    }

    /// Apply functions to measurement values
    pub fn alter_measurements<F, G, R>(&mut self, f: F, g: G) -> Vec<R>
    where
        F: Fn(IndexedElement<&<M::N as MightHave>::Wrapper<Shortname>, &mut Optical<M::O>>) -> R,
        G: Fn(IndexedElement<&Shortname, &mut Temporal<M::T>>) -> R,
    {
        self.measurements.alter_values(f, g)
    }

    /// Apply functions to measurement values with payload
    pub fn alter_measurements_zip<F, G, X, R>(
        &mut self,
        xs: Vec<X>,
        f: F,
        g: G,
    ) -> Result<Vec<R>, KeyLengthError>
    where
        F: Fn(IndexedElement<&<M::N as MightHave>::Wrapper<Shortname>, &mut Optical<M::O>>, X) -> R,
        G: Fn(IndexedElement<&Shortname, &mut Temporal<M::T>>, X) -> R,
    {
        self.measurements.alter_values_zip(xs, f, g)
    }

    /// Return mutable reference to time measurement as a name/value pair.
    pub fn temporal_mut(&mut self) -> Option<IndexedElement<&mut Shortname, &mut Temporal<M::T>>> {
        self.measurements.as_center_mut()
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
            .iter_common_values()
            .map(|(_, x)| x.longname.as_ref_opt())
            .collect()
    }

    /// Set all $PnS keywords to list of names.
    ///
    /// Will return false if length of supplied list does not match length
    /// of measurements; true otherwise. Since $PnS is an optional keyword for
    /// all versions, any name in the list may be None which will blank the
    /// keyword.
    pub fn set_longnames(&mut self, ns: Vec<Option<String>>) -> Result<(), KeyLengthError> {
        self.measurements
            .alter_common_values_zip(ns, |_, x, n| x.longname = n.map(Longname).into())
            .map(|_| ())
    }

    /// Show $PnB for each measurement
    pub fn widths(&self) -> Vec<Width> {
        self.measurements
            .iter_common_values()
            .map(|(_, x)| x.width)
            .collect()
    }

    /// Show $PnR for each measurement
    pub fn ranges(&self) -> Vec<Range> {
        self.measurements
            .iter_common_values()
            .map(|(_, x)| x.range)
            .collect()
    }

    /// Return $PAR, which is simply the number of measurements in this struct
    pub fn par(&self) -> Par {
        Par(self.measurements.len())
    }

    /// Convert to another FCS version.
    ///
    /// Conversion may fail if some required keywords in the target version
    /// are not present in current version.
    #[allow(clippy::type_complexity)]
    pub fn try_convert<ToM>(
        self,
        force: bool,
    ) -> DeferredResult<
        VersionedCore<A, D, ToM>,
        MetarootConvertWarning,
        VersionedConvertError<M::N, ToM::N>,
    >
    where
        M::N: Clone,
        ToM: VersionedMetaroot,
        ToM: ConvertFromMetaroot<M>,
        ToM::O: VersionedOptical,
        ToM::T: VersionedTemporal,
        ToM::N: MightHave,
        ToM::N: Clone,
        ToM::O: ConvertFromOptical<M::O>,
        ToM::T: ConvertFromTemporal<M::T>,
        <ToM::N as MightHave>::Wrapper<Shortname>: TryFrom<<M::N as MightHave>::Wrapper<Shortname>>,
    {
        let widths = self.widths();
        let convert = SizeConvert {
            widths,
            datatype: self.metaroot.datatype,
            size: self.metaroot.specific.byteord(),
        };
        let m = self
            .metaroot
            .try_convert(convert, force)
            .def_map_errors(ConvertErrorInner::Meta);
        let ps = self
            .measurements
            .map_center_value(|y| Ok(y.value.convert(y.index, force)))
            .def_map_errors(ConvertErrorInner::Temporal)
            .def_warnings_into()
            .def_and_maybe(|xs| {
                xs.map_non_center_values(|i, v| v.try_convert(i, force))
                    .def_map_errors(ConvertErrorInner::Optical)
                    .def_warnings_into()
            })
            .def_and_maybe(|x| {
                x.try_rewrapped()
                    .mult_map_errors(ConvertErrorInner::Rewrap)
                    .mult_to_deferred()
            });
        m.def_zip(ps)
            .def_map_value(|(metaroot, measurements)| Core {
                metaroot,
                measurements,
                data: self.data,
                analysis: self.analysis,
            })
            .def_map_errors(|error| ConvertError {
                from: M::O::fcs_version(),
                to: ToM::O::fcs_version(),
                inner: error,
            })
    }

    #[allow(clippy::type_complexity)]
    fn remove_measurement_by_name_inner(
        &mut self,
        n: &Shortname,
    ) -> Option<(MeasIndex, Element<Temporal<M::T>, Optical<M::O>>)> {
        if let Some(e) = self.measurements.remove_name(n) {
            self.metaroot.remove_name_index(n, e.0);
            Some(e)
        } else {
            None
        }
    }

    #[allow(clippy::type_complexity)]
    fn remove_measurement_by_index_inner(
        &mut self,
        index: MeasIndex,
    ) -> Result<EitherPair<M::N, Temporal<M::T>, Optical<M::O>>, ElementIndexError> {
        let res = self.measurements.remove_index(index)?;
        if let Element::NonCenter(left) = &res {
            if let Some(n) = M::N::as_opt(&left.key) {
                self.metaroot.remove_name_index(n, index);
            }
        }
        Ok(res)
    }

    fn check_existing_links(&mut self) -> Result<(), ExistingLinkError> {
        if self.trigger_name().is_some() {
            return Err(ExistingLinkError::Trigger);
        }
        let m = &self.metaroot;
        let s = &m.specific;
        if s.as_unstainedcenters().is_some() {
            return Err(ExistingLinkError::UnstainedCenters);
        }
        if s.as_compensation().is_some() {
            return Err(ExistingLinkError::Comp);
        }
        if s.as_spillover().is_some() {
            return Err(ExistingLinkError::Spillover);
        }
        Ok(())
    }

    fn set_measurements_inner(
        &mut self,
        xs: RawInput<M::N, Temporal<M::T>, Optical<M::O>>,
        prefix: ShortnamePrefix,
    ) -> Result<(), SetMeasurementsError> {
        self.check_existing_links()?;
        let ms = NamedVec::try_new(xs, prefix)?;
        self.measurements = ms;
        Ok(())
    }

    fn unset_measurements_inner(&mut self) -> Result<(), ExistingLinkError> {
        self.check_existing_links()?;
        self.measurements = NamedVec::default();
        Ok(())
    }

    fn header_and_raw_keywords(
        &self,
        tot: Tot,
        data_len: usize,
        analysis_len: usize,
    ) -> Option<(String, RawKeywords, usize)> {
        let version = M::O::fcs_version();
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
            offset_result.real_nextdata,
        ))
    }

    fn meta_meas_keywords<F, G, H, I>(
        &self,
        f_meas: F,
        f_time_meas: G,
        f_time_meta: H,
        f_meta: I,
    ) -> (RawPairs, RawPairs)
    where
        F: Fn(&Optical<M::O>, MeasIndex) -> RawPairs,
        G: Fn(&Temporal<M::T>, MeasIndex) -> RawPairs,
        H: Fn(&Temporal<M::T>) -> RawPairs,
        I: Fn(&Metaroot<M>, Par) -> RawPairs,
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
        let meta: Vec<_> = f_meta(&self.metaroot, self.par()).into_iter().collect();
        let all_meas: Vec<_> = meas.into_iter().chain(time_meas).collect();
        let all_meta: Vec<_> = meta.into_iter().chain(time_meta).collect();
        (all_meta, all_meas)
    }

    fn req_meta_meas_keywords(&self) -> (RawPairs, RawPairs, usize) {
        let (meta, mut meas) = self.meta_meas_keywords(
            Optical::all_req_keywords,
            Temporal::req_meas_keywords,
            Temporal::req_meta_keywords,
            Metaroot::all_req_keywords,
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
            Optical::all_opt_keywords,
            Temporal::opt_meas_keywords,
            |_| vec![],
            |s, _| Metaroot::all_opt_keywords(s),
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
            .map(|(i, n)| ReqIndexedKey::pair(n, i.into()))
            .collect()
    }

    fn meas_table(&self, delim: &str) -> Vec<String>
    where
        M::T: Clone,
        M::O: OpticalFromTemporal<M::T>,
    {
        let ms = &self.measurements;
        if let Some(m0) = ms.get(0.into()).ok().and_then(|x| x.non_center()) {
            let header = m0.1.table_header();
            let rows = self.measurements.iter().map(|(i, r)| {
                r.both(
                    |t| {
                        // NOTE this will force-convert all fields in the time
                        // measurement, which for this is actually want we want
                        <M::O as OpticalFromTemporal<M::T>>::from_temporal_unchecked(
                            t.value.clone(),
                        )
                        .0
                        .table_row(i, Some(&t.key))
                    },
                    |o| o.value.table_row(i, M::N::as_opt(&o.key)),
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
        M::O: OpticalFromTemporal<M::T>,
    {
        for e in self.meas_table(delim) {
            println!("{}", e);
        }
    }

    #[allow(clippy::type_complexity)]
    fn lookup_measurements(
        kws: &mut StdKeywords,
        par: Par,
        time_pat: Option<&TimePattern>,
        prefix: &ShortnamePrefix,
        ns_pat: Option<&NonStdMeasPattern>,
        nonstd: NonStdPairs,
    ) -> DeferredResult<
        (Measurements<M::N, M::T, M::O>, NonStdPairs),
        LookupMeasWarning,
        LookupKeysError,
    >
    where
        M: LookupMetaroot,
        M::T: LookupTemporal,
        M::O: LookupOptical,
    {
        // Use nonstandard measurement pattern to assign keyvals to their
        // measurement if they match. Only capture one warning because if the
        // pattern is wrong for one measurement it is probably wrong for all of
        // them.
        let tnt = if let Some(pat) = ns_pat {
            let res = (0..par.0)
                .map(|n| pat.from_index(n.into()))
                .collect::<Result<Vec<_>, _>>();
            match res {
                Ok(ps) => {
                    let mut meta_nonstd = vec![];
                    let mut meas_nonstds = vec![vec![]; par.0];
                    for (k, v) in nonstd {
                        if let Some(j) = ps.iter().position(|p| p.is_match(k.as_ref())) {
                            meas_nonstds[j].push((k, v));
                        } else {
                            meta_nonstd.push((k, v));
                        }
                    }
                    Tentative::new1((meta_nonstd, meas_nonstds))
                }
                Err(w) => Tentative::new((nonstd, vec![vec![]; par.0]), vec![w.into()], vec![]),
            }
        } else {
            Tentative::new1((nonstd, vec![vec![]; par.0]))
        };

        // then iterate over each measurement and look for standardized keys
        tnt.and_maybe(|(meta_nonstd, meas_nonstds)| {
            meas_nonstds
                .into_iter()
                .enumerate()
                .map(|(n, meas_nonstd)| {
                    let i = n.into();
                    // Try to find $PnN first, for later versions this will
                    // totally fail if not found since this is required. If it
                    // does exist, also check if it matches the time pattern and
                    // use it as the time measurement if it does.
                    M::lookup_shortname(kws, i).def_and_maybe(|wrapped| {
                        // TODO if more than one name matches the time pattern
                        // this will give a cryptic "cannot find $TIMESTEP" for
                        // each subsequent match, which is not helpful. Probably
                        // the best way around this is to add measurement index
                        // and possibly key to the error, so at least the user
                        // will know it is trying to find $TIMESTEP in a
                        // nonsense measurement.
                        let key = M::N::unwrap(wrapped).and_then(|name| {
                            if let Some(tp) = time_pat {
                                if tp.0.as_inner().is_match(name.as_ref()) {
                                    return Ok(name);
                                }
                            }
                            Err(M::N::wrap(name))
                        });
                        // Once we checked $PnN, pull all the rest of the
                        // standardized keywords from the hashtable and collect
                        // errors. In general, required keywords will trigger an
                        // error if they are missing and optional keywords will
                        // trigger a warning. Either can generate an
                        // error/warning if they fail to be parsed to their type
                        match key {
                            // TODO add switch to "downgrade" failed time
                            // channel to optical channel, which is more general
                            Ok(name) => Temporal::lookup_temporal(kws, i, meas_nonstd)
                                .def_map_value(|t| Element::Center((name, t))),
                            Err(k) => Optical::lookup_optical(kws, i, meas_nonstd)
                                .def_map_value(|m| Element::NonCenter((k, m))),
                        }
                    })
                })
                .gather()
                .map_err(DeferredFailure::mconcat)
                .map(Tentative::mconcat)
                .def_and_maybe(|xs| {
                    // Finally, attempt to put our proto-measurement binary soup
                    // into a named vector, which will have a special element
                    // for the time measurement if it exists, and will scream if
                    // we have more than one time measurement.
                    NamedVec::try_new(xs, prefix.clone())
                        .map(|ms| (ms, meta_nonstd))
                        .map_err(|e| LookupKeysError::Misc(e.into()))
                        .into_deferred()
                })
                .def_warnings_into()
        })
    }

    fn measurement_names(&self) -> HashSet<&Shortname> {
        self.measurements.indexed_names().map(|(_, x)| x).collect()
    }

    fn check_linked_names(&self) -> MultiResult<(), LinkedNameError> {
        let mut errs = vec![];
        let names = self.measurement_names();

        if let Err(e) = self.metaroot.check_trigger(&names) {
            errs.push(e);
        }

        if let Err(e) = self.metaroot.check_unstainedcenters(&names) {
            errs.push(e);
        }

        if let Err(e) = self.metaroot.check_spillover(&names) {
            errs.push(e);
        }

        NonEmpty::from_vec(errs).map_or(Ok(()), Err)
    }

    fn set_data_width_range(&mut self, xs: Vec<(Width, Range)>) -> Result<(), KeyLengthError> {
        self.measurements
            .alter_common_values_zip(xs, |_, x, (b, r)| {
                x.width = b;
                x.range = r;
            })
            .map(|_| ())
    }

    fn set_data_delimited_inner(&mut self, xs: Vec<u64>) -> Result<(), KeyLengthError> {
        let ys: Vec<_> = xs
            .into_iter()
            .map(|r| (Width::Variable, r.into()))
            .collect();
        self.set_data_width_range(ys)
    }

    fn set_to_floating_point(
        &mut self,
        is_double: bool,
        rs: Vec<Range>,
    ) -> Result<(), KeyLengthError> {
        let (dt, b) = if is_double {
            (AlphaNumType::Double, Width::new_f64())
        } else {
            (AlphaNumType::Single, Width::new_f32())
        };
        let xs: Vec<_> = rs.into_iter().map(|r| (b, r)).collect();
        // ASSUME time measurement will always be set to linear since we do that
        // a few lines above, so the only error/warning we need to screen is
        // for the length of the input
        self.set_data_width_range(xs)?;
        self.metaroot.datatype = dt;
        Ok(())
    }

    fn set_data_ascii_inner(&mut self, xs: Vec<AsciiRangeSetter>) -> Result<(), KeyLengthError> {
        let ys: Vec<_> = xs.into_iter().map(|s| s.truncated()).collect();
        self.set_data_width_range(ys)?;
        self.metaroot.datatype = AlphaNumType::Ascii;
        Ok(())
    }

    pub fn set_data_integer_inner(
        &mut self,
        xs: Vec<NumRangeSetter>,
    ) -> Result<(), KeyLengthError> {
        let ys: Vec<_> = xs.into_iter().map(|s| s.truncated()).collect();
        self.set_data_width_range(ys)?;
        self.metaroot.datatype = AlphaNumType::Integer;
        Ok(())
    }

    pub(crate) fn as_data_layout(
        &self,
        conf: &SharedConfig,
    ) -> DeferredResult<M::L, NewDataLayoutWarning, NewDataLayoutError> {
        M::as_data_layout(&self.metaroot, &self.measurements, conf)
    }
}

impl<M> VersionedCoreTEXT<M>
where
    M: VersionedMetaroot,
    M::N: Clone,
{
    /// Make a new CoreTEXT from raw keywords.
    ///
    /// Return any errors encountered, including messing required keywords,
    /// parse errors, and/or deprecation warnings.
    pub(crate) fn new_text_from_raw(
        kws: &mut StdKeywords,
        nonstd: NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> DeferredResult<Self, LookupMeasWarning, LookupKeysError>
    where
        M: LookupMetaroot,
        M::T: LookupTemporal,
        M::O: LookupOptical,
    {
        // Lookup $PAR first since we need this to get the measurements
        Par::remove_meta_req(kws)
            .map_err(|e| Box::new(e.inner_into()))
            .into_deferred()
            .def_and_maybe(|par| {
                // $NEXTDATA/$BEGINSTEXT/$ENDSTEXT should have already been
                // processed when we read the TEXT; remove them so they don't
                // trigger false positives later when we test for deviant keys
                let _ = kws.remove(&Nextdata::std());
                let _ = kws.remove(&Beginstext::std());
                let _ = kws.remove(&Endstext::std());

                // Lookup measurements and metaroot with $PAR
                let tp = conf.time.pattern.as_ref();
                let sp = &conf.shortname_prefix;
                let nsp = conf.nonstandard_measurement_pattern.as_ref();
                let ns: Vec<_> = nonstd.into_iter().collect();
                let mut tnt_core = Self::lookup_measurements(kws, par, tp, sp, nsp, ns)
                    .def_and_maybe(|(ms, meta_ns)| {
                        Metaroot::lookup_metaroot(kws, &ms, meta_ns)
                            .def_map_value(|metaroot| CoreTEXT::new_unchecked(metaroot, ms))
                            .def_warnings_into()
                    })?;

                // Check that the time measurement is present if we want it
                tnt_core.eval_error(|core| {
                    if let Some(pat) = tp {
                        if conf.time.ensure && core.measurements.as_center().is_none() {
                            return Some(LookupKeysError::Misc(MissingTime(pat.clone()).into()));
                        }
                    }
                    None
                });

                // At this point the only keywords that should be left are $TOT,
                // $BEGINDATA, $ENDDATA, $BEGINANALYSIS, and $ENDANALYSIS.
                // $TIMESTEP might also be present if it wasn't used for the
                // time measurement. Make sure this is actually true
                if kws.keys().any(|k| {
                    !(k == &Begindata::std()
                        || k == &Enddata::std()
                        || k == &Beginanalysis::std()
                        || k == &Endanalysis::std()
                        || k == &Tot::std()
                        || k == &Timestep::std())
                }) {
                    if conf.disallow_deviant {
                        tnt_core.push_error(DeviantError.into());
                    } else {
                        tnt_core.push_warning(DeviantError.into());
                    }
                }

                // make sure keywords which refer to $PnN are valid, if not then this
                // fails because the API assumes these are valid and provides no way
                // to fix otherwise.
                tnt_core.and_maybe(|core| {
                    core.check_linked_names()
                        .mult_to_deferred()
                        .def_map_value(|_| core)
                })
            })
    }

    /// Remove a measurement matching the given name.
    ///
    /// Return removed measurement and its index if found.
    #[allow(clippy::type_complexity)]
    pub fn remove_measurement_by_name(
        &mut self,
        n: &Shortname,
    ) -> Option<(MeasIndex, Element<Temporal<M::T>, Optical<M::O>>)> {
        self.remove_measurement_by_name_inner(n)
    }

    /// Remove a measurement at a given position
    ///
    /// Return removed measurement and its name if found.
    #[allow(clippy::type_complexity)]
    pub fn remove_measurement_by_index(
        &mut self,
        index: MeasIndex,
    ) -> Result<EitherPair<M::N, Temporal<M::T>, Optical<M::O>>, ElementIndexError> {
        self.remove_measurement_by_index_inner(index)
    }

    /// Add time measurement to the end of the measurement vector.
    ///
    /// Return error if time measurement already exists or name is non-unique.
    pub fn push_temporal(
        &mut self,
        n: Shortname,
        m: Temporal<M::T>,
    ) -> Result<(), InsertCenterError> {
        self.measurements.push_center(n, m)
    }

    /// Add time measurement at the given position
    ///
    /// Return error if time measurement already exists, name is non-unique, or
    /// index is out of bounds.
    pub fn insert_temporal(
        &mut self,
        i: MeasIndex,
        n: Shortname,
        m: Temporal<M::T>,
    ) -> Result<(), InsertCenterError> {
        self.measurements.insert_center(i, n, m)
    }

    /// Add optical measurement to the end of the measurement vector
    ///
    /// Return error if name is non-unique.
    pub fn push_optical(
        &mut self,
        n: <M::N as MightHave>::Wrapper<Shortname>,
        m: Optical<M::O>,
    ) -> Result<Shortname, NonUniqueKeyError> {
        self.measurements.push(n, m)
    }

    /// Add optical measurement at a given position
    ///
    /// Return error if name is non-unique, or index is out of bounds.
    pub fn insert_optical(
        &mut self,
        i: MeasIndex,
        n: <M::N as MightHave>::Wrapper<Shortname>,
        m: Optical<M::O>,
    ) -> Result<Shortname, InsertError> {
        self.measurements.insert(i, n, m)
    }

    /// Remove measurements
    pub fn unset_measurements(&mut self) -> Result<(), ExistingLinkError> {
        self.unset_measurements_inner()
    }

    /// Make new CoreDataset from CoreTEXT with supplied DATA and ANALYSIS
    ///
    /// Number of columns must match number of measurements and must all be the
    /// same length.
    pub fn into_coredataset(
        self,
        columns: Vec<AnyFCSColumn>,
        analysis: Analysis,
    ) -> Result<VersionedCoreDataset<M>, ColumsnToDataframeError> {
        let data = self.try_cols_to_dataframe(columns)?;
        Ok(self.into_coredataset_unchecked(data, analysis))
    }

    pub(crate) fn into_coredataset_unchecked(
        self,
        data: FCSDataFrame,
        analysis: Analysis,
    ) -> VersionedCoreDataset<M> {
        CoreDataset {
            metaroot: self.metaroot,
            measurements: self.measurements,
            data,
            analysis,
        }
    }
}

impl<M> VersionedCoreDataset<M>
where
    M: VersionedMetaroot,
    M::N: Clone,
    M::L: VersionedDataLayout,
{
    pub(crate) fn new_dataset_from_raw<R: Read + Seek>(
        h: &mut BufReader<R>,
        kws: &mut StdKeywords,
        nonstd: NonStdKeywords,
        data_seg: HeaderDataSegment,
        analysis_seg: HeaderAnalysisSegment,
        conf: &DataReadConfig,
        // TODO wrap this in a nice struct
    ) -> IODeferredResult<
        (Self, AnyDataSegment, AnyAnalysisSegment),
        StdDatasetFromRawWarning,
        StdDatasetFromRawError,
    >
    where
        M: LookupMetaroot,
        M::T: LookupTemporal,
        M::O: LookupOptical,
    {
        CoreTEXT::new_text_from_raw(kws, nonstd, &conf.standard)
            .def_inner_into()
            .def_errors_liftio()
            .def_and_maybe(|text| {
                text.as_data_layout(&conf.shared)
                    .def_inner_into()
                    .def_errors_liftio()
                    .def_and_maybe(|layout: M::L| {
                        let data_res = layout
                            .into_data_reader(kws, data_seg, &conf.reader)
                            .def_inner_into()
                            .def_errors_liftio();
                        let analysis_res =
                            M::L::as_analysis_reader(kws, analysis_seg, &conf.reader)
                                .def_inner_into()
                                .def_errors_liftio();
                        data_res.def_zip(analysis_res).def_and_maybe(|(dr, ar)| {
                            h_read_data_and_analysis(h, dr, ar)
                                .map(|(data, analysis, d_seg, a_seg)| {
                                    let c = Core {
                                        metaroot: text.metaroot,
                                        measurements: text.measurements,
                                        data,
                                        analysis,
                                    };
                                    (c, d_seg, a_seg)
                                })
                                .into_deferred::<_, StdDatasetFromRawWarning>()
                                .def_io_into()
                        })
                    })
            })
    }

    /// Write this dataset (HEADER+TEXT+DATA+ANALYSIS) to a handle
    pub fn h_write<W>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteConfig,
    ) -> IODeferredResult<(), NewDataLayoutWarning, StdWriterError>
    where
        W: Write,
    {
        let df = &self.data;
        self.as_data_layout(&conf.shared)
            .def_errors_into()
            .def_errors_liftio()
            .def_and_maybe(|layout| {
                layout
                    .as_writer(df, conf)
                    .mult_to_deferred()
                    .def_errors_liftio()
            })
            .def_and_maybe(|mut writer| {
                let tot = Tot(df.nrows());
                let analysis_len = self.analysis.0.len();
                // write HEADER+TEXT first
                self.h_write_text(h, tot, writer.nbytes(), analysis_len, conf)
                    .map_err(|e| e.inner_into())
                    .into_deferred()?;
                // write DATA
                writer.h_write(h).into_deferred()?;
                // write ANALYSIS
                h.write_all(&self.analysis.0).into_deferred()
            })
    }

    /// Return reference to dataframe representing DATA
    pub fn data(&self) -> &FCSDataFrame {
        &self.data
    }

    /// Add columns to this dataset.
    ///
    /// Return error if columns are not all the same length or number of columns
    /// doesn't match the number of measurement.
    pub fn set_data(&mut self, cols: Vec<AnyFCSColumn>) -> Result<(), ColumsnToDataframeError> {
        self.data = self.try_cols_to_dataframe(cols)?;
        Ok(())
    }

    /// Remove all measurements and data
    pub fn unset_data(&mut self) -> Result<(), ExistingLinkError> {
        self.unset_measurements_inner()?;
        self.data.clear();
        Ok(())
    }

    /// Remove a measurement matching the given name.
    ///
    /// Return removed measurement and its index if found.
    #[allow(clippy::type_complexity)]
    pub fn remove_measurement_by_name(
        &mut self,
        n: &Shortname,
    ) -> Option<(MeasIndex, Element<Temporal<M::T>, Optical<M::O>>)> {
        self.remove_measurement_by_name_inner(n).map(|(i, x)| {
            self.data.drop_in_place(i.into()).unwrap();
            (i, x)
        })
    }

    /// Remove a measurement at a given position
    ///
    /// Return removed measurement and its name if found.
    #[allow(clippy::type_complexity)]
    pub fn remove_measurement_by_index(
        &mut self,
        index: MeasIndex,
    ) -> Result<EitherPair<M::N, Temporal<M::T>, Optical<M::O>>, ElementIndexError> {
        let res = self.remove_measurement_by_index_inner(index)?;
        self.data.drop_in_place(index.into()).unwrap();
        Ok(res)
    }

    /// Add time measurement to the end of the measurement vector.
    ///
    /// Return error if time measurement already exists or name is non-unique.
    pub fn push_temporal(
        &mut self,
        n: Shortname,
        m: Temporal<M::T>,
        col: AnyFCSColumn,
    ) -> Result<(), PushTemporalError> {
        self.measurements.push_center(n, m)?;
        self.data.push_column(col)?;
        Ok(())
    }

    /// Add time measurement at the given position
    ///
    /// Return error if time measurement already exists, name is non-unique, or
    /// index is out of bounds.
    pub fn insert_temporal(
        &mut self,
        i: MeasIndex,
        n: Shortname,
        m: Temporal<M::T>,
        col: AnyFCSColumn,
    ) -> Result<(), PushTemporalError> {
        self.measurements.insert_center(i, n, m)?;
        // ASSUME index is within bounds here since it was checked above
        self.data.insert_column_nocheck(i.into(), col)?;
        Ok(())
    }

    /// Add measurement to the end of the measurement vector
    ///
    /// Return error if name is non-unique.
    pub fn push_optical(
        &mut self,
        n: <M::N as MightHave>::Wrapper<Shortname>,
        m: Optical<M::O>,
        col: AnyFCSColumn,
    ) -> Result<Shortname, PushOpticalError> {
        let k = self.measurements.push(n, m)?;
        self.data.push_column(col)?;
        Ok(k)
    }

    /// Add measurement at a given position
    ///
    /// Return error if name is non-unique, or index is out of bounds.
    pub fn insert_optical(
        &mut self,
        i: MeasIndex,
        n: <M::N as MightHave>::Wrapper<Shortname>,
        m: Optical<M::O>,
        col: AnyFCSColumn,
    ) -> Result<Shortname, InsertOpticalError> {
        let k = self.measurements.insert(i, n, m)?;
        // ASSUME index is within bounds here since it was checked above
        self.data.insert_column_nocheck(i.into(), col)?;
        Ok(k)
    }

    /// Convert this struct into a CoreTEXT.
    ///
    /// This simply entails taking ownership and dropping the ANALYSIS and DATA
    /// fields.
    pub fn into_coretext(self) -> VersionedCoreTEXT<M> {
        CoreTEXT::new_unchecked(self.metaroot, self.measurements)
    }
}

impl<M, T, P, N, W> CoreTEXT<M, T, P, N, W> {
    pub(crate) fn new_nomeas(metaroot: Metaroot<M>) -> Self {
        Self {
            metaroot,
            measurements: NamedVec::default(),
            data: (),
            analysis: (),
        }
    }

    pub(crate) fn new_unchecked(
        metaroot: Metaroot<M>,
        measurements: NamedVec<N, W, Temporal<T>, Optical<P>>,
    ) -> Self {
        Self {
            metaroot,
            measurements,
            data: (),
            analysis: (),
        }
    }
}

macro_rules! comp_methods {
    () => {
        /// Return matrix for $COMP
        pub fn compensation(&self) -> Option<&Compensation> {
            self.metaroot.specific.comp.as_ref_opt().map(|x| x.borrow())
        }

        /// Set matrix for $COMP
        ///
        /// Return true if successfully set. Return false if matrix is either not
        /// square or rows/columns are not the same length as $PAR.
        pub fn set_compensation(&mut self, matrix: DMatrix<f32>) -> Result<(), NewCompError> {
            Compensation::try_new(matrix).map(|comp| {
                self.metaroot.specific.comp = Some(comp.into()).into();
            })
        }

        /// Clear $COMP
        pub fn unset_compensation(&mut self) {
            self.metaroot.specific.comp = None.into();
        }
    };
}

macro_rules! timestamp_methods {
    ($timetype:ident) => {
        pub fn timestamps(&self) -> &Timestamps<$timetype> {
            &self.metaroot.specific.timestamps
        }

        pub fn timestamps_mut(&mut self) -> &mut Timestamps<$timetype> {
            &mut self.metaroot.specific.timestamps
        }
    };
}

macro_rules! spillover_methods {
    () => {
        /// Show $SPILLOVER
        pub fn spillover(&self) -> Option<&Spillover> {
            self.metaroot.specific.spillover.as_ref_opt()
        }

        /// Set names and matrix for $SPILLOVER
        ///
        /// Names must match number of rows/columns in matrix and also must be a
        /// subset of the measurement names (ie $PnN). Matrix must be square and
        /// at least 2x2.
        pub fn set_spillover(
            &mut self,
            ns: Vec<Shortname>,
            m: DMatrix<f32>,
        ) -> Result<(), SetSpilloverError> {
            let current = self.all_shortnames();
            let new: HashSet<_> = ns.iter().collect();
            if !new.is_subset(&current.iter().collect()) {
                return Err(SpilloverLinkError.into());
            }
            let m = Spillover::try_new(ns, m)?;
            self.metaroot.specific.spillover = Some(m).into();
            Ok(())
        }

        /// Clear $SPILLOVER
        pub fn unset_spillover(&mut self) {
            self.metaroot.specific.spillover = None.into();
        }
    };
}

macro_rules! display_methods {
    () => {
        pub fn displays(&self) -> Vec<Option<&Display>> {
            self.measurements
                .iter()
                .map(|x| {
                    x.1.both(
                        |t| t.value.specific.display.as_ref_opt(),
                        |m| m.value.specific.display.as_ref_opt(),
                    )
                })
                .collect()
        }

        pub fn set_displays(&mut self, ns: Vec<Option<Display>>) -> Result<(), KeyLengthError> {
            self.measurements
                .alter_values_zip(
                    ns,
                    |x, n| x.value.specific.display = n.into(),
                    |x, n| x.value.specific.display = n.into(),
                )
                .map(|_| ())
        }
    };
}

macro_rules! scale_get_set {
    ($t:path, $time_default:expr) => {
        /// Show $PnE for all measurements
        pub fn all_scales(&self) -> Vec<$t> {
            self.measurements
                .iter()
                .map(|(_, x)| x.both(|_| $time_default, |p| p.value.specific.scale.into()))
                .collect()
        }

        /// Show $PnE for optical measurements
        pub fn scales(&self) -> Vec<(MeasIndex, $t)> {
            self.measurements
                .iter_non_center_values()
                .map(|(i, m)| (i, m.specific.scale.into()))
                .collect()
        }

        /// Set $PnE for for all optical measurements
        pub fn set_scales(&mut self, xs: Vec<$t>) -> Result<(), KeyLengthError> {
            self.measurements
                .alter_non_center_values_zip(xs, |m, x| {
                    m.specific.scale = x.into();
                })
                .map(|_| ())
        }
    };
}

macro_rules! float_layout2_0 {
    () => {
        /// Set data layout to be 32-bit float for all measurements.
        pub fn set_data_f32(&mut self, rs: Vec<f32>) -> Result<(), SetFloatError> {
            let xs = rs
                .into_iter()
                .map(|r| Range::try_from(f64::from(r)))
                .collect::<Result<Vec<Range>, NanFloatOrInt>>()?;
            self.set_to_floating_point(false, xs)?;
            Ok(())
        }

        /// Set data layout to be 64-bit float for all measurements.
        pub fn set_data_f64(&mut self, rs: Vec<f64>) -> Result<(), SetFloatError> {
            let xs = rs
                .into_iter()
                .map(Range::try_from)
                .collect::<Result<Vec<Range>, NanFloatOrInt>>()?;
            self.set_to_floating_point(true, xs)?;
            Ok(())
        }
    };
}

macro_rules! int_layout_2_0 {
    () => {
        /// Set data layout to be Integer for all measurements
        pub fn set_data_integer(
            &mut self,
            rs: Vec<u64>,
            byteord: ByteOrd,
        ) -> Result<(), KeyLengthError> {
            let n = byteord.nbytes();
            let ys = rs
                .into_iter()
                .map(|r| RangeSetter { width: n, range: r })
                .collect();
            self.set_data_integer_inner(ys)?;
            self.metaroot.specific.byteord = byteord;
            Ok(())
        }
    };
}

macro_rules! set_shortnames_2_0 {
    () => {
        /// Set all optical $PnN keywords to list of names.
        pub fn set_measurement_shortnames_maybe(
            &mut self,
            ns: Vec<Option<Shortname>>,
        ) -> Result<NameMapping, SetKeysError> {
            let ks = ns.into_iter().map(|n| n.into()).collect();
            let mapping = self.measurements.set_non_center_keys(ks)?;
            self.metaroot.reassign_all(&mapping);
            Ok(mapping)
        }
    };
}

impl<A, D> Core2_0<A, D> {
    comp_methods!();
    scale_get_set!(Option<Scale>, Some(Scale::Linear));

    set_shortnames_2_0!();
    int_layout_2_0!();
    float_layout2_0!();

    /// Set data layout to be ASCII-delimited
    pub fn set_data_delimited(&mut self, xs: Vec<u64>) -> Result<(), KeyLengthError> {
        self.set_data_delimited_inner(xs)
    }

    /// Set data layout to be ASCII-fixed for all measurements
    pub fn set_data_ascii(&mut self, xs: Vec<AsciiRangeSetter>) -> Result<(), KeyLengthError> {
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

impl<A, D> Core3_0<A, D> {
    comp_methods!();
    scale_get_set!(Scale, Scale::Linear);

    set_shortnames_2_0!();
    int_layout_2_0!();
    float_layout2_0!();

    /// Set data layout to be ASCII-delimited
    pub fn set_data_delimited(&mut self, xs: Vec<u64>) -> Result<(), KeyLengthError> {
        self.set_data_delimited_inner(xs)
    }

    /// Set data layout to be ASCII-fixed for all measurements
    pub fn set_data_ascii(&mut self, xs: Vec<AsciiRangeSetter>) -> Result<(), KeyLengthError> {
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

impl<A, D> Core3_1<A, D> {
    scale_get_set!(Scale, Scale::Linear);
    spillover_methods!();

    /// Set data layout to be integers for all measurements.
    pub fn set_data_integer(&mut self, xs: Vec<NumRangeSetter>) -> Result<(), KeyLengthError> {
        self.set_data_integer_inner(xs)
    }

    float_layout2_0!();

    /// Set data layout to be fixed-ASCII for all measurements
    pub fn set_data_ascii(&mut self, xs: Vec<AsciiRangeSetter>) -> Result<(), KeyLengthError> {
        self.set_data_ascii_inner(xs)
    }

    /// Set data layout to be ASCII-delimited
    pub fn set_data_delimited(&mut self, xs: Vec<u64>) -> Result<(), KeyLengthError> {
        self.set_data_delimited_inner(xs)
    }

    pub fn get_big_endian(&self) -> bool {
        self.metaroot.specific.byteord == Endian::Big
    }

    pub fn set_big_endian(&mut self, is_big: bool) {
        self.metaroot.specific.byteord = Endian::is_big(is_big);
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

impl<A, D> Core3_2<A, D> {
    /// Show $UNSTAINEDCENTERS
    pub fn unstained_centers(&self) -> Option<&UnstainedCenters> {
        self.metaroot
            .specific
            .unstained
            .unstainedcenters
            .as_ref_opt()
    }

    /// Insert an unstained center
    pub fn insert_unstained_center(
        &mut self,
        k: Shortname,
        v: f32,
    ) -> Result<Option<f32>, MissingMeasurementNameError> {
        if !self.measurement_names().contains(&k) {
            Err(MissingMeasurementNameError(k))
        } else {
            let us = &mut self.metaroot.specific.unstained;
            let ret = if let Some(u) = us.unstainedcenters.0.as_mut() {
                u.insert(k, v)
            } else {
                us.unstainedcenters = Some(UnstainedCenters::new_1(k, v)).into();
                None
            };
            Ok(ret)
        }
    }

    /// Remove an unstained center
    pub fn remove_unstained_center(&mut self, k: &Shortname) -> Option<f32> {
        let us = &mut self.metaroot.specific.unstained;
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
        self.metaroot.specific.unstained.unstainedcenters = None.into()
    }

    scale_get_set!(Scale, Scale::Linear);
    spillover_methods!();

    /// Show datatype for all measurements
    ///
    /// This will be $PnDATATYPE if given and $DATATYPE otherwise at each
    /// measurement index
    pub fn datatypes(&self) -> Vec<AlphaNumType> {
        let dt = self.metaroot.datatype;
        self.measurements
            .iter()
            .map(|(_, x)| {
                x.both(
                    |p| p.value.specific.datatype.as_ref_opt(),
                    |p| p.value.specific.datatype.as_ref_opt(),
                )
                .map(|t| (*t).into())
                .unwrap_or(dt)
            })
            .collect()
    }

    /// Set data layout to be a mix of datatypes
    pub fn set_data_mixed(&mut self, xs: Vec<MixedColumnSetter>) -> Result<(), KeyLengthError> {
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
                    // ASSUME f32/f64 won't fail to get range because NaN won't
                    // be allowed inside
                    MixedColumnSetter::Float(range) => {
                        (Width::new_f32(), f64::from(range).try_into().unwrap(), pndt)
                    }
                    MixedColumnSetter::Double(range) => {
                        (Width::new_f64(), range.try_into().unwrap(), pndt)
                    }
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
            self.measurements.alter_values_zip(
                xs,
                |x, y| {
                    let (b, r, pndt) = go(y);
                    let m = x.value;
                    m.common.width = b;
                    m.common.range = r;
                    m.specific.datatype = pndt.into();
                },
                |x, y| {
                    let (b, r, pndt) = go(y);
                    let t = x.value;
                    t.common.width = b;
                    t.common.range = r;
                    t.specific.datatype = pndt.into();
                },
            )?;
            self.metaroot.datatype = dt;
            Ok(())
        } else {
            // this will only happen if the input is empty
            Err(KeyLengthError::empty(self.par().0))
        }
    }

    /// Set data layout to be integer for all measurements
    pub fn set_data_integer(&mut self, xs: Vec<NumRangeSetter>) -> Result<(), KeyLengthError> {
        self.set_data_integer_inner(xs)?;
        self.unset_meas_datatypes();
        Ok(())
    }

    /// Set data layout to be 32-bit float for all measurements.
    pub fn set_data_f32(&mut self, rs: Vec<f32>) -> Result<(), SetFloatError> {
        let xs = rs
            .into_iter()
            .map(|r| Range::try_from(f64::from(r)))
            .collect::<Result<Vec<Range>, NanFloatOrInt>>()?;
        self.set_to_floating_point_3_2(false, xs)?;
        Ok(())
    }

    /// Set data layout to be 64-bit float for all measurements.
    pub fn set_data_f64(&mut self, rs: Vec<f64>) -> Result<(), SetFloatError> {
        let xs = rs
            .into_iter()
            .map(Range::try_from)
            .collect::<Result<Vec<Range>, NanFloatOrInt>>()?;
        self.set_to_floating_point_3_2(true, xs)?;
        Ok(())
    }

    /// Set data layout to be fixed-ASCII for all measurements
    pub fn set_data_ascii(&mut self, xs: Vec<AsciiRangeSetter>) -> Result<(), KeyLengthError> {
        self.set_data_ascii_inner(xs)?;
        self.unset_meas_datatypes();
        Ok(())
    }

    /// Set data layout to be ASCII-delimited for all measurements
    pub fn set_data_delimited(&mut self, xs: Vec<u64>) -> Result<(), KeyLengthError> {
        self.set_data_delimited_inner(xs)?;
        self.unset_meas_datatypes();
        Ok(())
    }

    pub fn get_big_endian(&self) -> bool {
        self.metaroot.specific.byteord == Endian::Big
    }

    pub fn set_big_endian(&mut self, is_big: bool) {
        self.metaroot.specific.byteord = Endian::is_big(is_big);
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
        OpticalType,
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
    fn set_to_floating_point_3_2(
        &mut self,
        is_double: bool,
        rs: Vec<Range>,
    ) -> Result<(), KeyLengthError> {
        self.set_to_floating_point(is_double, rs)?;
        self.unset_meas_datatypes();
        Ok(())
    }
}

macro_rules! coretext_set_measurements2_0 {
    ($rawinput:path) => {
        /// Set measurements.
        ///
        /// Return error if names are not unique or there is more than one
        /// time measurement.
        pub fn set_measurements(
            &mut self,
            xs: $rawinput,
            prefix: ShortnamePrefix,
        ) -> Result<(), SetMeasurementsError> {
            self.set_measurements_inner(xs, prefix)
        }
    };
}

macro_rules! coretext_set_measurements3_1 {
    ($rawinput:path) => {
        /// Set measurements.
        ///
        /// Return error if names are not unique or there is more than one
        /// time measurement.
        pub fn set_measurements(&mut self, xs: $rawinput) -> Result<(), SetMeasurementsError> {
            self.set_measurements_inner(xs, ShortnamePrefix::default())
        }
    };
}

impl CoreTEXT2_0 {
    pub fn new(datatype: AlphaNumType, byteord: ByteOrd, mode: Mode) -> Self {
        let specific = InnerMetaroot2_0::new(mode, byteord);
        let metaroot = Metaroot::new(datatype, specific);
        CoreTEXT::new_nomeas(metaroot)
    }

    coretext_set_measurements2_0!(RawInput2_0);
}

impl CoreTEXT3_0 {
    pub fn new(datatype: AlphaNumType, byteord: ByteOrd, mode: Mode) -> Self {
        let specific = InnerMetaroot3_0::new(mode, byteord);
        let metaroot = Metaroot::new(datatype, specific);
        CoreTEXT::new_nomeas(metaroot)
    }

    coretext_set_measurements2_0!(RawInput3_0);
}

impl CoreTEXT3_1 {
    pub fn new(datatype: AlphaNumType, is_big: bool, mode: Mode) -> Self {
        let specific = InnerMetaroot3_1::new(mode, is_big);
        let metaroot = Metaroot::new(datatype, specific);
        CoreTEXT::new_nomeas(metaroot)
    }

    coretext_set_measurements3_1!(RawInput3_1);
}

impl CoreTEXT3_2 {
    pub fn new(datatype: AlphaNumType, is_big: bool, cyt: String) -> Self {
        let specific = InnerMetaroot3_2::new(is_big, cyt);
        let metaroot = Metaroot::new(datatype, specific);
        CoreTEXT::new_nomeas(metaroot)
    }

    coretext_set_measurements3_1!(RawInput3_2);
}

macro_rules! coredataset_set_measurements2_0 {
    ($rawinput:path) => {
        /// Set measurements and dataframe together
        ///
        /// Length of measurements must match the width of the input dataframe.
        pub fn set_measurements_and_data(
            &mut self,
            xs: $rawinput,
            cs: Vec<AnyFCSColumn>,
            prefix: ShortnamePrefix,
        ) -> Result<(), SetMeasurementsAndDataError> {
            let meas_n = xs.len();
            let data_n = cs.len();
            if meas_n != data_n {
                return Err(MeasDataMismatchError { meas_n, data_n }.into());
            }
            let df = FCSDataFrame::try_new(cs)?;
            self.set_measurements_inner(xs, prefix)?;
            self.data = df;
            Ok(())
        }

        /// Set measurements.
        ///
        /// Length of measurements must match the current width of the dataframe.
        pub fn set_measurements(
            &mut self,
            xs: $rawinput,
            prefix: ShortnamePrefix,
        ) -> Result<(), SetMeasurementsOnlyError> {
            let meas_n = xs.len();
            let data_n = self.par().0;
            if meas_n != data_n {
                return Err(MeasDataMismatchError { meas_n, data_n }.into());
            }
            self.set_measurements_inner(xs, prefix)?;
            Ok(())
        }
    };
}

macro_rules! coredataset_set_measurements3_1 {
    ($rawinput:path) => {
        /// Set measurements and dataframe together
        ///
        /// Length of measurements must match the width of the input dataframe.
        pub fn set_measurements_and_data(
            &mut self,
            xs: $rawinput,
            cs: Vec<AnyFCSColumn>,
        ) -> Result<(), SetMeasurementsAndDataError> {
            let meas_n = xs.len();
            let data_n = cs.len();
            if meas_n != data_n {
                return Err(MeasDataMismatchError { meas_n, data_n }.into());
            }
            let df = FCSDataFrame::try_new(cs)?;
            self.set_measurements_inner(xs, ShortnamePrefix::default())?;
            self.data = df;
            Ok(())
        }

        /// Set measurements.
        ///
        /// Length of measurements must match the current width of the dataframe.
        pub fn set_measurements(&mut self, xs: $rawinput) -> Result<(), SetMeasurementsOnlyError> {
            let meas_n = xs.len();
            let data_n = self.par().0;
            if meas_n != data_n {
                return Err(MeasDataMismatchError { meas_n, data_n }.into());
            }
            self.set_measurements_inner(xs, ShortnamePrefix::default())?;
            Ok(())
        }
    };
}

impl CoreDataset2_0 {
    coredataset_set_measurements2_0!(RawInput2_0);
}

impl CoreDataset3_0 {
    coredataset_set_measurements2_0!(RawInput3_0);
}

impl CoreDataset3_1 {
    coredataset_set_measurements3_1!(RawInput3_1);
}

impl CoreDataset3_2 {
    coredataset_set_measurements3_1!(RawInput3_2);
}

impl UnstainedData {
    fn lookup<E>(kws: &mut StdKeywords) -> LookupTentative<Self, E> {
        let c = lookup_meta_opt(kws, false);
        let i = lookup_meta_opt(kws, false);
        c.zip(i).map(|(unstainedcenters, unstainedinfo)| Self {
            unstainedcenters,
            unstainedinfo,
        })
    }

    fn opt_keywords(&self) -> RawOptPairs {
        [
            OptMetaKey::pair(&self.unstainedcenters),
            OptMetaKey::pair(&self.unstainedinfo),
        ]
        .into_iter()
        .collect()
    }

    fn check_loss(
        self,
        lossless: bool,
    ) -> Tentative<(), AnyMetarootKeyLossError, AnyMetarootKeyLossError> {
        let c = check_key_transfer(self.unstainedcenters, lossless);
        let i = check_key_transfer(self.unstainedinfo, lossless);
        c.zip(i).void()
    }
}

impl SubsetData {
    fn lookup<E>(kws: &mut StdKeywords, dep: bool) -> LookupTentative<OptionalKw<Self>, E> {
        lookup_meta_opt(kws, dep).and_tentatively(|m: OptionalKw<CSMode>| {
            if let Some(n) = m.0 {
                let it = (0..n.0).map(|i| lookup_indexed_opt::<CSVFlag, _>(kws, i.into(), dep));
                Tentative::mconcat_ne(NonEmpty::collect(it).unwrap()).and_tentatively(|flags| {
                    lookup_meta_opt::<CSVBits, _>(kws, dep)
                        .map(|bits| Some(Self { flags, bits }).into())
                })
            } else {
                Tentative::new1(None.into())
            }
        })
    }

    fn opt_keywords(&self) -> RawOptPairs {
        let m = OptionalKw(Some(CSMode(self.flags.len())));
        self.flags
            .iter()
            .enumerate()
            .map(|(i, f)| OptIndexedKey::pair(f, i.into()))
            .chain([OptMetaKey::pair(&self.bits), OptMetaKey::pair(&m)])
            .collect()
    }
}

impl Serialize for SubsetData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SubsetData", 2)?;
        state.serialize_field("bits", &self.bits)?;
        state.serialize_field("flags", &self.flags.iter().collect::<Vec<_>>())?;
        state.end()
    }
}

impl<I> UnivariateRegion<I> {
    fn map<F, J>(self, f: F) -> UnivariateRegion<J>
    where
        F: FnOnce(I) -> J,
    {
        UnivariateRegion {
            gate: self.gate,
            index: f(self.index),
        }
    }

    fn try_map<F, J, E>(self, f: F) -> Result<UnivariateRegion<J>, E>
    where
        F: FnOnce(I) -> Result<J, E>,
    {
        Ok(UnivariateRegion {
            gate: self.gate,
            index: f(self.index)?,
        })
    }
}

impl<I> BivariateRegion<I> {
    fn map<F, J>(self, mut f: F) -> BivariateRegion<J>
    where
        F: FnMut(I) -> J,
    {
        BivariateRegion {
            vertices: self.vertices,
            x_index: f(self.x_index),
            y_index: f(self.y_index),
        }
    }

    fn try_map<F, J, E>(self, mut f: F) -> Result<BivariateRegion<J>, E>
    where
        F: FnMut(I) -> Result<J, E>,
    {
        Ok(BivariateRegion {
            vertices: self.vertices,
            x_index: f(self.x_index)?,
            y_index: f(self.y_index)?,
        })
    }
}

impl<I: Serialize> Serialize for BivariateRegion<I> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BivariateRegion", 2)?;
        state.serialize_field("vertices", &self.vertices.iter().collect::<Vec<_>>())?;
        state.serialize_field("x_index", &self.x_index)?;
        state.serialize_field("y_index", &self.y_index)?;
        state.end()
    }
}

impl AppliedGates2_0 {
    fn lookup<E>(kws: &mut StdKeywords) -> LookupTentative<OptionalKw<Self>, E> {
        let ag = GatingRegions::lookup(kws, false, |k, i| Region::lookup(k, i, false));
        let gm = GatedMeasurements::lookup(kws, false);
        ag.zip(gm).and_tentatively(|(x, y)| {
            if let Some((applied, gated_measurements)) = x.0.zip(y.0) {
                let ret = Self {
                    gated_measurements,
                    regions: applied,
                };
                match ret.check_gates() {
                    Ok(_) => Tentative::new1(Some(ret).into()),
                    Err(e) => {
                        let w = LookupKeysWarning::Relation(e.into());
                        Tentative::new(None.into(), vec![w], vec![])
                    }
                }
            } else {
                Tentative::new1(None.into())
            }
        })
    }

    pub(crate) fn opt_keywords(&self) -> RawOptPairs {
        let gate = OptionalKw(Some(Gate(self.gated_measurements.0.len())));
        self.gated_measurements
            .0
            .iter()
            .enumerate()
            .flat_map(|(i, m)| m.opt_keywords(i.into()))
            .chain([OptMetaKey::pair(&gate)])
            .chain(self.regions.opt_keywords())
            .collect()
    }

    pub fn check_gates(&self) -> Result<(), GateMeasurementLinkError> {
        let n = self.gated_measurements.0.len();
        let it = self
            .regions
            .regions
            .as_ref()
            .flat_map(|(_, r)| r.clone().flatten())
            .into_iter()
            .filter(|i| usize::from(*i) > n);
        NonEmpty::collect(it).map_or(Ok(()), |xs| Err(GateMeasurementLinkError(xs)))
    }
}

impl AppliedGates3_0 {
    fn lookup<E>(kws: &mut StdKeywords, dep: bool) -> LookupTentative<OptionalKw<Self>, E> {
        let ag = GatingRegions::lookup(kws, false, |k, i| Region::lookup(k, i, false));
        let gm = GatedMeasurements::lookup(kws, dep);
        ag.zip(gm).and_tentatively(|(x, y)| {
            if let Some(applied) = x.0 {
                let ret = Self {
                    gated_measurements: y.0.map(|z| z.0.into()).unwrap_or_default(),
                    regions: applied,
                };
                match ret.check_gates() {
                    Ok(_) => Tentative::new1(Some(ret).into()),
                    Err(e) => {
                        let w = LookupKeysWarning::Relation(e.into());
                        Tentative::new(None.into(), vec![w], vec![])
                    }
                }
            } else {
                Tentative::new1(None.into())
            }
        })
    }

    pub(crate) fn opt_keywords(&self) -> RawOptPairs {
        let g = self.gated_measurements.len();
        let gate = OptionalKw(if g == 0 { None } else { Some(Gate(g)) });
        self.gated_measurements
            .iter()
            .enumerate()
            .flat_map(|(i, m)| m.opt_keywords(i.into()))
            .chain([OptMetaKey::pair(&gate)])
            .chain(self.regions.opt_keywords())
            .collect()
    }

    pub fn check_gates(&self) -> Result<(), GateMeasurementLinkError> {
        let n = self.gated_measurements.len();
        let it = self
            .regions
            .regions
            .as_ref()
            .flat_map(|(_, r)| r.clone().flatten())
            .into_iter()
            .flat_map(|i| GateIndex::try_from(i).ok())
            .filter(|i| usize::from(*i) > n);
        NonEmpty::collect(it).map_or(Ok(()), |xs| Err(GateMeasurementLinkError(xs)))
    }
}

impl AppliedGates3_2 {
    fn lookup<E>(kws: &mut StdKeywords) -> LookupTentative<OptionalKw<Self>, E> {
        GatingRegions::lookup(kws, true, |k, i| Region::lookup(k, i, true))
            .map(|x| x.map(|regions| Self { regions }))
    }

    pub(crate) fn opt_keywords(&self) -> RawOptPairs {
        self.regions.opt_keywords()
    }
}

impl GatedMeasurement {
    fn lookup<E>(kws: &mut StdKeywords, i: GateIndex, dep: bool) -> LookupTentative<Self, E> {
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
                Self {
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

    pub(crate) fn opt_keywords(&self, i: GateIndex) -> RawOptPairs {
        let j = i.into();
        [
            OptIndexedKey::pair(&self.scale, j),
            OptIndexedKey::pair(&self.filter, j),
            OptIndexedKey::pair(&self.shortname, j),
            OptIndexedKey::pair(&self.percent_emitted, j),
            OptIndexedKey::pair(&self.range, j),
            OptIndexedKey::pair(&self.longname, j),
            OptIndexedKey::pair(&self.detector_type, j),
            OptIndexedKey::pair(&self.detector_voltage, j),
        ]
        .into_iter()
        .collect()
    }
}

impl<I> GatingRegions<I> {
    fn lookup<E, F>(
        kws: &mut StdKeywords,
        dep: bool,
        get_region: F,
    ) -> LookupTentative<OptionalKw<Self>, E>
    where
        F: Fn(&mut StdKeywords, RegionIndex) -> LookupTentative<OptionalKw<Region<I>>, E>,
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
                        Ok(xs) => {
                            Tentative::mconcat_ne(xs).map(|regions| Some(Self { regions, gating }))
                        }
                        Err(w) => {
                            Tentative::new(None, vec![LookupKeysWarning::Relation(w)], vec![])
                        }
                    }
                } else {
                    Tentative::new1(None)
                }
            })
            .map(|x| x.into())
    }

    pub(crate) fn opt_keywords(&self) -> RawOptPairs
    where
        I: fmt::Display,
        I: FromStr,
        I: Copy,
    {
        self.regions
            .iter()
            .flat_map(|(ri, r)| r.opt_keywords(*ri))
            .chain([(Gating::std().to_string(), Some(self.gating.to_string()))])
            .collect()
    }

    fn inner_into<J>(self) -> GatingRegions<J>
    where
        J: From<I>,
    {
        GatingRegions {
            gating: self.gating,
            regions: self.regions.map(|(ri, r)| (ri, r.inner_into())),
        }
    }

    fn try_inner_into<J, E>(self) -> Result<GatingRegions<J>, E>
    where
        J: TryFrom<I, Error = E>,
    {
        Ok(GatingRegions {
            gating: self.gating,
            regions: self
                .regions
                .try_map(|(ri, r)| r.try_map(|i| i.try_into()).map(|x| (ri, x)))?,
        })
    }
}

impl<I> Region<I> {
    pub(crate) fn try_new(r_index: RegionGateIndex<I>, window: RegionWindow) -> Option<Self> {
        match (r_index, window) {
            (RegionGateIndex::Univariate(index), RegionWindow::Univariate(gate)) => {
                Some(Region::Univariate(UnivariateRegion { index, gate }))
            }
            (RegionGateIndex::Bivariate(x_index, y_index), RegionWindow::Bivariate(vertices)) => {
                Some(Region::Bivariate(BivariateRegion {
                    x_index,
                    y_index,
                    vertices,
                }))
            }
            _ => None,
        }
    }

    fn lookup<E>(
        kws: &mut StdKeywords,
        i: RegionIndex,
        dep: bool,
    ) -> LookupTentative<OptionalKw<Self>, E>
    where
        I: FromStr,
        I: fmt::Display,
        ParseOptKeyWarning: From<<RegionGateIndex<I> as FromStr>::Err>,
    {
        let n = lookup_indexed_opt::<RegionGateIndex<I>, _>(kws, i.into(), dep);
        let w = lookup_indexed_opt::<RegionWindow, _>(kws, i.into(), dep);
        n.zip(w)
            .and_tentatively(|(_n, _y)| {
                _n.0.zip(_y.0)
                    .and_then(|(gi, win)| Self::try_new(gi, win).map(|x| x.inner_into()))
                    .map_or_else(
                        || {
                            let warn =
                                LookupRelationalWarning::GateRegion(MismatchedIndexAndWindowError)
                                    .into();
                            Tentative::new(None, vec![warn], vec![])
                        },
                        |x| Tentative::new1(Some(x)),
                    )
            })
            .map(|x| x.into())
    }

    pub(crate) fn opt_keywords(&self, i: RegionIndex) -> RawOptPairs
    where
        I: Copy,
        I: FromStr,
        I: fmt::Display,
    {
        let (ri, rw) = self.split();
        [
            OptIndexedKey::pair(&OptionalKw(Some(ri)), i.into()),
            OptIndexedKey::pair(&OptionalKw(Some(rw)), i.into()),
        ]
        .into_iter()
        .collect()
    }

    pub(crate) fn split(&self) -> (RegionGateIndex<I>, RegionWindow)
    where
        I: Copy,
    {
        match self {
            Self::Univariate(x) => (
                RegionGateIndex::Univariate(x.index),
                // TODO clone
                RegionWindow::Univariate(x.gate.clone()),
            ),
            Self::Bivariate(x) => (
                RegionGateIndex::Bivariate(x.x_index, x.y_index),
                RegionWindow::Bivariate(x.vertices.clone()),
            ),
        }
    }

    pub(crate) fn map<F, J>(self, f: F) -> Region<J>
    where
        F: FnMut(I) -> J,
    {
        match self {
            Self::Univariate(x) => Region::Univariate(x.map(f)),
            Self::Bivariate(x) => Region::Bivariate(x.map(f)),
        }
    }

    pub(crate) fn try_map<F, J, E>(self, f: F) -> Result<Region<J>, E>
    where
        F: FnMut(I) -> Result<J, E>,
    {
        match self {
            Self::Univariate(x) => Ok(Region::Univariate(x.try_map(f)?)),
            Self::Bivariate(x) => Ok(Region::Bivariate(x.try_map(f)?)),
        }
    }

    pub(crate) fn inner_into<J>(self) -> Region<J>
    where
        J: From<I>,
    {
        self.map(|i| i.into())
    }

    pub(crate) fn flatten(self) -> NonEmpty<I> {
        match self {
            Self::Univariate(r) => NonEmpty::new(r.index),
            Self::Bivariate(r) => (r.x_index, vec![r.y_index]).into(),
        }
    }
}

impl TryFrom<MeasOrGateIndex> for PrefixedMeasIndex {
    type Error = RegionToMeasIndexError;
    fn try_from(value: MeasOrGateIndex) -> Result<Self, Self::Error> {
        match value {
            MeasOrGateIndex::Meas(i) => Ok(i.into()),
            MeasOrGateIndex::Gate(i) => Err(RegionToMeasIndexError(i)),
        }
    }
}

impl From<PrefixedMeasIndex> for MeasOrGateIndex {
    fn from(value: PrefixedMeasIndex) -> Self {
        Self::Meas(value.0)
    }
}

impl TryFrom<MeasOrGateIndex> for GateIndex {
    type Error = RegionToGateIndexError;
    fn try_from(value: MeasOrGateIndex) -> Result<Self, Self::Error> {
        match value {
            MeasOrGateIndex::Gate(i) => Ok(i),
            MeasOrGateIndex::Meas(i) => Err(RegionToGateIndexError(i)),
        }
    }
}

impl TryFrom<GateIndex> for PrefixedMeasIndex {
    type Error = GateToMeasIndexError;
    fn try_from(value: GateIndex) -> Result<Self, Self::Error> {
        Err(GateToMeasIndexError(value))
    }
}

impl TryFrom<PrefixedMeasIndex> for GateIndex {
    type Error = MeasToGateIndexError;
    fn try_from(value: PrefixedMeasIndex) -> Result<Self, Self::Error> {
        Err(MeasToGateIndexError(value))
    }
}

impl<I> Serialize for GatingRegions<I>
where
    I: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AppliedGates", 2)?;
        state.serialize_field("gating", &self.gating)?;
        state.serialize_field("regions", &self.regions.iter().collect::<Vec<_>>())?;
        state.end()
    }
}

impl GatedMeasurements {
    fn lookup<E>(kws: &mut StdKeywords, dep: bool) -> LookupTentative<OptionalKw<Self>, E> {
        lookup_meta_opt::<Gate, E>(kws, dep).and_tentatively(|maybe| {
            if let Some(n) = maybe.0 {
                // TODO this will be nicer with NonZeroUsize
                if n.0 > 0 {
                    let xs = NonEmpty::collect(
                        (0..n.0).map(|i| GatedMeasurement::lookup(kws, i.into(), dep)),
                    )
                    .unwrap();
                    return Tentative::mconcat_ne(xs).map(|x| Some(Self(x)).into());
                }
            }
            Tentative::new1(None.into())
        })
    }
}

impl Serialize for GatedMeasurements {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.iter().collect::<Vec<_>>().serialize(serializer)
    }
}

impl From<AppliedGates2_0> for AppliedGates3_0 {
    fn from(value: AppliedGates2_0) -> Self {
        Self {
            gated_measurements: value.gated_measurements.0.into(),
            regions: value.regions.inner_into(),
        }
    }
}

// TODO emit a warning if we "lose" anything
impl TryFrom<AppliedGates3_0> for AppliedGates2_0 {
    type Error = AppliedGates3_0To2_0Error;
    fn try_from(value: AppliedGates3_0) -> Result<Self, Self::Error> {
        let regions = value
            .regions
            .try_inner_into()
            .map_err(AppliedGates3_0To2_0Error::Index)?;
        if let Some(gs) = NonEmpty::from_vec(value.gated_measurements) {
            Ok(Self {
                gated_measurements: GatedMeasurements(gs),
                regions,
            })
        } else {
            Err(AppliedGates3_0To2_0Error::NoGates)
        }
    }
}

// TODO emit a warning if we "lose" anything
impl TryFrom<AppliedGates3_0> for AppliedGates3_2 {
    type Error = AppliedGates3_0To3_2Error;
    fn try_from(value: AppliedGates3_0) -> Result<Self, Self::Error> {
        let regions = value
            .regions
            .try_inner_into()
            .map_err(AppliedGates3_0To3_2Error::Index)?;
        if value.gated_measurements.is_empty() {
            Ok(Self { regions })
        } else {
            Err(AppliedGates3_0To3_2Error::HasGates)
        }
    }
}

impl From<AppliedGates3_2> for AppliedGates3_0 {
    fn from(value: AppliedGates3_2) -> Self {
        Self {
            gated_measurements: vec![],
            regions: value.regions.inner_into(),
        }
    }
}

impl ModificationData {
    fn lookup<E>(kws: &mut StdKeywords) -> LookupTentative<Self, E> {
        let lmr = lookup_meta_opt(kws, false);
        let lmd = lookup_meta_opt(kws, false);
        let ori = lookup_meta_opt(kws, false);
        lmr.zip3(lmd, ori)
            .map(|(last_modifier, last_modified, originality)| Self {
                last_modifier,
                last_modified,
                originality,
            })
    }

    fn opt_keywords(&self) -> RawOptPairs {
        [
            OptMetaKey::pair(&self.last_modifier),
            OptMetaKey::pair(&self.last_modified),
            OptMetaKey::pair(&self.originality),
        ]
        .into_iter()
        .collect()
    }

    fn check_loss(
        self,
        lossless: bool,
    ) -> Tentative<(), AnyMetarootKeyLossError, AnyMetarootKeyLossError> {
        let d = check_key_transfer(self.last_modified, lossless);
        let r = check_key_transfer(self.last_modifier, lossless);
        let o = check_key_transfer(self.originality, lossless);
        d.zip3(r, o).void()
    }
}

impl CarrierData {
    fn lookup<E>(kws: &mut StdKeywords) -> LookupTentative<Self, E> {
        let l = lookup_meta_opt(kws, false);
        let i = lookup_meta_opt(kws, false);
        let t = lookup_meta_opt(kws, false);
        l.zip3(i, t)
            .map(|(locationid, carrierid, carriertype)| Self {
                locationid,
                carrierid,
                carriertype,
            })
    }

    fn opt_keywords(&self) -> RawOptPairs {
        [
            OptMetaKey::pair(&self.carrierid),
            OptMetaKey::pair(&self.carriertype),
            OptMetaKey::pair(&self.locationid),
        ]
        .into_iter()
        .collect()
    }

    fn check_loss(
        self,
        lossless: bool,
    ) -> Tentative<(), AnyMetarootKeyLossError, AnyMetarootKeyLossError> {
        let i = check_key_transfer(self.carrierid, lossless);
        let t = check_key_transfer(self.carriertype, lossless);
        let l = check_key_transfer(self.locationid, lossless);
        i.zip3(t, l).void()
    }
}

impl PlateData {
    fn lookup<E>(kws: &mut StdKeywords, dep: bool) -> LookupTentative<Self, E> {
        let w = lookup_meta_opt(kws, dep);
        let n = lookup_meta_opt(kws, dep);
        let i = lookup_meta_opt(kws, dep);
        w.zip3(n, i).map(|(wellid, platename, plateid)| Self {
            wellid,
            platename,
            plateid,
        })
    }

    fn opt_keywords(&self) -> RawOptPairs {
        [
            OptMetaKey::pair(&self.wellid),
            OptMetaKey::pair(&self.platename),
            OptMetaKey::pair(&self.platename),
        ]
        .into_iter()
        .collect()
    }

    fn check_loss(
        self,
        lossless: bool,
    ) -> Tentative<(), AnyMetarootKeyLossError, AnyMetarootKeyLossError> {
        let n = check_key_transfer(self.platename, lossless);
        let i = check_key_transfer(self.plateid, lossless);
        let w = check_key_transfer(self.wellid, lossless);
        n.zip3(i, w).void()
    }
}

impl PeakData {
    fn lookup<E>(kws: &mut StdKeywords, i: MeasIndex, dep: bool) -> LookupTentative<Self, E> {
        let b = lookup_indexed_opt(kws, i.into(), dep);
        let s = lookup_indexed_opt(kws, i.into(), dep);
        b.zip(s).map(|(bin, size)| Self { bin, size })
    }

    pub(crate) fn opt_keywords(&self, i: MeasIndex) -> RawOptTriples {
        [
            OptIndexedKey::triple(&self.bin, i.into()),
            OptIndexedKey::triple(&self.size, i.into()),
        ]
        .into_iter()
        .collect()
    }
}

#[derive(Clone, Serialize)]
pub struct OptionalKwFamily;

impl MightHave for OptionalKwFamily {
    type Wrapper<T> = OptionalKw<T>;
    const INFALLABLE: bool = false;

    fn unwrap<T>(x: Self::Wrapper<T>) -> Result<T, Self::Wrapper<T>> {
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

    fn unwrap<T>(x: Self::Wrapper<T>) -> Result<T, Self::Wrapper<T>> {
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
    type Error = OptionalKwToIdentityError;
    fn try_from(value: OptionalKw<T>) -> Result<Self, Self::Error> {
        value.0.ok_or(OptionalKwToIdentityError).map(Identity)
    }
}

// This will never really fail but is implemented for symmetry with its inverse
impl<T> TryFrom<Identity<T>> for OptionalKw<T> {
    type Error = Infallible;
    fn try_from(value: Identity<T>) -> Result<Self, Infallible> {
        Ok(Some(value.0).into())
    }
}

impl From<FCSTime> for FCSTime60 {
    fn from(value: FCSTime) -> Self {
        Self(value.0)
    }
}

impl From<FCSTime> for FCSTime100 {
    fn from(value: FCSTime) -> Self {
        Self(value.0)
    }
}

impl From<FCSTime60> for FCSTime {
    fn from(value: FCSTime60) -> Self {
        // ASSUME this will never fail, we are just removing nanoseconds
        Self(value.0.with_nanosecond(0).unwrap())
    }
}

impl From<FCSTime100> for FCSTime {
    fn from(value: FCSTime100) -> Self {
        // ASSUME this will never fail, we are just removing nanoseconds
        Self(value.0.with_nanosecond(0).unwrap())
    }
}

impl From<FCSTime60> for FCSTime100 {
    fn from(value: FCSTime60) -> Self {
        Self(value.0)
    }
}

impl From<FCSTime100> for FCSTime60 {
    fn from(value: FCSTime100) -> Self {
        Self(value.0)
    }
}

impl From<Wavelength> for Wavelengths {
    fn from(value: Wavelength) -> Self {
        Self(NonEmpty {
            head: value.0,
            tail: vec![],
        })
    }
}

// impl From<Wavelengths> for Wavelength {
//     fn from(value: Wavelengths) -> Self {
//         Self(value.0.head)
//     }
// }

impl From<Calibration3_1> for Calibration3_2 {
    fn from(value: Calibration3_1) -> Self {
        Self {
            unit: value.unit,
            offset: 0.0,
            slope: value.slope,
        }
    }
}

impl From<Calibration3_2> for Calibration3_1 {
    fn from(value: Calibration3_2) -> Self {
        Self {
            unit: value.unit,
            slope: value.slope,
        }
    }
}

fn convert_wavelengths(
    w: OptionalKw<Wavelengths>,
    force: bool,
) -> Tentative<OptionalKw<Wavelength>, WavelengthsLossError, WavelengthsLossError> {
    w.0.map(|x| x.into_wavelength(!force))
        .map_or(Tentative::new1(None), |tnt| tnt.map(Some))
        .map(|x| x.into())
}

impl ConvertFromOptical<InnerOptical3_0> for InnerOptical2_0 {
    fn convert_from_optical(
        value: InnerOptical3_0,
        i: MeasIndex,
        force: bool,
    ) -> OpticalConvertResult<Self> {
        let out = check_indexed_key_transfer_own(value.gain, i.into(), !force)
            .inner_into()
            .map(|_| Self {
                scale: Some(value.scale).into(),
                wavelength: value.wavelength,
                peak: value.peak,
            });
        Ok(out)
    }
}

impl ConvertFromOptical<InnerOptical3_1> for InnerOptical2_0 {
    fn convert_from_optical(
        value: InnerOptical3_1,
        i: MeasIndex,
        force: bool,
    ) -> OpticalConvertResult<Self> {
        let j = i.into();
        let g = check_indexed_key_transfer_own(value.gain, j, !force);
        let c = check_indexed_key_transfer_own(value.calibration, j, !force);
        let d = check_indexed_key_transfer_own(value.display, j, !force);
        let w = convert_wavelengths(value.wavelengths, force).inner_into();
        let out = g
            .zip3(c, d)
            .inner_into()
            .zip(w)
            .map(|(_, wavelength)| Self {
                scale: Some(value.scale).into(),
                wavelength,
                peak: value.peak,
            });
        Ok(out)
    }
}

impl ConvertFromOptical<InnerOptical3_2> for InnerOptical2_0 {
    fn convert_from_optical(
        value: InnerOptical3_2,
        i: MeasIndex,
        force: bool,
    ) -> OpticalConvertResult<Self> {
        let j = i.into();
        let g = check_indexed_key_transfer_own(value.gain, j, !force);
        let c = check_indexed_key_transfer_own(value.calibration, j, !force);
        let d = check_indexed_key_transfer_own(value.display, j, !force);
        let a = check_indexed_key_transfer_own(value.analyte, j, !force);
        let f = check_indexed_key_transfer_own(value.feature, j, !force);
        let m = check_indexed_key_transfer_own(value.measurement_type, j, !force);
        let t = check_indexed_key_transfer_own(value.tag, j, !force);
        let n = check_indexed_key_transfer_own(value.detector_name, j, !force);
        let dt = check_indexed_key_transfer_own(value.datatype, j, !force);
        let w = convert_wavelengths(value.wavelengths, force).inner_into();
        let out = g
            .zip6(c, d, a, f, m)
            .zip4(t, n, dt)
            .inner_into()
            .zip(w)
            .map(|(_, wavelength)| Self {
                scale: Some(value.scale).into(),
                wavelength,
                peak: PeakData::default(),
            });
        Ok(out)
    }
}

impl ConvertFromOptical<InnerOptical2_0> for InnerOptical3_0 {
    fn convert_from_optical(
        value: InnerOptical2_0,
        i: MeasIndex,
        _: bool,
    ) -> OpticalConvertResult<Self> {
        value
            .scale
            .0
            .ok_or(NoScaleError(i))
            .map(|scale| Self {
                scale,
                wavelength: value.wavelength,
                gain: None.into(),
                peak: value.peak,
            })
            .into_deferred()
    }
}

impl ConvertFromOptical<InnerOptical3_1> for InnerOptical3_0 {
    fn convert_from_optical(
        value: InnerOptical3_1,
        i: MeasIndex,
        force: bool,
    ) -> OpticalConvertResult<Self> {
        let j = i.into();
        let c = check_indexed_key_transfer_own(value.calibration, j, !force);
        let d = check_indexed_key_transfer_own(value.display, j, !force);
        let w = convert_wavelengths(value.wavelengths, force).inner_into();
        let out = c.zip(d).inner_into().zip(w).map(|(_, wavelength)| Self {
            scale: value.scale,
            gain: value.gain,
            wavelength,
            peak: value.peak,
        });
        Ok(out)
    }
}

impl ConvertFromOptical<InnerOptical3_2> for InnerOptical3_0 {
    fn convert_from_optical(
        value: InnerOptical3_2,
        i: MeasIndex,
        force: bool,
    ) -> OpticalConvertResult<Self> {
        let j = i.into();
        let c = check_indexed_key_transfer_own(value.calibration, j, !force);
        let d = check_indexed_key_transfer_own(value.display, j, !force);
        let a = check_indexed_key_transfer_own(value.analyte, j, !force);
        let f = check_indexed_key_transfer_own(value.feature, j, !force);
        let m = check_indexed_key_transfer_own(value.measurement_type, j, !force);
        let t = check_indexed_key_transfer_own(value.tag, j, !force);
        let n = check_indexed_key_transfer_own(value.detector_name, j, !force);
        let dt = check_indexed_key_transfer_own(value.datatype, j, !force);
        let w = convert_wavelengths(value.wavelengths, force).inner_into();
        let out = c
            .zip5(d, a, f, m)
            .zip4(t, n, dt)
            .inner_into()
            .zip(w)
            .map(|(_, wavelength)| Self {
                scale: value.scale,
                gain: value.gain,
                wavelength,
                peak: PeakData::default(),
            });
        Ok(out)
    }
}

impl ConvertFromOptical<InnerOptical2_0> for InnerOptical3_1 {
    fn convert_from_optical(
        value: InnerOptical2_0,
        i: MeasIndex,
        _: bool,
    ) -> OpticalConvertResult<Self> {
        value
            .scale
            .0
            .ok_or(NoScaleError(i))
            .map(|scale| Self {
                scale,
                wavelengths: value.wavelength.map(|x| x.into()),
                gain: None.into(),
                calibration: None.into(),
                display: None.into(),
                peak: value.peak,
            })
            .into_deferred()
    }
}

impl ConvertFromOptical<InnerOptical3_0> for InnerOptical3_1 {
    fn convert_from_optical(
        value: InnerOptical3_0,
        _: MeasIndex,
        _: bool,
    ) -> OpticalConvertResult<Self> {
        Ok(Tentative::new1(Self {
            scale: value.scale,
            gain: value.gain,
            wavelengths: value.wavelength.map(|x| x.into()),
            peak: value.peak,
            calibration: None.into(),
            display: None.into(),
        }))
    }
}

impl ConvertFromOptical<InnerOptical3_2> for InnerOptical3_1 {
    fn convert_from_optical(
        value: InnerOptical3_2,
        i: MeasIndex,
        force: bool,
    ) -> OpticalConvertResult<Self> {
        let j = i.into();
        let a = check_indexed_key_transfer_own(value.analyte, j, !force);
        let f = check_indexed_key_transfer_own(value.feature, j, !force);
        let m = check_indexed_key_transfer_own(value.measurement_type, j, !force);
        let t = check_indexed_key_transfer_own(value.tag, j, !force);
        let n = check_indexed_key_transfer_own(value.detector_name, j, !force);
        let dt = check_indexed_key_transfer_own(value.datatype, j, !force);
        let out = a.zip3(f, m).zip4(t, n, dt).inner_into().map(|_| Self {
            scale: value.scale,
            gain: value.gain,
            wavelengths: value.wavelengths,
            peak: PeakData::default(),
            display: value.display,
            // TODO warn offset might be lost here
            calibration: value.calibration.map(|x| x.into()),
        });
        Ok(out)
    }
}

impl ConvertFromOptical<InnerOptical2_0> for InnerOptical3_2 {
    fn convert_from_optical(
        value: InnerOptical2_0,
        i: MeasIndex,
        _: bool,
    ) -> OpticalConvertResult<Self> {
        // TODO warn peak data will be lost
        value
            .scale
            .0
            .ok_or(NoScaleError(i))
            .map(|scale| Self {
                scale,
                wavelengths: value.wavelength.map(|x| x.into()),
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
            .into_deferred()
    }
}

impl ConvertFromOptical<InnerOptical3_0> for InnerOptical3_2 {
    fn convert_from_optical(
        value: InnerOptical3_0,
        _: MeasIndex,
        _: bool,
    ) -> OpticalConvertResult<Self> {
        // TODO warn peak data will be lost
        Ok(Tentative::new1(Self {
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
        }))
    }
}

impl ConvertFromOptical<InnerOptical3_1> for InnerOptical3_2 {
    fn convert_from_optical(
        value: InnerOptical3_1,
        _: MeasIndex,
        _: bool,
    ) -> OpticalConvertResult<Self> {
        // TODO warn peak data will be lost
        Ok(Tentative::new1(Self {
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
        }))
    }
}

type MetarootConvertResult<M> = DeferredResult<M, MetarootConvertWarning, MetarootConvertError>;

type OpticalConvertResult<M> = DeferredResult<M, OpticalConvertWarning, OpticalConvertError>;

type TemporalConvertTentative<M> = Tentative<M, TemporalConvertError, TemporalConvertError>;

enum_from_disp!(
    pub OpticalConvertError,
    [NoScale, NoScaleError],
    [Wavelengths, WavelengthsLossError],
    [Xfer, AnyMeasKeyLossError]
);

enum_from_disp!(
    pub OpticalConvertWarning,
    [Wavelengths, WavelengthsLossError],
    [Xfer, AnyMeasKeyLossError]
);

enum_from_disp!(
    pub TemporalConvertError,
    [Timestep, TimestepLossError],
    [Xfer, AnyMeasKeyLossError]
);

pub struct TimestepLossError(Timestep);

impl fmt::Display for TimestepLossError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "$TIMESTEP is {} and will be 1.0 after conversion",
            self.0
        )
    }
}

pub struct SizeConvert<O> {
    size: O,
    datatype: AlphaNumType,
    widths: Vec<Width>,
}

type ByteOrdConvert = SizeConvert<ByteOrd>;
type EndianConvert = SizeConvert<Endian>;

impl EndianConvert {
    fn try_as_byteord(self) -> DeferredResult<ByteOrd, WidthToBytesError, SingleWidthError> {
        Width::matrix_bytes(&self.widths[..], self.datatype)
            .def_map_value(|bytes| self.size.as_bytord(bytes))
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_0> for InnerMetaroot2_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_0,
        _: ByteOrdConvert,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        // TODO warn if subset present
        let c = check_key_transfer(value.cytsn, lossless);
        let u = check_key_transfer(value.unicode, lossless);
        let ret = c.zip(u).inner_into().and_tentatively(|_| {
            value
                .applied_gates
                .0
                .map(|x| x.try_into())
                .transpose()
                .map_or_else(
                    |w| Tentative::new_either(None, vec![w], lossless),
                    Tentative::new1,
                )
                .map(|ag| Self {
                    mode: value.mode,
                    byteord: value.byteord,
                    cyt: value.cyt,
                    comp: value.comp.map(|x| x.0.into()),
                    timestamps: value.timestamps.map(|d| d.into()),
                    applied_gates: ag.into(),
                })
        });
        Ok(ret)
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_1> for InnerMetaroot2_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_1,
        endian: EndianConvert,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        // TODO warn if subset present
        let c = check_key_transfer(value.cytsn, lossless);
        let v = check_key_transfer(value.vol, lossless);
        let s = check_key_transfer(value.spillover, lossless);
        let m = ModificationData::check_loss(value.modification, lossless);
        let p = PlateData::check_loss(value.plate, lossless);
        c.zip5(v, s, m, p).inner_into().and_maybe(|_| {
            value
                .applied_gates
                .0
                .map(|x| x.try_into())
                .transpose()
                .map_or_else(
                    |w| Tentative::new_either(None, vec![w], lossless),
                    Tentative::new1,
                )
                .and_maybe(|ag| {
                    endian
                        .try_as_byteord()
                        .def_inner_into()
                        .def_map_value(|byteord| Self {
                            mode: value.mode,
                            byteord,
                            cyt: value.cyt,
                            comp: None.into(),
                            timestamps: value.timestamps.map(|d| d.into()),
                            applied_gates: ag.into(),
                        })
                })
        })
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_2> for InnerMetaroot2_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_2,
        endian: EndianConvert,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let cy = check_key_transfer(value.cytsn, lossless);
        let v = check_key_transfer(value.vol, lossless);
        let s = check_key_transfer(value.spillover, lossless);
        let f = check_key_transfer(value.flowrate, lossless);
        let m = ModificationData::check_loss(value.modification, lossless);
        let p = PlateData::check_loss(value.plate, lossless);
        let d = check_datetimes_keys_transfer(value.datetimes, lossless);
        let ca = CarrierData::check_loss(value.carrier, lossless);
        let u = UnstainedData::check_loss(value.unstained, lossless);
        let mut ret = cy
            .zip6(v, s, f, m, p)
            .zip4(d, ca, u)
            .inner_into()
            .and_maybe(|_| {
                endian
                    .try_as_byteord()
                    .def_inner_into()
                    .def_map_value(|byteord| Self {
                        mode: Mode::List,
                        byteord,
                        cyt: Some(value.cyt).into(),
                        comp: None.into(),
                        timestamps: value.timestamps.map(|x| x.into()),
                        applied_gates: None.into(),
                    })
            });
        if value.applied_gates.0.is_some() {
            ret.def_push_error_or_warning(AppliedGates3_2To2_0Error, lossless);
        }
        ret
    }
}

impl ConvertFromMetaroot<InnerMetaroot2_0> for InnerMetaroot3_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot2_0,
        _: ByteOrdConvert,
        _: bool,
    ) -> MetarootConvertResult<Self> {
        Ok(Tentative::new1(Self {
            mode: value.mode,
            byteord: value.byteord,
            cyt: value.cyt,
            comp: value.comp.map(|x| x.0.into()),
            timestamps: value.timestamps.map(|d| d.into()),
            cytsn: None.into(),
            unicode: None.into(),
            subset: None.into(),
            applied_gates: None.into(),
        }))
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_1> for InnerMetaroot3_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_1,
        endian: EndianConvert,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let p = PlateData::check_loss(value.plate, lossless);
        let m = ModificationData::check_loss(value.modification, lossless);
        let v = check_key_transfer(value.vol, lossless);
        p.zip3(m, v).inner_into().and_maybe(|_| {
            endian
                .try_as_byteord()
                .def_inner_into()
                .def_map_value(|byteord| Self {
                    mode: value.mode,
                    byteord,
                    cyt: value.cyt,
                    cytsn: value.cytsn,
                    timestamps: value.timestamps.map(|d| d.into()),
                    comp: None.into(),
                    unicode: None.into(),
                    subset: None.into(),
                    applied_gates: value.applied_gates,
                })
        })
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_2> for InnerMetaroot3_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_2,
        endian: EndianConvert,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let v = check_key_transfer(value.vol, lossless);
        let f = check_key_transfer(value.flowrate, lossless);
        let m = ModificationData::check_loss(value.modification, lossless);
        let p = PlateData::check_loss(value.plate, lossless);
        let d = check_datetimes_keys_transfer(value.datetimes, lossless);
        let ca = CarrierData::check_loss(value.carrier, lossless);
        let u = UnstainedData::check_loss(value.unstained, lossless);
        v.zip6(f, m, p, d, ca).zip(u).inner_into().and_maybe(|_| {
            endian
                .try_as_byteord()
                .def_inner_into()
                .def_map_value(|byteord| Self {
                    mode: Mode::List,
                    byteord,
                    cyt: Some(value.cyt).into(),
                    cytsn: value.cytsn,
                    timestamps: value.timestamps.map(|x| x.into()),
                    comp: None.into(),
                    unicode: None.into(),
                    subset: None.into(),
                    applied_gates: value.applied_gates.map(|x| x.into()),
                })
        })
    }
}

impl ConvertFromMetaroot<InnerMetaroot2_0> for InnerMetaroot3_1 {
    fn convert_from_metaroot(
        value: InnerMetaroot2_0,
        _: ByteOrdConvert,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let mut res = value
            .byteord
            .try_into()
            .into_deferred()
            .def_map_value(|byteord| Self {
                mode: value.mode,
                byteord,
                cyt: value.cyt,
                timestamps: value.timestamps.map(|d| d.into()),
                cytsn: None.into(),
                spillover: None.into(),
                modification: ModificationData::default(),
                plate: PlateData::default(),
                vol: None.into(),
                subset: None.into(),
                applied_gates: value.applied_gates.map(|x| x.into()),
            });
        if value.comp.0.is_some() {
            res.def_push_error_or_warning(Comp2_0TransferError, lossless);
        }
        res
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_0> for InnerMetaroot3_1 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_0,
        _: ByteOrdConvert,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let c = check_key_transfer(value.comp, lossless);
        let u = check_key_transfer(value.unicode, lossless);
        c.zip(u).inner_into().and_maybe(|_| {
            value
                .byteord
                .try_into()
                .into_deferred()
                .def_map_value(|byteord| Self {
                    byteord,
                    mode: value.mode,
                    cyt: value.cyt,
                    cytsn: value.cytsn,
                    timestamps: value.timestamps.map(|d| d.into()),
                    spillover: None.into(),
                    modification: ModificationData::default(),
                    plate: PlateData::default(),
                    vol: None.into(),
                    subset: value.subset,
                    applied_gates: value.applied_gates,
                })
        })
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_2> for InnerMetaroot3_1 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_2,
        _: EndianConvert,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let d = check_datetimes_keys_transfer(value.datetimes, lossless);
        let ca = CarrierData::check_loss(value.carrier, lossless);
        let u = UnstainedData::check_loss(value.unstained, lossless);
        let f = check_key_transfer(value.flowrate, lossless);
        let ret = d.zip4(ca, u, f).inner_into().and_tentatively(|_| {
            Tentative::new1(Self {
                mode: Mode::List,
                byteord: value.byteord,
                cyt: Some(value.cyt).into(),
                cytsn: value.cytsn,
                timestamps: value.timestamps,
                spillover: value.spillover,
                plate: value.plate,
                modification: value.modification,
                vol: value.vol,
                subset: None.into(),
                applied_gates: value.applied_gates.map(|x| x.into()),
            })
        });
        Ok(ret)
    }
}

impl ConvertFromMetaroot<InnerMetaroot2_0> for InnerMetaroot3_2 {
    fn convert_from_metaroot(
        value: InnerMetaroot2_0,
        _: ByteOrdConvert,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let b = value.byteord.try_into().into_deferred();
        let c = value.cyt.0.ok_or(NoCytError).into_deferred();
        let mut res = b.def_zip(c).def_map_value(|(byteord, cyt)| Self {
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
            applied_gates: None.into(),
        });
        if value.applied_gates.0.is_some() {
            res.def_push_error_or_warning(AppliedGates2_0To3_2Error, lossless);
        }
        if value.comp.0.is_some() {
            res.def_push_error_or_warning(Comp2_0TransferError, lossless);
        }
        if value.mode != Mode::List {
            res.def_push_error_or_warning(ModeNotListError, lossless);
        }
        res
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_0> for InnerMetaroot3_2 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_0,
        _: ByteOrdConvert,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        // TODO warn if subset present
        let u = check_key_transfer(value.unicode, lossless);
        let co = check_key_transfer(value.comp, lossless);
        let mut res = u.zip(co).inner_into().and_maybe(|_| {
            value
                .applied_gates
                .0
                .map(|x| x.try_into())
                .transpose()
                .map_or_else(
                    |w| Tentative::new_either(None, vec![w], lossless),
                    Tentative::new1,
                )
                .and_maybe(|ag| {
                    let b = value.byteord.try_into().into_deferred();
                    let c = value.cyt.0.ok_or(NoCytError).into_deferred();
                    b.def_zip(c).def_map_value(|(byteord, cyt)| Self {
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
                        applied_gates: ag.into(),
                    })
                })
        });
        if value.mode != Mode::List {
            res.def_push_error_or_warning(ModeNotListError, lossless);
        }
        res
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_1> for InnerMetaroot3_2 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_1,
        _: EndianConvert,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        // TODO warn if subset present
        let mut res = value
            .applied_gates
            .0
            .map(|x| x.try_into())
            .transpose()
            .map_or_else(
                |w| Tentative::new_either(None, vec![w], lossless),
                Tentative::new1,
            )
            .and_maybe(|ag| {
                value
                    .cyt
                    .0
                    .ok_or(NoCytError)
                    .into_deferred()
                    .def_map_value(|cyt| Self {
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
                        applied_gates: ag.into(),
                    })
            });
        if value.mode != Mode::List {
            res.def_push_error_or_warning(ModeNotListError, lossless);
        }
        res
    }
}

fn check_indexed_key_transfer<T, E>(x: &OptionalKw<T>, i: IndexFromOne) -> Result<(), E>
where
    E: From<IndexedKeyLossError<T>>,
{
    if x.0.is_some() {
        Err(IndexedKeyLossError::<T>::new(i).into())
    } else {
        Ok(())
    }
}

fn check_optical_keys_transfer<X>(
    x: &Optical<X>,
    i: MeasIndex,
) -> MultiResult<(), AnyMeasKeyLossError> {
    let j = i.into();
    let f = check_indexed_key_transfer(&x.filter, j);
    let o = check_indexed_key_transfer(&x.power, j);
    let t = check_indexed_key_transfer(&x.detector_type, j);
    let p = check_indexed_key_transfer(&x.percent_emitted, j);
    let v = check_indexed_key_transfer(&x.detector_voltage, j);
    f.zip3(o, t).mult_zip(p.zip(v)).map(|_| ())
}

fn check_indexed_key_transfer_own<T>(
    x: OptionalKw<T>,
    i: IndexFromOne,
    lossless: bool,
) -> Tentative<(), AnyMeasKeyLossError, AnyMeasKeyLossError>
where
    AnyMeasKeyLossError: From<IndexedKeyLossError<T>>,
{
    let mut tnt = Tentative::new1(());
    if x.0.is_some() {
        tnt.push_error_or_warning(IndexedKeyLossError::<T>::new(i), lossless);
    }
    tnt
}

fn check_key_transfer<T>(
    x: OptionalKw<T>,
    lossless: bool,
) -> Tentative<(), AnyMetarootKeyLossError, AnyMetarootKeyLossError>
where
    AnyMetarootKeyLossError: From<UnitaryKeyLossError<T>>,
{
    let mut tnt = Tentative::new1(());
    if x.0.is_some() {
        tnt.push_error_or_warning(UnitaryKeyLossError::<T>::default(), lossless);
    }
    tnt
}

fn check_datetimes_keys_transfer(
    d: Datetimes,
    lossless: bool,
) -> Tentative<(), AnyMetarootKeyLossError, AnyMetarootKeyLossError> {
    let mut tnt = Tentative::new1(());
    if d.begin_naive().is_some() {
        tnt.push_error_or_warning(UnitaryKeyLossError::<BeginDateTime>::default(), lossless);
    }
    if d.end_naive().is_some() {
        tnt.push_error_or_warning(UnitaryKeyLossError::<EndDateTime>::default(), lossless);
    }
    tnt
}

fn check_timestep(x: Timestep, force: bool) -> TemporalConvertTentative<()> {
    let mut tnt = Tentative::new1(());
    if f32::from(x.0) != 1.0 {
        tnt.push_error_or_warning(TimestepLossError(x), !force);
    }
    tnt
}

impl ConvertFromTemporal<InnerTemporal3_0> for InnerTemporal2_0 {
    fn convert_from_temporal(
        value: InnerTemporal3_0,
        _: MeasIndex,
        force: bool,
    ) -> TemporalConvertTentative<Self> {
        check_timestep(value.timestep, force).map(|_| Self {
            peak: value.peak,
            scale: Some(TemporalScale).into(),
        })
    }
}

impl ConvertFromTemporal<InnerTemporal3_1> for InnerTemporal2_0 {
    fn convert_from_temporal(
        value: InnerTemporal3_1,
        i: MeasIndex,
        force: bool,
    ) -> TemporalConvertTentative<Self> {
        let t = check_timestep(value.timestep, force);
        let d = check_indexed_key_transfer_own(value.display, i.into(), !force).inner_into();
        t.zip(d).map(|_| Self {
            peak: value.peak,
            scale: Some(TemporalScale).into(),
        })
    }
}

impl ConvertFromTemporal<InnerTemporal3_2> for InnerTemporal2_0 {
    fn convert_from_temporal(
        value: InnerTemporal3_2,
        i: MeasIndex,
        force: bool,
    ) -> TemporalConvertTentative<Self> {
        let j = i.into();
        let dt = check_indexed_key_transfer_own(value.datatype, j, !force);
        let di = check_indexed_key_transfer_own(value.display, j, !force);
        let m = check_indexed_key_transfer_own(value.measurement_type, j, !force);
        let t = check_timestep(value.timestep, force);
        // TODO warn peak
        dt.zip3(di, m).inner_into().zip(t).map(|_| Self {
            peak: PeakData::default(),
            scale: Some(TemporalScale).into(),
        })
    }
}

impl ConvertFromTemporal<InnerTemporal2_0> for InnerTemporal3_0 {
    fn convert_from_temporal(
        value: InnerTemporal2_0,
        _: MeasIndex,
        _: bool,
    ) -> TemporalConvertTentative<Self> {
        Tentative::new1(Self {
            timestep: Timestep::default(),
            peak: value.peak,
        })
    }
}

impl ConvertFromTemporal<InnerTemporal3_1> for InnerTemporal3_0 {
    fn convert_from_temporal(
        value: InnerTemporal3_1,
        i: MeasIndex,
        force: bool,
    ) -> TemporalConvertTentative<Self> {
        check_indexed_key_transfer_own(value.display, i.into(), !force)
            .inner_into()
            .map(|_| Self {
                timestep: value.timestep,
                peak: value.peak,
            })
    }
}

impl ConvertFromTemporal<InnerTemporal3_2> for InnerTemporal3_0 {
    fn convert_from_temporal(
        value: InnerTemporal3_2,
        i: MeasIndex,
        force: bool,
    ) -> TemporalConvertTentative<Self> {
        let j = i.into();
        let dt = check_indexed_key_transfer_own(value.datatype, j, !force);
        let di = check_indexed_key_transfer_own(value.display, j, !force);
        let m = check_indexed_key_transfer_own(value.measurement_type, j, !force);
        // TODO warn peak
        dt.zip3(di, m).inner_into().map(|_| Self {
            timestep: value.timestep,
            peak: PeakData::default(),
        })
    }
}

impl ConvertFromTemporal<InnerTemporal2_0> for InnerTemporal3_1 {
    fn convert_from_temporal(
        value: InnerTemporal2_0,
        _: MeasIndex,
        _: bool,
    ) -> TemporalConvertTentative<Self> {
        Tentative::new1(Self {
            timestep: Timestep::default(),
            display: None.into(),
            peak: value.peak,
        })
    }
}

impl ConvertFromTemporal<InnerTemporal3_0> for InnerTemporal3_1 {
    fn convert_from_temporal(
        value: InnerTemporal3_0,
        _: MeasIndex,
        _: bool,
    ) -> TemporalConvertTentative<Self> {
        Tentative::new1(Self {
            timestep: value.timestep,
            display: None.into(),
            peak: value.peak,
        })
    }
}

impl ConvertFromTemporal<InnerTemporal3_2> for InnerTemporal3_1 {
    fn convert_from_temporal(
        value: InnerTemporal3_2,
        i: MeasIndex,
        force: bool,
    ) -> TemporalConvertTentative<Self> {
        let j = i.into();
        let dt = check_indexed_key_transfer_own(value.datatype, j, !force);
        let m = check_indexed_key_transfer_own(value.measurement_type, j, !force);
        // TODO warn peak
        dt.zip(m).inner_into().map(|_| Self {
            timestep: value.timestep,
            display: value.display,
            peak: PeakData::default(),
        })
    }
}

impl ConvertFromTemporal<InnerTemporal2_0> for InnerTemporal3_2 {
    fn convert_from_temporal(
        _: InnerTemporal2_0,
        _: MeasIndex,
        _: bool,
    ) -> TemporalConvertTentative<Self> {
        // TODO warn peak
        Tentative::new1(Self {
            timestep: Timestep::default(),
            display: None.into(),
            measurement_type: None.into(),
            datatype: None.into(),
        })
    }
}

impl ConvertFromTemporal<InnerTemporal3_0> for InnerTemporal3_2 {
    fn convert_from_temporal(
        value: InnerTemporal3_0,
        _: MeasIndex,
        _: bool,
    ) -> TemporalConvertTentative<Self> {
        // TODO warn peak
        Tentative::new1(Self {
            timestep: value.timestep,
            display: None.into(),
            measurement_type: None.into(),
            datatype: None.into(),
        })
    }
}

impl ConvertFromTemporal<InnerTemporal3_1> for InnerTemporal3_2 {
    fn convert_from_temporal(
        value: InnerTemporal3_1,
        _: MeasIndex,
        _: bool,
    ) -> TemporalConvertTentative<Self> {
        // TODO warn peak
        Tentative::new1(Self {
            timestep: value.timestep,
            display: value.display,
            measurement_type: None.into(),
            datatype: None.into(),
        })
    }
}

impl Versioned for InnerOptical2_0 {
    fn fcs_version() -> Version {
        Version::FCS2_0
    }
}

impl Versioned for InnerOptical3_0 {
    fn fcs_version() -> Version {
        Version::FCS3_0
    }
}

impl Versioned for InnerOptical3_1 {
    fn fcs_version() -> Version {
        Version::FCS3_1
    }
}

impl Versioned for InnerOptical3_2 {
    fn fcs_version() -> Version {
        Version::FCS3_2
    }
}

impl LookupOptical for InnerOptical2_0 {
    fn lookup_specific(kws: &mut StdKeywords, i: MeasIndex) -> LookupResult<Self> {
        let s = lookup_indexed_opt(kws, i.into(), false);
        let w = lookup_indexed_opt(kws, i.into(), false);
        let p = PeakData::lookup(kws, i, false);
        Ok(s.zip3(w, p).map(|(scale, wavelength, peak)| Self {
            scale,
            wavelength,
            peak,
        }))
    }
}

impl LookupOptical for InnerOptical3_0 {
    fn lookup_specific(kws: &mut StdKeywords, i: MeasIndex) -> LookupResult<Self> {
        let g = lookup_indexed_opt(kws, i.into(), false);
        let w = lookup_indexed_opt(kws, i.into(), false);
        let p = PeakData::lookup(kws, i, false);
        g.zip3(w, p).and_maybe(|(gain, wavelength, peak)| {
            lookup_indexed_req(kws, i.into()).def_map_value(|scale| Self {
                scale,
                gain,
                wavelength,
                peak,
            })
        })
    }
}

impl LookupOptical for InnerOptical3_1 {
    fn lookup_specific(kws: &mut StdKeywords, i: MeasIndex) -> LookupResult<Self> {
        let g = lookup_indexed_opt(kws, i.into(), false);
        let w = lookup_indexed_opt(kws, i.into(), false);
        let c = lookup_indexed_opt(kws, i.into(), false);
        let d = lookup_indexed_opt(kws, i.into(), false);
        let p = PeakData::lookup(kws, i, true);
        g.zip5(w, c, d, p)
            .and_maybe(|(gain, wavelengths, calibration, display, peak)| {
                lookup_indexed_req(kws, i.into()).def_map_value(|scale| Self {
                    scale,
                    gain,
                    wavelengths,
                    calibration,
                    display,
                    peak,
                })
            })
    }
}

impl LookupOptical for InnerOptical3_2 {
    fn lookup_specific(kws: &mut StdKeywords, i: MeasIndex) -> LookupResult<Self> {
        let g = lookup_indexed_opt(kws, i.into(), false);
        let w = lookup_indexed_opt(kws, i.into(), false);
        let c = lookup_indexed_opt(kws, i.into(), false);
        let d = lookup_indexed_opt(kws, i.into(), false);
        let de = lookup_indexed_opt(kws, i.into(), false);
        let ta = lookup_indexed_opt(kws, i.into(), false);
        let m = lookup_indexed_opt(kws, i.into(), false);
        let f = lookup_indexed_opt(kws, i.into(), false);
        let a = lookup_indexed_opt(kws, i.into(), false);
        let da = lookup_indexed_opt(kws, i.into(), false);
        g.zip5(w, c, d, de).zip5(ta, m, f, a).zip(da).and_maybe(
            |(
                (
                    (gain, wavelengths, calibration, display, detector_name),
                    tag,
                    measurement_type,
                    feature,
                    analyte,
                ),
                datatype,
            )| {
                lookup_indexed_req(kws, i.into()).def_map_value(|scale| Self {
                    scale,
                    gain,
                    wavelengths,
                    calibration,
                    display,
                    detector_name,
                    tag,
                    measurement_type,
                    feature,
                    analyte,
                    datatype,
                })
            },
        )
    }
}

impl LookupTemporal for InnerTemporal2_0 {
    fn lookup_specific(kws: &mut StdKeywords, i: MeasIndex) -> LookupResult<Self> {
        // TODO push meas index with error
        let s = lookup_indexed_opt(kws, i.into(), false);
        let p = PeakData::lookup(kws, i, false);
        Ok(s.zip(p).map(|(scale, peak)| Self { peak, scale }))
    }
}

impl LookupTemporal for InnerTemporal3_0 {
    fn lookup_specific(kws: &mut StdKeywords, i: MeasIndex) -> LookupResult<Self> {
        let mut tnt_gain = lookup_indexed_opt::<Gain, LookupKeysError>(kws, i.into(), false);
        tnt_gain.eval_error(|gain| {
            if gain.0.is_some() {
                Some(LookupKeysError::Misc(TemporalError::HasGain.into()))
            } else {
                None
            }
        });
        let tnt_peak = PeakData::lookup(kws, i, false);
        tnt_gain.zip(tnt_peak).and_maybe(|(_, peak)| {
            let s = lookup_indexed_req::<TemporalScale>(kws, i.into());
            let t = lookup_meta_req(kws);
            s.def_zip(t)
                .def_map_value(|(_, timestep)| Self { timestep, peak })
        })
    }
}

impl LookupTemporal for InnerTemporal3_1 {
    fn lookup_specific(kws: &mut StdKeywords, i: MeasIndex) -> LookupResult<Self> {
        let g = lookup_temporal_gain_3_0(kws, i.into());
        let d = lookup_indexed_opt(kws, i.into(), false);
        let p = PeakData::lookup(kws, i, true);
        g.zip3(d, p).and_maybe(|(_, display, peak)| {
            let s = lookup_indexed_req::<TemporalScale>(kws, i.into());
            let t = lookup_meta_req(kws);
            s.def_zip(t).def_map_value(|(_, timestep)| Self {
                timestep,
                display,
                peak,
            })
        })
    }
}

impl LookupTemporal for InnerTemporal3_2 {
    fn lookup_specific(kws: &mut StdKeywords, i: MeasIndex) -> LookupResult<Self> {
        let g = lookup_temporal_gain_3_0(kws, i.into());
        let di = lookup_indexed_opt(kws, i.into(), false);
        let m = lookup_indexed_opt(kws, i.into(), false);
        let da = lookup_indexed_opt(kws, i.into(), false);
        g.zip4(di, m, da)
            .and_maybe(|(_, display, measurement_type, datatype)| {
                let s = lookup_indexed_req::<TemporalScale>(kws, i.into());
                let t = lookup_meta_req(kws);
                s.def_zip(t).def_map_value(|(_, timestep)| Self {
                    timestep,
                    display,
                    measurement_type,
                    datatype,
                })
            })
    }
}

impl VersionedOptical for InnerOptical2_0 {
    fn datatype(&self) -> Option<NumType> {
        None
    }

    fn req_suffixes_inner(&self, _: MeasIndex) -> RawTriples {
        vec![]
    }

    fn opt_suffixes_inner(&self, i: MeasIndex) -> RawOptTriples {
        [
            OptIndexedKey::triple(&self.scale, i.into()),
            OptIndexedKey::triple(&self.wavelength, i.into()),
        ]
        .into_iter()
        .chain(self.peak.opt_keywords(i))
        .collect()
    }

    fn can_convert_to_temporal(&self, i: MeasIndex) -> MultiResult<(), OpticalToTemporalError> {
        let mut res = check_indexed_key_transfer(&self.wavelength, i.into())
            .map_err(OpticalToTemporalError::Xfer)
            .into_mult();
        if let Err(err) = res.as_mut() {
            if !self.scale.as_ref_opt().is_some_and(|s| *s == Scale::Linear) {
                err.push(OpticalNonLinearError.into());
            }
        }
        res
    }
}

impl VersionedOptical for InnerOptical3_0 {
    fn datatype(&self) -> Option<NumType> {
        None
    }

    fn req_suffixes_inner(&self, i: MeasIndex) -> RawTriples {
        [self.scale.triple(i.into())].into_iter().collect()
    }

    fn opt_suffixes_inner(&self, i: MeasIndex) -> RawOptTriples {
        [
            OptIndexedKey::triple(&self.wavelength, i.into()),
            OptIndexedKey::triple(&self.gain, i.into()),
        ]
        .into_iter()
        .chain(self.peak.opt_keywords(i))
        .collect()
    }

    fn can_convert_to_temporal(&self, i: MeasIndex) -> MultiResult<(), OpticalToTemporalError> {
        let mut res = check_indexed_key_transfer(&self.wavelength, i.into())
            .map_err(OpticalToTemporalError::Xfer)
            .into_mult();
        if let Err(err) = res.as_mut() {
            if self.scale != Scale::Linear {
                err.push(OpticalNonLinearError.into());
            }
            if self.gain.0.is_some() {
                err.push(OpticalHasGainError.into());
            }
        }
        res
    }
}

impl VersionedOptical for InnerOptical3_1 {
    fn datatype(&self) -> Option<NumType> {
        None
    }

    fn req_suffixes_inner(&self, i: MeasIndex) -> RawTriples {
        [self.scale.triple(i.into())].into_iter().collect()
    }

    fn opt_suffixes_inner(&self, i: MeasIndex) -> RawOptTriples {
        [
            OptIndexedKey::triple(&self.wavelengths, i.into()),
            OptIndexedKey::triple(&self.gain, i.into()),
            OptIndexedKey::triple(&self.calibration, i.into()),
            OptIndexedKey::triple(&self.display, i.into()),
        ]
        .into_iter()
        .chain(self.peak.opt_keywords(i))
        .collect()
    }

    fn can_convert_to_temporal(&self, i: MeasIndex) -> MultiResult<(), OpticalToTemporalError> {
        let j = i.into();
        let c = check_indexed_key_transfer::<_, AnyMeasKeyLossError>(&self.calibration, j);
        let w = check_indexed_key_transfer(&self.wavelengths, j);
        let mut res = c.zip(w).mult_errors_into().void();
        if let Err(err) = res.as_mut() {
            if self.scale != Scale::Linear {
                err.push(OpticalNonLinearError.into());
            }
            if self.gain.0.is_some() {
                err.push(OpticalHasGainError.into());
            }
        }
        res
    }
}

impl VersionedOptical for InnerOptical3_2 {
    fn datatype(&self) -> Option<NumType> {
        self.datatype.0.as_ref().copied()
    }

    fn req_suffixes_inner(&self, i: MeasIndex) -> RawTriples {
        [self.scale.triple(i.into())].into_iter().collect()
    }

    fn opt_suffixes_inner(&self, i: MeasIndex) -> RawOptTriples {
        [
            OptIndexedKey::triple(&self.wavelengths, i.into()),
            OptIndexedKey::triple(&self.gain, i.into()),
            OptIndexedKey::triple(&self.calibration, i.into()),
            OptIndexedKey::triple(&self.display, i.into()),
            OptIndexedKey::triple(&self.detector_name, i.into()),
            OptIndexedKey::triple(&self.tag, i.into()),
            OptIndexedKey::triple(&self.measurement_type, i.into()),
            OptIndexedKey::triple(&self.feature, i.into()),
            OptIndexedKey::triple(&self.analyte, i.into()),
            OptIndexedKey::triple(&self.datatype, i.into()),
        ]
        .into_iter()
        .collect()
    }

    fn can_convert_to_temporal(&self, i: MeasIndex) -> MultiResult<(), OpticalToTemporalError> {
        let j = i.into();
        let c = check_indexed_key_transfer::<_, AnyMeasKeyLossError>(&self.calibration, j);
        let w = check_indexed_key_transfer(&self.wavelengths, j);
        let m = check_indexed_key_transfer(&self.measurement_type, j);
        let a = check_indexed_key_transfer(&self.analyte, j);
        let t = check_indexed_key_transfer(&self.tag, j);
        let n = check_indexed_key_transfer(&self.detector_name, j);
        let f = check_indexed_key_transfer(&self.feature, j);
        let mut res = c
            .zip3(w, m)
            .mult_zip3(a.zip(t), n.zip(f))
            .mult_errors_into()
            .void();
        if let Err(err) = res.as_mut() {
            if self.scale != Scale::Linear {
                err.push(OpticalNonLinearError.into())
            }
            if self.gain.0.is_some() {
                err.push(OpticalHasGainError.into())
            }
        }
        res
    }
}

impl VersionedTemporal for InnerTemporal2_0 {
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

    fn opt_meas_keywords_inner(&self, _: MeasIndex) -> RawOptPairs {
        vec![]
    }

    fn can_convert_to_optical(&self, _: MeasIndex) -> MultiResult<(), TemporalToOpticalError> {
        Ok(())
    }
}

impl VersionedTemporal for InnerTemporal3_0 {
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

    fn opt_meas_keywords_inner(&self, _: MeasIndex) -> RawOptPairs {
        vec![]
    }

    fn can_convert_to_optical(&self, _: MeasIndex) -> MultiResult<(), TemporalToOpticalError> {
        Ok(())
    }
}

impl VersionedTemporal for InnerTemporal3_1 {
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

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> RawOptPairs {
        [OptIndexedKey::pair(&self.display, i.into())]
            .into_iter()
            .collect()
    }

    fn can_convert_to_optical(&self, _: MeasIndex) -> MultiResult<(), TemporalToOpticalError> {
        Ok(())
    }
}

impl VersionedTemporal for InnerTemporal3_2 {
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

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> RawOptPairs {
        [
            OptIndexedKey::pair(&self.display, i.into()),
            OptIndexedKey::pair(&self.datatype, i.into()),
        ]
        .into_iter()
        .collect()
    }

    fn can_convert_to_optical(&self, i: MeasIndex) -> MultiResult<(), TemporalToOpticalError> {
        check_indexed_key_transfer(&self.measurement_type, i.into()).map_err(NonEmpty::new)
    }
}

impl OpticalFromTemporal<InnerTemporal2_0> for InnerOptical2_0 {
    type TData = ();

    fn from_temporal_inner(t: InnerTemporal2_0) -> (Self, Self::TData) {
        let new = Self {
            scale: Some(Scale::Linear).into(),
            peak: t.peak,
            wavelength: None.into(),
        };
        (new, ())
    }
}

impl OpticalFromTemporal<InnerTemporal3_0> for InnerOptical3_0 {
    type TData = Timestep;

    fn from_temporal_inner(t: InnerTemporal3_0) -> (Self, Self::TData) {
        let new = Self {
            scale: Scale::Linear,
            peak: t.peak,
            wavelength: None.into(),
            gain: None.into(),
        };
        (new, t.timestep)
    }
}

impl OpticalFromTemporal<InnerTemporal3_1> for InnerOptical3_1 {
    type TData = Timestep;

    fn from_temporal_inner(t: InnerTemporal3_1) -> (Self, Self::TData) {
        let new = Self {
            scale: Scale::Linear,
            peak: t.peak,
            wavelengths: None.into(),
            gain: None.into(),
            calibration: None.into(),
            display: t.display,
        };
        (new, t.timestep)
    }
}

impl OpticalFromTemporal<InnerTemporal3_2> for InnerOptical3_2 {
    type TData = Timestep;

    fn from_temporal_inner(t: InnerTemporal3_2) -> (Self, Self::TData) {
        let new = Self {
            scale: Scale::Linear,
            wavelengths: None.into(),
            gain: None.into(),
            calibration: None.into(),
            display: t.display,
            analyte: None.into(),
            feature: None.into(),
            measurement_type: None.into(),
            tag: None.into(),
            detector_name: None.into(),
            datatype: t.datatype,
        };
        (new, t.timestep)
    }
}

impl TemporalFromOptical<InnerOptical2_0> for InnerTemporal2_0 {
    type TData = ();

    fn from_optical_inner(o: InnerOptical2_0, _: Self::TData) -> Self {
        Self {
            peak: o.peak,
            scale: o.scale.map(|_| TemporalScale),
        }
    }
}

impl TemporalFromOptical<InnerOptical3_0> for InnerTemporal3_0 {
    type TData = Timestep;

    fn from_optical_inner(t: InnerOptical3_0, timestep: Self::TData) -> Self {
        Self {
            timestep,
            peak: t.peak,
        }
    }
}

impl TemporalFromOptical<InnerOptical3_1> for InnerTemporal3_1 {
    type TData = Timestep;

    fn from_optical_inner(t: InnerOptical3_1, timestep: Self::TData) -> Self {
        Self {
            timestep,
            peak: t.peak,
            display: t.display,
        }
    }
}

impl TemporalFromOptical<InnerOptical3_2> for InnerTemporal3_2 {
    type TData = Timestep;

    fn from_optical_inner(t: InnerOptical3_2, timestep: Self::TData) -> Self {
        Self {
            timestep,
            display: t.display,
            measurement_type: None.into(),
            datatype: t.datatype,
        }
    }
}

#[derive(Clone, Copy)]
pub enum MixedColumnSetter {
    // ASSUME this won't have NaNs
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

impl LookupMetaroot for InnerMetaroot2_0 {
    fn lookup_shortname(
        kws: &mut StdKeywords,
        i: MeasIndex,
    ) -> LookupResult<<Self::N as MightHave>::Wrapper<Shortname>> {
        Ok(lookup_indexed_opt(kws, i.into(), false))
    }

    fn lookup_specific(kws: &mut StdKeywords, par: Par) -> LookupResult<Self> {
        let co = Compensation2_0::lookup(kws, par);
        let cy = lookup_meta_opt(kws, false);
        let t = Timestamps::lookup(kws, false);
        let g = AppliedGates2_0::lookup(kws);
        co.zip4(cy, t, g)
            .and_maybe(|(comp, cyt, timestamps, applied_gates)| {
                let b = lookup_meta_req(kws);
                let m = lookup_meta_req(kws);
                b.def_zip(m).def_map_value(|(byteord, mode)| Self {
                    mode,
                    byteord,
                    cyt,
                    comp,
                    timestamps,
                    applied_gates,
                })
            })
    }
}

impl LookupMetaroot for InnerMetaroot3_0 {
    fn lookup_shortname(
        kws: &mut StdKeywords,
        i: MeasIndex,
    ) -> LookupResult<<Self::N as MightHave>::Wrapper<Shortname>> {
        Ok(lookup_indexed_opt(kws, i.into(), false))
    }

    fn lookup_specific(kws: &mut StdKeywords, _: Par) -> LookupResult<Self> {
        let co = lookup_meta_opt(kws, false);
        let cy = lookup_meta_opt(kws, false);
        let sn = lookup_meta_opt(kws, false);
        let su = SubsetData::lookup(kws, false);
        let t = Timestamps::lookup(kws, false);
        let u = lookup_meta_opt(kws, false);
        let g = AppliedGates3_0::lookup(kws, false);
        co.zip4(cy, sn, su).zip4(t, u, g).and_maybe(
            |((comp, cyt, cytsn, subset), timestamps, unicode, applied_gates)| {
                let b = lookup_meta_req(kws);
                let m = lookup_meta_req(kws);
                b.def_zip(m).def_map_value(|(byteord, mode)| Self {
                    mode,
                    byteord,
                    cyt,
                    cytsn,
                    comp,
                    timestamps,
                    unicode,
                    subset,
                    applied_gates,
                })
            },
        )
    }
}

impl LookupMetaroot for InnerMetaroot3_1 {
    fn lookup_shortname(
        kws: &mut StdKeywords,
        i: MeasIndex,
    ) -> LookupResult<<Self::N as MightHave>::Wrapper<Shortname>> {
        lookup_indexed_req(kws, i.into()).map(|x| x.map(Identity))
    }

    fn lookup_specific(kws: &mut StdKeywords, _: Par) -> LookupResult<Self> {
        let cy = lookup_meta_opt(kws, false);
        let sp = lookup_meta_opt(kws, false);
        let sn = lookup_meta_opt(kws, false);
        let su = SubsetData::lookup(kws, true);
        let md = ModificationData::lookup(kws);
        let p = PlateData::lookup(kws, false);
        let t = Timestamps::lookup(kws, false);
        let v = lookup_meta_opt(kws, false);
        let g = AppliedGates3_0::lookup(kws, true);
        cy.zip5(sp, sn, su, md).zip5(p, t, v, g).and_maybe(
            |(
                (cyt, spillover, cytsn, subset, modification),
                plate,
                timestamps,
                vol,
                applied_gates,
            )| {
                let b = lookup_meta_req(kws);
                let mut mo = lookup_meta_req(kws);
                mo.def_eval_warning(|mode| match mode {
                    Mode::Correlated => {
                        Some(DeprecatedError::Value(DepValueWarning::ModeCorrelated).into())
                    }
                    Mode::Uncorrelated => {
                        Some(DeprecatedError::Value(DepValueWarning::ModeUncorrelated).into())
                    }
                    Mode::List => None,
                });
                b.def_zip(mo).def_map_value(|(byteord, mode)| Self {
                    mode,
                    byteord,
                    cyt,
                    cytsn,
                    vol,
                    spillover,
                    modification,
                    timestamps,
                    plate,
                    subset,
                    applied_gates,
                })
            },
        )
    }
}

impl LookupMetaroot for InnerMetaroot3_2 {
    fn lookup_shortname(
        kws: &mut StdKeywords,
        i: MeasIndex,
    ) -> LookupResult<<Self::N as MightHave>::Wrapper<Shortname>> {
        lookup_indexed_req(kws, i.into()).map(|x| x.map(Identity))
    }

    fn lookup_specific(kws: &mut StdKeywords, _: Par) -> LookupResult<Self> {
        let ca = CarrierData::lookup(kws);
        let d = Datetimes::lookup(kws);
        let f = lookup_meta_opt(kws, false);
        let md = ModificationData::lookup(kws);
        // Only L is allowed as of 3.2, so pull the value and check it if given.
        // The only thing we care about is that the value is valid, since we
        // don't need to use it anywhere.
        let mo = lookup_meta_opt::<Mode3_2, LookupKeysError>(kws, true);
        let sp = lookup_meta_opt(kws, false);
        let sn = lookup_meta_opt(kws, false);
        let p = PlateData::lookup(kws, true);
        let t = Timestamps::lookup(kws, false);
        let u = UnstainedData::lookup(kws);
        let v = lookup_meta_opt(kws, false);
        let g = AppliedGates3_2::lookup(kws);
        ca.zip6(d, f, md, mo, sp)
            .zip6(sn, p, t, u, v)
            .zip(g)
            .and_maybe(
                |(
                    (
                        (carrier, datetimes, flowrate, modification, _, spillover),
                        cytsn,
                        plate,
                        timestamps,
                        unstained,
                        vol,
                    ),
                    applied_gates,
                )| {
                    let b = lookup_meta_req(kws);
                    let c = lookup_meta_req(kws);
                    b.def_zip(c).def_map_value(|(byteord, cyt)| Self {
                        byteord,
                        cyt,
                        cytsn,
                        vol,
                        spillover,
                        modification,
                        timestamps,
                        plate,
                        carrier,
                        datetimes,
                        flowrate,
                        unstained,
                        applied_gates,
                    })
                },
            )
    }
}

impl VersionedMetaroot for InnerMetaroot2_0 {
    type O = InnerOptical2_0;
    type T = InnerTemporal2_0;
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
        self.comp.as_ref_opt().map(|x| x.borrow())
    }

    fn with_compensation<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut Compensation) -> Result<X, ClearOptional>,
    {
        self.comp.mut_or_unset(|c| f(&mut c.0))
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

    // TODO use iterators for all this string stuff
    fn keywords_opt_inner(&self) -> RawPairs {
        [OptMetaKey::pair(&self.cyt)]
            .into_iter()
            .chain(self.timestamps.opt_keywords())
            .chain(
                self.applied_gates
                    .as_ref_opt()
                    .map(|x| x.opt_keywords())
                    .unwrap_or_default(),
            )
            .flat_map(|(k, v)| v.map(|x| (k, x)))
            .chain(self.comp.as_ref_opt().map_or(vec![], |c| c.opt_keywords()))
            .collect()
    }

    fn as_data_layout(
        metaroot: &Metaroot<Self>,
        ms: &Measurements<Self::N, Self::T, Self::O>,
        conf: &SharedConfig,
    ) -> DeferredResult<Self::L, NewDataLayoutWarning, NewDataLayoutError> {
        Self::L::try_new(
            metaroot.datatype,
            metaroot.specific.byteord.clone(),
            ms.layout_data(),
            conf,
        )
    }

    fn swap_optical_temporal_inner(old_t: Self::T, old_o: Self::O) -> (Self::O, Self::T) {
        let new_t = Self::T {
            peak: old_o.peak,
            scale: old_o.scale.map(|_| TemporalScale),
        };
        let new_o = Self::O {
            peak: old_t.peak,
            scale: old_t.scale.map(|_| Scale::Linear),
            wavelength: None.into(),
        };
        (new_o, new_t)
    }
}

impl VersionedMetaroot for InnerMetaroot3_0 {
    type O = InnerOptical3_0;
    type T = InnerTemporal3_0;
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
        self.comp.as_ref_opt().map(|x| x.borrow())
    }

    fn with_compensation<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut Compensation) -> Result<X, ClearOptional>,
    {
        self.comp.mut_or_unset(|c| f(&mut c.0))
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
            OptMetaKey::pair(&self.cytsn),
            OptMetaKey::pair(&self.unicode),
        ]
        .into_iter()
        .chain(self.timestamps.opt_keywords())
        .chain(
            self.applied_gates
                .as_ref_opt()
                .map(|x| x.opt_keywords())
                .unwrap_or_default(),
        )
        .chain(
            self.subset
                .as_ref_opt()
                .map(|x| x.opt_keywords())
                .unwrap_or_default(),
        )
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .collect()
    }

    fn as_data_layout(
        metaroot: &Metaroot<Self>,
        ms: &Measurements<Self::N, Self::T, Self::O>,
        conf: &SharedConfig,
    ) -> DeferredResult<Self::L, NewDataLayoutWarning, NewDataLayoutError> {
        Self::L::try_new(
            metaroot.datatype,
            metaroot.specific.byteord.clone(),
            ms.layout_data(),
            conf,
        )
    }

    fn swap_optical_temporal_inner(old_t: Self::T, old_o: Self::O) -> (Self::O, Self::T) {
        let new_t = Self::T {
            peak: old_o.peak,
            timestep: old_t.timestep,
        };
        let new_o = Self::O {
            scale: Scale::Linear,
            wavelength: None.into(),
            gain: None.into(),
            peak: old_t.peak,
        };
        (new_o, new_t)
    }
}

impl VersionedMetaroot for InnerMetaroot3_1 {
    type O = InnerOptical3_1;
    type T = InnerTemporal3_1;
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
        [
            OptMetaKey::pair(&self.cyt),
            OptMetaKey::pair(&self.spillover),
            OptMetaKey::pair(&self.cytsn),
            OptMetaKey::pair(&self.vol),
        ]
        .into_iter()
        .chain(self.timestamps.opt_keywords())
        .chain(self.plate.opt_keywords())
        .chain(self.modification.opt_keywords())
        .chain(
            self.applied_gates
                .as_ref_opt()
                .map(|x| x.opt_keywords())
                .unwrap_or_default(),
        )
        .chain(
            self.subset
                .as_ref_opt()
                .map(|x| x.opt_keywords())
                .unwrap_or_default(),
        )
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .collect()
    }

    fn as_data_layout(
        metaroot: &Metaroot<Self>,
        ms: &Measurements<Self::N, Self::T, Self::O>,
        conf: &SharedConfig,
    ) -> DeferredResult<Self::L, NewDataLayoutWarning, NewDataLayoutError> {
        Self::L::try_new(
            metaroot.datatype,
            metaroot.specific.byteord,
            ms.layout_data(),
            conf,
        )
    }

    fn swap_optical_temporal_inner(old_t: Self::T, old_o: Self::O) -> (Self::O, Self::T) {
        let new_t = Self::T {
            peak: old_o.peak,
            display: old_o.display,
            timestep: old_t.timestep,
        };
        let new_o = Self::O {
            scale: Scale::Linear,
            wavelengths: None.into(),
            gain: None.into(),
            display: old_t.display,
            peak: old_t.peak,
            calibration: None.into(),
        };
        (new_o, new_t)
    }
}

impl VersionedMetaroot for InnerMetaroot3_2 {
    type O = InnerOptical3_2;
    type T = InnerTemporal3_2;
    type N = IdentityFamily;
    type L = DataLayout3_2;
    type D = Endian;

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
        [
            OptMetaKey::pair(&self.spillover),
            OptMetaKey::pair(&self.cytsn),
            OptMetaKey::pair(&self.vol),
            OptMetaKey::pair(&self.flowrate),
        ]
        .into_iter()
        .chain(self.timestamps.opt_keywords())
        .chain(self.datetimes.opt_keywords())
        .chain(self.plate.opt_keywords())
        .chain(self.unstained.opt_keywords())
        .chain(self.carrier.opt_keywords())
        .chain(self.modification.opt_keywords())
        .chain(
            self.applied_gates
                .as_ref_opt()
                .map(|x| x.opt_keywords())
                .unwrap_or_default(),
        )
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .collect()
    }

    fn as_data_layout(
        metaroot: &Metaroot<Self>,
        ms: &Measurements<Self::N, Self::T, Self::O>,
        conf: &SharedConfig,
    ) -> DeferredResult<Self::L, NewDataLayoutWarning, NewDataLayoutError> {
        let endian = metaroot.specific.byteord;
        let blank_cs = ms.layout_data();
        let cs: Vec<_> = ms
            .iter()
            .map(|x| {
                x.1.both(
                    |m| (&m.value.specific.datatype),
                    |t| (&t.value.specific.datatype),
                )
            })
            .map(|dt| dt.as_ref_opt().copied())
            .zip(blank_cs)
            .map(|(datatype, c)| ColumnLayoutData {
                width: c.width,
                range: c.range,
                datatype,
            })
            .collect();
        Self::L::try_new(metaroot.datatype, endian, cs, conf)
    }

    fn swap_optical_temporal_inner(old_t: Self::T, old_o: Self::O) -> (Self::O, Self::T) {
        let new_t = Self::T {
            display: old_o.display,
            datatype: old_o.datatype,
            timestep: old_t.timestep,
            measurement_type: None.into(),
        };
        let new_o = Self::O {
            scale: Scale::Linear,
            display: old_t.display,
            datatype: old_t.datatype,
            wavelengths: None.into(),
            gain: None.into(),
            calibration: None.into(),
            analyte: None.into(),
            measurement_type: None.into(),
            tag: None.into(),
            detector_name: None.into(),
            feature: None.into(),
        };
        (new_o, new_t)
    }
}

impl InnerTemporal3_0 {
    pub(crate) fn new(timestep: Timestep) -> Self {
        Self {
            timestep,
            peak: PeakData::default(),
        }
    }
}

impl InnerTemporal3_1 {
    pub(crate) fn new(timestep: Timestep) -> Self {
        Self {
            timestep,
            display: None.into(),
            peak: PeakData::default(),
        }
    }
}

impl InnerTemporal3_2 {
    pub(crate) fn new(timestep: Timestep) -> Self {
        Self {
            timestep,
            datatype: None.into(),
            display: None.into(),
            measurement_type: None.into(),
        }
    }
}

impl InnerOptical2_0 {
    pub(crate) fn new() -> Self {
        Self {
            scale: None.into(),
            wavelength: None.into(),
            peak: PeakData::default(),
        }
    }
}

impl InnerOptical3_0 {
    pub(crate) fn new(scale: Scale) -> Self {
        Self {
            scale,
            gain: None.into(),
            wavelength: None.into(),
            peak: PeakData::default(),
        }
    }
}

impl InnerOptical3_1 {
    pub(crate) fn new(scale: Scale) -> Self {
        Self {
            scale,
            calibration: None.into(),
            display: None.into(),
            gain: None.into(),
            wavelengths: None.into(),
            peak: PeakData::default(),
        }
    }
}

impl InnerOptical3_2 {
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

impl InnerMetaroot2_0 {
    pub(crate) fn new(mode: Mode, byteord: ByteOrd) -> Self {
        Self {
            mode,
            byteord,
            cyt: None.into(),
            timestamps: Timestamps::default(),
            comp: None.into(),
            applied_gates: None.into(),
        }
    }
}

impl InnerMetaroot3_0 {
    pub(crate) fn new(mode: Mode, byteord: ByteOrd) -> Self {
        Self {
            mode,
            byteord,
            cyt: None.into(),
            timestamps: Timestamps::default(),
            cytsn: None.into(),
            comp: None.into(),
            unicode: None.into(),
            subset: None.into(),
            applied_gates: None.into(),
        }
    }
}

impl InnerMetaroot3_1 {
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
            subset: None.into(),
            applied_gates: None.into(),
        }
    }
}

impl InnerMetaroot3_2 {
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
            applied_gates: None.into(),
        }
    }
}

impl Temporal2_0 {
    pub fn new(width: Width, range: Range) -> Self {
        let specific = InnerTemporal2_0::default();
        Temporal::new_common(width, range, specific)
    }
}

impl Temporal3_0 {
    pub fn new(width: Width, range: Range, timestep: Timestep) -> Self {
        let specific = InnerTemporal3_0::new(timestep);
        Temporal::new_common(width, range, specific)
    }
}

impl Temporal3_1 {
    pub fn new(width: Width, range: Range, timestep: Timestep) -> Self {
        let specific = InnerTemporal3_1::new(timestep);
        Temporal::new_common(width, range, specific)
    }
}

impl Temporal3_2 {
    pub fn new(width: Width, range: Range, timestep: Timestep) -> Self {
        let specific = InnerTemporal3_2::new(timestep);
        Temporal::new_common(width, range, specific)
    }
}

impl Optical2_0 {
    pub fn new(width: Width, range: Range) -> Self {
        let specific = InnerOptical2_0::new();
        Optical::new_common(width, range, specific)
    }
}

impl Optical3_0 {
    pub fn new(width: Width, range: Range, scale: Scale) -> Self {
        let specific = InnerOptical3_0::new(scale);
        Optical::new_common(width, range, specific)
    }
}

impl Optical3_1 {
    pub fn new(width: Width, range: Range, scale: Scale) -> Self {
        let specific = InnerOptical3_1::new(scale);
        Optical::new_common(width, range, specific)
    }
}

impl Optical3_2 {
    pub fn new(width: Width, range: Range, scale: Scale) -> Self {
        let specific = InnerOptical3_2::new(scale);
        Optical::new_common(width, range, specific)
    }
}

impl<X> AsMut<CommonMeasurement> for Optical<X> {
    fn as_mut(&mut self) -> &mut CommonMeasurement {
        &mut self.common
    }
}

impl<X> AsMut<CommonMeasurement> for Temporal<X> {
    fn as_mut(&mut self) -> &mut CommonMeasurement {
        &mut self.common
    }
}

impl<X> AsRef<CommonMeasurement> for Optical<X> {
    fn as_ref(&self) -> &CommonMeasurement {
        &self.common
    }
}

impl<X> AsRef<CommonMeasurement> for Temporal<X> {
    fn as_ref(&self) -> &CommonMeasurement {
        &self.common
    }
}

enum_from_disp!(
    pub SetFloatError,
    [Nan, NanFloatOrInt],
    [Length, KeyLengthError]
);

pub struct ConvertError<E> {
    from: Version,
    to: Version,
    inner: ConvertErrorInner<E>,
}

impl<E> fmt::Display for ConvertError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "could not convert from {} to {}: {}",
            self.from, self.to, self.inner
        )
    }
}

pub enum ConvertErrorInner<E> {
    Rewrap(IndexedElementError<E>),
    Meta(MetarootConvertError),
    Optical(IndexedElementError<OpticalConvertError>),
    Temporal(IndexedElementError<TemporalConvertError>),
}

impl<E> fmt::Display for ConvertErrorInner<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Rewrap(e) => e.fmt(f),
            Self::Meta(e) => e.fmt(f),
            Self::Optical(e) => e.fmt(f),
            Self::Temporal(e) => e.fmt(f),
        }
    }
}

pub struct BlankShortnames;

impl fmt::Display for BlankShortnames {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Some $PnN are blank and could not be converted",)
    }
}

pub struct OptionalKwToIdentityError;

impl fmt::Display for OptionalKwToIdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "optional keyword value is blank",)
    }
}

enum_from_disp!(
    pub StdReaderError,
    [Layout, NewDataLayoutError],
    [Reader, NewDataReaderError]
);

pub struct TerminalDataLayoutFailure;

pub struct TEXTOverflowError;

impl fmt::Display for TEXTOverflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "primary TEXT does not fit into first 99,999,999 bytes")
    }
}

enum_from_disp!(
    pub StdWriterError,
    [Layout, NewDataLayoutError],
    [Writer, ColumnWriterError],
    [TEXT, TEXTOverflowError]
);

pub enum ExistingLinkError {
    Trigger,
    UnstainedCenters,
    Comp,
    Spillover,
}

impl fmt::Display for ExistingLinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let s = match self {
            Self::Trigger => "$TR",
            Self::UnstainedCenters => "$UNSTAINEDCENTERS",
            Self::Comp => "$COMP",
            Self::Spillover => "$SPILLOVER",
        };
        write!(f, "{s} depends on existing $PnN")
    }
}

enum_from_disp!(
    pub SetSpilloverError,
    [New, SpilloverError],
    [Link, SpilloverLinkError]
);

pub struct SpilloverLinkError;

impl fmt::Display for SpilloverLinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "all $SPILLOVER names must match a $PnN")
    }
}

enum_from_disp!(
    pub SetMeasurementsError,
    [New, NewNamedVecError],
    [Link, ExistingLinkError]
);

enum_from_disp!(
    pub SetMeasurementsAndDataError,
    [Meas, SetMeasurementsError],
    [New, NewDataframeError],
    [Mismatch, MeasDataMismatchError]
);

enum_from_disp!(
    pub ColumsnToDataframeError,
    [New, NewDataframeError],
    [Mismatch, MeasDataMismatchError]
);

enum_from_disp!(
    pub SetMeasurementsOnlyError,
    [Meas, SetMeasurementsError],
    [Mismatch, MeasDataMismatchError]
);

enum_from_disp!(
    pub PushTemporalError,
    [Center, InsertCenterError],
    [Column, ColumnLengthError]
);

enum_from_disp!(
    pub PushOpticalError,
    [NonUnique, NonUniqueKeyError],
    [Column, ColumnLengthError]
);

enum_from_disp!(
    pub InsertOpticalError,
    [Insert, InsertError],
    [Column, ColumnLengthError]
);

pub struct MeasDataMismatchError {
    meas_n: usize,
    data_n: usize,
}

impl fmt::Display for MeasDataMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "measurement number ({}) does not match dataframe column number ({})",
            self.meas_n, self.data_n
        )
    }
}

pub struct MissingMeasurementNameError(Shortname);

impl fmt::Display for MissingMeasurementNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "name {} does not exist in measurements", self.0)
    }
}

enum_from_disp!(
    pub StdDatasetFromRawError,
    [TEXT, LookupKeysError],
    [Layout, NewDataLayoutError],
    [Data, NewDataReaderError],
    [Analysis, NewAnalysisReaderError],
    [DataRead, ReadDataError]
);

enum_from_disp!(
    pub StdDatasetFromRawWarning,
    [TEXT, LookupMeasWarning],
    [Layout, NewDataLayoutWarning],
    [Data, NewDataReaderWarning],
    [Analysis, NewAnalysisReaderWarning]
);

enum_from_disp!(
    pub LookupMeasWarning,
    [Parse, LookupKeysWarning],
    [Pattern, NonStdMeasRegexError],
    [Deviant, DeviantError]
);

pub struct RegionToMeasIndexError(GateIndex);

impl fmt::Display for RegionToMeasIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "cannot convert region index ({}) to measurement index since \
                   it refers to a gate",
            self.0
        )
    }
}

pub struct RegionToGateIndexError(MeasIndex);

impl fmt::Display for RegionToGateIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "cannot convert region index ({}) to gating index since \
                   it refers to a measurement",
            self.0
        )
    }
}

pub struct GateToMeasIndexError(GateIndex);

impl fmt::Display for GateToMeasIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "cannot convert gate index ({}) to measurement index",
            self.0
        )
    }
}

pub struct MeasToGateIndexError(PrefixedMeasIndex);

impl fmt::Display for MeasToGateIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "cannot convert measurement index ({}) to gate index",
            self.0
        )
    }
}

pub struct GateMeasurementLinkError(NonEmpty<GateIndex>);

impl fmt::Display for GateRegionLinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "regions in $GATING which do not have $RnI/$RnW")
    }
}

pub struct GateRegionLinkError;

impl fmt::Display for GateMeasurementLinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "$GATING regions reference nonexistent gates: {}",
            self.0.iter().join(",")
        )
    }
}

// for now this just means $PnE isn't set and should be to convert
pub struct NoScaleError(MeasIndex);

impl fmt::Display for NoScaleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{} must be set before converting measurement",
            Scale::std(self.0.into()),
        )
    }
}

enum_from_disp!(
    pub ReplaceTemporalError,
    [FromTemp, TemporalToOpticalError],
    [Set, SetCenterError]
);

// TODO these could be nested more clearly
enum_from_disp!(
    pub SetTemporalError,
    [ToTemp, OpticalToTemporalError],
    [Swap, SwapOpticalTemporalError],
    [Set, SetCenterError]
);

enum_from_disp!(
    pub SwapOpticalTemporalError,
    [ToTemporal, OpticalToTemporalError],
    [ToOptical, TemporalToOpticalError],
    [Xfer, AnyMeasKeyLossError]
);

enum_from_disp!(
    pub OpticalToTemporalError,
    [NonLinear, OpticalNonLinearError],
    [HasGain, OpticalHasGainError],
    [Xfer, AnyMeasKeyLossError]
);

enum_from_disp!(
    pub TemporalToOpticalError,
    [MeasType, IndexedKeyLossError<TemporalType>]
);

// pub enum OpticalToTemporalError {
//     NonLinear,
//     HasGain,
//     NotTimeType,
// }

pub enum SetTemporalIndexError {
    Convert(OpticalToTemporalError),
    Index(SetCenterError),
}

pub struct OpticalNonLinearError;
pub struct OpticalHasGainError;

impl fmt::Display for OpticalNonLinearError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "$PnE must be '0,0' to convert to temporal")
    }
}

impl fmt::Display for OpticalHasGainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "$PnG must not be set to convert to temporal")
    }
}

// impl fmt::Display for OpticalToTemporalError {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
//         match self {
//             OpticalToTemporalError::NonLinear => write!(f, "$PnE must be '0,0'"),
//             OpticalToTemporalError::HasGain => write!(f, "$PnG must not be set"),
//             OpticalToTemporalError::NotTimeType => write!(f, "$PnTYPE must not be set"),
//         }
//     }
// }

impl fmt::Display for SetTemporalIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            SetTemporalIndexError::Convert(c) => c.fmt(f),
            SetTemporalIndexError::Index(i) => i.fmt(f),
        }
    }
}

enum_from_disp!(
    pub MetarootConvertError,
    [NoCyt, NoCytError],
    [Byteord, EndianToByteOrdError],
    [Endian, SingleWidthError],
    [Mode, ModeNotListError],
    [GateLink, RegionToGateIndexError],
    [MeasLink, RegionToMeasIndexError],
    [GateToMeas, GateToMeasIndexError],
    [MeasToGate, MeasToGateIndexError],
    [Gates3_0To2_0, AppliedGates3_0To2_0Error],
    [Gates3_0To3_2, AppliedGates3_0To3_2Error],
    [Gates3_2To2_0, AppliedGates3_2To2_0Error],
    [Gates2_0To3_2, AppliedGates2_0To3_2Error],
    [Xfer, AnyMetarootKeyLossError],
    [Comp2_0, Comp2_0TransferError]
);

enum_from_disp!(
    pub MetarootConvertWarning,
    [Width, WidthToBytesError],
    [Mode, ModeNotListError],
    [Gates3_0To2_0, AppliedGates3_0To2_0Error],
    [Gates3_0To3_2, AppliedGates3_0To3_2Error],
    [Gates3_2To2_0, AppliedGates3_2To2_0Error],
    [Gates2_0To3_2, AppliedGates2_0To3_2Error],
    [Xfer, AnyMetarootKeyLossError],
    [Optical, OpticalConvertWarning],
    [Temporal, TemporalConvertError],
    [Comp2_0, Comp2_0TransferError]
);

enum_from_disp!(
    /// Error when a metaroot keyword will be lost when converting versions
    pub AnyMetarootKeyLossError,
    [Cytsn,            UnitaryKeyLossError<Cytsn>],
    [Unicode,          UnitaryKeyLossError<Unicode>],
    [Vol,              UnitaryKeyLossError<Vol>],
    [Flowrate,         UnitaryKeyLossError<Flowrate>],
    [Comp,             UnitaryKeyLossError<Compensation3_0>],
    [Platename,        UnitaryKeyLossError<Platename>],
    [Plateid,          UnitaryKeyLossError<Plateid>],
    [Wellid,           UnitaryKeyLossError<Wellid>],
    [Carrierid,        UnitaryKeyLossError<Carrierid>],
    [Locationid,       UnitaryKeyLossError<Locationid>],
    [Carriertype,      UnitaryKeyLossError<Carriertype>],
    [LastModifier,     UnitaryKeyLossError<LastModifier>],
    [LastModified,     UnitaryKeyLossError<ModifiedDateTime>],
    [Originality,      UnitaryKeyLossError<Originality>],
    [UnstainedCenters, UnitaryKeyLossError<UnstainedCenters>],
    [UnstainedInfo,    UnitaryKeyLossError<UnstainedInfo>],
    [Begindatetime,    UnitaryKeyLossError<BeginDateTime>],
    [Enddatetime,      UnitaryKeyLossError<EndDateTime>],
    [Spillover,        UnitaryKeyLossError<Spillover>]
);

enum_from_disp!(
    /// Error when an optical keyword will be lost when converting versions
    pub AnyMeasKeyLossError,
    [Filter,          IndexedKeyLossError<Filter>],
    [Power,           IndexedKeyLossError<Power>],
    [DetectorType,    IndexedKeyLossError<DetectorType>],
    [PercentEmitted,  IndexedKeyLossError<PercentEmitted>],
    [DetectorVoltage, IndexedKeyLossError<DetectorVoltage>],
    [Wavelength,      IndexedKeyLossError<Wavelength>],
    [Wavelengths,     IndexedKeyLossError<Wavelengths>],
    [MeasType,        IndexedKeyLossError<OpticalType>],
    [TempType,        IndexedKeyLossError<TemporalType>],
    [Analyte,         IndexedKeyLossError<Analyte>],
    [Tag,             IndexedKeyLossError<Tag>],
    [Gain,            IndexedKeyLossError<Gain>],
    [Display,         IndexedKeyLossError<Display>],
    [Datatype,        IndexedKeyLossError<NumType>],
    [DetectorName,    IndexedKeyLossError<DetectorName>],
    [Feature,         IndexedKeyLossError<Feature>],
    [Calibration3_1,  IndexedKeyLossError<Calibration3_1>],
    [Calibration3_2,  IndexedKeyLossError<Calibration3_2>]
);

pub struct Comp2_0TransferError;

impl fmt::Display for Comp2_0TransferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "$DFCiTOj keywords are set and not applicable to the target version"
        )
    }
}

pub enum AppliedGates3_0To2_0Error {
    Index(RegionToGateIndexError),
    NoGates,
}

impl fmt::Display for AppliedGates3_0To2_0Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Index(x) => x.fmt(f),
            Self::NoGates => write!(f, "no $Gn* keywords present"),
        }
    }
}

pub enum AppliedGates3_0To3_2Error {
    Index(RegionToMeasIndexError),
    HasGates,
}

impl fmt::Display for AppliedGates3_0To3_2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Index(x) => x.fmt(f),
            Self::HasGates => write!(f, "$GATING references $Gn* keywords"),
        }
    }
}

pub struct AppliedGates2_0To3_2Error;

impl fmt::Display for AppliedGates2_0To3_2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "cannot convert 2.0 $GATING/$Gn*/$RnI/$RnW keywords to 3.2"
        )
    }
}

pub struct AppliedGates3_2To2_0Error;

impl fmt::Display for AppliedGates3_2To2_0Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "cannot convert 3.2 $GATING/$RnI/$RnW keywords to 2.0")
    }
}

pub struct UnitaryKeyLossError<T>(PhantomData<T>);

impl<T> Default for UnitaryKeyLossError<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T> fmt::Display for UnitaryKeyLossError<T>
where
    T: Key,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{} is set but is not applicable to target version",
            T::std()
        )
    }
}

pub struct IndexedKeyLossError<T>(PhantomData<T>, IndexFromOne);

impl<T> IndexedKeyLossError<T> {
    fn new(i: IndexFromOne) -> Self {
        Self(PhantomData, i)
    }
}

impl<T> fmt::Display for IndexedKeyLossError<T>
where
    T: IndexedKey,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{} is set but is not applicable to target version",
            T::std(self.1)
        )
    }
}

pub struct NoCytError;

impl fmt::Display for NoCytError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "$CYT is missing")
    }
}

pub struct ModeNotListError;

impl fmt::Display for ModeNotListError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "$MODE is not L")
    }
}
