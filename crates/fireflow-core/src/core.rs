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
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::io::{BufReader, BufWriter, Read, Seek, Write};

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
    /// All "non-measurement" TEXT keywords.
    pub metadata: Metadata<M>,

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
        let res = match_anycore!(self, x, { x.metadata.specific.as_spillover() })
            .as_ref()
            .map(|s| s.print_table(delim));
        if res.is_none() {
            println!("None")
        }
    }

    // pub(crate) fn as_data_reader(
    //     &self,
    //     kws: &TotValue,
    //     conf: &DataReadConfig,
    //     data_seg: AnyDataSegment,
    // ) -> DeferredResult<DataReader, NewReaderWarning, StdReaderError> {
    //     match_anycore!(self, x, { x.as_data_reader(kws, conf, data_seg) })
    // }
}

impl AnyCoreTEXT {
    pub(crate) fn parse_raw(
        version: Version,
        std: &mut StdKeywords,
        nonstd: NonStdKeywords,
        conf: &StdTextReadConfig,
    ) -> DeferredResult<Self, LookupMeasWarning, ParseKeysError> {
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

    /// Values of $Gm*/$RnI/$RnW/$GATING/$GATE
    applied_gates: OptionalKw<AppliedGates2_0>,
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

    /// Aggregated values for $CS* keywords
    pub subset: OptionalKw<SubsetData>,

    /// Values of $Gm*/$RnI/$RnW/$GATING/$GATE
    applied_gates: OptionalKw<AppliedGates3_0>,
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

    /// Aggregated values for $CS* keywords
    pub subset: OptionalKw<SubsetData>,

    /// Values of $Gm*/$RnI/$RnW/$GATING/$GATE
    applied_gates: OptionalKw<AppliedGates3_0>,
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

    /// Values of $RnI/$RnW/$GATING
    applied_gates: OptionalKw<AppliedGates3_2>,
}

/// Temporal measurement fields specific to version 2.0
#[derive(Clone, Serialize, Default)]
pub struct InnerTemporal2_0 {
    /// Values of $Pkn/$PKNn
    pub peak: PeakData,
}

/// Temporal measurement fields specific to version 3.0
#[derive(Clone, Serialize)]
pub struct InnerTemporal3_0 {
    /// Value for $TIMESTEP
    pub timestep: Timestep,

    /// Values of $Pkn/$PKNn
    pub peak: PeakData,
}

/// Temporal measurement fields specific to version 3.1
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

/// The $GATING/$RnI/$RnW keywords in a unified bundle.
///
/// All regions in $GATING are assumed to have corresponding $RnI/$RnW keywords,
/// and each $RnI/$RnW pair is assumed to be consistent (ie both are univariate
/// or bivariate)
#[derive(Clone)]
pub struct GatingRegions<I> {
    pub gating: Gating,
    pub regions: NonEmpty<(RegionIndex, I)>,
}

/// The $GATING/$RnI/$RnW/$Gn* keywords in a unified bundle (2.0)
///
/// Each region is assumed to point to a member of ['gated_measurements'].
#[derive(Clone, Serialize)]
pub struct AppliedGates2_0 {
    pub gated_measurements: GatedMeasurements,
    pub regions: GatingRegions<Region2_0>,
}

/// The $GATING/$RnI/$RnW/$Gn* keywords in a unified bundle (3.0-3.1)
///
/// Each region is assumed to point to a member of ['gated_measurements'] or
/// a measurement in the ['Core'] struct
#[derive(Clone, Serialize)]
pub struct AppliedGates3_0 {
    pub gated_measurements: Vec<GatedMeasurement>,
    pub regions: GatingRegions<Region3_0>,
}

/// The $GATING/$RnI/$RnW keywords in a unified bundle (3.2)
///
/// Each region is assumed to point to a measurement in the ['Core'] struct
#[derive(Clone, Serialize)]
pub struct AppliedGates3_2 {
    pub regions: GatingRegions<Region3_2>,
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

pub type Metadata2_0 = Metadata<InnerMetadata2_0>;
pub type Metadata3_0 = Metadata<InnerMetadata3_0>;
pub type Metadata3_1 = Metadata<InnerMetadata3_1>;
pub type Metadata3_2 = Metadata<InnerMetadata3_2>;

/// A minimal representation of the TEXT segment
pub type CoreTEXT<M, T, P, N, W> = Core<(), (), M, T, P, N, W>;

/// A minimal representation of the TEXT+DATA+ANALYSIS segments
pub type CoreDataset<M, T, P, N, W> = Core<Analysis, FCSDataFrame, M, T, P, N, W>;

pub type CoreTEXT2_0 = CoreTEXT<
    InnerMetadata2_0,
    InnerTemporal2_0,
    InnerOptical2_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type CoreTEXT3_0 = CoreTEXT<
    InnerMetadata3_0,
    InnerTemporal3_0,
    InnerOptical3_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type CoreTEXT3_1 = CoreTEXT<
    InnerMetadata3_1,
    InnerTemporal3_1,
    InnerOptical3_1,
    IdentityFamily,
    Identity<Shortname>,
>;
pub type CoreTEXT3_2 = CoreTEXT<
    InnerMetadata3_2,
    InnerTemporal3_2,
    InnerOptical3_2,
    IdentityFamily,
    Identity<Shortname>,
>;

pub type CoreDataset2_0 = CoreDataset<
    InnerMetadata2_0,
    InnerTemporal2_0,
    InnerOptical2_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type CoreDataset3_0 = CoreDataset<
    InnerMetadata3_0,
    InnerTemporal3_0,
    InnerOptical3_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type CoreDataset3_1 = CoreDataset<
    InnerMetadata3_1,
    InnerTemporal3_1,
    InnerOptical3_1,
    IdentityFamily,
    Identity<Shortname>,
>;
pub type CoreDataset3_2 = CoreDataset<
    InnerMetadata3_2,
    InnerTemporal3_2,
    InnerOptical3_2,
    IdentityFamily,
    Identity<Shortname>,
>;

pub type Core2_0<A, D> = Core<
    A,
    D,
    InnerMetadata2_0,
    InnerTemporal2_0,
    InnerOptical2_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type Core3_0<A, D> = Core<
    A,
    D,
    InnerMetadata3_0,
    InnerTemporal3_0,
    InnerOptical3_0,
    OptionalKwFamily,
    OptionalKw<Shortname>,
>;
pub type Core3_1<A, D> = Core<
    A,
    D,
    InnerMetadata3_1,
    InnerTemporal3_1,
    InnerOptical3_1,
    IdentityFamily,
    Identity<Shortname>,
>;
pub type Core3_2<A, D> = Core<
    A,
    D,
    InnerMetadata3_2,
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

pub(crate) trait LookupMetadata: Sized + VersionedMetadata {
    fn lookup_shortname(
        kws: &mut StdKeywords,
        n: MeasIndex,
    ) -> LookupResult<<Self::N as MightHave>::Wrapper<Shortname>>;

    fn lookup_specific(st: &mut StdKeywords, par: Par) -> LookupResult<Self>;
}

pub trait TryFromMetadata<M>: Sized
where
    Self: VersionedMetadata,
    M: VersionedMetadata,
{
    fn try_from_meta(value: M, byteord: SizeConvert<M::D>) -> MetaConvertResult<Self>;
}

pub trait VersionedMetadata: Sized {
    type P: VersionedOptical;
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
        metadata: &Metadata<Self>,
        ms: &Measurements<Self::N, Self::T, Self::P>,
        conf: &SharedConfig,
    ) -> DeferredResult<Self::L, NewDataLayoutWarning, NewDataLayoutError>;
}

pub trait VersionedOptical: Sized + Versioned {
    fn req_suffixes_inner(&self, n: MeasIndex) -> RawTriples;

    fn opt_suffixes_inner(&self, n: MeasIndex) -> RawOptTriples;

    fn datatype(&self) -> Option<NumType>;

    fn can_convert_temporal(&self) -> Result<(), OpticalToTemporalError>;
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
}

pub(crate) trait LookupTemporal: VersionedTemporal {
    fn lookup_specific(kws: &mut StdKeywords, n: MeasIndex) -> LookupResult<Self>;
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

    fn convert<ToT>(self) -> Temporal<ToT>
    where
        ToT: From<T>,
    {
        Temporal {
            common: self.common,
            specific: self.specific.into(),
        }
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
        [OptMeasKey::pair(&self.common.longname, i.into())]
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

    fn try_convert<ToP: TryFrom<P, Error = OpticalConvertError>>(
        self,
    ) -> Result<Optical<ToP>, OpticalConvertError> {
        self.specific.try_into().map(|specific| Optical {
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
            OptMeasKey::triple(&self.common.longname, i.into()),
            OptMeasKey::triple(&self.filter, i.into()),
            OptMeasKey::triple(&self.power, i.into()),
            OptMeasKey::triple(&self.detector_type, i.into()),
            OptMeasKey::triple(&self.percent_emitted, i.into()),
            OptMeasKey::triple(&self.detector_voltage, i.into()),
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

impl<M> Metadata<M>
where
    M: VersionedMetadata,
{
    /// Make new version-specific metadata
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
    ) -> DeferredResult<Metadata<ToM>, WidthToBytesError, MetaConvertError> {
        // TODO this seems silly, break struct up into common bits
        ToM::try_from_meta(self.specific, convert).def_map_value(|specific| Metadata {
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

    fn lookup_metadata(
        kws: &mut StdKeywords,
        ms: &Measurements<M::N, M::T, M::P>,
        nonstd: NonStdPairs,
    ) -> LookupResult<Self>
    where
        M: LookupMetadata,
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
                            && M::P::fcs_version() >= Version::FCS3_1
                        {
                            Some(DepFeatureWarning::DatatypeASCII.into())
                        } else {
                            None
                        }
                    });
                    dt.def_zip(s)
                        .def_map_value(|(datatype, specific)| Metadata {
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

// for now this just means $PnE isn't set and should be to convert
pub struct OpticalConvertError;

impl fmt::Display for OpticalConvertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "scale must be set before converting measurement",)
    }
}

pub enum OpticalToTemporalError {
    NonLinear,
    HasGain,
    NotTimeType,
}

pub enum SetTemporalIndexError {
    Convert(OpticalToTemporalError),
    Index(SetCenterError),
}

impl fmt::Display for OpticalToTemporalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            OpticalToTemporalError::NonLinear => write!(f, "$PnE must be '0,0'"),
            OpticalToTemporalError::HasGain => write!(f, "$PnG must not be set"),
            OpticalToTemporalError::NotTimeType => write!(f, "$PnTYPE must not be set"),
        }
    }
}

impl fmt::Display for SetTemporalIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            SetTemporalIndexError::Convert(c) => c.fmt(f),
            SetTemporalIndexError::Index(i) => i.fmt(f),
        }
    }
}

enum_from_disp!(
    pub MetaConvertError,
    [NoCyt, NoCytError],
    [Byteord, EndianToByteOrdError],
    [Endian, SingleWidthError],
    [Mode, ModeNotListError],
    [GateLink, RegionToGateIndexError],
    [MeasLink, RegionToMeasIndexError],
    [GateToMeas, GateToMeasIndexError],
    [MeasToGate, MeasToGateIndexError]
);

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
    <M as VersionedMetadata>::T,
    <M as VersionedMetadata>::P,
    <M as VersionedMetadata>::N,
    <<M as VersionedMetadata>::N as MightHave>::Wrapper<Shortname>,
>;

pub(crate) type VersionedCoreDataset<M> = CoreDataset<
    M,
    <M as VersionedMetadata>::T,
    <M as VersionedMetadata>::P,
    <M as VersionedMetadata>::N,
    <<M as VersionedMetadata>::N as MightHave>::Wrapper<Shortname>,
>;

pub(crate) type VersionedCore<A, D, M> = Core<
    A,
    D,
    M,
    <M as VersionedMetadata>::T,
    <M as VersionedMetadata>::P,
    <M as VersionedMetadata>::N,
    <<M as VersionedMetadata>::N as MightHave>::Wrapper<Shortname>,
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
    M: VersionedMetadata,
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

    /// Set measurement name for $TR keyword.
    ///
    /// Return true if trigger exists and was renamed, false otherwise.
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
    ///
    /// Return true if trigger exists, false otherwise.
    pub fn set_trigger_threshold(&mut self, x: u32) -> bool {
        if let Some(tr) = self.metadata.tr.0.as_mut() {
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
        let ret = self.metadata.tr.0.is_some();
        self.metadata.tr = None.into();
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
        self.metadata.reassign_all(&mapping);
        Ok(mapping)
    }

    /// Set the measurement matching given name to be the time measurement.
    pub fn set_temporal(
        &mut self,
        n: &Shortname,
        force: bool,
    ) -> Result<bool, OpticalToTemporalError>
    where
        Optical<M::P>: From<Temporal<M::T>>,
        Temporal<M::T>: From<Optical<M::P>>,
    {
        if let Some((_, Element::NonCenter(o))) = self.measurements_named_vec().get_name(n) {
            if !force {
                o.specific.can_convert_temporal()?;
            }
        }

        // This is tricky because $TIMESTEP will be filled with a dummy value
        // when a Temporal is converted from an Optical. This will get annoying
        // for cases where the time measurement already exists and we are simply
        // "moving" it to a new measurement; $TIMESTEP should follow the move in
        // this case. Therefore, get the value of $TIMESTEP (if it exists) and
        // reassign to new time measurement (if it exists). The added
        // complication is that not all versions have $TIMESTEP, so consult
        // trait-level functions for this and wrap in Option.
        let ms = &mut self.measurements;
        let current_timestep = ms.as_center().and_then(|c| c.value.specific.timestep());
        let res = ms.set_center_by_name(n);
        if res {
            // Technically this is a bit more work than necessary because
            // we should know by this point if the center exists. Oh well..
            if let (Some(ts), Some(c)) = (current_timestep, ms.as_center_mut()) {
                M::T::set_timestep(&mut c.value.specific, ts);
            }
        }
        Ok(res)
    }

    /// Set the measurement at given index to the time measurement.
    pub fn set_temporal_at(
        &mut self,
        index: MeasIndex,
        force: bool,
    ) -> Result<bool, SetTemporalIndexError>
    where
        Optical<M::P>: From<Temporal<M::T>>,
        Temporal<M::T>: From<Optical<M::P>>,
    {
        if let Ok(Element::NonCenter((_, o))) = self.measurements_named_vec().get(index) {
            if !force {
                o.specific
                    .can_convert_temporal()
                    .map_err(SetTemporalIndexError::Convert)?;
            }
        }

        // TODO not DRY (ish, see above)
        let ms = &mut self.measurements;
        let current_timestep = ms.as_center().and_then(|c| c.value.specific.timestep());
        let res = ms
            .set_center_by_index(index)
            .map_err(SetTemporalIndexError::Index)?;
        if res {
            if let (Some(ts), Some(c)) = (current_timestep, ms.as_center_mut()) {
                M::T::set_timestep(&mut c.value.specific, ts);
            }
        }
        Ok(res)
    }

    /// Convert time measurement to optical measurement.
    ///
    /// Return true if a time measurement existed and was converted, false
    /// otherwise.
    pub fn unset_temporal(&mut self) -> bool
    where
        Optical<M::P>: From<Temporal<M::T>>,
    {
        // TODO what if the temporal measurement has $PnTYPE set, we throw error
        // when going optical->temporal, so why not here?
        self.measurements.unset_center()
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
    pub fn measurements_named_vec(&self) -> &Measurements<M::N, M::T, M::P> {
        &self.measurements
    }

    /// Replace optical measurement at index.
    ///
    /// If index points to a temporal measurement, replace it with the given
    /// optical measurement. In both cases the name is kept. Return the
    /// measurement that was replaced if the index was in bounds.
    #[allow(clippy::type_complexity)]
    pub fn replace_measurement_at(
        &mut self,
        index: MeasIndex,
        m: Optical<M::P>,
    ) -> Result<Element<Temporal<M::T>, Optical<M::P>>, ElementIndexError> {
        self.measurements.replace_at(index, m)
    }

    /// Replace optical measurement with name.
    ///
    /// If name refers to a temporal measurement, replace it with the given
    /// optical measurement. Return the measurement that was replaced if the
    /// index was in bounds.
    #[allow(clippy::type_complexity)]
    pub fn replace_measurement_named(
        &mut self,
        name: &Shortname,
        m: Optical<M::P>,
    ) -> Option<Element<Temporal<M::T>, Optical<M::P>>> {
        self.measurements.replace_named(name, m)
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
            self.metadata.reassign_all(&mapping);
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
        F: Fn(IndexedElement<&<M::N as MightHave>::Wrapper<Shortname>, &mut Optical<M::P>>) -> R,
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
        F: Fn(IndexedElement<&<M::N as MightHave>::Wrapper<Shortname>, &mut Optical<M::P>>, X) -> R,
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
    ) -> DeferredResult<
        VersionedCore<A, D, ToM>,
        WidthToBytesError,
        VersionedConvertError<M::N, ToM::N>,
    >
    where
        M::N: Clone,
        ToM: VersionedMetadata,
        ToM: TryFromMetadata<M>,
        ToM::P: VersionedOptical,
        ToM::T: VersionedTemporal,
        ToM::N: MightHave,
        ToM::N: Clone,
        ToM::P: TryFrom<M::P, Error = OpticalConvertError>,
        ToM::T: From<M::T>,
        <ToM::N as MightHave>::Wrapper<Shortname>: TryFrom<<M::N as MightHave>::Wrapper<Shortname>>,
    {
        let widths = self.widths();
        let convert = SizeConvert {
            widths,
            datatype: self.metadata.datatype,
            size: self.metadata.specific.byteord(),
        };
        let m = self
            .metadata
            .try_convert(convert)
            .def_map_errors(ConvertErrorInner::Meta);
        let ps = self
            .measurements
            .map_center_value(|y| y.value.convert())
            .map_non_center_values(|_, v| v.try_convert())
            .map_err(|es| es.map(ConvertErrorInner::Optical))
            .and_then(|x| {
                x.try_rewrapped()
                    .map_err(|es| es.map(ConvertErrorInner::Rewrap))
            })
            .mult_to_deferred();
        m.def_zip(ps)
            .def_map_value(|(metadata, measurements)| Core {
                metadata,
                measurements,
                data: self.data,
                analysis: self.analysis,
            })
            .def_map_errors(|error| ConvertError {
                from: M::P::fcs_version(),
                to: ToM::P::fcs_version(),
                inner: error,
            })
    }

    #[allow(clippy::type_complexity)]
    fn remove_measurement_by_name_inner(
        &mut self,
        n: &Shortname,
    ) -> Option<(MeasIndex, Element<Temporal<M::T>, Optical<M::P>>)> {
        if let Some(e) = self.measurements.remove_name(n) {
            self.metadata.remove_name_index(n, e.0);
            Some(e)
        } else {
            None
        }
    }

    #[allow(clippy::type_complexity)]
    fn remove_measurement_by_index_inner(
        &mut self,
        index: MeasIndex,
    ) -> Result<EitherPair<M::N, Temporal<M::T>, Optical<M::P>>, ElementIndexError> {
        let res = self.measurements.remove_index(index)?;
        if let Element::NonCenter(left) = &res {
            if let Some(n) = M::N::as_opt(&left.key) {
                self.metadata.remove_name_index(n, index);
            }
        }
        Ok(res)
    }

    fn check_existing_links(&mut self) -> Result<(), ExistingLinkError> {
        if self.trigger_name().is_some() {
            return Err(ExistingLinkError::Trigger);
        }
        let m = &self.metadata;
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
        xs: RawInput<M::N, Temporal<M::T>, Optical<M::P>>,
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
        F: Fn(&Optical<M::P>, MeasIndex) -> RawPairs,
        G: Fn(&Temporal<M::T>, MeasIndex) -> RawPairs,
        H: Fn(&Temporal<M::T>) -> RawPairs,
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
            Optical::all_req_keywords,
            Temporal::req_meas_keywords,
            Temporal::req_meta_keywords,
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
            Optical::all_opt_keywords,
            Temporal::opt_meas_keywords,
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
            .map(|(i, n)| ReqMeasKey::pair(n, i.into()))
            .collect()
    }

    fn meas_table(&self, delim: &str) -> Vec<String>
    where
        M::T: Clone,
        M::P: From<M::T>,
    {
        let ms = &self.measurements;
        if let Some(m0) = ms.get(0.into()).ok().and_then(|x| x.non_center()) {
            let header = m0.1.table_header();
            let rows = self.measurements.iter().map(|(i, r)| {
                r.both(
                    |c| Optical::<M::P>::from(c.value.clone()).table_row(i, Some(&c.key)),
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

    #[allow(clippy::type_complexity)]
    fn lookup_measurements(
        kws: &mut StdKeywords,
        par: Par,
        time_pat: Option<&TimePattern>,
        prefix: &ShortnamePrefix,
        ns_pat: Option<&NonStdMeasPattern>,
        nonstd: NonStdPairs,
    ) -> DeferredResult<
        (Measurements<M::N, M::T, M::P>, NonStdPairs),
        LookupMeasWarning,
        ParseKeysError,
    >
    where
        M: LookupMetadata,
        M::T: LookupTemporal,
        M::P: LookupOptical,
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
                        .map_err(|e| ParseKeysError::Other(e.into()))
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

        if let Err(e) = self.metadata.check_trigger(&names) {
            errs.push(e);
        }

        if let Err(e) = self.metadata.check_unstainedcenters(&names) {
            errs.push(e);
        }

        if let Err(e) = self.metadata.check_spillover(&names) {
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
        self.metadata.datatype = dt;
        Ok(())
    }

    fn set_data_ascii_inner(&mut self, xs: Vec<AsciiRangeSetter>) -> Result<(), KeyLengthError> {
        let ys: Vec<_> = xs.into_iter().map(|s| s.truncated()).collect();
        self.set_data_width_range(ys)?;
        self.metadata.datatype = AlphaNumType::Ascii;
        Ok(())
    }

    pub fn set_data_integer_inner(
        &mut self,
        xs: Vec<NumRangeSetter>,
    ) -> Result<(), KeyLengthError> {
        let ys: Vec<_> = xs.into_iter().map(|s| s.truncated()).collect();
        self.set_data_width_range(ys)?;
        self.metadata.datatype = AlphaNumType::Integer;
        Ok(())
    }

    pub(crate) fn as_data_layout(
        &self,
        conf: &SharedConfig,
    ) -> DeferredResult<M::L, NewDataLayoutWarning, NewDataLayoutError> {
        M::as_data_layout(&self.metadata, &self.measurements, conf)
    }

    // pub(crate) fn as_data_reader(
    //     &self,
    //     kws: &TotValue,
    //     conf: &DataReadConfig,
    //     data_seg: AnyDataSegment,
    // ) -> DeferredResult<DataReader, NewReaderWarning, StdReaderError> {
    //     M::as_data_layout(&self.metadata, &self.measurements, &conf.shared)
    //         .inner_into()
    //         .and_maybe(|dl| {
    //             dl.into_data_reader(kws, data_seg, conf)
    //                 .error_into()
    //                 .map_value(|column_reader| DataReader {
    //                     column_reader,
    //                     begin: u64::from(data_seg.inner.begin()),
    //                 })
    //         })
    // }
}

impl<M> VersionedCoreTEXT<M>
where
    M: VersionedMetadata,
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
    ) -> DeferredResult<Self, LookupMeasWarning, ParseKeysError>
    where
        M: LookupMetadata,
        M::T: LookupTemporal,
        M::P: LookupOptical,
    {
        // Lookup $PAR first since we need this to get the measurements
        Par::remove_meta_req(kws)
            .map_err(|e| Box::new(ParseReqKeyError::Int(e)))
            .into_deferred()
            .def_and_maybe(|par| {
                // $NEXTDATA/$BEGINSTEXT/$ENDSTEXT should have already been
                // processed when we read the TEXT; remove them so they don't
                // trigger false positives later when we test for deviant keys
                let _ = kws.remove(&Nextdata::std());
                let _ = kws.remove(&Beginstext::std());
                let _ = kws.remove(&Endstext::std());

                // Lookup measurements and metadata with $PAR
                let tp = conf.time.pattern.as_ref();
                let sp = &conf.shortname_prefix;
                let nsp = conf.nonstandard_measurement_pattern.as_ref();
                let ns: Vec<_> = nonstd.into_iter().collect();
                let mut tnt_core = Self::lookup_measurements(kws, par, tp, sp, nsp, ns)
                    .def_and_maybe(|(ms, meta_ns)| {
                        Metadata::lookup_metadata(kws, &ms, meta_ns)
                            .def_map_value(|metadata| CoreTEXT::new_unchecked(metadata, ms))
                            .def_warnings_into()
                    })?;

                // Check that the time measurement is present if we want it
                tnt_core.eval_error(|core| {
                    if let Some(pat) = tp {
                        if conf.time.ensure && core.measurements.as_center().is_none() {
                            return Some(ParseKeysError::Other(MissingTime(pat.clone()).into()));
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
    ) -> Option<(MeasIndex, Element<Temporal<M::T>, Optical<M::P>>)> {
        self.remove_measurement_by_name_inner(n)
    }

    /// Remove a measurement at a given position
    ///
    /// Return removed measurement and its name if found.
    #[allow(clippy::type_complexity)]
    pub fn remove_measurement_by_index(
        &mut self,
        index: MeasIndex,
    ) -> Result<EitherPair<M::N, Temporal<M::T>, Optical<M::P>>, ElementIndexError> {
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
        m: Optical<M::P>,
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
        m: Optical<M::P>,
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
            metadata: self.metadata,
            measurements: self.measurements,
            data,
            analysis,
        }
    }
}

impl<M> VersionedCoreDataset<M>
where
    M: VersionedMetadata,
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
        M: LookupMetadata,
        M::T: LookupTemporal,
        M::P: LookupOptical,
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
                                        metadata: text.metadata,
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
    ) -> Option<(MeasIndex, Element<Temporal<M::T>, Optical<M::P>>)> {
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
    ) -> Result<EitherPair<M::N, Temporal<M::T>, Optical<M::P>>, ElementIndexError> {
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
        m: Optical<M::P>,
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
        m: Optical<M::P>,
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
        CoreTEXT::new_unchecked(self.metadata, self.measurements)
    }
}

impl<M, T, P, N, W> CoreTEXT<M, T, P, N, W> {
    pub(crate) fn new_nomeas(metadata: Metadata<M>) -> Self {
        Self {
            metadata,
            measurements: NamedVec::default(),
            data: (),
            analysis: (),
        }
    }

    pub(crate) fn new_unchecked(
        metadata: Metadata<M>,
        measurements: NamedVec<N, W, Temporal<T>, Optical<P>>,
    ) -> Self {
        Self {
            metadata,
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
            self.metadata.specific.comp.as_ref_opt()
        }

        /// Set matrix for $COMP
        ///
        /// Return true if successfully set. Return false if matrix is either not
        /// square or rows/columns are not the same length as $PAR.
        pub fn set_compensation(&mut self, matrix: DMatrix<f32>) -> Result<(), NewCompError> {
            Compensation::try_new(matrix).map(|comp| {
                self.metadata.specific.comp = Some(comp).into();
            })
        }

        /// Clear $COMP
        pub fn unset_compensation(&mut self) {
            self.metadata.specific.comp = None.into();
        }
    };
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

macro_rules! spillover_methods {
    () => {
        /// Show $SPILLOVER
        pub fn spillover(&self) -> Option<&Spillover> {
            self.metadata.specific.spillover.as_ref_opt()
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
            self.metadata.specific.spillover = Some(m).into();
            Ok(())
        }

        /// Clear $SPILLOVER
        pub fn unset_spillover(&mut self) {
            self.metadata.specific.spillover = None.into();
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
            self.metadata.specific.byteord = byteord;
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
            self.metadata.reassign_all(&mapping);
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

impl<A, D> Core3_2<A, D> {
    /// Show $UNSTAINEDCENTERS
    pub fn unstained_centers(&self) -> Option<&UnstainedCenters> {
        self.metadata
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
            let us = &mut self.metadata.specific.unstained;
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
    spillover_methods!();

    /// Show datatype for all measurements
    ///
    /// This will be $PnDATATYPE if given and $DATATYPE otherwise at each
    /// measurement index
    pub fn datatypes(&self) -> Vec<AlphaNumType> {
        let dt = self.metadata.datatype;
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
            self.metadata.datatype = dt;
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
        let specific = InnerMetadata2_0::new(mode, byteord);
        let metadata = Metadata::new(datatype, specific);
        CoreTEXT::new_nomeas(metadata)
    }

    coretext_set_measurements2_0!(RawInput2_0);
}

impl CoreTEXT3_0 {
    pub fn new(datatype: AlphaNumType, byteord: ByteOrd, mode: Mode) -> Self {
        let specific = InnerMetadata3_0::new(mode, byteord);
        let metadata = Metadata::new(datatype, specific);
        CoreTEXT::new_nomeas(metadata)
    }

    coretext_set_measurements2_0!(RawInput3_0);
}

impl CoreTEXT3_1 {
    pub fn new(datatype: AlphaNumType, is_big: bool, mode: Mode) -> Self {
        let specific = InnerMetadata3_1::new(mode, is_big);
        let metadata = Metadata::new(datatype, specific);
        CoreTEXT::new_nomeas(metadata)
    }

    coretext_set_measurements3_1!(RawInput3_1);
}

impl CoreTEXT3_2 {
    pub fn new(datatype: AlphaNumType, is_big: bool, cyt: String) -> Self {
        let specific = InnerMetadata3_2::new(is_big, cyt);
        let metadata = Metadata::new(datatype, specific);
        CoreTEXT::new_nomeas(metadata)
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
    pub(crate) fn new_unchecked(
        us: OptionalKw<UnstainedCenters>,
        ui: OptionalKw<UnstainedInfo>,
    ) -> Self {
        Self {
            unstainedcenters: us,
            unstainedinfo: ui,
        }
    }
}

impl SubsetData {
    fn opt_keywords(&self) -> RawOptPairs {
        let m = OptionalKw(Some(CSMode(self.flags.len())));
        self.flags
            .iter()
            .enumerate()
            .map(|(i, f)| OptMeasKey::pair(f, i.into()))
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

// impl<I> Region<I> {
//     fn opt_keywords(&self) -> RawOptPairs {
//         self.
//     }
// }

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

    // fn opt_keywords(&self, i: RegionIndex) -> RawOptPairs {
    //     (RegionGateIndex2_0::std(i.into()), self.gate.to_string())
    // }
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

impl<I> Region<I> {
    pub(crate) fn map<F, J>(self, f: F) -> Region<J>
    where
        F: FnMut(I) -> J,
    {
        match self {
            Self::Univariate(x) => Region::Univariate(x.map(f)),
            Self::Bivariate(x) => Region::Bivariate(x.map(f)),
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
}

impl TryFrom<MeasOrGateIndex> for MeasIndex {
    type Error = RegionToMeasIndexError;
    fn try_from(value: MeasOrGateIndex) -> Result<Self, Self::Error> {
        match value {
            MeasOrGateIndex::Meas(i) => Ok(i),
            MeasOrGateIndex::Gate(i) => Err(RegionToMeasIndexError(i)),
        }
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

impl TryFrom<GateIndex> for MeasIndex {
    type Error = GateToMeasIndexError;
    fn try_from(value: GateIndex) -> Result<Self, Self::Error> {
        Err(GateToMeasIndexError(value))
    }
}

impl TryFrom<MeasIndex> for GateIndex {
    type Error = MeasToGateIndexError;
    fn try_from(value: MeasIndex) -> Result<Self, Self::Error> {
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

impl Serialize for GatedMeasurements {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.iter().collect::<Vec<_>>().serialize(serializer)
    }
}

impl AppliedGates2_0 {
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

impl From<OptionalKw<Wavelengths>> for OptionalKw<Wavelength> {
    fn from(value: OptionalKw<Wavelengths>) -> Self {
        value.0.map(|x| (*x.0.first()).into()).into()
    }
}

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

impl TryFrom<InnerOptical3_0> for InnerOptical2_0 {
    type Error = OpticalConvertError;

    fn try_from(value: InnerOptical3_0) -> Result<Self, Self::Error> {
        Ok(Self {
            scale: Some(value.scale).into(),
            wavelength: value.wavelength,
            peak: value.peak,
        })
    }
}

impl TryFrom<InnerOptical3_1> for InnerOptical2_0 {
    type Error = OpticalConvertError;

    fn try_from(value: InnerOptical3_1) -> Result<Self, Self::Error> {
        Ok(Self {
            scale: Some(value.scale).into(),
            wavelength: value.wavelengths.into(),
            peak: value.peak,
        })
    }
}

impl TryFrom<InnerOptical3_2> for InnerOptical2_0 {
    type Error = OpticalConvertError;

    fn try_from(value: InnerOptical3_2) -> Result<Self, Self::Error> {
        Ok(Self {
            scale: Some(value.scale).into(),
            wavelength: value.wavelengths.into(),
            peak: PeakData::default(),
        })
    }
}

impl TryFrom<InnerOptical2_0> for InnerOptical3_0 {
    type Error = OpticalConvertError;

    fn try_from(value: InnerOptical2_0) -> Result<Self, Self::Error> {
        value.scale.0.ok_or(OpticalConvertError).map(|scale| Self {
            scale,
            wavelength: value.wavelength,
            gain: None.into(),
            peak: value.peak,
        })
    }
}

impl TryFrom<InnerOptical3_1> for InnerOptical3_0 {
    type Error = OpticalConvertError;

    fn try_from(value: InnerOptical3_1) -> Result<Self, Self::Error> {
        Ok(Self {
            scale: value.scale,
            gain: value.gain,
            wavelength: value.wavelengths.into(),
            peak: value.peak,
        })
    }
}

impl TryFrom<InnerOptical3_2> for InnerOptical3_0 {
    type Error = OpticalConvertError;

    fn try_from(value: InnerOptical3_2) -> Result<Self, Self::Error> {
        Ok(Self {
            scale: value.scale,
            gain: value.gain,
            wavelength: value.wavelengths.into(),
            peak: PeakData::default(),
        })
    }
}

impl TryFrom<InnerOptical2_0> for InnerOptical3_1 {
    type Error = OpticalConvertError;

    fn try_from(value: InnerOptical2_0) -> Result<Self, Self::Error> {
        value.scale.0.ok_or(OpticalConvertError).map(|scale| Self {
            scale,
            wavelengths: value.wavelength.map(|x| x.into()),
            gain: None.into(),
            calibration: None.into(),
            display: None.into(),
            peak: value.peak,
        })
    }
}

impl TryFrom<InnerOptical3_0> for InnerOptical3_1 {
    type Error = OpticalConvertError;

    fn try_from(value: InnerOptical3_0) -> Result<Self, Self::Error> {
        Ok(Self {
            scale: value.scale,
            gain: value.gain,
            wavelengths: None.into(),
            calibration: None.into(),
            display: None.into(),
            peak: value.peak,
        })
    }
}

impl TryFrom<InnerOptical3_2> for InnerOptical3_1 {
    type Error = OpticalConvertError;

    fn try_from(value: InnerOptical3_2) -> Result<Self, Self::Error> {
        Ok(Self {
            scale: value.scale,
            gain: value.gain,
            wavelengths: value.wavelengths,
            calibration: value.calibration.map(|x| x.into()),
            display: value.display,
            peak: PeakData::default(),
        })
    }
}

impl TryFrom<InnerOptical2_0> for InnerOptical3_2 {
    type Error = OpticalConvertError;

    fn try_from(value: InnerOptical2_0) -> Result<Self, Self::Error> {
        value.scale.0.ok_or(OpticalConvertError).map(|scale| Self {
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

impl TryFrom<InnerOptical3_0> for InnerOptical3_2 {
    type Error = OpticalConvertError;

    fn try_from(value: InnerOptical3_0) -> Result<Self, Self::Error> {
        Ok(Self {
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

impl TryFrom<InnerOptical3_1> for InnerOptical3_2 {
    type Error = OpticalConvertError;

    fn try_from(value: InnerOptical3_1) -> Result<Self, Self::Error> {
        Ok(Self {
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

type MetaConvertResult<M> = DeferredResult<M, WidthToBytesError, MetaConvertError>;

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

// TODO add a way to force conversion (that is, ignore all ignorable errors and
// proceed anyway)
impl TryFromMetadata<InnerMetadata3_0> for InnerMetadata2_0 {
    fn try_from_meta(value: InnerMetadata3_0, _: ByteOrdConvert) -> MetaConvertResult<Self> {
        // let ags = value.applied_gates.0.map(|x| x.try_into()).transpose()?;
        Ok(Tentative::new1(Self {
            mode: value.mode,
            byteord: value.byteord,
            cyt: value.cyt,
            comp: value.comp,
            timestamps: value.timestamps.map(|d| d.into()),
            applied_gates: None.into(),
        }))
    }
}

impl TryFromMetadata<InnerMetadata3_1> for InnerMetadata2_0 {
    fn try_from_meta(value: InnerMetadata3_1, endian: EndianConvert) -> MetaConvertResult<Self> {
        endian
            .try_as_byteord()
            .def_errors_into()
            .def_map_value(|byteord| Self {
                mode: value.mode,
                byteord,
                cyt: value.cyt,
                comp: None.into(),
                timestamps: value.timestamps.map(|d| d.into()),
                applied_gates: None.into(),
            })
    }
}

impl TryFromMetadata<InnerMetadata3_2> for InnerMetadata2_0 {
    fn try_from_meta(value: InnerMetadata3_2, endian: EndianConvert) -> MetaConvertResult<Self> {
        endian
            .try_as_byteord()
            .def_errors_into()
            .def_map_value(|byteord| Self {
                mode: Mode::List,
                byteord,
                cyt: Some(value.cyt).into(),
                comp: None.into(),
                timestamps: value.timestamps.map(|d| d.into()),
                applied_gates: None.into(),
            })
    }
}

impl TryFromMetadata<InnerMetadata2_0> for InnerMetadata3_0 {
    fn try_from_meta(value: InnerMetadata2_0, _: ByteOrdConvert) -> MetaConvertResult<Self> {
        Ok(Tentative::new1(Self {
            mode: value.mode,
            byteord: value.byteord,
            cyt: value.cyt,
            comp: value.comp,
            timestamps: value.timestamps.map(|d| d.into()),
            cytsn: None.into(),
            unicode: None.into(),
            subset: None.into(),
            applied_gates: None.into(),
        }))
    }
}

impl TryFromMetadata<InnerMetadata3_1> for InnerMetadata3_0 {
    fn try_from_meta(value: InnerMetadata3_1, endian: EndianConvert) -> MetaConvertResult<Self> {
        endian
            .try_as_byteord()
            .def_errors_into()
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
    }
}

impl TryFromMetadata<InnerMetadata3_2> for InnerMetadata3_0 {
    fn try_from_meta(value: InnerMetadata3_2, endian: EndianConvert) -> MetaConvertResult<Self> {
        endian
            .try_as_byteord()
            .def_errors_into()
            .def_map_value(|byteord| Self {
                mode: Mode::List,
                byteord,
                cyt: Some(value.cyt).into(),
                cytsn: value.cytsn,
                timestamps: value.timestamps.map(|d| d.into()),
                comp: None.into(),
                unicode: None.into(),
                subset: None.into(),
                applied_gates: None.into(),
            })
    }
}

impl TryFromMetadata<InnerMetadata2_0> for InnerMetadata3_1 {
    fn try_from_meta(value: InnerMetadata2_0, _: ByteOrdConvert) -> MetaConvertResult<Self> {
        value
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
                applied_gates: None.into(),
            })
    }
}

impl TryFromMetadata<InnerMetadata3_0> for InnerMetadata3_1 {
    fn try_from_meta(value: InnerMetadata3_0, _: ByteOrdConvert) -> MetaConvertResult<Self> {
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
                subset: None.into(),
                applied_gates: value.applied_gates,
            })
    }
}

impl TryFromMetadata<InnerMetadata3_2> for InnerMetadata3_1 {
    fn try_from_meta(value: InnerMetadata3_2, _: EndianConvert) -> MetaConvertResult<Self> {
        Ok(Tentative::new1(Self {
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
            applied_gates: None.into(),
        }))
    }
}

impl TryFromMetadata<InnerMetadata2_0> for InnerMetadata3_2 {
    fn try_from_meta(value: InnerMetadata2_0, _: ByteOrdConvert) -> MetaConvertResult<Self> {
        let b = value.byteord.try_into().into_deferred();
        let c = value.cyt.0.ok_or(NoCytError).into_deferred();
        let m = if value.mode != Mode::List {
            Err(ModeNotListError)
        } else {
            Ok(())
        }
        .into_deferred();
        b.def_zip3(c, m).def_map_value(|(byteord, cyt, _)| Self {
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
        })
    }
}

impl TryFromMetadata<InnerMetadata3_0> for InnerMetadata3_2 {
    fn try_from_meta(value: InnerMetadata3_0, _: ByteOrdConvert) -> MetaConvertResult<Self> {
        let b = value.byteord.try_into().into_deferred();
        let c = value.cyt.0.ok_or(NoCytError).into_deferred();
        let m = if value.mode != Mode::List {
            Err(ModeNotListError)
        } else {
            Ok(())
        }
        .into_deferred();
        b.def_zip3(c, m).def_map_value(|(byteord, cyt, _)| Self {
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
            applied_gates: None.into(),
        })
    }
}

impl TryFromMetadata<InnerMetadata3_1> for InnerMetadata3_2 {
    fn try_from_meta(value: InnerMetadata3_1, _: EndianConvert) -> MetaConvertResult<Self> {
        let m = if value.mode != Mode::List {
            Err(ModeNotListError)
        } else {
            Ok(())
        }
        .into_deferred();
        let c = value.cyt.0.ok_or(NoCytError).into_deferred();
        m.def_zip(c).def_map_value(|(_, cyt)| Self {
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
            applied_gates: None.into(),
        })
    }
}

impl From<InnerTemporal3_0> for InnerTemporal2_0 {
    fn from(value: InnerTemporal3_0) -> Self {
        Self { peak: value.peak }
    }
}

impl From<InnerTemporal3_1> for InnerTemporal2_0 {
    fn from(value: InnerTemporal3_1) -> Self {
        Self { peak: value.peak }
    }
}

impl From<InnerTemporal3_2> for InnerTemporal2_0 {
    fn from(_: InnerTemporal3_2) -> Self {
        Self::default()
    }
}

impl From<InnerTemporal2_0> for InnerTemporal3_0 {
    fn from(value: InnerTemporal2_0) -> Self {
        Self {
            timestep: Timestep::default(),
            peak: value.peak,
        }
    }
}

impl From<InnerTemporal3_1> for InnerTemporal3_0 {
    fn from(value: InnerTemporal3_1) -> Self {
        Self {
            timestep: value.timestep,
            peak: value.peak,
        }
    }
}

impl From<InnerTemporal3_2> for InnerTemporal3_0 {
    fn from(value: InnerTemporal3_2) -> Self {
        Self {
            timestep: value.timestep,
            peak: PeakData::default(),
        }
    }
}

impl From<InnerTemporal2_0> for InnerTemporal3_1 {
    fn from(value: InnerTemporal2_0) -> Self {
        Self {
            timestep: Timestep::default(),
            display: None.into(),
            peak: value.peak,
        }
    }
}

impl From<InnerTemporal3_0> for InnerTemporal3_1 {
    fn from(value: InnerTemporal3_0) -> Self {
        Self {
            timestep: value.timestep,
            display: None.into(),
            peak: value.peak,
        }
    }
}

impl From<InnerTemporal3_2> for InnerTemporal3_1 {
    fn from(value: InnerTemporal3_2) -> Self {
        Self {
            timestep: value.timestep,
            display: value.display,
            peak: PeakData::default(),
        }
    }
}

impl From<InnerTemporal2_0> for InnerTemporal3_2 {
    fn from(_: InnerTemporal2_0) -> Self {
        Self {
            timestep: Timestep::default(),
            display: None.into(),
            datatype: None.into(),
            measurement_type: None.into(),
        }
    }
}

impl From<InnerTemporal3_0> for InnerTemporal3_2 {
    fn from(value: InnerTemporal3_0) -> Self {
        Self {
            timestep: value.timestep,
            display: None.into(),
            datatype: None.into(),
            measurement_type: None.into(),
        }
    }
}

impl From<InnerTemporal3_1> for InnerTemporal3_2 {
    fn from(value: InnerTemporal3_1) -> Self {
        Self {
            timestep: value.timestep,
            display: value.display,
            datatype: None.into(),
            measurement_type: None.into(),
        }
    }
}

impl From<InnerTemporal2_0> for InnerOptical2_0 {
    fn from(value: InnerTemporal2_0) -> Self {
        Self {
            scale: Some(Scale::Linear).into(),
            wavelength: None.into(),
            peak: value.peak,
        }
    }
}

impl From<InnerTemporal3_0> for InnerOptical3_0 {
    fn from(value: InnerTemporal3_0) -> Self {
        Self {
            scale: Scale::Linear,
            wavelength: None.into(),
            gain: None.into(),
            peak: value.peak,
        }
    }
}

impl From<InnerTemporal3_1> for InnerOptical3_1 {
    fn from(value: InnerTemporal3_1) -> Self {
        Self {
            scale: Scale::Linear,
            display: value.display,
            wavelengths: None.into(),
            gain: None.into(),
            calibration: None.into(),
            peak: value.peak,
        }
    }
}

impl From<InnerTemporal3_2> for InnerOptical3_2 {
    fn from(value: InnerTemporal3_2) -> Self {
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

impl From<InnerOptical2_0> for InnerTemporal2_0 {
    fn from(value: InnerOptical2_0) -> Self {
        Self { peak: value.peak }
    }
}

impl From<InnerOptical3_0> for InnerTemporal3_0 {
    fn from(value: InnerOptical3_0) -> Self {
        Self {
            timestep: Timestep::default(),
            peak: value.peak,
        }
    }
}

impl From<InnerOptical3_1> for InnerTemporal3_1 {
    fn from(value: InnerOptical3_1) -> Self {
        Self {
            timestep: Timestep::default(),
            display: value.display,
            peak: value.peak,
        }
    }
}

impl From<InnerOptical3_2> for InnerTemporal3_2 {
    fn from(value: InnerOptical3_2) -> Self {
        Self {
            timestep: Timestep::default(),
            display: value.display,
            datatype: value.datatype,
            measurement_type: None.into(),
        }
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
        let p = lookup_peakdata(kws, i, false);
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
        let p = lookup_peakdata(kws, i, false);
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
        let p = lookup_peakdata(kws, i, true);
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
        let mut tnt_scale = lookup_indexed_opt::<Scale, ParseKeysError>(kws, i.into(), false);
        tnt_scale.eval_error(|scale| {
            if scale.0.is_some_and(|x| x != Scale::Linear) {
                Some(ParseKeysError::Other(TemporalError::NonLinear.into()))
            } else {
                None
            }
        });
        let p = lookup_peakdata(kws, i, false);
        Ok(tnt_scale.zip(p).map(|(_, peak)| Self { peak }))
    }
}

impl LookupTemporal for InnerTemporal3_0 {
    fn lookup_specific(kws: &mut StdKeywords, i: MeasIndex) -> LookupResult<Self> {
        let mut tnt_gain = lookup_indexed_opt::<Gain, ParseKeysError>(kws, i.into(), false);
        tnt_gain.eval_error(|gain| {
            if gain.0.is_some() {
                Some(ParseKeysError::Other(TemporalError::HasGain.into()))
            } else {
                None
            }
        });
        let tnt_peak = lookup_peakdata(kws, i, false);
        tnt_gain.zip(tnt_peak).and_maybe(|(_, peak)| {
            let s = lookup_temporal_scale_3_0(kws, i.into());
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
        let p = lookup_peakdata(kws, i, true);
        g.zip3(d, p).and_maybe(|(_, display, peak)| {
            let s = lookup_temporal_scale_3_0(kws, i.into());
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
                let s = lookup_temporal_scale_3_0(kws, i.into());
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
            OptMeasKey::triple(&self.scale, i.into()),
            OptMeasKey::triple(&self.wavelength, i.into()),
        ]
        .into_iter()
        .collect()
    }

    fn can_convert_temporal(&self) -> Result<(), OpticalToTemporalError> {
        if !self.scale.as_ref_opt().is_some_and(|s| *s == Scale::Linear) {
            Err(OpticalToTemporalError::NonLinear)
        } else {
            Ok(())
        }
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
            OptMeasKey::triple(&self.wavelength, i.into()),
            OptMeasKey::triple(&self.gain, i.into()),
        ]
        .into_iter()
        .collect()
    }

    fn can_convert_temporal(&self) -> Result<(), OpticalToTemporalError> {
        if self.scale != Scale::Linear {
            Err(OpticalToTemporalError::NonLinear)
        } else if self.gain.0.is_some() {
            Err(OpticalToTemporalError::HasGain)
        } else {
            Ok(())
        }
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
            OptMeasKey::triple(&self.wavelengths, i.into()),
            OptMeasKey::triple(&self.gain, i.into()),
            OptMeasKey::triple(&self.calibration, i.into()),
            OptMeasKey::triple(&self.display, i.into()),
        ]
        .into_iter()
        .collect()
    }

    fn can_convert_temporal(&self) -> Result<(), OpticalToTemporalError> {
        if self.scale != Scale::Linear {
            Err(OpticalToTemporalError::NonLinear)
        } else if self.gain.0.is_some() {
            Err(OpticalToTemporalError::HasGain)
        } else {
            Ok(())
        }
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
            OptMeasKey::triple(&self.wavelengths, i.into()),
            OptMeasKey::triple(&self.gain, i.into()),
            OptMeasKey::triple(&self.calibration, i.into()),
            OptMeasKey::triple(&self.display, i.into()),
            OptMeasKey::triple(&self.detector_name, i.into()),
            OptMeasKey::triple(&self.tag, i.into()),
            OptMeasKey::triple(&self.measurement_type, i.into()),
            OptMeasKey::triple(&self.feature, i.into()),
            OptMeasKey::triple(&self.analyte, i.into()),
            OptMeasKey::triple(&self.datatype, i.into()),
        ]
        .into_iter()
        .collect()
    }

    fn can_convert_temporal(&self) -> Result<(), OpticalToTemporalError> {
        if self.scale != Scale::Linear {
            Err(OpticalToTemporalError::NonLinear)
        } else if self.gain.0.is_some() {
            Err(OpticalToTemporalError::HasGain)
        } else if self.measurement_type.0.is_some() {
            Err(OpticalToTemporalError::NotTimeType)
        } else {
            Ok(())
        }
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
        [OptMeasKey::pair(&self.display, i.into())]
            .into_iter()
            .collect()
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
            OptMeasKey::pair(&self.display, i.into()),
            OptMeasKey::pair(&self.datatype, i.into()),
        ]
        .into_iter()
        .collect()
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

impl LookupMetadata for InnerMetadata2_0 {
    fn lookup_shortname(
        kws: &mut StdKeywords,
        i: MeasIndex,
    ) -> LookupResult<<Self::N as MightHave>::Wrapper<Shortname>> {
        Ok(lookup_indexed_opt(kws, i.into(), false))
    }

    fn lookup_specific(kws: &mut StdKeywords, par: Par) -> LookupResult<Self> {
        let co = lookup_compensation_2_0(kws, par);
        let cy = lookup_meta_opt(kws, false);
        let t = lookup_timestamps(kws, false);
        let g = lookup_applied_gates2_0(kws);
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

impl LookupMetadata for InnerMetadata3_0 {
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
        let su = lookup_subset(kws, false);
        let t = lookup_timestamps(kws, false);
        let u = lookup_meta_opt(kws, false);
        let g = lookup_applied_gates3_0(kws, false);
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

impl LookupMetadata for InnerMetadata3_1 {
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
        let su = lookup_subset(kws, true);
        let md = lookup_modification(kws);
        let p = lookup_plate(kws, false);
        let t = lookup_timestamps(kws, false);
        let v = lookup_meta_opt(kws, false);
        let g = lookup_applied_gates3_0(kws, true);
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
                    Mode::Correlated => Some(DepFeatureWarning::ModeCorrelated.into()),
                    Mode::Uncorrelated => Some(DepFeatureWarning::ModeUncorrelated.into()),
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

impl LookupMetadata for InnerMetadata3_2 {
    fn lookup_shortname(
        kws: &mut StdKeywords,
        i: MeasIndex,
    ) -> LookupResult<<Self::N as MightHave>::Wrapper<Shortname>> {
        lookup_indexed_req(kws, i.into()).map(|x| x.map(Identity))
    }

    fn lookup_specific(kws: &mut StdKeywords, _: Par) -> LookupResult<Self> {
        let ca = lookup_carrier(kws);
        let d = lookup_datetimes(kws);
        let f = lookup_meta_opt(kws, false);
        let md = lookup_modification(kws);
        // Only L is allowed as of 3.2, so pull the value and check it if given.
        // The only thing we care about is that the value is valid, since we
        // don't need to use it anywhere.
        let mo = lookup_meta_opt::<Mode3_2, ParseKeysError>(kws, true);
        let sp = lookup_meta_opt(kws, false);
        let sn = lookup_meta_opt(kws, false);
        let p = lookup_plate(kws, true);
        let t = lookup_timestamps(kws, false);
        let u = lookup_unstained(kws);
        let v = lookup_meta_opt(kws, false);
        let g = lookup_applied_gates3_2(kws);
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

impl VersionedMetadata for InnerMetadata2_0 {
    type P = InnerOptical2_0;
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

    // TODO use iterators for all this string stuff
    fn keywords_opt_inner(&self) -> RawPairs {
        [
            OptMetaKey::pair(&self.cyt),
            OptMetaKey::pair(&self.timestamps.btim()),
            OptMetaKey::pair(&self.timestamps.etim()),
            OptMetaKey::pair(&self.timestamps.date()),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .chain(self.comp.as_ref_opt().map_or(vec![], |c| c.as_dfc_keys()))
        .collect()
    }

    fn as_data_layout(
        metadata: &Metadata<Self>,
        ms: &Measurements<Self::N, Self::T, Self::P>,
        conf: &SharedConfig,
    ) -> DeferredResult<Self::L, NewDataLayoutWarning, NewDataLayoutError> {
        Self::L::try_new(
            metadata.datatype,
            metadata.specific.byteord.clone(),
            ms.layout_data(),
            conf,
        )
    }
}

impl VersionedMetadata for InnerMetadata3_0 {
    type P = InnerOptical3_0;
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

    fn as_data_layout(
        metadata: &Metadata<Self>,
        ms: &Measurements<Self::N, Self::T, Self::P>,
        conf: &SharedConfig,
    ) -> DeferredResult<Self::L, NewDataLayoutWarning, NewDataLayoutError> {
        Self::L::try_new(
            metadata.datatype,
            metadata.specific.byteord.clone(),
            ms.layout_data(),
            conf,
        )
    }
}

impl VersionedMetadata for InnerMetadata3_1 {
    type P = InnerOptical3_1;
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

    fn as_data_layout(
        metadata: &Metadata<Self>,
        ms: &Measurements<Self::N, Self::T, Self::P>,
        conf: &SharedConfig,
    ) -> DeferredResult<Self::L, NewDataLayoutWarning, NewDataLayoutError> {
        Self::L::try_new(
            metadata.datatype,
            metadata.specific.byteord,
            ms.layout_data(),
            conf,
        )
    }
}

impl VersionedMetadata for InnerMetadata3_2 {
    type P = InnerOptical3_2;
    type T = InnerTemporal3_2;
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

    fn as_data_layout(
        metadata: &Metadata<Self>,
        ms: &Measurements<Self::N, Self::T, Self::P>,
        conf: &SharedConfig,
    ) -> DeferredResult<Self::L, NewDataLayoutWarning, NewDataLayoutError> {
        let endian = metadata.specific.byteord;
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
        Self::L::try_new(metadata.datatype, endian, cs, conf)
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

impl InnerMetadata2_0 {
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
            subset: None.into(),
            applied_gates: None.into(),
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
            subset: None.into(),
            applied_gates: None.into(),
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
    Meta(MetaConvertError),
    Optical(IndexedElementError<OpticalConvertError>),
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

// enum_from_disp!(
//     pub StdReaderTermination,
//     [Layout, TerminalDataLayoutFailure],
//     [TEXT, TEXTOverflowError]
// );

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
    [TEXT, ParseKeysError],
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

pub struct MeasToGateIndexError(MeasIndex);

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
