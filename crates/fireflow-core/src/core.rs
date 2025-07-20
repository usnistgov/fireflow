use crate::config::*;
use crate::data::*;
use crate::error::*;
use crate::header::*;
use crate::macros::match_many_to_one;
use crate::segment::*;
use crate::text::byteord::*;
use crate::text::compensation::*;
use crate::text::datetimes::*;
use crate::text::index::*;
use crate::text::keywords::*;
use crate::text::named_vec::*;
use crate::text::optional::*;
use crate::text::parser::*;
use crate::text::ranged_float::PositiveFloat;
use crate::text::scale::*;
use crate::text::spillover::*;
use crate::text::timestamps::*;
use crate::text::unstainedcenters::*;
use crate::validated::ascii_uint::Uint8DigitOverflow;
use crate::validated::dataframe::*;
use crate::validated::keys::*;
use crate::validated::shortname::*;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime, Timelike};
use derive_more::{AsMut, AsRef, Display, From};
use itertools::Itertools;
use nalgebra::DMatrix;
use nonempty::NonEmpty;
use num_traits::identities::One;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::io;
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
#[derive(Clone, Serialize, AsRef)]
pub struct Core<A, D, O, M, T, P, N, W, L> {
    /// Metaroot TEXT keywords.
    ///
    /// This includes all keywords that are not part of measurements or the data
    /// layout (ie the "root" of the metadata if thought of as a hierarchy)
    pub metaroot: Metaroot<M>,

    /// All measurement TEXT keywords.
    ///
    /// Specifically these are denoted by "$Pn*" keywords where "n" is the index
    /// of the measurement which also corresponds to its column in the DATA
    /// segment. The index of each measurement in this vector is n - 1.
    #[as_ref(NamedVec<N, W, Temporal<T>, Optical<P>>)]
    measurements: NamedVec<N, W, Temporal<T>, Optical<P>>,

    /// The byte layout of the DATA segment
    ///
    /// This is derived from $BYTEORD, $DATATYPE, $PnB, $PnR and maybe
    /// $PnDATATYPE for version 3.2.
    #[as_ref(Option<L>)]
    layout: MaybeValue<L>,

    /// DATA segment (if applicable)
    data: D,

    /// ANALYSIS segment (if applicable)
    pub analysis: A,

    /// Other segments (if applicable)
    pub others: O,
    // TODO add CRC
}

/// The ANALYSIS segment, which is just a string of bytes
#[derive(Clone, From)]
pub struct Analysis(pub Vec<u8>);

/// An OTHER segment, which is just a string of bytes
#[derive(Clone, From)]
pub struct Other(pub Vec<u8>);

/// All OTHER segments
#[derive(Clone, Default, From)]
pub struct Others(pub Vec<Other>);

/// Root of the metadata hierarchy.
///
/// Explicit fields are common to all FCS versions.
///
/// The generic type parameter allows version-specific data to be encoded.
#[derive(Clone, Serialize, AsRef, AsMut)]
pub struct Metaroot<X> {
    /// Value of $ABRT
    #[as_ref(Option<Abrt>)]
    #[as_mut(Option<Abrt>)]
    pub abrt: MaybeValue<Abrt>,

    /// Value of $COM
    #[as_ref(Option<Com>)]
    #[as_mut(Option<Com>)]
    pub com: MaybeValue<Com>,

    /// Value of $CELLS
    #[as_ref(Option<Cells>)]
    #[as_mut(Option<Cells>)]
    pub cells: MaybeValue<Cells>,

    /// Value of $EXP
    #[as_ref(Option<Exp>)]
    #[as_mut(Option<Exp>)]
    pub exp: MaybeValue<Exp>,

    /// Value of $FIL
    #[as_ref(Option<Fil>)]
    #[as_mut(Option<Fil>)]
    pub fil: MaybeValue<Fil>,

    /// Value of $INST
    #[as_ref(Option<Inst>)]
    #[as_mut(Option<Inst>)]
    pub inst: MaybeValue<Inst>,

    /// Value of $LOST
    #[as_ref(Option<Lost>)]
    #[as_mut(Option<Lost>)]
    pub lost: MaybeValue<Lost>,

    /// Value of $OP
    #[as_ref(Option<Op>)]
    #[as_mut(Option<Op>)]
    pub op: MaybeValue<Op>,

    /// Value of $PROJ
    #[as_ref(Option<Proj>)]
    #[as_mut(Option<Proj>)]
    pub proj: MaybeValue<Proj>,

    /// Value of $SMNO
    #[as_ref(Option<Smno>)]
    #[as_mut(Option<Smno>)]
    pub smno: MaybeValue<Smno>,

    /// Value of $SRC
    #[as_ref(Option<Src>)]
    #[as_mut(Option<Src>)]
    pub src: MaybeValue<Src>,

    /// Value of $SYS
    #[as_ref(Option<Sys>)]
    #[as_mut(Option<Sys>)]
    pub sys: MaybeValue<Sys>,

    /// Value of $TR
    tr: MaybeValue<Trigger>,

    /// Version-specific data
    pub specific: X,

    /// Non-standard keywords.
    ///
    /// This will include all the keywords that do not start with '$'.
    ///
    /// Keywords which do start with '$' but are not part of the standard are
    /// considered 'pseudostandard' and stored elsewhere since this structure
    /// will also be used to write FCS-compliant files (which do not allow
    /// nonstandard keywords starting with '$')
    pub nonstandard_keywords: NonStdKeywords,
}

#[derive(Clone, Serialize, Default, AsRef, AsMut)]
pub struct CommonMeasurement {
    /// Value for $PnS
    #[as_ref(Option<Longname>)]
    #[as_mut(Option<Longname>)]
    pub longname: MaybeValue<Longname>,

    /// Non standard keywords that belong to this measurement.
    ///
    /// These are found using a configurable pattern to filter matching keys.
    #[as_ref(HashMap<NonStdKey, String>)]
    #[as_mut(HashMap<NonStdKey, String>)]
    pub nonstandard_keywords: NonStdKeywords,
}

/// Structured data for time keywords.
///
/// Explicit fields are common to all versions. The generic type parameter
/// allows for version-specific information to be encoded.
#[derive(Clone, Serialize, AsRef, AsMut)]
pub struct Temporal<X> {
    /// Fields shared with optical measurements
    #[as_ref(forward)]
    #[as_mut(forward)]
    pub common: CommonMeasurement,

    /// Version specific data
    pub specific: X,
}

/// Structured data for optical keywords.
///
/// Explicit fields are common to all versions. The generic type parameter
/// allows for version-specific information to be encoded.
#[derive(Clone, Serialize, AsRef, AsMut)]
pub struct Optical<X> {
    /// Fields shared with optical measurements
    #[as_ref(forward)]
    #[as_mut(forward)]
    pub common: CommonMeasurement,

    /// Value for $PnF
    #[as_ref(Option<Filter>)]
    #[as_mut(Option<Filter>)]
    pub filter: MaybeValue<Filter>,

    /// Value for $PnO
    #[as_ref(Option<Power>)]
    #[as_mut(Option<Power>)]
    pub power: MaybeValue<Power>,

    /// Value for $PnD
    #[as_ref(Option<DetectorType>)]
    #[as_mut(Option<DetectorType>)]
    pub detector_type: MaybeValue<DetectorType>,

    /// Value for $PnP
    #[as_ref(Option<PercentEmitted>)]
    #[as_mut(Option<PercentEmitted>)]
    pub percent_emitted: MaybeValue<PercentEmitted>,

    /// Value for $PnV
    #[as_ref(Option<DetectorVoltage>)]
    #[as_mut(Option<DetectorVoltage>)]
    pub detector_voltage: MaybeValue<DetectorVoltage>,

    /// Version specific data
    pub specific: X,
}

/// Minimal TEXT data for any supported FCS version
#[derive(Clone, From)]
pub enum AnyCore<A, D, O> {
    #[from(Core2_0<A, D, O>)]
    FCS2_0(Box<Core2_0<A, D, O>>),
    #[from(Core3_0<A, D, O>)]
    FCS3_0(Box<Core3_0<A, D, O>>),
    #[from(Core3_1<A, D, O>)]
    FCS3_1(Box<Core3_1<A, D, O>>),
    #[from(Core3_2<A, D, O>)]
    FCS3_2(Box<Core3_2<A, D, O>>),
}

pub type AnyCoreTEXT = AnyCore<(), (), ()>;
pub type AnyCoreDataset = AnyCore<Analysis, FCSDataFrame, Others>;

impl<A, D, O> Serialize for AnyCore<A, D, O>
where
    A: Serialize,
    D: Serialize,
    O: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AnyCore", 2)?;
        match self {
            Self::FCS2_0(x) => {
                state.serialize_field("version", &x.fcs_version())?;
                state.serialize_field("data", &x)?;
            }
            Self::FCS3_0(x) => {
                state.serialize_field("version", &x.fcs_version())?;
                state.serialize_field("data", &x)?;
            }
            Self::FCS3_1(x) => {
                state.serialize_field("version", &x.fcs_version())?;
                state.serialize_field("data", &x)?;
            }
            Self::FCS3_2(x) => {
                state.serialize_field("version", &x.fcs_version())?;
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

impl<A, D, O> AnyCore<A, D, O> {
    pub fn version(&self) -> Version {
        match_many_to_one!(self, Self, [FCS2_0, FCS3_0, FCS3_1, FCS3_2], x, {
            (*x).fcs_version()
        })
    }

    pub fn shortnames(&self) -> Vec<Shortname> {
        match_anycore!(self, x, { x.all_shortnames() })
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
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        st: &ReadState<StdTextReadConfig>,
    ) -> DeferredResult<(Self, TEXTOffsets<Option<Tot>>), StdTEXTFromRawWarning, StdTEXTFromRawError>
    {
        match version {
            Version::FCS2_0 => CoreTEXT2_0::lookup(std, nonstd, data, analysis, st)
                .def_map_value(|(x, y)| (x.into(), y.into_common())),
            Version::FCS3_0 => CoreTEXT3_0::lookup(std, nonstd, data, analysis, st)
                .def_map_value(|(x, y)| (x.into(), y.into_common())),
            Version::FCS3_1 => CoreTEXT3_1::lookup(std, nonstd, data, analysis, st)
                .def_map_value(|(x, y)| (x.into(), y.into_common())),
            Version::FCS3_2 => CoreTEXT3_2::lookup(std, nonstd, data, analysis, st)
                .def_map_value(|(x, y)| (x.into(), y.into_common())),
        }
    }
}

impl AnyCoreDataset {
    pub fn as_data(&self) -> &FCSDataFrame {
        match_anycore!(self, x, { &x.data })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn parse_raw<R: Read + Seek>(
        h: &mut BufReader<R>,
        version: Version,
        kws: &mut StdKeywords,
        nonstd: NonStdKeywords,
        data_seg: HeaderDataSegment,
        analysis_seg: HeaderAnalysisSegment,
        other_segs: &[OtherSegment],
        conf: &ReadState<DataReadConfig>,
    ) -> IODeferredResult<
        (Self, AnyDataSegment, AnyAnalysisSegment),
        StdDatasetFromRawWarning,
        StdDatasetFromRawError,
    > {
        match version {
            Version::FCS2_0 => CoreDataset2_0::new_dataset_from_raw(
                h,
                kws,
                nonstd,
                data_seg,
                analysis_seg,
                other_segs,
                conf,
            )
            .def_map_value(|(x, y, z)| (x.into(), y, z)),
            Version::FCS3_0 => CoreDataset3_0::new_dataset_from_raw(
                h,
                kws,
                nonstd,
                data_seg,
                analysis_seg,
                other_segs,
                conf,
            )
            .def_map_value(|(x, y, z)| (x.into(), y, z)),
            Version::FCS3_1 => CoreDataset3_1::new_dataset_from_raw(
                h,
                kws,
                nonstd,
                data_seg,
                analysis_seg,
                other_segs,
                conf,
            )
            .def_map_value(|(x, y, z)| (x.into(), y, z)),
            Version::FCS3_2 => CoreDataset3_2::new_dataset_from_raw(
                h,
                kws,
                nonstd,
                data_seg,
                analysis_seg,
                other_segs,
                conf,
            )
            .def_map_value(|(x, y, z)| (x.into(), y, z)),
        }
    }
}

/// Metaroot fields specific to version 2.0
#[derive(Clone, Serialize, AsRef, AsMut)]
pub struct InnerMetaroot2_0 {
    /// Value of $MODE
    #[as_ref(Mode)]
    #[as_mut(Mode)]
    pub mode: Mode,

    /// Value of $CYT
    #[as_ref(Option<Cyt>)]
    #[as_mut(Option<Cyt>)]
    pub cyt: MaybeValue<Cyt>,

    /// Compensation matrix derived from 'DFCnTOm' key/value pairs
    #[as_ref(Option<Compensation2_0>)]
    comp: MaybeValue<Compensation2_0>,

    /// Values of $BTIM/ETIM/$DATE
    #[as_ref(Timestamps2_0)]
    #[as_mut(Timestamps2_0)]
    #[as_ref(Option<FCSDate>)]
    pub timestamps: Timestamps2_0,

    /// Values of $Gm*/$RnI/$RnW/$GATING/$GATE
    applied_gates: MaybeValue<AppliedGates2_0>,
}

/// Metaroot fields specific to version 3.0
#[derive(Clone, Serialize, AsRef, AsMut)]
pub struct InnerMetaroot3_0 {
    /// Value of $MODE
    #[as_ref(Mode)]
    #[as_mut(Mode)]
    pub mode: Mode,

    /// Value of $CYT
    #[as_ref(Option<Cyt>)]
    #[as_mut(Option<Cyt>)]
    pub cyt: MaybeValue<Cyt>,

    /// Value of $COMP
    #[as_ref(Option<Compensation3_0>)]
    comp: MaybeValue<Compensation3_0>,

    /// Values of $BTIM/ETIM/$DATE
    #[as_ref(Timestamps3_0)]
    #[as_mut(Timestamps3_0)]
    #[as_ref(Option<FCSDate>)]
    pub timestamps: Timestamps3_0,

    /// Value of $CYTSN
    #[as_ref(Option<Cytsn>)]
    #[as_mut(Option<Cytsn>)]
    pub cytsn: MaybeValue<Cytsn>,

    /// Value of $UNICODE
    #[as_ref(Option<Unicode>)]
    #[as_mut(Option<Unicode>)]
    pub unicode: MaybeValue<Unicode>,

    /// Aggregated values for $CS* keywords
    pub subset: MaybeValue<SubsetData>,

    /// Values of $Gm*/$RnI/$RnW/$GATING/$GATE
    applied_gates: MaybeValue<AppliedGates3_0>,
}

/// Metaroot fields specific to version 3.1
#[derive(Clone, Serialize, AsRef, AsMut)]
pub struct InnerMetaroot3_1 {
    /// Value of $MODE
    #[as_ref(Mode)]
    #[as_mut(Mode)]
    pub mode: Mode,

    /// Value of $CYT
    #[as_ref(Option<Cyt>)]
    #[as_mut(Option<Cyt>)]
    pub cyt: MaybeValue<Cyt>,

    /// Values of $BTIM/ETIM/$DATE
    #[as_ref(Timestamps3_1)]
    #[as_mut(Timestamps3_1)]
    #[as_ref(Option<FCSDate>)]
    pub timestamps: Timestamps3_1,

    /// Value of $CYTSN
    #[as_ref(Option<Cytsn>)]
    #[as_mut(Option<Cytsn>)]
    pub cytsn: MaybeValue<Cytsn>,

    /// Value of $SPILLOVER
    #[as_ref(Option<Spillover>)]
    spillover: MaybeValue<Spillover>,

    /// Values of $LAST_MODIFIED/$LAST_MODIFIER/$ORIGINALITY
    #[as_ref(Option<LastModifier>)]
    #[as_ref(Option<ModifiedDateTime>)]
    #[as_ref(Option<Originality>)]
    #[as_mut(Option<LastModifier>)]
    #[as_mut(Option<ModifiedDateTime>)]
    #[as_mut(Option<Originality>)]
    pub modification: ModificationData,

    /// Values of $PLATEID/$PLATENAME/$WELLID
    #[as_ref(Option<Plateid>)]
    #[as_ref(Option<Wellid>)]
    #[as_ref(Option<Platename>)]
    #[as_mut(Option<Plateid>)]
    #[as_mut(Option<Wellid>)]
    #[as_mut(Option<Platename>)]
    pub plate: PlateData,

    /// Value of $VOL
    #[as_ref(Option<Vol>)]
    #[as_mut(Option<Vol>)]
    pub vol: MaybeValue<Vol>,

    /// Aggregated values for $CS* keywords
    pub subset: MaybeValue<SubsetData>,

    /// Values of $Gm*/$RnI/$RnW/$GATING/$GATE
    applied_gates: MaybeValue<AppliedGates3_0>,
}

/// Metaroot fields specific to version 3.2
#[derive(Clone, Serialize, AsRef, AsMut)]
pub struct InnerMetaroot3_2 {
    /// Values of $BTIM/ETIM/$DATE
    #[as_ref(Timestamps3_1)]
    #[as_mut(Timestamps3_1)]
    #[as_ref(Option<FCSDate>)]
    pub timestamps: Timestamps3_1,

    /// Values of $BEGINDATETIME/$ENDDATETIME
    #[as_ref(Option<BeginDateTime>)]
    #[as_ref(Option<EndDateTime>)]
    #[as_ref(Datetimes)]
    #[as_mut(Datetimes)]
    pub datetimes: Datetimes,

    /// Value of $CYT
    #[as_ref(Cyt)]
    #[as_mut(Cyt)]
    pub cyt: Cyt,

    /// Value of $SPILLOVER
    #[as_ref(Option<Spillover>)]
    spillover: MaybeValue<Spillover>,

    /// Value of $CYTSN
    #[as_ref(Option<Cytsn>)]
    #[as_mut(Option<Cytsn>)]
    pub cytsn: MaybeValue<Cytsn>,

    /// Values of $LAST_MODIFIED/$LAST_MODIFIER/$ORIGINALITY
    // TODO it makes sense to verify this isn't before the file was created
    #[as_ref(Option<LastModifier>)]
    #[as_ref(Option<ModifiedDateTime>)]
    #[as_ref(Option<Originality>)]
    #[as_mut(Option<LastModifier>)]
    #[as_mut(Option<ModifiedDateTime>)]
    #[as_mut(Option<Originality>)]
    pub modification: ModificationData,

    /// Values of $PLATEID/$PLATENAME/$WELLID
    #[as_ref(Option<Plateid>)]
    #[as_ref(Option<Wellid>)]
    #[as_ref(Option<Platename>)]
    #[as_mut(Option<Plateid>)]
    #[as_mut(Option<Wellid>)]
    #[as_mut(Option<Platename>)]
    pub plate: PlateData,

    /// Value of $VOL
    #[as_ref(Option<Vol>)]
    #[as_mut(Option<Vol>)]
    pub vol: MaybeValue<Vol>,

    /// Values of $CARRIERID/$CARRIERTYPE/$LOCATIONID
    #[as_ref(Option<Carrierid>)]
    #[as_mut(Option<Carrierid>)]
    #[as_ref(Option<Carriertype>)]
    #[as_mut(Option<Carriertype>)]
    #[as_ref(Option<Locationid>)]
    #[as_mut(Option<Locationid>)]
    pub carrier: CarrierData,

    /// Values of $UNSTAINEDINFO/$UNSTAINEDCENTERS
    pub unstained: UnstainedData,

    /// Value of $FLOWRATE
    #[as_ref(Option<Flowrate>)]
    #[as_mut(Option<Flowrate>)]
    pub flowrate: MaybeValue<Flowrate>,

    /// Values of $RnI/$RnW/$GATING
    applied_gates: MaybeValue<AppliedGates3_2>,
}

/// Temporal measurement fields specific to version 2.0
#[derive(Clone, Serialize, Default, AsRef, AsMut)]
pub struct InnerTemporal2_0 {
    /// Value of $PnE
    ///
    /// Unlike subsequent versions, included here because it is optional rather
    /// than required and constant.
    // TODO this can just be a bool
    pub scale: MaybeValue<TemporalScale>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakNumber>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakNumber>)]
    pub peak: PeakData,
}

/// Temporal measurement fields specific to version 3.0
///
/// $PnE is implied as linear but not included since it only has one value
#[derive(Clone, Serialize, AsRef, AsMut)]
pub struct InnerTemporal3_0 {
    /// Value for $TIMESTEP
    #[as_ref(Timestep)]
    #[as_mut(Timestep)]
    pub timestep: Timestep,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakNumber>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakNumber>)]
    pub peak: PeakData,
}

/// Temporal measurement fields specific to version 3.1
///
/// $PnE is implied as linear but not included since it only has one value
#[derive(Clone, Serialize, AsRef, AsMut)]
pub struct InnerTemporal3_1 {
    /// Value for $TIMESTEP
    #[as_ref(Timestep)]
    #[as_mut(Timestep)]
    pub timestep: Timestep,

    /// Value for $PnDISPLAY
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    pub display: MaybeValue<Display>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakNumber>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakNumber>)]
    pub peak: PeakData,
}

/// Temporal measurement fields specific to version 3.2
///
/// $PnE is implied as linear but not included since it only has one value
#[derive(Clone, Serialize, AsRef, AsMut)]
pub struct InnerTemporal3_2 {
    /// Value for $TIMESTEP
    #[as_ref(Timestep)]
    #[as_mut(Timestep)]
    pub timestep: Timestep,

    /// Value for $PnDISPLAY
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    pub display: MaybeValue<Display>,

    /// Value for $PnTYPE
    // TODO this can just be a bool
    pub measurement_type: MaybeValue<TemporalType>,
}

/// Optical measurement fields specific to version 2.0
#[derive(Clone, Serialize, Default, AsRef, AsMut)]
pub struct InnerOptical2_0 {
    /// Value for $PnE
    ///
    /// This does not accessible via [`AsMut`] since this would expose this
    /// value to modification via [`Core::set_optical`] which we do not want
    /// since [`ScaleTransform`] needs to be synced with [`Core::layout`]. Consequently,
    /// the measurement array in `Core` is also private.
    ///
    /// There is no harm in modifying `scale` when this struct is on its own,
    /// however, so it is still public.
    #[as_ref(Option<Scale>)]
    pub scale: MaybeValue<Scale>,

    /// Value for $PnL
    #[as_ref(Option<Wavelength>)]
    #[as_mut(Option<Wavelength>)]
    pub wavelength: MaybeValue<Wavelength>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakNumber>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakNumber>)]
    pub peak: PeakData,
}

/// Optical measurement fields specific to version 3.0
#[derive(Clone, Serialize, AsRef, AsMut)]
pub struct InnerOptical3_0 {
    /// Value for $PnE/$PnG
    ///
    /// This does not accessible via [`AsMut`] since this would expose this
    /// value to modification via [`Core::set_optical`] which we do not want
    /// since [`ScaleTransform`] needs to be synced with [`Core::layout`]. Consequently,
    /// the measurement array in `Core` is also private.
    ///
    /// There is no harm in modifying `scale` when this struct is on its own,
    /// however, so it is still public.
    #[as_ref(ScaleTransform)]
    pub scale: ScaleTransform,

    /// Value for $PnL
    #[as_ref(Option<Wavelength>)]
    #[as_mut(Option<Wavelength>)]
    pub wavelength: MaybeValue<Wavelength>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakNumber>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakNumber>)]
    pub peak: PeakData,
}

/// Optical measurement fields specific to version 3.1
#[derive(Clone, Serialize, AsRef, AsMut)]
pub struct InnerOptical3_1 {
    /// Value for $PnE/$PnG
    ///
    /// This does not accessible via [`AsMut`] since this would expose this
    /// value to modification via [`Core::set_optical`] which we do not want
    /// since [`ScaleTransform`] needs to be synced with [`Core::layout`]. Consequently,
    /// the measurement array in `Core` is also private.
    ///
    /// There is no harm in modifying `scale` when this struct is on its own,
    /// however, so it is still public.
    #[as_ref(ScaleTransform)]
    pub scale: ScaleTransform,

    /// Value for $PnL
    #[as_ref(Option<Wavelengths>)]
    #[as_mut(Option<Wavelengths>)]
    pub wavelengths: MaybeValue<Wavelengths>,

    /// Value for $PnCALIBRATION
    #[as_ref(Option<Calibration3_1>)]
    #[as_mut(Option<Calibration3_1>)]
    pub calibration: MaybeValue<Calibration3_1>,

    /// Value for $PnDISPLAY
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    pub display: MaybeValue<Display>,

    /// Values of $Pkn/$PKNn
    #[as_ref(Option<PeakBin>)]
    #[as_ref(Option<PeakNumber>)]
    #[as_mut(Option<PeakBin>)]
    #[as_mut(Option<PeakNumber>)]
    pub peak: PeakData,
}

/// Optical measurement fields specific to version 3.2
#[derive(Clone, Serialize, AsRef, AsMut)]
pub struct InnerOptical3_2 {
    /// Value for $PnE/$PnG
    ///
    /// This does not accessible via [`AsMut`] since this would expose this
    /// value to modification via [`Core::set_optical`] which we do not want
    /// since [`ScaleTransform`] needs to be synced with [`Core::layout`]. Consequently,
    /// the measurement array in `Core` is also private.
    ///
    /// There is no harm in modifying `scale` when this struct is on its own,
    /// however, so it is still public.
    #[as_ref(ScaleTransform)]
    pub scale: ScaleTransform,

    /// Value for $PnL
    #[as_ref(Option<Wavelengths>)]
    #[as_mut(Option<Wavelengths>)]
    pub wavelengths: MaybeValue<Wavelengths>,

    /// Value for $PnCALIBRATION
    #[as_ref(Option<Calibration3_2>)]
    #[as_mut(Option<Calibration3_2>)]
    pub calibration: MaybeValue<Calibration3_2>,

    /// Value for $PnDISPLAY
    #[as_ref(Option<Display>)]
    #[as_mut(Option<Display>)]
    pub display: MaybeValue<Display>,

    /// Value for $PnANALYTE
    #[as_ref(Option<Analyte>)]
    #[as_mut(Option<Analyte>)]
    pub analyte: MaybeValue<Analyte>,

    /// Value for $PnFEATURE
    #[as_ref(Option<Feature>)]
    #[as_mut(Option<Feature>)]
    pub feature: MaybeValue<Feature>,

    /// Value for $PnTYPE
    #[as_ref(Option<OpticalType>)]
    #[as_mut(Option<OpticalType>)]
    pub measurement_type: MaybeValue<OpticalType>,

    /// Value for $PnTAG
    #[as_ref(Option<Tag>)]
    #[as_mut(Option<Tag>)]
    pub tag: MaybeValue<Tag>,

    /// Value for $PnDET
    #[as_ref(Option<DetectorName>)]
    #[as_mut(Option<DetectorName>)]
    pub detector_name: MaybeValue<DetectorName>,
}

/// The values for $Gm* keywords (2.0-3.1)
#[derive(Clone, Default, Serialize)]
pub struct GatedMeasurement {
    /// Value for $GmE
    pub scale: MaybeValue<GateScale>,

    /// Value for $GmF
    pub filter: MaybeValue<GateFilter>,

    /// Value for $GmN
    ///
    /// Unlike $PnN, this is not validated to be without commas
    pub shortname: MaybeValue<GateShortname>,

    /// Value for $GmP
    pub percent_emitted: MaybeValue<GatePercentEmitted>,

    /// Value for $GmR
    pub range: MaybeValue<GateRange>,

    /// Value for $GmS
    pub longname: MaybeValue<GateLongname>,

    /// Value for $GmT
    pub detector_type: MaybeValue<GateDetectorType>,

    /// Value for $GmV
    pub detector_voltage: MaybeValue<GateDetectorVoltage>,
}

/// A scale transform derived from $PnE/$PnG.
#[derive(Clone, Copy, Serialize, PartialEq)]
pub enum ScaleTransform {
    /// A linear transform ($PnE=0,0 and $PnG=1.0 or is null)
    Lin(PositiveFloat),
    /// A log transform ($PnE!=0,0 and $PnG!=1.0 or is null)
    Log(LogScale),
}

/// The $GATING/$RnI/$RnW/$Gn* keywords in a unified bundle (2.0)
///
/// Each region is assumed to point to a member of ['gated_measurements'].
// TODO updates to these are currently not validated
#[derive(Clone, Serialize)]
pub struct AppliedGates2_0 {
    pub gated_measurements: GatedMeasurements,
    pub regions: GatingRegions<GateIndex>,
}

/// The $GATING/$RnI/$RnW/$Gn* keywords in a unified bundle (3.0-3.1)
///
/// Each region is assumed to point to a member of ['gated_measurements'] or
/// a measurement in the ['Core'] struct
// TODO updates to these are currently not validated
#[derive(Clone, Serialize)]
pub struct AppliedGates3_0 {
    pub gated_measurements: Vec<GatedMeasurement>,
    pub regions: GatingRegions<MeasOrGateIndex>,
}

/// The $GATING/$RnI/$RnW keywords in a unified bundle (3.2)
///
/// Each region is assumed to point to a measurement in the ['Core'] struct
// TODO updates to these are currently not validated
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
#[derive(Clone, Default, Serialize, AsRef, AsMut)]
pub struct PeakData {
    /// Value of $Pkn
    #[as_ref(Option<PeakBin>)]
    #[as_mut(Option<PeakBin>)]
    pub bin: MaybeValue<PeakBin>,

    /// Value of $PkNn
    #[as_ref(Option<PeakNumber>)]
    #[as_mut(Option<PeakNumber>)]
    pub size: MaybeValue<PeakNumber>,
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
    pub bits: MaybeValue<CSVBits>,

    /// Values of $CSVnFLAG if given, with length equal to $CSMODE
    pub flags: NonEmpty<MaybeValue<CSVFlag>>,
}

/// A bundle for $ORIGINALITY, $LAST_MODIFIER, and $LAST_MODIFIED (3.1+)
#[derive(Clone, Serialize, Default, AsRef, AsMut)]
pub struct ModificationData {
    #[as_ref(Option<LastModifier>)]
    #[as_mut(Option<LastModifier>)]
    pub last_modifier: MaybeValue<LastModifier>,

    #[as_ref(Option<ModifiedDateTime>)]
    #[as_mut(Option<ModifiedDateTime>)]
    pub last_modified: MaybeValue<ModifiedDateTime>,

    #[as_ref(Option<Originality>)]
    #[as_mut(Option<Originality>)]
    pub originality: MaybeValue<Originality>,
}

/// A bundle for $PLATEID, $PLATENAME, and $WELLID (3.1+)
#[derive(Clone, Serialize, Default, AsRef, AsMut)]
pub struct PlateData {
    #[as_ref(Option<Plateid>)]
    #[as_mut(Option<Plateid>)]
    pub plateid: MaybeValue<Plateid>,

    #[as_ref(Option<Platename>)]
    #[as_mut(Option<Platename>)]
    pub platename: MaybeValue<Platename>,

    #[as_ref(Option<Wellid>)]
    #[as_mut(Option<Wellid>)]
    pub wellid: MaybeValue<Wellid>,
}

/// A bundle for $UNSTAINEDCENTERS and $UNSTAINEDINFO (3.2+)
#[derive(Clone, Serialize, Default, AsRef, AsMut)]
pub struct UnstainedData {
    unstainedcenters: MaybeValue<UnstainedCenters>,
    #[as_ref(Option<UnstainedInfo>)]
    #[as_mut(Option<UnstainedInfo>)]
    pub unstainedinfo: MaybeValue<UnstainedInfo>,
}

/// A bundle for $CARRIERID, $CARRIERTYPE, $LOCATIONID (3.2+)
#[derive(Clone, Serialize, Default, AsRef, AsMut)]
pub struct CarrierData {
    #[as_ref(Option<Carrierid>)]
    #[as_mut(Option<Carrierid>)]
    pub carrierid: MaybeValue<Carrierid>,

    #[as_ref(Option<Carriertype>)]
    #[as_mut(Option<Carriertype>)]
    pub carriertype: MaybeValue<Carriertype>,

    #[as_ref(Option<Locationid>)]
    #[as_mut(Option<Locationid>)]
    pub locationid: MaybeValue<Locationid>,
}

pub type Temporal2_0 = Temporal<InnerTemporal2_0>;
pub type Temporal3_0 = Temporal<InnerTemporal3_0>;
pub type Temporal3_1 = Temporal<InnerTemporal3_1>;
pub type Temporal3_2 = Temporal<InnerTemporal3_2>;

pub type Optical2_0 = Optical<InnerOptical2_0>;
pub type Optical3_0 = Optical<InnerOptical3_0>;
pub type Optical3_1 = Optical<InnerOptical3_1>;
pub type Optical3_2 = Optical<InnerOptical3_2>;

pub type Measurements2_0 = Measurements<MaybeFamily, InnerTemporal2_0, InnerOptical2_0>;
pub type Measurements3_0 = Measurements<MaybeFamily, InnerTemporal3_0, InnerOptical3_0>;
pub type Measurements3_1 = Measurements<AlwaysFamily, InnerTemporal3_1, InnerOptical3_1>;
pub type Measurements3_2 = Measurements<AlwaysFamily, InnerTemporal3_2, InnerOptical3_2>;

pub type Metaroot2_0 = Metaroot<InnerMetaroot2_0>;
pub type Metaroot3_0 = Metaroot<InnerMetaroot3_0>;
pub type Metaroot3_1 = Metaroot<InnerMetaroot3_1>;
pub type Metaroot3_2 = Metaroot<InnerMetaroot3_2>;

/// A minimal representation of the TEXT segment
pub type CoreTEXT<M, T, P, N, W, L> = Core<(), (), (), M, T, P, N, W, L>;

/// A minimal representation of the TEXT+DATA+ANALYSIS segments
pub type CoreDataset<M, T, P, N, W, L> = Core<Analysis, FCSDataFrame, Others, M, T, P, N, W, L>;

pub type Core2_0<A, D, O> = Core<
    A,
    D,
    O,
    InnerMetaroot2_0,
    InnerTemporal2_0,
    InnerOptical2_0,
    MaybeFamily,
    MaybeValue<Shortname>,
    DataLayout2_0,
>;
pub type Core3_0<A, D, O> = Core<
    A,
    D,
    O,
    InnerMetaroot3_0,
    InnerTemporal3_0,
    InnerOptical3_0,
    MaybeFamily,
    MaybeValue<Shortname>,
    DataLayout3_0,
>;
pub type Core3_1<A, D, O> = Core<
    A,
    D,
    O,
    InnerMetaroot3_1,
    InnerTemporal3_1,
    InnerOptical3_1,
    AlwaysFamily,
    AlwaysValue<Shortname>,
    DataLayout3_1,
>;
pub type Core3_2<A, D, O> = Core<
    A,
    D,
    O,
    InnerMetaroot3_2,
    InnerTemporal3_2,
    InnerOptical3_2,
    AlwaysFamily,
    AlwaysValue<Shortname>,
    DataLayout3_2,
>;

pub type CoreTEXT2_0 = Core2_0<(), (), ()>;
pub type CoreTEXT3_0 = Core3_0<(), (), ()>;
pub type CoreTEXT3_1 = Core3_1<(), (), ()>;
pub type CoreTEXT3_2 = Core3_2<(), (), ()>;

pub type CoreDataset2_0 = Core2_0<Analysis, FCSDataFrame, Others>;
pub type CoreDataset3_0 = Core3_0<Analysis, FCSDataFrame, Others>;
pub type CoreDataset3_1 = Core3_1<Analysis, FCSDataFrame, Others>;
pub type CoreDataset3_2 = Core3_2<Analysis, FCSDataFrame, Others>;

/// Reader for ANALYSIS segment
pub struct AnalysisReader {
    pub seg: AnyAnalysisSegment,
}

/// Reader for OTHER segments
pub struct OthersReader<'a> {
    pub segs: &'a [OtherSegment],
}

mod private {
    pub struct NoTouchy;
}

pub trait HasCompensation: AsRef<Option<Self::Comp>> {
    type Comp: From<Compensation> + AsRef<Compensation>;

    // set wrapped inner type with common outer type (Compensation)
    fn set_comp(&mut self, comp: Option<Compensation>, _: private::NoTouchy) {
        *self.comp_mut(private::NoTouchy) = comp.map(|c| c.into());
    }

    // almost like as_ref, except the reference needs to go on the inside since
    // the newtype wrapper needs to be removed
    fn comp(&self, _: private::NoTouchy) -> Option<&Compensation> {
        self.as_ref().as_ref().map(|x| x.as_ref())
    }

    // private as_mut
    fn comp_mut(&mut self, _: private::NoTouchy) -> &mut Option<Self::Comp>;
}

pub trait HasSpillover {
    // private as_mut
    fn spill_mut(&mut self, _: private::NoTouchy) -> &mut Option<Spillover>;
}

pub trait HasScale {
    fn scale_mut(&mut self, _: private::NoTouchy) -> &mut Option<Scale>;
}

pub trait HasScaleTransform {
    fn transform_mut(&mut self, _: private::NoTouchy) -> &mut ScaleTransform;
}

pub trait AsScaleTransform {
    fn as_transform(&self) -> ScaleTransform;
}

pub trait Versioned {
    type Layout: VersionedDataLayout;
    type Offsets: VersionedTEXTOffsets<TotDef = <Self::Layout as VersionedDataLayout>::TotDef>;

    fn fcs_version() -> Self;

    fn h_lookup_and_read<R: Read + Seek>(
        h: &mut BufReader<R>,
        kws: &StdKeywords,
        data_seg: HeaderDataSegment,
        analysis_seg: HeaderAnalysisSegment,
        st: &ReadState<DataReadConfig>,
    ) -> IODeferredResult<
        (FCSDataFrame, Analysis, AnyDataSegment, AnyAnalysisSegment),
        LookupAndReadDataAnalysisWarning,
        LookupAndReadDataAnalysisError,
    > {
        let layout_res = Self::Layout::lookup_ro(kws, &st.conf.standard)
            .def_inner_into()
            .def_errors_liftio();
        let offset_res = Self::Offsets::lookup_ro(
            kws,
            data_seg,
            analysis_seg,
            &st.map_inner(|conf| &conf.standard),
        )
        .def_inner_into()
        .def_errors_liftio();
        layout_res
            .def_zip(offset_res)
            .def_and_maybe(|(layout, offsets)| {
                let ar = AnalysisReader {
                    seg: offsets.analysis(),
                };
                // TODO what if data seg is non empty and layout is empty?
                let data_res = layout.map_or(Ok(Tentative::new1(FCSDataFrame::default())), |l| {
                    l.h_read_df(h, offsets.tot(), offsets.data(), &st.conf.reader)
                        .def_warnings_into()
                        .def_map_errors(|e| e.inner_into())
                });
                let analysis_res = ar.h_read(h).into_deferred();
                data_res
                    .def_zip(analysis_res)
                    .def_map_value(|(data, analysis)| {
                        (data, analysis, offsets.data(), offsets.analysis())
                    })
            })
    }
}

pub(crate) trait LookupMetaroot: Sized + VersionedMetaroot {
    fn lookup_shortname(
        kws: &mut StdKeywords,
        n: MeasIndex,
    ) -> LookupResult<<Self::Name as MightHave>::Wrapper<Shortname>>;

    fn lookup_specific(
        st: &mut StdKeywords,
        par: Par,
        names: &HashSet<&Shortname>,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>;
}

pub trait ConvertFromMetaroot<M>: Sized
where
    Self: VersionedMetaroot,
    M: VersionedMetaroot,
{
    fn convert_from_metaroot(value: M, force: bool) -> MetarootConvertResult<Self>;
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

pub trait ConvertFromLayout<T>: Sized
where
    Self: VersionedDataLayout,
{
    fn convert_from_layout(value: T) -> LayoutConvertResult<Self>;
}

pub trait VersionedMetaroot: Sized {
    type Ver: Versioned;
    type Optical: VersionedOptical<Ver = Self::Ver>;
    type Temporal: VersionedTemporal<Ver = Self::Ver>;
    type Name: MightHave;
    // type Layout: VersionedDataLayout;
    // type Offsets: VersionedTEXTOffsets<Tot = <Self::Layout as VersionedDataLayout>::Tot>;

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

    fn keywords_req_inner(&self) -> impl Iterator<Item = (String, String)>;

    fn keywords_opt_inner(&self) -> impl Iterator<Item = (String, String)>;

    /// Swap convert a temporal and optical channel into the other.
    ///
    /// This is necessary to have in one function since we may want to recover
    /// a bad conversion. Thus we need to first check if the two types can be
    /// converted into the other, and if so, actually do the conversion, and if
    /// not, return the originals with error(s).
    ///
    /// It may seem tempting to use two TryFroms to so this, but this won't work
    /// in the case where one conversion succeeds and the other fails. Rust's
    /// ownership model dictates that the successful conversion consume the
    /// original value, in which case we are stuck halfway with no path to
    /// recover the original state.
    #[allow(clippy::type_complexity)]
    fn swap_optical_temporal(
        t: Temporal<Self::Temporal>,
        o: Optical<Self::Optical>,
        i: MeasIndex,
        lossless: bool,
    ) -> PassthruResult<
        (Optical<Self::Optical>, Temporal<Self::Temporal>),
        Box<(Temporal<Self::Temporal>, Optical<Self::Optical>)>,
        SwapOpticalTemporalError,
        SwapOpticalTemporalError,
    > {
        let go = |old_t: Temporal<Self::Temporal>, old_o: Optical<Self::Optical>| {
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
        let o_common_res = check_optical_keys_transfer(&o, i)
            .mult_errors_into::<OpticalToTemporalError>()
            .mult_errors_into();
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

    fn swap_optical_temporal_inner(
        t: Self::Temporal,
        p: Self::Optical,
    ) -> (Self::Optical, Self::Temporal);
}

pub trait VersionedOptical: Sized {
    type Ver: Versioned;

    fn req_suffixes_inner(
        &self,
        n: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, String)>;

    fn opt_suffixes_inner(
        &self,
        n: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)>;

    fn can_convert_to_temporal(&self, i: MeasIndex) -> MultiResult<(), OpticalToTemporalError>;
}

pub(crate) trait LookupOptical: Sized + VersionedOptical {
    fn lookup_specific(
        kws: &mut StdKeywords,
        n: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>;
}

pub trait VersionedTemporal: Sized {
    type Ver: Versioned;

    fn req_meta_keywords_inner(&self) -> impl Iterator<Item = (String, String)>;

    fn opt_meas_keywords_inner(&self, _: MeasIndex) -> impl Iterator<Item = (String, String)>;

    fn can_convert_to_optical(&self, i: MeasIndex) -> MultiResult<(), TemporalToOpticalError>;
}

pub(crate) trait LookupTemporal: VersionedTemporal {
    fn lookup_specific(
        kws: &mut StdKeywords,
        n: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>;
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

pub trait VersionedTEXTOffsets: Sized {
    type TotDef: TotDefinition;

    fn lookup(
        kws: &mut StdKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        conf: &ReadState<StdTextReadConfig>,
    ) -> LookupTEXTOffsetsResult<Self>;

    fn lookup_ro(
        kws: &StdKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        conf: &ReadState<StdTextReadConfig>,
    ) -> LookupTEXTOffsetsResult<Self>;

    // TODO this doesn't seem necessary
    fn data(&self) -> AnyDataSegment;

    fn analysis(&self) -> AnyAnalysisSegment;

    fn tot(&self) -> <Self::TotDef as TotDefinition>::Tot;

    fn into_common(self) -> TEXTOffsets<Option<Tot>>;
}

pub struct TEXTOffsets<T> {
    pub data: AnyDataSegment,
    pub analysis: AnyAnalysisSegment,
    pub tot: T,
}

#[derive(From)]
pub struct TEXTOffsets2_0(pub TEXTOffsets<Option<Tot>>);

#[derive(From)]
pub struct TEXTOffsets3_0(pub TEXTOffsets<Tot>);

#[derive(From)]
pub struct TEXTOffsets3_2(pub TEXTOffsets<Tot>);

impl CommonMeasurement {
    fn lookup<E>(
        kws: &mut StdKeywords,
        i: MeasIndex,
        nonstd: NonStdPairs,
    ) -> LookupTentative<Self, E> {
        Longname::lookup_opt(kws, i.into()).map(|longname| Self {
            longname,
            nonstandard_keywords: nonstd.into_iter().collect(),
        })
    }
}

impl<T> Temporal<T> {
    fn new_common(specific: T) -> Self {
        Self {
            common: CommonMeasurement::default(),
            specific,
        }
    }

    fn lookup_temporal(
        kws: &mut StdKeywords,
        i: MeasIndex,
        nonstd: NonStdPairs,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>
    where
        T: LookupTemporal,
    {
        CommonMeasurement::lookup(kws, i, nonstd).and_maybe(|common| {
            T::lookup_specific(kws, i, conf).def_map_value(|specific| Temporal { common, specific })
        })
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

    fn req_meas_keywords(&self, _: MeasIndex) -> impl Iterator<Item = (String, String)>
    where
        T: VersionedTemporal,
    {
        [].into_iter()
    }

    fn req_meta_keywords(&self) -> impl Iterator<Item = (String, String)>
    where
        T: VersionedTemporal,
    {
        self.specific.req_meta_keywords_inner()
    }

    fn opt_meas_keywords(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)>
    where
        T: VersionedTemporal,
    {
        [OptIndexedKey::pair_opt(&self.common.longname, i.into())]
            .into_iter()
            .flat_map(|(k, v)| v.map(|x| (k, x)))
            .chain(self.specific.opt_meas_keywords_inner(i))
    }

    pub(crate) fn as_transform(&self) -> ScaleTransform {
        ScaleTransform::default()
    }
}

impl<O> Optical<O> {
    fn new_common(specific: O) -> Self {
        Self {
            common: CommonMeasurement::default(),
            specific,
            detector_type: None.into(),
            detector_voltage: None.into(),
            filter: None.into(),
            percent_emitted: None.into(),
            power: None.into(),
        }
    }

    fn try_convert<ToP: ConvertFromOptical<O>>(
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
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>
    where
        O: LookupOptical,
        Version: From<O::Ver>,
    {
        let dd = conf.disallow_deprecated;
        let version = O::Ver::fcs_version();
        let f = Filter::lookup_opt(kws, i.into());
        let p = Power::lookup_opt(kws, i.into());
        let d = DetectorType::lookup_opt(kws, i.into());
        let e = if Version::from(version) == Version::FCS3_2 {
            PercentEmitted::lookup_opt_dep(kws, i.into(), dd)
        } else {
            PercentEmitted::lookup_opt(kws, i.into())
        };
        let v = DetectorVoltage::lookup_opt(kws, i.into());
        f.zip5(p, d, e, v).errors_into().and_maybe(
            |(filter, power, detector_type, percent_emitted, detector_voltage)| {
                CommonMeasurement::lookup(kws, i, nonstd).and_maybe(|common| {
                    O::lookup_specific(kws, i, conf).def_map_value(|specific| Optical {
                        common,
                        filter,
                        power,
                        detector_type,
                        percent_emitted,
                        detector_voltage,
                        specific,
                    })
                })
            },
        )
    }

    fn req_keywords(&self, i: MeasIndex) -> impl Iterator<Item = (MeasHeader, String, String)>
    where
        O: VersionedOptical,
    {
        self.specific.req_suffixes_inner(i)
    }

    fn opt_keywords(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)>
    where
        O: VersionedOptical,
    {
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
    }

    // TODO move out, this is specific to the CLI interface
    // for table
    fn table_pairs(&self) -> impl Iterator<Item = (MeasHeader, Option<String>)>
    where
        O: VersionedOptical,
    {
        // zero is a dummy and not meaningful here
        let n = 0.into();
        self.req_keywords(n)
            .map(|(t, _, v)| (t, Some(v)))
            .chain(self.opt_keywords(n).map(|(k, _, v)| (k, v)))
    }

    fn table_header(&self, opt_layout: Vec<MeasHeader>) -> Vec<String>
    where
        O: VersionedOptical,
    {
        let req_layout = req_meas_headers();
        [MeasHeader("index".into()), Shortname::std_blank()]
            .into_iter()
            .chain(req_layout)
            .chain(self.table_pairs().map(|(k, _)| k))
            .chain(opt_layout)
            .map(|x| x.0)
            .collect()
    }

    fn table_row(
        &self,
        i: MeasIndex,
        n: Option<&Shortname>,
        req_layout: [String; 2],
        opt_layout: Vec<Option<String>>,
    ) -> Vec<String>
    where
        O: VersionedOptical,
    {
        let na = || "NA".into();
        [i.to_string(), n.map_or(na(), |x| x.to_string())]
            .into_iter()
            .chain(req_layout)
            .chain(
                self.table_pairs()
                    .map(|(_, v)| v)
                    .map(|v| v.unwrap_or(na())),
            )
            .chain(opt_layout.into_iter().map(|x| x.unwrap_or(na())))
            .collect()
    }

    // TODO this name is weird, this is standard+nonstandard keywords
    // after filtering out None values
    fn all_req_keywords(&self, n: MeasIndex) -> impl Iterator<Item = (String, String)>
    where
        O: VersionedOptical,
    {
        self.req_keywords(n).map(|(_, k, v)| (k, v))
    }

    fn all_opt_keywords(&self, n: MeasIndex) -> impl Iterator<Item = (String, String)>
    where
        O: VersionedOptical,
    {
        self.opt_keywords(n)
            .filter_map(|(_, k, v)| v.map(|x| (k, x)))
            .chain(
                self.common
                    .nonstandard_keywords
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone())),
            )
    }

    pub(crate) fn as_transform(&self) -> ScaleTransform
    where
        O: AsScaleTransform,
    {
        self.specific.as_transform()
    }
}

impl<M> Metaroot<M>
where
    M: VersionedMetaroot,
{
    /// Make new version-specific metaroot
    pub fn new(specific: M) -> Self {
        Metaroot {
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

    fn try_convert<ToM: ConvertFromMetaroot<M>>(
        self,
        force: bool,
    ) -> MetarootConvertResult<Metaroot<ToM>> {
        // TODO this seems silly, break struct up into common bits
        ToM::convert_from_metaroot(self.specific, force).def_map_value(|specific| Metaroot {
            abrt: self.abrt,
            cells: self.cells,
            com: self.com,
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

    fn lookup_metaroot(
        kws: &mut StdKeywords,
        ms: &Measurements<M::Name, M::Temporal, M::Optical>,
        nonstd: NonStdPairs,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self>
    where
        M: LookupMetaroot,
    {
        let par = Par(ms.len());
        let names: HashSet<_> = ms.indexed_names().map(|(_, n)| n).collect();
        let a = Abrt::lookup_opt(kws);
        let co = Com::lookup_opt(kws);
        let ce = Cells::lookup_opt(kws);
        let e = Exp::lookup_opt(kws);
        let f = Fil::lookup_opt(kws);
        let i = Inst::lookup_opt(kws);
        let l = Lost::lookup_opt(kws);
        let o = Op::lookup_opt(kws);
        let p = Proj::lookup_opt(kws);
        let sm = Smno::lookup_opt(kws);
        let sr = Src::lookup_opt(kws);
        let sy = Sys::lookup_opt(kws);
        let t = Trigger::lookup_opt(kws, &names);
        a.zip5(co, ce, e, f)
            .zip5(i, l, o, p)
            .zip5(sm, sr, sy, t)
            .and_maybe(
                |(((abrt, com, cells, exp, fil), inst, lost, op, proj), smno, src, sys, tr)| {
                    M::lookup_specific(kws, par, &names, conf).def_map_value(|specific| Metaroot {
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

    fn all_req_keywords(&self, par: Par) -> impl Iterator<Item = (String, String)> {
        [par.pair()]
            .into_iter()
            .chain(self.specific.keywords_req_inner())
    }

    fn all_opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            OptMetarootKey::pair_opt(&self.abrt),
            OptMetarootKey::pair_opt(&self.com),
            OptMetarootKey::pair_opt(&self.cells),
            OptMetarootKey::pair_opt(&self.exp),
            OptMetarootKey::pair_opt(&self.fil),
            OptMetarootKey::pair_opt(&self.inst),
            OptMetarootKey::pair_opt(&self.lost),
            OptMetarootKey::pair_opt(&self.op),
            OptMetarootKey::pair_opt(&self.proj),
            OptMetarootKey::pair_opt(&self.smno),
            OptMetarootKey::pair_opt(&self.src),
            OptMetarootKey::pair_opt(&self.sys),
            OptLinkedKey::pair_opt(&self.tr),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .chain(self.specific.keywords_opt_inner())
        .map(|(k, v)| (k.to_string(), v))
        .chain(
            self.nonstandard_keywords
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone())),
        )
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

pub(crate) type Measurements<N, T, O> =
    NamedVec<N, <N as MightHave>::Wrapper<Shortname>, Temporal<T>, Optical<O>>;

pub(crate) type VersionedCore<A, D, O, M> = Core<
    A,
    D,
    O,
    M,
    <M as VersionedMetaroot>::Temporal,
    <M as VersionedMetaroot>::Optical,
    <M as VersionedMetaroot>::Name,
    <<M as VersionedMetaroot>::Name as MightHave>::Wrapper<Shortname>,
    <<M as VersionedMetaroot>::Ver as Versioned>::Layout,
>;

pub(crate) type VersionedCoreTEXT<M> = VersionedCore<(), (), (), M>;

pub(crate) type VersionedCoreDataset<M> = VersionedCore<Analysis, FCSDataFrame, Others, M>;

pub(crate) type VersionedConvertError<N, ToN> = ConvertError<
    <<ToN as MightHave>::Wrapper<Shortname> as TryFrom<
        <N as MightHave>::Wrapper<Shortname>,
    >>::Error,
>;

impl<M, A, D, O> VersionedCore<A, D, O, M>
where
    M: VersionedMetaroot,
    M::Name: Clone,
{
    pub(crate) fn fcs_version(&self) -> Version
    where
        Version: From<M::Ver>,
    {
        M::Ver::fcs_version().into()
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

    /// Return all keywords as an ordered list of pairs
    ///
    /// Thiw will only include keywords that can be directly derived from
    /// [CoreTEXT]. This means it will not include $TOT, since this depends on
    /// the DATA segment.
    pub fn raw_keywords(&self, want_req: Option<bool>, want_meta: Option<bool>) -> RawKeywords {
        let req_meta: Vec<_> = self.req_root_keywords().collect();
        let opt_meta: Vec<_> = self.opt_root_keywords().collect();
        let req_meas: Vec<_> = self.req_meas_keywords().collect();
        let opt_meas: Vec<_> = self.opt_meas_keywords().collect();

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
            .map(|(_, x)| x.both(|t| Some(&t.key), |m| M::Name::as_opt(&m.key)))
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
        timestep: <M::Temporal as TemporalFromOptical<M::Optical>>::TData,
        force: bool,
    ) -> DeferredResult<bool, SetTemporalError, SetTemporalError>
    where
        M::Temporal: TemporalFromOptical<M::Optical>,
    {
        let lossless = !force;
        self.measurements.set_center_by_name(
            n,
            |i, old_o, old_t| M::swap_optical_temporal(old_o, old_t, i, lossless).def_inner_into(),
            |i, old_o| {
                <M::Temporal as TemporalFromOptical<M::Optical>>::from_optical(
                    old_o, i, timestep, lossless,
                )
                .def_inner_into::<SwapOpticalTemporalError, SwapOpticalTemporalError>()
                .def_inner_into()
            },
        )
    }

    /// Set the measurement at given index to the time measurement.
    pub fn set_temporal_at(
        &mut self,
        index: MeasIndex,
        timestep: <M::Temporal as TemporalFromOptical<M::Optical>>::TData,
        force: bool,
    ) -> DeferredResult<bool, SetTemporalError, SetTemporalError>
    where
        M::Temporal: TemporalFromOptical<M::Optical>,
    {
        let lossless = !force;
        self.measurements.set_center_by_index(
            index,
            |i, old_o, old_t| M::swap_optical_temporal(old_o, old_t, i, lossless).def_inner_into(),
            |i, old_o| {
                <M::Temporal as TemporalFromOptical<M::Optical>>::from_optical(
                    old_o, i, timestep, lossless,
                )
                .def_inner_into::<SwapOpticalTemporalError, SwapOpticalTemporalError>()
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
        Option<<M::Optical as OpticalFromTemporal<M::Temporal>>::TData>,
        TemporalToOpticalError,
        TemporalToOpticalError,
    >
    where
        M::Optical: OpticalFromTemporal<M::Temporal>,
    {
        self.measurements.unset_center(|i, old_t| {
            <M::Optical as OpticalFromTemporal<M::Temporal>>::from_temporal(old_t, i, !force)
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
        // TODO use a newtype for this so it can't be confused with a different
        // hashmap
        self.measurements
            .alter_common_values_zip(xs, |_, x: &mut HashMap<_, _>, (k, v)| x.insert(k, v))
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
            .alter_common_values_zip(xs, |_, x: &mut HashMap<_, _>, k| x.remove(k))
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
                .map(|((_, x), k): ((_, &HashMap<_, _>), _)| x.get(k))
                .collect();
            Some(res)
        }
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
        m: Optical<M::Optical>,
    ) -> Result<Element<Temporal<M::Temporal>, Optical<M::Optical>>, ElementIndexError> {
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
        m: Optical<M::Optical>,
    ) -> Option<Element<Temporal<M::Temporal>, Optical<M::Optical>>> {
        self.measurements.replace_named(name, m)
    }

    /// Replace temporal measurement at index.
    #[allow(clippy::type_complexity)]
    pub fn replace_temporal_at(
        &mut self,
        index: MeasIndex,
        m: Temporal<M::Temporal>,
        force: bool,
    ) -> DeferredResult<
        Element<Temporal<M::Temporal>, Optical<M::Optical>>,
        ReplaceTemporalError,
        ReplaceTemporalError,
    >
    where
        M::Optical: OpticalFromTemporal<M::Temporal>,
    {
        self.measurements.replace_center_at(index, m, |i, old_t| {
            <M::Optical as OpticalFromTemporal<M::Temporal>>::from_temporal(old_t, i, !force)
                .def_inner_into()
                .def_map_value(|(x, _)| x)
        })
    }

    /// Replace temporal measurement at index.
    #[allow(clippy::type_complexity)]
    pub fn replace_temporal_named(
        &mut self,
        name: &Shortname,
        m: Temporal<M::Temporal>,
        force: bool,
    ) -> DeferredResult<
        Option<Element<Temporal<M::Temporal>, Optical<M::Optical>>>,
        ReplaceTemporalError,
        ReplaceTemporalError,
    >
    where
        M::Optical: OpticalFromTemporal<M::Temporal>,
    {
        self.measurements
            .replace_center_by_name(name, m, |i, old_t| {
                <M::Optical as OpticalFromTemporal<M::Temporal>>::from_temporal(old_t, i, !force)
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
        key: <M::Name as MightHave>::Wrapper<Shortname>,
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
        F: Fn(
            IndexedElement<&<M::Name as MightHave>::Wrapper<Shortname>, &mut Optical<M::Optical>>,
        ) -> R,
        G: Fn(IndexedElement<&Shortname, &mut Temporal<M::Temporal>>) -> R,
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
        F: Fn(
            IndexedElement<&<M::Name as MightHave>::Wrapper<Shortname>, &mut Optical<M::Optical>>,
            X,
        ) -> R,
        G: Fn(IndexedElement<&Shortname, &mut Temporal<M::Temporal>>, X) -> R,
    {
        self.measurements.alter_values_zip(xs, f, g)
    }

    /// Return mutable reference to time measurement as a name/value pair.
    pub fn temporal_mut(
        &mut self,
    ) -> Option<IndexedElement<&mut Shortname, &mut Temporal<M::Temporal>>> {
        self.measurements.as_center_mut()
    }

    pub fn get_metaroot<X>(&self) -> &X
    where
        Metaroot<M>: AsRef<X>,
    {
        self.metaroot.as_ref()
    }

    /// Get value for $BTIM as a [`NaiveTime`]
    pub fn btim_naive<X>(&self) -> Option<NaiveTime>
    where
        X: Copy,
        NaiveTime: From<X>,
        Metaroot<M>: AsRef<Option<Btim<X>>>,
    {
        self.time_naive()
    }

    /// Get value for $ETIM as a [`NaiveTime`]
    pub fn etim_naive<X>(&self) -> Option<NaiveTime>
    where
        X: Copy,
        NaiveTime: From<X>,
        Metaroot<M>: AsRef<Option<Etim<X>>>,
    {
        self.time_naive()
    }

    /// Set value for $BTIM as a [`NaiveTime`]
    ///
    /// Return error if resulting $BTIM starts after $ETIM and $DATE is
    /// specified.
    pub fn set_btim_naive<X>(&mut self, time: Option<NaiveTime>) -> Result<(), ReversedTimestamps>
    where
        X: PartialOrd + From<NaiveTime>,
        Metaroot<M>: AsMut<Timestamps<X>>,
    {
        let t = self.metaroot.as_mut();
        t.set_btim(time.map(|x| Xtim(x.into())))
    }

    /// Set value for $ETIM as a [`NaiveTime`]
    ///
    /// Return error if resulting $BTIM starts after $ETIM and $DATE is
    /// specified.
    pub fn set_etim_naive<X>(&mut self, time: Option<NaiveTime>) -> Result<(), ReversedTimestamps>
    where
        X: PartialOrd + From<NaiveTime>,
        Metaroot<M>: AsMut<Timestamps<X>>,
    {
        let t = self.metaroot.as_mut();
        t.set_etim(time.map(|x| Xtim(x.into())))
    }

    /// Get $DATE as a [`NaiveDate`]
    pub fn date_naive(&self) -> Option<NaiveDate>
    where
        Metaroot<M>: AsRef<Option<FCSDate>>,
    {
        self.metaroot.as_ref().as_ref().map(|&x| x.into())
    }

    /// Set $DATE as a [`NaiveDate`]
    ///
    /// Return error if resulting $BTIM starts after $ETIM and $DATE is
    /// specified.
    pub fn set_date_naive<X>(&mut self, date: Option<NaiveDate>) -> Result<(), ReversedTimestamps>
    where
        X: PartialOrd,
        Metaroot<M>: AsMut<Timestamps<X>>,
    {
        self.metaroot.as_mut().set_date(date.map(|x| x.into()))
    }

    /// Get $BEGINDATETIME as a [`DateTime<FixedOffset>`]
    pub fn get_begindatetime(&self) -> Option<DateTime<FixedOffset>>
    where
        Metaroot<M>: AsRef<Option<BeginDateTime>>,
    {
        self.metaroot.as_ref().as_ref().map(|&x| x.into())
    }

    /// Get $ENDDATETIME as a [`DateTime<FixedOffset>`]
    pub fn get_enddatetime(&self) -> Option<DateTime<FixedOffset>>
    where
        Metaroot<M>: AsRef<Option<EndDateTime>>,
    {
        self.metaroot.as_ref().as_ref().map(|&x| x.into())
    }

    /// Set $BEGINDATETIME as a [`DateTime<FixedOffset>`]
    ///
    /// Return error if resulting $BEGINDATETIME is after $ENDDATETIME.
    pub fn set_begindatetime(
        &mut self,
        date: Option<DateTime<FixedOffset>>,
    ) -> Result<(), ReversedDatetimes>
    where
        Metaroot<M>: AsMut<Datetimes>,
    {
        self.metaroot.as_mut().set_begin(date.map(|x| x.into()))
    }

    /// Set $ENDDATETIME as a [`DateTime<FixedOffset>`]
    ///
    /// Return error if resulting $BEGINDATETIME is after $ENDDATETIME.
    pub fn set_enddatetime(
        &mut self,
        date: Option<DateTime<FixedOffset>>,
    ) -> Result<(), ReversedDatetimes>
    where
        Metaroot<M>: AsMut<Datetimes>,
    {
        self.metaroot.as_mut().set_end(date.map(|x| x.into()))
    }

    /// Get $TIMESTEP value if the time measurement exists.
    pub fn timestep(&self) -> Option<&Timestep>
    where
        Temporal<M::Temporal>: AsRef<Timestep>,
    {
        self.measurements.as_center().map(|x| x.value.as_ref())
    }

    /// Set $TIMESTEP value if the time measurement exists.
    ///
    /// Return `true` if the time measurement exist (which means its $TIMESTEP
    /// was updated) and `false` otherwise.
    pub fn set_timestep(&mut self, timestep: Timestep) -> bool
    where
        Temporal<M::Temporal>: AsMut<Timestep>,
    {
        self.measurements
            .as_center_mut()
            .map(|x| *x.value.as_mut() = timestep)
            .is_some()
    }

    pub fn compensation(&self) -> Option<&DMatrix<f32>>
    where
        M: HasCompensation,
    {
        self.metaroot
            .specific
            .comp(private::NoTouchy)
            .map(|x| x.as_ref())
    }

    /// Set matrix for $COMP
    ///
    /// Return true if successfully set. Return false if matrix is either not
    /// square or rows/columns are not the same length as $PAR.
    pub fn set_compensation(&mut self, matrix: DMatrix<f32>) -> Result<(), NewCompError>
    where
        M: HasCompensation,
    {
        // TODO also check $PAR
        Compensation::try_new(matrix).map(|comp| {
            self.metaroot
                .specific
                .set_comp(Some(comp), private::NoTouchy)
        })
    }

    /// Clear $COMP
    pub fn unset_compensation(&mut self)
    where
        M: HasCompensation,
    {
        self.metaroot.specific.set_comp(None, private::NoTouchy);
    }

    /// Show $SPILLOVER matrix
    pub fn spillover_matrix(&self) -> Option<&DMatrix<f32>>
    where
        M: AsRef<Option<Spillover>>,
    {
        self.metaroot.specific.as_ref().as_ref().map(|x| x.as_ref())
    }

    /// Show $SPILLOVER measurement names
    pub fn spillover_names(&self) -> Option<&[Shortname]>
    where
        M: AsRef<Option<Spillover>>,
    {
        self.metaroot.specific.as_ref().as_ref().map(|x| x.as_ref())
    }

    /// Set names and matrix for $SPILLOVER
    ///
    /// Names must match number of rows/columns in matrix and also must be a
    /// subset of the measurement names (ie $PnN). Matrix must be square and
    /// at least 2x2.
    pub fn set_spillover(
        &mut self,
        names: Vec<Shortname>,
        matrix: DMatrix<f32>,
    ) -> Result<(), SetSpilloverError>
    where
        M: HasSpillover,
    {
        let current = self.all_shortnames();
        let ns: HashSet<_> = names.iter().collect();
        if !ns.is_subset(&current.iter().collect()) {
            return Err(SpilloverLinkError.into());
        }
        let m = Spillover::try_new(names, matrix)?;
        *self.metaroot.specific.spill_mut(private::NoTouchy) = Some(m);
        Ok(())
    }

    /// Clear $SPILLOVER
    pub fn unset_spillover(&mut self)
    where
        M: HasSpillover,
    {
        *self.metaroot.specific.spill_mut(private::NoTouchy) = None;
    }

    pub fn get_all_scales(&self) -> impl Iterator<Item = Option<Scale>>
    where
        Optical<M::Optical>: AsRef<Option<Scale>>,
    {
        self.measurements.iter().map(|(_, x)| {
            x.both(
                |_| Some(Scale::Linear),
                |m| m.value.as_ref().as_ref().copied(),
            )
        })
    }

    pub fn get_all_transforms(&self) -> impl Iterator<Item = ScaleTransform>
    where
        Optical<M::Optical>: AsRef<ScaleTransform>,
    {
        self.measurements
            .iter()
            .map(|(_, x)| x.both(|_| ScaleTransform::default(), |m| *m.value.as_ref()))
    }

    pub fn set_scales(
        &mut self,
        scales: Vec<Option<Scale>>,
    ) -> MultiResult<(), SetMeasurementsError>
    where
        M::Optical: HasScale,
    {
        if let Some(l) = self.layout.0.as_ref() {
            let mut xforms: Vec<_> = scales
                .iter()
                .copied()
                .map(|s| s.map(ScaleTransform::from).unwrap_or_default())
                .collect();
            // If there is a center index and the input is too short, just let
            // it pass; the next check will throw an error if the final length
            // is incorrect
            if let Some(i) = self.measurements.center_index().map(|i| i.into()) {
                if i <= xforms.len() {
                    xforms.insert(i, ScaleTransform::default())
                }
            }
            l.check_transforms_and_len(&xforms[..]).mult_errors_into()?;
            // ASSUME this won't fail because we checked the length first
            self.measurements
                .alter_non_center_values_zip(scales, |m, x| {
                    *m.specific.scale_mut(private::NoTouchy) = x
                })
                .map(|_| ())
                .unwrap();
            Ok(())
        } else if scales.is_empty() {
            Ok(())
        } else {
            Err(EmptyLayoutError).into_mult()
        }
    }

    pub fn set_transforms(
        &mut self,
        mut xforms: Vec<ScaleTransform>,
    ) -> MultiResult<(), SetMeasurementsError>
    where
        M::Optical: HasScaleTransform,
    {
        // TODO very not DRY
        if let Some(l) = self.layout.0.as_ref() {
            // If there is a center index and the input is too short, just let
            // it pass; the next check will throw an error if the final length
            // is incorrect
            if let Some(i) = self.measurements.center_index().map(|i| i.into()) {
                if i <= xforms.len() {
                    xforms.insert(i, ScaleTransform::default())
                }
            }
            l.check_transforms_and_len(&xforms[..]).mult_errors_into()?;
            // ASSUME this won't fail because we checked the length first
            self.measurements
                .alter_non_center_values_zip(xforms, |m, x| {
                    *m.specific.transform_mut(private::NoTouchy) = x
                })
                .map(|_| ())
                .unwrap();
            Ok(())
        } else if xforms.is_empty() {
            Ok(())
        } else {
            Err(EmptyLayoutError).into_mult()
        }
    }

    pub fn get_metaroot_opt<X>(&self) -> Option<&X>
    where
        Metaroot<M>: AsRef<Option<X>>,
    {
        self.get_metaroot().as_ref()
    }

    pub fn set_metaroot<X>(&mut self, x: X)
    where
        Metaroot<M>: AsMut<X>,
    {
        *self.metaroot.as_mut() = x
    }

    pub fn get_meas<'a, X: 'a>(&'a self) -> impl Iterator<Item = (MeasIndex, &'a X)>
    where
        Temporal<M::Temporal>: AsRef<X>,
        Optical<M::Optical>: AsRef<X>,
    {
        self.measurements
            .iter()
            .map(|(i, x)| (i, x.both(|t| t.value.as_ref(), |m| m.value.as_ref())))
    }

    pub fn get_meas_opt<'a, X: 'a>(&'a self) -> impl Iterator<Item = (MeasIndex, Option<&'a X>)>
    where
        Temporal<M::Temporal>: AsRef<Option<X>>,
        Optical<M::Optical>: AsRef<Option<X>>,
    {
        self.get_meas::<Option<X>>().map(|(i, x)| (i, x.as_ref()))
    }

    pub fn set_meas<X>(&mut self, xs: Vec<X>) -> Result<(), KeyLengthError>
    where
        Temporal<M::Temporal>: AsMut<X>,
        Optical<M::Optical>: AsMut<X>,
    {
        self.measurements
            .alter_values_zip(
                xs,
                |m, x| *m.value.as_mut() = x,
                |m, x| *m.value.as_mut() = x,
            )
            .map(|_| ())
    }

    pub fn get_optical<'a, X: 'a>(&'a self) -> impl Iterator<Item = (MeasIndex, &'a X)>
    where
        Optical<M::Optical>: AsRef<X>,
    {
        self.measurements
            .iter_non_center_values()
            .map(|(i, m)| (i, m.as_ref()))
    }

    pub fn get_optical_opt<'a, X: 'a>(&'a self) -> impl Iterator<Item = (MeasIndex, Option<&'a X>)>
    where
        Optical<M::Optical>: AsRef<Option<X>>,
    {
        self.get_optical().map(|(i, m)| (i, m.as_ref()))
    }

    pub fn get_optical2<'a, X, Y>(&'a self) -> impl Iterator<Item = (MeasIndex, &'a Y)>
    where
        Optical<M::Optical>: AsRef<X>,
        X: AsRef<Y> + 'a,
        Y: 'a,
    {
        self.get_optical().map(|(i, m)| (i, m.as_ref()))
    }

    pub fn get_optical_opt2<'a, X, Y>(&'a self) -> impl Iterator<Item = (MeasIndex, Option<&'a Y>)>
    where
        Optical<M::Optical>: AsRef<Option<X>>,
        X: AsRef<Y> + 'a,
        Y: 'a,
    {
        self.get_optical_opt()
            .map(|(i, m)| (i, m.map(|x| x.as_ref())))
    }

    pub fn set_optical<X>(&mut self, xs: Vec<X>) -> Result<(), KeyLengthError>
    where
        Optical<M::Optical>: AsMut<X>,
    {
        self.measurements
            .alter_non_center_values_zip(xs, |m, x| *m.as_mut() = x)
            .map(|_| ())
    }

    /// Return a list of measurement names as stored in $PnS
    ///
    /// If not given, will be replaced by "Mn" where "n" is the measurement
    /// index starting at 1.
    pub fn longnames(&self) -> Vec<Option<&Longname>> {
        self.measurements
            .iter_common_values()
            .map(|(_, x): (_, &Option<Longname>)| x.as_ref())
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
            .alter_common_values_zip(ns, |_, x: &mut Option<Longname>, n| *x = n.map(Longname))
            .map(|_| ())
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
        VersionedCore<A, D, O, ToM>,
        MetarootConvertWarning,
        VersionedConvertError<M::Name, ToM::Name>,
    >
    where
        Version: From<M::Ver>,
        Version: From<ToM::Ver>,
        M::Name: Clone,
        ToM: VersionedMetaroot,
        ToM: ConvertFromMetaroot<M>,
        ToM::Optical: VersionedOptical,
        ToM::Temporal: VersionedTemporal,
        ToM::Name: MightHave,
        ToM::Name: Clone,
        ToM::Optical: ConvertFromOptical<M::Optical>,
        ToM::Temporal: ConvertFromTemporal<M::Temporal>,
        <ToM::Ver as Versioned>::Layout: ConvertFromLayout<<M::Ver as Versioned>::Layout>,
        <ToM::Name as MightHave>::Wrapper<Shortname>:
            TryFrom<<M::Name as MightHave>::Wrapper<Shortname>>,
    {
        let m = self
            .metaroot
            .try_convert(force)
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
        let lres = self
            .layout
            .map(ConvertFromLayout::convert_from_layout)
            .transpose()
            .mult_map_errors(ConvertErrorInner::Layout)
            .mult_to_deferred();
        m.def_zip3(ps, lres)
            .def_map_value(|(metaroot, measurements, layout)| Core {
                metaroot,
                measurements,
                layout,
                data: self.data,
                analysis: self.analysis,
                others: self.others,
            })
            .def_map_errors(|error| ConvertError {
                from: M::Ver::fcs_version().into(),
                to: ToM::Ver::fcs_version().into(),
                inner: error,
            })
    }

    fn time_naive<const IS_ETIM: bool, X>(&self) -> Option<NaiveTime>
    where
        X: Copy,
        NaiveTime: From<X>,
        Metaroot<M>: AsRef<Option<Xtim<IS_ETIM, X>>>,
    {
        let t: &Option<Xtim<IS_ETIM, X>> = self.metaroot.as_ref();
        t.as_ref().map(|&x| x.0.into())
    }

    #[allow(clippy::type_complexity)]
    fn remove_measurement_by_name_inner(
        &mut self,
        n: &Shortname,
    ) -> Option<(
        MeasIndex,
        Element<Temporal<M::Temporal>, Optical<M::Optical>>,
    )> {
        if let Some(e @ (i, _)) = self.measurements.remove_name(n) {
            self.metaroot.remove_name_index(n, i);
            self.layout.mut_or_unset_nofail(|l| l.remove_nocheck(i));
            Some(e)
        } else {
            None
        }
    }

    #[allow(clippy::type_complexity)]
    fn remove_measurement_by_index_inner(
        &mut self,
        index: MeasIndex,
    ) -> Result<EitherPair<M::Name, Temporal<M::Temporal>, Optical<M::Optical>>, ElementIndexError>
    {
        let res = self.measurements.remove_index(index)?;
        if let Element::NonCenter(left) = &res {
            if let Some(n) = M::Name::as_opt(&left.key) {
                self.metaroot.remove_name_index(n, index);
                self.layout.mut_or_unset_nofail(|l| l.remove_nocheck(index));
            }
        }
        Ok(res)
    }

    fn push_temporal_inner(
        &mut self,
        n: Shortname,
        m: Temporal<M::Temporal>,
        r: Range,
        notrunc: bool,
    ) -> DeferredResult<(), AnyRangeError, InsertTemporalError> {
        self.measurements
            .push_center(n, m)
            .into_deferred()
            .def_and_tentatively(|_| {
                self.layout
                    .0
                    .as_mut()
                    .map(|l| l.push(r, notrunc))
                    .unwrap_or_default()
                    .errors_into()
            })
    }

    fn insert_temporal_inner(
        &mut self,
        i: MeasIndex,
        n: Shortname,
        m: Temporal<M::Temporal>,
        r: Range,
        notrunc: bool,
    ) -> DeferredResult<(), AnyRangeError, InsertTemporalError> {
        self.measurements
            .insert_center(i, n, m)
            .into_deferred()
            .def_and_tentatively(|_| {
                self.layout
                    .0
                    .as_mut()
                    .map(|l| l.insert_nocheck(i, r, notrunc))
                    .unwrap_or_default()
                    .inner_into()
            })
    }

    fn push_optical_inner(
        &mut self,
        n: <M::Name as MightHave>::Wrapper<Shortname>,
        m: Optical<M::Optical>,
        r: Range,
        notrunc: bool,
    ) -> DeferredResult<Shortname, AnyRangeError, PushOpticalError> {
        self.measurements
            .push(n, m)
            .into_deferred()
            .def_and_tentatively(|ret| {
                self.layout
                    .0
                    .as_mut()
                    .map(|l| l.push(r, notrunc))
                    .unwrap_or_default()
                    .errors_into()
                    .map(|_| ret)
            })
    }

    fn insert_optical_inner(
        &mut self,
        i: MeasIndex,
        n: <M::Name as MightHave>::Wrapper<Shortname>,
        m: Optical<M::Optical>,
        r: Range,
        notrunc: bool,
    ) -> DeferredResult<Shortname, AnyRangeError, InsertOpticalError> {
        self.measurements
            .insert(i, n, m)
            .into_deferred()
            .def_and_tentatively(|ret| {
                self.layout
                    .0
                    .as_mut()
                    .map(|l| l.insert_nocheck(i, r, notrunc))
                    .unwrap_or_default()
                    .inner_into()
                    .map(|_| ret)
            })
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
        // TODO these two are mutually exclusive and can be combined
        if s.as_compensation().is_some() {
            return Err(ExistingLinkError::Comp);
        }
        if s.as_spillover().is_some() {
            return Err(ExistingLinkError::Spillover);
        }
        Ok(())
    }

    /// Set measurements.
    ///
    /// Return error if names are not unique, if there is more than one
    /// time measurement, or if the measurement length doesn't match the
    /// layout length.
    ///
    /// For FCS versions where $PnN is mandatory, the `prefix` argument will
    /// do nothing; for these cases use [`Core::set_measurements_noprefix`]
    /// which takes no prefix.
    pub fn set_measurements(
        &mut self,
        xs: RawInput<M::Name, Temporal<M::Temporal>, Optical<M::Optical>>,
        prefix: ShortnamePrefix,
    ) -> MultiResult<(), SetMeasurementsError>
    where
        M::Optical: AsScaleTransform,
    {
        self.check_existing_links().into_mult()?;
        let ms = NamedVec::try_new(xs, prefix).into_mult()?;
        if let Some(l) = self.layout.as_ref_opt() {
            l.check_measurement_vector(&ms).mult_errors_into()?;
            self.measurements = ms;
            Ok(())
        } else {
            Err(EmptyLayoutError).into_mult()
        }
    }

    /// Set data layout
    ///
    /// Will return error if layout does not have same number of columns as
    /// measurements.
    pub fn set_layout(
        &mut self,
        layout: <M::Ver as Versioned>::Layout,
    ) -> MultiResult<(), MeasLayoutMismatchError>
    where
        M::Optical: AsScaleTransform,
    {
        layout.check_measurement_vector(&self.measurements)?;
        self.layout = Some(layout).into();
        Ok(())
    }

    /// Set measurements and layout
    ///
    /// Return error if measurement names are not unique, there is more
    /// than one time measurement, or the layout and measurements have
    /// different lengths.
    ///
    /// For FCS versions where $PnN is mandatory, the `prefix` argument will
    /// do nothing; for these cases use [`Core::set_measurements_noprefix`]
    /// which takes no prefix.
    pub fn set_measurements_and_layout(
        &mut self,
        measurements: RawInput<M::Name, Temporal<M::Temporal>, Optical<M::Optical>>,
        layout: <M::Ver as Versioned>::Layout,
        prefix: ShortnamePrefix,
    ) -> MultiResult<(), SetMeasurementsError>
    where
        M::Optical: AsScaleTransform,
    {
        self.check_existing_links().into_mult()?;
        let ms = NamedVec::try_new(measurements, prefix).into_mult()?;
        layout.check_measurement_vector(&ms).mult_errors_into()?;
        self.measurements = ms;
        self.layout = Some(layout).into();
        Ok(())
    }

    fn unset_measurements_inner(&mut self) -> Result<(), ExistingLinkError> {
        self.check_existing_links()?;
        self.measurements = NamedVec::default();
        self.layout = None.into();
        Ok(())
    }

    fn header_and_raw_keywords(
        &self,
        tot: Tot,
        data_len: u64,
        analysis_len: u64,
        other_lens: Vec<u64>,
    ) -> Result<HeaderKeywordsToWrite, Uint8DigitOverflow>
    where
        Version: From<M::Ver>,
    {
        let req: Vec<_> = self
            .req_root_keywords()
            .chain([ReqMetarootKey::pair(&tot)])
            .chain(self.req_meas_keywords())
            .collect();
        let opt: Vec<_> = self
            .opt_root_keywords()
            .chain(self.opt_meas_keywords())
            .collect();
        if Version::from(M::Ver::fcs_version()) == Version::FCS2_0 {
            make_data_offset_keywords_2_0(req, opt, data_len, analysis_len, other_lens)
        } else {
            make_data_offset_keywords_3_0(req, opt, data_len, analysis_len, other_lens)
        }
    }

    fn opt_meas_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let ns = if !M::Name::INFALLABLE {
            Some(self.shortname_keywords())
        } else {
            None
        };
        let lv = self
            .layout
            .as_ref_opt()
            .map(|l| l.opt_meas_keywords())
            .unwrap_or_default();
        self.measurements
            .iter_with(
                &|i, x| Temporal::opt_meas_keywords(&x.value, i).collect::<Vec<_>>(),
                &|i, x| Optical::all_opt_keywords(&x.value, i).collect(),
            )
            .flatten()
            .chain(ns.into_iter().flatten())
            .chain(
                lv.into_iter()
                    .flatten()
                    .flat_map(|(k, v)| v.map(|x| (k, x))),
            )
    }

    fn req_meas_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let ns = if M::Name::INFALLABLE {
            Some(self.shortname_keywords())
        } else {
            None
        };
        let lv = self
            .layout
            .as_ref_opt()
            .map(|l| Vec::from(l.req_meas_keywords()))
            .unwrap_or_default();
        self.measurements
            .iter_with(
                &|i, x| Temporal::req_meas_keywords(&x.value, i).collect::<Vec<_>>(),
                &|i, x| Optical::all_req_keywords(&x.value, i).collect(),
            )
            .flatten()
            .chain(ns.into_iter().flatten())
            .chain(lv.into_iter().flatten())
    }

    fn req_root_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let time_meta = self
            .measurements
            .as_center()
            .map(|tc| Temporal::req_meta_keywords(tc.value));
        let lv = self
            .layout
            .as_ref_opt()
            .into_iter()
            .flat_map(|l| l.req_keywords().into_iter());
        Metaroot::all_req_keywords(&self.metaroot, self.par())
            .chain(time_meta.into_iter().flatten())
            .chain(lv)
    }

    fn opt_root_keywords(&self) -> impl Iterator<Item = (String, String)> {
        Metaroot::all_opt_keywords(&self.metaroot)
    }

    fn shortname_keywords(&self) -> impl Iterator<Item = (String, String)> {
        self.measurements
            .indexed_names()
            .map(|(i, n)| (Shortname::std(i.into()).to_string(), n.to_string()))
    }

    fn meas_table(&self, delim: &str) -> Vec<String>
    where
        M::Temporal: Clone,
        M::Optical: OpticalFromTemporal<M::Temporal> + Clone,
    {
        let ms = &self.measurements;
        if let Some(m0) = ms.get(0.into()).ok().and_then(|x| x.non_center()) {
            // ASSUME if there is one measurement then this won't be None
            let lt = self.layout.as_ref_opt().unwrap();
            let req_layout: Vec<_> = lt
                .req_meas_keywords()
                .into_iter()
                .map(|[x, y]| [x.1, y.1])
                .collect();
            let opt_layout: Vec<_> = lt
                .opt_meas_keywords()
                .into_iter()
                .map(|xs| xs.into_iter().map(|(_, v)| v).collect::<Vec<_>>())
                .collect();
            let header = m0.1.table_header(lt.opt_meas_headers());
            // TODO probably a more elegant way to do this
            let rows = self
                .measurements
                .iter()
                .map(|(i, r)| {
                    // NOTE this will force-convert all fields in the time
                    // measurement, which for this is actually want we want
                    r.both(
                        |t| {
                            let v = M::Optical::from_temporal_unchecked(t.value.clone());
                            (i, v.0, Some(&t.key))
                        },
                        |o| (i, o.value.clone(), M::Name::as_opt(&o.key)),
                    )
                })
                .zip(req_layout)
                .zip(opt_layout)
                .map(|(((i, v, n), lr), lo)| v.table_row(i, n, lr, lo));
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
        M::Temporal: Clone,
        M::Optical: OpticalFromTemporal<M::Temporal> + Clone,
    {
        for e in self.meas_table(delim) {
            println!("{}", e);
        }
    }

    #[allow(clippy::type_complexity)]
    fn lookup_measurements(
        kws: &mut StdKeywords,
        par: Par,
        nonstd: NonStdPairs,
        conf: &StdTextReadConfig,
    ) -> DeferredResult<
        (Measurements<M::Name, M::Temporal, M::Optical>, NonStdPairs),
        LookupMeasWarning,
        LookupKeysError,
    >
    where
        M: LookupMetaroot,
        M::Temporal: LookupTemporal,
        M::Optical: LookupOptical,
        Version: From<M::Ver>,
    {
        // Use nonstandard measurement pattern to assign keyvals to their
        // measurement if they match. Only capture one warning because if the
        // pattern is wrong for one measurement it is probably wrong for all of
        // them.
        let tnt = if let Some(pat) = conf.nonstandard_measurement_pattern.as_ref() {
            let res = (0..par.0)
                .map(|n| pat.apply_index(n.into()))
                .collect::<Result<Vec<_>, _>>();
            match res {
                Ok(ps) => {
                    let mut meta_nonstd = vec![];
                    let mut meas_nonstds = vec![vec![]; par.0];
                    for (k, v) in nonstd {
                        if let Some(j) = ps
                            .iter()
                            .position(|p| p.as_ref().is_match(AsRef::<str>::as_ref(&k)))
                        {
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
                        let key = M::Name::unwrap(wrapped).and_then(|name| {
                            if let Some(tp) = conf.time.pattern.as_ref() {
                                if tp.0.is_match(name.as_ref()) {
                                    return Ok(name);
                                }
                            }
                            Err(M::Name::wrap(name))
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
                            Ok(name) => Temporal::lookup_temporal(kws, i, meas_nonstd, conf)
                                .def_map_value(|t| Element::Center((name, t))),
                            Err(k) => Optical::lookup_optical(kws, i, meas_nonstd, conf)
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
                    NamedVec::try_new(xs.into(), conf.shortname_prefix.clone())
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
}

impl<M> VersionedCoreTEXT<M>
where
    M: VersionedMetaroot,
    M::Name: Clone,
{
    /// Make a new CoreTEXT from raw keywords.
    ///
    /// Return any errors encountered, including missing required keywords,
    /// parse errors, and/or deprecation warnings.
    pub(crate) fn lookup(
        kws: &mut StdKeywords,
        nonstd: NonStdKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        st: &ReadState<StdTextReadConfig>,
    ) -> DeferredResult<
        (Self, <M::Ver as Versioned>::Offsets),
        StdTEXTFromRawWarning,
        StdTEXTFromRawError,
    >
    where
        M: LookupMetaroot,
        M::Temporal: LookupTemporal,
        M::Optical: LookupOptical,
        Version: From<M::Ver>,
        <M::Ver as Versioned>::Layout: VersionedDataLayout,
    {
        // $NEXTDATA/$BEGINSTEXT/$ENDSTEXT should have already been
        // processed when we read the TEXT; remove them so they don't
        // trigger false positives later when we test for pseudostandard keys
        let _ = kws.remove(&Nextdata::std());
        let _ = kws.remove(&Beginstext::std());
        let _ = kws.remove(&Endstext::std());

        let conf = &st.conf;

        // Lookup $PAR first since we need this to get the measurements
        let par_res = Par::lookup_req(kws).def_inner_into();

        // Lookup DATA/ANALYSIS offsets and $TOT; these are not stored in the
        // Core struct but they will be needed later for parsing DATA and
        // ANALYSIS, and processing these keywords now will make it easier to
        // determine if TEXT is totally standardized or not.
        let offsets_res =
            <M::Ver as Versioned>::Offsets::lookup(kws, data, analysis, st).def_inner_into();

        par_res
            .def_and_maybe(|par| {
                // Lookup measurements/layout/metaroot with $PAR
                let ns: Vec<_> = nonstd.into_iter().collect();
                let meas_res = Self::lookup_measurements(kws, par, ns, conf).def_inner_into();
                let layout_res = <M::Ver as Versioned>::Layout::lookup(kws, conf, par)
                    .def_map_errors(Box::new)
                    .def_inner_into();
                meas_res
                    .def_zip(layout_res)
                    .def_and_maybe(|((ms, meta_ns), layout)| {
                        Metaroot::lookup_metaroot(kws, &ms, meta_ns, conf)
                            .def_map_value(|metaroot| {
                                CoreTEXT::new_unchecked(metaroot, ms, layout.into())
                            })
                            .def_inner_into()
                    })
                    .map(|mut tnt_core| {
                        // Check that the time measurement is present if we want it
                        tnt_core.eval_error(|core| {
                            if let Some(pat) = conf.time.pattern.as_ref() {
                                if !conf.time.allow_missing
                                    && core.measurements.as_center().is_none()
                                {
                                    let e = MissingTime(pat.clone());
                                    return Some(LookupKeysError::Misc(e.into()).into());
                                }
                            }
                            None
                        });

                        // The only keyword that might be left is $TIMESTEP if it
                        // wasn't used for the time measurement.
                        for k in kws.keys() {
                            if k != &Timestep::std() {
                                let e = PseudostandardError(k.clone());
                                if conf.allow_pseudostandard {
                                    tnt_core.push_warning(e.into());
                                } else {
                                    tnt_core.push_error(e.into());
                                }
                            }
                        }

                        tnt_core
                    })
            })
            .def_zip(offsets_res)
    }

    /// Remove a measurement matching the given name.
    ///
    /// Return removed measurement and its index if found.
    #[allow(clippy::type_complexity)]
    pub fn remove_measurement_by_name(
        &mut self,
        n: &Shortname,
    ) -> Option<(
        MeasIndex,
        Element<Temporal<M::Temporal>, Optical<M::Optical>>,
    )> {
        self.remove_measurement_by_name_inner(n)
    }

    /// Remove a measurement at a given position
    ///
    /// Return removed measurement and its name if found.
    #[allow(clippy::type_complexity)]
    pub fn remove_measurement_by_index(
        &mut self,
        index: MeasIndex,
    ) -> Result<EitherPair<M::Name, Temporal<M::Temporal>, Optical<M::Optical>>, ElementIndexError>
    {
        self.remove_measurement_by_index_inner(index)
    }

    /// Add time measurement to the end of the measurement vector.
    ///
    /// Return error if time measurement already exists or name is non-unique.
    pub fn push_temporal(
        &mut self,
        n: Shortname,
        m: Temporal<M::Temporal>,
        r: Range,
        notrunc: bool,
    ) -> DeferredResult<(), AnyRangeError, InsertTemporalError> {
        self.push_temporal_inner(n, m, r, notrunc)
    }

    /// Add time measurement at the given position
    ///
    /// Return error if time measurement already exists, name is non-unique, or
    /// index is out of bounds.
    pub fn insert_temporal(
        &mut self,
        i: MeasIndex,
        n: Shortname,
        m: Temporal<M::Temporal>,
        r: Range,
        notrunc: bool,
    ) -> DeferredResult<(), AnyRangeError, InsertTemporalError> {
        self.insert_temporal_inner(i, n, m, r, notrunc)
    }

    /// Add optical measurement to the end of the measurement vector
    ///
    /// Return error if name is non-unique.
    pub fn push_optical(
        &mut self,
        n: <M::Name as MightHave>::Wrapper<Shortname>,
        m: Optical<M::Optical>,
        r: Range,
        notrunc: bool,
    ) -> DeferredResult<Shortname, AnyRangeError, PushOpticalError> {
        self.push_optical_inner(n, m, r, notrunc)
    }

    /// Add optical measurement at a given position
    ///
    /// Return error if name is non-unique, or index is out of bounds.
    pub fn insert_optical(
        &mut self,
        i: MeasIndex,
        n: <M::Name as MightHave>::Wrapper<Shortname>,
        m: Optical<M::Optical>,
        r: Range,
        notrunc: bool,
    ) -> DeferredResult<Shortname, AnyRangeError, InsertOpticalError> {
        self.insert_optical_inner(i, n, m, r, notrunc)
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
        others: Others,
    ) -> Result<VersionedCoreDataset<M>, ColumsnToDataframeError> {
        let data = self.try_cols_to_dataframe(columns)?;
        Ok(self.into_coredataset_unchecked(data, analysis, others))
    }

    pub(crate) fn into_coredataset_unchecked(
        self,
        data: FCSDataFrame,
        analysis: Analysis,
        others: Others,
    ) -> VersionedCoreDataset<M> {
        CoreDataset {
            metaroot: self.metaroot,
            measurements: self.measurements,
            layout: self.layout,
            data,
            analysis,
            others,
        }
    }
}

impl<M> VersionedCoreDataset<M>
where
    M: VersionedMetaroot,
    M::Name: Clone,
    <M::Ver as Versioned>::Layout: VersionedDataLayout,
{
    pub(crate) fn new_dataset_from_raw<R: Read + Seek>(
        h: &mut BufReader<R>,
        kws: &mut StdKeywords,
        nonstd: NonStdKeywords,
        data_seg: HeaderDataSegment,
        analysis_seg: HeaderAnalysisSegment,
        other_segs: &[OtherSegment],
        st: &ReadState<DataReadConfig>,
        // TODO wrap this in a nice struct
    ) -> IODeferredResult<
        (Self, AnyDataSegment, AnyAnalysisSegment),
        StdDatasetFromRawWarning,
        StdDatasetFromRawError,
    >
    where
        M: LookupMetaroot,
        M::Temporal: LookupTemporal,
        M::Optical: LookupOptical,
        Version: From<M::Ver>,
    {
        VersionedCoreTEXT::<M>::lookup(
            kws,
            nonstd,
            data_seg,
            analysis_seg,
            &st.map_inner(|conf| &conf.standard),
        )
        .def_map_errors(Box::new)
        .def_inner_into()
        .def_errors_liftio()
        .def_and_maybe(|(text, offsets)| {
            let or = OthersReader { segs: other_segs };
            let ar = AnalysisReader {
                seg: offsets.analysis(),
            };
            // TODO what happens if the layout is empty but the segment
            // isn't?
            let data_res = text.layout.as_ref_opt().map_or(
                Ok(Tentative::new1(FCSDataFrame::default())),
                |l: &<M::Ver as Versioned>::Layout| {
                    l.h_read_df(h, offsets.tot(), offsets.data(), &st.conf.reader)
                        .def_warnings_into()
                        .def_map_errors(|e| e.inner_into())
                },
            );
            let analysis_res = ar.h_read(h).into_deferred();
            let others_res = or.h_read(h).into_deferred();
            data_res
                .def_zip3(analysis_res, others_res)
                .def_map_value(|(data, analysis, others)| {
                    let c = Core {
                        metaroot: text.metaroot,
                        measurements: text.measurements,
                        layout: text.layout,
                        data,
                        analysis,
                        others,
                    };
                    (c, offsets.data(), offsets.analysis())
                })
        })
    }

    /// Write this dataset (HEADER+TEXT+DATA+ANALYSIS+OTHER) to a handle
    pub fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        conf: &WriteConfig,
    ) -> IODeferredResult<(), ColumnError<IntRangeError<()>>, StdWriterError>
    where
        Version: From<M::Ver>,
    {
        let df = &self.data;
        let layout = self.layout.as_ref_opt();
        let others = &self.others;
        let delim = conf.delim.inner();
        let tot = Tot(df.nrows());
        let analysis_len = self.analysis.0.len() as u64;
        let other_lens = others.0.iter().map(|o| o.0.len() as u64).collect();

        layout
            .map_or(Ok(()), |l| l.check_writer(df))
            .mult_to_deferred()
            .def_errors_liftio()
            .def_and_maybe(|()| {
                let data_len = layout.map(|l| l.nbytes(df)).unwrap_or_default();
                let hdr_kws = self
                    .header_and_raw_keywords(tot, data_len, analysis_len, other_lens)
                    .map_err(ImpureError::Pure)
                    .map_err(|e| e.inner_into())
                    .map_err(DeferredFailure::new1)?;

                let mut go = || {
                    // write HEADER
                    hdr_kws.header.h_write(h, M::Ver::fcs_version().into())?;

                    // write OTHER
                    for o in others.0.iter() {
                        h.write_all(&o.0)?;
                    }

                    // write primary TEXT
                    hdr_kws.primary.h_write(h, delim)?;

                    // write supplemental TEXT
                    if !hdr_kws.supplemental.0.is_empty() {
                        hdr_kws.supplemental.h_write(h, delim)?;
                    }

                    // write DATA
                    if let Some(l) = layout {
                        l.h_write_df(h, df)?;
                    }

                    // write ANALYSIS
                    h.write_all(&self.analysis.0)
                };

                go().into_deferred()
            })
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

    // TODO add function to append event(s)

    /// Remove a measurement matching the given name.
    ///
    /// Return removed measurement and its index if found.
    #[allow(clippy::type_complexity)]
    pub fn remove_measurement_by_name(
        &mut self,
        n: &Shortname,
    ) -> Option<(
        MeasIndex,
        Element<Temporal<M::Temporal>, Optical<M::Optical>>,
    )> {
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
    ) -> Result<EitherPair<M::Name, Temporal<M::Temporal>, Optical<M::Optical>>, ElementIndexError>
    {
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
        m: Temporal<M::Temporal>,
        col: AnyFCSColumn,
        r: Range,
        notrunc: bool,
    ) -> DeferredResult<(), AnyRangeError, PushTemporalToDatasetError> {
        self.push_temporal_inner(n, m, r, notrunc)
            .def_errors_into()
            .def_and_maybe(|_| self.data.push_column(col).into_deferred())
    }

    /// Add time measurement at the given position
    ///
    /// Return error if time measurement already exists, name is non-unique, or
    /// index is out of bounds.
    pub fn insert_temporal(
        &mut self,
        i: MeasIndex,
        n: Shortname,
        m: Temporal<M::Temporal>,
        col: AnyFCSColumn,
        r: Range,
        notrunc: bool,
    ) -> DeferredResult<(), AnyRangeError, InsertTemporalToDatasetError> {
        self.insert_temporal_inner(i, n, m, r, notrunc)
            .def_errors_into()
            .def_and_maybe(|_| {
                // ASSUME index is within bounds here since it was checked above
                self.data
                    .insert_column_nocheck(i.into(), col)
                    .into_deferred()
            })
    }

    /// Add measurement to the end of the measurement vector
    ///
    /// Return error if name is non-unique.
    pub fn push_optical(
        &mut self,
        n: <M::Name as MightHave>::Wrapper<Shortname>,
        m: Optical<M::Optical>,
        col: AnyFCSColumn,
        r: Range,
        notrunc: bool,
    ) -> DeferredResult<Shortname, AnyRangeError, PushOpticalToDatasetError> {
        self.push_optical_inner(n, m, r, notrunc)
            .def_errors_into()
            .def_and_maybe(|k| {
                self.data
                    .push_column(col)
                    .into_deferred()
                    .def_map_value(|_| k)
            })
    }

    /// Add measurement at a given position
    ///
    /// Return error if name is non-unique, or index is out of bounds.
    pub fn insert_optical(
        &mut self,
        i: MeasIndex,
        n: <M::Name as MightHave>::Wrapper<Shortname>,
        m: Optical<M::Optical>,
        col: AnyFCSColumn,
        r: Range,
        notrunc: bool,
    ) -> DeferredResult<Shortname, AnyRangeError, InsertOpticalInDatasetError> {
        self.insert_optical_inner(i, n, m, r, notrunc)
            .def_errors_into()
            .def_and_maybe(|k| {
                // ASSUME index is within bounds here since it was checked above
                self.data
                    .insert_column_nocheck(i.into(), col)
                    .into_deferred()
                    .def_map_value(|_| k)
            })
    }

    /// Convert this struct into a CoreTEXT.
    ///
    /// This simply entails taking ownership and dropping the ANALYSIS and DATA
    /// fields.
    pub fn into_coretext(self) -> VersionedCoreTEXT<M> {
        CoreTEXT::new_unchecked(self.metaroot, self.measurements, self.layout)
    }

    /// Set measurements and dataframe together
    ///
    /// Length of measurements must match the width of the input dataframe.
    ///
    /// For FCS versions where $PnN is mandatory, the `prefix` argument will
    /// do nothing; for these cases use [`Core::set_measurements_noprefix`]
    /// which takes no prefix.
    pub fn set_measurements_and_data(
        &mut self,
        xs: RawInput<M::Name, Temporal<M::Temporal>, Optical<M::Optical>>,
        cs: Vec<AnyFCSColumn>,
        prefix: ShortnamePrefix,
    ) -> MultiResult<(), SetMeasurementsAndDataError>
    where
        M::Optical: AsScaleTransform,
    {
        let meas_n = xs.0.len();
        let data_n = cs.len();
        if meas_n != data_n {
            return Err(MeasDataMismatchError { meas_n, data_n }).into_mult();
        }
        let df = FCSDataFrame::try_new(cs).into_mult()?;
        self.set_measurements(xs, prefix).mult_errors_into()?;
        self.data = df;
        Ok(())
    }
}

impl<M, T, P, N, W, L> CoreTEXT<M, T, P, N, W, L> {
    pub(crate) fn new_nomeas(metaroot: Metaroot<M>) -> Self {
        Self {
            metaroot,
            measurements: NamedVec::default(),
            layout: None.into(),
            data: (),
            analysis: (),
            others: (),
        }
    }

    pub(crate) fn new_unchecked(
        metaroot: Metaroot<M>,
        measurements: NamedVec<N, W, Temporal<T>, Optical<P>>,
        layout: MaybeValue<L>,
    ) -> Self {
        Self {
            metaroot,
            measurements,
            layout,
            data: (),
            analysis: (),
            others: (),
        }
    }
}

impl HasCompensation for InnerMetaroot2_0 {
    type Comp = Compensation2_0;

    fn comp_mut(&mut self, _: private::NoTouchy) -> &mut Option<Self::Comp> {
        &mut self.comp.0
    }
}

impl HasCompensation for InnerMetaroot3_0 {
    type Comp = Compensation3_0;

    fn comp_mut(&mut self, _: private::NoTouchy) -> &mut Option<Self::Comp> {
        &mut self.comp.0
    }
}

impl HasSpillover for InnerMetaroot3_1 {
    fn spill_mut(&mut self, _: private::NoTouchy) -> &mut Option<Spillover> {
        &mut self.spillover.0
    }
}

impl HasSpillover for InnerMetaroot3_2 {
    fn spill_mut(&mut self, _: private::NoTouchy) -> &mut Option<Spillover> {
        &mut self.spillover.0
    }
}

impl HasScale for InnerOptical2_0 {
    fn scale_mut(&mut self, _: private::NoTouchy) -> &mut Option<Scale> {
        &mut self.scale.0
    }
}

impl HasScaleTransform for InnerOptical3_0 {
    fn transform_mut(&mut self, _: private::NoTouchy) -> &mut ScaleTransform {
        &mut self.scale
    }
}

impl HasScaleTransform for InnerOptical3_1 {
    fn transform_mut(&mut self, _: private::NoTouchy) -> &mut ScaleTransform {
        &mut self.scale
    }
}

impl HasScaleTransform for InnerOptical3_2 {
    fn transform_mut(&mut self, _: private::NoTouchy) -> &mut ScaleTransform {
        &mut self.scale
    }
}

impl<M, A, D, O> VersionedCore<A, D, O, M>
where
    M: VersionedMetaroot<Name = MaybeFamily>,
{
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
}

impl<A, D, O> Core3_2<A, D, O> {
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
}

impl<M, A, D, O> VersionedCore<A, D, O, M>
where
    M: VersionedMetaroot<Name = AlwaysFamily>,
{
    /// Set measurements.
    ///
    /// This is a more convenient version of [`Core::set_measurements`] for
    /// FCS versions where $PnN is mandatory, and thus the `prefix` argument
    /// is meaningless.
    pub fn set_measurements_noprefix(
        &mut self,
        xs: RawInput<AlwaysFamily, Temporal<M::Temporal>, Optical<M::Optical>>,
    ) -> MultiResult<(), SetMeasurementsError>
    where
        M::Optical: AsScaleTransform,
    {
        self.set_measurements(xs, ShortnamePrefix::default())
    }

    /// Set measurements and layout
    ///
    /// This is a more convenient version of
    /// [`Core::set_measurements_and_layout`] for FCS versions where $PnN is
    /// mandatory, and thus the `prefix` argument is meaningless.
    pub fn set_measurements_and_layout_noprefix(
        &mut self,
        xs: RawInput<AlwaysFamily, Temporal<M::Temporal>, Optical<M::Optical>>,
        layout: <M::Ver as Versioned>::Layout,
    ) -> MultiResult<(), SetMeasurementsError>
    where
        M::Optical: AsScaleTransform,
    {
        self.set_measurements_and_layout(xs, layout, ShortnamePrefix::default())
    }
}

impl<M> VersionedCoreDataset<M>
where
    M: VersionedMetaroot<Name = AlwaysFamily>,
{
    /// Set measurements and dataframe together
    ///
    /// Length of measurements must match the width of the input dataframe.
    ///
    /// This is a more convenient version of
    /// [`Core::set_measurements_and_layout`] for FCS versions where $PnN is
    /// mandatory, and thus the `prefix` argument is meaningless.
    pub fn set_measurements_and_data_noprefix(
        &mut self,
        xs: RawInput<AlwaysFamily, Temporal<M::Temporal>, Optical<M::Optical>>,
        cs: Vec<AnyFCSColumn>,
    ) -> MultiResult<(), SetMeasurementsAndDataError>
    where
        M::Optical: AsScaleTransform,
    {
        self.set_measurements_and_data(xs, cs, ShortnamePrefix::default())
    }
}

impl CoreTEXT2_0 {
    pub fn new(mode: Mode) -> Self {
        let specific = InnerMetaroot2_0::new(mode);
        let metaroot = Metaroot::new(specific);
        CoreTEXT::new_nomeas(metaroot)
    }
}

impl CoreTEXT3_0 {
    pub fn new(mode: Mode) -> Self {
        let specific = InnerMetaroot3_0::new(mode);
        let metaroot = Metaroot::new(specific);
        CoreTEXT::new_nomeas(metaroot)
    }
}

impl CoreTEXT3_1 {
    pub fn new(mode: Mode) -> Self {
        let specific = InnerMetaroot3_1::new(mode);
        let metaroot = Metaroot::new(specific);
        CoreTEXT::new_nomeas(metaroot)
    }
}

impl CoreTEXT3_2 {
    pub fn new(cyt: String) -> Self {
        let specific = InnerMetaroot3_2::new(cyt);
        let metaroot = Metaroot::new(specific);
        CoreTEXT::new_nomeas(metaroot)
    }
}

impl UnstainedData {
    fn lookup<E>(kws: &mut StdKeywords, names: &HashSet<&Shortname>) -> LookupTentative<Self, E> {
        let c = UnstainedCenters::lookup_opt(kws, names);
        let i = UnstainedInfo::lookup_opt(kws);
        c.zip(i).map(|(unstainedcenters, unstainedinfo)| Self {
            unstainedcenters,
            unstainedinfo,
        })
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            OptLinkedKey::pair_opt(&self.unstainedcenters),
            OptMetarootKey::pair_opt(&self.unstainedinfo),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn check_loss(self, lossless: bool) -> BiTentative<(), AnyMetarootKeyLossError> {
        let c = check_key_transfer(self.unstainedcenters, lossless);
        let i = check_key_transfer(self.unstainedinfo, lossless);
        c.zip(i).void()
    }
}

impl SubsetData {
    fn lookup<E>(kws: &mut StdKeywords) -> LookupTentative<MaybeValue<Self>, E> {
        Self::lookup_inner(
            kws,
            CSMode::lookup_opt,
            CSVFlag::lookup_opt,
            CSVBits::lookup_opt,
        )
        // CSMode::lookup_opt_dep(kws, is_dep, disallow_dep).and_tentatively(|m| {
        //     if let Some(n) = m.0 {
        //         let it = (0..n.0).map(|i| CSVFlag::lookup_opt(kws, i.into(), is_dep));
        //         Tentative::mconcat_ne(NonEmpty::collect(it).unwrap()).and_tentatively(|flags| {
        //             CSVBits::lookup_opt_dep(kws, is_dep, disallow_dep)
        //                 .map(|bits| Some(Self { flags, bits }).into())
        //         })
        //     } else {
        //         Tentative::new1(None.into())
        //     }
        // })
    }

    fn lookup_inner<E, F0, F1, F2>(
        kws: &mut StdKeywords,
        lookup_mode: F0,
        lookup_flag: F1,
        lookup_bits: F2,
    ) -> LookupTentative<MaybeValue<Self>, E>
    where
        F0: FnOnce(&mut StdKeywords) -> LookupTentative<MaybeValue<CSMode>, E>,
        F1: Fn(&mut StdKeywords, IndexFromOne) -> LookupTentative<MaybeValue<CSVFlag>, E>,
        F2: Fn(&mut StdKeywords) -> LookupTentative<MaybeValue<CSVBits>, E>,
    {
        lookup_mode(kws).and_tentatively(|m| {
            if let Some(n) = m.0 {
                let it = (0..n.0).map(|i| lookup_flag(kws, i.into()));
                Tentative::mconcat_ne(NonEmpty::collect(it).unwrap()).and_tentatively(|flags| {
                    lookup_bits(kws).map(|bits| Some(Self { flags, bits }).into())
                })
            } else {
                Tentative::new1(None.into())
            }
        })
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let m = CSMode(self.flags.len());
        self.flags
            .iter()
            .enumerate()
            .map(|(i, f)| OptIndexedKey::pair_opt(f, i.into()))
            .chain([OptMetarootKey::pair_opt(&self.bits)])
            .flat_map(|(k, v)| v.map(|x| (k, x)))
            .chain([OptMetarootKey::pair(&m)])
    }

    fn check_loss(self, lossless: bool) -> BiTentative<(), AnyMetarootKeyLossError> {
        let b = check_key_transfer(self.bits, lossless);
        let xs = self
            .flags
            .into_iter()
            .enumerate()
            .map(|(i, f)| check_indexed_key_transfer_own(f, i.into(), lossless))
            .collect();
        let fs = Tentative::mconcat(xs);
        let mut tnt = b.zip(fs).void();
        tnt.push_error_or_warning(UnitaryKeyLossError::<CSMode>::default(), lossless);
        tnt
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
    fn lookup(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError> {
        let ag = GatingRegions::lookup(kws, Gating::lookup_opt, Region::lookup);
        let gm = GatedMeasurements::lookup(kws, conf);
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

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let gate = Gate(self.gated_measurements.0.len());
        self.gated_measurements
            .0
            .iter()
            .enumerate()
            .flat_map(|(i, m)| m.opt_keywords(i.into()))
            .chain([OptMetarootKey::pair(&gate)])
            .chain(self.regions.opt_keywords())
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
    fn lookup<E>(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, E> {
        Self::lookup_inner(
            kws,
            |k| GatingRegions::lookup(k, Gating::lookup_opt, Region::lookup),
            |k| GatedMeasurements::lookup(k, conf),
        )
    }

    fn lookup_dep(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError> {
        let dd = conf.disallow_deprecated;
        Self::lookup_inner(
            kws,
            |k| {
                GatingRegions::lookup(
                    k,
                    |kk| Gating::lookup_opt_dep(kk, dd),
                    |kk, i| Region::lookup_dep(kk, i, dd),
                )
            },
            |k| GatedMeasurements::lookup_dep(k, conf),
        )
    }

    fn lookup_inner<E, F0, F1>(
        kws: &mut StdKeywords,
        lookup_regions: F0,
        lookup_meas: F1,
    ) -> LookupTentative<MaybeValue<Self>, E>
    where
        F0: FnOnce(&mut StdKeywords) -> LookupOptional<GatingRegions<MeasOrGateIndex>, E>,
        F1: FnOnce(&mut StdKeywords) -> LookupOptional<GatedMeasurements, E>,
    {
        let ag = lookup_regions(kws);
        let gm = lookup_meas(kws);
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

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        let g = self.gated_measurements.len();
        let gate = if g == 0 { None } else { Some(Gate(g)) };
        self.gated_measurements
            .iter()
            .enumerate()
            .flat_map(|(i, m)| m.opt_keywords(i.into()))
            .chain(self.regions.opt_keywords())
            .chain(gate.map(|x| OptMetarootKey::pair(&x)))
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

    fn try_into_2_0(
        self,
        lossless: bool,
    ) -> BiDeferredResult<AppliedGates2_0, AppliedGates3_0To2_0Error> {
        let (rs, es): (Vec<_>, Vec<_>) = self
            .regions
            .regions
            .into_iter()
            .map(|(ri, r)| r.try_map(|i| i.try_into()).map(|x| (ri, x)))
            .partition_result();
        let gr_res = NonEmpty::from_vec(rs)
            .map(|regions| GatingRegions {
                regions,
                gating: self.regions.gating,
            })
            .ok_or(AppliedGates3_0To2_0Error::NoRegions);
        let gms_res =
            NonEmpty::from_vec(self.gated_measurements).ok_or(AppliedGates3_0To2_0Error::NoGates);
        let mut res = gr_res
            .zip(gms_res)
            .mult_to_deferred()
            .def_map_value(|(regions, gs)| AppliedGates2_0 {
                gated_measurements: GatedMeasurements(gs),
                regions,
            });
        for e in es {
            res.def_push_error_or_warning(AppliedGates3_0To2_0Error::Index(e), lossless);
        }
        res
    }

    fn try_into_3_2(
        self,
        lossless: bool,
    ) -> BiDeferredResult<AppliedGates3_2, AppliedGates3_0To3_2Error> {
        let (rs, es): (Vec<_>, Vec<_>) = self
            .regions
            .regions
            .into_iter()
            .map(|(ri, r)| r.try_map(|i| i.try_into()).map(|x| (ri, x)))
            .partition_result();
        let mut res = NonEmpty::from_vec(rs)
            .map(|regions| GatingRegions {
                regions,
                gating: self.regions.gating,
            })
            .ok_or(AppliedGates3_0To3_2Error::NoRegions)
            .map(|regions| AppliedGates3_2 { regions })
            .into_deferred();
        for e in es {
            res.def_push_error_or_warning(AppliedGates3_0To3_2Error::Index(e), lossless);
        }
        let n_gates = self.gated_measurements.len();
        if n_gates > 0 {
            res.def_push_error_or_warning(AppliedGates3_0To3_2Error::HasGates(n_gates), lossless);
        }
        res
    }
}

impl AppliedGates3_2 {
    fn lookup(
        kws: &mut StdKeywords,
        disallow_deprecated: bool,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError> {
        GatingRegions::lookup(
            kws,
            |k| Gating::lookup_opt_dep(k, disallow_deprecated),
            |k, i| Region::lookup_dep(k, i, disallow_deprecated),
        )
        .map(|x| x.map(|regions| Self { regions }))
    }

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        self.regions.opt_keywords()
    }
}

impl GatedMeasurement {
    fn lookup<E>(
        kws: &mut StdKeywords,
        i: GateIndex,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self, E> {
        Self::lookup_inner(
            kws,
            i,
            |k, j| GateScale::lookup_fixed_opt(k, j, conf),
            GateFilter::lookup_opt,
            GateShortname::lookup_opt,
            GatePercentEmitted::lookup_opt,
            GateRange::lookup_opt,
            GateLongname::lookup_opt,
            GateDetectorType::lookup_opt,
            GateDetectorVoltage::lookup_opt,
        )
    }

    fn lookup_dep(
        kws: &mut StdKeywords,
        i: GateIndex,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<Self, DeprecatedError> {
        let dd = conf.disallow_deprecated;
        Self::lookup_inner(
            kws,
            i,
            |k, j| GateScale::lookup_fixed_opt_dep(k, j, conf),
            |k, j| GateFilter::lookup_opt_dep(k, j, dd),
            |k, j| GateShortname::lookup_opt_dep(k, j, dd),
            |k, j| GatePercentEmitted::lookup_opt_dep(k, j, dd),
            |k, j| GateRange::lookup_opt_dep(k, j, dd),
            |k, j| GateLongname::lookup_opt_dep(k, j, dd),
            |k, j| GateDetectorType::lookup_opt_dep(k, j, dd),
            |k, j| GateDetectorVoltage::lookup_opt_dep(k, j, dd),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn lookup_inner<E, F0, F1, F2, F3, F4, F5, F6, F7>(
        kws: &mut StdKeywords,
        i: GateIndex,
        lookup_scale: F0,
        lookup_filter: F1,
        lookup_shortname: F2,
        lookup_pe: F3,
        lookup_range: F4,
        lookup_longname: F5,
        lookup_det_type: F6,
        lookup_det_volt: F7,
    ) -> LookupTentative<Self, E>
    where
        F0: FnOnce(&mut StdKeywords, GateIndex) -> LookupOptional<GateScale, E>,
        F1: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupOptional<GateFilter, E>,
        F2: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupOptional<GateShortname, E>,
        F3: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupOptional<GatePercentEmitted, E>,
        F4: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupOptional<GateRange, E>,
        F5: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupOptional<GateLongname, E>,
        F6: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupOptional<GateDetectorType, E>,
        F7: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupOptional<GateDetectorVoltage, E>,
    {
        let j = i.into();
        let e = lookup_scale(kws, i);
        let f = lookup_filter(kws, j);
        let n = lookup_shortname(kws, j);
        let p = lookup_pe(kws, j);
        let r = lookup_range(kws, j);
        let s = lookup_longname(kws, j);
        let t = lookup_det_type(kws, j);
        let v = lookup_det_volt(kws, j);
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

    pub(crate) fn opt_keywords(&self, i: GateIndex) -> impl Iterator<Item = (String, String)> {
        let j = i.into();
        [
            OptIndexedKey::pair_opt(&self.scale, j),
            OptIndexedKey::pair_opt(&self.filter, j),
            OptIndexedKey::pair_opt(&self.shortname, j),
            OptIndexedKey::pair_opt(&self.percent_emitted, j),
            OptIndexedKey::pair_opt(&self.range, j),
            OptIndexedKey::pair_opt(&self.longname, j),
            OptIndexedKey::pair_opt(&self.detector_type, j),
            OptIndexedKey::pair_opt(&self.detector_voltage, j),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
    }
}

impl<I> GatingRegions<I> {
    fn lookup<F0, F1, E>(
        kws: &mut StdKeywords,
        lookup_gating: F0,
        lookup_region: F1,
    ) -> LookupTentative<MaybeValue<Self>, E>
    where
        F0: Fn(&mut StdKeywords) -> LookupTentative<MaybeValue<Gating>, E>,
        F1: Fn(&mut StdKeywords, RegionIndex) -> LookupTentative<MaybeValue<Region<I>>, E>,
    {
        lookup_gating(kws)
            .and_tentatively(|maybe| {
                if let Some(gating) = maybe.0 {
                    let res = gating.flatten().try_map(|ri| {
                        lookup_region(kws, ri)
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

    pub(crate) fn opt_keywords(&self) -> impl Iterator<Item = (String, String)>
    where
        I: fmt::Display,
        I: FromStr,
        I: Copy,
    {
        self.regions
            .iter()
            .flat_map(|(ri, r)| r.opt_keywords(*ri))
            .chain([OptMetarootKey::pair(&self.gating)])
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

    fn lookup<E>(kws: &mut StdKeywords, i: RegionIndex) -> LookupTentative<MaybeValue<Self>, E>
    where
        I: FromStr,
        I: fmt::Display,
        ParseOptKeyWarning: From<<RegionGateIndex<I> as FromStr>::Err>,
    {
        Self::lookup_inner(
            kws,
            i,
            RegionGateIndex::lookup_opt,
            RegionWindow::lookup_opt,
        )
    }

    fn lookup_dep(
        kws: &mut StdKeywords,
        i: RegionIndex,
        disallow_dep: bool,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError>
    where
        I: FromStr,
        I: fmt::Display,
        ParseOptKeyWarning: From<<RegionGateIndex<I> as FromStr>::Err>,
    {
        Self::lookup_inner(
            kws,
            i,
            |k, j| RegionGateIndex::lookup_opt_dep(k, j, disallow_dep),
            |k, j| RegionWindow::lookup_opt_dep(k, j, disallow_dep),
        )
    }

    fn lookup_inner<F0, F1, E>(
        kws: &mut StdKeywords,
        i: RegionIndex,
        lookup_index: F0,
        lookup_window: F1,
    ) -> LookupTentative<MaybeValue<Self>, E>
    where
        F0: FnOnce(
            &mut StdKeywords,
            IndexFromOne,
        ) -> LookupTentative<MaybeValue<RegionGateIndex<I>>, E>,
        F1: FnOnce(&mut StdKeywords, IndexFromOne) -> LookupTentative<MaybeValue<RegionWindow>, E>,
        I: FromStr,
        I: fmt::Display,
        ParseOptKeyWarning: From<<RegionGateIndex<I> as FromStr>::Err>,
    {
        let n = lookup_index(kws, i.into());
        let w = lookup_window(kws, i.into());
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

    pub(crate) fn opt_keywords(&self, i: RegionIndex) -> impl Iterator<Item = (String, String)>
    where
        I: Copy,
        I: FromStr,
        I: fmt::Display,
    {
        let (ri, rw) = self.split();
        [
            OptIndexedKey::pair(&ri, i.into()),
            OptIndexedKey::pair(&rw, i.into()),
        ]
        .into_iter()
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
    fn lookup<E>(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, E> {
        Self::lookup_inner(kws, Gate::lookup_opt, GatedMeasurement::lookup, conf)
    }

    fn lookup_dep(
        kws: &mut StdKeywords,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, DeprecatedError> {
        let dd = conf.disallow_deprecated;
        Self::lookup_inner(
            kws,
            |k| Gate::lookup_opt_dep(k, dd),
            GatedMeasurement::lookup_dep,
            conf,
        )
    }

    fn lookup_inner<E, F0, F1>(
        kws: &mut StdKeywords,
        lookup_gate: F0,
        lookup_meas: F1,
        conf: &StdTextReadConfig,
    ) -> LookupTentative<MaybeValue<Self>, E>
    where
        F0: FnOnce(&mut StdKeywords) -> LookupOptional<Gate, E>,
        F1: Fn(
            &mut StdKeywords,
            GateIndex,
            &StdTextReadConfig,
        ) -> LookupTentative<GatedMeasurement, E>,
    {
        lookup_gate(kws).and_tentatively(|maybe| {
            if let Some(n) = maybe.0 {
                // TODO this will be nicer with NonZeroUsize
                if n.0 > 0 {
                    let xs = NonEmpty::collect((0..n.0).map(|i| lookup_meas(kws, i.into(), conf)))
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
        let lmr = LastModifier::lookup_opt(kws);
        let lmd = ModifiedDateTime::lookup_opt(kws);
        let ori = Originality::lookup_opt(kws);
        lmr.zip3(lmd, ori)
            .map(|(last_modifier, last_modified, originality)| Self {
                last_modifier,
                last_modified,
                originality,
            })
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            OptMetarootKey::pair_opt(&self.last_modifier),
            OptMetarootKey::pair_opt(&self.last_modified),
            OptMetarootKey::pair_opt(&self.originality),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn check_loss(self, lossless: bool) -> BiTentative<(), AnyMetarootKeyLossError> {
        let d = check_key_transfer(self.last_modified, lossless);
        let r = check_key_transfer(self.last_modifier, lossless);
        let o = check_key_transfer(self.originality, lossless);
        d.zip3(r, o).void()
    }
}

impl CarrierData {
    fn lookup<E>(kws: &mut StdKeywords) -> LookupTentative<Self, E> {
        let l = Locationid::lookup_opt(kws);
        let i = Carrierid::lookup_opt(kws);
        let t = Carriertype::lookup_opt(kws);
        l.zip3(i, t)
            .map(|(locationid, carrierid, carriertype)| Self {
                locationid,
                carrierid,
                carriertype,
            })
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            OptMetarootKey::pair_opt(&self.carrierid),
            OptMetarootKey::pair_opt(&self.carriertype),
            OptMetarootKey::pair_opt(&self.locationid),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn check_loss(self, lossless: bool) -> BiTentative<(), AnyMetarootKeyLossError> {
        let i = check_key_transfer(self.carrierid, lossless);
        let t = check_key_transfer(self.carriertype, lossless);
        let l = check_key_transfer(self.locationid, lossless);
        i.zip3(t, l).void()
    }
}

impl PlateData {
    fn lookup<E>(kws: &mut StdKeywords) -> LookupTentative<Self, E> {
        let w = Wellid::lookup_opt(kws);
        let n = Platename::lookup_opt(kws);
        let i = Plateid::lookup_opt(kws);
        w.zip3(n, i).map(|(wellid, platename, plateid)| Self {
            wellid,
            platename,
            plateid,
        })
    }

    fn lookup_dep(
        kws: &mut StdKeywords,
        disallow_dep: bool,
    ) -> LookupTentative<Self, DeprecatedError> {
        let w = Wellid::lookup_opt_dep(kws, disallow_dep);
        let n = Platename::lookup_opt_dep(kws, disallow_dep);
        let i = Plateid::lookup_opt_dep(kws, disallow_dep);
        w.zip3(n, i).map(|(wellid, platename, plateid)| Self {
            wellid,
            platename,
            plateid,
        })
    }

    fn opt_keywords(&self) -> impl Iterator<Item = (String, String)> {
        [
            OptMetarootKey::pair_opt(&self.wellid),
            OptMetarootKey::pair_opt(&self.platename),
            OptMetarootKey::pair_opt(&self.platename),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn check_loss(self, lossless: bool) -> BiTentative<(), AnyMetarootKeyLossError> {
        let n = check_key_transfer(self.platename, lossless);
        let i = check_key_transfer(self.plateid, lossless);
        let w = check_key_transfer(self.wellid, lossless);
        n.zip3(i, w).void()
    }
}

impl PeakData {
    fn lookup<E>(kws: &mut StdKeywords, i: MeasIndex) -> LookupTentative<Self, E> {
        let b = PeakBin::lookup_opt(kws, i.into());
        let s = PeakNumber::lookup_opt(kws, i.into());
        b.zip(s).map(|(bin, size)| Self { bin, size })
    }

    fn lookup_dep(
        kws: &mut StdKeywords,
        i: MeasIndex,
        disallow_dep: bool,
    ) -> LookupTentative<Self, DeprecatedError> {
        let b = PeakBin::lookup_opt_dep(kws, i.into(), disallow_dep);
        let s = PeakNumber::lookup_opt_dep(kws, i.into(), disallow_dep);
        b.zip(s).map(|(bin, size)| Self { bin, size })
    }

    pub(crate) fn opt_keywords(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)> {
        [
            OptIndexedKey::triple(&self.bin, i.into()),
            OptIndexedKey::triple(&self.size, i.into()),
        ]
        .into_iter()
    }

    fn check_loss(self, i: MeasIndex, lossless: bool) -> BiTentative<(), AnyMeasKeyLossError> {
        let j = i.into();
        let b = check_indexed_key_transfer_own(self.bin, j, lossless);
        let s = check_indexed_key_transfer_own(self.size, j, lossless);
        b.zip(s).void()
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
    w: MaybeValue<Wavelengths>,
    force: bool,
) -> Tentative<MaybeValue<Wavelength>, WavelengthsLossError, WavelengthsLossError> {
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
        let out = ScaleTransform::try_convert_to_scale(value.scale, i, force)
            .inner_into()
            .map(|scale| Self {
                scale: Some(scale).into(),
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
        let s = ScaleTransform::try_convert_to_scale(value.scale, i, force);
        let c = check_indexed_key_transfer_own(value.calibration, j, !force);
        let d = check_indexed_key_transfer_own(value.display, j, !force);
        let w = convert_wavelengths(value.wavelengths, force).inner_into();
        let out = s
            .zip3(c, d)
            .inner_into()
            .zip(w)
            .map(|((scale, _, _), wavelength)| Self {
                scale: Some(scale).into(),
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
        let s = ScaleTransform::try_convert_to_scale(value.scale, i, force);
        let c = check_indexed_key_transfer_own(value.calibration, j, !force);
        let d = check_indexed_key_transfer_own(value.display, j, !force);
        let a = check_indexed_key_transfer_own(value.analyte, j, !force);
        let f = check_indexed_key_transfer_own(value.feature, j, !force);
        let m = check_indexed_key_transfer_own(value.measurement_type, j, !force);
        let t = check_indexed_key_transfer_own(value.tag, j, !force);
        let n = check_indexed_key_transfer_own(value.detector_name, j, !force);
        let w = convert_wavelengths(value.wavelengths, force).inner_into();
        let out = n.zip6(c, d, a, f, m).zip3(t, s).inner_into().zip(w).map(
            |((_, _, scale), wavelength)| Self {
                scale: Some(scale).into(),
                wavelength,
                peak: PeakData::default(),
            },
        );
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
            .map(|s| Self {
                scale: s.into(),
                wavelength: value.wavelength,
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
        let c =
            check_indexed_key_transfer_own::<_, AnyMeasKeyLossError>(value.calibration, j, !force);
        let d = check_indexed_key_transfer_own(value.display, j, !force);
        let w = convert_wavelengths(value.wavelengths, force).inner_into();
        let out = c.zip(d).inner_into().zip(w).map(|(_, wavelength)| Self {
            scale: value.scale,
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
        let c =
            check_indexed_key_transfer_own::<_, AnyMeasKeyLossError>(value.calibration, j, !force);
        let d = check_indexed_key_transfer_own(value.display, j, !force);
        let a = check_indexed_key_transfer_own(value.analyte, j, !force);
        let f = check_indexed_key_transfer_own(value.feature, j, !force);
        let m = check_indexed_key_transfer_own(value.measurement_type, j, !force);
        let t = check_indexed_key_transfer_own(value.tag, j, !force);
        let n = check_indexed_key_transfer_own(value.detector_name, j, !force);
        let w = convert_wavelengths(value.wavelengths, force).inner_into();
        let out = c
            .zip5(d, a, f, m)
            .zip3(t, n)
            .inner_into()
            .zip(w)
            .map(|(_, wavelength)| Self {
                scale: value.scale,
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
            .map(|s| Self {
                scale: s.into(),
                wavelengths: value.wavelength.map(|x| x.into()),
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
        let a = check_indexed_key_transfer_own::<_, AnyMeasKeyLossError>(value.analyte, j, !force);
        let f = check_indexed_key_transfer_own(value.feature, j, !force);
        let m = check_indexed_key_transfer_own(value.measurement_type, j, !force);
        let t = check_indexed_key_transfer_own(value.tag, j, !force);
        let n = check_indexed_key_transfer_own(value.detector_name, j, !force);
        let out = a.zip3(f, m).zip3(t, n).inner_into().map(|_| Self {
            scale: value.scale,
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
        force: bool,
    ) -> OpticalConvertResult<Self> {
        value
            .peak
            .check_loss(i, !force)
            .inner_into()
            .and_maybe(|_| {
                value
                    .scale
                    .0
                    .ok_or(NoScaleError(i))
                    .map(|s| Self {
                        scale: s.into(),
                        wavelengths: value.wavelength.map(|x| x.into()),
                        calibration: None.into(),
                        display: None.into(),
                        analyte: None.into(),
                        feature: None.into(),
                        tag: None.into(),
                        detector_name: None.into(),
                        measurement_type: None.into(),
                    })
                    .into_deferred()
            })
    }
}

impl ConvertFromOptical<InnerOptical3_0> for InnerOptical3_2 {
    fn convert_from_optical(
        value: InnerOptical3_0,
        i: MeasIndex,
        force: bool,
    ) -> OpticalConvertResult<Self> {
        let out = value.peak.check_loss(i, !force).inner_into().map(|_| Self {
            scale: value.scale,
            wavelengths: value.wavelength.map(|x| x.into()),
            calibration: None.into(),
            display: None.into(),
            analyte: None.into(),
            feature: None.into(),
            tag: None.into(),
            detector_name: None.into(),
            measurement_type: None.into(),
        });
        Ok(out)
    }
}

impl ConvertFromOptical<InnerOptical3_1> for InnerOptical3_2 {
    fn convert_from_optical(
        value: InnerOptical3_1,
        i: MeasIndex,
        force: bool,
    ) -> OpticalConvertResult<Self> {
        let out = value.peak.check_loss(i, !force).inner_into().map(|_| Self {
            scale: value.scale,
            wavelengths: value.wavelengths,
            calibration: value.calibration.map(|x| x.into()),
            display: value.display,
            analyte: None.into(),
            feature: None.into(),
            tag: None.into(),
            detector_name: None.into(),
            measurement_type: None.into(),
        });
        Ok(out)
    }
}

type MetarootConvertResult<M> = DeferredResult<M, MetarootConvertWarning, MetarootConvertError>;

type OpticalConvertResult<M> = DeferredResult<M, OpticalConvertWarning, OpticalConvertError>;

type TemporalConvertTentative<M> = BiTentative<M, TemporalConvertError>;

pub(crate) type LayoutConvertResult<L> = MultiResult<L, LayoutConvertError>;

#[derive(From, Display)]
pub enum OpticalConvertError {
    NoScale(NoScaleError),
    Wavelengths(WavelengthsLossError),
    Xfer(AnyMeasKeyLossError),
}

#[derive(From, Display)]
pub enum OpticalConvertWarning {
    Wavelengths(WavelengthsLossError),
    Xfer(AnyMeasKeyLossError),
}

#[derive(From, Display)]
pub enum TemporalConvertError {
    Timestep(TimestepLossError),
    Xfer(AnyMeasKeyLossError),
}

#[derive(From, Display)]
pub enum LayoutConvertError {
    OrderToEndian(OrderedToEndianError),
    Width(ConvertWidthError),
    MixedToOrdered(MixedToOrderedLayoutError),
    MixedToNonMixed(MixedToNonMixedLayoutError),
}

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

macro_rules! impl_ref {
    ($outer:ident, $inner:ident) => {
        impl AsRef<$inner> for $outer<$inner> {
            fn as_ref(&self) -> &$inner {
                &self.specific
            }
        }

        impl AsMut<$inner> for $outer<$inner> {
            fn as_mut(&mut self) -> &mut $inner {
                &mut self.specific
            }
        }
    };
}

impl_ref!(Metaroot, InnerMetaroot2_0);
impl_ref!(Metaroot, InnerMetaroot3_0);
impl_ref!(Metaroot, InnerMetaroot3_1);
impl_ref!(Metaroot, InnerMetaroot3_2);

impl_ref!(Optical, InnerOptical2_0);
impl_ref!(Optical, InnerOptical3_0);
impl_ref!(Optical, InnerOptical3_1);
impl_ref!(Optical, InnerOptical3_2);

impl_ref!(Temporal, InnerTemporal2_0);
impl_ref!(Temporal, InnerTemporal3_0);
impl_ref!(Temporal, InnerTemporal3_1);
impl_ref!(Temporal, InnerTemporal3_2);

macro_rules! impl_ref_specific_ro {
    ($outer:ident, $inner:ident, $($ref:path),*) => {
        $(
            impl AsRef<$ref> for $outer<$inner> {
                fn as_ref(&self) -> &$ref {
                    self.specific.as_ref()
                }
            }
        )*
    };
}

macro_rules! impl_ref_specific_rw {
    ($outer:ident, $inner:ident, $($ref:path),*) => {
        $(
            impl AsMut<$ref> for $outer<$inner> {
                fn as_mut(&mut self) -> &mut $ref {
                    self.specific.as_mut()
                }
            }

            impl_ref_specific_ro!($outer, $inner, $ref);
        )*
    };
}

impl_ref_specific_rw!(Metaroot, InnerMetaroot2_0, Mode, Option<Cyt>, Timestamps2_0);

impl_ref_specific_rw!(
    Metaroot,
    InnerMetaroot3_0,
    Mode,
    Option<Cyt>,
    Option<Cytsn>,
    Option<Unicode>,
    Timestamps3_0
);

impl_ref_specific_rw!(
    Metaroot,
    InnerMetaroot3_1,
    Mode,
    Option<Cyt>,
    Option<Cytsn>,
    Option<LastModifier>,
    Option<ModifiedDateTime>,
    Option<Originality>,
    Option<Plateid>,
    Option<Wellid>,
    Option<Platename>,
    Option<Vol>,
    Timestamps3_1
);

impl_ref_specific_rw!(
    Metaroot,
    InnerMetaroot3_2,
    Cyt,
    Datetimes,
    Option<Cytsn>,
    Option<LastModifier>,
    Option<ModifiedDateTime>,
    Option<Originality>,
    Option<Plateid>,
    Option<Wellid>,
    Option<Platename>,
    Option<Carrierid>,
    Option<Carriertype>,
    Option<Locationid>,
    Option<Vol>,
    Option<Flowrate>,
    Timestamps3_1
);

impl_ref_specific_rw!(
    Optical,
    InnerOptical2_0,
    Option<Wavelength>,
    Option<PeakBin>,
    Option<PeakNumber>
);

impl_ref_specific_rw!(
    Optical,
    InnerOptical3_0,
    Option<Wavelength>,
    Option<PeakBin>,
    Option<PeakNumber>
);

impl_ref_specific_rw!(
    Optical,
    InnerOptical3_1,
    Option<Wavelengths>,
    Option<PeakBin>,
    Option<PeakNumber>,
    Option<Calibration3_1>,
    Option<Display>
);

impl_ref_specific_rw!(
    Optical,
    InnerOptical3_2,
    Option<Wavelengths>,
    Option<Calibration3_2>,
    Option<Display>,
    Option<Analyte>,
    Option<Feature>,
    Option<OpticalType>,
    Option<Tag>,
    Option<DetectorName>
);

// impl_ref_specific!(Temporal, InnerTemporal2_0,);

impl_ref_specific_rw!(Temporal, InnerTemporal3_0, Timestep);

impl_ref_specific_rw!(Temporal, InnerTemporal3_1, Timestep, Option<Display>);

impl_ref_specific_rw!(Temporal, InnerTemporal3_2, Timestep, Option<Display>);

impl_ref_specific_ro!(
    Metaroot,
    InnerMetaroot2_0,
    Option<FCSDate>,
    Option<Compensation2_0>
);

impl_ref_specific_ro!(
    Metaroot,
    InnerMetaroot3_0,
    Option<FCSDate>,
    Option<Compensation3_0>
);

impl_ref_specific_ro!(Metaroot, InnerMetaroot3_1, Option<FCSDate>);

impl_ref_specific_ro!(
    Metaroot,
    InnerMetaroot3_2,
    Option<FCSDate>,
    Option<BeginDateTime>,
    Option<EndDateTime>
);

impl_ref_specific_ro!(Optical, InnerOptical2_0, Option<Scale>);

impl_ref_specific_ro!(Optical, InnerOptical3_0, ScaleTransform);

impl_ref_specific_ro!(Optical, InnerOptical3_1, ScaleTransform);

impl_ref_specific_ro!(Optical, InnerOptical3_2, ScaleTransform);

impl<M, T, P, N, W, L> AsRef<FCSDataFrame> for CoreDataset<M, T, P, N, W, L> {
    fn as_ref(&self) -> &FCSDataFrame {
        &self.data
    }
}

impl<M, T, P, N, W, L> AsRef<Analysis> for CoreDataset<M, T, P, N, W, L> {
    fn as_ref(&self) -> &Analysis {
        &self.analysis
    }
}

impl<M, T, P, N, W, L> AsRef<Others> for CoreDataset<M, T, P, N, W, L> {
    fn as_ref(&self) -> &Others {
        &self.others
    }
}

impl<X, M, const IS_ETIM: bool> AsRef<Option<Xtim<IS_ETIM, X>>> for Metaroot<M>
where
    Metaroot<M>: AsRef<Timestamps<X>>,
    Timestamps<X>: AsRef<Option<Xtim<IS_ETIM, X>>>,
{
    fn as_ref(&self) -> &Option<Xtim<IS_ETIM, X>> {
        self.as_ref().as_ref()
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_0> for InnerMetaroot2_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_0,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let c = check_key_transfer(value.cytsn, lossless);
        let u = check_key_transfer(value.unicode, lossless);
        let s = value
            .subset
            .0
            .map(|ss| ss.check_loss(lossless))
            .unwrap_or(Tentative::new1(()));
        let ret = c.zip3(u, s).inner_into().and_tentatively(|_| {
            value
                .applied_gates
                .0
                .map_or(Tentative::new1(None), |x| {
                    x.try_into_2_0(lossless).def_unfail().inner_into()
                })
                .map(|ag| Self {
                    mode: value.mode,
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
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let c = check_key_transfer(value.cytsn, lossless);
        let v = check_key_transfer(value.vol, lossless);
        let s = check_key_transfer(value.spillover, lossless);
        let m = value.modification.check_loss(lossless);
        let p = value.plate.check_loss(lossless);
        let ss = value
            .subset
            .0
            .map(|ss| ss.check_loss(lossless))
            .unwrap_or(Tentative::new1(()));
        let out = c.zip6(v, s, m, p, ss).inner_into().and_tentatively(|_| {
            value
                .applied_gates
                .0
                .map_or(Tentative::new1(None), |x| {
                    x.try_into_2_0(lossless).def_unfail().inner_into()
                })
                .map(|ag| Self {
                    mode: value.mode,
                    cyt: value.cyt,
                    comp: None.into(),
                    timestamps: value.timestamps.map(|d| d.into()),
                    applied_gates: ag.into(),
                })
        });
        Ok(out)
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_2> for InnerMetaroot2_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_2,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let cy = check_key_transfer(value.cytsn, lossless);
        let v = check_key_transfer(value.vol, lossless);
        let s = check_key_transfer(value.spillover, lossless);
        let f = check_key_transfer(value.flowrate, lossless);
        let m = value.modification.check_loss(lossless);
        let p = value.plate.check_loss(lossless);
        let d = value.datetimes.check_loss(lossless);
        let ca = value.carrier.check_loss(lossless);
        let u = value.unstained.check_loss(lossless);
        let mut ret = cy
            .zip6(v, s, f, m, p)
            .zip4(d, ca, u)
            .inner_into()
            .map(|_| Self {
                mode: Mode::List,
                cyt: Some(value.cyt).into(),
                comp: None.into(),
                timestamps: value.timestamps.map(|x| x.into()),
                applied_gates: None.into(),
            });
        if value.applied_gates.0.is_some() {
            ret.push_error_or_warning(AppliedGates3_2To2_0Error, lossless);
        }
        Ok(ret)
    }
}

impl ConvertFromMetaroot<InnerMetaroot2_0> for InnerMetaroot3_0 {
    fn convert_from_metaroot(value: InnerMetaroot2_0, _: bool) -> MetarootConvertResult<Self> {
        Ok(Tentative::new1(Self {
            mode: value.mode,
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
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let p = value.plate.check_loss(lossless);
        let m = value.modification.check_loss(lossless);
        let v = check_key_transfer(value.vol, lossless);
        let out = p.zip3(m, v).inner_into().map(|_| Self {
            mode: value.mode,
            cyt: value.cyt,
            cytsn: value.cytsn,
            timestamps: value.timestamps.map(|d| d.into()),
            comp: None.into(),
            unicode: None.into(),
            subset: None.into(),
            applied_gates: value.applied_gates,
        });
        Ok(out)
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_2> for InnerMetaroot3_0 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_2,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let v = check_key_transfer(value.vol, lossless);
        let f = check_key_transfer(value.flowrate, lossless);
        let m = value.modification.check_loss(lossless);
        let p = value.plate.check_loss(lossless);
        let d = value.datetimes.check_loss(lossless);
        let ca = value.carrier.check_loss(lossless);
        let u = value.unstained.check_loss(lossless);
        let out = v.zip6(f, m, p, d, ca).zip(u).inner_into().map(|_| Self {
            mode: Mode::List,
            cyt: Some(value.cyt).into(),
            cytsn: value.cytsn,
            timestamps: value.timestamps.map(|x| x.into()),
            comp: None.into(),
            unicode: None.into(),
            subset: None.into(),
            applied_gates: value.applied_gates.map(|x| x.into()),
        });
        Ok(out)
    }
}

impl ConvertFromMetaroot<InnerMetaroot2_0> for InnerMetaroot3_1 {
    fn convert_from_metaroot(
        value: InnerMetaroot2_0,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let mut out = Tentative::new1(Self {
            mode: value.mode,
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
            out.push_error_or_warning(Comp2_0TransferError, lossless);
        }
        Ok(out)
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_0> for InnerMetaroot3_1 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_0,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let c = check_key_transfer(value.comp, lossless);
        let u = check_key_transfer(value.unicode, lossless);
        let out = c.zip(u).inner_into().map(|_| Self {
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
        });
        Ok(out)
    }
}

impl ConvertFromMetaroot<InnerMetaroot3_2> for InnerMetaroot3_1 {
    fn convert_from_metaroot(
        value: InnerMetaroot3_2,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let d = value.datetimes.check_loss(lossless);
        let ca = value.carrier.check_loss(lossless);
        let u = value.unstained.check_loss(lossless);
        let f = check_key_transfer(value.flowrate, lossless);
        let ret = d.zip4(ca, u, f).inner_into().map(|_| Self {
            mode: Mode::List,
            cyt: Some(value.cyt).into(),
            cytsn: value.cytsn,
            timestamps: value.timestamps,
            spillover: value.spillover,
            plate: value.plate,
            modification: value.modification,
            vol: value.vol,
            subset: None.into(),
            applied_gates: value.applied_gates.map(|x| x.into()),
        });
        Ok(ret)
    }
}

impl ConvertFromMetaroot<InnerMetaroot2_0> for InnerMetaroot3_2 {
    fn convert_from_metaroot(
        value: InnerMetaroot2_0,
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let mut res = value
            .cyt
            .0
            .ok_or(NoCytError)
            .into_deferred()
            .def_map_value(|cyt| Self {
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
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let u = check_key_transfer(value.unicode, lossless);
        let co = check_key_transfer(value.comp, lossless);
        let ss = value
            .subset
            .0
            .map(|ss| ss.check_loss(lossless))
            .unwrap_or(Tentative::new1(()));
        let mut res = u.zip3(co, ss).inner_into().and_maybe(|_| {
            value
                .applied_gates
                .0
                .map_or(Tentative::new1(None), |x| {
                    x.try_into_3_2(lossless).def_unfail().inner_into()
                })
                .and_maybe(|ag| {
                    value
                        .cyt
                        .0
                        .ok_or(NoCytError)
                        .into_deferred()
                        .def_map_value(|cyt| Self {
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
        lossless: bool,
    ) -> MetarootConvertResult<Self> {
        let ss = value
            .subset
            .0
            .map(|ss| ss.check_loss(lossless))
            .unwrap_or(Tentative::new1(()))
            .inner_into();
        let a = value.applied_gates.0.map_or(Tentative::new1(None), |x| {
            x.try_into_3_2(lossless).def_unfail().inner_into()
        });
        let mut res = ss.zip(a).and_maybe(|(_, ag)| {
            value
                .cyt
                .0
                .ok_or(NoCytError)
                .into_deferred()
                .def_map_value(|cyt| Self {
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

fn check_indexed_key_transfer<T, E>(x: &MaybeValue<T>, i: IndexFromOne) -> Result<(), E>
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
) -> MultiResult<(), AnyOpticalToTemporalKeyLossError> {
    let j = i.into();
    let f = check_indexed_key_transfer(&x.filter, j);
    let o = check_indexed_key_transfer(&x.power, j);
    let t = check_indexed_key_transfer(&x.detector_type, j);
    let p = check_indexed_key_transfer(&x.percent_emitted, j);
    let v = check_indexed_key_transfer(&x.detector_voltage, j);
    f.zip3(o, t).mult_zip(p.zip(v)).map(|_| ())
}

fn check_indexed_key_transfer_own<T, E>(
    x: MaybeValue<T>,
    i: IndexFromOne,
    lossless: bool,
) -> BiTentative<(), E>
where
    E: From<IndexedKeyLossError<T>>,
{
    let mut tnt = Tentative::new1(());
    if x.0.is_some() {
        tnt.push_error_or_warning(IndexedKeyLossError::<T>::new(i), lossless);
    }
    tnt
}

fn check_key_transfer<T>(
    x: MaybeValue<T>,
    lossless: bool,
) -> BiTentative<(), AnyMetarootKeyLossError>
where
    AnyMetarootKeyLossError: From<UnitaryKeyLossError<T>>,
{
    let mut tnt = Tentative::new1(());
    if x.0.is_some() {
        tnt.push_error_or_warning(UnitaryKeyLossError::<T>::default(), lossless);
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

impl ScaleTransform {
    /// Convert to a simple scale value (just $PnE, no $PnG).
    ///
    /// This may be lossy because the $PnG value cannot be represented with
    /// just a `Scale` object, and thus is needs to be dropped if present and
    /// not equal to 1.0.
    fn try_convert_to_scale(
        self,
        i: MeasIndex,
        force: bool,
    ) -> BiTentative<Scale, AnyMeasKeyLossError> {
        match self {
            Self::Lin(x) => {
                let mut ret = Tentative::new1(Scale::Linear);
                if f32::from(x) != 1.0 {
                    ret.push_error_or_warning(IndexedKeyLossError::<Gain>::new(i.into()), !force);
                }
                ret
            }
            Self::Log(x) => Tentative::new1(Scale::Log(x)),
        }
    }

    fn lookup(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<ScaleTransform> {
        Gain::lookup_opt(kws, i.into()).and_maybe(|g| {
            Scale::lookup_fixed_req(kws, i, conf.fix_log_scale_offsets).def_and_maybe(|s| {
                ScaleTransform::try_from((s, g))
                    .into_deferred::<_, LookupMiscError>()
                    .def_errors_into()
            })
        })
    }

    fn req_suffixes(&self, i: MeasIndex) -> impl Iterator<Item = (MeasHeader, String, String)> {
        let (scale, _): (Scale, _) = (*self).into();
        [scale.triple(i.into())].into_iter()
    }

    fn opt_suffixes(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)> {
        let (_, gain): (_, MaybeValue<Gain>) = (*self).into();
        [OptIndexedKey::triple(&gain, i.into())].into_iter()
    }

    pub(crate) fn is_noop(&self) -> bool {
        *self == Self::default()
    }
}

impl From<Scale> for ScaleTransform {
    fn from(value: Scale) -> Self {
        match value {
            Scale::Linear => Self::Lin(PositiveFloat::one()),
            Scale::Log(x) => Self::Log(x),
        }
    }
}

impl From<ScaleTransform> for (Scale, MaybeValue<Gain>) {
    fn from(value: ScaleTransform) -> Self {
        match value {
            ScaleTransform::Lin(g) => (Scale::Linear, Some(Gain(g)).into()),
            ScaleTransform::Log(l) => (Scale::Log(l), None.into()),
        }
    }
}

impl TryFrom<(Scale, MaybeValue<Gain>)> for ScaleTransform {
    type Error = ScaleTransformError;

    /// Convert values for $PnE and $PnG to a scale transform (3.0+)
    ///
    /// If scale is linear, return a linear transform with slope equal to $PnG
    /// or 1.0 if $PnG not given.
    ///
    /// If scale is log, return a log transform with the parameters in $PnE.
    /// Return error if $PnG is given and not 1.0.
    fn try_from(value: (Scale, MaybeValue<Gain>)) -> Result<Self, Self::Error> {
        let scale = value.0;
        let gain = (value.1).0;
        match scale {
            Scale::Linear => Ok(Self::Lin(gain.map(|g| g.0).unwrap_or(PositiveFloat::one()))),
            Scale::Log(l) => {
                if let Some(g) = gain {
                    if f32::from(g.0) != 1.0 {
                        return Err(ScaleTransformError { scale, gain: g });
                    }
                }
                Ok(Self::Log(l))
            }
        }
    }
}

impl Default for ScaleTransform {
    fn default() -> Self {
        Self::Lin(PositiveFloat::one())
    }
}

impl fmt::Display for ScaleTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Lin(x) => write!(f, "Lin({x})"),
            Self::Log(x) => write!(f, "Log({x})"),
        }
    }
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
        let d = check_indexed_key_transfer_own::<_, AnyMeasKeyLossError>(
            value.display,
            i.into(),
            !force,
        )
        .inner_into();
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
        let di = check_indexed_key_transfer_own::<_, AnyMeasKeyLossError>(value.display, j, !force);
        let m = check_indexed_key_transfer_own(value.measurement_type, j, !force);
        let t = check_timestep(value.timestep, force);
        di.zip(m).inner_into().zip(t).map(|_| Self {
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
        check_indexed_key_transfer_own::<_, AnyMeasKeyLossError>(value.display, i.into(), !force)
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
        let di = check_indexed_key_transfer_own::<_, AnyMeasKeyLossError>(value.display, j, !force);
        let m = check_indexed_key_transfer_own(value.measurement_type, j, !force);
        di.zip(m).inner_into().map(|_| Self {
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
        check_indexed_key_transfer_own::<_, AnyMeasKeyLossError>(
            value.measurement_type,
            i.into(),
            !force,
        )
        .inner_into()
        .map(|_| Self {
            timestep: value.timestep,
            display: value.display,
            peak: PeakData::default(),
        })
    }
}

impl ConvertFromTemporal<InnerTemporal2_0> for InnerTemporal3_2 {
    fn convert_from_temporal(
        value: InnerTemporal2_0,
        i: MeasIndex,
        force: bool,
    ) -> TemporalConvertTentative<Self> {
        value.peak.check_loss(i, !force).inner_into().map(|_| Self {
            timestep: Timestep::default(),
            display: None.into(),
            measurement_type: None.into(),
        })
    }
}

impl ConvertFromTemporal<InnerTemporal3_0> for InnerTemporal3_2 {
    fn convert_from_temporal(
        value: InnerTemporal3_0,
        i: MeasIndex,
        force: bool,
    ) -> TemporalConvertTentative<Self> {
        value.peak.check_loss(i, !force).inner_into().map(|_| Self {
            timestep: value.timestep,
            display: None.into(),
            measurement_type: None.into(),
        })
    }
}

impl ConvertFromTemporal<InnerTemporal3_1> for InnerTemporal3_2 {
    fn convert_from_temporal(
        value: InnerTemporal3_1,
        i: MeasIndex,
        force: bool,
    ) -> TemporalConvertTentative<Self> {
        value.peak.check_loss(i, !force).inner_into().map(|_| Self {
            timestep: value.timestep,
            display: value.display,
            measurement_type: None.into(),
        })
    }
}

impl ConvertFromLayout<DataLayout3_0> for DataLayout2_0 {
    fn convert_from_layout(value: DataLayout3_0) -> LayoutConvertResult<Self> {
        Ok(Self(value.0.phantom_into()))
    }
}

impl ConvertFromLayout<DataLayout3_1> for DataLayout2_0 {
    fn convert_from_layout(value: DataLayout3_1) -> LayoutConvertResult<Self> {
        value.into_ordered().map(|x| x.into())
    }
}

impl ConvertFromLayout<DataLayout3_2> for DataLayout2_0 {
    fn convert_from_layout(value: DataLayout3_2) -> LayoutConvertResult<Self> {
        value.into_ordered().map(|x| x.into())
    }
}

impl ConvertFromLayout<DataLayout2_0> for DataLayout3_0 {
    fn convert_from_layout(value: DataLayout2_0) -> LayoutConvertResult<Self> {
        Ok(Self(value.0.phantom_into()))
    }
}

impl ConvertFromLayout<DataLayout3_1> for DataLayout3_0 {
    fn convert_from_layout(value: DataLayout3_1) -> LayoutConvertResult<Self> {
        value.into_ordered().map(|x| x.into())
    }
}

impl ConvertFromLayout<DataLayout3_2> for DataLayout3_0 {
    fn convert_from_layout(value: DataLayout3_2) -> LayoutConvertResult<Self> {
        value.into_ordered().map(|x| x.into())
    }
}

impl ConvertFromLayout<DataLayout2_0> for DataLayout3_1 {
    fn convert_from_layout(value: DataLayout2_0) -> LayoutConvertResult<Self> {
        value.0.into_3_1()
    }
}

impl ConvertFromLayout<DataLayout3_0> for DataLayout3_1 {
    fn convert_from_layout(value: DataLayout3_0) -> LayoutConvertResult<Self> {
        value.0.into_3_1()
    }
}

impl ConvertFromLayout<DataLayout3_2> for DataLayout3_1 {
    fn convert_from_layout(value: DataLayout3_2) -> LayoutConvertResult<Self> {
        match value {
            DataLayout3_2::NonMixed(x) => Ok(Self(x.phantom_into())),
            DataLayout3_2::Mixed(x) => x.try_into_non_mixed().map(Self).mult_errors_into(),
        }
    }
}

impl ConvertFromLayout<DataLayout2_0> for DataLayout3_2 {
    fn convert_from_layout(value: DataLayout2_0) -> LayoutConvertResult<Self> {
        value.0.into_3_2()
    }
}

impl ConvertFromLayout<DataLayout3_0> for DataLayout3_2 {
    fn convert_from_layout(value: DataLayout3_0) -> LayoutConvertResult<Self> {
        value.0.into_3_2()
    }
}

impl ConvertFromLayout<DataLayout3_1> for DataLayout3_2 {
    fn convert_from_layout(value: DataLayout3_1) -> LayoutConvertResult<Self> {
        Ok(DataLayout3_2::NonMixed(value.0.phantom_into()))
    }
}

impl Versioned for Version2_0 {
    type Layout = DataLayout2_0;
    type Offsets = TEXTOffsets2_0;

    fn fcs_version() -> Self {
        Version2_0
    }
}

impl Versioned for Version3_0 {
    type Layout = DataLayout3_0;
    type Offsets = TEXTOffsets3_0;

    fn fcs_version() -> Self {
        Version3_0
    }
}

impl Versioned for Version3_1 {
    type Layout = DataLayout3_1;
    type Offsets = TEXTOffsets3_0;

    fn fcs_version() -> Self {
        Version3_1
    }
}

impl Versioned for Version3_2 {
    type Layout = DataLayout3_2;
    type Offsets = TEXTOffsets3_2;

    fn fcs_version() -> Self {
        Version3_2
    }
}

impl AsScaleTransform for InnerOptical2_0 {
    fn as_transform(&self) -> ScaleTransform {
        self.scale.0.map(|s| s.into()).unwrap_or_default()
    }
}

impl AsScaleTransform for InnerOptical3_0 {
    fn as_transform(&self) -> ScaleTransform {
        self.scale
    }
}

impl AsScaleTransform for InnerOptical3_1 {
    fn as_transform(&self) -> ScaleTransform {
        self.scale
    }
}

impl AsScaleTransform for InnerOptical3_2 {
    fn as_transform(&self) -> ScaleTransform {
        self.scale
    }
}

impl LookupOptical for InnerOptical2_0 {
    fn lookup_specific(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let s = Scale::lookup_fixed_opt(kws, i, conf);
        let w = Wavelength::lookup_opt(kws, i.into());
        let p = PeakData::lookup(kws, i);
        Ok(s.zip3(w, p).map(|(scale, wavelength, peak)| Self {
            scale,
            wavelength,
            peak,
        }))
    }
}

impl LookupOptical for InnerOptical3_0 {
    fn lookup_specific(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let w = Wavelength::lookup_opt(kws, i.into());
        let p = PeakData::lookup(kws, i);
        w.zip(p).and_maybe(|(wavelength, peak)| {
            ScaleTransform::lookup(kws, i, conf).def_map_value(|scale| Self {
                scale,
                wavelength,
                peak,
            })
        })
    }
}

impl LookupOptical for InnerOptical3_1 {
    fn lookup_specific(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let w = Wavelengths::lookup_opt(kws, i.into());
        let c = Calibration3_1::lookup_opt(kws, i.into());
        let d = Display::lookup_opt(kws, i.into());
        let p = PeakData::lookup_dep(kws, i, conf.disallow_deprecated);
        w.zip4(c, d, p)
            .errors_into()
            .and_maybe(|(wavelengths, calibration, display, peak)| {
                ScaleTransform::lookup(kws, i, conf).def_map_value(|scale| Self {
                    scale,
                    wavelengths,
                    calibration,
                    display,
                    peak,
                })
            })
    }
}

impl LookupOptical for InnerOptical3_2 {
    fn lookup_specific(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let w = Wavelengths::lookup_opt(kws, i.into());
        let c = Calibration3_2::lookup_opt(kws, i.into());
        let d = Display::lookup_opt(kws, i.into());
        let de = DetectorName::lookup_opt(kws, i.into());
        let ta = Tag::lookup_opt(kws, i.into());
        let m = OpticalType::lookup_opt(kws, i.into());
        let f = Feature::lookup_opt(kws, i.into());
        let a = Analyte::lookup_opt(kws, i.into());
        w.zip4(c, d, de).zip5(ta, m, f, a).and_maybe(
            |(
                (wavelengths, calibration, display, detector_name),
                tag,
                measurement_type,
                feature,
                analyte,
            )| {
                ScaleTransform::lookup(kws, i, conf).def_map_value(|scale| Self {
                    scale,
                    wavelengths,
                    calibration,
                    display,
                    detector_name,
                    tag,
                    measurement_type,
                    feature,
                    analyte,
                })
            },
        )
    }
}

impl LookupTemporal for InnerTemporal2_0 {
    fn lookup_specific(
        kws: &mut StdKeywords,
        i: MeasIndex,
        _: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        // TODO push meas index with error
        let s = TemporalScale::lookup_opt(kws, i.into());
        let p = PeakData::lookup(kws, i);
        Ok(s.zip(p).map(|(scale, peak)| Self { peak, scale }))
    }
}

impl LookupTemporal for InnerTemporal3_0 {
    fn lookup_specific(
        kws: &mut StdKeywords,
        i: MeasIndex,
        _: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let mut tnt_gain = Gain::lookup_opt(kws, i.into());
        tnt_gain.eval_error(|gain| {
            if gain.0.is_some() {
                Some(LookupKeysError::Misc(TemporalError::HasGain.into()))
            } else {
                None
            }
        });
        let tnt_peak = PeakData::lookup(kws, i);
        tnt_gain.zip(tnt_peak).and_maybe(|(_, peak)| {
            let s = TemporalScale::lookup_req(kws, i.into());
            let t = Timestep::lookup_req(kws);
            s.def_zip(t)
                .def_map_value(|(_, timestep)| Self { timestep, peak })
        })
    }
}

impl LookupTemporal for InnerTemporal3_1 {
    fn lookup_specific(
        kws: &mut StdKeywords,
        i: MeasIndex,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let g = lookup_temporal_gain_3_0(kws, i.into());
        let d = Display::lookup_opt(kws, i.into());
        let p = PeakData::lookup_dep(kws, i, conf.disallow_deprecated).errors_into();
        g.zip3(d, p).and_maybe(|(_, display, peak)| {
            let s = TemporalScale::lookup_req(kws, i.into());
            let t = Timestep::lookup_req(kws);
            s.def_zip(t).def_map_value(|(_, timestep)| Self {
                timestep,
                display,
                peak,
            })
        })
    }
}

impl LookupTemporal for InnerTemporal3_2 {
    fn lookup_specific(
        kws: &mut StdKeywords,
        i: MeasIndex,
        _: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let g = lookup_temporal_gain_3_0(kws, i.into());
        let di = Display::lookup_opt(kws, i.into());
        let m = TemporalType::lookup_opt(kws, i.into());
        g.zip3(di, m).and_maybe(|(_, display, measurement_type)| {
            let s = TemporalScale::lookup_req(kws, i.into());
            let t = Timestep::lookup_req(kws);
            s.def_zip(t).def_map_value(|(_, timestep)| Self {
                timestep,
                display,
                measurement_type,
            })
        })
    }
}

impl VersionedOptical for InnerOptical2_0 {
    type Ver = Version2_0;
    fn req_suffixes_inner(
        &self,
        _: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, String)> {
        [].into_iter()
    }

    fn opt_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)> {
        [
            OptIndexedKey::triple(&self.scale, i.into()),
            OptIndexedKey::triple(&self.wavelength, i.into()),
        ]
        .into_iter()
        .chain(self.peak.opt_keywords(i))
    }

    fn can_convert_to_temporal(&self, i: MeasIndex) -> MultiResult<(), OpticalToTemporalError> {
        let mut res = check_indexed_key_transfer(&self.wavelength, i.into())
            .map_err(OpticalToTemporalError::Loss)
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
    type Ver = Version3_0;
    fn req_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, String)> {
        self.scale.req_suffixes(i)
    }

    fn opt_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)> {
        [OptIndexedKey::triple(&self.wavelength, i.into())]
            .into_iter()
            .chain(self.peak.opt_keywords(i))
            .chain(self.scale.opt_suffixes(i))
    }

    fn can_convert_to_temporal(&self, i: MeasIndex) -> MultiResult<(), OpticalToTemporalError> {
        let j = i.into();
        let w =
            check_indexed_key_transfer(&self.wavelength, j).map_err(OpticalToTemporalError::Loss);
        let s = if !self.scale.is_noop() {
            Err(OpticalNonLinearError.into())
        } else {
            Ok(())
        };
        w.zip(s).void()
    }
}

impl VersionedOptical for InnerOptical3_1 {
    type Ver = Version3_1;
    fn req_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, String)> {
        self.scale.req_suffixes(i)
    }

    fn opt_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)> {
        [
            OptIndexedKey::triple(&self.wavelengths, i.into()),
            OptIndexedKey::triple(&self.calibration, i.into()),
            OptIndexedKey::triple(&self.display, i.into()),
        ]
        .into_iter()
        .chain(self.peak.opt_keywords(i))
        .chain(self.scale.opt_suffixes(i))
    }

    fn can_convert_to_temporal(&self, i: MeasIndex) -> MultiResult<(), OpticalToTemporalError> {
        let j = i.into();
        let c =
            check_indexed_key_transfer(&self.calibration, j).map_err(OpticalToTemporalError::Loss);
        let w =
            check_indexed_key_transfer(&self.wavelengths, j).map_err(OpticalToTemporalError::Loss);
        let s = if !self.scale.is_noop() {
            Err(OpticalNonLinearError.into())
        } else {
            Ok(())
        };
        c.zip3(w, s).void()
    }
}

impl VersionedOptical for InnerOptical3_2 {
    type Ver = Version3_2;
    fn req_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, String)> {
        self.scale.req_suffixes(i)
    }

    fn opt_suffixes_inner(
        &self,
        i: MeasIndex,
    ) -> impl Iterator<Item = (MeasHeader, String, Option<String>)> {
        [
            OptIndexedKey::triple(&self.wavelengths, i.into()),
            OptIndexedKey::triple(&self.calibration, i.into()),
            OptIndexedKey::triple(&self.display, i.into()),
            OptIndexedKey::triple(&self.detector_name, i.into()),
            OptIndexedKey::triple(&self.tag, i.into()),
            OptIndexedKey::triple(&self.measurement_type, i.into()),
            OptIndexedKey::triple(&self.feature, i.into()),
            OptIndexedKey::triple(&self.analyte, i.into()),
        ]
        .into_iter()
        .chain(self.scale.opt_suffixes(i))
    }

    fn can_convert_to_temporal(&self, i: MeasIndex) -> MultiResult<(), OpticalToTemporalError> {
        let j = i.into();
        let c =
            check_indexed_key_transfer::<_, AnyOpticalToTemporalKeyLossError>(&self.calibration, j);
        let w = check_indexed_key_transfer(&self.wavelengths, j);
        let m = check_indexed_key_transfer(&self.measurement_type, j);
        let a = check_indexed_key_transfer(&self.analyte, j);
        let t = check_indexed_key_transfer(&self.tag, j);
        let n = check_indexed_key_transfer(&self.detector_name, j);
        let f = check_indexed_key_transfer(&self.feature, j);
        let res = c
            .zip3(w, m)
            .mult_zip3(a.zip(t), n.zip(f))
            .mult_errors_into();
        let s = if !self.scale.is_noop() {
            Err(OpticalNonLinearError)
        } else {
            Ok(())
        };
        res.mult_zip(s.into_mult()).void()
    }
}

impl VersionedTemporal for InnerTemporal2_0 {
    type Ver = Version2_0;

    fn req_meta_keywords_inner(&self) -> impl Iterator<Item = (String, String)> {
        [].into_iter()
    }

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        self.peak
            .opt_keywords(i)
            .flat_map(|(_, k, v)| v.map(|x| (k, x)))
    }

    fn can_convert_to_optical(&self, _: MeasIndex) -> MultiResult<(), TemporalToOpticalError> {
        Ok(())
    }
}

impl VersionedTemporal for InnerTemporal3_0 {
    type Ver = Version3_0;

    fn req_meta_keywords_inner(&self) -> impl Iterator<Item = (String, String)> {
        [ReqMetarootKey::pair(&self.timestep)].into_iter()
    }

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        self.peak
            .opt_keywords(i)
            .flat_map(|(_, k, v)| v.map(|x| (k, x)))
    }

    fn can_convert_to_optical(&self, _: MeasIndex) -> MultiResult<(), TemporalToOpticalError> {
        Ok(())
    }
}

impl VersionedTemporal for InnerTemporal3_1 {
    type Ver = Version3_1;

    fn req_meta_keywords_inner(&self) -> impl Iterator<Item = (String, String)> {
        [ReqMetarootKey::pair(&self.timestep)].into_iter()
    }

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        self.peak
            .opt_keywords(i)
            .map(|(_, k, v)| (k, v))
            .chain([OptIndexedKey::pair_opt(&self.display, i.into())])
            .flat_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn can_convert_to_optical(&self, _: MeasIndex) -> MultiResult<(), TemporalToOpticalError> {
        Ok(())
    }
}

impl VersionedTemporal for InnerTemporal3_2 {
    type Ver = Version3_2;

    fn req_meta_keywords_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.timestep.pair()].into_iter()
    }

    fn opt_meas_keywords_inner(&self, i: MeasIndex) -> impl Iterator<Item = (String, String)> {
        [OptIndexedKey::pair_opt(&self.display, i.into())]
            .into_iter()
            .flat_map(|(k, v)| v.map(|x| (k, x)))
    }

    fn can_convert_to_optical(&self, i: MeasIndex) -> MultiResult<(), TemporalToOpticalError> {
        check_indexed_key_transfer(&self.measurement_type, i.into())
            .map_err(TemporalToOpticalError::Loss)
            .map_err(NonEmpty::new)
    }
}

impl VersionedTEXTOffsets for TEXTOffsets2_0 {
    type TotDef = MaybeTot;

    fn lookup(
        kws: &mut StdKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        _: &ReadState<StdTextReadConfig>,
    ) -> LookupTEXTOffsetsResult<Self> {
        Ok(Tot::remove_metaroot_opt(kws)
            .map_or_else(
                |w| Tentative::new(None, vec![w.into()], vec![]),
                |t| Tentative::new1(t.0),
            )
            .map(|tot| {
                TEXTOffsets {
                    data: data.into_any(),
                    analysis: analysis.into_any(),
                    tot,
                }
                .into()
            }))
    }

    fn lookup_ro(
        kws: &StdKeywords,
        data: HeaderDataSegment,
        analysis: HeaderAnalysisSegment,
        _: &ReadState<StdTextReadConfig>,
    ) -> LookupTEXTOffsetsResult<Self> {
        Ok(Tot::get_metaroot_opt(kws)
            .map_or_else(
                |w| Tentative::new(None, vec![w.into()], vec![]),
                |t| Tentative::new1(t.0),
            )
            .map(|tot| {
                TEXTOffsets {
                    data: data.into_any(),
                    analysis: analysis.into_any(),
                    tot,
                }
                .into()
            }))
    }

    fn data(&self) -> AnyDataSegment {
        self.0.data
    }

    fn analysis(&self) -> AnyAnalysisSegment {
        self.0.analysis
    }

    fn tot(&self) -> <Self::TotDef as TotDefinition>::Tot {
        self.0.tot
    }

    fn into_common(self) -> TEXTOffsets<Option<Tot>> {
        let x = self.0;
        TEXTOffsets {
            tot: x.tot,
            data: x.data,
            analysis: x.analysis,
        }
    }
}

impl VersionedTEXTOffsets for TEXTOffsets3_0 {
    type TotDef = KnownTot;

    fn lookup(
        kws: &mut StdKeywords,
        data_seg: HeaderDataSegment,
        analysis_seg: HeaderAnalysisSegment,
        st: &ReadState<StdTextReadConfig>,
    ) -> LookupTEXTOffsetsResult<Self> {
        let tot_res = Tot::remove_metaroot_req(kws).into_deferred();
        let data_res = KeyedReqSegment::remove_or(
            kws,
            st.conf.data,
            data_seg,
            st.conf.raw.ignore_text_data_offsets,
            st,
        )
        .def_inner_into();
        let analysis_res = KeyedReqSegment::remove_or(
            kws,
            st.conf.analysis,
            analysis_seg,
            st.conf.raw.ignore_text_analysis_offsets,
            st,
        )
        .def_inner_into();
        tot_res
            .def_zip3(data_res, analysis_res)
            .def_map_value(|(tot, data, analysis)| {
                TEXTOffsets {
                    data,
                    analysis,
                    tot,
                }
                .into()
            })
    }

    fn lookup_ro(
        kws: &StdKeywords,
        data_seg: HeaderDataSegment,
        analysis_seg: HeaderAnalysisSegment,
        st: &ReadState<StdTextReadConfig>,
    ) -> LookupTEXTOffsetsResult<Self> {
        let conf = &st.conf;
        let tot_res = Tot::get_metaroot_req(kws).into_deferred();
        let data_res = KeyedReqSegment::get_or(
            kws,
            conf.data,
            data_seg,
            conf.raw.ignore_text_data_offsets,
            st,
        )
        .def_inner_into();
        let analysis_res = KeyedReqSegment::get_or(
            kws,
            conf.analysis,
            analysis_seg,
            conf.raw.ignore_text_analysis_offsets,
            st,
        )
        .def_inner_into();
        tot_res
            .def_zip3(data_res, analysis_res)
            .def_map_value(|(tot, data, analysis)| {
                TEXTOffsets {
                    data,
                    analysis,
                    tot,
                }
                .into()
            })
    }

    fn data(&self) -> AnyDataSegment {
        self.0.data
    }

    fn analysis(&self) -> AnyAnalysisSegment {
        self.0.analysis
    }

    fn tot(&self) -> <Self::TotDef as TotDefinition>::Tot {
        self.0.tot
    }

    fn into_common(self) -> TEXTOffsets<Option<Tot>> {
        let x = self.0;
        TEXTOffsets {
            tot: Some(x.tot),
            data: x.data,
            analysis: x.analysis,
        }
    }
}

impl VersionedTEXTOffsets for TEXTOffsets3_2 {
    type TotDef = KnownTot;

    fn lookup(
        kws: &mut StdKeywords,
        data_seg: HeaderDataSegment,
        analysis_seg: HeaderAnalysisSegment,
        st: &ReadState<StdTextReadConfig>,
    ) -> LookupTEXTOffsetsResult<Self> {
        let conf = &st.conf;
        let tot_res = Tot::remove_metaroot_req(kws).into_deferred();
        let data_res = KeyedReqSegment::remove_or(
            kws,
            conf.data,
            data_seg,
            conf.raw.ignore_text_data_offsets,
            st,
        )
        .def_inner_into();
        tot_res
            .def_zip(data_res)
            .def_and_tentatively(|(tot, data)| {
                {
                    KeyedOptSegment::remove_or(
                        kws,
                        conf.analysis,
                        analysis_seg,
                        conf.raw.ignore_text_analysis_offsets,
                        st,
                    )
                    .inner_into()
                    .map(|analysis| {
                        TEXTOffsets {
                            data,
                            analysis,
                            tot,
                        }
                        .into()
                    })
                }
            })
    }

    fn lookup_ro(
        kws: &StdKeywords,
        data_seg: HeaderDataSegment,
        analysis_seg: HeaderAnalysisSegment,
        st: &ReadState<StdTextReadConfig>,
    ) -> LookupTEXTOffsetsResult<Self> {
        let conf = &st.conf;
        let tot_res = Tot::get_metaroot_req(kws).into_deferred();
        let data_res = KeyedReqSegment::get_or(
            kws,
            conf.data,
            data_seg,
            conf.raw.ignore_text_data_offsets,
            st,
        )
        .def_inner_into();
        tot_res
            .def_zip(data_res)
            .def_and_tentatively(|(tot, data)| {
                KeyedOptSegment::get_or(
                    kws,
                    conf.analysis,
                    analysis_seg,
                    conf.raw.ignore_text_analysis_offsets,
                    st,
                )
                .inner_into()
                .map(|analysis| {
                    TEXTOffsets {
                        data,
                        analysis,
                        tot,
                    }
                    .into()
                })
            })
    }

    fn data(&self) -> AnyDataSegment {
        self.0.data
    }

    fn analysis(&self) -> AnyAnalysisSegment {
        self.0.analysis
    }

    fn tot(&self) -> <Self::TotDef as TotDefinition>::Tot {
        self.0.tot
    }

    fn into_common(self) -> TEXTOffsets<Option<Tot>> {
        let x = self.0;
        TEXTOffsets {
            tot: Some(x.tot),
            data: x.data,
            analysis: x.analysis,
        }
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
            scale: ScaleTransform::default(),
            peak: t.peak,
            wavelength: None.into(),
        };
        (new, t.timestep)
    }
}

impl OpticalFromTemporal<InnerTemporal3_1> for InnerOptical3_1 {
    type TData = Timestep;

    fn from_temporal_inner(t: InnerTemporal3_1) -> (Self, Self::TData) {
        let new = Self {
            scale: ScaleTransform::default(),
            peak: t.peak,
            wavelengths: None.into(),
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
            scale: ScaleTransform::default(),
            wavelengths: None.into(),
            calibration: None.into(),
            display: t.display,
            analyte: None.into(),
            feature: None.into(),
            measurement_type: None.into(),
            tag: None.into(),
            detector_name: None.into(),
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
        }
    }
}

type Timestamps2_0 = Timestamps<FCSTime>;
type Timestamps3_0 = Timestamps<FCSTime60>;
type Timestamps3_1 = Timestamps<FCSTime100>;

impl LookupMetaroot for InnerMetaroot2_0 {
    fn lookup_shortname(
        kws: &mut StdKeywords,
        i: MeasIndex,
    ) -> LookupResult<<Self::Name as MightHave>::Wrapper<Shortname>> {
        Ok(Shortname::lookup_opt(kws, i.into()))
    }

    fn lookup_specific(
        kws: &mut StdKeywords,
        par: Par,
        _: &HashSet<&Shortname>,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let co = Compensation2_0::lookup(kws, par);
        let cy = Cyt::lookup_opt(kws);
        let t = Timestamps::lookup(kws);
        let g = AppliedGates2_0::lookup(kws, conf);
        co.zip4(cy, t, g)
            .errors_into()
            .and_maybe(|(comp, cyt, timestamps, applied_gates)| {
                Mode::lookup_req(kws).def_map_value(|mode| Self {
                    mode,
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
    ) -> LookupResult<<Self::Name as MightHave>::Wrapper<Shortname>> {
        Ok(Shortname::lookup_opt(kws, i.into()))
    }

    fn lookup_specific(
        kws: &mut StdKeywords,
        _: Par,
        _: &HashSet<&Shortname>,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let co = Compensation3_0::lookup_opt(kws);
        let cy = Cyt::lookup_opt(kws);
        let sn = Cytsn::lookup_opt(kws);
        let su = SubsetData::lookup(kws);
        let t = Timestamps::lookup(kws);
        let u = Unicode::lookup_opt(kws);
        let g = AppliedGates3_0::lookup(kws, conf);
        co.zip4(cy, sn, su).zip4(t, u, g).and_maybe(
            |((comp, cyt, cytsn, subset), timestamps, unicode, applied_gates)| {
                Mode::lookup_req(kws).def_map_value(|mode| Self {
                    mode,
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
    ) -> LookupResult<<Self::Name as MightHave>::Wrapper<Shortname>> {
        Shortname::lookup_req(kws, i.into()).map(|x| x.map(AlwaysValue))
    }

    fn lookup_specific(
        kws: &mut StdKeywords,
        _: Par,
        names: &HashSet<&Shortname>,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let cy = Cyt::lookup_opt(kws);
        let sp = Spillover::lookup_opt(kws, names);
        let sn = Cytsn::lookup_opt(kws);
        let su = SubsetData::lookup(kws);
        let md = ModificationData::lookup(kws);
        let p = PlateData::lookup(kws);
        let t = Timestamps::lookup(kws);
        let v = Vol::lookup_opt(kws);
        let g = AppliedGates3_0::lookup_dep(kws, conf).errors_into();
        cy.zip5(sp, sn, su, md).zip5(p, t, v, g).and_maybe(
            |(
                (cyt, spillover, cytsn, subset, modification),
                plate,
                timestamps,
                vol,
                applied_gates,
            )| {
                let mut mo = Mode::lookup_req(kws);
                mo.def_eval_warning(|mode| match mode {
                    Mode::Correlated => {
                        Some(DeprecatedError::Value(DepValueWarning::ModeCorrelated).into())
                    }
                    Mode::Uncorrelated => {
                        Some(DeprecatedError::Value(DepValueWarning::ModeUncorrelated).into())
                    }
                    Mode::List => None,
                });
                mo.def_map_value(|mode| Self {
                    mode,
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
    ) -> LookupResult<<Self::Name as MightHave>::Wrapper<Shortname>> {
        Shortname::lookup_req(kws, i.into()).map(|x| x.map(AlwaysValue))
    }

    fn lookup_specific(
        kws: &mut StdKeywords,
        _: Par,
        names: &HashSet<&Shortname>,
        conf: &StdTextReadConfig,
    ) -> LookupResult<Self> {
        let dd = conf.disallow_deprecated;
        let ca = CarrierData::lookup(kws);
        let d = Datetimes::lookup(kws);
        let f = Flowrate::lookup_opt(kws);
        let md = ModificationData::lookup(kws);
        // Only L is allowed as of 3.2, so pull the value and check it if given.
        // The only thing we care about is that the value is valid, since we
        // don't need to use it anywhere.
        let mo = Mode3_2::lookup_opt_dep(kws, dd);
        let sp = Spillover::lookup_opt(kws, names);
        let sn = Cytsn::lookup_opt(kws);
        let p = PlateData::lookup_dep(kws, dd);
        let t = Timestamps::lookup_dep(kws, dd);
        let u = UnstainedData::lookup(kws, names);
        let v = Vol::lookup_opt(kws);
        let g = AppliedGates3_2::lookup(kws, dd);
        ca.zip6(d, f, md, mo, sp)
            .zip6(sn, p, t, u, v)
            .zip(g)
            .errors_into()
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
                    Cyt::lookup_req(kws).def_map_value(|cyt| Self {
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
    type Ver = Version2_0;
    type Optical = InnerOptical2_0;
    type Temporal = InnerTemporal2_0;
    type Name = MaybeFamily;

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
        self.comp.as_ref_opt().map(|x| x.as_ref())
    }

    fn with_compensation<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut Compensation) -> Result<X, ClearOptional>,
    {
        self.comp.mut_or_unset_nofail(|c| f(&mut c.0))
    }

    fn timestamps_valid(&self) -> bool {
        self.timestamps.valid()
    }

    fn datetimes_valid(&self) -> bool {
        true
    }

    fn keywords_req_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.mode.pair()].into_iter()
    }

    fn keywords_opt_inner(&self) -> impl Iterator<Item = (String, String)> {
        [OptMetarootKey::pair_opt(&self.cyt)]
            .into_iter()
            .flat_map(|(k, v)| v.map(|x| (k, x)))
            .chain(
                self.applied_gates
                    .as_ref_opt()
                    .map(|x| x.opt_keywords())
                    .into_iter()
                    .flatten(),
            )
            .chain(self.timestamps.opt_keywords())
            .chain(self.comp.as_ref_opt().map_or(vec![], |c| c.opt_keywords()))
    }

    fn swap_optical_temporal_inner(
        old_t: Self::Temporal,
        old_o: Self::Optical,
    ) -> (Self::Optical, Self::Temporal) {
        let new_t = Self::Temporal {
            peak: old_o.peak,
            scale: old_o.scale.map(|_| TemporalScale),
        };
        let new_o = Self::Optical {
            peak: old_t.peak,
            scale: old_t.scale.map(|_| Scale::Linear),
            wavelength: None.into(),
        };
        (new_o, new_t)
    }
}

impl VersionedMetaroot for InnerMetaroot3_0 {
    type Ver = Version3_0;
    type Optical = InnerOptical3_0;
    type Temporal = InnerTemporal3_0;
    type Name = MaybeFamily;

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
        self.comp.as_ref_opt().map(|x| x.as_ref())
    }

    fn with_compensation<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut Compensation) -> Result<X, ClearOptional>,
    {
        self.comp.mut_or_unset_nofail(|c| f(&mut c.0))
    }

    fn timestamps_valid(&self) -> bool {
        self.timestamps.valid()
    }

    fn datetimes_valid(&self) -> bool {
        true
    }

    fn keywords_req_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.mode.pair()].into_iter()
    }

    fn keywords_opt_inner(&self) -> impl Iterator<Item = (String, String)> {
        [
            OptMetarootKey::pair_opt(&self.cyt),
            OptMetarootKey::pair_opt(&self.comp),
            OptMetarootKey::pair_opt(&self.cytsn),
            OptMetarootKey::pair_opt(&self.unicode),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .chain(
            self.applied_gates
                .as_ref_opt()
                .map(|x| x.opt_keywords())
                .into_iter()
                .flatten(),
        )
        .chain(
            self.subset
                .as_ref_opt()
                .map(|x| x.opt_keywords())
                .into_iter()
                .flatten(),
        )
        .chain(self.timestamps.opt_keywords())
    }

    fn swap_optical_temporal_inner(
        old_t: Self::Temporal,
        old_o: Self::Optical,
    ) -> (Self::Optical, Self::Temporal) {
        let new_t = Self::Temporal {
            peak: old_o.peak,
            timestep: old_t.timestep,
        };
        let new_o = Self::Optical {
            scale: ScaleTransform::default(),
            wavelength: None.into(),
            peak: old_t.peak,
        };
        (new_o, new_t)
    }
}

impl VersionedMetaroot for InnerMetaroot3_1 {
    type Ver = Version3_1;
    type Optical = InnerOptical3_1;
    type Temporal = InnerTemporal3_1;
    type Name = AlwaysFamily;

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
        self.spillover.mut_or_unset_nofail(f)
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

    fn keywords_req_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.mode.pair()].into_iter()
    }

    fn keywords_opt_inner(&self) -> impl Iterator<Item = (String, String)> {
        [
            OptMetarootKey::pair_opt(&self.cyt),
            OptLinkedKey::pair_opt(&self.spillover),
            OptMetarootKey::pair_opt(&self.cytsn),
            OptMetarootKey::pair_opt(&self.vol),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .chain(
            self.applied_gates
                .as_ref_opt()
                .map(|x| x.opt_keywords())
                .into_iter()
                .flatten(),
        )
        .chain(
            self.subset
                .as_ref_opt()
                .map(|x| x.opt_keywords())
                .into_iter()
                .flatten(),
        )
        .chain(self.modification.opt_keywords())
        .chain(self.plate.opt_keywords())
        .chain(self.timestamps.opt_keywords())
    }

    fn swap_optical_temporal_inner(
        old_t: Self::Temporal,
        old_o: Self::Optical,
    ) -> (Self::Optical, Self::Temporal) {
        let new_t = Self::Temporal {
            peak: old_o.peak,
            display: old_o.display,
            timestep: old_t.timestep,
        };
        let new_o = Self::Optical {
            scale: ScaleTransform::default(),
            wavelengths: None.into(),
            display: old_t.display,
            peak: old_t.peak,
            calibration: None.into(),
        };
        (new_o, new_t)
    }
}

impl VersionedMetaroot for InnerMetaroot3_2 {
    type Ver = Version3_2;
    type Optical = InnerOptical3_2;
    type Temporal = InnerTemporal3_2;
    type Name = AlwaysFamily;

    fn as_unstainedcenters(&self) -> Option<&UnstainedCenters> {
        self.unstained.unstainedcenters.as_ref_opt()
    }

    fn with_unstainedcenters<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut UnstainedCenters) -> Result<X, ClearOptional>,
    {
        self.unstained.unstainedcenters.mut_or_unset_nofail(f)
    }

    fn as_spillover(&self) -> Option<&Spillover> {
        self.spillover.as_ref_opt()
    }

    fn with_spillover<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut Spillover) -> Result<X, ClearOptional>,
    {
        self.spillover.mut_or_unset_nofail(f)
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

    fn keywords_req_inner(&self) -> impl Iterator<Item = (String, String)> {
        [self.cyt.pair()].into_iter()
    }

    fn keywords_opt_inner(&self) -> impl Iterator<Item = (String, String)> {
        [
            OptLinkedKey::pair_opt(&self.spillover),
            OptMetarootKey::pair_opt(&self.cytsn),
            OptMetarootKey::pair_opt(&self.vol),
            OptMetarootKey::pair_opt(&self.flowrate),
        ]
        .into_iter()
        .flat_map(|(k, v)| v.map(|x| (k, x)))
        .chain(
            self.applied_gates
                .as_ref_opt()
                .map(|x| x.opt_keywords())
                .into_iter()
                .flatten(),
        )
        .chain(self.unstained.opt_keywords())
        .chain(self.modification.opt_keywords())
        .chain(self.carrier.opt_keywords())
        .chain(self.plate.opt_keywords())
        .chain(self.timestamps.opt_keywords())
        .chain(self.datetimes.opt_keywords())
    }

    fn swap_optical_temporal_inner(
        old_t: Self::Temporal,
        old_o: Self::Optical,
    ) -> (Self::Optical, Self::Temporal) {
        let new_t = Self::Temporal {
            display: old_o.display,
            timestep: old_t.timestep,
            measurement_type: None.into(),
        };
        let new_o = Self::Optical {
            scale: ScaleTransform::default(),
            display: old_t.display,
            wavelengths: None.into(),
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
            display: None.into(),
            measurement_type: None.into(),
        }
    }
}

impl InnerOptical3_0 {
    pub(crate) fn new(scale: Scale) -> Self {
        Self {
            scale: scale.into(),
            wavelength: None.into(),
            peak: PeakData::default(),
        }
    }
}

impl InnerOptical3_1 {
    pub(crate) fn new(scale: Scale) -> Self {
        Self {
            scale: scale.into(),
            calibration: None.into(),
            display: None.into(),
            wavelengths: None.into(),
            peak: PeakData::default(),
        }
    }
}

impl InnerOptical3_2 {
    pub(crate) fn new(scale: Scale) -> Self {
        Self {
            scale: scale.into(),
            analyte: None.into(),
            calibration: None.into(),
            detector_name: None.into(),
            display: None.into(),
            feature: None.into(),
            measurement_type: None.into(),
            tag: None.into(),
            wavelengths: None.into(),
        }
    }
}

impl InnerMetaroot2_0 {
    pub(crate) fn new(mode: Mode) -> Self {
        Self {
            mode,
            cyt: None.into(),
            timestamps: Timestamps::default(),
            comp: None.into(),
            applied_gates: None.into(),
        }
    }
}

impl InnerMetaroot3_0 {
    pub(crate) fn new(mode: Mode) -> Self {
        Self {
            mode,
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
    pub(crate) fn new(mode: Mode) -> Self {
        Self {
            mode,
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
    pub(crate) fn new(cyt: String) -> Self {
        Self {
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

impl Default for Temporal2_0 {
    fn default() -> Self {
        let specific = InnerTemporal2_0::default();
        Self::new_common(specific)
    }
}

impl Temporal3_0 {
    pub fn new(timestep: Timestep) -> Self {
        let specific = InnerTemporal3_0::new(timestep);
        Self::new_common(specific)
    }
}

impl Temporal3_1 {
    pub fn new(timestep: Timestep) -> Self {
        let specific = InnerTemporal3_1::new(timestep);
        Self::new_common(specific)
    }
}

impl Temporal3_2 {
    pub fn new(timestep: Timestep) -> Self {
        let specific = InnerTemporal3_2::new(timestep);
        Self::new_common(specific)
    }
}

impl Default for Optical2_0 {
    fn default() -> Self {
        let specific = InnerOptical2_0::default();
        Self::new_common(specific)
    }
}

impl Optical3_0 {
    pub fn new(scale: Scale) -> Self {
        let specific = InnerOptical3_0::new(scale);
        Self::new_common(specific)
    }
}

impl Optical3_1 {
    pub fn new(scale: Scale) -> Self {
        let specific = InnerOptical3_1::new(scale);
        Self::new_common(specific)
    }
}

impl Optical3_2 {
    pub fn new(scale: Scale) -> Self {
        let specific = InnerOptical3_2::new(scale);
        Self::new_common(specific)
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

impl AnalysisReader {
    pub(crate) fn h_read<R: Read + Seek>(&self, h: &mut BufReader<R>) -> io::Result<Analysis> {
        let mut buf = vec![];
        self.seg.inner.h_read_contents(h, &mut buf)?;
        Ok(buf.into())
    }
}

impl OthersReader<'_> {
    pub(crate) fn h_read<R: Read + Seek>(&self, h: &mut BufReader<R>) -> io::Result<Others> {
        let mut buf = vec![];
        let mut others = vec![];
        for s in self.segs.iter() {
            s.inner.h_read_contents(h, &mut buf)?;
            others.push(Other(buf.clone()));
            buf.clear();
        }
        Ok(Others(others))
    }
}

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
    Layout(LayoutConvertError),
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
            Self::Layout(e) => e.fmt(f),
        }
    }
}

pub struct BlankShortnames;

impl fmt::Display for BlankShortnames {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "Some $PnN are blank and could not be converted",)
    }
}

#[derive(From, Display)]
pub enum StdReaderError {
    Layout(NewDataLayoutError),
    Reader(NewDataReaderError),
}

pub struct TerminalDataLayoutFailure;

#[derive(From, Display)]
pub enum StdWriterError {
    Layout(NewDataLayoutError),
    Check(ColumnError<AnyLossError>),
    Overflow(Uint8DigitOverflow),
}

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

#[derive(From, Display)]
pub enum SetSpilloverError {
    New(SpilloverError),
    Link(SpilloverLinkError),
}

pub struct SpilloverLinkError;

impl fmt::Display for SpilloverLinkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "all $SPILLOVER names must match a $PnN")
    }
}

#[derive(From, Display)]
pub enum SetMeasurementsError {
    New(NewNamedVecError),
    Link(ExistingLinkError),
    Layout(MeasLayoutMismatchError),
    Empty(EmptyLayoutError),
}

#[derive(From, Display)]
pub enum SetMeasurementsAndDataError {
    Meas(SetMeasurementsError),
    New(NewDataframeError),
    Mismatch(MeasDataMismatchError),
}

#[derive(From, Display)]
pub enum ColumsnToDataframeError {
    New(NewDataframeError),
    Mismatch(MeasDataMismatchError),
}

#[derive(From, Display)]
pub enum SetMeasurementsOnlyError {
    Meas(SetMeasurementsError),
    Mismatch(MeasDataMismatchError),
}

#[derive(From, Display)]
pub enum InsertTemporalError {
    Center(InsertCenterError),
    Layout(AnyRangeError),
}

#[derive(From, Display)]
pub enum PushOpticalError {
    Unique(NonUniqueKeyError),
    Layout(AnyRangeError),
}

#[derive(From, Display)]
pub enum InsertOpticalError {
    Insert(InsertError),
    Layout(AnyRangeError),
}

#[derive(From, Display)]
pub enum PushTemporalToDatasetError {
    Measurement(InsertTemporalError),
    Column(ColumnLengthError),
}

#[derive(From, Display)]
pub enum InsertTemporalToDatasetError {
    Measurement(InsertTemporalError),
    Column(ColumnLengthError),
}

#[derive(From, Display)]
pub enum PushOpticalToDatasetError {
    Measurement(PushOpticalError),
    Column(ColumnLengthError),
}

#[derive(From, Display)]
pub enum InsertOpticalInDatasetError {
    Measurement(InsertOpticalError),
    Column(ColumnLengthError),
}

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

#[derive(From, Display)]
pub enum StdTEXTFromRawError {
    Metaroot(LookupKeysError),
    Layout(Box<LookupLayoutError>),
    Offsets(LookupTEXTOffsetsError),
    Pseudostandard(PseudostandardError),
}

#[derive(From, Display)]
pub enum StdTEXTFromRawWarning {
    Metaroot(LookupKeysWarning),
    Meas(LookupMeasWarning),
    Layout(LookupLayoutWarning),
    Offsets(LookupTEXTOffsetsWarning),
    Pseudostandard(PseudostandardError),
}

#[derive(From, Display)]
pub enum StdDatasetFromRawError {
    TEXT(Box<StdTEXTFromRawError>),
    Dataframe(ReadDataframeError),
    Offsets(LookupTEXTOffsetsError),
    Warn(StdDatasetFromRawWarning),
}

#[derive(From, Display)]
pub enum StdDatasetFromRawWarning {
    TEXT(StdTEXTFromRawWarning),
    Offsets(LookupTEXTOffsetsWarning),
    Layout(ReadDataframeWarning),
}

#[derive(From, Display)]
pub enum LookupMeasWarning {
    Parse(LookupKeysWarning),
    Pattern(NonStdMeasRegexError),
}

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

#[derive(From, Display)]
pub enum ReplaceTemporalError {
    ToOptical(TemporalToOpticalError),
    Set(SetCenterError),
}

#[derive(From, Display)]
pub enum SetTemporalError {
    Swap(SwapOpticalTemporalError),
    Set(SetCenterError),
}

#[derive(From, Display)]
pub enum SwapOpticalTemporalError {
    ToTemporal(OpticalToTemporalError),
    ToOptical(TemporalToOpticalError),
}

#[derive(From, Display)]
pub enum OpticalToTemporalError {
    NonLinear(OpticalNonLinearError),
    Loss(AnyOpticalToTemporalKeyLossError),
}

#[derive(From, Display)]
pub enum TemporalToOpticalError {
    Loss(AnyTemporalToOpticalKeyLossError),
}

#[derive(From, Display)]
pub enum SetTemporalIndexError {
    Convert(OpticalToTemporalError),
    Index(SetCenterError),
}

pub struct OpticalNonLinearError;

impl fmt::Display for OpticalNonLinearError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "$PnE must be '0,0' and $PnG must be null or unity to convert to temporal"
        )
    }
}

#[derive(From, Display)]
pub enum MetarootConvertError {
    NoCyt(NoCytError),
    Mode(ModeNotListError),
    GateLink(RegionToGateIndexError),
    MeasLink(RegionToMeasIndexError),
    GateToMeas(GateToMeasIndexError),
    MeasToGate(MeasToGateIndexError),
    Gates3_0To2_0(AppliedGates3_0To2_0Error),
    Gates3_0To3_2(AppliedGates3_0To3_2Error),
    Gates3_2To2_0(AppliedGates3_2To2_0Error),
    Gates2_0To3_2(AppliedGates2_0To3_2Error),
    Loss(AnyMetarootKeyLossError),
    Comp2_0(Comp2_0TransferError),
}

#[derive(From, Display)]
pub enum MetarootConvertWarning {
    Mode(ModeNotListError),
    Gates3_0To2_0(AppliedGates3_0To2_0Error),
    Gates3_0To3_2(AppliedGates3_0To3_2Error),
    Gates3_2To2_0(AppliedGates3_2To2_0Error),
    Gates2_0To3_2(AppliedGates2_0To3_2Error),
    Loss(AnyMetarootKeyLossError),
    Optical(OpticalConvertWarning),
    Temporal(TemporalConvertError),
    Comp2_0(Comp2_0TransferError),
}

/// Error when a metaroot keyword will be lost when converting versions
#[derive(From, Display)]
pub enum AnyMetarootKeyLossError {
    Cytsn(UnitaryKeyLossError<Cytsn>),
    Unicode(UnitaryKeyLossError<Unicode>),
    Vol(UnitaryKeyLossError<Vol>),
    Flowrate(UnitaryKeyLossError<Flowrate>),
    Comp(UnitaryKeyLossError<Compensation3_0>),
    Platename(UnitaryKeyLossError<Platename>),
    Plateid(UnitaryKeyLossError<Plateid>),
    Wellid(UnitaryKeyLossError<Wellid>),
    Carrierid(UnitaryKeyLossError<Carrierid>),
    Locationid(UnitaryKeyLossError<Locationid>),
    Carriertype(UnitaryKeyLossError<Carriertype>),
    LastModifier(UnitaryKeyLossError<LastModifier>),
    LastModified(UnitaryKeyLossError<ModifiedDateTime>),
    Originality(UnitaryKeyLossError<Originality>),
    UnstainedCenters(UnitaryKeyLossError<UnstainedCenters>),
    UnstainedInfo(UnitaryKeyLossError<UnstainedInfo>),
    Begindatetime(UnitaryKeyLossError<BeginDateTime>),
    Enddatetime(UnitaryKeyLossError<EndDateTime>),
    Spillover(UnitaryKeyLossError<Spillover>),
    CSMode(UnitaryKeyLossError<CSMode>),
    CSVBits(UnitaryKeyLossError<CSVBits>),
    CSVFlag(IndexedKeyLossError<CSVFlag>),
}

/// Error when an optical keyword will be lost when converting versions
#[derive(From, Display)]
pub enum AnyMeasKeyLossError {
    Filter(IndexedKeyLossError<Filter>),
    Power(IndexedKeyLossError<Power>),
    DetectorType(IndexedKeyLossError<DetectorType>),
    PercentEmitted(IndexedKeyLossError<PercentEmitted>),
    DetectorVoltage(IndexedKeyLossError<DetectorVoltage>),
    Wavelength(IndexedKeyLossError<Wavelength>),
    Wavelengths(IndexedKeyLossError<Wavelengths>),
    MeasType(IndexedKeyLossError<OpticalType>),
    TempType(IndexedKeyLossError<TemporalType>),
    Analyte(IndexedKeyLossError<Analyte>),
    Tag(IndexedKeyLossError<Tag>),
    Gain(IndexedKeyLossError<Gain>),
    Display(IndexedKeyLossError<Display>),
    DetectorName(IndexedKeyLossError<DetectorName>),
    Feature(IndexedKeyLossError<Feature>),
    PeakBin(IndexedKeyLossError<PeakBin>),
    PeakNumber(IndexedKeyLossError<PeakNumber>),
    Calibration3_1(IndexedKeyLossError<Calibration3_1>),
    Calibration3_2(IndexedKeyLossError<Calibration3_2>),
}

/// Error when an optical keyword will be lost when converting to temporal
#[derive(From, Display)]
pub enum AnyOpticalToTemporalKeyLossError {
    Filter(IndexedKeyLossError<Filter>),
    Power(IndexedKeyLossError<Power>),
    DetectorType(IndexedKeyLossError<DetectorType>),
    PercentEmitted(IndexedKeyLossError<PercentEmitted>),
    DetectorVoltage(IndexedKeyLossError<DetectorVoltage>),
    Wavelength(IndexedKeyLossError<Wavelength>),
    Wavelengths(IndexedKeyLossError<Wavelengths>),
    MeasType(IndexedKeyLossError<OpticalType>),
    Analyte(IndexedKeyLossError<Analyte>),
    Tag(IndexedKeyLossError<Tag>),
    Gain(IndexedKeyLossError<Gain>),
    DetectorName(IndexedKeyLossError<DetectorName>),
    Feature(IndexedKeyLossError<Feature>),
    Calibration3_1(IndexedKeyLossError<Calibration3_1>),
    Calibration3_2(IndexedKeyLossError<Calibration3_2>),
}

/// Error when a temporal keyword will be lost when converting to optical
#[derive(From, Display)]
pub enum AnyTemporalToOpticalKeyLossError {
    TempType(IndexedKeyLossError<TemporalType>),
}

#[derive(From, Display)]
pub enum LookupAndReadDataAnalysisError {
    Offsets(LookupTEXTOffsetsError),
    Layout(RawToLayoutError),
    Dataframe(ReadDataframeError),
    Warn(LookupAndReadDataAnalysisWarning),
}

#[derive(From, Display)]
pub enum LookupAndReadDataAnalysisWarning {
    Offsets(LookupTEXTOffsetsWarning),
    Layout(RawToLayoutWarning),
    Data(ReadDataframeWarning),
}

#[derive(From, Display)]
pub enum LookupTEXTOffsetsWarning {
    Tot(ParseKeyError<std::num::ParseIntError>),
    ReqData(ReqSegmentWithDefaultWarning<DataSegmentId>),
    ReqAnalysis(ReqSegmentWithDefaultWarning<AnalysisSegmentId>),
    MismatchAnalysis(OptSegmentWithDefaultWarning<AnalysisSegmentId>),
}

#[derive(From, Display)]
pub enum LookupTEXTOffsetsError {
    Tot(ReqKeyError<std::num::ParseIntError>),
    ReqData(ReqSegmentWithDefaultError<DataSegmentId>),
    ReqAnalysis(ReqSegmentWithDefaultError<AnalysisSegmentId>),
    MismatchData(SegmentMismatchWarning<DataSegmentId>),
    MismatchAnalysis(SegmentMismatchWarning<AnalysisSegmentId>),
}

type LookupTEXTOffsetsResult<T> =
    DeferredResult<T, LookupTEXTOffsetsWarning, LookupTEXTOffsetsError>;

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
    NoRegions,
}

impl fmt::Display for AppliedGates3_0To2_0Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Index(x) => x.fmt(f),
            Self::NoGates => write!(f, "no $Gn* keywords present"),
            Self::NoRegions => write!(f, "no valid $Rn* keywords present"),
        }
    }
}

pub enum AppliedGates3_0To3_2Error {
    Index(RegionToMeasIndexError),
    HasGates(usize),
    NoRegions,
}

impl fmt::Display for AppliedGates3_0To3_2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::Index(x) => x.fmt(f),
            Self::HasGates(n) => write!(f, "$GATING references {n} $Gn* keywords"),
            Self::NoRegions => write!(f, "no valid $Rn* keywords present"),
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

#[derive(Debug)]
pub struct ColumnNumberError {
    this_len: usize,
    other_len: usize,
}

impl fmt::Display for ColumnNumberError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "number of columns is {}, input should match but got {}",
            self.this_len, self.other_len,
        )
    }
}

pub struct ScaleTransformError {
    scale: Scale,
    gain: Gain,
}

impl fmt::Display for ScaleTransformError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "could not make scale transform with log scale '{}' and non-unit gain '{}'",
            self.scale, self.gain
        )
    }
}

pub struct EmptyLayoutError;

impl fmt::Display for EmptyLayoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "tried to set measurements with an empty data layout",)
    }
}
