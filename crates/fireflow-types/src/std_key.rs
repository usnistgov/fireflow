use crate::{
    config::OpticalOnlyKey,
    index::{GateIndex, IndexFromOne, MeasIndex, RegionIndex},
    keystring::{CowKeyString, KeyString},
    ne_str,
    nonempty_string::{
        DisplayNE as _, DisplayableNE as _, NEAlt, NEConcat, NEConcat3, NEConcat4, NESliceExt as _,
        NEStr, ToDisplayNE, ToNE,
    },
};

use derive_more::{Display, From};
use derive_new::new;
use nonempty_collections::NESlice;
use thiserror::Error;

use std::{borrow::Cow, str::FromStr};

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display)]
pub enum AnyStdKey {
    Real(StdKey),
    Pseudo(PseudoStdKey),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display)]
#[display("${_0}")]
pub struct PseudoStdKey(pub KeyString);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
#[display("${}", self.as_displayable())]
pub enum StdKey {
    Root(RootKey),
    Meas(MeasKey),
    Peak(PeakKey),
    Gate(GateKey),
    Region(RegionKey),
    CsvFlag(CsvFlag),
    Dfc(DfcKey),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RootKey {
    Byteord,
    Datatype,
    Mode,
    Par,
    Tot,
    Cyt,
    Abrt,
    Cells,
    Com,
    Exp,
    Fil,
    Inst,
    Lost,
    Op,
    Proj,
    Smno,
    Src,
    Sys,
    Tr,
    Cytsn,
    Timestep,
    Vol,
    Unicode,
    Flowrate,
    // Offset keywords
    Begindata,
    Beginanalysis,
    Beginstext,
    Enddata,
    Endanalysis,
    Endstext,
    Nextdata,
    // Time keywords
    Btim,
    Etim,
    Date,
    Begindatetime,
    Enddatetime,
    Comp,
    Spillover,
    // modified keywords
    LastModified,
    LastModifier,
    Originality,
    // plate keywords
    Plateid,
    Platename,
    Wellid,
    // unstained kewords
    UnstainedCenters,
    UnstainedInfo,
    // carrier keywords
    CarrierId,
    CarrierType,
    LocationId,
    // Subset keywords
    Csmode,
    Csvbits,
    Cstot,
    // Gating keywords
    Gating,
    Gate,
}

#[derive(new, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IndexedKey<I, K> {
    pub index: I,
    pub id: K,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DfcKey {
    pub index0: MeasIndex,
    pub index1: MeasIndex,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CsvFlag {
    pub index: IndexFromOne,
}

pub type MeasKey = IndexedKey<MeasIndex, MeasKeySuffix>;
pub type PeakKey = IndexedKey<MeasIndex, PeakKeyPrefix>;
pub type GateKey = IndexedKey<GateIndex, GateKeySuffix>;
pub type RegionKey = IndexedKey<RegionIndex, RegionKeySuffix>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, From)]
pub enum MeasKeySuffix {
    #[from(ParamKeySuffix)]
    Param(ParamKeySuffix),
    B,
    L,
    O,
    G,
    D,
    Det,
    Tag,
    Type,
    Feature,
    Analyte,
    Datatype,
    Calibration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PeakKeyPrefix {
    Pk,
    Pkn,
}

pub type GateKeySuffix = ParamKeySuffix;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ParamKeySuffix {
    N,
    R,
    E,
    S,
    F,
    T,
    P,
    V,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RegionKeySuffix {
    I,
    W,
}

/// Error when parsing [`StdKey`] from string
#[derive(PartialEq, Debug, Error, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub enum StdKeyError {
    #[error("key is not printable ASCII, got {0}")]
    NonAscii(String),
    #[error("key is not standard, got {0}")]
    Pseudo(PseudoStdKey),
    #[error("key was just a '$' character")]
    Dollar,
    #[error("prefix must be '$', got {0}")]
    Prefix(char),
    #[error("standard key must not be empty")]
    Empty,
}

impl FromStr for StdKey {
    type Err = StdKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !is_printable_ascii(s.as_bytes()) {
            Err(StdKeyError::NonAscii(s.into()))
        } else if let Some((b0, bs)) = s.as_bytes().split_first() {
            if *b0 != STD_PREFIX {
                Err(StdKeyError::Prefix((*b0).into()))
            } else if let Some(ne) = NESlice::try_from_slice(bs) {
                // SAFETY: we checked that bytes are ASCII above
                let k = unsafe { AnyStdKey::from_ascii_bytes(&ne) };
                match k {
                    AnyStdKey::Pseudo(x) => Err(StdKeyError::Pseudo(x)),
                    AnyStdKey::Real(x) => Ok(x),
                }
            } else {
                Err(StdKeyError::Dollar)
            }
        } else {
            Err(StdKeyError::Empty)
        }
    }
}

impl StdKey {
    #[must_use]
    pub fn as_keystring(&self) -> KeyString {
        self.as_cow_keystring().into_keystring()
    }

    #[must_use]
    pub fn as_cow_keystring(&self) -> CowKeyString<'_> {
        let res = match self {
            Self::Root(k) => k.as_ne_str().try_into(),
            Self::Meas(k) => k.as_ne_string().try_into(),
            Self::Gate(k) => k.as_ne_string().try_into(),
            Self::Peak(k) => k.as_ne_string().try_into(),
            Self::Region(k) => k.as_ne_string().try_into(),
            Self::Dfc(k) => k.as_ne_string().try_into(),
            Self::CsvFlag(k) => k.as_ne_string().try_into(),
        };
        res.expect("standard key should make valid keystring")
    }

    #[must_use]
    pub fn from_optical_only_key(k: OpticalOnlyKey, i: MeasIndex) -> Self {
        Self::Meas(MeasKey::from_optical_only_key(k, i))
    }
}

type NEStdKey = NEAlt<
    NEAlt<NEAlt<ToNE<RootKey>, ToNE<MeasKey>>, NEAlt<ToNE<PeakKey>, ToNE<GateKey>>>,
    NEAlt<NEAlt<ToNE<RegionKey>, ToNE<CsvFlag>>, ToNE<DfcKey>>,
>;

impl<'a> ToDisplayNE<'a> for StdKey {
    type NE = NEConcat<char, NEStdKey>;
    fn to_ne(&'a self) -> Self::NE {
        let inner = match self {
            Self::Root(x) => NEAlt::Left(NEAlt::Left(NEAlt::Left(ToNE(*x)))),
            Self::Meas(x) => NEAlt::Left(NEAlt::Left(NEAlt::Right(ToNE(*x)))),
            Self::Peak(x) => NEAlt::Left(NEAlt::Right(NEAlt::Left(ToNE(*x)))),
            Self::Gate(x) => NEAlt::Left(NEAlt::Right(NEAlt::Right(ToNE(*x)))),
            Self::Region(x) => NEAlt::Right(NEAlt::Left(NEAlt::Left(ToNE(*x)))),
            Self::CsvFlag(x) => NEAlt::Right(NEAlt::Left(NEAlt::Right(ToNE(*x)))),
            Self::Dfc(x) => NEAlt::Right(NEAlt::Right(ToNE(*x))),
        };
        NEConcat::new('$', inner)
    }
}

impl<'a> ToDisplayNE<'a> for RootKey {
    type NE = &'static NEStr;
    fn to_ne(&'a self) -> Self::NE {
        <&'static NEStr>::from(*self)
    }
}

impl<'a> ToDisplayNE<'a> for MeasKey {
    type NE = NEConcat3<char, ToNE<MeasIndex>, &'static NEStr>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat::new(NEConcat::new('P', ToNE(self.index)), self.id.into())
    }
}

impl<'a> ToDisplayNE<'a> for PeakKey {
    type NE = NEConcat<&'static NEStr, ToNE<MeasIndex>>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat::new(self.id.into(), ToNE(self.index))
    }
}

impl<'a> ToDisplayNE<'a> for GateKey {
    type NE = NEConcat3<char, ToNE<GateIndex>, &'static NEStr>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat::new(NEConcat::new('G', ToNE(self.index)), self.id.into())
    }
}

impl<'a> ToDisplayNE<'a> for RegionKey {
    type NE = NEConcat3<char, ToNE<RegionIndex>, &'static NEStr>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat::new(NEConcat::new('R', ToNE(self.index)), self.id.into())
    }
}

impl<'a> ToDisplayNE<'a> for CsvFlag {
    type NE = NEConcat3<&'static NEStr, ToNE<IndexFromOne>, &'static NEStr>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat::new(
            NEConcat::new(ne_str!("CSV"), ToNE(self.index)),
            ne_str!("FLAG"),
        )
    }
}

impl<'a> ToDisplayNE<'a> for DfcKey {
    type NE = NEConcat4<&'static NEStr, ToNE<MeasIndex>, &'static NEStr, ToNE<MeasIndex>>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat::new(
            NEConcat::new(
                NEConcat::new(ne_str!("DFC"), ToNE(self.index0)),
                ne_str!("TO"),
            ),
            ToNE(self.index1),
        )
    }
}

impl From<RootKey> for &'static NEStr {
    fn from(value: RootKey) -> Self {
        value.as_ne_str()
    }
}

impl From<MeasKeySuffix> for &'static NEStr {
    fn from(value: MeasKeySuffix) -> Self {
        value.as_ne_str()
    }
}

impl From<PeakKeyPrefix> for &'static NEStr {
    fn from(value: PeakKeyPrefix) -> Self {
        value.as_ne_str()
    }
}

impl From<ParamKeySuffix> for &'static NEStr {
    fn from(value: ParamKeySuffix) -> Self {
        value.as_ne_str()
    }
}

impl From<RegionKeySuffix> for &'static NEStr {
    fn from(value: RegionKeySuffix) -> Self {
        value.to_ne_str()
    }
}

impl AnyStdKey {
    #[must_use]
    pub fn from_bytes_maybe(bytes: &NESlice<'_, u8>) -> Option<Self> {
        is_printable_ascii(bytes.as_ref()).then(|| {
            // SAFETY: we checked that bytes are ASCII
            unsafe { Self::from_ascii_bytes(bytes) }
        })
    }

    /// Parse a standard key a sequence of bytes (non-empty).
    ///
    /// # Safety
    ///
    /// The caller must check that the bytes are printable ASCII (32-126).
    unsafe fn from_ascii_bytes(bytes: &NESlice<'_, u8>) -> Self {
        let (b0, bs) = bytes.split_first();
        match b0.to_ascii_uppercase() {
            // Try to match $Pn*, $PKn, or $PKNn first based on the first letter
            // being "P".
            b'P' => {
                if let Some((b1, bs1)) = bs.split_first()
                    && b1.eq_ignore_ascii_case(&b'K')
                {
                    if let Some((b2, bs2)) = bs.split_first()
                        && b2.eq_ignore_ascii_case(&b'N')
                        && let Some((i, rest)) = split_index_and_suffix(bs2)
                        && rest.is_empty()
                    {
                        // $PKNn
                        let k = PeakKey::new(i.into(), PeakKeyPrefix::Pkn);
                        Self::Real(StdKey::Peak(k))
                    } else if let Some((i, rest)) = split_index_and_suffix(bs1)
                        && rest.is_empty()
                    {
                        // $PKn
                        let k = PeakKey::new(i.into(), PeakKeyPrefix::Pk);
                        Self::Real(StdKey::Peak(k))
                    } else {
                        // something else
                        //
                        // SAFETY: function is unsafe
                        unsafe { Self::from_ascii_bytes_nonparam(bytes) }
                    }
                } else if let Some((i, rest)) = split_index_and_suffix(bs)
                    && let Some(mid) = NESlice::try_from_slice(rest)
                        .and_then(|suffix| MeasKeySuffix::from_suffix(&suffix))
                {
                    // $Pn*
                    let k = MeasKey::new(i.into(), mid);
                    Self::Real(StdKey::Meas(k))
                } else {
                    // something else
                    //
                    // SAFETY: function is unsafe
                    unsafe { Self::from_ascii_bytes_nonparam(bytes) }
                }
            }
            // Try to match $Gn*
            b'G' => {
                if let Some((i, rest)) = split_index_and_suffix(bs)
                    && rest.len() == 1
                    && let Some(gid) = GateKeySuffix::from_byte(rest[0])
                {
                    let k = GateKey::new(i.into(), gid);
                    Self::Real(StdKey::Gate(k))
                } else {
                    // SAFETY: function is unsafe
                    unsafe { Self::from_ascii_bytes_nonparam(bytes) }
                }
            }
            // Try to match $Rn*
            b'R' => {
                if let Some((i, rest)) = split_index_and_suffix(bs)
                    && rest.len() == 1
                    && let Some(rid) = RegionKeySuffix::from_byte(rest[0])
                {
                    let k = RegionKey::new(i.into(), rid);
                    Self::Real(StdKey::Region(k))
                } else {
                    // SAFETY: function is unsafe
                    unsafe { Self::from_ascii_bytes_nonparam(bytes) }
                }
            }
            // We didn't find any of these prefixes, try all the other keywords.
            //
            // SAFETY: function is unsafe
            _ => unsafe { Self::from_ascii_bytes_nonparam(bytes) },
        }
    }

    /// Parse a non-parameter standard key a sequence of bytes (non-empty).
    ///
    /// # Safety
    ///
    /// The caller must check that the bytes are printable ASCII (32-126)
    unsafe fn from_ascii_bytes_nonparam(bytes: &NESlice<'_, u8>) -> Self {
        if let Some(rk) = RootKey::from_bytes(bytes.as_ref()) {
            Self::Real(StdKey::Root(rk))
        } else if let Some(csv) = CsvFlag::from_bytes(bytes.as_ref()) {
            Self::Real(StdKey::CsvFlag(csv))
        } else if let Some(dfc) = DfcKey::from_bytes(bytes.as_ref()) {
            Self::Real(StdKey::Dfc(dfc))
        } else {
            // SAFETY: function is unsafe
            let p = unsafe { KeyString::from_bytes(bytes) };
            Self::Pseudo(PseudoStdKey(p))
        }
    }
}

impl RootKey {
    #[must_use]
    pub const fn as_ne_str(&self) -> &'static NEStr {
        match self {
            Self::Byteord => ne_str!("BYTEORD"),
            Self::Datatype => ne_str!("DATETYPE"),
            Self::Mode => ne_str!("MODE"),
            Self::Par => ne_str!("PAR"),
            Self::Tot => ne_str!("TOT"),
            Self::Cyt => ne_str!("CYT"),
            Self::Abrt => ne_str!("ABRT"),
            Self::Cells => ne_str!("CELLS"),
            Self::Com => ne_str!("COM"),
            Self::Exp => ne_str!("EXP"),
            Self::Fil => ne_str!("FIL"),
            Self::Inst => ne_str!("INST"),
            Self::Lost => ne_str!("LOST"),
            Self::Op => ne_str!("OP"),
            Self::Proj => ne_str!("PROJ"),
            Self::Smno => ne_str!("SMNO"),
            Self::Src => ne_str!("SRC"),
            Self::Sys => ne_str!("SYS"),
            Self::Tr => ne_str!("TR"),
            Self::Cytsn => ne_str!("CYTSN"),
            Self::Timestep => ne_str!("TIMESTEP"),
            Self::Vol => ne_str!("VOL"),
            Self::Unicode => ne_str!("UNICODE"),
            Self::Flowrate => ne_str!("FLOWRATE"),
            Self::Begindata => ne_str!("BEGINDATA"),
            Self::Beginanalysis => ne_str!("BEGINANALYSIS"),
            Self::Beginstext => ne_str!("BEGINSTEXT"),
            Self::Enddata => ne_str!("ENDDATA"),
            Self::Endanalysis => ne_str!("ENDANALYSIS"),
            Self::Endstext => ne_str!("ENDSTEXT"),
            Self::Nextdata => ne_str!("NEXTDATA"),
            Self::Btim => ne_str!("BTIM"),
            Self::Etim => ne_str!("ETIM"),
            Self::Date => ne_str!("DATE"),
            Self::Begindatetime => ne_str!("BEGINDATETIME"),
            Self::Enddatetime => ne_str!("ENDDATETIME"),
            Self::Comp => ne_str!("COMP"),
            Self::Spillover => ne_str!("SPILLOVER"),
            Self::LastModified => ne_str!("LASTMODIFIED"),
            Self::LastModifier => ne_str!("LASTMODIFIER"),
            Self::Originality => ne_str!("ORIGINALITY"),
            Self::Plateid => ne_str!("PLATEID"),
            Self::Platename => ne_str!("PLATENAME"),
            Self::Wellid => ne_str!("WELLID"),
            Self::UnstainedCenters => ne_str!("UNSTAINEDCENTERS"),
            Self::UnstainedInfo => ne_str!("UNSTAINEDINFO"),
            Self::CarrierId => ne_str!("CARRIERID"),
            Self::CarrierType => ne_str!("CARRIERTYPE"),
            Self::LocationId => ne_str!("LOCATIONID"),
            Self::Csmode => ne_str!("CSMODE"),
            Self::Csvbits => ne_str!("CSVBITS"),
            Self::Cstot => ne_str!("CSTOT"),
            Self::Gating => ne_str!("GATING"),
            Self::Gate => ne_str!("GATE"),
        }
    }

    const fn from_bytes(bytes: &[u8]) -> Option<Self> {
        macro_rules! match_bytes {
            ($($bytes:expr => $var:ident),*) => {{
                $(
                    if bytes.eq_ignore_ascii_case($bytes) {
                        return Some(Self::$var)
                    }
                )*
                None
            }};
        }
        match bytes.len() {
            2 => match_bytes!(
                b"OP" => Op,
                b"TR" => Tr
            ),
            3 => match_bytes!(
                b"COM" => Com,
                b"CYT" => Cyt,
                b"EXP" => Exp,
                b"FIL" => Fil,
                b"PAR" => Par,
                b"TOT" => Tot,
                b"SRC" => Src,
                b"SYS" => Sys,
                b"VOL" => Vol
            ),
            4 => match_bytes!(
                b"ABRT" => Abrt,
                b"BTIM" => Btim,
                b"COMP" => Comp,
                b"DATE" => Date,
                b"ETIM" => Etim,
                b"GATE" => Gate,
                b"INST" => Inst,
                b"LOST" => Lost,
                b"MODE" => Mode,
                b"PROJ" => Proj,
                b"SMNO" => Smno
            ),
            5 => match_bytes!(
                b"CELLS" => Cells,
                b"CYTSN" => Cytsn,
                b"CSTOT" => Cstot
            ),
            6 => match_bytes!(
                b"CSMODE" => Csmode,
                b"GATING" => Gating,
                b"WELLID" => Wellid
            ),
            7 => match_bytes!(
                b"BYTEORD" => Byteord,
                b"CSVBITS" => Csvbits,
                b"ENDDATA" => Enddata,
                b"PLATEID" => Plateid,
                b"UNICODE" => Unicode
            ),
            8 => match_bytes!(
                b"DATETYPE" => Datatype,
                b"ENDSTEXT" => Endstext,
                b"FLOWRATE" => Flowrate,
                b"NEXTDATA" => Nextdata,
                b"TIMESTEP" => Timestep
            ),
            9 => match_bytes!(
                b"BEGINDATA" => Begindata,
                b"CARRIERID" => CarrierId,
                b"PLATENAME" => Platename,
                b"SPILLOVER" => Spillover
            ),
            10 => match_bytes!(
                b"BEGINSTEXT" => Beginstext,
                b"LOCATIONID" => LocationId
            ),
            11 => match_bytes!(
                b"CARRIERTYPE" => CarrierType,
                b"ENDANALYSIS" => Endanalysis,
                b"ENDDATETIME" => Enddatetime,
                b"ORIGINALITY" => Originality
            ),
            12 => match_bytes!(
                b"LASTMODIFIED" => LastModified,
                b"LASTMODIFIER" => LastModifier
            ),
            13 => match_bytes!(
                b"BEGINANALYSIS" => Beginanalysis,
                b"BEGINDATETIME" => Begindatetime,
                b"UNSTAINEDINFO" => UnstainedInfo
            ),
            _ => match_bytes!(b"UNSTAINEDCENTERS" => UnstainedCenters),
        }
    }
}

impl MeasKey {
    fn from_optical_only_key(k: OpticalOnlyKey, i: MeasIndex) -> Self {
        Self::new(i, MeasKeySuffix::from_optical_only_key(k))
    }
}

impl MeasKeySuffix {
    const fn as_ne_str(self) -> &'static NEStr {
        match self {
            Self::Param(p) => p.as_ne_str(),
            Self::B => ne_str!("B"),
            Self::L => ne_str!("L"),
            Self::O => ne_str!("O"),
            Self::G => ne_str!("G"),
            Self::D => ne_str!("D"),
            Self::Det => ne_str!("DET"),
            Self::Tag => ne_str!("TAG"),
            Self::Type => ne_str!("TYPE"),
            Self::Feature => ne_str!("FEATURE"),
            Self::Analyte => ne_str!("ANALYTE"),
            Self::Datatype => ne_str!("DATATYPE"),
            Self::Calibration => ne_str!("CALIBRATION"),
        }
    }

    fn from_suffix(bytes: &NESlice<'_, u8>) -> Option<Self> {
        macro_rules! match_bytes {
            ($($bytes:expr => $var:ident),*) => {{
                $(
                    if bytes.as_ref().eq_ignore_ascii_case($bytes) {
                        return Some(Self::$var)
                    }
                )*
                None
            }};
        }

        let sn = bytes.len().get();
        match sn {
            1 => {
                let s0 = bytes.first();
                if let Some(pid) = ParamKeySuffix::from_byte(*s0) {
                    Some(Self::Param(pid))
                } else {
                    match s0.to_ascii_uppercase() {
                        b'B' => Some(Self::B),
                        b'L' => Some(Self::L),
                        b'O' => Some(Self::O),
                        b'G' => Some(Self::G),
                        b'D' => Some(Self::D),
                        _ => None,
                    }
                }
            }
            3 => match_bytes!(b"DET" => Det, b"TAG" => Tag),
            7 => match_bytes!(
                b"FEATURE" => Feature,
                b"ANALYTE" => Analyte
            ),
            _ => match_bytes!(
                b"TYPE" => Type,
                b"DATATYPE" => Datatype,
                b"CALIBRATION" => Calibration
            ),
        }
    }

    fn from_optical_only_key(k: OpticalOnlyKey) -> Self {
        match k {
            OpticalOnlyKey::Gain => Self::G,
            OpticalOnlyKey::Filter => Self::Param(ParamKeySuffix::F),
            OpticalOnlyKey::Wavelength => Self::L,
            OpticalOnlyKey::Power => Self::O,
            OpticalOnlyKey::DetectorType => Self::Param(ParamKeySuffix::T),
            OpticalOnlyKey::DetectorVoltage => Self::Param(ParamKeySuffix::V),
            OpticalOnlyKey::PercentEmitted => Self::Param(ParamKeySuffix::P),
            OpticalOnlyKey::Calibration => Self::Calibration,
            OpticalOnlyKey::DetectorName => Self::Det,
            OpticalOnlyKey::Tag => Self::Tag,
            OpticalOnlyKey::Feature => Self::Feature,
            OpticalOnlyKey::Analyte => Self::Analyte,
        }
    }
}

impl PeakKeyPrefix {
    const fn as_ne_str(self) -> &'static NEStr {
        match self {
            Self::Pk => ne_str!("PK"),
            Self::Pkn => ne_str!("PKN"),
        }
    }
}

impl ParamKeySuffix {
    const fn as_ne_str(self) -> &'static NEStr {
        match self {
            Self::N => ne_str!("N"),
            Self::R => ne_str!("R"),
            Self::E => ne_str!("E"),
            Self::S => ne_str!("S"),
            Self::F => ne_str!("F"),
            Self::T => ne_str!("T"),
            Self::P => ne_str!("P"),
            Self::V => ne_str!("V"),
        }
    }

    fn from_byte(b: u8) -> Option<Self> {
        match b.to_ascii_uppercase() {
            b'N' => Some(Self::N),
            b'R' => Some(Self::R),
            b'E' => Some(Self::E),
            b'S' => Some(Self::S),
            b'F' => Some(Self::F),
            b'T' => Some(Self::T),
            b'P' => Some(Self::P),
            b'V' => Some(Self::V),
            _ => None,
        }
    }
}

impl RegionKeySuffix {
    const fn to_ne_str(self) -> &'static NEStr {
        match self {
            Self::I => ne_str!("I"),
            Self::W => ne_str!("W"),
        }
    }

    const fn from_byte(b: u8) -> Option<Self> {
        match b.to_ascii_uppercase() {
            b'I' => Some(Self::I),
            b'W' => Some(Self::W),
            _ => None,
        }
    }
}

impl DfcKey {
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() >= 7
            && bytes[0..3].eq_ignore_ascii_case(b"DFC")
            && let Some((i0, rest0)) = split_index_and_suffix(&bytes[3..])
            && rest0.len() > 2
            && rest0[0..2].eq_ignore_ascii_case(b"TO")
            && let Some((i1, rest1)) = split_index_and_suffix(&rest0[2..])
            && rest1.is_empty()
        {
            Some(Self {
                index0: i0.into(),
                index1: i1.into(),
            })
        } else {
            None
        }
    }
}

impl CsvFlag {
    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() >= 9
            && bytes[0..3].eq_ignore_ascii_case(b"CSV")
            && let Some((i, rest)) = split_index_and_suffix(&bytes[3..])
            && rest.eq_ignore_ascii_case(b"FLAG")
        {
            Some(Self { index: i.into() })
        } else {
            None
        }
    }
}

/// Split an numeric index from a byte-string.
///
/// Index will be 0-based. Index will only be parsed if the byte string
/// starts with it.
fn split_index_and_suffix(bytes: &[u8]) -> Option<(usize, &[u8])> {
    let mut index = 0_usize;
    let mut it = bytes.iter();
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
        assert!(index > 0, "index should be greater than 0 here");
        Some((index - 1, bytes.split_at(k).1))
    } else {
        None
    }
}

fn is_printable_ascii(xs: &[u8]) -> bool {
    xs.iter().all(|x| 32 <= *x && *x <= 126)
}

#[cfg(feature = "serde")]
impl Serialize for StdKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

pub const STD_PREFIX: u8 = 36; // '$'
