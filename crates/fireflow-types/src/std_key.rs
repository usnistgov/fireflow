use crate::{
    config::OpticalOnlyKey,
    index::{BiMeasIndex, GateIndex, MeasIndex, RegionIndex, SubsetIndex},
    keystring::{CowKeyString, KeyString},
    keywords::{Version, VersionMembership},
    ne_str,
    nonempty::{
        DisplayableNE as _, NEAlt, NEConcat, NEConcat3, NEConcat4, NESlice, NEStr, ToDisplayNE,
        ToNE,
    },
};

use bytemuck::{NoUninit, must_cast_ref};
use derive_more::{AsRef, Display, From, TryInto};
use derive_new::new;
use strum::{EnumCount, VariantArray};
use strum_macros::{EnumCount as EnumCount_, VariantArray};
use thiserror::Error;

use std::iter;
use std::ops;
use std::slice::Iter;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString},
    pyo3::prelude::*,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum RealOrPseudoStdKey {
    Real(StdKey),
    Pseudo(PseudoStdKey),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, AsRef)]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyObject))]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[display("${_0}")]
#[as_ref(KeyString)]
pub struct PseudoStdKey(pub KeyString);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, TryInto)]
#[cfg_attr(feature = "python", derive(FromPyString, IntoPyString))]
#[display("${}", self.as_displayable())]
pub enum StdKey {
    Root(RootKey),
    Meas(MeasKey),
    Gate(GateKey),
    Region(RegionKey),
    CsvFlag(CsvFlagKey),
    Dfc(DfcKey),
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, EnumCount_, VariantArray, NoUninit,
)]
#[repr(usize)]
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
pub struct IndexedKey<const LEN: usize, I, K> {
    pub index: I,
    pub id: K,
}

#[derive(new, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DfcKey {
    pub index: BiMeasIndex,
}

#[derive(new, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CsvFlagKey {
    pub index: SubsetIndex,
}

pub struct CsvFlagKeyMarker;

pub struct DfcKeyMarker;

pub type MeasKey = IndexedKey<N_MEAS, MeasIndex, MeasKeyId>;
pub type GateKey = IndexedKey<N_GATE, GateIndex, GateKeyId>;
pub type RegionKey = IndexedKey<N_REGION, RegionIndex, RegionKeyId>;

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, EnumCount_, VariantArray, NoUninit,
)]
#[repr(usize)]
pub enum MeasKeyId {
    N,
    R,
    E,
    S,
    F,
    T,
    P,
    V,
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
    Pk,
    Pkn,
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, EnumCount_, VariantArray, NoUninit,
)]
#[repr(usize)]
pub enum GateKeyId {
    N,
    R,
    E,
    S,
    F,
    T,
    P,
    V,
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, EnumCount_, VariantArray, NoUninit,
)]
#[repr(usize)]
pub enum RegionKeyId {
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

/// An enum which can be used to index into an array.
///
/// The following properties must hold:
///
/// * Enum must be repr(usize).
/// * Enum must have no fields.
/// * Enum variants must map to 0-N in order.
/// * The supplied constant must match the number of variants.
///
/// The constant parameter is meant to be used to enforce array lengths since
/// Rust does not allow using associated constants in const/type definitions
/// (yet).
pub trait EnumIndex<const LEN: usize>: VariantArray + EnumCount + NoUninit {
    const _CHECK: () = {
        assert!(LEN == Self::COUNT, "const does not match var count");
        assert!(
            is_zero_to_n_usize(Self::VARIANTS),
            "all variants must be monotonically increasing usize starting from 0",
        );
    };

    #[allow(path_statements)]
    fn iter_ref() -> Iter<'static, Self> {
        Self::_CHECK;
        Self::VARIANTS.iter()
    }

    fn iter() -> EnumIndexIter<Self> {
        Self::iter_ref().copied()
    }

    fn index(&self) -> usize {
        *must_cast_ref(self)
    }
}

pub trait AnyIndex {
    type SubDimension;
    type Generator: Iterator<Item = Self>;

    fn generate(sub: &Self::SubDimension) -> Self::Generator;

    fn offset(&self, sub: &Self::SubDimension) -> usize;

    fn offset0(&self) -> usize
    where
        Self: AnyIndex<SubDimension = ()>,
    {
        self.offset(&())
    }
}

pub trait ToStd {
    type Index;

    fn to_std(&self, index: &Self::Index) -> StdKey;

    fn to_std0(&self) -> StdKey
    where
        Self: ToStd<Index = ()>,
    {
        self.to_std(&())
    }
}

impl EnumIndex<N_ROOT> for RootKey {}
impl EnumIndex<N_MEAS> for MeasKeyId {}
impl EnumIndex<N_GATE> for GateKeyId {}
impl EnumIndex<N_REGION> for RegionKeyId {}

pub type EnumIndexIter<T> = iter::Copied<Iter<'static, T>>;

pub type RootKeyGenerator = EnumIndexIter<RootKey>;

pub type IndexedKeyGenerator<const LEN: usize, I, K> = iter::Map<
    iter::Zip<iter::Cycle<EnumIndexIter<K>>, ops::RangeFrom<usize>>,
    fn((K, usize)) -> IndexedKey<LEN, I, K>,
>;

pub type CsvFlagGenerator = iter::Map<ops::RangeFrom<usize>, fn(usize) -> CsvFlagKey>;

pub type DfcKeyGenerator = iter::Map<
    iter::Zip<
        iter::Zip<iter::Cycle<ops::Range<usize>>, ops::RangeFrom<usize>>,
        iter::Repeat<usize>,
    >,
    fn(((usize, usize), usize)) -> DfcKey,
>;

impl AnyIndex for RootKey {
    type SubDimension = ();
    type Generator = RootKeyGenerator;

    fn generate((): &Self::SubDimension) -> Self::Generator {
        Self::iter()
    }

    fn offset(&self, (): &Self::SubDimension) -> usize {
        self.index()
    }
}

impl<const LEN: usize, I, K> AnyIndex for IndexedKey<LEN, I, K>
where
    K: EnumIndex<LEN>,
    I: From<usize> + Into<usize> + Copy,
{
    type SubDimension = ();
    type Generator = IndexedKeyGenerator<LEN, I, K>;

    fn generate((): &Self::SubDimension) -> Self::Generator {
        // TODO get rid of division with custom iterator
        K::iter()
            .cycle()
            .zip(0_usize..)
            .map(|(id, i)| Self::new((i / LEN).into(), id))
    }

    fn offset(&self, (): &Self::SubDimension) -> usize {
        self.index.into() * K::COUNT + self.id.index()
    }
}

impl AnyIndex for CsvFlagKey {
    type SubDimension = ();
    type Generator = CsvFlagGenerator;

    fn generate((): &Self::SubDimension) -> Self::Generator {
        (0_usize..).map(|i| Self::new(i.into()))
    }

    fn offset(&self, (): &Self::SubDimension) -> usize {
        self.index.into()
    }
}

impl AnyIndex for DfcKey {
    type SubDimension = usize;
    type Generator = DfcKeyGenerator;

    fn generate(sub: &Self::SubDimension) -> Self::Generator {
        // TODO get rid of division with custom iterator
        (0_usize..*sub)
            .cycle()
            .zip(0_usize..)
            .zip(iter::repeat(*sub))
            .map(|((col, i), len)| Self::new(BiMeasIndex::new((i / len).into(), col.into())))
    }

    fn offset(&self, sub: &Self::SubDimension) -> usize {
        usize::from(self.index.i0) * sub + usize::from(self.index.i1)
    }
}

pub const N_ROOT: usize = 54;
pub const N_MEAS: usize = 22;
pub const N_GATE: usize = 8;
pub const N_REGION: usize = 2;

macro_rules! match_bytes {
    ($src:expr, $($bytes:expr => $var:path),*) => {{
        $(
            if $src.eq_ignore_ascii_case($bytes.as_str().as_bytes()) {
                return Some($var)
            }
        )*
        None
    }};
}

impl ToStd for RootKey {
    type Index = ();

    fn to_std(&self, (): &Self::Index) -> StdKey {
        (*self).into()
    }
}

impl ToStd for MeasKeyId {
    type Index = MeasIndex;

    fn to_std(&self, index: &Self::Index) -> StdKey {
        IndexedKey::new(*index, *self).into()
    }
}

impl ToStd for GateKeyId {
    type Index = GateIndex;

    fn to_std(&self, index: &Self::Index) -> StdKey {
        IndexedKey::new(*index, *self).into()
    }
}

impl ToStd for RegionKeyId {
    type Index = RegionIndex;

    fn to_std(&self, index: &Self::Index) -> StdKey {
        IndexedKey::new(*index, *self).into()
    }
}

impl ToStd for CsvFlagKeyMarker {
    type Index = SubsetIndex;

    fn to_std(&self, index: &Self::Index) -> StdKey {
        CsvFlagKey::new(*index).into()
    }
}

impl ToStd for DfcKeyMarker {
    type Index = BiMeasIndex;

    fn to_std(&self, index: &Self::Index) -> StdKey {
        DfcKey::new(*index).into()
    }
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
                let k = unsafe { RealOrPseudoStdKey::from_ascii_bytes(ne) };
                match k {
                    RealOrPseudoStdKey::Pseudo(x) => Err(StdKeyError::Pseudo(x)),
                    RealOrPseudoStdKey::Real(x) => Ok(x),
                }
            } else {
                Err(StdKeyError::Dollar)
            }
        } else {
            Err(StdKeyError::Empty)
        }
    }
}

// impl AnyStdKey {
//     #[must_use]
//     pub fn into_keystring(self) -> KeyString {
//         match self {
//             Self::Real(x) => x.as_keystring(),
//             Self::Pseudo(x) => x.0,
//         }
//     }
// }

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
            Self::Region(k) => k.as_ne_string().try_into(),
            Self::Dfc(k) => k.as_ne_string().try_into(),
            Self::CsvFlag(k) => k.as_ne_string().try_into(),
        };
        res.expect("standard key should make valid keystring")
    }

    #[must_use]
    pub fn from_optical_only_key(k: OpticalOnlyKey, i: MeasIndex) -> Self {
        Self::Meas(MeasKey::new(i, MeasKeyId::from_optical_only_key(k)))
    }

    #[must_use]
    pub const fn membership(&self) -> VersionMembership {
        match self {
            Self::Root(k) => k.membership(),
            Self::Meas(k) => k.id.membership(),
            Self::Gate(_) => GateKeyId::membership(),
            Self::Region(_) => RegionKeyId::membership(),
            Self::Dfc(_) => DfcKey::membership(),
            Self::CsvFlag(_) => CsvFlagKey::membership(),
        }
    }
}

type NEStdKey = NEAlt<
    NEAlt<ToNE<RootKey>, NEAlt<ToNE<MeasKey>, ToNE<GateKey>>>,
    NEAlt<ToNE<RegionKey>, NEAlt<ToNE<CsvFlagKey>, ToNE<DfcKey>>>,
>;

impl<'a> ToDisplayNE<'a> for StdKey {
    type NE = NEConcat<char, NEStdKey>;
    fn to_ne(&'a self) -> Self::NE {
        let inner = match self {
            Self::Root(x) => NEAlt::Left(NEAlt::Left(ToNE(*x))),
            Self::Meas(x) => NEAlt::Left(NEAlt::Right(NEAlt::Left(ToNE(*x)))),
            Self::Gate(x) => NEAlt::Left(NEAlt::Right(NEAlt::Right(ToNE(*x)))),
            Self::Region(x) => NEAlt::Right(NEAlt::Left(ToNE(*x))),
            Self::CsvFlag(x) => NEAlt::Right(NEAlt::Right(NEAlt::Left(ToNE(*x)))),
            Self::Dfc(x) => NEAlt::Right(NEAlt::Right(NEAlt::Right(ToNE(*x)))),
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
    type NE = NEAlt<
        NEConcat3<char, ToNE<MeasIndex>, &'static NEStr>,
        NEConcat<&'static NEStr, ToNE<MeasIndex>>,
    >;
    fn to_ne(&'a self) -> Self::NE {
        let i = ToNE(self.index);
        match self.id.prefix_or_suffix() {
            PrefixOrSuffix::Suffix(s) => NEAlt::Left(NEConcat::new('P', i).append(s)),
            PrefixOrSuffix::Prefix(p) => NEAlt::Right(NEConcat::new(p, i)),
        }
    }
}

impl<'a> ToDisplayNE<'a> for GateKey {
    type NE = NEConcat3<char, ToNE<GateIndex>, &'static NEStr>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat::new(NEConcat::new('G', ToNE(self.index)), self.id.suffix())
    }
}

impl<'a> ToDisplayNE<'a> for RegionKey {
    type NE = NEConcat3<char, ToNE<RegionIndex>, &'static NEStr>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat::new(NEConcat::new('R', ToNE(self.index)), self.id.suffix())
    }
}

impl<'a> ToDisplayNE<'a> for CsvFlagKey {
    type NE = NEConcat3<&'static NEStr, ToNE<SubsetIndex>, &'static NEStr>;
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
                NEConcat::new(ne_str!("DFC"), ToNE(self.index.i0)),
                ne_str!("TO"),
            ),
            ToNE(self.index.i1),
        )
    }
}

impl From<RootKey> for &'static NEStr {
    fn from(value: RootKey) -> Self {
        value.as_ne_str()
    }
}

// impl From<MeasKeyId> for &'static NEStr {
//     fn from(value: MeasKeyId) -> Self {
//         value.as_ne_str()
//     }
// }

// impl From<GateKeyId> for &'static NEStr {
//     fn from(value: GateKeyId) -> Self {
//         value.as_ne_str()
//     }
// }

// impl From<RegionKeyId> for &'static NEStr {
//     fn from(value: RegionKeyId) -> Self {
//         value.to_ne_str()
//     }
// }

impl RealOrPseudoStdKey {
    #[must_use]
    pub fn from_bytes_maybe(bytes: &NESlice<u8>) -> Option<Self> {
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
    unsafe fn from_ascii_bytes(bytes: &NESlice<u8>) -> Self {
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
                        let k = MeasKey::new(i.into(), MeasKeyId::Pkn);
                        Self::Real(StdKey::Meas(k))
                    } else if let Some((i, rest)) = split_index_and_suffix(bs1)
                        && rest.is_empty()
                    {
                        // $PKn
                        let k = MeasKey::new(i.into(), MeasKeyId::Pk);
                        Self::Real(StdKey::Meas(k))
                    } else {
                        // something else
                        //
                        // SAFETY: function is unsafe
                        unsafe { Self::from_ascii_bytes_nonparam(bytes) }
                    }
                } else if let Some((i, rest)) = split_index_and_suffix(bs)
                    && let Some(mid) =
                        NESlice::try_from_slice(rest).and_then(MeasKeyId::from_suffix)
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
                    && let Some(gid) = GateKeyId::from_byte(rest[0])
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
                    && let Some(rid) = RegionKeyId::from_byte(rest[0])
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
    unsafe fn from_ascii_bytes_nonparam(bytes: &NESlice<u8>) -> Self {
        if let Some(rk) = RootKey::from_bytes(bytes.as_ref()) {
            Self::Real(StdKey::Root(rk))
        } else if let Some(csv) = CsvFlagKey::from_bytes(bytes.as_ref()) {
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
            Self::Byteord => BYTEORD,
            Self::Datatype => DATATYPE,
            Self::Mode => MODE,
            Self::Par => PAR,
            Self::Tot => TOT,
            Self::Cyt => CYT,
            Self::Abrt => ABRT,
            Self::Cells => CELLS,
            Self::Com => COM,
            Self::Exp => EXP,
            Self::Fil => FIL,
            Self::Inst => INST,
            Self::Lost => LOST,
            Self::Op => OP,
            Self::Proj => PROJ,
            Self::Smno => SMNO,
            Self::Src => SRC,
            Self::Sys => SYS,
            Self::Tr => TR,
            Self::Cytsn => CYTSN,
            Self::Timestep => TIMESTEP,
            Self::Vol => VOL,
            Self::Unicode => UNICODE,
            Self::Flowrate => FLOWRATE,
            Self::Begindata => BEGINDATA,
            Self::Beginanalysis => BEGINANALYSIS,
            Self::Beginstext => BEGINSTEXT,
            Self::Enddata => ENDDATA,
            Self::Endanalysis => ENDANALYSIS,
            Self::Endstext => ENDSTEXT,
            Self::Nextdata => NEXTDATA,
            Self::Btim => BTIM,
            Self::Etim => ETIM,
            Self::Date => DATE,
            Self::Begindatetime => BEGINDATETIME,
            Self::Enddatetime => ENDDATETIME,
            Self::Comp => COMP,
            Self::Spillover => SPILLOVER,
            Self::LastModified => LAST_MODIFIED,
            Self::LastModifier => LAST_MODIFIER,
            Self::Originality => ORIGINALITY,
            Self::Plateid => PLATEID,
            Self::Platename => PLATENAME,
            Self::Wellid => WELLID,
            Self::UnstainedCenters => UNSTAINEDCENTERS,
            Self::UnstainedInfo => UNSTAINEDINFO,
            Self::CarrierId => CARRIERID,
            Self::CarrierType => CARRIERTYPE,
            Self::LocationId => LOCATIONID,
            Self::Csmode => CSMODE,
            Self::Csvbits => CSVBITS,
            Self::Cstot => CSTOT,
            Self::Gating => GATING,
            Self::Gate => GATE,
        }
    }

    const fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match bytes.len() {
            2 => match_bytes!(
                bytes,
                OP => Self::Op,
                TR => Self::Tr
            ),
            3 => match_bytes!(
                bytes,
                COM => Self::Com,
                CYT => Self::Cyt,
                EXP => Self::Exp,
                FIL => Self::Fil,
                PAR => Self::Par,
                TOT => Self::Tot,
                SRC => Self::Src,
                SYS => Self::Sys,
                VOL => Self::Vol
            ),
            4 => match_bytes!(
                bytes,
                ABRT => Self::Abrt,
                BTIM => Self::Btim,
                COMP => Self::Comp,
                DATE => Self::Date,
                ETIM => Self::Etim,
                GATE => Self::Gate,
                INST => Self::Inst,
                LOST => Self::Lost,
                MODE => Self::Mode,
                PROJ => Self::Proj,
                SMNO => Self::Smno
            ),
            5 => match_bytes!(
                bytes,
                CELLS => Self::Cells,
                CYTSN => Self::Cytsn,
                CSTOT => Self::Cstot
            ),
            6 => match_bytes!(
                bytes,
                CSMODE => Self::Csmode,
                GATING => Self::Gating,
                WELLID => Self::Wellid
            ),
            7 => match_bytes!(
                bytes,
                BYTEORD => Self::Byteord,
                CSVBITS => Self::Csvbits,
                ENDDATA => Self::Enddata,
                PLATEID => Self::Plateid,
                UNICODE => Self::Unicode
            ),
            8 => match_bytes!(
                bytes,
                DATATYPE => Self::Datatype,
                ENDSTEXT => Self::Endstext,
                FLOWRATE => Self::Flowrate,
                NEXTDATA => Self::Nextdata,
                TIMESTEP => Self::Timestep
            ),
            9 => match_bytes!(
                bytes,
                BEGINDATA => Self::Begindata,
                CARRIERID => Self::CarrierId,
                PLATENAME => Self::Platename,
                SPILLOVER => Self::Spillover
            ),
            10 => match_bytes!(
                bytes,
                BEGINSTEXT => Self::Beginstext,
                LOCATIONID => Self::LocationId
            ),
            11 => match_bytes!(
                bytes,
                CARRIERTYPE => Self::CarrierType,
                ENDANALYSIS => Self::Endanalysis,
                ENDDATETIME => Self::Enddatetime,
                ORIGINALITY => Self::Originality
            ),
            13 => match_bytes!(
                bytes,
                BEGINANALYSIS => Self::Beginanalysis,
                BEGINDATETIME => Self::Begindatetime,
                LAST_MODIFIED => Self::LastModified,
                LAST_MODIFIER => Self::LastModifier,
                UNSTAINEDINFO => Self::UnstainedInfo
            ),
            _ => match_bytes!(bytes, UNSTAINEDCENTERS => Self::UnstainedCenters),
        }
    }

    const fn membership(self) -> VersionMembership {
        match self {
            Self::Begindata
            | Self::Beginanalysis
            | Self::Beginstext
            | Self::Enddata
            | Self::Endanalysis
            | Self::Endstext
            | Self::Cytsn
            | Self::Timestep => {
                VersionMembership::Three([Version::FCS3_0, Version::FCS3_1, Version::FCS3_2])
            }
            Self::Gate => {
                VersionMembership::Three([Version::FCS2_0, Version::FCS3_0, Version::FCS3_1])
            }
            Self::Unicode | Self::Comp => VersionMembership::One(Version::FCS3_0),
            Self::Vol
            | Self::Spillover
            | Self::LastModified
            | Self::LastModifier
            | Self::Originality
            | Self::Plateid
            | Self::Platename
            | Self::Wellid => VersionMembership::Two([Version::FCS3_1, Version::FCS3_2]),
            Self::Begindatetime
            | Self::Enddatetime
            | Self::UnstainedCenters
            | Self::UnstainedInfo
            | Self::CarrierId
            | Self::CarrierType
            | Self::LocationId
            | Self::Flowrate => VersionMembership::One(Version::FCS3_2),
            Self::Csmode | Self::Csvbits | Self::Cstot => {
                VersionMembership::Two([Version::FCS3_0, Version::FCS3_1])
            }
            _ => VersionMembership::All,
        }
    }
}

impl<const LEN: usize, I, K> IndexedKey<LEN, I, K> {
    // pub fn offset(&self) -> usize
    // where
    //     K: EnumIndex<LEN>,
    //     I: Into<usize> + Copy,
    // {
    //     K::COUNT * self.index.into() + self.id.index()
    // }

    pub fn keys_at(index: I) -> impl Iterator<Item = Self>
    where
        K: EnumIndex<LEN>,
        I: Clone,
    {
        iter::repeat(index)
            .zip(K::iter())
            .map(|(i, b)| Self::new(i, b))
    }
}

enum PrefixOrSuffix {
    Prefix(&'static NEStr),
    Suffix(&'static NEStr),
}

impl MeasKeyId {
    const fn prefix_or_suffix(self) -> PrefixOrSuffix {
        match self {
            Self::N => PrefixOrSuffix::Suffix(N_KW_SUFFIX),
            Self::R => PrefixOrSuffix::Suffix(R_KW_SUFFIX),
            Self::E => PrefixOrSuffix::Suffix(E_KW_SUFFIX),
            Self::S => PrefixOrSuffix::Suffix(S_KW_SUFFIX),
            Self::F => PrefixOrSuffix::Suffix(F_KW_SUFFIX),
            Self::T => PrefixOrSuffix::Suffix(T_KW_SUFFIX),
            Self::P => PrefixOrSuffix::Suffix(P_KW_SUFFIX),
            Self::V => PrefixOrSuffix::Suffix(V_KW_SUFFIX),
            Self::B => PrefixOrSuffix::Suffix(B_KW_SUFFIX),
            Self::L => PrefixOrSuffix::Suffix(L_KW_SUFFIX),
            Self::O => PrefixOrSuffix::Suffix(O_KW_SUFFIX),
            Self::G => PrefixOrSuffix::Suffix(G_KW_SUFFIX),
            Self::D => PrefixOrSuffix::Suffix(D_KW_SUFFIX),
            Self::Det => PrefixOrSuffix::Suffix(DET_KW_SUFFIX),
            Self::Tag => PrefixOrSuffix::Suffix(TAG_KW_SUFFIX),
            Self::Type => PrefixOrSuffix::Suffix(TYPE_KW_SUFFIX),
            Self::Feature => PrefixOrSuffix::Suffix(FEATURE_KW_SUFFIX),
            Self::Analyte => PrefixOrSuffix::Suffix(ANALYTE_KW_SUFFIX),
            Self::Datatype => PrefixOrSuffix::Suffix(DATATYPE_KW_SUFFIX),
            Self::Calibration => PrefixOrSuffix::Suffix(CALIBRATION_KW_SUFFIX),
            Self::Pk => PrefixOrSuffix::Prefix(PK_KW_PREFIX),
            Self::Pkn => PrefixOrSuffix::Prefix(PKN_KW_PREFIX),
        }
    }

    const fn membership(self) -> VersionMembership {
        match self {
            Self::G => {
                VersionMembership::Three([Version::FCS3_0, Version::FCS3_1, Version::FCS3_2])
            }
            Self::D | Self::Calibration => {
                VersionMembership::Two([Version::FCS3_1, Version::FCS3_2])
            }
            Self::Det | Self::Tag | Self::Type | Self::Feature | Self::Analyte | Self::Datatype => {
                VersionMembership::One(Version::FCS3_2)
            }
            Self::Pk | Self::Pkn => {
                VersionMembership::Three([Version::FCS2_0, Version::FCS3_0, Version::FCS3_1])
            }
            _ => VersionMembership::All,
        }
    }

    fn from_suffix(bytes: &NESlice<u8>) -> Option<Self> {
        let sn = bytes.len().get();
        match sn {
            1 => {
                match_bytes!(
                    [*bytes.first()],
                    N_KW_SUFFIX => Self::N,
                    R_KW_SUFFIX => Self::R,
                    E_KW_SUFFIX => Self::E,
                    S_KW_SUFFIX => Self::S,
                    F_KW_SUFFIX => Self::F,
                    T_KW_SUFFIX => Self::T,
                    P_KW_SUFFIX => Self::P,
                    V_KW_SUFFIX => Self::V,
                    B_KW_SUFFIX => Self::B,
                    L_KW_SUFFIX => Self::L,
                    O_KW_SUFFIX => Self::O,
                    G_KW_SUFFIX => Self::G,
                    D_KW_SUFFIX => Self::D
                )
            }
            3 => match_bytes!(
                bytes.as_ref(),
                DET_KW_SUFFIX => Self::Det,
                TAG_KW_SUFFIX => Self::Tag
            ),
            7 => match_bytes!(
                bytes.as_ref(),
                FEATURE_KW_SUFFIX => Self::Feature,
                ANALYTE_KW_SUFFIX => Self::Analyte
            ),
            _ => match_bytes!(
                bytes.as_ref(),
                TYPE_KW_SUFFIX => Self::Type,
                DATATYPE_KW_SUFFIX => Self::Datatype,
                CALIBRATION_KW_SUFFIX => Self::Calibration
            ),
        }
    }

    fn from_optical_only_key(k: OpticalOnlyKey) -> Self {
        match k {
            OpticalOnlyKey::Gain => Self::G,
            OpticalOnlyKey::Filter => Self::F,
            OpticalOnlyKey::Wavelength => Self::L,
            OpticalOnlyKey::Power => Self::O,
            OpticalOnlyKey::DetectorType => Self::T,
            OpticalOnlyKey::DetectorVoltage => Self::V,
            OpticalOnlyKey::PercentEmitted => Self::P,
            OpticalOnlyKey::Calibration => Self::Calibration,
            OpticalOnlyKey::DetectorName => Self::Det,
            OpticalOnlyKey::Tag => Self::Tag,
            OpticalOnlyKey::Feature => Self::Feature,
            OpticalOnlyKey::Analyte => Self::Analyte,
        }
    }
}

impl GateKeyId {
    const fn suffix(self) -> &'static NEStr {
        match self {
            Self::N => N_KW_SUFFIX,
            Self::R => R_KW_SUFFIX,
            Self::E => E_KW_SUFFIX,
            Self::S => S_KW_SUFFIX,
            Self::F => F_KW_SUFFIX,
            Self::T => T_KW_SUFFIX,
            Self::P => P_KW_SUFFIX,
            Self::V => V_KW_SUFFIX,
        }
    }

    #[must_use]
    pub const fn blank(self) -> &'static NEStr {
        match self {
            Self::N => PNN,
            Self::R => PNR,
            Self::E => PNE,
            Self::S => PNS,
            Self::F => PNF,
            Self::T => PNT,
            Self::P => PNP,
            Self::V => PNV,
        }
    }

    const fn membership() -> VersionMembership {
        VersionMembership::Three([Version::FCS2_0, Version::FCS3_0, Version::FCS3_1])
    }

    fn from_byte(b: u8) -> Option<Self> {
        match_bytes!(
            [b],
            N_KW_SUFFIX => Self::N,
            R_KW_SUFFIX => Self::R,
            E_KW_SUFFIX => Self::E,
            S_KW_SUFFIX => Self::S,
            F_KW_SUFFIX => Self::F,
            T_KW_SUFFIX => Self::T,
            P_KW_SUFFIX => Self::P,
            V_KW_SUFFIX => Self::V
        )
    }
}

impl RegionKeyId {
    const fn suffix(self) -> &'static NEStr {
        match self {
            Self::I => REGION_I_KW_SUFFIX,
            Self::W => REGION_W_KW_SUFFIX,
        }
    }

    #[must_use]
    pub const fn blank(self) -> &'static NEStr {
        match self {
            Self::I => RNI,
            Self::W => RNW,
        }
    }

    const fn from_byte(b: u8) -> Option<Self> {
        match_bytes!(
            [b],
            REGION_I_KW_SUFFIX => Self::I,
            REGION_W_KW_SUFFIX => Self::W
        )
    }

    const fn membership() -> VersionMembership {
        VersionMembership::All
    }
}

impl DfcKey {
    // pub fn offset(&self, matrix_size: usize) -> usize {
    //     usize::from(self.index.i0) * matrix_size + usize::from(self.index.i1)
    // }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() >= 7
            && bytes[0..3].eq_ignore_ascii_case(b"DFC")
            && let Some((i0, rest0)) = split_index_and_suffix(&bytes[3..])
            && rest0.len() > 2
            && rest0[0..2].eq_ignore_ascii_case(b"TO")
            && let Some((i1, rest1)) = split_index_and_suffix(&rest0[2..])
            && rest1.is_empty()
        {
            Some(Self::new(BiMeasIndex::new(i0.into(), i1.into())))
        } else {
            None
        }
    }

    const fn membership() -> VersionMembership {
        VersionMembership::One(Version::FCS2_0)
    }
}

impl CsvFlagKey {
    pub const BLANK: &NEStr = ne_str!("CSVnFLAG");

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() >= 9
            && bytes[0..3].eq_ignore_ascii_case(b"CSV")
            && let Some((i, rest)) = split_index_and_suffix(&bytes[3..])
            && rest.eq_ignore_ascii_case(b"FLAG")
        {
            Some(Self::new(i.into()))
        } else {
            None
        }
    }

    const fn membership() -> VersionMembership {
        VersionMembership::Two([Version::FCS3_0, Version::FCS3_1])
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

pub const STD_PREFIX: u8 = 36; // '$'

// Load list of all keyword constants from build script
include!(concat!(env!("OUT_DIR"), "/kw_strs.rs"));

// other keywords not in build script
pub const PKN: &NEStr = ne_str!("$PKn");
pub const PKNN: &NEStr = ne_str!("$PKNn");

pub const PK_KW_PREFIX: &NEStr = ne_str!("PK");
pub const PKN_KW_PREFIX: &NEStr = ne_str!("PKN");

pub const RNI: &NEStr = ne_str!("$RNI");
pub const RNW: &NEStr = ne_str!("$RNW");

pub const REGION_I_KW_SUFFIX: &NEStr = ne_str!("I");
pub const REGION_W_KW_SUFFIX: &NEStr = ne_str!("W");

const fn is_zero_to_n_usize<X: NoUninit>(xs: &[X]) -> bool {
    let mut i = 0_usize;

    while i < xs.len() {
        let x: &usize = must_cast_ref(&xs[i]);
        if *x != i {
            return false;
        }
        i += 1;
    }

    true
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

#[cfg(feature = "serde")]
pub trait BlankKeyword {
    fn blank(&self) -> &'static NEStr;
}

#[cfg(feature = "serde")]
impl BlankKeyword for MeasKeyId {
    fn blank(&self) -> &'static NEStr {
        match self {
            Self::N => PNN,
            Self::R => PNR,
            Self::E => PNE,
            Self::S => PNS,
            Self::F => PNF,
            Self::T => PNT,
            Self::P => PNP,
            Self::V => PNV,
            Self::B => PNB,
            Self::L => PNL,
            Self::O => PNO,
            Self::G => PNG,
            Self::D => PND,
            Self::Det => PNDET,
            Self::Tag => PNTAG,
            Self::Type => PNTYPE,
            Self::Feature => PNFEATURE,
            Self::Analyte => PNANALYTE,
            Self::Datatype => PNDATATYPE,
            Self::Calibration => PNCALIBRATION,
            Self::Pk => PKN,
            Self::Pkn => PKNN,
        }
    }
}
