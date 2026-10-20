use crate::config::OpticalOnlyKey;
use crate::index::{BiMeasIndex, GateIndex, MeasIndex, RegionIndex, SubsetIndex};
use crate::keys::DollarStdKey;
use crate::keys::nonstd::DollarWrap;
use crate::keywords::{Version, VersionMembership};

use nonempty::{
    DisplayableNE as _, NEAlt, NEConcat, NEConcat3, NEConcat4, NESlice, NEStr, NEString,
    ToDisplayNE, ToNE, ne_str,
};

use bytemuck::{NoUninit, must_cast_ref};
use derive_more::{Display, From, TryInto};
use derive_new::new;
use strum::{EnumCount, VariantArray};
use strum_macros::{EnumCount as EnumCount_, VariantArray};
use thiserror::Error;

use std::iter;
use std::marker::PhantomData;
use std::ops;
use std::slice::Iter;

#[cfg(feature = "python")]
use {crate::python as py, fireflow_core_proc::DisplayAsPyErr};

/// A key defined in the FCS standard.
///
/// The '$' is not included or implied by any string conversions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, TryInto)]
#[display("{}", self.as_displayable())]
pub enum RawStdKey {
    Root(RootKey),
    Meas(MeasKey),
    Gate(GateKey),
    Region(RegionKey),
    CsvFlag(CsvFlagKey),
    Dfc(DfcKey),
}

/// An FCS key which does not use any indices.
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

/// A $Pn* key.
pub type MeasKey = IndexedKey<N_MEAS, MeasIndex, MeasKeyId>;

/// A $Gn* key.
pub type GateKey = IndexedKey<N_GATE, GateIndex, GateKeyId>;

/// A $Rn* key.
pub type RegionKey = IndexedKey<N_REGION, RegionIndex, RegionKeyId>;

/// An FCS keyword with an index.
#[derive(new, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IndexedKey<const LEN: usize, I, K> {
    pub index: I,
    pub id: K,
}

/// A $CSVnFLAG key.
#[derive(new, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CsvFlagKey {
    pub index: SubsetIndex,
}

/// A $DFCmTOn key.
#[derive(new, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DfcKey {
    pub index: BiMeasIndex,
}

/// A marker type representing the $CSVnFLAG key.
pub struct CsvFlagKeyMarker;

/// A marker type representing the $DFCmTOn key.
pub struct DfcKeyMarker;

/// An identifier corresponding to a $Pn* keyword.
///
/// Note that these can either be prefixes or suffixes depending on the key.
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

/// An identifier corresponding to a $Pn* keyword (sans peaks)
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum NonPeakMeasKeyId {
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
}

/// An identifier corresponding to a peak $Pn* keyword
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum PeakMeasKeyId {
    Pk,
    Pkn,
}

/// An identifier corresponding to a $Gn* keyword.
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

/// An identifier corresponding to a $Rn* keyword.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, EnumCount_, VariantArray, NoUninit,
)]
#[repr(usize)]
pub enum RegionKeyId {
    I,
    W,
}

#[derive(PartialEq, Debug, Error, Clone)]
#[error("could not make standard key from string, got {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeyError))]
pub struct KeyNotStdError(NEString);

/// Iterator for enums which map to numbers starting at 0
pub type NumericEnumIter<T> = iter::Copied<Iter<'static, T>>;

/// Index generator for root keys.
pub type RootKeyGenerator = NumericEnumIter<RootKey>;

/// Index generator for indexed keys.
pub struct IndexedKeyGenerator<const LEN: usize, I, K: 'static> {
    key_orig: NumericEnumIter<K>,
    key: NumericEnumIter<K>,
    index: usize,
    _index: PhantomData<I>,
}

/// Index generator for $CSVnFLAG keys.
pub type CsvFlagGenerator = iter::Map<ops::RangeFrom<usize>, fn(usize) -> CsvFlagKey>;

/// Index generator for $DFCmTOn keys.
pub type DfcKeyGenerator = iter::Map<
    iter::Zip<
        iter::Zip<iter::Cycle<ops::Range<usize>>, ops::RangeFrom<usize>>,
        iter::Repeat<usize>,
    >,
    fn(((usize, usize), usize)) -> DfcKey,
>;

/// The number of root keys.
pub const N_ROOT: usize = 54;

/// The number of $Pn* keys (suffixes or prefixes).
pub const N_MEAS: usize = 22;

/// The number of $Gn* keys (suffixes)
pub const N_GATE: usize = 8;

/// The number of $Rn* keys (suffixes)
pub const N_REGION: usize = 2;

/// The unindexed name for the $PKn key
pub const PKN: &NEStr = ne_str!("$PKn");

/// The unindexed name for the $PKNn key
pub const PKNN: &NEStr = ne_str!("$PKNn");

/// The prefix for the $PKn key.
pub const PK_KW_PREFIX: &NEStr = ne_str!("PK");

/// The prefix for the $PKNn key.
pub const PKN_KW_PREFIX: &NEStr = ne_str!("PKN");

/// The unindexed name for the $RnI key
pub const RNI: &NEStr = ne_str!("$RnI");

/// The unindexed name for the $RnW key
pub const RNW: &NEStr = ne_str!("$RnW");

/// The suffix for the $RnI key.
pub const REGION_I_KW_SUFFIX: &NEStr = ne_str!("I");

/// The suffix for the $RnW key.
pub const REGION_W_KW_SUFFIX: &NEStr = ne_str!("W");

// Include list of all keyword names/suffixes defined in build script
include!(concat!(env!("OUT_DIR"), "/kw_strs.rs"));

// Implement numeric mapping for key id enums

/// An enum which exactly maps to a sequence of numbers starting at 0.
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
pub trait NumericEnum<const LEN: usize>: VariantArray + EnumCount + NoUninit {
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

    fn iter() -> NumericEnumIter<Self> {
        Self::iter_ref().copied()
    }

    fn index(&self) -> usize {
        *must_cast_ref(self)
    }
}

impl NumericEnum<N_ROOT> for RootKey {}
impl NumericEnum<N_MEAS> for MeasKeyId {}
impl NumericEnum<N_GATE> for GateKeyId {}
impl NumericEnum<N_REGION> for RegionKeyId {}

// Implement enum index properties for key types

/// An enum which can be used to index into an array.
pub trait EnumIndex {
    /// Additional data used to describe the bounds of the index.
    ///
    /// This is currently only used for $DFCmTOn keys since these have two
    /// dimensions; this holds the length of each dimension (they are the same).
    type SubDimension;

    /// Type to generate the full sequence of offsets for array indexing.
    ///
    /// This may be infinite depending on [`Self`].
    type Generator: Iterator<Item = Self>;

    fn generate(sub: &Self::SubDimension) -> Self::Generator;

    fn offset(&self, sub: &Self::SubDimension) -> usize;

    fn offset0(&self) -> usize
    where
        Self: EnumIndex<SubDimension = ()>,
    {
        self.offset(&())
    }
}

impl EnumIndex for RootKey {
    type SubDimension = ();
    type Generator = RootKeyGenerator;

    fn generate((): &Self::SubDimension) -> Self::Generator {
        Self::iter()
    }

    fn offset(&self, (): &Self::SubDimension) -> usize {
        self.index()
    }
}

impl<const LEN: usize, I, K> Iterator for IndexedKeyGenerator<LEN, I, K>
where
    usize: Into<I>,
    K: Copy + NumericEnum<LEN>,
{
    type Item = IndexedKey<LEN, I, K>;

    fn next(&mut self) -> Option<Self::Item> {
        let k = if let Some(k) = self.key.next() {
            k
        } else {
            self.index += 1;
            self.key = self.key_orig.clone();
            self.key.next().unwrap()
        };
        Some(IndexedKey::new(self.index.into(), k))
    }
}

impl<const LEN: usize, I, K> EnumIndex for IndexedKey<LEN, I, K>
where
    K: NumericEnum<LEN>,
    I: From<usize> + Into<usize> + Copy,
{
    type SubDimension = ();
    type Generator = IndexedKeyGenerator<LEN, I, K>;

    fn generate((): &Self::SubDimension) -> Self::Generator {
        IndexedKeyGenerator {
            key_orig: K::iter(),
            key: K::iter(),
            index: 0,
            _index: PhantomData,
        }
    }

    fn offset(&self, (): &Self::SubDimension) -> usize {
        self.index.into() * K::COUNT + self.id.index()
    }
}

impl EnumIndex for CsvFlagKey {
    type SubDimension = ();
    type Generator = CsvFlagGenerator;

    fn generate((): &Self::SubDimension) -> Self::Generator {
        (0_usize..).map(|i| Self::new(i.into()))
    }

    fn offset(&self, (): &Self::SubDimension) -> usize {
        self.index.into()
    }
}

impl EnumIndex for DfcKey {
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

// Implement key id -> std key mappings

/// Convert a key identifier to a standard key (with index as necessary).
pub trait ToStd {
    type Index;

    fn to_raw_std(&self, index: &Self::Index) -> RawStdKey;

    fn to_raw_std0(&self) -> RawStdKey
    where
        Self: ToStd<Index = ()>,
    {
        self.to_raw_std(&())
    }

    fn to_std<const HAS_PRE: bool>(&self, index: &Self::Index) -> DollarStdKey<HAS_PRE> {
        DollarWrap(self.to_raw_std(index))
    }

    fn to_std0<const HAS_PRE: bool>(&self) -> DollarStdKey<HAS_PRE>
    where
        Self: ToStd<Index = ()>,
    {
        DollarWrap(self.to_raw_std0())
    }
}

impl ToStd for RootKey {
    type Index = ();

    fn to_raw_std(&self, (): &Self::Index) -> RawStdKey {
        (*self).into()
    }
}

impl ToStd for MeasKeyId {
    type Index = MeasIndex;

    fn to_raw_std(&self, index: &Self::Index) -> RawStdKey {
        IndexedKey::new(*index, *self).into()
    }
}

impl ToStd for GateKeyId {
    type Index = GateIndex;

    fn to_raw_std(&self, index: &Self::Index) -> RawStdKey {
        IndexedKey::new(*index, *self).into()
    }
}

impl ToStd for RegionKeyId {
    type Index = RegionIndex;

    fn to_raw_std(&self, index: &Self::Index) -> RawStdKey {
        IndexedKey::new(*index, *self).into()
    }
}

impl ToStd for CsvFlagKeyMarker {
    type Index = SubsetIndex;

    fn to_raw_std(&self, index: &Self::Index) -> RawStdKey {
        CsvFlagKey::new(*index).into()
    }
}

impl ToStd for DfcKeyMarker {
    type Index = BiMeasIndex;

    fn to_raw_std(&self, index: &Self::Index) -> RawStdKey {
        DfcKey::new(*index).into()
    }
}

// Implement non-empty display for std key types.

type NEStdKey = NEAlt<
    NEAlt<ToNE<RootKey>, NEAlt<ToNE<MeasKey>, ToNE<GateKey>>>,
    NEAlt<ToNE<RegionKey>, NEAlt<ToNE<CsvFlagKey>, ToNE<DfcKey>>>,
>;

impl<'a> ToDisplayNE<'a> for RawStdKey {
    type NE = NEStdKey;
    fn to_ne(&'a self) -> Self::NE {
        match self {
            Self::Root(x) => NEAlt::Left(NEAlt::Left(ToNE(*x))),
            Self::Meas(x) => NEAlt::Left(NEAlt::Right(NEAlt::Left(ToNE(*x)))),
            Self::Gate(x) => NEAlt::Left(NEAlt::Right(NEAlt::Right(ToNE(*x)))),
            Self::Region(x) => NEAlt::Right(NEAlt::Left(ToNE(*x))),
            Self::CsvFlag(x) => NEAlt::Right(NEAlt::Right(NEAlt::Left(ToNE(*x)))),
            Self::Dfc(x) => NEAlt::Right(NEAlt::Right(NEAlt::Right(ToNE(*x)))),
        }
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

// Implement misc methods on std key types.

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

impl RawStdKey {
    pub(crate) fn from_ne_str(s: &NEStr) -> Option<Self> {
        Self::from_bytes(s.as_ne_bytes())
    }

    pub(crate) fn from_bytes(bytes: &NESlice<u8>) -> Option<Self> {
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
                        Some(Self::Meas(k))
                    } else if let Some((i, rest)) = split_index_and_suffix(bs1)
                        && rest.is_empty()
                    {
                        // $PKn
                        let k = MeasKey::new(i.into(), MeasKeyId::Pk);
                        Some(Self::Meas(k))
                    } else {
                        // something else
                        Self::from_bytes_nonparam(bytes)
                    }
                } else if let Some((i, rest)) = split_index_and_suffix(bs)
                    && let Some(mid) = NonPeakMeasKeyId::from_bytes(rest)
                {
                    // $Pn*
                    let k = MeasKey::new(i.into(), mid.into());
                    Some(Self::Meas(k))
                } else {
                    // something else
                    Self::from_bytes_nonparam(bytes)
                }
            }
            // Try to match $Gn*
            b'G' => {
                if let Some((i, rest)) = split_index_and_suffix(bs)
                    && let Some(gid) = GateKeyId::from_bytes(rest)
                {
                    let k = GateKey::new(i.into(), gid);
                    Some(Self::Gate(k))
                } else {
                    Self::from_bytes_nonparam(bytes)
                }
            }
            // Try to match $Rn*
            b'R' => {
                if let Some((i, rest)) = split_index_and_suffix(bs)
                    && let Some(rid) = RegionKeyId::from_bytes(rest)
                {
                    let k = RegionKey::new(i.into(), rid);
                    Some(Self::Region(k))
                } else {
                    Self::from_bytes_nonparam(bytes)
                }
            }
            // We didn't find any of these prefixes, try all the other keywords.
            _ => Self::from_bytes_nonparam(bytes),
        }
    }

    fn from_bytes_nonparam(bytes: &NESlice<u8>) -> Option<Self> {
        if let Some(rk) = RootKey::from_bytes(bytes.as_ref()) {
            Some(Self::Root(rk))
        } else if let Some(csv) = CsvFlagKey::from_bytes(bytes.as_ref()) {
            Some(Self::CsvFlag(csv))
        } else {
            DfcKey::from_bytes(bytes.as_ref()).map(Self::Dfc)
        }
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

impl TryFrom<NEString> for RawStdKey {
    type Error = KeyNotStdError;
    fn try_from(value: NEString) -> Result<Self, Self::Error> {
        Self::from_ne_str(value.as_ne_str()).ok_or(KeyNotStdError(value))
    }
}

impl TryFrom<&NEStr> for RawStdKey {
    type Error = KeyNotStdError;
    fn try_from(value: &NEStr) -> Result<Self, Self::Error> {
        Self::from_ne_str(value).ok_or_else(|| KeyNotStdError(value.to_owned()))
    }
}

impl RootKey {
    #[must_use]
    pub const fn as_ne_str(&self) -> &'static NEStr {
        match self {
            Self::Byteord => BYTEORD_KW,
            Self::Datatype => DATATYPE_KW,
            Self::Mode => MODE_KW,
            Self::Par => PAR_KW,
            Self::Tot => TOT_KW,
            Self::Cyt => CYT_KW,
            Self::Abrt => ABRT_KW,
            Self::Cells => CELLS_KW,
            Self::Com => COM_KW,
            Self::Exp => EXP_KW,
            Self::Fil => FIL_KW,
            Self::Inst => INST_KW,
            Self::Lost => LOST_KW,
            Self::Op => OP_KW,
            Self::Proj => PROJ_KW,
            Self::Smno => SMNO_KW,
            Self::Src => SRC_KW,
            Self::Sys => SYS_KW,
            Self::Tr => TR_KW,
            Self::Cytsn => CYTSN_KW,
            Self::Timestep => TIMESTEP_KW,
            Self::Vol => VOL_KW,
            Self::Unicode => UNICODE_KW,
            Self::Flowrate => FLOWRATE_KW,
            Self::Begindata => BEGINDATA_KW,
            Self::Beginanalysis => BEGINANALYSIS_KW,
            Self::Beginstext => BEGINSTEXT_KW,
            Self::Enddata => ENDDATA_KW,
            Self::Endanalysis => ENDANALYSIS_KW,
            Self::Endstext => ENDSTEXT_KW,
            Self::Nextdata => NEXTDATA_KW,
            Self::Btim => BTIM_KW,
            Self::Etim => ETIM_KW,
            Self::Date => DATE_KW,
            Self::Begindatetime => BEGINDATETIME_KW,
            Self::Enddatetime => ENDDATETIME_KW,
            Self::Comp => COMP_KW,
            Self::Spillover => SPILLOVER_KW,
            Self::LastModified => LAST_MODIFIED_KW,
            Self::LastModifier => LAST_MODIFIER_KW,
            Self::Originality => ORIGINALITY_KW,
            Self::Plateid => PLATEID_KW,
            Self::Platename => PLATENAME_KW,
            Self::Wellid => WELLID_KW,
            Self::UnstainedCenters => UNSTAINEDCENTERS_KW,
            Self::UnstainedInfo => UNSTAINEDINFO_KW,
            Self::CarrierId => CARRIERID_KW,
            Self::CarrierType => CARRIERTYPE_KW,
            Self::LocationId => LOCATIONID_KW,
            Self::Csmode => CSMODE_KW,
            Self::Csvbits => CSVBITS_KW,
            Self::Cstot => CSTOT_KW,
            Self::Gating => GATING_KW,
            Self::Gate => GATE_KW,
        }
    }

    const fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match bytes.len() {
            2 => match_bytes!(
                bytes,
                OP_KW => Self::Op,
                TR_KW => Self::Tr
            ),
            3 => match_bytes!(
                bytes,
                COM_KW => Self::Com,
                CYT_KW => Self::Cyt,
                EXP_KW => Self::Exp,
                FIL_KW => Self::Fil,
                PAR_KW => Self::Par,
                TOT_KW => Self::Tot,
                SRC_KW => Self::Src,
                SYS_KW => Self::Sys,
                VOL_KW => Self::Vol
            ),
            4 => match_bytes!(
                bytes,
                ABRT_KW => Self::Abrt,
                BTIM_KW => Self::Btim,
                COMP_KW => Self::Comp,
                DATE_KW => Self::Date,
                ETIM_KW => Self::Etim,
                GATE_KW => Self::Gate,
                INST_KW => Self::Inst,
                LOST_KW => Self::Lost,
                MODE_KW => Self::Mode,
                PROJ_KW => Self::Proj,
                SMNO_KW => Self::Smno
            ),
            5 => match_bytes!(
                bytes,
                CELLS_KW => Self::Cells,
                CYTSN_KW => Self::Cytsn,
                CSTOT_KW => Self::Cstot
            ),
            6 => match_bytes!(
                bytes,
                CSMODE_KW => Self::Csmode,
                GATING_KW => Self::Gating,
                WELLID_KW => Self::Wellid
            ),
            7 => match_bytes!(
                bytes,
                BYTEORD_KW => Self::Byteord,
                CSVBITS_KW => Self::Csvbits,
                ENDDATA_KW => Self::Enddata,
                PLATEID_KW => Self::Plateid,
                UNICODE_KW => Self::Unicode
            ),
            8 => match_bytes!(
                bytes,
                DATATYPE_KW => Self::Datatype,
                ENDSTEXT_KW => Self::Endstext,
                FLOWRATE_KW => Self::Flowrate,
                NEXTDATA_KW => Self::Nextdata,
                TIMESTEP_KW => Self::Timestep
            ),
            9 => match_bytes!(
                bytes,
                BEGINDATA_KW => Self::Begindata,
                CARRIERID_KW => Self::CarrierId,
                PLATENAME_KW => Self::Platename,
                SPILLOVER_KW => Self::Spillover
            ),
            10 => match_bytes!(
                bytes,
                BEGINSTEXT_KW => Self::Beginstext,
                LOCATIONID_KW => Self::LocationId
            ),
            11 => match_bytes!(
                bytes,
                CARRIERTYPE_KW => Self::CarrierType,
                ENDANALYSIS_KW => Self::Endanalysis,
                ENDDATETIME_KW => Self::Enddatetime,
                ORIGINALITY_KW => Self::Originality
            ),
            13 => match_bytes!(
                bytes,
                BEGINANALYSIS_KW => Self::Beginanalysis,
                BEGINDATETIME_KW => Self::Begindatetime,
                LAST_MODIFIED_KW => Self::LastModified,
                LAST_MODIFIER_KW => Self::LastModifier,
                UNSTAINEDINFO_KW => Self::UnstainedInfo
            ),
            _ => match_bytes!(bytes, UNSTAINEDCENTERS_KW => Self::UnstainedCenters),
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

pub(crate) enum PrefixOrSuffix {
    Prefix(&'static NEStr),
    Suffix(&'static NEStr),
}

impl MeasKeyId {
    const fn prefix_or_suffix(self) -> PrefixOrSuffix {
        match self.split_peak() {
            Ok(x) => PrefixOrSuffix::Suffix(x.suffix()),
            Err(x) => PrefixOrSuffix::Prefix(x.prefix()),
        }
    }

    pub(crate) const fn split_peak(self) -> Result<NonPeakMeasKeyId, PeakMeasKeyId> {
        match self {
            Self::N => Ok(NonPeakMeasKeyId::N),
            Self::R => Ok(NonPeakMeasKeyId::R),
            Self::E => Ok(NonPeakMeasKeyId::E),
            Self::S => Ok(NonPeakMeasKeyId::S),
            Self::F => Ok(NonPeakMeasKeyId::F),
            Self::T => Ok(NonPeakMeasKeyId::T),
            Self::P => Ok(NonPeakMeasKeyId::P),
            Self::V => Ok(NonPeakMeasKeyId::V),
            Self::B => Ok(NonPeakMeasKeyId::B),
            Self::L => Ok(NonPeakMeasKeyId::L),
            Self::O => Ok(NonPeakMeasKeyId::O),
            Self::G => Ok(NonPeakMeasKeyId::G),
            Self::D => Ok(NonPeakMeasKeyId::D),
            Self::Det => Ok(NonPeakMeasKeyId::Det),
            Self::Tag => Ok(NonPeakMeasKeyId::Tag),
            Self::Type => Ok(NonPeakMeasKeyId::Type),
            Self::Feature => Ok(NonPeakMeasKeyId::Feature),
            Self::Analyte => Ok(NonPeakMeasKeyId::Analyte),
            Self::Datatype => Ok(NonPeakMeasKeyId::Datatype),
            Self::Calibration => Ok(NonPeakMeasKeyId::Calibration),
            Self::Pk => Err(PeakMeasKeyId::Pk),
            Self::Pkn => Err(PeakMeasKeyId::Pkn),
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

impl From<NonPeakMeasKeyId> for MeasKeyId {
    fn from(value: NonPeakMeasKeyId) -> Self {
        match value {
            NonPeakMeasKeyId::N => Self::N,
            NonPeakMeasKeyId::R => Self::R,
            NonPeakMeasKeyId::E => Self::E,
            NonPeakMeasKeyId::S => Self::S,
            NonPeakMeasKeyId::F => Self::F,
            NonPeakMeasKeyId::T => Self::T,
            NonPeakMeasKeyId::P => Self::P,
            NonPeakMeasKeyId::V => Self::V,
            NonPeakMeasKeyId::B => Self::B,
            NonPeakMeasKeyId::L => Self::L,
            NonPeakMeasKeyId::O => Self::O,
            NonPeakMeasKeyId::G => Self::G,
            NonPeakMeasKeyId::D => Self::D,
            NonPeakMeasKeyId::Det => Self::Det,
            NonPeakMeasKeyId::Tag => Self::Tag,
            NonPeakMeasKeyId::Type => Self::Type,
            NonPeakMeasKeyId::Feature => Self::Feature,
            NonPeakMeasKeyId::Analyte => Self::Analyte,
            NonPeakMeasKeyId::Datatype => Self::Datatype,
            NonPeakMeasKeyId::Calibration => Self::Calibration,
        }
    }
}

impl From<PeakMeasKeyId> for MeasKeyId {
    fn from(value: PeakMeasKeyId) -> Self {
        match value {
            PeakMeasKeyId::Pk => Self::Pk,
            PeakMeasKeyId::Pkn => Self::Pkn,
        }
    }
}

impl NonPeakMeasKeyId {
    pub(crate) const fn suffix(self) -> &'static NEStr {
        match self {
            Self::N => N_KW_SUFFIX,
            Self::R => R_KW_SUFFIX,
            Self::E => E_KW_SUFFIX,
            Self::S => S_KW_SUFFIX,
            Self::F => F_KW_SUFFIX,
            Self::T => T_KW_SUFFIX,
            Self::P => P_KW_SUFFIX,
            Self::V => V_KW_SUFFIX,
            Self::B => B_KW_SUFFIX,
            Self::L => L_KW_SUFFIX,
            Self::O => O_KW_SUFFIX,
            Self::G => G_KW_SUFFIX,
            Self::D => D_KW_SUFFIX,
            Self::Det => DET_KW_SUFFIX,
            Self::Tag => TAG_KW_SUFFIX,
            Self::Type => TYPE_KW_SUFFIX,
            Self::Feature => FEATURE_KW_SUFFIX,
            Self::Analyte => ANALYTE_KW_SUFFIX,
            Self::Datatype => DATATYPE_KW_SUFFIX,
            Self::Calibration => CALIBRATION_KW_SUFFIX,
        }
    }

    pub(crate) const fn from_bytes(bytes: &[u8]) -> Option<Self> {
        match bytes.len() {
            1 => {
                match_bytes!(
                    bytes,
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
                bytes,
                DET_KW_SUFFIX => Self::Det,
                TAG_KW_SUFFIX => Self::Tag
            ),
            7 => match_bytes!(
                bytes,
                FEATURE_KW_SUFFIX => Self::Feature,
                ANALYTE_KW_SUFFIX => Self::Analyte
            ),
            _ => match_bytes!(
                bytes,
                TYPE_KW_SUFFIX => Self::Type,
                DATATYPE_KW_SUFFIX => Self::Datatype,
                CALIBRATION_KW_SUFFIX => Self::Calibration
            ),
        }
    }
}

impl PeakMeasKeyId {
    pub(crate) const fn prefix(self) -> &'static NEStr {
        match self {
            Self::Pk => PK_KW_PREFIX,
            Self::Pkn => PKN_KW_PREFIX,
        }
    }
}

impl GateKeyId {
    pub(crate) const fn suffix(self) -> &'static NEStr {
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

    pub(crate) const fn from_bytes(bs: &[u8]) -> Option<Self> {
        if bs.len() == 1 {
            match_bytes!(
                bs,
                N_KW_SUFFIX => Self::N,
                R_KW_SUFFIX => Self::R,
                E_KW_SUFFIX => Self::E,
                S_KW_SUFFIX => Self::S,
                F_KW_SUFFIX => Self::F,
                T_KW_SUFFIX => Self::T,
                P_KW_SUFFIX => Self::P,
                V_KW_SUFFIX => Self::V
            )
        } else {
            None
        }
    }
}

impl RegionKeyId {
    pub(crate) const fn suffix(self) -> &'static NEStr {
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

    pub(crate) const fn from_bytes(bs: &[u8]) -> Option<Self> {
        if bs.len() == 1 {
            match_bytes!(
                bs,
                REGION_I_KW_SUFFIX => Self::I,
                REGION_W_KW_SUFFIX => Self::W
            )
        } else {
            None
        }
    }

    const fn membership() -> VersionMembership {
        VersionMembership::All
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
        if bytes.len() >= 8
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

// Implement blank representations for keywords.

/// A key which has a blank string representation without an index.
///
/// Example: '$PnN'
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

// Random local functions

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

#[cfg(test)]
mod test {
    use super::*;

    use proptest::prelude::*;

    // All these tests are more or less the same: make a "random" string of
    // case insensitive values that match the pattern for a given keyword and
    // test that it parses to the correct enum.

    macro_rules! test_root_key {
        ($name:ident, $pat:expr, $var:ident) => {
            proptest! {
                #[test]
                fn $name(s in $pat) {
                    let ne = NEStr::try_new(s.as_str()).unwrap();
                    assert_eq!(RawStdKey::from_ne_str(ne), Some(RootKey::$var.into()));
                }
            }
        };
    }

    test_root_key!(byteord, "(?i-u)byteord", Byteord);
    test_root_key!(datatype, "(?i-u)datatype", Datatype);
    test_root_key!(mode, "(?i-u)mode", Mode);
    test_root_key!(par, "(?i-u)par", Par);
    test_root_key!(tot, "(?i-u)tot", Tot);
    test_root_key!(cyt, "(?i-u)cyt", Cyt);
    test_root_key!(abrt, "(?i-u)abrt", Abrt);
    test_root_key!(cells, "(?i-u)cells", Cells);
    test_root_key!(com, "(?i-u)com", Com);
    test_root_key!(exp, "(?i-u)exp", Exp);
    test_root_key!(fil, "(?i-u)fil", Fil);
    test_root_key!(inst, "(?i-u)inst", Inst);
    test_root_key!(lost, "(?i-u)lost", Lost);
    test_root_key!(op, "(?i-u)op", Op);
    test_root_key!(proj, "(?i-u)proj", Proj);
    test_root_key!(smno, "(?i-u)smno", Smno);
    test_root_key!(src, "(?i-u)src", Src);
    test_root_key!(sys, "(?i-u)sys", Sys);
    test_root_key!(tr, "(?i-u)tr", Tr);
    test_root_key!(cytsn, "(?i-u)cytsn", Cytsn);
    test_root_key!(timestep, "(?i-u)timestep", Timestep);
    test_root_key!(vol, "(?i-u)vol", Vol);
    test_root_key!(unicode, "(?i-u)unicode", Unicode);
    test_root_key!(flowrate, "(?i-u)flowrate", Flowrate);
    test_root_key!(begindata, "(?i-u)begindata", Begindata);
    test_root_key!(beginanalysis, "(?i-u)beginanalysis", Beginanalysis);
    test_root_key!(beginstext, "(?i-u)beginstext", Beginstext);
    test_root_key!(enddata, "(?i-u)enddata", Enddata);
    test_root_key!(endanalysis, "(?i-u)endanalysis", Endanalysis);
    test_root_key!(endstext, "(?i-u)endstext", Endstext);
    test_root_key!(nextdata, "(?i-u)nextdata", Nextdata);
    test_root_key!(btim, "(?i-u)btim", Btim);
    test_root_key!(etim, "(?i-u)etim", Etim);
    test_root_key!(date, "(?i-u)date", Date);
    test_root_key!(begindatetime, "(?i-u)begindatetime", Begindatetime);
    test_root_key!(enddatetime, "(?i-u)enddatetime", Enddatetime);
    test_root_key!(comp, "(?i-u)comp", Comp);
    test_root_key!(spillover, "(?i-u)spillover", Spillover);
    test_root_key!(lastmodified, "(?i-u)last_modified", LastModified);
    test_root_key!(lastmodifier, "(?i-u)last_modifier", LastModifier);
    test_root_key!(originality, "(?i-u)originality", Originality);
    test_root_key!(plateid, "(?i-u)plateid", Plateid);
    test_root_key!(platename, "(?i-u)platename", Platename);
    test_root_key!(wellid, "(?i-u)wellid", Wellid);
    test_root_key!(unstainedcenters, "(?i-u)unstainedcenters", UnstainedCenters);
    test_root_key!(unstainedinfo, "(?i-u)unstainedinfo", UnstainedInfo);
    test_root_key!(carrierid, "(?i-u)carrierid", CarrierId);
    test_root_key!(carriertype, "(?i-u)carriertype", CarrierType);
    test_root_key!(locationid, "(?i-u)locationid", LocationId);
    test_root_key!(csmode, "(?i-u)csmode", Csmode);
    test_root_key!(csvbits, "(?i-u)csvbits", Csvbits);
    test_root_key!(cstot, "(?i-u)cstot", Cstot);
    test_root_key!(gating, "(?i-u)gating", Gating);
    test_root_key!(gate, "(?i-u)gate", Gate);

    macro_rules! test_meas_key {
        ($name:ident, $pat:expr, $var:ident) => {
            proptest! {
                #[test]
                fn $name(s0 in "(?i)p", i in 0_usize..1000, s1 in $pat) {
                    let s = format!("{s0}{}{s1}", i + 1);
                    let ne = NEStr::try_new(s.as_str()).unwrap();
                    let k = MeasKey::new(i.into(), MeasKeyId::$var);
                    assert_eq!(RawStdKey::from_ne_str(ne), Some(k.into()));
                }
            }
        };
    }

    test_meas_key!(pnn, "(?i-u)n", N);
    test_meas_key!(pnr, "(?i-u)r", R);
    test_meas_key!(pne, "(?i-u)e", E);
    test_meas_key!(pns, "(?i-u)s", S);
    test_meas_key!(pnf, "(?i-u)f", F);
    test_meas_key!(pnt, "(?i-u)t", T);
    test_meas_key!(pnp, "(?i-u)p", P);
    test_meas_key!(pnv, "(?i-u)v", V);
    test_meas_key!(pnb, "(?i-u)b", B);
    test_meas_key!(pnl, "(?i-u)l", L);
    test_meas_key!(pno, "(?i-u)o", O);
    test_meas_key!(png, "(?i-u)g", G);
    test_meas_key!(pnd, "(?i-u)d", D);
    test_meas_key!(pndet, "(?i-u)det", Det);
    test_meas_key!(pntag, "(?i-u)tag", Tag);
    test_meas_key!(pntype, "(?i-u)type", Type);
    test_meas_key!(pnfeature, "(?i-u)feature", Feature);
    test_meas_key!(pnanalyte, "(?i-u)analyte", Analyte);
    test_meas_key!(pndatatype, "(?i-u)datatype", Datatype);
    test_meas_key!(pncalibration, "(?i-u)calibration", Calibration);

    macro_rules! test_gate_key {
        ($name:ident, $pat:expr, $var:ident) => {
            proptest! {
                #[test]
                fn $name(s0 in "(?i)g", i in 0_usize..1000, s1 in $pat) {
                    let s = format!("{s0}{}{s1}", i + 1);
                    let ne = NEStr::try_new(s.as_str()).unwrap();
                    let k = GateKey::new(i.into(), GateKeyId::$var);
                    assert_eq!(RawStdKey::from_ne_str(ne), Some(k.into()));
                }
            }
        };
    }

    test_gate_key!(gnn, "(?i-u)n", N);
    test_gate_key!(gnr, "(?i-u)r", R);
    test_gate_key!(gne, "(?i-u)e", E);
    test_gate_key!(gns, "(?i-u)s", S);
    test_gate_key!(gnf, "(?i-u)f", F);
    test_gate_key!(gnt, "(?i-u)t", T);
    test_gate_key!(gnp, "(?i-u)p", P);
    test_gate_key!(gnv, "(?i-u)v", V);

    proptest! {
        #[test]
        fn rni(s0 in "(?i)r", i in 0_usize..1000, s1 in "(?i-u)i") {
            let s = format!("{s0}{}{s1}", i + 1);
            let ne = NEStr::try_new(s.as_str()).unwrap();
            let k = RegionKey::new(i.into(), RegionKeyId::I);
            assert_eq!(RawStdKey::from_ne_str(ne), Some(k.into()));
        }
    }

    proptest! {
        #[test]
        fn rnw(s0 in "(?i)r", i in 0_usize..1000, s1 in "(?i-u)w") {
            let s = format!("{s0}{}{s1}", i + 1);
            let ne = NEStr::try_new(s.as_str()).unwrap();
            let k = RegionKey::new(i.into(), RegionKeyId::W);
            assert_eq!(RawStdKey::from_ne_str(ne), Some(k.into()));
        }
    }

    proptest! {
        #[test]
        fn csvflag(s0 in "(?i-u)csv", i in 0_usize..1000, s1 in "(?i-u)flag") {
            let s = format!("{s0}{}{s1}", i + 1);
            let ne = NEStr::try_new(s.as_str()).unwrap();
            let k = CsvFlagKey::new(i.into());
            assert_eq!(RawStdKey::from_ne_str(ne), Some(k.into()));
        }
    }

    proptest! {
        #[test]
        fn dfc(s0 in "(?i-u)dfc", i in 0_usize..1000, s1 in "(?i)to", j in 0_usize..1000) {
            let s = format!("{s0}{}{s1}{}", i + 1, j + 1);
            let ne = NEStr::try_new(s.as_str()).unwrap();
            let k = DfcKey::new(BiMeasIndex::new(i.into(), j.into()));
            assert_eq!(RawStdKey::from_ne_str(ne), Some(k.into()));
        }
    }
}
