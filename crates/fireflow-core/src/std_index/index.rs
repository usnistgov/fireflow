use crate::config::{EvaledReadRepairKeywordsConfig, EvaledReadStdKeywordsConfig};
use crate::logging::{DeferredWarningsAndErrors, LogResult, WarningsAndErrorsResult};
use crate::std_index::masked::{
    LookupMask, LookupStatus, MaskedEnumString, MaskedString, MaskedVariableString, RepairMask,
};
use crate::std_index::nested_string::{NestedEnumString, NestedStringSize, NestedVariableString};
use crate::text::keywords::{Gate, Par};
use crate::validated::keys::{
    NonStdKey, NonStdKeywords, NonStdKeywordsExt as _, PseudoStdKeywords, TruncatedNEString,
    ValueToStdKey,
};

use fireflow_types::case_ins_regex::CaseInsRegex;
use fireflow_types::config::{
    KeywordFailureFlag, OpticalOnlyKey, OpticalOnlyKeys, ProcessOpticalOnlyKeys,
    TemporalHasOpticalKeyError, TriErrorFlag as _,
};
use fireflow_types::index::MeasIndex;
use fireflow_types::keystring::{KeyString, KeyStringOrPattern, KeyStringsOrPatterns};
use fireflow_types::keywords::Version;
use fireflow_types::std_key::{
    CsvFlagKey, DfcKey, DollarPseudoStdKey, DollarStdKey, DollarWrap, EnumIndex as _, GateKey,
    MeasKey, N_ROOT, RegionKey, RootKey, StdKey, ToStd as _,
};
use fireflow_types::sub_pattern::SubPattern;
use nonempty::{NEStr, NEString, NEVec};

use derive_more::{Display, From};
use derive_new::new;
use hashbrown::{HashMap, hash_map::Entry};
use itertools::Itertools as _;
use thiserror::Error;

use std::mem;

#[cfg(feature = "serde")]
use serde::{Serialize, Serializer, ser::SerializeMap as _};

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
    pyo3::prelude::*,
};

type OpticalOnlyResult = WarningsAndErrorsResult<
    Vec<(DollarStdKey, TruncatedNEString)>,
    (),
    TemporalHasOpticalKeyError,
    TemporalHasOpticalKeyError,
>;

pub(crate) type DroppedStdKeywords = Vec<(DollarStdKey, NEString)>;

/// Leftover standard keyword after parsing
#[derive(Clone, new, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct ExtraStdKeywords {
    pub optional: DroppedStdKeywords,
    pub hyper_par: DroppedStdKeywords,
    pub hyper_gate: DroppedStdKeywords,
    pub other_version: DroppedStdKeywords,
    pub timestep: Option<NEString>,
}

/// Error when extra standard keywords are found
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum ExtraStdKeywordError {
    Timestep(TimestepFoundError),
    HyperPar(HyperParError),
    HyperGate(HyperGateError),
    OtherVersion(KeywordOtherVersionError),
    Pseudo(PseudoStdKeyError),
}

/// Error denoting that measurement keyword within standard but above $PAR was found
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error("measurement keyword is part of standard but outside $PAR ({par}): {key}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ExtraKeywordError))]
pub struct HyperParError {
    pub par: Par,
    pub key: DollarStdKey,
}

/// Error denoting that gating keyword within standard but above $GATE was found
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error("gating keyword is part of standard but outside $GATE ({gate}): {key}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ExtraKeywordError))]
pub struct HyperGateError {
    pub gate: Gate,
    pub key: DollarStdKey,
}

/// Error denoting that keyword from different version was found
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error(
    "keyword is not compatible with {current} but is compatible with {os}: {key}",
    os = self.key.0.membership().versions().iter().join(", ")
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ExtraKeywordError))]
pub struct KeywordOtherVersionError {
    pub key: DollarStdKey,
    pub current: Version,
}

/// Error denoting that $TIMESTEP was unused and possibly should have been
#[derive(Debug, Error, PartialEq, Clone)]
#[error("$TIMESTEP found, this may indicate a time measurement exists but was not identified")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ExtraKeywordError))]
pub struct TimestepFoundError;

/// Error when pseudostandard keywords are encountered.
#[derive(new, Debug, Error, PartialEq, Clone)]
#[error("found pseudostandard key '{key}' with value '{value}'")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ExtraKeywordError))]
pub struct PseudoStdKeyError {
    pub key: DollarPseudoStdKey,
    pub value: TruncatedNEString,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum LookupAction {
    None,
    Demote,
    Drop,
}

impl LookupAction {
    pub(crate) fn from_flag<F: KeywordFailureFlag>(flag: F) -> Option<Self> {
        flag.is_demote_or_drop().map(
            |is_demote| {
                if is_demote { Self::Demote } else { Self::Drop }
            },
        )
    }
}

/// Error when keyword repair process resulted in colliding non-unique keys.
#[derive(Debug, Display, Error, PartialEq, Clone, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum RepairError {
    RenameStd(RenameStdNonUniqueError),
    RenamePseudoStd(RenamePseudoStdNonUniqueError),
    PromoteNonUnique(PromoteNonUniqueError),
    AppendNonUnique(AppendNonUniqueError),
    PromotePseudo(PromotePseudoStdError),
}

/// Error when renaming standard keys which are not unique.
#[derive(new, Debug, Error, PartialEq, Clone)]
#[error("standard key {k0} could not be renamed to {k1} because {k1} already exists")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct RenameStdNonUniqueError {
    k0: DollarStdKey,
    k1: DollarStdKey,
}

/// Error when renaming pseudostandard keys which are not unique.
#[derive(new, Debug, Error, PartialEq, Clone)]
#[error("pseudostandard key {k0} could not be renamed to {k1} because {k1} already exists")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct RenamePseudoStdNonUniqueError {
    k0: DollarPseudoStdKey,
    k1: DollarStdKey,
}

/// Error when promoting keys which are not unique.
#[derive(new, Debug, Error, PartialEq, Clone)]
#[error(
    "non-standard key {key} with value {value} could not be promoted because \
     {key} already exists as a standard key."
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct PromoteNonUniqueError {
    key: DollarStdKey,
    value: TruncatedNEString,
}

/// Error when appending keys which are not unique.
#[derive(new, Debug, Error, PartialEq, Clone)]
#[error(
    "standard {key} with value {value} could not be appended because \
     {key} already exists as a standard key."
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct AppendNonUniqueError {
    key: DollarStdKey,
    value: TruncatedNEString,
}

/// Error when promoting nonstandard keyword that is pseudostandard
#[derive(Debug, Error, PartialEq, Clone)]
#[error("could not promote key {0} because it is pseudostandard")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct PromotePseudoStdError(NonStdKey);

/// Diagnostic output from repairing the keyword list.
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[allow(clippy::too_many_arguments)]
pub struct RepairDiagnostics {
    /// Standard keys which were demoted.
    pub demoted: Vec<DollarStdKey>,

    /// Non-standard keys which were promoted.
    pub promoted: Vec<NonStdKey>,

    /// Standard keys which had values that were substituted.
    ///
    /// Values here are the original.
    pub subbed: Vec<(DollarStdKey, TruncatedNEString)>,

    /// Standard keys which had values that were replaced.
    ///
    /// Values here are the original.
    pub replaced: Vec<(DollarStdKey, TruncatedNEString)>,

    /// Standard keys which were renamed.
    ///
    /// First key in pair is the original.
    pub renamed_std: Vec<(DollarStdKey, DollarStdKey)>,

    /// Pseudostandard keys which were renamed.
    ///
    /// First key in pair is the original.
    pub renamed_pseudo_std: Vec<(DollarPseudoStdKey, DollarStdKey)>,

    /// Standard keys not renamed because they collided with an existing key.
    pub renamed_std_non_unique: Vec<(DollarStdKey, DollarStdKey)>,

    /// Pseudostandard keys not renamed because they collided with an existing key.
    pub renamed_pseudo_std_non_unique: Vec<(DollarPseudoStdKey, DollarStdKey)>,

    /// Standard keys which were ignored.
    pub ignored: Vec<(DollarStdKey, TruncatedNEString)>,

    /// Standard keys which were removed.
    ///
    /// This only happens when a substitution pattern returns a blank.
    pub removed: Vec<(DollarStdKey, TruncatedNEString)>,

    /// Non-standard keys which collided with a standard key when promoted.
    ///
    /// These keys were not moved.
    pub promoted_non_unique: Vec<(DollarStdKey, TruncatedNEString)>,

    /// Non-standard keys which are promoted and also demoted as standard keys.
    ///
    /// These keys were not moved.
    pub promoted_demoted_noop: Vec<NonStdKey>,

    /// Non-standard keys which are promoted and also ignored as standard keys.
    ///
    /// These keys were not moved.
    pub promoted_ignored_noop: Vec<NonStdKey>,

    /// Non-standard keys which were promoted but are pseudostandard.
    ///
    /// These keys were not moved.
    pub promoted_pseudo_std: Vec<NonStdKey>,

    /// Appended keys which collided with an existing standard key.
    pub appended_non_unique: Vec<(DollarStdKey, TruncatedNEString)>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct StdKeywords {
    root: NestedRoot,
    // TODO it might make sense to break this up into several sub-strings. As is
    // this will have many empty entries, which will make the index vector very
    // long. In practice, most files will have $PnB, $PnR, $PnE, and $PnN, so
    // this index will be efficiently populated. All the others might be sparse.
    // This could be solved by having a double-index that first indexes on the
    // measurement and returns 'none' if there are no keywords for that index.
    // From there it directs to the real index that points to the strings.
    meas: NestedVariableString<MeasKey, ()>,
    gate: NestedVariableString<GateKey, ()>,
    region: NestedVariableString<RegionKey, ()>,
    csv_flag: NestedVariableString<CsvFlagKey, ()>,
    dfc: NestedVariableString<DfcKey, usize>,
}

pub struct StdTransaction<'a, M> {
    root: MaskedEnumString<'a, N_ROOT, RootKey, M>,
    meas: MaskedVariableString<'a, MeasKey, (), M>,
    gate: MaskedVariableString<'a, GateKey, (), M>,
    region: MaskedVariableString<'a, RegionKey, (), M>,
    csv_flag: MaskedVariableString<'a, CsvFlagKey, (), M>,
    dfc: MaskedVariableString<'a, DfcKey, usize, M>,
}

pub(crate) type StdRepairTx<'a> = StdTransaction<'a, RepairMask>;
pub type StdLookupTx<'a> = StdTransaction<'a, LookupMask>;

type NestedRoot = NestedEnumString<N_ROOT, RootKey>;

impl Default for StdKeywords {
    fn default() -> Self {
        Self {
            root: NestedRoot::init_array(0),
            meas: NestedVariableString::default(),
            gate: NestedVariableString::default(),
            region: NestedVariableString::default(),
            csv_flag: NestedVariableString::default(),
            dfc: NestedVariableString::default(),
        }
    }
}

// TODO sealme in mod

/// A "compiled" object to match keys efficiently.
pub(crate) struct KeyMatcher<'a, T> {
    literal: HashMap<&'a KeyString, &'a T>,
    pattern: Vec<(&'a CaseInsRegex, &'a T)>,
}

impl<'a, T> KeyMatcher<'a, T> {
    pub(crate) fn from_keys(keys: &'a KeyStringsOrPatterns<T>) -> Self {
        keys.0.iter().collect()
    }
}

impl KeyMatcher<'_, ()> {
    fn is_match(&self, other: &KeyString) -> bool {
        self.literal.contains_key(other)
            || self
                .pattern
                .iter()
                .any(|p| p.0.as_ref().is_match(other.as_ref()))
    }
}

impl<T> KeyMatcher<'_, T> {
    fn get(&self, other: &KeyString) -> Option<&T> {
        self.literal.get(other).copied().or(self
            .pattern
            .iter()
            .find(|p| p.0.as_ref().is_match(other.as_ref()))
            .map(|(_, x)| *x))
    }
}

impl<'a, X> FromIterator<(&'a KeyStringOrPattern, &'a X)> for KeyMatcher<'a, X> {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = (&'a KeyStringOrPattern, &'a X)>,
    {
        let (literal, pattern): (HashMap<_, _>, Vec<_>) = iter
            .into_iter()
            .map(|(k, v)| match k {
                KeyStringOrPattern::Literal(l) => Ok((l, v)),
                KeyStringOrPattern::Pattern(p) => Err((p, v)),
            })
            .partition_result();
        Self { literal, pattern }
    }
}

/// All compiled key matchers to prevent repeated allocations in loops
pub(crate) struct AllKeyMatchers<'a> {
    pub(crate) promote: KeyMatcher<'a, ()>,
    pub(crate) demote: KeyMatcher<'a, ()>,
    pub(crate) ignore: KeyMatcher<'a, ()>,
    pub(crate) subs: KeyMatcher<'a, SubPattern>,
}

impl<'a> AllKeyMatchers<'a> {
    pub(crate) fn from_config(conf: &'a EvaledReadRepairKeywordsConfig) -> Self {
        Self {
            promote: KeyMatcher::from_keys(&conf.promote_nonstandard_keys),
            demote: KeyMatcher::from_keys(&conf.demote_standard_keys),
            ignore: KeyMatcher::from_keys(&conf.ignore_standard_keys),
            subs: KeyMatcher::from_keys(&conf.substitute_standard_key_values),
        }
    }
}

impl StdKeywords {
    pub fn iter_dollar_keywords(&self) -> impl Iterator<Item = (DollarStdKey, &NEStr)> {
        self.iter_keywords().map(|(k, v)| (DollarWrap(k), v))
    }

    pub fn iter_keywords(&self) -> impl Iterator<Item = (StdKey, &NEStr)> {
        self.root
            .iter_std()
            .chain(self.meas.iter_std())
            .chain(self.gate.iter_std())
            .chain(self.region.iter_std())
            .chain(self.csv_flag.iter_std())
            .chain(self.dfc.iter_std())
    }

    #[must_use]
    pub fn from_vec<V>(
        mut pairs: Vec<(StdKey, V)>,
    ) -> (Self, Vec<(DollarStdKey, TruncatedNEString)>)
    where
        V: AsRef<NEStr>,
    {
        pairs.sort_by_key(|(k, _)| *k);
        let dedup_split = partition_dedup_by_key(&mut pairs, |(k, _)| *k);
        let (std_final, non_unique_std_) = pairs.split_at(dedup_split);
        let non_unique_std = non_unique_std_
            .iter()
            .map(|(k, v)| (DollarWrap(*k), TruncatedNEString(v.as_ref().to_owned())))
            .collect();

        // SAFETY: we sorted and deduplicated above
        let index = unsafe { Self::from_slice(std_final) };
        (index, non_unique_std)
    }

    #[must_use]
    pub fn get(&self, k: &StdKey) -> Option<&NEStr> {
        let s = match k {
            StdKey::Root(rk) => self.root.get(rk),
            StdKey::Meas(mk) => self.meas.get(mk),
            StdKey::Gate(gk) => self.gate.get(gk),
            StdKey::Region(rk) => self.region.get(rk),
            StdKey::CsvFlag(ck) => self.csv_flag.get(ck),
            StdKey::Dfc(dk) => self.dfc.get(dk),
        }?;
        NEStr::try_new(s)
    }

    pub(crate) fn contains_key(&self, k: &StdKey) -> bool {
        self.get(k).is_some()
    }

    pub(crate) fn n_strings(&self) -> usize {
        self.root.n_strings()
            + self.meas.n_strings()
            + self.gate.n_strings()
            + self.region.n_strings()
            + self.csv_flag.n_strings()
            + self.dfc.n_strings()
    }

    pub(crate) fn concat(self, other: Self) -> (Self, Vec<(DollarStdKey, TruncatedNEString)>) {
        if self.n_strings() == 0 {
            (other, vec![])
        } else if other.n_strings() == 0 {
            (self, vec![])
        } else {
            // TODO this is not optimal, but this will only happen for files
            // that store standard keys in STEXT (of where there are basically
            // none)
            let tmp = self.iter_keywords().chain(other.iter_keywords()).collect();
            Self::from_vec(tmp)
        }
    }

    pub(crate) fn as_transaction<M: Default>(&self) -> StdTransaction<'_, M> {
        StdTransaction {
            root: MaskedString::init_array(&self.root),
            meas: MaskedString::init_var(&self.meas),
            gate: MaskedString::init_var(&self.gate),
            region: MaskedString::init_var(&self.region),
            csv_flag: MaskedString::init_var(&self.csv_flag),
            dfc: MaskedString::init_var(&self.dfc),
        }
    }

    /// Make a new standard key index from a vector of pairs.
    ///
    /// # SAFETY
    ///
    /// Caller must ensure input is sorted and does not have duplicates.
    #[must_use]
    pub unsafe fn from_slice<V>(pairs: &[(StdKey, V)]) -> Self
    where
        V: AsRef<NEStr>,
    {
        let mut root_n_bytes = 0;
        let mut root_n_strings = 0;
        let mut meas_size = NestedStringSize::default();
        let mut gate_size = NestedStringSize::default();
        let mut region_size = NestedStringSize::default();
        let mut csv_flag_size = NestedStringSize::default();
        let mut dfc_n_bytes = 0;
        let mut dfc_matrix_size = 0;

        for (k, v) in pairs {
            let n_bytes = v.as_ref().as_ne_bytes().len().get();
            match k {
                StdKey::Root(_) => {
                    root_n_strings += 1;
                    root_n_bytes += n_bytes;
                }
                StdKey::Meas(_) => {
                    meas_size.n_strings += 1;
                    meas_size.n_bytes += n_bytes;
                }
                StdKey::Gate(_) => {
                    gate_size.n_strings += 1;
                    gate_size.n_bytes += n_bytes;
                }
                StdKey::Region(_) => {
                    region_size.n_strings += 1;
                    region_size.n_bytes += n_bytes;
                }
                StdKey::CsvFlag(_) => {
                    csv_flag_size.n_strings += 1;
                    csv_flag_size.n_bytes += n_bytes;
                }
                StdKey::Dfc(dk) => {
                    dfc_n_bytes += n_bytes;
                    let i0 = usize::from(dk.index.i0);
                    let i1 = usize::from(dk.index.i1);
                    dfc_matrix_size = dfc_matrix_size.max(i0.max(i1));
                }
            }
        }

        let dfc_size = NestedStringSize::new(dfc_n_bytes, dfc_matrix_size * dfc_matrix_size);

        let mut root = NestedRoot::init_array(root_n_bytes);
        let mut meas = NestedVariableString::init_var(&meas_size, ());
        let mut gate = NestedVariableString::init_var(&gate_size, ());
        let mut region = NestedVariableString::init_var(&region_size, ());
        let mut csv_flag = NestedVariableString::init_var(&csv_flag_size, ());
        let mut dfc = NestedVariableString::init_var(&dfc_size, dfc_matrix_size);

        let mut it = pairs.iter();
        let root_it = it
            .by_ref()
            .take(root_n_strings)
            .map(|(k, v)| (RootKey::try_from(*k).unwrap(), v));
        // SAFETY: input is sorted and deduplicated
        unsafe {
            root.set_keys(root_it);
        }

        let meas_it = it
            .by_ref()
            .take(meas_size.n_strings)
            .map(|(k, v)| (MeasKey::try_from(*k).unwrap().offset0(), v));
        // SAFETY: input is sorted and deduplicated
        unsafe {
            meas.extend_pairs(meas_it);
        }

        let gate_it = it
            .by_ref()
            .take(gate_size.n_strings)
            .map(|(k, v)| (GateKey::try_from(*k).unwrap().offset0(), v));
        // SAFETY: input is sorted and deduplicated
        unsafe {
            gate.extend_pairs(gate_it);
        }

        let region_it = it
            .by_ref()
            .take(region_size.n_strings)
            .map(|(k, v)| (RegionKey::try_from(*k).unwrap().offset0(), v));
        // SAFETY: input is sorted and deduplicated
        unsafe {
            region.extend_pairs(region_it);
        }

        let csv_flag_it = it
            .by_ref()
            .take(csv_flag_size.n_strings)
            .map(|(k, v)| (usize::from(CsvFlagKey::try_from(*k).unwrap().index), v));
        // SAFETY: input is sorted and deduplicated
        unsafe {
            csv_flag.extend_pairs(csv_flag_it);
        }

        let dfc_it = it.map(|(k, v)| {
            let i = DfcKey::try_from(*k).unwrap().offset(&dfc_matrix_size);
            (i, v)
        });
        // SAFETY: input is sorted and deduplicated
        unsafe {
            dfc.extend_pairs(dfc_it);
        }

        Self {
            root,
            meas,
            gate,
            region,
            csv_flag,
            dfc,
        }
    }
}

impl<'a> StdRepairTx<'a> {
    pub(crate) fn into_lookup_transaction(self) -> StdLookupTx<'a> {
        StdTransaction {
            root: MaskedString::into_lookup_array(self.root),
            meas: MaskedString::into_lookup_var(self.meas),
            gate: MaskedString::into_lookup_var(self.gate),
            region: MaskedString::into_lookup_var(self.region),
            csv_flag: MaskedString::into_lookup_var(self.csv_flag),
            dfc: MaskedString::into_lookup_var(self.dfc),
        }
    }

    #[allow(clippy::too_many_lines)]
    pub(crate) fn repair(
        &mut self,
        pstd: &mut PseudoStdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &EvaledReadRepairKeywordsConfig,
    ) -> DeferredWarningsAndErrors<RepairDiagnostics, RepairError, RepairError> {
        // Operation order:
        // 1. drop/demote
        // 2. promote
        // 3. sub/replace
        // 4. rename
        // 5. append

        let matchers = AllKeyMatchers::from_config(conf);

        // drop and demote

        let mut demoted = vec![];
        let mut ignored = vec![];

        for (k, v, m) in self.iter_ne_masked_mut() {
            let dk = DollarWrap(k);
            let ks = k.as_keystring();
            let demote_match = matchers.demote.is_match(&ks);
            let ignore_match = matchers.ignore.is_match(&ks);
            if demote_match {
                *m = RepairMask::Delete;
                demoted.push(dk);
            } else if ignore_match {
                *m = RepairMask::Delete;
                ignored.push((dk, TruncatedNEString(v.to_owned())));
            }
        }

        // promote

        let mut promote_demoted_noop = vec![];
        let mut promote_ignored_noop = vec![];
        let mut promote_non_unique = vec![];
        let mut promote_pseudo_std = vec![];
        let mut promoted = vec![];

        nonstd.retain(|k, v| {
            let ks = k.as_ref();
            if matchers.promote.is_match(ks) {
                if matchers.demote.is_match(ks) {
                    // Key is promoted but also demoted. These cancel so do
                    // nothing and warn user.
                    promote_demoted_noop.push(k.to_owned());
                    true
                } else if matchers.ignore.is_match(ks) {
                    // Key is promoted but also ignored. This is probably a
                    // mistake, so do nothing and warn user.
                    promote_ignored_noop.push(k.to_owned());
                    true
                } else if let Ok(sk) = ks.as_str().parse::<StdKey>() {
                    // Key is promoted and std. Try to insert and take out
                    // of nonstd list if successful.
                    if self.insert(&sk, v.to_owned()).is_some() {
                        promote_non_unique.push((DollarWrap(sk), TruncatedNEString(v.to_owned())));
                        true
                    } else {
                        promoted.push(k.to_owned());
                        false
                    }
                } else {
                    // Key is promoted but is pseudostandard. This is likely
                    // a mistake so do nothing and warn user.
                    promote_pseudo_std.push(k.to_owned());
                    true
                }
            } else {
                // Key is not promoted, do nothing.
                true
            }
        });

        // replace/sub

        let replace = &conf.replace_standard_key_values;
        let mut removed = vec![];
        let mut subbed = vec![];
        let mut replaced = vec![];

        for (k, v, m) in self.iter_ne_masked_mut() {
            let dk = DollarWrap(k);
            let ks = k.as_keystring();
            if let Some(subpat) = matchers.subs.get(&ks) {
                if let Ok(vf) = NEString::try_from(subpat.sub(v.as_str())) {
                    subbed.push((dk, TruncatedNEString(v.to_owned())));
                    *m = RepairMask::Insert(vf);
                } else {
                    removed.push((dk, TruncatedNEString(v.to_owned())));
                    *m = RepairMask::Delete;
                }
            } else if let Some(r) = replace.get(&k) {
                replaced.push((dk, TruncatedNEString(v.to_owned())));
                *m = RepairMask::Insert(r.to_owned());
            }
        }

        // rename

        let (std_rename, pstd_rename) = conf.rename_standard_keys.split();

        let mut renamed_pseudo_std_non_unique = vec![];
        let mut renamed_std_non_unique = vec![];
        let mut renamed_pseudo_std = vec![];
        let mut renamed_std = vec![];

        for (k0, k1) in Vec::from(pstd_rename) {
            let dk1 = DollarWrap(k1);
            if let Entry::Occupied(e) = pstd.entry(DollarWrap(k0.clone())) {
                let k0_ = e.key().to_owned();
                if self.insert(&k1, e.remove()).is_some() {
                    renamed_pseudo_std_non_unique.push((k0_, dk1));
                } else {
                    renamed_pseudo_std.push((k0_, dk1));
                }
            }
        }

        for (k0, k1) in Vec::from(std_rename) {
            let dk0 = DollarWrap(k0);
            let dk1 = DollarWrap(k1);
            if self.key_has_value(&k1) {
                renamed_std_non_unique.push((dk0, dk1));
            } else if let Some(v) = self.delete(&k0) {
                renamed_std.push((dk0, dk1));
                let vf = v.to_owned();
                // we checked above so this shouldn't return anything
                let _ = self.insert(&k1, vf);
            }
        }

        // append

        let mut appended_non_unique = vec![];

        // TODO this is easy to optimize since we know the length of the inputs
        // and there are no pesky regex expressions
        for (k, v) in &conf.append_standard_keywords {
            if let Some(vf) = self.insert(k, v.to_owned()) {
                appended_non_unique.push((DollarWrap(*k), TruncatedNEString(vf)));
            }
        }

        // finalize

        let ret = RepairDiagnostics {
            demoted,
            promoted,
            subbed,
            replaced,
            renamed_std,
            renamed_pseudo_std,
            renamed_std_non_unique,
            renamed_pseudo_std_non_unique,
            ignored,
            removed,
            promoted_demoted_noop: promote_demoted_noop,
            promoted_ignored_noop: promote_ignored_noop,
            promoted_non_unique: promote_non_unique,
            promoted_pseudo_std: promote_pseudo_std,
            appended_non_unique,
        };

        let e0 = ret
            .renamed_std_non_unique
            .iter()
            .map(|(k0, k1)| RenameStdNonUniqueError::new(*k0, *k1))
            .map(RepairError::from);
        let e1 = ret
            .renamed_pseudo_std_non_unique
            .iter()
            .map(|(k0, k1)| RenamePseudoStdNonUniqueError::new(k0.clone(), *k1))
            .map(RepairError::from);
        let e2 = ret
            .promoted_non_unique
            .iter()
            .map(|(k, v)| PromoteNonUniqueError::new(*k, v.clone()))
            .map(RepairError::from);
        let e3 = ret
            .appended_non_unique
            .iter()
            .map(|(k, v)| AppendNonUniqueError::new(*k, v.clone()))
            .map(RepairError::from);
        let e4 = ret
            .promoted_pseudo_std
            .iter()
            .map(|k| PromotePseudoStdError(k.clone()))
            .map(RepairError::from);
        let es = e0.chain(e1).chain(e2).chain(e3).chain(e4);

        let flag = conf.allow_repair_non_unique;
        LogResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .set_deferred_value(ret)
    }

    fn delete(&mut self, k: &StdKey) -> Option<&NEStr> {
        match k {
            StdKey::Root(rk) => self.root.delete(rk),
            StdKey::Meas(mk) => self.meas.delete(mk),
            StdKey::Gate(gk) => self.gate.delete(gk),
            StdKey::Region(rk) => self.region.delete(rk),
            StdKey::CsvFlag(ck) => self.csv_flag.delete(ck),
            StdKey::Dfc(dk) => self.dfc.delete(dk),
        }
    }

    fn insert(&mut self, k: &StdKey, v: NEString) -> Option<NEString> {
        match k {
            StdKey::Root(rk) => self.root.insert(rk, v),
            StdKey::Meas(mk) => self.meas.insert(mk, v),
            StdKey::Gate(gk) => self.gate.insert(gk, v),
            StdKey::Region(rk) => self.region.insert(rk, v),
            StdKey::CsvFlag(ck) => self.csv_flag.insert(ck, v),
            StdKey::Dfc(dk) => self.dfc.insert(dk, v),
        }
    }

    fn key_has_value(&self, k: &StdKey) -> bool {
        match k {
            StdKey::Root(rk) => self.root.key_has_value(rk),
            StdKey::Meas(mk) => self.meas.key_has_value(mk),
            StdKey::Gate(gk) => self.gate.key_has_value(gk),
            StdKey::Region(rk) => self.region.key_has_value(rk),
            StdKey::CsvFlag(ck) => self.csv_flag.key_has_value(ck),
            StdKey::Dfc(dk) => self.dfc.key_has_value(dk),
        }
    }

    fn iter_ne_masked_mut(&mut self) -> impl Iterator<Item = (StdKey, &NEStr, &mut RepairMask)> {
        self.root
            .iter_ne_masked_mut()
            .chain(self.meas.iter_ne_masked_mut())
            .chain(self.gate.iter_ne_masked_mut())
            .chain(self.region.iter_ne_masked_mut())
            .chain(self.csv_flag.iter_ne_masked_mut())
            .chain(self.dfc.iter_ne_masked_mut())
    }
}

impl StdLookupTx<'_> {
    #[allow(clippy::too_many_lines)]
    pub(crate) fn finalize(
        &self,
        par: Par,
        gate: Gate,
        version: Version,
        nonstd: &mut NonStdKeywords,
        pstd: &mut PseudoStdKeywords,
        conf: &EvaledReadStdKeywordsConfig,
    ) -> WarningsAndErrorsResult<ExtraStdKeywords, (), ExtraStdKeywordError, ExtraStdKeywordError>
    {
        let mut optional = vec![];
        let mut hyper_par_ = vec![];
        let mut hyper_gate_ = vec![];
        let mut other_version_ = vec![];
        let mut timestep_ = None;

        // NOTE for dropped optional keywords we don't throw an error here. This
        // is because the error that caused the keyword to be dropped in the
        // first place was captured upstream at the call site where the optional
        // keyword was requested. The reason why this is split comes down to
        // speed and memory. It is more efficient (without some insane code
        // gymnastics) to not store the error as part of the mask, and instead
        // only store if it was dropped or not.
        let mut go = |k, v: &NEStr, was_demoted| {
            if was_demoted {
                nonstd.insert_demoted(k, v.to_owned());
            } else {
                optional.push((DollarWrap(k), v.to_owned()));
            }
        };

        for (k, v, m) in self.root.iter_masked() {
            match m {
                LookupStatus::Unseen => {
                    let vo = v.to_owned();
                    match k {
                        RootKey::Timestep if version > Version::FCS2_0 => {
                            timestep_ = Some(vo);
                        }
                        // BEGIN/ENDSTEXT (FCS3.0+) and NEXTDATA will still be
                        // unseen since these are only used when parsing the
                        // header and not standardization. Therefore simply
                        // ignore them when considering if the key belongs to
                        // another version or not.
                        RootKey::Nextdata => (),
                        RootKey::Beginstext | RootKey::Endstext if version > Version::FCS2_0 => (),
                        _ => other_version_.push((DollarWrap(k.into()), vo)),
                    }
                }
                LookupStatus::Seen(a) => match a {
                    LookupAction::None => (),
                    LookupAction::Demote => go(k.into(), v, true),
                    LookupAction::Drop => go(k.into(), v, false),
                },
            }
        }

        let mut meas_it = self.meas.iter_masked();

        for (k, v, m) in meas_it.by_ref() {
            if usize::from(k.index) >= usize::from(par) {
                hyper_par_.push((DollarWrap(k.into()), v.to_owned()));
                break;
            }
            match m {
                LookupStatus::Unseen => other_version_.push((DollarWrap(k.into()), v.to_owned())),
                LookupStatus::Seen(a) => match a {
                    LookupAction::None => (),
                    LookupAction::Demote => go(k.into(), v, true),
                    LookupAction::Drop => go(k.into(), v, false),
                },
            }
        }

        hyper_par_.extend(meas_it.map(|(k, v, _)| (DollarWrap(k.into()), v.to_owned())));

        let mut gate_it = self.gate.iter_masked();

        for (k, v, m) in gate_it.by_ref() {
            if usize::from(k.index) >= usize::from(gate) {
                hyper_gate_.push((DollarWrap(k.into()), v.to_owned()));
                break;
            }
            match m {
                LookupStatus::Unseen => other_version_.push((DollarWrap(k.into()), v.to_owned())),
                LookupStatus::Seen(a) => match a {
                    LookupAction::None => (),
                    LookupAction::Demote => go(k.into(), v, true),
                    LookupAction::Drop => go(k.into(), v, false),
                },
            }
        }

        hyper_gate_.extend(gate_it.map(|(k, v, _)| (DollarWrap(k.into()), v.to_owned())));

        // TODO we could also do something like hyper_par/gate with these but
        // they are hardly used anyways and doing so would be complex
        for (k, v, m) in self.region.iter_masked() {
            match m {
                LookupStatus::Unseen => other_version_.push((DollarWrap(k.into()), v.to_owned())),
                LookupStatus::Seen(a) => match a {
                    LookupAction::None => (),
                    LookupAction::Demote => go(k.into(), v, true),
                    LookupAction::Drop => go(k.into(), v, false),
                },
            }
        }

        // TODO ditto $CSMODE
        for (k, v, m) in self.csv_flag.iter_masked() {
            match m {
                LookupStatus::Unseen => other_version_.push((DollarWrap(k.into()), v.to_owned())),
                LookupStatus::Seen(a) => match a {
                    LookupAction::None => (),
                    LookupAction::Demote => go(k.into(), v, true),
                    LookupAction::Drop => go(k.into(), v, false),
                },
            }
        }

        for (k, v, m) in self.dfc.iter_masked() {
            let is_hyper_par = usize::from(k.index.i0) > usize::from(par)
                || usize::from(k.index.i1) > usize::from(par);
            match m {
                LookupStatus::Unseen => {
                    if is_hyper_par {
                        hyper_par_.push((DollarWrap(k.into()), v.to_owned()));
                    } else {
                        other_version_.push((DollarWrap(k.into()), v.to_owned()));
                    }
                }
                LookupStatus::Seen(a) => match a {
                    LookupAction::None => (),
                    LookupAction::Demote => go(k.into(), v, true),
                    LookupAction::Drop => go(k.into(), v, false),
                },
            }
        }

        let mut errors = vec![];
        let mut warnings = vec![];

        macro_rules! extend_errors {
            ($flag:ident, $errors:expr, $fun:expr) => {{
                let it = $errors.iter().map($fun).map(ExtraStdKeywordError::from);
                match conf.$flag.is_error() {
                    Some(true) => errors.extend(it),
                    Some(false) => warnings.extend(it),
                    None => (),
                }
                if conf.$flag.is_demote() {
                    for (k, v) in $errors {
                        nonstd.insert_demoted(k.0, v);
                    }
                    Default::default()
                } else {
                    $errors
                }
            }};
        }

        let hyper_par = extend_errors!(process_hyper_par, hyper_par_, |(k, _)| {
            HyperParError::new(par, *k)
        });
        let hyper_gate = extend_errors!(process_hyper_par, hyper_gate_, |(k, _)| {
            HyperGateError::new(gate, *k)
        });
        let other_version = extend_errors!(process_other_version, other_version_, |(k, _)| {
            KeywordOtherVersionError::new(*k, version)
        });

        if timestep_.is_some() {
            match conf.process_extra_timestep.is_error() {
                Some(true) => errors.push(TimestepFoundError.into()),
                Some(false) => warnings.push(TimestepFoundError.into()),
                None => (),
            }
        }

        let timestep = timestep_.and_then(|ts| {
            if conf.process_extra_timestep.is_demote() {
                nonstd.insert_demoted(RootKey::Timestep.to_std0(), ts);
                None
            } else {
                Some(ts)
            }
        });

        // TODO this might not be the best spot for this. It is here because we
        // need to modify the nonstd keyword list depending on the config flag,
        // and this just happens to be the easiest place to do this that will
        // affect the entire standardization procedure.
        for (k, v) in pstd.iter() {
            let e = || {
                let v_ = TruncatedNEString(v.to_owned());
                PseudoStdKeyError::new(k.clone(), v_).into()
            };
            match conf.process_pseudostandard.is_error() {
                Some(true) => errors.push(e()),
                Some(false) => warnings.push(e()),
                None => (),
            }
        }

        if conf.process_pseudostandard.is_demote() {
            nonstd.extend(pstd.drain().map(|(k, v)| (NonStdKey::from(k.0), v)));
        }

        if let Some(ne) = NEVec::try_from_vec(errors) {
            LogResult::new_from_ne_err_iter(ne, ()).set_commutative_warnings(warnings)
        } else {
            let ret = ExtraStdKeywords {
                optional,
                hyper_par,
                hyper_gate,
                other_version,
                timestep,
            };
            LogResult::new_ok(ret).set_commutative_warnings(warnings)
        }
    }

    pub(crate) fn remove_optical_only(
        &mut self,
        targets: &[OpticalOnlyKey],
        keys: &OpticalOnlyKeys,
        i: MeasIndex,
        flag: ProcessOpticalOnlyKeys,
    ) -> OpticalOnlyResult {
        let mut es = vec![];
        let mut ws = vec![];
        let mut pairs = vec![];
        let (demote, warn) = match flag {
            ProcessOpticalOnlyKeys::DemoteWarn => (true, true),
            ProcessOpticalOnlyKeys::DemoteSilent => (true, false),
            ProcessOpticalOnlyKeys::DropWarn => (false, true),
            ProcessOpticalOnlyKeys::DropSilent => (false, false),
        };
        let action = if demote {
            LookupAction::Demote
        } else {
            LookupAction::Drop
        };
        // This is in contrast to looking up all other values since in those
        // cases we need to parse the keywords and therefore record and error if
        // this fails. This is easier to do at the call site rather than storing
        // it lazily in the index. Here we only care about the pair and if
        // it has a non-empty value.
        for t in targets {
            let k = StdKey::from_optical_only_key(*t, i);
            if let Some(v) = self.remove_unseen(&k) {
                let err = || TemporalHasOpticalKeyError::new(i, *t);
                if keys.0.contains(t) {
                    let vf = v.to_owned();
                    self.set_lookup_action_seen(&k, action);
                    if warn {
                        ws.push(err());
                    }
                    pairs.push((DollarWrap(k), vf.into()));
                } else {
                    es.push(err());
                }
            }
        }
        let mut ret = LogResult::new_from_err_iter(es, pairs, ());
        ret.extend_commutative_warnings(ws);
        ret
    }

    pub(crate) fn read<K: ValueToStdKey>(&self, i: &K::Index) -> Option<&NEStr> {
        self.get_unseen(&K::std(i))
    }

    pub(crate) fn remove<K: ValueToStdKey>(&mut self, i: &K::Index) -> Option<&NEStr> {
        self.remove_unseen(&K::std(i))
    }

    pub(crate) fn remove_and_parse<F, X, K: ValueToStdKey>(
        &mut self,
        i: &K::Index,
        f: F,
    ) -> Option<X>
    where
        F: FnOnce(&NEStr) -> (Option<LookupAction>, X),
    {
        self.parse_unseen(&K::std(i), f)
    }

    pub(crate) fn set_failure_flag<F: KeywordFailureFlag>(&mut self, k: &StdKey, f: F) {
        if let Some(a) = LookupAction::from_flag(f) {
            self.set_lookup_action_seen(k, a);
        }
    }

    pub(crate) fn set_lookup_action_seen(&mut self, k: &StdKey, a: LookupAction) {
        match k {
            StdKey::Root(rk) => self.root.set_lookup_action_seen(rk, a),
            StdKey::Meas(mk) => self.meas.set_lookup_action_seen(mk, a),
            StdKey::Gate(gk) => self.gate.set_lookup_action_seen(gk, a),
            StdKey::Region(rk) => self.region.set_lookup_action_seen(rk, a),
            StdKey::CsvFlag(ck) => self.csv_flag.set_lookup_action_seen(ck, a),
            StdKey::Dfc(dk) => self.dfc.set_lookup_action_seen(dk, a),
        }
    }

    fn get_unseen(&self, k: &StdKey) -> Option<&NEStr> {
        match k {
            StdKey::Root(rk) => self.root.get_unseen(rk),
            StdKey::Meas(mk) => self.meas.get_unseen(mk),
            StdKey::Gate(gk) => self.gate.get_unseen(gk),
            StdKey::Region(rk) => self.region.get_unseen(rk),
            StdKey::CsvFlag(ck) => self.csv_flag.get_unseen(ck),
            StdKey::Dfc(dk) => self.dfc.get_unseen(dk),
        }
    }

    fn remove_unseen(&mut self, k: &StdKey) -> Option<&NEStr> {
        match k {
            StdKey::Root(rk) => self.root.remove_unseen(rk),
            StdKey::Meas(mk) => self.meas.remove_unseen(mk),
            StdKey::Gate(gk) => self.gate.remove_unseen(gk),
            StdKey::Region(rk) => self.region.remove_unseen(rk),
            StdKey::CsvFlag(ck) => self.csv_flag.remove_unseen(ck),
            StdKey::Dfc(dk) => self.dfc.remove_unseen(dk),
        }
    }

    fn parse_unseen<F, X>(&mut self, k: &StdKey, f: F) -> Option<X>
    where
        F: FnOnce(&NEStr) -> (Option<LookupAction>, X),
    {
        match k {
            StdKey::Root(rk) => self.root.parse_unseen(rk, f),
            StdKey::Meas(mk) => self.meas.parse_unseen(mk, f),
            StdKey::Gate(gk) => self.gate.parse_unseen(gk, f),
            StdKey::Region(rk) => self.region.parse_unseen(rk, f),
            StdKey::CsvFlag(ck) => self.csv_flag.parse_unseen(ck, f),
            StdKey::Dfc(dk) => self.dfc.parse_unseen(dk, f),
        }
    }
}

// TODO this is a function I stole from nightly. It seems to work and the reason
// it hasn't been mainlined is because there is disagreement about the API (see
// https://github.com/rust-lang/rust/issues/54279).
//
// I think it is clearer to return the partition point and do with it as one
// wishes (unlike the function in Vec) so here it is.
fn partition_dedup_by<T, F>(xs: &mut Vec<T>, mut same_bucket: F) -> usize
where
    F: FnMut(&mut T, &mut T) -> bool,
{
    // Although we have a mutable reference to `self`, we cannot make
    // *arbitrary* changes. The `same_bucket` calls could panic, so we
    // must ensure that the slice is in a valid state at all times.
    //
    // The way that we handle this is by using swaps; we iterate
    // over all the elements, swapping as we go so that at the end
    // the elements we wish to keep are in the front, and those we
    // wish to reject are at the back. We can then split the slice.
    // This operation is still `O(n)`.
    //
    // Example: We start in this state, where `r` represents "next
    // read" and `w` represents "next_write".
    //
    //           r
    //     +---+---+---+---+---+---+
    //     | 0 | 1 | 1 | 2 | 3 | 3 |
    //     +---+---+---+---+---+---+
    //           w
    //
    // Comparing self[r] against self[w-1], this is not a duplicate, so
    // we swap self[r] and self[w] (no effect as r==w) and then increment both
    // r and w, leaving us with:
    //
    //               r
    //     +---+---+---+---+---+---+
    //     | 0 | 1 | 1 | 2 | 3 | 3 |
    //     +---+---+---+---+---+---+
    //               w
    //
    // Comparing self[r] against self[w-1], this value is a duplicate,
    // so we increment `r` but leave everything else unchanged:
    //
    //                   r
    //     +---+---+---+---+---+---+
    //     | 0 | 1 | 1 | 2 | 3 | 3 |
    //     +---+---+---+---+---+---+
    //               w
    //
    // Comparing self[r] against self[w-1], this is not a duplicate,
    // so swap self[r] and self[w] and advance r and w:
    //
    //                       r
    //     +---+---+---+---+---+---+
    //     | 0 | 1 | 2 | 1 | 3 | 3 |
    //     +---+---+---+---+---+---+
    //                   w
    //
    // Not a duplicate, repeat:
    //
    //                           r
    //     +---+---+---+---+---+---+
    //     | 0 | 1 | 2 | 3 | 1 | 3 |
    //     +---+---+---+---+---+---+
    //                       w
    //
    // Duplicate, advance r. End of slice. Split at w.

    let len = xs.len();
    if len <= 1 {
        return len;
    }

    let ptr = xs.as_mut_ptr();
    let mut next_read: usize = 1;
    let mut next_write: usize = 1;

    // SAFETY: the `while` condition guarantees `next_read` and `next_write`
    // are less than `len`, thus are inside `self`. `prev_ptr_write` points to
    // one element before `ptr_write`, but `next_write` starts at 1, so
    // `prev_ptr_write` is never less than 0 and is inside the slice.
    // This fulfils the requirements for dereferencing `ptr_read`, `prev_ptr_write`
    // and `ptr_write`, and for using `ptr.add(next_read)`, `ptr.add(next_write - 1)`
    // and `prev_ptr_write.offset(1)`.
    //
    // `next_write` is also incremented at most once per loop at most meaning
    // no element is skipped when it may need to be swapped.
    //
    // `ptr_read` and `prev_ptr_write` never point to the same element. This
    // is required for `&mut *ptr_read`, `&mut *prev_ptr_write` to be safe.
    // The explanation is simply that `next_read >= next_write` is always true,
    // thus `next_read > next_write - 1` is too.
    #[allow(clippy::multiple_unsafe_ops_per_block)]
    #[allow(clippy::swap_ptr_to_ref)]
    unsafe {
        // Avoid bounds checks by using raw pointers.
        while next_read < len {
            let ptr_read = ptr.add(next_read);
            let prev_ptr_write = ptr.add(next_write - 1);
            if !same_bucket(&mut *ptr_read, &mut *prev_ptr_write) {
                if next_read != next_write {
                    let ptr_write = prev_ptr_write.add(1);
                    mem::swap(&mut *ptr_read, &mut *ptr_write);
                }
                next_write += 1;
            }
            next_read += 1;
        }
    }
    next_write
}

fn partition_dedup_by_key<T, K, F>(xs: &mut Vec<T>, mut key: F) -> usize
where
    F: FnMut(&mut T) -> K,
    K: PartialEq,
{
    partition_dedup_by(xs, |a, b| key(a) == key(b))
}

#[cfg(feature = "serde")]
impl Serialize for StdKeywords {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.n_strings()))?;
        for (k, v) in self.iter_keywords() {
            map.serialize_entry(&DollarWrap(k), v)?;
        }
        map.end()
    }
}

#[cfg(feature = "python")]
mod python {
    use super::StdKeywords;

    use fireflow_types::std_key::DollarStdKey;
    use nonempty::NEString;

    use pyo3::{prelude::*, types::PyDict};

    impl<'py> FromPyObject<'_, 'py> for StdKeywords {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            // Cast to dict rather than going through rust hashmap to preserve
            // order. It will be sorted anyways but this might avoid some
            // overhead since the input will likely be partly grouped.
            let tmp = obj
                .cast::<PyDict>()?
                .iter()
                .map(|(k, v)| Ok((k.extract::<DollarStdKey>()?.0, v.extract::<NEString>()?)))
                .collect::<Result<Vec<_>, PyErr>>()?;
            // Ignore duplicates since the input dict should not have any
            Ok(Self::from_vec(tmp).0)
        }
    }

    impl<'py> IntoPyObject<'py> for StdKeywords {
        type Target = PyDict;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            // Use dict to preserve order
            let out = PyDict::new(py);
            for (k, v) in self.iter_dollar_keywords() {
                let k_ = k.into_pyobject(py)?;
                let v_ = v.to_owned().into_pyobject(py)?;
                out.set_item(k_, v_)?;
            }
            Ok(out)
        }
    }
}
