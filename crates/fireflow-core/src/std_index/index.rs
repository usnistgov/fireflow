use crate::config::{EvaledReadRepairKeywordsConfig, EvaledReadStdKeywordsConfig};
use crate::logging::{DeferredWarningsAndErrors, LogResult, WarningsAndErrorsResult};
use crate::std_index::masked::{
    LookupMask, LookupStatus, MaskedEnumString, MaskedString, MaskedVariableString, RepairMask,
};
use crate::std_index::nested_string::{NestedEnumString, NestedStringSize, NestedVariableString};
use crate::text::keywords::{Gate, Par};
use crate::validated::keys::{
    NonStdKeywords, PseudoNonStdKeywords, PseudoStdKeywords, TruncatedNEString, ValueToStdKey,
};

use fireflow_types::config::{
    KeywordFailureFlag, OpticalOnlyKey, OpticalOnlyKeys, ProcessOpticalOnlyKeys,
    TemporalHasOpticalKeyError, TriErrorFlag as _,
};
use fireflow_types::index::MeasIndex;
use fireflow_types::keys::nonstd::DollarWrap;
use fireflow_types::keys::raw_std::{
    CsvFlagKey, DfcKey, EnumIndex as _, GateKey, MeasKey, N_ROOT, RawStdKey, RegionKey, RootKey,
    ToStd as _,
};
use fireflow_types::keys::{
    AnyKey, PseudoNonStdKey, PseudoNonStdKeywordsExt as _, PseudoStdKey, StdKey,
};
use fireflow_types::keywords::Version;
use nonempty::{NEStr, NEString, NEVec};

use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools as _;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::{Serialize, Serializer, ser::SerializeMap as _};

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
    pyo3::prelude::*,
};

type OpticalOnlyResult = WarningsAndErrorsResult<
    Vec<(StdKey, TruncatedNEString)>,
    (),
    TemporalHasOpticalKeyError,
    TemporalHasOpticalKeyError,
>;

pub(crate) type DroppedStdKeywords = Vec<(StdKey, NEString)>;

/// Leftover standard keyword after parsing
#[derive(Clone, new, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct ExtraStdKeywords {
    pub optional: DroppedStdKeywords,
    pub hyper_par: DroppedStdKeywords,
    pub hyper_gate: DroppedStdKeywords,
    pub other_version: DroppedStdKeywords,
    pub undemoted_pseudostandard: Vec<(PseudoStdKey, TruncatedNEString)>,
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
    pub key: StdKey,
}

/// Error denoting that gating keyword within standard but above $GATE was found
#[derive(Debug, Error, new, PartialEq, Clone)]
#[error("gating keyword is part of standard but outside $GATE ({gate}): {key}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ExtraKeywordError))]
pub struct HyperGateError {
    pub gate: Gate,
    pub key: StdKey,
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
    pub key: StdKey,
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
    pub key: PseudoStdKey,
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
    RenameStd(RenameNonUniqueError),
    PromoteNonUnique(PromoteNonUniqueError),
    AppendNonUnique(AppendNonUniqueError),
}

/// Error when renaming standard keys which are not unique.
#[derive(new, Debug, Error, PartialEq, Clone)]
#[error("key {k0} could not be renamed to {k1} because {k1} already exists")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct RenameNonUniqueError {
    k0: AnyKey,
    k1: AnyKey,
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
    key: PseudoNonStdKey,
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
    key: StdKey,
    value: TruncatedNEString,
}

/// Diagnostic output from repairing the keyword list.
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[allow(clippy::too_many_arguments)]
pub struct RepairDiagnostics {
    /// Standard keys which were demoted.
    pub demoted: Vec<StdKey>,

    /// Non-standard keys which were promoted.
    pub promoted: Vec<PseudoNonStdKey>,

    /// Standard keys which had values that were substituted.
    ///
    /// Values here are the original.
    pub subbed: Vec<(StdKey, TruncatedNEString)>,

    /// Standard keys which had values that were replaced.
    ///
    /// Values here are the original.
    pub replaced: Vec<(StdKey, TruncatedNEString)>,

    /// Keys which were renamed.
    ///
    /// First key in pair is the original.
    pub renamed: Vec<(AnyKey, AnyKey)>,

    /// Keys not renamed because they collided with an existing key.
    pub renamed_non_unique: Vec<(AnyKey, AnyKey)>,

    /// Standard keys which were ignored.
    pub ignored: Vec<(StdKey, TruncatedNEString)>,

    /// Standard keys which were removed.
    ///
    /// This only happens when a substitution pattern returns a blank.
    pub removed: Vec<(StdKey, TruncatedNEString)>,

    /// Non-standard keys which collided with a standard key when promoted.
    ///
    /// These keys were not moved.
    pub promoted_non_unique: Vec<(PseudoNonStdKey, TruncatedNEString)>,

    /// Appended keys which collided with an existing standard key.
    pub appended_non_unique: Vec<(StdKey, TruncatedNEString)>,
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

impl StdKeywords {
    pub fn iter_dollar_keywords(&self) -> impl Iterator<Item = (StdKey, &NEStr)> {
        self.iter_keywords().map(|(k, v)| (DollarWrap(k), v))
    }

    pub fn iter_keywords(&self) -> impl Iterator<Item = (RawStdKey, &NEStr)> {
        self.root
            .iter_std()
            .chain(self.meas.iter_std())
            .chain(self.gate.iter_std())
            .chain(self.region.iter_std())
            .chain(self.csv_flag.iter_std())
            .chain(self.dfc.iter_std())
    }

    #[must_use]
    pub fn from_vec<V>(pairs: Vec<(RawStdKey, V)>) -> (Self, Vec<(StdKey, TruncatedNEString)>)
    where
        V: AsRef<NEStr>,
    {
        let mut root_n_bytes = 0;
        let mut meas_size = NestedStringSize::default();
        let mut gate_size = NestedStringSize::default();
        let mut region_size = NestedStringSize::default();
        let mut csv_flag_size = NestedStringSize::default();
        let mut dfc_n_bytes = 0;
        let mut dfc_matrix_size = 0;

        for (k, v) in &pairs {
            let n_bytes = v.as_ref().as_ne_bytes().len().get();
            match k {
                RawStdKey::Root(_) => {
                    root_n_bytes += n_bytes;
                }
                RawStdKey::Meas(i) => {
                    meas_size.n_offsets = meas_size.n_offsets.max(i.offset0() + 1);
                    meas_size.n_bytes += n_bytes;
                }
                RawStdKey::Gate(i) => {
                    gate_size.n_offsets = gate_size.n_offsets.max(i.offset0() + 1);
                    gate_size.n_bytes += n_bytes;
                }
                RawStdKey::Region(i) => {
                    region_size.n_offsets = region_size.n_offsets.max(i.offset0() + 1);
                    region_size.n_bytes += n_bytes;
                }
                RawStdKey::CsvFlag(i) => {
                    csv_flag_size.n_offsets = csv_flag_size.n_offsets.max(i.offset0() + 1);
                    csv_flag_size.n_bytes += n_bytes;
                }
                RawStdKey::Dfc(dk) => {
                    dfc_n_bytes += n_bytes;
                    let i0 = usize::from(dk.index.i0);
                    let i1 = usize::from(dk.index.i1);
                    dfc_matrix_size = dfc_matrix_size.max(i0.max(i1));
                }
            }
        }

        let dfc_size = NestedStringSize::new(dfc_n_bytes, dfc_matrix_size * dfc_matrix_size + 1);

        let mut root = NestedRoot::init_array(root_n_bytes);
        let mut meas = NestedVariableString::init_var(&meas_size, ());
        let mut gate = NestedVariableString::init_var(&gate_size, ());
        let mut region = NestedVariableString::init_var(&region_size, ());
        let mut csv_flag = NestedVariableString::init_var(&csv_flag_size, ());
        let mut dfc = NestedVariableString::init_var(&dfc_size, dfc_matrix_size);

        let mut duplicates = vec![];

        for (k, v) in pairs {
            if let Some(dup) = match k {
                RawStdKey::Root(i) => root.push_dedup(&i, v),
                RawStdKey::Meas(i) => meas.push_dedup(&i, v),
                RawStdKey::Gate(i) => gate.push_dedup(&i, v),
                RawStdKey::Region(i) => region.push_dedup(&i, v),
                RawStdKey::CsvFlag(i) => csv_flag.push_dedup(&i, v),
                RawStdKey::Dfc(i) => dfc.push_dedup(&i, v),
            } {
                duplicates.push((DollarWrap(k), dup.as_ref().to_owned().into()));
            }
        }

        let ret = Self {
            root,
            meas,
            gate,
            region,
            csv_flag,
            dfc,
        };
        (ret, duplicates)
    }

    #[must_use]
    pub fn get(&self, k: &RawStdKey) -> Option<&NEStr> {
        let s = match k {
            RawStdKey::Root(rk) => self.root.get(rk),
            RawStdKey::Meas(mk) => self.meas.get(mk),
            RawStdKey::Gate(gk) => self.gate.get(gk),
            RawStdKey::Region(rk) => self.region.get(rk),
            RawStdKey::CsvFlag(ck) => self.csv_flag.get(ck),
            RawStdKey::Dfc(dk) => self.dfc.get(dk),
        }?;
        NEStr::try_new(s)
    }

    pub(crate) fn contains_key(&self, k: &RawStdKey) -> bool {
        self.get(k).is_some()
    }

    pub(crate) fn n_offsets(&self) -> usize {
        self.root.n_offsets()
            + self.meas.n_offsets()
            + self.gate.n_offsets()
            + self.region.n_offsets()
            + self.csv_flag.n_offsets()
            + self.dfc.n_offsets()
    }

    pub(crate) fn concat(self, other: Self) -> (Self, Vec<(StdKey, TruncatedNEString)>) {
        if self.n_offsets() == 0 {
            (other, vec![])
        } else if other.n_offsets() == 0 {
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
        pnonstd: &mut PseudoNonStdKeywords,
        nonstd: &mut NonStdKeywords,
        conf: &EvaledReadRepairKeywordsConfig,
    ) -> DeferredWarningsAndErrors<RepairDiagnostics, RepairError, RepairError> {
        let match_promote = conf.promote_nonstandard_keys.as_matcher();
        let match_demote = conf.demote_standard_keys.as_matcher();
        let match_ignore = conf.ignore_standard_keys.as_matcher();
        let match_subs = conf.substitute_standard_key_values.as_matcher();

        // ignore

        let mut ignored = vec![];

        for (k, ()) in &match_ignore.literals {
            if let Some(v) = self.delete(&k.0) {
                ignored.push((*k, TruncatedNEString(v.to_owned())));
            }
        }

        if match_ignore.has_wildcards() {
            for (k, v, m) in self.iter_ne_masked_mut() {
                if match_ignore.is_wildcard_match(&k) {
                    *m = RepairMask::Delete;
                    ignored.push((DollarWrap(k), TruncatedNEString(v.to_owned())));
                }
            }
        }

        // rename

        let mut renamed = vec![];
        let mut renamed_non_unique = vec![];

        for (k0, k1) in &conf.rename_standard_keys {
            macro_rules! go {
                () => {
                    match k0 {
                        AnyKey::Std(k0_) => self.delete(&k0_.0).map(|v| {
                            renamed.push((k0.clone(), k1.clone()));
                            v.to_owned()
                        }),
                        AnyKey::PseudoNonStd(k0_) => pnonstd.remove(k0_).inspect(|_| {
                            renamed.push((k0.clone(), k1.clone()));
                        }),
                        AnyKey::PseudoStd(k0_) => pstd.remove(k0_).inspect(|_| {
                            renamed.push((k0.clone(), k1.clone()));
                        }),
                        AnyKey::NonStd(k0_) => nonstd.remove(k0_).inspect(|_| {
                            renamed.push((k0.clone(), k1.clone()));
                        }),
                    }
                };
            }

            match k1 {
                AnyKey::Std(k1_) => {
                    if self.key_has_value(&k1_.0) {
                        renamed_non_unique.push((k0.clone(), k1.clone()));
                    } else if let Some(v) = go!() {
                        // we checked above so this shouldn't return anything
                        let _ = self.insert(&k1_.0, v);
                    }
                }
                AnyKey::PseudoNonStd(k1_) => {
                    if pnonstd.contains_key(k1_) {
                        renamed_non_unique.push((k0.clone(), k1.clone()));
                    } else if let Some(v) = go!() {
                        // we checked above so this shouldn't return anything
                        let _ = pnonstd.insert(*k1_, v);
                    }
                }
                AnyKey::PseudoStd(k1_) => {
                    if pstd.contains_key(k1_) {
                        renamed_non_unique.push((k0.clone(), k1.clone()));
                    } else if let Some(v) = go!() {
                        // we checked above so this shouldn't return anything
                        let _ = pstd.insert(k1_.clone(), v);
                    }
                }
                AnyKey::NonStd(k1_) => {
                    if nonstd.contains_key(k1_) {
                        renamed_non_unique.push((k0.clone(), k1.clone()));
                    } else if let Some(v) = go!() {
                        // we checked above so this shouldn't return anything
                        let _ = nonstd.insert(k1_.clone(), v);
                    }
                }
            }
        }

        // demote

        let mut demoted = vec![];

        for (k, ()) in &match_demote.literals {
            if let Some(v) = self.delete(&k.0) {
                pnonstd.insert_demoted(nonstd, k.0, v.to_owned());
                demoted.push(*k);
            }
        }

        if match_demote.has_wildcards() {
            for (k, v, m) in self.iter_ne_masked_mut() {
                if match_demote.is_wildcard_match(&k) {
                    *m = RepairMask::Delete;
                    // TODO this could be made more efficient by only moving once
                    // the index queried for errors
                    pnonstd.insert_demoted(nonstd, k, v.to_owned());
                    demoted.push(DollarWrap(k));
                }
            }
        }

        // promote

        let mut promote_non_unique = vec![];
        let mut promoted = vec![];

        for (k, ()) in &match_promote.literals {
            if let Some(v) = pnonstd.remove(k) {
                if let Some(vf) = self.insert(&k.0, v) {
                    promote_non_unique.push((*k, TruncatedNEString(vf)));
                } else {
                    promoted.push(*k);
                }
            }
        }

        if match_promote.has_wildcards() {
            pnonstd.retain(|k, v| {
                if match_promote.is_wildcard_match(&k.0) {
                    if let Some(vf) = self.insert(&k.0, v.to_owned()) {
                        promote_non_unique.push((*k, TruncatedNEString(vf)));
                        true
                    } else {
                        promoted.push(*k);
                        false
                    }
                } else {
                    true
                }
            });
        }

        // replace

        let mut replaced = vec![];

        for (dk, vf) in &conf.replace_standard_key_values {
            if let Some(v) = self.delete(&dk.0) {
                replaced.push((*dk, TruncatedNEString(v.to_owned())));
                let _ = self.insert(&dk.0, vf.to_owned());
            }
        }

        // sub

        let mut removed = vec![];
        let mut subbed = vec![];

        for (k, subpat) in &match_subs.literals {
            if let Some(v) = self.delete(&k.0) {
                if let Ok(vf) = NEString::try_from(subpat.sub(v.as_str())) {
                    subbed.push((*k, TruncatedNEString(v.to_owned())));
                    let _ = self.insert(&k.0, vf);
                } else {
                    removed.push((*k, TruncatedNEString(v.to_owned())));
                }
            }
        }

        if match_subs.has_wildcards() {
            for (k, v, m) in self.iter_ne_masked_mut() {
                if let Some(subpat) = match_subs.get_wildcard(&k) {
                    let dk = DollarWrap(k);
                    if let Ok(vf) = NEString::try_from(subpat.sub(v.as_str())) {
                        subbed.push((dk, TruncatedNEString(v.to_owned())));
                        *m = RepairMask::Insert(vf);
                    } else {
                        removed.push((dk, TruncatedNEString(v.to_owned())));
                        *m = RepairMask::Delete;
                    }
                }
            }
        }

        // append

        let mut appended_non_unique = vec![];

        // TODO this is easy to optimize since we know the length of the inputs
        // and there are no pesky regex expressions
        for (k, v) in &conf.append_standard_keywords {
            if let Some(vf) = self.insert(&k.0, v.to_owned()) {
                appended_non_unique.push((*k, TruncatedNEString(vf)));
            }
        }

        // finalize

        let ret = RepairDiagnostics {
            demoted,
            promoted,
            subbed,
            replaced,
            renamed,
            renamed_non_unique,
            ignored,
            removed,
            promoted_non_unique: promote_non_unique,
            appended_non_unique,
        };

        let e0 = ret
            .renamed_non_unique
            .iter()
            .map(|(k0, k1)| RenameNonUniqueError::new(k0.clone(), k1.clone()))
            .map(RepairError::from);
        let e1 = ret
            .promoted_non_unique
            .iter()
            .map(|(k, v)| PromoteNonUniqueError::new(*k, v.clone()))
            .map(RepairError::from);
        let e2 = ret
            .appended_non_unique
            .iter()
            .map(|(k, v)| AppendNonUniqueError::new(*k, v.clone()))
            .map(RepairError::from);
        let es = e0.chain(e1).chain(e2);

        let flag = conf.allow_repair_non_unique;
        LogResult::new_deferred_switchable_iter3((), es, flag)
            .switchable_into_commutative()
            .set_deferred_value(ret)
    }

    fn delete(&mut self, k: &RawStdKey) -> Option<&NEStr> {
        match k {
            RawStdKey::Root(rk) => self.root.delete(rk),
            RawStdKey::Meas(mk) => self.meas.delete(mk),
            RawStdKey::Gate(gk) => self.gate.delete(gk),
            RawStdKey::Region(rk) => self.region.delete(rk),
            RawStdKey::CsvFlag(ck) => self.csv_flag.delete(ck),
            RawStdKey::Dfc(dk) => self.dfc.delete(dk),
        }
    }

    fn insert(&mut self, k: &RawStdKey, v: NEString) -> Option<NEString> {
        match k {
            RawStdKey::Root(rk) => self.root.insert(rk, v),
            RawStdKey::Meas(mk) => self.meas.insert(mk, v),
            RawStdKey::Gate(gk) => self.gate.insert(gk, v),
            RawStdKey::Region(rk) => self.region.insert(rk, v),
            RawStdKey::CsvFlag(ck) => self.csv_flag.insert(ck, v),
            RawStdKey::Dfc(dk) => self.dfc.insert(dk, v),
        }
    }

    fn key_has_value(&self, k: &RawStdKey) -> bool {
        match k {
            RawStdKey::Root(rk) => self.root.key_has_value(rk),
            RawStdKey::Meas(mk) => self.meas.key_has_value(mk),
            RawStdKey::Gate(gk) => self.gate.key_has_value(gk),
            RawStdKey::Region(rk) => self.region.key_has_value(rk),
            RawStdKey::CsvFlag(ck) => self.csv_flag.key_has_value(ck),
            RawStdKey::Dfc(dk) => self.dfc.key_has_value(dk),
        }
    }

    fn iter_ne_masked_mut(&mut self) -> impl Iterator<Item = (RawStdKey, &NEStr, &mut RepairMask)> {
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
    pub(crate) fn commit(self) -> StdKeywords {
        StdKeywords {
            root: self.root.commit_array(),
            meas: self.meas.commit_var(),
            gate: self.gate.commit_var(),
            region: self.region.commit_var(),
            csv_flag: self.csv_flag.commit_var(),
            dfc: self.dfc.commit_var(),
        }
    }

    #[allow(clippy::too_many_lines)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn finalize(
        &self,
        par: Par,
        gate: Gate,
        version: Version,
        pnonstd: &mut PseudoNonStdKeywords,
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
                pnonstd.insert_demoted(nonstd, k, v.to_owned());
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
                        pnonstd.insert_demoted(nonstd, k.0, v);
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
                pnonstd.insert_demoted(nonstd, RootKey::Timestep.to_std0(), ts);
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

        let undemoted_pseudostandard = if conf.process_pseudostandard.is_demote() {
            let (demoted, not_demoted): (Vec<_>, Vec<_>) = pstd
                .drain()
                .map(|(k, v)| match k.demote() {
                    Ok(x) => Ok((x, v)),
                    Err(x) => Err((x, TruncatedNEString(v))),
                })
                .partition_result();
            nonstd.extend(demoted);
            not_demoted
        } else {
            vec![]
        };

        if let Some(ne) = NEVec::try_from_vec(errors) {
            LogResult::new_from_ne_err_iter(ne, ()).set_commutative_warnings(warnings)
        } else {
            let ret = ExtraStdKeywords {
                optional,
                hyper_par,
                hyper_gate,
                other_version,
                undemoted_pseudostandard,
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
            let k = RawStdKey::from_optical_only_key(*t, i);
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

    pub(crate) fn set_failure_flag<F: KeywordFailureFlag>(&mut self, k: &RawStdKey, f: F) {
        if let Some(a) = LookupAction::from_flag(f) {
            self.set_lookup_action_seen(k, a);
        }
    }

    pub(crate) fn set_lookup_action_seen(&mut self, k: &RawStdKey, a: LookupAction) {
        match k {
            RawStdKey::Root(rk) => self.root.set_lookup_action_seen(rk, a),
            RawStdKey::Meas(mk) => self.meas.set_lookup_action_seen(mk, a),
            RawStdKey::Gate(gk) => self.gate.set_lookup_action_seen(gk, a),
            RawStdKey::Region(rk) => self.region.set_lookup_action_seen(rk, a),
            RawStdKey::CsvFlag(ck) => self.csv_flag.set_lookup_action_seen(ck, a),
            RawStdKey::Dfc(dk) => self.dfc.set_lookup_action_seen(dk, a),
        }
    }

    fn get_unseen(&self, k: &RawStdKey) -> Option<&NEStr> {
        match k {
            RawStdKey::Root(rk) => self.root.get_unseen(rk),
            RawStdKey::Meas(mk) => self.meas.get_unseen(mk),
            RawStdKey::Gate(gk) => self.gate.get_unseen(gk),
            RawStdKey::Region(rk) => self.region.get_unseen(rk),
            RawStdKey::CsvFlag(ck) => self.csv_flag.get_unseen(ck),
            RawStdKey::Dfc(dk) => self.dfc.get_unseen(dk),
        }
    }

    fn remove_unseen(&mut self, k: &RawStdKey) -> Option<&NEStr> {
        match k {
            RawStdKey::Root(rk) => self.root.remove_unseen(rk),
            RawStdKey::Meas(mk) => self.meas.remove_unseen(mk),
            RawStdKey::Gate(gk) => self.gate.remove_unseen(gk),
            RawStdKey::Region(rk) => self.region.remove_unseen(rk),
            RawStdKey::CsvFlag(ck) => self.csv_flag.remove_unseen(ck),
            RawStdKey::Dfc(dk) => self.dfc.remove_unseen(dk),
        }
    }

    fn parse_unseen<F, X>(&mut self, k: &RawStdKey, f: F) -> Option<X>
    where
        F: FnOnce(&NEStr) -> (Option<LookupAction>, X),
    {
        match k {
            RawStdKey::Root(rk) => self.root.parse_unseen(rk, f),
            RawStdKey::Meas(mk) => self.meas.parse_unseen(mk, f),
            RawStdKey::Gate(gk) => self.gate.parse_unseen(gk, f),
            RawStdKey::Region(rk) => self.region.parse_unseen(rk, f),
            RawStdKey::CsvFlag(ck) => self.csv_flag.parse_unseen(ck, f),
            RawStdKey::Dfc(dk) => self.dfc.parse_unseen(dk, f),
        }
    }
}

#[cfg(feature = "serde")]
impl Serialize for StdKeywords {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let n = self.iter_keywords().count();
        let mut map = serializer.serialize_map(Some(n))?;
        for (k, v) in self.iter_keywords() {
            map.serialize_entry(&DollarWrap::<true, _>(k), v)?;
        }
        map.end()
    }
}

#[cfg(feature = "python")]
mod python {
    use super::StdKeywords;

    use fireflow_types::keys::StdKey;
    use nonempty::NEString;

    use pyo3::{prelude::*, types::PyDict};

    // TODO this is inefficient. Whenever the user wants to use this object like
    // a dict, we need to make a new dict. Gross. Turn this into a real python
    // class with methods that make it act like a dict.
    impl<'py> FromPyObject<'_, 'py> for StdKeywords {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            // Cast to dict rather than going through rust hashmap to preserve
            // order. It will be sorted anyways but this might avoid some
            // overhead since the input will likely be partly grouped.
            let tmp = obj
                .cast::<PyDict>()?
                .iter()
                .map(|(k, v)| Ok((k.extract::<StdKey>()?.0, v.extract::<NEString>()?)))
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
