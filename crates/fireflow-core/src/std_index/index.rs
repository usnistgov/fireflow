use super::{
    masked::{MaskedEnumString, MaskedString, MaskedVariableString, Status},
    nested_string::{NestedEnumString, NestedStringSize, NestedVariableString},
};
use crate::{
    config::EvaledReadDataKeywordsConfig,
    logging::{WarningAndErrorResult, WarningsAndErrorsResult},
    text::keywords::{Gate, Par},
    validated::keys::{AnyKey, NonStdKey, TruncatedNEString, ValueToStdKey},
};

use fireflow_types::{
    case_ins_regex::CaseInsRegex,
    config::{
        KeywordFailureFlag, OpticalOnlyKey, OpticalOnlyKeys, ProcessOpticalOnlyKeys,
        TemporalHasOpticalKeyError,
    },
    index::MeasIndex,
    keystring::{KeyString, KeyStringOrPattern, KeyStringsOrPatterns},
    keywords::Version,
    nonempty::{NEStr, NEString, NEVec},
    std_key::{
        AnyIndex as _, CsvFlagKey, DfcKey, GateKey, GateKeyId, MeasKey, MeasKeyId, N_ROOT,
        PseudoStdKey, RegionKey, RootKey, StdKey,
    },
    sub_pattern::SubPattern,
};

use derive_new::new;
use hashbrown::{HashMap, hash_map::OccupiedEntry};
use itertools::Itertools as _;
use strum::EnumCount as _;
use thiserror::Error;

use std::iter::Chain;
use std::mem;

#[cfg(feature = "serde")]
use serde::{Serialize, Serializer, ser::SerializeMap};

#[cfg(feature = "python")]
use {fireflow_core_proc::DisplayAsPyErr, fireflow_types::python as py, pyo3::prelude::*};

type MaskedRoot<'a> = MaskedEnumString<'a, N_ROOT, RootKey>;

type OpticalOnlyResult = WarningsAndErrorsResult<
    Vec<(StdKey, NEString)>,
    (),
    TemporalHasOpticalKeyError,
    TemporalHasOpticalKeyError,
>;

pub type NestedRoot = NestedEnumString<N_ROOT, RootKey>;

// pub type IterStdKeywords<'a> = Chain<
//     Chain<
//         Chain<
//             Chain<
//                 Chain<IterEnumKeywords<'a, N_ROOT, RootKey>, IterVariableKeywords<'a, (), MeasKey>>,
//                 IterVariableKeywords<'a, (), GateKey>,
//             >,
//             IterVariableKeywords<'a, (), RegionKey>,
//         >,
//         IterVariableKeywords<'a, (), CsvFlagKey>,
//     >,
//     IterVariableKeywords<'a, usize, DfcKey>,
// >;

pub(crate) type DroppedStdKeywords = Vec<(StdKey, NEString)>;
pub(crate) type DroppedPseudoStdKeywords = Vec<(PseudoStdKey, NEString)>;

/// Leftover standard keyword after parsing
#[derive(Clone, new, PartialEq)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct ExtraStdKeywords {
    pub optional: DroppedStdKeywords,
    // pub pseudostandard: DroppedPseudoStdKeywords,
    pub hyper_par: DroppedStdKeywords,
    pub hyper_gate: DroppedStdKeywords,
    pub other_version: DroppedStdKeywords,
    pub timestep: Option<NEString>,
}

#[derive(Clone, Copy)]
pub(crate) enum LookupAction {
    None,
    ErrorDrop,
    ErrorDemote,
}

impl LookupAction {
    // fn key_had_error(&self) -> bool {
    //     !matches!(self, Self::None)
    // }

    pub(crate) fn from_flag<F: KeywordFailureFlag>(flag: F) -> Option<Self> {
        flag.is_demote_or_drop().map(|is_demote| {
            if is_demote {
                Self::ErrorDemote
            } else {
                Self::ErrorDrop
            }
        })
    }
}

/// Error when keyword repair process resulted in colliding non-unique keys.
#[derive(Debug, Error, PartialEq, Clone)]
#[error(
    "the following keys were non-unique when demoting or promoting: {}",
    self.0.iter().join(","),
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::RelationalError))]
pub struct RepairCollisionError(NEVec<AnyKey>);

/// Diagnostic output from repairing the keyword list.
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[allow(clippy::too_many_arguments)]
pub struct RepairDiagnostics {
    /// Standard keys which appear more than once with their values.
    pub non_unique_std: Vec<(StdKey, TruncatedNEString)>,

    /// Non-standard keys which appear more than once with their values.
    pub non_unique_nonstd: Vec<(NonStdKey, TruncatedNEString)>,

    /// Standard keys which were demoted.
    pub demoted: Vec<StdKey>,

    /// Non-standard keys which were promoted.
    pub promoted: Vec<NonStdKey>,

    /// Standard keys which had values that were substituted.
    ///
    /// Values here are the original.
    pub subbed: Vec<(StdKey, TruncatedNEString)>,

    /// Standard keys which had values that were replaced.
    ///
    /// Values here are the original.
    pub replaced: Vec<(StdKey, TruncatedNEString)>,

    /// Standard keys which were renamed.
    ///
    /// First key in pair is the original.
    pub renamed: Vec<(StdKey, StdKey)>,

    /// Standard keys which were ignored.
    pub ignored: Vec<(StdKey, TruncatedNEString)>,

    /// Standard keys which were removed.
    pub removed: Vec<(StdKey, TruncatedNEString)>,
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

pub(crate) struct StdTransaction<'a> {
    root: MaskedRoot<'a>,
    meas: MaskedVariableString<'a, MeasKey, ()>,
    gate: MaskedVariableString<'a, GateKey, ()>,
    region: MaskedVariableString<'a, RegionKey, ()>,
    csv_flag: MaskedVariableString<'a, CsvFlagKey, ()>,
    dfc: MaskedVariableString<'a, DfcKey, usize>,
}

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
    pub(crate) fn from_config(conf: &'a EvaledReadDataKeywordsConfig) -> Self {
        Self {
            promote: KeyMatcher::from_keys(&conf.promote_nonstandard_keys),
            demote: KeyMatcher::from_keys(&conf.demote_standard_keys),
            ignore: KeyMatcher::from_keys(&conf.ignore_standard_keys),
            subs: KeyMatcher::from_keys(&conf.substitute_standard_key_values),
        }
    }
}

impl StdKeywords {
    #[must_use]
    pub fn get(&self, k: &StdKey) -> &str {
        match k {
            StdKey::Root(rk) => self.root.get(rk),
            StdKey::Meas(mk) => self.meas.get(mk),
            StdKey::Gate(gk) => self.gate.get(gk),
            StdKey::Region(rk) => self.region.get(rk),
            StdKey::CsvFlag(ck) => self.csv_flag.get(ck),
            StdKey::Dfc(dk) => self.dfc.get(dk),
        }
    }

    pub fn contains_key(&self, k: &StdKey) -> bool {
        !self.get(k).is_empty()
    }

    pub fn n_strings(&self) -> usize {
        self.root.n_strings()
            + self.meas.n_strings()
            + self.gate.n_strings()
            + self.region.n_strings()
            + self.csv_flag.n_strings()
            + self.dfc.n_strings()
    }

    pub fn append(self, other: Self) -> (Self, Vec<(StdKey, TruncatedNEString)>) {
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

    pub(crate) fn into_transation<'a>(self) -> StdTransaction<'a> {
        StdTransaction {
            root: MaskedString::init_array(self.root),
            meas: MaskedString::init_var(self.meas),
            gate: MaskedString::init_var(self.gate),
            region: MaskedString::init_var(self.region),
            csv_flag: MaskedString::init_var(self.csv_flag),
            dfc: MaskedString::init_var(self.dfc),
        }
    }

    // #[must_use]
    // pub fn get_root(&self, k: RootKey) -> &str {
    //     self.root.get(usize::from(k))
    // }

    // #[must_use]
    // pub fn get_meas(&self, k: MeasKey) -> &str {
    //     self.meas.get(k.meas_offset())
    // }

    // #[must_use]
    // pub fn get_gate(&self, k: GateKey) -> &str {
    //     self.gate.get(k.offset())
    // }

    // #[must_use]
    // pub fn get_region(&self, k: RegionKey) -> &str {
    //     self.region.get(k.offset())
    // }

    // #[must_use]
    // pub fn get_csv_flags(&self, k: CsvFlagKey) -> &str {
    //     self.csv_flag.get(k.index.into())
    // }

    // #[must_use]
    // pub fn get_dfc(&self, k: DfcKey) -> &str {
    //     self.dfc.get(k.offset(self.dfc_matrix_size))
    // }

    pub fn iter_keywords<'a>(&'a self) -> impl Iterator<Item = (StdKey, &NEStr)> {
        self.root
            .iter_std()
            .chain(self.meas.iter_std())
            .chain(self.gate.iter_std())
            .chain(self.region.iter_std())
            .chain(self.csv_flag.iter_std())
            .chain(self.dfc.iter_std())
    }

    pub fn from_vec<V>(mut pairs: Vec<(StdKey, V)>) -> (Self, Vec<(StdKey, TruncatedNEString)>)
    where
        V: AsRef<NEStr>,
    {
        pairs.sort_by_key(|(k, _)| *k);
        let dedup_split = partition_dedup_by_key(&mut pairs, |(k, _)| *k);
        let (std_final, _non_unique_std) = pairs.split_at(dedup_split);
        let non_unique_std = _non_unique_std
            .into_iter()
            .map(|(k, v)| (*k, TruncatedNEString(v.as_ref().to_owned())))
            .collect();

        // SAFETY: we sorted and deduplicated above
        let index = unsafe { StdKeywords::from_slice(std_final) };
        (index, non_unique_std)
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

        let mut it = pairs.into_iter();
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

impl<'a> StdTransaction<'a> {
    pub(crate) fn repair(
        &mut self,
        conf: &EvaledReadDataKeywordsConfig,
    ) -> WarningAndErrorResult<RepairDiagnostics, (), RepairCollisionError, RepairCollisionError>
    {
        unimplemented!()
        // let matchers = AllKeyMatchers::from_config(conf);
        // let mut ignored = vec![];
        // let mut non_unique_std = vec![];
        // let mut non_unique_nonstd = vec![];
        // let mut removed = vec![];
        // let mut replaced = vec![];
        // let mut renamed = vec![];
        // let mut subbed = vec![];
        // let mut demoted = vec![];
        // let mut promoted = vec![];

        // // Update standard keys
        // self.std = mem::take(&mut self.std)
        //     .into_iter()
        //     .filter_map(|(k, v)| {
        //         // TODO this seem inefficient; every std key needs to be
        //         // converted to a string to make this work, which doesn't seem
        //         // right
        //         let ks = k.as_keystring();
        //         if matchers.ignore.is_match(&ks) {
        //             // First remove keys that should be flat-out ignored

        //             // DROP

        //             // ignored.push((k, TruncatedNEString(v)));
        //             // None
        //         } else if matchers.demote.is_match(&ks) {
        //             // Next remove keys that should be demoted and put them
        //             // in non-std.

        //             // DEMOTE

        //             // let nsk = NonStdKey(ks);
        //             // if self.nonstd.contains_key(&nsk) {
        //             //     non_unique_nonstd.push((nsk, TruncatedNEString(v)));
        //             // } else {
        //             //     demoted.push(k);
        //             //     let _ = self.nonstd.insert(nsk, v);
        //             // }
        //             // None
        //         } else if let Some(s) = matchers.subs.get(&ks) {
        //             // Next try to sub the value of keys with matches; this
        //             // might produce a blank key which will effectively remove
        //             // it.

        //             // UPDATE(s)

        //             // if let Ok(vf) = NEString::try_from(s.sub(v.as_str())) {
        //             //     subbed.push((k.clone(), TruncatedNEString(v)));
        //             //     Some((k, vf))
        //             // } else {
        //             //     removed.push((k, TruncatedNEString(v)));
        //             //     None
        //             // }
        //         } else {
        //             Some((k, v))
        //         }
        //     })
        //     .map(|(k, v)| {
        //         // After removing everything we can, update values as needed.

        //         // UPDATE(s)

        //         // let replace = &conf.replace_standard_key_values;
        //         // let ks = k.as_keystring();
        //         // if let Some(vf) = replace.get(&ks).cloned() {
        //         //     replaced.push((k.clone(), TruncatedNEString(v)));
        //         //     (k, vf)
        //         // } else {
        //         //     (k, v)
        //         // }
        //     })
        //     .map(|(k, v)| {
        //         // Finally, rename keys. Assume that this name mapping is
        //         // validated such that we will never get a name collision.
        //         let to_rename = conf.rename_standard_keys.as_ref();
        //         let ks = k.as_keystring();
        //         if let Some(kf) = to_rename.get(&ks).cloned().map(StdKey) {
        //             renamed.push((k, kf.clone()));
        //             (kf, v)
        //         } else {
        //             (k, v)
        //         }
        //     })
        //     .collect();

        // // Update non-standard keys
        // let nonstd_removed = self
        //     .nonstd
        //     .extract_if(|k, _| matchers.promote.is_match(k.as_ref()));

        // for (k, v) in nonstd_removed {
        //     let sk = StdKey(k.0);
        //     if self.std.contains_key(&sk) {
        //         non_unique_std.push((sk, TruncatedNEString(v)));
        //     } else {
        //         promoted.push(NonStdKey(sk.0.clone()));
        //         let _ = self.std.insert(sk, v);
        //     }
        // }

        // let non_unique_appended = conf.append_standard_keywords.iter().filter_map(|(k, v)| {
        //     match self.std.entry(StdKey(k.clone())) {
        //         Entry::Occupied(e) => Some((e.key().clone(), TruncatedNEString(v.clone()))),
        //         Entry::Vacant(e) => {
        //             e.insert(v.clone());
        //             None
        //         }
        //     }
        // });
        // non_unique_std.extend(non_unique_appended);
        // let res = match conf.allow_repair_non_unique.is_error() {
        //     Some(is_err) => {
        //         let ss = non_unique_std.iter().cloned().map(|(k, _)| AnyKey::Std(k));
        //         let ns = non_unique_nonstd
        //             .iter()
        //             .cloned()
        //             .map(|(k, _)| AnyKey::NonStd(k));
        //         let xs = ss.chain(ns).collect();
        //         if let Some(ne) = NEVec::try_from_vec(xs) {
        //             let e = RepairCollisionError(ne);
        //             if is_err {
        //                 LogResult::new_err(e)
        //             } else {
        //                 LogResult::new_ok(()).set_commutative_warnings(Some(e))
        //             }
        //         } else {
        //             LogResult::new_ok(())
        //         }
        //     }
        //     None => LogResult::new_ok(()),
        // };

        // let ret = RepairDiagnostics {
        //     non_unique_std,
        //     non_unique_nonstd,
        //     demoted,
        //     promoted,
        //     subbed,
        //     replaced,
        //     renamed,
        //     ignored,
        //     removed,
        // };
        // res.set_ok_value(ret)
    }

    pub(crate) fn finalize(&self, par: Par, gate: Gate, version: Version) -> ExtraStdKeywords {
        unimplemented!()
        // let n_meas = MeasKeyId::COUNT * usize::from(par);
        // let n_gate = GateKeyId::COUNT * usize::from(gate);
        // let mut optional = vec![];
        // let mut other_version = vec![];
        // let mut timestep = None;

        // for (k, v) in self.root.iter() {
        //     match self.root.get_mask(&k) {
        //         Status::Unseen => {
        //             if matches!(k, RootKey::Timestep) && version > Version::FCS2_0 {
        //                 timestep = Some(v.to_owned());
        //             } else {
        //                 other_version.push((k.into(), v.to_owned()));
        //             }
        //         }
        //         Status::Seen(a) => match a {
        //             LookupAction::None => (),
        //             LookupAction::Demote | LookupAction::Drop => {
        //                 if matches!(k, RootKey::Timestep) && version > Version::FCS2_0 {
        //                     timestep = Some(v.to_owned());
        //                 } else {
        //                     optional.push((k.into(), v.to_owned()))
        //                 }
        //             }
        //         },
        //     }
        // }

        // let mut meas_it = self.meas.iter();

        // for (k, v) in meas_it.by_ref().take(n_meas) {
        //     match self.meas.get_mask(&k) {
        //         Status::Unseen => {
        //             other_version.push((k.into(), v.to_owned()));
        //         }
        //         Status::Seen(a) => match a {
        //             LookupAction::None => (),
        //             LookupAction::Demote | LookupAction::Drop => {
        //                 optional.push((k.into(), v.to_owned()))
        //             }
        //         },
        //     }
        // }

        // let mut hyper_par: Vec<_> = meas_it.map(|(k, v)| (k.into(), v.to_owned())).collect();

        // let mut gate_it = self.gate.iter();

        // for (k, v) in gate_it.by_ref().take(n_gate) {
        //     match self.gate.get_mask(&k) {
        //         Status::Unseen => {
        //             other_version.push((k.into(), v.to_owned()));
        //         }
        //         Status::Seen(a) => match a {
        //             LookupAction::None => (),
        //             LookupAction::Demote | LookupAction::Drop => {
        //                 optional.push((k.into(), v.to_owned()))
        //             }
        //         },
        //     }
        // }

        // let hyper_gate = gate_it.map(|(k, v)| (k.into(), v.to_owned())).collect();

        // // TODO we could also do something like hyper_par/gate with these but
        // // they are hardly used anyways and doing so would be complex
        // for (k, v) in self.region.iter() {
        //     match self.region.get_mask(&k) {
        //         Status::Unseen => {
        //             other_version.push((k.into(), v.to_owned()));
        //         }
        //         Status::Seen(a) => match a {
        //             LookupAction::None => (),
        //             LookupAction::Demote | LookupAction::Drop => {
        //                 optional.push((k.into(), v.to_owned()))
        //             }
        //         },
        //     }
        // }

        // // TODO ditto $CSMODE
        // for (k, v) in self.csv_flag.iter() {
        //     match self.csv_flag.get_mask(&k) {
        //         Status::Unseen => {
        //             other_version.push((k.into(), v.to_owned()));
        //         }
        //         Status::Seen(a) => match a {
        //             LookupAction::None => (),
        //             LookupAction::Demote | LookupAction::Drop => {
        //                 optional.push((k.into(), v.to_owned()))
        //             }
        //         },
        //     }
        // }

        // for (k, v) in self.dfc.iter() {
        //     let is_hyper_par = usize::from(k.index.i0) > usize::from(par)
        //         || usize::from(k.index.i1) > usize::from(par);
        //     match self.dfc.get_mask(&k) {
        //         Status::Unseen => {
        //             if is_hyper_par {
        //                 hyper_par.push((k.into(), v.to_owned()));
        //             } else {
        //                 other_version.push((k.into(), v.to_owned()));
        //             }
        //         }
        //         Status::Seen(a) => match a {
        //             LookupAction::None => (),
        //             LookupAction::Demote | LookupAction::Drop => {
        //                 if is_hyper_par {
        //                     hyper_par.push((k.into(), v.to_owned()));
        //                 } else {
        //                     optional.push((k.into(), v.to_owned()))
        //                 }
        //             }
        //         },
        //     }
        // }

        // ExtraStdKeywords {
        //     optional,
        //     hyper_par,
        //     hyper_gate,
        //     other_version,
        //     timestep,
        // }
    }

    pub(crate) fn remove_optical_only(
        &mut self,
        targets: &[OpticalOnlyKey],
        keys: &OpticalOnlyKeys,
        i: MeasIndex,
        flag: ProcessOpticalOnlyKeys,
    ) -> OpticalOnlyResult {
        unimplemented!()
        // let mut es = vec![];
        // let mut ws = vec![];
        // let mut pairs = vec![];
        // let (demote, warn) = match flag {
        //     ProcessOpticalOnlyKeys::DemoteWarn => (true, true),
        //     ProcessOpticalOnlyKeys::DemoteSilent => (true, false),
        //     ProcessOpticalOnlyKeys::DropWarn => (false, true),
        //     ProcessOpticalOnlyKeys::DropSilent => (false, false),
        // };
        // let action = if demote {
        //     KeywordAction::Demote
        // } else {
        //     KeywordAction::Drop
        // };
        // // TODO it should not be necessary to push and return vectors here.
        // // If we simply not which index is the temporal index, we can recover
        // // all this information when we finalize the index. Keys that were
        // // Seen were present and removed. Keys that were Dropped/Demoted should
        // // be dealt with accordingly. In all cases were can make a list of all
        // // pairs that are present.
        // //
        // // This is in contrast to looking up all other values since in those
        // // cases we need to parse the keywords and therefore record and error if
        // // this fails. This is easier to do at the call site rather than storing
        // // it lazily in the index. Here we only care about the pair and if
        // // it has a non-empty value.
        // for t in targets {
        //     let k = StdKey::from_optical_only_key(*t, i);
        //     if let Some(v) = self.remove(&k) {
        //         let err = || TemporalHasOpticalKeyError::new(i, *t);
        //         if keys.0.contains(t) {
        //             self.set_action_at_key(&k, action);
        //             if warn {
        //                 ws.push(err());
        //             }
        //             pairs.push((k, v));
        //         } else {
        //             es.push(err());
        //         }
        //     }
        // }
        // let mut res = LogResult::new_from_err_iter(es, pairs, ());
        // res.extend_commutative_warnings(ws);
        // res
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
            match k {
                StdKey::Root(rk) => self.root.set_lookup_action(rk, a),
                StdKey::Meas(mk) => self.meas.set_lookup_action(mk, a),
                StdKey::Gate(gk) => self.gate.set_lookup_action(gk, a),
                StdKey::Region(rk) => self.region.set_lookup_action(rk, a),
                StdKey::CsvFlag(ck) => self.csv_flag.set_lookup_action(ck, a),
                StdKey::Dfc(dk) => self.dfc.set_lookup_action(dk, a),
            }
        }
    }

    // pub(crate) fn demote_key(&mut self, k: &StdKey) {
    //     match k {
    //         StdKey::Root(rk) => self.root.demote_unseen(rk),
    //         StdKey::Meas(mk) => self.meas.demote_unseen(mk),
    //         StdKey::Gate(gk) => self.gate.demote_unseen(gk),
    //         StdKey::Region(rk) => self.region.demote_unseen(rk),
    //         StdKey::CsvFlag(ck) => self.csv_flag.demote_unseen(ck),
    //         StdKey::Dfc(dk) => self.dfc.demote_unseen(dk),
    //     }
    // }

    // pub(crate) fn drop_key(&mut self, k: &StdKey) {
    //     match k {
    //         StdKey::Root(rk) => self.root.drop_unseen(rk),
    //         StdKey::Meas(mk) => self.meas.drop_unseen(mk),
    //         StdKey::Gate(gk) => self.gate.drop_unseen(gk),
    //         StdKey::Region(rk) => self.region.drop_unseen(rk),
    //         StdKey::CsvFlag(ck) => self.csv_flag.drop_unseen(ck),
    //         StdKey::Dfc(dk) => self.dfc.drop_unseen(dk),
    //     }
    // }

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

    // fn get_value(&self, k: &StdKey) -> &str {
    //     match k {
    //         StdKey::Root(rk) => self.root.get_value(rk, &()),
    //         StdKey::Meas(mk) => self.meas.get_value(mk, &()),
    //         StdKey::Gate(gk) => self.gate.get_value(gk, &()),
    //         StdKey::Region(rk) => self.region.get_value(rk, &()),
    //         StdKey::CsvFlag(ck) => self.csv_flag.get_value(ck, &()),
    //         StdKey::Dfc(dk) => self.dfc.get_value(dk, &self.dfc_matrix_size),
    //     }
    // }

    // fn get_mask(&self, k: &StdKey) -> &Status {
    //     match k {
    //         StdKey::Root(rk) => self.root.get_mask(rk, &()),
    //         StdKey::Meas(mk) => self.meas.get_mask(mk, &()),
    //         StdKey::Gate(gk) => self.gate.get_mask(gk, &()),
    //         StdKey::Region(rk) => self.region.get_mask(rk, &()),
    //         StdKey::CsvFlag(ck) => self.csv_flag.get_mask(ck, &()),
    //         StdKey::Dfc(dk) => self.dfc.get_mask(dk, &self.dfc_matrix_size),
    //     }
    // }

    // fn get_value_and_mask(&self, k: &StdKey) -> (&str, &Status) {
    //     match k {
    //         StdKey::Root(rk) => self.root.get_value_and_mask(rk),
    //         StdKey::Meas(mk) => self.meas.get_value_and_mask(mk),
    //         StdKey::Gate(gk) => self.gate.get_value_and_mask(gk),
    //         StdKey::Region(rk) => self.region.get_value_and_mask(rk),
    //         StdKey::CsvFlag(ck) => self.csv_flag.get_value_and_mask(ck),
    //         StdKey::Dfc(dk) => self.dfc.get_value_and_mask(dk),
    //     }
    // }

    // fn set_mask(&mut self, k: &StdKey, m: Status<'a>) {
    //     match k {
    //         StdKey::Root(rk) => self.root.set_mask(rk, m),
    //         StdKey::Meas(mk) => self.meas.set_mask(mk, m),
    //         StdKey::Gate(gk) => self.gate.set_mask(gk, m),
    //         StdKey::Region(rk) => self.region.set_mask(rk, m),
    //         StdKey::CsvFlag(ck) => self.csv_flag.set_mask(ck, m),
    //         StdKey::Dfc(dk) => self.dfc.set_mask(dk, m),
    //     }
    // }
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
            map.serialize_entry(&k, v)?;
        }
        map.end()
    }
}

#[cfg(feature = "python")]
mod python {
    use super::StdKeywords;

    use fireflow_types::{nonempty::NEString, std_key::StdKey};

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
                .map(|(k, v)| Ok((k.extract::<StdKey>()?, v.extract::<NEString>()?)))
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
            for (k, v) in self.iter_keywords() {
                let k_ = k.into_pyobject(py)?;
                let v_ = v.to_owned().into_pyobject(py)?;
                out.set_item(k_, v_)?;
            }
            Ok(out)
        }
    }
}
