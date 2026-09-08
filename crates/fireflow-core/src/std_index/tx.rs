use crate::{logging::LogResult, validated::keys::ValueToStdKey};

use super::masked::{MaskedEnumString, MaskedVariableString};

use fireflow_types::{
    config::{
        KeywordFailureFlag, OpticalOnlyKey, OpticalOnlyKeys, ProcessOpticalOnlyKeys,
        TemporalHasOpticalKeyError,
    },
    index::MeasIndex,
    nonempty_string::NEStr,
    std_key::{EnumIndex as _, N_ROOT, RootKey, StdKey},
};

type MaskedRoot = MaskedEnumString<N_ROOT, RootKey, Status>;

#[derive(Clone, Copy)]
enum Status {
    Unseen,
    Seen,
    Deferred(KeywordAction),
}

#[derive(Clone, Copy)]
pub enum KeywordAction {
    Drop,
    Demote,
}

pub struct StdIndexTx {
    root: MaskedRoot,
    meas: MaskedVariableString<Status>,
    gate: MaskedVariableString<Status>,
    region: MaskedVariableString<Status>,
    csv_flag: MaskedVariableString<Status>,
    dfc: MaskedVariableString<Status>,
    dfc_matrix_size: usize,
}

type OpticalOnlyResult = WarningsAndErrorsResult<
    Vec<(StdKey, NEString)>,
    (),
    TemporalHasOpticalKeyError,
    TemporalHasOpticalKeyError,
>;

impl StdIndexTx {
    pub(crate) fn read<K: ValueToStdKey>(&self, i: &K::Index) -> Option<&NEStr> {
        self.read_key(&K::std(i))
    }

    pub(crate) fn remove<K: ValueToStdKey>(&mut self, i: &K::Index) -> Option<&NEStr> {
        self.remove_key(&K::std(i))
    }

    pub(crate) fn remove_and_parse<F, X, K: ValueToStdKey>(
        &mut self,
        i: &K::Index,
        f: F,
    ) -> Option<X>
    where
        F: FnOnce(&NEStr) -> (Option<KeywordAction>, X),
    {
        self.remove_and_parse_key(&K::std(i), f)
    }

    pub(crate) fn set_action_at_key(&mut self, k: &StdKey, a: KeywordAction) {
        self.set_mask(&k, Status::Deferred(a));
    }

    pub(crate) fn demote_key(&mut self, k: &StdKey) {
        self.set_mask(&k, Status::Deferred(KeywordAction::Demote));
    }

    pub(crate) fn drop_key(&mut self, k: &StdKey) {
        self.set_mask(&k, Status::Deferred(KeywordAction::Drop));
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
            KeywordAction::Demote
        } else {
            KeywordAction::Drop
        };
        // TODO it should not be necessary to push and return vectors here.
        // If we simply not which index is the temporal index, we can recover
        // all this information when we finalize the index. Keys that were
        // Seen were present and removed. Keys that were Dropped/Demoted should
        // be dealt with accordingly. In all cases were can make a list of all
        // pairs that are present.
        //
        // This is in contrast to looking up all other values since in those
        // cases we need to parse the keywords and therefore record and error if
        // this fails. This is easier to do at the call site rather than storing
        // it lazily in the index. Here we only care about the pair and if
        // it has a non-empty value.
        for t in targets {
            let k = StdKey::from_optical_only_key(*t, i);
            if let Some(v) = self.remove(&k) {
                let err = || TemporalHasOpticalKeyError::new(i, *t);
                if keys.0.contains(t) {
                    self.set_action_at_key(&k, action);
                    if warn {
                        ws.push(err());
                    }
                    pairs.push((k, v));
                } else {
                    es.push(err());
                }
            }
        }
        let mut res = LogResult::new_from_err_iter(es, pairs, ());
        res.extend_commutative_warnings(ws);
        res
    }

    // fn iter_pairs(&self) -> impl Iterator<Item = (StdKey, &'a NEStr)> {
    //     // TODO not DRY
    //     let root = self.root.iter_keys().map(|(k, v)| (StdKey::Root(k), v));
    //     let meas_keys = (0..)
    //         .flat_map(|i| MeasKey::keys_at(i.into()))
    //         .map(StdKey::Meas);
    //     let gate_keys = (0..)
    //         .flat_map(|i| GateKey::keys_at(i.into()))
    //         .map(StdKey::Gate);
    //     let region_keys = (0..)
    //         .flat_map(|i| RegionKey::keys_at(i.into()))
    //         .map(StdKey::Region);
    //     let csv_flag_keys = (0..)
    //         .map(|i| CsvFlagKey::new(i.into()))
    //         .map(StdKey::CsvFlag);
    //     let dfc_keys = (self.dfc_matrix_size..)
    //         .flat_map(|i0| iter::repeat(i0).zip(self.dfc_matrix_size..))
    //         .map(|(i0, i1)| DfcKey::new(BiMeasIndex::new(i0.into(), i1.into())))
    //         .map(StdKey::Dfc);
    //     root.chain(meas_keys.zip(self.meas.iter()))
    //         .chain(gate_keys.zip(self.gate.iter()))
    //         .chain(region_keys.zip(self.region.iter()))
    //         .chain(csv_flag_keys.zip(self.csv_flag.iter()))
    //         .chain(dfc_keys.zip(self.dfc.iter()))
    // }

    fn read_key(&self, k: &StdKey) -> Option<&NEStr> {
        self.check_unseen(k);
        NEStr::try_new(self.get_value(k))
    }

    fn remove_key(&mut self, k: &StdKey) -> Option<&NEStr> {
        self.check_unseen(k);
        self.set_mask(k, Status::Seen);
        NEStr::try_new(self.get_value(k))
    }

    fn remove_and_parse_key<F, X>(&mut self, k: &StdKey, f: F) -> Option<X>
    where
        F: FnOnce(&NEStr) -> (Option<KeywordAction>, X),
    {
        self.check_unseen(k);
        if let Some(ne) = NEStr::try_new(self.get_value(k)) {
            let (action, ret) = f(ne);
            let new_status = action.map_or(Status::Seen, Status::Deferred);
            self.set_mask(k, new_status);
            Some(ret)
        } else {
            // Mark as seen here so that we can only look up each key once,
            // even if its value does not exist.
            self.set_mask(k, Status::Seen);
            None
        }
    }

    fn get_value(&self, k: &StdKey) -> &str {
        match k {
            StdKey::Root(rk) => self.root.get_value(rk.index()),
            StdKey::Meas(mk) => self.meas.get_value(mk.offset()),
            StdKey::Gate(gk) => self.gate.get_value(gk.offset()),
            StdKey::Region(rk) => self.region.get_value(rk.offset()),
            StdKey::CsvFlag(ck) => self.csv_flag.get_value(ck.index.into()),
            StdKey::Dfc(dk) => self.dfc.get_value(dk.offset(self.dfc_matrix_size)),
        }
    }

    fn get_mask(&self, k: &StdKey) -> &Status {
        match k {
            StdKey::Root(rk) => self.root.get_mask(rk.index()),
            StdKey::Meas(mk) => self.meas.get_mask(mk.offset()),
            StdKey::Gate(gk) => self.gate.get_mask(gk.offset()),
            StdKey::Region(rk) => self.region.get_mask(rk.offset()),
            StdKey::CsvFlag(ck) => self.csv_flag.get_mask(ck.index.into()),
            StdKey::Dfc(dk) => self.dfc.get_mask(dk.offset(self.dfc_matrix_size)),
        }
    }

    fn set_mask(&mut self, k: &StdKey, m: Status) {
        match k {
            StdKey::Root(rk) => self.root.set_mask(rk.index(), m),
            StdKey::Meas(mk) => self.meas.set_mask(mk.offset(), m),
            StdKey::Gate(gk) => self.gate.set_mask(gk.offset(), m),
            StdKey::Region(rk) => self.region.set_mask(rk.offset(), m),
            StdKey::CsvFlag(ck) => self.csv_flag.set_mask(ck.index.into(), m),
            StdKey::Dfc(dk) => self.dfc.set_mask(dk.offset(self.dfc_matrix_size), m),
        }
    }

    fn check_unseen(&self, k: &StdKey) {
        let e = match self.get_mask(k) {
            Status::Unseen => return (),
            Status::Seen => "seen",
            Status::Deferred(KeywordAction::Demote) => "demoted",
            Status::Deferred(KeywordAction::Drop) => "dropped",
        };
        panic!("tried to look up {k} which was already {e}")
    }
}

fn with_unseen_only<X>(a: &Status, k: &StdKey, x: X) -> X {
    let res = match a {
        Status::Unseen => Ok(x),
        Status::Seen => Err("seen"),
        Status::Deferred(KeywordAction::Demote) => Err("demoted"),
        Status::Deferred(KeywordAction::Drop) => Err("dropped"),
    };
    match res {
        Ok(s) => s,
        Err(e) => panic!("tried to look up {k} which was already {e}"),
    }
}

impl KeywordAction {
    pub(crate) fn from_flag<F: KeywordFailureFlag>(flag: F) -> Option<Self> {
        flag.is_demote_or_drop().map(
            |is_demote| {
                if is_demote { Self::Demote } else { Self::Drop }
            },
        )
    }
}
