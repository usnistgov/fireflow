use crate::validated::keys::ValueToStdKey;

use super::masked::{MaskedEnumString, MaskedVariableString};

use fireflow_types::{
    config::KeywordFailureFlag,
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

impl StdIndexTx {
    pub fn read<K: ValueToStdKey>(&self, i: &K::Index) -> Option<&NEStr> {
        self.read_key(&K::std(i))
    }

    pub fn remove<K: ValueToStdKey>(&mut self, i: &K::Index) -> Option<&NEStr> {
        self.remove_key(&K::std(i))
    }

    pub fn remove_and_parse<F, X, K: ValueToStdKey>(&mut self, i: &K::Index, f: F) -> Option<X>
    where
        F: FnOnce(&NEStr) -> (Option<KeywordAction>, X),
    {
        self.remove_and_parse_key(&K::std(i), f)
    }

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
