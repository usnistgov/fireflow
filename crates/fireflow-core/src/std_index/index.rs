use super::nested_string::{NestedEnumString, NestedStringSize, NestedVariableString};

use fireflow_types::{
    index::BiMeasIndex,
    nonempty_string::NEStr,
    std_key::{
        CsvFlagKey, DfcKey, GateKey, IndexedKey, MeasKey, MeasKeyId, RegionKey, RootKey, StdKey,
    },
};

use std::iter;

pub type NestedRoot = NestedEnumString<N_ROOT_KWS, RootKey>;

pub(crate) const N_ROOT_KWS: usize = 52;

pub struct StdIndex {
    root: NestedRoot,
    // TODO it might make sense to break this up into several sub-strings. As is
    // this will have many empty entries, which will make the index vector very
    // long. In practice, most files will have $PnB, $PnR, $PnE, and $PnN, so
    // this index will be efficiently populated. All the others might be sparse.
    // This could be solved by having a double-index that first indexes on the
    // measurement and returns 'none' if there are no keywords for that index.
    // From there it directs to the real index that points to the strings.
    meas: NestedVariableString,
    gate: NestedVariableString,
    region: NestedVariableString,
    csv_flag: NestedVariableString,
    dfc: NestedVariableString,
    dfc_matrix_size: usize,
}

impl StdIndex {
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

    pub fn iter_keys(&self) -> impl Iterator<Item = (StdKey, &str)> {
        let root = self.root.iter_keys().map(|(k, v)| (StdKey::Root(k), v));
        let meas_keys = (0..)
            .flat_map(|i| MeasKey::keys_at(i.into()))
            .map(StdKey::Meas);
        let gate_keys = (0..)
            .flat_map(|i| GateKey::keys_at(i.into()))
            .map(StdKey::Gate);
        let region_keys = (0..)
            .flat_map(|i| RegionKey::keys_at(i.into()))
            .map(StdKey::Region);
        let csv_flag_keys = (0..)
            .map(|i| CsvFlagKey::new(i.into()))
            .map(StdKey::CsvFlag);
        let dfc_keys = (self.dfc_matrix_size..)
            .flat_map(|i0| iter::repeat(i0).zip(self.dfc_matrix_size..))
            .map(|(i0, i1)| DfcKey::new(BiMeasIndex::new(i0.into(), i1.into())))
            .map(StdKey::Dfc);
        root.chain(meas_keys.zip(self.meas.iter()))
            .chain(gate_keys.zip(self.gate.iter()))
            .chain(region_keys.zip(self.region.iter()))
            .chain(csv_flag_keys.zip(self.csv_flag.iter()))
            .chain(dfc_keys.zip(self.dfc.iter()))
    }

    /// Make a new standard key index from a vector of pairs.
    ///
    /// # SAFETY
    ///
    /// Caller must ensure input is sorted and does not have duplicates.
    #[must_use]
    pub unsafe fn from_vec(pairs: Vec<(StdKey, &NEStr)>) -> Self {
        let mut root_n_bytes = 0;
        let mut root_n_strings = 0;
        let mut meas_size = NestedStringSize::default();
        let mut gate_size = NestedStringSize::default();
        let mut region_size = NestedStringSize::default();
        let mut csv_flag_size = NestedStringSize::default();
        let mut dfc_n_bytes = 0;
        let mut dfc_matrix_size = 0;

        for (k, v) in &pairs {
            let n_bytes = v.as_ne_bytes().len().get();
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
        let mut meas = NestedVariableString::init_var(&meas_size);
        let mut gate = NestedVariableString::init_var(&gate_size);
        let mut region = NestedVariableString::init_var(&region_size);
        let mut csv_flag = NestedVariableString::init_var(&csv_flag_size);
        let mut dfc = NestedVariableString::init_var(&dfc_size);

        let mut it = pairs.into_iter();
        let root_it = it
            .by_ref()
            .take(root_n_strings)
            .map(|(k, v)| (RootKey::try_from(k).unwrap(), v));
        // SAFETY: input is sorted and deduplicated
        unsafe {
            root.set_keys(root_it);
        }

        let meas_it = it
            .by_ref()
            .take(meas_size.n_strings)
            .map(|(k, v)| (MeasKey::try_from(k).unwrap().offset(), v));
        // SAFETY: input is sorted and deduplicated
        unsafe {
            meas.extend_pairs(meas_it);
        }

        let gate_it = it
            .by_ref()
            .take(gate_size.n_strings)
            .map(|(k, v)| (GateKey::try_from(k).unwrap().offset(), v));
        // SAFETY: input is sorted and deduplicated
        unsafe {
            gate.extend_pairs(gate_it);
        }

        let region_it = it
            .by_ref()
            .take(region_size.n_strings)
            .map(|(k, v)| (RegionKey::try_from(k).unwrap().offset(), v));
        // SAFETY: input is sorted and deduplicated
        unsafe {
            region.extend_pairs(region_it);
        }

        let csv_flag_it = it
            .by_ref()
            .take(csv_flag_size.n_strings)
            .map(|(k, v)| (usize::from(CsvFlagKey::try_from(k).unwrap().index), v));
        // SAFETY: input is sorted and deduplicated
        unsafe {
            csv_flag.extend_pairs(csv_flag_it);
        }

        let dfc_it = it.map(|(k, v)| {
            let i = DfcKey::try_from(k).unwrap().offset(dfc_matrix_size);
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
            dfc_matrix_size,
        }
    }
}
