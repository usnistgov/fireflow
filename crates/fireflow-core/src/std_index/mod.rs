mod nested_string;

use itertools::Itertools;
use nested_string::{NestedEnumString, NestedStringSize, NestedVariableString};

use fireflow_types::{
    nonempty_string::NEStr,
    std_key::{
        CsvFlagKey, DfcKey, GateKey, GateKeySuffix, IndexedKey, MeasKey, MeasKeyBase, RegionKey,
        RegionKeySuffix, RootKey, StdKey,
    },
};

use derive_new::new;
use strum::{EnumCount, IntoEnumIterator};

use std::iter;

pub type NestedRoot = NestedEnumString<52, RootKey>;

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

#[derive(new, Default)]
pub struct StdIndexSize {
    root_len: usize,
    meas_size: NestedStringSize,
    gate_size: NestedStringSize,
    region_size: NestedStringSize,
    csv_flag_size: NestedStringSize,
    dfc_size: NestedStringSize,
}

impl StdIndex {
    fn iter_keys(&self) -> impl Iterator<Item = (StdKey, &str)> {
        let root = self.root.iter_keys().map(|(k, v)| (StdKey::Root(k), v));
        let meas = (0..)
            .flat_map(|i| {
                MeasKeyBase::iter()
                    .zip(iter::repeat(i.into()))
                    .map(|(b, j)| IndexedKey::new(j, b))
            })
            .map(StdKey::Meas)
            .zip(self.meas.iter());
        let gate = (0..)
            .flat_map(|i| GateKey::keys_at(i.into()))
            .map(StdKey::Gate)
            .zip(self.gate.iter());
        let region = (0..)
            .flat_map(|i| RegionKey::keys_at(i.into()))
            .map(StdKey::Region)
            .zip(self.gate.iter());
        let csv_flag = (0..)
            .map(|i| CsvFlagKey { index: i.into() })
            .map(StdKey::CsvFlag)
            .zip(self.csv_flag.iter());
        let dfc = (self.dfc_matrix_size..)
            .flat_map(|i0| {
                (self.dfc_matrix_size..)
                    .zip(iter::repeat(i0))
                    .map(|(j0, j1)| DfcKey::new(j0.into(), j1.into()))
            })
            .map(StdKey::Dfc)
            .zip(self.dfc.iter());
        root.chain(meas)
            .chain(gate)
            .chain(region)
            .chain(csv_flag)
            .chain(dfc)
    }

    /// # SAFETY
    ///
    /// Caller must ensure input is sorted and does not have duplicates.
    unsafe fn from_vec<'a>(pairs: Vec<(StdKey, &'a NEStr)>) -> Self {
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
                    let i0 = usize::from(dk.index0);
                    let i1 = usize::from(dk.index1);
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
            .map(|(k, v)| (RootKey::try_from(k).expect("keys are in order"), v));
        // SAFETY: input is sorted and deduplicated
        unsafe {
            root.set_keys(root_it);
        }

        let meas_it = it.by_ref().take(meas_size.n_strings).map(|(k, v)| {
            let i = MeasKey::try_from(k)
                .expect("keys are in order")
                .meas_offset();
            (i, v)
        });
        // SAFETY: input is sorted and deduplicated
        unsafe {
            meas.extend_pairs(meas_it);
        }

        let gate_it = it
            .by_ref()
            .take(gate_size.n_strings)
            .map(|(k, v)| (GateKey::try_from(k).expect("keys are in order").offset(), v));
        // SAFETY: input is sorted and deduplicated
        unsafe {
            gate.extend_pairs(gate_it);
        }

        let region_it = it.by_ref().take(region_size.n_strings).map(|(k, v)| {
            let i = RegionKey::try_from(k).expect("keys are in order").offset();
            (i, v)
        });
        // SAFETY: input is sorted and deduplicated
        unsafe {
            region.extend_pairs(region_it);
        }

        let csv_flag_it = it.by_ref().take(csv_flag_size.n_strings).map(|(k, v)| {
            let i = CsvFlagKey::try_from(k).expect("keys are in order").index;
            (usize::from(i), v)
        });
        // SAFETY: input is sorted and deduplicated
        unsafe {
            csv_flag.extend_pairs(csv_flag_it);
        }

        let dfc_it = it.map(|(k, v)| {
            let i = DfcKey::try_from(k)
                .expect("keys are in order")
                .offset(dfc_matrix_size);
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
