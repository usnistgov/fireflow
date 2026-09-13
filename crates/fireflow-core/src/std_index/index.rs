use super::nested_string::{
    IterEnumKeywords, IterKeywords, IterVariableKeywords, NestedEnumString, NestedStringSize,
    NestedVariableString,
};
use crate::validated::keys::TruncatedNEString;

use fireflow_types::{
    nonempty::NEStr,
    std_key::{
        AnyIndex as _, CsvFlagKey, DfcKey, GateKey, MeasKey, N_ROOT, RegionKey, RootKey, StdKey,
    },
};

#[cfg(feature = "serde")]
use serde::{Serialize, Serializer, ser::SerializeMap};

use std::iter::Chain;
use std::mem;

pub type NestedRoot = NestedEnumString<N_ROOT, RootKey>;

pub type IterStdKeywords<'a> = Chain<
    Chain<
        Chain<
            Chain<
                Chain<IterEnumKeywords<'a, N_ROOT, RootKey>, IterVariableKeywords<'a, MeasKey>>,
                IterVariableKeywords<'a, GateKey>,
            >,
            IterVariableKeywords<'a, RegionKey>,
        >,
        IterVariableKeywords<'a, CsvFlagKey>,
    >,
    IterVariableKeywords<'a, DfcKey>,
>;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct StdIndex {
    root: NestedRoot,
    // TODO it might make sense to break this up into several sub-strings. As is
    // this will have many empty entries, which will make the index vector very
    // long. In practice, most files will have $PnB, $PnR, $PnE, and $PnN, so
    // this index will be efficiently populated. All the others might be sparse.
    // This could be solved by having a double-index that first indexes on the
    // measurement and returns 'none' if there are no keywords for that index.
    // From there it directs to the real index that points to the strings.
    meas: NestedVariableString<MeasKey>,
    gate: NestedVariableString<GateKey>,
    region: NestedVariableString<RegionKey>,
    csv_flag: NestedVariableString<CsvFlagKey>,
    dfc: NestedVariableString<DfcKey>,
    dfc_matrix_size: usize,
}

impl Default for StdIndex {
    fn default() -> Self {
        Self {
            root: NestedRoot::init_array(0),
            meas: NestedVariableString::default(),
            gate: NestedVariableString::default(),
            region: NestedVariableString::default(),
            csv_flag: NestedVariableString::default(),
            dfc: NestedVariableString::default(),
            dfc_matrix_size: 0,
        }
    }
}

impl StdIndex {
    #[must_use]
    pub fn get(&self, k: &StdKey) -> &str {
        match k {
            StdKey::Root(rk) => self.root.get0(rk),
            StdKey::Meas(mk) => self.meas.get0(mk),
            StdKey::Gate(gk) => self.gate.get0(gk),
            StdKey::Region(rk) => self.region.get0(rk),
            StdKey::CsvFlag(ck) => self.csv_flag.get0(ck),
            StdKey::Dfc(dk) => self.dfc.get(dk, &self.dfc_matrix_size),
        }
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
            let tmp = self.iter_pairs().chain(other.iter_pairs()).collect();
            Self::from_vec(tmp)
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

    pub fn iter_pairs<'a>(&'a self) -> IterStdKeywords<'a> {
        self.root
            .iter_keywords(&())
            .chain(self.meas.iter_keywords(&()))
            .chain(self.gate.iter_keywords(&()))
            .chain(self.region.iter_keywords(&()))
            .chain(self.csv_flag.iter_keywords(&()))
            .chain(self.dfc.iter_keywords(&self.dfc_matrix_size))
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
        let index = unsafe { StdIndex::from_slice(std_final) };
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
        let mut meas = NestedVariableString::init_var(&meas_size);
        let mut gate = NestedVariableString::init_var(&gate_size);
        let mut region = NestedVariableString::init_var(&region_size);
        let mut csv_flag = NestedVariableString::init_var(&csv_flag_size);
        let mut dfc = NestedVariableString::init_var(&dfc_size);

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
            dfc_matrix_size,
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
impl Serialize for StdIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.n_strings()))?;
        for (k, v) in self.iter_pairs() {
            map.serialize_entry(&k, v)?;
        }
        map.end()
    }
}

#[cfg(feature = "python")]
mod python {
    use super::StdIndex;

    use fireflow_types::{nonempty::NEString, std_key::StdKey};

    use pyo3::{prelude::*, types::PyDict};

    impl<'py> FromPyObject<'_, 'py> for StdIndex {
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

    impl<'py> IntoPyObject<'py> for StdIndex {
        type Target = PyDict;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            // Use dict to preserve order
            let out = PyDict::new(py);
            for (k, v) in self.iter_pairs() {
                let k_ = k.into_pyobject(py)?;
                let v_ = v.to_owned().into_pyobject(py)?;
                out.set_item(k_, v_)?;
            }
            Ok(out)
        }
    }
}
