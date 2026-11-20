use fireflow_types::keys::raw_std::RawStdKey;
use itertools::{EitherOrBoth, Itertools as _};
use nonempty::NEStr;

use derive_new::new;
use hashbrown::{HashMap, hash_map::Iter as HashIter};

use std::fmt;
use std::hash::Hash;
use std::iter;
use std::ops::Range;

use super::index::LookupAction;
use super::masked::LookupMask;

pub type SparseString<K> = SparseString_<K, Vec<u8>, ()>;

pub type MaskedSparseString<'a, K, M> = SparseString_<K, &'a [u8], M>;

#[derive(Clone, Debug)]
pub struct SparseString_<K, S, M> {
    inner: S,
    offsets: HashMap<K, (Range<usize>, M)>,
}

#[derive(new, Clone, Copy, Default)]
pub struct SparseStringSize {
    /// The number of bytes to allocate to the inner buffer.
    ///
    /// This is intended for performance.
    pub n_bytes: usize,

    /// The number of offsets to include.
    ///
    /// This must exactly match the indices that will be used. Otherwise
    /// populating with data will panic.
    ///
    /// This is probably more than the number of strings which is equal to the
    /// number of non-empty offsets.
    pub n_offsets: usize,
}

pub(crate) struct Iter<'a, K, S, M> {
    inner: &'a SparseString_<K, S, M>,
    offsets: HashIter<'a, K, (Range<usize>, M)>,
}

pub(crate) type IterStd<'a, K, S, M> =
    iter::Map<Iter<'a, K, S, M>, fn((&K, &'a NEStr)) -> (RawStdKey, &'a NEStr)>;

impl<K> SparseString<K> {
    pub fn init_sparse(size: &SparseStringSize) -> Self {
        let offsets = HashMap::with_capacity(size.n_offsets);
        Self {
            inner: Vec::with_capacity(size.n_bytes),
            offsets,
        }
    }

    pub(crate) fn as_masked<M: Default>(&self) -> MaskedSparseString<'_, K, M>
    where
        K: Hash + Eq + Copy,
    {
        let offsets = self
            .offsets
            .iter()
            .map(|(k, (rng, ()))| (*k, (rng.clone(), M::default())))
            .collect();
        SparseString_ {
            inner: &self.inner[..],
            offsets,
        }
    }

    /// Insert new value into index.
    ///
    /// If index already has value, return new value and leave old unchanged.
    pub(crate) fn try_insert_sparse<V>(&mut self, k: &K, v: V) -> Option<V>
    where
        K: Hash + Eq + Copy,
        V: AsRef<NEStr>,
    {
        if self.offsets.contains_key(k) {
            Some(v)
        } else {
            self.insert_ne(*k, v.as_ref());
            None
        }
    }

    fn insert_ne(&mut self, k: K, v: &NEStr)
    where
        K: Hash + Eq,
    {
        let start = self.inner.len();
        self.inner.extend(v.as_str().as_bytes());
        let end = self.inner.len();
        self.offsets.insert(k, (start..end, ()));
    }

    pub(crate) fn replace_when<V, Fwhen, Fwith, T>(&mut self, mut fwhen: Fwhen, mut fwith: Fwith)
    where
        V: AsRef<NEStr>,
        K: Hash + Eq + Copy,
        Fwhen: FnMut(&K) -> Option<T>,
        Fwith: FnMut(&K, &NEStr, T) -> Option<V>,
    {
        let to_replace: Vec<_> = self.offsets.keys().map(|k| (*k, fwhen(k))).collect();
        for (k, flag) in to_replace {
            if let Some(f) = flag {
                let (rng, ()) = self.offsets.remove(&k).unwrap();
                let v = self.get_range_unchecked(&rng);
                if let Some(new) = fwith(&k, v, f) {
                    self.insert_ne(k, new.as_ref());
                }
            }
        }
    }

    pub(crate) fn retain<F>(&mut self, mut f: F)
    where
        K: Hash + Eq + Copy,
        F: FnMut(&K, &NEStr) -> bool,
    {
        let to_keep: Vec<_> = self.iter().map(|(k, v)| (*k, f(k, v))).collect();
        for (k, keep) in to_keep {
            if !keep {
                let _ = self.offsets.remove(&k);
            }
        }
    }

    /// Remove an entry from the index.
    ///
    /// The old data is not actually removed. Only the range that indexes
    /// the string pool will be reset for the given index.
    ///
    /// Return old data if available. Return none if nothing was deleted.
    pub(crate) fn delete(&mut self, k: &K) -> Option<&NEStr>
    where
        K: Hash + Eq,
    {
        if let Some((rng, ())) = self.offsets.get(k).cloned() {
            let _ = self.offsets.remove(k);
            Some(self.get_range_unchecked(&rng))
        } else {
            None
        }
    }

    /// Put a key/value pair into the index.
    ///
    /// Order does not matter. Duplicates will be tracked and returned with the
    /// first value being kept.
    ///
    /// Values are assumed to reference to non-empty strings.
    pub(crate) fn push_dedup<V>(&mut self, k: &K, v: V) -> Option<V>
    where
        K: Hash + Eq + Copy,
        V: AsRef<NEStr>,
    {
        if self.offsets.contains_key(k) {
            Some(v)
        } else {
            self.insert_ne(*k, v.as_ref());
            None
        }
    }

    /// Put a sequence of key/value pairs in the index.
    ///
    /// Order does not matter. Duplicates will be tracked and returned with the
    /// first value being kept.
    ///
    /// Values are assumed to reference to non-empty strings.
    pub(crate) fn extend_dedup<V>(&mut self, pairs: impl IntoIterator<Item = (K, V)>) -> Vec<(K, V)>
    where
        K: Hash + Eq,
        V: AsRef<NEStr>,
    {
        let mut duplicates = vec![];
        for (k, v) in pairs {
            if self.offsets.contains_key(&k) {
                duplicates.push((k, v));
            } else {
                self.insert_ne(k, v.as_ref());
            }
        }
        duplicates
    }
}

impl<K> MaskedSparseString<'_, K, LookupMask> {
    pub(crate) fn commit_sparse(self) -> SparseString<K>
    where
        K: Copy + Hash + Eq,
    {
        let n_bytes = self.iter_final().map(|(_, v)| v.len().get()).sum();
        let n_offsets = self.len();
        let size = SparseStringSize::new(n_bytes, n_offsets);
        let mut new = SparseString::init_sparse(&size);
        let dups = new.extend_dedup(self.iter_final());
        assert!(dups.is_empty(), "there should be no duplicates");
        new
    }

    pub(crate) fn iter_final(&self) -> impl Iterator<Item = (K, &NEStr)>
    where
        K: Copy,
    {
        self.iter_masked().filter_map(|(k, v, m)| match m {
            LookupMask::Unseen | LookupMask::Seen(LookupAction::None) => Some((k, v)),
            LookupMask::Seen(_) => None,
        })
    }

    pub(crate) fn iter_masked(&self) -> impl Iterator<Item = (K, &NEStr, &LookupMask)>
    where
        K: Copy,
    {
        self.offsets
            .iter()
            .map(|(k, (rng, m))| (*k, self.get_range_unchecked(rng), m))
    }

    pub(crate) fn set_lookup_action_seen(&mut self, k: &K, a: LookupAction)
    where
        K: Eq + Hash,
    {
        assert!(
            self.get(k).is_some(),
            "attempted to set lookup action on empty value"
        );
        match self.get_mask_mut(k) {
            Some(LookupMask::Unseen) => {
                panic!("attempted to set lookup action on unseen value")
            }
            Some(LookupMask::Seen(s)) => *s = a,
            None => panic!("index out of bounds"),
        }
    }

    pub(crate) fn get_unseen(&self, k: &K) -> Option<&NEStr>
    where
        K: Hash + Eq + fmt::Debug,
    {
        let m = self.get_mask(k)?;
        let v = self.get(k)?;
        Some(m.with_unseen(k, v))
    }

    pub(crate) fn remove_unseen(&mut self, k: &K) -> Option<&NEStr>
    where
        K: Hash + Eq + fmt::Debug,
    {
        let (v, m) = self.get_value_and_mask_mut(k)?;
        let ret = m.with_unseen(k, v);
        *m = LookupMask::Seen(LookupAction::None);
        Some(ret)
    }

    pub(crate) fn parse_unseen<F, X>(&mut self, k: &K, f: F) -> Option<X>
    where
        F: FnOnce(&NEStr) -> (Option<LookupAction>, X),
        K: Hash + Eq + fmt::Debug,
    {
        let (v, m) = self.get_value_and_mask_mut(k)?;
        let (action, ret) = f(m.with_unseen(k, v));
        let new_action = action.unwrap_or(LookupAction::None);
        *m = LookupMask::Seen(new_action);
        Some(ret)
    }

    fn get_mask(&self, k: &K) -> Option<LookupMask>
    where
        K: Hash + Eq,
    {
        Some(self.offsets.get(k)?.1)
    }

    fn get_mask_mut(&mut self, k: &K) -> Option<&mut LookupMask>
    where
        K: Hash + Eq,
    {
        Some(&mut self.offsets.get_mut(k)?.1)
    }

    fn get_value_and_mask_mut(&mut self, k: &K) -> Option<(&NEStr, &mut LookupMask)>
    where
        K: Hash + Eq,
    {
        let (rng, m) = self.offsets.get_mut(k)?;
        let v = Self::get_range_unchecked_(&self.inner, rng);
        Some((v, m))
    }
}

impl<K, S, M> SparseString_<K, S, M> {
    pub(crate) fn len(&self) -> usize {
        self.offsets.len()
    }

    pub(crate) fn get(&self, k: &K) -> Option<&NEStr>
    where
        K: Hash + Eq,
        S: AsRef<[u8]>,
    {
        self.offsets
            .get(k)
            .map(|(rng, _)| self.get_range_unchecked(rng))
    }

    pub(crate) fn get_range_unchecked(&self, rng: &Range<usize>) -> &NEStr
    where
        S: AsRef<[u8]>,
    {
        Self::get_range_unchecked_(&self.inner, rng)
    }

    pub(crate) fn get_range_unchecked_<'a>(bytes: &'a S, rng: &Range<usize>) -> &'a NEStr
    where
        S: AsRef<[u8]>,
    {
        let bytes_ = bytes.as_ref();
        // SAFETY: this struct is validated such that each slice is a string
        let s = unsafe { str::from_utf8_unchecked(&bytes_[rng.start..rng.end]) };
        NEStr::try_new(s).expect("range is always non-empty")
    }

    pub(crate) fn iter_std(&self) -> IterStd<'_, K, S, M>
    where
        K: Into<RawStdKey> + Copy,
        S: AsRef<[u8]>,
    {
        self.iter().map(|(k, v)| ((*k).into(), v))
    }

    pub(crate) fn iter(&self) -> Iter<'_, K, S, M>
    where
        S: AsRef<[u8]>,
    {
        Iter {
            inner: self,
            offsets: self.offsets.iter(),
        }
    }
}

impl<K, S: Default, M> Default for SparseString_<K, S, M> {
    fn default() -> Self {
        Self {
            inner: S::default(),
            offsets: HashMap::new(),
        }
    }
}

impl<K, S, M> PartialEq for SparseString_<K, S, M>
where
    K: PartialEq,
    S: PartialEq + AsRef<[u8]>,
    M: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.iter().zip_longest(other.iter()).all(|e| {
            if let EitherOrBoth::Both(x, y) = e {
                x == y
            } else {
                false
            }
        })
    }
}

impl<'a, K, S, M> Iterator for Iter<'a, K, S, M>
where
    S: AsRef<[u8]>,
{
    type Item = (&'a K, &'a NEStr);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((k, (rng, _))) = self.offsets.next() {
            Some((k, self.inner.get_range_unchecked(rng)))
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.offsets.size_hint()
    }
}
