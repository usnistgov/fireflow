use crate::validated::shortname::{Shortname, ShortnamePrefix};

use itertools::Itertools;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

#[derive(Clone, Serialize)]
pub enum NamedVec<K, W, U, V> {
    // W is an associated type constructor defined by K, so we need to bind K
    // but won't actually use it, hence phantom hack thing
    Split(SplitVec<W, U, V>, PhantomData<K>),
    Unsplit(UnsplitVec<W, V>),
}

#[derive(Clone, Serialize)]
struct SplitVec<K, U, V> {
    left: DistinctVec<K, V>,
    // TODO boxme
    center: DistinctPair<Shortname, U>,
    right: DistinctVec<K, V>,
    prefix: ShortnamePrefix,
}

#[derive(Clone, Serialize)]
struct UnsplitVec<K, V> {
    members: DistinctVec<K, V>,
    prefix: ShortnamePrefix,
}

type DistinctVec<K, V> = Vec<DistinctPair<K, V>>;

#[derive(Clone, Serialize)]
struct DistinctPair<K, V> {
    key: K,
    value: V,
}

pub type NameMapping = HashMap<Shortname, Shortname>;

impl<K: MightHave, U, V> NamedVec<K, K::Wrapper<Shortname>, U, V> {
    pub fn new(
        xs: Vec<Result<(K::Wrapper<Shortname>, V), (Shortname, U)>>,
        prefix: ShortnamePrefix,
    ) -> Result<NamedVec<K, K::Wrapper<Shortname>, U, V>, String> {
        // TODO these clones shouldn't be necessary
        let names: Vec<K::Wrapper<&Shortname>> = xs
            .iter()
            .map(|x| {
                x.as_ref()
                    .map_or_else(|e| K::into_wrapped(&e.0), |o| K::as_ref(&o.0))
            })
            .collect();
        if prefix.all_unique::<K>(names.into_iter().collect()) {
            let mut left = vec![];
            let mut center = None;
            let mut right = vec![];
            for x in xs {
                match x {
                    Ok(y) => {
                        let p = DistinctPair {
                            key: y.0,
                            value: y.1,
                        };
                        if center.is_none() {
                            left.push(p);
                        } else {
                            right.push(p);
                        }
                    }
                    Err(y) => {
                        if center.is_none() {
                            center = Some(y);
                        } else {
                            return Err("".to_string());
                        }
                    }
                }
            }
            let s = if let Some(c) = center {
                NamedVec::Split(
                    SplitVec {
                        left,
                        center: DistinctPair {
                            key: c.0,
                            value: c.1,
                        },
                        right,
                        prefix,
                    },
                    PhantomData,
                )
            } else {
                NamedVec::Unsplit(UnsplitVec {
                    members: left,
                    prefix,
                })
            };
            Ok(s)
        } else {
            Err("".to_string())
        }
    }

    pub fn as_center(&self) -> Option<(&Shortname, &U)> {
        match self {
            NamedVec::Split(s, _) => Some((&s.center.key, &s.center.value)),
            NamedVec::Unsplit(_) => None,
        }
    }

    pub fn as_prefix(&self) -> &ShortnamePrefix {
        match self {
            NamedVec::Split(s, _) => &s.prefix,
            NamedVec::Unsplit(u) => &u.prefix,
        }
    }

    fn iter_dpairs(&self) -> impl Iterator<Item = &DistinctPair<K::Wrapper<Shortname>, V>> + '_ {
        match self {
            NamedVec::Split(s, _) => s.left.iter().chain(s.right.iter()),
            // chain thing is a dirty hack to get the types to match,
            // unfortunately this doesn't work for mut because it requires two
            // borrows
            NamedVec::Unsplit(u) => u.members.iter().chain(u.members[0..0].iter()),
        }
    }

    pub fn itr_pairs(&self) -> impl Iterator<Item = (&K::Wrapper<Shortname>, &V)> + '_ {
        self.iter_dpairs().map(|x| (&x.key, &x.value))
    }

    pub fn iter_values(&self) -> impl Iterator<Item = &V> + '_ {
        self.iter_dpairs().map(|x| &x.value)
    }

    // pub fn map_values_in_place<F, X>(&mut self, f: F)
    // where
    //     F: Fn(&mut V),
    // {
    //     match self {
    //         NamedVec::Split(s, _) => {
    //             for p in s.left.iter_mut() {
    //                 f(&mut p.value);
    //             }
    //         }
    //         NamedVec::Unsplit(u) => {
    //             for p in u.members.iter_mut() {
    //                 f(&mut p.value);
    //             }
    //         }
    //     }
    // }

    pub fn map_values<E, F, W>(
        self,
        f: F,
    ) -> Result<NamedVec<K, K::Wrapper<Shortname>, U, W>, Vec<E>>
    where
        F: Fn(usize, V) -> Result<W, E>,
    {
        let go = |xs: DistinctVec<K::Wrapper<Shortname>, V>, offset: usize| {
            let (ls, lfail): (Vec<_>, Vec<_>) = xs
                .into_iter()
                .enumerate()
                .map(|(i, p)| {
                    f(i + offset, p.value).map(|value| DistinctPair { key: p.key, value })
                })
                .partition_result();
            (ls, lfail)
        };
        match self {
            NamedVec::Split(s, _) => {
                let nleft = s.left.len();
                let (ls, lfail) = go(s.left, 0);
                let (rs, rfail) = go(s.right, nleft + 1);
                let fail: Vec<_> = lfail.into_iter().chain(rfail).collect();
                if fail.is_empty() {
                    Ok(NamedVec::Split(
                        SplitVec {
                            left: ls,
                            center: s.center,
                            right: rs,
                            prefix: s.prefix,
                        },
                        PhantomData,
                    ))
                } else {
                    Err(fail)
                }
            }
            NamedVec::Unsplit(u) => {
                let (members, fail) = go(u.members, 0);
                if fail.is_empty() {
                    Ok(NamedVec::Unsplit(UnsplitVec {
                        members,
                        prefix: u.prefix,
                    }))
                } else {
                    Err(fail)
                }
            }
        }
    }

    pub fn map_center<F, W>(self, f: F) -> NamedVec<K, K::Wrapper<Shortname>, W, V>
    where
        F: Fn(usize, U) -> W,
    {
        match self {
            NamedVec::Split(s, _) => {
                let i = s.left.len();
                let c = s.center;
                NamedVec::Split(
                    SplitVec {
                        left: s.left,
                        center: DistinctPair {
                            key: c.key,
                            value: f(i, c.value),
                        },
                        right: s.right,
                        prefix: s.prefix,
                    },
                    PhantomData,
                )
            }
            NamedVec::Unsplit(u) => NamedVec::Unsplit(u),
        }
    }

    pub fn iter_keys(&self) -> impl Iterator<Item = &K::Wrapper<Shortname>> + '_ {
        self.iter_dpairs().map(|x| &x.key)
    }

    pub fn len(&self) -> usize {
        self.iter_dpairs().count()
    }

    pub fn non_center_len(&self) -> usize {
        match self {
            NamedVec::Split(s, _) => s.left.len() + s.right.len(),
            NamedVec::Unsplit(u) => u.members.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // fn remove_index_unchecked(&mut self, i: usize) -> (K, V) {
    //     let p = self.members.remove(i);
    //     (p.key, p.value)
    // }
}

impl<K: MightHave + Clone, U, V> NamedVec<K, K::Wrapper<Shortname>, U, V> {
    // pub fn position_by_name(&self, n: &Shortname) -> Option<usize> {
    //     self.iter_keys()
    //         .position(|k| K::as_opt(k).is_some_and(|kn| kn == n))
    // }

    pub fn find_by_name(&self, n: &Shortname) -> Option<&V> {
        self.iter_dpairs()
            .find(|p| K::as_opt(&p.key).is_some_and(|kn| kn == n))
            .map(|p| &p.value)
    }

    /// Remove key/value pair by name of key.
    ///
    /// Return None if name is not found.
    pub fn remove_name(&mut self, n: &Shortname) -> Option<(usize, K::Wrapper<Shortname>, V)> {
        let go = |xs: &mut Vec<_>| {
            if let Some(i) = position_by_name::<K, V>(&xs, n) {
                let p = xs.remove(i);
                Some((i, p.key, p.value))
            } else {
                None
            }
        };
        match self {
            NamedVec::Split(s, _) => go(&mut s.left).or(go(&mut s.right)),
            NamedVec::Unsplit(u) => go(&mut u.members),
        }
    }

    pub fn iter_maybe_names(&self) -> impl Iterator<Item = Option<&Shortname>> + '_ {
        self.iter_dpairs().map(|x| K::as_opt(&x.key))
    }

    pub fn iter_names(&self) -> impl Iterator<Item = Shortname> + '_ {
        let prefix = self.as_prefix();
        self.iter_dpairs()
            .enumerate()
            .map(|(i, x)| K::as_opt(&x.key).cloned().unwrap_or(prefix.as_indexed(i)))
    }

    // TODO add functions to remove/add center value

    // TODO this only can insert a non-center value
    pub fn insert(
        &mut self,
        i: usize,
        key: K::Wrapper<Shortname>,
        value: V,
    ) -> Result<Shortname, DistinctError> {
        let p = self.len();
        let k = self.as_prefix().as_opt_or_indexed::<K>(K::as_ref(&key), i);
        if i > p {
            Err(DistinctError::Index { index: i, len: p })
        } else if self.iter_names().any(|n| n == k) {
            Err(DistinctError::Membership(k))
        } else {
            let p = DistinctPair { key, value };
            match self {
                NamedVec::Split(s, _) => {
                    let ln = s.left.len();
                    if i <= ln {
                        s.left.insert(i, p);
                    } else {
                        s.right.insert(i - ln - 1, p);
                    }
                }
                NamedVec::Unsplit(u) => u.members.insert(i, p),
            }
            Ok(k)
        }
    }

    // this will ONLY be able to set the keys of the non-center members
    pub fn set_non_center_keys(
        &mut self,
        ks: Vec<K::Wrapper<Shortname>>,
    ) -> Result<NameMapping, DistinctKeysError> {
        let new_len = ks.len();
        let old_len = self.non_center_len();
        if new_len != old_len {
            Err(DistinctKeysError::Length { old_len, new_len })
        } else if !self
            .as_prefix()
            .all_unique::<K>(ks.iter().map(K::as_ref).collect())
        {
            Err(DistinctKeysError::NonUnique)
        } else {
            let mut mapping = HashMap::new();
            let mut go = |side: &mut DistinctVec<K::Wrapper<Shortname>, V>,
                          ks_side: Vec<K::Wrapper<Shortname>>| {
                for (p, k) in side.iter_mut().zip(ks_side) {
                    if let (Some(old), Some(new)) = (K::as_opt(&p.key), K::as_opt(&k)) {
                        mapping.insert(old.clone(), new.clone());
                    }
                    p.key = k;
                }
            };
            match self {
                NamedVec::Split(s, _) => {
                    let mut ks_left = ks;
                    let ks_right = ks_left.split_off(s.left.len());
                    go(&mut s.left, ks_left);
                    go(&mut s.right, ks_right);
                }
                NamedVec::Unsplit(u) => go(&mut u.members, ks),
            }
            Ok(mapping)
        }
    }

    // TODO not DRY
    pub fn set_names(&mut self, ns: Vec<Shortname>) -> Result<NameMapping, DistinctKeysError> {
        let new_len = ns.len();
        let old_len = self.len();
        if new_len != old_len {
            Err(DistinctKeysError::Length { old_len, new_len })
        } else if !all_unique(ns.iter()) {
            Err(DistinctKeysError::NonUnique)
        } else {
            let mut mapping = HashMap::new();
            let mut go = |side: &mut DistinctVec<K::Wrapper<Shortname>, V>,
                          ns_side: Vec<Shortname>| {
                for (p, n) in side.iter_mut().zip(ns_side) {
                    if let Some(old) = K::as_opt(&p.key) {
                        mapping.insert(old.clone(), n.clone());
                    }
                    p.key = K::into_wrapped(n);
                }
            };
            match self {
                NamedVec::Split(s, _) => {
                    let mut ns_left = ns;
                    let mut ns_right = ns_left.split_off(s.left.len());
                    // ASSUME this won't fail because we already checked length
                    let n_center = ns_right.pop().unwrap();
                    go(&mut s.left, ns_left);
                    go(&mut s.right, ns_right);
                    mapping.insert(s.center.key.clone(), n_center.clone());
                    s.center.key = n_center;
                }
                NamedVec::Unsplit(u) => go(&mut u.members, ns),
            }
            Ok(mapping)
        }
    }

    // TODO this seems like it could be more general (like "map_keys" or something)
    pub fn try_new_names<J: MightHave>(self) -> Option<NamedVec<J, J::Wrapper<Shortname>, U, V>> {
        let go = |p: DistinctPair<K::Wrapper<Shortname>, V>| {
            let name = K::to_opt(p.key)?;
            let newkey = J::into_wrapped(name);
            Some(DistinctPair {
                key: newkey,
                value: p.value,
            })
        };
        match self {
            NamedVec::Split(s, _) => Some(NamedVec::Split(
                SplitVec {
                    left: s.left.into_iter().map(go).collect::<Option<Vec<_>>>()?,
                    right: s.right.into_iter().map(go).collect::<Option<Vec<_>>>()?,
                    center: s.center,
                    prefix: s.prefix,
                },
                PhantomData,
            )),
            NamedVec::Unsplit(u) => Some(NamedVec::Unsplit(UnsplitVec {
                members: u.members.into_iter().map(go).collect::<Option<Vec<_>>>()?,
                prefix: u.prefix,
            })),
        }
    }
}

pub enum DistinctError {
    Index { index: usize, len: usize },
    Membership(Shortname),
}

pub enum DistinctKeysError {
    Length { old_len: usize, new_len: usize },
    NonUnique,
}

impl fmt::Display for DistinctError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            DistinctError::Index { index, len } => {
                write!(f, "Index must be {len} or less, got {index}")
            }
            DistinctError::Membership(k) => {
                write!(f, "New key named '{k}' already in list")
            }
        }
    }
}

impl fmt::Display for DistinctKeysError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            DistinctKeysError::Length { old_len, new_len } => {
                write!(f, "New key list length must be {old_len} but is {new_len}")
            }
            DistinctKeysError::NonUnique => {
                write!(f, "Keys to be inserted are not unique")
            }
        }
    }
}

pub trait MightHave {
    type Wrapper<T>;

    fn to_opt<T>(x: Self::Wrapper<T>) -> Option<T>;

    fn as_ref<T>(x: &Self::Wrapper<T>) -> Self::Wrapper<&T>;

    fn as_opt<T>(x: &Self::Wrapper<T>) -> Option<&T> {
        Self::to_opt(Self::as_ref(x))
    }

    // TODO this is basically From<T>
    fn into_wrapped<T>(n: T) -> Self::Wrapper<T>;
}

impl ShortnamePrefix {
    fn as_opt_or_indexed<X: MightHave>(&self, x: X::Wrapper<&Shortname>, i: usize) -> Shortname {
        X::to_opt(x).cloned().unwrap_or(self.as_indexed(i))
    }

    fn all_unique<X: MightHave>(&self, xs: Vec<X::Wrapper<&Shortname>>) -> bool {
        all_unique(
            xs.into_iter()
                .enumerate()
                .map(|(i, x)| self.as_opt_or_indexed::<X>(x, i)),
        )
    }
}

fn all_unique<'a, T: Hash + Eq>(xs: impl Iterator<Item = T> + 'a) -> bool {
    let mut unique = HashSet::new();
    for x in xs {
        if unique.contains(&x) {
            return false;
        } else {
            unique.insert(x);
        }
    }
    true
}

pub fn position_by_name<K: MightHave, V>(
    xs: &DistinctVec<K::Wrapper<Shortname>, V>,
    n: &Shortname,
) -> Option<usize> {
    xs.iter()
        .position(|p| K::as_opt(&p.key).is_some_and(|kn| kn == n))
}

// impl<K, U, V> IntoIterator for NamedVec<K, U, V> {
//     type Item = DistinctPair<K, V>;
//     type IntoIter = std::vec::IntoIter<Self::Item>;

//     fn into_iter(self) -> Self::IntoIter {
//         self.members.into_iter()
//     }
// }

// TODO this will probably look really bad
// impl<K: Serialize, U: Serialize, V: Serialize> Serialize for NamedVec<K, U, V> {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         match self {
//             NamedVec::Split(s) => s.serialize(serializer),
//             NamedVec::Unsplit(u) => u.serialize(serializer),
//         }
//     }
// }
