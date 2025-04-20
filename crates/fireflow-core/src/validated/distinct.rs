use crate::validated::shortname::{Shortname, ShortnamePrefix};

use itertools::Itertools;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;

/// A vector with members that are unique by a key that might exist.
#[derive(Clone)]
pub struct DistinctVec<K, V> {
    members: Vec<DistinctPair<K, V>>,
    prefix: ShortnamePrefix,
}

#[derive(Clone, Serialize)]
pub struct DistinctPair<K, V> {
    key: K,
    value: V,
}

pub type NameMapping = HashMap<Shortname, Shortname>;

impl<K, V> DistinctVec<K, V> {
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_ {
        self.members.iter().map(|x| (&x.key, &x.value))
    }

    pub fn iter_values(&self) -> impl Iterator<Item = &V> + '_ {
        self.members.iter().map(|x| &x.value)
    }

    pub fn iter_values_mut(&mut self) -> impl Iterator<Item = &mut V> + '_ {
        self.members.iter_mut().map(|x| &mut x.value)
    }

    pub fn map_values<E, F, U>(self, f: F) -> Result<DistinctVec<K, U>, Vec<E>>
    where
        F: Fn(usize, V) -> Result<U, E>,
    {
        let (members, fail): (Vec<_>, Vec<_>) = self
            .members
            .into_iter()
            .enumerate()
            .map(|(i, p)| f(i, p.value).map(|value| DistinctPair { key: p.key, value }))
            .partition_result();
        if fail.is_empty() {
            Ok(DistinctVec {
                members,
                prefix: self.prefix,
            })
        } else {
            Err(fail)
        }
    }

    pub fn iter_keys(&self) -> impl Iterator<Item = &K> + '_ {
        self.members.iter().map(|x| &x.key)
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    fn remove_index_unchecked(&mut self, i: usize) -> (K, V) {
        let p = self.members.remove(i);
        (p.key, p.value)
    }
}

impl<K: IntoShortname, V> DistinctVec<K, V> {
    pub fn position_by_name(&self, n: &Shortname) -> Option<usize> {
        self.iter_keys()
            .position(|k| k.as_name_opt().is_some_and(|kn| kn == n))
    }

    pub fn find_by_name(&self, n: &Shortname) -> Option<&V> {
        self.iter()
            .find(|(k, _)| k.as_name_opt().is_some_and(|kn| kn == n))
            .map(|p| p.1)
    }

    pub fn remove_name(&mut self, n: &Shortname) -> Option<(usize, K, V)> {
        if let Some(i) = self.position_by_name(n) {
            let (k, v) = self.remove_index_unchecked(i);
            Some((i, k, v))
        } else {
            None
        }
    }

    pub fn iter_maybe_names(&self) -> impl Iterator<Item = Option<&Shortname>> + '_ {
        self.members.iter().map(|x| x.key.as_name_opt())
    }

    pub fn iter_names(&self) -> impl Iterator<Item = Shortname> + '_ {
        self.members.iter().enumerate().map(|(i, x)| {
            x.key
                .as_name_opt()
                .cloned()
                .unwrap_or(self.prefix.as_indexed(i))
        })
    }

    pub fn from_vec(xs: Vec<(K, V)>, prefix: ShortnamePrefix) -> Option<Self> {
        if IntoShortname::all_unique(xs.iter().map(|x| &x.0).collect(), &prefix) {
            Some(DistinctVec {
                prefix,
                members: xs
                    .into_iter()
                    .map(|(key, value)| DistinctPair { key, value })
                    .collect(),
            })
        } else {
            None
        }
    }

    pub fn insert(&mut self, i: usize, key: K, value: V) -> Result<Shortname, DistinctError> {
        let p = self.members.len();
        let k = key.into_name(i, &self.prefix);
        if i > p {
            Err(DistinctError::Index { index: i, len: p })
        } else if self.iter_names().any(|n| n == k) {
            Err(DistinctError::Membership(k))
        } else {
            self.members.insert(i, DistinctPair { key, value });
            Ok(k)
        }
    }

    pub fn set_keys(&mut self, ks: Vec<K>) -> Result<NameMapping, DistinctKeysError> {
        let new_len = ks.len();
        let old_len = self.members.len();
        if new_len != old_len {
            Err(DistinctKeysError::Length { old_len, new_len })
        } else if !IntoShortname::all_unique(ks.iter().collect(), &self.prefix) {
            Err(DistinctKeysError::NonUnique)
        } else {
            let mut mapping = HashMap::new();
            for (p, k) in self.members.iter_mut().zip(ks) {
                if let (Some(old), Some(new)) = (p.key.as_name_opt(), k.as_name_opt()) {
                    mapping.insert(old.clone(), new.clone());
                }
                p.key = k;
            }
            Ok(mapping)
        }
    }

    pub fn set_names(&mut self, ns: Vec<Shortname>) -> Result<NameMapping, DistinctKeysError> {
        let new_len = ns.len();
        let old_len = self.members.len();
        if new_len != old_len {
            Err(DistinctKeysError::Length { old_len, new_len })
        } else if !all_unique(ns.iter()) {
            Err(DistinctKeysError::NonUnique)
        } else {
            let mut mapping = HashMap::new();
            for (p, n) in self.members.iter_mut().zip(ns) {
                if let Some(old) = p.key.as_name_opt() {
                    mapping.insert(old.clone(), n.clone());
                }
                p.key = IntoShortname::from_name(n);
            }
            Ok(mapping)
        }
    }

    pub fn try_new_names<J: IntoShortname>(self) -> Option<DistinctVec<J, V>> {
        let mut new = vec![];
        for p in self.members {
            let name = p.key.as_name_opt()?;
            let newkey: J = IntoShortname::from_name(name.clone());
            new.push((newkey, p.value));
        }
        // This probably isn't necessary, but there is not guarantee based on
        // the conversion above that the values of the keys won't change. For
        // the use cases here we could assume that the only difference will be
        // if Shortname is wrapped in OptionalKw, in which case the only thing
        // that we need to worry about are collisions brought about by None's.
        DistinctVec::from_vec(new, self.prefix)
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

pub trait IntoShortname {
    fn as_name_opt(&self) -> Option<&Shortname>;

    fn from_name(n: Shortname) -> Self;

    fn into_name(&self, i: usize, prefix: &ShortnamePrefix) -> Shortname {
        self.as_name_opt().cloned().unwrap_or(prefix.as_indexed(i))
    }

    fn all_unique(xs: Vec<&Self>, prefix: &ShortnamePrefix) -> bool {
        all_unique(xs.iter().enumerate().map(|(i, x)| x.into_name(i, &prefix)))
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

impl<K, V> IntoIterator for DistinctVec<K, V> {
    type Item = DistinctPair<K, V>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.members.into_iter()
    }
}

impl<K: Serialize, V: Serialize> Serialize for DistinctVec<K, V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.members.serialize(serializer)
    }
}
