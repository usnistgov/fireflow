use serde::Serialize;
use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;

// TODO could make this much more useful if I locked down the return type
// of the projection to be a shortname, and also enforced the "default" names
// like X1, X2... here

/// A vector with members that are unique by a key that might exist.
#[derive(Clone)]
pub struct DistinctVec<K, V> {
    members: Vec<DistinctPair<K, V>>,
}

#[derive(Clone, Serialize)]
struct DistinctPair<K, V> {
    key: K,
    value: V,
}

impl<K, V> DistinctVec<K, V> {
    pub fn iter(&self) -> impl Iterator<Item = &V> + '_ {
        self.members.iter().map(|x| &x.value)
    }

    pub fn iter_pairs(&self) -> impl Iterator<Item = (&K, &V)> + '_ {
        self.members.iter().map(|x| (&x.key, &x.value))
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut V> + '_ {
        self.members.iter_mut().map(|x| &mut x.value)
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

    pub fn remove(&mut self, i: usize) -> DistinctPair<K, V> {
        self.members.remove(i)
    }
}

impl<K: DistinctBy, V> DistinctVec<K, V> {
    pub fn iter_projections(&self) -> impl Iterator<Item = Option<&K::Inner>> + '_ {
        self.members.iter().map(|x| x.key.project())
    }

    pub fn from_vec(xs: Vec<(K, V)>) -> Option<Self> {
        if DistinctBy::all_unique(xs.iter().map(|x| &x.0).collect()) {
            Some(DistinctVec {
                members: xs
                    .into_iter()
                    .map(|(key, value)| DistinctPair { key, value })
                    .collect(),
            })
        } else {
            None
        }
    }

    pub fn insert(&mut self, i: usize, key: K, value: V) -> Result<(), DistinctError> {
        let p = self.members.len();
        if i > p {
            Err(DistinctError::IndexError { index: i, len: p })
        } else if self.members.iter().any(|m| {
            m.key
                .project()
                .zip(key.project())
                .is_some_and(|(a, b)| a == b)
        }) {
            Err(DistinctError::MembershipError(K::WHAT))
        } else {
            self.members.insert(i, DistinctPair { key, value });
            Ok(())
        }
    }
}

pub enum DistinctError {
    IndexError { index: usize, len: usize },
    MembershipError(&'static str),
}

impl fmt::Display for DistinctError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            DistinctError::IndexError { index, len } => {
                write!(f, "Index must be {len} or less, got {index}")
            }
            DistinctError::MembershipError(what) => {
                write!(f, "New member does not have a unique {what}")
            }
        }
    }
}

pub trait DistinctBy {
    type Inner: Eq + Hash;
    const WHAT: &'static str;

    fn project(&self) -> Option<&Self::Inner>;

    fn all_unique(xs: Vec<&Self>) -> bool {
        let mut unique = HashSet::new();
        for x in xs {
            if let Some(y) = x.project() {
                if unique.contains(y) {
                    return false;
                } else {
                    unique.insert(y);
                }
            }
        }
        true
    }
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
