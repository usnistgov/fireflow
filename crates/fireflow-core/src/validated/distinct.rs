use crate::validated::shortname::{Shortname, ShortnamePrefix};

use itertools::Itertools;
use serde::Serialize;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;

/// A list of potentially named values with an optional "center value".
///
/// Each element is a pair consisting of a key and a value. The key is a
/// wrapper type which may have a name in it. If their is no name, that
/// element has a "default" name which is created from its index and a
/// supplied prefix, which is stored along with the data. Each name (including)
/// these "default" names) must be unique.
///
/// Additionally, up to one element may be designated the "center" value, which
/// must have a name (ie not in the same wrapper type as the others) and can
/// have a value type distinct from the rest.
///
/// All elements, including the center if it exists, are stored in a defined
/// order.
#[derive(Clone, Serialize)]
pub enum NamedVec<K, W, U, V> {
    // W is an associated type constructor defined by K, so we need to bind K
    // but won't actually use it, hence phantom hack thing
    Split(SplitVec<W, U, V>, PhantomData<K>),
    Unsplit(UnsplitVec<W, V>),
}

#[derive(Clone, Serialize)]
pub struct SplitVec<K, U, V> {
    left: PairedVec<K, V>,
    center: Box<Center<U>>,
    right: PairedVec<K, V>,
    prefix: ShortnamePrefix,
}

#[derive(Clone, Serialize)]
pub struct UnsplitVec<K, V> {
    members: PairedVec<K, V>,
    prefix: ShortnamePrefix,
}

pub trait MightHave {
    type Wrapper<T>: From<T>;

    fn to_opt<T>(x: Self::Wrapper<T>) -> Option<T>;

    fn as_ref<T>(x: &Self::Wrapper<T>) -> Self::Wrapper<&T>;

    fn as_opt<T>(x: &Self::Wrapper<T>) -> Option<&T> {
        Self::to_opt(Self::as_ref(x))
    }

    fn into_wrapped<T>(n: T) -> Self::Wrapper<T> {
        n.into()
    }
}

type PairedVec<K, V> = Vec<Pair<K, V>>;

#[derive(Clone, Serialize)]
struct Pair<K, V> {
    key: K,
    value: V,
}

type WrappedNamedVec<K, U, V> = NamedVec<K, <K as MightHave>::Wrapper<Shortname>, U, V>;

type WrappedPair<K, V> = Pair<<K as MightHave>::Wrapper<Shortname>, V>;

type WrappedPairedVec<K, V> = PairedVec<<K as MightHave>::Wrapper<Shortname>, V>;

type Center<U> = Pair<Shortname, U>;

type RawInput<K, U, V> = Vec<Result<(<K as MightHave>::Wrapper<Shortname>, V), (Shortname, U)>>;

pub type NameMapping = HashMap<Shortname, Shortname>;

impl<K: MightHave, U, V> WrappedNamedVec<K, U, V> {
    /// Build new NamedVec using either center or non-center values.
    ///
    /// Must contain either one or zero center values, otherwise return error.
    /// All names within keys (including center) must be unique.
    pub fn new(
        xs: RawInput<K, U, V>,
        prefix: ShortnamePrefix,
    ) -> Result<WrappedNamedVec<K, U, V>, String> {
        let names: Vec<_> = xs
            .iter()
            .map(|x| {
                x.as_ref()
                    .map_or_else(|e| K::into_wrapped(&e.0), |o| K::as_ref(&o.0))
            })
            .collect();
        if prefix.all_unique::<K>(names) {
            let mut left = vec![];
            let mut center = None;
            let mut right = vec![];
            for x in xs {
                match x {
                    Ok(y) => {
                        let p = Pair {
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
                let cp = Pair {
                    key: c.0,
                    value: c.1,
                };
                Self::new_split(left, cp, right, prefix)
            } else {
                Self::new_unsplit(left, prefix)
            };
            Ok(s)
        } else {
            Err("".to_string())
        }
    }

    /// Return reference to center
    pub fn as_center(&self) -> Option<(&Shortname, &U)> {
        match self {
            NamedVec::Split(s, _) => Some((&s.center.key, &s.center.value)),
            NamedVec::Unsplit(_) => None,
        }
    }

    /// Return mutable reference to center
    pub fn as_center_mut(&mut self) -> Option<(&mut Shortname, &mut U)> {
        match self {
            NamedVec::Split(s, _) => Some((&mut s.center.key, &mut s.center.value)),
            NamedVec::Unsplit(_) => None,
        }
    }

    /// Return reference to prefix
    pub fn as_prefix(&self) -> &ShortnamePrefix {
        match self {
            NamedVec::Split(s, _) => &s.prefix,
            NamedVec::Unsplit(u) => &u.prefix,
        }
    }

    /// Set the prefix
    pub fn set_prefix(&mut self, prefix: ShortnamePrefix) {
        match self {
            NamedVec::Split(s, _) => {
                s.prefix = prefix;
            }
            NamedVec::Unsplit(u) => {
                u.prefix = prefix;
            }
        }
    }

    /// Return iterator over borrowed non-center values
    pub fn iter_values(&self) -> impl Iterator<Item = &V> + '_ {
        self.iter_dpairs().map(|x| &x.value)
    }

    /// Return iterator over borrowed non-center keys
    pub fn iter_keys(&self) -> impl Iterator<Item = &K::Wrapper<Shortname>> + '_ {
        self.iter_dpairs().map(|x| &x.key)
    }

    /// Apply function to non-center values, altering them in place
    pub fn alter_values<F, X>(&mut self, f: F) -> Vec<X>
    where
        F: Fn(&mut V) -> X,
    {
        match self {
            NamedVec::Split(s, _) => s
                .left
                .iter_mut()
                .map(|p| f(&mut p.value))
                .chain(s.right.iter_mut().map(|p| f(&mut p.value)))
                .collect(),
            NamedVec::Unsplit(u) => u.members.iter_mut().map(|p| f(&mut p.value)).collect(),
        }
    }

    /// Return position of center, if it exists
    pub fn center_index(&self) -> Option<usize> {
        match self {
            NamedVec::Split(s, _) => Some(s.left.len() + 1),
            NamedVec::Unsplit(_) => None,
        }
    }

    /// Apply function over center value, possibly changing it's type
    pub fn map_center<F, W>(self, f: F) -> NamedVec<K, K::Wrapper<Shortname>, W, V>
    where
        F: Fn(usize, U) -> W,
    {
        match self {
            NamedVec::Split(s, _) => {
                let i = s.left.len();
                let c = s.center;
                let center = Pair {
                    key: c.key,
                    value: f(i, c.value),
                };
                NamedVec::new_split(s.left, center, s.right, s.prefix)
            }
            NamedVec::Unsplit(u) => NamedVec::Unsplit(u),
        }
    }

    /// Apply function over non-center values, possibly changing their type
    pub fn map_values<E, F, W>(self, f: F) -> Result<WrappedNamedVec<K, U, W>, Vec<E>>
    where
        F: Fn(usize, V) -> Result<W, E>,
    {
        let go = |xs: WrappedPairedVec<K, V>, offset: usize| {
            let (ls, lfail): (Vec<_>, Vec<_>) = xs
                .into_iter()
                .enumerate()
                .map(|(i, p)| f(i + offset, p.value).map(|value| Pair { key: p.key, value }))
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
                    Ok(NamedVec::new_split(ls, *s.center, rs, s.prefix))
                } else {
                    Err(fail)
                }
            }
            NamedVec::Unsplit(u) => {
                let (members, fail) = go(u.members, 0);
                if fail.is_empty() {
                    Ok(NamedVec::new_unsplit(members, u.prefix))
                } else {
                    Err(fail)
                }
            }
        }
    }

    /// Return number of all elements.
    pub fn len(&self) -> usize {
        self.iter_dpairs().count()
    }

    /// Return number of non-center elements.
    pub fn len_non_center(&self) -> usize {
        match self {
            NamedVec::Split(s, _) => s.left.len() + s.right.len(),
            NamedVec::Unsplit(u) => u.members.len(),
        }
    }

    /// Return true if there are no contained elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return iterator over key names which may or may not exist.
    pub fn iter_names_opt(&self) -> impl Iterator<Item = Option<&Shortname>> + '_ {
        self.iter_dpairs().map(|x| K::as_opt(&x.key))
    }

    /// Return iterator over key names with non-existent names as default.
    // TODO seems like we should give a different type for this
    pub fn iter_all_names(&self) -> impl Iterator<Item = Shortname> + '_ {
        let prefix = self.as_prefix();
        self.iter_dpairs()
            .enumerate()
            .map(|(i, x)| K::as_opt(&x.key).cloned().unwrap_or(prefix.as_indexed(i)))
    }

    /// Get reference at position.
    ///
    /// If position points to the center element, return None.
    pub fn get(&self, i: usize) -> Option<&V> {
        match self {
            NamedVec::Split(s, _) => {
                let left_len = s.left.len();
                match i.cmp(&left_len) {
                    Ordering::Less => s.left.get(i),
                    Ordering::Equal => None,
                    Ordering::Greater => s.left.get(i - left_len - 1),
                }
            }
            NamedVec::Unsplit(u) => u.members.get(i),
        }
        .map(|p| &p.value)
    }

    /// Get mutable reference at position.
    ///
    /// If position points to the center element, return None.
    pub fn get_mut(&mut self, i: usize) -> Option<&mut V> {
        match self {
            NamedVec::Split(s, _) => {
                let left_len = s.left.len();
                match i.cmp(&left_len) {
                    Ordering::Less => s.left.get_mut(i),
                    Ordering::Equal => None,
                    Ordering::Greater => s.left.get_mut(i - left_len - 1),
                }
            }
            NamedVec::Unsplit(u) => u.members.get_mut(i),
        }
        .map(|p| &mut p.value)
    }

    /// Get reference with name.
    ///
    /// The center element can never be returned.
    pub fn get_name(&self, n: &Shortname) -> Option<&V> {
        self.iter_dpairs()
            .find(|p| K::as_opt(&p.key).is_some_and(|kn| kn == n))
            .map(|p| &p.value)
    }

    /// Get mutable reference with name.
    ///
    /// The center element can never be returned.
    pub fn get_name_mut(&mut self, n: &Shortname) -> Option<&mut V> {
        match self {
            NamedVec::Split(s, _) => {
                Self::value_by_name_mut(&mut s.left, n).or(Self::value_by_name_mut(&mut s.right, n))
            }
            NamedVec::Unsplit(u) => Self::value_by_name_mut(&mut u.members, n),
        }
    }

    /// Insert a new non-center element at a given position.
    pub fn insert(
        &mut self,
        index: usize,
        key: K::Wrapper<Shortname>,
        value: V,
    ) -> Result<Shortname, DistinctError> {
        let len = self.len();
        let k = self
            .as_prefix()
            .as_opt_or_indexed::<K>(K::as_ref(&key), index);
        if index > len {
            Err(DistinctError::Index { index, len })
        } else if self.iter_all_names().any(|n| n == k) {
            Err(DistinctError::Membership(k))
        } else {
            let p = Pair { key, value };
            match self {
                NamedVec::Split(s, _) => {
                    let ln = s.left.len();
                    if index <= ln {
                        s.left.insert(index, p);
                    } else {
                        s.right.insert(index - ln - 1, p);
                    }
                }
                NamedVec::Unsplit(u) => u.members.insert(index, p),
            }
            Ok(k)
        }
    }

    /// Remove key/value pair by name of key.
    ///
    /// Return None if name is not found.
    pub fn remove_name(&mut self, n: &Shortname) -> Option<(usize, K::Wrapper<Shortname>, V)> {
        let go = |xs: &mut Vec<_>| {
            if let Some(i) = Self::position_by_name(xs, n) {
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

    /// Set non-center keys to list
    ///
    /// The center key cannot be replaced by this method since the list will
    /// contain wrapped names which may or may not have a name inside, and
    /// the center value always has a name.
    ///
    /// List must be the same length as all non-center keys and must be unique
    /// (including the center key).
    pub fn set_non_center_keys(
        &mut self,
        ks: Vec<K::Wrapper<Shortname>>,
    ) -> Result<NameMapping, DistinctKeysError> {
        let new_len = ks.len();
        let old_len = self.len_non_center();
        let center = self.as_center().map(|x| K::into_wrapped(x.0));
        let all_keys = ks.iter().map(K::as_ref).chain(center).collect();
        if new_len != old_len {
            Err(DistinctKeysError::Length { old_len, new_len })
        } else if !self.as_prefix().all_unique::<K>(all_keys) {
            Err(DistinctKeysError::NonUnique)
        } else {
            let mut mapping = HashMap::new();
            let mut go = |side: &mut WrappedPairedVec<K, V>,
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

    /// Set all names to list of Shortnames
    ///
    /// This will update the center value along with everything else. Non-center
    /// keys will be wrapped such that they will contain a name.
    ///
    /// Supplied list must be unique and have the same length as the target
    /// vector.
    pub fn set_names(&mut self, ns: Vec<Shortname>) -> Result<NameMapping, DistinctKeysError> {
        let new_len = ns.len();
        let old_len = self.len();
        if new_len != old_len {
            Err(DistinctKeysError::Length { old_len, new_len })
        } else if !all_unique(ns.iter()) {
            Err(DistinctKeysError::NonUnique)
        } else {
            let mut mapping = HashMap::new();
            let mut go = |side: &mut WrappedPairedVec<K, V>, ns_side: Vec<Shortname>| {
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

    // TODO make an indexed version as well?
    /// Set center to be the member with name 'n' if it exists.
    ///
    /// Return true if vector is updated.
    pub fn set_center_by_name<E>(&mut self, n: &Shortname) -> Result<(), E>
    where
        // TODO just make these both tryfrom? it would be more general and I
        // can just blanket the from trait to make a tryfrom<infallible>
        U: TryFrom<V, Error = TryFromErrorReset<E, V>>,
        V: From<U>,
    {
        // ASSUME pop-ing the center won't fail because we already located the
        // target index, therefore we know it exists and has a key with a name
        // in it
        let split_new_center = |i, xs: WrappedPairedVec<K, V>| {
            let mut it = xs.into_iter();
            let new_left: Vec<_> = it.by_ref().take(i).collect();
            match it
                .next()
                .and_then(|p| Self::try_to_center(p).transpose())
                .unwrap()
            {
                Ok(new_center) => Ok((new_left, new_center, it)),
                Err(e) => Err(TryFromErrorReset {
                    error: e.error,
                    value: new_left.into_iter().chain([e.value]).chain(it).collect(),
                }),
            }
        };
        let merge_old_center =
            |xs: WrappedPairedVec<K, V>, c: Center<U>, ys: WrappedPairedVec<K, V>| {
                xs.into_iter()
                    .chain([Self::from_center(c)])
                    .chain(ys)
                    .collect()
            };
        // Use "dummy enum trick" to replace self with a dummy so I can "move"
        // the real value out. The only caveat is that self needs to be replaced
        // with the original value if the manipulations don't need to happen.
        let (newself, ret) = match mem::replace(self, dummy()) {
            NamedVec::Split(s, _) => {
                if let Some(i) = Self::position_by_name(&s.left, n) {
                    match split_new_center(i, s.left) {
                        Err(e) => (
                            Self::new_split(e.value, *s.center, s.right, s.prefix),
                            Err(e.error),
                        ),
                        Ok((left, new_center, l_iter)) => {
                            let right = merge_old_center(l_iter.collect(), *s.center, s.right);
                            (Self::new_split(left, new_center, right, s.prefix), Ok(()))
                        }
                    }
                } else if let Some(i) = Self::position_by_name(&s.right, n) {
                    match split_new_center(i, s.right) {
                        Err(e) => (
                            Self::new_split(s.left, *s.center, e.value, s.prefix),
                            Err(e.error),
                        ),
                        Ok((on_left, center, r_iter)) => {
                            let right = r_iter.collect();
                            let left = merge_old_center(s.left, *s.center, on_left);
                            (Self::new_split(left, center, right, s.prefix), Ok(()))
                        }
                    }
                } else {
                    (NamedVec::Split(s, PhantomData), Ok(()))
                }
            }
            NamedVec::Unsplit(u) => {
                if let Some(i) = Self::position_by_name(&u.members, n) {
                    match split_new_center(i, u.members) {
                        Err(e) => (Self::new_unsplit(e.value, u.prefix), Err(e.error)),
                        Ok((left, center, r_iter)) => {
                            let right = r_iter.collect();
                            (Self::new_split(left, center, right, u.prefix), Ok(()))
                        }
                    }
                } else {
                    (NamedVec::Unsplit(u), Ok(()))
                }
            }
        };
        *self = newself;
        ret
    }

    /// Convert the center element into a non-center element.
    ///
    /// Has no effect if there already is no center element.
    ///
    /// Return true if vector is updated.
    pub fn unset_center<E>(&mut self) -> bool
    where
        V: From<U>,
    {
        let (newself, ret) = match mem::replace(self, dummy()) {
            NamedVec::Split(s, _) => {
                let members = s
                    .left
                    .into_iter()
                    .chain([Self::from_center(*s.center)])
                    .chain(s.right)
                    .collect();
                (Self::new_unsplit(members, s.prefix), true)
            }
            NamedVec::Unsplit(u) => (NamedVec::Unsplit(u), false),
        };
        *self = newself;
        ret
    }

    // TODO this seems like it could be more general (like "map_keys" or something)
    /// Unwrap and rewrap the non-center names of vector.
    ///
    /// This may fail if the original wrapped name cannot be converted.
    pub fn try_rewrapped<J>(self) -> Option<WrappedNamedVec<J, U, V>>
    where
        J: MightHave,
        J::Wrapper<Shortname>: TryFrom<K::Wrapper<Shortname>>,
    {
        let go = |xs: WrappedPairedVec<K, V>| {
            xs.into_iter()
                .map(Self::try_into_wrapper::<J>)
                .collect::<Option<Vec<_>>>()
        };
        let x = match self {
            NamedVec::Split(s, _) => {
                NamedVec::new_split(go(s.left)?, *s.center, go(s.right)?, s.prefix)
            }
            NamedVec::Unsplit(u) => NamedVec::new_unsplit(go(u.members)?, u.prefix),
        };
        Some(x)
    }

    fn try_into_wrapper<J>(p: WrappedPair<K, V>) -> Option<WrappedPair<J, V>>
    where
        J: MightHave,
        J::Wrapper<Shortname>: TryFrom<K::Wrapper<Shortname>>,
    {
        let newkey = p.key.try_into().ok()?;
        Some(Pair {
            key: newkey,
            value: p.value,
        })
    }

    fn try_to_center<E>(
        p: WrappedPair<K, V>,
    ) -> Result<Option<Center<U>>, TryFromErrorReset<E, WrappedPair<K, V>>>
    where
        U: TryFrom<V, Error = TryFromErrorReset<E, V>>,
    {
        match p.value.try_into() {
            Ok(value) => Ok(K::to_opt(p.key).map(|key| Pair { key, value })),
            Err(e) => Err(TryFromErrorReset {
                error: e.error,
                value: Pair {
                    key: p.key,
                    value: e.value,
                },
            }),
        }
    }

    fn from_center(p: Center<U>) -> WrappedPair<K, V>
    where
        V: From<U>,
    {
        Pair {
            key: K::into_wrapped(p.key),
            value: p.value.into(),
        }
    }

    fn position_by_name(xs: &WrappedPairedVec<K, V>, n: &Shortname) -> Option<usize> {
        xs.iter()
            .position(|p| K::as_opt(&p.key).is_some_and(|kn| kn == n))
    }

    fn value_by_name_mut<'a>(
        xs: &'a mut WrappedPairedVec<K, V>,
        n: &Shortname,
    ) -> Option<&'a mut V> {
        xs.iter_mut()
            .find(|p| K::as_opt(&p.key).is_some_and(|kn| kn == n))
            .map(|p| &mut p.value)
    }

    fn iter_dpairs(&self) -> impl Iterator<Item = &Pair<K::Wrapper<Shortname>, V>> + '_ {
        match self {
            NamedVec::Split(s, _) => s.left.iter().chain(s.right.iter()),
            // chain thing is a dirty hack to get the types to match,
            // unfortunately this doesn't work for mut because it requires two
            // borrows
            NamedVec::Unsplit(u) => u.members.iter().chain(u.members[0..0].iter()),
        }
    }

    fn new_split(
        left: WrappedPairedVec<K, V>,
        center: Center<U>,
        right: WrappedPairedVec<K, V>,
        prefix: ShortnamePrefix,
    ) -> Self {
        NamedVec::Split(
            SplitVec {
                left,
                center: Box::new(center),
                right,
                prefix,
            },
            PhantomData,
        )
    }

    fn new_unsplit(members: WrappedPairedVec<K, V>, prefix: ShortnamePrefix) -> Self {
        NamedVec::Unsplit(UnsplitVec { members, prefix })
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

// dummy value to use when mutating NamedVec in place
fn dummy<K, W, U, V>() -> NamedVec<K, W, U, V> {
    NamedVec::Unsplit(UnsplitVec {
        members: vec![],
        prefix: ShortnamePrefix::default(),
    })
}

pub struct TryFromErrorReset<E, T> {
    error: E,
    value: T,
}
