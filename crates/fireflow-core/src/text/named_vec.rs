use crate::validated::nonstandard::MeasIdx;
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

impl<K, W, U, V> Default for NamedVec<K, W, U, V> {
    fn default() -> Self {
        NamedVec::Unsplit(UnsplitVec {
            prefix: ShortnamePrefix::default(),
            members: vec![],
        })
    }
}

/// An error that allows returning of the original value
///
/// This is useful in TryFrom impl's where one wants to try a conversion but
/// re-use the input on failure.
// TODO this probably belongs somewhere else, more general than just this mod
pub struct TryFromErrorReset<E, T> {
    pub error: E,
    pub value: T,
}

pub struct IndexedElement<K, V> {
    pub index: MeasIdx,
    pub key: K,
    pub value: V,
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

/// Encodes a type which might have something in it.
///
/// Intended to be used as a "type family" pattern.
pub trait MightHave {
    /// Concrete wrapper type which might have something
    type Wrapper<T>: From<T>;

    /// If true, the wrapper will always have a value.
    ///
    /// Obviously, the implementation needs to ensure this is in sync with the
    /// meaning of Wrapper<T>.
    const INFALLABLE: bool;

    /// Consume a wrapped value and possibly return its contents.
    ///
    /// If no contents exist, return the original input so the caller can
    /// take back ownership.
    fn to_res<T>(x: Self::Wrapper<T>) -> Result<T, Self::Wrapper<T>>;

    /// Borrow a wrapped value and return a new wrapper with borrowed contents.
    fn as_ref<T>(x: &Self::Wrapper<T>) -> Self::Wrapper<&T>;

    /// Consume a wrapped value and possibly return its contents.
    fn to_opt<T>(x: Self::Wrapper<T>) -> Option<T> {
        Self::to_res(x).ok()
    }

    /// Borrow a wrapped value and possibly return borrowed contents.
    fn as_opt<T>(x: &Self::Wrapper<T>) -> Option<&T> {
        Self::to_opt(Self::as_ref(x))
    }

    /// Consume a value and return it as a wrapped value
    fn into_wrapped<T>(n: T) -> Self::Wrapper<T> {
        n.into()
    }
}

type PairedVec<K, V> = Vec<Pair<K, V>>;

#[derive(Clone, Serialize)]
pub struct Pair<K, V> {
    pub key: K,
    pub value: V,
}

type WrappedNamedVec<K, U, V> = NamedVec<K, <K as MightHave>::Wrapper<Shortname>, U, V>;

type WrappedPair<K, V> = Pair<<K as MightHave>::Wrapper<Shortname>, V>;

type WrappedPairedVec<K, V> = PairedVec<<K as MightHave>::Wrapper<Shortname>, V>;

type Center<U> = Pair<Shortname, U>;

type Either<K, V, U> = Result<(<K as MightHave>::Wrapper<Shortname>, V), (Shortname, U)>;

pub type EitherPair<K, V, U> =
    Result<Pair<<K as MightHave>::Wrapper<Shortname>, V>, Pair<Shortname, U>>;

pub type RawInput<K, U, V> = Vec<Either<K, V, U>>;

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
    pub fn as_center(&self) -> Option<IndexedElement<&Shortname, &U>> {
        match self {
            NamedVec::Split(s, _) => Some(IndexedElement {
                index: s.left.len().into(),
                key: &s.center.key,
                value: &s.center.value,
            }),
            NamedVec::Unsplit(_) => None,
        }
    }

    /// Return mutable reference to center
    pub fn as_center_mut(&mut self) -> Option<IndexedElement<&mut Shortname, &mut U>> {
        match self {
            NamedVec::Split(s, _) => Some(IndexedElement {
                index: s.left.len().into(),
                key: &mut s.center.key,
                value: &mut s.center.value,
            }),
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

    // pub fn into_iter(
    //     self,
    // ) -> impl IntoIterator<Item = (MeasIdx, Result<WrappedPair<K, V>, Pair<Shortname, U>>)> {
    //     let go =
    //         |xs: Vec<WrappedPair<K, V>>| xs.into_iter().enumerate().map(|(i, p)| (i.into(), Ok(p)));
    //     match self {
    //         NamedVec::Split(s, _) => {
    //             let c = (s.left.len().into(), Err(*s.center));
    //             go(s.left).chain(vec![c]).chain(go(s.right))
    //         }
    //         NamedVec::Unsplit(u) => go(u.members).chain(vec![]).chain(go(vec![])),
    //     }
    // }

    /// Return iterator over all elements with indices
    pub fn iter<'a>(
        &'a self,
    ) -> impl Iterator<
        Item = (
            MeasIdx,
            Result<&'a WrappedPair<K, V>, &'a Pair<Shortname, U>>,
        ),
    > + 'a {
        let go =
            |xs: &'a [WrappedPair<K, V>]| xs.iter().enumerate().map(|(i, p)| (i.into(), Ok(p)));
        match self {
            NamedVec::Split(s, _) => {
                let c = (s.left.len().into(), Err(&(*s.center)));
                go(&s.left).chain(vec![c]).chain(go(&s.right))
            }
            NamedVec::Unsplit(u) => go(&u.members).chain(vec![]).chain(go(&u.members[0..0])),
        }
    }

    pub fn iter_common_values<'a, T: 'a>(&'a self) -> impl Iterator<Item = (MeasIdx, &'a T)> + 'a
    where
        U: AsRef<T>,
        V: AsRef<T>,
    {
        self.iter()
            .map(|(i, x)| (i, x.map_or_else(|l| l.value.as_ref(), |r| r.value.as_ref())))
    }

    /// Return iterator over borrowed non-center values
    pub fn iter_non_center_values(&self) -> impl Iterator<Item = (MeasIdx, &V)> + '_ {
        self.iter().flat_map(|(i, x)| x.ok().map(|p| (i, &p.value)))
    }

    /// Return iterator over borrowed non-center keys
    pub fn iter_non_center_keys(&self) -> impl Iterator<Item = &K::Wrapper<Shortname>> + '_ {
        self.iter().flat_map(|(_, x)| x.ok().map(|p| &p.key))
    }

    /// Return all existing names in the vector with their indices
    pub fn indexed_names(&self) -> impl Iterator<Item = (MeasIdx, &Shortname)> + '_ {
        self.iter().flat_map(|(i, r)| {
            r.map_or_else(|x| Some(&x.key), |x| K::as_opt(&x.key))
                .map(|x| (i, x))
        })
    }

    /// Return iterator over key names with non-existent names as default.
    // TODO seems like we should give a different type for this
    pub fn iter_all_names(&self) -> impl Iterator<Item = Shortname> + '_ {
        let prefix = self.as_prefix();
        self.iter().map(|(i, r)| {
            r.map_or_else(
                |x| x.key.clone(),
                |x| K::as_opt(&x.key).cloned().unwrap_or(prefix.as_indexed(i)),
            )
        })
    }

    /// Alter values with a function and payload.
    ///
    /// Center and non-center values will be projected to a common type.
    pub fn alter_common_values_zip<F, X, R, T>(&mut self, xs: Vec<X>, f: F) -> Option<Vec<R>>
    where
        F: Fn(MeasIdx, &mut T, X) -> R,
        U: AsMut<T>,
        V: AsMut<T>,
    {
        self.alter_values_zip(
            xs,
            |v, x| f(v.index, v.value.as_mut(), x),
            |v, x| f(v.index, v.value.as_mut(), x),
        )
    }

    /// Alter values with a function and payload.
    ///
    /// Center and non-center values will be projected to a common type.
    pub fn alter_common_values<F, R, T>(&mut self, f: F) -> Vec<R>
    where
        F: Fn(MeasIdx, &mut T) -> R,
        U: AsMut<T>,
        V: AsMut<T>,
    {
        self.alter_values(
            |v| f(v.index, v.value.as_mut()),
            |v| f(v.index, v.value.as_mut()),
        )
    }

    /// Apply functions to values with payload, altering them in place.
    ///
    /// This will alter all values, including center and non-center values. The
    /// two functions apply to the different values contained. Return None
    /// if input vector is not the same length.
    pub fn alter_values_zip<F, G, X, R>(&mut self, xs: Vec<X>, f: F, g: G) -> Option<Vec<R>>
    where
        F: Fn(IndexedElement<&K::Wrapper<Shortname>, &mut V>, X) -> R,
        G: Fn(IndexedElement<&Shortname, &mut U>, X) -> R,
    {
        let this_len = self.len();
        let other_len = xs.len();
        if this_len != other_len {
            return None;
        }

        let go = |zs: &mut PairedVec<K::Wrapper<Shortname>, V>, ys: Vec<X>, offset: usize| {
            zs.iter_mut()
                .zip(ys)
                .enumerate()
                .map(|(i, (y, x))| {
                    f(
                        IndexedElement {
                            index: (i + offset).into(),
                            key: &y.key,
                            value: &mut y.value,
                        },
                        x,
                    )
                })
                .collect()
        };
        let x = match self {
            NamedVec::Split(s, _) => {
                let nleft = s.left.len();
                let nright = s.right.len();
                let mut it = xs.into_iter();
                let left_r: Vec<_> = go(&mut s.left, it.by_ref().take(nleft).collect(), 0);
                let c = &mut s.center;
                let center_r = g(
                    IndexedElement {
                        index: nleft.into(),
                        key: &c.key,
                        value: &mut c.value,
                    },
                    it.next().unwrap(),
                );
                let right_r: Vec<_> =
                    go(&mut s.right, it.by_ref().take(nright).collect(), 1 + nleft);
                left_r
                    .into_iter()
                    .chain([center_r])
                    .chain(right_r)
                    .collect()
            }
            NamedVec::Unsplit(u) => go(&mut u.members, xs, 0),
        };
        Some(x)
    }

    /// Apply function(s) to all values, altering them in place.
    pub fn alter_values<F, G, R>(&mut self, f: F, g: G) -> Vec<R>
    where
        F: Fn(IndexedElement<&K::Wrapper<Shortname>, &mut V>) -> R,
        G: Fn(IndexedElement<&Shortname, &mut U>) -> R,
    {
        let xs = vec![(); self.len()];
        self.alter_values_zip(xs, |x, _| f(x), |x, _| g(x)).unwrap()
    }

    /// Apply function to non-center values, altering them in place
    pub fn alter_non_center_values<F, X>(&mut self, f: F) -> Vec<X>
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

    /// Apply function to non-center values with values, altering them in place
    pub fn alter_non_center_values_zip<E, F, X>(&mut self, xs: Vec<X>, f: F) -> Option<Vec<E>>
    where
        F: Fn(&mut V, X) -> E,
    {
        let this_len = self.len_non_center();
        let other_len = xs.len();
        if this_len != other_len {
            return None;
        }
        let res = match self {
            NamedVec::Split(s, _) => {
                let nleft = s.left.len();
                let nright = s.right.len();
                let mut it = xs.into_iter();
                let left_r: Vec<_> = s
                    .left
                    .iter_mut()
                    .zip(it.by_ref().take(nleft))
                    .map(|(y, x)| f(&mut y.value, x))
                    .collect();
                let right_r: Vec<_> = s
                    .right
                    .iter_mut()
                    .zip(it.by_ref().take(nright))
                    .map(|(y, x)| f(&mut y.value, x))
                    .collect();
                left_r.into_iter().chain(right_r).collect()
            }
            NamedVec::Unsplit(u) => u
                .members
                .iter_mut()
                .zip(xs)
                .map(|(p, x)| f(&mut p.value, x))
                .collect(),
        };
        Some(res)
    }

    /// Return position of center, if it exists
    pub fn center_index(&self) -> Option<MeasIdx> {
        match self {
            NamedVec::Split(s, _) => Some(s.left.len().into()),
            NamedVec::Unsplit(_) => None,
        }
    }

    /// Apply function over center value, possibly changing it's type
    pub fn map_center_value<F, W>(self, f: F) -> NamedVec<K, K::Wrapper<Shortname>, W, V>
    where
        F: Fn(IndexedElement<&Shortname, U>) -> W,
    {
        match self {
            NamedVec::Split(s, _) => {
                let c = s.center;
                let e = IndexedElement {
                    index: s.left.len().into(),
                    key: &c.key,
                    value: c.value,
                };
                let center = Pair {
                    value: f(e),
                    key: c.key,
                };
                NamedVec::new_split(s.left, center, s.right, s.prefix)
            }
            NamedVec::Unsplit(u) => NamedVec::Unsplit(u),
        }
    }

    /// Apply function over non-center values, possibly changing their type
    pub fn map_non_center_values<E, F, W>(self, f: F) -> Result<WrappedNamedVec<K, U, W>, Vec<E>>
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
        self.iter().count()
    }

    /// Return number of non-center elements.
    pub fn len_non_center(&self) -> usize {
        self.iter().filter(|(_, r)| r.is_ok()).count()
    }

    /// Return true if there are no contained elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get reference at position.
    pub fn get(
        &self,
        index: MeasIdx,
    ) -> Option<Result<(&K::Wrapper<Shortname>, &V), (&Shortname, &U)>> {
        let i: usize = index.into();
        match self {
            NamedVec::Split(s, _) => {
                let left_len = s.left.len();
                match i.cmp(&left_len) {
                    Ordering::Less => Ok(s.left.get(i)).transpose(),
                    Ordering::Equal => Some(Err(&s.center)),
                    Ordering::Greater => Ok(s.left.get(i - left_len - 1)).transpose(),
                }
            }
            NamedVec::Unsplit(u) => Ok(u.members.get(i)).transpose(),
        }
        .map(|x| {
            x.map(|p| (&p.key, &p.value))
                .map_err(|p| (&p.key, &p.value))
        })
    }

    /// Get mutable reference at position.
    #[allow(clippy::type_complexity)]
    pub fn get_mut(
        &mut self,
        index: MeasIdx,
    ) -> Option<Result<(&K::Wrapper<Shortname>, &mut V), (&Shortname, &mut U)>> {
        let i: usize = index.into();
        match self {
            NamedVec::Split(s, _) => {
                let left_len = s.left.len();
                match i.cmp(&left_len) {
                    Ordering::Less => Ok(s.left.get_mut(i)).transpose(),
                    Ordering::Equal => Some(Err(&mut s.center)),
                    Ordering::Greater => Ok(s.left.get_mut(i - left_len - 1)).transpose(),
                }
            }
            NamedVec::Unsplit(u) => Ok(u.members.get_mut(i)).transpose(),
        }
        .map(|x| {
            x.map(|p| (&p.key, &mut p.value))
                .map_err(|p| (&p.key, &mut p.value))
        })
    }

    /// Get reference to value with name.
    pub fn get_name(&self, n: &Shortname) -> Option<(MeasIdx, Result<&V, &U>)> {
        if let Some(c) = self.as_center() {
            if c.key == n {
                return Some((c.index, Err(c.value)));
            }
        }
        self.iter()
            .flat_map(|(i, r)| r.ok().map(|x| (i, x)))
            .find(|(_, p)| K::as_opt(&p.key).is_some_and(|kn| kn == n))
            .map(|(i, p)| (i, Ok(&p.value)))
    }

    /// Get mutable reference to value with name.
    pub fn get_name_mut(&mut self, n: &Shortname) -> Option<(MeasIdx, Result<&mut V, &mut U>)> {
        match self {
            NamedVec::Split(s, _) => {
                let nleft = s.left.len();
                Self::value_by_name_mut(&mut s.left, n)
                    .map(|(i, p)| (i.into(), Ok(p)))
                    .or(if &s.center.key == n {
                        Some((nleft.into(), Err(&mut s.center.value)))
                    } else {
                        None
                    })
                    .or(Self::value_by_name_mut(&mut s.right, n)
                        .map(|(i, p)| ((i + nleft + 1).into(), Ok(p))))
            }
            NamedVec::Unsplit(u) => {
                Self::value_by_name_mut(&mut u.members, n).map(|(i, p)| (i.into(), Ok(p)))
            }
        }
    }

    /// Add a new non-center element at the end of the vector
    pub fn push(
        &mut self,
        key: K::Wrapper<Shortname>,
        value: V,
    ) -> Result<Shortname, DistinctError> {
        let index = self.len().into();
        let k = self
            .as_prefix()
            .as_opt_or_indexed::<K>(K::as_ref(&key), index);
        if self.iter_all_names().any(|n| n == k) {
            Err(DistinctError::Membership(k))
        } else {
            let p = Pair { key, value };
            match self {
                NamedVec::Split(s, _) => {
                    s.right.push(p);
                }
                NamedVec::Unsplit(u) => u.members.push(p),
            }
            Ok(k)
        }
    }

    /// Insert a new non-center element at a given position.
    pub fn insert(
        &mut self,
        index: MeasIdx,
        key: K::Wrapper<Shortname>,
        value: V,
    ) -> Result<Shortname, DistinctError> {
        let len = self.len();
        let k = self
            .as_prefix()
            .as_opt_or_indexed::<K>(K::as_ref(&key), index);
        if usize::from(index) > len {
            Err(DistinctError::Index { index, len })
        } else if self.iter_all_names().any(|n| n == k) {
            Err(DistinctError::Membership(k))
        } else {
            let p = Pair { key, value };
            match self {
                NamedVec::Split(s, _) => {
                    let ln = s.left.len();
                    if usize::from(index) <= ln {
                        s.left.insert(index.into(), p);
                    } else {
                        s.right.insert(usize::from(index) - ln - 1, p);
                    }
                }
                NamedVec::Unsplit(u) => u.members.insert(index.into(), p),
            }
            Ok(k)
        }
    }

    /// Replace a non-center value with a new value at given position.
    ///
    /// Return value that was replaced.
    ///
    /// Return none if index is out of bounds. If index points to the center,
    /// convert it to a non-center value.
    pub fn replace_index(&mut self, index: MeasIdx, value: V) -> Option<Result<V, U>> {
        let len = self.len();
        let i = usize::from(index);
        if i > len {
            None
        } else {
            let (newself, ret) = match mem::replace(self, dummy()) {
                NamedVec::Split(mut s, p) => {
                    let ln = s.left.len();
                    if i <= ln {
                        let ret = mem::replace(&mut s.left[i].value, value);
                        (NamedVec::Split(s, p), Ok(ret))
                    } else if i == ln {
                        let ret = s.center.value;
                        let key = s.center.key;
                        let members = s
                            .left
                            .into_iter()
                            .chain([Pair {
                                key: K::into_wrapped(key),
                                value,
                            }])
                            .chain(s.right)
                            .collect();
                        (Self::new_unsplit(members, s.prefix), Err(ret))
                    } else {
                        let ret = mem::replace(&mut s.left[i - ln - 1].value, value);
                        (NamedVec::Split(s, p), Ok(ret))
                    }
                }
                NamedVec::Unsplit(mut u) => {
                    let ret = mem::replace(&mut u.members[i].value, value);
                    (NamedVec::Unsplit(u), Ok(ret))
                }
            };
            *self = newself;
            Some(ret)
        }
    }

    /// Push a new center element to the end of the vector
    ///
    /// Return error if center already exists.
    pub fn push_center(&mut self, key: Shortname, value: U) -> Result<(), InsertCenterError> {
        if self.iter_all_names().any(|n| n == key) {
            Err(InsertCenterError::Distinct(DistinctError::Membership(key)))
        } else {
            let p = Pair { key, value };
            let (newself, ret) = match mem::replace(self, dummy()) {
                NamedVec::Unsplit(u) => {
                    (NamedVec::new_split(u.members, p, vec![], u.prefix), Ok(()))
                }
                s => (s, Err(InsertCenterError::Present)),
            };
            *self = newself;
            ret
        }
    }

    /// Insert a new center element at a given position.
    ///
    /// Return error if center already exists.
    pub fn insert_center(
        &mut self,
        index: MeasIdx,
        key: Shortname,
        value: U,
    ) -> Result<(), InsertCenterError> {
        let len = self.len();
        if usize::from(index) > len {
            Err(InsertCenterError::Distinct(DistinctError::Index {
                index,
                len,
            }))
        } else if self.iter_all_names().any(|n| n == key) {
            Err(InsertCenterError::Distinct(DistinctError::Membership(key)))
        } else {
            let p = Pair { key, value };
            let (newself, ret) = match mem::replace(self, dummy()) {
                NamedVec::Unsplit(u) => {
                    let mut it = u.members.into_iter();
                    let left: Vec<_> = it.by_ref().take(index.into()).collect();
                    let right: Vec<_> = it.collect();
                    (NamedVec::new_split(left, p, right, u.prefix), Ok(()))
                }
                s => (s, Err(InsertCenterError::Present)),
            };
            *self = newself;
            ret
        }
    }

    /// Remove non-center key/value pair by name of key.
    ///
    /// Return None if index is not found.
    pub fn remove_index(&mut self, index: MeasIdx) -> Option<EitherPair<K, V, U>> {
        let i: usize = index.into();
        let (newself, ret) = match mem::replace(self, dummy()) {
            NamedVec::Split(mut s, p) => {
                let nleft = s.left.len();
                if i < nleft {
                    let x = s.left.remove(i);
                    (NamedVec::Split(s, p), Some(Ok(x)))
                } else if i == nleft {
                    let new = s.left.into_iter().chain(s.right).collect();
                    let ret = Some(Err(*s.center));
                    (NamedVec::new_unsplit(new, s.prefix), ret)
                } else if i - nleft - 1 < s.right.len() {
                    let x = s.right.remove(i);
                    (NamedVec::Split(s, p), Some(Ok(x)))
                } else {
                    (NamedVec::Split(s, p), None)
                }
            }
            NamedVec::Unsplit(mut u) => {
                let x = if i < u.members.len() {
                    let x = u.members.remove(i);
                    Some(Ok(x))
                } else {
                    None
                };
                (NamedVec::Unsplit(u), x)
            }
        };
        *self = newself;
        ret
    }

    /// Remove non-center key/value pair by name of key.
    ///
    /// Return None if name is not found.
    pub fn remove_name(&mut self, n: &Shortname) -> Option<(MeasIdx, Result<V, U>)> {
        let go = |xs: &mut Vec<_>| {
            if let Some(i) = Self::position_by_name(xs, n) {
                let p = xs.remove(i);
                Some((i.into(), p.value))
            } else {
                None
            }
        };
        let (newself, ret) = match mem::replace(self, dummy()) {
            NamedVec::Split(mut s, p) => {
                if let Some((i, v)) = go(&mut s.left).or(go(&mut s.right)) {
                    (NamedVec::Split(s, p), Some((i, Ok(v))))
                } else if &s.center.key == n {
                    let i = s.left.len().into();
                    let xs = s.left.into_iter().chain(s.right).collect();
                    let new = NamedVec::new_unsplit(xs, s.prefix);
                    (new, Some((i, Err(s.center.value))))
                } else {
                    (NamedVec::Split(s, p), None)
                }
            }
            NamedVec::Unsplit(mut u) => {
                let ret = go(&mut u.members);
                (NamedVec::Unsplit(u), ret.map(|(i, v)| (i, Ok(v))))
            }
        };
        *self = newself;
        ret
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
        let center = self.as_center().map(|x| K::into_wrapped(x.key));
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
    /// Return 'Ok(())' if vector is updated, otherwise return errors
    /// encountered when converting non-center to center value. The reverse
    /// process (center to non-center) should not fail.
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
    /// Return true if vector is updated. The conversion process from center
    /// value to non-center value cannot fail.
    pub fn unset_center(&mut self) -> bool
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
    ) -> Option<(usize, &'a mut V)> {
        xs.iter_mut()
            .enumerate()
            .find(|(_, p)| K::as_opt(&p.key).is_some_and(|kn| kn == n))
            .map(|(i, p)| (i, &mut p.value))
    }

    // fn iter_dpairs(&self) -> impl Iterator<Item = &Pair<K::Wrapper<Shortname>, V>> + '_ {
    //     match self {
    //         NamedVec::Split(s, _) => s.left.iter().chain(s.right.iter()),
    //         // chain thing is a dirty hack to get the types to match,
    //         // unfortunately this doesn't work for mut because it requires two
    //         // borrows
    //         NamedVec::Unsplit(u) => u.members.iter().chain(u.members[0..0].iter()),
    //     }
    // }

    // fn iter_non_center_indexed_dpairs<'a>(
    //     &'a self,
    // ) -> impl Iterator<Item = IndexedElement<K::Wrapper<&'a Shortname>, &'a V>> + 'a {
    //     let go = |xs: &'a [WrappedPair<K, V>]| {
    //         xs.iter().enumerate().map(|(i, p)| IndexedElement {
    //             index: MeasIdx(i),
    //             key: K::as_ref(&p.key),
    //             value: &p.value,
    //         })
    //     };
    //     match self {
    //         NamedVec::Split(s, _) => go(&s.left).chain(go(&s.right)),
    //         NamedVec::Unsplit(u) => go(&u.members).chain(go(&u.members[0..0])),
    //     }
    // }

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
    Index { index: MeasIdx, len: usize },
    Membership(Shortname),
}

pub enum InsertCenterError {
    Distinct(DistinctError),
    Present,
}

pub enum DistinctKeysError {
    Length { old_len: usize, new_len: usize },
    NonUnique,
}

impl fmt::Display for InsertCenterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            InsertCenterError::Distinct(d) => d.fmt(f),
            InsertCenterError::Present => {
                write!(f, "Center already exists")
            }
        }
    }
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
    fn as_opt_or_indexed<X: MightHave>(&self, x: X::Wrapper<&Shortname>, i: MeasIdx) -> Shortname {
        X::to_opt(x).cloned().unwrap_or(self.as_indexed(i))
    }

    fn all_unique<X: MightHave>(&self, xs: Vec<X::Wrapper<&Shortname>>) -> bool {
        all_unique(
            xs.into_iter()
                .enumerate()
                .map(|(i, x)| self.as_opt_or_indexed::<X>(x, i.into())),
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
