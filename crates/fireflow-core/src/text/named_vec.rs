use crate::logging::{
    CommutativeResult, CommutativeResultIter as _, ErrorGroup, ErrorResult, ErrorsResult,
    LogResult, ResultExt as _,
};
use crate::macros::def_summary;
use crate::text::index::{BoundaryIndexError, IndexError, IndexFromOne, MeasIndex};
use crate::text::optional::MightHave;
use crate::validated::shortname::Shortname;

use type_families::{
    BifunctorOnce, Functor, Monoid, Pointed, impl_functor_once, impl_kind1, impl_kind2,
};

use derive_more::{Display, From, Into};
use derive_new::new;
use itertools::Itertools as _;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::hash::Hash;
use std::mem;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
use fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr};

use Ordering::{Equal, Greater, Less};

/// A list of potentially named values with an optional "center value".
///
/// Each element is a pair consisting of a key and a value. The key is a
/// wrapper type which may have a name in it. If there is no name, that
/// element has a default name of "Pn" where "n" is the index starting at 1.
/// Each name (including) these "default" names) must be unique.
///
/// Additionally, up to one element may be designated the "center" value, which
/// must have a name (ie not in the same wrapper type as the others) and can
/// have a value type distinct from the rest.
///
/// All elements, including the center if it exists, are stored in a defined
/// order.
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum NamedVec<K, U, V> {
    // W is an associated type constructor defined by K, so we need to bind K
    // but won't actually use it, hence phantom hack thing
    Split(SplitVec<K, U, V>),
    Unsplit(UnsplitVec<K, V>),
}

impl<K, U, V> Default for NamedVec<K, U, V> {
    fn default() -> Self {
        Self::Unsplit(UnsplitVec { members: vec![] })
    }
}

/// An key/value pair with an index
#[derive(new)]
pub struct IndexedElement<K, V> {
    pub index: MeasIndex,
    pub key: K,
    pub value: V,
}

/// Inner type for [`NamedVec`] which has a center element
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct SplitVec<K, U, V> {
    left: PairedVec<K, V>,
    center: Box<Center<U>>,
    right: PairedVec<K, V>,
}

/// Inner type for [`NamedVec`] which does not have a center element
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct UnsplitVec<K, V> {
    members: PairedVec<K, V>,
}

/// A member in [`NamedVec`], either a "center" or "non-center" value
#[derive(Clone)]
#[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
pub enum Element<U, V> {
    Center(U),
    NonCenter(V),
}

impl_kind2!(ElementFamily, Element);

/// Standalone wrapper representing a center value in [`NamedVec`]
#[derive(Clone, From, Into)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct NonCenterElement<V>(pub Element<(), V>);

impl_kind1!(NonCenterElementFamily, NonCenterElement);
impl_functor_once!(
    NonCenterElement,
    self,
    f,
    NonCenterElement(self.0.second_once(f))
);

type PairedVec<K, V> = Vec<Pair<K, V>>;

/// A key/value pair
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Pair<K, V> {
    pub key: K,
    pub value: V,
}

/// All names from [`NamedVec`] in a set-like structure.
#[derive(new)]
#[new(visibility = "pub(crate)")]
pub struct NamedSet<'a> {
    center: Option<&'a Shortname>,
    non_center: HashSet<&'a Shortname>,
}

type Center<U> = Pair<Shortname, U>;

type Either<K, U, V> = Element<(Shortname, U), (K, V)>;

pub type EitherPair<K, U, V> = Element<Pair<Shortname, U>, Pair<K, V>>;

pub type Eithers<K, U, V> = Vec<Either<K, U, V>>;

pub type NameMapping = HashMap<Shortname, Shortname>;

impl NamedSet<'_> {
    pub(crate) fn contains_non_center_name(&self, name: &Shortname) -> bool {
        self.non_center.contains(name)
    }

    pub(crate) fn contains_any_name(&self, name: &Shortname) -> bool {
        self.contains_non_center_name(name) || self.center.as_ref().is_some_and(|n| n == &name)
    }
}

impl<K, U, V> NamedVec<K, U, V> {
    /// Build new NamedVec using either center or non-center values.
    ///
    /// Must contain either one or zero center values, otherwise return error.
    /// All names within keys (including center) must be unique.
    pub(crate) fn try_new(xs: Eithers<K, U, V>) -> Result<Self, NewNamedVecError>
    where
        K: MightHave<Shortname>,
    {
        let names = xs
            .iter()
            .map(|x| x.as_ref().both(|e| Some(&e.0), |o| o.0.as_opt()));
        if !all_unique_names(names) {
            return Err(NonUniqueKeysError.into());
        }
        let mut left = vec![];
        let mut center = None;
        let mut right = vec![];
        for x in xs {
            match x {
                Element::NonCenter(y) => {
                    let p = Pair::new(y.0, y.1);
                    if center.is_none() {
                        left.push(p);
                    } else {
                        right.push(p);
                    }
                }
                Element::Center(y) => {
                    if center.is_none() {
                        center = Some(Pair::new(y.0, y.1));
                    } else {
                        return Err(CenterPresentError.into());
                    }
                }
            }
        }
        let s = if let Some(c) = center {
            Self::new_split(left, c, right)
        } else {
            Self::new_unsplit(left)
        };
        Ok(s)
    }

    /// Return all names as a [`NamedSet`]
    pub(crate) fn named_set(&self) -> NamedSet<'_>
    where
        K: MightHave<Shortname>,
    {
        let c = self.as_center().map(|e| e.key);
        let nc = self.indexed_non_center_names().map(|(_, n)| n).collect();
        NamedSet::new(c, nc)
    }

    /// Return reference to center
    pub(crate) fn as_center(&self) -> Option<IndexedElement<&Shortname, &U>> {
        match self {
            Self::Split(s) => Some(IndexedElement::new(
                s.left.len().into(),
                &s.center.key,
                &s.center.value,
            )),
            Self::Unsplit(_) => None,
        }
    }

    /// Return mutable reference to center
    pub fn as_center_mut(&mut self) -> Option<IndexedElement<&mut Shortname, &mut U>> {
        match self {
            Self::Split(s) => Some(IndexedElement::new(
                s.left.len().into(),
                &mut s.center.key,
                &mut s.center.value,
            )),
            Self::Unsplit(_) => None,
        }
    }

    // pub fn into_iter(
    //     self,
    // ) -> impl IntoIterator<Item = (MeasIdx, Result<Pair<K, V>, Pair<Shortname, U>>)> {
    //     let go =
    //         |xs: Vec<Pair<K, V>>| xs.into_iter().enumerate().map(|(i, p)| (i.into(), Ok(p)));
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
    ) -> impl Iterator<Item = Element<&'a Pair<Shortname, U>, &'a Pair<K, V>>> + 'a {
        let go = |xs: &'a [Pair<K, V>]| xs.iter().map(Element::NonCenter);
        match self {
            Self::Split(s) => {
                let c = Element::Center(&(*s.center));
                go(&s.left).chain(vec![c]).chain(go(&s.right))
            }
            Self::Unsplit(u) => go(&u.members).chain(vec![]).chain(go(&u.members[0..0])),
        }
    }

    /// Return iterator over all elements with indices
    pub fn iter_mut<'a>(&'a mut self) -> impl Iterator<Item = Element<&'a mut U, &'a mut V>> + 'a {
        let go = |xs: &'a mut [Pair<K, V>]| xs.iter_mut().map(|p| Element::NonCenter(&mut p.value));
        match self {
            Self::Split(s) => {
                let c = Element::Center(&mut s.center.value);
                go(&mut s.left).chain(vec![c]).chain(go(&mut s.right))
            }
            Self::Unsplit(u) => go(&mut u.members).chain(vec![]).chain(go(&mut [])),
        }
    }

    pub(crate) fn iter_common_values<'a, T: 'a>(&'a self) -> impl Iterator<Item = &'a T> + 'a
    where
        U: AsRef<T>,
        V: AsRef<T>,
    {
        self.iter()
            .map(|x| x.both(|l| l.value.as_ref(), |r| r.value.as_ref()))
    }

    pub(crate) fn iter_with<'a, T, F, G>(
        &'a self,
        f: &'a F,
        g: &'a G,
    ) -> impl Iterator<Item = T> + 'a
    where
        F: Fn(MeasIndex, &'a Pair<Shortname, U>) -> T,
        G: Fn(MeasIndex, &'a Pair<K, V>) -> T,
    {
        self.iter()
            .enumerate()
            .map(|(i, e)| e.both(|x| f(i.into(), x), |x| g(i.into(), x)))
    }

    // /// Return iterator over borrowed non-center values
    // pub(crate) fn iter_non_center_values(&self) -> impl Iterator<Item = (MeasIndex, &V)> + '_ {
    //     self.iter()
    //         .flat_map(|(i, x)| x.non_center().map(|p| (i, &p.value)))
    // }

    // /// Return iterator over borrowed non-center keys
    // pub(crate) fn iter_non_center_keys(&self) -> impl Iterator<Item = &K> + '_ {
    //     self.iter()
    //         .flat_map(|(_, x)| x.non_center().map(|p| &p.key))
    // }

    /// Return all existing names in the vector with their indices
    pub(crate) fn indexed_names(&self) -> impl Iterator<Item = (MeasIndex, &Shortname)> + '_
    where
        K: MightHave<Shortname>,
    {
        self.iter().enumerate().filter_map(|(i, r)| {
            r.both(|x| Some(&x.key), |x| x.key.as_opt())
                .map(|x| (i.into(), x))
        })
    }

    /// Return all existing non-center names in the vector with their indices
    pub(crate) fn indexed_non_center_names(
        &self,
    ) -> impl Iterator<Item = (MeasIndex, &Shortname)> + '_
    where
        K: MightHave<Shortname>,
    {
        self.iter()
            .enumerate()
            .filter_map(|(i, r)| r.both(|_| None, |x| x.key.as_opt()).map(|x| (i.into(), x)))
    }

    /// Return iterator over key names with non-existent names as default.
    pub(crate) fn iter_all_names(&self) -> impl Iterator<Item = Shortname> + '_
    where
        K: MightHave<Shortname>,
    {
        self.iter().enumerate().map(|(i, r)| {
            r.both(
                |x| x.key.clone(),
                |x| x.key.as_opt().cloned().unwrap_or(MeasIndex::from(i).into()),
            )
        })
    }

    /// Alter values with a function and payload.
    ///
    /// Center and non-center values will be projected to a common type.
    pub(crate) fn alter_common_values_zip<F, X, R, T>(
        &mut self,
        xs: impl IntoIterator<Item = X>,
        f: F,
    ) -> Result<Vec<R>, InputLengthError>
    where
        F: Fn(MeasIndex, &mut T, X) -> R,
        U: AsMut<T>,
        V: AsMut<T>,
    {
        self.alter_values_zip(
            xs.into_iter().collect(),
            |v, x| f(v.index, v.value.as_mut(), x),
            |v, x| f(v.index, v.value.as_mut(), x),
        )
    }

    /// Set current values to new values.
    ///
    /// The center in the new vector must be in the same position as the old.
    pub(crate) fn set_values(&mut self, xs: Vec<Element<U, V>>) -> Result<(), SetValuesError> {
        // check length and center position before doing anything, otherwise
        // we would need to reset the new vector if any error is found
        self.check_keys_length(&xs[..], true)?;
        let errs = self
            .iter()
            .zip(xs.iter())
            .enumerate()
            .map(|(i, (old, new))| match (old, new) {
                (Element::Center(_), Element::NonCenter(_)) => Some((i, true)),
                (Element::NonCenter(_), Element::Center(_)) => Some((i, false)),
                _ => None,
            })
            .filter_map(|x| x.map(|(i, is_center)| ElementMismatchError::new(i.into(), is_center)));
        ErrorGroup::try_new(errs)?;
        let _ = self.alter_values_zip(
            xs,
            |e, y| y.both(|z| *e.value = z, |_| ()),
            |e, y| y.both(|_| (), |z| *e.value = z),
        );
        Ok(())
    }

    /// Apply functions to values with payload, altering them in place.
    ///
    /// This will alter all values, including center and non-center values. The
    /// two functions apply to the different values contained. Return None
    /// if input vector is not the same length.
    pub(crate) fn alter_values_zip<G, F, X, R>(
        &mut self,
        xs: Vec<X>,
        f: F,
        g: G,
    ) -> Result<Vec<R>, InputLengthError>
    where
        F: Fn(IndexedElement<&Shortname, &mut U>, X) -> R,
        G: Fn(IndexedElement<&K, &mut V>, X) -> R,
    {
        self.check_keys_length(&xs[..], true)?;
        let go = |zs, ys, offset| Self::alter_paired_vec(zs, ys, offset, &g);
        let x = match self {
            Self::Split(s) => {
                let nleft = s.left.len();
                let nright = s.right.len();
                let mut it = xs.into_iter();
                let left_r = go(&mut s.left, it.by_ref().take(nleft).collect(), 0);
                let c = &mut s.center;
                let center_r = f(
                    IndexedElement::new(nleft.into(), &c.key, &mut c.value),
                    it.next().unwrap(),
                );
                let right_r = go(&mut s.right, it.by_ref().take(nright).collect(), 1 + nleft);
                left_r.chain([center_r]).chain(right_r).collect()
            }
            Self::Unsplit(u) => go(&mut u.members, xs, 0).collect(),
        };
        Ok(x)
    }

    pub(crate) fn alter_elements_zip<Fnoncenter, Fcenter, Ferror, X, Y, R, E, G>(
        &mut self,
        xs: Vec<Element<X, Y>>,
        g: G,
        f_noncenter: Fnoncenter,
        f_center: Fcenter,
        f_error: Ferror,
    ) -> Result<Vec<R>, SetElementsError<ErrorGroup<E, G>>>
    where
        Fnoncenter: Fn(IndexedElement<&K, &mut V>, Y) -> R,
        Fcenter: Fn(IndexedElement<&Shortname, &mut U>, X) -> R,
        Ferror: Fn(MeasIndex, bool) -> E,
    {
        let go = |zs, ys, offset| Self::alter_paired_vec(zs, ys, offset, &f_noncenter);

        let check_optical = |ys: Vec<Element<X, Y>>, offset: usize| {
            ys.into_iter()
                .enumerate()
                .map(|(i, x)| x.both(|_| ErrorsResult::new_err(i), ErrorsResult::new_ok))
                .mappend_commutative()
                .map_errors(|i| f_error((i + offset).into(), false))
        };

        self.check_keys_length(&xs[..], true)
            .map_err(SetElementsError::Length)?;
        let res = match self {
            Self::Split(s) => {
                let nleft = s.left.len();
                let mut it = xs.into_iter();
                let xs_left = it.by_ref().take(nleft).collect();
                // ASSUME this won't fail because we already counted
                let x_center = it.by_ref().next().unwrap();
                let xs_right = it.collect();
                let left_res = check_optical(xs_left, 0);
                let center_res = x_center
                    .center()
                    .ok_or(f_error(nleft.into(), true))
                    .into_log();
                let right_res = check_optical(xs_right, nleft + 1);
                left_res
                    .zip3_commutative(center_res, right_res)
                    .map_ok_value(|(ys_left, y_center, ys_right)| {
                        let left_out = go(&mut s.left, ys_left, 0);
                        let c = &mut s.center;
                        let center_index = IndexedElement::new(nleft.into(), &c.key, &mut c.value);
                        let center_out = f_center(center_index, y_center);
                        let right_out = go(&mut s.right, ys_right, 1 + nleft);
                        left_out.chain([center_out]).chain(right_out).collect()
                    })
            }
            Self::Unsplit(u) => {
                check_optical(xs, 0).map_ok_value(|ys| go(&mut u.members, ys, 0).collect())
            }
        };
        res.group_with(g)
            .resolve_nowarn()
            .map_err(SetElementsError::Mismatch)
    }

    /// Apply function(s) to all values, altering them in place.
    pub(crate) fn alter_values<F, G, R>(&mut self, f: F, g: G) -> Vec<R>
    where
        F: Fn(IndexedElement<&Shortname, &mut U>) -> R,
        G: Fn(IndexedElement<&K, &mut V>) -> R,
    {
        let xs = vec![(); self.len()];
        self.alter_values_zip(xs, |x, ()| f(x), |x, ()| g(x))
            .unwrap()
    }

    // /// Apply function to non-center values, altering them in place
    // pub(crate) fn alter_non_center_values<F, X>(&mut self, f: F) -> Vec<X>
    // where
    //     F: Fn(&mut V) -> X,
    // {
    //     match self {
    //         NamedVec::Split(s, _) => s
    //             .left
    //             .iter_mut()
    //             .map(|p| f(&mut p.value))
    //             .chain(s.right.iter_mut().map(|p| f(&mut p.value)))
    //             .collect(),
    //         NamedVec::Unsplit(u) => u.members.iter_mut().map(|p| f(&mut p.value)).collect(),
    //     }
    // }

    // /// Apply function to non-center values with values, altering them in place
    // pub(crate) fn alter_non_center_values_zip<E, F, X>(
    //     &mut self,
    //     xs: Vec<X>,
    //     f: F,
    // ) -> Result<Vec<E>, KeyLengthError>
    // where
    //     F: Fn(&mut V, X) -> E,
    // {
    //     self.check_keys_length(&xs[..], false)?;
    //     let res = match self {
    //         NamedVec::Split(s, _) => {
    //             let nleft = s.left.len();
    //             let nright = s.right.len();
    //             let mut it = xs.into_iter();
    //             let left_r: Vec<_> = s
    //                 .left
    //                 .iter_mut()
    //                 .zip(it.by_ref().take(nleft))
    //                 .map(|(y, x)| f(&mut y.value, x))
    //                 .collect();
    //             let right_r: Vec<_> = s
    //                 .right
    //                 .iter_mut()
    //                 .zip(it.by_ref().take(nright))
    //                 .map(|(y, x)| f(&mut y.value, x))
    //                 .collect();
    //             left_r.into_iter().chain(right_r).collect()
    //         }
    //         NamedVec::Unsplit(u) => u
    //             .members
    //             .iter_mut()
    //             .zip(xs)
    //             .map(|(p, x)| f(&mut p.value, x))
    //             .collect(),
    //     };
    //     Ok(res)
    // }

    /// Return position of center, if it exists
    pub(crate) fn center_index(&self) -> Option<MeasIndex> {
        match self {
            Self::Split(s) => Some(s.left.len().into()),
            Self::Unsplit(_) => None,
        }
    }

    /// Apply function over center value, possibly changing it's type
    #[allow(clippy::type_complexity)]
    pub(crate) fn map_center_value<F, Uf, P, LWC, RWC, E, EC>(
        self,
        f: F,
    ) -> LogResult<NamedVec<K, Uf, V>, P, LWC, RWC, (), E, EC>
    where
        F: Fn(IndexedElement<&Shortname, U>) -> LogResult<Uf, P, LWC, RWC, (), E, EC>,
        EC: Functor<E>,
        LWC: Default,
    {
        match self {
            Self::Split(s) => {
                let c = s.center;
                let index = s.left.len().into();
                let ckey = c.key;
                let e = IndexedElement::new(index, &ckey, c.value);
                f(e).map_ok_value(|value| {
                    let center = Pair::new(ckey, value);
                    NamedVec::new_split(s.left, center, s.right)
                })
            }
            Self::Unsplit(u) => LogResult::new_ok(NamedVec::Unsplit(u)),
        }
    }

    /// Apply function over non-center values, possibly changing their type
    #[allow(clippy::type_complexity)]
    pub(crate) fn map_non_center_values<F, Vf, WC, E, EC>(
        self,
        f: F,
    ) -> CommutativeResult<NamedVec<K, U, Vf>, (), WC, E, EC>
    where
        F: Fn(MeasIndex, V) -> CommutativeResult<Vf, (), WC, E, EC>,
        WC: Monoid,
        EC: Functor<E> + IntoIterator<Item = E> + Extend<E>,
    {
        let go = |xs: PairedVec<K, V>, offset: usize| {
            xs.into_iter()
                .enumerate()
                .map(|(i, p)| {
                    let j = i + offset;
                    f(j.into(), p.value).map_ok_value(|value| Pair::new(p.key, value))
                })
                .mappend_commutative()
        };
        match self {
            Self::Split(s) => {
                let nleft = s.left.len();
                let lres = go(s.left, 0);
                let rres = go(s.right, nleft + 1);
                lres.zip_commutative(rres)
                    .map_ok_value(|(left, right)| NamedVec::new_split(left, *s.center, right))
            }
            Self::Unsplit(u) => {
                go(u.members, 0).map_ok_value(|members| NamedVec::new_unsplit(members))
            }
        }
    }

    /// Return number of all elements.
    pub(crate) fn len(&self) -> usize {
        self.iter().count()
    }

    /// Return number of non-center elements.
    pub(crate) fn len_non_center(&self) -> usize {
        self.iter()
            .filter(|e| matches!(e, Element::NonCenter(_)))
            .count()
    }

    /// Return true if there are no contained elements.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get reference at position.
    #[allow(clippy::type_complexity)]
    pub fn get(
        &self,
        index: MeasIndex,
    ) -> Result<Element<(&Shortname, &U), (&K, &V)>, ElementIndexError> {
        let i = self.check_element_index(index, true)?;
        match self {
            Self::Split(s) => {
                let left_len = s.left.len();
                match i.cmp(&left_len) {
                    Less => Ok(Element::NonCenter(&s.left[i])),
                    Equal => Ok(Element::Center(&s.center)),
                    Greater => Ok(Element::NonCenter(&s.left[i - left_len - 1])),
                }
            }
            Self::Unsplit(u) => Ok(Element::NonCenter(&u.members[i])),
        }
        .map(|x| x.bimap_once(|p| (&p.key, &p.value), |p| (&p.key, &p.value)))
    }

    /// Get reference with name.
    pub fn get_name(&self, n: &Shortname) -> Result<(MeasIndex, Element<&U, &V>), NameNotFoundError>
    where
        K: MightHave<Shortname>,
    {
        self.iter()
            .enumerate()
            .find_map(|(i, e)| {
                let x = e.as_ref();
                x.both(
                    |t| &t.key == n,
                    |o| o.key.as_opt().is_some_and(|kn| kn == n),
                )
                .then_some((i.into(), e.bimap_once(|p| &p.value, |p| &p.value)))
            })
            .ok_or_else(|| NameNotFoundError(n.clone()))
    }

    // /// Get mutable reference at position.
    // #[allow(clippy::type_complexity)]
    // pub fn get_mut(
    //     &mut self,
    //     index: MeasIndex,
    // ) -> Result<Element<(&Shortname, &mut U), (&K, &mut V)>, ElementIndexError>
    // {
    //     let i = self.check_element_index(index, true)?;
    //     match self {
    //         Self::Split(s) => {
    //             let left_len = s.left.len();
    //             match i.cmp(&left_len) {
    //                 Less => Ok(Element::NonCenter(&mut s.left[i])),
    //                 Equal => Ok(Element::Center(&mut s.center)),
    //                 Greater => Ok(Element::NonCenter(&mut s.left[i - left_len - 1])),
    //             }
    //         }
    //         Self::Unsplit(u) => Ok(Element::NonCenter(&mut u.members[i])),
    //     }
    //     .map(|x| x.bimap(|p| (&p.key, &mut p.value), |p| (&p.key, &mut p.value)))
    // }

    // /// Get reference to value with name.
    // pub(crate) fn get_name(&self, n: &Shortname) -> Option<(MeasIndex, Element<&U, &V>)> {
    //     if let Some(c) = self.as_center() {
    //         if c.key == n {
    //             return Some((c.index, Element::Center(c.value)));
    //         }
    //     }
    //     self.iter()
    //         .flat_map(|(i, r)| r.non_center().map(|x| (i, x)))
    //         .find(|(_, p)| K::as_opt(&p.key).is_some_and(|kn| kn == n))
    //         .map(|(i, p)| (i, Element::NonCenter(&p.value)))
    // }

    // /// Get mutable reference to value with name.
    // pub(crate) fn get_name_mut(
    //     &mut self,
    //     n: &Shortname,
    // ) -> Option<(MeasIndex, Element<&mut U, &mut V>)> {
    //     match self {
    //         Self::Split(s) => {
    //             let nleft = s.left.len();
    //             Self::value_by_name_mut(&mut s.left, n)
    //                 .map(|(i, p)| (i.into(), Element::NonCenter(p)))
    //                 .or(if &s.center.key == n {
    //                     Some((nleft.into(), Element::Center(&mut s.center.value)))
    //                 } else {
    //                     None
    //                 })
    //                 .or(Self::value_by_name_mut(&mut s.right, n)
    //                     .map(|(i, p)| ((i + nleft + 1).into(), Element::NonCenter(p))))
    //         }
    //         Self::Unsplit(u) => Self::value_by_name_mut(&mut u.members, n)
    //             .map(|(i, p)| (i.into(), Element::NonCenter(p))),
    //     }
    // }

    /// Check if new name can be pushed
    pub(crate) fn check_push<'a>(&self, name: &'a K) -> Result<Cow<'a, Shortname>, NamePresentError>
    where
        K: MightHave<Shortname>,
    {
        let index = self.len().into();
        self.check_key(name, index)
    }

    /// Check if new name can be pushed
    pub(crate) fn check_insert<'a>(
        &self,
        index: MeasIndex,
        name: &'a K,
    ) -> ErrorsResult<Cow<'a, Shortname>, (), InsertError>
    where
        K: MightHave<Shortname>,
    {
        let a = self.check_boundary_index(index).map_err(InsertError::from);
        let b = self.check_key(name, index).map_err(InsertError::from);
        a.zip(b).map_ok_value(|((), k)| k).set_err_value(())
    }

    /// Add a new non-center element at the end of the vector.
    ///
    /// Does not guarantee keys are unique.
    pub(crate) fn push_nocheck(&mut self, key: K, value: V)
    where
        K: MightHave<Shortname>,
    {
        debug_assert!(self.check_push(&key).is_ok(), "Name is not unique");
        let p = Pair::new(key, value);
        match self {
            Self::Split(s) => s.right.push(p),
            Self::Unsplit(u) => u.members.push(p),
        }
    }

    /// Insert a new non-center element at a given position.
    ///
    /// Will panic if index is out of bounds. Does not guarantee keys are unique.
    pub(crate) fn insert_nocheck(&mut self, index: MeasIndex, key: K, value: V)
    where
        K: MightHave<Shortname>,
    {
        // only check key here because index will panic if out of bounds
        debug_assert!(self.check_key(&key, index).is_ok(), "Name is not unique");
        let i = usize::from(index);
        let p = Pair::new(key, value);
        match self {
            Self::Split(s) => {
                let ln = s.left.len();
                match i.cmp(&ln) {
                    Less | Equal => s.left.insert(i, p),
                    Greater => s.right.insert(i - ln - 1, p),
                }
            }
            Self::Unsplit(u) => u.members.insert(i, p),
        }
    }

    /// Replace a non-center value with a new value at given position.
    ///
    /// Return value that was replaced.
    ///
    /// Return error if index is out of bounds. If index points to the center,
    /// convert it to a non-center value.
    pub(crate) fn replace_at(
        &mut self,
        index: MeasIndex,
        value: V,
    ) -> Result<Element<U, V>, ElementIndexError>
    where
        K: Pointed<Shortname>,
    {
        let _ = self.check_element_index(index, true)?;
        Ok(self.replace_at_nocheck(index, value))
    }

    fn replace_at_nocheck(&mut self, index: MeasIndex, value: V) -> Element<U, V>
    where
        K: Pointed<Shortname>,
    {
        let i = usize::from(index);
        let (newself, ret) = match mem::take(self) {
            Self::Split(mut s) => {
                let ln = s.left.len();
                match i.cmp(&ln) {
                    Less => {
                        let ret = mem::replace(&mut s.left[i].value, value);
                        (Self::Split(s), Element::NonCenter(ret))
                    }
                    Equal => {
                        let key = K::wrap(s.center.key);
                        let members = s
                            .left
                            .into_iter()
                            .chain([Pair::new(key, value)])
                            .chain(s.right)
                            .collect();
                        (Self::new_unsplit(members), Element::Center(s.center.value))
                    }
                    Greater => {
                        let ret = mem::replace(&mut s.left[i - ln - 1].value, value);
                        (Self::Split(s), Element::NonCenter(ret))
                    }
                }
            }
            Self::Unsplit(mut u) => {
                let ret = mem::replace(&mut u.members[i].value, value);
                (Self::Unsplit(u), Element::NonCenter(ret))
            }
        };
        *self = newself;
        ret
    }

    /// Replace a value with a new value with a given name.
    ///
    /// Return value that was replaced.
    ///
    /// Return none if name is not present.
    pub(crate) fn replace_named(
        &mut self,
        name: &Shortname,
        value: V,
    ) -> Result<Element<U, V>, NameNotFoundError>
    where
        K: MightHave<Shortname>,
    {
        let index = self.find_with_name(name)?;
        Ok(self.replace_at_nocheck(index, value))
    }

    /// Rename an element at index.
    ///
    /// If index points to the center element and the wrapped name contains
    /// nothing, the default name will be assigned. Return error if index is
    /// out of bounds or name is not unique. Return pair of old and new name
    /// on success.
    pub(crate) fn rename(
        &mut self,
        index: MeasIndex,
        key: K,
    ) -> Result<(Shortname, Shortname), RenameError>
    where
        K: MightHave<Shortname>,
    {
        let i = self
            .check_element_index(index, true)
            .map_err(RenameError::Index)?;
        let k = to_opt_or_indexed(key.as_opt(), index);
        if self
            .iter_all_names()
            .enumerate()
            .any(|(j, n)| j != i && n == k)
        {
            Err(RenameError::NonUnique(NamePresentError { name: k }))
        } else {
            let old = match self {
                Self::Split(s) => {
                    let ln = s.left.len();
                    match i.cmp(&ln) {
                        Less => mem::replace(&mut s.left[i].key, key),
                        Equal => K::wrap(mem::replace(&mut s.center.key, k.clone())),
                        Greater => mem::replace(&mut s.right[i - ln - 1].key, key),
                    }
                }
                Self::Unsplit(u) => mem::replace(&mut u.members[i].key, key),
            };
            let old_k = to_opt_or_indexed(old.as_opt(), index);
            Ok((old_k, k))
        }
    }

    /// Rename center element.
    ///
    /// Return previous name if center exists.
    pub(crate) fn rename_center(&mut self, name: Shortname) -> Option<Shortname> {
        match self {
            Self::Split(s) => Some(mem::replace(&mut s.center.key, name)),
            Self::Unsplit(_) => None,
        }
    }

    /// Test if new center with name can be pushed
    pub(crate) fn check_push_center(
        &self,
        name: &Shortname,
    ) -> ErrorsResult<(), (), PushCenterError>
    where
        K: MightHave<Shortname>,
    {
        let a = self.check_name(name).map_err(PushCenterError::from);
        let b = matches!(self, Self::Split(_))
            .then_some(CenterPresentError.into())
            .map_or(Ok(()), Err);
        a.zip(b).set_ok_value(())
    }

    /// Test if new center with name can be inserted at index
    pub(crate) fn check_insert_center(
        &self,
        index: MeasIndex,
        name: &Shortname,
    ) -> ErrorsResult<(), (), InsertCenterError>
    where
        K: MightHave<Shortname>,
    {
        let a = self
            .check_push_center(name)
            .map_errors(InsertCenterError::from);
        let b = self
            .check_boundary_index(index)
            .map_err(InsertCenterError::from)
            .into_nowarn();
        a.zip_commutative(b).set_ok_value(())
    }

    /// Push a new center element to the end of the vector
    ///
    /// Will noop if center is already present and will not guarantee
    /// name uniqueness.
    pub(crate) fn push_center_nocheck(&mut self, name: Shortname, value: U)
    where
        K: MightHave<Shortname>,
    {
        debug_assert!(self.check_name(&name).is_ok(), "Name is not unique");
        let p = Pair::new(name, value);
        *self = match mem::take(self) {
            Self::Unsplit(u) => Self::new_split(u.members, p, vec![]),
            s @ Self::Split(_) => {
                debug_assert!(false, "Center already present");
                s
            }
        };
    }

    /// Insert a new center element at a given position.
    ///
    /// Will noop if center already exists, will not guarantee name uniqueness,
    /// and will silently truncate any indices that are over length of vector.
    pub(crate) fn insert_center_nocheck(&mut self, index: MeasIndex, name: Shortname, value: U)
    where
        K: MightHave<Shortname>,
    {
        debug_assert!(self.check_name(&name).is_ok(), "Name is not unique");
        let i = usize::from(index);
        debug_assert!(i <= self.len(), "Index is out of bounds");
        let p = Pair::new(name, value);
        *self = match mem::take(self) {
            Self::Unsplit(u) => {
                let mut it = u.members.into_iter();
                let left: Vec<_> = it.by_ref().take(i).collect();
                let right: Vec<_> = it.collect();
                Self::new_split(left, p, right)
            }
            s @ Self::Split(_) => {
                debug_assert!(false, "Center already present");
                s
            }
        };
    }

    /// Remove key/value pair by name.
    pub(crate) fn remove_index(
        &mut self,
        index: MeasIndex,
    ) -> Result<EitherPair<K, U, V>, ElementIndexError> {
        let i = self.check_element_index(index, true)?;
        let (newself, ret) = match mem::take(self) {
            Self::Split(mut s) => {
                let nleft = s.left.len();
                match i.cmp(&nleft) {
                    Less => {
                        let x = s.left.remove(i);
                        (Self::Split(s), Ok(Element::NonCenter(x)))
                    }
                    Equal => {
                        let new = s.left.into_iter().chain(s.right).collect();
                        let ret = Ok(Element::Center(*s.center));
                        (Self::new_unsplit(new), ret)
                    }
                    Greater => {
                        let x = s.right.remove(i - nleft - 1);
                        (Self::Split(s), Ok(Element::NonCenter(x)))
                    }
                }
            }
            Self::Unsplit(mut u) => {
                let x = u.members.remove(i);
                (Self::Unsplit(u), Ok(Element::NonCenter(x)))
            }
        };
        *self = newself;
        ret
    }

    /// Remove key/value pair by name of key.
    ///
    /// Return error if name not found.
    pub(crate) fn remove_name(
        &mut self,
        n: &Shortname,
    ) -> Result<(MeasIndex, Element<U, V>), NameNotFoundError>
    where
        K: MightHave<Shortname>,
    {
        let go = |xs: &mut Vec<_>| {
            let i = Self::position_by_name(xs, n)?;
            let p = xs.remove(i);
            Ok((i.into(), p.value))
        };
        let (newself, ret) = match mem::take(self) {
            Self::Split(mut s) => {
                if let Ok((i, v)) = go(&mut s.left).or(go(&mut s.right)) {
                    (Self::Split(s), Ok((i, Element::NonCenter(v))))
                } else if &s.center.key == n {
                    let i = s.left.len().into();
                    let xs = s.left.into_iter().chain(s.right).collect();
                    let new = Self::new_unsplit(xs);
                    (new, Ok((i, Element::Center(s.center.value))))
                } else {
                    (Self::Split(s), Err(NameNotFoundError(n.clone())))
                }
            }
            Self::Unsplit(mut u) => {
                let ret = go(&mut u.members);
                (
                    Self::Unsplit(u),
                    ret.map(|(i, v)| (i, Element::NonCenter(v))),
                )
            }
        };
        *self = newself;
        ret
    }

    /// Set keys to list
    ///
    /// If center key does not exist, return an error.
    ///
    /// List must be the same length as all non-center keys and must be unique
    /// (including the center key).
    pub(crate) fn set_keys(&mut self, ks: Vec<K>) -> Result<NameMapping, SetKeysError>
    where
        K: Clone + MightHave<Shortname>,
    {
        self.check_keys_length(&ks[..], true)
            .map_err(SetNamesError::Length)?;
        if !all_unique_names(ks.iter().map(MightHave::as_opt)) {
            return Err(SetNamesError::NonUnique(NonUniqueKeysError).into());
        }
        let mut mapping = HashMap::new();
        let mut go = |side: &mut PairedVec<K, V>, ks_side: Vec<K>| {
            for (p, k) in side.iter_mut().zip(ks_side) {
                let old = mem::replace(&mut p.key, k.clone());
                if let (Some(old_name), Some(new_name)) = (K::to_opt(old), K::to_opt(k)) {
                    mapping.insert(old_name, new_name);
                }
            }
        };
        match self {
            Self::Split(s) => {
                let mut it = ks.into_iter();
                // ASSUME this won't fail because we checked length above
                let ks_left = it.by_ref().take(s.left.len()).collect();
                let center = it.by_ref().next().unwrap();
                let ks_right = it.collect();
                if let Some(center_name) = K::to_opt(center) {
                    go(&mut s.left, ks_left);
                    s.center.key = center_name;
                    go(&mut s.right, ks_right);
                } else {
                    return Err(MissingCenterError.into());
                }
            }
            Self::Unsplit(u) => go(&mut u.members, ks),
        }
        Ok(mapping)
    }

    // /// Set non-center keys to list
    // ///
    // /// The center key cannot be replaced by this method since the list will
    // /// contain wrapped names which may or may not have a name inside, and
    // /// the center value always has a name.
    // ///
    // /// List must be the same length as all non-center keys and must be unique
    // /// (including the center key).
    // pub(crate) fn set_non_center_keys(
    //     &mut self,
    //     ks: Vec<K>,
    // ) -> Result<NameMapping, SetNamesError>
    // where
    //     K: Clone,
    // {
    //     self.check_keys_length(&ks[..], false)
    //         .map_err(SetNamesError::Length)?;
    //     let center = self.as_center().map(|x| K::wrap(x.key));
    //     let all_keys = ks.iter().map(K::as_ref).chain(center).collect();
    //     if !self.as_prefix().all_unique::<K>(all_keys) {
    //         return Err(SetNamesError::NonUnique);
    //     }
    //     let mut mapping = HashMap::new();
    //     let mut go = |side: &mut PairedVec<K, V>, ks_side: Vec<K>| {
    //         for (p, k) in side.iter_mut().zip(ks_side) {
    //             let old = mem::replace(&mut p.key, k.clone());
    //             if let (Some(old_name), Some(new_name)) = (K::to_opt(old), K::to_opt(k)) {
    //                 mapping.insert(old_name, new_name);
    //             }
    //         }
    //     };
    //     match self {
    //         Self::Split(s) => {
    //             let mut ks_left = ks;
    //             let ks_right = ks_left.split_off(s.left.len());
    //             go(&mut s.left, ks_left);
    //             go(&mut s.right, ks_right);
    //         }
    //         Self::Unsplit(u) => go(&mut u.members, ks),
    //     }
    //     Ok(mapping)
    // }

    /// Set all names to list of Shortnames
    ///
    /// This will update the center value along with everything else. Non-center
    /// keys will be wrapped such that they will contain a name.
    ///
    /// Supplied list must be unique and have the same length as the target
    /// vector.
    pub(crate) fn set_names(&mut self, ns: Vec<Shortname>) -> Result<NameMapping, SetNamesError>
    where
        K: MightHave<Shortname>,
    {
        self.check_keys_length(&ns[..], true)
            .map_err(SetNamesError::Length)?;
        if !all_unique(&ns) {
            return Err(NonUniqueKeysError.into());
        }
        let mut mapping = HashMap::new();
        let mut go = |side: &mut PairedVec<K, V>, ns_side: Vec<Shortname>| {
            for (p, n) in side.iter_mut().zip(ns_side) {
                let old = mem::replace(&mut p.key, K::wrap(n.clone()));
                if let Some(old_name) = old.to_opt() {
                    mapping.insert(old_name, n);
                }
            }
        };
        match self {
            Self::Split(s) => {
                // // ASSUME this won't fail because we already checked length
                let mut it = ns.into_iter();
                let ns_left = it.by_ref().take(s.left.len()).collect();
                let n_center = it.next().unwrap();
                let ns_right = it.collect();
                go(&mut s.left, ns_left);
                go(&mut s.right, ns_right);
                let old = mem::replace(&mut s.center.key, n_center.clone());
                mapping.insert(old, n_center);
            }
            Self::Unsplit(u) => go(&mut u.members, ns),
        }
        Ok(mapping)
    }

    /// Replace any value with a center value with name.
    pub(crate) fn replace_center_by_name<F, LWC, RWC, E, EC>(
        &mut self,
        n: &Shortname,
        value: U,
        to_v: F,
    ) -> LogResult<Element<U, V>, (), LWC, RWC, (), E, EC>
    where
        F: FnOnce(MeasIndex, U) -> LogResult<V, U, LWC, RWC, (), E, EC>,
        E: From<NameNotFoundError>,
        EC: Default,
        LWC: Default,
        RWC: Default,
        K: MightHave<Shortname>,
    {
        self.find_with_name(n)
            .map_err(E::from)
            .into_log()
            .nowarn_and_then(|index| self.replace_center_at_inner(index, value, to_v))
    }

    /// Replace any value with a center value with name.
    pub(crate) fn replace_center_by_name_nofail<F>(
        &mut self,
        n: &Shortname,
        value: U,
        to_v: F,
    ) -> Result<Element<U, V>, NameNotFoundError>
    where
        F: FnOnce(MeasIndex, U) -> V,
        K: MightHave<Shortname>,
    {
        let index = self.find_with_name(n)?;
        // ASSUME this won't fail since the index above is valid
        Ok(self.replace_center_at_nofail(index, value, to_v).unwrap())
    }

    /// Replace any value with a center value under index.
    ///
    /// If successful, return the replaced value. If index points to a center
    /// element, return the replaced center value. If index points to non-center,
    /// convert the current center value to non-center value and replace/return
    /// the non-center value under index.
    ///
    /// Fail if name at index to be converted is blank or
    /// if the previous center value cannot be converted back to a non-center
    /// value.
    pub(crate) fn replace_center_at<F, LWC, RWC, E, EC>(
        &mut self,
        index: MeasIndex,
        value: U,
        to_v: F,
    ) -> LogResult<Element<U, V>, (), LWC, RWC, (), E, EC>
    where
        F: FnOnce(MeasIndex, U) -> LogResult<V, U, LWC, RWC, (), E, EC>,
        E: From<SetCenterError>,
        LWC: Default,
        RWC: Default,
        EC: Default,
        K: MightHave<Shortname>,
    {
        self.get_index_if_named(index)
            .map_err(E::from)
            .into_log()
            .nowarn_and_then(|i| self.replace_center_at_inner(i.into(), value, to_v))
    }

    /// Replace any value with a center value under index.
    ///
    /// If successful, return the replaced value. If index points to a center
    /// element, return the replaced center value. If index points to non-center,
    /// convert the current center value to non-center value and replace/return
    /// the non-center value under index.
    ///
    /// Fail if name at index to be converted is blank or
    /// if the previous center value cannot be converted back to a non-center
    /// value.
    pub(crate) fn replace_center_at_nofail<F>(
        &mut self,
        index: MeasIndex,
        value: U,
        to_v: F,
    ) -> Result<Element<U, V>, SetCenterError>
    where
        F: FnOnce(MeasIndex, U) -> V,
        K: MightHave<Shortname>,
    {
        let go = |j, u| ErrorResult::<_, _, Infallible>::new_ok(to_v(j, u));

        self.get_index_if_named(index).map(|i| {
            let res = self.replace_center_at_inner(i.into(), value, go);
            res.infallible_nowarn_into()
        })
    }

    fn alter_paired_vec<X, F, R>(
        xs: &mut PairedVec<K, V>,
        ys: impl IntoIterator<Item = X>,
        offset: usize,
        f: &F,
    ) -> impl Iterator<Item = R>
    where
        F: Fn(IndexedElement<&K, &mut V>, X) -> R,
    {
        // ASSUME both xs and ys are the same length
        xs.iter_mut().zip(ys).zip(offset..).map(|((y, x), i)| {
            let e = IndexedElement::new(i.into(), &y.key, &mut y.value);
            f(e, x)
        })
    }

    fn replace_center_at_inner<F, LWC, RWC, E, EC>(
        &mut self,
        index: MeasIndex,
        value: U,
        to_v: F,
    ) -> LogResult<Element<U, V>, (), LWC, RWC, (), E, EC>
    where
        F: FnOnce(MeasIndex, U) -> LogResult<V, U, LWC, RWC, (), E, EC>,
        LWC: Default,
        K: MightHave<Shortname>,
    {
        let res = match mem::take(self) {
            Self::Split(s) => match s.split_at_index(index.into()) {
                PartialSplit::Left(split) => to_v(index, split.center_value)
                    .inject_value((split.stable, split.selected_left_value))
                    .map_ok_value(|(new_right_val, (stable, old_left_val))| {
                        let sp = Self::new_split_from_left(value, new_right_val, stable).unwrap();
                        (sp, Element::NonCenter(old_left_val))
                    })
                    .map_err_value(|(center_val, (stable, old_left_val))| {
                        Self::recover_split_from_left(old_left_val, center_val, stable)
                    }),

                PartialSplit::Center(x) => {
                    let center = Pair::new(x.center.key, value);
                    let sp = Self::new_split(x.left, center, x.right);
                    LogResult::new_ok((sp, Element::Center(x.center.value)))
                }

                PartialSplit::Right(split) => to_v(index, split.center_value)
                    .inject_value((split.stable, split.selected_right_value))
                    .map_ok_value(|(new_left_val, (stable, old_right_val))| {
                        let sp = Self::new_split_from_right(value, new_left_val, stable).unwrap();
                        (sp, Element::NonCenter(old_right_val))
                    })
                    .map_err_value(|(center_val, (stable, old_right_val))| {
                        Self::recover_split_from_right(old_right_val, center_val, stable)
                    }),
            },

            Self::Unsplit(u) => {
                let x = split_paired_vec(u.members, index.into());
                let ret = x.selected.value;
                let center = Pair::new(x.selected.key.to_opt().unwrap(), value);
                let sp = Self::new_split(x.left, center, x.right);
                LogResult::new_ok((sp, Element::NonCenter(ret)))
            }
        };

        res.map_ok_value(|(newself, ret)| {
            *self = newself;
            ret
        })
        .map_err_value(|newself| *self = newself)
    }

    /// Set center to be the element with name if it exists.
    pub(crate) fn set_center_by_name<Fswap, FtoU, LWC, RWC, E, EC>(
        &mut self,
        n: &Shortname,
        swap: Fswap,
        to_u: FtoU,
    ) -> LogResult<bool, (), LWC, RWC, (), E, EC>
    where
        Fswap: FnOnce(
            (MeasIndex, U),
            (MeasIndex, V),
        ) -> LogResult<(V, U), (U, V), LWC, RWC, (), E, EC>,
        FtoU: FnOnce(MeasIndex, V) -> LogResult<U, V, LWC, RWC, (), E, EC>,
        E: From<NameNotFoundError>,
        EC: Default,
        LWC: Default,
        RWC: Default,
        K: MightHave<Shortname>,
    {
        self.find_with_name(n)
            .map_err(E::from)
            .into_log()
            .nowarn_and_then(|index| self.set_center_by_index_inner(index.into(), swap, to_u))
    }

    /// Set center to be the element with index if it exists.
    pub(crate) fn set_center_by_index<Fswap, FtoU, LWC, RWC, E, EC>(
        &mut self,
        index: MeasIndex,
        swap: Fswap,
        to_u: FtoU,
    ) -> LogResult<bool, (), LWC, RWC, (), E, EC>
    where
        Fswap: FnOnce(
            (MeasIndex, U),
            (MeasIndex, V),
        ) -> LogResult<(V, U), (U, V), LWC, RWC, (), E, EC>,
        FtoU: FnOnce(MeasIndex, V) -> LogResult<U, V, LWC, RWC, (), E, EC>,
        E: From<SetCenterError>,
        EC: Default,
        RWC: Default,
        LWC: Default,
        K: MightHave<Shortname>,
    {
        self.get_index_if_named(index)
            .map_err(E::from)
            .into_log()
            .nowarn_and_then(|i| self.set_center_by_index_inner(i, swap, to_u))
    }

    fn set_center_by_index_inner<Fswap, FtoU, LWC, RWC, E, EC>(
        &mut self,
        index: usize,
        swap: Fswap,
        to_u: FtoU,
    ) -> LogResult<bool, (), LWC, RWC, (), E, EC>
    where
        Fswap: FnOnce(
            (MeasIndex, U),
            (MeasIndex, V),
        ) -> LogResult<(V, U), (U, V), LWC, RWC, (), E, EC>,
        FtoU: FnOnce(MeasIndex, V) -> LogResult<U, V, LWC, RWC, (), E, EC>,
        EC: Default,
        RWC: Default,
        LWC: Default,
        K: MightHave<Shortname>,
    {
        // ASSUME index is valid
        let j = index.into();
        let res = match mem::take(self) {
            Self::Split(s) => match s.split_at_index(index) {
                PartialSplit::Left(split) => {
                    let old_center = (split.stable.center_index, split.center_value);
                    let new_center = (j, split.selected_left_value);
                    swap(old_center, new_center)
                        .inject_value(split.stable)
                        .map_ok_value(|((new_right_val, new_center_val), stable)| {
                            let sp =
                                Self::new_split_from_left(new_center_val, new_right_val, stable);
                            (sp.unwrap(), true)
                        })
                        .map_err_value(|((old_center_val, old_left_val), stable)| {
                            Self::recover_split_from_left(old_left_val, old_center_val, stable)
                        })
                }

                PartialSplit::Center(sc) => LogResult::new_ok((Self::Split(sc), false)),

                PartialSplit::Right(split) => {
                    let old_center = (split.stable.center_index, split.center_value);
                    let new_center = (j, split.selected_right_value);
                    swap(old_center, new_center)
                        .inject_value(split.stable)
                        .map_ok_value(|((new_right_val, new_center_val), stable)| {
                            let sp =
                                Self::new_split_from_right(new_center_val, new_right_val, stable);
                            (sp.unwrap(), true)
                        })
                        .map_err_value(|((old_center_val, old_right_val), stable)| {
                            Self::recover_split_from_right(old_right_val, old_center_val, stable)
                        })
                }
            },

            Self::Unsplit(u) => {
                let x = split_paired_vec(u.members, index);
                to_u(j, x.selected.value)
                    .inject_value((x.left, x.selected.key, x.right))
                    .map_ok_value(|(new_value, (left, key, right))| {
                        let center = Pair::new(key.to_opt().unwrap(), new_value);
                        (Self::new_split(left, center, right), true)
                    })
                    .map_err_value(|(old_value, (left, key, right))| {
                        let center = Pair::new(key, old_value);
                        let new = left.into_iter().chain([center]).chain(right).collect();
                        Self::new_unsplit(new)
                    })
            }
        };
        res.map_ok_value(|(newself, flag)| {
            *self = newself;
            flag
        })
        .map_err_value(|newself| *self = newself)
    }

    /// Convert the center element into a non-center element.
    ///
    /// Has no effect if there already is no center element.
    ///
    /// Return old center element if vector is updated.
    pub(crate) fn unset_center<F, X, LWC, RWC, E, EC>(
        &mut self,
        to_v: F,
    ) -> LogResult<Option<X>, (), LWC, RWC, (), E, EC>
    where
        F: FnOnce(MeasIndex, U) -> LogResult<(V, X), U, LWC, RWC, (), E, EC>,
        LWC: Default,
        K: Pointed<Shortname>,
    {
        match mem::take(self) {
            Self::Split(s) => {
                let index = (s.left.len()).into();
                to_v(index, s.center.value)
                    .inject_value((s.left, s.center.key, s.right))
                    .map_ok_value(|((value, ret), (left, center_key, right))| {
                        let non_center = Pair::new(K::wrap(center_key), value);
                        let members = left.into_iter().chain([non_center]).chain(right).collect();
                        (Self::new_unsplit(members), Some(ret))
                    })
                    .map_err_value(|(value, (left, center_key, right))| {
                        let center = Pair::new(center_key, value);
                        Self::new_split(left, center, right)
                    })
            }
            Self::Unsplit(u) => LogResult::new_ok((Self::Unsplit(u), None)),
        }
        .map_ok_value(|(newself, flag)| {
            *self = newself;
            flag
        })
        .map_err_value(|newself| *self = newself)
    }

    /// Unwrap and rewrap the non-center names of vector.
    ///
    /// This may fail if the original wrapped name cannot be converted.
    #[allow(clippy::type_complexity)]
    pub(crate) fn try_rewrapped<J, E, F>(self, f: F) -> ErrorsResult<NamedVec<J, U, V>, (), E>
    where
        F: Fn(MeasIndex, K) -> Result<J, E>,
    {
        let try_go = |i: usize, p: Pair<K, V>| Ok(Pair::new(f(i.into(), p.key)?, p.value));
        let go = |xs: PairedVec<K, V>, offset: usize| {
            xs.into_iter()
                .enumerate()
                .map(|(i, p)| try_go(i + offset, p).into_nowarn())
                .mappend_commutative()
        };
        match self {
            Self::Split(s) => {
                let offset = s.left.len() + 1;
                let lres = go(s.left, 0);
                let rres = go(s.right, offset);
                lres.zip_commutative(rres)
                    .map_ok_value(|(left, right)| NamedVec::new_split(left, *s.center, right))
            }
            Self::Unsplit(u) => go(u.members, 0).map_ok_value(NamedVec::new_unsplit),
        }
    }

    // #[allow(clippy::type_complexity)]
    // fn try_into_wrapper<J>(p: Pair<K, V>) -> Result<Pair<J, V>, <J as TryFrom<K>>::Error>
    // where
    //     J: TryFrom<K>,
    // {
    //     Ok(Pair::new(p.key.try_into()?, p.value))
    // }

    // fn from_center(p: Center<U>) -> Pair<K, V>
    // where
    //     V: From<U>,
    // {
    //     Pair {
    //         key: K::wrap(p.key),
    //         value: p.value.into(),
    //     }
    // }

    fn position_by_name(xs: &PairedVec<K, V>, n: &Shortname) -> Result<usize, NameNotFoundError>
    where
        K: MightHave<Shortname>,
    {
        xs.iter()
            .position(|p| p.key.as_opt().is_some_and(|kn| kn == n))
            .ok_or(NameNotFoundError(n.to_owned()))
    }

    // fn value_by_name_mut<'a>(
    //     xs: &'a mut PairedVec<K, V>,
    //     n: &Shortname,
    // ) -> Option<(usize, &'a mut V)> {
    //     xs.iter_mut()
    //         .enumerate()
    //         .find(|(_, p)| K::as_opt(&p.key).is_some_and(|kn| kn == n))
    //         .map(|(i, p)| (i, &mut p.value))
    // }

    fn check_key<'a>(
        &self,
        key: &'a K,
        index: MeasIndex,
    ) -> Result<Cow<'a, Shortname>, NamePresentError>
    where
        K: MightHave<Shortname>,
    {
        let name = key
            .as_opt()
            .map_or_else(|| Cow::Owned(Shortname::from(index)), Cow::Borrowed);
        if self.iter_all_names().any(|n| &n == name.as_ref()) {
            Err(NamePresentError::new(name.into_owned()))
        } else {
            Ok(name)
        }
    }

    fn check_name(&self, name: &Shortname) -> Result<(), NamePresentError>
    where
        K: MightHave<Shortname>,
    {
        if self.iter_all_names().any(|n| &n == name) {
            Err(NamePresentError::new(name.clone()))
        } else {
            Ok(())
        }
    }

    fn check_element_index(
        &self,
        index: MeasIndex,
        include_center: bool,
    ) -> Result<usize, ElementIndexError> {
        let len = self.len();
        IndexFromOne::from(index).check_index(len).map_or_else(
            |e| Err(ElementIndexError::new(e, None)),
            |i| {
                if let Some(j) = self.center_index()
                    && !include_center
                    && usize::from(j) == i
                {
                    let e = IndexError::new(i.into(), len);
                    return Err(ElementIndexError::new(e, Some(j)));
                }
                Ok(i)
            },
        )
    }

    fn get_index_if_named(&self, index: MeasIndex) -> Result<usize, SetCenterError>
    where
        K: MightHave<Shortname>,
    {
        let i = self
            .check_element_index(index, true)
            .map_err(SetCenterError::Index)?;
        // ASSUME this won't panic since we checked the index is valid
        let has_name = self
            .get(index)
            .unwrap()
            .both(|_| true, |(n, _)| n.as_opt().is_some());
        if has_name {
            Ok(i)
        } else {
            Err(NoNameError.into())
        }
    }

    fn check_boundary_index(&self, index: MeasIndex) -> Result<(), BoundaryIndexError> {
        IndexFromOne::from(index).check_boundary_index(self.len())
    }

    fn check_keys_length<X>(&self, xs: &[X], include_center: bool) -> Result<(), InputLengthError> {
        let this_len = if include_center {
            self.len()
        } else {
            self.len_non_center()
        };
        let other_len = xs.len();
        if this_len != other_len {
            return Err(InputLengthError {
                this_len,
                other_len,
                include_center,
            });
        }
        Ok(())
    }

    fn find_with_name(&self, name: &Shortname) -> Result<MeasIndex, NameNotFoundError>
    where
        K: MightHave<Shortname>,
    {
        self.iter()
            .find_position(|x| {
                x.as_ref().both(
                    |l| &l.key == name,
                    |r| r.key.as_opt().is_some_and(|k| k == name),
                )
            })
            .map(|(i, _)| i.into())
            .ok_or(NameNotFoundError(name.to_owned()))
    }

    fn new_split(left: PairedVec<K, V>, center: Center<U>, right: PairedVec<K, V>) -> Self {
        Self::Split(SplitVec::new(left, Box::new(center), right))
    }

    fn new_split_from_left(
        new_center_value: U,
        new_right_value: V,
        stable: LeftSplitStable<K, V>,
    ) -> Option<Self>
    where
        K: MightHave<Shortname>,
    {
        let new_center = Pair::new(stable.selected_left_key.to_opt()?, new_center_value);
        let new_right_pair = Pair::new(K::wrap(stable.center_key), new_right_value);
        let new_right = stable
            .left_right
            .into_iter()
            .chain([new_right_pair])
            .chain(stable.right)
            .collect();
        Some(Self::new_split(stable.left_left, new_center, new_right))
    }

    fn recover_split_from_left(
        old_left_value: V,
        old_center_value: U,
        stable: LeftSplitStable<K, V>,
    ) -> Self {
        let center = Pair::new(stable.center_key, old_center_value);
        let new_left_value = Pair::new(stable.selected_left_key, old_left_value);
        let new_left = stable
            .left_left
            .into_iter()
            .chain([new_left_value])
            .chain(stable.left_right)
            .collect();
        Self::new_split(new_left, center, stable.right)
    }

    fn new_split_from_right(
        new_center_value: U,
        new_left_value: V,
        stable: RightSplitStable<K, V>,
    ) -> Option<Self>
    where
        K: MightHave<Shortname>,
    {
        let new_center = Pair::new(stable.selected_right_key.to_opt()?, new_center_value);
        let new_left_pair = Pair::new(K::wrap(stable.center_key), new_left_value);
        let new_left = stable
            .left
            .into_iter()
            .chain([new_left_pair])
            .chain(stable.right_left)
            .collect();
        Some(Self::new_split(new_left, new_center, stable.right_right))
    }

    fn recover_split_from_right(
        old_right_value: V,
        old_center_value: U,
        stable: RightSplitStable<K, V>,
    ) -> Self {
        let center = Pair::new(stable.center_key, old_center_value);
        let new_right_value = Pair::new(stable.selected_right_key, old_right_value);
        let new_right = stable
            .right_left
            .into_iter()
            .chain([new_right_value])
            .chain(stable.right_right)
            .collect();
        Self::new_split(stable.left, center, new_right)
    }

    fn new_unsplit(members: PairedVec<K, V>) -> Self {
        Self::Unsplit(UnsplitVec { members })
    }
}

impl<A, B> BifunctorOnce<A, B> for Element<A, B> {
    fn first_once<F: FnOnce(A) -> C, C>(self, f: F) -> Element<C, B> {
        match self {
            Self::Center(x) => Element::Center(f(x)),
            Self::NonCenter(x) => Element::NonCenter(x),
        }
    }

    fn second_once<F: FnOnce(B) -> C, C>(self, f: F) -> Element<A, C> {
        match self {
            Self::Center(x) => Element::Center(x),
            Self::NonCenter(x) => Element::NonCenter(f(x)),
        }
    }
}

impl<K, U, V> EitherPair<K, U, V> {
    pub fn unzip(self) -> (K, Element<U, V>)
    where
        K: Pointed<Shortname>,
    {
        self.both(
            |p| (K::wrap(p.key), Element::Center(p.value)),
            |p| (p.key, Element::NonCenter(p.value)),
        )
    }
}

impl<K, U, V> Either<K, U, V> {
    pub fn values_into<Uf, Vf>(self) -> Either<K, Uf, Vf>
    where
        U: Into<Uf>,
        V: Into<Vf>,
    {
        self.first_once(|(k, v)| (k, v.into()))
            .second_once(|(k, v)| (k, v.into()))
    }
}

impl<U, V> Element<U, V> {
    pub fn both<F, G, X>(self, f: F, g: G) -> X
    where
        F: FnOnce(U) -> X,
        G: FnOnce(V) -> X,
    {
        match self {
            Self::Center(u) => f(u),
            Self::NonCenter(v) => g(v),
        }
    }

    pub fn as_ref(&self) -> Element<&U, &V> {
        match self {
            Self::Center(u) => Element::Center(u),
            Self::NonCenter(v) => Element::NonCenter(v),
        }
    }

    pub fn non_center(self) -> Option<V> {
        match self {
            Self::Center(_) => None,
            Self::NonCenter(v) => Some(v),
        }
    }

    pub fn center(self) -> Option<U> {
        match self {
            Self::Center(u) => Some(u),
            Self::NonCenter(_) => None,
        }
    }

    pub fn is_center(&self) -> bool {
        self.as_ref().center().is_some()
    }
}

impl<X> Element<X, X> {
    pub fn unwrap(self) -> X {
        self.both(|x| x, |y| y)
    }
}

fn to_opt_or_indexed(x: Option<&Shortname>, i: MeasIndex) -> Shortname {
    x.cloned().unwrap_or(i.into())
}

fn all_unique_names<'a>(xs: impl IntoIterator<Item = Option<&'a Shortname>>) -> bool {
    all_unique(
        xs.into_iter()
            .enumerate()
            .map(|(i, x)| to_opt_or_indexed(x, i.into())),
    )
}

fn all_unique<'a, T: Hash + Eq>(xs: impl IntoIterator<Item = T> + 'a) -> bool {
    let mut unique = HashSet::new();
    for x in xs {
        if unique.contains(&x) {
            return false;
        }
        unique.insert(x);
    }
    true
}

enum PartialSplit<K, U, V> {
    Left(LeftSplit<K, U, V>),
    Center(SplitVec<K, U, V>),
    Right(RightSplit<K, U, V>),
}

#[derive(new)]
struct LeftSplit<K, U, V> {
    selected_left_value: V,
    center_value: U,
    stable: LeftSplitStable<K, V>,
}

#[derive(new)]
struct RightSplit<K, U, V> {
    selected_right_value: V,
    center_value: U,
    stable: RightSplitStable<K, V>,
}

#[derive(new)]
struct LeftSplitStable<K, V> {
    left_left: PairedVec<K, V>,
    selected_left_key: K,
    left_right: PairedVec<K, V>,
    center_key: Shortname,
    center_index: MeasIndex,
    right: PairedVec<K, V>,
}

#[derive(new)]
struct RightSplitStable<K, V> {
    left: PairedVec<K, V>,
    center_key: Shortname,
    center_index: MeasIndex,
    right_left: PairedVec<K, V>,
    selected_right_key: K,
    right_right: PairedVec<K, V>,
}

struct PairedSplit<K, V> {
    left: PairedVec<K, V>,
    selected: Pair<K, V>,
    right: PairedVec<K, V>,
}

fn split_paired_vec<K, V>(xs: PairedVec<K, V>, index: usize) -> PairedSplit<K, V> {
    let mut it = xs.into_iter();
    PairedSplit {
        left: it.by_ref().take(index).collect(),
        selected: it.next().unwrap(),
        right: it.collect(),
    }
}

impl<K, U, V> SplitVec<K, U, V> {
    fn split_at_index(self, index: usize) -> PartialSplit<K, U, V> {
        let nleft = self.left.len();
        match index.cmp(&nleft) {
            Less => {
                let split_left = split_paired_vec(self.left, index);
                let stable = LeftSplitStable::new(
                    split_left.left,
                    split_left.selected.key,
                    split_left.right,
                    self.center.key,
                    nleft.into(),
                    self.right,
                );
                let split = LeftSplit::new(split_left.selected.value, self.center.value, stable);
                PartialSplit::Left(split)
            }
            Equal => PartialSplit::Center(self),
            Greater => {
                let split_right = split_paired_vec(self.right, index);
                let stable = RightSplitStable::new(
                    self.left,
                    self.center.key,
                    nleft.into(),
                    split_right.left,
                    split_right.selected.key,
                    split_right.right,
                );
                let split = RightSplit::new(split_right.selected.value, self.center.value, stable);
                PartialSplit::Right(split)
            }
        }
    }
}

/// Error when inserting new element into [`NamedVec`]
#[derive(From, Debug, Display, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum InsertError {
    /// Index out of range
    Index(BoundaryIndexError),
    /// New name is not unique
    NonUnique(NamePresentError),
}

/// Error when renaming element's name at index in [`NamedVec`]
#[derive(Debug, Display, Error)]
pub enum RenameError {
    /// Index not found
    Index(ElementIndexError),
    /// Name change results in duplicates
    NonUnique(NamePresentError),
}

/// Error when inserting new center element into [`NamedVec`]
#[derive(Debug, Error, Display, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum InsertCenterError {
    Push(PushCenterError),
    Index(BoundaryIndexError),
}

/// Error when pushing new center element to the right of [`NamedVec`]
#[derive(Debug, Error, Display, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum PushCenterError {
    NonUnique(NamePresentError),
    Present(CenterPresentError),
}

/// Error when setting all keys in a [`NamedVec`].
///
/// This is distinct from setting "names" which are [`Shortname`]. "Keys"
/// are names in containers which may or may not contain them.
#[derive(Debug, Error, Display, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetKeysError {
    Names(SetNamesError),
    MissingCenter(MissingCenterError),
}

/// Error when setting names ([`Shortname`]) in a [`NamedVec`]
#[derive(Debug, Error, Display, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetNamesError {
    Length(InputLengthError),
    NonUnique(NonUniqueKeysError),
}

/// Error when assigning an element in [`NamedVec`] to be the center element
#[derive(Debug, Error, Display, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetCenterError {
    Index(ElementIndexError),
    NoName(NoNameError),
}

/// Error when assigning an element in [`NamedVec`] to be the center element
#[derive(Debug, Error, Display, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum SetValuesError {
    Length(InputLengthError),
    Set(ElementMismatchErrors),
}

/// Error when building new [`NamedVec`] from list of elements
#[derive(Debug, Error, Display, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum NewNamedVecError {
    NonUnique(NonUniqueKeysError),
    MultiCenter(CenterPresentError),
}

/// Error when setting/altering the elements of [`NamedVec`]
#[derive(Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(E: Into<Self>))]
pub enum SetElementsError<E> {
    Length(InputLengthError),
    Mismatch(E),
}

/// Error when the center element of [`NamedVec`] is already present
#[derive(Debug, Error)]
#[error("center value specified multiple times")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct CenterPresentError;

/// Error when element in [`NamedVec`] does not have a name but one is expected.
#[derive(Debug, Error)]
#[error("index refers to element with no name")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct NoNameError;

/// Error when the center element of [`NamedVec`] is missing but expected
#[derive(Debug, Error)]
#[error("center must not be missing")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct MissingCenterError;

/// Error when final state of keys in [`NamedVec`] results in duplicates
#[derive(Debug, Error)]
#[error("not all supplied keys are unique")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct NonUniqueKeysError;

/// Error when name in [`NamedVec`] is not found
#[derive(Debug, Error)]
#[error("'{0}' matches no measurement")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr), pyerr(PyKeyError))]
pub struct NameNotFoundError(pub Shortname);

/// Error when name is already present in [`NamedVec`]
#[derive(Debug, Error, new)]
#[error("'{name}' already present")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct NamePresentError {
    name: Shortname,
}

/// Error when index is out of bounds for [`NamedVec`], optionally including center.
#[derive(Debug, Error, new)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr), pyerr(PyIndexError))]
pub struct ElementIndexError {
    index: IndexError,
    center: Option<MeasIndex>,
}

/// Error when element types do not match in [`NamedVec`]
#[derive(Debug, Error, new)]
#[error(
    "attempted to set a {to} at {index} when {from} is needed",
    to = if self.original_is_center { "non-center" } else { "center" },
    from = if self.original_is_center { "center" } else { "non-center" }
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct ElementMismatchError {
    index: MeasIndex,
    original_is_center: bool,
}

pub type ElementMismatchErrors = ErrorGroup<ElementMismatchError, ElementMismatchSummary>;

def_summary!(ElementMismatchSummary, "could not set new values");

impl fmt::Display for ElementIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let len = self.index.len;
        let x = usize::from(self.index.index);
        if let Some(c) = self.center.as_ref() {
            write!(
                f,
                "0-index must be 0 <= i < {len} and not \
                 include center at {c}, got {x}",
            )
        } else {
            write!(f, "0-index must be 0 <= i < {len}, got {x}")
        }
    }
}

/// Error when input collection does not match number of elements in [`NamedVec`]
#[derive(Debug, Error)]
#[error(
    "input must be {this_len} ({c}including center) elements long, got {other_len}",
    c = if self.include_center { "" } else { "not " }
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(crate::python::RelationalError))]
pub struct InputLengthError {
    this_len: usize,
    other_len: usize,
    include_center: bool,
}

#[cfg(feature = "python")]
mod python {
    use super::{Element, NonCenterElement};
    use pyo3::prelude::*;
    use pyo3::types::PyTuple;

    impl<'py, V> FromPyObject<'py> for NonCenterElement<V>
    where
        V: FromPyObject<'py>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            if let Ok(t) = ob.downcast::<PyTuple>()
                && t.is_empty()
            {
                return Ok(Self(Element::Center(())));
            }
            Ok(Self(Element::NonCenter(ob.extract::<V>()?)))
        }
    }
}
