use crate::data::ColumnError;
use crate::logging::{
    CmtResult, CmtResultIter as _, ErrorsResult, Functor, Mapped, IntoZeroOrMore, Kind1,
    LogResult, LogResultExt as _, ResultExt as _, Semigroup,
};
use crate::text::optional::MightHave;
use crate::validated::shortname::Shortname;

use super::index::{BoundaryIndexError, IndexError, IndexFromOne, MeasIndex};
use super::optional::NeverValue;

use derive_more::{Display, From, Into};
use derive_new::new;
use itertools::Itertools as _;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

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
pub enum NamedVec<K, W, U, V> {
    // W is an associated type constructor defined by K, so we need to bind K
    // but won't actually use it, hence phantom hack thing
    Split(SplitVec<W, U, V>, PhantomData<K>),
    Unsplit(UnsplitVec<W, V>),
}

impl<K, W, U, V> Default for NamedVec<K, W, U, V> {
    fn default() -> Self {
        Self::Unsplit(UnsplitVec { members: vec![] })
    }
}

#[derive(new)]
pub struct IndexedElement<K, V> {
    pub index: MeasIndex,
    pub key: K,
    pub value: V,
}

#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct SplitVec<K, U, V> {
    left: PairedVec<K, V>,
    center: Box<Center<U>>,
    right: PairedVec<K, V>,
}

#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct UnsplitVec<K, V> {
    members: PairedVec<K, V>,
}

#[derive(Clone)]
#[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
pub enum Element<U, V> {
    Center(U),
    NonCenter(V),
}

#[derive(Clone, From, Into)]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct NonCenterElement<V>(pub Element<(), V>);

type PairedVec<K, V> = Vec<Pair<K, V>>;

#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Pair<K, V> {
    pub key: K,
    pub value: V,
}

type WrappedNamedVec<K, U, V> = NamedVec<K, <K as MightHave>::Wrapper<Shortname>, U, V>;

type WrappedPair<K, V> = Pair<<K as MightHave>::Wrapper<Shortname>, V>;

type WrappedPairedVec<K, V> = PairedVec<<K as MightHave>::Wrapper<Shortname>, V>;

type Center<U> = Pair<Shortname, U>;

type Either<K, U, V> = Element<(Shortname, U), (<K as MightHave>::Wrapper<Shortname>, V)>;

pub type EitherPair<K, U, V> =
    Element<Pair<Shortname, U>, Pair<<K as MightHave>::Wrapper<Shortname>, V>>;

#[derive(From, Into)]
pub struct Eithers<K: MightHave, U, V>(pub Vec<Either<K, U, V>>);

pub type NameMapping = HashMap<Shortname, Shortname>;

impl<K: MightHave, U, V> WrappedNamedVec<K, U, V> {
    /// Build new NamedVec using either center or non-center values.
    ///
    /// Must contain either one or zero center values, otherwise return error.
    /// All names within keys (including center) must be unique.
    pub(crate) fn try_new(xs: Eithers<K, U, V>) -> Result<Self, NewNamedVecError> {
        let names: Vec<_> =
            xs.0.iter()
                .map(|x| x.as_ref().both(|e| K::wrap(&e.0), |o| K::as_ref(&o.0)))
                .collect();
        if !all_unique_names::<K>(names) {
            return Err(NewNamedVecError::NonUnique);
        }
        let mut left = vec![];
        let mut center = None;
        let mut right = vec![];
        for x in xs.0 {
            match x {
                Element::NonCenter(y) => {
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
                Element::Center(y) => {
                    if center.is_none() {
                        let cp = Pair {
                            key: y.0,
                            value: y.1,
                        };
                        center = Some(cp);
                    } else {
                        return Err(NewNamedVecError::MultiCenter);
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

    /// Return reference to center
    pub(crate) fn as_center(&self) -> Option<IndexedElement<&Shortname, &U>> {
        match self {
            Self::Split(s, _) => Some(IndexedElement::new(
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
            Self::Split(s, _) => Some(IndexedElement::new(
                s.left.len().into(),
                &mut s.center.key,
                &mut s.center.value,
            )),
            Self::Unsplit(_) => None,
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
    ) -> impl Iterator<Item = Element<&'a Pair<Shortname, U>, &'a WrappedPair<K, V>>> + 'a {
        let go = |xs: &'a [WrappedPair<K, V>]| xs.iter().map(Element::NonCenter);
        match self {
            Self::Split(s, _) => {
                let c = Element::Center(&(*s.center));
                go(&s.left).chain(vec![c]).chain(go(&s.right))
            }
            Self::Unsplit(u) => go(&u.members).chain(vec![]).chain(go(&u.members[0..0])),
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
        G: Fn(MeasIndex, &'a WrappedPair<K, V>) -> T,
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
    // pub(crate) fn iter_non_center_keys(&self) -> impl Iterator<Item = &K::Wrapper<Shortname>> + '_ {
    //     self.iter()
    //         .flat_map(|(_, x)| x.non_center().map(|p| &p.key))
    // }

    /// Return all existing names in the vector with their indices
    pub(crate) fn indexed_names(&self) -> impl Iterator<Item = (MeasIndex, &Shortname)> + '_ {
        self.iter().enumerate().filter_map(|(i, r)| {
            r.both(|x| Some(&x.key), |x| K::as_opt(&x.key))
                .map(|x| (i.into(), x))
        })
    }

    // /// Return all existing non-center names in the vector with their indices
    // pub(crate) fn indexed_non_center_names(
    //     &self,
    // ) -> impl Iterator<Item = (MeasIndex, &Shortname)> + '_ {
    //     self.iter().flat_map(|(i, r)| {
    //         r.non_center()
    //             .and_then(|x| K::as_opt(&x.key))
    //             .map(|x| (i, x))
    //     })
    // }

    /// Return iterator over key names with non-existent names as default.
    pub(crate) fn iter_all_names(&self) -> impl Iterator<Item = Shortname> + '_ {
        self.iter().enumerate().map(|(i, r)| {
            r.both(
                |x| x.key.clone(),
                |x| {
                    K::as_opt(&x.key)
                        .cloned()
                        .unwrap_or(MeasIndex::from(i).into())
                },
            )
        })
    }

    /// Alter values with a function and payload.
    ///
    /// Center and non-center values will be projected to a common type.
    pub(crate) fn alter_common_values_zip<F, X, R, T>(
        &mut self,
        xs: Vec<X>,
        f: F,
    ) -> Result<Vec<R>, KeyLengthError>
    where
        F: Fn(MeasIndex, &mut T, X) -> R,
        U: AsMut<T>,
        V: AsMut<T>,
    {
        self.alter_values_zip(
            xs,
            |v, x| f(v.index, v.value.as_mut(), x),
            |v, x| f(v.index, v.value.as_mut(), x),
        )
    }

    /// Apply functions to values with payload, altering them in place.
    ///
    /// This will alter all values, including center and non-center values. The
    /// two functions apply to the different values contained. Return None
    /// if input vector is not the same length.
    pub(crate) fn alter_values_zip<F, G, X, R>(
        &mut self,
        xs: Vec<X>,
        f: F,
        g: G,
    ) -> Result<Vec<R>, KeyLengthError>
    where
        F: Fn(IndexedElement<&K::Wrapper<Shortname>, &mut V>, X) -> R,
        G: Fn(IndexedElement<&Shortname, &mut U>, X) -> R,
    {
        self.check_keys_length(&xs[..], true)?;
        let go = |zs, ys, offset| Self::alter_paired_vec(zs, ys, offset, &f);
        let x = match self {
            Self::Split(s, _) => {
                let nleft = s.left.len();
                let nright = s.right.len();
                let mut it = xs.into_iter();
                let left_r: Vec<_> = go(&mut s.left, it.by_ref().take(nleft).collect(), 0);
                let c = &mut s.center;
                let center_r = g(
                    IndexedElement::new(nleft.into(), &c.key, &mut c.value),
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
            Self::Unsplit(u) => go(&mut u.members, xs, 0),
        };
        Ok(x)
    }

    pub(crate) fn alter_elements_zip<Fnoncenter, Fcenter, X, Y, R>(
        &mut self,
        // TODO make this (and others like it) take an iterator
        xs: Vec<Element<X, Y>>,
        f_noncenter: Fnoncenter,
        f_center: Fcenter,
    ) -> ErrorsResult<Vec<R>, (), SetElementsError>
    where
        Fnoncenter: Fn(IndexedElement<&K::Wrapper<Shortname>, &mut V>, Y) -> R,
        Fcenter: Fn(IndexedElement<&Shortname, &mut U>, X) -> R,
    {
        let go = |zs, ys, offset| Self::alter_paired_vec(zs, ys, offset, &f_noncenter);

        let check_optical = |ys: Vec<Element<X, Y>>| {
            ys.into_iter()
                .enumerate()
                .map(|(i, x)| x.both(|_| Result::new_err1::<_, _, Vec<_>>(i), Result::new_ok))
                .mappend_cmt()
                .map_non_fung_errors(|i| ColumnError::new(i, OpticalMismatchError::new(false)))
        };

        self.check_keys_length(&xs[..], true)
            .map_err(SetElementsError::from)?;

        let res = match self {
            Self::Split(s, _) => {
                let nleft = s.left.len();
                let mut it = xs.into_iter();
                // ASSUME this won't fail because we already counted
                let xs_left = it.by_ref().take(nleft).collect();
                let x_center = it.by_ref().next().unwrap();
                let xs_right = it.collect();
                let left_res = check_optical(xs_left);
                let center_res = x_center
                    .center()
                    .ok_or(ColumnError::new(nleft, OpticalMismatchError::new(false)))
                    .into_log();
                let right_res = check_optical(xs_right);
                left_res.zip3_cmt(center_res, right_res).map_ok_value(
                    |(ys_left, y_center, ys_right)| {
                        let left_out = go(&mut s.left, ys_left, 0);
                        let c = &mut s.center;
                        let center_index = IndexedElement::new(nleft.into(), &c.key, &mut c.value);
                        let center_out = f_center(center_index, y_center);
                        let right_out = go(&mut s.right, ys_right, 1 + nleft);
                        left_out
                            .into_iter()
                            .chain([center_out])
                            .chain(right_out)
                            .collect()
                    },
                )
            }
            Self::Unsplit(u) => check_optical(xs).map_ok_value(|ys| go(&mut u.members, ys, 0)),
        };
        res.map_non_fung_errors(SetElementsError::from)
    }

    /// Apply function(s) to all values, altering them in place.
    pub(crate) fn alter_values<F, G, R>(&mut self, f: F, g: G) -> Vec<R>
    where
        F: Fn(IndexedElement<&K::Wrapper<Shortname>, &mut V>) -> R,
        G: Fn(IndexedElement<&Shortname, &mut U>) -> R,
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
            Self::Split(s, _) => Some(s.left.len().into()),
            Self::Unsplit(_) => None,
        }
    }

    /// Apply function over center value, possibly changing it's type
    #[allow(clippy::type_complexity)]
    pub(crate) fn map_center_value<F, X, Y, E, LWC, RWC, EC>(
        self,
        f: F,
    ) -> LogResult<
        NamedVec<K, K::Wrapper<Shortname>, X, V>,
        Y,
        IndexedElementError<E>,
        LWC,
        RWC,
        Mapped<EC, IndexedElementError<E>>,
    >
    where
        F: Fn(IndexedElement<&Shortname, U>) -> LogResult<X, Y, E, LWC, RWC, EC>,
        EC: Functor<E>,
        LWC: Default,
    {
        match self {
            Self::Split(s, _) => {
                let c = s.center;
                let index = s.left.len().into();
                let ckey = c.key;
                let e = IndexedElement::new(index, &ckey, c.value);
                f(e).map_ok_value(|value| {
                    let center = Pair { value, key: ckey };
                    NamedVec::new_split(s.left, center, s.right)
                })
                .map_non_fung_errors(|error| IndexedElementError::new(error, index))
            }
            Self::Unsplit(u) => Result::new_ok(NamedVec::Unsplit(u)),
        }
    }

    /// Apply function over non-center values, possibly changing their type
    pub(crate) fn map_non_center_values<F, Vf, E, WC, EC>(
        self,
        f: F,
    ) -> CmtResult<
        WrappedNamedVec<K, U, Vf>,
        (),
        IndexedElementError<E>,
        WC,
        Vec<IndexedElementError<E>>,
    >
    where
        F: Fn(MeasIndex, V) -> CmtResult<Vf, (), E, WC, EC>,
        WC: Default + Semigroup,
        EC: Functor<E>,
        <EC::Family as Kind1>::Inner<IndexedElementError<E>>: IntoIterator<Item = IndexedElementError<E>>
            + Extend<IndexedElementError<E>>
            + IntoZeroOrMore<Vec<IndexedElementError<E>>>,
    {
        let go = |xs: WrappedPairedVec<K, V>, offset: usize| {
            xs.into_iter()
                .enumerate()
                .map(|(i, p)| {
                    let j = i + offset;
                    f(j.into(), p.value)
                        .map_ok_value(|value| Pair::new(p.key, value))
                        .map_non_fung_errors(|error| IndexedElementError::new(error, j.into()))
                })
                .mappend_cmt()
        };
        match self {
            Self::Split(s, _) => {
                let nleft = s.left.len();
                let lres = go(s.left, 0);
                let rres = go(s.right, nleft + 1);
                lres.zip_cmt(rres)
                    .map_ok_value(|(left, right)| NamedVec::new_split(left, *s.center, right))
            }
            Self::Unsplit(u) => go(u.members, 0)
                .map_ok_value(|members| NamedVec::new_unsplit(members))
                .repack_errors(),
        }
    }

    /// Return number of all elements.
    pub(crate) fn len(&self) -> usize {
        self.iter().count()
    }

    /// Return number of non-center elements.
    pub(crate) fn len_non_center(&self) -> usize {
        self.iter().filter(Element::is_non_center).count()
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
    ) -> Result<Element<(&Shortname, &U), (&K::Wrapper<Shortname>, &V)>, ElementIndexError> {
        let i = self.check_element_index(index, true)?;
        match self {
            Self::Split(s, _) => {
                let left_len = s.left.len();
                match i.cmp(&left_len) {
                    Less => Ok(Element::NonCenter(&s.left[i])),
                    Equal => Ok(Element::Center(&s.center)),
                    Greater => Ok(Element::NonCenter(&s.left[i - left_len - 1])),
                }
            }
            Self::Unsplit(u) => Ok(Element::NonCenter(&u.members[i])),
        }
        .map(|x| x.bimap(|p| (&p.key, &p.value), |p| (&p.key, &p.value)))
    }

    /// Get reference with name.
    pub fn get_name(
        &self,
        n: &Shortname,
    ) -> Result<(MeasIndex, Element<&U, &V>), KeyNotFoundError> {
        self.iter()
            .enumerate()
            .find_map(|(i, e)| {
                let x = e.as_ref();
                x.both(
                    |t| &t.key == n,
                    |o| K::as_opt(&o.key).is_some_and(|kn| kn == n),
                )
                .then_some((i.into(), e.bimap(|p| &p.value, |p| &p.value)))
            })
            .ok_or_else(|| KeyNotFoundError(n.clone()))
    }

    // /// Get mutable reference at position.
    // #[allow(clippy::type_complexity)]
    // pub fn get_mut(
    //     &mut self,
    //     index: MeasIndex,
    // ) -> Result<Element<(&Shortname, &mut U), (&K::Wrapper<Shortname>, &mut V)>, ElementIndexError>
    // {
    //     let i = self.check_element_index(index, true)?;
    //     match self {
    //         Self::Split(s, _) => {
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
    //         Self::Split(s, _) => {
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

    /// Add a new non-center element at the end of the vector
    pub(crate) fn push(
        &mut self,
        key: K::Wrapper<Shortname>,
        value: V,
    ) -> Result<Shortname, NonUniqueKeyError> {
        let index = self.len().into();
        let (ckey, name) = self.check_key(key, index)?;
        let p = Pair { key: ckey, value };
        match self {
            Self::Split(s, _) => s.right.push(p),
            Self::Unsplit(u) => u.members.push(p),
        }
        Ok(name)
    }

    /// Insert a new non-center element at a given position.
    pub(crate) fn insert(
        &mut self,
        index: MeasIndex,
        key: K::Wrapper<Shortname>,
        value: V,
    ) -> Result<Shortname, InsertError> {
        let i = self
            .check_boundary_index(index)
            .map_err(InsertError::Index)?;
        let (ckey, name) = self.check_key(key, index).map_err(InsertError::NonUnique)?;
        let p = Pair { key: ckey, value };
        match self {
            Self::Split(s, _) => {
                let ln = s.left.len();
                match i.cmp(&ln) {
                    Less | Equal => s.left.insert(i, p),
                    Greater => s.right.insert(i - ln - 1, p),
                }
            }
            Self::Unsplit(u) => u.members.insert(i, p),
        }
        Ok(name)
    }

    /// Replace a non-center value with a new value at given position.
    ///
    /// Return value that was replaced.
    ///
    /// Return none if index is out of bounds. If index points to the center,
    /// convert it to a non-center value.
    pub(crate) fn replace_at(
        &mut self,
        index: MeasIndex,
        value: V,
    ) -> Result<Element<U, V>, ElementIndexError> {
        let i = self.check_element_index(index, true)?;
        let (newself, ret) = match mem::replace(self, dummy()) {
            Self::Split(mut s, p) => {
                let ln = s.left.len();
                match i.cmp(&ln) {
                    Less => {
                        let ret = mem::replace(&mut s.left[i].value, value);
                        (Self::Split(s, p), Element::NonCenter(ret))
                    }
                    Equal => {
                        let key = K::wrap(s.center.key);
                        let members = s
                            .left
                            .into_iter()
                            .chain([Pair { key, value }])
                            .chain(s.right)
                            .collect();
                        (Self::new_unsplit(members), Element::Center(s.center.value))
                    }
                    Greater => {
                        let ret = mem::replace(&mut s.left[i - ln - 1].value, value);
                        (Self::Split(s, p), Element::NonCenter(ret))
                    }
                }
            }
            Self::Unsplit(mut u) => {
                let ret = mem::replace(&mut u.members[i].value, value);
                (Self::Unsplit(u), Element::NonCenter(ret))
            }
        };
        *self = newself;
        Ok(ret)
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
    ) -> Result<Element<U, V>, KeyNotFoundError> {
        let index = self.find_with_name(name)?;
        // ASSUME this won't fail because we have a valid index from above
        Ok(self.replace_at(index, value).unwrap())
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
        key: K::Wrapper<Shortname>,
    ) -> Result<(Shortname, Shortname), RenameError> {
        let i = self
            .check_element_index(index, true)
            .map_err(RenameError::Index)?;
        let k = to_opt_or_indexed::<K>(K::as_ref(&key), index);
        if self
            .iter_all_names()
            .enumerate()
            .any(|(j, n)| j != i && n == k)
        {
            Err(RenameError::NonUnique(NonUniqueKeyError { name: k }))
        } else {
            let old = match self {
                Self::Split(s, _) => {
                    let ln = s.left.len();
                    match i.cmp(&ln) {
                        Less => mem::replace(&mut s.left[i].key, key),
                        Equal => K::wrap(mem::replace(&mut s.center.key, k.clone())),
                        Greater => mem::replace(&mut s.right[i - ln - 1].key, key),
                    }
                }
                Self::Unsplit(u) => mem::replace(&mut u.members[i].key, key),
            };
            let old_k = to_opt_or_indexed::<K>(K::as_ref(&old), index);
            Ok((old_k, k))
        }
    }

    /// Rename center element.
    ///
    /// Return previous name if center exists.
    pub(crate) fn rename_center(&mut self, name: Shortname) -> Option<Shortname> {
        match self {
            Self::Split(s, _) => Some(mem::replace(&mut s.center.key, name)),
            Self::Unsplit(_) => None,
        }
    }

    /// Push a new center element to the end of the vector
    ///
    /// Return error if center already exists.
    pub(crate) fn push_center(
        &mut self,
        name: Shortname,
        value: U,
    ) -> Result<(), InsertCenterError> {
        let key = self
            .check_name(name)
            .map_err(InsertError::NonUnique)
            .map_err(InsertCenterError::Insert)?;
        let p = Pair { key, value };
        let (newself, ret) = match mem::replace(self, dummy()) {
            Self::Unsplit(u) => (Self::new_split(u.members, p, vec![]), Ok(())),
            s @ Self::Split(_, _) => (s, Err(InsertCenterError::Present)),
        };
        *self = newself;
        ret
    }

    /// Insert a new center element at a given position.
    ///
    /// Return error if center already exists.
    pub(crate) fn insert_center(
        &mut self,
        index: MeasIndex,
        name: Shortname,
        value: U,
    ) -> Result<(), InsertCenterError> {
        let i = self
            .check_boundary_index(index)
            .map_err(InsertError::Index)
            .map_err(InsertCenterError::Insert)?;
        let key = self
            .check_name(name)
            .map_err(InsertError::NonUnique)
            .map_err(InsertCenterError::Insert)?;
        let p = Pair { key, value };
        let (newself, ret) = match mem::replace(self, dummy()) {
            Self::Unsplit(u) => {
                let mut it = u.members.into_iter();
                let left: Vec<_> = it.by_ref().take(i).collect();
                let right: Vec<_> = it.collect();
                (Self::new_split(left, p, right), Ok(()))
            }
            s @ Self::Split(_, _) => (s, Err(InsertCenterError::Present)),
        };
        *self = newself;
        ret
    }

    /// Remove key/value pair by name.
    pub(crate) fn remove_index(
        &mut self,
        index: MeasIndex,
    ) -> Result<EitherPair<K, U, V>, ElementIndexError> {
        let i = self.check_element_index(index, true)?;
        let (newself, ret) = match mem::replace(self, dummy()) {
            Self::Split(mut s, p) => {
                let nleft = s.left.len();
                match i.cmp(&nleft) {
                    Less => {
                        let x = s.left.remove(i);
                        (Self::Split(s, p), Ok(Element::NonCenter(x)))
                    }
                    Equal => {
                        let new = s.left.into_iter().chain(s.right).collect();
                        let ret = Ok(Element::Center(*s.center));
                        (Self::new_unsplit(new), ret)
                    }
                    Greater => {
                        let x = s.right.remove(i - nleft - 1);
                        (Self::Split(s, p), Ok(Element::NonCenter(x)))
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
    ) -> Result<(MeasIndex, Element<U, V>), KeyNotFoundError> {
        let go = |xs: &mut Vec<_>| {
            let i = Self::position_by_name(xs, n)?;
            let p = xs.remove(i);
            Ok((i.into(), p.value))
        };
        let (newself, ret) = match mem::replace(self, dummy()) {
            Self::Split(mut s, p) => {
                if let Ok((i, v)) = go(&mut s.left).or(go(&mut s.right)) {
                    (Self::Split(s, p), Ok((i, Element::NonCenter(v))))
                } else if &s.center.key == n {
                    let i = s.left.len().into();
                    let xs = s.left.into_iter().chain(s.right).collect();
                    let new = Self::new_unsplit(xs);
                    (new, Ok((i, Element::Center(s.center.value))))
                } else {
                    (Self::Split(s, p), Err(KeyNotFoundError(n.clone())))
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
    pub(crate) fn set_keys(
        &mut self,
        ks: Vec<K::Wrapper<Shortname>>,
    ) -> Result<NameMapping, SetKeysError>
    where
        K::Wrapper<Shortname>: Clone,
    {
        self.check_keys_length(&ks[..], true)
            .map_err(SetNamesError::Length)?;
        if !all_unique_names::<K>(ks.iter().map(K::as_ref).collect()) {
            return Err(SetNamesError::NonUnique.into());
        }
        let mut mapping = HashMap::new();
        let mut go = |side: &mut WrappedPairedVec<K, V>, ks_side: Vec<K::Wrapper<Shortname>>| {
            for (p, k) in side.iter_mut().zip(ks_side) {
                let old = mem::replace(&mut p.key, k.clone());
                if let (Some(old_name), Some(new_name)) = (K::to_opt(old), K::to_opt(k)) {
                    mapping.insert(old_name, new_name);
                }
            }
        };
        match self {
            Self::Split(s, _) => {
                let mut it = ks.into_iter();
                // ASSUME this won't fail because we checked length above
                let ks_left = it.by_ref().take(s.left.len()).collect();
                let center = it.by_ref().next().expect("center should be set");
                let ks_right = it.collect();
                if let Some(center_name) = K::to_opt(center) {
                    go(&mut s.left, ks_left);
                    s.center.key = center_name;
                    go(&mut s.right, ks_right);
                } else {
                    return Err(SetKeysError::MissingCenter);
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
    //     ks: Vec<K::Wrapper<Shortname>>,
    // ) -> Result<NameMapping, SetNamesError>
    // where
    //     K::Wrapper<Shortname>: Clone,
    // {
    //     self.check_keys_length(&ks[..], false)
    //         .map_err(SetNamesError::Length)?;
    //     let center = self.as_center().map(|x| K::wrap(x.key));
    //     let all_keys = ks.iter().map(K::as_ref).chain(center).collect();
    //     if !self.as_prefix().all_unique::<K>(all_keys) {
    //         return Err(SetNamesError::NonUnique);
    //     }
    //     let mut mapping = HashMap::new();
    //     let mut go = |side: &mut WrappedPairedVec<K, V>, ks_side: Vec<K::Wrapper<Shortname>>| {
    //         for (p, k) in side.iter_mut().zip(ks_side) {
    //             let old = mem::replace(&mut p.key, k.clone());
    //             if let (Some(old_name), Some(new_name)) = (K::to_opt(old), K::to_opt(k)) {
    //                 mapping.insert(old_name, new_name);
    //             }
    //         }
    //     };
    //     match self {
    //         Self::Split(s, _) => {
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
    pub(crate) fn set_names(&mut self, ns: Vec<Shortname>) -> Result<NameMapping, SetNamesError> {
        self.check_keys_length(&ns[..], true)
            .map_err(SetNamesError::Length)?;
        if !all_unique(ns.iter()) {
            return Err(SetNamesError::NonUnique);
        }
        let mut mapping = HashMap::new();
        let mut go = |side: &mut WrappedPairedVec<K, V>, ns_side: Vec<Shortname>| {
            for (p, n) in side.iter_mut().zip(ns_side) {
                let old = mem::replace(&mut p.key, K::wrap(n.clone()));
                if let Some(old_name) = K::to_opt(old) {
                    mapping.insert(old_name, n);
                }
            }
        };
        match self {
            Self::Split(s, _) => {
                let mut ns_left = ns;
                let mut ns_right = ns_left.split_off(s.left.len());
                // ASSUME this won't fail because we already checked length
                let n_center = ns_right.pop().unwrap();
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
    pub(crate) fn replace_center_by_name<F, E, LWC, RWC, EC>(
        &mut self,
        n: &Shortname,
        value: U,
        to_v: F,
    ) -> LogResult<Element<U, V>, (), E, LWC, RWC, EC>
    where
        F: FnOnce(MeasIndex, U) -> LogResult<V, Box<U>, E, LWC, RWC, EC>,
        E: From<KeyNotFoundError>,
        EC: Default,
        RWC: Default,
        LWC: Default,
    {
        let index = self.find_with_name(n).map_err(E::from)?;
        self.replace_center_at_inner(index, value, to_v)
    }

    /// Replace any value with a center value with name.
    pub(crate) fn replace_center_by_name_nofail<F>(
        &mut self,
        n: &Shortname,
        value: U,
        to_v: F,
    ) -> Result<Element<U, V>, KeyNotFoundError>
    where
        F: FnOnce(MeasIndex, U) -> V,
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
    pub(crate) fn replace_center_at<F, E, WC, EC>(
        &mut self,
        index: MeasIndex,
        value: U,
        to_v: F,
    ) -> CmtResult<Element<U, V>, (), E, WC, EC>
    where
        F: FnOnce(MeasIndex, U) -> CmtResult<V, Box<U>, E, WC, EC>,
        E: From<SetCenterError>,
        WC: Default + Semigroup,
        EC: Default,
    {
        if !self
            .get(index)
            .unwrap()
            .both(|_| true, |(n, _)| K::as_opt(n).is_some())
        {
            return Result::new_err1(SetCenterError::NoName.into());
        }

        self.check_element_index(index, true)
            .map_err(SetCenterError::Index)
            .map_err(E::from)
            .into_log::<_, _, EC>()
            .and_then_cmt(|i| self.replace_center_at_inner(i.into(), value, to_v))
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
    {
        if !self
            .get(index)
            .unwrap()
            .both(|_| true, |(n, _)| K::as_opt(n).is_some())
        {
            return Err(SetCenterError::NoName);
        }

        let i = self.check_element_index(index, true)?;

        let go = |j, u| Result::new_ok::<_, _, _, NeverValue<Infallible>>(to_v(j, u));

        let res = self.replace_center_at_inner(i.into(), value, go);
        Ok(res.infallible_nowarn_into())
    }

    fn alter_paired_vec<X, F, R>(
        zs: &mut PairedVec<K::Wrapper<Shortname>, V>,
        ys: Vec<X>,
        offset: usize,
        f: &F,
    ) -> Vec<R>
    where
        F: Fn(IndexedElement<&K::Wrapper<Shortname>, &mut V>, X) -> R,
    {
        // ASSUME both vectors are the same length
        zs.iter_mut()
            .zip(ys)
            .enumerate()
            .map(|(i, (y, x))| {
                f(
                    IndexedElement::new((i + offset).into(), &y.key, &mut y.value),
                    x,
                )
            })
            .collect()
    }

    fn replace_center_at_inner<F, E, LWC, RWC, EC>(
        &mut self,
        index: MeasIndex,
        value: U,
        to_v: F,
    ) -> LogResult<Element<U, V>, (), E, LWC, RWC, EC>
    where
        F: FnOnce(MeasIndex, U) -> LogResult<V, Box<U>, E, LWC, RWC, EC>,
        LWC: Default,
    {
        let res = match mem::replace(self, dummy()) {
            Self::Split(s, _) => match split_at_index::<K, U, V>(s, index.into()) {
                PartialSplit::Left(split) => to_v(index, split.center_value)
                    .inject_value((split.stable, split.selected_left_value))
                    .map_ok_value(|(new_right_val, (stable, old_left_val))| {
                        let sp = Self::new_split_from_left(value, new_right_val, stable).unwrap();
                        (sp, Element::NonCenter(old_left_val))
                    })
                    .map_err_value(|(center_val, (stable, old_left_val))| {
                        Self::recover_split_from_left(old_left_val, *center_val, stable)
                    }),

                PartialSplit::Center(x) => {
                    let center = Pair::new(x.center.key, value);
                    let sp = Self::new_split(x.left, center, x.right);
                    Result::new_ok((sp, Element::Center(x.center.value)))
                }

                PartialSplit::Right(split) => to_v(index, split.center_value)
                    .inject_value((split.stable, split.selected_right_value))
                    .map_ok_value(|(new_left_val, (stable, old_right_val))| {
                        let sp = Self::new_split_from_right(value, new_left_val, stable).unwrap();
                        (sp, Element::NonCenter(old_right_val))
                    })
                    .map_err_value(|(center_val, (stable, old_right_val))| {
                        Self::recover_split_from_right(old_right_val, *center_val, stable)
                    }),
            },

            Self::Unsplit(u) => {
                let x = split_paired_vec::<K, V>(u.members, index.into());
                let ret = x.selected.value;
                let center = Pair {
                    key: K::to_opt(x.selected.key).unwrap(),
                    value,
                };
                let sp = Self::new_split(x.left, center, x.right);
                Result::new_ok((sp, Element::NonCenter(ret)))
            }
        };

        res.map_ok_value(|(newself, ret)| {
            *self = newself;
            ret
        })
        .map_err_value(|newself| *self = newself)
    }

    /// Set center to be the element with name if it exists.
    pub(crate) fn set_center_by_name<Fswap, FtoU, E, LWC, RWC, EC>(
        &mut self,
        n: &Shortname,
        swap: Fswap,
        to_u: FtoU,
    ) -> LogResult<bool, (), E, LWC, RWC, EC>
    where
        Fswap: FnOnce(MeasIndex, U, V) -> LogResult<(V, U), Box<(U, V)>, E, LWC, RWC, EC>,
        FtoU: FnOnce(MeasIndex, V) -> LogResult<U, Box<V>, E, LWC, RWC, EC>,
        E: From<SetCenterError> + From<KeyNotFoundError>,
        EC: Default,
        RWC: Default,
        LWC: Default,
    {
        let index = self.find_with_name(n).map_err(E::from)?;
        self.set_center_by_index(index, swap, to_u)
    }

    /// Set center to be the element with index if it exists.
    pub(crate) fn set_center_by_index<Fswap, FtoU, E, LWC, RWC, EC>(
        &mut self,
        index: MeasIndex,
        swap: Fswap,
        to_u: FtoU,
    ) -> LogResult<bool, (), E, LWC, RWC, EC>
    where
        Fswap: FnOnce(MeasIndex, U, V) -> LogResult<(V, U), Box<(U, V)>, E, LWC, RWC, EC>,
        FtoU: FnOnce(MeasIndex, V) -> LogResult<U, Box<V>, E, LWC, RWC, EC>,
        E: From<SetCenterError>,
        EC: Default,
        RWC: Default,
        LWC: Default,
    {
        if !self
            .get(index)
            .unwrap()
            .both(|_| true, |(n, _)| K::as_opt(n).is_some())
        {
            return Result::new_err1(SetCenterError::NoName.into());
        }
        let i = self
            .check_element_index(index, true)
            .map_err(SetCenterError::Index)
            .map_err(E::from)?;

        let res = match mem::replace(self, dummy()) {
            Self::Split(s, p) => match split_at_index::<K, U, V>(s, i) {
                PartialSplit::Left(split) => {
                    swap(i.into(), split.center_value, split.selected_left_value)
                        .map_err_value(|x| *x)
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

                PartialSplit::Center(sc) => Result::new_ok((Self::Split(sc, p), false)),

                PartialSplit::Right(split) => {
                    swap(i.into(), split.center_value, split.selected_right_value)
                        .map_err_value(|x| *x)
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
                let x = split_paired_vec::<K, V>(u.members, i);
                to_u(i.into(), x.selected.value)
                    .inject_value((x.left, x.selected.key, x.right))
                    .map_ok_value(|(new_value, (left, key, right))| {
                        let center = Pair::new(K::to_opt(key).unwrap(), new_value);
                        (Self::new_split(left, center, right), true)
                    })
                    .map_err_value(|(old_value, (left, key, right))| {
                        let center = Pair::new(key, *old_value);
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
    pub(crate) fn unset_center<F, X, E, LWC, RWC, EC>(
        &mut self,
        to_v: F,
    ) -> LogResult<Option<X>, (), E, LWC, RWC, EC>
    where
        F: FnOnce(MeasIndex, U) -> LogResult<(V, X), Box<U>, E, LWC, RWC, EC>,
        LWC: Default,
    {
        match mem::replace(self, dummy()) {
            Self::Split(s, _) => {
                let index = (s.left.len()).into();
                to_v(index, s.center.value)
                    .inject_value((s.left, s.center.key, s.right))
                    .map_ok_value(|((value, ret), (left, center_key, right))| {
                        let non_center = Pair::new(K::wrap(center_key), value);
                        let members = left.into_iter().chain([non_center]).chain(right).collect();
                        (Self::new_unsplit(members), Some(ret))
                    })
                    .map_err_value(|(value, (left, center_key, right))| {
                        let center = Pair::new(center_key, *value);
                        Self::new_split(left, center, right)
                    })
            }
            Self::Unsplit(u) => Result::new_ok((Self::Unsplit(u), None)),
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
    pub(crate) fn try_rewrapped<J>(
        self,
    ) -> ErrorsResult<
        WrappedNamedVec<J, U, V>,
        (),
        IndexedElementError<<J::Wrapper<Shortname> as TryFrom<K::Wrapper<Shortname>>>::Error>,
    >
    where
        J: MightHave,
        J::Wrapper<Shortname>: TryFrom<K::Wrapper<Shortname>>,
    {
        let go = |xs: WrappedPairedVec<K, V>, offset: usize| {
            xs.into_iter()
                .enumerate()
                .map(|(i, p)| {
                    Self::try_into_wrapper::<J>(p)
                        .map_err(|error| IndexedElementError::new(error, (i + offset).into()))
                        .into_nowarn()
                })
                .mappend_cmt()
        };
        match self {
            Self::Split(s, _) => {
                let offset = s.left.len() + 1;
                let lres = go(s.left, 0);
                let rres = go(s.right, offset);
                lres.zip_cmt(rres)
                    .map_ok_value(|(left, right)| NamedVec::new_split(left, *s.center, right))
            }
            Self::Unsplit(u) => go(u.members, 0).map_ok_value(NamedVec::new_unsplit),
        }
    }

    #[allow(clippy::type_complexity)]
    fn try_into_wrapper<J>(
        p: WrappedPair<K, V>,
    ) -> Result<WrappedPair<J, V>, <J::Wrapper<Shortname> as TryFrom<K::Wrapper<Shortname>>>::Error>
    where
        J: MightHave,
        J::Wrapper<Shortname>: TryFrom<K::Wrapper<Shortname>>,
    {
        Ok(Pair {
            key: p.key.try_into()?,
            value: p.value,
        })
    }

    // fn from_center(p: Center<U>) -> WrappedPair<K, V>
    // where
    //     V: From<U>,
    // {
    //     Pair {
    //         key: K::wrap(p.key),
    //         value: p.value.into(),
    //     }
    // }

    fn position_by_name(
        xs: &WrappedPairedVec<K, V>,
        n: &Shortname,
    ) -> Result<usize, KeyNotFoundError> {
        xs.iter()
            .position(|p| K::as_opt(&p.key).is_some_and(|kn| kn == n))
            .ok_or(KeyNotFoundError(n.to_owned()))
    }

    // fn value_by_name_mut<'a>(
    //     xs: &'a mut WrappedPairedVec<K, V>,
    //     n: &Shortname,
    // ) -> Option<(usize, &'a mut V)> {
    //     xs.iter_mut()
    //         .enumerate()
    //         .find(|(_, p)| K::as_opt(&p.key).is_some_and(|kn| kn == n))
    //         .map(|(i, p)| (i, &mut p.value))
    // }

    fn check_key(
        &self,
        key: K::Wrapper<Shortname>,
        index: MeasIndex,
    ) -> Result<(K::Wrapper<Shortname>, Shortname), NonUniqueKeyError> {
        let name = to_opt_or_indexed::<K>(K::as_ref(&key), index);
        if self.iter_all_names().any(|n| n == name) {
            Err(NonUniqueKeyError { name })
        } else {
            Ok((key, name))
        }
    }

    fn check_name(&self, name: Shortname) -> Result<Shortname, NonUniqueKeyError> {
        if self.iter_all_names().any(|n| n == name) {
            Err(NonUniqueKeyError { name })
        } else {
            Ok(name)
        }
    }

    fn check_element_index(
        &self,
        index: MeasIndex,
        include_center: bool,
    ) -> Result<usize, ElementIndexError> {
        let len = self.len();
        IndexFromOne::from(index).check_index(len).map_or_else(
            |e| {
                Err(ElementIndexError {
                    index: e,
                    center: None,
                })
            },
            |i| {
                if let Some(j) = self.center_index()
                    && !include_center
                    && usize::from(j) == i
                {
                    return Err(ElementIndexError {
                        index: IndexError {
                            index: i.into(),
                            len,
                        },
                        center: Some(j),
                    });
                }
                Ok(i)
            },
        )
    }

    fn check_boundary_index(&self, index: MeasIndex) -> Result<usize, BoundaryIndexError> {
        IndexFromOne::from(index).check_boundary_index(self.len())
    }

    fn check_keys_length<X>(&self, xs: &[X], include_center: bool) -> Result<(), KeyLengthError> {
        let this_len = if include_center {
            self.len()
        } else {
            self.len_non_center()
        };
        let other_len = xs.len();
        if this_len != other_len {
            return Err(KeyLengthError {
                this_len,
                other_len,
                include_center,
            });
        }
        Ok(())
    }

    fn find_with_name(&self, name: &Shortname) -> Result<MeasIndex, KeyNotFoundError> {
        self.iter()
            .find_position(|x| {
                x.as_ref().both(
                    |l| &l.key == name,
                    |r| K::as_opt(&r.key).is_some_and(|k| k == name),
                )
            })
            .map(|(i, _)| i.into())
            .ok_or(KeyNotFoundError(name.to_owned()))
    }

    fn new_split(
        left: WrappedPairedVec<K, V>,
        center: Center<U>,
        right: WrappedPairedVec<K, V>,
    ) -> Self {
        Self::Split(SplitVec::new(left, Box::new(center), right), PhantomData)
    }

    fn new_split_from_left(
        new_center_value: U,
        new_right_value: V,
        stable: LeftSplitStable<K, V>,
    ) -> Option<Self> {
        let new_center = Pair::new(K::to_opt(stable.selected_left_key)?, new_center_value);
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
    ) -> Option<Self> {
        let new_center = Pair::new(K::to_opt(stable.selected_right_key)?, new_center_value);
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

    fn new_unsplit(members: WrappedPairedVec<K, V>) -> Self {
        Self::Unsplit(UnsplitVec { members })
    }
}

impl<K: MightHave, U, V> Clone for Eithers<K, U, V>
where
    K::Wrapper<Shortname>: Clone,
    U: Clone,
    V: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<K: MightHave, U, V> Eithers<K, U, V> {
    pub(crate) fn non_center_names(&self) -> impl Iterator<Item = &Shortname> {
        self.0
            .iter()
            .filter_map(|x| x.as_ref().non_center().and_then(|v| K::as_opt(&v.0)))
    }

    #[must_use]
    pub fn inner_into<U0, V0>(self) -> Eithers<K, U0, V0>
    where
        U0: From<U>,
        V0: From<V>,
    {
        Eithers(
            self.0
                .into_iter()
                .map(|e| e.bimap(|(n, y)| (n, y.into()), |(n, y)| (n, y.into())))
                .collect(),
        )
    }
}

impl<U, V> Element<U, V> {
    pub fn inner_into<U1, V1>(self) -> Element<U1, V1>
    where
        U1: From<U>,
        V1: From<V>,
    {
        self.bimap(Into::into, Into::into)
    }

    pub fn unzip<K: MightHave>(e: EitherPair<K, U, V>) -> (K::Wrapper<Shortname>, Self) {
        e.both(
            |p| (K::wrap(p.key), Self::Center(p.value)),
            |p| (p.key, Self::NonCenter(p.value)),
        )
    }

    pub fn bimap<F, G, X, Y>(self, f: F, g: G) -> Element<X, Y>
    where
        F: Fn(U) -> X,
        G: Fn(V) -> Y,
    {
        match self {
            Self::Center(u) => Element::Center(f(u)),
            Self::NonCenter(v) => Element::NonCenter(g(v)),
        }
    }

    pub fn map_center<F, X>(self, f: F) -> Element<X, V>
    where
        F: Fn(U) -> X,
    {
        match self {
            Self::Center(u) => Element::Center(f(u)),
            Self::NonCenter(v) => Element::NonCenter(v),
        }
    }

    pub fn map_non_center<F, X>(self, f: F) -> Element<U, X>
    where
        F: Fn(V) -> X,
    {
        match self {
            Self::Center(u) => Element::Center(u),
            Self::NonCenter(v) => Element::NonCenter(f(v)),
        }
    }

    pub fn both<F, G, X>(self, f: F, g: G) -> X
    where
        F: Fn(U) -> X,
        G: Fn(V) -> X,
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

    pub fn is_non_center(&self) -> bool {
        match self {
            Self::Center(_) => false,
            Self::NonCenter(_) => true,
        }
    }

    pub fn is_center(&self) -> bool {
        match self {
            Self::Center(_) => true,
            Self::NonCenter(_) => false,
        }
    }
}

impl<X> Element<X, X> {
    pub fn unwrap(self) -> X {
        self.both(|x| x, |y| y)
    }
}

fn to_opt_or_indexed<X: MightHave>(x: X::Wrapper<&Shortname>, i: MeasIndex) -> Shortname {
    X::to_opt(x).cloned().unwrap_or(i.into())
}

fn all_unique_names<X: MightHave>(xs: Vec<X::Wrapper<&Shortname>>) -> bool {
    all_unique(
        xs.into_iter()
            .enumerate()
            .map(|(i, x)| to_opt_or_indexed::<X>(x, i.into())),
    )
}

fn all_unique<'a, T: Hash + Eq>(xs: impl Iterator<Item = T> + 'a) -> bool {
    let mut unique = HashSet::new();
    for x in xs {
        if unique.contains(&x) {
            return false;
        }
        unique.insert(x);
    }
    true
}

// dummy value to use when mutating NamedVec in place
fn dummy<K, W, U, V>() -> NamedVec<K, W, U, V> {
    NamedVec::Unsplit(UnsplitVec { members: vec![] })
}

enum PartialSplit<K: MightHave, U, V> {
    // Left(PairedSplit<K, V>, Box<Center<U>>, PairedVec<K, V>),
    Left(LeftSplit<K, U, V>),
    Center(SplitVec<<K as MightHave>::Wrapper<Shortname>, U, V>),
    // Right(PairedVec<K, V>, Box<Center<U>>, PairedSplit<K, V>),
    Right(RightSplit<K, U, V>),
}

#[derive(new)]
struct LeftSplit<K: MightHave, U, V> {
    selected_left_value: V,
    center_value: U,
    stable: LeftSplitStable<K, V>,
}

#[derive(new)]
struct RightSplit<K: MightHave, U, V> {
    selected_right_value: V,
    center_value: U,
    stable: RightSplitStable<K, V>,
}

#[derive(new)]
struct LeftSplitStable<K: MightHave, V> {
    left_left: WrappedPairedVec<K, V>,
    selected_left_key: K::Wrapper<Shortname>,
    left_right: WrappedPairedVec<K, V>,
    center_key: Shortname,
    right: WrappedPairedVec<K, V>,
}

#[derive(new)]
struct RightSplitStable<K: MightHave, V> {
    left: WrappedPairedVec<K, V>,
    center_key: Shortname,
    right_left: WrappedPairedVec<K, V>,
    selected_right_key: K::Wrapper<Shortname>,
    right_right: WrappedPairedVec<K, V>,
}

struct PairedSplit<K, V> {
    left: PairedVec<K, V>,
    selected: Pair<K, V>,
    right: PairedVec<K, V>,
}

fn split_paired_vec<K: MightHave, V>(
    xs: WrappedPairedVec<K, V>,
    index: usize,
) -> PairedSplit<<K as MightHave>::Wrapper<Shortname>, V> {
    let mut it = xs.into_iter();
    PairedSplit {
        left: it.by_ref().take(index).collect(),
        selected: it.next().unwrap(),
        right: it.collect(),
    }
}

fn split_at_index<K: MightHave, U, V>(
    s: SplitVec<<K as MightHave>::Wrapper<Shortname>, U, V>,
    index: usize,
) -> PartialSplit<K, U, V> {
    let nleft = s.left.len();
    match index.cmp(&nleft) {
        Less => {
            let split_left = split_paired_vec::<K, V>(s.left, index);
            let stable = LeftSplitStable::new(
                split_left.left,
                split_left.selected.key,
                split_left.right,
                s.center.key,
                s.right,
            );
            let split = LeftSplit::new(split_left.selected.value, s.center.value, stable);
            PartialSplit::Left(split)
        }
        Equal => PartialSplit::Center(s),
        Greater => {
            let split_right = split_paired_vec::<K, V>(s.right, index);
            let stable = RightSplitStable::new(
                s.left,
                s.center.key,
                split_right.left,
                split_right.selected.key,
                split_right.right,
            );
            let split = RightSplit::new(split_right.selected.value, s.center.value, stable);
            PartialSplit::Right(split)
        }
    }
}

#[derive(Debug, Display, Error)]
pub enum InsertError {
    Index(BoundaryIndexError),
    NonUnique(NonUniqueKeyError),
}

#[derive(Debug, Display, Error)]
pub enum RenameError {
    Index(ElementIndexError),
    NonUnique(NonUniqueKeyError),
}

#[derive(Debug, Error)]
#[error("'{0}' matches no measurement")]
pub struct KeyNotFoundError(Shortname);

#[derive(Debug, Error)]
pub enum InsertCenterError {
    #[error("Center already exists")]
    Present,
    #[error("{0}")]
    Insert(InsertError),
}

#[derive(Debug, Error)]
pub enum SetKeysError {
    #[error("{0}")]
    Names(#[from] SetNamesError),
    #[error("center must not be missing")]
    MissingCenter,
}

#[derive(Debug, Error)]
pub enum SetNamesError {
    #[error("{0}")]
    Length(#[from] KeyLengthError),
    #[error("not all supplied keys are unique")]
    NonUnique,
}

#[derive(Debug, Error)]
pub enum SetCenterError {
    #[error("{0}")]
    Index(#[from] ElementIndexError),
    #[error("index refers to element with no name")]
    NoName,
}

#[derive(Debug, Error)]
#[error("'{name}' already present")]
pub struct NonUniqueKeyError {
    name: Shortname,
}

#[derive(Debug, Error)]
pub struct ElementIndexError {
    index: IndexError,
    center: Option<MeasIndex>,
}

impl fmt::Display for ElementIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if let Some(c) = self.center.as_ref() {
            write!(
                f,
                "0-index must be 0 <= i < {} and not include center at {c}, got {}",
                self.index.len,
                usize::from(self.index.index)
            )
        } else {
            self.index.fmt(f)
        }
    }
}

#[derive(Debug, Error)]
#[error(
    "supplied list must be {this_len} ({c}including center) \
     elements long, got {other_len}",
    c = if self.include_center { "" } else { "not " }
)]
pub struct KeyLengthError {
    this_len: usize,
    other_len: usize,
    include_center: bool,
}

#[derive(Debug, Error)]
pub enum NewNamedVecError {
    #[error("names must be unique")]
    NonUnique,
    #[error("only zero or one center values allowed")]
    MultiCenter,
}

#[derive(Debug, Error, new)]
#[error("error for element {index}: {error}")]
pub struct IndexedElementError<E> {
    error: E,
    index: MeasIndex,
}

#[derive(From, Display, Debug, Error)]
pub enum SetElementsError {
    Length(KeyLengthError),
    Mismatch(ColumnError<OpticalMismatchError>),
}

#[derive(Debug, Error, new)]
pub struct OpticalMismatchError {
    new_is_optical: bool,
}

impl fmt::Display for OpticalMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let opt = "optical";
        let tmp = "temporal";
        let (x, y) = if self.new_is_optical {
            (opt, tmp)
        } else {
            (tmp, opt)
        };
        write!(f, "tried to assign {x} value to {y} measurement")
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{
        Eithers, Element, ElementIndexError, KeyLengthError, KeyNotFoundError, NonCenterElement,
        SetCenterError, SetKeysError, SetNamesError,
    };
    use crate::python::macros::{impl_index_err, impl_pyreflow_err};
    use crate::text::optional::MightHave;
    use crate::validated::shortname::Shortname;
    use pyo3::prelude::*;
    use pyo3::types::PyTuple;

    impl_index_err!(ElementIndexError);
    impl_index_err!(KeyNotFoundError);

    // derive(FromPyObject) will get confused by the wrapper; this is trivial
    // otherwise
    impl<'py, K, U, V> FromPyObject<'py> for Eithers<K, U, V>
    where
        K: MightHave,
        K::Wrapper<Shortname>: FromPyObject<'py>,
        U: FromPyObject<'py>,
        V: FromPyObject<'py>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            Ok(Self(ob.extract()?))
        }
    }

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

    impl_pyreflow_err!(KeyLengthError);
    impl_pyreflow_err!(SetNamesError);
    impl_pyreflow_err!(SetKeysError);
    impl_pyreflow_err!(SetCenterError);
}
