use crate::core::{AnyMetarootKeyLossError, IndexedKeyLossError, UnitaryKeyLossError};
use crate::error::{BiTentative, Tentative};

use super::index::IndexFromOne;

use derive_more::{AsMut, AsRef};
use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::mem;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// A value that might exist.
///
/// This is basically [`Option`] but more obvious in what it indicates. It also
/// allows some nice methods to be built on top of [`Option`].
#[derive(Debug, Clone, PartialEq, Eq, AsRef, AsMut)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyObject))]
pub struct MaybeValue<T>(pub Option<T>);

/// A value that always exists.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyObject))]
pub struct AlwaysValue<T>(pub T);

/// A value that always exists.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct NeverValue<T>(pub PhantomData<T>);

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

    /// Consume a value and return it as a wrapped value
    fn wrap<T>(n: T) -> Self::Wrapper<T> {
        n.into()
    }

    /// Consume a wrapped value and possibly return its contents.
    ///
    /// If no contents exist, return the original input so the caller can
    /// take back ownership.
    fn unwrap<T>(x: Self::Wrapper<T>) -> Result<T, Self::Wrapper<T>>;

    /// Borrow a wrapped value and return a new wrapper with borrowed contents.
    fn as_ref<T>(x: &Self::Wrapper<T>) -> Self::Wrapper<&T>;

    /// Consume a wrapped value and possibly return its contents.
    fn to_opt<T>(x: Self::Wrapper<T>) -> Option<T> {
        Self::unwrap(x).ok()
    }

    /// Borrow a wrapped value and possibly return borrowed contents.
    fn as_opt<T>(x: &Self::Wrapper<T>) -> Option<&T> {
        Self::to_opt(Self::as_ref(x))
    }

    /// Apply function to inner value.
    fn map<T, T0, F>(x: Self::Wrapper<T>, f: F) -> Self::Wrapper<T0>
    where
        F: FnOnce(T) -> T0;
}

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct MaybeFamily;

impl MightHave for MaybeFamily {
    type Wrapper<T> = MaybeValue<T>;
    const INFALLABLE: bool = false;

    fn unwrap<T>(x: Self::Wrapper<T>) -> Result<T, Self::Wrapper<T>> {
        x.0.ok_or(None.into())
    }

    fn as_ref<T>(x: &Self::Wrapper<T>) -> Self::Wrapper<&T> {
        x.as_ref()
    }

    fn map<T, T0, F>(x: Self::Wrapper<T>, f: F) -> Self::Wrapper<T0>
    where
        F: FnOnce(T) -> T0,
    {
        x.0.map(f).into()
    }
}

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AlwaysFamily;

impl MightHave for AlwaysFamily {
    type Wrapper<T> = AlwaysValue<T>;
    const INFALLABLE: bool = true;

    fn unwrap<T>(x: Self::Wrapper<T>) -> Result<T, Self::Wrapper<T>> {
        Ok(x.0)
    }

    fn as_ref<T>(x: &Self::Wrapper<T>) -> Self::Wrapper<&T> {
        AlwaysValue(&x.0)
    }

    fn map<T, T0, F>(x: Self::Wrapper<T>, f: F) -> Self::Wrapper<T0>
    where
        F: FnOnce(T) -> T0,
    {
        AlwaysValue(f(x.0))
    }
}

impl<T> From<T> for AlwaysValue<T> {
    fn from(value: T) -> Self {
        AlwaysValue(value)
    }
}

impl<T> From<T> for MaybeValue<T> {
    fn from(value: T) -> Self {
        Some(value).into()
    }
}

impl<T> TryFrom<MaybeValue<T>> for AlwaysValue<T> {
    type Error = MaybeToAlwaysError;
    fn try_from(value: MaybeValue<T>) -> Result<Self, Self::Error> {
        value.0.ok_or(MaybeToAlwaysError).map(AlwaysValue)
    }
}

// This will never really fail but is implemented for symmetry with its inverse
impl<T> TryFrom<AlwaysValue<T>> for MaybeValue<T> {
    type Error = Infallible;
    fn try_from(value: AlwaysValue<T>) -> Result<Self, Infallible> {
        Ok(Some(value.0).into())
    }
}

impl<T> From<Option<T>> for MaybeValue<T> {
    fn from(value: Option<T>) -> Self {
        MaybeValue(value)
    }
}

impl<T> From<MaybeValue<T>> for Option<T> {
    fn from(value: MaybeValue<T>) -> Self {
        value.0
    }
}

impl<T: Copy> Copy for MaybeValue<T> {}

impl<T> Default for MaybeValue<T> {
    fn default() -> MaybeValue<T> {
        MaybeValue(None)
    }
}

impl<V> MaybeValue<V> {
    pub fn as_ref(&self) -> MaybeValue<&V> {
        MaybeValue(self.0.as_ref())
    }

    pub fn as_ref_opt(&self) -> Option<&V> {
        self.0.as_ref()
    }

    pub fn map<F, W>(self, f: F) -> MaybeValue<W>
    where
        F: Fn(V) -> W,
    {
        MaybeValue(self.0.map(f))
    }

    /// Mutate thing in Option if present, and possibly unset Option entirely
    pub fn mut_or_unset<E, F, X>(&mut self, f: F) -> Result<Option<X>, E>
    where
        F: Fn(&mut V) -> Result<X, ClearOptionalOr<E>>,
    {
        match mem::replace(self, None.into()).0 {
            None => Ok(None),
            Some(mut x) => match f(&mut x) {
                Ok(y) => Ok(Some(y)),
                Err(ClearOptionalOr::Clear) => {
                    *self = None.into();
                    Ok(None)
                }
                Err(ClearOptionalOr::Error(e)) => Err(e),
            },
        }
    }

    pub fn mut_or_unset_nofail<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut V) -> Result<X, ClearOptional>,
    {
        let Ok(x) = self.mut_or_unset(f);
        x
    }

    pub(crate) fn check_indexed_key_transfer<E>(&self, i: IndexFromOne) -> Result<(), E>
    where
        E: From<IndexedKeyLossError<V>>,
    {
        if self.0.is_some() {
            Err(IndexedKeyLossError::<V>::new(i).into())
        } else {
            Ok(())
        }
    }

    pub(crate) fn check_indexed_key_transfer_own<E>(
        self,
        i: IndexFromOne,
        lossless: bool,
    ) -> BiTentative<(), E>
    where
        E: From<IndexedKeyLossError<V>>,
    {
        let mut tnt = Tentative::default();
        if self.0.is_some() {
            tnt.push_error_or_warning(IndexedKeyLossError::<V>::new(i), lossless);
        }
        tnt
    }

    pub(crate) fn check_key_transfer(
        self,
        lossless: bool,
    ) -> BiTentative<(), AnyMetarootKeyLossError>
    where
        AnyMetarootKeyLossError: From<UnitaryKeyLossError<V>>,
    {
        let mut tnt = Tentative::default();
        if self.0.is_some() {
            tnt.push_error_or_warning(UnitaryKeyLossError::<V>::default(), lossless);
        }
        tnt
    }
}

impl<V, E> MaybeValue<Result<V, E>> {
    pub fn transpose(self) -> Result<MaybeValue<V>, E> {
        self.0.transpose().map(|x| x.into())
    }
}

pub type ClearOptional = ClearOptionalOr<Infallible>;

#[derive(Default)]
pub enum ClearOptionalOr<E> {
    #[default]
    Clear,
    Error(E),
}

impl<V: fmt::Display> MaybeValue<V> {
    pub fn as_opt_string(&self) -> Option<String> {
        self.0.as_ref().map(|x| x.to_string())
    }
}

// impl<T: Serialize> Serialize for MaybeValue<T> {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         match self.0.as_ref() {
//             Some(x) => serializer.serialize_some(x),
//             None => serializer.serialize_none(),
//         }
//     }
// }

pub struct MaybeToAlwaysError;

impl fmt::Display for MaybeToAlwaysError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "optional keyword value is blank",)
    }
}
