use crate::core::{AnyMetarootKeyLossError, IndexedKeyLossError, UnitaryKeyLossError};
use crate::error::{BiTentative, Tentative};
use crate::validated::keys::{IndexedKey, Key, MeasHeader};

use super::index::IndexFromOne;

use derive_more::{AsMut, AsRef, Display, From, FromStr};
use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::string::ToString;
use thiserror::Error;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// A value that might exist.
///
/// This is basically [`Option`] but more obvious in what it indicates. It also
/// allows some nice methods to be built on top of [`Option`].
#[derive(Debug, Clone, PartialEq, Eq, AsRef, AsMut, From)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject))]
pub struct MaybeValue<T>(pub Option<T>);

/// A value that always exists.
#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct AlwaysValue<T>(pub T);

/// A value that always exists.
#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct NeverValue<T>(pub PhantomData<T>);

#[derive(Debug, Clone, PartialEq, Eq, AsRef, AsMut, From, Default, Display, FromStr)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromPyObject, IntoPyObject))]
#[as_ref(str)]
pub struct OptionalString(pub String);

pub(crate) trait IsDefault {
    fn is_default(&self) -> bool;
}

impl<T: Default + PartialEq> IsDefault for T {
    fn is_default(&self) -> bool {
        self == &T::default()
    }
}

pub(crate) trait DisplayMaybe: IsDefault {
    type Inner;

    fn display_maybe(&self) -> Option<String>;

    fn metaroot_opt_pair(&self) -> (String, Option<String>)
    where
        Self::Inner: Key,
    {
        (Self::Inner::std().to_string(), self.display_maybe())
    }

    fn meas_opt_pair(&self, i: impl Into<IndexFromOne>) -> (String, Option<String>)
    where
        Self::Inner: IndexedKey,
    {
        (Self::Inner::std(i).to_string(), self.display_maybe())
    }

    fn meas_opt_triple(&self, i: impl Into<IndexFromOne>) -> (MeasHeader, String, Option<String>)
    where
        Self::Inner: IndexedKey,
    {
        (
            Self::Inner::std_blank(),
            Self::Inner::std(i).to_string(),
            self.display_maybe(),
        )
    }
}

pub(crate) trait CheckMaybe: Sized + IsDefault {
    type Inner;

    fn check_key_transfer(&self, allow_loss: bool) -> BiTentative<(), AnyMetarootKeyLossError>
    where
        AnyMetarootKeyLossError: From<UnitaryKeyLossError<Self::Inner>>,
    {
        let mut tnt = Tentative::default();
        if !self.is_default() {
            tnt.push_error_or_warning(UnitaryKeyLossError::<Self::Inner>::new(), !allow_loss);
        }
        tnt
    }

    fn check_indexed_key_transfer_tnt<E>(
        &self,
        i: impl Into<IndexFromOne>,
        allow_loss: bool,
    ) -> BiTentative<(), E>
    where
        E: From<IndexedKeyLossError<Self::Inner>>,
    {
        let mut tnt = Tentative::default();
        if !self.is_default() {
            tnt.push_error_or_warning(IndexedKeyLossError::<Self::Inner>::new(i), !allow_loss);
        }
        tnt
    }

    fn check_indexed_key_transfer<E>(&self, i: impl Into<IndexFromOne>) -> Result<(), E>
    where
        E: From<IndexedKeyLossError<Self::Inner>>,
    {
        if self.is_default() {
            Ok(())
        } else {
            Err(IndexedKeyLossError::<Self::Inner>::new(i).into())
        }
    }
}

impl<T: fmt::Display + PartialEq> DisplayMaybe for MaybeValue<T> {
    type Inner = T;
    fn display_maybe(&self) -> Option<String> {
        self.0.display_maybe()
    }
}

impl<T: fmt::Display + PartialEq> DisplayMaybe for Option<T> {
    type Inner = T;
    fn display_maybe(&self) -> Option<String> {
        self.as_ref().map(ToString::to_string)
    }
}

impl<T: fmt::Display + PartialEq> CheckMaybe for MaybeValue<T> {
    type Inner = T;
}

impl<T: fmt::Display + PartialEq> CheckMaybe for Option<T> {
    type Inner = T;
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

#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct MaybeFamily;

impl MightHave for MaybeFamily {
    type Wrapper<T> = MaybeValue<T>;
    const INFALLABLE: bool = false;

    fn unwrap<T>(x: Self::Wrapper<T>) -> Result<T, Self::Wrapper<T>> {
        x.0.ok_or(MaybeValue::default())
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

#[derive(Clone, PartialEq)]
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
        Self(value)
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

impl<T> From<AlwaysValue<T>> for MaybeValue<T> {
    fn from(value: AlwaysValue<T>) -> Self {
        Some(value.0).into()
    }
}

impl<T> From<MaybeValue<T>> for Option<T> {
    fn from(value: MaybeValue<T>) -> Self {
        value.0
    }
}

impl<T: Copy> Copy for MaybeValue<T> {}

impl<T> Default for MaybeValue<T> {
    fn default() -> Self {
        Self(None)
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
        F: Fn(&mut V) -> ClearMaybeError<X, E>,
    {
        match mem::replace(self, None.into()).0 {
            None => Ok(None),
            Some(mut x) => {
                let c = f(&mut x);
                let (ret, newself) = match c.clear {
                    None => (Ok(Some(c.value)), Some(x).into()),
                    Some(ClearOptionalOr::Error(e)) => (Err(e), Some(x).into()),
                    Some(ClearOptionalOr::Clear) => (Ok(Some(c.value)), None.into()),
                };
                *self = newself;
                ret
            }
        }
    }

    pub fn mut_or_unset_nofail<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut V) -> ClearMaybe<X>,
    {
        let Ok(x) = self.mut_or_unset(f);
        x
    }
}

impl<V, E> MaybeValue<Result<V, E>> {
    pub fn transpose(self) -> Result<MaybeValue<V>, E> {
        self.0.transpose().map(Into::into)
    }
}

pub type ClearMaybe<V> = ClearMaybeError<V, Infallible>;

impl<V: Default, E> Default for ClearMaybeError<V, E> {
    fn default() -> Self {
        Self {
            value: V::default(),
            clear: None,
        }
    }
}

pub struct ClearMaybeError<V, E> {
    pub value: V,
    pub clear: Option<ClearOptionalOr<E>>,
}

pub type ClearOptional = ClearOptionalOr<Infallible>;

#[derive(Default)]
pub enum ClearOptionalOr<E> {
    #[default]
    Clear,
    Error(E),
}

#[derive(Debug, Error)]
#[error("optional keyword value is blank")]
pub struct MaybeToAlwaysError;

#[cfg(feature = "python")]
mod python {
    use super::{AlwaysValue, MaybeValue};

    use pyo3::prelude::*;

    impl<'py, T> FromPyObject<'py> for AlwaysValue<T>
    where
        T: FromPyObject<'py>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            Ok(Self(ob.extract()?))
        }
    }

    impl<'py, T> FromPyObject<'py> for MaybeValue<T>
    where
        T: FromPyObject<'py>,
    {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            Ok(Self(ob.extract()?))
        }
    }
}
