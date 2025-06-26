use serde::Serialize;
use std::convert::Infallible;
use std::fmt;
use std::mem;

/// Denotes that the value for a key (or collection of keys) is optional.
///
/// This is basically an Option but more obvious in what it indicates. It also
/// allows some nice methods to be built on top of option.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionalValue<T>(pub Option<T>);

/// A wrapper to contrast OptionalKw at the same abstraction level.
#[derive(Clone, Serialize)]
pub struct Identity<T>(pub T);

impl<T> From<Option<T>> for OptionalValue<T> {
    fn from(value: Option<T>) -> Self {
        OptionalValue(value)
    }
}

impl<T> From<OptionalValue<T>> for Option<T> {
    fn from(value: OptionalValue<T>) -> Self {
        value.0
    }
}

impl<T: Copy> Copy for OptionalValue<T> {}

// slightly hacky thing to let us copy the inner bit while re-wrapping as option
// impl<T: Copy> From<OptionalKw<T>> for Option<T> {
//     fn from(value: &OptionalKw<T>) -> Self {
//         value.0
//     }
// }

impl<T> Default for OptionalValue<T> {
    fn default() -> OptionalValue<T> {
        OptionalValue(None)
    }
}

impl<V> OptionalValue<V> {
    pub fn as_ref(&self) -> OptionalValue<&V> {
        OptionalValue(self.0.as_ref())
    }

    pub fn as_ref_opt(&self) -> Option<&V> {
        self.0.as_ref()
    }

    pub fn map<F, W>(self, f: F) -> OptionalValue<W>
    where
        F: Fn(V) -> W,
    {
        OptionalValue(self.0.map(f))
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
}

impl<V, E> OptionalValue<Result<V, E>> {
    pub fn transpose(self) -> Result<OptionalValue<V>, E> {
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

impl<V: fmt::Display> OptionalValue<V> {
    pub fn as_opt_string(&self) -> Option<String> {
        self.0.as_ref().map(|x| x.to_string())
    }
}

impl<T: Serialize> Serialize for OptionalValue<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0.as_ref() {
            Some(x) => serializer.serialize_some(x),
            None => serializer.serialize_none(),
        }
    }
}
