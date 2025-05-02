use serde::Serialize;
use std::fmt;
use std::mem;

/// Denotes that the value for a key is optional.
///
/// This is basically an Option but more obvious in what it indicates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionalKw<T>(pub Option<T>);

impl<T> From<Option<T>> for OptionalKw<T> {
    fn from(value: Option<T>) -> Self {
        OptionalKw(value)
    }
}

impl<T> From<OptionalKw<T>> for Option<T> {
    fn from(value: OptionalKw<T>) -> Self {
        value.0
    }
}

impl<T: Copy> Copy for OptionalKw<T> {}

// slightly hacky thing to let us copy the inner bit while re-wrapping as option
// impl<T: Copy> From<OptionalKw<T>> for Option<T> {
//     fn from(value: &OptionalKw<T>) -> Self {
//         value.0
//     }
// }

impl<T> Default for OptionalKw<T> {
    fn default() -> OptionalKw<T> {
        OptionalKw(None)
    }
}

impl<V> OptionalKw<V> {
    pub fn as_ref(&self) -> OptionalKw<&V> {
        OptionalKw(self.0.as_ref())
    }

    pub fn as_ref_opt(&self) -> Option<&V> {
        self.0.as_ref()
    }

    pub fn map<F, W>(self, f: F) -> OptionalKw<W>
    where
        F: Fn(V) -> W,
    {
        OptionalKw(self.0.map(f))
    }

    /// Mutate thing in Option if present, and possibly unset Option entirely
    pub fn mut_or_unset<F, X>(&mut self, f: F) -> Option<X>
    where
        F: Fn(&mut V) -> Result<X, ClearOptional>,
    {
        match mem::replace(self, None.into()).0 {
            None => None,
            Some(mut x) => match f(&mut x) {
                Ok(y) => Some(y),
                Err(_) => {
                    *self = None.into();
                    None
                }
            },
        }
    }
}

pub struct ClearOptional;

impl<V: fmt::Display> OptionalKw<V> {
    pub fn as_opt_string(&self) -> Option<String> {
        self.0.as_ref().map(|x| x.to_string())
    }
}

impl<T: Serialize> Serialize for OptionalKw<T> {
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
