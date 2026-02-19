use type_families::{impl_functor, impl_kind1};

use std::iter::{self, once};

/// A stack allocated container that holds one or two things.
pub enum OneOrTwo<X> {
    One(X),
    Two(X, X),
}

impl_kind1!(pub OneOrTwoFamily, OneOrTwo);

impl_functor!(
    OneOrTwo,
    self,
    mut f,
    match self {
        Self::One(x) => OneOrTwo::One(f(x)),
        Self::Two(x, y) => OneOrTwo::Two(f(x), f(y)),
    }
);

impl<X> IntoIterator for OneOrTwo<X> {
    type Item = X;
    type IntoIter = iter::Chain<iter::Once<X>, <Option<X> as IntoIterator>::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        let (x, y) = self.split();
        once(x).chain(y)
    }
}

impl<X> OneOrTwo<X> {
    pub(crate) fn split(self) -> (X, Option<X>) {
        match self {
            Self::One(x) => (x, None),
            Self::Two(x, y) => (x, Some(y)),
        }
    }

    pub(crate) fn from_results<A, B>(x: Result<A, X>, y: Result<B, X>) -> Result<(A, B), Self> {
        match (x, y) {
            (Ok(a), Ok(b)) => Ok((a, b)),
            (Err(a), Ok(_)) => Err(Self::One(a)),
            (Ok(_), Err(b)) => Err(Self::One(b)),
            (Err(a), Err(b)) => Err(Self::Two(a, b)),
        }
    }
}
