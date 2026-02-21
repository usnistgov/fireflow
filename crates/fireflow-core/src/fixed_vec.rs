use type_families::{impl_functor, impl_kind1};

use nonempty::NonEmpty;

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

impl<X> From<OneOrTwo<X>> for NonEmpty<X> {
    fn from(value: OneOrTwo<X>) -> Self {
        match value {
            OneOrTwo::One(x) => Self::new(x),
            OneOrTwo::Two(x, y) => Self::from((x, vec![y])),
        }
    }
}

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

    // pub(crate) fn filter<F>(self, mut f: F) -> Option<Self>
    // where
    //     F: FnMut(&X) -> bool,
    // {
    //     match self {
    //         Self::One(x) => f(&x).then_some(Self::One(x)),
    //         Self::Two(x, y) => match (f(&x), f(&y)) {
    //             (true, true) => Some(Self::Two(x, y)),
    //             (true, false) => Some(Self::One(x)),
    //             (false, true) => Some(Self::One(y)),
    //             (false, false) => None,
    //         },
    //     }
    // }

    pub(crate) fn filter_map<F, Y>(self, mut f: F) -> Option<OneOrTwo<Y>>
    where
        F: FnMut(X) -> Option<Y>,
    {
        match self {
            Self::One(x0) => f(x0).map(OneOrTwo::One),
            Self::Two(x0, x1) => match (f(x0), f(x1)) {
                (Some(y0), Some(y1)) => Some(OneOrTwo::Two(y0, y1)),
                (Some(y0), None) => Some(OneOrTwo::One(y0)),
                (None, Some(y1)) => Some(OneOrTwo::One(y1)),
                (None, None) => None,
            },
        }
    }
}

// impl<X> OneOrTwo<&X> {
//     pub(crate) fn copied(self) -> OneOrTwo<X>
//     where
//         X: Copy,
//     {
//         self.fmap(|x| *x)
//     }
// }
