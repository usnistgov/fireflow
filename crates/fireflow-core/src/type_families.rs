use crate::text::optional::{AlwaysValue, NeverValue};

pub type Sibling1<T, A> = <<T as IsKind1>::Family as Kind1>::Type<A>;
pub type Sibling2<T, A, B> = <<T as IsKind2>::Family as Kind2>::Type<A, B>;

pub trait Kind1 {
    type Type<X>: IsKind1<Family = Self>;
}

pub trait Kind2 {
    type Type<A, B>: IsKind2<Family = Self>;
}

pub trait IsKind1 {
    type Family: Kind1;
}

pub trait IsKind2 {
    type Family: Kind2;
}

pub trait Functor<X>: Sized + IsKind1 {
    fn fmap<F, Y>(self, f: F) -> Sibling1<Self, Y>
    where
        F: Fn(X) -> Y;
}

pub trait BiFunctor<A, B>: Sized + IsKind2 {
    fn bimap<F, G, C, D>(self, f: F, g: G) -> <Self::Family as Kind2>::Type<C, D>
    where
        F: Fn(A) -> C,
        G: Fn(B) -> D;
}

pub trait Applicative<A>: Functor<A> {
    fn pure(a: A) -> Self;

    // TODO add lift_a2...but call it zip_a2 because rust
}

pub trait Comonad<X>: Functor<X> {
    fn cm_extract(self) -> X;

    fn cm_extract_ref(&self) -> &X;

    // TODO add cm_extend
}

macro_rules! impl_kind1 {
    ($f:ident, $t:ident) => {
        impl Kind1 for $f {
            type Type<T> = $t<T>;
        }

        impl<T> IsKind1 for $t<T> {
            type Family = $f;
        }
    };
}

pub struct OptFamily;

pub struct BoxFamily;

pub struct IdFamily;

pub struct VecFamily;

pub struct NullFamily;

impl_kind1!(NullFamily, NeverValue);
impl_kind1!(IdFamily, AlwaysValue);
impl_kind1!(BoxFamily, Box);
impl_kind1!(OptFamily, Option);
impl_kind1!(VecFamily, Vec);

impl<X> Functor<X> for NeverValue<X> {
    fn fmap<F: Fn(X) -> Y, Y>(self, _: F) -> NeverValue<Y> {
        NeverValue::default()
    }
}

impl<X> Functor<X> for AlwaysValue<X> {
    fn fmap<F: Fn(X) -> Y, Y>(self, f: F) -> AlwaysValue<Y> {
        AlwaysValue(f(self.0))
    }
}

impl<X> Functor<X> for Box<X> {
    fn fmap<F: Fn(X) -> Y, Y>(self, f: F) -> Box<Y> {
        Box::new(f(*self))
    }
}

impl<X> Functor<X> for Option<X> {
    fn fmap<F: Fn(X) -> Y, Y>(self, f: F) -> Option<Y> {
        self.map(f)
    }
}

impl<X> Functor<X> for Vec<X> {
    fn fmap<F: Fn(X) -> Y, Y>(self, f: F) -> Vec<Y> {
        self.into_iter().map(f).collect()
    }
}

impl<X> Comonad<X> for AlwaysValue<X> {
    fn cm_extract(self) -> X {
        self.0
    }

    fn cm_extract_ref(&self) -> &X {
        &self.0
    }
}

impl<X> Comonad<X> for Box<X> {
    fn cm_extract(self) -> X {
        *self
    }

    fn cm_extract_ref(&self) -> &X {
        self.as_ref()
    }
}

impl<A> Applicative<A> for NeverValue<A> {
    fn pure(_: A) -> Self {
        Self::default()
    }
}

impl<A> Applicative<A> for AlwaysValue<A> {
    fn pure(a: A) -> Self {
        Self(a)
    }
}

impl<A> Applicative<A> for Option<A> {
    fn pure(a: A) -> Self {
        Some(a)
    }
}

impl<X> Applicative<X> for Box<X> {
    fn pure(a: X) -> Self {
        Self::new(a)
    }
}

impl<X> Applicative<X> for Vec<X> {
    fn pure(a: X) -> Self {
        vec![a]
    }
}
