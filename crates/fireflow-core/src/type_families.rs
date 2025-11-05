use crate::text::optional::{Identity, Nothing};

pub type Sibling1<T, A> = <<T as IsKind1>::Family as Kind1>::Type<A>;
pub type Sibling2<T, A, B> = <<T as IsKind2>::Family as Kind2>::Type<A, B>;
pub type Sibling3<T, A, B, C> = <<T as IsKind3>::Family as Kind3>::Type<A, B, C>;

pub trait Kind1 {
    type Type<X>: IsKind1<Family = Self>;
}

pub trait Kind2 {
    type Type<A, B>: IsKind2<Family = Self>;
}

pub trait Kind3 {
    type Type<A, B, C>: IsKind3<Family = Self>;
}

pub trait IsKind1 {
    type Family: Kind1;
}

pub trait IsKind2 {
    type Family: Kind2;
}

pub trait IsKind3 {
    type Family: Kind3;
}

pub trait Semigroup {
    #[must_use]
    fn sappend(self, other: Self) -> Self;
}

pub trait Monoid: Semigroup + Default {
    #[must_use]
    fn mappend(self, other: Self) -> Self {
        self.sappend(other)
    }

    #[must_use]
    fn mempty() -> Self {
        Self::default()
    }

    // to be overridden since not all instances of this will be optimal
    fn mconcat(xs: impl IntoIterator<Item = Self>) -> Self {
        xs.into_iter().fold(Self::mempty(), Self::mappend)
    }
}

pub trait Functor<A>: Sized + IsKind1 {
    fn fmap<F: FnMut(A) -> Y, Y>(self, f: F) -> Sibling1<Self, Y>;
}

pub trait FunctorOnce<X>: Sized + IsKind1 {
    fn fmap_once<F: FnOnce(X) -> Y, Y>(self, f: F) -> Sibling1<Self, Y>;
}

macro_rules! trait_apply {
    (
        $n:ident,
        $f:ident,
        $bound:ident,
        $lift_f2:ident,
        $lift_f3:ident,
        $lift_f4:ident,
        $lift_f5:ident,
        $lift_f6:ident,
        $zip_f2:ident,
        $zip_f3:ident,
        $zip_f4:ident,
        $zip_f5:ident,
        $zip_f6:ident
    ) => {
        pub trait $n<A>: $f<A> {
            fn $lift_f2<F, B, C>(self, other: Sibling1<Self, B>, f: F) -> Sibling1<Self, C>
            where
                F: $bound(A, B) -> C;

            fn $zip_f2<B>(self, b: Sibling1<Self, B>) -> Sibling1<Self, (A, B)> {
                self.$lift_f2(b, |ax, bx| (ax, bx))
            }

            fn $lift_f3<Fun, B, C, D>(
                self,
                b: Sibling1<Self, B>,
                c: Sibling1<Self, C>,
                fun: Fun,
            ) -> Sibling1<Self, D>
            where
                Sibling1<Self, (A, B)>: $n<(A, B)>,
                Fun: $bound(A, B, C) -> D,
            {
                self.$zip_f2(b).$lift_f2(c, |(ax, bx), cx| fun(ax, bx, cx))
            }

            fn $zip_f3<B, C>(
                self,
                b: Sibling1<Self, B>,
                c: Sibling1<Self, C>,
            ) -> Sibling1<Self, (A, B, C)>
            where
                Sibling1<Self, (A, B)>: $n<(A, B)>,
            {
                self.$lift_f3(b, c, |ax, bx, cx| (ax, bx, cx))
            }

            fn $lift_f4<Fun, B, C, D, E>(
                self,
                b: Sibling1<Self, B>,
                c: Sibling1<Self, C>,
                d: Sibling1<Self, D>,
                fun: Fun,
            ) -> Sibling1<Self, E>
            where
                Sibling1<Self, (A, B)>: $n<(A, B)>,
                Sibling1<Self, (A, B, C)>: $n<(A, B, C)>,
                Fun: $bound(A, B, C, D) -> E,
            {
                self.$zip_f3(b, c)
                    .$lift_f2(d, |(ax, bx, cx), dx| fun(ax, bx, cx, dx))
            }

            fn $zip_f4<B, C, D>(
                self,
                b: Sibling1<Self, B>,
                c: Sibling1<Self, C>,
                d: Sibling1<Self, D>,
            ) -> Sibling1<Self, (A, B, C, D)>
            where
                Sibling1<Self, (A, B)>: $n<(A, B)>,
                Sibling1<Self, (A, B, C)>: $n<(A, B, C)>,
            {
                self.$lift_f4(b, c, d, |ax, bx, cx, dx| (ax, bx, cx, dx))
            }

            fn $lift_f5<Fun, B, C, D, E, F>(
                self,
                b: Sibling1<Self, B>,
                c: Sibling1<Self, C>,
                d: Sibling1<Self, D>,
                e: Sibling1<Self, E>,
                fun: Fun,
            ) -> Sibling1<Self, F>
            where
                Sibling1<Self, (A, B)>: $n<(A, B)>,
                Sibling1<Self, (A, B, C)>: $n<(A, B, C)>,
                Sibling1<Self, (A, B, C, D)>: $n<(A, B, C, D)>,
                Fun: $bound(A, B, C, D, E) -> F,
            {
                self.$zip_f4(b, c, d)
                    .$lift_f2(e, |(ax, bx, cx, dx), ex| fun(ax, bx, cx, dx, ex))
            }

            fn $zip_f5<B, C, D, E>(
                self,
                b: Sibling1<Self, B>,
                c: Sibling1<Self, C>,
                d: Sibling1<Self, D>,
                e: Sibling1<Self, E>,
            ) -> Sibling1<Self, (A, B, C, D, E)>
            where
                Sibling1<Self, (A, B)>: $n<(A, B)>,
                Sibling1<Self, (A, B, C)>: $n<(A, B, C)>,
                Sibling1<Self, (A, B, C, D)>: $n<(A, B, C, D)>,
            {
                self.$lift_f5(b, c, d, e, |ax, bx, cx, dx, ex| (ax, bx, cx, dx, ex))
            }

            fn $lift_f6<Fun, B, C, D, E, F, G>(
                self,
                b: Sibling1<Self, B>,
                c: Sibling1<Self, C>,
                d: Sibling1<Self, D>,
                e: Sibling1<Self, E>,
                f: Sibling1<Self, F>,
                fun: Fun,
            ) -> Sibling1<Self, G>
            where
                // if only rust had rankN polymorphism :(
                Sibling1<Self, (A, B)>: $n<(A, B)>,
                Sibling1<Self, (A, B, C)>: $n<(A, B, C)>,
                Sibling1<Self, (A, B, C, D)>: $n<(A, B, C, D)>,
                Sibling1<Self, (A, B, C, D, E)>: $n<(A, B, C, D, E)>,
                Fun: $bound(A, B, C, D, E, F) -> G,
            {
                self.$zip_f5(b, c, d, e)
                    .$lift_f2(f, |(ax, bx, cx, dx, ex), fx| fun(ax, bx, cx, dx, ex, fx))
            }

            fn $zip_f6<B, C, D, E, F>(
                self,
                b: Sibling1<Self, B>,
                c: Sibling1<Self, C>,
                d: Sibling1<Self, D>,
                e: Sibling1<Self, E>,
                f: Sibling1<Self, F>,
            ) -> Sibling1<Self, (A, B, C, D, E, F)>
            where
                Sibling1<Self, (A, B)>: $n<(A, B)>,
                Sibling1<Self, (A, B, C)>: $n<(A, B, C)>,
                Sibling1<Self, (A, B, C, D)>: $n<(A, B, C, D)>,
                Sibling1<Self, (A, B, C, D, E)>: $n<(A, B, C, D, E)>,
            {
                self.$lift_f6(b, c, d, e, f, |ax, bx, cx, dx, ex, fx| {
                    (ax, bx, cx, dx, ex, fx)
                })
            }
        }
    };
}

trait_apply!(
    Apply, Functor, Fn, lift_f2, lift_f3, lift_f4, lift_f5, lift_f6, zip_f2, zip_f3, zip_f4,
    zip_f5, zip_f6
);

trait_apply!(
    ApplyOnce,
    FunctorOnce,
    FnOnce,
    lift_f2_once,
    lift_f3_once,
    lift_f4_once,
    lift_f5_once,
    lift_f6_once,
    zip_f2_once,
    zip_f3_once,
    zip_f4_once,
    zip_f5_once,
    zip_f6_once
);

pub trait Applicative<A>: Apply<A> {
    fn pure(a: A) -> Self;

    fn lift_a2<F, B, C>(self, other: Sibling1<Self, B>, f: F) -> Sibling1<Self, C>
    where
        F: Fn(A, B) -> C,
    {
        self.lift_f2(other, f)
    }
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

pub(crate) use impl_kind1;

pub struct OptFamily;

pub struct IdFamily;

pub struct VecFamily;

pub struct NullFamily;

impl_kind1!(NullFamily, Nothing);
impl_kind1!(IdFamily, Identity);
impl_kind1!(OptFamily, Option);
impl_kind1!(VecFamily, Vec);

impl<A> Semigroup for Nothing<A> {
    fn sappend(self, _: Self) -> Self {
        Self::default()
    }
}

impl<X> Semigroup for Vec<X> {
    fn sappend(mut self, other: Self) -> Self {
        self.extend(other);
        self
    }
}

impl<X> Monoid for Nothing<X> {}
impl<X> Monoid for Vec<X> {}

macro_rules! impl_functor_common {
    ($t:ident, $trait:ident, $fun:ident, $bound:ident, $self:ident, $f:pat, $body:expr) => {
        impl<X> $trait<X> for $t<X> {
            fn $fun<F: $bound(X) -> Y, Y>($self, $f: F) -> $t<Y> {
                $body
            }
        }
    };
}

macro_rules! impl_functor {
    ($t:ident, $self:ident, mut $f:ident, $body:expr) => {
        impl_functor_common!($t, Functor, fmap, FnMut, $self, mut $f, $body);
    };

    ($t:ident, $self:ident, $f:ident, $body:expr) => {
        impl_functor_common!($t, Functor, fmap, FnMut, $self, $f, $body);
    };
}

macro_rules! impl_functor_once {
    ($t:ident, $self:ident, mut $f:ident, $body:expr) => {
        impl_functor!($t, $self, mut $f, $body);
        impl_functor_common!($t, FunctorOnce, fmap_once, FnOnce, $self, $f, $body);
    };

    ($t:ident, $self:ident, $f:ident, $body:expr) => {
        impl_functor!($t, $self, $f, $body);
        impl_functor_common!($t, FunctorOnce, fmap_once, FnOnce, $self, $f, $body);
    };
}

impl_functor_once!(Nothing, self, _f, Nothing::default());
impl_functor_once!(Identity, self, mut f, Identity(f(self.0)));
impl_functor_once!(Option, self, f, self.map(f));
impl_functor!(Vec, self, f, self.into_iter().map(f).collect());

// impl<X> Comonad<X> for Identity<X> {
//     fn cm_extract(self) -> X {
//         self.0
//     }

//     fn cm_extract_ref(&self) -> &X {
//         &self.0
//     }
// }

impl<A> Apply<A> for Nothing<A> {
    fn lift_f2<F: Fn(A, B) -> C, B, C>(self, _: Sibling1<Self, B>, _: F) -> Sibling1<Self, C> {
        Nothing::default()
    }
}

impl<A> Apply<A> for Identity<A> {
    fn lift_f2<F: Fn(A, B) -> C, B, C>(self, other: Sibling1<Self, B>, f: F) -> Sibling1<Self, C> {
        Identity(f(self.0, other.0))
    }
}

impl<A> Apply<A> for Option<A> {
    fn lift_f2<F: Fn(A, B) -> C, B, C>(self, other: Sibling1<Self, B>, f: F) -> Sibling1<Self, C> {
        self.zip(other).map(|(x, y)| f(x, y))
    }
}

impl<X> Apply<X> for Vec<X> {
    fn lift_f2<F: Fn(X, B) -> C, B, C>(self, other: Sibling1<Self, B>, f: F) -> Sibling1<Self, C> {
        self.into_iter().zip(other).map(|(a, b)| f(a, b)).collect()
    }
}

impl<A> Applicative<A> for Nothing<A> {
    fn pure(_: A) -> Self {
        Self::default()
    }
}

impl<A> Applicative<A> for Identity<A> {
    fn pure(a: A) -> Self {
        Self(a)
    }
}

impl<A> Applicative<A> for Option<A> {
    fn pure(a: A) -> Self {
        Some(a)
    }
}

impl<X> Applicative<X> for Vec<X> {
    fn pure(a: X) -> Self {
        vec![a]
    }
}
