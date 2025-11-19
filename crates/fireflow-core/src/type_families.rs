/// Haskell Functor stack and friends for rust (kinda)
///
/// This is a hybrid combining two common approaches usually seen to do this.
///
/// One uses type families to anchor instances of Functor et al. Here one
/// declares the trait on the family type and the trait itself encodes for the
/// inner value.
///
/// The second approach declares Functor et al on generic types themselves such
/// as Option, Vec, etc. The problem with the approach is that there is no way
/// to tell the type checker that the input container and output container need
/// to match (as is the case for Functor and many others).
///
/// We can rescue this second approach with type families by constraining the
/// input and output type to the same family. The only other thing needed to
/// make this work is to link families to the types they represent. Here this is
/// done using the Kind* and IsKind* traits. The former is declared on the type
/// family (ie OptionFamily), and the latter is declared on the type itself (ie
/// Option<T>). These both have a bidirectional link such the one can be
/// obtained from the other in a type bound. If the inner type changes, this is
/// called a "sibling".
///
/// Caveats:
///
/// 1 Bounds hell: bounding these traits can become a problem, especially when
///   dealing with many siblings (see Apply trait below for nasty example). The
///   problem is that rust doesn't have rankN bound polymorphism, which prevents
///   us from saying something like "all siblings with any inner type X are
///   instances of Apply". Instead, we need to list every generic parameter in
///   use, which can be painful if we have many. This is also a problem if
///   we end up chaining lots of generic functions together (something like
///   fmap . fmap . fmap)
///
///   One "solution" for this problem is to break traits into smaller pieces.
///   Here, this means Applicative is actually Pointed + Apply and we have
///   explicit impls for the latter two. This is in contrast to the "proper" way
///   to deal with this in Haskell since Pointed isn't a very useful category
///   by itself. The advantage in rust code is that we don't need to implement
///   everything unless we need it, and functions which use these traits can
///   have simpler bounds.
///
/// 2 Lack of specialization: Currently, it is impossible to declare blanket
///   instances (Iterator, IntoIterator, etc, all of which are obviously
///   Functors) since one would need to make a blanket impl for IsKind* on
///   a generic parameter which is constrained by the the trait in question
///   (Iterator for example). With current rust, this will conflict with all
///   other impls for IsKind*.
///
/// 3 Lots of boilerplate: In general, implementing these traits is not worth it
///   unless a type is to be used within a highly generic context where we need
///   to say "this is a Functor" and we know nothing else about it. There are
///   some cases were implementing one trait allows us to get other functions
///   for free such as the lift_f3/4/5/6 functions in Apply. Besides this,
///   classic rust implementation blocks are probably easier to
///   implement/maintain.
///
/// 4 Function cardinality: The ownership model in Rust creates another
///   dimension that Haskell doesn't need to consider. Any trait which takes a
///   function (like Functor) needs to consider that the supplied function may
///   need to be run only once or many types (ie FnOnce vs FnMut/Fn). Absent any
///   polymorphism for these function types, this means that each higher order
///   function trait effectively needs two separate versions to handle each
///   case.
///
///   A nasty side effect is that traits that take FnOnce could also take Fn or
///   FnMut which implies that they should auto-derive the less constrained
///   trait as well so that it can be used in place where an Fn/FnMut is needed.
///   For instance, we may wish to map over both Option and Vec which would
///   require using a Functor trait bound which takes an FnMut. However, Option
///   naively only needs to implement FunctorOnce, and Functor should be
///   auto-derived. This would be much easier if Rust had specialization, but
///   absent this, Functor needs to be manually implemented for all traits that
///   implement FunctorOnce.
use crate::text::optional::{Identity, Nothing};

pub type Sibling1<T, A> = <<T as IsKind1>::Family as Kind1>::Type<A>;
pub type Sibling2<T, A, B> = <<T as IsKind2>::Family as Kind2>::Type<A, B>;
pub type Sibling3<T, A, B, C> = <<T as IsKind3>::Family as Kind3>::Type<A, B, C>;

/// A type family representing all types which take 1 argument
pub trait Kind1 {
    type Type<X>: IsKind1<Family = Self>;
}

/// A type family representing all types which take 2 arguments
pub trait Kind2 {
    type Type<A, B>: IsKind2<Family = Self>;
}

/// A type family representing all types which take 3 arguments
pub trait Kind3 {
    type Type<A, B, C>: IsKind3<Family = Self>;
}

/// A type which takes 1 argument
pub trait IsKind1 {
    type Family: Kind1;
}

/// A type which takes 2 arguments
pub trait IsKind2 {
    type Family: Kind2;
}

/// A type which takes 3 arguments
pub trait IsKind3 {
    type Family: Kind3;
}

/// A type which can be appended/added to itself
pub trait Semigroup {
    #[must_use]
    fn sappend(self, other: Self) -> Self;
}

/// A Semigroup with an identity (aka zero) value
///
/// In Rust the identity is (always?) Default::default(), so use that here.
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

/// A type which can be "mapped over" with a function (many times)
pub trait Functor<A>: Sized + IsKind1 {
    fn fmap<F: FnMut(A) -> B, B>(self, f: F) -> Sibling1<Self, B>;

    fn fmap_into<B>(self) -> Sibling1<Self, B>
    where
        A: Into<B>,
    {
        self.fmap(Into::into)
    }
}

/// A type which can be "mapped over" with a function (one time)
pub trait FunctorOnce<A>: Sized + IsKind1 {
    fn fmap_once<F: FnOnce(A) -> B, B>(self, f: F) -> Sibling1<Self, B>;

    fn fmap_into_once<B>(self) -> Sibling1<Self, B>
    where
        A: Into<B>,
    {
        self.fmap_once(Into::into)
    }
}

/// A type which represents a context whose contents can be combined.
///
/// This is Applicative without "pure". In Rust this is a combination of what
/// are often called zip and map.
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

/// A type which can wrap an inner type.
///
/// This is Applicative without <*> or liftA2. It is also not a very
/// well-defined class in Haskell and is often frowned upon. Rust isn't Haskell,
/// so we can cheat and not have the Great Math Gods (TM) smite us for violating
/// category laws.
pub trait Pointed<A>: IsKind1 {
    fn wrap(a: A) -> Self;
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

impl<A> ApplyOnce<A> for Option<A> {
    fn lift_f2_once<F: FnOnce(A, B) -> C, B, C>(
        self,
        other: Sibling1<Self, B>,
        f: F,
    ) -> Sibling1<Self, C> {
        self.zip(other).map(|(x, y)| f(x, y))
    }
}

impl<A> Pointed<A> for Nothing<A> {
    fn wrap(_: A) -> Self {
        Self::default()
    }
}

impl<A> Pointed<A> for Identity<A> {
    fn wrap(a: A) -> Self {
        Self(a)
    }
}

impl<A> Pointed<A> for Option<A> {
    fn wrap(a: A) -> Self {
        Some(a)
    }
}

impl<X> Pointed<X> for Vec<X> {
    fn wrap(a: X) -> Self {
        vec![a]
    }
}
