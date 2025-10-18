//! Error handling for fireflow
//!
//! The fundamental problem to solve is collecting multiple errors/warnings and
//! displaying them all at once. Then once we reach the API boundary, trigger
//! a "real error" with all these errors plus a summary.
//!
//! Generally, this will entail creating a [`DeferredResult`] from at least one
//! error and any number of warnings which contains a [`Tentative`] and
//! [`DeferredFailure`] in Ok and Err. This will be Ok if the result is still
//! usable for downstream computation but may ultimately not pass some quality
//! threshold demanded from the API; otherwise it will be Err.
//!
//! This can then be "resolved" into a [`TerminalResult`] which will be passed
//! to the user at the API boundary. The only way to get the value out of
//! such a result is to run a function to process the errors/warnings.

use crate::config::SharedConfig;
use crate::text::optional::NeverValue;

use derive_new::new;
use nonempty::NonEmpty;
use std::convert::Infallible;
use std::io;
use std::iter;
use std::marker::PhantomData;
use std::mem;
use std::vec;
use thiserror::Error;

// TODO add a cap to the error buffer so the user doesn't DOS themselves if
// their file is particularly screwed up

/// Final result which may be passing or not passing in IO context.
///
/// Passing side may have multiple warnings and failure side has at least one IO
/// failure, an error summary, and may have multiple warnings.
pub type IOTerminalResult<V, W, E, T> = TerminalResult<V, W, ImpureError<E>, T>;

/// Final result which may be passing or not passing in IO context.
///
/// Passing side may have multiple warnings and failure side has at least one
/// failure, an error summary, and may have multiple warnings.
pub type TerminalResult<V, W, E, T> = Result<Terminal<V, W>, TerminalFailure<W, E, T>>;

/// Final result which may be passing or not passing in IO context.
///
/// Passing side may have one warnings and failure side has one error.
pub type IOTerminalResultOne<V, W, E> = TerminalResultOne<V, W, ImpureError<E>>;

/// Final result which may be passing or not passing.
///
/// Passing side may have one warnings and failure side has one error.
pub type TerminalResultOne<V, W, E> = Result<TerminalOne<V, W>, TerminalFailureOne<E>>;

pub type TerminalResultNoWarn<V, E, T> = Result<
    TerminalInner<V, Infallible, NullFamily>,
    TerminalFailureInner<Infallible, E, T, NullFamily, VecFamily>,
>;

pub type TerminalResultInner<V, W, E, T, LWI, RWI, REI> =
    Result<TerminalInner<V, W, LWI>, TerminalFailureInner<W, E, T, RWI, REI>>;

/// Passing result with 0 or 1 warning.
pub type TerminalOne<V, W> = TerminalInner<V, W, OptionFamily>;

/// Passing result with possibly many warnings.
pub type Terminal<V, W> = TerminalInner<V, W, VecFamily>;

/// Final passing result, possibly with warnings
#[derive(new)]
#[new(visibility = "")]
pub struct TerminalInner<V, W, I: ZeroOrMore> {
    value: V,
    warnings: I::Wrapper<W>,
}

/// Failure with at least one error, an error summary, and possibly many warnings.
pub type TerminalFailure<W, E, T> = TerminalFailureInner<W, E, T, VecFamily, VecFamily>;

/// Failure with one error.
pub type TerminalFailureOne<E> = TerminalFailureInner<Infallible, E, (), NullFamily, NullFamily>;

/// Final failed result with either one error or multiple errors with a summary.
#[derive(new)]
#[new(visibility = "")]
pub struct TerminalFailureInner<W, E, T, WI: ZeroOrMore, EI: ZeroOrMore> {
    warnings: WI::Wrapper<W>,
    errors: GenNonEmpty<E, EI>,
    reason: T,
}

/// A NonEmpty container with a generic tail that may/may not contain anything.
#[derive(new)]
pub struct GenNonEmpty<E, EI: ZeroOrMore> {
    head: Box<E>,
    tail: EI::Wrapper<E>,
}

impl<E, EI: ZeroOrMore> IntoIterator for GenNonEmpty<E, EI> {
    type Item = E;
    type IntoIter = iter::Chain<iter::Once<E>, <EI::Wrapper<E> as IntoIterator>::IntoIter>;
    fn into_iter(self) -> Self::IntoIter {
        iter::once(*self.head).chain(self.tail)
    }
}

impl<T, C: ZeroOrMore> Extend<T> for GenNonEmpty<T, C>
where
    C::Wrapper<T>: Extend<T>,
{
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = T>,
    {
        self.tail.extend(iter);
    }
}

impl<T, C: ZeroOrMore> GenNonEmpty<T, C> {
    fn new1(x: T) -> Self {
        Self::new(x.into(), C::Wrapper::<T>::default())
    }

    fn map<X, F>(self, f: F) -> GenNonEmpty<X, C>
    where
        F: Fn(T) -> X,
    {
        GenNonEmpty::new(f(*self.head).into(), C::map(self.tail, f))
    }

    fn repack<EIF: ZeroOrMore>(self) -> GenNonEmpty<T, EIF>
    where
        C: IntoZeroOrMore<EIF>,
    {
        GenNonEmpty::new(self.head, C::into_zero_or_more(self.tail))
    }

    fn prepend<I>(&mut self, other: I)
    where
        I: IntoIterator<Item = T>,
        C::Wrapper<T>: Extend<T>,
    {
        let mut it = other.into_iter();
        if let Some(x0) = it.by_ref().next() {
            let mut new = GenNonEmpty::new1(x0);
            new.extend(it);
            let oldself = mem::replace(self, new);
            self.extend(oldself.into_iter());
        }
    }
}

impl<E, EI: ZeroOrMore> From<(E, EI::Wrapper<E>)> for GenNonEmpty<E, EI> {
    fn from(value: (E, EI::Wrapper<E>)) -> Self {
        Self::new(value.0.into(), value.1)
    }
}

/// Result which may have at least one error
pub type DeferredResult<V, W, E> = Result<Tentative<V, W, E>, DeferredFailure<(), W, E>>;
pub type DeferredResultOne<V, W, E> = Result<TentativeOne<V, W>, DeferredFailureOne<(), E>>;
pub type DeferredResultInner<V, W, E, TWI, TEI, FWI, FEI> =
    PassthruResultInner<V, (), W, E, TWI, TEI, FWI, FEI>;

pub type BiDeferredResult<V, E> = DeferredResult<V, E, E>;

/// Result which may have at least one error (with passthru)
pub type PassthruResult<V, P, W, E> = Result<Tentative<V, W, E>, DeferredFailure<P, W, E>>;
pub type PassthruResultOne<V, P, W, E> = Result<TentativeOne<V, W>, DeferredFailureOne<P, E>>;
pub type PassthruResultInner<V, P, W, E, WI0, EI0, WI1, EI1> =
    Result<TentativeInner<V, W, E, WI0, EI0>, DeferredFailureInner<P, W, E, WI1, EI1>>;

/// Result which may have at least one error in IO context
pub type IODeferredResult<V, W, E> = DeferredResult<V, W, ImpureError<E>>;
pub type IODeferredResultOne<V, W, E> = DeferredResultOne<V, W, ImpureError<E>>;

pub type Tentative<V, W, E> = TentativeInner<V, W, E, VecFamily, VecFamily>;
pub type TentativeOne<V, W> = TentativeInner<V, W, Infallible, OptionFamily, NullFamily>;

/// Result which might have warnings or errors
#[derive(new)]
pub struct TentativeInner<V, W, E, WI: ZeroOrMore, EI: ZeroOrMore> {
    value: V,
    warnings: WI::Wrapper<W>,
    errors: EI::Wrapper<E>,
}

/// Tentative where both error and warning are the same type
pub type BiTentative<V, T> = Tentative<V, T, T>;

pub type DeferredFailure<P, W, E> = DeferredFailureInner<P, W, E, VecFamily, VecFamily>;

pub type DeferredFailureOne<P, E> = DeferredFailureInner<P, Infallible, E, NullFamily, NullFamily>;

/// Result which has 1+ errors, 0+ warnings, and the input type.
///
/// Passthru is meant to hold the input type for the failed computation such
/// that it can be reused if needed rather than consumed in a black hole by
/// the ownership model. Obvious use case: failed From<X>-like methods where
/// we may want to do something with the original value after trying and failing
/// to convert it.
#[derive(new)]
pub struct DeferredFailureInner<P, W, E, WI: ZeroOrMore, EI: ZeroOrMore> {
    warnings: WI::Wrapper<W>,
    errors: GenNonEmpty<E, EI>,
    passthru: P,
}

/// Result for which failure can have multiple errors
pub type MultiResult<X, E> = Result<X, NonEmpty<E>>;

/// An error which is either pure or impure (IO)
///
/// Used in a similar manner to "IO a" in Haskell to denote IO-based
/// computations which may fail.
#[derive(Debug, Error)]
pub enum ImpureError<E> {
    #[error("IO error: {0}")]
    IO(#[from] io::Error),
    #[error("{0}")]
    Pure(E),
}

pub type IOResult<V, E> = Result<V, ImpureError<E>>;

/// Run an iterator and collect successes or failures and return as Result
///
/// Ok will have Vec of success types, and Err will have NonEmpty of error
/// type if at least one error is found.
pub(crate) trait ErrorIter<T, E>: Iterator<Item = Result<T, E>> + Sized {
    fn gather(mut self) -> MultiResult<Vec<T>, E> {
        let mut pass = vec![];
        let mut error_head = None;
        for x in self.by_ref() {
            match x {
                Ok(y) => pass.push(y),
                Err(y) => {
                    error_head = Some(y);
                    break;
                }
            }
        }
        if let Some(h) = error_head {
            let tail = self.filter_map(Result::err).collect();
            Err((h, tail).into())
        } else {
            Ok(pass)
        }
    }
}

pub(crate) trait ErrorIter1<T, P, W, E, LWI, LEI, RWI, REI>:
    Iterator<Item = PassthruResultInner<T, P, W, E, LWI, LEI, RWI, REI>> + Sized
where
    LWI: ZeroOrMore,
    LEI: ZeroOrMore,
    RWI: ZeroOrMore,
    REI: ZeroOrMore,
{
    #[allow(clippy::type_complexity)]
    fn gather1(
        mut self,
    ) -> PassthruResultInner<Vec<T>, (Vec<T>, Vec<P>), W, E, LWI, LEI, RWI, VecFamily>
    where
        LWI::Wrapper<W>: Extend<W>,
        LEI::Wrapper<E>: Extend<E>,
        RWI::Wrapper<W>: Extend<W>,
        LWI: IntoZeroOrMore<RWI>,
        REI: IntoZeroOrMore<VecFamily>,
    {
        let mut left_vs = vec![];
        let mut left_ws = LWI::Wrapper::<W>::default();
        let mut left_es = LEI::Wrapper::<E>::default();
        let mut error_head = None;
        for x in self.by_ref() {
            match x {
                Ok(y) => {
                    left_vs.push(y.value);
                    left_ws.extend(y.warnings);
                    left_es.extend(y.errors);
                }
                Err(y) => {
                    error_head = Some(y);
                    break;
                }
            }
        }
        if let Some(h) = error_head {
            let mut right_vs = vec![h.passthru];
            let mut right_ws = LWI::into_zero_or_more(left_ws);
            let mut right_es = match LEI::try_into_one_and_iter(left_es) {
                Some((x, xs)) => GenNonEmpty::new(x.into(), xs.chain(h.errors).collect()),
                None => h.errors.repack(),
            };
            for x in self {
                match x {
                    Ok(y) => {
                        left_vs.push(y.value);
                        right_ws.extend(y.warnings);
                        right_es.extend(y.errors);
                    }
                    Err(y) => {
                        right_vs.push(y.passthru);
                        right_ws.extend(y.warnings);
                        right_es.extend(y.errors);
                    }
                }
            }
            Err(DeferredFailureInner::new(
                right_ws,
                right_es.into(),
                (left_vs, right_vs),
            ))
        } else {
            Ok(TentativeInner::new(left_vs, left_ws, left_es))
        }
    }
}

impl<I: Iterator<Item = Result<T, E>>, T, E> ErrorIter<T, E> for I {}
impl<
        I: Iterator<Item = PassthruResultInner<T, P, W, E, LWI, LEI, RWI, REI>>,
        T,
        P,
        W,
        E,
        LWI,
        LEI,
        RWI,
        REI,
    > ErrorIter1<T, P, W, E, LWI, LEI, RWI, REI> for I
where
    LWI: ZeroOrMore,
    LEI: ZeroOrMore,
    RWI: ZeroOrMore,
    REI: ZeroOrMore,
{
}

/// Generic higher-order type for something which has zero or more things.
///
/// Use cases: Option, Vec, and NeverValue
pub trait ZeroOrMore: Sized {
    type Wrapper<T>: IntoIterator<Item = T> + Default;
    type IterOne<X>: Iterator<Item = X>;

    fn map<F, X, Y>(t: Self::Wrapper<X>, f: F) -> Self::Wrapper<Y>
    where
        F: Fn(X) -> Y;

    fn try_into_one_and_iter<X>(x: Self::Wrapper<X>) -> Option<(X, Self::IterOne<X>)>;

    fn try_into_one_or_more<X>(x: Self::Wrapper<X>) -> Option<GenNonEmpty<X, Self>>;
}

/// Generic higher-order type for anything which can hold one thing.
///
/// Use cases: Option, Vec, NonEmpty, Alwaysvalue
pub trait CanHoldOne: ZeroOrMore {
    fn wrap<X>(x: X) -> Self::Wrapper<X>;
}

// /// Generic higher-order type for anything which holds up to infinite things
// ///
// /// Use cases: Vec, NonEmpty
// pub trait CanHoldMany: ZeroOrMore {
//     fn push<X>(t: &mut Self::Wrapper<X>, x: X);

//     fn extend<X>(t: &mut Self::Wrapper<X>, x: impl IntoIterator<Item = X>);
// }

// /// Generic "adder" for types
// pub trait Appendable<Other> {
//     type Out;
//     fn append_het(self, other: Other) -> Self::Out;
// }

pub trait Semigroup {
    fn sconcat(self, other: Self) -> Self;
}

pub trait Monoid: Default + Semigroup {
    fn mappend(self, other: Self) -> Self {
        Self::sconcat(self, other)
    }
}

/// Convert containers that many hold values into those that may hold more.
pub trait IntoZeroOrMore<Other: ZeroOrMore>: ZeroOrMore {
    fn into_zero_or_more<X>(x: Self::Wrapper<X>) -> Other::Wrapper<X>;
}

pub struct OptionFamily;

pub struct VecFamily;

pub struct NullFamily;

impl ZeroOrMore for NullFamily {
    type Wrapper<T> = NeverValue<T>;
    type IterOne<X> = iter::Empty<X>;

    fn map<F, X, Y>(_: Self::Wrapper<X>, _: F) -> Self::Wrapper<Y>
    where
        F: Fn(X) -> Y,
    {
        NeverValue(PhantomData)
    }

    fn try_into_one_and_iter<X>(_: Self::Wrapper<X>) -> Option<(X, Self::IterOne<X>)> {
        None
    }

    fn try_into_one_or_more<X>(_: Self::Wrapper<X>) -> Option<GenNonEmpty<X, Self>> {
        None
    }
}

impl ZeroOrMore for OptionFamily {
    type Wrapper<T> = Option<T>;
    type IterOne<X> = iter::Empty<X>;

    fn map<F, X, Y>(t: Self::Wrapper<X>, f: F) -> Self::Wrapper<Y>
    where
        F: Fn(X) -> Y,
    {
        t.map(f)
    }

    fn try_into_one_and_iter<X>(x: Self::Wrapper<X>) -> Option<(X, Self::IterOne<X>)> {
        x.map(|x| (x, iter::empty()))
    }

    fn try_into_one_or_more<X>(x: Self::Wrapper<X>) -> Option<GenNonEmpty<X, Self>> {
        Self::try_into_one_and_iter(x).map(|(y, _)| GenNonEmpty::new(y.into(), None))
    }
}

impl ZeroOrMore for VecFamily {
    type Wrapper<T> = Vec<T>;
    type IterOne<X> = vec::IntoIter<X>;

    fn map<F, X, Y>(t: Self::Wrapper<X>, f: F) -> Self::Wrapper<Y>
    where
        F: Fn(X) -> Y,
    {
        t.into_iter().map(f).collect()
    }

    fn try_into_one_and_iter<X>(x: Self::Wrapper<X>) -> Option<(X, Self::IterOne<X>)> {
        let mut it = x.into_iter();
        it.by_ref().next().map(|x0| (x0, it))
    }

    fn try_into_one_or_more<X>(x: Self::Wrapper<X>) -> Option<GenNonEmpty<X, Self>> {
        Self::try_into_one_and_iter(x).map(|(y, ys)| GenNonEmpty::new(y.into(), ys.collect()))
    }
}

// impl ZeroOrMore for SingletonFamily {
//     fn into_nonempty<X>(xs: Self::Wrapper<X>) -> NonEmpty<X> {
//         NonEmpty::new(xs.0)
//     }
// }

// impl ZeroOrMore for NonEmptyFamily {
//     fn into_nonempty<X>(xs: Self::Wrapper<X>) -> NonEmpty<X> {
//         xs
//     }
// }

impl CanHoldOne for OptionFamily {
    fn wrap<X>(x: X) -> Self::Wrapper<X> {
        Some(x)
    }
}

impl CanHoldOne for VecFamily {
    fn wrap<X>(x: X) -> Self::Wrapper<X> {
        vec![x]
    }
}

// impl CanHoldOne for SingletonFamily {
//     fn wrap<X>(x: X) -> Self::Wrapper<X> {
//         AlwaysValue(x)
//     }
// }

// impl CanHoldOne for NonEmptyFamily {
//     fn wrap<X>(x: X) -> Self::Wrapper<X> {
//         NonEmpty::new(x)
//     }
// }

// macro_rules! impl_holds_many {
//     ($t:ident, $inner:ident) => {
//         impl CanHoldMany for $t {
//             fn push<X>(t: &mut Self::Wrapper<X>, x: X) {
//                 t.push(x);
//             }

//             fn extend<X>(t: &mut Self::Wrapper<X>, xs: impl IntoIterator<Item = X>) {
//                 t.extend(xs);
//             }
//         }
//     };
// }

// impl_holds_many!(VecFamily, Vec);
// impl_holds_many!(NonEmptyFamily, NonEmpty);

impl<T> Semigroup for Vec<T> {
    fn sconcat(mut self, other: Self) -> Self {
        self.extend(other);
        self
    }
}

impl<T> Semigroup for NeverValue<T> {
    fn sconcat(self, _: Self) -> Self {
        self
    }
}

// impl<T, I: ZeroOrMore + CanHoldMany> Semigroup for GenNonEmpty<T, I> {
//     fn sconcat(mut self, other: Self) -> Self {
//         self.extend(other);
//         self
//     }
// }

impl<T> Monoid for Vec<T> {}

// macro_rules! impl_concat_null_left {
//     ($t:ident) => {
//         impl<T> Appendable<$t<T>> for NeverValue<T> {
//             type Out = $t<T>;
//             fn append_het(self, other: $t<T>) -> Self::Out {
//                 other
//             }
//         }
//     };
// }

// macro_rules! impl_concat_null_right {
//     ($t:ident) => {
//         impl<T> Appendable<NeverValue<T>> for $t<T> {
//             type Out = $t<T>;
//             fn append_het(self, _: NeverValue<T>) -> Self::Out {
//                 self
//             }
//         }
//     };
// }

// macro_rules! impl_concat_chain_iter {
//     ($a:ident, $b:ident) => {
//         impl<T> Appendable<$b<T>> for $a<T> {
//             type Out = Vec<T>;
//             fn append_het(self, other: $b<T>) -> Self::Out {
//                 self.into_iter().chain(other).collect()
//             }
//         }
//     };
// }

// macro_rules! impl_concat_chain_iter_ne {
//     ($t:ident) => {
//         impl<T, I0: ZeroOrMore> Appendable<$t<T>> for GenNonEmpty<T, I0> {
//             type Out = GenNonEmpty<T, VecFamily>;

//             fn append_het(self, xs: $t<T>) -> Self::Out {
//                 GenNonEmpty::new(self.head, self.tail.into_iter().chain(xs).collect())
//             }
//         }
//     };
// }

// impl_concat_null_left!(Vec);
// impl_concat_null_left!(Option);
// impl_concat_null_left!(NeverValue);

// impl_concat_null_right!(Vec);
// impl_concat_null_right!(Option);

// impl_concat_chain_iter!(Option, Option);
// impl_concat_chain_iter!(Option, Vec);
// impl_concat_chain_iter!(Vec, Option);
// impl_concat_chain_iter!(Vec, Vec);

// impl_concat_chain_iter_ne!(NeverValue);
// impl_concat_chain_iter_ne!(Option);
// impl_concat_chain_iter_ne!(Vec);

// impl<T, I0, I1> Appendable<GenNonEmpty<T, I1>> for GenNonEmpty<T, I0>
// where
//     I0: ZeroOrMore,
//     I1: ZeroOrMore,
// {
//     type Out = GenNonEmpty<T, VecFamily>;

//     fn append_het(self, other: GenNonEmpty<T, I1>) -> Self::Out {
//         let tail = self.tail.into_iter().chain(other).collect();
//         GenNonEmpty::new(self.head, tail)
//     }
// }

// impl<T, I1: ZeroOrMore> Appendable<GenNonEmpty<T, I1>> for NeverValue<T> {
//     type Out = GenNonEmpty<T, VecFamily>;

//     fn append_het(self, other: GenNonEmpty<T, I1>) -> Self::Out {
//         // NOTE we could use IntoZeroOrMore but this requires one less constraint
//         GenNonEmpty::new(other.head, other.tail.into_iter().collect())
//     }
// }

// impl<T, I1: ZeroOrMore> Appendable<GenNonEmpty<T, I1>> for Option<T> {
//     type Out = GenNonEmpty<T, VecFamily>;

//     fn append_het(self, other: GenNonEmpty<T, I1>) -> Self::Out {
//         match self {
//             None => GenNonEmpty::new(other.head, other.tail.into_iter().collect()),
//             Some(x) => GenNonEmpty::new(x.into(), other.into_iter().collect()),
//         }
//     }
// }

// impl<T, I1: ZeroOrMore> Appendable<GenNonEmpty<T, I1>> for Vec<T> {
//     type Out = GenNonEmpty<T, VecFamily>;

//     fn append_het(self, other: GenNonEmpty<T, I1>) -> Self::Out {
//         let mut it = self.into_iter();
//         match it.next() {
//             None => GenNonEmpty::new(other.head, other.tail.into_iter().collect()),
//             Some(x) => GenNonEmpty::new(x.into(), it.chain(other).collect()),
//         }
//     }
// }

impl<T: ZeroOrMore> IntoZeroOrMore<T> for T {
    fn into_zero_or_more<X>(x: Self::Wrapper<X>) -> T::Wrapper<X> {
        x
    }
}

impl IntoZeroOrMore<VecFamily> for NullFamily {
    fn into_zero_or_more<X>(_: Self::Wrapper<X>) -> Vec<X> {
        vec![]
    }
}

impl IntoZeroOrMore<OptionFamily> for NullFamily {
    fn into_zero_or_more<X>(_: Self::Wrapper<X>) -> Option<X> {
        None
    }
}

impl IntoZeroOrMore<VecFamily> for OptionFamily {
    fn into_zero_or_more<X>(x: Self::Wrapper<X>) -> Vec<X> {
        x.into_iter().collect()
    }
}

// impl<T: ZeroOrMore> IntoZeroOrMore<T> for T {
//     fn into_one_or_more<X>(x: Self::Wrapper<X>) -> T::Wrapper<X> {
//         x
//     }
// }

// impl IntoZeroOrMore<NonEmptyFamily> for SingletonFamily {
//     fn into_one_or_more<X>(x: Self::Wrapper<X>) -> NonEmpty<X> {
//         NonEmpty::new(x.0)
//     }
// }

impl<V, W, WI: ZeroOrMore> TerminalInner<V, W, WI> {
    fn new1(value: impl Into<V>) -> Self {
        Self::new(value.into(), WI::Wrapper::<W>::default())
    }

    pub fn repack_warnings<WIF>(self) -> TerminalInner<V, W, WIF>
    where
        WI: IntoZeroOrMore<WIF>,
        WIF: ZeroOrMore,
    {
        TerminalInner::new(self.value, WI::into_zero_or_more(self.warnings))
    }

    pub fn value_into<U: From<V>>(self) -> TerminalInner<U, W, WI> {
        self.map(Into::into)
    }

    pub fn map<F: FnOnce(V) -> X, X>(self, f: F) -> TerminalInner<X, W, WI> {
        TerminalInner::new(f(self.value), self.warnings)
    }

    pub fn warnings_into<X: From<W>>(self) -> TerminalInner<V, X, WI> {
        self.warnings_map(Into::into)
    }

    pub fn warnings_map<F: Fn(W) -> X, X>(self, f: F) -> TerminalInner<V, X, WI> {
        TerminalInner::new(self.value, WI::map(self.warnings, f))
    }

    pub fn resolve<F, X>(self, f: F) -> (V, X)
    where
        F: FnOnce(WI::Wrapper<W>) -> X,
    {
        (self.value, f(self.warnings))
    }

    fn warnings_to_errors<T, E, F, WF, EI>(
        self,
        reason: T,
        f: F,
    ) -> TerminalResultInner<V, WF, E, T, WI, WI, EI>
    where
        F: Fn(W) -> E,
        EI: ZeroOrMore,
        // TODO this feels sketchy, the only point of this may be to convert
        // iterator-like things into non-empty
        WI: IntoZeroOrMore<EI>,
    {
        match WI::try_into_one_or_more(self.warnings) {
            None => Ok(TerminalInner::new1(self.value)),
            Some(ws) => Err(TerminalFailureInner::new1(ws.map(f).repack(), reason)),
        }
    }
}

impl<V, I: ZeroOrMore> TerminalInner<V, Infallible, I> {
    pub fn inner(self) -> V {
        self.value
    }
}

impl<V, W> Terminal<V, W> {
    fn new_vec(value: impl Into<V>, warnings: impl IntoIterator<Item = W>) -> Self {
        Self::new(value.into(), warnings.into_iter().collect())
    }

    pub fn and_finally<E, T, F, X>(mut self, f: F) -> TerminalResult<X, W, E, T>
    where
        F: FnOnce(V) -> TerminalResult<X, W, E, T>,
    {
        match f(self.value) {
            Ok(s) => {
                self.warnings.extend(s.warnings);
                Ok(Terminal::new_vec(s.value, self.warnings))
            }
            Err(e) => {
                self.warnings.extend(e.warnings);
                Err(TerminalFailure::new(self.warnings, e.errors, e.reason))
            }
        }
    }

    pub fn and_maybe<E, F, X, T>(mut self, reason: T, f: F) -> TerminalResult<X, W, E, T>
    where
        F: FnOnce(V) -> DeferredResult<X, W, E>,
    {
        match f(self.value) {
            Ok(s) => {
                self.warnings.extend(s.warnings);
                Tentative::new_vec(s.value, self.warnings, s.errors).terminate(reason)
            }
            Err(e) => {
                self.warnings.extend(e.warnings);
                // termination will throw away the passthru value so this
                // only needs to be a dummy
                Err(DeferredFailureInner::new(self.warnings, e.errors, ()).terminate(reason))
            }
        }
    }

    pub fn and_tentatively<F, X, E, T>(mut self, reason: T, f: F) -> TerminalResult<X, W, E, T>
    where
        F: FnOnce(V) -> Tentative<X, W, E>,
    {
        let s = f(self.value);
        self.warnings.extend(s.warnings);
        Tentative::new_vec(s.value, self.warnings, s.errors).terminate(reason)
    }
}

impl<W, E, T, WI: ZeroOrMore, EI: ZeroOrMore> TerminalFailureInner<W, E, T, WI, EI> {
    fn new1(errors: GenNonEmpty<E, EI>, reason: T) -> Self {
        Self::new(WI::Wrapper::<W>::default(), errors.into(), reason)
    }

    pub fn repack_warnings<WIF>(self) -> TerminalFailureInner<W, E, T, WIF, EI>
    where
        WI: IntoZeroOrMore<WIF>,
        WIF: ZeroOrMore,
    {
        TerminalFailureInner::new(
            WI::into_zero_or_more(self.warnings),
            self.errors,
            self.reason,
        )
    }

    pub fn repack_errors<EIF>(self) -> TerminalFailureInner<W, E, T, WI, EIF>
    where
        EI: IntoZeroOrMore<EIF>,
        EIF: ZeroOrMore,
    {
        TerminalFailureInner::new(self.warnings, self.errors.repack(), self.reason)
    }

    pub fn resolve<F, G, X, Y>(self, f: F, g: G) -> (X, Y)
    where
        F: FnOnce(WI::Wrapper<W>) -> X,
        G: FnOnce(GenNonEmpty<E, EI>, T) -> Y,
    {
        (f(self.warnings), g(self.errors, self.reason))
    }

    fn warnings_to_errors<F, WF, WIF>(mut self, f: F) -> TerminalFailureInner<WF, E, T, WIF, EI>
    where
        F: Fn(W) -> E,
        WIF: ZeroOrMore,
        EI::Wrapper<E>: Extend<E>,
    {
        self.errors.extend(WI::map(self.warnings, f));
        TerminalFailureInner::new1(self.errors, self.reason)
    }
}

impl<W, E, T> TerminalFailure<W, E, T> {
    fn new_vec(
        warnings: impl IntoIterator<Item = W>,
        errors: impl Into<GenNonEmpty<E, VecFamily>>,
        reason: impl Into<T>,
    ) -> Self {
        Self::new(warnings.into_iter().collect(), errors.into(), reason.into())
    }

    // pub fn map_warnings<F, X>(self, f: F) -> TerminalFailure<X, E, T>
    // where
    //     F: Fn(W) -> X,
    // {
    //     TerminalFailure {
    //         warnings: self.warnings.into_iter().map(f).collect(),
    //         errors: self.errors,
    //         reason: self.reason,
    //     }
    // }

    // pub fn map_errors<F, X>(self, f: F) -> TerminalFailure<W, X, T>
    // where
    //     F: Fn(E) -> X,
    // {
    //     TerminalFailure {
    //         warnings: self.warnings,
    //         errors: self.errors.map(f),
    //         reason: self.reason,
    //     }
    // }

    // pub fn map_reason<F, X>(self, f: F) -> TerminalFailure<W, E, X>
    // where
    //     F: FnOnce(T) -> X,
    // {
    //     TerminalFailure {
    //         warnings: self.warnings,
    //         errors: self.errors,
    //         reason: f(self.reason),
    //     }
    // }

    // pub fn warnings_into<X>(self) -> TerminalFailure<X, E, T>
    // where
    //     X: From<W>,
    // {
    //     self.map_warnings(Into::into)
    // }

    // pub fn errors_into<X>(self) -> TerminalFailure<W, X, T>
    // where
    //     X: From<E>,
    // {
    //     self.map_errors(Into::into)
    // }

    // pub fn value_into<X>(self) -> TerminalFailure<W, E, X>
    // where
    //     X: From<T>,
    // {
    //     self.map_reason(Into::into)
    // }
}

impl<V, W, E, WI: ZeroOrMore, EI: ZeroOrMore> TentativeInner<V, W, E, WI, EI> {
    pub fn new1(value: V) -> Self {
        Self::new(
            value,
            WI::Wrapper::<W>::default(),
            EI::Wrapper::<E>::default(),
        )
    }

    pub fn new_either<M>(value: V, msg: M, are_errors: bool) -> Self
    where
        E: From<M>,
        W: From<M>,
        WI: CanHoldOne,
        EI: CanHoldOne,
    {
        if are_errors {
            Self::new(value, WI::Wrapper::<W>::default(), EI::wrap(E::from(msg)))
        } else {
            Self::new(value, WI::wrap(W::from(msg)), EI::Wrapper::<E>::default())
        }
    }

    pub fn mconcat(xs: impl IntoIterator<Item = Self>) -> TentativeInner<Vec<V>, W, E, WI, EI>
    where
        WI::Wrapper<W>: Extend<W>,
        EI::Wrapper<E>: Extend<E>,
    {
        let mut ret: TentativeInner<_, _, _, WI, EI> = TentativeInner::new1(vec![]);
        for x in xs {
            ret.value.push(x.value);
            ret.warnings.extend(x.warnings);
            ret.errors.extend(x.errors);
        }
        ret
    }

    pub fn repack_warnings<WIF>(self) -> TentativeInner<V, W, E, WIF, EI>
    where
        WI: IntoZeroOrMore<WIF>,
        WIF: ZeroOrMore,
    {
        let ws = WI::into_zero_or_more(self.warnings);
        TentativeInner::new(self.value, ws, self.errors)
    }

    pub fn repack_errors<EIF>(self) -> TentativeInner<V, W, E, WI, EIF>
    where
        EI: IntoZeroOrMore<EIF>,
        EIF: ZeroOrMore,
    {
        let es = EI::into_zero_or_more(self.errors);
        TentativeInner::new(self.value, self.warnings, es)
    }

    pub fn map<F: FnOnce(V) -> X, X>(self, f: F) -> TentativeInner<X, W, E, WI, EI> {
        TentativeInner::new(f(self.value), self.warnings, self.errors)
    }

    pub fn map_warnings<F: Fn(W) -> X, X>(self, f: F) -> TentativeInner<V, X, E, WI, EI> {
        TentativeInner::new(self.value, WI::map(self.warnings, f), self.errors)
    }

    pub fn map_errors<F: Fn(E) -> X, X>(self, f: F) -> TentativeInner<V, W, X, WI, EI> {
        TentativeInner::new(self.value, self.warnings, EI::map(self.errors, f))
    }

    pub fn value_into<X: From<V>>(self) -> TentativeInner<X, W, E, WI, EI> {
        self.map(Into::into)
    }

    pub fn warnings_into<X: From<W>>(self) -> TentativeInner<V, X, E, WI, EI> {
        self.map_warnings(Into::into)
    }

    pub fn errors_into<X: From<E>>(self) -> TentativeInner<V, W, X, WI, EI> {
        self.map_errors(Into::into)
    }

    pub fn inner_into<X: From<W>, Y: From<E>>(self) -> TentativeInner<V, X, Y, WI, EI> {
        self.errors_into().warnings_into()
    }

    pub fn errors_liftio(self) -> TentativeInner<V, W, ImpureError<E>, WI, EI> {
        self.map_errors(ImpureError::Pure)
    }

    // TODO misleading name since this has an extra function arg
    pub fn mappend<F, V0, VF>(
        mut self,
        other: TentativeInner<V0, W, E, WI, EI>,
        f: F,
    ) -> TentativeInner<VF, W, E, WI, EI>
    where
        F: FnOnce(V, V0) -> VF,
        WI::Wrapper<W>: Extend<W>,
        EI::Wrapper<E>: Extend<E>,
    {
        self.warnings.extend(other.warnings);
        self.errors.extend(other.errors);
        TentativeInner::new(f(self.value, other.value), self.warnings, self.errors)
    }

    pub fn and_tentatively<F, X>(mut self, f: F) -> TentativeInner<X, W, E, WI, EI>
    where
        F: FnOnce(V) -> TentativeInner<X, W, E, WI, EI>,
        WI::Wrapper<W>: Extend<W>,
        EI::Wrapper<E>: Extend<E>,
    {
        let s = f(self.value);
        self.warnings.extend(s.warnings);
        self.errors.extend(s.errors);
        TentativeInner::new(s.value, self.warnings, self.errors)
    }

    pub fn and_maybe<F, X, P>(mut self, f: F) -> PassthruResultInner<P, X, W, E, WI, EI, WI, EI>
    where
        F: FnOnce(V) -> PassthruResultInner<P, X, W, E, WI, EI, WI, EI>,
        WI::Wrapper<W>: Extend<W>,
        EI::Wrapper<E>: Extend<E>,
    {
        match f(self.value) {
            Ok(s) => {
                self.warnings.extend(s.warnings);
                self.errors.extend(s.errors);
                Ok(TentativeInner::new(s.value, self.warnings, self.errors))
            }
            Err(mut e) => {
                self.warnings.extend(e.warnings);
                e.errors.prepend(self.errors);
                Err(DeferredFailureInner::new(
                    self.warnings,
                    e.errors,
                    e.passthru,
                ))
            }
        }
    }

    pub fn and_fail<F, P>(mut self, other: F) -> DeferredFailureInner<P, W, E, WI, EI>
    where
        F: FnOnce(V) -> DeferredFailureInner<P, W, E, WI, EI>,
        WI::Wrapper<W>: Extend<W>,
        EI::Wrapper<E>: Extend<E>,
    {
        let mut e = other(self.value);
        self.warnings.extend(e.warnings);
        e.errors.prepend(self.errors);
        DeferredFailureInner::new(self.warnings, e.errors, e.passthru)
    }

    pub fn terminate<T>(self, reason: T) -> TerminalResultInner<V, W, E, T, WI, WI, EI> {
        self.terminate_inner(reason).map(|(t, _)| t)
    }

    pub fn terminate_def<T: Default>(self) -> TerminalResultInner<V, W, E, T, WI, WI, EI> {
        self.terminate(T::default())
    }

    #[allow(clippy::type_complexity)]
    fn terminate_inner<T>(
        self,
        reason: T,
    ) -> Result<(TerminalInner<V, W, WI>, T), TerminalFailureInner<W, E, T, WI, EI>> {
        match EI::try_into_one_or_more(self.errors) {
            Some(errors) => Err(TerminalFailureInner::new(self.warnings, errors, reason)),
            None => Ok((TerminalInner::new(self.value, self.warnings), reason)),
        }
    }

    pub fn terminate_warn2err<F, T, WF>(
        self,
        reason: T,
        f: F,
    ) -> TerminalResultInner<V, WF, E, T, WI, WI, EI>
    where
        F: Fn(W) -> E,
        WI: IntoZeroOrMore<EI>,
        // LWIF: ZeroOrMore,
        // RWIF: ZeroOrMore,
        // EIF: ZeroOrMore,
        EI::Wrapper<E>: Extend<E>,
    {
        match self.terminate_inner(reason) {
            Ok((t, r)) => t.warnings_to_errors(r, f),
            Err(e) => Err(e.warnings_to_errors(f)),
        }
    }

    pub fn terminate_nowarn<T, WIF>(
        self,
        reason: T,
    ) -> TerminalResultInner<V, (), E, T, WIF, WIF, EI>
    where
        WIF: ZeroOrMore,
    {
        match EI::try_into_one_or_more(self.errors) {
            None => Ok(TerminalInner::new1(self.value)),
            Some(e) => Err(TerminalFailureInner::new1(e, reason)),
        }
    }

    pub fn push_warning(&mut self, x: impl Into<W>)
    where
        WI::Wrapper<W>: Extend<W>,
    {
        self.warnings.extend(iter::once(x.into()))
    }

    pub fn push_error(&mut self, x: impl Into<E>)
    where
        EI::Wrapper<E>: Extend<E>,
    {
        self.errors.extend(iter::once(x.into()))
    }

    pub fn push_error_or_warning<X>(&mut self, x: X, is_error: bool)
    where
        X: Into<E> + Into<W>,
        EI::Wrapper<E>: Extend<E>,
        WI::Wrapper<W>: Extend<W>,
    {
        if is_error {
            self.push_error(x);
        } else {
            self.push_warning(x);
        }
    }

    pub fn extend_warnings(&mut self, xs: impl Iterator<Item = impl Into<W>>)
    where
        WI::Wrapper<W>: Extend<W>,
    {
        self.warnings.extend(xs.map(Into::into))
    }

    pub fn extend_errors(&mut self, xs: impl Iterator<Item = impl Into<E>>)
    where
        EI::Wrapper<E>: Extend<E>,
    {
        self.errors.extend(xs.map(Into::into))
    }

    pub fn extend_errors_or_warnings<X>(&mut self, xs: impl Iterator<Item = X>, is_error: bool)
    where
        X: Into<W> + Into<E>,
        WI::Wrapper<W>: Extend<W>,
        EI::Wrapper<E>: Extend<E>,
    {
        if is_error {
            self.extend_errors(xs);
        } else {
            self.extend_warnings(xs);
        }
    }

    pub fn eval_warning<F, X>(&mut self, f: F)
    where
        X: Into<W>,
        F: FnOnce(&V) -> Option<X>,
        WI::Wrapper<W>: Extend<W>,
    {
        if let Some(e) = f(&self.value) {
            self.push_warning(e.into());
        }
    }

    pub fn eval_error<F, X>(&mut self, f: F)
    where
        X: Into<E>,
        F: FnOnce(&V) -> Option<X>,
        EI::Wrapper<E>: Extend<E>,
    {
        if let Some(e) = f(&self.value) {
            self.push_error(e.into());
        }
    }

    pub fn eval_error_or_warning<F, X>(&mut self, is_error: bool, f: F)
    where
        X: Into<W> + Into<E>,
        F: FnOnce(&V) -> Option<X>,
        WI::Wrapper<W>: Extend<W>,
        EI::Wrapper<E>: Extend<E>,
    {
        if is_error {
            self.eval_error(f);
        } else {
            self.eval_warning(f);
        }
    }

    pub fn eval_errors<F, X, I>(&mut self, f: F)
    where
        E: From<X>,
        F: FnOnce(&V) -> I,
        I: IntoIterator<Item = X>,
        EI::Wrapper<E>: Extend<E>,
    {
        self.extend_errors(f(&self.value).into_iter());
    }

    pub fn void(self) -> TentativeInner<(), W, E, WI, EI> {
        TentativeInner::new((), self.warnings, self.errors)
    }

    #[cfg(test)]
    pub(crate) fn value(&self) -> &V {
        &self.value
    }

    #[cfg(test)]
    pub(crate) fn errors(&self) -> &[E] {
        &self.errors[..]
    }

    #[cfg(test)]
    pub(crate) fn warnings(&self) -> &[W] {
        &self.warnings[..]
    }
}

impl<V, W, E> Tentative<V, W, E> {
    pub fn new_vec(
        value: impl Into<V>,
        warnings: impl IntoIterator<Item = W>,
        errors: impl IntoIterator<Item = E>,
    ) -> Self {
        Self::new(
            value.into(),
            warnings.into_iter().collect(),
            errors.into_iter().collect(),
        )
    }

    pub fn new_vec_either<M>(value: V, msgs: impl IntoIterator<Item = M>, are_errors: bool) -> Self
    where
        E: From<M>,
        W: From<M>,
    {
        if are_errors {
            Self::new_vec(value, [], msgs.into_iter().map(Into::into))
        } else {
            Self::new_vec(value, msgs.into_iter().map(Into::into), [])
        }
    }

    pub fn and_finally<F, X, T>(mut self, mut f: F) -> TerminalResult<X, W, E, T>
    where
        F: FnMut(V) -> TerminalResult<X, W, E, T>,
    {
        match f(self.value) {
            Ok(s) => {
                self.warnings.extend(s.warnings);
                Ok(Terminal::new_vec(s.value, self.warnings))
            }
            Err(e) => {
                self.warnings.extend(e.warnings);
                Err(TerminalFailure::new_vec(self.warnings, e.errors, e.reason))
            }
        }
    }

    pub fn mconcat_ne(xs: NonEmpty<Self>) -> Tentative<NonEmpty<V>, W, E> {
        let mut vs = NonEmpty::new(xs.head.value);
        let mut ws = xs.head.warnings;
        let mut es = xs.head.errors;
        for x in xs.tail {
            vs.push(x.value);
            ws.extend(x.warnings);
            es.extend(x.errors);
        }
        Tentative::new_vec(vs, ws, es)
    }

    pub fn zip<A>(self, a: Tentative<A, W, E>) -> Tentative<(V, A), W, E> {
        self.zip_with(a, |x, y| (x, y))
    }

    pub fn zip3<A, B>(
        self,
        a: Tentative<A, W, E>,
        b: Tentative<B, W, E>,
    ) -> Tentative<(V, A, B), W, E> {
        self.zip(a).zip(b).map(|((x, ax), bx)| (x, ax, bx))
    }

    pub fn zip4<A, B, C>(
        self,
        a: Tentative<A, W, E>,
        b: Tentative<B, W, E>,
        c: Tentative<C, W, E>,
    ) -> Tentative<(V, A, B, C), W, E> {
        self.zip3(a, b)
            .zip(c)
            .map(|((x, ax, bx), cx)| (x, ax, bx, cx))
    }

    pub fn zip5<A, B, C, D>(
        self,
        a: Tentative<A, W, E>,
        b: Tentative<B, W, E>,
        c: Tentative<C, W, E>,
        d: Tentative<D, W, E>,
    ) -> Tentative<(V, A, B, C, D), W, E> {
        self.zip4(a, b, c)
            .zip(d)
            .map(|((x, ax, bx, cx), dx)| (x, ax, bx, cx, dx))
    }

    #[allow(clippy::many_single_char_names)]
    pub fn zip6<A, B, C, D, F>(
        self,
        a: Tentative<A, W, E>,
        b: Tentative<B, W, E>,
        c: Tentative<C, W, E>,
        d: Tentative<D, W, E>,
        e: Tentative<F, W, E>,
    ) -> Tentative<(V, A, B, C, D, F), W, E> {
        self.zip5(a, b, c, d)
            .zip(e)
            .map(|((x, ax, bx, cx, dx), ex)| (x, ax, bx, cx, dx, ex))
    }

    pub fn zip_with<F, X, Y>(mut self, other: Tentative<X, W, E>, f: F) -> Tentative<Y, W, E>
    where
        F: Fn(V, X) -> Y,
    {
        self.warnings.extend(other.warnings);
        self.errors.extend(other.errors);
        Tentative::new_vec(f(self.value, other.value), self.warnings, self.errors)
    }
}

impl<V, E> BiTentative<V, E> {
    pub fn new_either1(x: V, error: Option<E>, is_error: bool) -> Self {
        if let Some(e) = error {
            if is_error {
                Self::new_vec(x, [], [e])
            } else {
                Self::new_vec(x, [e], [])
            }
        } else {
            Self::new1(x)
        }
    }
}

impl<V, W> Tentative<V, W, Infallible> {
    pub fn into_terminal(self) -> Terminal<V, W> {
        Terminal::new_vec(self.value, self.warnings)
    }
}

impl<V, WI: ZeroOrMore, EI: ZeroOrMore> TentativeInner<V, (), Infallible, WI, EI> {
    pub fn from_infallible<W, E>(self) -> TentativeInner<V, W, E, WI, EI> {
        TentativeInner::new1(self.value)
    }
}

impl<V> BiTentative<V, Infallible> {
    pub fn unwrap_infallible(self) -> V {
        self.value
    }

    pub fn new_infallible(value: V) -> Self {
        Self::new1(value)
    }
}

impl<V, W, E, WI: ZeroOrMore, EI: ZeroOrMore> TentativeInner<Option<V>, W, E, WI, EI> {
    pub fn transpose(self) -> Option<TentativeInner<V, W, E, WI, EI>> {
        if let Some(value) = self.value {
            Some(TentativeInner::new(value, self.warnings, self.errors))
        } else {
            None
        }
    }
}

impl<V, W, E, WI, EI> Default for TentativeInner<V, W, E, WI, EI>
where
    V: Default,
    WI: ZeroOrMore,
    EI: ZeroOrMore,
{
    fn default() -> Self {
        Self::new1(V::default())
    }
}

impl<V, W, WI> Default for TerminalInner<V, W, WI>
where
    V: Default,
    WI: ZeroOrMore,
{
    fn default() -> Self {
        Self::new1(V::default())
    }
}

impl<P, W, E, WI: ZeroOrMore, EI: ZeroOrMore> DeferredFailureInner<P, W, E, WI, EI> {
    // TODO misleading name
    pub fn mappend<F, P0, PF>(
        mut self,
        mut other: DeferredFailureInner<P0, W, E, WI, EI>,
        f: F,
    ) -> DeferredFailureInner<PF, W, E, WI, EI>
    where
        F: FnOnce(P, P0) -> PF,
        WI::Wrapper<W>: Extend<W>,
        EI::Wrapper<E>: Extend<E>,
    {
        self.warnings.extend(other.warnings);
        other.errors.prepend(self.errors);
        let p = f(self.passthru, other.passthru);
        DeferredFailureInner::new(self.warnings, other.errors, p)
    }

    // TODO misleading name
    #[must_use]
    pub fn mconcat(es: NonEmpty<Self>) -> DeferredFailureInner<(), W, E, WI, EI>
    where
        WI::Wrapper<W>: Extend<W>,
        EI::Wrapper<E>: Extend<E>,
    {
        let mut acc = es.head.void();
        for x in es.tail {
            acc = acc.mappend(x, |(), _| ());
        }
        acc
    }

    pub fn repack_warnings<WIF>(self) -> DeferredFailureInner<P, W, E, WIF, EI>
    where
        WI: IntoZeroOrMore<WIF>,
        WIF: ZeroOrMore,
    {
        let ws = WI::into_zero_or_more(self.warnings);
        DeferredFailureInner::new(ws, self.errors, self.passthru)
    }

    pub fn repack_errors<EIF>(self) -> DeferredFailureInner<P, W, E, WI, EIF>
    where
        EI: IntoZeroOrMore<EIF>,
        EIF: ZeroOrMore,
    {
        DeferredFailureInner::new(self.warnings, self.errors.repack(), self.passthru)
    }

    pub fn and_tentatively<F, V>(mut self, other: F) -> Self
    where
        F: FnOnce() -> TentativeInner<V, W, E, WI, EI>,
        WI::Wrapper<W>: Extend<W>,
        EI::Wrapper<E>: Extend<E>,
    {
        let tnt = other();
        self.warnings.extend(tnt.warnings);
        self.errors.extend(tnt.errors);
        self
    }

    pub fn map_passthru<F, X>(self, f: F) -> DeferredFailureInner<X, W, E, WI, EI>
    where
        F: FnOnce(P) -> X,
    {
        let x = f(self.passthru);
        DeferredFailureInner::new(self.warnings, self.errors, x)
    }

    pub fn map_warnings<F, X>(self, f: F) -> DeferredFailureInner<P, X, E, WI, EI>
    where
        F: Fn(W) -> X,
    {
        let ws = WI::map(self.warnings, f);
        DeferredFailureInner::new(ws, self.errors, self.passthru)
    }

    pub fn map_errors<F, X>(self, f: F) -> DeferredFailureInner<P, W, X, WI, EI>
    where
        F: Fn(E) -> X,
    {
        DeferredFailureInner::new(self.warnings, self.errors.map(f), self.passthru)
    }

    pub fn warnings_into<X>(self) -> DeferredFailureInner<P, X, E, WI, EI>
    where
        X: From<W>,
    {
        self.map_warnings(Into::into)
    }

    pub fn errors_into<X>(self) -> DeferredFailureInner<P, W, X, WI, EI>
    where
        X: From<E>,
    {
        self.map_errors(Into::into)
    }

    pub fn push_warning(&mut self, x: impl Into<W>)
    where
        WI::Wrapper<W>: Extend<W>,
    {
        self.warnings.extend(iter::once(x.into()));
    }

    pub fn push_error(&mut self, x: impl Into<E>)
    where
        EI::Wrapper<E>: Extend<E>,
    {
        self.errors.extend(iter::once(x.into()));
    }

    pub fn push_error_or_warning<X>(&mut self, x: X, is_error: bool)
    where
        X: Into<E> + Into<W>,
        WI::Wrapper<W>: Extend<W>,
        EI::Wrapper<E>: Extend<E>,
    {
        if is_error {
            self.push_error(x);
        } else {
            self.push_warning(x);
        }
    }

    pub fn void(self) -> DeferredFailureInner<(), W, E, WI, EI> {
        DeferredFailureInner::new(self.warnings, self.errors, ())
    }

    pub fn terminate<T>(self, reason: T) -> TerminalFailureInner<W, E, T, WI, EI> {
        TerminalFailureInner::new(self.warnings, self.errors, reason)
    }

    pub fn terminate_warn2err<T, F, WF, WIF>(
        mut self,
        reason: T,
        f: F,
    ) -> TerminalFailureInner<WF, E, T, WIF, EI>
    where
        F: Fn(W) -> E,
        EI::Wrapper<E>: Extend<E>,
        WIF: ZeroOrMore,
    {
        self.errors.extend(WI::map(self.warnings, f));
        TerminalFailureInner::new1(self.errors, reason)
    }
}

impl<P, WI: ZeroOrMore, EI: ZeroOrMore> DeferredFailureInner<P, Infallible, Infallible, WI, EI> {
    pub fn unwrap_infallible(self) -> P {
        self.passthru
    }
}

impl<P, W, E> DeferredFailure<P, W, E> {
    pub fn new_vec(
        warnings: impl IntoIterator<Item = W>,
        errors: NonEmpty<E>,
        passthru: impl Into<P>,
    ) -> Self {
        Self::new(
            warnings.into_iter().collect(),
            GenNonEmpty::new(errors.head.into(), errors.tail),
            passthru.into(),
        )
    }

    pub fn zip<P1>(self, a: DeferredFailure<P1, W, E>) -> DeferredFailure<(P, P1), W, E> {
        self.zip_with(a, |x, y| (x, y))
    }

    pub fn zip3<P1, P2>(
        self,
        a: DeferredFailure<P1, W, E>,
        b: DeferredFailure<P2, W, E>,
    ) -> DeferredFailure<(P, P1, P2), W, E> {
        self.zip(a).zip(b).map_passthru(|((x, ax), bx)| (x, ax, bx))
    }

    // pub fn zip4<A, B, C>(
    //     self,
    //     a: Tentative<A, W, E>,
    //     b: Tentative<B, W, E>,
    //     c: Tentative<C, W, E>,
    // ) -> Tentative<(V, A, B, C), W, E> {
    //     self.zip3(a, b)
    //         .zip(c)
    //         .map(|((x, ax, bx), cx)| (x, ax, bx, cx))
    // }

    // pub fn zip5<A, B, C, D>(
    //     self,
    //     a: Tentative<A, W, E>,
    //     b: Tentative<B, W, E>,
    //     c: Tentative<C, W, E>,
    //     d: Tentative<D, W, E>,
    // ) -> Tentative<(V, A, B, C, D), W, E> {
    //     self.zip4(a, b, c)
    //         .zip(d)
    //         .map(|((x, ax, bx, cx), dx)| (x, ax, bx, cx, dx))
    // }

    // pub fn zip6<A, B, C, D, F>(
    //     self,
    //     a: Tentative<A, W, E>,
    //     b: Tentative<B, W, E>,
    //     c: Tentative<C, W, E>,
    //     d: Tentative<D, W, E>,
    //     e: Tentative<F, W, E>,
    // ) -> Tentative<(V, A, B, C, D, F), W, E> {
    //     self.zip5(a, b, c, d)
    //         .zip(e)
    //         .map(|((x, ax, bx, cx, dx), ex)| (x, ax, bx, cx, dx, ex))
    // }

    pub fn zip_with<F, P1, X>(
        mut self,
        other: DeferredFailure<P1, W, E>,
        f: F,
    ) -> DeferredFailure<X, W, E>
    where
        F: Fn(P, P1) -> X,
    {
        self.warnings.extend(other.warnings);
        self.errors.extend(other.errors);
        DeferredFailureInner::new(self.warnings, self.errors, f(self.passthru, other.passthru))
    }
}

impl<W, E, WI: ZeroOrMore, EI: ZeroOrMore> DeferredFailureInner<(), W, E, WI, EI> {
    pub fn new1(e: impl Into<E>) -> Self {
        Self::new(WI::Wrapper::<W>::default(), GenNonEmpty::new1(e.into()), ())
    }
}

impl<W, E> DeferredFailure<(), W, E> {
    pub fn new2(errors: NonEmpty<E>) -> Self {
        Self::new_vec([], errors, ())
    }

    pub fn unfail_with<V>(self, value: V) -> Tentative<V, W, E> {
        Tentative::new_vec(value, self.warnings, self.errors)
    }
}

// TODO this name is confusing, the "mutex" refers to the warning and error
// being mutually exclusive, nothing to do with IO locks
pub type MutexResult<V, E> =
    DeferredResultInner<V, E, E, OptionFamily, NullFamily, NullFamily, NullFamily>;

pub type MultiMutexResult<V, E> =
    DeferredResultInner<V, E, E, VecFamily, NullFamily, VecFamily, VecFamily>;

pub type MultiResult1<V, E> =
    DeferredResultInner<V, (), E, NullFamily, NullFamily, NullFamily, VecFamily>;

pub type SingletonResult<V, E> =
    DeferredResultInner<V, (), E, NullFamily, NullFamily, NullFamily, NullFamily>;

pub trait ResultExt: Sized {
    type V;
    type E;

    fn new_singleton(
        value: Self::V,
        error: Self::E,
        is_error: bool,
    ) -> SingletonResult<Self::V, Self::E> {
        if is_error {
            Err(DeferredFailureInner::new1(error))
        } else {
            Ok(TentativeInner::new(
                value,
                NeverValue::default(),
                NeverValue::default(),
            ))
        }
    }

    fn new_mutex(value: Self::V, error: Self::E, is_error: bool) -> MutexResult<Self::V, Self::E> {
        if is_error {
            Err(DeferredFailureInner::new1(error))
        } else {
            Ok(TentativeInner::new(
                value,
                Some(error),
                NeverValue::default(),
            ))
        }
    }

    // TODO necessary?
    fn new_multi_value(value: Self::V) -> MultiResult1<Self::V, Self::E> {
        Ok(TentativeInner::new1(value))
    }

    // fn new_multi_error(errors: NonEmpty<Self::E>) -> MultiResult1<Self::V, Self::E> {
    //     Err(DeferredFailureInner::new(NeverValue::default(), errors, ()))
    // }

    fn into_mult<ToE>(self) -> MultiResult<Self::V, ToE>
    where
        ToE: From<Self::E>;

    fn into_deferred<W, E1>(self) -> DeferredResult<Self::V, W, E1>
    where
        E1: From<Self::E>,
    {
        self.into_deferred_inner()
    }

    #[allow(clippy::type_complexity)]
    fn into_deferred_inner<W, E1, WI0, WI1, EI0, EI1>(
        self,
    ) -> Result<TentativeInner<Self::V, W, E1, WI0, EI0>, DeferredFailureInner<(), W, E1, WI1, EI1>>
    where
        WI0: ZeroOrMore,
        WI1: ZeroOrMore,
        EI0: ZeroOrMore,
        EI1: ZeroOrMore + CanHoldOne,
        E1: From<Self::E>;

    fn zip<A>(self, a: Result<A, Self::E>) -> MultiResult<(Self::V, A), Self::E>;

    fn zip3<A, B>(
        self,
        a: Result<A, Self::E>,
        b: Result<B, Self::E>,
    ) -> MultiResult<(Self::V, A, B), Self::E>;

    fn void(self) -> Result<(), Self::E>;

    fn into_tentative<WI, EI>(
        self,
        default: Self::V,
        is_error: bool,
    ) -> TentativeInner<Self::V, Self::E, Self::E, WI, EI>
    where
        WI: ZeroOrMore + CanHoldOne,
        EI: ZeroOrMore + CanHoldOne,
    {
        self.into_tentative_opt(is_error)
            .map(|x| x.unwrap_or(default))
    }

    fn into_tentative_warn<X, WI, EI>(
        self,
        default: Self::V,
    ) -> TentativeInner<Self::V, Self::E, X, WI, EI>
    where
        WI: ZeroOrMore + CanHoldOne,
        EI: ZeroOrMore,
    {
        self.into_tentative_warn_opt().map(|x| x.unwrap_or(default))
    }

    fn into_tentative_err<X, WI, EI>(
        self,
        default: Self::V,
    ) -> TentativeInner<Self::V, X, Self::E, WI, EI>
    where
        WI: ZeroOrMore,
        EI: ZeroOrMore + CanHoldOne,
    {
        self.into_tentative_err_opt().map(|x| x.unwrap_or(default))
    }

    fn into_tentative_def<WI, EI>(
        self,
        is_error: bool,
    ) -> TentativeInner<Self::V, Self::E, Self::E, WI, EI>
    where
        Self::V: Default,
        WI: ZeroOrMore + CanHoldOne,
        EI: ZeroOrMore + CanHoldOne,
    {
        self.into_tentative_opt(is_error)
            .map(Option::unwrap_or_default)
    }

    fn into_tentative_warn_def<X, WI, EI>(self) -> TentativeInner<Self::V, Self::E, X, WI, EI>
    where
        Self::V: Default,
        WI: ZeroOrMore + CanHoldOne,
        EI: ZeroOrMore,
    {
        self.into_tentative_warn_opt()
            .map(Option::unwrap_or_default)
    }

    fn into_tentative_err_def<X, WI, EI>(self) -> TentativeInner<Self::V, X, Self::E, WI, EI>
    where
        Self::V: Default,
        WI: ZeroOrMore,
        EI: ZeroOrMore + CanHoldOne,
    {
        self.into_tentative_err_opt().map(Option::unwrap_or_default)
    }

    fn into_tentative_opt<WI, EI>(
        self,
        is_error: bool,
    ) -> TentativeInner<Option<Self::V>, Self::E, Self::E, WI, EI>
    where
        WI: ZeroOrMore + CanHoldOne,
        EI: ZeroOrMore + CanHoldOne;

    fn into_tentative_warn_opt<X, WI, EI>(
        self,
    ) -> TentativeInner<Option<Self::V>, Self::E, X, WI, EI>
    where
        WI: ZeroOrMore + CanHoldOne,
        EI: ZeroOrMore;

    fn into_tentative_err_opt<X, WI, EI>(
        self,
    ) -> TentativeInner<Option<Self::V>, X, Self::E, WI, EI>
    where
        WI: ZeroOrMore,
        EI: ZeroOrMore + CanHoldOne;

    fn terminate<T, W>(self, reason: T) -> TerminalResult<Self::V, W, Self::E, T>;
}

impl<V, E> ResultExt for Result<V, E> {
    type V = V;
    type E = E;

    fn into_mult<ToE>(self) -> MultiResult<Self::V, ToE>
    where
        ToE: From<Self::E>,
    {
        self.map_err(|e| NonEmpty::new(e.into()))
    }

    fn into_deferred_inner<W, E1, WI0, WI1, EI0, EI1>(
        self,
    ) -> Result<TentativeInner<Self::V, W, E1, WI0, EI0>, DeferredFailureInner<(), W, E1, WI1, EI1>>
    where
        WI0: ZeroOrMore,
        WI1: ZeroOrMore,
        EI0: ZeroOrMore,
        EI1: ZeroOrMore + CanHoldOne,
        E1: From<Self::E>,
    {
        self.map(TentativeInner::new1)
            .map_err(DeferredFailureInner::new1)
    }

    fn zip<A>(self, a: Result<A, Self::E>) -> MultiResult<(Self::V, A), Self::E> {
        match (self, a) {
            (Ok(ax), Ok(bx)) => Ok((ax, bx)),
            (ae, be) => Err(NonEmpty::from_vec(
                [ae.err(), be.err()].into_iter().flatten().collect(),
            )
            .unwrap()),
        }
    }

    fn zip3<A, B>(
        self,
        a: Result<A, Self::E>,
        b: Result<B, Self::E>,
    ) -> MultiResult<(Self::V, A, B), Self::E> {
        match (self, a, b) {
            (Ok(ax), Ok(bx), Ok(cx)) => Ok((ax, bx, cx)),
            (ae, be, ce) => Err(NonEmpty::from_vec(
                [ae.err(), be.err(), ce.err()]
                    .into_iter()
                    .flatten()
                    .collect(),
            )
            .unwrap()),
        }
    }

    fn void(self) -> Result<(), Self::E> {
        self.map(|_| ())
    }

    fn into_tentative_opt<WI, EI>(
        self,
        is_error: bool,
    ) -> TentativeInner<Option<Self::V>, Self::E, Self::E, WI, EI>
    where
        WI: ZeroOrMore + CanHoldOne,
        EI: ZeroOrMore + CanHoldOne,
    {
        self.map_or_else(
            |e| TentativeInner::new_either(None, e, is_error),
            |v| TentativeInner::new1(Some(v)),
        )
    }

    fn into_tentative_warn_opt<X, WI, EI>(
        self,
    ) -> TentativeInner<Option<Self::V>, Self::E, X, WI, EI>
    where
        WI: ZeroOrMore + CanHoldOne,
        EI: ZeroOrMore,
    {
        self.map_or_else(
            |e| TentativeInner::new(None, WI::wrap(e), EI::Wrapper::<X>::default()),
            |v| TentativeInner::new1(Some(v)),
        )
    }

    fn into_tentative_err_opt<X, WI, EI>(
        self,
    ) -> TentativeInner<Option<Self::V>, X, Self::E, WI, EI>
    where
        WI: ZeroOrMore,
        EI: ZeroOrMore + CanHoldOne,
    {
        self.map_or_else(
            |e| TentativeInner::new(None, WI::Wrapper::<X>::default(), EI::wrap(e)),
            |v| TentativeInner::new1(Some(v)),
        )
    }

    fn terminate<T, W>(self, reason: T) -> TerminalResult<Self::V, W, Self::E, T> {
        self.map_err(|e| TerminalFailure::new1(GenNonEmpty::new1(e), reason))
            .map(Terminal::new1)
    }
}

pub trait MultiResultExt: Sized {
    type V;
    type E;

    fn mult_to_deferred<F, W>(self) -> DeferredResult<Self::V, W, F>
    where
        F: From<Self::E>;

    fn mult_zip<A>(self, a: MultiResult<A, Self::E>) -> MultiResult<(Self::V, A), Self::E>;

    fn mult_zip3<A, B>(
        self,
        a: MultiResult<A, Self::E>,
        b: MultiResult<B, Self::E>,
    ) -> MultiResult<(Self::V, A, B), Self::E>;

    fn mult_errors_into<ToE>(self) -> MultiResult<Self::V, ToE>
    where
        ToE: From<Self::E>,
    {
        self.mult_map_errors(Into::into)
    }

    fn mult_map_errors<F, X>(self, f: F) -> MultiResult<Self::V, X>
    where
        F: Fn(Self::E) -> X;

    fn mult_terminate<T>(self, reason: T) -> TerminalResult<Self::V, Infallible, Self::E, T>;

    fn mult_terminate_def<T: Default>(self) -> TerminalResult<Self::V, Infallible, Self::E, T> {
        self.mult_terminate(T::default())
    }

    fn mult_head(self) -> Result<Self::V, Self::E>;
}

impl<V, E> MultiResultExt for MultiResult<V, E> {
    type V = V;
    type E = E;

    fn mult_to_deferred<F, W>(self) -> DeferredResult<Self::V, W, F>
    where
        F: From<Self::E>,
    {
        self.map(Tentative::new1)
            .map_err(DeferredFailure::new2)
            .def_errors_into()
    }

    fn mult_zip<A>(self, a: MultiResult<A, Self::E>) -> MultiResult<(Self::V, A), Self::E> {
        self.zip(a).map_err(NonEmpty::flatten)
    }

    fn mult_zip3<A, B>(
        self,
        a: MultiResult<A, Self::E>,
        b: MultiResult<B, Self::E>,
    ) -> MultiResult<(Self::V, A, B), Self::E> {
        self.zip3(a, b).map_err(NonEmpty::flatten)
    }

    fn mult_map_errors<F, X>(self, f: F) -> MultiResult<Self::V, X>
    where
        F: Fn(Self::E) -> X,
    {
        self.map_err(|es| es.map(f))
    }

    fn mult_terminate<T>(self, reason: T) -> TerminalResult<Self::V, Infallible, Self::E, T> {
        self.map(Terminal::new1)
            .map_err(|es| DeferredFailure::new2(es).terminate(reason))
    }

    fn mult_head(self) -> Result<Self::V, Self::E> {
        self.map_err(|e| e.head)
    }
}

pub trait PassthruExt: Sized {
    type P;
    type V;
    type E;
    type W;
    type LWI: ZeroOrMore;
    type LEI: ZeroOrMore;
    type RWI: ZeroOrMore;
    type REI: ZeroOrMore;

    #[allow(clippy::type_complexity)]
    fn def_value_into<ToV>(
        self,
    ) -> PassthruResultInner<
        ToV,
        Self::P,
        Self::W,
        Self::E,
        Self::LWI,
        Self::LEI,
        Self::RWI,
        Self::REI,
    >
    where
        ToV: From<Self::V>,
    {
        self.def_map_value(Into::into)
    }

    #[allow(clippy::type_complexity)]
    fn def_inner_into<ToW, ToE>(
        self,
    ) -> PassthruResultInner<Self::V, Self::P, ToW, ToE, Self::LWI, Self::LEI, Self::RWI, Self::REI>
    where
        ToW: From<Self::W>,
        ToE: From<Self::E>,
    {
        self.def_errors_into().def_warnings_into()
    }

    #[allow(clippy::type_complexity)]
    fn def_errors_into<ToE>(
        self,
    ) -> PassthruResultInner<
        Self::V,
        Self::P,
        Self::W,
        ToE,
        Self::LWI,
        Self::LEI,
        Self::RWI,
        Self::REI,
    >
    where
        ToE: From<Self::E>,
    {
        self.def_map_errors(Into::into)
    }

    #[allow(clippy::type_complexity)]
    fn def_errors_liftio(
        self,
    ) -> PassthruResultInner<
        Self::V,
        Self::P,
        Self::W,
        ImpureError<Self::E>,
        Self::LWI,
        Self::LEI,
        Self::RWI,
        Self::REI,
    > {
        self.def_map_errors(ImpureError::Pure)
    }

    #[allow(clippy::type_complexity)]
    fn def_warnings_into<ToW>(
        self,
    ) -> PassthruResultInner<
        Self::V,
        Self::P,
        ToW,
        Self::E,
        Self::LWI,
        Self::LEI,
        Self::RWI,
        Self::REI,
    >
    where
        ToW: From<Self::W>,
    {
        self.def_map_warnings(Into::into)
    }

    #[allow(clippy::type_complexity)]
    fn def_map_value<F, X>(
        self,
        f: F,
    ) -> PassthruResultInner<X, Self::P, Self::W, Self::E, Self::LWI, Self::LEI, Self::RWI, Self::REI>
    where
        F: FnOnce(Self::V) -> X;

    #[allow(clippy::type_complexity)]
    fn def_map_warnings<F, X>(
        self,
        f: F,
    ) -> PassthruResultInner<Self::V, Self::P, X, Self::E, Self::LWI, Self::LEI, Self::RWI, Self::REI>
    where
        F: Fn(Self::W) -> X;

    #[allow(clippy::type_complexity)]
    fn def_map_errors<F, X>(
        self,
        f: F,
    ) -> PassthruResultInner<Self::V, Self::P, Self::W, X, Self::LWI, Self::LEI, Self::RWI, Self::REI>
    where
        F: Fn(Self::E) -> X;

    #[allow(clippy::type_complexity)]
    fn def_and_tentatively<F, X>(
        self,
        f: F,
    ) -> PassthruResultInner<X, Self::P, Self::W, Self::E, Self::LWI, Self::LEI, Self::RWI, Self::REI>
    where
        <Self::LWI as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
        <Self::LEI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        F: FnOnce(Self::V) -> TentativeInner<X, Self::W, Self::E, Self::LWI, Self::LEI>;

    fn def_eval_error<F, X>(&mut self, f: F)
    where
        X: Into<Self::E>,
        F: FnOnce(&Self::V) -> Option<X>,
        <Self::LEI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>;

    fn def_eval_warning<F, X>(&mut self, f: F)
    where
        X: Into<Self::W>,
        F: FnOnce(&Self::V) -> Option<X>,
        <Self::LWI as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>;

    fn def_push_error(&mut self, e: impl Into<Self::E>)
    where
        <Self::LEI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        <Self::REI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>;

    fn def_push_warning(&mut self, w: impl Into<Self::W>)
    where
        <Self::LWI as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
        <Self::RWI as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>;

    fn def_push_error_or_warning<X>(&mut self, x: X, is_error: bool)
    where
        X: Into<Self::W> + Into<Self::E>,
        <Self::LEI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        <Self::REI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        <Self::LWI as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
        <Self::RWI as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
    {
        if is_error {
            self.def_push_error(x);
        } else {
            self.def_push_warning(x);
        }
    }

    #[allow(clippy::type_complexity)]
    fn def_void(
        self,
    ) -> PassthruResultInner<
        (),
        Self::P,
        Self::W,
        Self::E,
        Self::LWI,
        Self::LEI,
        Self::RWI,
        Self::REI,
    > {
        self.def_map_value(|_| ())
    }

    #[allow(clippy::type_complexity)]
    fn def_void_passthru(
        self,
    ) -> DeferredResultInner<Self::V, Self::W, Self::E, Self::LWI, Self::LEI, Self::RWI, Self::REI>;

    #[allow(clippy::type_complexity)]
    fn def_repack_warnings<LWIF, RWIF>(
        self,
    ) -> PassthruResultInner<Self::V, Self::P, Self::W, Self::E, LWIF, Self::LEI, RWIF, Self::REI>
    where
        Self::LWI: IntoZeroOrMore<LWIF>,
        Self::RWI: IntoZeroOrMore<RWIF>,
        LWIF: ZeroOrMore,
        RWIF: ZeroOrMore;

    #[allow(clippy::type_complexity)]
    fn def_repack_errors<LEIF, REIF>(
        self,
    ) -> PassthruResultInner<Self::V, Self::P, Self::W, Self::E, Self::LWI, LEIF, Self::RWI, REIF>
    where
        Self::LEI: IntoZeroOrMore<LEIF>,
        Self::REI: IntoZeroOrMore<REIF>,
        LEIF: ZeroOrMore,
        REIF: ZeroOrMore;
}

impl<V, P, W, E, LWI, LEI, RWI, REI> PassthruExt
    for PassthruResultInner<V, P, W, E, LWI, LEI, RWI, REI>
where
    LWI: ZeroOrMore,
    LEI: ZeroOrMore,
    RWI: ZeroOrMore,
    REI: ZeroOrMore,
{
    type V = V;
    type P = P;
    type W = W;
    type E = E;
    type LWI = LWI;
    type LEI = LEI;
    type RWI = RWI;
    type REI = REI;

    fn def_map_value<F, X>(self, f: F) -> PassthruResultInner<X, P, W, E, LWI, LEI, RWI, REI>
    where
        F: FnOnce(Self::V) -> X,
    {
        self.map(|x| x.map(f))
    }

    fn def_map_warnings<F, X>(self, f: F) -> PassthruResultInner<V, P, X, E, LWI, LEI, RWI, REI>
    where
        F: Fn(Self::W) -> X,
    {
        match self {
            Ok(x) => Ok(x.map_warnings(f)),
            Err(x) => Err(x.map_warnings(f)),
        }
    }

    fn def_map_errors<F, X>(self, f: F) -> PassthruResultInner<V, P, W, X, LWI, LEI, RWI, REI>
    where
        F: Fn(Self::E) -> X,
    {
        match self {
            Ok(x) => Ok(x.map_errors(f)),
            Err(x) => Err(x.map_errors(f)),
        }
    }

    fn def_and_tentatively<F, X>(self, f: F) -> PassthruResultInner<X, P, W, E, LWI, LEI, RWI, REI>
    where
        F: FnOnce(Self::V) -> TentativeInner<X, W, E, LWI, LEI>,
        LWI::Wrapper<W>: Extend<W>,
        LEI::Wrapper<E>: Extend<E>,
    {
        self.map(|x| x.and_tentatively(f))
    }

    fn def_eval_error<F, X>(&mut self, f: F)
    where
        X: Into<Self::E>,
        F: FnOnce(&Self::V) -> Option<X>,
        LEI::Wrapper<E>: Extend<E>,
    {
        if let Ok(tnt) = self.as_mut() {
            tnt.eval_error(f);
        }
    }

    fn def_eval_warning<F, X>(&mut self, f: F)
    where
        X: Into<Self::W>,
        F: FnOnce(&Self::V) -> Option<X>,
        LWI::Wrapper<W>: Extend<W>,
    {
        if let Ok(tnt) = self.as_mut() {
            tnt.eval_warning(f);
        }
    }

    fn def_push_error(&mut self, e: impl Into<E>)
    where
        LEI::Wrapper<E>: Extend<E>,
        REI::Wrapper<E>: Extend<E>,
    {
        match self {
            Ok(tnt) => tnt.push_error(e),
            Err(f) => f.push_error(e),
        }
    }

    fn def_push_warning(&mut self, w: impl Into<W>)
    where
        LWI::Wrapper<W>: Extend<W>,
        RWI::Wrapper<W>: Extend<W>,
    {
        match self {
            Ok(tnt) => tnt.push_warning(w),
            Err(f) => f.push_warning(w),
        }
    }

    fn def_void_passthru(self) -> DeferredResultInner<V, W, E, LWI, LEI, RWI, REI> {
        self.map_err(DeferredFailureInner::void)
    }

    #[allow(clippy::type_complexity)]
    fn def_repack_warnings<LWIF, RWIF>(
        self,
    ) -> PassthruResultInner<V, P, W, E, LWIF, LEI, RWIF, REI>
    where
        LWI: IntoZeroOrMore<LWIF>,
        RWI: IntoZeroOrMore<RWIF>,
        LWIF: ZeroOrMore,
        RWIF: ZeroOrMore,
    {
        self.map(TentativeInner::repack_warnings)
            .map_err(DeferredFailureInner::repack_warnings)
    }

    #[allow(clippy::type_complexity)]
    fn def_repack_errors<LEIF, REIF>(self) -> PassthruResultInner<V, P, W, E, LWI, LEIF, RWI, REIF>
    where
        LEI: IntoZeroOrMore<LEIF>,
        REI: IntoZeroOrMore<REIF>,
        LEIF: ZeroOrMore,
        REIF: ZeroOrMore,
    {
        self.map(TentativeInner::repack_errors)
            .map_err(DeferredFailureInner::repack_errors)
    }
}

pub trait InfalliblePassthruExt: Sized {
    type V;

    fn def_unwrap_infallible(self) -> Self::V;
}

impl<V> InfalliblePassthruExt for PassthruResult<V, (), Infallible, Infallible> {
    type V = V;

    fn def_unwrap_infallible(self) -> V {
        self.map_err(DeferredFailure::unwrap_infallible)
            .unwrap()
            .unwrap_infallible()
    }
}

pub(crate) trait InfalliblePassthruExt1 {
    type V;
    type P;
    type W;
    type LWI: ZeroOrMore;
    type LEI: ZeroOrMore;
    type RWI: ZeroOrMore;
    type REI: ZeroOrMore;

    fn def_unwrap_infallible1<X>(self)
        -> TentativeInner<Self::V, Self::W, X, Self::LWI, Self::LEI>;
}

impl<V, P, W, LWI, LEI, RWI, REI> InfalliblePassthruExt1
    for PassthruResultInner<V, P, W, Infallible, LWI, LEI, RWI, REI>
where
    LWI: ZeroOrMore,
    LEI: ZeroOrMore,
    RWI: ZeroOrMore,
    REI: ZeroOrMore,
{
    type V = V;
    type P = P;
    type W = W;
    type LWI = LWI;
    type LEI = LEI;
    type RWI = RWI;
    type REI = REI;

    fn def_unwrap_infallible1<X>(
        self,
    ) -> TentativeInner<Self::V, Self::W, X, Self::LWI, Self::LEI> {
        // NOTE dirty hack because rust can't tell if a higher order type (the
        // Wrapper in this case) has an Infallible in it. In this case, the
        // wrapper needs to have at least one value in it (NonEmpty or
        // AlwaysValue), and since these can never be constructed with an
        // Infallible inside the whole Error side can never be constructed. I
        // guess rust disagrees because it can't figure out that the Wrapper
        // type may have PhantomData inside (ie is a phantom type) which can
        // be constructed with Infallible.
        //
        // TODO One way to make this much nicer would be to move the single
        // value out into its own field. This has the advantage of making the
        // container types uniform and doesn't require NonEmpty/AlwaysValue. It
        // would also solve this infallible problem.
        match self {
            Ok(tnt) => {
                let es = <Self::LEI as ZeroOrMore>::Wrapper::<X>::default();
                Some(TentativeInner::new(tnt.value, tnt.warnings, es))
            }
            Err(_) => None,
        }
        .expect("infallible result should not happen")
    }
}

pub trait DeferredExt: Sized + PassthruExt {
    #[allow(clippy::type_complexity)]
    fn def_zip<V1>(
        self,
        a: DeferredResultInner<V1, Self::W, Self::E, Self::LWI, Self::LEI, Self::RWI, Self::REI>,
    ) -> DeferredResultInner<
        (Self::V, V1),
        Self::W,
        Self::E,
        Self::LWI,
        Self::LEI,
        Self::RWI,
        Self::REI,
    >
    where
        <Self::LWI as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
        <Self::LEI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>;

    #[allow(clippy::type_complexity)]
    fn def_zip3<V1, V2>(
        self,
        a: DeferredResultInner<V1, Self::W, Self::E, Self::LWI, Self::LEI, Self::RWI, Self::REI>,
        b: DeferredResultInner<V2, Self::W, Self::E, Self::LWI, Self::LEI, Self::RWI, Self::REI>,
    ) -> DeferredResultInner<
        (Self::V, V1, V2),
        Self::W,
        Self::E,
        Self::LWI,
        Self::LEI,
        Self::RWI,
        Self::REI,
    >
    where
        <Self::LWI as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
        <Self::LEI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: PassthruExt<LWI = <Self as PassthruExt>::RWI, LEI = <Self as PassthruExt>::REI>,
    {
        let res = self.def_zip(a);
        DeferredExt::def_zip(res, b).def_map_value(|((x, y), z)| (x, y, z))
    }

    #[allow(clippy::type_complexity)]
    fn def_and_maybe<F, X>(
        self,
        f: F,
    ) -> DeferredResultInner<X, Self::W, Self::E, Self::LWI, Self::LEI, Self::RWI, Self::REI>
    where
        F: FnOnce(
            Self::V,
        ) -> DeferredResultInner<
            X,
            Self::W,
            Self::E,
            Self::LWI,
            Self::LEI,
            Self::RWI,
            Self::REI,
        >,
        <Self::LWI as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
        <Self::LEI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>;

    #[allow(clippy::type_complexity)]
    fn def_and_then<F, X>(
        self,
        f: F,
    ) -> DeferredResultInner<X, Self::W, Self::E, Self::LWI, Self::LEI, Self::RWI, Self::REI>
    where
        F: FnOnce(Self::V) -> Result<X, Self::E>,
        <Self::LWI as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
        <Self::LEI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
    {
        self.def_and_maybe(|x| {
            f(x).map(TentativeInner::new1)
                .map_err(DeferredFailureInner::new1)
        })
    }

    // fn def_and_then<F, X>(self, f: F) -> DeferredResult<X, Self::W, Self::E>
    // where
    //     F: FnOnce(Self::V) -> Result<X, Self::E>;
}

impl<V, W, E, LWI, LEI> DeferredExt for DeferredResultInner<V, W, E, LWI, LEI, LWI, LEI>
where
    LWI: ZeroOrMore,
    LEI: ZeroOrMore,
{
    #[allow(clippy::type_complexity)]
    fn def_zip<V1>(
        self,
        a: DeferredResultInner<V1, W, E, LWI, LEI, LWI, LEI>,
    ) -> DeferredResultInner<(V, V1), W, E, LWI, LEI, LWI, LEI>
    where
        LWI::Wrapper<W>: Extend<W>,
        LEI::Wrapper<E>: Extend<E>,
    {
        match (self, a) {
            (Ok(ax), Ok(bx)) => Ok(ax.mappend(bx, |x, y| (x, y))),
            (Ok(ax), Err(bx)) => Err(ax.and_fail(|_| bx)),
            (Err(ax), Ok(bx)) => Err(ax.and_tentatively(|| bx)),
            (Err(ax), Err(bx)) => Err(ax.mappend(bx, |(), ()| ())),
        }
    }

    fn def_and_maybe<F, X>(self, f: F) -> DeferredResultInner<X, W, E, LWI, LEI, LWI, LEI>
    where
        F: FnOnce(V) -> DeferredResultInner<X, W, E, LWI, LEI, LWI, LEI>,
        LWI::Wrapper<W>: Extend<W>,
        LEI::Wrapper<E>: Extend<E>,
    {
        self?.and_maybe(f)
    }
}

pub trait TerminalExt: Sized + DeferredExt {
    #[allow(clippy::type_complexity)]
    fn def_terminate<T, REIF>(
        self,
        reason: T,
    ) -> TerminalResultInner<Self::V, Self::W, Self::E, T, Self::LWI, Self::RWI, REIF>
    where
        REIF: ZeroOrMore,
        Self::LEI: IntoZeroOrMore<REIF>,
        Self::REI: IntoZeroOrMore<REIF>;

    #[allow(clippy::type_complexity)]
    fn def_terminate_def<T: Default, REIF>(
        self,
    ) -> TerminalResultInner<Self::V, Self::W, Self::E, T, Self::LWI, Self::RWI, REIF>
    where
        REIF: ZeroOrMore,
        Self::LEI: IntoZeroOrMore<REIF>,
        Self::REI: IntoZeroOrMore<REIF>,
    {
        self.def_terminate(T::default())
    }

    #[allow(clippy::type_complexity)]
    fn def_terminate_warn2err<T, F, WF>(
        self,
        reason: T,
        f: F,
    ) -> TerminalResultInner<Self::V, WF, Self::E, T, Self::LWI, Self::RWI, Self::REI>
    where
        F: Fn(Self::W) -> Self::E,
        Self::LWI: IntoZeroOrMore<Self::REI>,
        <Self::LEI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>;

    #[allow(clippy::type_complexity)]
    fn def_terminate_warn2err_def<T: Default, F, WF>(
        self,
        f: F,
    ) -> TerminalResultInner<Self::V, WF, Self::E, T, Self::LWI, Self::RWI, Self::REI>
    where
        F: Fn(Self::W) -> Self::E,
        Self::LWI: IntoZeroOrMore<Self::REI>,
        <Self::LEI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
    {
        self.def_terminate_warn2err(T::default(), f)
    }

    #[allow(clippy::type_complexity)]
    fn def_terminate_nowarn<T, WIF>(
        self,
        reason: T,
    ) -> TerminalResultInner<Self::V, (), Self::E, T, WIF, WIF, Self::REI>
    where
        Self::LEI: IntoZeroOrMore<Self::REI>,
        Self::REI: ZeroOrMore + IntoZeroOrMore<Self::REI>,
        WIF: ZeroOrMore;

    #[allow(clippy::type_complexity)]
    fn def_terminate_maybe_warn<T, F>(
        self,
        reason: T,
        conf: &SharedConfig,
        f: F,
    ) -> TerminalResultInner<Self::V, Self::W, Self::E, T, Self::LWI, Self::RWI, Self::REI>
    where
        F: Fn(Self::W) -> Self::E,
        Self::LWI: IntoZeroOrMore<Self::REI>,
        <Self::LEI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: PassthruExt<LWI = <Self as PassthruExt>::RWI, LEI = <Self as PassthruExt>::REI>,
    {
        if conf.warnings_are_errors {
            self.def_terminate_warn2err(reason, f)
        } else if conf.hide_warnings {
            self.def_terminate_nowarn::<_, Self::LWI>(reason)
                .map(|t| TerminalInner::new1(t.value))
                .map_err(|e| TerminalFailureInner::new1(e.errors, e.reason))
        } else {
            self.def_terminate(reason)
        }
    }

    #[allow(clippy::type_complexity)]
    fn def_terminate_maybe_warn_def<T: Default, F>(
        self,
        conf: &SharedConfig,
        f: F,
    ) -> TerminalResultInner<Self::V, Self::W, Self::E, T, Self::LWI, Self::RWI, Self::REI>
    where
        F: Fn(Self::W) -> Self::E,
        Self::LWI: IntoZeroOrMore<Self::REI>,
        <Self::LEI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: PassthruExt<LWI = <Self as PassthruExt>::RWI, LEI = <Self as PassthruExt>::REI>,
    {
        self.def_terminate_maybe_warn(T::default(), conf, f)
    }
}

impl<V, W, E, LWI, LEI> TerminalExt for DeferredResultInner<V, W, E, LWI, LEI, LWI, LEI>
where
    LWI: ZeroOrMore,
    LEI: ZeroOrMore,
{
    fn def_terminate<T, REIF>(
        self,
        reason: T,
    ) -> Result<TerminalInner<V, W, LWI>, TerminalFailureInner<W, E, T, LWI, REIF>>
    where
        REIF: ZeroOrMore,
        LEI: IntoZeroOrMore<REIF>,
    {
        match self {
            Ok(t) => t.terminate(reason).map_err(|e| e.repack_errors()),
            Err(e) => Err(e.terminate(reason).repack_errors()),
        }
    }

    fn def_terminate_warn2err<T, F, WF>(
        self,
        reason: T,
        f: F,
    ) -> TerminalResultInner<Self::V, WF, Self::E, T, LWI, LWI, LEI>
    where
        F: Fn(W) -> E,
        LWI: IntoZeroOrMore<LEI>,
        LEI::Wrapper<E>: Extend<E>,
    {
        match self {
            Ok(t) => t.terminate_warn2err(reason, f),
            Err(e) => Err(e.terminate_warn2err(reason, f)),
        }
    }

    fn def_terminate_nowarn<T, WIF>(
        self,
        reason: T,
    ) -> TerminalResultInner<Self::V, (), Self::E, T, WIF, WIF, LEI>
    where
        WIF: ZeroOrMore,
    {
        match self {
            Ok(t) => t.terminate_nowarn(reason),
            Err(e) => Err(TerminalFailureInner::new1(e.errors.repack(), reason)),
        }
    }
}

pub trait IODeferredExt: Sized + PassthruExt
where
    Self: PassthruExt<E = ImpureError<Self::InnerE>>,
{
    type InnerE;

    #[allow(clippy::type_complexity)]
    fn def_io_into<ToE>(
        self,
    ) -> DeferredResultInner<
        Self::V,
        Self::W,
        ImpureError<ToE>,
        Self::LWI,
        Self::LEI,
        Self::RWI,
        Self::REI,
    >
    where
        Self: PassthruExt<P = ()>,
        ToE: From<Self::InnerE>,
    {
        self.def_map_errors(ImpureError::inner_into)
    }

    fn def_io_push_error_or_warning<X>(&mut self, x: X, is_error: bool)
    where
        X: Into<Self::W> + Into<Self::InnerE>,
        <Self::LWI as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
        <Self::LEI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        <Self::RWI as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
        <Self::REI as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
    {
        if is_error {
            self.def_push_error(ImpureError::Pure(x.into()));
        } else {
            self.def_push_warning(x);
        }
    }
}

impl<V, W, E> IODeferredExt for IODeferredResult<V, W, E> {
    type InnerE = E;
}

impl<E> ImpureError<E> {
    pub fn inner_into<F>(self) -> ImpureError<F>
    where
        F: From<E>,
    {
        self.map_inner(Into::into)
    }

    pub fn map_inner<F, X>(self, f: F) -> ImpureError<X>
    where
        F: FnOnce(E) -> X,
    {
        match self {
            Self::IO(x) => ImpureError::IO(x),
            Self::Pure(e) => ImpureError::Pure(f(e)),
        }
    }
}

impl ImpureError<Infallible> {
    #[must_use]
    pub fn infallible<E>(self) -> ImpureError<E> {
        match self {
            Self::IO(e) => ImpureError::IO(e),
        }
    }
}

impl<W, E, WI, EI> From<E> for DeferredFailureInner<(), W, E, WI, EI>
where
    WI: ZeroOrMore,
    EI: ZeroOrMore + CanHoldOne,
{
    fn from(value: E) -> Self {
        Self::new1(value)
    }
}

// impl<W, E, WI> From<NonEmpty<E>> for DeferredFailureInner<(), W, E, WI, VecFamily>
// where
//     WI: ZeroOrMore,
// {
//     fn from(value: NonEmpty<E>) -> Self {
//         Self::new(WI::Wrapper::<W>::default(), value.into(), ())
//     }
// }

impl<W, E, WI, EI> From<io::Error> for DeferredFailureInner<(), W, ImpureError<E>, WI, EI>
where
    WI: ZeroOrMore,
    EI: ZeroOrMore + CanHoldOne,
{
    fn from(value: io::Error) -> Self {
        Self::new1(value)
    }
}

impl<W, E, WI, EI, T> From<E> for TerminalFailureInner<W, E, T, WI, EI>
where
    WI: ZeroOrMore,
    EI: ZeroOrMore,
    T: Default,
{
    fn from(value: E) -> Self {
        Self::new1(GenNonEmpty::new1(value), T::default())
    }
}

impl<W, E, WI, T> From<NonEmpty<E>> for TerminalFailureInner<W, E, T, WI, VecFamily>
where
    WI: ZeroOrMore,
    T: Default,
{
    fn from(value: NonEmpty<E>) -> Self {
        Self::new1(
            GenNonEmpty::new(value.head.into(), value.tail),
            T::default(),
        )
    }
}

impl<W, E, WI, EI, T> From<io::Error> for TerminalFailureInner<W, ImpureError<E>, T, WI, EI>
where
    WI: ZeroOrMore,
    EI: ZeroOrMore + CanHoldOne,
    T: Default,
{
    fn from(value: io::Error) -> Self {
        Self::new1(GenNonEmpty::new1(ImpureError::from(value)), T::default())
    }
}

#[cfg(feature = "python")]
mod python {
    use super::ImpureError;

    use pyo3::prelude::*;

    impl<T: Into<Self>> From<ImpureError<T>> for PyErr {
        fn from(value: ImpureError<T>) -> Self {
            match value {
                ImpureError::Pure(e) => e.into(),
                // This should be an OSError of some kind
                ImpureError::IO(e) => e.into(),
            }
        }
    }
}
