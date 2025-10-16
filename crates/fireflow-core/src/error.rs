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
use crate::text::optional::{AlwaysValue, NeverValue};

use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use std::convert::Infallible;
use std::io;
use std::marker::PhantomData;
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
    TerminalFailureInner<Infallible, E, T, NullFamily, NonEmptyFamily>,
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
pub type TerminalFailure<W, E, T> = TerminalFailureInner<W, E, T, VecFamily, NonEmptyFamily>;

/// Failure with one error.
pub type TerminalFailureOne<E> =
    TerminalFailureInner<Infallible, E, (), NullFamily, SingletonFamily>;

/// Final failed result with either one error or multiple errors with a summary.
#[derive(new)]
#[new(visibility = "")]
pub struct TerminalFailureInner<W, E, T, WI: ZeroOrMore, EI: OneOrMore> {
    warnings: WI::Wrapper<W>,
    errors: Box<EI::Wrapper<E>>,
    reason: T,
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

/// A type that may be either an error or warning.
///
/// Useful for functions which only return one error or warning for which
/// using a tentative would be unnecessary.
#[derive(Debug, PartialEq)]
pub enum Leveled<T> {
    Error(T),
    Warning(T),
}

/// Tentative where both error and warning are the same type
pub type BiTentative<V, T> = Tentative<V, T, T>;

pub type DeferredFailure<P, W, E> = DeferredFailureInner<P, W, E, VecFamily, NonEmptyFamily>;

pub type DeferredFailureOne<P, E> =
    DeferredFailureInner<P, Infallible, E, NullFamily, SingletonFamily>;

/// Result which has 1+ errors, 0+ warnings, and the input type.
///
/// Passthru is meant to hold the input type for the failed computation such
/// that it can be reused if needed rather than consumed in a black hole by
/// the ownership model. Obvious use case: failed From<X>-like methods where
/// we may want to do something with the original value after trying and failing
/// to convert it.
#[derive(new)]
pub struct DeferredFailureInner<P, W, E, WI: ZeroOrMore, EI: OneOrMore> {
    warnings: WI::Wrapper<W>,
    errors: Box<EI::Wrapper<E>>,
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

impl<I: Iterator<Item = Result<T, E>>, T, E> ErrorIter<T, E> for I {}

/// Generic higher-order type for something which has zero or more things.
///
/// Use cases: Option, Vec, and NeverValue
pub trait Container {
    type Wrapper<T>: IntoIterator<Item = T>;

    fn map<F, X, Y>(t: Self::Wrapper<X>, f: F) -> Self::Wrapper<Y>
    where
        F: Fn(X) -> Y;
}

/// Generic higher-order type for something which has zero or more things.
///
/// Use cases: Option, Vec, and NeverValue
pub trait ZeroOrMore: Container {}

/// Generic higher-order type for something which has at least one thing.
///
/// Use cases: NonEmpty and AlwaysValue
pub trait OneOrMore: CanHoldOne + Container {
    fn from_one<X>(x: X) -> Self::Wrapper<X> {
        Self::wrap(x)
    }

    fn into_nonempty<X>(xs: Self::Wrapper<X>) -> NonEmpty<X>;
}

/// Generic higher-order type for anything which can hold one thing.
///
/// Use cases: Option, Vec, NonEmpty, Alwaysvalue
pub trait CanHoldOne: Container {
    fn wrap<X>(x: X) -> Self::Wrapper<X>;
}

/// Generic higher-order type for anything which holds up to infinite things
///
/// Use cases: Vec, NonEmpty
pub trait CanHoldMany: Container {
    fn push<X>(t: &mut Self::Wrapper<X>, x: X);

    fn extend<X>(t: &mut Self::Wrapper<X>, x: impl IntoIterator<Item = X>);
}

/// Generic "adder" for types
pub trait Appendable<Other> {
    type Out;
    fn append_het(self, other: Other) -> Self::Out;
}

/// Lifting containers that many hold values into those that may hold more..
pub trait IntoZeroOrMore<Other: ZeroOrMore>: ZeroOrMore {
    fn into_zero_or_more<X>(x: Self::Wrapper<X>) -> Other::Wrapper<X>;
}

/// Lifting container that holds some number of values into those that hold many.
pub trait IntoOneOrMore<Other: OneOrMore>: ZeroOrMore {
    fn into_one_or_more<X>(x: Self::Wrapper<X>) -> Option<Other::Wrapper<X>>;
}

pub struct OptionFamily;

pub struct VecFamily;

pub struct SingletonFamily;

pub struct NonEmptyFamily;

pub struct NullFamily;

impl Container for OptionFamily {
    type Wrapper<T> = Option<T>;

    fn map<F, X, Y>(t: Self::Wrapper<X>, f: F) -> Self::Wrapper<Y>
    where
        F: Fn(X) -> Y,
    {
        t.map(f)
    }
}

impl Container for VecFamily {
    type Wrapper<T> = Vec<T>;

    fn map<F, X, Y>(t: Self::Wrapper<X>, f: F) -> Self::Wrapper<Y>
    where
        F: Fn(X) -> Y,
    {
        t.into_iter().map(f).collect()
    }
}

impl Container for NullFamily {
    type Wrapper<T> = NeverValue<T>;

    fn map<F, X, Y>(_: Self::Wrapper<X>, _: F) -> Self::Wrapper<Y>
    where
        F: Fn(X) -> Y,
    {
        NeverValue(PhantomData)
    }
}

impl Container for SingletonFamily {
    type Wrapper<T> = AlwaysValue<T>;

    fn map<F, X, Y>(t: Self::Wrapper<X>, f: F) -> Self::Wrapper<Y>
    where
        F: Fn(X) -> Y,
    {
        AlwaysValue(f(t.0))
    }
}

impl Container for NonEmptyFamily {
    type Wrapper<T> = NonEmpty<T>;

    fn map<F, X, Y>(t: Self::Wrapper<X>, f: F) -> Self::Wrapper<Y>
    where
        F: Fn(X) -> Y,
    {
        t.map(f)
    }
}

impl ZeroOrMore for NullFamily {}
impl ZeroOrMore for VecFamily {}
impl ZeroOrMore for OptionFamily {}

impl OneOrMore for SingletonFamily {
    fn into_nonempty<X>(xs: Self::Wrapper<X>) -> NonEmpty<X> {
        NonEmpty::new(xs.0)
    }
}

impl OneOrMore for NonEmptyFamily {
    fn into_nonempty<X>(xs: Self::Wrapper<X>) -> NonEmpty<X> {
        xs
    }
}

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

impl CanHoldOne for SingletonFamily {
    fn wrap<X>(x: X) -> Self::Wrapper<X> {
        AlwaysValue(x)
    }
}

impl CanHoldOne for NonEmptyFamily {
    fn wrap<X>(x: X) -> Self::Wrapper<X> {
        NonEmpty::new(x)
    }
}

macro_rules! impl_holds_many {
    ($t:ident, $inner:ident) => {
        impl CanHoldMany for $t {
            fn push<X>(t: &mut Self::Wrapper<X>, x: X) {
                t.push(x);
            }

            fn extend<X>(t: &mut Self::Wrapper<X>, xs: impl IntoIterator<Item = X>) {
                t.extend(xs);
            }
        }
    };
}

impl_holds_many!(VecFamily, Vec);
impl_holds_many!(NonEmptyFamily, NonEmpty);

macro_rules! impl_concat_null_left {
    ($t:ident) => {
        impl<T> Appendable<$t<T>> for NeverValue<T> {
            type Out = $t<T>;
            fn append_het(self, other: $t<T>) -> Self::Out {
                other
            }
        }
    };
}

macro_rules! impl_concat_null_right {
    ($t:ident) => {
        impl<T> Appendable<NeverValue<T>> for $t<T> {
            type Out = $t<T>;
            fn append_het(self, _: NeverValue<T>) -> Self::Out {
                self
            }
        }
    };
}

macro_rules! impl_concat_chain_iter {
    ($a:ident, $b:ident) => {
        impl<T> Appendable<$b<T>> for $a<T> {
            type Out = Vec<T>;
            fn append_het(self, other: $b<T>) -> Self::Out {
                self.into_iter().chain(other).collect()
            }
        }
    };
}

macro_rules! impl_concat_chain_iter_ne {
    ($a:ident) => {
        impl<T> Appendable<NonEmpty<T>> for $a<T> {
            type Out = NonEmpty<T>;
            fn append_het(self, other: NonEmpty<T>) -> Self::Out {
                NonEmpty::collect(self.into_iter().chain(other)).unwrap()
            }
        }
    };
}

macro_rules! impl_concat_extend {
    ($a:ident, $b:ident, $c:ident) => {
        impl<T> Appendable<$b<T>> for $a<T> {
            type Out = $c<T>;
            fn append_het(mut self, other: $b<T>) -> Self::Out {
                self.extend(other);
                self
            }
        }
    };
}

impl_concat_null_left!(Vec);
impl_concat_null_left!(NonEmpty);
impl_concat_null_left!(Option);
impl_concat_null_left!(AlwaysValue);
impl_concat_null_left!(NeverValue);

impl_concat_null_right!(Vec);
impl_concat_null_right!(NonEmpty);
impl_concat_null_right!(AlwaysValue);
impl_concat_null_right!(Option);

impl_concat_chain_iter!(Option, Option);
impl_concat_chain_iter!(Option, Vec);
impl_concat_chain_iter!(Option, AlwaysValue);
impl_concat_chain_iter!(Vec, Option);
impl_concat_chain_iter!(Vec, Vec);
impl_concat_chain_iter!(Vec, AlwaysValue);
impl_concat_chain_iter!(AlwaysValue, Option);
impl_concat_chain_iter!(AlwaysValue, Vec);
impl_concat_chain_iter!(AlwaysValue, AlwaysValue);

impl_concat_chain_iter_ne!(Option);
impl_concat_chain_iter_ne!(Vec);
impl_concat_chain_iter_ne!(AlwaysValue);

impl_concat_extend!(NonEmpty, NonEmpty, NonEmpty);
impl_concat_extend!(NonEmpty, Vec, NonEmpty);
impl_concat_extend!(NonEmpty, Option, NonEmpty);
impl_concat_extend!(NonEmpty, AlwaysValue, NonEmpty);

impl IntoZeroOrMore<Self> for NullFamily {
    fn into_zero_or_more<X>(x: Self::Wrapper<X>) -> NeverValue<X> {
        x
    }
}

impl IntoZeroOrMore<Self> for VecFamily {
    fn into_zero_or_more<X>(x: Self::Wrapper<X>) -> Vec<X> {
        x
    }
}

impl IntoZeroOrMore<Self> for OptionFamily {
    fn into_zero_or_more<X>(x: Self::Wrapper<X>) -> Option<X> {
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

impl IntoOneOrMore<SingletonFamily> for OptionFamily {
    fn into_one_or_more<X>(x: Self::Wrapper<X>) -> Option<AlwaysValue<X>> {
        x.map(AlwaysValue)
    }
}

impl IntoOneOrMore<NonEmptyFamily> for OptionFamily {
    fn into_one_or_more<X>(x: Self::Wrapper<X>) -> Option<NonEmpty<X>> {
        NonEmpty::collect(x)
    }
}

impl IntoOneOrMore<NonEmptyFamily> for VecFamily {
    fn into_one_or_more<X>(x: Self::Wrapper<X>) -> Option<NonEmpty<X>> {
        NonEmpty::collect(x)
    }
}

impl<V, W, WI: ZeroOrMore> TerminalInner<V, W, WI> {
    fn new1(value: impl Into<V>) -> Self
    where
        WI::Wrapper<W>: Default,
    {
        Self::new(value.into(), WI::Wrapper::<W>::default())
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

    fn warnings_to_errors<T, E, F, EI>(
        self,
        reason: T,
        f: F,
    ) -> Result<Self, TerminalFailureInner<W, E, T, WI, EI>>
    where
        F: Fn(W) -> E,
        WI: IntoOneOrMore<EI>,
        EI: OneOrMore,
        WI::Wrapper<W>: Default,
    {
        match WI::into_one_or_more(self.warnings) {
            None => Ok(Self::new1(self.value)),
            Some(ws) => Err(TerminalFailureInner::new1(EI::map(ws, f), reason)),
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
                Err(TerminalFailure::new_vec(self.warnings, *e.errors, e.reason))
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

impl<W, E, T, WI: ZeroOrMore, EI: OneOrMore> TerminalFailureInner<W, E, T, WI, EI> {
    fn new1(errors: EI::Wrapper<E>, reason: T) -> Self
    where
        WI::Wrapper<W>: Default,
    {
        Self::new(WI::Wrapper::<W>::default(), errors.into(), reason)
    }

    pub fn resolve<F, G, X, Y>(self, f: F, g: G) -> (X, Y)
    where
        F: FnOnce(WI::Wrapper<W>) -> X,
        G: FnOnce(EI::Wrapper<E>, T) -> Y,
    {
        (f(self.warnings), g(*self.errors, self.reason))
    }

    fn warnings_to_errors<F>(mut self, f: F) -> Self
    where
        F: Fn(W) -> E,
        WI::Wrapper<W>: Default,
        EI: CanHoldMany,
    {
        EI::extend(&mut *self.errors, self.warnings.into_iter().map(f));
        Self::new1(*self.errors, self.reason)
    }
}

impl<W, E, T> TerminalFailure<W, E, T> {
    fn new_vec(
        warnings: impl IntoIterator<Item = W>,
        errors: impl Into<Box<NonEmpty<E>>>,
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
    pub fn new1(value: V) -> Self
    where
        WI::Wrapper<W>: Default,
        EI::Wrapper<E>: Default,
    {
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
        WI::Wrapper<W>: Default,
        EI::Wrapper<E>: Default,
    {
        if are_errors {
            Self::new(value, WI::Wrapper::<W>::default(), EI::wrap(E::from(msg)))
        } else {
            Self::new(value, WI::wrap(W::from(msg)), EI::Wrapper::<E>::default())
        }
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

    pub fn mappend<F, V0, VF, WI0, EI0, WIF, EIF>(
        self,
        other: TentativeInner<V0, W, E, WI0, EI0>,
        f: F,
    ) -> TentativeInner<VF, W, E, WIF, EIF>
    where
        F: FnOnce(V, V0) -> VF,
        WI::Wrapper<W>: Appendable<WI0::Wrapper<W>, Out = WIF::Wrapper<W>>,
        EI::Wrapper<E>: Appendable<EI0::Wrapper<E>, Out = EIF::Wrapper<E>>,
        WI0: ZeroOrMore,
        EI0: ZeroOrMore,
        WIF: ZeroOrMore,
        EIF: ZeroOrMore,
    {
        let ws = WI::Wrapper::<W>::append_het(self.warnings, other.warnings);
        let es = EI::Wrapper::<E>::append_het(self.errors, other.errors);
        TentativeInner::new(f(self.value, other.value), ws, es)
    }

    pub fn and_tentatively_gen<F, X, WI0, EI0, WIF, EIF>(
        self,
        f: F,
    ) -> TentativeInner<X, W, E, WIF, EIF>
    where
        F: FnOnce(V) -> TentativeInner<X, W, E, WI0, EI0>,
        WI::Wrapper<W>: Appendable<WI0::Wrapper<W>, Out = WIF::Wrapper<W>>,
        EI::Wrapper<E>: Appendable<EI0::Wrapper<E>, Out = EIF::Wrapper<E>>,
        WI0: ZeroOrMore,
        EI0: ZeroOrMore,
        WIF: ZeroOrMore,
        EIF: ZeroOrMore,
    {
        let s = f(self.value);
        let ws = WI::Wrapper::<W>::append_het(self.warnings, s.warnings);
        let es = EI::Wrapper::<E>::append_het(self.errors, s.errors);
        TentativeInner::new(s.value, ws, es)
    }

    pub fn and_tentatively<F, X>(self, f: F) -> TentativeInner<X, W, E, WI, EI>
    where
        F: FnOnce(V) -> TentativeInner<X, W, E, WI, EI>,
        WI::Wrapper<W>: Appendable<WI::Wrapper<W>, Out = WI::Wrapper<W>>,
        EI::Wrapper<E>: Appendable<EI::Wrapper<E>, Out = EI::Wrapper<E>>,
    {
        self.and_tentatively_gen::<F, X, WI, EI, WI, EI>(f)
    }

    pub fn and_maybe_gen<F, X, P, LWI0, LEI0, LWIF, LEIF, RWI0, REI0, RWIF, REIF>(
        self,
        f: F,
    ) -> PassthruResultInner<P, X, W, E, LWIF, LEIF, RWIF, REIF>
    where
        F: FnOnce(V) -> PassthruResultInner<P, X, W, E, LWI0, LEI0, RWI0, REI0>,
        WI::Wrapper<W>: Appendable<LWI0::Wrapper<W>, Out = LWIF::Wrapper<W>>
            + Appendable<RWI0::Wrapper<W>, Out = RWIF::Wrapper<W>>,
        EI::Wrapper<E>: Appendable<LEI0::Wrapper<E>, Out = LEIF::Wrapper<E>>
            + Appendable<REI0::Wrapper<E>, Out = REIF::Wrapper<E>>,
        LWI0: ZeroOrMore,
        LEI0: ZeroOrMore,
        LWIF: ZeroOrMore,
        LEIF: ZeroOrMore,
        RWI0: ZeroOrMore,
        REI0: OneOrMore,
        RWIF: ZeroOrMore,
        REIF: OneOrMore,
    {
        match f(self.value) {
            Ok(s) => {
                let ws = WI::Wrapper::<W>::append_het(self.warnings, s.warnings);
                let es = EI::Wrapper::<E>::append_het(self.errors, s.errors);
                Ok(TentativeInner::new(s.value, ws, es))
            }
            Err(e) => {
                let ws = WI::Wrapper::<W>::append_het(self.warnings, e.warnings);
                let es = EI::Wrapper::<E>::append_het(self.errors, *e.errors);
                Err(DeferredFailureInner::new(ws, es.into(), e.passthru))
            }
        }
    }

    pub fn and_maybe<F, X, P, RWIF, REIF>(
        self,
        f: F,
    ) -> PassthruResultInner<P, X, W, E, WI, EI, RWIF, REIF>
    where
        F: FnOnce(V) -> PassthruResultInner<P, X, W, E, WI, EI, RWIF, REIF>,
        WI::Wrapper<W>: Appendable<WI::Wrapper<W>, Out = WI::Wrapper<W>>
            + Appendable<RWIF::Wrapper<W>, Out = RWIF::Wrapper<W>>,
        EI::Wrapper<E>: Appendable<EI::Wrapper<E>, Out = EI::Wrapper<E>>
            + Appendable<REIF::Wrapper<E>, Out = REIF::Wrapper<E>>,
        RWIF: ZeroOrMore,
        REIF: OneOrMore,
    {
        self.and_maybe_gen::<F, X, P, WI, EI, WI, EI, RWIF, REIF, RWIF, REIF>(f)
    }

    pub fn and_fail<F, P, RWI0, REI0, RWIF, REIF>(
        self,
        other: F,
    ) -> DeferredFailureInner<P, W, E, RWIF, REIF>
    where
        F: FnOnce(V) -> DeferredFailureInner<P, W, E, RWI0, REI0>,
        WI::Wrapper<W>: Appendable<RWI0::Wrapper<W>, Out = RWIF::Wrapper<W>>,
        EI::Wrapper<E>: Appendable<REI0::Wrapper<E>, Out = REIF::Wrapper<E>>,
        RWI0: ZeroOrMore,
        REI0: OneOrMore,
        RWIF: ZeroOrMore,
        REIF: OneOrMore,
    {
        let e = other(self.value);
        let ws = WI::Wrapper::<W>::append_het(self.warnings, e.warnings);
        let es = EI::Wrapper::<E>::append_het(self.errors, *e.errors);
        DeferredFailureInner::new(ws, es.into(), e.passthru)
    }

    pub fn terminate<T, REI>(self, reason: T) -> TerminalResultInner<V, W, E, T, WI, WI, REI>
    where
        REI: OneOrMore,
        EI: IntoOneOrMore<REI>,
    {
        self.terminate_inner(reason).map(|(t, _)| t)
    }

    pub fn terminate_def<T: Default, REI>(self) -> TerminalResultInner<V, W, E, T, WI, WI, REI>
    where
        REI: OneOrMore,
        EI: IntoOneOrMore<REI>,
    {
        self.terminate(T::default())
    }

    #[allow(clippy::type_complexity)]
    fn terminate_inner<T, REI>(
        self,
        reason: T,
    ) -> Result<(TerminalInner<V, W, WI>, T), TerminalFailureInner<W, E, T, WI, REI>>
    where
        REI: OneOrMore,
        EI: IntoOneOrMore<REI>,
    {
        match EI::into_one_or_more(self.errors) {
            Some(errors) => Err(TerminalFailureInner::new(
                self.warnings,
                errors.into(),
                reason,
            )),
            None => Ok((TerminalInner::new(self.value, self.warnings), reason)),
        }
    }

    pub fn terminate_warn2err<F, T, REI>(
        self,
        reason: T,
        f: F,
    ) -> TerminalResultInner<V, W, E, T, WI, WI, REI>
    where
        F: Fn(W) -> E,
        WI::Wrapper<W>: Default,
        WI: IntoOneOrMore<REI>,
        EI: IntoOneOrMore<REI>,
        REI: OneOrMore + CanHoldMany,
    {
        match self.terminate_inner(reason) {
            Ok((t, r)) => t.warnings_to_errors(r, f),
            Err(e) => Err(e.warnings_to_errors(f)),
        }
    }

    pub fn terminate_nowarn<T, REI>(self, reason: T) -> TerminalResultInner<V, W, E, T, WI, WI, REI>
    where
        WI::Wrapper<W>: Default,
        EI: IntoOneOrMore<REI>,
        REI: OneOrMore + CanHoldMany,
    {
        match EI::into_one_or_more(self.errors) {
            None => Ok(TerminalInner::new1(self.value)),
            Some(e) => Err(TerminalFailureInner::new1(e, reason)),
        }
    }

    pub fn push_error(&mut self, x: impl Into<E>)
    where
        EI: CanHoldMany,
    {
        EI::push(&mut self.errors, x.into());
    }

    pub fn push_warning(&mut self, x: impl Into<W>)
    where
        WI: CanHoldMany,
    {
        WI::push(&mut self.warnings, x.into());
    }

    pub fn push_error_or_warning<X>(&mut self, x: X, is_error: bool)
    where
        X: Into<E> + Into<W>,
        WI: CanHoldMany,
        EI: CanHoldMany,
    {
        if is_error {
            self.push_error(x);
        } else {
            self.push_warning(x);
        }
    }

    pub fn extend_warnings(&mut self, xs: impl Iterator<Item = impl Into<W>>)
    where
        WI: CanHoldMany,
    {
        WI::extend(&mut self.warnings, xs.map(Into::into));
    }

    pub fn extend_errors(&mut self, xs: impl Iterator<Item = impl Into<E>>)
    where
        EI: CanHoldMany,
    {
        EI::extend(&mut self.errors, xs.map(Into::into));
    }

    pub fn extend_errors_or_warnings<X>(&mut self, xs: impl Iterator<Item = X>, is_error: bool)
    where
        X: Into<W> + Into<E>,
        WI: CanHoldMany,
        EI: CanHoldMany,
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
        WI: CanHoldMany,
    {
        if let Some(e) = f(&self.value) {
            self.push_warning(e.into());
        }
    }

    pub fn eval_error<F, X>(&mut self, f: F)
    where
        X: Into<E>,
        F: FnOnce(&V) -> Option<X>,
        EI: CanHoldMany,
    {
        if let Some(e) = f(&self.value) {
            self.push_error(e.into());
        }
    }

    pub fn eval_error_or_warning<F, X>(&mut self, is_error: bool, f: F)
    where
        X: Into<W> + Into<E>,
        F: FnOnce(&V) -> Option<X>,
        WI: CanHoldMany,
        EI: CanHoldMany,
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
        EI: CanHoldMany,
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
                Err(TerminalFailure::new_vec(self.warnings, *e.errors, e.reason))
            }
        }
    }

    pub fn mconcat(xs: impl IntoIterator<Item = Self>) -> Tentative<Vec<V>, W, E> {
        let mut ret = Tentative::new1(vec![]);
        for x in xs {
            ret.value.push(x.value);
            ret.warnings.extend(x.warnings);
            ret.errors.extend(x.errors);
        }
        ret
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
    WI::Wrapper<W>: Default,
    EI::Wrapper<E>: Default,
{
    fn default() -> Self {
        Self::new1(V::default())
    }
}

impl<V, W, WI> Default for TerminalInner<V, W, WI>
where
    V: Default,
    WI: ZeroOrMore,
    WI::Wrapper<W>: Default,
{
    fn default() -> Self {
        Self::new1(V::default())
    }
}

impl<P, W, E, WI: ZeroOrMore, EI: OneOrMore> DeferredFailureInner<P, W, E, WI, EI> {
    pub fn mappend<F, P0, PF, WI0, EI0, WIF, EIF>(
        self,
        other: DeferredFailureInner<P0, W, E, WI0, EI0>,
        f: F,
    ) -> DeferredFailureInner<PF, W, E, WIF, EIF>
    where
        F: FnOnce(P, P0) -> PF,
        WI::Wrapper<W>: Appendable<WI0::Wrapper<W>, Out = WIF::Wrapper<W>>,
        EI::Wrapper<E>: Appendable<EI0::Wrapper<E>, Out = EIF::Wrapper<E>>,
        WI0: ZeroOrMore,
        EI0: OneOrMore,
        WIF: ZeroOrMore,
        EIF: OneOrMore,
    {
        let ws = WI::Wrapper::<W>::append_het(self.warnings, other.warnings);
        let es = EI::Wrapper::<E>::append_het(*self.errors, *other.errors);
        DeferredFailureInner::new(ws, es.into(), f(self.passthru, other.passthru))
    }

    #[must_use]
    pub fn mconcat<WIF>(es: NonEmpty<Self>) -> DeferredFailureInner<(), W, E, WIF, NonEmptyFamily>
    where
        WI: IntoZeroOrMore<WIF>,
        WIF::Wrapper<W>: Appendable<WI::Wrapper<W>, Out = WIF::Wrapper<W>>,
        NonEmpty<E>: Appendable<EI::Wrapper<E>, Out = NonEmpty<E>>,
        WIF: ZeroOrMore,
    {
        let mut acc = DeferredFailureInner::new(
            WI::into_zero_or_more(es.head.warnings),
            EI::into_nonempty(*es.head.errors).into(),
            (),
        );
        for x in es.tail {
            acc = acc.mappend::<_, _, _, WI, EI, WIF, NonEmptyFamily>(x, |(), _| ());
        }
        acc
    }

    pub fn and_tentatively<F, V, WI0, EI0, WIF, EIF>(
        self,
        other: F,
    ) -> DeferredFailureInner<P, W, E, WIF, EIF>
    where
        F: FnOnce() -> TentativeInner<V, W, E, WI0, EI0>,
        WI::Wrapper<W>: Appendable<WI0::Wrapper<W>, Out = WIF::Wrapper<W>>,
        EI::Wrapper<E>: Appendable<EI0::Wrapper<E>, Out = EIF::Wrapper<E>>,
        WI0: ZeroOrMore,
        EI0: ZeroOrMore,
        WIF: ZeroOrMore,
        EIF: OneOrMore,
    {
        let tnt = other();
        let ws = WI::Wrapper::<W>::append_het(self.warnings, tnt.warnings);
        let es = EI::Wrapper::<E>::append_het(*self.errors, tnt.errors);
        DeferredFailureInner::new(ws, es.into(), self.passthru)
    }

    pub fn map_passthru<F, X>(self, f: F) -> DeferredFailureInner<X, W, E, WI, EI>
    where
        F: FnOnce(P) -> X,
    {
        DeferredFailureInner::new(self.warnings, self.errors, f(self.passthru))
    }

    pub fn map_warnings<F, X>(self, f: F) -> DeferredFailureInner<P, X, E, WI, EI>
    where
        F: Fn(W) -> X,
    {
        DeferredFailureInner::new(WI::map(self.warnings, f), self.errors, self.passthru)
    }

    pub fn map_errors<F, X>(self, f: F) -> DeferredFailureInner<P, W, X, WI, EI>
    where
        F: Fn(E) -> X,
    {
        DeferredFailureInner::new(
            self.warnings,
            EI::map(*self.errors, f).into(),
            self.passthru,
        )
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
        WI: CanHoldMany,
    {
        WI::push(&mut self.warnings, x.into());
    }

    pub fn push_error(&mut self, x: impl Into<E>)
    where
        EI: CanHoldMany,
    {
        EI::push(&mut *self.errors, x.into());
    }

    pub fn push_error_or_warning<X>(&mut self, x: X, is_error: bool)
    where
        X: Into<E> + Into<W>,
        WI: CanHoldMany,
        EI: CanHoldMany,
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

    pub fn terminate_warn2err<T, F>(
        mut self,
        reason: T,
        f: F,
    ) -> TerminalFailureInner<W, E, T, WI, EI>
    where
        F: Fn(W) -> E,
        WI::Wrapper<W>: Default,
        EI: CanHoldMany,
    {
        EI::extend(&mut *self.errors, self.warnings.into_iter().map(f));
        TerminalFailureInner::new1(*self.errors, reason)
    }
}

impl<P, WI: ZeroOrMore, EI: OneOrMore> DeferredFailureInner<P, Infallible, Infallible, WI, EI> {
    pub fn unwrap_infallible(self) -> P {
        self.passthru
    }
}

impl<P, W, E> DeferredFailure<P, W, E> {
    pub fn new_vec(
        warnings: impl IntoIterator<Item = W>,
        errors: impl Into<Box<NonEmpty<E>>>,
        passthru: impl Into<P>,
    ) -> Self {
        Self::new(
            warnings.into_iter().collect(),
            errors.into(),
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
        self.errors.extend(*other.errors);
        DeferredFailure::new_vec(self.warnings, self.errors, f(self.passthru, other.passthru))
    }
}

impl<W, E, WI: ZeroOrMore, EI: OneOrMore> DeferredFailureInner<(), W, E, WI, EI> {
    pub fn new1(e: impl Into<E>) -> Self
    where
        WI::Wrapper<W>: Default,
        EI: CanHoldOne,
    {
        Self::new(
            WI::Wrapper::<W>::default(),
            EI::from_one(e.into()).into(),
            (),
        )
    }
}

impl<W, E> DeferredFailure<(), W, E> {
    pub fn new2(errors: NonEmpty<E>) -> Self {
        Self::new_vec([], errors, ())
    }

    pub fn unfail_with<V>(self, value: V) -> Tentative<V, W, E> {
        Tentative::new_vec(value, self.warnings, *self.errors)
    }
}

impl<T> Leveled<T> {
    pub fn new(value: T, is_error: bool) -> Self {
        if is_error {
            Self::Error(value)
        } else {
            Self::Warning(value)
        }
    }

    #[must_use]
    pub fn many_to_tentative(xs: Vec<Self>) -> BiTentative<(), T> {
        let (ws, es): (Vec<_>, Vec<_>) = xs
            .into_iter()
            .map(|x| match x {
                Self::Warning(y) => Ok(y),
                Self::Error(y) => Err(y),
            })
            .partition_result();
        Tentative::new_vec((), ws, es)
    }

    // pub fn many_to_deferred(xs: Vec<Self>) -> BiDeferredResult<(), T> {
    //     let (ws, es): (Vec<_>, Vec<_>) = xs
    //         .into_iter()
    //         .map(|x| match x {
    //             Self::Warning(y) => Ok(y),
    //             Self::Error(y) => Err(y),
    //         })
    //         .partition_result();
    //     match NonEmpty::from_vec(es) {
    //         None => Ok(Tentative::new((), ws, vec![])),
    //         Some(xs) => Err(DeferredFailure::new(ws, xs, ())),
    //     }
    // }

    pub fn inner_into<X>(self) -> Leveled<X>
    where
        X: From<T>,
    {
        self.map(Into::into)
    }

    pub fn map<F, X>(self, f: F) -> Leveled<X>
    where
        F: FnOnce(T) -> X,
    {
        match self {
            Self::Error(x) => Leveled::Error(f(x)),
            Self::Warning(x) => Leveled::Warning(f(x)),
        }
    }
}

pub trait ResultExt: Sized {
    type V;
    type E;

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
        EI1: OneOrMore + CanHoldOne,
        WI0::Wrapper<W>: Default,
        WI1::Wrapper<W>: Default,
        EI0::Wrapper<E1>: Default,
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
        WI::Wrapper<Self::E>: Default,
        EI::Wrapper<Self::E>: Default,
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
        WI::Wrapper<Self::E>: Default,
        EI::Wrapper<X>: Default,
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
        WI::Wrapper<X>: Default,
        EI::Wrapper<Self::E>: Default,
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
        WI::Wrapper<Self::E>: Default,
        EI::Wrapper<Self::E>: Default,
    {
        self.into_tentative_opt(is_error)
            .map(Option::unwrap_or_default)
    }

    fn into_tentative_warn_def<X, WI, EI>(self) -> TentativeInner<Self::V, Self::E, X, WI, EI>
    where
        Self::V: Default,
        WI: ZeroOrMore + CanHoldOne,
        EI: ZeroOrMore,
        WI::Wrapper<Self::E>: Default,
        EI::Wrapper<X>: Default,
    {
        self.into_tentative_warn_opt()
            .map(Option::unwrap_or_default)
    }

    fn into_tentative_err_def<X, WI, EI>(self) -> TentativeInner<Self::V, X, Self::E, WI, EI>
    where
        Self::V: Default,
        WI: ZeroOrMore,
        EI: ZeroOrMore + CanHoldOne,
        WI::Wrapper<X>: Default,
        EI::Wrapper<Self::E>: Default,
    {
        self.into_tentative_err_opt().map(Option::unwrap_or_default)
    }

    fn into_tentative_opt<WI, EI>(
        self,
        is_error: bool,
    ) -> TentativeInner<Option<Self::V>, Self::E, Self::E, WI, EI>
    where
        WI: ZeroOrMore + CanHoldOne,
        EI: ZeroOrMore + CanHoldOne,
        WI::Wrapper<Self::E>: Default,
        EI::Wrapper<Self::E>: Default;

    fn into_tentative_warn_opt<X, WI, EI>(
        self,
    ) -> TentativeInner<Option<Self::V>, Self::E, X, WI, EI>
    where
        WI: ZeroOrMore + CanHoldOne,
        EI: ZeroOrMore,
        WI::Wrapper<Self::E>: Default,
        EI::Wrapper<X>: Default;

    fn into_tentative_err_opt<X, WI, EI>(
        self,
    ) -> TentativeInner<Option<Self::V>, X, Self::E, WI, EI>
    where
        WI: ZeroOrMore,
        EI: ZeroOrMore + CanHoldOne,
        WI::Wrapper<X>: Default,
        EI::Wrapper<Self::E>: Default;

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
        EI1: OneOrMore + CanHoldOne,
        WI0::Wrapper<W>: Default,
        WI1::Wrapper<W>: Default,
        EI0::Wrapper<E1>: Default,
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
        WI::Wrapper<Self::E>: Default,
        EI::Wrapper<Self::E>: Default,
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
        WI::Wrapper<Self::E>: Default,
        EI::Wrapper<X>: Default,
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
        WI::Wrapper<X>: Default,
        EI::Wrapper<Self::E>: Default,
    {
        self.map_or_else(
            |e| TentativeInner::new(None, WI::Wrapper::<X>::default(), EI::wrap(e)),
            |v| TentativeInner::new1(Some(v)),
        )
    }

    fn terminate<T, W>(self, reason: T) -> TerminalResult<Self::V, W, Self::E, T> {
        self.map_err(|e| TerminalFailure::new1(NonEmpty::new(e), reason))
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
    type REI: OneOrMore;

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
    fn def_and_tentatively<F, X, LWI0, LEI0, LWIF, LEIF>(
        self,
        f: F,
    ) -> PassthruResultInner<X, Self::P, Self::W, Self::E, LWIF, LEIF, Self::RWI, Self::REI>
    where
        <Self::LWI as Container>::Wrapper<Self::W>:
            Appendable<LWI0::Wrapper<Self::W>, Out = LWIF::Wrapper<Self::W>>,
        <Self::LEI as Container>::Wrapper<Self::E>:
            Appendable<LEI0::Wrapper<Self::E>, Out = LEIF::Wrapper<Self::E>>,
        LWI0: ZeroOrMore,
        LEI0: ZeroOrMore,
        LWIF: ZeroOrMore,
        LEIF: ZeroOrMore,
        F: FnOnce(Self::V) -> TentativeInner<X, Self::W, Self::E, LWI0, LEI0>;

    fn def_eval_error<F, X>(&mut self, f: F)
    where
        X: Into<Self::E>,
        F: FnOnce(&Self::V) -> Option<X>,
        Self::LEI: CanHoldMany;

    fn def_eval_warning<F, X>(&mut self, f: F)
    where
        X: Into<Self::W>,
        F: FnOnce(&Self::V) -> Option<X>,
        Self::LWI: CanHoldMany;

    fn def_push_error(&mut self, e: impl Into<Self::E>)
    where
        Self::LEI: CanHoldMany,
        Self::REI: CanHoldMany;

    fn def_push_warning(&mut self, w: impl Into<Self::W>)
    where
        Self::LWI: CanHoldMany,
        Self::RWI: CanHoldMany;

    fn def_push_error_or_warning<X>(&mut self, x: X, is_error: bool)
    where
        X: Into<Self::W> + Into<Self::E>,
        Self::LWI: CanHoldMany,
        Self::LEI: CanHoldMany,
        Self::RWI: CanHoldMany,
        Self::REI: CanHoldMany,
    {
        if is_error {
            self.def_push_error(x);
        } else {
            self.def_push_warning(x);
        }
    }

    #[allow(clippy::type_complexity)]
    fn def_void_passthru(
        self,
    ) -> DeferredResultInner<Self::V, Self::W, Self::E, Self::LWI, Self::LEI, Self::RWI, Self::REI>;
}

impl<V, P, W, E, LWI, LEI, RWI, REI> PassthruExt
    for PassthruResultInner<V, P, W, E, LWI, LEI, RWI, REI>
where
    LWI: ZeroOrMore,
    LEI: ZeroOrMore,
    RWI: ZeroOrMore,
    REI: OneOrMore,
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

    fn def_and_tentatively<F, X, LWI0, LEI0, LWIF, LEIF>(
        self,
        f: F,
    ) -> PassthruResultInner<X, P, W, E, LWIF, LEIF, RWI, REI>
    where
        LWI::Wrapper<W>: Appendable<LWI0::Wrapper<W>, Out = LWIF::Wrapper<W>>,
        LEI::Wrapper<E>: Appendable<LEI0::Wrapper<E>, Out = LEIF::Wrapper<E>>,
        LWI0: ZeroOrMore,
        LEI0: ZeroOrMore,
        LWIF: ZeroOrMore,
        LEIF: ZeroOrMore,
        F: FnOnce(Self::V) -> TentativeInner<X, W, E, LWI0, LEI0>,
    {
        self.map(|x| x.and_tentatively_gen(f))
    }

    fn def_eval_error<F, X>(&mut self, f: F)
    where
        X: Into<Self::E>,
        F: FnOnce(&Self::V) -> Option<X>,
        LEI: CanHoldMany,
    {
        if let Ok(tnt) = self.as_mut() {
            tnt.eval_error(f);
        }
    }

    fn def_eval_warning<F, X>(&mut self, f: F)
    where
        X: Into<Self::W>,
        F: FnOnce(&Self::V) -> Option<X>,
        LWI: CanHoldMany,
    {
        if let Ok(tnt) = self.as_mut() {
            tnt.eval_warning(f);
        }
    }

    fn def_push_error(&mut self, e: impl Into<E>)
    where
        LEI: CanHoldMany,
        REI: CanHoldMany,
    {
        match self {
            Ok(tnt) => tnt.push_error(e),
            Err(f) => f.push_error(e),
        }
    }

    fn def_push_warning(&mut self, w: impl Into<W>)
    where
        LWI: CanHoldMany,
        RWI: CanHoldMany,
    {
        match self {
            Ok(tnt) => tnt.push_warning(w),
            Err(f) => f.push_warning(w),
        }
    }

    fn def_void_passthru(self) -> DeferredResultInner<V, W, E, LWI, LEI, RWI, REI> {
        self.map_err(DeferredFailureInner::void)
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

pub trait DeferredExt: Sized + PassthruExt {
    #[allow(clippy::type_complexity)]
    fn def_zip_gen<V1, LWI0, LEI0, RWI0, REI0, LWIF, LEIF, RWIF, REIF>(
        self,
        a: DeferredResultInner<V1, Self::W, Self::E, LWI0, LEI0, RWI0, REI0>,
    ) -> DeferredResultInner<(Self::V, V1), Self::W, Self::E, LWIF, LEIF, RWIF, REIF>
    where
        <Self::LWI as Container>::Wrapper<Self::W>: Appendable<LWI0::Wrapper<Self::W>, Out = LWIF::Wrapper<Self::W>>
            + Appendable<RWI0::Wrapper<Self::W>, Out = RWIF::Wrapper<Self::W>>,
        <Self::LEI as Container>::Wrapper<Self::E>: Appendable<LEI0::Wrapper<Self::E>, Out = LEIF::Wrapper<Self::E>>
            + Appendable<REI0::Wrapper<Self::E>, Out = REIF::Wrapper<Self::E>>,
        <Self::RWI as Container>::Wrapper<Self::W>: Appendable<LWI0::Wrapper<Self::W>, Out = RWIF::Wrapper<Self::W>>
            + Appendable<RWI0::Wrapper<Self::W>, Out = RWIF::Wrapper<Self::W>>,
        <Self::REI as Container>::Wrapper<Self::E>: Appendable<LEI0::Wrapper<Self::E>, Out = REIF::Wrapper<Self::E>>
            + Appendable<REI0::Wrapper<Self::E>, Out = REIF::Wrapper<Self::E>>,
        LWI0: ZeroOrMore,
        LEI0: ZeroOrMore,
        LWIF: ZeroOrMore,
        LEIF: ZeroOrMore,
        RWI0: ZeroOrMore,
        REI0: OneOrMore,
        RWIF: ZeroOrMore,
        REIF: OneOrMore;

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
        <Self::LWI as Container>::Wrapper<Self::W>: Appendable<
                <Self::LWI as Container>::Wrapper<Self::W>,
                Out = <Self::LWI as Container>::Wrapper<Self::W>,
            > + Appendable<
                <Self::RWI as Container>::Wrapper<Self::W>,
                Out = <Self::RWI as Container>::Wrapper<Self::W>,
            >,
        <Self::LEI as Container>::Wrapper<Self::E>: Appendable<
                <Self::LEI as Container>::Wrapper<Self::E>,
                Out = <Self::LEI as Container>::Wrapper<Self::E>,
            > + Appendable<
                <Self::REI as Container>::Wrapper<Self::E>,
                Out = <Self::REI as Container>::Wrapper<Self::E>,
            >,
        <Self::RWI as Container>::Wrapper<Self::W>: Appendable<
                <Self::LWI as Container>::Wrapper<Self::W>,
                Out = <Self::RWI as Container>::Wrapper<Self::W>,
            > + Appendable<
                <Self::RWI as Container>::Wrapper<Self::W>,
                Out = <Self::RWI as Container>::Wrapper<Self::W>,
            >,
        <Self::REI as Container>::Wrapper<Self::E>: Appendable<
                <Self::LEI as Container>::Wrapper<Self::E>,
                Out = <Self::REI as Container>::Wrapper<Self::E>,
            > + Appendable<
                <Self::REI as Container>::Wrapper<Self::E>,
                Out = <Self::REI as Container>::Wrapper<Self::E>,
            >,
    {
        self.def_zip_gen::<V1, Self::LWI, Self::LEI, Self::RWI, Self::REI, Self::LWI, Self::LEI, Self::RWI, Self::REI>(a)
    }

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
        <Self::LWI as Container>::Wrapper<Self::W>: Appendable<
                <Self::LWI as Container>::Wrapper<Self::W>,
                Out = <Self::LWI as Container>::Wrapper<Self::W>,
            > + Appendable<
                <Self::RWI as Container>::Wrapper<Self::W>,
                Out = <Self::RWI as Container>::Wrapper<Self::W>,
            >,
        <Self::LEI as Container>::Wrapper<Self::E>: Appendable<
                <Self::LEI as Container>::Wrapper<Self::E>,
                Out = <Self::LEI as Container>::Wrapper<Self::E>,
            > + Appendable<
                <Self::REI as Container>::Wrapper<Self::E>,
                Out = <Self::REI as Container>::Wrapper<Self::E>,
            >,
        <Self::RWI as Container>::Wrapper<Self::W>: Appendable<
                <Self::LWI as Container>::Wrapper<Self::W>,
                Out = <Self::RWI as Container>::Wrapper<Self::W>,
            > + Appendable<
                <Self::RWI as Container>::Wrapper<Self::W>,
                Out = <Self::RWI as Container>::Wrapper<Self::W>,
            >,
        <Self::REI as Container>::Wrapper<Self::E>: Appendable<
                <Self::LEI as Container>::Wrapper<Self::E>,
                Out = <Self::REI as Container>::Wrapper<Self::E>,
            > + Appendable<
                <Self::REI as Container>::Wrapper<Self::E>,
                Out = <Self::REI as Container>::Wrapper<Self::E>,
            >,
    {
        self.def_zip(a)
            .def_zip(b)
            .def_map_value(|((x, y), z)| (x, y, z))
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
        <Self::LWI as Container>::Wrapper<Self::W>: Appendable<
                <Self::LWI as Container>::Wrapper<Self::W>,
                Out = <Self::LWI as Container>::Wrapper<Self::W>,
            > + Appendable<
                <Self::RWI as Container>::Wrapper<Self::W>,
                Out = <Self::RWI as Container>::Wrapper<Self::W>,
            >,
        <Self::LEI as Container>::Wrapper<Self::E>: Appendable<
                <Self::LEI as Container>::Wrapper<Self::E>,
                Out = <Self::LEI as Container>::Wrapper<Self::E>,
            > + Appendable<
                <Self::REI as Container>::Wrapper<Self::E>,
                Out = <Self::REI as Container>::Wrapper<Self::E>,
            >;

    // TODO the result can be generalized into the above
    #[allow(clippy::type_complexity)]
    fn def_and_then<F, X>(
        self,
        f: F,
    ) -> DeferredResultInner<X, Self::W, Self::E, Self::LWI, Self::LEI, Self::RWI, Self::REI>
    where
        F: FnOnce(Self::V) -> Result<X, Self::E>,
        <Self::LWI as Container>::Wrapper<Self::W>: Appendable<
                <Self::LWI as Container>::Wrapper<Self::W>,
                Out = <Self::LWI as Container>::Wrapper<Self::W>,
            > + Appendable<
                <Self::RWI as Container>::Wrapper<Self::W>,
                Out = <Self::RWI as Container>::Wrapper<Self::W>,
            > + Default,
        <Self::LEI as Container>::Wrapper<Self::E>: Appendable<
                <Self::LEI as Container>::Wrapper<Self::E>,
                Out = <Self::LEI as Container>::Wrapper<Self::E>,
            > + Appendable<
                <Self::REI as Container>::Wrapper<Self::E>,
                Out = <Self::REI as Container>::Wrapper<Self::E>,
            > + Default,
        <Self::RWI as Container>::Wrapper<Self::W>: Default,
        Self::REI: CanHoldOne,
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

impl<V, W, E, LWI, LEI, RWI, REI> DeferredExt for DeferredResultInner<V, W, E, LWI, LEI, RWI, REI>
where
    LWI: ZeroOrMore,
    LEI: ZeroOrMore,
    RWI: ZeroOrMore,
    REI: OneOrMore,
{
    fn def_zip_gen<V1, LWI0, LEI0, RWI0, REI0, LWIF, LEIF, RWIF, REIF>(
        self,
        a: DeferredResultInner<V1, W, E, LWI0, LEI0, RWI0, REI0>,
    ) -> DeferredResultInner<(V, V1), W, E, LWIF, LEIF, RWIF, REIF>
    where
        LWI::Wrapper<W>: Appendable<LWI0::Wrapper<W>, Out = LWIF::Wrapper<W>>
            + Appendable<RWI0::Wrapper<W>, Out = RWIF::Wrapper<W>>,
        LEI::Wrapper<E>: Appendable<LEI0::Wrapper<E>, Out = LEIF::Wrapper<E>>
            + Appendable<REI0::Wrapper<E>, Out = REIF::Wrapper<E>>,
        RWI::Wrapper<W>: Appendable<LWI0::Wrapper<W>, Out = RWIF::Wrapper<W>>
            + Appendable<RWI0::Wrapper<W>, Out = RWIF::Wrapper<W>>,
        REI::Wrapper<E>: Appendable<LEI0::Wrapper<E>, Out = REIF::Wrapper<E>>
            + Appendable<REI0::Wrapper<E>, Out = REIF::Wrapper<E>>,
        LWI0: ZeroOrMore,
        LEI0: ZeroOrMore,
        LWIF: ZeroOrMore,
        LEIF: ZeroOrMore,
        RWI0: ZeroOrMore,
        REI0: OneOrMore,
        RWIF: ZeroOrMore,
        REIF: OneOrMore,
    {
        match (self, a) {
            (Ok(ax), Ok(bx)) => Ok(ax.mappend(bx, |x, y| (x, y))),
            (Ok(ax), Err(bx)) => Err(ax.and_fail(|_| bx)),
            (Err(ax), Ok(bx)) => Err(ax.and_tentatively(|| bx)),
            (Err(ax), Err(bx)) => Err(ax.mappend(bx, |(), ()| ())),
        }
    }

    fn def_and_maybe<F, X>(
        self,
        f: F,
    ) -> DeferredResultInner<X, W, E, Self::LWI, Self::LEI, Self::RWI, Self::REI>
    where
        F: FnOnce(V) -> DeferredResultInner<X, W, E, Self::LWI, Self::LEI, Self::RWI, Self::REI>,
        LWI::Wrapper<Self::W>: Appendable<LWI::Wrapper<Self::W>, Out = LWI::Wrapper<Self::W>>
            + Appendable<RWI::Wrapper<Self::W>, Out = RWI::Wrapper<Self::W>>,
        LEI::Wrapper<Self::E>: Appendable<LEI::Wrapper<Self::E>, Out = LEI::Wrapper<Self::E>>
            + Appendable<REI::Wrapper<Self::E>, Out = REI::Wrapper<Self::E>>,
    {
        let x = self?;
        x.and_maybe_gen(f)
    }
}

pub trait TerminalExt: Sized + DeferredExt {
    #[allow(clippy::type_complexity)]
    fn def_terminate<T>(
        self,
        reason: T,
    ) -> TerminalResultInner<Self::V, Self::W, Self::E, T, Self::RWI, Self::RWI, Self::REI>
    where
        Self::LEI: IntoOneOrMore<Self::REI>;

    #[allow(clippy::type_complexity)]
    fn def_terminate_def<T: Default>(
        self,
    ) -> TerminalResultInner<Self::V, Self::W, Self::E, T, Self::RWI, Self::RWI, Self::REI>
    where
        Self::LEI: IntoOneOrMore<Self::REI>,
    {
        self.def_terminate(T::default())
    }

    #[allow(clippy::type_complexity)]
    fn def_terminate_warn2err<T, F>(
        self,
        reason: T,
        f: F,
    ) -> TerminalResultInner<Self::V, Self::W, Self::E, T, Self::RWI, Self::RWI, Self::REI>
    where
        F: Fn(Self::W) -> Self::E,
        <Self::LWI as Container>::Wrapper<Self::W>: Default,
        Self::LWI: IntoOneOrMore<Self::REI>,
        Self::LEI: IntoOneOrMore<Self::REI>,
        Self::REI: CanHoldMany;

    #[allow(clippy::type_complexity)]
    fn def_terminate_warn2err_def<T: Default, F>(
        self,
        f: F,
    ) -> TerminalResultInner<Self::V, Self::W, Self::E, T, Self::RWI, Self::RWI, Self::REI>
    where
        F: Fn(Self::W) -> Self::E,
        <Self::LWI as Container>::Wrapper<Self::W>: Default,
        Self::LWI: IntoOneOrMore<Self::REI>,
        Self::LEI: IntoOneOrMore<Self::REI>,
        Self::REI: CanHoldMany,
    {
        self.def_terminate_warn2err(T::default(), f)
    }

    #[allow(clippy::type_complexity)]
    fn def_terminate_nowarn<T>(
        self,
        reason: T,
    ) -> TerminalResultInner<Self::V, Self::W, Self::E, T, Self::RWI, Self::RWI, Self::REI>
    where
        <Self::LWI as Container>::Wrapper<Self::W>: Default,
        Self::LEI: IntoOneOrMore<Self::REI>,
        Self::REI: OneOrMore + CanHoldMany;

    #[allow(clippy::type_complexity)]
    fn def_terminate_maybe_warn<T, F>(
        self,
        reason: T,
        conf: &SharedConfig,
        f: F,
    ) -> TerminalResultInner<Self::V, Self::W, Self::E, T, Self::RWI, Self::RWI, Self::REI>
    where
        F: Fn(Self::W) -> Self::E,
        <Self::LWI as Container>::Wrapper<Self::W>: Default,
        Self::LEI: IntoOneOrMore<Self::REI>,
        Self::LWI: IntoOneOrMore<Self::REI>,
        Self::REI: OneOrMore + CanHoldMany,
    {
        if conf.warnings_are_errors {
            self.def_terminate_warn2err(reason, f)
        } else if conf.hide_warnings {
            self.def_terminate_nowarn(reason)
        } else {
            self.def_terminate(reason)
        }
    }

    #[allow(clippy::type_complexity)]
    fn def_terminate_maybe_warn_def<T: Default, F>(
        self,
        conf: &SharedConfig,
        f: F,
    ) -> TerminalResultInner<Self::V, Self::W, Self::E, T, Self::RWI, Self::RWI, Self::REI>
    where
        F: Fn(Self::W) -> Self::E,
        <Self::LWI as Container>::Wrapper<Self::W>: Default,
        Self::LEI: IntoOneOrMore<Self::REI>,
        Self::LWI: IntoOneOrMore<Self::REI>,
        Self::REI: OneOrMore + CanHoldMany,
    {
        self.def_terminate_maybe_warn(T::default(), conf, f)
    }
}

impl<V, W, E, LWI, LEI, REI> TerminalExt for DeferredResultInner<V, W, E, LWI, LEI, LWI, REI>
where
    LWI: ZeroOrMore,
    LEI: ZeroOrMore,
    REI: OneOrMore,
{
    fn def_terminate<T>(
        self,
        reason: T,
    ) -> Result<
        TerminalInner<Self::V, Self::W, Self::LWI>,
        TerminalFailureInner<Self::W, Self::E, T, Self::RWI, Self::REI>,
    >
    where
        LEI: IntoOneOrMore<REI>,
    {
        match self {
            Ok(t) => t.terminate(reason),
            Err(e) => Err(e.terminate(reason)),
        }
    }

    fn def_terminate_warn2err<T, F>(
        self,
        reason: T,
        f: F,
    ) -> TerminalResultInner<Self::V, Self::W, Self::E, T, Self::RWI, Self::RWI, Self::REI>
    where
        F: Fn(W) -> E,
        LWI::Wrapper<W>: Default,
        LWI: IntoOneOrMore<REI>,
        LEI: IntoOneOrMore<REI>,
        REI: OneOrMore + CanHoldMany,
    {
        match self {
            Ok(t) => t.terminate_warn2err(reason, f),
            Err(e) => Err(e.terminate_warn2err(reason, f)),
        }
    }

    fn def_terminate_nowarn<T>(
        self,
        reason: T,
    ) -> TerminalResultInner<Self::V, Self::W, Self::E, T, Self::LWI, Self::RWI, Self::REI>
    where
        LWI::Wrapper<W>: Default,
        LEI: IntoOneOrMore<REI>,
        REI: OneOrMore + CanHoldMany,
    {
        match self {
            Ok(t) => t.terminate_nowarn(reason),
            Err(e) => Err(TerminalFailureInner::new1(*e.errors, reason)),
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
        Self::LWI: CanHoldMany,
        Self::LEI: CanHoldMany,
        Self::RWI: CanHoldMany,
        Self::REI: CanHoldMany,
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
    WI::Wrapper<W>: Default,
    EI: OneOrMore + CanHoldOne,
{
    fn from(value: E) -> Self {
        Self::new1(value)
    }
}

impl<W, E, WI> From<NonEmpty<E>> for DeferredFailureInner<(), W, E, WI, NonEmptyFamily>
where
    WI: ZeroOrMore,
    WI::Wrapper<W>: Default,
{
    fn from(value: NonEmpty<E>) -> Self {
        Self::new(WI::Wrapper::<W>::default(), value.into(), ())
    }
}

impl<W, E, WI, EI> From<io::Error> for DeferredFailureInner<(), W, ImpureError<E>, WI, EI>
where
    WI: ZeroOrMore,
    WI::Wrapper<W>: Default,
    EI: OneOrMore + CanHoldOne,
{
    fn from(value: io::Error) -> Self {
        Self::new1(value)
    }
}

impl<W, E, WI, EI, T> From<E> for TerminalFailureInner<W, E, T, WI, EI>
where
    WI: ZeroOrMore,
    WI::Wrapper<W>: Default,
    EI: OneOrMore + CanHoldOne,
    T: Default,
{
    fn from(value: E) -> Self {
        Self::new1(EI::wrap(value), T::default())
    }
}

impl<W, E, WI, T> From<NonEmpty<E>> for TerminalFailureInner<W, E, T, WI, NonEmptyFamily>
where
    WI: ZeroOrMore,
    WI::Wrapper<W>: Default,
    T: Default,
{
    fn from(value: NonEmpty<E>) -> Self {
        Self::new1(value, T::default())
    }
}

impl<W, E, WI, EI, T> From<io::Error> for TerminalFailureInner<W, ImpureError<E>, T, WI, EI>
where
    WI: ZeroOrMore,
    WI::Wrapper<W>: Default,
    EI: OneOrMore + CanHoldOne,
    T: Default,
{
    fn from(value: io::Error) -> Self {
        Self::new1(EI::wrap(ImpureError::from(value)), T::default())
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
