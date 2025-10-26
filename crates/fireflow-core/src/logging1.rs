#![allow(clippy::type_complexity)]

use crate::config::SharedConfig;
use crate::text::optional::NeverValue;

use derive_new::new;
use std::convert::Infallible;
use std::fmt;
use std::io;
use std::iter;
use std::marker::PhantomData;
use std::vec;
use thiserror::Error;

pub type WarningsAndIOSummaryResult<V, W, E, S> = WarningsAndSummaryResult<V, W, ImpureError<E>, S>;

pub type WarningsAndSummaryResult<V, W, E, S> =
    WarningsAndErrorResult<V, (), W, ErrorSummary<E, S>>;

pub type SummaryResult<V, E, S> = ErrorResult<V, (), ErrorSummary<E, S>>;

pub type IOSummaryResult<V, E, S> = SummaryResult<V, ImpureError<E>, S>;

// TODO maybe add wrapper type for Success which roughly means "result with
// issues that may be errors"

pub type RecoverableErrorResult<V, I> = ErrorResult<V, (), I>;
pub type RecoverableErrorsResult<V, I> = ErrorsResult<V, (), I>;

pub type ErrorResult<V, P, E> = NowarnResult<V, P, E, NullFamily>;
pub type ErrorsResult<V, P, E> = NowarnResult<V, P, E, VecFamily>;

pub type IOErrorResult<V, P, E> = ErrorResult<V, P, ImpureError<E>>;
pub type IOErrorsResult<V, P, E> = ErrorsResult<V, P, ImpureError<E>>;

pub type FungibleErrorResult<V, P, E> = NonCmtFungibleResult<V, P, E, NeverValue<E>>;
pub type FungibleErrorsResult<V, P, E> = NonCmtFungibleResult<V, P, E, Vec<E>>;

pub type WarningOrErrorResult<V, P, W, E> = NonCmtResult<V, P, E, Option<W>, NeverValue<E>>;
pub type WarningsOrErrorResult<V, P, W, E> = NonCmtResult<V, P, E, Vec<W>, NeverValue<E>>;
pub type WarningOrErrorsResult<V, P, W, E> = NonCmtResult<V, P, E, Option<W>, Vec<E>>;
pub type WarningsOrErrorsResult<V, P, W, E> = NonCmtResult<V, P, E, Vec<W>, Vec<E>>;

pub type WarningAndErrorResult<V, P, W, E> = CmtResult<V, P, E, Option<W>, NeverValue<E>>;
pub type WarningsAndErrorResult<V, P, W, E> = CmtResult<V, P, E, Vec<W>, NeverValue<E>>;
pub type WarningAndErrorsResult<V, P, W, E> = CmtResult<V, P, E, Option<W>, Vec<E>>;
pub type WarningsAndErrorsResult<V, P, W, E> = CmtResult<V, P, E, Vec<W>, Vec<E>>;

pub type IOWarningAndErrorResult<V, P, W, E> = WarningAndErrorResult<V, P, W, ImpureError<E>>;
pub type IOWarningsAndErrorResult<V, P, W, E> = WarningsAndErrorResult<V, P, W, ImpureError<E>>;
pub type IOWarningAndErrorsResult<V, P, W, E> = WarningAndErrorsResult<V, P, W, ImpureError<E>>;
pub type IOWarningsAndErrorsResult<V, P, W, E> = WarningsAndErrorsResult<V, P, W, ImpureError<E>>;

pub type CmtFungibleErrorResult<V, P, E> = WarningAndErrorResult<V, P, E, E>;
pub type CmtFungibleErrorsResult<V, P, E> = WarningsAndErrorsResult<V, P, E, E>;

//

pub type DeferredError<V, E> = NowarnResult<V, V, E, NullFamily>;
pub type DeferredErrors<V, E> = NowarnResult<V, V, E, VecFamily>;

pub type DeferredIOError<V, E> = DeferredError<V, ImpureError<E>>;
pub type DeferredIOErrors<V, E> = DeferredErrors<V, ImpureError<E>>;

pub type DeferredWarningAndError<V, W, E> = WarningAndErrorResult<V, V, W, E>;
pub type DeferredWarningsAndError<V, W, E> = WarningsAndErrorResult<V, V, W, E>;
pub type DeferredWarningAndErrors<V, W, E> = WarningAndErrorsResult<V, V, W, E>;
pub type DeferredWarningsAndErrors<V, W, E> = WarningsAndErrorsResult<V, V, W, E>;

pub type DeferredFungibleError<V, E> = CmtFungibleErrorResult<V, V, E>;
pub type DeferredFungibleErrors<V, E> = CmtFungibleErrorsResult<V, V, E>;

//

pub type NonCmtFungibleResult<V, P, E, EC> =
    NonCmtResult<V, P, E, <EC as FungibleError<E>>::Warn, EC>;
pub type CmtFungibleResult<V, P, E, EC> = CmtResult<V, P, E, <EC as FungibleError<E>>::Warn, EC>;

pub type NowarnResult<V, P, E, EC> = CmtResult<V, P, E, NeverValue<()>, EC>;

pub type Deferred<V, E, WC, EC> = CmtResult<V, V, E, WC, EC>;

pub type DeferredNowarn<V, E, EC> = NowarnResult<V, V, E, EC>;

pub type DeferredFungible<V, E, EC> = Deferred<V, E, <EC as FungibleError<E>>::Warn, EC>;

pub type CmtResult<V, P, E, WC, EC> = LogResult<V, P, E, WC, WC, EC>;

pub type NonCmtResult<V, P, E, WC, EC> = LogResult<V, P, E, WC, NeverValue<()>, EC>;

pub type FungibleResult<V, P, E, RWC, EC> =
    LogResult<V, P, E, <EC as FungibleError<E>>::Warn, RWC, EC>;

pub type LogResult<V, P, E, LWC, RWC, EF> = Result<Success<V, LWC>, Failure<P, E, RWC, EF>>;

// pub struct Tentative<V, P, I, IC: ZeroOrMore>(NowarnResult<V, P, I, IC>);

#[derive(new)]
// #[new(visibility = "")]
pub struct Success<V, WC> {
    value: V,
    warnings: WC,
}

#[derive(new)]
pub struct Failure<P, E, WC, EC> {
    warnings: WC,
    errors: GenNonEmpty<E, EC>,
    value: P,
}

#[derive(new)]
pub struct ErrorSummary<E, S> {
    pub summary: S,
    pub errors: GenNonEmpty<E, Vec<E>>,
}

// TODO eventually will want to make a boxable version of this, which will
// likely involve a higher-kinded type which is either a box or dumb newtype
// wrapper. tail doesn't need box because the only two values (for now) are
// vec and nevervalue, and the former is heap anyways.
#[derive(new)]
pub struct GenNonEmpty<X, C> {
    head: X,
    tail: C,
}

#[derive(Debug, Error)]
pub enum ImpureError<E> {
    #[error("IO error: {0}")]
    IO(#[from] io::Error),
    #[error("{0}")]
    Pure(E),
}

// /// Named unit type to use as the Err value at the top of the call stack.
// ///
// /// This is to prevent ambiguous return types like `ErrorsResult<(), (), E>`
// /// which technically is a deferred result and thus the Err side could be
// /// accidentally executed.
// #[derive(Default)]
// pub struct Term;

pub struct OptFamily;

pub struct VecFamily;

pub struct NullFamily;

pub trait Kind1 {
    type Inner<X>;
}

pub trait InFamily {
    type Family;
}

pub trait Functor: Sized + Kind1 {
    fn fmap<F, X, Y>(t: Self::Inner<X>, f: F) -> Self::Inner<Y>
    where
        F: Fn(X) -> Y;
}

pub trait Pure: Functor {
    fn wrap<X>(x: X) -> Self::Inner<X>;
}

pub trait Concatable {
    type Out;
    fn concat(self, other: Self) -> Self::Out;
}

pub trait Semigroup: Concatable<Out = Self> {}

pub trait FungibleError<E>: Sized {
    type Warn;

    fn errors_to_warnings(errors: GenNonEmpty<E, Self>) -> Self::Warn;

    // reinvention of Pure without the need to use a type family which will
    // probably be simpler for most cases where this is used
    fn error_to_warning(error: E) -> Self::Warn;
}

// pub trait FungibleErrorFamily: Kind1 {
//     type WarnFam: Kind1;

//     fn errors_to_warnings<E>(
//         errors: GenNonEmpty<E, Self::Inner<E>>,
//     ) -> <Self::WarnFam as Kind1>::Inner<E>
//     where
//         Self::WarnFam: Kind1<Inner<E> = <Self::Inner<E> as FungibleError>::Warn>,
//         Self::Inner<E>: FungibleError;
// }

pub trait IntoZeroOrMore<Other> {
    fn into_zero_or_more(self) -> Other;
}

impl Kind1 for NullFamily {
    type Inner<T> = NeverValue<T>;
}

impl Kind1 for OptFamily {
    type Inner<T> = Option<T>;
}

impl Kind1 for VecFamily {
    type Inner<T> = Vec<T>;
}

impl<T> InFamily for NeverValue<T> {
    type Family = NullFamily;
}

impl<T> InFamily for Vec<T> {
    type Family = VecFamily;
}

impl<T> InFamily for Option<T> {
    type Family = OptFamily;
}

impl Functor for NullFamily {
    fn fmap<F, X, Y>(_: Self::Inner<X>, _: F) -> Self::Inner<Y>
    where
        F: Fn(X) -> Y,
    {
        NeverValue(PhantomData)
    }
}

impl Functor for OptFamily {
    fn fmap<F, X, Y>(t: Self::Inner<X>, f: F) -> Self::Inner<Y>
    where
        F: Fn(X) -> Y,
    {
        t.map(f)
    }
}

impl Functor for VecFamily {
    fn fmap<F, X, Y>(t: Self::Inner<X>, f: F) -> Self::Inner<Y>
    where
        F: Fn(X) -> Y,
    {
        t.into_iter().map(f).collect()
    }
}

impl<E, EC: IntoIterator<Item = E>> IntoIterator for GenNonEmpty<E, EC> {
    type Item = E;
    type IntoIter = iter::Chain<iter::Once<E>, <EC as IntoIterator>::IntoIter>;
    fn into_iter(self) -> Self::IntoIter {
        iter::once(self.head).chain(self.tail)
    }
}

impl<T> IntoZeroOrMore<T> for T {
    fn into_zero_or_more(self) -> T {
        self
    }
}

impl<T> IntoZeroOrMore<Vec<T>> for NeverValue<T> {
    fn into_zero_or_more(self) -> Vec<T> {
        vec![]
    }
}

impl<T> IntoZeroOrMore<Option<T>> for NeverValue<T> {
    fn into_zero_or_more(self) -> Option<T> {
        None
    }
}

impl<T> IntoZeroOrMore<Vec<T>> for Option<T> {
    fn into_zero_or_more(self) -> Vec<T> {
        self.into_iter().collect()
    }
}

impl Pure for OptFamily {
    fn wrap<X>(x: X) -> Self::Inner<X> {
        Some(x)
    }
}

impl Pure for VecFamily {
    fn wrap<X>(x: X) -> Self::Inner<X> {
        vec![x]
    }
}

impl<T> Concatable for NeverValue<T> {
    type Out = Self;
    fn concat(self, _: Self) -> Self::Out {
        self
    }
}

impl<T> Concatable for Option<T> {
    type Out = Vec<T>;
    fn concat(self, other: Self) -> Self::Out {
        self.into_iter().chain(other).collect()
    }
}

impl<T> Concatable for Vec<T> {
    type Out = Self;
    fn concat(mut self, other: Self) -> Self::Out {
        self.extend(other);
        self
    }
}

impl<T> Semigroup for Vec<T> {}

impl<T> Semigroup for NeverValue<T> {}

impl<E> FungibleError<E> for NeverValue<E> {
    type Warn = Option<E>;

    fn errors_to_warnings(errors: GenNonEmpty<E, Self>) -> Self::Warn {
        Some(errors.head)
    }

    fn error_to_warning(error: E) -> Self::Warn {
        Some(error)
    }
}

impl<E> FungibleError<E> for Vec<E> {
    type Warn = Self;

    fn errors_to_warnings(errors: GenNonEmpty<E, Self>) -> Self::Warn {
        errors.into_iter().collect()
    }

    fn error_to_warning(error: E) -> Self::Warn {
        vec![error]
    }
}

// impl FungibleErrorFamily for VecFamily {
//     fn errors_to_warnings<E>(errors: GenNonEmpty<E, Self::Inner<E>>) -> Vec<E> {
//         errors.into_iter().collect()
//     }
// }

// impl FungibleErrorFamily for NullFamily {
//     fn errors_to_warnings<E>(errors: GenNonEmpty<E, Self::Inner<E>>) -> Option<E> {
//         Some(errors.head)
//     }
// }

// impl<V, I, IC: ZeroOrMore> Tentative<V, I, IC> {
//     pub(crate) fn into_ok<RW, RWC>(self, default: V) -> FungibleResult<V, (), RW, I, RWC, IC>
//     where
//         RWC: ZeroOrMore,
//     {
//         let ret = match self.0 {
//             Ok(s) => Success::new(s.value, s.warnings),
//             Err(f) => Success::new(default, f.errors),
//         };
//         Ok(ret)
//     }
// }

impl<V, WC> Success<V, WC> {
    pub(crate) fn new1(value: V) -> Self
    where
        WC: Default,
    {
        Self::new(value, WC::default())
    }

    pub(crate) fn repack<WCf>(self) -> Success<V, WCf>
    where
        WC: IntoZeroOrMore<WCf>,
    {
        Success::new(self.value, self.warnings.into_zero_or_more())
    }

    pub(crate) fn map_value<F: FnOnce(V) -> X, X>(self, f: F) -> Success<X, WC> {
        Success::new(f(self.value), self.warnings)
    }

    pub(crate) fn map_warnings<F, W, Wf>(self, f: F) -> Success<V, <WC::Family as Kind1>::Inner<Wf>>
    where
        WC::Family: Functor<Inner<W> = WC>,
        WC: InFamily,
        F: Fn(W) -> Wf,
    {
        Success::new(self.value, WC::Family::fmap(self.warnings, f))
    }

    pub(crate) fn set_warnings<WCf>(self, ws: WCf) -> Success<V, WCf> {
        Success::new(self.value, ws)
    }

    pub(crate) fn push_warning<W>(&mut self, w: W)
    where
        WC: Extend<W>,
    {
        self.warnings.extend(iter::once(w));
    }

    pub(crate) fn extend_warnings<W>(&mut self, ws: impl IntoIterator<Item = W>)
    where
        WC: Extend<W>,
    {
        self.warnings.extend(ws);
    }

    pub(crate) fn eval_warning<F, W>(&mut self, f: F)
    where
        F: FnOnce(&V) -> Option<W>,
        WC: Extend<W>,
    {
        if let Some(e) = f(&self.value) {
            self.push_warning(e);
        }
    }

    // pub(crate) fn and_then<F, Vf>(self, f: F) -> Success<Vf, W, WC>
    // where
    //     F: FnOnce(V) -> Success<Vf, W, WC>,
    //     WC::Inner<W>: Semigroup,
    // {
    //     let other = f(self.value);
    //     Success::new(other.value, self.warnings.concat(other.warnings))
    // }

    pub(crate) fn and_maybe<F, ToV, P, E, WCf, EC>(self, f: F) -> CmtResult<ToV, P, E, WCf, EC>
    where
        F: FnOnce(V) -> CmtResult<ToV, P, E, WC, EC>,
        WC: Concatable<Out = WCf>,
    {
        match f(self.value) {
            Ok(s) => {
                let ws = self.warnings.concat(s.warnings);
                Ok(Success::new(s.value, ws))
            }
            Err(e) => {
                let ws = self.warnings.concat(e.warnings);
                Err(Failure::new(ws, e.errors, e.value))
            }
        }
    }

    pub(crate) fn fail<E, EC>(self, errors: GenNonEmpty<E, EC>) -> Failure<V, E, WC, EC> {
        Failure::new(self.warnings, errors, self.value)
    }

    pub(crate) fn with_failure<F, P, PF, E, WCf, EC>(
        self,
        other: Failure<P, E, WC, EC>,
        f: F,
    ) -> Failure<PF, E, WCf, EC>
    where
        F: FnOnce(V, P) -> PF,
        WC: Concatable<Out = WCf>,
    {
        let ws = self.warnings.concat(other.warnings);
        Failure::new(ws, other.errors, f(self.value, other.value))
    }

    pub(crate) fn zip_with<F, V0, Vf, WCf>(self, other: Success<V0, WC>, f: F) -> Success<Vf, WCf>
    where
        F: FnOnce(V, V0) -> Vf,
        WC: Concatable<Out = WCf>,
    {
        let ws = self.warnings.concat(other.warnings);
        Success::new(f(self.value, other.value), ws)
    }

    fn aggregate_warnings<F, Wf>(self, f: F) -> Success<V, Option<Wf>>
    where
        F: FnOnce(WC) -> Wf,
    {
        Success::new(self.value, Some(f(self.warnings)))
    }

    /// Remove warnings while maintaining the warning type.
    ///
    /// This is useful for cases where warnings might be optionally removed
    /// so we can't just set them to `()`
    fn remove_warnings(self) -> Self
    where
        WC: Default,
    {
        Self::new1(self.value)
    }

    /// Convert warnings to errors while maintaining the warning type.
    ///
    /// This is useful for cases where warnings might be optionally converted
    /// so we can't just set them to `()`
    fn warnings_to_errors<W, E, F, G, EC, P>(self, f: F, g: G) -> LogResult<V, P, E, WC, WC, EC>
    where
        F: Fn(W) -> E,
        G: FnOnce(V) -> P,
        EC: Extend<E> + Default,
        WC: Default + IntoIterator<Item = W>,
    {
        match GenNonEmpty::<E, EC>::collect(self.warnings.into_iter().map(f)) {
            None => Ok(Self::new1(self.value)),
            Some(es) => Err(Failure::new_from_many(es, g(self.value))),
        }
    }

    fn resolve<F, X>(self, f: F) -> (V, X)
    where
        F: FnOnce(WC) -> X,
    {
        (self.value, f(self.warnings))
    }
}

impl<V> Success<V, NeverValue<()>> {
    fn nowarn_into_warn<WC: Default>(self) -> Success<V, WC> {
        Success::new1(self.value)
    }
}

impl<P, E, WC, EC> Failure<P, E, WC, EC> {
    pub(crate) fn new_from_one(error: E, value: P) -> Self
    where
        EC: Default,
        WC: Default,
    {
        Self::new_from_many(GenNonEmpty::new1(error), value)
    }

    pub(crate) fn new_from_many(errors: GenNonEmpty<E, EC>, value: P) -> Self
    where
        WC: Default,
    {
        Self::new(WC::default(), errors, value)
    }

    fn repack_warnings<WCf>(self) -> Failure<P, E, WCf, EC>
    where
        WC: IntoZeroOrMore<WCf>,
    {
        Failure::new(
            WC::into_zero_or_more(self.warnings),
            self.errors,
            self.value,
        )
    }

    fn repack_errors<ECf>(self) -> Failure<P, E, WC, ECf>
    where
        EC: IntoZeroOrMore<ECf>,
    {
        Failure::new(self.warnings, self.errors.repack(), self.value)
    }

    fn map_warnings<F, W, Wf>(self, f: F) -> Failure<P, E, <WC::Family as Kind1>::Inner<Wf>, EC>
    where
        F: Fn(W) -> Wf,
        WC: InFamily,
        WC::Family: Functor<Inner<W> = WC>,
    {
        Failure::new(WC::Family::fmap(self.warnings, f), self.errors, self.value)
    }

    fn map_errors<F, Ef>(self, f: F) -> Failure<P, Ef, WC, <EC::Family as Kind1>::Inner<Ef>>
    where
        F: Fn(E) -> Ef,
        EC: InFamily,
        EC::Family: Functor<Inner<E> = EC>,
    {
        Failure::new(self.warnings, self.errors.map(f), self.value)
    }

    fn map_value<F, Pf>(self, f: F) -> Failure<Pf, E, WC, EC>
    where
        F: FnOnce(P) -> Pf,
    {
        Failure::new(self.warnings, self.errors, f(self.value))
    }

    fn set_warnings<WCf>(self, ws: WCf) -> Failure<P, E, WCf, EC> {
        Failure::new(ws, self.errors, self.value)
    }

    fn push_warning<W>(&mut self, w: W)
    where
        WC: Extend<W>,
    {
        self.warnings.extend(iter::once(w));
    }

    fn eval_warning<F, W>(&mut self, f: F)
    where
        F: FnOnce(&P) -> Option<W>,
        WC: Extend<W>,
    {
        if let Some(e) = f(&self.value) {
            self.push_warning(e);
        }
    }

    fn extend_warnings<W>(&mut self, ws: impl IntoIterator<Item = W>)
    where
        WC: Extend<W>,
    {
        self.warnings.extend(ws);
    }

    fn push_error(&mut self, e: E)
    where
        EC: Extend<E>,
    {
        self.errors.extend(iter::once(e));
    }

    fn extend_errors(&mut self, es: impl IntoIterator<Item = E>)
    where
        EC: Extend<E>,
    {
        self.errors.extend(es);
    }

    // fn eval_error<F>(&mut self, f: F)
    // where
    //     F: FnOnce(&P) -> Option<E>,
    //     EC::Inner<E>: Extend<E>,
    // {
    //     if let Some(e) = f(&self.value) {
    //         self.push_error(e);
    //     }
    // }

    fn with_value<F, V, Pf, WCf, EC1>(mut self, f: F) -> CmtResult<V, Pf, E, WCf, EC>
    where
        F: FnOnce(P) -> CmtResult<V, Pf, E, WC, EC1>,
        WC: Concatable<Out = WCf>,
        EC: Extend<E> + IntoIterator<Item = E>,
        EC1: IntoIterator<Item = E>,
    {
        match f(self.value) {
            Ok(s) => {
                let ws = self.warnings.concat(s.warnings);
                Ok(Success::new(s.value, ws))
            }
            Err(e) => {
                let ws = self.warnings.concat(e.warnings);
                self.errors.extend(e.errors);
                Err(Failure::new(ws, self.errors, e.value))
            }
        }
    }

    fn zip_with<F, P0, Pf, WCf, ECf>(
        self,
        other: Failure<P0, E, WC, EC>,
        f: F,
    ) -> Failure<Pf, E, WCf, ECf>
    where
        F: FnOnce(P, P0) -> Pf,
        EC: IntoZeroOrMore<ECf> + IntoIterator<Item = E>,
        WC: Concatable<Out = WCf>,
        ECf: Extend<E>,
    {
        let ws = self.warnings.concat(other.warnings);
        let mut es = self.errors.repack();
        es.extend(other.errors);
        Failure::new(ws, es, f(self.value, other.value))
    }

    fn aggregate_warnings<F, Wf>(self, f: F) -> Failure<P, E, Option<Wf>, EC>
    where
        F: FnOnce(WC) -> Wf,
    {
        Failure::new(Some(f(self.warnings)), self.errors, self.value)
    }

    fn aggregate_errors<F, Ef>(self, f: F) -> Failure<P, Ef, WC, NeverValue<Ef>>
    where
        F: FnOnce(GenNonEmpty<E, EC>) -> Ef,
    {
        let es = GenNonEmpty::new1(f(self.errors));
        Failure::new(self.warnings, es, self.value)
    }

    // fn summarize_errors<S>(self, summary: S) -> Failure<P, W, ErrorSummary<E, S>, WC, NullFamily>
    // where
    //     EC: IntoZeroOrMore<VecFamily>,
    // {
    //     self.aggregate_errors(|es| ErrorSummary::new(summary, es.into_zero_or_more()))
    // }

    fn with_success<F, V, PF, WCf>(self, other: Success<V, WC>, f: F) -> Failure<PF, E, WCf, EC>
    where
        F: FnOnce(P, V) -> PF,
        WC: Concatable<Out = WCf>,
    {
        let ws = self.warnings.concat(other.warnings);
        Failure::new(ws, self.errors, f(self.value, other.value))
    }

    /// Remove warnings while maintaining the warning type
    ///
    /// This is useful for cases where warnings might be optionally removed
    /// so we can't just set them to `()`
    fn remove_warnings(self) -> Self
    where
        WC: Default,
    {
        Self::new(WC::default(), self.errors, self.value)
    }

    /// Convert warnings to errors while maintaining the warning type.
    ///
    /// This is useful for cases where warnings might be optionally converted
    /// so we can't just set them to `()`
    fn warnings_to_errors<W, F>(mut self, f: F) -> Self
    where
        F: Fn(W) -> E,
        EC: Extend<E>,
        WC: IntoIterator<Item = W> + Default,
    {
        self.errors.extend(self.warnings.into_iter().map(f));
        Self::new_from_many(self.errors, self.value)
    }
}

impl<P, E, EC> Failure<P, E, NeverValue<()>, EC> {
    fn nowarn_into_warn<WC>(self) -> Failure<P, E, WC, EC>
    where
        WC: Default,
    {
        Failure::new_from_many(self.errors, self.value)
    }
}

// impl<W, E, P, WC: ZeroOrMore> Failure<P, W, E, WC, NullFamily> {
//     fn resolve<F, G, X, Y>(self, f: F, g: G) -> (X, Y)
//     where
//         F: FnOnce(WC::Inner<W>) -> X,
//         G: FnOnce(E) -> Y,
//     {
//         (f(self.warnings), g(self.errors.head))
//     }
// }

impl<T, C> Extend<T> for GenNonEmpty<T, C>
where
    C: Extend<T>,
{
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = T>,
    {
        self.tail.extend(iter);
    }
}

impl<T, C> GenNonEmpty<T, C> {
    fn collect(xs: impl IntoIterator<Item = T>) -> Option<Self>
    where
        C: Extend<T> + Default,
    {
        let mut it = xs.into_iter();
        it.by_ref().next().map(|x0| {
            let mut ret = Self::new1(x0);
            ret.extend(it);
            ret
        })
    }

    fn new1(x: T) -> Self
    where
        C: Default,
    {
        Self::new(x, C::default())
    }

    fn map<X, F>(self, f: F) -> GenNonEmpty<X, <C::Family as Kind1>::Inner<X>>
    where
        C: InFamily,
        C::Family: Functor<Inner<T> = C>,
        F: Fn(T) -> X,
    {
        GenNonEmpty::new(f(self.head), C::Family::fmap(self.tail, f))
    }

    fn repack<Cf>(self) -> GenNonEmpty<T, Cf>
    where
        C: IntoZeroOrMore<Cf>,
    {
        GenNonEmpty::new(self.head, self.tail.into_zero_or_more())
    }

    // fn prepend<I>(&mut self, other: I)
    // where
    //     I: IntoIterator<Item = T>,
    //     C::Inner<T>: Extend<T>,
    // {
    //     let mut it = other.into_iter();
    //     if let Some(x0) = it.by_ref().next() {
    //         let mut new = GenNonEmpty::new1(x0);
    //         new.extend(it);
    //         let oldself = mem::replace(self, new);
    //         self.extend(oldself.into_iter());
    //     }
    // }

    // fn into_zero_or_more<Fi, Ff>(self) -> GenNonEmpty<T, Ff::Inner<T>>
    // where
    //     Ff: ZeroOrMore,
    //     Fi: IntoZeroOrMore<Ff> + ZeroOrMore<Inner<T> = C>,
    // {
    //     GenNonEmpty::new(self.head, C::into_zero_or_more(self.tail))
    // }
}

impl<E, C> From<(E, C)> for GenNonEmpty<E, C> {
    fn from(value: (E, C)) -> Self {
        Self::new(value.0, value.1)
    }
}

pub trait OptionExt: Sized {
    type Inner;

    fn into_option(self) -> Option<Self::Inner>;

    // fn transpose_success<V, W, WC>(self) -> Success<Option<V>, W, WC>
    // where
    //     Self: OptionExt<Inner = Success<V, W, WC>>,
    //     WC: ZeroOrMore,
    // {
    //     self.into_option()
    //         .map_or(Success::new1(None), |x| x.map_value(Some))
    // }

    fn transpose_log_result<V, P, E, LWC, RWC, EC>(self) -> LogResult<Option<V>, P, E, LWC, RWC, EC>
    where
        Self: OptionExt<Inner = LogResult<V, P, E, LWC, RWC, EC>>,
        LWC: Default,
    {
        self.into_option()
            .map_or(Result::new_ok(None), |x| x.map_ok_value(Some))
    }
}

impl<T> OptionExt for Option<T> {
    type Inner = T;

    fn into_option(self) -> Self {
        self
    }
}

pub trait ResultExt: Sized {
    type Ok;
    type Error;

    fn into_result(self) -> Result<Self::Ok, Self::Error>;

    fn as_result(&self) -> Result<&Self::Ok, &Self::Error>;

    fn as_result_mut(&mut self) -> Result<&mut Self::Ok, &mut Self::Error>;

    fn new_ok<P, LWC, RWC, EC>(value: Self::Ok) -> LogResult<Self::Ok, P, Self::Error, LWC, RWC, EC>
    where
        LWC: Default,
    {
        Ok(Success::new1(value))
    }

    fn new_ok_def<P, LWC, RWC, EC>() -> LogResult<Self::Ok, P, Self::Error, LWC, RWC, EC>
    where
        Self::Ok: Default,
        LWC: Default,
    {
        Self::new_ok(Self::Ok::default())
    }

    fn new_err1<LWC, RWC, EC>(
        error: Self::Error,
    ) -> LogResult<Self::Ok, (), Self::Error, LWC, RWC, EC>
    where
        RWC: Default,
        EC: Default,
    {
        Err(Failure::new_from_one(error, ()))
    }

    // TODO generic input?
    fn new_err<LWC, RWC, EC>(
        error: GenNonEmpty<Self::Error, EC>,
    ) -> LogResult<Self::Ok, (), Self::Error, LWC, RWC, EC>
    where
        RWC: Default,
        EC: Default,
    {
        Err(Failure::new_from_many(error, ()))
    }

    fn new_err_from_iter<I, LWC, RWC, EC>(
        errors: I,
        default: Self::Ok,
    ) -> LogResult<Self::Ok, (), Self::Error, LWC, RWC, EC>
    where
        I: IntoIterator<Item = Self::Error>,
        EC: Extend<Self::Error> + Default,
        RWC: Default,
        LWC: Default,
    {
        GenNonEmpty::collect(errors).map_or(Result::new_ok(default), Result::new_err)
    }

    fn new_non_fungible<P, LWC, RWC, EC>(
        value: Self::Ok,
        default: P,
        error: Self::Error,
        is_error: bool,
    ) -> LogResult<Self::Ok, P, Self::Error, LWC, RWC, EC>
    where
        LWC: Default,
        RWC: Default,
        EC: Default,
    {
        if is_error {
            Err(Failure::new_from_one(error, default))
        } else {
            Ok(Success::new1(value))
        }
    }

    fn new_fungible<P, W, RWC, EC>(
        value: Self::Ok,
        default: P,
        error: Self::Error,
        is_error: bool,
    ) -> FungibleResult<Self::Ok, P, Self::Error, RWC, EC>
    where
        EC: FungibleError<Self::Error> + Default,
        RWC: Default,
    {
        if is_error {
            Err(Failure::new_from_one(error, default))
        } else {
            Ok(Success::new(value, EC::error_to_warning(error)))
        }
    }

    fn new_deferred_fungible<W, RWC, EC>(
        value: Self::Ok,
        error: Self::Error,
        is_error: bool,
    ) -> FungibleResult<Self::Ok, Self::Ok, Self::Error, RWC, EC>
    where
        EC: FungibleError<Self::Error> + Default,
        RWC: Default,
    {
        if is_error {
            Err(Failure::new_from_one(error, value))
        } else {
            Ok(Success::new(value, EC::error_to_warning(error)))
        }
    }

    fn into_nowarn1(self) -> NowarnResult<Self::Ok, (), Self::Error, NeverValue<()>> {
        self.into_log()
    }

    fn into_nowarn(self) -> NowarnResult<Self::Ok, (), Self::Error, NeverValue<()>> {
        self.into_log()
    }

    fn into_warn1(self) -> NowarnResult<Self::Ok, (), Self::Error, NeverValue<()>> {
        self.into_log()
    }

    fn into_log<LWC, RWC, EC>(self) -> LogResult<Self::Ok, (), Self::Error, LWC, RWC, EC>
    where
        EC: Default,
        LWC: Default,
        RWC: Default,
    {
        self.into_result()
            .map(Success::new1)
            .map_err(|e| Failure::new_from_one(e, ()))
    }

    fn into_deferred_fungible<EC>(
        self,
        is_error: bool,
    ) -> DeferredFungible<Self::Ok, Self::Error, EC>
    where
        Self::Ok: Default,
        EC: FungibleError<Self::Error> + Default,
        EC::Warn: Default,
    {
        match self.into_result() {
            Ok(s) => Ok(Success::new1(s)),
            Err(e) => {
                if is_error {
                    Err(Failure::new_from_one(e, Self::Ok::default()))
                } else {
                    Ok(Success::new(Self::Ok::default(), EC::error_to_warning(e)))
                }
            }
        }
    }

    fn into_deferred_fungible_opt<EC>(
        self,
        is_error: bool,
    ) -> DeferredFungible<Option<Self::Ok>, Self::Error, EC>
    where
        EC: FungibleError<Self::Error> + Default,
        EC::Warn: Default,
    {
        self.into_result()
            .map(Some)
            .into_deferred_fungible::<EC>(is_error)
    }

    fn into_deferred_fungible_def<EC>(
        self,
        default: Self::Ok,
        is_error: bool,
    ) -> DeferredFungible<Self::Ok, Self::Error, EC>
    where
        EC: FungibleError<Self::Error> + Default,
        EC::Warn: Default,
    {
        self.into_result()
            .into_deferred_fungible_opt(is_error)
            .map_def_value(|v| v.unwrap_or(default))
    }

    fn into_succ<P, E, LWC, RWC, EC>(
        self,
    ) -> LogResult<Self::Ok, P, E, LWC::Inner<Self::Error>, RWC, EC>
    where
        Self::Ok: Default,
        LWC: Pure,
        LWC::Inner<Self::Error>: Default,
    {
        let ret = self.into_result().map_or_else(
            |e| Success::new(Self::Ok::default(), LWC::wrap(e)),
            Success::new1,
        );
        Ok(ret)
    }

    fn into_succ_opt<P, E, LWC, RWC, EC>(
        self,
    ) -> LogResult<Option<Self::Ok>, P, E, LWC::Inner<Self::Error>, RWC, EC>
    where
        LWC: Pure,
        LWC::Inner<Self::Error>: Default,
    {
        self.into_result().map(Some).into_succ::<_, _, LWC, _, _>()
    }

    fn into_succ_or<P, RW, E, LWC, RWC, EC>(
        self,
        default: Self::Ok,
    ) -> LogResult<Self::Ok, P, E, LWC::Inner<Self::Error>, RWC, EC>
    where
        LWC: Pure,
        LWC::Inner<Self::Error>: Default,
    {
        self.into_succ_opt::<_, _, LWC, _, _>()
            .map_ok_value(|x| x.unwrap_or(default))
    }

    // TODO versions of the above that go to errors?
}

impl<V, E> ResultExt for Result<V, E> {
    type Ok = V;
    type Error = E;

    fn into_result(self) -> Self {
        self
    }

    fn as_result(&self) -> Result<&V, &E> {
        self.as_ref()
    }

    fn as_result_mut(&mut self) -> Result<&mut V, &mut E> {
        self.as_mut()
    }
}

type FunctorOut<C, T> = <<C as InFamily>::Family as Kind1>::Inner<T>;

pub trait LogResultExt
where
    Self: Sized
        + ResultExt<
            Ok = Success<Self::V, Self::LWC>,
            Error = Failure<Self::P, Self::E, Self::RWC, Self::EC>,
        >,
{
    type V;
    type P;
    type E;
    type LWC;
    type RWC;
    type EC;

    fn recover_with<Ferr, Fsucc, V, P, E, WC, EC>(
        self,
        f_err: Ferr,
        f_succ: Fsucc,
    ) -> CmtResult<V, P, E, WC, EC>
    where
        Fsucc: FnOnce(Self::V) -> CmtResult<V, P, E, Self::LWC, EC>,
        Ferr: FnOnce(Self::P, GenNonEmpty<Self::E, Self::EC>) -> CmtResult<V, P, E, Self::LWC, EC>,
        Self: CommutativeResultExt,
        Self::LWC: Concatable<Out = WC>,
    {
        match self.into_result() {
            Ok(s) => s.and_maybe(f_succ),
            Err(f) => Success::new(f.value, f.warnings).and_maybe(|v| f_err(v, f.errors)),
        }
    }

    /// Lift Result with no warnings to non-commutative Result
    fn nowarn_into_non_cmt_warn<Wf, LWCf>(
        self,
    ) -> NonCmtResult<Self::V, Self::P, Self::E, LWCf, Self::EC>
    where
        Self: NowarnExt,
        LWCf: Default,
    {
        self.into_result().map(Success::nowarn_into_warn)
    }

    /// Lift Result with no warnings to commutative Result
    fn nowarn_into_warn<LWCf>(self) -> CmtResult<Self::V, Self::P, Self::E, LWCf, Self::EC>
    where
        Self: NowarnExt,
        Self::LWC: Default,
        LWCf: Default,
    {
        self.into_result()
            .map(Success::nowarn_into_warn)
            .non_cmt_into_cmt()
    }

    /// Lift non-commutative Result into commutative Result
    fn non_cmt_into_cmt(self) -> CmtResult<Self::V, Self::P, Self::E, Self::LWC, Self::EC>
    where
        Self: NonCommutativeResultExt,
        Self::LWC: Default,
    {
        self.into_result().map_err(Failure::nowarn_into_warn)
    }

    /// Map function over Ok value of Result
    fn map_ok_value<F, Vf>(
        self,
        f: F,
    ) -> LogResult<Vf, Self::P, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: FnOnce(Self::V) -> Vf,
    {
        self.into_result().map(|s| s.map_value(f))
    }

    /// Run function when result is Ok
    fn when_ok<F>(
        self,
        f: F,
    ) -> LogResult<Self::V, Self::P, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: FnOnce(),
    {
        self.map_ok_value(|v| {
            f();
            v
        })
    }

    /// Map function over Error value of Result
    fn map_err_value<F, Pf>(
        self,
        f: F,
    ) -> LogResult<Self::V, Pf, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: FnOnce(Self::P) -> Pf,
    {
        self.into_result().map_err(|e| e.map_value(f))
    }

    /// Set value of Ok Result
    fn set_ok_value<Vf>(
        self,
        x: Vf,
    ) -> LogResult<Vf, Self::P, Self::E, Self::LWC, Self::RWC, Self::EC> {
        self.map_ok_value(|_| x)
    }

    /// Set value of Error Result
    fn set_err_value<Pf>(
        self,
        x: Pf,
    ) -> LogResult<Self::V, Pf, Self::E, Self::LWC, Self::RWC, Self::EC> {
        self.map_err_value(|_| x)
    }

    /// Set value of deferred Result
    fn set_def_value<Vf>(self, x: Vf) -> Deferred<Vf, Self::E, Self::LWC, Self::EC>
    where
        Self: DeferredExt,
    {
        match self.into_result() {
            Ok(s) => Ok(s.map_value(|_| x)),
            Err(e) => Err(e.map_value(|_| x)),
        }
    }

    /// Add a member to both the Ok and Error value, returning both as a tuple.
    ///
    /// This seems weird but is useful for cases where we need to use a non-Copy
    /// variable in two closures for both branches but one closure will "eat"
    /// (move) the value before the other can use it. This function will move
    /// the value once depending on the branch where is can be consumed by
    /// both closures as an argument.
    fn inject_value<X>(
        self,
        x: X,
    ) -> LogResult<(Self::V, X), (Self::P, X), Self::E, Self::LWC, Self::RWC, Self::EC> {
        match self.into_result() {
            Ok(s) => Ok(s.map_value(|v| (v, x))),
            Err(e) => Err(e.map_value(|v| (v, x))),
        }
    }

    /// Convert warnings of a non-commutative Result
    fn non_cmt_warnings_into<W, Wf>(
        self,
    ) -> LogResult<Self::V, Self::P, Self::E, FunctorOut<Self::LWC, Wf>, Self::RWC, Self::EC>
    where
        Self: NonCommutativeResultExt,
        W: Into<Wf>,
        Self::LWC: InFamily,
        <Self::LWC as InFamily>::Family: Functor<Inner<W> = Self::LWC>,
    {
        self.map_non_cmt_warnings::<_, W, Wf>(Into::into)
    }

    /// Map function over warnings of a non-commutative Result
    fn map_non_cmt_warnings<F, W, Wf>(
        self,
        f: F,
    ) -> LogResult<Self::V, Self::P, Self::E, FunctorOut<Self::LWC, Wf>, Self::RWC, Self::EC>
    where
        Self: NonCommutativeResultExt,
        F: Fn(W) -> Wf,
        Self::LWC: InFamily,
        <Self::LWC as InFamily>::Family: Functor<Inner<W> = Self::LWC>,
    {
        self.into_result().map(|s| s.map_warnings(f))
    }

    /// Convert warnings of commutative Result
    fn cmt_warnings_into<W, Wf>(
        self,
    ) -> CmtResult<Self::V, Self::P, Self::E, FunctorOut<Self::LWC, Wf>, Self::EC>
    where
        W: Into<Wf>,
        Self: CommutativeResultExt,
        Self::LWC: InFamily,
        <Self::LWC as InFamily>::Family: Functor<Inner<W> = Self::LWC>,
    {
        self.map_cmt_warnings::<_, W, Wf>(Into::into)
    }

    /// Map function over warnings of commutative Result
    fn map_cmt_warnings<F, W, Wf>(
        self,
        f: F,
    ) -> CmtResult<Self::V, Self::P, Self::E, FunctorOut<Self::LWC, Wf>, Self::EC>
    where
        F: Fn(W) -> Wf,
        Self: CommutativeResultExt,
        Self::LWC: InFamily,
        <Self::LWC as InFamily>::Family: Functor<Inner<W> = Self::LWC>,
    {
        self.into_result()
            .map(|s| s.map_warnings(&f))
            .map_err(|e| e.map_warnings(f))
    }

    /// Convert errors in Result
    ///
    /// This function will work on any Result type but may change a fungible
    /// Result to non-fungible one, which is generally not a good idea.
    /// See [`*_fung_errors_into`] for functions that will map over warnings
    /// if they are the same type as errors.
    fn non_fung_errors_into<Ef>(
        self,
    ) -> LogResult<Self::V, Self::P, Ef, Self::LWC, Self::RWC, FunctorOut<Self::EC, Ef>>
    where
        Self::E: Into<Ef>,
        Self::EC: InFamily,
        <Self::EC as InFamily>::Family: Functor<Inner<Self::E> = Self::EC>,
    {
        self.map_non_fung_errors(Into::into)
    }

    /// Map function over errors in Result
    ///
    /// This function will work on any Result type but may change a fungible
    /// Result to non-fungible one, which is generally not a good idea.
    /// See [`map_*_fung_errors`] for functions that will map over warnings
    /// if they are the same type as errors.
    fn map_non_fung_errors<F, Ef>(
        self,
        f: F,
    ) -> LogResult<Self::V, Self::P, Ef, Self::LWC, Self::RWC, FunctorOut<Self::EC, Ef>>
    where
        F: Fn(Self::E) -> Ef,
        Self::EC: InFamily,
        <Self::EC as InFamily>::Family: Functor<Inner<Self::E> = Self::EC>,
    {
        self.into_result().map_err(|e| e.map_errors(f))
    }

    /// Convert errors in commutative/fungible Results
    fn non_cmt_fung_errors_into<Ef>(
        self,
    ) -> LogResult<
        Self::V,
        Self::P,
        Ef,
        FunctorOut<Self::LWC, Ef>,
        NeverValue<()>,
        FunctorOut<Self::EC, Ef>,
    >
    where
        Self::E: Into<Ef>,
        Self: NonCommutativeResultExt + FungibleExt,
        Self::EC: FungibleError<Self::E> + InFamily,
        <Self::EC as InFamily>::Family: Functor<Inner<Self::E> = Self::EC>,
        <Self::EC as FungibleError<Self::E>>::Warn: InFamily,
        <<Self::EC as FungibleError<Self::E>>::Warn as InFamily>::Family:
            Functor<Inner<Self::E> = Self::LWC>,
    {
        self.map_non_cmt_fung_errors(Into::into)
    }

    /// Map function over errors in commutative/fungible Results
    fn cmt_fung_errors_into<Ef>(
        self,
    ) -> LogResult<
        Self::V,
        Self::P,
        Ef,
        FunctorOut<Self::LWC, Ef>,
        FunctorOut<Self::LWC, Ef>,
        FunctorOut<Self::EC, Ef>,
    >
    where
        Self::E: Into<Ef>,
        Self: CommutativeResultExt + FungibleExt,
        Self::EC: FungibleError<Self::E> + InFamily,
        <Self::EC as InFamily>::Family: Functor<Inner<Self::E> = Self::EC>,
        <Self::EC as FungibleError<Self::E>>::Warn: InFamily,
        <<Self::EC as FungibleError<Self::E>>::Warn as InFamily>::Family:
            Functor<Inner<Self::E> = Self::LWC>,
    {
        self.map_cmt_fung_errors(Into::into)
    }

    /// Convert errors in non-commutative/fungible Results
    fn map_non_cmt_fung_errors<F, Ef>(
        self,
        f: F,
    ) -> LogResult<
        Self::V,
        Self::P,
        Ef,
        FunctorOut<Self::LWC, Ef>,
        NeverValue<()>,
        FunctorOut<Self::EC, Ef>,
    >
    where
        F: Fn(Self::E) -> Ef,
        Self: NonCommutativeResultExt + FungibleExt,
        Self::EC: FungibleError<Self::E> + InFamily,
        <Self::EC as InFamily>::Family: Functor<Inner<Self::E> = Self::EC>,
        <Self::EC as FungibleError<Self::E>>::Warn: InFamily,
        <<Self::EC as FungibleError<Self::E>>::Warn as InFamily>::Family:
            Functor<Inner<Self::E> = Self::LWC>,
    {
        match self.into_result() {
            Ok(s) => Ok(s.map_warnings(f)),
            Err(e) => Err(e.map_errors(f)),
        }
    }

    /// Map function over errors in non-commutative/fungible Results
    fn map_cmt_fung_errors<F, Ef>(
        self,
        f: F,
    ) -> LogResult<
        Self::V,
        Self::P,
        Ef,
        FunctorOut<Self::LWC, Ef>,
        FunctorOut<Self::LWC, Ef>,
        FunctorOut<Self::EC, Ef>,
    >
    where
        F: Fn(Self::E) -> Ef,
        Self: CommutativeResultExt + FungibleExt,
        Self::EC: FungibleError<Self::E> + InFamily,
        <Self::EC as InFamily>::Family: Functor<Inner<Self::E> = Self::EC>,
        <Self::EC as FungibleError<Self::E>>::Warn: InFamily,
        <<Self::EC as FungibleError<Self::E>>::Warn as InFamily>::Family:
            Functor<Inner<Self::E> = Self::LWC>,
    {
        match self.into_result() {
            Ok(s) => Ok(s.map_warnings(f)),
            Err(e) => Err(e.map_errors(&f).map_warnings(f)),
        }
    }

    /// Map function over Ok and Error value of result (assumed same type).
    fn map_def_value<F, Vf>(
        self,
        f: F,
    ) -> LogResult<Vf, Vf, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: FnOnce(Self::V) -> Vf,
        Self: DeferredExt,
    {
        match self.into_result() {
            Ok(s) => Ok(s.map_value(f)),
            Err(e) => Err(e.map_value(f)),
        }
    }

    fn repack<LWCf, RWCf, ECf>(self) -> LogResult<Self::V, Self::P, Self::E, LWCf, RWCf, ECf>
    where
        Self::LWC: IntoZeroOrMore<LWCf>,
        Self::RWC: IntoZeroOrMore<RWCf>,
        Self::EC: IntoZeroOrMore<ECf>,
    {
        self.repack_left_warnings()
            .repack_right_warnings()
            .repack_errors()
    }

    fn into_semigroup<LWC, RWC>(self) -> LogResult<Self::V, Self::P, Self::E, LWC, RWC, VecFamily>
    where
        Self::LWC: IntoZeroOrMore<LWC>,
        Self::RWC: IntoZeroOrMore<RWC>,
        Self::EC: IntoZeroOrMore<VecFamily>,
    {
        self.repack()
    }

    fn repack_warnings<WCf>(self) -> LogResult<Self::V, Self::P, Self::E, WCf, WCf, Self::EC>
    where
        Self::LWC: IntoZeroOrMore<WCf>,
        Self::RWC: IntoZeroOrMore<WCf>,
    {
        self.into_result()
            .repack_right_warnings()
            .repack_left_warnings()
    }

    fn repack_left_warnings<LWCf>(
        self,
    ) -> LogResult<Self::V, Self::P, Self::E, LWCf, Self::RWC, Self::EC>
    where
        Self::LWC: IntoZeroOrMore<LWCf>,
    {
        self.into_result().map(Success::repack)
    }

    fn repack_right_warnings<RWCf>(
        self,
    ) -> LogResult<Self::V, Self::P, Self::E, Self::LWC, RWCf, Self::EC>
    where
        Self::RWC: IntoZeroOrMore<RWCf>,
    {
        self.into_result().map_err(Failure::repack_warnings)
    }

    fn repack_errors<ECf>(self) -> LogResult<Self::V, Self::P, Self::E, Self::LWC, Self::RWC, ECf>
    where
        Self::EC: IntoZeroOrMore<ECf>,
    {
        self.into_result().map_err(Failure::repack_errors)
    }

    fn cmt_warnings_to_errors<F, W>(
        self,
        conf: &SharedConfig,
        f: F,
    ) -> CmtResult<Self::V, (), Self::E, Self::RWC, Self::EC>
    where
        F: Fn(W) -> Self::E,
        Self: CommutativeResultExt,
        Self::EC: Extend<Self::E> + Default,
        Self::LWC: IntoZeroOrMore<Self::EC> + IntoIterator<Item = W> + Default,
    {
        let res = self.into_result();
        if conf.warnings_are_errors {
            match res {
                Ok(s) => s.warnings_to_errors(f, |_| ()),
                Err(e) => Err(e.warnings_to_errors(f).map_value(|_| ())),
            }
        } else if conf.hide_warnings {
            res.map(Success::remove_warnings)
                .map_err(Failure::remove_warnings)
                .set_err_value(())
        } else {
            res.set_err_value(())
        }
    }

    // fn remove_warnings(self) -> NowarnResult<Self::V, Self::P, Self::E, Self::EC> {
    //     self.into_result()
    //         .map(|s| s.remove_warnings())
    //         .map_err(|e| e.remove_warnings())
    // }

    // TODO private? this seems wonky to have in the external api
    // fn warnings_to_errors<F0, F1, F2, P>(
    //     self,
    //     f0: F0,
    //     f1: F1,
    //     f2: F2,
    // ) -> NowarnResult<Self::V, P, Self::E, Self::EC>
    // where
    //     F0: Fn(Self::LW) -> Self::E,
    //     F1: FnOnce(Self::V) -> P,
    //     F2: FnOnce(Self::P) -> P,
    //     Self::LWC: IntoZeroOrMore<Self::EC>,
    //     <Self::EC as ZeroOrMore>::Inner<Self::E>: Extend<Self::E>,
    //     Self: CommutativeResultExt,
    // {
    //     match self.into_result() {
    //         Ok(s) => s.warnings_to_errors(f0, f1),
    //         Err(e) => Err(e.warnings_to_errors(f0).map_passthru(f2)),
    //     }
    // }

    // /// Map warnings in deferred Result to errors
    // fn def_warnings_to_errors<F>(self, f: F) -> DeferredNowarn<Self::V, Self::E, Self::EC>
    // where
    //     F: Fn(Self::LW) -> Self::E,
    //     Self::LWC: IntoZeroOrMore<Self::EC>,
    //     <Self::EC as ZeroOrMore>::Inner<Self::E>: Extend<Self::E>,
    //     Self: DeferredExt,
    // {
    //     self.warnings_to_errors(f, |x| x, |x| x)
    // }

    /// Aggregate non-fungible errors into one error.
    fn aggregate_non_fung_errors<F, Ef>(
        self,
        f: F,
    ) -> LogResult<Self::V, Self::P, Ef, Self::LWC, Self::RWC, NeverValue<Ef>>
    where
        // NOTE pretend there is a negative trait bound for "non-fungible"
        F: FnOnce(GenNonEmpty<Self::E, Self::EC>) -> Ef,
    {
        self.into_result().map_err(|e| e.aggregate_errors(f))
    }

    fn summarize_errors<S>(
        self,
    ) -> LogResult<
        Self::V,
        Self::P,
        ErrorSummary<Self::E, S>,
        Self::LWC,
        Self::RWC,
        NeverValue<ErrorSummary<Self::E, S>>,
    >
    where
        Self: LogResultExt<EC = Vec<<Self as LogResultExt>::E>>,
        S: Default,
    {
        self.summarize_errors_with(S::default())
    }

    fn summarize_errors_with<S>(
        self,
        s: S,
    ) -> LogResult<
        Self::V,
        Self::P,
        ErrorSummary<Self::E, S>,
        Self::LWC,
        Self::RWC,
        NeverValue<ErrorSummary<Self::E, S>>,
    >
    where
        Self: LogResultExt<EC = Vec<<Self as LogResultExt>::E>>,
    {
        self.aggregate_non_fung_errors(|es| ErrorSummary::new(s, es))
    }

    /// Aggregate non-commutative/fungible errors into one error.
    fn aggregate_non_cmt_fung_errors<F, G, Ef>(
        self,
        f: F,
        g: G,
    ) -> NonCmtFungibleResult<Self::V, Self::P, Ef, NeverValue<Ef>>
    where
        F: FnOnce(Self::LWC) -> Ef,
        G: FnOnce(GenNonEmpty<Self::E, Self::EC>) -> Ef,
        Self: NonCommutativeResultExt + FungibleExt,
        Self::EC: FungibleError<Self::E>,
    {
        match self.into_result() {
            Ok(s) => Ok(s.aggregate_warnings(f)),
            Err(e) => Err(e.aggregate_errors(g)),
        }
    }

    /// Aggregate commutative/fungible errors into one error.
    fn aggregate_cmt_fung_errors<F, G, Ef>(
        self,
        f: F,
        g: G,
    ) -> CmtFungibleResult<Self::V, Self::P, Ef, NeverValue<Ef>>
    where
        F: FnOnce(Self::LWC) -> Ef,
        G: FnOnce(GenNonEmpty<Self::E, Self::EC>) -> Ef,
        Self: CommutativeResultExt + FungibleExt,
        Self::EC: FungibleError<Self::E>,
    {
        match self.into_result() {
            Ok(s) => Ok(s.aggregate_warnings(f)),
            Err(e) => Err(e.aggregate_errors(g).aggregate_warnings(f)),
        }
    }

    fn infallible_into<P, RWC, E, EC>(self) -> LogResult<Self::V, P, E, Self::LWC, RWC, EC>
    where
        Self: LogResultExt<E = Infallible>,
    {
        let Ok(ret) = self.into_result();
        Ok(ret)
    }

    fn infallible_with_warn_into<F, Wres>(self, f: F) -> (Self::V, Wres)
    where
        F: FnOnce(Self::LWC) -> Wres,
        Self: LogResultExt<E = Infallible>,
    {
        let Ok(ret) = self.into_result();
        (ret.value, f(ret.warnings))
    }

    fn infallible_nowarn_into(self) -> Self::V
    where
        Self: NowarnExt<E = Infallible>,
    {
        let Ok(ret) = self.into_result();
        ret.value
    }

    /// Resolve Result with no warnings into regular Result type.
    fn resolve_nowarn<F, ErrRes>(self, f: F) -> Result<Self::V, ErrRes>
    where
        Self: NowarnExt + ResolvableExt,
        F: FnOnce(Self::E) -> ErrRes,
    {
        self.into_result()
            .map(|s| s.value)
            .map_err(|e| f(e.errors.head))
    }

    /// Resolve non-commutative Result with regular Result type.
    ///
    /// Warnings will be given on the Ok side since non-commutative Result's
    /// by definition cannot have warnings in the Err branch.
    fn resolve_non_cmt<Fwarn, Ferr, WarnRes, ErrRes>(
        self,
        f_warnings: Fwarn,
        f_errors: Ferr,
    ) -> Result<(Self::V, WarnRes), ErrRes>
    where
        Self: NonCommutativeResultExt + ResolvableExt,
        Fwarn: FnOnce(Self::LWC) -> WarnRes,
        Ferr: FnOnce(Self::E) -> ErrRes,
    {
        self.into_result()
            .map(|s| s.resolve(f_warnings))
            .map_err(|e| f_errors(e.errors.head))
    }

    /// Resolve commutative Result with into regular Result type.
    ///
    /// Warnings will be given outside the result since commutative Results by
    /// definition allow the same warnings in both Ok and Error branches.
    fn resolve_cmt<Fwarn, Ferr, WarnRes, ErrRes>(
        self,
        f_warnings: Fwarn,
        f_errors: Ferr,
    ) -> (WarnRes, Result<Self::V, ErrRes>)
    where
        Self: CommutativeResultExt + ResolvableExt,
        Fwarn: FnOnce(Self::LWC) -> WarnRes,
        Ferr: FnOnce(Self::E) -> ErrRes,
    {
        match self.into_result() {
            Ok(s) => {
                let (v, warn_res) = s.resolve(f_warnings);
                (warn_res, Ok(v))
            }
            Err(e) => (f_warnings(e.warnings), Err(f_errors(e.errors.head))),
        }
    }

    /// Push a warning based on the Ok value of a non-deferred Result.
    ///
    /// Will only store warning on the Ok side since the value isn't present
    /// on the error side to be evaluated.
    fn eval_non_def_warning<W, F>(&mut self, f: F)
    where
        F: FnOnce(&Self::V) -> Option<W>,
        Self::LWC: Extend<W>,
    {
        if let Ok(s) = self.as_result_mut() {
            s.eval_warning(f);
        }
    }

    // /// Push a warning based on the Ok value of a non-commutative Result.
    // ///
    // /// Does nothing if result is Error since warnings cannot be stored on
    // /// the error side by definition.
    // fn eval_non_cmt_warning<F>(&mut self, f: F)
    // where
    //     F: FnOnce(&Self::V) -> Option<Self::LW>,
    //     <Self::LWC as ZeroOrMore>::Inner<Self::LW>: Extend<Self::LW>,
    //     Self: NonCommutativeResultExt,
    // {
    //     if let Ok(s) = self.as_result_mut() {
    //         s.eval_warning(f)
    //     }
    // }

    // TODO this function likely is nonsense because it does nothing by
    // definition for the Error side despite a warning being explicitly given,
    // which suggests the warning is legit and should be recorded.

    // fn push_non_cmt_warning(&mut self, w: Self::LW)
    // where
    //     <Self::LWC as ZeroOrMore>::Inner<Self::LW>: Extend<Self::LW>,
    //     Self: NonCommutativeResultExt,
    // {
    //     if let Ok(s) = self.as_result_mut() {
    //         s.push_warning(w)
    //     }
    // }

    /// Push a warning based on the value in a deferred Result.
    ///
    /// This must be a deferred result because the same value type must exist
    /// on both Ok and Error sides.
    fn eval_def_warning<W, F>(&mut self, f: F)
    where
        F: FnOnce(&Self::V) -> Option<W>,
        Self::LWC: Extend<W>,
        Self: DeferredExt,
    {
        match self.as_result_mut() {
            Ok(s) => s.eval_warning(f),
            Err(e) => e.eval_warning(f),
        }
    }

    /// Set warnings in both Ok and Error sides of Result
    fn set_cmt_warnings<WC>(self, ws: WC) -> CmtResult<Self::V, Self::P, Self::E, WC, Self::EC>
    where
        Self: NowarnExt,
    {
        match self.into_result() {
            Ok(s) => Ok(s.set_warnings(ws)),
            Err(e) => Err(e.set_warnings(ws)),
        }
    }

    /// Set warnings in Ok side of Result with no warnings
    fn set_non_cmt_warnings<WC>(
        self,
        ws: WC,
    ) -> NonCmtResult<Self::V, Self::P, Self::E, WC, Self::EC>
    where
        Self: NowarnExt,
    {
        self.into_result().map(|s| s.set_warnings(ws))
    }

    /// Push a warning to a commutative Result.
    fn push_cmt_warning<W>(&mut self, w: W)
    where
        Self::LWC: Extend<W>,
        Self: CommutativeResultExt,
    {
        match self.as_result_mut() {
            Ok(s) => s.push_warning(w),
            Err(e) => e.push_warning(w),
        }
    }

    /// Add warnings to a commutative Result.
    fn extend_cmt_warnings<W>(&mut self, ws: impl IntoIterator<Item = W>)
    where
        Self::LWC: Extend<W>,
        Self: CommutativeResultExt,
    {
        match self.as_result_mut() {
            Ok(s) => s.extend_warnings(ws),
            Err(e) => e.extend_warnings(ws),
        }
    }

    /// Push an error based on the value in a deferred Result.
    ///
    /// If Result is Ok and the evaluation returns an error, the result will
    /// be converted to an error.
    ///
    /// This must be a deferred result because the same value type must exist
    /// on both Ok and Error sides.
    fn eval_def_error<F>(self, f: F) -> CmtResult<Self::V, Self::P, Self::E, Self::LWC, Self::EC>
    where
        F: FnOnce(&Self::V) -> Option<Self::E>,
        Self::EC: Extend<Self::E> + Default,
        Self: DeferredExt,
    {
        match self.into_result() {
            Ok(succ) => match f(&succ.value) {
                Some(e) => Err(succ.fail(GenNonEmpty::new1(e))),
                None => Ok(succ),
            },
            Err(mut err) => {
                if let Some(e) = f(&err.value) {
                    err.push_error(e);
                }
                Err(err)
            }
        }
    }

    /// Push an error based on the value in non-deferred Result.
    ///
    /// If Result is Ok and the evaluation returns an error, the result will
    /// be converted to an error. If already an error, do nothing since
    /// there is no value to use.
    ///
    /// This must be commutative since and OK might flip to an error, and thus
    /// the warnings must match.
    fn eval_non_def_error<F>(self, f: F) -> CmtResult<Self::V, (), Self::E, Self::LWC, Self::EC>
    where
        F: FnOnce(&Self::V) -> Option<Self::E>,
        Self::EC: Extend<Self::E> + Default,
        Self: CommutativeResultExt,
    {
        let succ = self.into_result().set_err_value(())?;
        match f(&succ.value) {
            Some(e) => Err(succ.fail(GenNonEmpty::new1(e)).map_value(|_| ())),
            None => Ok(succ),
        }
    }

    /// Push an error to a deferred Result.
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    fn push_def_error(self, e: Self::E) -> CmtResult<Self::V, Self::P, Self::E, Self::LWC, Self::EC>
    where
        Self::EC: Extend<Self::E> + Default,
        Self: DeferredExt,
    {
        match self.into_result() {
            Ok(succ) => Err(succ.fail(GenNonEmpty::new1(e))),
            Err(mut err) => {
                err.push_error(e);
                Err(err)
            }
        }
    }

    /// Push errors to a deferred Result.
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    fn extend_def_errors(
        self,
        es: impl IntoIterator<Item = Self::E>,
    ) -> CmtResult<Self::V, Self::P, Self::E, Self::LWC, Self::EC>
    where
        Self::EC: Extend<Self::E> + Default,
        Self: DeferredExt,
    {
        match self.into_result() {
            Ok(succ) => {
                let mut it = es.into_iter();
                if let Some(e0) = it.by_ref().next() {
                    let mut es_ = GenNonEmpty::new1(e0);
                    es_.extend(it);
                    Err(succ.fail(es_))
                } else {
                    Ok(succ)
                }
            }
            Err(mut err) => {
                err.extend_errors(es);
                Err(err)
            }
        }
    }

    /// Push fungible error to a deferred Result based on its value.
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    fn eval_def_fung_error<W, F>(
        mut self,
        is_error: bool,
        f: F,
    ) -> DeferredFungible<Self::V, Self::E, Self::EC>
    where
        F: FnOnce(&Self::V) -> Option<Self::E>,
        Self::LWC: Extend<Self::E>,
        Self::EC: Extend<Self::E> + Default + FungibleError<Self::E>,
        Self: FungibleExt + DeferredExt,
    {
        if is_error {
            self.eval_def_error(f)
        } else {
            self.eval_def_warning(f);
            self.into_result()
        }
    }

    /// Push non-fungible error to a deferred Result based on its value.
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    fn eval_def_non_fung_error<F, E>(
        mut self,
        is_error: bool,
        f: F,
    ) -> Deferred<Self::V, Self::E, Self::LWC, Self::EC>
    where
        F: FnOnce(&Self::V) -> Option<E>,
        E: Into<Self::E>,
        Self::LWC: Extend<Self::E>,
        Self::EC: Extend<Self::E> + Default,
        Self: DeferredExt,
    {
        if is_error {
            self.eval_def_error(|x| f(x).map(Into::into))
        } else {
            self.eval_def_warning(|x| f(x).map(Into::into));
            self.into_result()
        }
    }

    /// Push fungible error to a deferred Result.
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    fn push_def_fung_error(
        self,
        e: Self::E,
        is_error: bool,
    ) -> DeferredFungible<Self::V, Self::E, Self::EC>
    where
        Self::LWC: Extend<Self::E>,
        Self::EC: Extend<Self::E> + FungibleError<Self::E> + Default,
        Self: FungibleExt + DeferredExt,
    {
        self.extend_def_fung_errors(iter::once(e), is_error)
    }

    fn extend_fung_errors<W, Fv, Fp, Fw, Fe, P, E>(
        mut self,
        errors: impl IntoIterator<Item = E>,
        fv: Fv,
        fp: Fp,
        fw: Fw,
        fe: Fe,
        is_error: bool,
    ) -> CmtResult<Self::V, P, Self::E, Self::LWC, Self::EC>
    where
        Fv: FnOnce(Self::V) -> P,
        Fp: FnOnce(Self::P) -> P,
        Fe: Fn(E) -> Self::E,
        Fw: Fn(E) -> W,
        Self::LWC: Extend<W>,
        Self::EC: Extend<Self::E> + Default + FungibleError<Self::E>,
        Self: CommutativeResultExt,
    {
        if is_error {
            let mut it = errors.into_iter().map(fe);
            match self.into_result() {
                Ok(succ) => {
                    if let Some(e0) = it.by_ref().next() {
                        let mut es_ = GenNonEmpty::new1(e0);
                        es_.extend(it);
                        Err(succ.fail(es_).map_value(fv))
                    } else {
                        Ok(succ)
                    }
                }
                Err(mut err) => {
                    err.extend_errors(it);
                    Err(err.map_value(fp))
                }
            }
        } else {
            self.extend_cmt_warnings(errors.into_iter().map(fw));
            self.into_result().map_err_value(fp)
        }
    }

    /// Push fungible errors to a deferred Result.
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    fn extend_def_fung_errors(
        mut self,
        xs: impl IntoIterator<Item = Self::E>,
        is_error: bool,
    ) -> DeferredFungible<Self::V, Self::E, Self::EC>
    where
        Self::LWC: Extend<Self::E>,
        Self::EC: Extend<Self::E> + Default + FungibleError<Self::E>,
        Self: FungibleExt + DeferredExt,
    {
        if is_error {
            self.extend_def_errors(xs)
        } else {
            self.extend_cmt_warnings(xs);
            self.into_result()
        }
    }

    fn and_cmt<F>(self, f: F) -> CmtResult<Self::V, Self::P, Self::E, Self::LWC, Self::EC>
    where
        F: FnOnce() -> CmtResult<(), Self::P, Self::E, Self::LWC, Self::EC>,
        Self::LWC: Semigroup,
        Self: CommutativeResultExt,
    {
        self.and_then_cmt(|v| f().map_ok_value(|()| v))
    }

    /// Monad-ically chain commutative result operations.
    ///
    /// Function will be called on value if result is Ok. Original error will
    /// be returned otherwise.
    ///
    /// This only works on commutative results since the warnings from the
    /// original result need to be considered if the provided function
    /// returns error.
    ///
    /// Inner for warnings must be a semigroup, which specifically means
    /// that Option<T> must be converted to a vector before calling this.
    fn and_then_cmt<F, Vf>(self, f: F) -> CmtResult<Vf, Self::P, Self::E, Self::LWC, Self::EC>
    where
        F: FnOnce(Self::V) -> CmtResult<Vf, Self::P, Self::E, Self::LWC, Self::EC>,
        Self::LWC: Semigroup,
        Self: CommutativeResultExt,
    {
        self.into_result()?.and_maybe(f)
    }

    /// Monad-ically apply commutative result operation to deferred result.
    ///
    /// Function will be called on value of either the Ok or Error branch
    /// depending on the input Result. Warnings and errors from input Result
    /// will be aggregated with the Result returned by the provided function.
    ///
    /// Input Result must be deferred because the value types match between
    /// Ok and Error. The output does not necessarily need to be deferred
    /// (although it likely will be in many cases).
    ///
    /// Inner for warnings must be a semigroup, which specifically means
    /// that Option<T> must be converted to a vector before calling this.
    ///
    /// Inner for errors must be able to hold multiple values.
    fn and_then_def<F, Vf, Pf>(self, f: F) -> CmtResult<Vf, Pf, Self::E, Self::LWC, Self::EC>
    where
        F: FnOnce(Self::V) -> CmtResult<Vf, Pf, Self::E, Self::LWC, Self::EC>,
        Self: DeferredExt,
        Self::LWC: Semigroup,
        Self::EC: Extend<Self::E> + IntoIterator<Item = Self::E>,
    {
        match self.into_result() {
            Ok(s) => s.and_maybe(f),
            Err(e) => e.with_value(f),
        }
    }

    /// Combine two commutative results.
    ///
    /// Ok values will be wrapped in a tuple. Error values if they exist will
    /// be voided.
    ///
    /// Inners for warnings and errors must be the same. The former must
    /// be a semigroup (which here means Option<T> must be converted to Vec<T>
    /// prior to calling). The latter will be converted to a Vec<T> since
    /// there could be more than one errors.
    fn zip_cmt<V1, P1>(
        self,
        a: CmtResult<V1, P1, Self::E, Self::LWC, Self::EC>,
    ) -> CmtResult<(Self::V, V1), (), Self::E, Self::LWC, Vec<Self::E>>
    where
        Self::EC: IntoZeroOrMore<Vec<Self::E>> + IntoIterator<Item = Self::E>,
        Self::LWC: Semigroup,
        Self: CommutativeResultExt,
    {
        match (self.into_result(), a) {
            (Ok(ax), Ok(bx)) => Ok(ax.zip_with(bx, |x, y| (x, y))),
            (Ok(ax), Err(bx)) => Err(ax.with_failure(bx, |_, _| ()).repack_errors()),
            (Err(ax), Ok(bx)) => Err(ax.with_success(bx, |_, _| ()).repack_errors()),
            (Err(ax), Err(bx)) => Err(ax.zip_with(bx, |_, _| ())),
        }
    }

    /// Combine three commutative results.
    fn zip3_cmt<V1, V2, P1, P2>(
        self,
        a: CmtResult<V1, P1, Self::E, Self::LWC, Self::EC>,
        b: CmtResult<V2, P2, Self::E, Self::LWC, Self::EC>,
    ) -> CmtResult<(Self::V, V1, V2), (), Self::E, Self::LWC, Vec<Self::E>>
    where
        Self::EC: IntoZeroOrMore<Vec<Self::E>> + IntoIterator<Item = Self::E>,
        Self::LWC: Semigroup,
        Self: CommutativeResultExt,
    {
        self.zip_cmt(a)
            .zip_cmt(b.repack())
            .map_ok_value(|((ax, bx), cx)| (ax, bx, cx))
    }

    /// Combine four commutative results.
    fn zip4_cmt<V1, V2, V3, P1, P2, P3>(
        self,
        a: CmtResult<V1, P1, Self::E, Self::LWC, Self::EC>,
        b: CmtResult<V2, P2, Self::E, Self::LWC, Self::EC>,
        c: CmtResult<V3, P3, Self::E, Self::LWC, Self::EC>,
    ) -> CmtResult<(Self::V, V1, V2, V3), (), Self::E, Self::LWC, Vec<Self::E>>
    where
        Self::EC: IntoZeroOrMore<Vec<Self::E>> + IntoIterator<Item = Self::E>,
        Self::LWC: Semigroup,
        Self: CommutativeResultExt,
    {
        self.zip3_cmt(a, b)
            .zip_cmt(c.repack())
            .map_ok_value(|((ax, bx, cx), dx)| (ax, bx, cx, dx))
    }

    /// Combine five commutative results.
    fn zip5_cmt<V1, V2, V3, V4, P1, P2, P3, P4>(
        self,
        a: CmtResult<V1, P1, Self::E, Self::LWC, Self::EC>,
        b: CmtResult<V2, P2, Self::E, Self::LWC, Self::EC>,
        c: CmtResult<V3, P3, Self::E, Self::LWC, Self::EC>,
        d: CmtResult<V4, P4, Self::E, Self::LWC, Self::EC>,
    ) -> CmtResult<(Self::V, V1, V2, V3, V4), (), Self::E, Self::LWC, Vec<Self::E>>
    where
        Self::EC: IntoZeroOrMore<Vec<Self::E>> + IntoIterator<Item = Self::E>,
        Self::LWC: Semigroup,
        Self: CommutativeResultExt,
    {
        self.zip4_cmt(a, b, c)
            .zip_cmt(d.repack())
            .map_ok_value(|((ax, bx, cx, dx), ex)| (ax, bx, cx, dx, ex))
    }

    /// Combine six commutative results.
    fn zip6_cmt<V1, V2, V3, V4, V5, P1, P2, P3, P4, P5>(
        self,
        a: CmtResult<V1, P1, Self::E, Self::LWC, Self::EC>,
        b: CmtResult<V2, P2, Self::E, Self::LWC, Self::EC>,
        c: CmtResult<V3, P3, Self::E, Self::LWC, Self::EC>,
        d: CmtResult<V4, P4, Self::E, Self::LWC, Self::EC>,
        e: CmtResult<V5, P5, Self::E, Self::LWC, Self::EC>,
    ) -> CmtResult<(Self::V, V1, V2, V3, V4, V5), (), Self::E, Self::LWC, Vec<Self::E>>
    where
        Self::EC: IntoZeroOrMore<Vec<Self::E>> + IntoIterator<Item = Self::E>,
        Self::LWC: Semigroup,
        Self: CommutativeResultExt,
    {
        self.zip5_cmt(a, b, c, d)
            .zip_cmt(e.repack())
            .map_ok_value(|((ax, bx, cx, dx, ex), fx)| (ax, bx, cx, dx, ex, fx))
    }

    /// Combine two deferred results.
    ///
    /// Ok and Error values will be wrapped in a tuple. Inputs must be
    /// deferred to ensure value types match between Ok and Error branches.
    ///
    /// Inners for warnings and errors must be the same. The former must
    /// be a semigroup (which here means Option<T> must be converted to Vec<T>
    /// prior to calling). The latter will be converted to a Vec<T> since
    /// there could be more than one errors.
    fn zip_def<V1>(
        self,
        a: Deferred<V1, Self::E, Self::LWC, Self::EC>,
    ) -> Deferred<(Self::V, V1), Self::E, Self::LWC, Vec<Self::E>>
    where
        Self::EC: IntoZeroOrMore<Vec<Self::E>> + IntoIterator<Item = Self::E>,
        Self::LWC: Semigroup,
        Self: DeferredExt,
    {
        match (self.into_result(), a) {
            (Ok(ax), Ok(bx)) => Ok(ax.zip_with(bx, |x, y| (x, y))),
            (Ok(ax), Err(bx)) => Err(ax.with_failure(bx, |x, y| (x, y)).repack_errors()),
            (Err(ax), Ok(bx)) => Err(ax.with_success(bx, |x, y| (x, y)).repack_errors()),
            (Err(ax), Err(bx)) => Err(ax.zip_with(bx, |x, y| (x, y))),
        }
    }

    /// Combine three deferred results.
    fn zip3_def<V1, V2>(
        self,
        a: Deferred<V1, Self::E, Self::LWC, Self::EC>,
        b: Deferred<V2, Self::E, Self::LWC, Self::EC>,
    ) -> Deferred<(Self::V, V1, V2), Self::E, Self::LWC, Vec<Self::E>>
    where
        Self::EC: IntoZeroOrMore<Vec<Self::E>> + IntoIterator<Item = Self::E>,
        Self::LWC: Semigroup,
        Self: DeferredExt,
    {
        self.zip_def(a)
            .zip_def(b.repack())
            .map_def_value(|((ax, bx), cx)| (ax, bx, cx))
    }

    /// Combine four deferred results.
    fn zip4_def<V1, V2, V3>(
        self,
        a: Deferred<V1, Self::E, Self::LWC, Self::EC>,
        b: Deferred<V2, Self::E, Self::LWC, Self::EC>,
        c: Deferred<V3, Self::E, Self::LWC, Self::EC>,
    ) -> Deferred<(Self::V, V1, V2, V3), Self::E, Self::LWC, Vec<Self::E>>
    where
        Self::EC: IntoZeroOrMore<Vec<Self::E>> + IntoIterator<Item = Self::E>,
        Self::LWC: Semigroup,
        Self: DeferredExt,
    {
        self.zip3_def(a, b)
            .zip_def(c.repack())
            .map_def_value(|((ax, bx, cx), dx)| (ax, bx, cx, dx))
    }

    /// Combine five deferred results.
    fn zip5_def<V1, V2, V3, V4>(
        self,
        a: Deferred<V1, Self::E, Self::LWC, Self::EC>,
        b: Deferred<V2, Self::E, Self::LWC, Self::EC>,
        c: Deferred<V3, Self::E, Self::LWC, Self::EC>,
        d: Deferred<V4, Self::E, Self::LWC, Self::EC>,
    ) -> Deferred<(Self::V, V1, V2, V3, V4), Self::E, Self::LWC, Vec<Self::E>>
    where
        Self::EC: IntoZeroOrMore<Vec<Self::E>> + IntoIterator<Item = Self::E>,
        Self::LWC: Semigroup,
        Self: DeferredExt,
    {
        self.zip4_def(a, b, c)
            .zip_def(d.repack())
            .map_def_value(|((ax, bx, cx, dx), ex)| (ax, bx, cx, dx, ex))
    }

    /// Combine six deferred results.
    fn zip6_def<V1, V2, V3, V4, V5>(
        self,
        a: Deferred<V1, Self::E, Self::LWC, Self::EC>,
        b: Deferred<V2, Self::E, Self::LWC, Self::EC>,
        c: Deferred<V3, Self::E, Self::LWC, Self::EC>,
        d: Deferred<V4, Self::E, Self::LWC, Self::EC>,
        e: Deferred<V5, Self::E, Self::LWC, Self::EC>,
    ) -> Deferred<(Self::V, V1, V2, V3, V4, V5), Self::E, Self::LWC, Vec<Self::E>>
    where
        Self::EC: IntoZeroOrMore<Vec<Self::E>> + IntoIterator<Item = Self::E>,
        Self::LWC: Semigroup,
        Self: DeferredExt,
    {
        self.zip5_def(a, b, c, d)
            .zip_def(e.repack())
            .map_def_value(|((ax, bx, cx, dx, ex), fx)| (ax, bx, cx, dx, ex, fx))
    }
}

impl<V, P, E, LWC, RWC, EC> LogResultExt for LogResult<V, P, E, LWC, RWC, EC> {
    type V = V;
    type P = P;
    type E = E;
    type LWC = LWC;
    type RWC = RWC;
    type EC = EC;
}

/// Constraint for non-commutative results.
///
/// Warnings on the Error side must be an empty set.
pub trait NonCommutativeResultExt: LogResultExt<RWC = NeverValue<()>> {}

impl<V, P, E, WC, EC> NonCommutativeResultExt for NonCmtResult<V, P, E, WC, EC> {}

/// Constraint for commutative results.
///
/// Warning cardinality and type must match between Ok and Error sides
pub trait CommutativeResultExt: LogResultExt<RWC = <Self as LogResultExt>::LWC> {}

impl<V, P, E, WC, EC> CommutativeResultExt for CmtResult<V, P, E, WC, EC> {}

/// Constraint for deferred results.
///
/// In addition to being commutative, value must match between Ok and Error.
pub trait DeferredExt: CommutativeResultExt<V = <Self as LogResultExt>::P> {}

impl<V, E, WC, EC> DeferredExt for Deferred<V, E, WC, EC> {}

/// Constraint for fungible results.
///
/// Error and warning must have the same cardinality and type.
pub trait FungibleExt:
    LogResultExt<LWC = <<Self as LogResultExt>::EC as FungibleError<<Self as LogResultExt>::E>>::Warn>
where
    Self::EC: FungibleError<Self::E>,
{
}

impl<V, P, E, RWC, EC: FungibleError<E>> FungibleExt for LogResult<V, P, E, EC::Warn, RWC, EC> {}

/// Constraint for results with no warnings.
///
/// In addition to be non-commutative, warnings on the Ok side must be an empty
/// set.
pub trait NowarnExt: NonCommutativeResultExt<LWC = NeverValue<()>> {}

impl<V, P, E, EC> NowarnExt for NowarnResult<V, P, E, EC> {}

/// Constraint for results which can be resolved.
///
/// The only requirement is that there must only be one error, which will be
/// used to map to a regular result.
pub trait ResolvableExt: LogResultExt<P = (), EC = NeverValue<<Self as LogResultExt>::E>> {}

impl<V, E, LWC, RWC> ResolvableExt for LogResult<V, (), E, LWC, RWC, NeverValue<E>> {}

/// Monoid-ically combine commutative results.
///
/// Ok values will be collected and returned as a single vector upon success.
/// Presence of any Error will cause Error to be returned. In any case,
/// warnings and errors as applicable will appended in order and returned.
///
/// The wrapper for warning must be a semigroup and wrapper for error must be
/// extendable since it might hold more than one error.
pub(crate) trait CmtResultIter<T, P, E, WC, EC>:
    Iterator<Item = CmtResult<T, P, E, WC, EC>> + Sized
{
    fn mappend_cmt(mut self) -> CmtResult<Vec<T>, (), E, WC, EC>
    where
        WC: Semigroup + Default,
        EC: Extend<E> + IntoIterator<Item = E>,
    {
        let mut left_vs = vec![];
        let mut ws = WC::default();
        let mut error_head = None;
        for x in self.by_ref() {
            match x {
                Ok(y) => {
                    left_vs.push(y.value);
                    ws = ws.concat(y.warnings);
                }
                Err(y) => {
                    error_head = Some(y);
                    break;
                }
            }
        }
        if let Some(h) = error_head {
            let mut es = h.errors;
            for x in self {
                match x {
                    Ok(y) => {
                        ws = ws.concat(y.warnings);
                    }
                    Err(y) => {
                        ws = ws.concat(y.warnings);
                        es.extend(y.errors);
                    }
                }
            }
            Err(Failure::new(ws, es, ()))
        } else {
            Ok(Success::new(left_vs, ws))
        }
    }
}

impl<I, V, P, E, WC, EC> CmtResultIter<V, P, E, WC, EC> for I where
    I: Iterator<Item = CmtResult<V, P, E, WC, EC>>
{
}

/// Monoid-ically combine deferred results.
///
/// Values from Ok or Error will be collected and returned in a single vector
/// independent of the presence of warnings or errors.
///
/// The wrapper for warning must be a semigroup and wrapper for error must be
/// extendable since it might hold more than one error.
pub(crate) trait DeferredIter<T, E, WC, EC>:
    Iterator<Item = Deferred<T, E, WC, EC>> + Sized
{
    // TODO not DRY
    fn mappend_def(mut self) -> Deferred<Vec<T>, E, WC, EC>
    where
        WC: Semigroup + Default,
        EC: Extend<E> + IntoIterator<Item = E>,
    {
        let mut vs = vec![];
        let mut ws = WC::default();
        let mut error_head = None;
        for x in self.by_ref() {
            match x {
                Ok(y) => {
                    vs.push(y.value);
                    ws = ws.concat(y.warnings);
                }
                Err(y) => {
                    error_head = Some(y);
                    break;
                }
            }
        }
        if let Some(h) = error_head {
            vs.push(h.value);
            let mut es = h.errors;
            for x in self {
                match x {
                    Ok(y) => {
                        vs.push(y.value);
                        ws = ws.concat(y.warnings);
                    }
                    Err(y) => {
                        vs.push(y.value);
                        ws = ws.concat(y.warnings);
                        es.extend(y.errors);
                    }
                }
            }
            Err(Failure::new(ws, es, vs))
        } else {
            Ok(Success::new(vs, ws))
        }
    }

    fn mappend_def_void(self) -> Deferred<(), E, WC, EC>
    where
        WC: Semigroup + Default,
        EC: Extend<E> + IntoIterator<Item = E>,
    {
        self.mappend_def().set_def_value(())
    }
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

impl<I, V, E, WC, EC> DeferredIter<V, E, WC, EC> for I where
    I: Iterator<Item = Deferred<V, E, WC, EC>>
{
}

impl<P, E, WC, EC> From<io::Error> for Failure<P, ImpureError<E>, WC, EC>
where
    P: Default,
    WC: Default,
    EC: Default,
{
    fn from(value: io::Error) -> Self {
        Self::new_from_one(value.into(), P::default())
    }
}

impl<P, E, WC, EC> From<E> for Failure<P, E, WC, EC>
where
    P: Default,
    WC: Default,
    EC: Default,
{
    fn from(value: E) -> Self {
        Self::new_from_one(value, P::default())
    }
}

impl<E: fmt::Display, S: fmt::Display> fmt::Display for ErrorSummary<E, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        writeln!(f, "Toplevel Error: {}", self.summary)?;
        let es = &self.errors;
        for e in iter::once(&es.head).chain(es.tail.iter()) {
            for l in e.to_string().lines() {
                writeln!(f, "  {l}")?;
            }
        }
        Ok(())
    }
}

#[cfg(feature = "python")]
mod python {
    use crate::python::exceptions::PyreflowException;

    use super::{ErrorSummary, ImpureError};

    use pyo3::prelude::*;
    use std::fmt::Display;

    impl<T: Into<Self>> From<ImpureError<T>> for PyErr {
        fn from(value: ImpureError<T>) -> Self {
            match value {
                ImpureError::Pure(e) => e.into(),
                // This should be an OSError of some kind
                ImpureError::IO(e) => e.into(),
            }
        }
    }

    impl<E: Display, S: Display> From<ErrorSummary<E, S>> for PyErr {
        fn from(value: ErrorSummary<E, S>) -> Self {
            PyreflowException::new_err(value.to_string())
        }
    }
}
