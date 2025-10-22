use crate::config::SharedConfig;
use crate::text::optional::NeverValue;

use derive_new::new;
use std::convert::Infallible;
use std::io;
use std::iter;
use std::marker::PhantomData;
use std::mem;
use std::vec;
use thiserror::Error;

// TODO maybe add wrapper type for Success which roughly means "result with
// issues that may be errors"

pub type RecoverableErrorResult<V, I> = ErrorResult<V, (), I>;
pub type RecoverableErrorsResult<V, I> = ErrorsResult<V, (), I>;

pub type ErrorResult<V, P, E> = NowarnResult<V, P, E, NullFamily>;
pub type ErrorsResult<V, P, E> = NowarnResult<V, P, E, VecFamily>;

pub type IOErrorResult<V, P, E> = ErrorResult<V, P, ImpureError<E>>;
pub type IOErrorsResult<V, P, E> = ErrorsResult<V, P, ImpureError<E>>;

pub type FungibleErrorResult<V, P, E> = NonCmtFungibleResult<V, P, E, NullFamily>;
pub type FungibleErrorsResult<V, P, E> = NonCmtFungibleResult<V, P, E, VecFamily>;

pub type WarningOrErrorResult<V, P, W, E> = NonCmtResult<V, P, W, E, OptFamily, NullFamily>;
pub type WarningsOrErrorResult<V, P, W, E> = NonCmtResult<V, P, W, E, VecFamily, NullFamily>;
pub type WarningOrErrorsResult<V, P, W, E> = NonCmtResult<V, P, W, E, OptFamily, VecFamily>;
pub type WarningsOrErrorsResult<V, P, W, E> = NonCmtResult<V, P, W, E, VecFamily, VecFamily>;

pub type WarningAndErrorResult<V, P, W, E> = CmtResult<V, P, W, E, OptFamily, NullFamily>;
pub type WarningsAndErrorResult<V, P, W, E> = CmtResult<V, P, W, E, VecFamily, NullFamily>;
pub type WarningAndErrorsResult<V, P, W, E> = CmtResult<V, P, W, E, OptFamily, VecFamily>;
pub type WarningsAndErrorsResult<V, P, W, E> = CmtResult<V, P, W, E, VecFamily, VecFamily>;

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
    NonCmtResult<V, P, E, E, <EC as FungibleErrorFamily>::WarnFam, EC>;
pub type CmtFungibleResult<V, P, E, EC> =
    CmtResult<V, P, E, E, <EC as FungibleErrorFamily>::WarnFam, EC>;

pub type NowarnResult<V, P, E, EC> = CmtResult<V, P, (), E, NullFamily, EC>;

pub type Deferred<V, W, E, WC, EC> = CmtResult<V, V, W, E, WC, EC>;

pub type DeferredNowarn<V, E, EC> = NowarnResult<V, V, E, EC>;

pub type DeferredFungible<V, E, EC> = Deferred<V, E, E, <EC as FungibleErrorFamily>::WarnFam, EC>;

pub type CmtResult<V, P, W, E, WC, EC> = GenericResult<V, P, W, W, E, WC, WC, EC>;

pub type NonCmtResult<V, P, W, E, WC, EC> = GenericResult<V, P, W, (), E, WC, NullFamily, EC>;

pub type FungibleResult<V, P, W, E, RWC, EC> =
    GenericResult<V, P, E, W, E, <EC as FungibleErrorFamily>::WarnFam, RWC, EC>;

pub type GenericResult<V, P, LW, RW, E, LWC, RWC, EC> =
    Result<Success<V, LW, LWC>, Failure<P, RW, E, RWC, EC>>;

// pub struct Tentative<V, P, I, IC: ZeroOrMore>(NowarnResult<V, P, I, IC>);

#[derive(new)]
// #[new(visibility = "")]
pub struct Success<V, W, I: ZeroOrMore> {
    value: V,
    warnings: I::Wrapper<W>,
}

#[derive(new)]
pub struct Failure<P, W, E, WC: ZeroOrMore, EC: ZeroOrMore> {
    warnings: WC::Wrapper<W>,
    errors: GenNonEmpty<E, EC>,
    passthru: P,
}

#[derive(new)]
pub struct ErrorSummary<E, S> {
    summary: S,
    errors: GenNonEmpty<E, VecFamily>,
}

// TODO eventually will want to make a boxable version of this, which will
// likely involve a higher-kinded type which is either a box or dumb newtype
// wrapper. tail doesn't need box because the only two values (for now) are
// vec and nevervalue, and the former is heap anyways.
#[derive(new)]
pub struct GenNonEmpty<X, C: ZeroOrMore> {
    head: X,
    tail: C::Wrapper<X>,
}

#[derive(Debug, Error)]
pub enum ImpureError<E> {
    #[error("IO error: {0}")]
    IO(#[from] io::Error),
    #[error("{0}")]
    Pure(E),
}

pub struct OptFamily;

pub struct VecFamily;

pub struct NullFamily;

// TODO clean this up, this is basically a functor (the map thing) plus other
// stuff, kinda a combo of applicative (the "one" stuff) and traversible (the
// iterator stuff)
pub(crate) trait ZeroOrMore: Sized {
    type Wrapper<T>: IntoIterator<Item = T> + Default;
    type IterOne<X>: Iterator<Item = X>;

    fn map<F, X, Y>(t: Self::Wrapper<X>, f: F) -> Self::Wrapper<Y>
    where
        F: Fn(X) -> Y;

    fn try_into_one_and_iter<X>(x: Self::Wrapper<X>) -> Option<(X, Self::IterOne<X>)>;

    fn try_into_one_or_more<X>(x: Self::Wrapper<X>) -> Option<GenNonEmpty<X, Self>>;
}

pub(crate) trait IntoZeroOrMore<Other: ZeroOrMore>: ZeroOrMore {
    fn into_zero_or_more<X>(x: Self::Wrapper<X>) -> Other::Wrapper<X>;
}

pub(crate) trait Concatable {
    type Out;
    fn concat(self, other: Self) -> Self::Out;
}

pub(crate) trait Semigroup: Concatable<Out = Self> {}

pub trait CanHoldOne: ZeroOrMore {
    fn wrap<X>(x: X) -> Self::Wrapper<X>;
}

pub trait FungibleErrorFamily: ZeroOrMore {
    type WarnFam: ZeroOrMore;

    fn errors_to_warnings<E>(
        errors: GenNonEmpty<E, Self>,
    ) -> <Self::WarnFam as ZeroOrMore>::Wrapper<E>;
}

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

impl ZeroOrMore for OptFamily {
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

impl<E, EI: ZeroOrMore> IntoIterator for GenNonEmpty<E, EI> {
    type Item = E;
    type IntoIter = iter::Chain<iter::Once<E>, <EI::Wrapper<E> as IntoIterator>::IntoIter>;
    fn into_iter(self) -> Self::IntoIter {
        iter::once(self.head).chain(self.tail)
    }
}

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

impl IntoZeroOrMore<OptFamily> for NullFamily {
    fn into_zero_or_more<X>(_: Self::Wrapper<X>) -> Option<X> {
        None
    }
}

impl IntoZeroOrMore<VecFamily> for OptFamily {
    fn into_zero_or_more<X>(x: Self::Wrapper<X>) -> Vec<X> {
        x.into_iter().collect()
    }
}

impl CanHoldOne for OptFamily {
    fn wrap<X>(x: X) -> Self::Wrapper<X> {
        Some(x)
    }
}

impl CanHoldOne for VecFamily {
    fn wrap<X>(x: X) -> Self::Wrapper<X> {
        vec![x]
    }
}

impl<T> Concatable for NeverValue<T> {
    type Out = NeverValue<T>;
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
    type Out = Vec<T>;
    fn concat(mut self, other: Self) -> Self::Out {
        self.extend(other);
        self
    }
}

impl<T> Semigroup for Vec<T> {}

impl<T> Semigroup for NeverValue<T> {}

impl FungibleErrorFamily for VecFamily {
    type WarnFam = VecFamily;

    fn errors_to_warnings<E>(errors: GenNonEmpty<E, Self>) -> Vec<E> {
        errors.into_iter().collect()
    }
}

impl FungibleErrorFamily for NullFamily {
    type WarnFam = OptFamily;

    fn errors_to_warnings<E>(errors: GenNonEmpty<E, Self>) -> Option<E> {
        Some(errors.head)
    }
}

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

impl<V, W, WC: ZeroOrMore> Success<V, W, WC> {
    pub(crate) fn new1(value: V) -> Self {
        Self::new(value, WC::Wrapper::<W>::default())
    }

    pub(crate) fn repack<WIF>(self) -> Success<V, W, WIF>
    where
        WC: IntoZeroOrMore<WIF>,
        WIF: ZeroOrMore,
    {
        Success::new(self.value, WC::into_zero_or_more(self.warnings))
    }

    pub(crate) fn value_into<U: From<V>>(self) -> Success<U, W, WC> {
        self.map_value(Into::into)
    }

    pub(crate) fn map_value<F: FnOnce(V) -> X, X>(self, f: F) -> Success<X, W, WC> {
        Success::new(f(self.value), self.warnings)
    }

    pub(crate) fn warnings_into<X: From<W>>(self) -> Success<V, X, WC> {
        self.map_warnings(Into::into)
    }

    pub(crate) fn map_warnings<F: Fn(W) -> Wf, Wf>(self, f: F) -> Success<V, Wf, WC> {
        Success::new(self.value, WC::map(self.warnings, f))
    }

    pub(crate) fn set_warnings<Wf, WCf>(self, ws: WCf::Wrapper<Wf>) -> Success<V, Wf, WCf>
    where
        WCf: ZeroOrMore,
    {
        Success::new(self.value, ws)
    }

    pub(crate) fn push_warning(&mut self, w: W)
    where
        WC::Wrapper<W>: Extend<W>,
    {
        self.warnings.extend(iter::once(w));
    }

    pub(crate) fn extend_warnings(&mut self, ws: impl IntoIterator<Item = W>)
    where
        WC::Wrapper<W>: Extend<W>,
    {
        self.warnings.extend(ws);
    }

    pub(crate) fn eval_warning<F>(&mut self, f: F)
    where
        F: FnOnce(&V) -> Option<W>,
        WC::Wrapper<W>: Extend<W>,
    {
        if let Some(e) = f(&self.value) {
            self.push_warning(e);
        }
    }

    pub(crate) fn and_then<F, Vf>(self, f: F) -> Success<Vf, W, WC>
    where
        F: FnOnce(V) -> Success<Vf, W, WC>,
        WC::Wrapper<W>: Semigroup,
    {
        let other = f(self.value);
        Success::new(other.value, self.warnings.concat(other.warnings))
    }

    pub(crate) fn and_maybe<F, ToV, P, E, WCf, EC>(self, f: F) -> CmtResult<ToV, P, W, E, WCf, EC>
    where
        F: FnOnce(V) -> CmtResult<ToV, P, W, E, WC, EC>,
        EC: ZeroOrMore,
        WCf: ZeroOrMore,
        WC::Wrapper<W>: Concatable<Out = WCf::Wrapper<W>>,
    {
        match f(self.value) {
            Ok(s) => {
                let ws = self.warnings.concat(s.warnings);
                Ok(Success::new(s.value, ws))
            }
            Err(e) => {
                let ws = self.warnings.concat(e.warnings);
                Err(Failure::new(ws, e.errors, e.passthru))
            }
        }
    }

    pub(crate) fn fail<E, EC>(self, errors: GenNonEmpty<E, EC>) -> Failure<V, W, E, WC, EC>
    where
        EC: ZeroOrMore,
    {
        Failure::new(self.warnings, errors, self.value)
    }

    pub(crate) fn with_failure<F, P, PF, E, WCf, EC>(
        self,
        other: Failure<P, W, E, WC, EC>,
        f: F,
    ) -> Failure<PF, W, E, WCf, EC>
    where
        F: FnOnce(V, P) -> PF,
        WCf: ZeroOrMore,
        EC: ZeroOrMore,
        WC::Wrapper<W>: Concatable<Out = WCf::Wrapper<W>>,
    {
        let ws = self.warnings.concat(other.warnings);
        Failure::new(ws, other.errors, f(self.value, other.passthru))
    }

    pub(crate) fn zip_with<F, V0, VF, WCf>(
        self,
        other: Success<V0, W, WC>,
        f: F,
    ) -> Success<VF, W, WCf>
    where
        F: FnOnce(V, V0) -> VF,
        WCf: ZeroOrMore,
        WC::Wrapper<W>: Concatable<Out = WCf::Wrapper<W>>,
    {
        let ws = self.warnings.concat(other.warnings);
        Success::new(f(self.value, other.value), ws)
    }

    fn aggregate_warnings<F, Wf>(self, f: F) -> Success<V, Wf, OptFamily>
    where
        F: FnOnce(WC::Wrapper<W>) -> Wf,
    {
        Success::new(self.value, Some(f(self.warnings)))
    }

    fn remove_warnings(self) -> Success<V, (), NullFamily> {
        Success::new1(self.value)
    }

    fn warnings_to_errors<E, F, G, EC, P>(self, f: F, g: G) -> NowarnResult<V, P, E, EC>
    where
        F: Fn(W) -> E,
        G: FnOnce(V) -> P,
        EC: ZeroOrMore,
        WC: IntoZeroOrMore<EC>,
    {
        match WC::try_into_one_or_more(self.warnings) {
            None => Ok(Success::new1(self.value)),
            Some(ws) => Err(Failure::new_from_many(ws.map(f).repack(), g(self.value))),
        }
    }

    fn resolve<F, X>(self, f: F) -> (V, X)
    where
        F: FnOnce(WC::Wrapper<W>) -> X,
    {
        (self.value, f(self.warnings))
    }
}

impl<V> Success<V, (), NullFamily> {
    fn lift_simple<W, WC: ZeroOrMore>(self) -> Success<V, W, WC> {
        Success::new1(self.value)
    }
}

impl<W, E, P, WC: ZeroOrMore, EC: ZeroOrMore> Failure<P, W, E, WC, EC> {
    pub(crate) fn new_from_one(error: E, passthru: P) -> Self {
        Self::new_from_many(GenNonEmpty::new1(error), passthru)
    }

    pub(crate) fn new_from_many(errors: GenNonEmpty<E, EC>, passthru: P) -> Self {
        Self::new(WC::Wrapper::<W>::default(), errors.into(), passthru)
    }

    fn repack_warnings<WIF>(self) -> Failure<P, W, E, WIF, EC>
    where
        WC: IntoZeroOrMore<WIF>,
        WIF: ZeroOrMore,
    {
        Failure::new(
            WC::into_zero_or_more(self.warnings),
            self.errors,
            self.passthru,
        )
    }

    fn repack_errors<ECf>(self) -> Failure<P, W, E, WC, ECf>
    where
        EC: IntoZeroOrMore<ECf>,
        ECf: ZeroOrMore,
    {
        Failure::new(self.warnings, self.errors.repack(), self.passthru)
    }

    fn map_warnings<F, Wf>(self, f: F) -> Failure<P, Wf, E, WC, EC>
    where
        F: Fn(W) -> Wf,
    {
        Failure {
            warnings: WC::map(self.warnings, f),
            errors: self.errors,
            passthru: self.passthru,
        }
    }

    fn map_errors<F, Ef>(self, f: F) -> Failure<P, W, Ef, WC, EC>
    where
        F: Fn(E) -> Ef,
    {
        Failure::new(self.warnings, self.errors.map(f), self.passthru)
    }

    fn map_passthru<F, ToP>(self, f: F) -> Failure<ToP, W, E, WC, EC>
    where
        F: FnOnce(P) -> ToP,
    {
        Failure::new(self.warnings, self.errors, f(self.passthru))
    }

    fn set_warnings<Wf, WCf>(self, ws: WCf::Wrapper<Wf>) -> Failure<P, Wf, E, WCf, EC>
    where
        WCf: ZeroOrMore,
    {
        Failure::new(ws, self.errors, self.passthru)
    }

    fn push_warning(&mut self, w: W)
    where
        WC::Wrapper<W>: Extend<W>,
    {
        self.warnings.extend(iter::once(w));
    }

    fn eval_warning<F>(&mut self, f: F)
    where
        F: FnOnce(&P) -> Option<W>,
        WC::Wrapper<W>: Extend<W>,
    {
        if let Some(e) = f(&self.passthru) {
            self.push_warning(e);
        }
    }

    fn extend_warnings(&mut self, ws: impl IntoIterator<Item = W>)
    where
        WC::Wrapper<W>: Extend<W>,
    {
        self.warnings.extend(ws);
    }

    fn push_error(&mut self, e: E)
    where
        EC::Wrapper<E>: Extend<E>,
    {
        self.errors.extend(iter::once(e));
    }

    fn extend_errors(&mut self, es: impl IntoIterator<Item = E>)
    where
        EC::Wrapper<E>: Extend<E>,
    {
        self.errors.extend(es);
    }

    fn eval_error<F>(&mut self, f: F)
    where
        F: FnOnce(&P) -> Option<E>,
        EC::Wrapper<E>: Extend<E>,
    {
        if let Some(e) = f(&self.passthru) {
            self.push_error(e);
        }
    }

    fn with_passthru<F, V, Pf, WCf>(mut self, f: F) -> CmtResult<V, Pf, W, E, WCf, EC>
    where
        F: FnOnce(P) -> CmtResult<V, Pf, W, E, WC, EC>,
        WCf: ZeroOrMore,
        WC::Wrapper<W>: Concatable<Out = WCf::Wrapper<W>>,
        EC::Wrapper<E>: Extend<E>,
    {
        match f(self.passthru) {
            Ok(s) => {
                let ws = self.warnings.concat(s.warnings);
                Ok(Success::new(s.value, ws))
            }
            Err(e) => {
                let ws = self.warnings.concat(e.warnings);
                self.errors.extend(e.errors);
                Err(Failure::new(ws, self.errors, e.passthru))
            }
        }
    }

    fn zip_with<F, P0, Pf, WCf, ECf>(
        self,
        other: Failure<P0, W, E, WC, EC>,
        f: F,
    ) -> Failure<Pf, W, E, WCf, ECf>
    where
        F: FnOnce(P, P0) -> Pf,
        WCf: ZeroOrMore,
        ECf: ZeroOrMore,
        EC: IntoZeroOrMore<ECf>,
        WC::Wrapper<W>: Concatable<Out = WCf::Wrapper<W>>,
        ECf::Wrapper<E>: Extend<E>,
    {
        let ws = self.warnings.concat(other.warnings);
        let mut es = self.errors.into_zero_or_more();
        es.extend(other.errors);
        Failure::new(ws, es, f(self.passthru, other.passthru))
    }

    fn aggregate_warnings<F, Wf>(self, f: F) -> Failure<P, Wf, E, OptFamily, EC>
    where
        F: FnOnce(WC::Wrapper<W>) -> Wf,
    {
        Failure::new(Some(f(self.warnings)), self.errors, self.passthru)
    }

    fn aggregate_errors<F, EF>(self, f: F) -> Failure<P, W, EF, WC, NullFamily>
    where
        F: FnOnce(GenNonEmpty<E, EC>) -> EF,
    {
        let es = GenNonEmpty::new1(f(self.errors));
        Failure::new(self.warnings, es, self.passthru)
    }

    fn summarize_errors<S>(self, summary: S) -> Failure<P, W, ErrorSummary<E, S>, WC, NullFamily>
    where
        EC: IntoZeroOrMore<VecFamily>,
    {
        self.aggregate_errors(|es| ErrorSummary::new(summary, es.into_zero_or_more()))
    }

    fn with_success<F, V, PF, WCf>(
        self,
        other: Success<V, W, WC>,
        f: F,
    ) -> Failure<PF, W, E, WCf, EC>
    where
        F: FnOnce(P, V) -> PF,
        WCf: ZeroOrMore,
        WC::Wrapper<W>: Concatable<Out = WCf::Wrapper<W>>,
    {
        let ws = self.warnings.concat(other.warnings);
        Failure::new(ws, self.errors, f(self.passthru, other.value))
    }

    fn remove_warnings(self) -> Failure<P, (), E, NullFamily, EC> {
        Failure::new(NeverValue::default(), self.errors, self.passthru)
    }

    fn warnings_to_errors<F>(mut self, f: F) -> Failure<P, (), E, NullFamily, EC>
    where
        F: Fn(W) -> E,
        EC::Wrapper<E>: Extend<E>,
    {
        self.errors.extend(WC::map(self.warnings, f));
        Failure::new_from_many(self.errors, self.passthru)
    }
}

impl<P, E, EC: ZeroOrMore> Failure<P, (), E, NullFamily, EC> {
    fn lift_simple<W, WC: ZeroOrMore>(self) -> Failure<P, W, E, WC, EC> {
        Failure::new_from_many(self.errors, self.passthru)
    }
}

// impl<W, E, P, WC: ZeroOrMore> Failure<P, W, E, WC, NullFamily> {
//     fn resolve<F, G, X, Y>(self, f: F, g: G) -> (X, Y)
//     where
//         F: FnOnce(WC::Wrapper<W>) -> X,
//         G: FnOnce(E) -> Y,
//     {
//         (f(self.warnings), g(self.errors.head))
//     }
// }

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
    fn collect(xs: impl IntoIterator<Item = T>) -> Option<Self>
    where
        C::Wrapper<T>: Extend<T>,
    {
        let mut it = xs.into_iter();
        it.by_ref().next().map(|x0| {
            let mut ret = Self::new1(x0);
            ret.extend(it);
            ret
        })
    }

    fn new1(x: T) -> Self {
        Self::new(x.into(), C::Wrapper::<T>::default())
    }

    fn map<X, F>(self, f: F) -> GenNonEmpty<X, C>
    where
        F: Fn(T) -> X,
    {
        GenNonEmpty::new(f(self.head).into(), C::map(self.tail, f))
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

    fn into_zero_or_more<CF>(self) -> GenNonEmpty<T, CF>
    where
        CF: ZeroOrMore,
        C: IntoZeroOrMore<CF>,
    {
        GenNonEmpty::new(self.head, C::into_zero_or_more(self.tail))
    }
}

impl<E, EI: ZeroOrMore> From<(E, EI::Wrapper<E>)> for GenNonEmpty<E, EI> {
    fn from(value: (E, EI::Wrapper<E>)) -> Self {
        Self::new(value.0.into(), value.1)
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

    fn transpose_generic_result<V, P, LW, RW, E, LWC, RWC, EC>(
        self,
    ) -> GenericResult<Option<V>, P, LW, RW, E, LWC, RWC, EC>
    where
        Self: OptionExt<Inner = GenericResult<V, P, LW, RW, E, LWC, RWC, EC>>,
        LWC: ZeroOrMore,
        RWC: ZeroOrMore,
        EC: ZeroOrMore,
    {
        self.into_option()
            .map_or(Result::new_ok(None), |x| x.map_value(Some))
    }
}

impl<T> OptionExt for Option<T> {
    type Inner = T;

    fn into_option(self) -> Option<T> {
        self
    }
}

pub trait ResultExt: Sized {
    type Ok;
    type Error;

    fn into_result(self) -> Result<Self::Ok, Self::Error>;

    fn as_result(&self) -> Result<&Self::Ok, &Self::Error>;

    fn as_result_mut(&mut self) -> Result<&mut Self::Ok, &mut Self::Error>;

    fn new_ok<P, LW, RW, LWC, RWC, EC>(
        value: Self::Ok,
    ) -> GenericResult<Self::Ok, P, LW, RW, Self::Error, LWC, RWC, EC>
    where
        LWC: ZeroOrMore,
        RWC: ZeroOrMore,
        EC: ZeroOrMore,
    {
        Ok(Success::new1(value))
    }

    fn new_ok_def<P, LW, RW, LWC, RWC, EC>(
    ) -> GenericResult<Self::Ok, P, LW, RW, Self::Error, LWC, RWC, EC>
    where
        Self::Ok: Default,
        LWC: ZeroOrMore,
        RWC: ZeroOrMore,
        EC: ZeroOrMore,
    {
        Self::new_ok(Self::Ok::default())
    }

    fn new_err1<LW, RW, LWC, RWC, EC>(
        error: Self::Error,
    ) -> GenericResult<Self::Ok, (), LW, RW, Self::Error, LWC, RWC, EC>
    where
        LWC: ZeroOrMore,
        RWC: ZeroOrMore,
        EC: ZeroOrMore,
    {
        Err(Failure::new_from_one(error, ()))
    }

    // TODO generic input?
    fn new_err<LW, RW, LWC, RWC, EC>(
        error: GenNonEmpty<Self::Error, EC>,
    ) -> GenericResult<Self::Ok, (), LW, RW, Self::Error, LWC, RWC, EC>
    where
        LWC: ZeroOrMore,
        RWC: ZeroOrMore,
        EC: ZeroOrMore,
    {
        Err(Failure::new_from_many(error, ()))
    }

    fn new_err_from_iter<I, LW, RW, LWC, RWC, EC>(
        errors: I,
        default: Self::Ok,
    ) -> GenericResult<Self::Ok, (), LW, RW, Self::Error, LWC, RWC, EC>
    where
        I: IntoIterator<Item = Self::Error>,
        LWC: ZeroOrMore,
        RWC: ZeroOrMore,
        EC: ZeroOrMore,
        EC::Wrapper<Self::Error>: Extend<Self::Error>,
    {
        GenNonEmpty::collect(errors).map_or(Result::new_ok(default), Result::new_err)
    }

    fn new_non_fungible<P, LW, RW, LWC, RWC, EC>(
        value: Self::Ok,
        default: P,
        error: Self::Error,
        is_error: bool,
    ) -> GenericResult<Self::Ok, P, LW, RW, Self::Error, LWC, RWC, EC>
    where
        LWC: ZeroOrMore,
        RWC: ZeroOrMore,
        EC: ZeroOrMore,
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
    ) -> FungibleResult<Self::Ok, P, W, Self::Error, RWC, EC>
    where
        RWC: ZeroOrMore,
        EC: FungibleErrorFamily,
        EC::WarnFam: CanHoldOne,
    {
        if is_error {
            Err(Failure::new_from_one(error, default))
        } else {
            Ok(Success::new(value, EC::WarnFam::wrap(error)))
        }
    }

    fn new_deferred_fungible<W, RWC, EC>(
        value: Self::Ok,
        error: Self::Error,
        is_error: bool,
    ) -> FungibleResult<Self::Ok, Self::Ok, W, Self::Error, RWC, EC>
    where
        RWC: ZeroOrMore,
        EC: FungibleErrorFamily,
        EC::WarnFam: CanHoldOne,
    {
        if is_error {
            Err(Failure::new_from_one(error, value))
        } else {
            Ok(Success::new(value, EC::WarnFam::wrap(error)))
        }
    }

    fn into_nowarn1(self) -> NowarnResult<Self::Ok, (), Self::Error, NullFamily> {
        self.into_generic()
    }

    fn into_nowarn(self) -> NowarnResult<Self::Ok, (), Self::Error, VecFamily> {
        self.into_generic()
    }

    fn into_warn1(self) -> NowarnResult<Self::Ok, (), Self::Error, NullFamily> {
        self.into_generic()
    }

    fn into_generic<LW, RW, LWC, RWC, EC>(
        self,
    ) -> GenericResult<Self::Ok, (), LW, RW, Self::Error, LWC, RWC, EC>
    where
        LWC: ZeroOrMore,
        RWC: ZeroOrMore,
        EC: ZeroOrMore,
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
        EC: FungibleErrorFamily,
        <EC as FungibleErrorFamily>::WarnFam: CanHoldOne,
    {
        match self.into_result() {
            Ok(s) => Ok(Success::new1(s)),
            Err(e) => {
                if is_error {
                    Err(Failure::new_from_one(e, Self::Ok::default()))
                } else {
                    Ok(Success::new(Self::Ok::default(), EC::WarnFam::wrap(e)))
                }
            }
        }
    }

    fn into_deferred_fungible_opt<EC>(
        self,
        is_error: bool,
    ) -> DeferredFungible<Option<Self::Ok>, Self::Error, EC>
    where
        EC: FungibleErrorFamily,
        <EC as FungibleErrorFamily>::WarnFam: CanHoldOne,
    {
        self.into_result()
            .map(Some)
            .into_deferred_fungible(is_error)
    }

    fn into_deferred_fungible_def<EC>(
        self,
        default: Self::Ok,
        is_error: bool,
    ) -> DeferredFungible<Self::Ok, Self::Error, EC>
    where
        EC: FungibleErrorFamily,
        <EC as FungibleErrorFamily>::WarnFam: CanHoldOne,
    {
        self.into_result()
            .into_deferred_fungible_opt(is_error)
            .map_def_value(|v| v.unwrap_or(default))
    }

    fn into_succ<P, RW, E, LWC, RWC, EC>(
        self,
    ) -> GenericResult<Self::Ok, P, Self::Error, RW, E, LWC, RWC, EC>
    where
        Self::Ok: Default,
        LWC: ZeroOrMore + CanHoldOne,
        RWC: ZeroOrMore,
        EC: ZeroOrMore,
    {
        let ret = self.into_result().map_or_else(
            |e| Success::new(Self::Ok::default(), LWC::wrap(e)),
            |s| Success::new1(s),
        );
        Ok(ret)
    }

    fn into_succ_opt<P, RW, E, LWC, RWC, EC>(
        self,
    ) -> GenericResult<Option<Self::Ok>, P, Self::Error, RW, E, LWC, RWC, EC>
    where
        LWC: ZeroOrMore + CanHoldOne,
        RWC: ZeroOrMore,
        EC: ZeroOrMore,
    {
        self.into_result().map(Some).into_succ()
    }

    fn into_succ_or<P, RW, E, LWC, RWC, EC>(
        self,
        default: Self::Ok,
    ) -> GenericResult<Self::Ok, P, Self::Error, RW, E, LWC, RWC, EC>
    where
        LWC: ZeroOrMore + CanHoldOne,
        RWC: ZeroOrMore,
        EC: ZeroOrMore,
    {
        self.into_succ_opt().map_value(|x| x.unwrap_or(default))
    }

    // TODO versions of the above that go to errors?
}

impl<V, E> ResultExt for Result<V, E> {
    type Ok = V;
    type Error = E;

    fn into_result(self) -> Result<V, E> {
        self
    }

    fn as_result(&self) -> Result<&V, &E> {
        self.as_ref()
    }

    fn as_result_mut(&mut self) -> Result<&mut V, &mut E> {
        self.as_mut()
    }
}

pub trait GenericResultExt
where
    Self: Sized
        + ResultExt<
            Ok = Success<Self::V, Self::LW, Self::LWC>,
            Error = Failure<Self::P, Self::RW, Self::E, Self::RWC, Self::EC>,
        >,
{
    type V;
    type P;
    type E;
    type LW;
    type RW;
    type LWC: ZeroOrMore;
    type RWC: ZeroOrMore;
    type EC: ZeroOrMore;

    fn recover_or<RW, RWC>(
        self,
        default: Self::V,
    ) -> FungibleResult<Self::V, (), RW, Self::E, RWC, Self::EC>
    where
        RWC: ZeroOrMore,
        Self: GenericResultExt<P = ()>,
        Self::EC: FungibleErrorFamily,
    {
        self.recover_with(
            |es| Ok(Success::new(default, Self::EC::errors_to_warnings(es))),
            |v| Ok(Success::new1(v)),
        )
    }

    fn recover_or_default<RW, RWC>(self) -> FungibleResult<Self::V, (), RW, Self::E, RWC, Self::EC>
    where
        Self::V: Default,
        RWC: ZeroOrMore,
        Self: GenericResultExt<P = ()>,
        Self::EC: FungibleErrorFamily,
    {
        self.recover_or(Self::V::default())
    }

    // TODO this function smells funny
    fn recover_with<Ferr, Fsucc, X>(self, f_err: Ferr, f_succ: Fsucc) -> X
    where
        Fsucc: FnOnce(Self::V) -> X,
        Ferr: FnOnce(GenNonEmpty<Self::E, Self::EC>) -> X,
        Self: GenericResultExt<P = ()>,
        Self::EC: FungibleErrorFamily,
        X: GenericResultExt,
    {
        match self.into_result() {
            Ok(s) => f_succ(s.value),
            Err(f) => f_err(f.errors),
        }
    }

    /// Lift Result with no warnings to non-commutative Result
    fn nowarn_into_non_cmt<Wf, LWCf>(
        self,
    ) -> NonCmtResult<Self::V, Self::P, Wf, Self::E, LWCf, Self::EC>
    where
        LWCf: ZeroOrMore,
        Self: NowarnExt,
    {
        self.into_result().map(|s| s.lift_simple())
    }

    /// Lift Result with no warnings to commutative Result
    // TODO misleading name since technically nowarn is also commutative
    fn nowarn_into_cmt<Wf, LWCf>(self) -> CmtResult<Self::V, Self::P, Wf, Self::E, LWCf, Self::EC>
    where
        LWCf: ZeroOrMore,
        Self: NowarnExt,
    {
        self.into_result().map(|s| s.lift_simple()).into_cmt()
    }

    /// Lift non-commutative Result into commutative Result
    fn into_cmt(self) -> CmtResult<Self::V, Self::P, Self::LW, Self::E, Self::LWC, Self::EC>
    where
        Self: NonCommutativeResultExt,
    {
        self.into_result().map_err(|e| e.lift_simple())
    }

    /// Convert Ok value of Result
    fn value_into<Vf>(
        self,
    ) -> GenericResult<Vf, Self::P, Self::LW, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        Vf: From<Self::V>,
    {
        self.map_value(Into::into)
    }

    /// Map function over Ok value of Result
    fn map_value<F, Vf>(
        self,
        f: F,
    ) -> GenericResult<Vf, Self::P, Self::LW, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: FnOnce(Self::V) -> Vf,
    {
        self.into_result().map(|s| s.map_value(f))
    }

    /// Map function over Error value of Result
    fn map_passthru<F, Pf>(
        self,
        f: F,
    ) -> GenericResult<Self::V, Pf, Self::LW, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: FnOnce(Self::P) -> Pf,
    {
        self.into_result().map_err(|e| e.map_passthru(f))
    }

    /// Set value of Ok Result
    fn set_value<Vf>(
        self,
        x: Vf,
    ) -> GenericResult<Vf, Self::P, Self::LW, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    {
        self.map_value(|_| x)
    }

    /// Set value of Error Result
    fn set_passthru<Pf>(
        self,
        x: Pf,
    ) -> GenericResult<Self::V, Pf, Self::LW, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    {
        self.map_passthru(|_| x)
    }

    /// Set value of deferred Result
    fn set_def_value<Vf>(self, x: Vf) -> Deferred<Vf, Self::LW, Self::E, Self::LWC, Self::EC>
    where
        Self: DeferredExt,
    {
        match self.into_result() {
            Ok(s) => Ok(s.map_value(|_| x)),
            Err(e) => Err(e.map_passthru(|_| x)),
        }
    }

    /// Convert warnings of a non-commutative Result
    fn non_cmt_warnings_into<Wf>(
        self,
    ) -> GenericResult<Self::V, Self::P, Wf, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        Self: NonCommutativeResultExt,
        Wf: From<Self::LW>,
    {
        self.map_non_cmt_warnings(Into::into)
    }

    /// Map function over warnings of a non-commutative Result
    fn map_non_cmt_warnings<F, Wf>(
        self,
        f: F,
    ) -> GenericResult<Self::V, Self::P, Wf, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        Self: NonCommutativeResultExt,
        F: Fn(Self::LW) -> Wf,
    {
        self.into_result().map(|s| s.map_warnings(f))
    }

    /// Convert warnings of commutative Result
    fn cmt_warnings_into<Wf>(self) -> CmtResult<Self::V, Self::P, Wf, Self::E, Self::LWC, Self::EC>
    where
        Wf: From<Self::LW>,
        Self: CommutativeResultExt,
    {
        self.map_cmt_warnings(Into::into)
    }

    /// Map function over warnings of commutative Result
    fn map_cmt_warnings<F, Wf>(
        self,
        f: F,
    ) -> CmtResult<Self::V, Self::P, Wf, Self::E, Self::LWC, Self::EC>
    where
        F: Fn(Self::LW) -> Wf,
        Self: CommutativeResultExt,
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
    ) -> GenericResult<Self::V, Self::P, Self::LW, Self::RW, Ef, Self::LWC, Self::RWC, Self::EC>
    where
        Self::E: Into<Ef>,
    {
        self.map_non_fung_errors(Into::into)
    }

    /// Map function over errors in Result
    ///
    /// This function will work on any Result type but may change a fungible
    /// Result to non-fungible one, which is generally not a good idea.
    /// See [`map_*_fung_errors`] for functions that will map over warnings
    /// if they are the same type as errors.
    fn map_non_fung_errors<F, ToE>(
        self,
        f: F,
    ) -> GenericResult<Self::V, Self::P, Self::LW, Self::RW, ToE, Self::LWC, Self::RWC, Self::EC>
    where
        F: Fn(Self::E) -> ToE,
    {
        self.into_result().map_err(|e| e.map_errors(f))
    }

    /// Convert errors in commutative/fungible Results
    fn non_cmt_fung_errors_into<Ef>(self) -> NonCmtFungibleResult<Self::V, Self::P, Ef, Self::EC>
    where
        Self::E: Into<Ef>,
        Self: NonCommutativeResultExt + FungibleExt,
        Self::EC: FungibleErrorFamily,
    {
        self.map_non_cmt_fung_errors(Into::into)
    }

    /// Map function over errors in commutative/fungible Results
    fn cmt_fung_errors_into<Ef>(self) -> CmtFungibleResult<Self::V, Self::P, Ef, Self::EC>
    where
        Self::E: Into<Ef>,
        Self: CommutativeResultExt + FungibleExt,
        Self::EC: FungibleErrorFamily,
    {
        self.map_cmt_fung_errors(Into::into)
    }

    /// Convert errors in non-commutative/fungible Results
    fn map_non_cmt_fung_errors<F, Ef>(
        self,
        f: F,
    ) -> NonCmtFungibleResult<Self::V, Self::P, Ef, Self::EC>
    where
        F: Fn(Self::E) -> Ef,
        Self: NonCommutativeResultExt + FungibleExt,
        Self::EC: FungibleErrorFamily,
    {
        match self.into_result() {
            Ok(s) => Ok(s.map_warnings(f)),
            Err(e) => Err(e.map_errors(f)),
        }
    }

    /// Map function over errors in non-commutative/fungible Results
    fn map_cmt_fung_errors<F, Ef>(self, f: F) -> CmtFungibleResult<Self::V, Self::P, Ef, Self::EC>
    where
        F: Fn(Self::E) -> Ef,
        Self: CommutativeResultExt + FungibleExt,
        Self::EC: FungibleErrorFamily,
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
    ) -> GenericResult<Vf, Vf, Self::LW, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: FnOnce(Self::V) -> Vf,
        Self: DeferredExt,
    {
        match self.into_result() {
            Ok(s) => Ok(s.map_value(f)),
            Err(e) => Err(e.map_passthru(f)),
        }
    }

    fn repack<LWCf, RWCf, ECf>(
        self,
    ) -> GenericResult<Self::V, Self::P, Self::LW, Self::RW, Self::E, LWCf, RWCf, ECf>
    where
        Self::LWC: IntoZeroOrMore<LWCf>,
        LWCf: ZeroOrMore,
        Self::RWC: IntoZeroOrMore<RWCf>,
        RWCf: ZeroOrMore,
        Self::EC: IntoZeroOrMore<ECf>,
        ECf: ZeroOrMore,
    {
        self.repack_left_warnings()
            .repack_right_warnings()
            .repack_errors()
    }

    fn repack_left_warnings<LWCf>(
        self,
    ) -> GenericResult<Self::V, Self::P, Self::LW, Self::RW, Self::E, LWCf, Self::RWC, Self::EC>
    where
        Self::LWC: IntoZeroOrMore<LWCf>,
        LWCf: ZeroOrMore,
    {
        self.into_result().map(Success::repack)
    }

    fn repack_right_warnings<RWCf>(
        self,
    ) -> GenericResult<Self::V, Self::P, Self::LW, Self::RW, Self::E, Self::LWC, RWCf, Self::EC>
    where
        Self::RWC: IntoZeroOrMore<RWCf>,
        RWCf: ZeroOrMore,
    {
        self.into_result().map_err(Failure::repack_warnings)
    }

    fn repack_errors<ECf>(
        self,
    ) -> GenericResult<Self::V, Self::P, Self::LW, Self::RW, Self::E, Self::LWC, Self::RWC, ECf>
    where
        Self::EC: IntoZeroOrMore<ECf>,
        ECf: ZeroOrMore,
    {
        self.into_result().map_err(Failure::repack_errors)
    }

    fn cmt_warnings_to_errors<F>(
        self,
        conf: &SharedConfig,
        f: F,
    ) -> CmtResult<Self::V, (), Self::LW, Self::E, Self::RWC, Self::EC>
    where
        F: Fn(Self::LW) -> Self::E,
        Self: CommutativeResultExt,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self::LWC: IntoZeroOrMore<Self::EC>,
    {
        let res = self.into_result();
        if conf.warnings_are_errors {
            let ret = match res {
                Ok(s) => s.warnings_to_errors(f, |_| ()),
                Err(e) => Err(e.warnings_to_errors(f).map_passthru(|_| ())),
            };
            ret.nowarn_into_cmt()
        } else if conf.hide_warnings {
            res.remove_warnings().nowarn_into_cmt().set_passthru(())
        } else {
            res.set_passthru(())
        }
    }

    fn remove_warnings(self) -> NowarnResult<Self::V, Self::P, Self::E, Self::EC> {
        self.into_result()
            .map(|s| s.remove_warnings())
            .map_err(|e| e.remove_warnings())
    }

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
    //     <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
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
    //     <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
    //     Self: DeferredExt,
    // {
    //     self.warnings_to_errors(f, |x| x, |x| x)
    // }

    /// Aggregate non-fungible errors into one error.
    fn aggregate_non_fung_errors<F, Ef>(
        self,
        f: F,
    ) -> GenericResult<Self::V, Self::P, Self::LW, Self::RW, Ef, Self::LWC, Self::RWC, NullFamily>
    where
        // NOTE pretend there is a negative trait bound for "non-fungible"
        F: FnOnce(GenNonEmpty<Self::E, Self::EC>) -> Ef,
    {
        self.into_result().map_err(|e| e.aggregate_errors(f))
    }

    /// Aggregate non-commutative/fungible errors into one error.
    fn aggregate_non_cmt_fung_errors<F, G, Ef>(
        self,
        f: F,
        g: G,
    ) -> NonCmtFungibleResult<Self::V, Self::P, Ef, NullFamily>
    where
        F: FnOnce(<Self::LWC as ZeroOrMore>::Wrapper<Self::E>) -> Ef,
        G: FnOnce(GenNonEmpty<Self::E, Self::EC>) -> Ef,
        Self: NonCommutativeResultExt + FungibleExt,
        Self::EC: FungibleErrorFamily,
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
    ) -> CmtFungibleResult<Self::V, Self::P, Ef, NullFamily>
    where
        F: FnOnce(<Self::LWC as ZeroOrMore>::Wrapper<Self::E>) -> Ef,
        G: FnOnce(GenNonEmpty<Self::E, Self::EC>) -> Ef,
        Self: CommutativeResultExt + FungibleExt,
        Self::EC: FungibleErrorFamily,
    {
        match self.into_result() {
            Ok(s) => Ok(s.aggregate_warnings(f)),
            Err(e) => Err(e.aggregate_errors(g).aggregate_warnings(f)),
        }
    }

    fn from_infallible<PF, EF>(
        self,
    ) -> GenericResult<Self::V, PF, Self::LW, Self::RW, EF, Self::LWC, Self::RWC, Self::EC>
    where
        Self: GenericResultExt<E = Infallible>,
    {
        let Ok(ret) = self.into_result();
        Ok(ret)
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

    /// Resolve non-commutative Result with into regular Result type.
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
        Fwarn: FnOnce(<Self::LWC as ZeroOrMore>::Wrapper<Self::LW>) -> WarnRes,
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
        Fwarn: FnOnce(<Self::LWC as ZeroOrMore>::Wrapper<Self::LW>) -> WarnRes,
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
    /// The emitted error must be convertible to the type of the Ok warning.
    /// This is meant to be used for cases where a Result may have any
    /// given type configuration but we have a warning subclassed in this type
    /// which requires the value. In this sense, this is a non-commutative error
    /// but emitted within a warning type that may not be.
    fn eval_non_def_warning<F, W>(&mut self, f: F)
    where
        F: FnOnce(&Self::V) -> Option<W>,
        W: Into<Self::LW>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Extend<Self::LW>,
    {
        if let Ok(s) = self.as_result_mut() {
            s.eval_warning(|v| f(v).map(Into::into))
        }
    }

    /// Push a warning based on the Ok value of a non-commutative Result.
    ///
    /// Does nothing if result is Error since warnings cannot be stored on
    /// the error side by definition.
    fn eval_non_cmt_warning<F>(&mut self, f: F)
    where
        F: FnOnce(&Self::V) -> Option<Self::LW>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Extend<Self::LW>,
        Self: NonCommutativeResultExt,
    {
        if let Ok(s) = self.as_result_mut() {
            s.eval_warning(f)
        }
    }

    // TODO this function likely is nonsense because it does nothing by
    // definition for the Error side despite a warning being explicitly given,
    // which suggests the warning is legit and should be recorded.

    // fn push_non_cmt_warning(&mut self, w: Self::LW)
    // where
    //     <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Extend<Self::LW>,
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
    fn eval_def_warning<F>(&mut self, f: F)
    where
        F: FnOnce(&Self::V) -> Option<Self::LW>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Extend<Self::LW>,
        Self: DeferredExt,
    {
        match self.as_result_mut() {
            Ok(s) => s.eval_warning(f),
            Err(e) => e.eval_warning(f),
        }
    }

    /// Set warnings in both Ok and Error sides of Result
    fn set_cmt_warnings<W, WC>(
        self,
        ws: WC::Wrapper<W>,
    ) -> CmtResult<Self::V, Self::P, W, Self::E, WC, Self::EC>
    where
        WC: ZeroOrMore,
        Self: NowarnExt,
    {
        match self.into_result() {
            Ok(s) => Ok(s.set_warnings(ws)),
            Err(e) => Err(e.set_warnings(ws)),
        }
    }

    /// Set warnings in Ok side of Result with no warnings
    fn set_non_cmt_warnings<W, WC>(
        self,
        ws: WC::Wrapper<W>,
    ) -> NonCmtResult<Self::V, Self::P, W, Self::E, WC, Self::EC>
    where
        WC: ZeroOrMore,
        Self: NowarnExt,
    {
        self.into_result().map(|s| s.set_warnings(ws))
    }

    /// Push a warning to a commutative Result.
    fn push_cmt_warning(&mut self, w: Self::LW)
    where
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Extend<Self::LW>,
        Self: CommutativeResultExt,
    {
        match self.as_result_mut() {
            Ok(s) => s.push_warning(w),
            Err(e) => e.push_warning(w),
        }
    }

    /// Add warnings to a commutative Result.
    fn extend_cmt_warnings(&mut self, ws: impl IntoIterator<Item = Self::LW>)
    where
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Extend<Self::LW>,
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
    fn eval_def_error<F>(
        self,
        f: F,
    ) -> CmtResult<Self::V, Self::P, Self::LW, Self::E, Self::LWC, Self::EC>
    where
        F: FnOnce(&Self::V) -> Option<Self::E>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: DeferredExt,
    {
        match self.into_result() {
            Ok(succ) => match f(&succ.value) {
                Some(e) => Err(succ.fail(GenNonEmpty::new1(e))),
                None => Ok(succ),
            },
            Err(mut err) => {
                if let Some(e) = f(&err.passthru) {
                    err.push_error(e);
                };
                Err(err)
            }
        }
    }

    /// Push an error to a deferred Result.
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    fn push_def_error(
        self,
        e: Self::E,
    ) -> CmtResult<Self::V, Self::P, Self::LW, Self::E, Self::LWC, Self::EC>
    where
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
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
    ) -> CmtResult<Self::V, Self::P, Self::LW, Self::E, Self::LWC, Self::EC>
    where
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
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
    fn eval_def_fung_error<F>(
        mut self,
        is_error: bool,
        f: F,
    ) -> DeferredFungible<Self::V, Self::E, Self::EC>
    where
        F: FnOnce(&Self::V) -> Option<Self::E>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Extend<Self::LW>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: FungibleExt + DeferredExt,
        Self::EC: FungibleErrorFamily,
    {
        if is_error {
            self.eval_def_error(f)
        } else {
            self.eval_def_warning(f);
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
        <Self::LWC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: FungibleExt + DeferredExt,
        Self::EC: FungibleErrorFamily,
    {
        self.extend_def_fung_errors(iter::once(e), is_error)
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
        <Self::LWC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: FungibleExt + DeferredExt,
        Self::EC: FungibleErrorFamily,
    {
        if is_error {
            self.extend_def_errors(xs.into_iter().map(Into::into))
        } else {
            self.extend_cmt_warnings(xs.into_iter().map(Into::into));
            self.into_result()
        }
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
    /// Wrapper for warnings must be a semigroup, which specifically means
    /// that Option<T> must be converted to a vector before calling this.
    fn and_then_cmt<F, Vf>(
        self,
        f: F,
    ) -> CmtResult<Vf, Self::P, Self::LW, Self::E, Self::LWC, Self::EC>
    where
        F: FnOnce(Self::V) -> CmtResult<Vf, Self::P, Self::LW, Self::E, Self::LWC, Self::EC>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Semigroup,
        Self: CommutativeResultExt,
    {
        self.into_result().and_then(|s| s.and_maybe(f))
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
    /// Wrapper for warnings must be a semigroup, which specifically means
    /// that Option<T> must be converted to a vector before calling this.
    ///
    /// Wrapper for errors must be able to hold multiple values.
    fn and_then_def<F, Vf, Pf>(
        self,
        f: F,
    ) -> CmtResult<Vf, Pf, Self::LW, Self::E, Self::LWC, Self::EC>
    where
        F: FnOnce(Self::V) -> CmtResult<Vf, Pf, Self::LW, Self::E, Self::LWC, Self::EC>,
        Self: DeferredExt,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Semigroup,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
    {
        match self.into_result() {
            Ok(s) => s.and_maybe(f),
            Err(e) => e.with_passthru(f),
        }
    }

    /// Combine two commutative results.
    ///
    /// Ok values will be wrapped in a tuple. Error values if they exist will
    /// be voided.
    ///
    /// Wrappers for warnings and errors must be the same. The former must
    /// be a semigroup (which here means Option<T> must be converted to Vec<T>
    /// prior to calling). The latter will be converted to a Vec<T> since
    /// there could be more than one errors.
    fn zip_cmt<V1, P1>(
        self,
        a: CmtResult<V1, P1, Self::LW, Self::E, Self::LWC, Self::EC>,
    ) -> CmtResult<(Self::V, V1), (), Self::LW, Self::E, Self::LWC, VecFamily>
    where
        Self::EC: IntoZeroOrMore<VecFamily>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Semigroup,
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
        a: CmtResult<V1, P1, Self::LW, Self::E, Self::LWC, Self::EC>,
        b: CmtResult<V2, P2, Self::LW, Self::E, Self::LWC, Self::EC>,
    ) -> CmtResult<(Self::V, V1, V2), (), Self::LW, Self::E, Self::LWC, VecFamily>
    where
        Self::EC: IntoZeroOrMore<VecFamily>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Semigroup,
        Self: CommutativeResultExt,
    {
        self.zip_cmt(a)
            .zip_cmt(b.repack())
            .map_value(|((ax, bx), cx)| (ax, bx, cx))
    }

    /// Combine four commutative results.
    fn zip4_cmt<V1, V2, V3, P1, P2, P3>(
        self,
        a: CmtResult<V1, P1, Self::LW, Self::E, Self::LWC, Self::EC>,
        b: CmtResult<V2, P2, Self::LW, Self::E, Self::LWC, Self::EC>,
        c: CmtResult<V3, P3, Self::LW, Self::E, Self::LWC, Self::EC>,
    ) -> CmtResult<(Self::V, V1, V2, V3), (), Self::LW, Self::E, Self::LWC, VecFamily>
    where
        Self::EC: IntoZeroOrMore<VecFamily>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Semigroup,
        Self: CommutativeResultExt,
    {
        self.zip3_cmt(a, b)
            .zip_cmt(c.repack())
            .map_value(|((ax, bx, cx), dx)| (ax, bx, cx, dx))
    }

    /// Combine five commutative results.
    fn zip5_cmt<V1, V2, V3, V4, P1, P2, P3, P4>(
        self,
        a: CmtResult<V1, P1, Self::LW, Self::E, Self::LWC, Self::EC>,
        b: CmtResult<V2, P2, Self::LW, Self::E, Self::LWC, Self::EC>,
        c: CmtResult<V3, P3, Self::LW, Self::E, Self::LWC, Self::EC>,
        d: CmtResult<V4, P4, Self::LW, Self::E, Self::LWC, Self::EC>,
    ) -> CmtResult<(Self::V, V1, V2, V3, V4), (), Self::LW, Self::E, Self::LWC, VecFamily>
    where
        Self::EC: IntoZeroOrMore<VecFamily>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Semigroup,
        Self: CommutativeResultExt,
    {
        self.zip4_cmt(a, b, c)
            .zip_cmt(d.repack())
            .map_value(|((ax, bx, cx, dx), ex)| (ax, bx, cx, dx, ex))
    }

    /// Combine six commutative results.
    fn zip6_cmt<V1, V2, V3, V4, V5, P1, P2, P3, P4, P5>(
        self,
        a: CmtResult<V1, P1, Self::LW, Self::E, Self::LWC, Self::EC>,
        b: CmtResult<V2, P2, Self::LW, Self::E, Self::LWC, Self::EC>,
        c: CmtResult<V3, P3, Self::LW, Self::E, Self::LWC, Self::EC>,
        d: CmtResult<V4, P4, Self::LW, Self::E, Self::LWC, Self::EC>,
        e: CmtResult<V5, P5, Self::LW, Self::E, Self::LWC, Self::EC>,
    ) -> CmtResult<(Self::V, V1, V2, V3, V4, V5), (), Self::LW, Self::E, Self::LWC, VecFamily>
    where
        Self::EC: IntoZeroOrMore<VecFamily>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Semigroup,
        Self: CommutativeResultExt,
    {
        self.zip5_cmt(a, b, c, d)
            .zip_cmt(e.repack())
            .map_value(|((ax, bx, cx, dx, ex), fx)| (ax, bx, cx, dx, ex, fx))
    }

    /// Combine two deferred results.
    ///
    /// Ok and Error values will be wrapped in a tuple. Inputs must be
    /// deferred to ensure value types match between Ok and Error branches.
    ///
    /// Wrappers for warnings and errors must be the same. The former must
    /// be a semigroup (which here means Option<T> must be converted to Vec<T>
    /// prior to calling). The latter will be converted to a Vec<T> since
    /// there could be more than one errors.
    fn zip_def<V1>(
        self,
        a: Deferred<V1, Self::LW, Self::E, Self::LWC, Self::EC>,
    ) -> Deferred<(Self::V, V1), Self::LW, Self::E, Self::LWC, VecFamily>
    where
        Self::EC: IntoZeroOrMore<VecFamily>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Semigroup,
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
        a: Deferred<V1, Self::LW, Self::E, Self::LWC, Self::EC>,
        b: Deferred<V2, Self::LW, Self::E, Self::LWC, Self::EC>,
    ) -> Deferred<(Self::V, V1, V2), Self::LW, Self::E, Self::LWC, VecFamily>
    where
        Self::EC: IntoZeroOrMore<VecFamily>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Semigroup,
        Self: DeferredExt,
    {
        self.zip_def(a)
            .zip_def(b.repack())
            .map_def_value(|((ax, bx), cx)| (ax, bx, cx))
    }

    /// Combine four deferred results.
    fn zip4_def<V1, V2, V3>(
        self,
        a: Deferred<V1, Self::LW, Self::E, Self::LWC, Self::EC>,
        b: Deferred<V2, Self::LW, Self::E, Self::LWC, Self::EC>,
        c: Deferred<V3, Self::LW, Self::E, Self::LWC, Self::EC>,
    ) -> Deferred<(Self::V, V1, V2, V3), Self::LW, Self::E, Self::LWC, VecFamily>
    where
        Self::EC: IntoZeroOrMore<VecFamily>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Semigroup,
        Self: DeferredExt,
    {
        self.zip3_def(a, b)
            .zip_def(c.repack())
            .map_def_value(|((ax, bx, cx), dx)| (ax, bx, cx, dx))
    }

    /// Combine five deferred results.
    fn zip5_def<V1, V2, V3, V4>(
        self,
        a: Deferred<V1, Self::LW, Self::E, Self::LWC, Self::EC>,
        b: Deferred<V2, Self::LW, Self::E, Self::LWC, Self::EC>,
        c: Deferred<V3, Self::LW, Self::E, Self::LWC, Self::EC>,
        d: Deferred<V4, Self::LW, Self::E, Self::LWC, Self::EC>,
    ) -> Deferred<(Self::V, V1, V2, V3, V4), Self::LW, Self::E, Self::LWC, VecFamily>
    where
        Self::EC: IntoZeroOrMore<VecFamily>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Semigroup,
        Self: DeferredExt,
    {
        self.zip4_def(a, b, c)
            .zip_def(d.repack())
            .map_def_value(|((ax, bx, cx, dx), ex)| (ax, bx, cx, dx, ex))
    }

    /// Combine six deferred results.
    fn zip6_def<V1, V2, V3, V4, V5>(
        self,
        a: Deferred<V1, Self::LW, Self::E, Self::LWC, Self::EC>,
        b: Deferred<V2, Self::LW, Self::E, Self::LWC, Self::EC>,
        c: Deferred<V3, Self::LW, Self::E, Self::LWC, Self::EC>,
        d: Deferred<V4, Self::LW, Self::E, Self::LWC, Self::EC>,
        e: Deferred<V5, Self::LW, Self::E, Self::LWC, Self::EC>,
    ) -> Deferred<(Self::V, V1, V2, V3, V4, V5), Self::LW, Self::E, Self::LWC, VecFamily>
    where
        Self::EC: IntoZeroOrMore<VecFamily>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Semigroup,
        Self: DeferredExt,
    {
        self.zip5_def(a, b, c, d)
            .zip_def(e.repack())
            .map_def_value(|((ax, bx, cx, dx, ex), fx)| (ax, bx, cx, dx, ex, fx))
    }
}

impl<V, P, LW, RW, E, LWC, RWC, EC> GenericResultExt
    for GenericResult<V, P, LW, RW, E, LWC, RWC, EC>
where
    LWC: ZeroOrMore,
    RWC: ZeroOrMore,
    EC: ZeroOrMore,
{
    type V = V;
    type P = P;
    type E = E;
    type LW = LW;
    type RW = RW;
    type LWC = LWC;
    type RWC = RWC;
    type EC = EC;
}

/// Constraint for non-commutative results.
///
/// Warnings on the Error side must be an empty set.
pub trait NonCommutativeResultExt: GenericResultExt<RW = (), RWC = NullFamily> {}

impl<V, P, W, E, WC: ZeroOrMore, EC: ZeroOrMore> NonCommutativeResultExt
    for NonCmtResult<V, P, W, E, WC, EC>
{
}

/// Constraint for commutative results.
///
/// Warning cardinality and type must match between Ok and Error sides
pub trait CommutativeResultExt:
    GenericResultExt<RW = <Self as GenericResultExt>::LW, RWC = <Self as GenericResultExt>::LWC>
{
}

impl<V, P, W, E, WC: ZeroOrMore, EC: ZeroOrMore> CommutativeResultExt
    for CmtResult<V, P, W, E, WC, EC>
{
}

/// Constraint for deferred results.
///
/// In addition to being commutative, value must match between Ok and Error.
pub trait DeferredExt: CommutativeResultExt<V = <Self as GenericResultExt>::P> {}

impl<V, W, E, WC: ZeroOrMore, EC: ZeroOrMore> DeferredExt for Deferred<V, W, E, WC, EC> {}

/// Constraint for fungible results.
///
/// Error and warning must have the same cardinality and type.
pub trait FungibleExt:
    GenericResultExt<
    LW = <Self as GenericResultExt>::E,
    LWC = <<Self as GenericResultExt>::EC as FungibleErrorFamily>::WarnFam,
>
where
    Self::EC: FungibleErrorFamily,
{
}

impl<V, P, E, RW, RWC: ZeroOrMore, EC: FungibleErrorFamily> FungibleExt
    for GenericResult<V, P, E, RW, E, EC::WarnFam, RWC, EC>
{
}

/// Constraint for results with no warnings.
///
/// In addition to be non-commutative, warnings on the Ok side must be an empty
/// set.
pub trait NowarnExt: NonCommutativeResultExt<LW = (), LWC = NullFamily> {}

impl<V, P, E, EC: ZeroOrMore> NowarnExt for NowarnResult<V, P, E, EC> {}

/// Constraint for results which can be resolved.
///
/// The only requirement is that there must only be one error, which will be
/// used to map to a regular result.
pub trait ResolvableExt: GenericResultExt<EC = NullFamily> {}

impl<V, P, E, RW, LWC: ZeroOrMore, RWC: ZeroOrMore> ResolvableExt
    for GenericResult<V, P, E, RW, E, LWC, RWC, NullFamily>
{
}

/// Monoid-ically combine commutative results.
///
/// Ok values will be collected and returned as a single vector upon success.
/// Presence of any Error will cause Error to be returned. In any case,
/// warnings and errors as applicable will appended in order and returned.
///
/// The wrapper for warning must be a semigroup and wrapper for error must be
/// extendable since it might hold more than one error.
pub(crate) trait CmtResultIter<T, P, W, E, WC, EC>:
    Iterator<Item = CmtResult<T, P, W, E, WC, EC>> + Sized
where
    WC: ZeroOrMore,
    EC: ZeroOrMore,
{
    fn mappend_cmt(mut self) -> CmtResult<Vec<T>, (), W, E, WC, EC>
    where
        WC::Wrapper<W>: Semigroup,
        EC::Wrapper<E>: Extend<E>,
    {
        let mut left_vs = vec![];
        let mut ws = WC::Wrapper::<W>::default();
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

impl<I, V, P, W, E, WC, EC> CmtResultIter<V, P, W, E, WC, EC> for I
where
    I: Iterator<Item = CmtResult<V, P, W, E, WC, EC>>,
    WC: ZeroOrMore,
    EC: ZeroOrMore,
{
}

/// Monoid-ically combine deferred results.
///
/// Values from Ok or Error will be collected and returned in a single vector
/// independent of the presence of warnings or errors.
///
/// The wrapper for warning must be a semigroup and wrapper for error must be
/// extendable since it might hold more than one error.
pub(crate) trait DeferredIter<T, W, E, WC, EC>:
    Iterator<Item = Deferred<T, W, E, WC, EC>> + Sized
where
    WC: ZeroOrMore,
    EC: ZeroOrMore,
{
    // TODO not DRY
    fn mappend_def(mut self) -> Deferred<Vec<T>, W, E, WC, EC>
    where
        WC::Wrapper<W>: Semigroup,
        EC::Wrapper<E>: Extend<E>,
    {
        let mut vs = vec![];
        let mut ws = WC::Wrapper::<W>::default();
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
            vs.push(h.passthru);
            let mut es = h.errors;
            for x in self {
                match x {
                    Ok(y) => {
                        vs.push(y.value);
                        ws = ws.concat(y.warnings);
                    }
                    Err(y) => {
                        vs.push(y.passthru);
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

    fn mappend_def_void(mut self) -> Deferred<(), W, E, WC, EC>
    where
        WC::Wrapper<W>: Semigroup,
        EC::Wrapper<E>: Extend<E>,
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

impl<I, V, W, E, WC, EC> DeferredIter<V, W, E, WC, EC> for I
where
    I: Iterator<Item = Deferred<V, W, E, WC, EC>>,
    WC: ZeroOrMore,
    EC: ZeroOrMore,
{
}

impl<W, E, WC, EC> From<io::Error> for Failure<(), W, ImpureError<E>, WC, EC>
where
    WC: ZeroOrMore,
    EC: ZeroOrMore,
{
    fn from(value: io::Error) -> Self {
        Self::new_from_one(value.into(), ())
    }
}

impl<W, E, WC, EC> From<E> for Failure<(), W, E, WC, EC>
where
    WC: ZeroOrMore,
    EC: ZeroOrMore,
{
    fn from(value: E) -> Self {
        Self::new_from_one(value, ())
    }
}
