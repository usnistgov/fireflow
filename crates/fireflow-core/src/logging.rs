use crate::config::SharedConfig;
use crate::text::optional::{Identity, Nothing};
use crate::type_families::{
    Applicative, BiFunctor, Comonad, Functor, IsKind1, IsKind2, Kind1, Kind2, Sibling1, Sibling2,
};

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

pub type WarningsAndIOBoxedSummaryResult<V, W, E, S> =
    WarningsAndBoxedSummaryResult<V, W, ImpureError<E>, S>;

pub type WarningsAndBoxedSummaryResult<V, W, E, S> =
    WarningsAndBoxedErrorResult<V, (), W, ErrorSummary<E, S>>;

pub type SummaryResult<V, E, S> = ErrorResult<V, (), ErrorSummary<E, S>>;

pub type IOSummaryResult<V, E, S> = SummaryResult<V, ImpureError<E>, S>;

// TODO maybe add wrapper type for Success which roughly means "result with
// issues that may be errors"

pub type RecoverableErrorResult<V, I> = ErrorResult<V, (), I>;
pub type RecoverableErrorsResult<V, I> = ErrorsResult<V, (), I>;

//
// Results without warnings
//

pub type ErrorResult<V, P, E> = NowarnResult<V, P, Identity<E>, Nothing<E>>;
pub type ErrorsResult<V, P, E> = NowarnResult<V, P, Identity<E>, Vec<E>>;

pub type BoxedErrorsResult<V, P, E> = NowarnResult<V, P, Box<E>, Vec<E>>;

pub type IOErrorResult<V, P, E> = ErrorResult<V, P, ImpureError<E>>;
pub type IOErrorsResult<V, P, E> = ErrorsResult<V, P, ImpureError<E>>;

pub type IOBoxedErrorResult<V, P, E> = ErrorResult<V, P, ImpureError<E>>;
pub type IOBoxedErrorsResult<V, P, E> = ErrorsResult<V, P, ImpureError<E>>;

//
// Results with errors which can also be warnings but are not commutable.
//
// NOTE: None of these have boxed versions because the error type is also on
// the warning side (in an option, which makes it insignificantly larger).
// Thus the stack space required for either is about the same.
//

pub type FungibleErrorResult<V, P, E> = NonCmtFungibleResult<V, P, Identity<E>, Nothing<E>>;
pub type FungibleErrorsResult<V, P, E> = NonCmtFungibleResult<V, P, Identity<E>, Vec<E>>;

//
// Results with warnings and errors of differing types which are not commutable
//

pub type WarningOrErrorResult<V, P, W, E> =
    NonCmtResult<V, P, Option<W>, Identity<E>, Nothing<E>>;
pub type WarningsOrErrorResult<V, P, W, E> =
    NonCmtResult<V, P, Vec<W>, Identity<E>, Nothing<E>>;
pub type WarningOrErrorsResult<V, P, W, E> = NonCmtResult<V, P, Option<W>, Identity<E>, Vec<E>>;
pub type WarningsOrErrorsResult<V, P, W, E> = NonCmtResult<V, P, Vec<W>, Identity<E>, Vec<E>>;

pub type WarningOrBoxedErrorResult<V, P, W, E> =
    NonCmtResult<V, P, Option<W>, Box<E>, Nothing<E>>;
pub type WarningsOrBoxedErrorResult<V, P, W, E> = NonCmtResult<V, P, Vec<W>, Box<E>, Nothing<E>>;
pub type WarningOrBoxedErrorsResult<V, P, W, E> = NonCmtResult<V, P, Option<W>, Box<E>, Vec<E>>;
pub type WarningsOrBoxedErrorsResult<V, P, W, E> = NonCmtResult<V, P, Vec<W>, Box<E>, Vec<E>>;

//
// Results with warnings and errors of differing types which are commutable
//

pub type WarningAndErrorResult<V, P, W, E> =
    CmtResult<V, P, Option<W>, Identity<E>, Nothing<E>>;
pub type WarningsAndErrorResult<V, P, W, E> =
    CmtResult<V, P, Vec<W>, Identity<E>, Nothing<E>>;
pub type WarningAndErrorsResult<V, P, W, E> = CmtResult<V, P, Option<W>, Identity<E>, Vec<E>>;
pub type WarningsAndErrorsResult<V, P, W, E> = CmtResult<V, P, Vec<W>, Identity<E>, Vec<E>>;

pub type WarningAndBoxedErrorResult<V, P, W, E> = CmtResult<V, P, Option<W>, Box<E>, Nothing<E>>;
pub type WarningsAndBoxedErrorResult<V, P, W, E> = CmtResult<V, P, Vec<W>, Box<E>, Nothing<E>>;
pub type WarningAndBoxedErrorsResult<V, P, W, E> = CmtResult<V, P, Option<W>, Box<E>, Vec<E>>;
pub type WarningsAndBoxedErrorsResult<V, P, W, E> = CmtResult<V, P, Vec<W>, Box<E>, Vec<E>>;

pub type IOWarningAndErrorResult<V, P, W, E> = WarningAndErrorResult<V, P, W, ImpureError<E>>;
pub type IOWarningsAndErrorResult<V, P, W, E> = WarningsAndErrorResult<V, P, W, ImpureError<E>>;
pub type IOWarningAndErrorsResult<V, P, W, E> = WarningAndErrorsResult<V, P, W, ImpureError<E>>;
pub type IOWarningsAndErrorsResult<V, P, W, E> = WarningsAndErrorsResult<V, P, W, ImpureError<E>>;

pub type IOWarningAndBoxedErrorResult<V, P, W, E> =
    WarningAndBoxedErrorResult<V, P, W, ImpureError<E>>;
pub type IOWarningsAndBoxedErrorResult<V, P, W, E> =
    WarningsAndBoxedErrorResult<V, P, W, ImpureError<E>>;
pub type IOWarningAndBoxedErrorsResult<V, P, W, E> =
    WarningAndBoxedErrorsResult<V, P, W, ImpureError<E>>;
pub type IOWarningsAndBoxedErrorsResult<V, P, W, E> =
    WarningsAndBoxedErrorsResult<V, P, W, ImpureError<E>>;

//
// Results with errors which can be warnings and is also commutable
//

pub type CmtFungibleErrorResult<V, P, E> = WarningAndErrorResult<V, P, E, E>;
pub type CmtFungibleErrorsResult<V, P, E> = WarningsAndErrorsResult<V, P, E, E>;

pub type CmtFungibleBoxedErrorResult<V, P, E> = WarningAndBoxedErrorResult<V, P, E, E>;
pub type CmtFungibleBoxedErrorsResult<V, P, E> = WarningsAndBoxedErrorsResult<V, P, E, E>;

//
// Deferred versions of the above types (ie the value on both sides is equal)
//

pub type DeferredError<V, E> = ErrorResult<V, V, E>;
pub type DeferredErrors<V, E> = ErrorsResult<V, V, E>;
pub type DeferredBoxedErrors<V, E> = BoxedErrorsResult<V, V, E>;

pub type DeferredIOError<V, E> = DeferredError<V, ImpureError<E>>;
pub type DeferredIOErrors<V, E> = DeferredErrors<V, ImpureError<E>>;
pub type DeferredIOBoxedErrors<V, E> = DeferredBoxedErrors<V, ImpureError<E>>;

pub type DeferredWarningAndError<V, W, E> = WarningAndErrorResult<V, V, W, E>;
pub type DeferredWarningsAndError<V, W, E> = WarningsAndErrorResult<V, V, W, E>;
pub type DeferredWarningAndErrors<V, W, E> = WarningAndErrorsResult<V, V, W, E>;
pub type DeferredWarningsAndErrors<V, W, E> = WarningsAndErrorsResult<V, V, W, E>;

pub type DeferredWarningAndBoxedError<V, W, E> = WarningAndBoxedErrorResult<V, V, W, E>;
pub type DeferredWarningsAndBoxedError<V, W, E> = WarningsAndBoxedErrorResult<V, V, W, E>;
pub type DeferredWarningAndBoxedErrors<V, W, E> = WarningAndBoxedErrorsResult<V, V, W, E>;
pub type DeferredWarningsAndBoxedErrors<V, W, E> = WarningsAndBoxedErrorsResult<V, V, W, E>;

pub type DeferredFungibleError<V, E> = CmtFungibleErrorResult<V, V, E>;
pub type DeferredFungibleErrors<V, E> = CmtFungibleErrorsResult<V, V, E>;

pub type DeferredFungibleBoxedError<V, E> = CmtFungibleBoxedErrorResult<V, V, E>;
pub type DeferredFungibleBoxedErrors<V, E> = CmtFungibleBoxedErrorsResult<V, V, E>;

//
// helper types for constructing the "complete" types above
//

pub type NonCmtFungibleResult<V, P, EC0, ECn> =
    NonCmtResult<V, P, <ECn as FungibleError>::Warn, EC0, ECn>;
pub type CmtFungibleResult<V, P, EC0, ECn> =
    CmtResult<V, P, <ECn as FungibleError>::Warn, EC0, ECn>;

pub type NowarnResult<V, P, EC0, ECn> = CmtResult<V, P, Nothing<()>, EC0, ECn>;

pub type Deferred<V, WC, EC0, ECn> = CmtResult<V, V, WC, EC0, ECn>;

pub type DeferredNowarn<V, EC0, ECn> = NowarnResult<V, V, EC0, ECn>;

pub type DeferredFungible<V, EC0, ECn> = Deferred<V, <ECn as FungibleError>::Warn, EC0, ECn>;

pub type CmtResult<V, P, WC, EC0, ECn> = LogResult<V, P, WC, WC, EC0, ECn>;

pub type NonCmtResult<V, P, WC, EC0, ECn> = LogResult<V, P, WC, Nothing<()>, EC0, ECn>;

pub type FungibleResult<V, P, RWC, EC0, ECn> =
    LogResult<V, P, <ECn as FungibleError>::Warn, RWC, EC0, ECn>;

pub enum LogResult<V, P, LWC, RWC, EC0, ECn> {
    Succ(Success<V, LWC>),
    Fail(Failure<P, RWC, EC0, ECn>),
}

use LogResult::{Fail, Succ};

#[derive(new)]
#[new(visibility = "")]
pub struct Success<V, WC> {
    value: V,
    warnings: WC,
}

#[derive(new)]
pub struct Failure<P, WC, EC0, ECn> {
    warnings: WC,
    errors: GenNonEmpty<EC0, ECn>,
    value: P,
}

#[derive(new)]
pub struct ErrorSummary<E, S> {
    pub summary: S,
    pub errors: GenNonEmpty<Identity<E>, Vec<E>>,
}

#[derive(new)]
pub struct GenNonEmpty<C0, Cn> {
    head: C0,
    tail: Cn,
}

pub type IOResult<T, E> = Result<T, ImpureError<E>>;

#[derive(Debug, Error)]
pub enum ImpureError<E> {
    #[error("IO error: {0}")]
    IO(#[from] io::Error),
    #[error("{0}")]
    Pure(E),
}

pub struct GenNonEmptyFamilyInner<C0, C>(PhantomData<C0>, PhantomData<C>);

pub struct GenNonEmptyFamilyOuter;

pub struct LogResultFamily<LWC, RWC, EC0, ECn>(
    PhantomData<LWC>,
    PhantomData<RWC>,
    PhantomData<EC0>,
    PhantomData<ECn>,
);

pub trait Concatable {
    type Out;
    fn concat(self, other: Self) -> Self::Out;
}

pub trait Semigroup: Concatable<Out = Self> {}

pub trait FungibleError: Sized {
    type Inner;
    type Warn: Applicative<Self::Inner>;

    fn errors_to_warnings<EC0>(errors: GenNonEmpty<EC0, Self>) -> Self::Warn
    where
        EC0: Comonad<Self::Inner>;

    fn error_to_warning(error: Self::Inner) -> Self::Warn {
        Self::Warn::pure(error)
    }
}

pub trait IntoNewCardinality<Other> {
    fn into_new_cardinality(self) -> Other;
}

pub trait IntoNewWrapper<Other> {
    fn into_new_wrapper(self) -> Other;
}

impl<C0: Kind1, C: Kind1> Kind1 for GenNonEmptyFamilyInner<C0, C> {
    type Type<T> = GenNonEmpty<C0::Type<T>, C::Type<T>>;
}

impl<C: IsKind1, C0: IsKind1> IsKind1 for GenNonEmpty<C0, C> {
    type Family = GenNonEmptyFamilyInner<C0::Family, C::Family>;
}

impl Kind2 for GenNonEmptyFamilyOuter {
    type Type<A, B> = GenNonEmpty<A, B>;
}

impl<C0, C> IsKind2 for GenNonEmpty<C0, C> {
    type Family = GenNonEmptyFamilyOuter;
}

impl<LWC, RWC, EC0, ECn> Kind2 for LogResultFamily<LWC, RWC, EC0, ECn> {
    type Type<A, B> = LogResult<A, B, LWC, RWC, EC0, ECn>;
}

impl<A, B, LWC, RWC, EC0, ECn> IsKind2 for LogResult<A, B, LWC, RWC, EC0, ECn> {
    type Family = LogResultFamily<LWC, RWC, EC0, ECn>;
}

impl<A, C0: Functor<A>, Cn: Functor<A>> Functor<A> for GenNonEmpty<C0, Cn> {
    fn fmap<F: Fn(A) -> B, B>(self, f: F) -> Sibling1<Self, B> {
        GenNonEmpty::new(self.head.fmap(&f), self.tail.fmap(f))
    }
}

impl<A, B> BiFunctor<A, B> for GenNonEmpty<A, B> {
    fn bimap<F, G, C, D>(self, f: F, g: G) -> Sibling2<Self, C, D>
    where
        F: Fn(A) -> C,
        G: Fn(B) -> D,
    {
        GenNonEmpty::new(f(self.head), g(self.tail))
    }
}

impl<E, EC0, EC> IntoIterator for GenNonEmpty<EC0, EC>
where
    EC0: Comonad<E>,
    EC: IntoIterator<Item = E>,
{
    type Item = E;
    type IntoIter = iter::Chain<iter::Once<E>, <EC as IntoIterator>::IntoIter>;
    fn into_iter(self) -> Self::IntoIter {
        iter::once(self.head.cm_extract()).chain(self.tail)
    }
}

impl<T> IntoNewCardinality<T> for T {
    fn into_new_cardinality(self) -> T {
        self
    }
}

impl<T> IntoNewCardinality<Vec<T>> for Nothing<T> {
    fn into_new_cardinality(self) -> Vec<T> {
        vec![]
    }
}

impl<T> IntoNewCardinality<Option<T>> for Nothing<T> {
    fn into_new_cardinality(self) -> Option<T> {
        None
    }
}

impl<T> IntoNewCardinality<Vec<T>> for Option<T> {
    fn into_new_cardinality(self) -> Vec<T> {
        self.into_iter().collect()
    }
}

impl<T> IntoNewWrapper<T> for T {
    fn into_new_wrapper(self) -> T {
        self
    }
}

impl<T> IntoNewWrapper<Box<T>> for Identity<T> {
    fn into_new_wrapper(self) -> Box<T> {
        Box::new(self.0)
    }
}

impl<T> IntoNewWrapper<Identity<T>> for Box<T> {
    fn into_new_wrapper(self) -> Identity<T> {
        Identity(*self)
    }
}

impl<A, C0: Applicative<A>, Cn: Applicative<A> + Default> Applicative<A> for GenNonEmpty<C0, Cn> {
    fn pure(a: A) -> Self {
        Self::new(C0::pure(a), Cn::default())
    }
}

impl<T> Concatable for Nothing<T> {
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

impl<T> Semigroup for Nothing<T> {}

impl<E> FungibleError for Nothing<E> {
    type Inner = E;
    type Warn = Option<E>;

    fn errors_to_warnings<EC0>(errors: GenNonEmpty<EC0, Self>) -> Self::Warn
    where
        EC0: Comonad<E>,
    {
        Some(errors.head.cm_extract())
    }

    fn error_to_warning(error: E) -> Self::Warn {
        Some(error)
    }
}

impl<E> FungibleError for Vec<E> {
    type Inner = E;
    type Warn = Self;

    fn errors_to_warnings<EC0>(errors: GenNonEmpty<EC0, Self>) -> Self::Warn
    where
        EC0: Comonad<E>,
    {
        errors.into_iter().collect()
    }

    fn error_to_warning(error: E) -> Self::Warn {
        vec![error]
    }
}

impl<V, WC> Success<V, WC> {
    pub fn new1(value: V) -> Self
    where
        WC: Default,
    {
        Self::new(value, WC::default())
    }

    pub(crate) fn repack<WCf>(self) -> Success<V, WCf>
    where
        WC: IntoNewCardinality<WCf>,
    {
        Success::new(self.value, self.warnings.into_new_cardinality())
    }

    pub(crate) fn map_value<F: FnOnce(V) -> X, X>(self, f: F) -> Success<X, WC> {
        Success::new(f(self.value), self.warnings)
    }

    pub(crate) fn map_warnings<F, W, Wf>(self, f: F) -> Success<V, Sibling1<WC, Wf>>
    where
        WC: Functor<W>,
        F: Fn(W) -> Wf,
    {
        Success::new(self.value, self.warnings.fmap(f))
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

    pub(crate) fn and_maybe<F, ToV, P, EC0, WCf, ECn>(
        self,
        f: F,
    ) -> CmtResult<ToV, P, WCf, EC0, ECn>
    where
        F: FnOnce(V) -> CmtResult<ToV, P, WC, EC0, ECn>,
        WC: Concatable<Out = WCf>,
    {
        match f(self.value) {
            Succ(s) => {
                let ws = self.warnings.concat(s.warnings);
                Succ(Success::new(s.value, ws))
            }
            Fail(e) => {
                let ws = self.warnings.concat(e.warnings);
                Fail(Failure::new(ws, e.errors, e.value))
            }
        }
    }

    pub(crate) fn fail<E, EC>(self, errors: GenNonEmpty<E, EC>) -> Failure<V, WC, E, EC> {
        Failure::new(self.warnings, errors, self.value)
    }

    pub(crate) fn with_failure<F, P, PF, E, WCf, EC>(
        self,
        other: Failure<P, WC, E, EC>,
        f: F,
    ) -> Failure<PF, WCf, E, EC>
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
    fn warnings_to_errors<E, W, P, EC0, EC, F, G>(
        self,
        f: F,
        g: G,
    ) -> LogResult<V, P, WC, WC, EC0, EC>
    where
        F: Fn(W) -> E,
        G: FnOnce(V) -> P,
        EC0: Applicative<E>,
        EC: Extend<E> + Default,
        WC: Default + IntoIterator<Item = W>,
    {
        match GenNonEmpty::<EC0, EC>::collect(self.warnings.into_iter().map(f)) {
            None => Succ(Self::new1(self.value)),
            Some(es) => Fail(Failure::new_from_many(es, g(self.value))),
        }
    }

    fn resolve<F, X>(self, f: F) -> (V, X)
    where
        F: FnOnce(WC) -> X,
    {
        (self.value, f(self.warnings))
    }
}

impl<V> Success<V, Nothing<()>> {
    fn nowarn_into_warn<WC: Default>(self) -> Success<V, WC> {
        Success::new1(self.value)
    }
}

impl<P, EC0, WC, ECn> Failure<P, WC, EC0, ECn> {
    // TODO this is just pure which wraps NonEmpty which wraps EC0
    pub(crate) fn new_from_one<E>(error: E, value: P) -> Self
    where
        EC0: Applicative<E>,
        ECn: Default,
        WC: Default,
    {
        Self::new_from_many(GenNonEmpty::new1(EC0::pure(error)), value)
    }

    pub(crate) fn new_from_many(errors: GenNonEmpty<EC0, ECn>, value: P) -> Self
    where
        WC: Default,
    {
        Self::new(WC::default(), errors, value)
    }

    fn repack_warnings<WCf>(self) -> Failure<P, WCf, EC0, ECn>
    where
        WC: IntoNewCardinality<WCf>,
    {
        Failure::new(
            WC::into_new_cardinality(self.warnings),
            self.errors,
            self.value,
        )
    }

    fn repack_error<EC0f>(self) -> Failure<P, WC, EC0f, ECn>
    where
        EC0: IntoNewWrapper<EC0f>,
    {
        let es = self.errors.bimap(IntoNewWrapper::into_new_wrapper, |x| x);
        Failure::new(self.warnings, es, self.value)
    }

    fn repack_errors<ECf>(self) -> Failure<P, WC, EC0, ECf>
    where
        ECn: IntoNewCardinality<ECf>,
    {
        Failure::new(self.warnings, self.errors.repack(), self.value)
    }

    fn map_warnings<F, W, Wf>(self, f: F) -> Failure<P, Sibling1<WC, Wf>, EC0, ECn>
    where
        F: Fn(W) -> Wf,
        WC: Functor<W>,
    {
        Failure::new(self.warnings.fmap(f), self.errors, self.value)
    }

    fn map_errors<F, Ei, Ef>(self, f: F) -> Failure<P, WC, Sibling1<EC0, Ef>, Sibling1<ECn, Ef>>
    where
        F: Fn(Ei) -> Ef,
        EC0: Functor<Ei>,
        ECn: Functor<Ei>,
    {
        Failure::new(self.warnings, self.errors.map(f), self.value)
    }

    fn map_value<F, Pf>(self, f: F) -> Failure<Pf, WC, EC0, ECn>
    where
        F: FnOnce(P) -> Pf,
    {
        Failure::new(self.warnings, self.errors, f(self.value))
    }

    fn set_warnings<WCf>(self, ws: WCf) -> Failure<P, WCf, EC0, ECn> {
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

    fn push_error<E>(&mut self, e: E)
    where
        ECn: Extend<E>,
    {
        self.errors.extend(iter::once(e));
    }

    fn extend_errors<E>(&mut self, es: impl IntoIterator<Item = E>)
    where
        ECn: Extend<E>,
    {
        self.errors.extend(es);
    }

    fn with_value<F, V, E, Pf, WCf, ECn1>(mut self, f: F) -> CmtResult<V, Pf, WCf, EC0, ECn>
    where
        F: FnOnce(P) -> CmtResult<V, Pf, WC, EC0, ECn1>,
        WC: Concatable<Out = WCf>,
        EC0: Comonad<E>,
        ECn: Extend<E> + IntoIterator<Item = E>,
        ECn1: IntoIterator<Item = E>,
    {
        match f(self.value) {
            Succ(s) => {
                let ws = self.warnings.concat(s.warnings);
                Succ(Success::new(s.value, ws))
            }
            Fail(e) => {
                let ws = self.warnings.concat(e.warnings);
                self.errors.extend(e.errors);
                Fail(Failure::new(ws, self.errors, e.value))
            }
        }
    }

    fn zip_with<F, E, P0, Pf, WCf, ECf>(
        self,
        other: Failure<P0, WC, EC0, ECn>,
        f: F,
    ) -> Failure<Pf, WCf, EC0, ECf>
    where
        F: FnOnce(P, P0) -> Pf,
        EC0: Comonad<E>,
        ECn: IntoNewCardinality<ECf> + IntoIterator<Item = E>,
        WC: Concatable<Out = WCf>,
        ECf: Extend<E>,
    {
        let ws = self.warnings.concat(other.warnings);
        let mut es = self.errors.repack();
        es.extend(other.errors);
        Failure::new(ws, es, f(self.value, other.value))
    }

    fn aggregate_warnings<F, Wf>(self, f: F) -> Failure<P, Option<Wf>, EC0, ECn>
    where
        F: FnOnce(WC) -> Wf,
    {
        Failure::new(Some(f(self.warnings)), self.errors, self.value)
    }

    fn aggregate_errors<F, E>(self, f: F) -> Failure<P, WC, Sibling1<EC0, E>, Nothing<E>>
    where
        EC0: IsKind1,
        Sibling1<EC0, E>: Applicative<E>,
        F: FnOnce(GenNonEmpty<EC0, ECn>) -> E,
    {
        let es = GenNonEmpty::new1(Sibling1::<EC0, E>::pure(f(self.errors)));
        Failure::new(self.warnings, es, self.value)
    }

    fn with_success<F, V, PF, WCf>(self, other: Success<V, WC>, f: F) -> Failure<PF, WCf, EC0, ECn>
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
    fn warnings_to_errors<E, W, F>(mut self, f: F) -> Self
    where
        F: Fn(W) -> E,
        EC0: Applicative<E>,
        ECn: Extend<E>,
        WC: IntoIterator<Item = W> + Default,
    {
        self.errors.extend(self.warnings.into_iter().map(f));
        Self::new_from_many(self.errors, self.value)
    }
}

impl<P, E, EC> Failure<P, Nothing<()>, E, EC> {
    fn nowarn_into_warn<WC>(self) -> Failure<P, WC, E, EC>
    where
        WC: Default,
    {
        Failure::new_from_many(self.errors, self.value)
    }
}

impl<X, C0, C> Extend<X> for GenNonEmpty<C0, C>
where
    C: Extend<X>,
{
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = X>,
    {
        self.tail.extend(iter);
    }
}

impl<C0, C> GenNonEmpty<C0, C> {
    fn collect<X>(xs: impl IntoIterator<Item = X>) -> Option<Self>
    where
        C0: Applicative<X>,
        C: Extend<X> + Default,
    {
        let mut it = xs.into_iter();
        it.by_ref().next().map(|x0| {
            let mut ret = Self::new1(C0::pure(x0));
            ret.extend(it);
            ret
        })
    }

    fn new1(x: C0) -> Self
    where
        C: Default,
    {
        Self::new(x, C::default())
    }

    fn map<X, Y, F>(self, f: F) -> GenNonEmpty<Sibling1<C0, Y>, Sibling1<C, Y>>
    where
        C0: Functor<X>,
        C: Functor<X>,
        F: Fn(X) -> Y,
    {
        GenNonEmpty::new(self.head.fmap(&f), self.tail.fmap(f))
    }

    fn repack<Cf>(self) -> GenNonEmpty<C0, Cf>
    where
        C: IntoNewCardinality<Cf>,
    {
        GenNonEmpty::new(self.head, self.tail.into_new_cardinality())
    }
}

impl<E, C> From<(E, C)> for GenNonEmpty<E, C> {
    fn from(value: (E, C)) -> Self {
        Self::new(value.0, value.1)
    }
}

pub trait OptionExt: Sized {
    type Inner;

    fn into_option(self) -> Option<Self::Inner>;

    fn transpose_log_result<V, P, LWC, RWC, EC0, ECn>(
        self,
    ) -> LogResult<Option<V>, P, LWC, RWC, EC0, ECn>
    where
        Self: OptionExt<Inner = LogResult<V, P, LWC, RWC, EC0, ECn>>,
        LWC: Default,
    {
        self.into_option()
            .map_or(LogResult::new_ok(None), |x| x.map_ok_value(Some))
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

    fn into_nowarn1<EC0>(self) -> NowarnResult<Self::Ok, (), EC0, Nothing<Self::Error>>
    where
        EC0: Applicative<Self::Error>,
    {
        self.into_log()
    }

    fn into_nowarn<EC0>(self) -> NowarnResult<Self::Ok, (), EC0, Vec<Self::Error>>
    where
        EC0: Applicative<Self::Error>,
    {
        self.into_log()
    }

    fn into_warn1<EC0>(self) -> NowarnResult<Self::Ok, (), EC0, Nothing<Self::Error>>
    where
        EC0: Applicative<Self::Error>,
    {
        self.into_log()
    }

    fn into_log<LWC, RWC, EC0, ECn>(self) -> LogResult<Self::Ok, (), LWC, RWC, EC0, ECn>
    where
        EC0: Applicative<Self::Error>,
        ECn: Default,
        LWC: Default,
        RWC: Default,
    {
        self.into_result().map_or_else(
            |e| Fail(Failure::new_from_one(e, ())),
            |s| Succ(Success::new1(s)),
        )
    }

    fn into_io_log<E, LWC, RWC, EC0, ECn>(self) -> LogResult<Self::Ok, (), LWC, RWC, EC0, ECn>
    where
        Self: ResultExt<Error = io::Error>,
        EC0: Applicative<ImpureError<E>>,
        ECn: Default,
        LWC: Default,
        RWC: Default,
    {
        self.into_result().map_err(ImpureError::IO).into_log()
    }

    fn into_deferred_fungible<EC0, ECn>(
        self,
        is_error: bool,
    ) -> DeferredFungible<Self::Ok, EC0, ECn>
    where
        Self::Ok: Default,
        EC0: Applicative<Self::Error>,
        ECn: FungibleError<Inner = Self::Error> + Default,
        ECn::Warn: Default,
    {
        match self.into_result() {
            Ok(s) => Succ(Success::new1(s)),
            Err(e) => {
                if is_error {
                    Fail(Failure::new_from_one(e, Self::Ok::default()))
                } else {
                    Succ(Success::new(Self::Ok::default(), ECn::error_to_warning(e)))
                }
            }
        }
    }

    fn into_deferred_fungible_opt<EC0, ECn>(
        self,
        is_error: bool,
    ) -> DeferredFungible<Option<Self::Ok>, EC0, ECn>
    where
        EC0: Applicative<Self::Error>,
        ECn: FungibleError<Inner = Self::Error> + Default,
        ECn::Warn: Default,
    {
        self.into_result()
            .map(Some)
            .into_deferred_fungible(is_error)
    }

    fn into_deferred_fungible_def<EC0, ECn>(
        self,
        default: Self::Ok,
        is_error: bool,
    ) -> DeferredFungible<Self::Ok, EC0, ECn>
    where
        EC0: Applicative<Self::Error>,
        ECn: FungibleError<Inner = Self::Error> + Default,
        ECn::Warn: Default,
    {
        self.into_result()
            .into_deferred_fungible_opt(is_error)
            .map_def_value(|v| v.unwrap_or(default))
    }

    fn into_succ<P, LWC, RWC, EC0, ECn>(self) -> LogResult<Self::Ok, P, LWC, RWC, EC0, ECn>
    where
        Self::Ok: Default,
        LWC: Default + Applicative<Self::Error>,
    {
        let ret = self.into_result().map_or_else(
            |e| Success::new(Self::Ok::default(), LWC::pure(e)),
            Success::new1,
        );
        Succ(ret)
    }

    fn into_succ_opt<P, LWC, RWC, EC0, ECn>(
        self,
    ) -> LogResult<Option<Self::Ok>, P, LWC, RWC, EC0, ECn>
    where
        LWC: Default + Applicative<Self::Error>,
    {
        self.into_result().map(Some).into_succ()
    }

    fn into_succ_or<P, LWC, RWC, EC0, ECn>(
        self,
        default: Self::Ok,
    ) -> LogResult<Self::Ok, P, LWC, RWC, EC0, ECn>
    where
        LWC: Applicative<Self::Error> + Default,
    {
        self.into_succ_opt().map_ok_value(|x| x.unwrap_or(default))
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

// fungible
impl<V, P, RWC, EC0, ECn> LogResult<V, P, ECn::Warn, RWC, EC0, ECn>
where
    ECn: FungibleError,
{
    pub(crate) fn new_fungible<E>(value: V, default: P, error: E, is_error: bool) -> Self
    where
        EC0: Applicative<E>,
        ECn: FungibleError<Inner = E> + Default,
        RWC: Default,
    {
        if is_error {
            Fail(Failure::new_from_one(error, default))
        } else {
            Succ(Success::new(value, ECn::error_to_warning(error)))
        }
    }
}

// commutative
impl<V, P, WC, EC0, ECn> CmtResult<V, P, WC, EC0, ECn> {
    pub(crate) fn recover_with<Ferr, Fsucc, Vf, Pf, EC0f, WCf, ECnf>(
        self,
        f_err: Ferr,
        f_succ: Fsucc,
    ) -> CmtResult<Vf, Pf, WCf, EC0f, ECnf>
    where
        Fsucc: FnOnce(V) -> CmtResult<Vf, Pf, WC, EC0f, ECnf>,
        Ferr: FnOnce(P, GenNonEmpty<EC0, ECn>) -> CmtResult<Vf, Pf, WC, EC0f, ECnf>,
        WC: Concatable<Out = WCf>,
    {
        match self {
            Succ(s) => s.and_maybe(f_succ),
            Fail(f) => Success::new(f.value, f.warnings).and_maybe(|v| f_err(v, f.errors)),
        }
    }

    /// Convert warnings of commutative Result
    pub(crate) fn cmt_warnings_into<W, Wf>(self) -> CmtResult<V, P, Sibling1<WC, Wf>, EC0, ECn>
    where
        W: Into<Wf>,
        WC: Functor<W>,
    {
        self.map_cmt_warnings(Into::into)
    }

    /// Map function over warnings of commutative Result
    pub(crate) fn map_cmt_warnings<F, W, Wf>(
        self,
        f: F,
    ) -> CmtResult<V, P, Sibling1<WC, Wf>, EC0, ECn>
    where
        F: Fn(W) -> Wf,
        WC: Functor<W>,
    {
        self.map(|s| s.map_warnings(&f))
            .map_err(|e| e.map_warnings(f))
    }

    pub(crate) fn cmt_warnings_to_errors<F, W, E>(
        self,
        conf: &SharedConfig,
        f: F,
    ) -> CmtResult<V, (), WC, EC0, ECn>
    where
        F: Fn(W) -> E,
        EC0: Applicative<E>,
        ECn: Extend<E> + Default,
        WC: IntoIterator<Item = W> + Default,
    {
        let res = self;
        if conf.warnings_are_errors {
            match res {
                Succ(s) => s.warnings_to_errors(f, |_| ()),
                Fail(e) => Fail(e.warnings_to_errors(f).map_value(|_| ())),
            }
        } else if conf.hide_warnings {
            res.map(Success::remove_warnings)
                .map_err(Failure::remove_warnings)
                .set_err_value(())
        } else {
            res.set_err_value(())
        }
    }

    /// Push a warning to a commutative Result.
    pub(crate) fn push_cmt_warning<W>(&mut self, w: W)
    where
        WC: Extend<W>,
    {
        match self {
            Succ(s) => s.push_warning(w),
            Fail(e) => e.push_warning(w),
        }
    }

    /// Add warnings to a commutative Result.
    pub(crate) fn extend_cmt_warnings<W>(&mut self, ws: impl IntoIterator<Item = W>)
    where
        WC: Extend<W>,
    {
        match self {
            Succ(s) => s.extend_warnings(ws),
            Fail(e) => e.extend_warnings(ws),
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
    pub(crate) fn eval_non_def_error<E, F>(self, f: F) -> CmtResult<V, (), WC, EC0, ECn>
    where
        F: FnOnce(&V) -> Option<E>,
        EC0: Applicative<E>,
        ECn: Extend<E> + Default,
    {
        match self.set_err_value(()) {
            Succ(x) => match f(&x.value) {
                Some(e) => Fail(x.fail(GenNonEmpty::new1(EC0::pure(e))).map_value(|_| ())),
                None => Succ(x),
            },
            Fail(x) => Fail(x),
        }
    }

    pub(crate) fn extend_fung_errors<M, E, W, Fv, Fp, Fw, Fe>(
        mut self,
        errors: impl IntoIterator<Item = M>,
        fv: Fv,
        fp: Fp,
        fw: Fw,
        fe: Fe,
        is_error: bool,
    ) -> Self
    where
        Fv: FnOnce(V) -> P,
        Fp: FnOnce(P) -> P,
        Fe: Fn(M) -> E,
        Fw: Fn(M) -> W,
        WC: Extend<W>,
        EC0: Applicative<E>,
        ECn: Extend<E> + Default + FungibleError<Inner = E>,
    {
        if is_error {
            let mut it = errors.into_iter().map(fe);
            match self {
                Succ(succ) => {
                    if let Some(e0) = it.by_ref().next() {
                        let mut es_ = GenNonEmpty::new1(EC0::pure(e0));
                        es_.extend(it);
                        Fail(succ.fail(es_).map_value(fv))
                    } else {
                        Succ(succ)
                    }
                }
                Fail(mut err) => {
                    err.extend_errors(it);
                    Fail(err.map_value(fp))
                }
            }
        } else {
            self.extend_cmt_warnings(errors.into_iter().map(fw));
            self.map_err_value(fp)
        }
    }

    pub(crate) fn and_cmt<F>(self, f: F) -> Self
    where
        F: FnOnce() -> CmtResult<(), P, WC, EC0, ECn>,
        WC: Semigroup,
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
    pub(crate) fn and_then_cmt<F, Vf>(self, f: F) -> CmtResult<Vf, P, WC, EC0, ECn>
    where
        F: FnOnce(V) -> CmtResult<Vf, P, WC, EC0, ECn>,
        WC: Semigroup,
    {
        match self {
            Succ(x) => x.and_maybe(f),
            Fail(x) => Fail(x),
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
    pub(crate) fn zip_cmt<E, V1, P1>(
        self,
        a: CmtResult<V1, P1, WC, EC0, ECn>,
    ) -> CmtResult<(V, V1), (), WC, EC0, Vec<E>>
    where
        EC0: Comonad<E>,
        ECn: IntoNewCardinality<Vec<E>> + IntoIterator<Item = E>,
        WC: Semigroup,
    {
        match (self, a) {
            (Succ(ax), Succ(bx)) => Succ(ax.zip_with(bx, |x, y| (x, y))),
            (Succ(ax), Fail(bx)) => Fail(ax.with_failure(bx, |_, _| ()).repack_errors()),
            (Fail(ax), Succ(bx)) => Fail(ax.with_success(bx, |_, _| ()).repack_errors()),
            (Fail(ax), Fail(bx)) => Fail(ax.zip_with(bx, |_, _| ())),
        }
    }

    /// Combine three commutative results.
    pub(crate) fn zip3_cmt<E, V1, V2, P1, P2>(
        self,
        a: CmtResult<V1, P1, WC, EC0, ECn>,
        b: CmtResult<V2, P2, WC, EC0, ECn>,
    ) -> CmtResult<(V, V1, V2), (), WC, EC0, Vec<E>>
    where
        EC0: Comonad<E>,
        ECn: IntoNewCardinality<Vec<E>> + IntoIterator<Item = E>,
        WC: Semigroup,
    {
        self.zip_cmt(a)
            .zip_cmt(b.repack())
            .map_ok_value(|((ax, bx), cx)| (ax, bx, cx))
    }

    /// Combine four commutative results.
    #[allow(clippy::type_complexity)]
    pub(crate) fn zip4_cmt<E, V1, V2, V3, P1, P2, P3>(
        self,
        a: CmtResult<V1, P1, WC, EC0, ECn>,
        b: CmtResult<V2, P2, WC, EC0, ECn>,
        c: CmtResult<V3, P3, WC, EC0, ECn>,
    ) -> CmtResult<(V, V1, V2, V3), (), WC, EC0, Vec<E>>
    where
        EC0: Comonad<E>,
        ECn: IntoNewCardinality<Vec<E>> + IntoIterator<Item = E>,
        WC: Semigroup,
    {
        self.zip3_cmt(a, b)
            .zip_cmt(c.repack())
            .map_ok_value(|((ax, bx, cx), dx)| (ax, bx, cx, dx))
    }

    /// Combine five commutative results.
    #[allow(clippy::type_complexity)]
    pub(crate) fn zip5_cmt<E, V1, V2, V3, V4, P1, P2, P3, P4>(
        self,
        a: CmtResult<V1, P1, WC, EC0, ECn>,
        b: CmtResult<V2, P2, WC, EC0, ECn>,
        c: CmtResult<V3, P3, WC, EC0, ECn>,
        d: CmtResult<V4, P4, WC, EC0, ECn>,
    ) -> CmtResult<(V, V1, V2, V3, V4), (), WC, EC0, Vec<E>>
    where
        EC0: Comonad<E>,
        ECn: IntoNewCardinality<Vec<E>> + IntoIterator<Item = E>,
        WC: Semigroup,
    {
        self.zip4_cmt(a, b, c)
            .zip_cmt(d.repack())
            .map_ok_value(|((ax, bx, cx, dx), ex)| (ax, bx, cx, dx, ex))
    }

    /// Combine six commutative results.
    #[allow(clippy::type_complexity)]
    pub(crate) fn zip6_cmt<E, V1, V2, V3, V4, V5, P1, P2, P3, P4, P5>(
        self,
        x1: CmtResult<V1, P1, WC, EC0, ECn>,
        x2: CmtResult<V2, P2, WC, EC0, ECn>,
        x3: CmtResult<V3, P3, WC, EC0, ECn>,
        x4: CmtResult<V4, P4, WC, EC0, ECn>,
        x5: CmtResult<V5, P5, WC, EC0, ECn>,
    ) -> CmtResult<(V, V1, V2, V3, V4, V5), (), WC, EC0, Vec<E>>
    where
        EC0: Comonad<E>,
        ECn: IntoNewCardinality<Vec<E>> + IntoIterator<Item = E>,
        WC: Semigroup,
    {
        self.zip5_cmt(x1, x2, x3, x4)
            .zip_cmt(x5.repack())
            .map_ok_value(|((y0, y1, y2, y3, y4), y5)| (y0, y1, y2, y3, y4, y5))
    }
}

// commutative/resolvable
impl<V, WC, EC0, E> CmtResult<V, (), WC, EC0, Nothing<E>> {
    /// Resolve commutative Result with into regular Result type.
    ///
    /// Warnings will be given outside the result since commutative Results by
    /// definition allow the same warnings in both Succ and Failor branches.
    pub fn resolve_cmt<Fwarn, Ferr, WarnRes, FailRes>(
        self,
        f_warnings: Fwarn,
        f_errors: Ferr,
    ) -> (WarnRes, Result<V, FailRes>)
    where
        EC0: Comonad<E>,
        Fwarn: FnOnce(WC) -> WarnRes,
        Ferr: FnOnce(E) -> FailRes,
    {
        match self {
            Succ(s) => {
                let (v, warn_res) = s.resolve(f_warnings);
                (warn_res, Ok(v))
            }
            Fail(e) => (
                f_warnings(e.warnings),
                Err(f_errors(e.errors.head.cm_extract())),
            ),
        }
    }
}

// deferred
impl<V, WC, EC0, ECn> Deferred<V, WC, EC0, ECn> {
    /// Set value of deferred Result
    pub(crate) fn set_def_value<Vf>(self, x: Vf) -> Deferred<Vf, WC, EC0, ECn> {
        self.map_def_value(|_| x)
    }

    /// Map function over Succ and Failor value of result (assumed same type).
    pub(crate) fn map_def_value<F, Vf>(self, f: F) -> Deferred<Vf, WC, EC0, ECn>
    where
        F: FnOnce(V) -> Vf,
    {
        match self {
            Succ(s) => Succ(s.map_value(f)),
            Fail(e) => Fail(e.map_value(f)),
        }
    }

    /// Push a warning based on the value in a deferred Result.
    ///
    /// This must be a deferred result because the same value type must exist
    /// on both Succ and Failor sides.
    pub(crate) fn eval_def_warning<W, F>(&mut self, f: F)
    where
        F: FnOnce(&V) -> Option<W>,
        WC: Extend<W>,
    {
        match self {
            Succ(s) => s.eval_warning(f),
            Fail(e) => e.eval_warning(f),
        }
    }

    /// Push an error based on the value in a deferred Result.
    ///
    /// If Result is Ok and the evaluation returns an error, the result will
    /// be converted to an error.
    ///
    /// This must be a deferred result because the same value type must exist
    /// on both Ok and Error sides.
    pub(crate) fn eval_def_error<E, F>(self, f: F) -> Self
    where
        F: FnOnce(&V) -> Option<E>,
        EC0: Applicative<E>,
        ECn: Extend<E> + Default,
    {
        match self {
            Succ(succ) => match f(&succ.value) {
                Some(e) => Fail(succ.fail(GenNonEmpty::new1(EC0::pure(e)))),
                None => Succ(succ),
            },
            Fail(mut err) => {
                if let Some(e) = f(&err.value) {
                    err.push_error(e);
                }
                Fail(err)
            }
        }
    }

    /// Push an error to a deferred Result.
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    pub(crate) fn push_def_error(self, e: EC0) -> Self
    where
        ECn: Extend<EC0> + Default,
    {
        match self {
            Succ(succ) => Fail(succ.fail(GenNonEmpty::new1(e))),
            Fail(mut err) => {
                err.push_error(e);
                Fail(err)
            }
        }
    }

    /// Push errors to a deferred Result.
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    pub(crate) fn extend_def_errors<E>(self, es: impl IntoIterator<Item = E>) -> Self
    where
        EC0: Applicative<E>,
        ECn: Extend<E> + Default,
    {
        match self {
            Succ(succ) => {
                let mut it = es.into_iter();
                if let Some(e0) = it.by_ref().next() {
                    let mut es_ = GenNonEmpty::new1(EC0::pure(e0));
                    es_.extend(it);
                    Fail(succ.fail(es_))
                } else {
                    Succ(succ)
                }
            }
            Fail(mut err) => {
                err.extend_errors(es);
                Fail(err)
            }
        }
    }

    /// Push non-fungible error to a deferred Result based on its value.
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    pub(crate) fn eval_def_non_fung_error<F, M, W, E>(mut self, is_error: bool, f: F) -> Self
    where
        F: FnOnce(&V) -> Option<M>,
        M: Into<E> + Into<W>,
        WC: Extend<W>,
        EC0: Applicative<E>,
        ECn: Extend<E> + Default,
    {
        if is_error {
            self.eval_def_error(|x| f(x).map(Into::into))
        } else {
            self.eval_def_warning(|x| f(x).map(Into::into));
            self
        }
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
    pub(crate) fn and_then_def<F, E, Vf, Pf>(self, f: F) -> CmtResult<Vf, Pf, WC, EC0, ECn>
    where
        F: FnOnce(V) -> CmtResult<Vf, Pf, WC, EC0, ECn>,
        EC0: Comonad<E>,
        WC: Semigroup,
        ECn: Extend<E> + IntoIterator<Item = E>,
    {
        match self {
            Succ(s) => s.and_maybe(f),
            Fail(e) => e.with_value(f),
        }
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
    pub(crate) fn zip_def<E, V1>(
        self,
        a: Deferred<V1, WC, EC0, ECn>,
    ) -> Deferred<(V, V1), WC, EC0, Vec<E>>
    where
        EC0: Comonad<E>,
        ECn: IntoNewCardinality<Vec<E>> + IntoIterator<Item = E>,
        WC: Semigroup,
    {
        match (self, a) {
            (Succ(ax), Succ(bx)) => Succ(ax.zip_with(bx, |x, y| (x, y))),
            (Succ(ax), Fail(bx)) => Fail(ax.with_failure(bx, |x, y| (x, y)).repack_errors()),
            (Fail(ax), Succ(bx)) => Fail(ax.with_success(bx, |x, y| (x, y)).repack_errors()),
            (Fail(ax), Fail(bx)) => Fail(ax.zip_with(bx, |x, y| (x, y))),
        }
    }

    /// Combine three deferred results.
    pub(crate) fn zip3_def<E, V1, V2>(
        self,
        a: Deferred<V1, WC, EC0, ECn>,
        b: Deferred<V2, WC, EC0, ECn>,
    ) -> Deferred<(V, V1, V2), WC, EC0, Vec<E>>
    where
        EC0: Comonad<E>,
        ECn: IntoNewCardinality<Vec<E>> + IntoIterator<Item = E>,
        WC: Semigroup,
    {
        self.zip_def(a)
            .zip_def(b.repack())
            .map_def_value(|((ax, bx), cx)| (ax, bx, cx))
    }

    /// Combine four deferred results.
    #[allow(clippy::type_complexity)]
    pub(crate) fn zip4_def<E, V1, V2, V3>(
        self,
        a: Deferred<V1, WC, EC0, ECn>,
        b: Deferred<V2, WC, EC0, ECn>,
        c: Deferred<V3, WC, EC0, ECn>,
    ) -> Deferred<(V, V1, V2, V3), WC, EC0, Vec<E>>
    where
        EC0: Comonad<E>,
        ECn: IntoNewCardinality<Vec<E>> + IntoIterator<Item = E>,
        WC: Semigroup,
    {
        self.zip3_def(a, b)
            .zip_def(c.repack())
            .map_def_value(|((ax, bx, cx), dx)| (ax, bx, cx, dx))
    }

    /// Combine five deferred results.
    #[allow(clippy::type_complexity)]
    pub(crate) fn zip5_def<E, V1, V2, V3, V4>(
        self,
        a: Deferred<V1, WC, EC0, ECn>,
        b: Deferred<V2, WC, EC0, ECn>,
        c: Deferred<V3, WC, EC0, ECn>,
        d: Deferred<V4, WC, EC0, ECn>,
    ) -> Deferred<(V, V1, V2, V3, V4), WC, EC0, Vec<E>>
    where
        EC0: Comonad<E>,
        ECn: IntoNewCardinality<Vec<E>> + IntoIterator<Item = E>,
        WC: Semigroup,
    {
        self.zip4_def(a, b, c)
            .zip_def(d.repack())
            .map_def_value(|((ax, bx, cx, dx), ex)| (ax, bx, cx, dx, ex))
    }

    /// Combine six deferred results.
    #[allow(clippy::type_complexity)]
    pub(crate) fn zip6_def<E, V1, V2, V3, V4, V5>(
        self,
        x1: Deferred<V1, WC, EC0, ECn>,
        x2: Deferred<V2, WC, EC0, ECn>,
        x3: Deferred<V3, WC, EC0, ECn>,
        x4: Deferred<V4, WC, EC0, ECn>,
        x5: Deferred<V5, WC, EC0, ECn>,
    ) -> Deferred<(V, V1, V2, V3, V4, V5), WC, EC0, Vec<E>>
    where
        EC0: Comonad<E>,
        ECn: IntoNewCardinality<Vec<E>> + IntoIterator<Item = E>,
        WC: Semigroup,
    {
        self.zip5_def(x1, x2, x3, x4)
            .zip_def(x5.repack())
            .map_def_value(|((y0, y1, y2, y3, y4), y5)| (y0, y1, y2, y3, y4, y5))
    }
}

// nowarn
impl<V, P, EC0, ECn> LogResult<V, P, Nothing<()>, Nothing<()>, EC0, ECn> {
    /// Lift Result with no warnings to non-commutative Result
    pub(crate) fn nowarn_into_non_cmt_warn<LWCf>(self) -> NonCmtResult<V, P, LWCf, EC0, ECn>
    where
        LWCf: Default,
    {
        self.map_either(Success::nowarn_into_warn, |x| x)
    }

    /// Lift Result with no warnings to commutative Result
    pub(crate) fn nowarn_into_warn<LWCf>(self) -> CmtResult<V, P, LWCf, EC0, ECn>
    where
        LWCf: Default,
    {
        self.map_either(Success::nowarn_into_warn, |x| x)
            .non_cmt_into_cmt()
    }

    pub(crate) fn infallible_nowarn_into(self) -> V
    where
        EC0: IsKind1,
        EC0::Family: Kind1<Type<Infallible> = EC0>,
    {
        let ret = match self {
            Succ(x) => Some(x),
            Fail(_) => None,
        };
        ret.expect("should be infallible").value
    }

    /// Set warnings in Succ side of Result with no warnings
    pub(crate) fn set_non_cmt_warnings<WC>(self, ws: WC) -> NonCmtResult<V, P, WC, EC0, ECn> {
        self.map(|s| s.set_warnings(ws))
    }

    /// Set warnings in both Succ and Error sides of Result
    pub(crate) fn set_cmt_warnings<WC>(self, ws: WC) -> CmtResult<V, P, WC, EC0, ECn> {
        match self {
            Succ(s) => Succ(s.set_warnings(ws)),
            Fail(e) => Fail(e.set_warnings(ws)),
        }
    }

    /// Monad-ically (kinda) chain a LogResult with no warnings.
    ///
    /// This is more generally than the commutative case because there we can't
    /// assume that the warnings on either side are empty. If the function
    /// returns Fail and the input is Succ, then the warnings from the input
    /// need to be appended to those in the Fail type, which means their types
    /// need to match.
    ///
    /// If we know there are no warnings, then the function can return a
    /// non-commutative result type.
    pub(crate) fn and_then_nowarn<F, Vf, LWC, RWC>(
        self,
        f: F,
    ) -> LogResult<Vf, P, LWC, RWC, EC0, ECn>
    where
        F: FnOnce(V) -> LogResult<Vf, P, LWC, RWC, EC0, ECn>,
        RWC: Default,
    {
        match self {
            Succ(x) => f(x.value),
            Fail(x) => Fail(x.nowarn_into_warn()),
        }
    }
}

impl<V, EC0, E> LogResult<V, (), Nothing<()>, Nothing<()>, EC0, Nothing<E>> {
    /// Resolve Result with no warnings into regular Result type.
    pub fn resolve_nowarn<F, FailRes>(self, f: F) -> Result<V, FailRes>
    where
        EC0: Comonad<E>,
        F: FnOnce(E) -> FailRes,
    {
        match self {
            Succ(s) => Ok(s.value),
            Fail(x) => Err(f(x.errors.head.cm_extract())),
        }
    }
}

// non-cummutative
impl<V, P, LWC, EC0, ECn> LogResult<V, P, LWC, Nothing<()>, EC0, ECn> {
    /// Lift non-commutative Result into commutative Result
    pub(crate) fn non_cmt_into_cmt(self) -> CmtResult<V, P, LWC, EC0, ECn>
    where
        LWC: Default,
    {
        self.map_err(Failure::nowarn_into_warn)
    }

    /// Convert warnings of a non-commutative Result
    pub(crate) fn non_cmt_warnings_into<W, Wf>(
        self,
    ) -> NonCmtResult<V, P, Sibling1<LWC, Wf>, EC0, ECn>
    where
        W: Into<Wf>,
        LWC: Functor<W>,
    {
        self.map_non_cmt_warnings(Into::into)
    }

    /// Map function over warnings of a non-commutative Result
    pub(crate) fn map_non_cmt_warnings<F, W, Wf>(
        self,
        f: F,
    ) -> NonCmtResult<V, P, Sibling1<LWC, Wf>, EC0, ECn>
    where
        F: Fn(W) -> Wf,
        LWC: Functor<W>,
    {
        self.map(|s| s.map_warnings(f))
    }

    /// Aggregate non-commutative/fungible errors into one error.
    pub(crate) fn aggregate_non_cmt_fung_errors<F, G, E>(
        self,
        f: F,
        g: G,
    ) -> NonCmtFungibleResult<V, P, Sibling1<EC0, E>, Nothing<E>>
    where
        F: FnOnce(LWC) -> E,
        G: FnOnce(GenNonEmpty<EC0, ECn>) -> E,
        EC0: IsKind1,
        Sibling1<EC0, E>: Applicative<E>,
        ECn: FungibleError<Inner = E>,
    {
        match self {
            Succ(s) => Succ(s.aggregate_warnings(f)),
            Fail(e) => Fail(e.aggregate_errors(g)),
        }
    }
}

// non-commutative/resolveable
impl<V, LWC, EC0, E> LogResult<V, (), LWC, Nothing<()>, EC0, Nothing<E>> {
    /// Resolve non-commutative Result with regular Result type.
    ///
    /// Warnings will be given on the Succ side since non-commutative Result's
    /// by definition cannot have warnings in the Fail branch.
    pub(crate) fn resolve_non_cmt<Fwarn, Ferr, WarnRes, FailRes>(
        self,
        f_warnings: Fwarn,
        f_errors: Ferr,
    ) -> Result<(V, WarnRes), FailRes>
    where
        EC0: Comonad<E>,
        Fwarn: FnOnce(LWC) -> WarnRes,
        Ferr: FnOnce(E) -> FailRes,
    {
        match self {
            Succ(x) => Ok(x.resolve(f_warnings)),
            Fail(x) => Err(f_errors(x.errors.head.cm_extract())),
        }
    }
}

// non-cummutative/fungible
impl<V, P, LWC, EC0, ECn> LogResult<V, P, LWC, Nothing<()>, EC0, ECn>
where
    ECn: FungibleError<Warn = LWC>,
{
    /// Convert errors in commutative/fungible Results
    #[allow(clippy::type_complexity)]
    pub(crate) fn non_cmt_fung_errors_into<Ei, Ef>(
        self,
    ) -> LogResult<V, P, Sibling1<LWC, Ef>, Nothing<()>, Sibling1<EC0, Ef>, Sibling1<ECn, Ef>>
    where
        Ei: Into<Ef>,
        EC0: Functor<Ei>,
        ECn: FungibleError<Inner = Ei> + Functor<Ei>,
        <ECn as FungibleError>::Warn: Functor<Ei>,
    {
        self.map_non_cmt_fung_errors(Into::into)
    }

    /// Convert errors in non-commutative/fungible Results
    #[allow(clippy::type_complexity)]
    pub(crate) fn map_non_cmt_fung_errors<F, Ei, Ef>(
        self,
        f: F,
    ) -> LogResult<V, P, Sibling1<LWC, Ef>, Nothing<()>, Sibling1<EC0, Ef>, Sibling1<ECn, Ef>>
    where
        F: Fn(Ei) -> Ef,
        EC0: Functor<Ei>,
        ECn: FungibleError<Inner = Ei> + Functor<Ei>,
        <ECn as FungibleError>::Warn: Functor<Ei>,
    {
        match self {
            Succ(s) => Succ(s.map_warnings(f)),
            Fail(e) => Fail(e.map_errors(f)),
        }
    }
}

// commutative/fungible
impl<V, P, EC0, ECn> LogResult<V, P, ECn::Warn, ECn::Warn, EC0, ECn>
where
    ECn: FungibleError,
{
    /// Map function over errors in commutative/fungible Results
    #[allow(clippy::type_complexity)]
    pub(crate) fn cmt_fung_errors_into<Ei, Ef>(
        self,
    ) -> CmtResult<V, P, Sibling1<ECn::Warn, Ef>, Sibling1<EC0, Ef>, Sibling1<ECn, Ef>>
    where
        Ei: Into<Ef>,
        EC0: Functor<Ei>,
        ECn: FungibleError<Inner = Ei> + Functor<Ei>,
        ECn::Warn: Functor<Ei>,
    {
        self.map_cmt_fung_errors(Into::into)
    }

    /// Map function over errors in non-commutative/fungible Results
    #[allow(clippy::type_complexity)]
    pub(crate) fn map_cmt_fung_errors<F, Ei, Ef>(
        self,
        f: F,
    ) -> CmtResult<V, P, Sibling1<ECn::Warn, Ef>, Sibling1<EC0, Ef>, Sibling1<ECn, Ef>>
    where
        F: Fn(Ei) -> Ef,
        EC0: Functor<Ei>,
        ECn: FungibleError<Inner = Ei> + Functor<Ei>,
        ECn::Warn: Functor<Ei>,
    {
        match self {
            Succ(s) => Succ(s.map_warnings(f)),
            Fail(e) => Fail(e.map_errors(&f).map_warnings(f)),
        }
    }

    /// Aggregate commutative/fungible errors into one error.
    pub(crate) fn aggregate_cmt_fung_errors<F, G, E>(
        self,
        f: F,
        g: G,
    ) -> CmtFungibleResult<V, P, Sibling1<EC0, E>, Nothing<E>>
    where
        F: FnOnce(ECn::Warn) -> E,
        G: FnOnce(GenNonEmpty<EC0, ECn>) -> E,
        EC0: IsKind1,
        Sibling1<EC0, E>: Applicative<E>,
        ECn: FungibleError<Inner = E>,
    {
        match self {
            Succ(s) => Succ(s.aggregate_warnings(f)),
            Fail(e) => Fail(e.aggregate_errors(g).aggregate_warnings(f)),
        }
    }
}

// deferred/fungible
impl<V, EC0, ECn> Deferred<V, ECn::Warn, EC0, ECn>
where
    ECn: FungibleError,
{
    pub(crate) fn new_deferred_fungible<E>(value: V, error: E, is_error: bool) -> Self
    where
        EC0: Applicative<E>,
        ECn: FungibleError<Inner = E> + Default,
        ECn::Warn: Default,
    {
        if is_error {
            Fail(Failure::new_from_one(error, value))
        } else {
            Succ(Success::new(value, ECn::error_to_warning(error)))
        }
    }

    /// Push fungible error to a deferred Result based on its value.
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    pub(crate) fn eval_def_fung_error<E, F>(mut self, is_error: bool, f: F) -> Self
    where
        F: FnOnce(&V) -> Option<E>,
        EC0: Applicative<E>,
        ECn: Extend<E> + Default + FungibleError,
        ECn::Warn: Extend<E>,
    {
        if is_error {
            self.eval_def_error(f)
        } else {
            self.eval_def_warning(f);
            self
        }
    }

    /// Push fungible error to a deferred Result.
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    pub(crate) fn push_def_fung_error<E>(self, e: E, is_error: bool) -> Self
    where
        EC0: Applicative<E>,
        ECn: Extend<E> + FungibleError<Inner = E> + Default,
        ECn::Warn: Extend<E>,
    {
        self.extend_def_fung_errors(iter::once(e), is_error)
    }

    /// Push fungible errors to a deferred Result.
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    pub(crate) fn extend_def_fung_errors<E>(
        mut self,
        xs: impl IntoIterator<Item = E>,
        is_error: bool,
    ) -> Self
    where
        EC0: Applicative<E>,
        ECn: Extend<E> + Default + FungibleError<Inner = E>,
        ECn::Warn: Extend<E>,
    {
        if is_error {
            self.extend_def_errors(xs)
        } else {
            self.extend_cmt_warnings(xs);
            self
        }
    }
}

impl<V, LWC, RWC, EC0, ECn> LogResult<V, (), LWC, RWC, EC0, ECn> {
    pub(crate) fn new_err1<E>(error: E) -> Self
    where
        RWC: Default,
        EC0: Applicative<E>,
        ECn: Default,
    {
        Fail(Failure::new_from_one(error, ()))
    }

    // TODO generic input?
    pub(crate) fn new_err<E>(error: GenNonEmpty<EC0, ECn>) -> Self
    where
        RWC: Default,
        EC0: Applicative<E>,
        ECn: Default,
    {
        Fail(Failure::new_from_many(error, ()))
    }

    pub(crate) fn new_err_from_iter<I, E>(errors: I, default: V) -> Self
    where
        I: IntoIterator<Item = E>,
        EC0: Applicative<E>,
        ECn: Extend<E> + Default,
        RWC: Default,
        LWC: Default,
    {
        match GenNonEmpty::collect(errors) {
            None => Self::new_ok(default),
            Some(e) => Self::new_err(e),
        }
    }
}

impl<V, P, LWC, RWC, EC0, ECn> LogResult<V, P, LWC, RWC, EC0, ECn> {
    pub(crate) fn map_either<F, G, Vf, Pf, LWCf, RWCf, EC0f, ECnf>(
        self,
        f: F,
        g: G,
    ) -> LogResult<Vf, Pf, LWCf, RWCf, EC0f, ECnf>
    where
        F: FnOnce(Success<V, LWC>) -> Success<Vf, LWCf>,
        G: FnOnce(Failure<P, RWC, EC0, ECn>) -> Failure<Pf, RWCf, EC0f, ECnf>,
    {
        match self {
            Succ(s) => Succ(f(s)),
            Fail(e) => Fail(g(e)),
        }
    }

    pub(crate) fn map<F, Vf, LWCf>(self, f: F) -> LogResult<Vf, P, LWCf, RWC, EC0, ECn>
    where
        F: FnOnce(Success<V, LWC>) -> Success<Vf, LWCf>,
    {
        self.map_either(f, |x| x)
    }

    pub(crate) fn map_err<F, Pf, RWCf, EC0f, ECnf>(
        self,
        f: F,
    ) -> LogResult<V, Pf, LWC, RWCf, EC0f, ECnf>
    where
        F: FnOnce(Failure<P, RWC, EC0, ECn>) -> Failure<Pf, RWCf, EC0f, ECnf>,
    {
        self.map_either(|x| x, f)
    }

    pub(crate) fn new_ok(value: V) -> Self
    where
        LWC: Default,
    {
        Succ(Success::new1(value))
    }

    pub(crate) fn new_ok_def() -> Self
    where
        V: Default,
        LWC: Default,
    {
        Self::new_ok(V::default())
    }

    pub(crate) fn new_non_fungible<E>(value: V, default: P, error: E, is_error: bool) -> Self
    where
        LWC: Default,
        RWC: Default,
        EC0: Applicative<E>,
        ECn: Default,
    {
        if is_error {
            Fail(Failure::new_from_one(error, default))
        } else {
            Succ(Success::new1(value))
        }
    }

    /// Map function over Succ value of Result
    pub(crate) fn map_ok_value<F, Vf>(self, f: F) -> LogResult<Vf, P, LWC, RWC, EC0, ECn>
    where
        F: FnOnce(V) -> Vf,
    {
        self.map(|s| s.map_value(f))
    }

    /// Run function when result is Succ
    pub(crate) fn when_ok<F>(self, f: F) -> Self
    where
        F: FnOnce(),
    {
        self.map_ok_value(|v| {
            f();
            v
        })
    }

    /// Map function over Failor value of Result
    pub(crate) fn map_err_value<F, Pf>(self, f: F) -> LogResult<V, Pf, LWC, RWC, EC0, ECn>
    where
        F: FnOnce(P) -> Pf,
    {
        self.map_err(|e| e.map_value(f))
    }

    /// Set value of Succ Result
    pub(crate) fn set_ok_value<Vf>(self, x: Vf) -> LogResult<Vf, P, LWC, RWC, EC0, ECn> {
        self.map_ok_value(|_| x)
    }

    /// Set value of Failor Result
    pub(crate) fn set_err_value<Pf>(self, x: Pf) -> LogResult<V, Pf, LWC, RWC, EC0, ECn> {
        self.map_err_value(|_| x)
    }

    /// Add a member to both the Succ and Failor value, returning both as a tuple.
    ///
    /// This seems weird but is useful for cases where we need to use a non-Copy
    /// variable in two closures for both branches but one closure will "eat"
    /// (move) the value before the other can use it. This function will move
    /// the value once depending on the branch where is can be consumed by
    /// both closures as an argument.
    pub(crate) fn inject_value<X>(self, x: X) -> LogResult<(V, X), (P, X), LWC, RWC, EC0, ECn> {
        match self {
            Succ(s) => Succ(s.map_value(|v| (v, x))),
            Fail(e) => Fail(e.map_value(|v| (v, x))),
        }
    }

    pub(crate) fn infallible_into<Pf, RWCf, EC0f, ECnf>(
        self,
    ) -> LogResult<V, Pf, LWC, RWCf, EC0f, ECnf>
    where
        EC0: IsKind1,
        EC0::Family: Kind1<Type<Infallible> = EC0>,
    {
        let ret = match self {
            Succ(x) => Some(x),
            Fail(_) => None,
        };
        Succ(ret.expect("should be infallible"))
    }

    pub(crate) fn infallible_with_warn_into<F, Wres>(self, f: F) -> (V, Wres)
    where
        F: FnOnce(LWC) -> Wres,
        EC0: IsKind1,
        EC0::Family: Kind1<Type<Infallible> = EC0>,
    {
        let ret = match self {
            Succ(x) => Some(x),
            Fail(_) => None,
        };
        let ret_ = ret.expect("should be infallible");
        (ret_.value, f(ret_.warnings))
    }

    /// Convert errors in Result
    ///
    /// This function will work on any Result type but may change a fungible
    /// Result to non-fungible one, which is generally not a good idea.
    /// See [`*_fung_errors_into`] for functions that will map over warnings
    /// if they are the same type as errors.
    pub(crate) fn non_fung_errors_into<Ei, Ef>(
        self,
    ) -> LogResult<V, P, LWC, RWC, Sibling1<EC0, Ef>, Sibling1<ECn, Ef>>
    where
        Ei: Into<Ef>,
        EC0: Functor<Ei>,
        ECn: Functor<Ei>,
    {
        self.map_non_fung_errors(Into::into)
    }

    /// Map function over errors in Result
    ///
    /// This function will work on any Result type but may change a fungible
    /// Result to non-fungible one, which is generally not a good idea.
    /// See [`map_*_fung_errors`] for functions that will map over warnings
    /// if they are the same type as errors.
    pub(crate) fn map_non_fung_errors<F, Ei, Ef>(
        self,
        f: F,
    ) -> LogResult<V, P, LWC, RWC, Sibling1<EC0, Ef>, Sibling1<ECn, Ef>>
    where
        F: Fn(Ei) -> Ef,
        EC0: Functor<Ei>,
        ECn: Functor<Ei>,
    {
        self.map_err(|e| e.map_errors(f))
    }

    pub(crate) fn repack<LWCf, RWCf, ECf>(self) -> LogResult<V, P, LWCf, RWCf, EC0, ECf>
    where
        LWC: IntoNewCardinality<LWCf>,
        RWC: IntoNewCardinality<RWCf>,
        ECn: IntoNewCardinality<ECf>,
    {
        self.repack_left_warnings()
            .repack_right_warnings()
            .repack_errors()
    }

    pub(crate) fn into_semigroup<E, LWCf, RWCf>(self) -> LogResult<V, P, LWCf, RWCf, EC0, Vec<E>>
    where
        LWC: IntoNewCardinality<LWCf>,
        RWC: IntoNewCardinality<RWCf>,
        ECn: IntoNewCardinality<Vec<E>>,
    {
        self.repack()
    }

    pub(crate) fn repack_warnings<WCf>(self) -> LogResult<V, P, WCf, WCf, EC0, ECn>
    where
        LWC: IntoNewCardinality<WCf>,
        RWC: IntoNewCardinality<WCf>,
    {
        self.repack_right_warnings().repack_left_warnings()
    }

    pub(crate) fn repack_left_warnings<LWCf>(self) -> LogResult<V, P, LWCf, RWC, EC0, ECn>
    where
        LWC: IntoNewCardinality<LWCf>,
    {
        self.map(Success::repack)
    }

    pub(crate) fn repack_right_warnings<RWCf>(self) -> LogResult<V, P, LWC, RWCf, EC0, ECn>
    where
        RWC: IntoNewCardinality<RWCf>,
    {
        self.map_err(Failure::repack_warnings)
    }

    pub(crate) fn repack_errors<ECf>(self) -> LogResult<V, P, LWC, RWC, EC0, ECf>
    where
        ECn: IntoNewCardinality<ECf>,
    {
        self.map_err(Failure::repack_errors)
    }

    pub(crate) fn repack_error<EC0f>(self) -> LogResult<V, P, LWC, RWC, EC0f, ECn>
    where
        EC0: IntoNewWrapper<EC0f>,
    {
        self.map_err(Failure::repack_error)
    }

    // fn remove_warnings(self) -> NowarnResult<Self::V, Self::P, Self::E, Self::EC> {
    //     self
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
    //     match self {
    //         Succ(s) => s.warnings_to_errors(f0, f1),
    //         Fail(e) => Fail(e.warnings_to_errors(f0).map_passthru(f2)),
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
    pub(crate) fn aggregate_non_fung_errors<F, E>(
        self,
        f: F,
    ) -> LogResult<V, P, LWC, RWC, Sibling1<EC0, E>, Nothing<E>>
    where
        // NOTE pretend there is a negative trait bound for "non-fungible"
        EC0: IsKind1,
        Sibling1<EC0, E>: Applicative<E>,
        F: FnOnce(GenNonEmpty<EC0, ECn>) -> E,
    {
        self.map_err(|e| e.aggregate_errors(f))
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn summarize_errors<E, S>(
        self,
    ) -> LogResult<V, P, LWC, RWC, Sibling1<EC0, ErrorSummary<E, S>>, Nothing<ErrorSummary<E, S>>>
    where
        EC0: IsKind1 + IntoNewWrapper<Identity<E>>,
        Sibling1<EC0, ErrorSummary<E, S>>: Applicative<ErrorSummary<E, S>>,
        ECn: IntoNewCardinality<Vec<E>>,
        S: Default,
    {
        self.summarize_errors_with(S::default())
    }

    // TODO pub only needed for python interface
    #[allow(clippy::type_complexity)]
    pub fn summarize_errors_with<E, S>(
        self,
        s: S,
    ) -> LogResult<V, P, LWC, RWC, Sibling1<EC0, ErrorSummary<E, S>>, Nothing<ErrorSummary<E, S>>>
    where
        EC0: IsKind1 + IntoNewWrapper<Identity<E>>,
        Sibling1<EC0, ErrorSummary<E, S>>: Applicative<ErrorSummary<E, S>>,
        ECn: IntoNewCardinality<Vec<E>>,
    {
        self.aggregate_non_fung_errors(|es| {
            let xs = es.bimap(
                IntoNewWrapper::into_new_wrapper,
                IntoNewCardinality::into_new_cardinality,
            );
            ErrorSummary::new(s, xs)
        })
    }

    /// Push a warning based on the Succ value of a non-deferred Result.
    ///
    /// Will only store warning on the Succ side since the value isn't present
    /// on the error side to be evaluated.
    pub(crate) fn eval_non_def_warning<W, F>(&mut self, f: F)
    where
        F: FnOnce(&V) -> Option<W>,
        LWC: Extend<W>,
    {
        if let Succ(s) = self {
            s.eval_warning(f);
        }
    }
}

/// Monoid-ically combine commutative results.
///
/// Ok values will be collected and returned as a single vector upon success.
/// Presence of any Error will cause Error to be returned. In any case,
/// warnings and errors as applicable will appended in order and returned.
///
/// The wrapper for warning must be a semigroup and wrapper for error must be
/// extendable since it might hold more than one error.
pub(crate) trait CmtResultIter<T, P, WC, EC0, ECn>:
    Iterator<Item = CmtResult<T, P, WC, EC0, ECn>> + Sized
{
    fn mappend_cmt<E>(mut self) -> CmtResult<Vec<T>, (), WC, EC0, ECn>
    where
        WC: Semigroup + Default,
        EC0: Comonad<E>,
        ECn: Extend<E> + IntoIterator<Item = E>,
    {
        let mut left_vs = vec![];
        let mut ws = WC::default();
        let mut error_head = None;
        for x in self.by_ref() {
            match x {
                Succ(y) => {
                    left_vs.push(y.value);
                    ws = ws.concat(y.warnings);
                }
                Fail(y) => {
                    error_head = Some(y);
                    break;
                }
            }
        }
        if let Some(h) = error_head {
            let mut es = h.errors;
            for x in self {
                match x {
                    Succ(y) => {
                        ws = ws.concat(y.warnings);
                    }
                    Fail(y) => {
                        ws = ws.concat(y.warnings);
                        es.extend(y.errors);
                    }
                }
            }
            Fail(Failure::new(ws, es, ()))
        } else {
            Succ(Success::new(left_vs, ws))
        }
    }
}

impl<I, V, P, WC, EC0, ECn> CmtResultIter<V, P, WC, EC0, ECn> for I where
    I: Iterator<Item = CmtResult<V, P, WC, EC0, ECn>>
{
}

/// Monoid-ically combine deferred results.
///
/// Values from Ok or Error will be collected and returned in a single vector
/// independent of the presence of warnings or errors.
///
/// The wrapper for warning must be a semigroup and wrapper for error must be
/// extendable since it might hold more than one error.
pub(crate) trait DeferredIter<T, WC, EC0, ECn>:
    Iterator<Item = Deferred<T, WC, EC0, ECn>> + Sized
{
    // TODO not DRY
    fn mappend_def<E>(mut self) -> Deferred<Vec<T>, WC, EC0, ECn>
    where
        WC: Semigroup + Default,
        EC0: Comonad<E>,
        ECn: Extend<E> + IntoIterator<Item = E>,
    {
        let mut vs = vec![];
        let mut ws = WC::default();
        let mut error_head = None;
        for x in self.by_ref() {
            match x {
                Succ(y) => {
                    vs.push(y.value);
                    ws = ws.concat(y.warnings);
                }
                Fail(y) => {
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
                    Succ(y) => {
                        vs.push(y.value);
                        ws = ws.concat(y.warnings);
                    }
                    Fail(y) => {
                        vs.push(y.value);
                        ws = ws.concat(y.warnings);
                        es.extend(y.errors);
                    }
                }
            }
            Fail(Failure::new(ws, es, vs))
        } else {
            Succ(Success::new(vs, ws))
        }
    }

    fn mappend_def_void<E>(self) -> Deferred<(), WC, EC0, ECn>
    where
        WC: Semigroup + Default,
        EC0: Comonad<E>,
        ECn: Extend<E> + IntoIterator<Item = E>,
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

impl<I, V, WC, EC0, ECn> DeferredIter<V, WC, EC0, ECn> for I where
    I: Iterator<Item = Deferred<V, WC, EC0, ECn>>
{
}

impl<E, S> fmt::Display for ErrorSummary<E, S>
where
    E: fmt::Display,
    S: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        writeln!(f, "Toplevel Error: {}", self.summary)?;
        let es = &self.errors;
        for e in iter::once(&es.head.0).chain(es.tail.iter()) {
            for l in e.to_string().lines() {
                writeln!(f, "  {l}")?;
            }
        }
        Ok(())
    }
}

#[cfg(feature = "python")]
mod python {
    use crate::{
        python::exceptions::{PyreflowException, PyreflowWarning},
        text::optional::Nothing,
    };

    use super::{
        CmtResult, Comonad, ErrorSummary, ImpureError, IsKind1, Kind1, LogResult, NowarnResult,
    };

    use pyo3::prelude::*;
    use std::convert::Infallible;
    use std::ffi::CString;
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

    impl<E, S> From<ErrorSummary<E, S>> for PyErr
    where
        E: Display,
        S: Display,
    {
        fn from(value: ErrorSummary<E, S>) -> Self {
            PyreflowException::new_err(value.to_string())
        }
    }

    impl<V, WC, EC0, E> CmtResult<V, (), WC, EC0, Nothing<E>> {
        pub fn py_termfail_resolve<W>(self) -> PyResult<V>
        where
            WC: IntoIterator<Item = W>,
            W: Display,
            E: Into<PyErr>,
            EC0: Comonad<E>,
        {
            let (warn, res) = self.resolve_cmt(emit_warnings, Into::into);
            warn?;
            res
        }
    }

    impl<V, EC0, E> NowarnResult<V, (), EC0, Nothing<E>> {
        pub fn py_termfail_resolve_nowarn(self) -> PyResult<V>
        where
            E: Into<PyErr>,
            EC0: Comonad<E>,
        {
            self.resolve_nowarn(Into::into)
        }
    }

    impl<V, LWC, RWC, EC0, E> LogResult<V, (), LWC, RWC, EC0, Nothing<E>> {
        pub fn py_term_resolve_noerror<W>(self) -> PyResult<V>
        where
            LWC: IntoIterator<Item = W>,
            W: Display,
            EC0: IsKind1,
            EC0::Family: Kind1<Type<Infallible> = EC0>,
        {
            let (value, warn) = self.infallible_with_warn_into(emit_warnings);
            warn?;
            Ok(value)
        }
    }

    fn emit_warnings<W>(ws: impl IntoIterator<Item = W>) -> PyResult<()>
    where
        W: Display,
    {
        Python::with_gil(|py| -> PyResult<()> {
            let wt = py.get_type::<PyreflowWarning>();
            for w in ws {
                let s = CString::new(w.to_string())?;
                PyErr::warn(py, &wt, &s, 0)?;
            }
            Ok(())
        })
    }
}
