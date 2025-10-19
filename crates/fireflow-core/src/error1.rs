use crate::text::optional::NeverValue;

use derive_new::new;
use std::convert::Infallible;
use std::io;
use std::iter;
use std::marker::PhantomData;
use std::mem;
use std::vec;
use thiserror::Error;

pub type MonoResult<V, P, E> = SimpleResult<V, P, E, NullFamily>;
pub type PolyResult<V, P, E> = SimpleResult<V, P, E, VecFamily>;

pub type MonoWarnableResult<V, P, E> = WarnableResult<V, P, E, OptFamily, NullFamily>;
pub type PolyWarnableResult<V, P, E> = WarnableResult<V, P, E, VecFamily, VecFamily>;

pub type WarningOrErrorResult<V, P, W, E> = DependentResult<V, P, W, E, OptFamily, NullFamily>;
pub type WarningsOrErrorResult<V, P, W, E> = DependentResult<V, P, W, E, VecFamily, NullFamily>;
pub type WarningOrErrorsResult<V, P, W, E> = DependentResult<V, P, W, E, OptFamily, VecFamily>;
pub type WarningsOrErrorsResult<V, P, W, E> = DependentResult<V, P, W, E, VecFamily, VecFamily>;

pub type WarningAndErrorResult<V, P, W, E> = IndependentResult<V, P, W, E, OptFamily, NullFamily>;
pub type WarningsAndErrorResult<V, P, W, E> = IndependentResult<V, P, W, E, VecFamily, NullFamily>;
pub type WarningAndErrorsResult<V, P, W, E> = IndependentResult<V, P, W, E, OptFamily, VecFamily>;
pub type WarningsAndErrorsResult<V, P, W, E> = IndependentResult<V, P, W, E, VecFamily, VecFamily>;

pub type SimpleResult<V, P, E, EC> = GenericResult<V, P, (), E, NullFamily, NullFamily, EC>;

pub type WarnableResult<V, P, E, WC, EC> = GenericResult<V, P, E, E, WC, NullFamily, EC>;

pub type DependentResult<V, P, W, E, WC, EC> = GenericResult<V, P, W, E, WC, NullFamily, EC>;

pub type IndependentResult<V, P, W, E, WC, EC> = GenericResult<V, P, W, E, WC, WC, EC>;

pub type GenericResult<V, P, W, E, LWC, RWC, EC> =
    Result<Success<V, W, LWC>, Failure<P, W, E, RWC, EC>>;

#[derive(new)]
#[new(visibility = "")]
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

pub trait ZeroOrMore: Sized {
    type Wrapper<T>: IntoIterator<Item = T> + Default;
    type IterOne<X>: Iterator<Item = X>;

    fn map<F, X, Y>(t: Self::Wrapper<X>, f: F) -> Self::Wrapper<Y>
    where
        F: Fn(X) -> Y;

    fn try_into_one_and_iter<X>(x: Self::Wrapper<X>) -> Option<(X, Self::IterOne<X>)>;

    fn try_into_one_or_more<X>(x: Self::Wrapper<X>) -> Option<GenNonEmpty<X, Self>>;
}

pub trait IntoZeroOrMore<Other: ZeroOrMore>: ZeroOrMore {
    fn into_zero_or_more<X>(x: Self::Wrapper<X>) -> Other::Wrapper<X>;
}

pub trait Concatable {
    type Out;
    fn concat(self, other: Self) -> Self::Out;
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

impl<V, W, WC: ZeroOrMore> Success<V, W, WC> {
    fn new1(value: impl Into<V>) -> Self {
        Self::new(value.into(), WC::Wrapper::<W>::default())
    }

    pub fn repack_warnings<WIF>(self) -> Success<V, W, WIF>
    where
        WC: IntoZeroOrMore<WIF>,
        WIF: ZeroOrMore,
    {
        Success::new(self.value, WC::into_zero_or_more(self.warnings))
    }

    pub fn value_into<U: From<V>>(self) -> Success<U, W, WC> {
        self.map(Into::into)
    }

    pub fn map<F: FnOnce(V) -> X, X>(self, f: F) -> Success<X, W, WC> {
        Success::new(f(self.value), self.warnings)
    }

    pub fn warnings_into<X: From<W>>(self) -> Success<V, X, WC> {
        self.map_warnings(Into::into)
    }

    pub fn map_warnings<F: Fn(W) -> ToW, ToW>(self, f: F) -> Success<V, ToW, WC> {
        Success::new(self.value, WC::map(self.warnings, f))
    }

    pub fn push_warning(&mut self, w: W)
    where
        WC::Wrapper<W>: Extend<W>,
    {
        self.warnings.extend(iter::once(w));
    }

    pub fn eval_warning<F>(&mut self, f: F)
    where
        F: FnOnce(&V) -> Option<W>,
        WC::Wrapper<W>: Extend<W>,
    {
        if let Some(e) = f(&self.value) {
            self.push_warning(e);
        }
    }

    pub fn and_maybe<F, ToV, P, E, WCF, EC>(self, f: F) -> GenericResult<ToV, P, W, E, WCF, WCF, EC>
    where
        F: FnOnce(V) -> GenericResult<ToV, P, W, E, WC, WC, EC>,
        EC: ZeroOrMore,
        WCF: ZeroOrMore,
        WC::Wrapper<W>: Concatable<Out = WCF::Wrapper<W>>,
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

    pub fn fail<E, EC>(self, errors: GenNonEmpty<E, EC>) -> Failure<V, W, E, WC, EC>
    where
        EC: ZeroOrMore,
    {
        Failure::new(self.warnings, errors, self.value)
    }

    pub fn with_failure<F, P, PF, E, WCF, EC>(
        self,
        other: Failure<P, W, E, WC, EC>,
        f: F,
    ) -> Failure<PF, W, E, WCF, EC>
    where
        F: FnOnce(V, P) -> PF,
        WCF: ZeroOrMore,
        EC: ZeroOrMore,
        WC::Wrapper<W>: Concatable<Out = WCF::Wrapper<W>>,
    {
        let ws = self.warnings.concat(other.warnings);
        Failure::new(ws, other.errors, f(self.value, other.passthru))
    }

    pub fn zip_with<F, V0, VF, WCF>(self, other: Success<V0, W, WC>, f: F) -> Success<VF, W, WCF>
    where
        F: FnOnce(V, V0) -> VF,
        WCF: ZeroOrMore,
        WC::Wrapper<W>: Concatable<Out = WCF::Wrapper<W>>,
    {
        let ws = self.warnings.concat(other.warnings);
        Success::new(f(self.value, other.value), ws)
    }

    fn remove_warnings<WF, WCF>(self) -> Success<V, WF, WCF>
    where
        WCF: ZeroOrMore,
    {
        Success::new1(self.value)
    }

    fn warnings_to_errors<E, F, WF, EC>(self, f: F) -> GenericResult<V, V, WF, E, WC, WC, EC>
    where
        F: Fn(W) -> E,
        EC: ZeroOrMore,
        WC: IntoZeroOrMore<EC>,
    {
        match WC::try_into_one_or_more(self.warnings) {
            None => Ok(Success::new1(self.value)),
            Some(ws) => Err(Failure::new_from_many(ws.map(f).repack(), self.value)),
        }
    }

    pub fn resolve<F, X>(self, f: F) -> (V, X)
    where
        F: FnOnce(WC::Wrapper<W>) -> X,
    {
        (self.value, f(self.warnings))
    }
}

impl<W, E, P, WC: ZeroOrMore, EC: ZeroOrMore> Failure<P, W, E, WC, EC> {
    fn new_from_one(error: E, passthru: P) -> Self {
        Self::new_from_many(GenNonEmpty::new1(error), passthru)
    }

    fn new_from_many(errors: GenNonEmpty<E, EC>, passthru: P) -> Self {
        Self::new(WC::Wrapper::<W>::default(), errors.into(), passthru)
    }

    pub fn repack_warnings<WIF>(self) -> Failure<P, W, E, WIF, EC>
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

    pub fn repack_errors<ECF>(self) -> Failure<P, W, E, WC, ECF>
    where
        EC: IntoZeroOrMore<ECF>,
        ECF: ZeroOrMore,
    {
        Failure::new(self.warnings, self.errors.repack(), self.passthru)
    }

    pub fn map_warnings<F, ToW>(self, f: F) -> Failure<P, ToW, E, WC, EC>
    where
        F: Fn(W) -> ToW,
    {
        Failure {
            warnings: WC::map(self.warnings, f),
            errors: self.errors,
            passthru: self.passthru,
        }
    }

    pub fn map_errors<F, ToE>(self, f: F) -> Failure<P, W, ToE, WC, EC>
    where
        F: Fn(E) -> ToE,
    {
        Failure {
            warnings: self.warnings,
            errors: self.errors.map(f),
            passthru: self.passthru,
        }
    }

    pub fn map_passthru<F, ToP>(self, f: F) -> Failure<ToP, W, E, WC, EC>
    where
        F: FnOnce(P) -> ToP,
    {
        Failure::new(self.warnings, self.errors, f(self.passthru))
    }

    pub fn push_warning(&mut self, w: W)
    where
        WC::Wrapper<W>: Extend<W>,
    {
        self.warnings.extend(iter::once(w));
    }

    pub fn eval_warning<F>(&mut self, f: F)
    where
        F: FnOnce(&P) -> Option<W>,
        WC::Wrapper<W>: Extend<W>,
    {
        if let Some(e) = f(&self.passthru) {
            self.push_warning(e);
        }
    }

    pub fn push_error(&mut self, e: E)
    where
        EC::Wrapper<E>: Extend<E>,
    {
        self.errors.extend(iter::once(e));
    }

    pub fn eval_error<F>(&mut self, f: F)
    where
        F: FnOnce(&P) -> Option<E>,
        EC::Wrapper<E>: Extend<E>,
    {
        if let Some(e) = f(&self.passthru) {
            self.push_error(e);
        }
    }

    pub fn with_passthru<F, V, ToP, WCF>(
        mut self,
        f: F,
    ) -> GenericResult<V, ToP, W, E, WCF, WCF, EC>
    where
        F: FnOnce(P) -> GenericResult<V, ToP, W, E, WC, WC, EC>,
        WCF: ZeroOrMore,
        WC::Wrapper<W>: Concatable<Out = WCF::Wrapper<W>>,
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

    pub fn zip_with<F, P0, PF, WCF, ECF>(
        self,
        other: Failure<P0, W, E, WC, EC>,
        f: F,
    ) -> Failure<PF, W, E, WCF, ECF>
    where
        F: FnOnce(P, P0) -> PF,
        WCF: ZeroOrMore,
        ECF: ZeroOrMore,
        EC: IntoZeroOrMore<ECF>,
        WC::Wrapper<W>: Concatable<Out = WCF::Wrapper<W>>,
        ECF::Wrapper<E>: Extend<E>,
    {
        let ws = self.warnings.concat(other.warnings);
        let mut es = self.errors.into_zero_or_more();
        es.extend(other.errors);
        Failure::new(ws, es, f(self.passthru, other.passthru))
    }

    pub fn gather_errors<F, EF>(self, f: F) -> Failure<P, W, EF, WC, NullFamily>
    where
        F: FnOnce(GenNonEmpty<E, EC>) -> EF,
    {
        let es = GenNonEmpty::new1(f(self.errors));
        Failure::new(self.warnings, es, self.passthru)
    }

    pub fn summarize_errors<S>(
        self,
        summary: S,
    ) -> Failure<P, W, ErrorSummary<E, S>, WC, NullFamily>
    where
        EC: IntoZeroOrMore<VecFamily>,
    {
        self.gather_errors(|es| ErrorSummary::new(summary, es.into_zero_or_more()))
    }

    pub fn with_success<F, V, PF, WCF>(
        self,
        other: Success<V, W, WC>,
        f: F,
    ) -> Failure<PF, W, E, WCF, EC>
    where
        F: FnOnce(P, V) -> PF,
        WCF: ZeroOrMore,
        WC::Wrapper<W>: Concatable<Out = WCF::Wrapper<W>>,
    {
        let ws = self.warnings.concat(other.warnings);
        Failure::new(ws, self.errors, f(self.passthru, other.value))
    }

    fn remove_warnings<WF, WCF>(self) -> Failure<P, WF, E, WCF, EC>
    where
        WCF: ZeroOrMore,
    {
        Failure::new(WCF::Wrapper::<WF>::default(), self.errors, self.passthru)
    }

    fn warnings_to_errors<F, WF, WCF>(mut self, f: F) -> Failure<P, WF, E, WCF, EC>
    where
        F: Fn(W) -> E,
        WCF: ZeroOrMore,
        EC::Wrapper<E>: Extend<E>,
    {
        self.errors.extend(WC::map(self.warnings, f));
        Failure::new_from_many(self.errors, self.passthru)
    }
}

impl<W, E, P, WC: ZeroOrMore> Failure<P, W, E, WC, NullFamily> {
    pub fn resolve<F, G, X, Y>(self, f: F, g: G) -> (X, Y)
    where
        F: FnOnce(WC::Wrapper<W>) -> X,
        G: FnOnce(E) -> Y,
    {
        (f(self.warnings), g(self.errors.head))
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

pub trait ResultExt: Sized {
    type Ok;
    type Error;

    fn new_mono(
        value: Self::Ok,
        error: Self::Error,
        is_error: bool,
    ) -> MonoResult<Self::Ok, (), Self::Error> {
        if is_error {
            Err(Failure::new_from_one(error, ()))
        } else {
            Ok(Success::new1(value))
        }
    }

    fn new_mono_warnable(
        value: Self::Ok,
        error: Self::Error,
        is_error: bool,
    ) -> MonoWarnableResult<Self::Ok, (), Self::Error> {
        if is_error {
            Err(Failure::new_from_one(error, ()))
        } else {
            Ok(Success::new(value, Some(error)))
        }
    }

    fn into_result(self) -> Result<Self::Ok, Self::Error>;

    fn as_result(&self) -> Result<&Self::Ok, &Self::Error>;

    fn as_result_mut(&mut self) -> Result<&mut Self::Ok, &mut Self::Error>;

    fn into_generic<W, LWC, RWC, EC>(
        self,
    ) -> GenericResult<Self::Ok, (), W, Self::Error, LWC, RWC, EC>
    where
        LWC: ZeroOrMore,
        RWC: ZeroOrMore,
        EC: ZeroOrMore;

    fn map_ok<ToOk, F>(self, f: F) -> Result<ToOk, Self::Error>
    where
        F: FnOnce(Self::Ok) -> ToOk;

    fn map_error<ToError, F>(self, f: F) -> Result<Self::Ok, ToError>
    where
        F: FnOnce(Self::Error) -> ToError;

    fn bind_ok<F, ToOk>(self, f: F) -> Result<ToOk, Self::Error>
    where
        F: FnOnce(Self::Ok) -> Result<ToOk, Self::Error>;

    fn bind_error<F, ToError>(self, f: F) -> Result<Self::Ok, ToError>
    where
        F: FnOnce(Self::Error) -> Result<Self::Ok, ToError>;

    fn bibind<F, G, ToOk, ToError>(self, f: F, g: G) -> Result<ToOk, ToError>
    where
        F: FnOnce(Self::Ok) -> Result<ToOk, ToError>,
        G: FnOnce(Self::Error) -> Result<ToOk, ToError>;
}

impl<V, E> ResultExt for Result<V, E> {
    type Ok = V;
    type Error = E;

    fn into_generic<W, LWC, RWC, EC>(self) -> GenericResult<V, (), W, E, LWC, RWC, EC>
    where
        LWC: ZeroOrMore,
        RWC: ZeroOrMore,
        EC: ZeroOrMore,
    {
        self.map(Success::new1)
            .map_err(|e| Failure::new_from_one(e, ()))
    }

    fn into_result(self) -> Result<V, E> {
        self
    }

    fn as_result(&self) -> Result<&V, &E> {
        self.as_ref()
    }

    fn as_result_mut(&mut self) -> Result<&mut V, &mut E> {
        self.as_mut()
    }

    fn map_ok<ToV, F>(self, f: F) -> Result<ToV, E>
    where
        F: FnOnce(V) -> ToV,
    {
        self.map(f)
    }

    fn map_error<ToE, F>(self, f: F) -> Result<V, ToE>
    where
        F: FnOnce(E) -> ToE,
    {
        self.map_err(f)
    }

    fn bind_ok<F, ToV>(self, f: F) -> Result<ToV, E>
    where
        F: FnOnce(V) -> Result<ToV, E>,
    {
        f(self?)
    }

    fn bind_error<F, ToE>(self, f: F) -> Result<V, ToE>
    where
        F: FnOnce(E) -> Result<V, ToE>,
    {
        match self {
            Ok(x) => Ok(x),
            Err(e) => f(e),
        }
    }

    fn bibind<F, G, ToV, ToE>(self, f: F, g: G) -> Result<ToV, ToE>
    where
        F: FnOnce(V) -> Result<ToV, ToE>,
        G: FnOnce(E) -> Result<ToV, ToE>,
    {
        match self {
            Ok(x) => f(x),
            Err(e) => g(e),
        }
    }
}

pub trait GenericResultExt
where
    Self: Sized
        + ResultExt<
            Ok = Success<Self::V, Self::W, Self::LWC>,
            Error = Failure<Self::P, Self::W, Self::E, Self::RWC, Self::EC>,
        >,
{
    type V;
    type P;
    type E;
    type W;
    type LWC: ZeroOrMore;
    type RWC: ZeroOrMore;
    type EC: ZeroOrMore;

    fn value_into<ToV>(
        self,
    ) -> GenericResult<ToV, Self::P, Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        ToV: From<Self::V>,
    {
        self.map_value(Into::into)
    }

    fn warnings_into<ToW>(
        self,
    ) -> GenericResult<Self::V, Self::P, ToW, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        ToW: From<Self::W>,
    {
        self.map_warnings(Into::into)
    }

    fn errors_into<ToE>(
        self,
    ) -> GenericResult<Self::V, Self::P, Self::W, ToE, Self::LWC, Self::RWC, Self::EC>
    where
        ToE: From<Self::E>,
    {
        self.map_errors(Into::into)
    }

    fn map_value<F, ToV>(
        self,
        f: F,
    ) -> GenericResult<ToV, Self::P, Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: FnOnce(Self::V) -> ToV,
    {
        self.map_ok(|s| s.map(f))
    }

    fn map_passthru<F, ToP>(
        self,
        f: F,
    ) -> GenericResult<Self::V, ToP, Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: FnOnce(Self::P) -> ToP,
    {
        self.map_error(|e| e.map_passthru(f))
    }

    fn map_warnings<F, ToW>(
        self,
        f: F,
    ) -> GenericResult<Self::V, Self::P, ToW, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: Fn(Self::W) -> ToW,
    {
        self.map_ok(|s| s.map_warnings(&f))
            .map_error(|e| e.map_warnings(f))
    }

    fn map_errors<F, ToE>(
        self,
        f: F,
    ) -> GenericResult<Self::V, Self::P, Self::W, ToE, Self::LWC, Self::RWC, Self::EC>
    where
        F: Fn(Self::E) -> ToE,
    {
        self.map_error(|e| e.map_errors(f))
    }

    fn liftio_errors(
        self,
    ) -> GenericResult<
        Self::V,
        Self::P,
        Self::W,
        ImpureError<Self::E>,
        Self::LWC,
        Self::RWC,
        Self::EC,
    > {
        self.map_errors(ImpureError::Pure)
    }

    fn repack<LWCF, RWCF, ECF>(
        self,
    ) -> GenericResult<Self::V, Self::P, Self::W, Self::E, LWCF, RWCF, ECF>
    where
        Self::LWC: IntoZeroOrMore<LWCF>,
        LWCF: ZeroOrMore,
        Self::RWC: IntoZeroOrMore<RWCF>,
        RWCF: ZeroOrMore,
        Self::EC: IntoZeroOrMore<ECF>,
        ECF: ZeroOrMore,
    {
        self.repack_left_warnings()
            .repack_right_warnings()
            .repack_errors()
    }

    fn repack_left_warnings<LWCF>(
        self,
    ) -> GenericResult<Self::V, Self::P, Self::W, Self::E, LWCF, Self::RWC, Self::EC>
    where
        Self::LWC: IntoZeroOrMore<LWCF>,
        LWCF: ZeroOrMore,
    {
        self.map_ok(Success::repack_warnings)
    }

    fn repack_right_warnings<RWCF>(
        self,
    ) -> GenericResult<Self::V, Self::P, Self::W, Self::E, Self::LWC, RWCF, Self::EC>
    where
        Self::RWC: IntoZeroOrMore<RWCF>,
        RWCF: ZeroOrMore,
    {
        self.map_error(Failure::repack_warnings)
    }

    fn repack_errors<ECF>(
        self,
    ) -> GenericResult<Self::V, Self::P, Self::W, Self::E, Self::LWC, Self::RWC, ECF>
    where
        Self::EC: IntoZeroOrMore<ECF>,
        ECF: ZeroOrMore,
    {
        self.map_error(Failure::repack_errors)
    }

    fn eval_dep_warning<F>(&mut self, f: F)
    where
        F: FnOnce(&Self::V) -> Option<Self::W>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
        <Self::RWC as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
        Self: GenericResultExt<V = <Self as GenericResultExt>::P>,
    {
        match self.as_result_mut() {
            Ok(s) => s.eval_warning(f),
            Err(e) => e.eval_warning(f),
        }
    }

    fn eval_indep_warning<F>(&mut self, f: F)
    where
        F: FnOnce(&Self::V) -> Option<Self::W>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
    {
        if let Ok(s) = self.as_result_mut() {
            s.eval_warning(f)
        }
    }

    fn eval_error<F>(
        self,
        f: F,
    ) -> GenericResult<Self::V, Self::P, Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: FnOnce(&Self::V) -> Option<Self::E>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: GenericResultExt<
            V = <Self as GenericResultExt>::P,
            LWC = <Self as GenericResultExt>::RWC,
        >,
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

    fn push_dep_warning(&mut self, w: Self::W)
    where
        <Self::LWC as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
    {
        if let Ok(s) = self.as_result_mut() {
            s.push_warning(w)
        }
    }

    fn push_indep_warning(&mut self, w: Self::W)
    where
        <Self::LWC as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
        <Self::RWC as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
    {
        match self.as_result_mut() {
            Ok(s) => s.push_warning(w),
            Err(e) => e.push_warning(w),
        }
    }

    fn push_error(
        self,
        e: Self::E,
    ) -> GenericResult<Self::V, Self::P, Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: GenericResultExt<
            V = <Self as GenericResultExt>::P,
            LWC = <Self as GenericResultExt>::RWC,
        >,
    {
        match self.into_result() {
            Ok(succ) => Err(succ.fail(GenNonEmpty::new1(e))),
            Err(mut err) => {
                err.push_error(e);
                Err(err)
            }
        }
    }

    fn push_error_or_warning<X>(
        mut self,
        x: X,
        is_error: bool,
    ) -> GenericResult<Self::V, Self::P, Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        X: Into<Self::W> + Into<Self::E>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::W>: Extend<Self::W>,
        Self: GenericResultExt<
            V = <Self as GenericResultExt>::P,
            LWC = <Self as GenericResultExt>::RWC,
        >,
    {
        if is_error {
            self.push_error(x.into())
        } else {
            // NOTE call independent warning version to ensure warning goes
            // on both sides
            self.push_indep_warning(x.into());
            self.into_result()
        }
    }

    fn void_value(
        self,
    ) -> GenericResult<(), Self::P, Self::W, Self::E, Self::LWC, Self::RWC, Self::EC> {
        self.map_value(|_| ())
    }

    fn void_passthru(
        self,
    ) -> GenericResult<Self::V, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC> {
        self.map_passthru(|_| ())
    }

    fn and_maybe<F, ToV, WCF>(
        self,
        f: F,
    ) -> GenericResult<ToV, Self::P, Self::W, Self::E, WCF, WCF, Self::EC>
    where
        F: FnOnce(
            Self::V,
        )
            -> GenericResult<ToV, Self::P, Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
        Self: GenericResultExt<LWC = <Self as GenericResultExt>::RWC>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::W>: Concatable<Out = WCF::Wrapper<Self::W>>,
        WCF: ZeroOrMore,
        Self::RWC: IntoZeroOrMore<WCF>,
    {
        self.map_error(|e| e.repack_warnings())
            .bind_ok(|s| s.and_maybe(f))
    }

    fn and_tentatively<F, ToV, ToP, WCF>(
        self,
        f: F,
    ) -> GenericResult<ToV, ToP, Self::W, Self::E, WCF, WCF, Self::EC>
    where
        F: FnOnce(
            Self::V,
        )
            -> GenericResult<ToV, ToP, Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
        Self: GenericResultExt<
            LWC = <Self as GenericResultExt>::RWC,
            V = <Self as GenericResultExt>::P,
        >,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::W>: Concatable<Out = WCF::Wrapper<Self::W>>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        WCF: ZeroOrMore,
    {
        match self.into_result() {
            Ok(s) => s.and_maybe(f),
            Err(e) => e.with_passthru(f),
        }
    }

    fn zip<V1, WCF, ECF>(
        self,
        a: GenericResult<V1, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
    ) -> GenericResult<(Self::V, V1), (), Self::W, Self::E, WCF, WCF, ECF>
    where
        WCF: ZeroOrMore,
        ECF: ZeroOrMore,
        ECF::Wrapper<Self::E>: Extend<Self::E>,
        Self::EC: IntoZeroOrMore<ECF>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::W>: Concatable<Out = WCF::Wrapper<Self::W>>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: GenericResultExt<P = (), LWC = <Self as GenericResultExt>::RWC>,
    {
        match (self.into_result(), a) {
            (Ok(ax), Ok(bx)) => Ok(ax.zip_with(bx, |x, y| (x, y))),
            (Ok(ax), Err(bx)) => Err(ax.with_failure(bx, |_, ()| ()).repack_errors()),
            (Err(ax), Ok(bx)) => Err(ax.with_success(bx, |(), _| ()).repack_errors()),
            (Err(ax), Err(bx)) => Err(ax.zip_with(bx, |(), ()| ())),
        }
    }

    fn zip3<V1, V2, WCF, ECF>(
        self,
        a: GenericResult<V1, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
        b: GenericResult<V2, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
    ) -> GenericResult<(Self::V, V1, V2), (), Self::W, Self::E, WCF, WCF, ECF>
    where
        WCF: ZeroOrMore,
        ECF: ZeroOrMore,
        ECF::Wrapper<Self::E>: Extend<Self::E>,
        Self::EC: IntoZeroOrMore<ECF>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::W>: Concatable<Out = WCF::Wrapper<Self::W>>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: GenericResultExt<P = (), LWC = <Self as GenericResultExt>::RWC>,
        Self::LWC: IntoZeroOrMore<WCF>,
        Self::EC: IntoZeroOrMore<ECF>,
        WCF::Wrapper<Self::W>: Concatable<Out = WCF::Wrapper<Self::W>>,
    {
        let res = self.zip(a);
        let b_ = b.repack();
        GenericResultExt::zip(res, b_).map_value(|((ax, bx), cx)| (ax, bx, cx))
    }

    fn zip4<V1, V2, V3, WCF, ECF>(
        self,
        a: GenericResult<V1, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
        b: GenericResult<V2, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
        c: GenericResult<V3, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
    ) -> GenericResult<(Self::V, V1, V2, V3), (), Self::W, Self::E, WCF, WCF, ECF>
    where
        WCF: ZeroOrMore,
        ECF: ZeroOrMore,
        ECF::Wrapper<Self::E>: Extend<Self::E>,
        Self::EC: IntoZeroOrMore<ECF>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::W>: Concatable<Out = WCF::Wrapper<Self::W>>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: GenericResultExt<P = (), LWC = <Self as GenericResultExt>::RWC>,
        Self::LWC: IntoZeroOrMore<WCF>,
        Self::EC: IntoZeroOrMore<ECF>,
        WCF::Wrapper<Self::W>: Concatable<Out = WCF::Wrapper<Self::W>>,
    {
        let res = self.zip3(a, b);
        let c_ = c.repack();
        GenericResultExt::zip(res, c_).map_value(|((ax, bx, cx), dx)| (ax, bx, cx, dx))
    }

    fn zip5<V1, V2, V3, V4, WCF, ECF>(
        self,
        a: GenericResult<V1, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
        b: GenericResult<V2, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
        c: GenericResult<V3, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
        d: GenericResult<V4, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
    ) -> GenericResult<(Self::V, V1, V2, V3, V4), (), Self::W, Self::E, WCF, WCF, ECF>
    where
        WCF: ZeroOrMore,
        ECF: ZeroOrMore,
        ECF::Wrapper<Self::E>: Extend<Self::E>,
        Self::EC: IntoZeroOrMore<ECF>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::W>: Concatable<Out = WCF::Wrapper<Self::W>>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: GenericResultExt<P = (), LWC = <Self as GenericResultExt>::RWC>,
        Self::LWC: IntoZeroOrMore<WCF>,
        Self::EC: IntoZeroOrMore<ECF>,
        WCF::Wrapper<Self::W>: Concatable<Out = WCF::Wrapper<Self::W>>,
    {
        let res = self.zip4(a, b, c);
        let d_ = d.repack();
        GenericResultExt::zip(res, d_).map_value(|((ax, bx, cx, dx), ex)| (ax, bx, cx, dx, ex))
    }

    fn zip6<V1, V2, V3, V4, V5, WCF, ECF>(
        self,
        a: GenericResult<V1, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
        b: GenericResult<V2, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
        c: GenericResult<V3, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
        d: GenericResult<V4, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
        e: GenericResult<V5, (), Self::W, Self::E, Self::LWC, Self::RWC, Self::EC>,
    ) -> GenericResult<(Self::V, V1, V2, V3, V4, V5), (), Self::W, Self::E, WCF, WCF, ECF>
    where
        WCF: ZeroOrMore,
        ECF: ZeroOrMore,
        ECF::Wrapper<Self::E>: Extend<Self::E>,
        Self::EC: IntoZeroOrMore<ECF>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::W>: Concatable<Out = WCF::Wrapper<Self::W>>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: GenericResultExt<P = (), LWC = <Self as GenericResultExt>::RWC>,
        Self::LWC: IntoZeroOrMore<WCF>,
        Self::EC: IntoZeroOrMore<ECF>,
        WCF::Wrapper<Self::W>: Concatable<Out = WCF::Wrapper<Self::W>>,
    {
        let res = self.zip5(a, b, c, d);
        let e_ = e.repack();
        GenericResultExt::zip(res, e_)
            .map_value(|((ax, bx, cx, dx, ex), fx)| (ax, bx, cx, dx, ex, fx))
    }

    fn gather_errors<F, EF>(
        self,
        f: F,
    ) -> GenericResult<Self::V, Self::P, Self::W, EF, Self::LWC, Self::RWC, NullFamily>
    where
        F: FnOnce(GenNonEmpty<Self::E, Self::EC>) -> EF,
    {
        self.map_error(|e| e.gather_errors(f))
    }

    fn summarize_errors<S>(
        self,
        summary: S,
    ) -> GenericResult<
        Self::V,
        Self::P,
        Self::W,
        ErrorSummary<Self::E, S>,
        Self::LWC,
        Self::RWC,
        NullFamily,
    >
    where
        Self::EC: IntoZeroOrMore<VecFamily>,
    {
        self.map_error(|e| e.summarize_errors(summary))
    }

    fn from_infallible<PF, EF>(
        self,
    ) -> GenericResult<Self::V, PF, Self::W, EF, Self::LWC, Self::RWC, Self::EC>
    where
        Self: GenericResultExt<E = Infallible>,
    {
        // NOTE dirty hack because rust can't tell that the error side can never
        // happen when E is Infallible. The error side has one field with type
        // E that isn't wrapped (which means it can't be PhantomData<E>) and
        // thus can't be instantiated with Infallible by definition.
        let Ok(ret) = self.into_result();
        Ok(ret)
        // let ret = match self.into_result() {
        //     Ok(tnt) => Some(Success::new(tnt.value, tnt.warnings)),
        //     Err(_) => None,
        // }
        // .expect("infallible result should not happen");
        // Ok(ret)
    }

    fn resolve_errors<Ferr, ErrRes>(self, f_errors: Ferr) -> Result<Self::V, ErrRes>
    where
        Self: GenericResultExt<EC = NullFamily, LWC = NullFamily, RWC = NullFamily>,
        Ferr: FnOnce(Self::E) -> ErrRes,
    {
        match self.into_result() {
            Ok(s) => {
                let (v, ()) = s.resolve(|_| ());
                Ok(v)
            }
            Err(e) => {
                let ((), err_res) = e.resolve(|_| (), f_errors);
                Err(err_res)
            }
        }
    }

    fn resolve_warnings_or_errors<Fwarn, Ferr, WarnRes, ErrRes>(
        self,
        f_warnings: Fwarn,
        f_errors: Ferr,
    ) -> Result<(WarnRes, Self::V), ErrRes>
    where
        Self: GenericResultExt<EC = NullFamily, RWC = NullFamily>,
        Fwarn: FnOnce(<Self::LWC as ZeroOrMore>::Wrapper<Self::W>) -> WarnRes,
        Ferr: FnOnce(Self::E) -> ErrRes,
    {
        match self.into_result() {
            Ok(s) => {
                let (v, warn_res) = s.resolve(f_warnings);
                Ok((warn_res, v))
            }
            Err(e) => {
                let ((), err_res) = e.resolve(|_| (), f_errors);
                Err(err_res)
            }
        }
    }

    fn resolve_warnings_and_errors<Fwarn, Ferr, WarnRes, ErrRes>(
        self,
        f_warnings: Fwarn,
        f_errors: Ferr,
    ) -> (WarnRes, Result<Self::V, ErrRes>)
    where
        Self: GenericResultExt<EC = NullFamily, LWC = <Self as GenericResultExt>::RWC>,
        Fwarn: FnOnce(<Self::LWC as ZeroOrMore>::Wrapper<Self::W>) -> WarnRes,
        Ferr: FnOnce(Self::E) -> ErrRes,
    {
        match self.into_result() {
            Ok(s) => {
                let (v, warn_res) = s.resolve(f_warnings);
                (warn_res, Ok(v))
            }
            Err(e) => {
                let (warn_res, err_res) = e.resolve(f_warnings, f_errors);
                (warn_res, Err(err_res))
            }
        }
    }
}

impl<V, P, W, E, LWC, RWC, EC> GenericResultExt for GenericResult<V, P, W, E, LWC, RWC, EC>
where
    LWC: ZeroOrMore,
    RWC: ZeroOrMore,
    EC: ZeroOrMore,
{
    type V = V;
    type P = P;
    type E = E;
    type W = W;
    type LWC = LWC;
    type RWC = RWC;
    type EC = EC;
}

pub(crate) trait ErrorIter<T, P, W, E, WC, EC>:
    Iterator<Item = GenericResult<T, P, W, E, WC, WC, EC>> + Sized
where
    WC: ZeroOrMore,
    EC: ZeroOrMore,
{
    #[allow(clippy::type_complexity)]
    fn gather<WCF>(mut self) -> GenericResult<Vec<T>, (Vec<T>, Vec<P>), W, E, WCF, WCF, EC>
    where
        WC: IntoZeroOrMore<WCF>,
        WCF: ZeroOrMore,
        WCF::Wrapper<W>: Extend<W>,
        EC::Wrapper<E>: Extend<E>,
    {
        let mut left_vs = vec![];
        let mut ws = WCF::Wrapper::<W>::default();
        let mut error_head = None;
        for x in self.by_ref() {
            match x {
                Ok(y) => {
                    left_vs.push(y.value);
                    ws.extend(y.warnings);
                }
                Err(y) => {
                    error_head = Some(y);
                    break;
                }
            }
        }
        if let Some(h) = error_head {
            let mut right_vs = vec![h.passthru];
            let mut es = h.errors;
            for x in self {
                match x {
                    Ok(y) => {
                        left_vs.push(y.value);
                        ws.extend(y.warnings);
                    }
                    Err(y) => {
                        right_vs.push(y.passthru);
                        ws.extend(y.warnings);
                        es.extend(y.errors);
                    }
                }
            }
            Err(Failure::new(ws, es, (left_vs, right_vs)))
        } else {
            Ok(Success::new(left_vs, ws))
        }
    }
}

impl<I, V, P, W, E, WC, EC> ErrorIter<V, P, W, E, WC, EC> for I
where
    I: Iterator<Item = GenericResult<V, P, W, E, WC, WC, EC>>,
    WC: ZeroOrMore,
    EC: ZeroOrMore,
{
}
