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

pub type WarningOrErrorResult<V, P, W, E> = TransResult<V, P, W, E, OptFamily, NullFamily>;
pub type WarningsOrErrorResult<V, P, W, E> = TransResult<V, P, W, E, VecFamily, NullFamily>;
pub type WarningOrErrorsResult<V, P, W, E> = TransResult<V, P, W, E, OptFamily, VecFamily>;
pub type WarningsOrErrorsResult<V, P, W, E> = TransResult<V, P, W, E, VecFamily, VecFamily>;

pub type WarningAndErrorResult<V, P, W, E> = CisResult<V, P, W, E, OptFamily, NullFamily>;
pub type WarningsAndErrorResult<V, P, W, E> = CisResult<V, P, W, E, VecFamily, NullFamily>;
pub type WarningAndErrorsResult<V, P, W, E> = CisResult<V, P, W, E, OptFamily, VecFamily>;
pub type WarningsAndErrorsResult<V, P, W, E> = CisResult<V, P, W, E, VecFamily, VecFamily>;

pub type SimpleResult<V, P, E, EC> = CommutativeResult<V, P, (), E, NullFamily, EC>;

pub type WarnableResult<V, P, E, WC, EC> = NonCommutativeResult<V, P, E, E, WC, EC>;

pub type TransResult<V, P, W, E, WC, EC> = NonCommutativeResult<V, P, W, E, WC, EC>;

pub type CisResult<V, P, W, E, WC, EC> = CommutativeResult<V, P, W, E, WC, EC>;

pub type CommutativeResult<V, P, W, E, WC, EC> = GenericResult<V, P, W, W, E, WC, WC, EC>;

pub type NonCommutativeResult<V, P, W, E, WC, EC> =
    GenericResult<V, P, W, (), E, WC, NullFamily, EC>;

pub type GenericResult<V, P, LW, RW, E, LWC, RWC, EC> =
    Result<Success<V, LW, LWC>, Failure<P, RW, E, RWC, EC>>;

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

    fn repack_warnings<WIF>(self) -> Success<V, W, WIF>
    where
        WC: IntoZeroOrMore<WIF>,
        WIF: ZeroOrMore,
    {
        Success::new(self.value, WC::into_zero_or_more(self.warnings))
    }

    fn value_into<U: From<V>>(self) -> Success<U, W, WC> {
        self.map(Into::into)
    }

    fn map<F: FnOnce(V) -> X, X>(self, f: F) -> Success<X, W, WC> {
        Success::new(f(self.value), self.warnings)
    }

    fn warnings_into<X: From<W>>(self) -> Success<V, X, WC> {
        self.map_warnings(Into::into)
    }

    fn map_warnings<F: Fn(W) -> ToW, ToW>(self, f: F) -> Success<V, ToW, WC> {
        Success::new(self.value, WC::map(self.warnings, f))
    }

    fn push_warning(&mut self, w: W)
    where
        WC::Wrapper<W>: Extend<W>,
    {
        self.warnings.extend(iter::once(w));
    }

    fn eval_warning<F>(&mut self, f: F)
    where
        F: FnOnce(&V) -> Option<W>,
        WC::Wrapper<W>: Extend<W>,
    {
        if let Some(e) = f(&self.value) {
            self.push_warning(e);
        }
    }

    fn and_maybe<F, ToV, P, E, WCf, EC>(self, f: F) -> CommutativeResult<ToV, P, W, E, WCf, EC>
    where
        F: FnOnce(V) -> CommutativeResult<ToV, P, W, E, WC, EC>,
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

    fn fail<E, EC>(self, errors: GenNonEmpty<E, EC>) -> Failure<V, W, E, WC, EC>
    where
        EC: ZeroOrMore,
    {
        Failure::new(self.warnings, errors, self.value)
    }

    fn with_failure<F, P, PF, E, WCf, EC>(
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

    fn zip_with<F, V0, VF, WCf>(self, other: Success<V0, W, WC>, f: F) -> Success<VF, W, WCf>
    where
        F: FnOnce(V, V0) -> VF,
        WCf: ZeroOrMore,
        WC::Wrapper<W>: Concatable<Out = WCf::Wrapper<W>>,
    {
        let ws = self.warnings.concat(other.warnings);
        Success::new(f(self.value, other.value), ws)
    }

    fn remove_warnings(self) -> Success<V, (), NullFamily> {
        Success::new1(self.value)
    }

    fn warnings_to_errors<E, F, EC>(self, f: F) -> NonCommutativeResult<V, V, (), E, NullFamily, EC>
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
    fn new_from_one(error: E, passthru: P) -> Self {
        Self::new_from_many(GenNonEmpty::new1(error), passthru)
    }

    fn new_from_many(errors: GenNonEmpty<E, EC>, passthru: P) -> Self {
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

    fn map_warnings<F, ToW>(self, f: F) -> Failure<P, ToW, E, WC, EC>
    where
        F: Fn(W) -> ToW,
    {
        Failure {
            warnings: WC::map(self.warnings, f),
            errors: self.errors,
            passthru: self.passthru,
        }
    }

    fn map_errors<F, ToE>(self, f: F) -> Failure<P, W, ToE, WC, EC>
    where
        F: Fn(E) -> ToE,
    {
        Failure {
            warnings: self.warnings,
            errors: self.errors.map(f),
            passthru: self.passthru,
        }
    }

    fn map_passthru<F, ToP>(self, f: F) -> Failure<ToP, W, E, WC, EC>
    where
        F: FnOnce(P) -> ToP,
    {
        Failure::new(self.warnings, self.errors, f(self.passthru))
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

    fn push_error(&mut self, e: E)
    where
        EC::Wrapper<E>: Extend<E>,
    {
        self.errors.extend(iter::once(e));
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

    fn with_passthru<F, V, Pf, WCf>(mut self, f: F) -> CommutativeResult<V, Pf, W, E, WCf, EC>
    where
        F: FnOnce(P) -> CommutativeResult<V, Pf, W, E, WC, EC>,
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

    fn gather_errors<F, EF>(self, f: F) -> Failure<P, W, EF, WC, NullFamily>
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
        self.gather_errors(|es| ErrorSummary::new(summary, es.into_zero_or_more()))
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

impl<W, E, P, WC: ZeroOrMore> Failure<P, W, E, WC, NullFamily> {
    fn resolve<F, G, X, Y>(self, f: F, g: G) -> (X, Y)
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

    fn value_into<ToV>(
        self,
    ) -> GenericResult<ToV, Self::P, Self::LW, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        ToV: From<Self::V>,
    {
        self.map_value(Into::into)
    }

    fn warnings_into<Wf>(
        self,
    ) -> GenericResult<Self::V, Self::P, Wf, Wf, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        Wf: From<Self::LW>,
        Self: GenericResultExt<LW = <Self as GenericResultExt>::RW>,
    {
        self.map_warnings(Into::into)
    }

    fn left_warnings_into<Wf>(
        self,
    ) -> GenericResult<Self::V, Self::P, Wf, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        Wf: From<Self::LW>,
    {
        self.map_left_warnings(Into::into)
    }

    fn right_warnings_into<Wf>(
        self,
    ) -> GenericResult<Self::V, Self::P, Wf, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        Wf: From<Self::LW>,
    {
        self.map_left_warnings(Into::into)
    }

    fn errors_into<ToE>(
        self,
    ) -> GenericResult<Self::V, Self::P, Self::LW, Self::RW, ToE, Self::LWC, Self::RWC, Self::EC>
    where
        ToE: From<Self::E>,
    {
        self.map_errors(Into::into)
    }

    fn map_value<F, ToV>(
        self,
        f: F,
    ) -> GenericResult<ToV, Self::P, Self::LW, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: FnOnce(Self::V) -> ToV,
    {
        self.into_result().map(|s| s.map(f))
    }

    fn map_passthru<F, ToP>(
        self,
        f: F,
    ) -> GenericResult<Self::V, ToP, Self::LW, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: FnOnce(Self::P) -> ToP,
    {
        self.into_result().map_err(|e| e.map_passthru(f))
    }

    fn map_warnings<F, Wf>(
        self,
        f: F,
    ) -> GenericResult<Self::V, Self::P, Wf, Wf, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: Fn(Self::LW) -> Wf,
        Self: GenericResultExt<LW = <Self as GenericResultExt>::RW>,
    {
        self.into_result()
            .map(|s| s.map_warnings(&f))
            .map_err(|e| e.map_warnings(f))
    }

    fn map_left_warnings<F, Wf>(
        self,
        f: F,
    ) -> GenericResult<Self::V, Self::P, Wf, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: Fn(Self::LW) -> Wf,
    {
        self.into_result().map(|s| s.map_warnings(f))
    }

    fn map_right_warnings<F, Wf>(
        self,
        f: F,
    ) -> GenericResult<Self::V, Self::P, Self::LW, Wf, Self::E, Self::LWC, Self::RWC, Self::EC>
    where
        F: Fn(Self::RW) -> Wf,
    {
        self.into_result().map_err(|s| s.map_warnings(f))
    }

    fn map_errors<F, ToE>(
        self,
        f: F,
    ) -> GenericResult<Self::V, Self::P, Self::LW, Self::RW, ToE, Self::LWC, Self::RWC, Self::EC>
    where
        F: Fn(Self::E) -> ToE,
    {
        self.into_result().map_err(|e| e.map_errors(f))
    }

    fn liftio_errors(
        self,
    ) -> GenericResult<
        Self::V,
        Self::P,
        Self::LW,
        Self::RW,
        ImpureError<Self::E>,
        Self::LWC,
        Self::RWC,
        Self::EC,
    > {
        self.map_errors(ImpureError::Pure)
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
        self.into_result().map(Success::repack_warnings)
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

    fn void_value(
        self,
    ) -> GenericResult<(), Self::P, Self::LW, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    {
        self.map_value(|_| ())
    }

    fn void_passthru(
        self,
    ) -> GenericResult<Self::V, (), Self::LW, Self::RW, Self::E, Self::LWC, Self::RWC, Self::EC>
    {
        self.map_passthru(|_| ())
    }

    fn remove_warnings(self) -> SimpleResult<Self::V, Self::P, Self::E, Self::EC> {
        self.into_result()
            .map(|s| s.remove_warnings())
            .map_err(|e| e.remove_warnings())
    }

    fn warnings_to_errors<F>(self, f: F) -> SimpleResult<Self::V, Self::P, Self::E, Self::EC>
    where
        F: Fn(Self::LW) -> Self::E,
        Self::LWC: IntoZeroOrMore<Self::EC>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: GenericResultExt<
            P = <Self as GenericResultExt>::V,
            LW = <Self as GenericResultExt>::RW,
        >,
    {
        match self.into_result() {
            Ok(s) => s.warnings_to_errors(f),
            Err(e) => Err(e.warnings_to_errors(f)),
        }
    }

    fn gather_errors<F, Ef>(
        self,
        f: F,
    ) -> GenericResult<Self::V, Self::P, Self::LW, Self::RW, Ef, Self::LWC, Self::RWC, NullFamily>
    where
        F: FnOnce(GenNonEmpty<Self::E, Self::EC>) -> Ef,
    {
        self.into_result().map_err(|e| e.gather_errors(f))
    }

    fn summarize_errors<S>(
        self,
        summary: S,
    ) -> GenericResult<
        Self::V,
        Self::P,
        Self::LW,
        Self::RW,
        ErrorSummary<Self::E, S>,
        Self::LWC,
        Self::RWC,
        NullFamily,
    >
    where
        Self::EC: IntoZeroOrMore<VecFamily>,
    {
        self.into_result().map_err(|e| e.summarize_errors(summary))
    }

    fn from_infallible<PF, EF>(
        self,
    ) -> GenericResult<Self::V, PF, Self::LW, Self::RW, EF, Self::LWC, Self::RWC, Self::EC>
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
        Fwarn: FnOnce(<Self::LWC as ZeroOrMore>::Wrapper<Self::LW>) -> WarnRes,
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
        Self: GenericResultExt<
            EC = NullFamily,
            LW = <Self as GenericResultExt>::RW,
            LWC = <Self as GenericResultExt>::RWC,
        >,
        Fwarn: FnOnce(<Self::LWC as ZeroOrMore>::Wrapper<Self::LW>) -> WarnRes,
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

pub trait NonCommutativeResultExt
where
    Self: Sized + GenericResultExt<RW = (), RWC = NullFamily>,
{
    fn eval_non_commutative_warning<F>(&mut self, f: F)
    where
        F: FnOnce(&Self::V) -> Option<Self::LW>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Extend<Self::LW>,
    {
        if let Ok(s) = self.as_result_mut() {
            s.eval_warning(f)
        }
    }

    fn push_non_commutative_warning(&mut self, w: Self::LW)
    where
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Extend<Self::LW>,
    {
        if let Ok(s) = self.as_result_mut() {
            s.push_warning(w)
        }
    }

    fn into_commutative(
        self,
    ) -> CommutativeResult<Self::V, Self::P, Self::LW, Self::E, Self::LWC, Self::EC> {
        self.into_result().map_err(|e| e.lift_simple())
    }
}

pub trait CommutativeResultExt
where
    Self: Sized
        + GenericResultExt<LW = <Self as GenericResultExt>::RW, LWC = <Self as GenericResultExt>::RWC>,
{
    fn eval_warning<F>(&mut self, f: F)
    where
        F: FnOnce(&Self::V) -> Option<Self::LW>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Extend<Self::LW>,
        Self: GenericResultExt<V = <Self as GenericResultExt>::P>,
    {
        match self.as_result_mut() {
            Ok(s) => s.eval_warning(f),
            Err(e) => e.eval_warning(f),
        }
    }

    fn eval_error<F>(
        self,
        f: F,
    ) -> CommutativeResult<Self::V, Self::P, Self::LW, Self::E, Self::LWC, Self::EC>
    where
        F: FnOnce(&Self::V) -> Option<Self::E>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: GenericResultExt<V = <Self as GenericResultExt>::P>,
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

    fn push_warning(&mut self, w: Self::LW)
    where
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Extend<Self::LW>,
    {
        match self.as_result_mut() {
            Ok(s) => s.push_warning(w),
            Err(e) => e.push_warning(w),
        }
    }

    fn push_error(
        self,
        e: Self::E,
    ) -> CommutativeResult<Self::V, Self::P, Self::LW, Self::E, Self::LWC, Self::EC>
    where
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: GenericResultExt<V = <Self as GenericResultExt>::P>,
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
    ) -> CommutativeResult<Self::V, Self::P, Self::LW, Self::E, Self::LWC, Self::EC>
    where
        X: Into<Self::LW> + Into<Self::E>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Extend<Self::LW>,
        Self: GenericResultExt<V = <Self as GenericResultExt>::P>,
    {
        if is_error {
            self.push_error(x.into())
        } else {
            self.push_warning(x.into());
            self.into_result()
        }
    }

    fn and_maybe<F, ToV, WCf>(
        self,
        f: F,
    ) -> CommutativeResult<ToV, Self::P, Self::LW, Self::E, WCf, Self::EC>
    where
        F: FnOnce(
            Self::V,
        )
            -> CommutativeResult<ToV, Self::P, Self::LW, Self::E, Self::LWC, Self::EC>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Concatable<Out = WCf::Wrapper<Self::LW>>,
        WCf: ZeroOrMore,
        Self::RWC: IntoZeroOrMore<WCf>,
    {
        match self.into_result() {
            Ok(s) => s.and_maybe(f),
            Err(e) => Err(e.repack_warnings()),
        }
    }

    fn and_tentatively<F, ToV, ToP, WCf>(
        self,
        f: F,
    ) -> CommutativeResult<ToV, ToP, Self::LW, Self::E, WCf, Self::EC>
    where
        F: FnOnce(Self::V) -> CommutativeResult<ToV, ToP, Self::LW, Self::E, Self::LWC, Self::EC>,
        Self: GenericResultExt<V = <Self as GenericResultExt>::P>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Concatable<Out = WCf::Wrapper<Self::LW>>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        WCf: ZeroOrMore,
    {
        match self.into_result() {
            Ok(s) => s.and_maybe(f),
            Err(e) => e.with_passthru(f),
        }
    }

    fn zip<V1, WCf, ECf>(
        self,
        a: CommutativeResult<V1, (), Self::LW, Self::E, Self::LWC, Self::EC>,
    ) -> CommutativeResult<(Self::V, V1), (), Self::LW, Self::E, WCf, ECf>
    where
        WCf: ZeroOrMore,
        ECf: ZeroOrMore,
        ECf::Wrapper<Self::E>: Extend<Self::E>,
        Self::EC: IntoZeroOrMore<ECf>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Concatable<Out = WCf::Wrapper<Self::LW>>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: GenericResultExt<P = ()>,
    {
        match (self.into_result(), a) {
            (Ok(ax), Ok(bx)) => Ok(ax.zip_with(bx, |x, y| (x, y))),
            (Ok(ax), Err(bx)) => Err(ax.with_failure(bx, |_, ()| ()).repack_errors()),
            (Err(ax), Ok(bx)) => Err(ax.with_success(bx, |(), _| ()).repack_errors()),
            (Err(ax), Err(bx)) => Err(ax.zip_with(bx, |(), ()| ())),
        }
    }

    fn zip3<V1, V2, WCf, ECf>(
        self,
        a: CommutativeResult<V1, (), Self::LW, Self::E, Self::LWC, Self::EC>,
        b: CommutativeResult<V2, (), Self::LW, Self::E, Self::LWC, Self::EC>,
    ) -> CommutativeResult<(Self::V, V1, V2), (), Self::LW, Self::E, WCf, ECf>
    where
        WCf: ZeroOrMore,
        ECf: ZeroOrMore,
        ECf::Wrapper<Self::E>: Extend<Self::E>,
        Self::EC: IntoZeroOrMore<ECf>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Concatable<Out = WCf::Wrapper<Self::LW>>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: GenericResultExt<P = ()>,
        Self::LWC: IntoZeroOrMore<WCf>,
        Self::EC: IntoZeroOrMore<ECf>,
        WCf::Wrapper<Self::LW>: Concatable<Out = WCf::Wrapper<Self::LW>>,
    {
        let res = self.zip(a);
        let b_ = b.repack();
        CommutativeResultExt::zip(res, b_).map_value(|((ax, bx), cx)| (ax, bx, cx))
    }

    fn zip4<V1, V2, V3, WCf, ECf>(
        self,
        a: CommutativeResult<V1, (), Self::LW, Self::E, Self::LWC, Self::EC>,
        b: CommutativeResult<V2, (), Self::LW, Self::E, Self::LWC, Self::EC>,
        c: CommutativeResult<V3, (), Self::LW, Self::E, Self::LWC, Self::EC>,
    ) -> CommutativeResult<(Self::V, V1, V2, V3), (), Self::LW, Self::E, WCf, ECf>
    where
        WCf: ZeroOrMore,
        ECf: ZeroOrMore,
        ECf::Wrapper<Self::E>: Extend<Self::E>,
        Self::EC: IntoZeroOrMore<ECf>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Concatable<Out = WCf::Wrapper<Self::LW>>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: GenericResultExt<P = ()>,
        Self::LWC: IntoZeroOrMore<WCf>,
        Self::EC: IntoZeroOrMore<ECf>,
        WCf::Wrapper<Self::LW>: Concatable<Out = WCf::Wrapper<Self::LW>>,
    {
        let res = self.zip3(a, b);
        let c_ = c.repack();
        CommutativeResultExt::zip(res, c_).map_value(|((ax, bx, cx), dx)| (ax, bx, cx, dx))
    }

    fn zip5<V1, V2, V3, V4, WCf, ECf>(
        self,
        a: CommutativeResult<V1, (), Self::LW, Self::E, Self::LWC, Self::EC>,
        b: CommutativeResult<V2, (), Self::LW, Self::E, Self::LWC, Self::EC>,
        c: CommutativeResult<V3, (), Self::LW, Self::E, Self::LWC, Self::EC>,
        d: CommutativeResult<V4, (), Self::LW, Self::E, Self::LWC, Self::EC>,
    ) -> CommutativeResult<(Self::V, V1, V2, V3, V4), (), Self::LW, Self::E, WCf, ECf>
    where
        WCf: ZeroOrMore,
        ECf: ZeroOrMore,
        ECf::Wrapper<Self::E>: Extend<Self::E>,
        Self::EC: IntoZeroOrMore<ECf>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Concatable<Out = WCf::Wrapper<Self::LW>>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: GenericResultExt<P = ()>,
        Self::LWC: IntoZeroOrMore<WCf>,
        Self::EC: IntoZeroOrMore<ECf>,
        WCf::Wrapper<Self::LW>: Concatable<Out = WCf::Wrapper<Self::LW>>,
    {
        let res = self.zip4(a, b, c);
        let d_ = d.repack();
        CommutativeResultExt::zip(res, d_).map_value(|((ax, bx, cx, dx), ex)| (ax, bx, cx, dx, ex))
    }

    fn zip6<V1, V2, V3, V4, V5, WCf, ECf>(
        self,
        a: CommutativeResult<V1, (), Self::LW, Self::E, Self::LWC, Self::EC>,
        b: CommutativeResult<V2, (), Self::LW, Self::E, Self::LWC, Self::EC>,
        c: CommutativeResult<V3, (), Self::LW, Self::E, Self::LWC, Self::EC>,
        d: CommutativeResult<V4, (), Self::LW, Self::E, Self::LWC, Self::EC>,
        e: CommutativeResult<V5, (), Self::LW, Self::E, Self::LWC, Self::EC>,
    ) -> CommutativeResult<(Self::V, V1, V2, V3, V4, V5), (), Self::LW, Self::E, WCf, ECf>
    where
        WCf: ZeroOrMore,
        ECf: ZeroOrMore,
        ECf::Wrapper<Self::E>: Extend<Self::E>,
        Self::EC: IntoZeroOrMore<ECf>,
        <Self::LWC as ZeroOrMore>::Wrapper<Self::LW>: Concatable<Out = WCf::Wrapper<Self::LW>>,
        <Self::EC as ZeroOrMore>::Wrapper<Self::E>: Extend<Self::E>,
        Self: GenericResultExt<P = ()>,
        Self::LWC: IntoZeroOrMore<WCf>,
        Self::EC: IntoZeroOrMore<ECf>,
        WCf::Wrapper<Self::LW>: Concatable<Out = WCf::Wrapper<Self::LW>>,
    {
        let res = self.zip5(a, b, c, d);
        let e_ = e.repack();
        CommutativeResultExt::zip(res, e_)
            .map_value(|((ax, bx, cx, dx, ex), fx)| (ax, bx, cx, dx, ex, fx))
    }
}

pub trait SimpleResultExt
where
    Self: Sized + NonCommutativeResultExt<LW = (), RW = (), LWC = NullFamily, RWC = NullFamily>,
{
    fn simple_into_non_commutative<Wf, LWCf>(
        self,
    ) -> NonCommutativeResult<Self::V, Self::P, Wf, Self::E, LWCf, Self::EC>
    where
        LWCf: ZeroOrMore,
    {
        self.into_result().map(|s| s.lift_simple())
    }

    fn simple_into_commutative<Wf, LWCf>(
        self,
    ) -> CommutativeResult<Self::V, Self::P, Wf, Self::E, LWCf, Self::EC>
    where
        LWCf: ZeroOrMore,
    {
        self.into_result()
            .map(|s| s.lift_simple())
            .into_commutative()
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

impl<V, P, W, E, WC, EC> NonCommutativeResultExt for NonCommutativeResult<V, P, W, E, WC, EC>
where
    WC: ZeroOrMore,
    EC: ZeroOrMore,
{
}

impl<V, P, W, E, WC, EC> CommutativeResultExt for CommutativeResult<V, P, W, E, WC, EC>
where
    WC: ZeroOrMore,
    EC: ZeroOrMore,
{
}

impl<V, P, E, EC: ZeroOrMore> SimpleResultExt for SimpleResult<V, P, E, EC> {}

pub(crate) trait ErrorIter<T, P, W, E, WC, EC>:
    Iterator<Item = CommutativeResult<T, P, W, E, WC, EC>> + Sized
where
    WC: ZeroOrMore,
    EC: ZeroOrMore,
{
    #[allow(clippy::type_complexity)]
    fn gather<WCf>(mut self) -> CommutativeResult<Vec<T>, (Vec<T>, Vec<P>), W, E, WCf, EC>
    where
        WC: IntoZeroOrMore<WCf>,
        WCf: ZeroOrMore,
        WCf::Wrapper<W>: Extend<W>,
        EC::Wrapper<E>: Extend<E>,
    {
        let mut left_vs = vec![];
        let mut ws = WCf::Wrapper::<W>::default();
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
    I: Iterator<Item = CommutativeResult<V, P, W, E, WC, EC>>,
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
