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

use derive_new::new;
use itertools::Itertools as _;
use nonempty::NonEmpty;
use std::convert::Infallible;
use std::io;
use thiserror::Error;

// TODO add a cap to the error buffer so the user doesn't DOS themselves if
// their file is particularly screwed up

/// Final result which may be passing or not passing
pub type TerminalResult<V, W, E, T> = Result<Terminal<V, W>, TerminalFailure<W, E, T>>;

/// Final result which may be passing or not passing in IO context
pub type IOTerminalResult<V, W, E, T> = TerminalResult<V, W, ImpureError<E>, T>;

/// Final passing result, possibly with warnings
#[derive(new)]
#[new(visibility = "")]
pub struct Terminal<V, W> {
    #[new(into)]
    value: V,
    #[new(into_iter = "W")]
    warnings: Vec<W>,
}

/// Final failed result with either one error or multiple errors with a summary.
#[derive(new)]
#[new(visibility = "")]
pub struct TerminalFailure<W, E, T> {
    #[new(into_iter = "W")]
    warnings: Vec<W>,
    #[new(into)]
    errors: Box<NonEmpty<E>>,
    #[new(into)]
    reason: T,
}

/// Result which may have at least one error
pub type DeferredResult<V, W, E> = Result<Tentative<V, W, E>, DeferredFailure<(), W, E>>;

pub type BiDeferredResult<V, E> = DeferredResult<V, E, E>;

/// Result which may have at least one error (with passthru)
pub type PassthruResult<V, P, W, E> = Result<Tentative<V, W, E>, DeferredFailure<P, W, E>>;

/// Result which may have at least one error in IO context
pub type IODeferredResult<V, W, E> = DeferredResult<V, W, ImpureError<E>>;

/// Result which might have warnings or errors
#[derive(new)]
pub struct Tentative<V, W, E> {
    #[new(into)]
    value: V,
    #[new(into_iter = "W")]
    warnings: Vec<W>,
    #[new(into_iter = "E")]
    errors: Vec<E>,
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

/// Result which has 1+ errors, 0+ warnings, and the input type.
///
/// Passthru is meant to hold the input type for the failed computation such
/// that it can be reused if needed rather than consumed in a black hole by
/// the ownership model. Obvious use case: failed From<X>-like methods where
/// we may want to do something with the original value after trying and failing
/// to convert it.
#[derive(new)]
pub struct DeferredFailure<P, W, E> {
    #[new(into_iter = "W")]
    warnings: Vec<W>,
    #[new(into)]
    errors: Box<NonEmpty<E>>,
    #[new(into)]
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

impl<V, W> Terminal<V, W> {
    pub fn new1(value: impl Into<V>) -> Self {
        Self::new(value, [])
    }

    pub fn value_into<U: From<V>>(self) -> Terminal<U, W> {
        self.map(Into::into)
    }

    pub fn map<F: FnOnce(V) -> X, X>(self, f: F) -> Terminal<X, W> {
        Terminal::new(f(self.value), self.warnings)
    }

    pub fn warnings_map<F: Fn(W) -> X, X>(self, f: F) -> Terminal<V, X> {
        Terminal::new(self.value, self.warnings.into_iter().map(f))
    }

    pub fn warnings_into<X: From<W>>(self) -> Terminal<V, X> {
        self.warnings_map(Into::into)
    }

    pub fn and_finally<E, T, F, X>(mut self, f: F) -> TerminalResult<X, W, E, T>
    where
        F: FnOnce(V) -> TerminalResult<X, W, E, T>,
    {
        match f(self.value) {
            Ok(s) => {
                self.warnings.extend(s.warnings);
                Ok(Terminal::new(s.value, self.warnings))
            }
            Err(e) => {
                self.warnings.extend(e.warnings);
                Err(TerminalFailure::new(self.warnings, *e.errors, e.reason))
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
                Tentative::new(s.value, self.warnings, s.errors).terminate(reason)
            }
            Err(e) => {
                self.warnings.extend(e.warnings);
                // termination will throw away the passthru value so this
                // only needs to be a dummy
                Err(DeferredFailure::new(self.warnings, *e.errors, ()).terminate(reason))
            }
        }
    }

    pub fn and_tentatively<F, X, E, T>(mut self, reason: T, f: F) -> TerminalResult<X, W, E, T>
    where
        F: FnOnce(V) -> Tentative<X, W, E>,
    {
        let s = f(self.value);
        self.warnings.extend(s.warnings);
        Tentative::new(s.value, self.warnings, s.errors).terminate(reason)
    }

    pub fn resolve<F, X>(self, f: F) -> (V, X)
    where
        F: FnOnce(Vec<W>) -> X,
    {
        (self.value, f(self.warnings))
    }

    fn warnings_to_errors<T, E, F>(self, reason: T, f: F) -> TerminalResult<V, W, E, T>
    where
        F: Fn(W) -> E,
    {
        match NonEmpty::from_vec(self.warnings) {
            None => Ok(Self::new1(self.value)),
            Some(ws) => Err(TerminalFailure::new1(ws.map(f), reason)),
        }
    }
}

impl<V> Terminal<V, Infallible> {
    pub fn inner(self) -> V {
        self.value
    }
}

impl<W, E, T> TerminalFailure<W, E, T> {
    pub fn new1(errors: NonEmpty<E>, reason: T) -> Self {
        Self::new([], errors, reason)
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

    fn warnings_to_errors<F>(mut self, f: F) -> Self
    where
        F: Fn(W) -> E,
    {
        self.errors.extend(self.warnings.into_iter().map(f));
        Self::new([], self.errors, self.reason)
    }

    pub fn resolve<F, G, X, Y>(self, f: F, g: G) -> (X, Y)
    where
        F: FnOnce(Vec<W>) -> X,
        G: FnOnce(NonEmpty<E>, T) -> Y,
    {
        (f(self.warnings), g(*self.errors, self.reason))
    }
}

impl<V, W, E> Tentative<V, W, E> {
    pub fn new_either<M>(value: V, msgs: impl IntoIterator<Item = M>, are_errors: bool) -> Self
    where
        E: From<M>,
        W: From<M>,
    {
        if are_errors {
            Self::new(value, [], msgs.into_iter().map(Into::into))
        } else {
            Self::new(value, msgs.into_iter().map(Into::into), [])
        }
    }

    pub fn new1(value: V) -> Self {
        Self::new(value, [], [])
    }

    pub fn push_warning(&mut self, x: impl Into<W>) {
        self.warnings.push(x.into());
    }

    pub fn push_error(&mut self, x: impl Into<E>) {
        self.errors.push(x.into());
    }

    pub fn push_error_or_warning<X>(&mut self, x: X, is_error: bool)
    where
        X: Into<E> + Into<W>,
    {
        if is_error {
            self.push_error(x);
        } else {
            self.push_warning(x);
        }
    }

    pub fn extend_errors_or_warnings<X>(&mut self, xs: impl Iterator<Item = X>, is_error: bool)
    where
        X: Into<W> + Into<E>,
    {
        if is_error {
            self.extend_errors(xs);
        } else {
            self.extend_warnings(xs);
        }
    }

    pub fn extend_warnings(&mut self, xs: impl Iterator<Item = impl Into<W>>) {
        self.warnings.extend(xs.map(Into::into));
    }

    pub fn extend_errors(&mut self, xs: impl Iterator<Item = impl Into<E>>) {
        self.errors.extend(xs.map(Into::into));
    }

    pub fn and_finally<F, X, T>(mut self, mut f: F) -> TerminalResult<X, W, E, T>
    where
        F: FnMut(V) -> TerminalResult<X, W, E, T>,
    {
        match f(self.value) {
            Ok(s) => {
                self.warnings.extend(s.warnings);
                Ok(Terminal::new(s.value, self.warnings))
            }
            Err(e) => {
                self.warnings.extend(e.warnings);
                Err(TerminalFailure::new(self.warnings, *e.errors, e.reason))
            }
        }
    }

    pub fn and_maybe<F, X, P>(mut self, f: F) -> PassthruResult<P, X, W, E>
    where
        F: FnOnce(V) -> PassthruResult<P, X, W, E>,
    {
        match f(self.value) {
            Ok(s) => {
                self.warnings.extend(s.warnings);
                self.errors.extend(s.errors);
                Ok(Tentative::new(s.value, self.warnings, self.errors))
            }
            Err(e) => {
                self.warnings.extend(e.warnings);
                self.errors.extend(*e.errors);
                Err(DeferredFailure::new(
                    self.warnings,
                    NonEmpty::from_vec(self.errors).unwrap(),
                    e.passthru,
                ))
            }
        }
    }

    pub fn and_tentatively<F, X>(mut self, f: F) -> Tentative<X, W, E>
    where
        F: FnOnce(V) -> Tentative<X, W, E>,
    {
        let s = f(self.value);
        self.warnings.extend(s.warnings);
        self.errors.extend(s.errors);
        Tentative::new(s.value, self.warnings, self.errors)
    }

    pub fn eval_error<F, X>(&mut self, f: F)
    where
        X: Into<E>,
        F: FnOnce(&V) -> Option<X>,
    {
        if let Some(e) = f(&self.value) {
            self.errors.push(e.into());
        }
    }

    pub fn eval_warning<F, X>(&mut self, f: F)
    where
        X: Into<W>,
        F: FnOnce(&V) -> Option<X>,
    {
        if let Some(e) = f(&self.value) {
            self.warnings.push(e.into());
        }
    }

    pub fn eval_error_or_warning<F, X>(&mut self, is_error: bool, f: F)
    where
        X: Into<W> + Into<E>,
        F: FnOnce(&V) -> Option<X>,
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
    {
        self.errors
            .extend(f(&self.value).into_iter().map(Into::into));
    }

    pub fn map<F: FnOnce(V) -> X, X>(self, f: F) -> Tentative<X, W, E> {
        Tentative::new(f(self.value), self.warnings, self.errors)
    }

    pub fn map_warnings<F: Fn(W) -> X, X>(self, f: F) -> Tentative<V, X, E> {
        Tentative::new(self.value, self.warnings.into_iter().map(f), self.errors)
    }

    pub fn map_errors<F: Fn(E) -> X, X>(self, f: F) -> Tentative<V, W, X> {
        Tentative::new(self.value, self.warnings, self.errors.into_iter().map(f))
    }

    pub fn value_into<X: From<V>>(self) -> Tentative<X, W, E> {
        self.map(Into::into)
    }

    pub fn warnings_into<X: From<W>>(self) -> Tentative<V, X, E> {
        self.map_warnings(Into::into)
    }

    pub fn errors_into<X: From<E>>(self) -> Tentative<V, W, X> {
        self.map_errors(Into::into)
    }

    pub fn inner_into<X: From<W>, Y: From<E>>(self) -> Tentative<V, X, Y> {
        self.errors_into().warnings_into()
    }

    pub fn errors_liftio(self) -> Tentative<V, W, ImpureError<E>> {
        self.map_errors(ImpureError::Pure)
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
        Tentative::new(vs, ws, es)
    }

    pub fn terminate<T>(self, reason: T) -> TerminalResult<V, W, E, T> {
        self.terminate_inner(reason).map(|(t, _)| t)
    }

    #[allow(clippy::type_complexity)]
    fn terminate_inner<T>(
        self,
        reason: T,
    ) -> Result<(Terminal<V, W>, T), TerminalFailure<W, E, T>> {
        match NonEmpty::from_vec(self.errors) {
            Some(errors) => Err(TerminalFailure::new(self.warnings, errors, reason)),
            None => Ok((Terminal::new(self.value, self.warnings), reason)),
        }
    }

    pub fn terminate_warn2err<F, T>(self, reason: T, f: F) -> TerminalResult<V, W, E, T>
    where
        F: Fn(W) -> E,
    {
        match self.terminate_inner(reason) {
            Ok((t, r)) => t.warnings_to_errors(r, f),
            Err(e) => Err(e.warnings_to_errors(f)),
        }
    }

    pub fn terminate_nowarn<T>(self, reason: T) -> TerminalResult<V, W, E, T> {
        match NonEmpty::from_vec(self.errors) {
            None => Ok(Terminal::new1(self.value)),
            Some(e) => Err(TerminalFailure::new([], e, reason)),
        }
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
        Tentative::new(f(self.value, other.value), self.warnings, self.errors)
    }

    pub fn void(self) -> Tentative<(), W, E> {
        Tentative::new((), self.warnings, self.errors)
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

impl<V, E> BiTentative<V, E> {
    pub fn new_either1(x: V, error: Option<E>, is_error: bool) -> Self {
        if let Some(e) = error {
            if is_error {
                Self::new(x, [], [e])
            } else {
                Self::new(x, [e], [])
            }
        } else {
            Self::new1(x)
        }
    }
}

impl<V, W> Tentative<V, W, Infallible> {
    pub fn into_terminal(self) -> Terminal<V, W> {
        Terminal::new(self.value, self.warnings)
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

impl<V, W, E> Tentative<Option<V>, W, E> {
    pub fn transpose(self) -> Option<Tentative<V, W, E>> {
        if let Some(value) = self.value {
            Some(Tentative::new(value, self.warnings, self.errors))
        } else {
            None
        }
    }
}

impl<V: Default, W, E> Default for Tentative<V, W, E> {
    fn default() -> Self {
        Self::new1(V::default())
    }
}

impl<V: Default, W> Default for Terminal<V, W> {
    fn default() -> Self {
        Self::new1(V::default())
    }
}

impl<P, W, E> DeferredFailure<P, W, E> {
    pub fn push_warning(&mut self, x: impl Into<W>) {
        self.warnings.push(x.into());
    }

    pub fn push_error(&mut self, x: impl Into<E>) {
        self.errors.push(x.into());
    }

    pub fn push_error_or_warning<X>(&mut self, x: X, is_error: bool)
    where
        X: Into<E> + Into<W>,
    {
        if is_error {
            self.push_error(x);
        } else {
            self.push_warning(x);
        }
    }

    pub fn map_passthru<F, X>(self, f: F) -> DeferredFailure<X, W, E>
    where
        F: FnOnce(P) -> X,
    {
        DeferredFailure::new(self.warnings, self.errors, f(self.passthru))
    }

    pub fn map_warnings<F, X>(self, f: F) -> DeferredFailure<P, X, E>
    where
        F: Fn(W) -> X,
    {
        DeferredFailure::new(self.warnings.into_iter().map(f), self.errors, self.passthru)
    }

    pub fn map_errors<F, X>(self, f: F) -> DeferredFailure<P, W, X>
    where
        F: Fn(E) -> X,
    {
        DeferredFailure::new(self.warnings, self.errors.map(f), self.passthru)
    }

    pub fn warnings_into<X>(self) -> DeferredFailure<P, X, E>
    where
        X: From<W>,
    {
        self.map_warnings(Into::into)
    }

    pub fn errors_into<X>(self) -> DeferredFailure<P, W, X>
    where
        X: From<E>,
    {
        self.map_errors(Into::into)
    }

    pub fn unfail(self) -> Tentative<P, W, E> {
        Tentative::new(self.passthru, self.warnings, *self.errors)
    }

    pub fn drop(self) -> DeferredFailure<(), W, E> {
        DeferredFailure::new(self.warnings, self.errors, ())
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
        DeferredFailure::new(self.warnings, self.errors, f(self.passthru, other.passthru))
    }

    pub fn void(self) -> DeferredFailure<(), W, E> {
        DeferredFailure::new(self.warnings, self.errors, ())
    }
}

impl<P> DeferredFailure<P, Infallible, Infallible> {
    pub fn unwrap_infallible(self) -> P {
        self.passthru
    }
}

impl<W, E> DeferredFailure<(), W, E> {
    pub fn new1(e: impl Into<E>) -> Self {
        Self::new([], NonEmpty::new(e.into()), ())
    }

    pub fn new2(errors: NonEmpty<E>) -> Self {
        Self::new([], errors, ())
    }

    pub fn mappend(&mut self, other: Self) {
        self.warnings.extend(other.warnings);
        self.errors.extend(*other.errors);
    }

    #[must_use]
    pub fn mconcat(es: NonEmpty<Self>) -> Self {
        let mut acc = es.head;
        for x in es.tail {
            acc.mappend(x);
        }
        acc
    }

    pub fn terminate<T>(self, reason: T) -> TerminalFailure<W, E, T> {
        TerminalFailure::new(self.warnings, self.errors, reason)
    }

    pub fn terminate_warn2err<T, F>(mut self, reason: T, f: F) -> TerminalFailure<W, E, T>
    where
        F: Fn(W) -> E,
    {
        self.errors.extend(self.warnings.into_iter().map(f));
        TerminalFailure::new([], self.errors, reason)
    }

    pub fn unfail_with<V>(self, value: V) -> Tentative<V, W, E> {
        Tentative::new(value, self.warnings, *self.errors)
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
        Tentative::new((), ws, es)
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
        E1: From<Self::E>;

    fn zip<A>(self, a: Result<A, Self::E>) -> MultiResult<(Self::V, A), Self::E>;

    fn zip3<A, B>(
        self,
        a: Result<A, Self::E>,
        b: Result<B, Self::E>,
    ) -> MultiResult<(Self::V, A, B), Self::E>;

    fn void(self) -> Result<(), Self::E>;

    fn into_tentative(
        self,
        default: Self::V,
        is_error: bool,
    ) -> Tentative<Self::V, Self::E, Self::E> {
        self.into_tentative_opt(is_error)
            .map(|x| x.unwrap_or(default))
    }

    fn into_tentative_warn<X>(self, default: Self::V) -> Tentative<Self::V, Self::E, X> {
        self.into_tentative_warn_opt().map(|x| x.unwrap_or(default))
    }

    fn into_tentative_err<X>(self, default: Self::V) -> Tentative<Self::V, X, Self::E> {
        self.into_tentative_err_opt().map(|x| x.unwrap_or(default))
    }

    fn into_tentative_def(self, is_error: bool) -> Tentative<Self::V, Self::E, Self::E>
    where
        Self::V: Default,
    {
        self.into_tentative_opt(is_error)
            .map(Option::unwrap_or_default)
    }

    fn into_tentative_warn_def<X>(self) -> Tentative<Self::V, Self::E, X>
    where
        Self::V: Default,
    {
        self.into_tentative_warn_opt()
            .map(Option::unwrap_or_default)
    }

    fn into_tentative_err_def<X>(self) -> Tentative<Self::V, X, Self::E>
    where
        Self::V: Default,
    {
        self.into_tentative_err_opt().map(Option::unwrap_or_default)
    }

    fn into_tentative_opt(self, is_error: bool) -> Tentative<Option<Self::V>, Self::E, Self::E>;

    fn into_tentative_warn_opt<X>(self) -> Tentative<Option<Self::V>, Self::E, X>;

    fn into_tentative_err_opt<X>(self) -> Tentative<Option<Self::V>, X, Self::E>;

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

    fn into_deferred<W, E1>(self) -> DeferredResult<Self::V, W, E1>
    where
        E1: From<Self::E>,
    {
        self.map(Tentative::new1).map_err(DeferredFailure::new1)
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

    fn into_tentative_opt(self, is_error: bool) -> Tentative<Option<Self::V>, Self::E, Self::E> {
        self.map_or_else(
            |e| Tentative::new_either(None, [e], is_error),
            |v| Tentative::new1(Some(v)),
        )
    }

    fn into_tentative_warn_opt<X>(self) -> Tentative<Option<Self::V>, Self::E, X> {
        self.map_or_else(
            |e| Tentative::new(None, [e], []),
            |v| Tentative::new1(Some(v)),
        )
    }

    fn into_tentative_err_opt<X>(self) -> Tentative<Option<Self::V>, X, Self::E> {
        self.map_or_else(
            |e| Tentative::new(None, [], [e]),
            |v| Tentative::new1(Some(v)),
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

    fn def_value_into<ToV>(self) -> PassthruResult<ToV, Self::P, Self::W, Self::E>
    where
        ToV: From<Self::V>,
    {
        self.def_map_value(Into::into)
    }

    fn def_inner_into<ToW, ToE>(self) -> PassthruResult<Self::V, Self::P, ToW, ToE>
    where
        ToW: From<Self::W>,
        ToE: From<Self::E>,
    {
        self.def_errors_into().def_warnings_into()
    }

    fn def_errors_into<ToE>(self) -> PassthruResult<Self::V, Self::P, Self::W, ToE>
    where
        ToE: From<Self::E>,
    {
        self.def_map_errors(Into::into)
    }

    fn def_errors_liftio(self) -> PassthruResult<Self::V, Self::P, Self::W, ImpureError<Self::E>> {
        self.def_map_errors(ImpureError::Pure)
    }

    fn def_warnings_into<ToW>(self) -> PassthruResult<Self::V, Self::P, ToW, Self::E>
    where
        ToW: From<Self::W>,
    {
        self.def_map_warnings(Into::into)
    }

    fn def_map_value<F, X>(self, f: F) -> PassthruResult<X, Self::P, Self::W, Self::E>
    where
        F: FnOnce(Self::V) -> X;

    fn def_map_warnings<F, X>(self, f: F) -> PassthruResult<Self::V, Self::P, X, Self::E>
    where
        F: Fn(Self::W) -> X;

    fn def_map_errors<F, X>(self, f: F) -> PassthruResult<Self::V, Self::P, Self::W, X>
    where
        F: Fn(Self::E) -> X;

    fn def_and_tentatively<F, X>(self, f: F) -> PassthruResult<X, Self::P, Self::W, Self::E>
    where
        F: FnOnce(Self::V) -> Tentative<X, Self::W, Self::E>;

    fn def_eval_error<F, X>(&mut self, f: F)
    where
        X: Into<Self::E>,
        F: FnOnce(&Self::V) -> Option<X>;

    fn def_eval_warning<F, X>(&mut self, f: F)
    where
        X: Into<Self::W>,
        F: FnOnce(&Self::V) -> Option<X>;

    fn def_push_error(&mut self, e: impl Into<Self::E>);

    fn def_push_warning(&mut self, w: impl Into<Self::W>);

    fn def_push_error_or_warning<X>(&mut self, x: X, is_error: bool)
    where
        X: Into<Self::W> + Into<Self::E>,
    {
        if is_error {
            self.def_push_error(x);
        } else {
            self.def_push_warning(x);
        }
    }

    fn def_void_passthru(self) -> DeferredResult<Self::V, Self::W, Self::E>;
}

impl<V, P, W, E> PassthruExt for PassthruResult<V, P, W, E> {
    type V = V;
    type P = P;
    type W = W;
    type E = E;

    fn def_map_value<F, X>(self, f: F) -> PassthruResult<X, Self::P, Self::W, Self::E>
    where
        F: FnOnce(Self::V) -> X,
    {
        self.map(|x| x.map(f))
    }

    fn def_map_warnings<F, X>(self, f: F) -> PassthruResult<Self::V, Self::P, X, Self::E>
    where
        F: Fn(Self::W) -> X,
    {
        match self {
            Ok(x) => Ok(x.map_warnings(f)),
            Err(x) => Err(x.map_warnings(f)),
        }
    }

    fn def_map_errors<F, X>(self, f: F) -> PassthruResult<Self::V, Self::P, Self::W, X>
    where
        F: Fn(Self::E) -> X,
    {
        match self {
            Ok(x) => Ok(x.map_errors(f)),
            Err(x) => Err(x.map_errors(f)),
        }
    }

    fn def_and_tentatively<F, X>(self, f: F) -> PassthruResult<X, Self::P, Self::W, Self::E>
    where
        F: FnOnce(Self::V) -> Tentative<X, Self::W, Self::E>,
    {
        self.map(|x| x.and_tentatively(f))
    }

    fn def_eval_error<F, X>(&mut self, f: F)
    where
        X: Into<Self::E>,
        F: FnOnce(&Self::V) -> Option<X>,
    {
        if let Ok(tnt) = self.as_mut() {
            tnt.eval_error(f);
        }
    }

    fn def_eval_warning<F, X>(&mut self, f: F)
    where
        X: Into<Self::W>,
        F: FnOnce(&Self::V) -> Option<X>,
    {
        if let Ok(tnt) = self.as_mut() {
            tnt.eval_warning(f);
        }
    }

    fn def_push_error(&mut self, e: impl Into<Self::E>) {
        match self {
            Ok(tnt) => tnt.push_error(e),
            Err(f) => f.push_error(e),
        }
    }

    fn def_push_warning(&mut self, w: impl Into<Self::W>) {
        match self {
            Ok(tnt) => tnt.push_warning(w),
            Err(f) => f.push_warning(w),
        }
    }

    fn def_void_passthru(self) -> DeferredResult<Self::V, Self::W, Self::E> {
        self.map_err(DeferredFailure::void)
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
    fn def_zip<V1>(
        self,
        a: DeferredResult<V1, Self::W, Self::E>,
    ) -> DeferredResult<(Self::V, V1), Self::W, Self::E>;

    #[allow(clippy::type_complexity)]
    fn def_zip3<V1, V2>(
        self,
        a: DeferredResult<V1, Self::W, Self::E>,
        b: DeferredResult<V2, Self::W, Self::E>,
    ) -> DeferredResult<(Self::V, V1, V2), Self::W, Self::E>;

    fn def_and_maybe<F, X>(self, f: F) -> DeferredResult<X, Self::W, Self::E>
    where
        F: FnOnce(Self::V) -> DeferredResult<X, Self::W, Self::E>;

    fn def_and_then<F, X>(self, f: F) -> DeferredResult<X, Self::W, Self::E>
    where
        F: FnOnce(Self::V) -> Result<X, Self::E>;

    fn def_terminate<T>(self, reason: T) -> TerminalResult<Self::V, Self::W, Self::E, T>;

    fn def_terminate_warn2err<T, F>(
        self,
        reason: T,
        f: F,
    ) -> TerminalResult<Self::V, Self::W, Self::E, T>
    where
        F: Fn(Self::W) -> Self::E;

    fn def_terminate_nowarn<T>(self, reason: T) -> TerminalResult<Self::V, Self::W, Self::E, T>;

    fn def_terminate_maybe_warn<T, F>(
        self,
        reason: T,
        conf: &SharedConfig,
        f: F,
    ) -> TerminalResult<Self::V, Self::W, Self::E, T>
    where
        F: Fn(Self::W) -> Self::E,
    {
        if conf.warnings_are_errors {
            self.def_terminate_warn2err(reason, f)
        } else if conf.hide_warnings {
            self.def_terminate_nowarn(reason)
        } else {
            self.def_terminate(reason)
        }
    }

    fn def_unfail(self) -> Tentative<Option<Self::V>, Self::W, Self::E>;

    fn def_unfail_default(self) -> Tentative<Self::V, Self::W, Self::E>
    where
        Self::V: Default,
    {
        self.def_unfail().map(|_| Self::V::default())
    }
}

impl<V, W, E> DeferredExt for DeferredResult<V, W, E> {
    fn def_zip<V1>(
        self,
        a: DeferredResult<V1, Self::W, Self::E>,
    ) -> DeferredResult<(Self::V, V1), Self::W, Self::E> {
        self.zip(a)
            .map_err(DeferredFailure::mconcat)
            .map(|(ax, bx)| ax.zip(bx))
    }

    fn def_zip3<V1, V2>(
        self,
        a: DeferredResult<V1, Self::W, Self::E>,
        b: DeferredResult<V2, Self::W, Self::E>,
    ) -> DeferredResult<(Self::V, V1, V2), Self::W, Self::E> {
        self.zip3(a, b)
            .map_err(DeferredFailure::mconcat)
            .map(|(ax, bx, cx)| ax.zip3(bx, cx))
    }

    fn def_and_maybe<F, X>(self, f: F) -> DeferredResult<X, Self::W, Self::E>
    where
        F: FnOnce(Self::V) -> DeferredResult<X, Self::W, Self::E>,
    {
        let x = self?;
        x.and_maybe(f)
    }

    fn def_and_then<F, X>(self, f: F) -> DeferredResult<X, Self::W, Self::E>
    where
        F: FnOnce(Self::V) -> Result<X, Self::E>,
    {
        self.def_and_maybe(|x| f(x).map(Tentative::new1).map_err(DeferredFailure::new1))
    }

    fn def_terminate<T>(self, reason: T) -> TerminalResult<Self::V, Self::W, Self::E, T> {
        match self {
            Ok(t) => t.terminate(reason),
            Err(e) => Err(e.terminate(reason)),
        }
    }

    fn def_terminate_warn2err<T, F>(
        self,
        reason: T,
        f: F,
    ) -> TerminalResult<Self::V, Self::W, Self::E, T>
    where
        F: Fn(W) -> E,
    {
        match self {
            Ok(t) => t.terminate_warn2err(reason, f),
            Err(e) => Err(e.terminate_warn2err(reason, f)),
        }
    }

    fn def_terminate_nowarn<T>(self, reason: T) -> TerminalResult<Self::V, Self::W, Self::E, T> {
        match self {
            Ok(t) => t.terminate_nowarn(reason),
            Err(e) => Err(TerminalFailure::new([], *e.errors, reason)),
        }
    }

    fn def_unfail(self) -> Tentative<Option<Self::V>, Self::W, Self::E> {
        self.map_or_else(|fail| fail.unfail_with(None), |tnt| tnt.map(Some))
    }
}

pub fn def_transpose<X, W, E>(
    x: Option<DeferredResult<X, W, E>>,
) -> DeferredResult<Option<X>, W, E> {
    x.map_or(Ok(Tentative::new1(None)), |y| y.def_map_value(Some))
}

pub trait IODeferredExt: Sized + PassthruExt
where
    Self: PassthruExt<E = ImpureError<Self::InnerE>>,
{
    type InnerE;

    fn def_io_into<ToE, ToW>(self) -> IODeferredResult<Self::V, ToW, ToE>
    where
        Self: PassthruExt<P = ()>,
        ToE: From<Self::InnerE>,
        ToW: From<Self::W>,
    {
        self.def_map_errors(ImpureError::inner_into)
            .def_warnings_into()
    }

    fn def_io_push_error_or_warning<X>(&mut self, x: X, is_error: bool)
    where
        X: Into<Self::W> + Into<Self::InnerE>,
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
