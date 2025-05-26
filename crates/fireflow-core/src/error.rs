use nonempty::NonEmpty;
use std::fmt;
use std::io;

// TODO add a cap to the error buffer so the user doesn't DOS themselves if
// their file is particularly screwed up

pub type DeferredResult<V, W, E> = Result<Tentative<V, W, E>, DeferredFailure<W, E>>;

pub struct Tentative<V, W, E> {
    value: V,
    warnings: Vec<W>,
    errors: Vec<E>,
}

pub struct DeferredFailure<W, E> {
    warnings: Vec<W>,
    errors: NonEmpty<E>,
}

pub type TerminalResult<V, W, E, T> = Result<Terminal<V, W>, TerminalFailure<W, E, T>>;

pub struct Terminal<V, W> {
    value: V,
    warnings: Vec<W>,
}

pub struct TerminalFailure<W, E, T> {
    warnings: Vec<W>,
    failure: Failure<E, T>,
}

pub enum Failure<E, T> {
    Single(T),
    Many(T, Box<NonEmpty<E>>),
}

pub enum ImpureError<E> {
    IO(io::Error),
    Pure(E),
}

pub type MultiResult<X, E> = Result<X, NonEmpty<E>>;

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
            let tail = self.flat_map(|x| x.err()).collect();
            Err((h, tail).into())
        } else {
            Ok(pass)
        }
    }
}

impl<I: Iterator<Item = Result<T, E>>, T, E> ErrorIter<T, E> for I {}

impl<V, W> Terminal<V, W> {
    pub fn new(value: V) -> Self {
        Self {
            value,
            warnings: vec![],
        }
    }

    pub fn value_into<U>(self) -> Terminal<U, W>
    where
        U: From<V>,
    {
        self.map(|v| v.into())
    }

    pub fn map<F, X>(self, f: F) -> Terminal<X, W>
    where
        F: FnOnce(V) -> X,
    {
        Terminal {
            value: f(self.value),
            warnings: self.warnings,
        }
    }

    pub fn warnings_map<F, X>(self, f: F) -> Terminal<V, X>
    where
        F: Fn(W) -> X,
    {
        Terminal {
            value: self.value,
            warnings: self.warnings.into_iter().map(f).collect(),
        }
    }

    pub fn warnings_into<X>(self) -> Terminal<V, X>
    where
        X: From<W>,
    {
        self.warnings_map(|w| w.into())
    }

    pub fn and_finally<E, T, F, X>(mut self, f: F) -> TerminalResult<X, W, E, T>
    where
        F: FnOnce(V) -> TerminalResult<X, W, E, T>,
    {
        match f(self.value) {
            Ok(s) => {
                self.warnings.extend(s.warnings);
                Ok(Terminal {
                    value: s.value,
                    warnings: self.warnings,
                })
            }
            Err(e) => {
                self.warnings.extend(e.warnings);
                Err(TerminalFailure {
                    warnings: self.warnings,
                    failure: e.failure,
                })
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
                Tentative {
                    value: s.value,
                    warnings: self.warnings,
                    errors: s.errors,
                }
                .terminate(reason)
            }
            Err(e) => {
                self.warnings.extend(e.warnings);
                Err(DeferredFailure {
                    warnings: self.warnings,
                    errors: e.errors,
                }
                .terminate(reason))
            }
        }
    }

    pub fn and_tentatively<F, X, E, T>(mut self, reason: T, f: F) -> TerminalResult<X, W, E, T>
    where
        F: FnOnce(V) -> Tentative<X, W, E>,
    {
        let s = f(self.value);
        self.warnings.extend(s.warnings);
        Tentative {
            value: s.value,
            warnings: self.warnings,
            errors: s.errors,
        }
        .terminate(reason)
    }

    pub fn resolve<F, X>(self, f: F) -> (V, X)
    where
        F: FnOnce(Vec<W>) -> X,
    {
        (self.value, f(self.warnings))
    }
}

impl<V> Terminal<V, ()> {
    pub fn inner(self) -> V {
        self.value
    }
}

impl<E, T> Failure<E, T> {
    pub fn map_errors<F, X>(self, f: F) -> Failure<X, T>
    where
        F: Fn(E) -> X,
    {
        match self {
            Failure::Many(t, es) => Failure::Many(t, Box::new(es.map(f))),
            Failure::Single(t) => Failure::Single(t),
        }
    }

    pub fn map_terminal<F, X>(self, f: F) -> Failure<E, X>
    where
        F: FnOnce(T) -> X,
    {
        match self {
            Failure::Many(t, es) => Failure::Many(f(t), es),
            Failure::Single(t) => Failure::Single(f(t)),
        }
    }
}

impl<W, E, T> TerminalFailure<W, E, T> {
    pub fn new(errors: Failure<E, T>) -> Self {
        TerminalFailure {
            warnings: vec![],
            failure: errors,
        }
    }

    pub fn new_single(t: T) -> Self {
        Self::new(Failure::Single(t))
    }

    pub fn new_many(t: T, es: NonEmpty<E>) -> Self {
        Self::new(Failure::Many(t, Box::new(es)))
    }

    pub fn warnings_map<F, X>(self, f: F) -> TerminalFailure<X, E, T>
    where
        F: Fn(W) -> X,
    {
        TerminalFailure {
            warnings: self.warnings.into_iter().map(f).collect(),
            failure: self.failure,
        }
    }

    pub fn errors_map<F, X>(self, f: F) -> TerminalFailure<W, X, T>
    where
        F: Fn(E) -> X,
    {
        TerminalFailure {
            warnings: self.warnings,
            failure: self.failure.map_errors(f),
        }
    }

    pub fn terminal_map<F, X>(self, f: F) -> TerminalFailure<W, E, X>
    where
        F: FnOnce(T) -> X,
    {
        TerminalFailure {
            warnings: self.warnings,
            failure: self.failure.map_terminal(f),
        }
    }

    pub fn warnings_into<X>(self) -> TerminalFailure<X, E, T>
    where
        X: From<W>,
    {
        self.warnings_map(|w| w.into())
    }

    pub fn errors_into<X>(self) -> TerminalFailure<W, X, T>
    where
        X: From<E>,
    {
        self.errors_map(|e| e.into())
    }

    pub fn terminal_into<X>(self) -> TerminalFailure<W, E, X>
    where
        X: From<T>,
    {
        self.terminal_map(|e| e.into())
    }

    pub fn resolve<F, G, X, Y>(self, f: F, g: G) -> (X, Y)
    where
        F: FnOnce(Vec<W>) -> X,
        G: FnOnce(Failure<E, T>) -> Y,
    {
        (f(self.warnings), g(self.failure))
    }
}

impl<V, W, E> Tentative<V, W, E> {
    pub fn new(value: V, warnings: Vec<W>, errors: Vec<E>) -> Self {
        Self {
            value,
            warnings,
            errors,
        }
    }

    pub fn new1(value: V) -> Self {
        Self::new(value, vec![], vec![])
    }

    pub fn push_warning(&mut self, x: W) {
        self.warnings.push(x)
    }

    pub fn push_error(&mut self, x: E) {
        self.errors.push(x)
    }

    pub fn extend_warnings(&mut self, xs: Vec<W>) {
        self.warnings.extend(xs)
    }

    pub fn extend_errors(&mut self, xs: Vec<E>) {
        self.errors.extend(xs)
    }

    pub fn map<F, X>(self, f: F) -> Tentative<X, W, E>
    where
        F: FnOnce(V) -> X,
    {
        Tentative {
            value: f(self.value),
            warnings: self.warnings,
            errors: self.errors,
        }
    }

    pub fn and_finally<F, X, T>(mut self, mut f: F) -> TerminalResult<X, W, E, T>
    where
        F: FnMut(V) -> TerminalResult<X, W, E, T>,
    {
        match f(self.value) {
            Ok(s) => {
                self.warnings.extend(s.warnings);
                Ok(Terminal {
                    value: s.value,
                    warnings: self.warnings,
                })
            }
            Err(e) => {
                self.warnings.extend(e.warnings);
                Err(TerminalFailure {
                    warnings: self.warnings,
                    failure: e.failure,
                })
            }
        }
    }

    pub fn and_maybe<F, X>(mut self, f: F) -> DeferredResult<X, W, E>
    where
        F: FnOnce(V) -> DeferredResult<X, W, E>,
    {
        match f(self.value) {
            Ok(s) => {
                self.warnings.extend(s.warnings);
                self.errors.extend(s.errors);
                Ok(Tentative {
                    value: s.value,
                    warnings: self.warnings,
                    errors: self.errors,
                })
            }
            Err(e) => {
                self.warnings.extend(e.warnings);
                self.errors.extend(e.errors);
                Err(DeferredFailure {
                    warnings: self.warnings,
                    errors: NonEmpty::from_vec(self.errors).unwrap(),
                })
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
        Tentative {
            value: s.value,
            warnings: self.warnings,
            errors: self.errors,
        }
    }

    // TODO make warning version
    pub fn eval_error<F>(&mut self, f: F)
    where
        F: Fn(&V) -> Option<E>,
    {
        if let Some(e) = f(&self.value) {
            self.errors.push(e);
        }
    }

    pub fn eval_errors<F>(&mut self, f: F)
    where
        F: Fn(&V) -> Vec<E>,
    {
        self.errors.extend(f(&self.value));
    }

    pub fn warnings_map<F, X>(self, f: F) -> Tentative<V, X, E>
    where
        F: Fn(W) -> X,
    {
        Tentative {
            value: self.value,
            warnings: self.warnings.into_iter().map(f).collect(),
            errors: self.errors,
        }
    }

    pub fn errors_map<F, X>(self, f: F) -> Tentative<V, W, X>
    where
        F: Fn(E) -> X,
    {
        Tentative {
            value: self.value,
            warnings: self.warnings,
            errors: self.errors.into_iter().map(f).collect(),
        }
    }

    pub fn warnings_into<X>(self) -> Tentative<V, X, E>
    where
        X: From<W>,
    {
        self.warnings_map(|w| w.into())
    }

    pub fn errors_into<X>(self) -> Tentative<V, W, X>
    where
        X: From<E>,
    {
        self.errors_map(|e| e.into())
    }

    pub fn mconcat(xs: Vec<Self>) -> Tentative<Vec<V>, W, E> {
        let mut ret = Tentative::new1(vec![]);
        for x in xs {
            ret.value.push(x.value);
            ret.warnings.extend(x.warnings);
            ret.errors.extend(x.errors);
        }
        ret
    }

    pub fn terminate<T>(self, reason: T) -> TerminalResult<V, W, E, T> {
        match NonEmpty::from_vec(self.errors) {
            Some(errors) => Err(TerminalFailure {
                warnings: self.warnings,
                failure: Failure::Many(reason, Box::new(errors)),
            }),
            None => Ok(Terminal {
                value: self.value,
                warnings: self.warnings,
            }),
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
        Tentative {
            value: f(self.value, other.value),
            warnings: self.warnings,
            errors: self.errors,
        }
    }
}

impl<W, E> DeferredFailure<W, E> {
    pub fn new(warnings: Vec<W>, errors: NonEmpty<E>) -> Self {
        Self { warnings, errors }
    }

    pub fn new1(e: E) -> Self {
        Self::new(vec![], NonEmpty::new(e))
    }

    pub fn new2(errors: NonEmpty<E>) -> Self {
        Self::new(vec![], errors)
    }

    pub fn warnings_map<F, X>(self, f: F) -> DeferredFailure<X, E>
    where
        F: Fn(W) -> X,
    {
        DeferredFailure {
            warnings: self.warnings.into_iter().map(f).collect(),
            errors: self.errors,
        }
    }

    pub fn errors_map<F, X>(self, f: F) -> DeferredFailure<W, X>
    where
        F: Fn(E) -> X,
    {
        DeferredFailure {
            warnings: self.warnings,
            errors: self.errors.map(f),
        }
    }

    pub fn warnings_into<X>(self) -> DeferredFailure<X, E>
    where
        X: From<W>,
    {
        self.warnings_map(|w| w.into())
    }

    pub fn errors_into<X>(self) -> DeferredFailure<W, X>
    where
        X: From<E>,
    {
        self.errors_map(|e| e.into())
    }

    pub fn mappend(mut self, other: Self) -> Self {
        self.warnings.extend(other.warnings);
        self.errors.extend(other.errors);
        Self {
            warnings: self.warnings,
            errors: self.errors,
        }
    }

    pub fn fold(es: NonEmpty<Self>) -> Self {
        es.tail.into_iter().fold(es.head, |acc, x| acc.mappend(x))
    }

    pub fn terminate<T>(self, reason: T) -> TerminalFailure<W, E, T> {
        TerminalFailure {
            warnings: self.warnings,
            failure: Failure::Many(reason, Box::new(self.errors)),
        }
    }
}

pub trait ResultExt {
    type V;
    type E;

    fn into_mult<F>(self) -> MultiResult<Self::V, F>
    where
        F: From<Self::E>;

    fn into_deferred0<F, W>(self) -> DeferredResult<Self::V, W, F>
    where
        F: From<Self::E>;

    fn zip<A>(self, a: Result<A, Self::E>) -> MultiResult<(Self::V, A), Self::E>;

    fn zip3<A, B>(
        self,
        a: Result<A, Self::E>,
        b: Result<B, Self::E>,
    ) -> MultiResult<(Self::V, A, B), Self::E>;
}

impl<V, E> ResultExt for Result<V, E> {
    type V = V;
    type E = E;

    fn into_mult<F>(self) -> MultiResult<Self::V, F>
    where
        F: From<Self::E>,
    {
        self.map_err(|e| NonEmpty::new(e.into()))
    }

    fn into_deferred0<F, W>(self) -> DeferredResult<Self::V, W, F>
    where
        F: From<Self::E>,
    {
        self.map(Tentative::new1)
            .map_err(|e| DeferredFailure::new1(e.into()))
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
}

pub trait MultiResultExt {
    type V;
    type E;

    fn into_deferred1<F, W>(self) -> DeferredResult<Self::V, W, F>
    where
        F: From<Self::E>;

    fn zip_mult<A>(self, a: MultiResult<A, Self::E>) -> MultiResult<(Self::V, A), Self::E>;

    fn zip_mult3<A, B>(
        self,
        a: MultiResult<A, Self::E>,
        b: MultiResult<B, Self::E>,
    ) -> MultiResult<(Self::V, A, B), Self::E>;
}

impl<V, E> MultiResultExt for MultiResult<V, E> {
    type V = V;
    type E = E;

    fn into_deferred1<F, W>(self) -> DeferredResult<Self::V, W, F>
    where
        F: From<Self::E>,
    {
        self.map(Tentative::new1)
            .map_err(DeferredFailure::new2)
            .error_into()
    }

    fn zip_mult<A>(self, a: MultiResult<A, Self::E>) -> MultiResult<(Self::V, A), Self::E> {
        self.zip(a).map_err(NonEmpty::flatten)
    }

    fn zip_mult3<A, B>(
        self,
        a: MultiResult<A, Self::E>,
        b: MultiResult<B, Self::E>,
    ) -> MultiResult<(Self::V, A, B), Self::E> {
        self.zip3(a, b).map_err(NonEmpty::flatten)
    }
}

pub trait DeferredExt {
    type V;
    type E;
    type W;

    fn inner_into<ToW, ToE>(self) -> DeferredResult<Self::V, ToW, ToE>
    where
        ToW: From<Self::W>,
        ToE: From<Self::E>;

    fn error_into<ToE>(self) -> DeferredResult<Self::V, Self::W, ToE>
    where
        ToE: From<Self::E>;

    fn error_impure(self) -> DeferredResult<Self::V, Self::W, ImpureError<Self::E>>;

    fn warning_into<ToW>(self) -> DeferredResult<Self::V, ToW, Self::E>
    where
        ToW: From<Self::W>;

    fn map_value<F, X>(self, f: F) -> DeferredResult<X, Self::W, Self::E>
    where
        F: FnOnce(Self::V) -> X;

    fn warnings_map<F, X>(self, f: F) -> DeferredResult<Self::V, X, Self::E>
    where
        F: Fn(Self::W) -> X;

    fn errors_map<F, X>(self, f: F) -> DeferredResult<Self::V, Self::W, X>
    where
        F: Fn(Self::E) -> X;

    fn zip_def<A>(
        self,
        a: DeferredResult<A, Self::W, Self::E>,
    ) -> DeferredResult<(Self::V, A), Self::W, Self::E>;

    #[allow(clippy::type_complexity)]
    fn zip_def3<A, B>(
        self,
        a: DeferredResult<A, Self::W, Self::E>,
        b: DeferredResult<B, Self::W, Self::E>,
    ) -> DeferredResult<(Self::V, A, B), Self::W, Self::E>;

    fn and_tentatively<F, X>(self, f: F) -> DeferredResult<X, Self::W, Self::E>
    where
        F: FnOnce(Self::V) -> Tentative<X, Self::W, Self::E>;

    fn and_maybe<F, X>(self, f: F) -> DeferredResult<X, Self::W, Self::E>
    where
        F: FnOnce(Self::V) -> DeferredResult<X, Self::W, Self::E>;

    fn terminate<T>(self, reason: T) -> TerminalResult<Self::V, Self::W, Self::E, T>;
}

impl<V, W, E> DeferredExt for DeferredResult<V, W, E> {
    type V = V;
    type W = W;
    type E = E;

    fn inner_into<ToW, ToE>(self) -> DeferredResult<Self::V, ToW, ToE>
    where
        ToW: From<Self::W>,
        ToE: From<Self::E>,
    {
        self.map_err(|e| e.errors_into().warnings_into())
            .map(|t| t.errors_into().warnings_into())
    }

    fn error_into<ToE>(self) -> DeferredResult<Self::V, Self::W, ToE>
    where
        ToE: From<Self::E>,
    {
        self.map_err(|e| e.errors_into()).map(|t| t.errors_into())
    }

    fn error_impure(self) -> DeferredResult<Self::V, Self::W, ImpureError<E>> {
        self.map(|t| t.errors_map(ImpureError::Pure))
            .map_err(|e| e.errors_map(ImpureError::Pure))
    }

    fn warning_into<ToW>(self) -> DeferredResult<Self::V, ToW, Self::E>
    where
        ToW: From<Self::W>,
    {
        self.map_err(|e| e.warnings_into())
            .map(|t| t.warnings_into())
    }

    fn map_value<F, X>(self, f: F) -> DeferredResult<X, Self::W, Self::E>
    where
        F: FnOnce(Self::V) -> X,
    {
        self.map(|x| x.map(f))
    }

    fn warnings_map<F, X>(self, f: F) -> DeferredResult<Self::V, X, Self::E>
    where
        F: Fn(Self::W) -> X,
    {
        match self {
            Ok(x) => Ok(x.warnings_map(f)),
            Err(x) => Err(x.warnings_map(f)),
        }
    }

    fn errors_map<F, X>(self, f: F) -> DeferredResult<Self::V, Self::W, X>
    where
        F: Fn(Self::E) -> X,
    {
        match self {
            Ok(x) => Ok(x.errors_map(f)),
            Err(x) => Err(x.errors_map(f)),
        }
    }

    fn zip_def<A>(
        self,
        a: DeferredResult<A, Self::W, Self::E>,
    ) -> DeferredResult<(Self::V, A), Self::W, Self::E> {
        self.zip(a)
            .map_err(DeferredFailure::fold)
            .map(|(ax, bx)| ax.zip(bx))
    }

    fn zip_def3<A, B>(
        self,
        a: DeferredResult<A, Self::W, Self::E>,
        b: DeferredResult<B, Self::W, Self::E>,
    ) -> DeferredResult<(Self::V, A, B), Self::W, Self::E> {
        self.zip3(a, b)
            .map_err(DeferredFailure::fold)
            .map(|(ax, bx, cx)| ax.zip3(bx, cx))
    }

    fn and_tentatively<F, X>(self, f: F) -> DeferredResult<X, Self::W, Self::E>
    where
        F: FnOnce(Self::V) -> Tentative<X, Self::W, Self::E>,
    {
        self.map(|x| x.and_tentatively(f))
    }

    fn and_maybe<F, X>(self, f: F) -> DeferredResult<X, Self::W, Self::E>
    where
        F: FnOnce(Self::V) -> DeferredResult<X, Self::W, Self::E>,
    {
        self.and_then(|x| x.and_maybe(f))
    }

    fn terminate<T>(self, reason: T) -> TerminalResult<Self::V, Self::W, Self::E, T> {
        match self {
            Ok(t) => t.terminate(reason),
            Err(e) => Err(e.terminate(reason)),
        }
    }
}

impl<E> fmt::Display for ImpureError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::IO(i) => write!(f, "IO error: {i}"),
            Self::Pure(e) => e.fmt(f),
        }
    }
}

impl<E> ImpureError<E> {
    pub fn inner_into<F>(self) -> ImpureError<F>
    where
        F: From<E>,
    {
        self.map_inner(|e| e.into())
    }

    pub fn map_inner<F, X>(self, f: F) -> ImpureError<X>
    where
        F: Fn(E) -> X,
    {
        match self {
            Self::IO(x) => ImpureError::IO(x),
            Self::Pure(e) => ImpureError::Pure(f(e)),
        }
    }
}

impl<E> From<io::Error> for ImpureError<E> {
    fn from(value: io::Error) -> Self {
        ImpureError::IO(value)
    }
}
