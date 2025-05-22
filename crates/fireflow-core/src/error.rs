use nonempty::NonEmpty;
use std::io;

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum PureErrorLevel {
    Error,
    Warning,
    // TODO debug, info, etc
}

pub type Deferred<X, E> = Result<X, NonEmpty<E>>;

pub fn combine_results<A, B, Z>(a: Result<A, Z>, b: Result<B, Z>) -> Deferred<(A, B), Z> {
    match (a, b) {
        (Ok(ax), Ok(bx)) => Ok((ax, bx)),
        (ae, be) => {
            Err(NonEmpty::from_vec([ae.err(), be.err()].into_iter().flatten().collect()).unwrap())
        }
    }
}

pub fn combine_results3<A, B, C, Z>(
    a: Result<A, Z>,
    b: Result<B, Z>,
    c: Result<C, Z>,
) -> Deferred<(A, B, C), Z> {
    match (a, b, c) {
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

pub fn combine_results4<A, B, C, D, Z>(
    a: Result<A, Z>,
    b: Result<B, Z>,
    c: Result<C, Z>,
    d: Result<D, Z>,
) -> Deferred<(A, B, C, D), Z> {
    match (a, b, c, d) {
        (Ok(ax), Ok(bx), Ok(cx), Ok(dx)) => Ok((ax, bx, cx, dx)),
        (ae, be, ce, de) => Err(NonEmpty::from_vec(
            [ae.err(), be.err(), ce.err(), de.err()]
                .into_iter()
                .flatten()
                .collect(),
        )
        .unwrap()),
    }
}

pub fn combine_results5<A, B, C, D, E, Z>(
    a: Result<A, Z>,
    b: Result<B, Z>,
    c: Result<C, Z>,
    d: Result<D, Z>,
    e: Result<E, Z>,
) -> Deferred<(A, B, C, D, E), Z> {
    match (a, b, c, d, e) {
        (Ok(ax), Ok(bx), Ok(cx), Ok(dx), Ok(ex)) => Ok((ax, bx, cx, dx, ex)),
        (ae, be, ce, de, ee) => Err(NonEmpty::from_vec(
            [ae.err(), be.err(), ce.err(), de.err(), ee.err()]
                .into_iter()
                .flatten()
                .collect(),
        )
        .unwrap()),
    }
}

pub(crate) trait ErrorIter<T, E>: Iterator<Item = Result<T, E>> + Sized {
    fn gather(self) -> Deferred<Vec<T>, E> {
        let mut pass = vec![];
        let mut fail = vec![];
        for x in self {
            match x {
                Ok(y) => pass.push(y),
                Err(y) => fail.push(y),
            }
        }
        NonEmpty::from_vec(fail).map(Err).unwrap_or(Ok(pass))
    }
}

impl<I: Iterator<Item = Result<T, E>>, T, E> ErrorIter<T, E> for I {}

pub type DeferredResult<V, W, E> = Result<Tentative<V, W, E>, DeferredFailure<W, E>>;

// TODO tentative shouldn't be in Ok, that's the point
pub type TerminalResult<V, W, E, T> = Result<Tentative<V, W, E>, TerminalFailure<W, E, T>>;

pub struct Tentative<V, W, E> {
    pub value: V,
    pub warnings: Vec<W>,
    pub errors: Vec<E>,
}

pub struct DeferredFailure<W, E> {
    pub warnings: Vec<W>,
    pub errors: NonEmpty<E>,
}

pub struct TerminalFailure<W, E, T> {
    pub warnings: Vec<W>,
    pub errors: TerminalFailureType<E, T>,
}

pub enum TerminalFailureType<E, T> {
    Single(T),
    Many(T, NonEmpty<E>),
}

impl<W, E, T> TerminalFailure<W, E, T> {
    pub fn new(errors: TerminalFailureType<E, T>) -> Self {
        TerminalFailure {
            warnings: vec![],
            errors,
        }
    }

    pub fn new_single(t: T) -> Self {
        Self::new(TerminalFailureType::Single(t))
    }

    pub fn new_many(t: T, es: NonEmpty<E>) -> Self {
        Self::new(TerminalFailureType::Many(t, es))
    }
}

impl<W, E> From<io::Error> for TerminalFailure<W, E, ImpureError> {
    fn from(value: io::Error) -> Self {
        TerminalFailure {
            warnings: vec![],
            errors: TerminalFailureType::Single(ImpureError::IO(value)),
        }
    }
}

impl<V, W, E> Tentative<V, W, E> {
    pub fn new(value: V) -> Self {
        Self {
            value,
            warnings: vec![],
            errors: vec![],
        }
    }

    pub fn map<F, X>(self, f: F) -> Tentative<X, W, E>
    where
        F: Fn(V) -> X,
    {
        Tentative {
            value: f(self.value),
            warnings: self.warnings,
            errors: self.errors,
        }
    }

    pub fn and_maybe<F, X>(mut self, f: F) -> DeferredResult<X, W, E>
    where
        F: Fn(V) -> DeferredResult<X, W, E>,
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
        F: Fn(V) -> Tentative<X, W, E>,
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

    pub fn from_opt_result_warn(r: Result<Option<V>, W>) -> Tentative<Option<V>, W, E> {
        r.map_or_else(
            |w| Tentative {
                value: None,
                warnings: vec![w],
                errors: vec![],
            },
            Tentative::new,
        )
    }

    pub fn append_failure(mut self, other: DeferredFailure<W, E>) -> Self {
        self.warnings.extend(other.warnings);
        self.errors.extend(other.errors);
        self
    }

    // pub fn append_result_warnings<U>(
    //     mut self,
    //     other: Deferred<U, W>,
    // ) -> DeferredResult<(V, U), W, E> {
    //     match other {
    //         Ok(y) => Ok(self.map(|x| (x, y))),
    //         Err(warnings) => {
    //             self.warnings.extend(warnings);
    //             Err(DeferredFailure {
    //                 errors: self.errors,
    //                 warnings: self.warnings,
    //             })
    //         }
    //     }
    // }

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
        let mut ret = Tentative::new(vec![]);
        for x in xs {
            ret.warnings.extend(x.warnings);
            ret.errors.extend(x.errors);
        }
        ret
    }
}

impl<W, E> DeferredFailure<W, E> {
    pub fn new(e: E) -> Self {
        Self {
            warnings: vec![],
            errors: NonEmpty::new(e),
        }
    }

    pub fn new_with_many(errors: NonEmpty<E>) -> Self {
        Self {
            warnings: vec![],
            errors,
        }
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
            errors: TerminalFailureType::Many(reason, self.errors),
        }
    }
}

// pub fn result_to_deferred<V, E>(res: Deferred<V, E>) -> DeferredResult<V, E, E> {
//     res.map_err(|errors| DeferredFailure {
//         warnings: vec![],
//         errors,
//     })
//     .map(Tentative::new)
// }

/// A pure error thrown during FCS file parsing.
///
/// This is very basic, since the only functionality we need is capturing a
/// message to show the user and an error level. The latter will dictate how the
/// error(s) is/are handled when we finish parsing.
#[derive(Eq, PartialEq)]
pub struct PureError<E> {
    pub msg: E,
    pub level: PureErrorLevel,
}

/// A collection of pure FCS errors.
///
/// Rather than exiting when we encounter the first error, we wish to capture
/// all possible errors and show the user all at once so they know what issues
/// in their files to fix. Therefore make an "error" type which is actually many
/// errors.
pub struct PureErrorBuf<E> {
    pub errors: Vec<PureError<E>>,
}

/// The result of a successful pure FCS computation which may have errors.
///
/// Since we are collecting errors and displaying them at the end of the parse
/// process, "success" needs to include any errors that have been previously
/// thrown (aka they are "deferred"). Decide later if these are a real issue and
/// parsed data needs to be withheld from the user.
pub struct PureSuccess<X, E> {
    pub deferred: PureErrorBuf<E>,
    pub data: X,
}

/// The result of a failed computation.
///
/// This includes the immediate reason for failure as well as any errors
/// encountered previously which were deferred until now.
pub struct Failure<E, F> {
    pub reason: E,
    pub deferred: PureErrorBuf<F>,
}

/// The result of a failed pure FCS computation.
pub type PureFailure<E> = Failure<String, E>;

/// Success or failure of a pure FCS computation.
// TODO shouldn't E be different on left and right?
// TODO the left side should have not errors, and the right side and have errors
// and anything else
pub type PureResult<T, E> = Result<PureSuccess<T, E>, PureFailure<E>>;

/// Result which may have failed but does not imply immediate termination.
///
/// This is different from Result<T, E> because it would not be clear how
/// warnings or other error types should be stored.
// TODO this is ill-defined, since it is possible to have no result and also no
// errors, which is nonsense
// pub type PureMaybe<T, E> = PureSuccess<Option<T>, E>;

/// Error which may either be pure or impure (within IO context).
///
/// In the pure case this only has a single error rather than the collection
/// of errors. The pure case is meant to be used as the single reason for
/// a critical error; deferred errors will be captured elsewhere. Given that
/// this is only meant to be used in the failure case, pure errors do not have
/// an error level (they are always "critical").
///
/// The impure case is always "critical" as usually this indicates something
/// went wrong with file IO, which is usually an OS issue.
pub enum ImpureErrorInner<E> {
    IO(io::Error),
    Pure(E),
}

impl<E> ImpureErrorInner<E> {
    pub fn inner_into<F>(self) -> ImpureErrorInner<F>
    where
        F: From<E>,
    {
        self.map_inner(|e| e.into())
    }

    pub fn map_inner<F, X>(self, f: F) -> ImpureErrorInner<X>
    where
        F: Fn(E) -> X,
    {
        match self {
            Self::IO(x) => ImpureErrorInner::IO(x),
            Self::Pure(e) => ImpureErrorInner::Pure(f(e)),
        }
    }
}

pub type ImpureError = ImpureErrorInner<String>;

/// The result of either a failed pure or impure computation.
pub type ImpureFailure<E> = Failure<ImpureError, E>;

/// Success or failure of a pure or impure computation.
pub type ImpureResult<T, E> = Result<PureSuccess<T, E>, ImpureFailure<E>>;

impl<E> PureError<E> {
    pub fn new_error(msg: E) -> Self {
        Self {
            msg,
            level: PureErrorLevel::Error,
        }
    }

    pub fn new_warning(msg: E) -> Self {
        Self {
            msg,
            level: PureErrorLevel::Warning,
        }
    }

    pub fn new(msg: E, is_error: bool) -> Self {
        if is_error {
            Self::new_error(msg)
        } else {
            Self::new_warning(msg)
        }
    }
}

impl<E> Default for PureErrorBuf<E> {
    fn default() -> Self {
        Self { errors: vec![] }
    }
}

impl<R, E> Failure<R, E> {
    pub fn new(reason: R) -> Failure<R, E> {
        Failure {
            reason,
            deferred: PureErrorBuf::default(),
        }
    }

    pub fn from_many(reason: R, deferred: PureErrorBuf<E>) -> Self {
        Failure { reason, deferred }
    }

    pub fn from_many_msgs(reason: R, msgs: Vec<E>, level: PureErrorLevel) -> Self {
        Self::from_many(reason, PureErrorBuf::from_many(msgs, level))
    }

    pub fn from_many_errors(reason: R, msgs: Vec<E>) -> Self {
        Self::from_many_msgs(reason, msgs, PureErrorLevel::Error)
    }

    pub fn map<X, F: Fn(R) -> X>(self, f: F) -> Failure<X, E> {
        Failure {
            reason: f(self.reason),
            deferred: self.deferred,
        }
    }

    pub fn from_result<X>(res: Result<X, R>) -> Result<X, Failure<R, E>> {
        res.map_err(Failure::new)
    }

    pub fn extend(&mut self, other: PureErrorBuf<E>) {
        self.deferred.errors.extend(other.errors);
    }
}

impl<E> PureErrorBuf<E> {
    pub fn from(msg: E, level: PureErrorLevel) -> Self {
        PureErrorBuf {
            errors: vec![PureError { msg, level }],
        }
    }

    pub fn concat(&mut self, other: Self) {
        self.errors.extend(other.errors)
    }

    pub fn chain(self, other: Self) -> Self {
        PureErrorBuf {
            errors: self.errors.into_iter().chain(other.errors).collect(),
        }
    }

    pub fn from_many(msgs: Vec<E>, level: PureErrorLevel) -> PureErrorBuf<E> {
        PureErrorBuf {
            errors: msgs
                .into_iter()
                .map(|msg| PureError { msg, level })
                .collect(),
        }
    }

    pub fn push(&mut self, e: PureError<E>) {
        self.errors.push(e)
    }

    // TODO not DRY
    pub fn push_msg(&mut self, msg: E, level: PureErrorLevel) {
        self.push(PureError { msg, level })
    }

    pub fn push_msg_leveled(&mut self, msg: E, is_error: bool) {
        if is_error {
            self.push_error(msg);
        } else {
            self.push_warning(msg);
        }
    }

    pub fn push_error(&mut self, msg: E) {
        self.push_msg(msg, PureErrorLevel::Error)
    }

    pub fn push_warning(&mut self, msg: E) {
        self.push_msg(msg, PureErrorLevel::Warning)
    }

    pub fn has_errors(&self) -> bool {
        self.errors
            .iter()
            .filter(|e| e.level == PureErrorLevel::Error)
            .count()
            > 0
    }

    pub fn split(self) -> (Vec<E>, Vec<E>) {
        let (err, warn): (Vec<_>, Vec<_>) = self
            .errors
            .into_iter()
            .partition(|e| e.level == PureErrorLevel::Error);
        (
            err.into_iter().map(|e| e.msg).collect(),
            warn.into_iter().map(|e| e.msg).collect(),
        )
    }

    pub fn into_errors(self) -> Vec<E> {
        self.into_level(PureErrorLevel::Error)
    }

    pub fn into_warnings(self) -> Vec<E> {
        self.into_level(PureErrorLevel::Warning)
    }

    fn into_level(self, level: PureErrorLevel) -> Vec<E> {
        self.errors
            .into_iter()
            .filter(|e| e.level == level)
            .map(|e| e.msg)
            .collect()
    }

    pub fn mconcat(xs: Vec<Self>) -> Self {
        let errors = xs.into_iter().fold(vec![], |mut acc, mut next| {
            acc.append(&mut next.errors);
            acc
        });
        Self { errors }
    }
}

impl<X, E> From<X> for PureSuccess<X, E> {
    fn from(data: X) -> Self {
        PureSuccess {
            data,
            deferred: PureErrorBuf::default(),
        }
    }
}

impl<X, E> PureSuccess<X, E> {
    pub fn push(&mut self, e: PureError<E>) {
        self.deferred.errors.push(e)
    }

    pub fn push_msg(&mut self, msg: E, level: PureErrorLevel) {
        self.push(PureError { msg, level })
    }

    pub fn push_msg_leveled(&mut self, msg: E, is_error: bool) {
        if is_error {
            self.push_error(msg);
        } else {
            self.push_warning(msg);
        }
    }

    pub fn push_error(&mut self, msg: E) {
        self.push_msg(msg, PureErrorLevel::Error)
    }

    pub fn push_warning(&mut self, msg: E) {
        self.push_msg(msg, PureErrorLevel::Warning)
    }

    pub fn extend(&mut self, es: PureErrorBuf<E>) {
        self.deferred.errors.extend(es.errors)
    }

    pub fn map<Y, F: FnOnce(X) -> Y>(self, f: F) -> PureSuccess<Y, E> {
        let data = f(self.data);
        PureSuccess {
            data,
            deferred: self.deferred,
        }
    }

    pub fn and_then<Y, F: FnOnce(X) -> PureSuccess<Y, E>>(self, f: F) -> PureSuccess<Y, E> {
        let mut new = f(self.data);
        // TODO order?
        new.extend(self.deferred);
        new
    }

    pub fn try_map<R, Y, F>(self, f: F) -> Result<PureSuccess<Y, E>, Failure<R, E>>
    where
        F: FnOnce(X) -> Result<PureSuccess<Y, E>, Failure<R, E>>,
    {
        match f(self.data) {
            Ok(mut new) => {
                new.deferred.errors.extend(self.deferred.errors);
                Ok(new)
            }
            Err(mut err) => {
                // TODO order?
                err.deferred.errors.extend(self.deferred.errors);
                Err(err)
            }
        }
    }

    pub fn combine<Y, Z, F: FnOnce(X, Y) -> Z>(
        self,
        other: PureSuccess<Y, E>,
        f: F,
    ) -> PureSuccess<Z, E> {
        PureSuccess {
            data: f(self.data, other.data),
            deferred: self.deferred.chain(other.deferred),
        }
    }

    pub fn sequence(xs: Vec<PureSuccess<X, E>>) -> PureSuccess<Vec<X>, E> {
        let (data, es): (Vec<_>, Vec<_>) = xs.into_iter().map(|x| (x.data, x.deferred)).unzip();
        PureSuccess {
            data,
            deferred: PureErrorBuf::mconcat(es),
        }
    }

    pub fn combine3<A, B, Y, F: FnOnce(X, A, B) -> Y>(
        self,
        a: PureSuccess<A, E>,
        b: PureSuccess<B, E>,
        f: F,
    ) -> PureSuccess<Y, E> {
        PureSuccess {
            data: f(self.data, a.data, b.data),
            deferred: self.deferred.chain(a.deferred).chain(b.deferred),
        }
    }

    pub fn combine4<A, B, C, Y, F: FnOnce(X, A, B, C) -> Y>(
        self,
        a: PureSuccess<A, E>,
        b: PureSuccess<B, E>,
        c: PureSuccess<C, E>,
        f: F,
    ) -> PureSuccess<Y, E> {
        PureSuccess {
            data: f(self.data, a.data, b.data, c.data),
            deferred: self
                .deferred
                .chain(a.deferred)
                .chain(b.deferred)
                .chain(c.deferred),
        }
    }

    pub fn combine_result<R, F, Y, Z>(
        self,
        other: Result<PureSuccess<Y, E>, Failure<R, E>>,
        f: F,
    ) -> Result<PureSuccess<Z, E>, Failure<R, E>>
    where
        F: FnOnce(X, Y) -> Z,
    {
        match other {
            Ok(pass) => Ok(self.combine(pass, f)),
            Err(mut fail) => {
                fail.extend(self.deferred);
                Err(fail)
            }
        }
    }

    pub fn combine_some_result<R, F, Y, Z>(
        self,
        other: Result<Y, Failure<R, E>>,
        f: F,
    ) -> Result<PureSuccess<Z, E>, Failure<R, E>>
    where
        F: FnOnce(X, Y) -> Z,
    {
        match other {
            Ok(pass) => Ok(PureSuccess {
                data: f(self.data, pass),
                deferred: self.deferred,
            }),
            Err(mut fail) => {
                fail.extend(self.deferred);
                Err(fail)
            }
        }
    }
}

// impl<X, E> PureMaybe<X, E> {
//     pub fn empty() -> PureMaybe<X, E> {
//         PureSuccess::from(None)
//     }

//     pub fn map_maybe<Y, F: FnOnce(X) -> Y>(self, f: F) -> PureMaybe<Y, E> {
//         self.map(|x| x.map(f))
//     }

//     pub fn into_result(self, reason: String) -> PureResult<X, E> {
//         if let Some(d) = self.data {
//             Ok(PureSuccess {
//                 data: d,
//                 deferred: self.deferred,
//             })
//         } else {
//             Err(PureFailure {
//                 reason,
//                 deferred: self.deferred,
//             })
//         }
//     }

//     pub fn from_result_1(res: Result<X, E>, level: PureErrorLevel) -> Self {
//         match res {
//             Ok(data) => PureSuccess::from(Some(data)),
//             Err(msg) => PureSuccess {
//                 data: None,
//                 deferred: PureErrorBuf::from(msg, level),
//             },
//         }
//     }

//     pub fn from_result(res: Result<X, PureErrorBuf<E>>) -> Self {
//         match res {
//             Ok(data) => PureSuccess::from(Some(data)),
//             Err(deferred) => PureSuccess {
//                 data: None,
//                 deferred,
//             },
//         }
//     }

//     pub fn from_result_strs(res: Result<X, Vec<E>>, level: PureErrorLevel) -> Self {
//         match res {
//             Ok(data) => PureSuccess::from(Some(data)),
//             Err(msgs) => PureSuccess {
//                 data: None,
//                 deferred: PureErrorBuf::from_many(msgs, level),
//             },
//         }
//     }

//     pub fn from_result_errors(res: Result<X, Vec<E>>) -> Self {
//         Self::from_result_strs(res, PureErrorLevel::Error)
//     }

//     pub fn and_then_opt<Y, F: FnOnce(X) -> PureMaybe<Y, E>>(self, f: F) -> PureMaybe<Y, E> {
//         match self.data {
//             Some(d) => {
//                 let mut new = f(d);
//                 // TODO order?
//                 new.extend(self.deferred);
//                 new
//             }
//             None => PureSuccess {
//                 data: None,
//                 deferred: self.deferred,
//             },
//         }
//     }
// }

impl<E> From<PureFailure<E>> for ImpureFailure<E> {
    fn from(value: PureFailure<E>) -> Self {
        value.map(ImpureError::Pure)
    }
}

impl<E> From<io::Error> for ImpureFailure<E> {
    fn from(value: io::Error) -> Self {
        Failure::new(ImpureError::IO(value))
    }
}

impl<E> From<io::Error> for ImpureErrorInner<E> {
    fn from(value: io::Error) -> Self {
        ImpureErrorInner::IO(value)
    }
}
