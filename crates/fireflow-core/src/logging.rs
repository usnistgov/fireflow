//! A flexible handler for warnings and errors. (not used publicly)
//!
//! This is predicated on the following needs:
//!
//! 1. We need to handle entire groups of errors all at once (rather than return
//!    the first encountered error)
//! 2. Dynamic dispatch is yucky and evil, therefore we need a way to group
//!    errors efficiently into enums.
//! 3. Some warnings and errors should be interchangable depending on config.
//! 4. Error type and cardinality should be obvious based on the type
//! 5. Invalid and nonsensible logging states should be made impossible, which
//!    in turn will guide the happy path to only permit sane operations
//! 6. IO errors are special and should short-circuit execution no matter what,
//!    which is different from non-IO errors which can be collected and returned
//!    as a group.
//!
//! For 6. this is not generally true of all code, but here it can be assumed
//! because IO errors are going to depend on a state which will likely not
//! change within the execution sequence of any given code path. For instance,
//! an IO error may be thrown if a file is unreadable. There is only one file
//! being read in this case and if it is not readable for one function this will
//! likely be true for all. This is not true in general for all code because
//! some code can read multiple files.
//!
//! This simplification allows IO errors to be stored and thrown almost
//! independently of other errors. In Haskell terms, this can be thought of
//! like a transformer stack where pure errors are handled on one layer and
//! an IO error is handled on a different layer.

use crate::config::{ErrorFlag, ReadSharedConfig, TriErrorFlag};
use crate::text::optional::Nothing;

use type_families::{
    ApplyOnce, Functor, FunctorOnce, IsKind1, IsKind2, Kind1, Kind2, Monoid, Pointed, Semigroup,
    Sibling1, impl_kind1,
};

use derive_new::new;
use nonempty::NonEmpty;
use std::convert::Infallible;
use std::fmt;
use std::io::Error as IOError;
use std::iter;
use std::marker::PhantomData;
use std::vec;
use thiserror::Error;

#[cfg(feature = "python")]
use fireflow_core_proc::AllIntoPyErr;

//
// Group Results to be used at library boundaries
//

pub type WarningsAndIOGroupResult<V, W, E, G> =
    WarningsAndErrorResult<V, (), W, IOErrorGroup<E, G>>;

pub type WarningAndIOGroupResult<V, W, E, G> = WarningAndErrorResult<V, (), W, IOErrorGroup<E, G>>;

pub type WarningsAndGroupResult<V, W, E, S> = WarningsAndErrorResult<V, (), W, ErrorGroup<E, S>>;

pub type WarningAndGroupResult<V, W, E, S> = WarningAndErrorResult<V, (), W, ErrorGroup<E, S>>;

pub type GroupResult<V, E, S> = Result<V, ErrorGroup<E, S>>;

pub type IOGroupResult<V, E, G> = Result<V, IOErrorGroup<E, G>>;

//
// Boring regular result which may have an IO error
//

pub type IOResult<T, E> = Result<T, ImpureError<E>>;

pub type WarningAndIOResult<V, W, E> = WarningAndErrorResult<V, (), W, ImpureError<E>>;
pub type WarningsAndIOResult<V, W, E> = WarningsAndErrorResult<V, (), W, ImpureError<E>>;

//
// Results with only warnings
//

pub type WarningsResult<V, W> = Success<V, (), Vec<W>>;

//
// Results without warnings
//

pub type ErrorResult<V, P, E> = NowarnResult<V, P, E, Nothing<E>>;
pub type ErrorsResult<V, P, E> = NowarnResult<V, P, E, Vec<E>>;

//
// Results with errors which can also be warnings
//

pub(crate) type SwitchableErrorResult<V, P, X, E> = SwitchableResult<V, P, X, E, Nothing<E>>;
pub(crate) type SwitchableErrorsResult<V, P, X, E> = SwitchableResult<V, P, X, E, Vec<E>>;

pub(crate) type DeferredSwitchableError<V, X, E> = SwitchableErrorResult<V, V, X, E>;
pub(crate) type DeferredSwitchableErrors<V, X, E> = SwitchableErrorsResult<V, V, X, E>;

//
// Results with warnings and errors of differing types which are not commutable
//

pub type WarningOrErrorResult<V, P, W, E> = NonCommutativeResult<V, P, Option<W>, E, Nothing<E>>;
pub type WarningsOrErrorResult<V, P, W, E> = NonCommutativeResult<V, P, Vec<W>, E, Nothing<E>>;
pub type WarningOrErrorsResult<V, P, W, E> = NonCommutativeResult<V, P, Option<W>, E, Vec<E>>;
pub type WarningsOrErrorsResult<V, P, W, E> = NonCommutativeResult<V, P, Vec<W>, E, Vec<E>>;

//
// Results with warnings and errors of differing types which are commutable
//

pub type WarningAndErrorResult<V, P, W, E> = CommutativeResult<V, P, Option<W>, E, Nothing<E>>;
pub type WarningsAndErrorResult<V, P, W, E> = CommutativeResult<V, P, Vec<W>, E, Nothing<E>>;
pub type WarningAndErrorsResult<V, P, W, E> = CommutativeResult<V, P, Option<W>, E, Vec<E>>;
pub type WarningsAndErrorsResult<V, P, W, E> = CommutativeResult<V, P, Vec<W>, E, Vec<E>>;

//
// Deferred versions of the above types (ie the value on both sides is equal)
//

pub type DeferredError<V, E> = ErrorResult<V, V, E>;
pub type DeferredErrors<V, E> = ErrorsResult<V, V, E>;

pub type DeferredWarningAndError<V, W, E> = WarningAndErrorResult<V, V, W, E>;
pub type DeferredWarningsAndError<V, W, E> = WarningsAndErrorResult<V, V, W, E>;
pub type DeferredWarningAndErrors<V, W, E> = WarningAndErrorsResult<V, V, W, E>;
pub type DeferredWarningsAndErrors<V, W, E> = WarningsAndErrorsResult<V, V, W, E>;

//
// helper types for constructing the "complete" types above
//

type NowarnResult<V, P, E, EC> = CommutativeResult<V, P, Nothing<()>, E, EC>;

type Deferred<V, WC, E, EC> = CommutativeResult<V, V, WC, E, EC>;

pub(crate) type CommutativeResult<V, P, WC, E, EC> = LogResult<V, P, WC, WC, (), E, EC>;

type NonCommutativeResult<V, P, WC, E, EC> = LogResult<V, P, WC, Nothing<()>, (), E, EC>;

type SwitchableResult<V, P, X, E, EC> =
    LogResult<V, P, <EC as SwitchableErrorContainer>::Warn, Nothing<()>, X, E, EC>;

type DeferredSwitchable<V, X, E, EC> = SwitchableResult<V, V, X, E, EC>;

type GroupLogResult<V, P, LWC, RWC, X, E, G> =
    LogResult<V, P, LWC, RWC, X, ErrorGroup<E, G>, Nothing<ErrorGroup<E, G>>>;

type IOGroupLogResult<V, P, LWC, RWC, X, E, G> =
    LogResult<V, P, LWC, RWC, X, IOErrorGroup<E, G>, Nothing<IOErrorGroup<E, G>>>;

/// A result which may have many warnings, errors, and a value on the error side.
///
/// This can be thought of like a regular [`Result`] except that the Ok side has
/// zero or more warnings in addition to the value, and the error side has
/// a value, zero or more warnings, and one or more errors. Additionally,
/// the Succ side can encode a flag for results which may be switched between
/// warnings and errors depending on configuration (ie "switchable").
///
/// This is primarily meant to deal with complex error handling involving
/// multiple errors and/or warnings that must be "collected" and returned all at
/// once.
///
/// This is highly generic to encode many cardinalities of warnings and errors.
/// The meaning of each parameter is:
///
/// * `V`: value of the success side
/// * `P`: value of the error side ("P" for "passthrough" since this is almost
///   always an upstream value that is still valid despite failure)
/// * `LWC`: "left warning container" ie a type to hold the warnings on the
///   left (Success) side
/// * `LWC`: "right warning container" ie a type to hold the warnings on the
///   right (Failure) side
/// * `X`: the flag used to switch warnings and errors
///   (or `()` if not applicable)
/// * `E`: the type of the errors on the Failure side
/// * `EC`: error container, the container for the errors on the Failure side
///
/// Note that each of the containers may hold zero or more of the thing they are
/// designed to contain. Also, the warning type is not explicitly listed
/// anywhere since this is implied by the container type (alas, there are no
/// higher kinded types in Rust...yet); however, the warning type must be the
/// same for both sides.
///
/// The number of "things" inside each container is referred to as
/// "cardinality". Cardinality may be controlled using the following types for
/// each container:
///
/// * [`Nothing<T>`]: zero warnings or one error
/// * [`Option<T>`]: zero or one warning (not applicable for errors)
/// * [`Vec<T>`]: zero or more warnings or one or more errors
///
/// ## Common patterns
///
/// Despite its generic nature, there are only a few patterns that make sense
/// for this type. These are collectively referred to here and throughout the
/// code using the following terminology:
///
/// ### Commutative: `LWC` = `RWC`
///
/// These are so named because the warning may happen (temporally) in any order
/// relative to a failure, which is reflected in the ability to store it in
/// either the failure or success side. This also implies that `X` (the flag) is
/// `()` which means that the warnings and errors are not switchable. The
/// property of commutativity also means these types can be easily combined (in
/// Haskell typeclasses, they are instances of Applicative) since Failure and
/// Success can happen in any order/combination and yet the errors and warnings
/// can still be appended to each other (this assumes that the container types
/// are appendable).
///
/// ### Nowarn: `LWC` and `RWC` are both [`Nothing<T>`]
///
/// These indicate that there are no warnings. These are also commutative.
///
/// ### Deferred: `V` = `P`
///
/// These are so named because the failure is "deferred" into the future by
/// virtue of the type being present on both sides. This means that downstream
/// code can use a plausible return value in either case. For non-switchable
/// errors (`X` = `()`), this almost always implies that the result is
/// commutative, since it only makes sense to return the same type on both sides
/// if the warnings are also the same type.
///
/// ### Switchable: `X` != `()`, `RCW` = [`Nothing<T>`], warnings and errors are the same type
///
/// Presumably `X` is a boolean flag representing an error or non-error state.
/// Furthermore, `LWC` must be in sync with `EC` in that they must have the same
/// upper bound (ie `LWC` is [`Option<E>`] if `EC` is [`Nothing<E>`]) Unlike
/// commutative errors, these cannot be combined since the value of the flag is
/// encoded at runtime and not statically at the type level. Combining such
/// types opens the possibility of combining two results with different flag
/// values, which is nonsensical and contrary to the purpose of this type (the
/// only way around this to to make Nowarn results with multiple errors and then
/// "upgrade" them to switchable results which will encode the value of the
/// flag).
///
/// ### Resolvable: `P` = `()` and `EC` = [`Nothing<()>`]
///
/// These are errors that may be returned at library boundaries. In plain
/// language, an error is resolvable if it has no passthrough value (ie an error
/// value that should be dealt with) and has only one error (which may be one
/// collection of many errors).
///
/// ## All possible types:
///
/// | N warn | N err | `LWC`           | `RWC`           | `EC`             | commutative | switchable |
/// |--------|-------|-----------------|-----------------|----------------|-------------|------------|
/// |      0 | 0-1   | [`Nothing<()>`] | [`Nothing<()>`] | [`Nothing<E>`] | X           |            |
/// |      0 | 0-inf | [`Nothing<()>`] | [`Nothing<()>`] | [`Vec<E>`]     | X           |            |
/// |    0-1 | 0-1   | [`Option<W>`]   | [`Nothing<W>`]  | [`Nothing<E>`] |             | X          |
/// |  0-inf | 0-inf | [`Vec<W>`]      | [`Nothing<W>`]  | [`Vec<E>`]     |             | X          |
/// |    0-1 | 0-1   | [`Option<W>`]   | [`Nothing<W>`]  | [`Nothing<E>`] |             |            |
/// |    0-1 | 0-inf | [`Option<W>`]   | [`Nothing<W>`]  | [`Vec<E>`]     |             |            |
/// |  0-inf | 0-1   | [`Vec<W>`]      | [`Nothing<W>`]  | [`Nothing<E>`] |             |            |
/// |  0-inf | 0-inf | [`Vec<W>`]      | [`Nothing<W>`]  | [`Vec<E>`]     |             |            |
/// |    0-1 | 0-1   | [`Option<W>`]   | [`Option<W>`]   | [`Nothing<E>`] | X           |            |
/// |    0-1 | 0-inf | [`Option<W>`]   | [`Option<W>`]   | [`Vec<E>`]     | X           |            |
/// |  0-inf | 0-1   | [`Vec<W>`]      | [`Vec<W>`]      | [`Nothing<E>`] | X           |            |
/// |  0-inf | 0-inf | [`Vec<W>`]      | [`Vec<W>`]      | [`Vec<E>`]     | X           |            |
#[derive(Debug, PartialEq)]
pub enum LogResult<V, P, LWC, RWC, X, E, EC> {
    Succ(Success<V, X, LWC>),
    Fail(Failure<P, RWC, E, EC>),
}

use LogResult::{Fail, Succ};

/// A successful computation, possibly with warnings.
///
/// This may also be used by itself to represent an "infallible result with
/// warnings".
#[derive(Debug, PartialEq, new)]
#[new(visibility = "")]
pub struct Success<V, X, WC> {
    value: V,
    flag: X,
    warnings: WC,
}

/// A failed computation, possibly with warnings, errors, and a value.
#[derive(Debug, PartialEq, new)]
#[new(visibility = "")]
pub struct Failure<P, WC, E, EC> {
    warnings: WC,
    errors: GenNonEmpty<E, EC>,
    value: P,
}

type Failure1<P, WC, E> = Failure<P, WC, E, Nothing<E>>;

#[derive(Error, Debug)]
pub enum IOErrorGroup<E, G> {
    IO(IOError, Option<ErrorGroup<E, G>>),
    Pure(ErrorGroup<E, G>),
}

impl<E, G> IOErrorGroup<E, G> {
    pub(crate) fn set_group<G0>(self, g: G0) -> IOErrorGroup<E, G0> {
        match self {
            Self::IO(i, x) => IOErrorGroup::IO(i, x.map(|y| y.set_group(g))),
            Self::Pure(x) => IOErrorGroup::Pure(x.set_group(g)),
        }
    }
}

impl<E> IOErrorGroup<E, ()> {
    pub(crate) fn new_pure_one(e: E) -> Self {
        Self::Pure(ErrorGroup::new((), GenNonEmpty::new1(e)))
    }

    pub(crate) fn deanonymize_as<G>(self, g: G) -> IOErrorGroup<E, G> {
        match self {
            Self::IO(i, e) => IOErrorGroup::IO(i, e.map(|x| x.deanonymize_as(g))),
            Self::Pure(e) => IOErrorGroup::Pure(e.deanonymize_as(g)),
        }
    }

    pub(crate) fn deanonymize<G: Default>(self) -> IOErrorGroup<E, G> {
        self.deanonymize_as(G::default())
    }
}

impl<E> Extend<E> for IOErrorGroup<E, ()> {
    fn extend<I: IntoIterator<Item = E>>(&mut self, iter: I) {
        match self {
            Self::IO(_, p) => {
                if let Some(g) = p.as_mut() {
                    g.errors.extend(iter);
                } else {
                    *p = GenNonEmpty::collect(iter).map(|ys| ErrorGroup::new((), ys));
                }
            }
            Self::Pure(p) => p.errors.extend(iter),
        }
    }
}

impl<E, G> From<IOError> for IOErrorGroup<E, G> {
    fn from(value: IOError) -> Self {
        Self::IO(value, None)
    }
}

impl<E> From<ImpureError<E>> for IOAnonErrorGroup<E> {
    fn from(value: ImpureError<E>) -> Self {
        match value {
            ImpureError::IO(e) => Self::IO(e, None),
            ImpureError::Pure(e) => Self::new_pure_one(e),
        }
    }
}

impl<E, G> fmt::Display for IOErrorGroup<E, G>
where
    ErrorGroup<E, G>: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::IO(i, x) => {
                write!(f, "IO Error: {i}")?;
                if let Some(g) = x {
                    writeln!(f)?;
                    g.fmt(f)?;
                }
                Ok(())
            }
            Self::Pure(g) => g.fmt(f),
        }
    }
}

/// A group of errors with a summary
#[derive(Debug, Error, new)]
pub struct ErrorGroup<E, G> {
    pub summary: G,
    pub errors: GenNonEmpty<E, Vec<E>>,
}

pub(crate) type AnonErrorGroup<E> = ErrorGroup<E, ()>;
pub(crate) type IOAnonErrorGroup<E> = IOErrorGroup<E, ()>;

impl<E> AnonErrorGroup<E> {
    pub(crate) fn deanonymize_as<G>(self, g: G) -> ErrorGroup<E, G> {
        ErrorGroup::new(g, self.errors)
    }
}

impl<E, G> ErrorGroup<E, G> {
    // pub(crate) fn new1(e: E) -> Self
    // where
    //     G: Default,
    // {
    //     Self::new1_with(G::default(), e)
    // }

    // pub(crate) fn new1_with(s: G, e: E) -> Self {
    //     Self::new(s, GenNonEmpty::new1(e))
    // }

    pub(crate) fn try_new(es: impl IntoIterator<Item = E>) -> Result<(), Self>
    where
        G: Default,
    {
        Self::try_new_with(G::default(), es)
    }

    pub(crate) fn try_new_with(s: G, es: impl IntoIterator<Item = E>) -> Result<(), Self> {
        GenNonEmpty::collect(es)
            .map(|xs| Self::new(s, xs))
            .map_or(Ok(()), Err)
    }

    pub(crate) fn set_group<G0>(self, g: G0) -> ErrorGroup<E, G0> {
        ErrorGroup::new(g, self.errors)
    }
}

impl<E, S> fmt::Display for ErrorGroup<E, S>
where
    E: fmt::Display,
    S: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        writeln!(f, "Error summary: {}", self.summary)?;
        let es = &self.errors;
        let mut es_it = iter::once(&es.head).chain(es.tail.iter()).peekable();
        while let Some(e) = es_it.next() {
            let s = e.to_string();
            let mut s_it = s.lines().peekable();
            while let Some(l) = s_it.next() {
                write!(f, "  {l}")?;
                if es_it.peek().is_some() || s_it.peek().is_some() {
                    writeln!(f)?;
                }
            }
        }
        Ok(())
    }
}

/// A non-empty container.
///
/// The generic sub-container `C` may hold zero or more `X` values.
#[derive(Debug, PartialEq, new)]
pub struct GenNonEmpty<X, C> {
    head: X,
    tail: C,
}

/// Either a pure error or impure (IO) error.
#[derive(Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(E: Into<pyo3::PyErr>))]
pub enum ImpureError<E> {
    #[error("IO error: {0}")]
    IO(#[from] IOError),
    #[error("{0}")]
    Pure(E),
}

impl<E> FunctorOnce<E> for ImpureError<E> {
    fn fmap_once<F: FnOnce(E) -> X, X>(self, f: F) -> ImpureError<X> {
        match self {
            Self::IO(x) => ImpureError::IO(x),
            Self::Pure(e) => ImpureError::Pure(f(e)),
        }
    }
}

impl_kind1!(pub ImpureErrorFamily, ImpureError);

/// Type for IOErrorGroup
pub struct IOErrorGroupFamily<G>(PhantomData<G>);

impl<G> Kind1 for IOErrorGroupFamily<G> {
    type Type<T> = IOErrorGroup<T, G>;
}

impl<E, G> IsKind1 for IOErrorGroup<E, G> {
    type Family = IOErrorGroupFamily<G>;
}

/// Type for ErrorGroup
pub struct ErrorGroupFamily<G>(PhantomData<G>);

impl<G> Kind1 for ErrorGroupFamily<G> {
    type Type<T> = ErrorGroup<T, G>;
}

impl<E, G> IsKind1 for ErrorGroup<E, G> {
    type Family = ErrorGroupFamily<G>;
}

/// Type family for `GenNonEmpty` where the container type is partially applied.
pub struct GenNonEmptyFamily<C>(PhantomData<C>);

impl<C: Kind1> Kind1 for GenNonEmptyFamily<C> {
    type Type<T> = GenNonEmpty<T, C::Type<T>>;
}

impl<X, C: IsKind1> IsKind1 for GenNonEmpty<X, C> {
    type Family = GenNonEmptyFamily<C::Family>;
}

/// Type family for `LogResult` where all but the value type is partially applied.
pub struct LogResultFamily<LWC, RWC, X, E, EC>(
    PhantomData<LWC>,
    PhantomData<RWC>,
    PhantomData<X>,
    PhantomData<E>,
    PhantomData<EC>,
);

impl<LWC, RWC, X, E, EC> Kind2 for LogResultFamily<LWC, RWC, X, E, EC> {
    type Type<A, B> = LogResult<A, B, LWC, RWC, X, E, EC>;
}

impl<A, B, LWC, RWC, X, E, EC> IsKind2 for LogResult<A, B, LWC, RWC, X, E, EC> {
    type Family = LogResultFamily<LWC, RWC, X, E, EC>;
}

/// Type family for Success where all but value are partially applied.
pub struct SuccessFamily<X, WC>(PhantomData<X>, PhantomData<WC>);

impl<X, WC> Kind1 for SuccessFamily<X, WC> {
    type Type<V> = Success<V, X, WC>;
}

impl<V, X, WC> IsKind1 for Success<V, X, WC> {
    type Family = SuccessFamily<X, WC>;
}

/// Type family for Failure where all but value are partially applied.
pub struct FailureFamily<WC, E, EC>(PhantomData<WC>, PhantomData<E>, PhantomData<EC>);

impl<WC, E, EC> Kind1 for FailureFamily<WC, E, EC> {
    type Type<P> = Failure<P, WC, E, EC>;
}

impl<V, WC, E, EC> IsKind1 for Failure<V, WC, E, EC> {
    type Family = FailureFamily<WC, E, EC>;
}

/// Type family for [`LogResult`] instances where are commutative and deferred.
///
/// This is useful for implementing Applicative instances for these.
pub struct DeferredFamily<WC, E, EC>(PhantomData<WC>, PhantomData<E>, PhantomData<EC>);

impl<WC, E, EC> Kind1 for DeferredFamily<WC, E, EC> {
    type Type<V> = Deferred<V, WC, E, EC>;
}

impl<V, WC, E, EC> IsKind1 for Deferred<V, WC, E, EC> {
    type Family = DeferredFamily<WC, E, EC>;
}

/// Extension trait for converting [`Option<T>`] to [`LogResult`]
pub(crate) trait OptionExt: Sized {
    type Inner;

    fn into_option(self) -> Option<Self::Inner>;

    fn transpose_log_result<V, P, LWC, RWC, E, EC>(
        self,
    ) -> LogResult<Option<V>, P, LWC, RWC, (), E, EC>
    where
        Self: OptionExt<Inner = LogResult<V, P, LWC, RWC, (), E, EC>>,
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

/// Extension trait for converting [`Result<T, E>`] to [`LogResult`]
pub(crate) trait ResultExt: Sized {
    type Ok;
    type Error;

    fn into_result(self) -> Result<Self::Ok, Self::Error>;

    fn into_nowarn1(self) -> NowarnResult<Self::Ok, (), Self::Error, Nothing<Self::Error>> {
        self.into_log()
    }

    fn into_nowarn(self) -> NowarnResult<Self::Ok, (), Self::Error, Vec<Self::Error>> {
        self.into_log()
    }

    // fn into_deferred_nowarn<EC>(self) -> NowarnResult<Self::Ok, Self::Ok, Self::Error, EC>
    // where
    //     Self::Ok: Default,
    //     EC: Default,
    // {
    //     self.into_log().set_err_value(Self::Ok::default())
    // }

    fn into_log<LWC, RWC, EC>(self) -> LogResult<Self::Ok, (), LWC, RWC, (), Self::Error, EC>
    where
        EC: Default,
        LWC: Default,
        RWC: Default,
    {
        self.into_result().map_or_else(
            |e| Fail(Failure::new_from_one(e, ())),
            |s| Succ(Success::new_non_switchable(s)),
        )
    }

    fn into_deferred_switchable<X, EC>(
        self,
        flag: X,
    ) -> DeferredSwitchable<Self::Ok, X, Self::Error, EC>
    where
        Self::Ok: Default,
        EC: SwitchableErrorContainer<Inner = Self::Error> + Default,
        EC::Warn: Default,
        X: ErrorFlag,
    {
        match self.into_result() {
            Ok(s) => Succ(Success::new_flagged(s, flag)),
            Err(e) => {
                if flag.is_error() {
                    Fail(Failure::new_from_one(e, Self::Ok::default()))
                } else {
                    let ws = EC::error_to_warning(e);
                    Succ(Success::new(Self::Ok::default(), flag, ws))
                }
            }
        }
    }

    fn into_deferred_switchable3<X, EC>(
        self,
        flag: X,
    ) -> DeferredSwitchable<Self::Ok, X, Self::Error, EC>
    where
        Self::Ok: Default,
        EC: SwitchableErrorContainer<Inner = Self::Error> + Default,
        EC::Warn: Default,
        X: TriErrorFlag,
    {
        match self.into_result() {
            Ok(s) => Succ(Success::new_flagged(s, flag)),
            Err(e) => match flag.is_error() {
                None => Succ(Success::new_flagged(Self::Ok::default(), flag)),
                Some(true) => Fail(Failure::new_from_one(e, Self::Ok::default())),
                Some(false) => {
                    let ws = EC::error_to_warning(e);
                    Succ(Success::new(Self::Ok::default(), flag, ws))
                }
            },
        }
    }

    // fn into_deferred_switchable_opt<X, EC>(
    //     self,
    //     flag: X,
    // ) -> DeferredSwitchable<Option<Self::Ok>, X, Self::Error, EC>
    // where
    //     EC: SwitchableErrorContainer<Inner = Self::Error> + Default,
    //     EC::Warn: Default,
    //     X: ErrorFlag,
    // {
    //     self.into_result().map(Some).into_deferred_switchable(flag)
    // }

    fn into_deferred_switchable_opt3<X, EC>(
        self,
        flag: X,
    ) -> DeferredSwitchable<Option<Self::Ok>, X, Self::Error, EC>
    where
        EC: SwitchableErrorContainer<Inner = Self::Error> + Default,
        EC::Warn: Default,
        X: TriErrorFlag,
    {
        self.into_result().map(Some).into_deferred_switchable3(flag)
    }

    fn into_succ<LWC>(self) -> Success<Self::Ok, (), LWC>
    where
        Self::Ok: Default,
        LWC: Default + Pointed<Self::Error>,
    {
        self.into_result().map_or_else(
            |e| Success::new(Self::Ok::default(), (), LWC::wrap(e)),
            Success::new_non_switchable,
        )
    }

    fn into_succ_opt<LWC>(self) -> Success<Option<Self::Ok>, (), LWC>
    where
        LWC: Default + Pointed<Self::Error>,
    {
        self.into_result().map(Some).into_succ()
    }

    fn into_succ_or<LWC>(self, default: Self::Ok) -> Success<Self::Ok, (), LWC>
    where
        LWC: Pointed<Self::Error> + Default,
    {
        self.into_succ_opt().fmap_once(|x| x.unwrap_or(default))
    }

    fn infallible_err_into<E>(self) -> Option<E>
    where
        Self: ResultExt<Ok = (), Error = Infallible>,
    {
        None
    }

    fn unwrap_infallible(self) -> Self::Ok
    where
        Self: ResultExt<Error = Infallible>,
    {
        let Ok(x) = self.into_result();
        x
    }

    #[allow(clippy::type_complexity)]
    fn zip<V, LWC, RWC>(
        self,
        a: Result<V, Self::Error>,
    ) -> LogResult<(Self::Ok, V), (), LWC, RWC, (), Self::Error, Vec<Self::Error>>
    where
        LWC: Default,
        RWC: Default,
    {
        match (self.into_result(), a) {
            (Ok(x0), Ok(x1)) => LogResult::new_ok((x0, x1)),
            (Ok(_), Err(e)) | (Err(e), Ok(_)) => LogResult::new_err(e),
            (Err(e0), Err(e1)) => {
                let mut ret = Failure::new_from_one(e0, ());
                ret.extend_errors(iter::once(e1));
                Fail(ret)
            }
        }
    }

    fn ungroup<E>(self) -> ErrorsResult<Self::Ok, (), E>
    where
        Self: ResultExt<Error = AnonErrorGroup<E>>,
    {
        match self.into_result() {
            Ok(x) => LogResult::new_ok(x),
            Err(g) => Fail(Failure::new_from_many(g.errors, ())),
        }
    }
}

impl<V, E> ResultExt for Result<V, E> {
    type Ok = V;
    type Error = E;

    fn into_result(self) -> Self {
        self
    }
}

/// Combine successes.
///
/// This is effectively the `sequence` function from Haskell's Data.Traversable
/// where the Traversible is an iterator and `Success` forms a Monad.
pub(crate) trait SuccessResultIter<V, WC>:
    Iterator<Item = Success<V, (), WC>> + Sized
{
    fn sequence_success(self) -> Success<Vec<V>, (), WC>
    where
        WC: Monoid,
    {
        let mut xs = vec![];
        let mut ws = WC::default();
        for x in self {
            xs.push(x.value);
            ws = ws.mappend(x.warnings);
        }
        Success::new_non_switchable(xs).set_warnings(ws)
    }
}

impl<I, V, WC> SuccessResultIter<V, WC> for I where I: Iterator<Item = Success<V, (), WC>> {}

/// Combine commutative results.
///
/// Ok values will be collected and returned as a single vector upon success.
/// Presence of any Error will cause Error to be returned. In any case,
/// warnings and errors as applicable will appended in order and returned.
///
/// The wrapper for warning must be a semigroup and wrapper for error must be
/// extendable since it might hold more than one error.
///
/// This is kinda like the `sequence_` function from Haskell's Data.Foldable
/// where the foldable in this case is an iterator, except that commutative
/// `LogResult` types in this case are not a perfect Monad.
pub(crate) trait CommutativeResultIter<T, P, WC, E, EC>:
    Iterator<Item = CommutativeResult<T, P, WC, E, EC>> + Sized
{
    fn sequence_commutative(mut self) -> CommutativeResult<Vec<T>, (), WC, E, EC>
    where
        WC: Monoid,
        EC: Extend<E> + IntoIterator<Item = E>,
    {
        let mut left_vs = vec![];
        let mut ws = WC::default();
        let mut error_head = None;
        for x in self.by_ref() {
            match x {
                Succ(y) => {
                    left_vs.push(y.value);
                    ws = ws.sappend(y.warnings);
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
                        ws = ws.sappend(y.warnings);
                    }
                    Fail(y) => {
                        ws = ws.sappend(y.warnings);
                        es.extend(y.errors);
                    }
                }
            }
            Fail(Failure::new(ws, es, ()))
        } else {
            Succ(Success::new(left_vs, (), ws))
        }
    }
}

impl<I, V, P, WC, E, EC> CommutativeResultIter<V, P, WC, E, EC> for I where
    I: Iterator<Item = CommutativeResult<V, P, WC, E, EC>>
{
}

/// Combine deferred results.
///
/// Values from Ok or Error will be collected and returned in a single vector
/// independent of the presence of warnings or errors.
///
/// The wrapper for warning must be a semigroup and wrapper for error must be
/// extendable since it might hold more than one error.
///
/// This is effectively the `sequence` function from Haskell's Data.Traversable
/// where the Traversible is an iterator and `LogResult` forms a Monad.
pub(crate) trait DeferredIter<T, WC, E, EC>:
    Iterator<Item = Deferred<T, WC, E, EC>> + Sized
{
    fn sequence_def(mut self) -> Deferred<Vec<T>, WC, E, EC>
    where
        WC: Monoid,
        EC: Extend<E> + IntoIterator<Item = E>,
    {
        let mut vs = vec![];
        let mut ws = WC::default();
        let mut error_head = None;
        for x in self.by_ref() {
            match x {
                Succ(y) => {
                    vs.push(y.value);
                    ws = ws.sappend(y.warnings);
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
                        ws = ws.sappend(y.warnings);
                    }
                    Fail(y) => {
                        vs.push(y.value);
                        ws = ws.sappend(y.warnings);
                        es.extend(y.errors);
                    }
                }
            }
            Fail(Failure::new(ws, es, vs))
        } else {
            Succ(Success::new(vs, (), ws))
        }
    }

    fn sequence_def_void(self) -> Deferred<(), WC, E, EC>
    where
        WC: Monoid,
        EC: Extend<E> + IntoIterator<Item = E>,
    {
        self.sequence_def().set_deferred_value(())
    }
}

impl<I, V, WC, E, EC> DeferredIter<V, WC, E, EC> for I where
    I: Iterator<Item = Deferred<V, WC, E, EC>>
{
}

/// A constraint relating error and warning containers for switchable errors.
// TODO this doesn't need to be public
pub trait SwitchableErrorContainer: Sized {
    type Inner;
    type Warn: Pointed<Self::Inner>;

    fn errors_to_warnings(errors: GenNonEmpty<Self::Inner, Self>) -> Self::Warn;

    fn error_to_warning(error: Self::Inner) -> Self::Warn {
        Self::Warn::wrap(error)
    }
}

/// A type which may be converted to a new cardinality (either the same or bigger).
pub trait IntoNewCardinality<Other> {
    fn into_new_cardinality(self) -> Other;
}

impl<A, C: Functor<A>> Functor<A> for GenNonEmpty<A, C> {
    fn fmap<F: FnMut(A) -> B, B>(self, mut f: F) -> Sibling1<Self, B> {
        GenNonEmpty::new(f(self.head), self.tail.fmap(f))
    }
}

impl<A, G> Functor<A> for IOErrorGroup<A, G> {
    fn fmap<F: FnMut(A) -> B, B>(self, f: F) -> Sibling1<Self, B> {
        match self {
            Self::IO(i, g) => IOErrorGroup::IO(i, g.map(|x| x.fmap(f))),
            Self::Pure(g) => IOErrorGroup::Pure(g.fmap(f)),
        }
    }
}

impl<A, G> Functor<A> for ErrorGroup<A, G> {
    fn fmap<F: FnMut(A) -> B, B>(self, f: F) -> Sibling1<Self, B> {
        ErrorGroup::new(self.summary, self.errors.fmap(f))
    }
}

impl<E, EC> IntoIterator for GenNonEmpty<E, EC>
where
    EC: IntoIterator<Item = E>,
{
    type Item = E;
    type IntoIter = iter::Chain<iter::Once<E>, <EC as IntoIterator>::IntoIter>;
    fn into_iter(self) -> Self::IntoIter {
        iter::once(self.head).chain(self.tail)
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

impl<A, T: IsKind1 + Default> Pointed<A> for GenNonEmpty<A, T> {
    fn wrap(a: A) -> Self {
        Self::new(a, T::default())
    }
}

impl<E> SwitchableErrorContainer for Nothing<E> {
    type Inner = E;
    type Warn = Option<E>;

    fn errors_to_warnings(errors: GenNonEmpty<E, Self>) -> Self::Warn {
        Some(errors.head)
    }

    fn error_to_warning(error: E) -> Self::Warn {
        Some(error)
    }
}

impl<E> SwitchableErrorContainer for Vec<E> {
    type Inner = E;
    type Warn = Self;

    fn errors_to_warnings(errors: GenNonEmpty<E, Self>) -> Self::Warn {
        errors.into_iter().collect()
    }

    fn error_to_warning(error: E) -> Self::Warn {
        vec![error]
    }
}

impl<V, X, WC> FunctorOnce<V> for Success<V, X, WC> {
    fn fmap_once<F: FnOnce(V) -> Y, Y>(self, f: F) -> Sibling1<Self, Y> {
        Success::new(f(self.value), self.flag, self.warnings)
    }
}

impl<V, WC, E, EC> FunctorOnce<V> for Failure<V, WC, E, EC> {
    fn fmap_once<F: FnOnce(V) -> Y, Y>(self, f: F) -> Sibling1<Self, Y> {
        Failure::new(self.warnings, self.errors, f(self.value))
    }
}

impl<V, X, WC: Semigroup> ApplyOnce<V> for Success<V, X, WC> {
    fn lift_f2_once<F, B, C>(self, other: Sibling1<Self, B>, f: F) -> Sibling1<Self, C>
    where
        F: FnOnce(V, B) -> C,
    {
        Success::new(
            f(self.value, other.value),
            self.flag,
            self.warnings.sappend(other.warnings),
        )
    }
}

impl<V, WC, E, EC> ApplyOnce<V> for Failure<V, WC, E, EC>
where
    WC: Semigroup,
    EC: Extend<E> + IntoIterator<Item = E>,
{
    fn lift_f2_once<F, B, C>(mut self, other: Sibling1<Self, B>, f: F) -> Sibling1<Self, C>
    where
        F: FnOnce(V, B) -> C,
    {
        let ws = self.warnings.sappend(other.warnings);
        self.errors.extend(other.errors);
        Failure::new(ws, self.errors, f(self.value, other.value))
    }
}

impl<V, P, LWC, RWC, X, E, G> From<IOError>
    for LogResult<V, P, LWC, RWC, X, IOErrorGroup<E, G>, Nothing<IOErrorGroup<E, G>>>
where
    RWC: Default,
    P: Default,
{
    fn from(value: IOError) -> Self {
        let e = IOErrorGroup::from(value);
        Fail(Failure::new_from_one(e, P::default()))
    }
}

impl<V, WC, E, EC> FunctorOnce<V> for Deferred<V, WC, E, EC> {
    fn fmap_once<F: FnOnce(V) -> Y, Y>(self, f: F) -> Sibling1<Self, Y> {
        match self {
            Succ(s) => Succ(s.fmap_once(f)),
            Fail(s) => Fail(s.fmap_once(f)),
        }
    }
}

impl<V, WC, E, EC> ApplyOnce<V> for Deferred<V, WC, E, EC>
where
    WC: Monoid,
    EC: Extend<E> + IntoIterator<Item = E>,
{
    fn lift_f2_once<F, V0, Vf>(self, other: Sibling1<Self, V0>, f: F) -> Sibling1<Self, Vf>
    where
        F: FnOnce(V, V0) -> Vf,
    {
        match (self, other) {
            (Succ(ax), Succ(bx)) => Succ(ax.lift_f2_once(bx, f)),
            (Succ(ax), Fail(bx)) => Fail(ax.with_failure(bx, f)),
            (Fail(ax), Succ(bx)) => Fail(ax.with_success(bx, f)),
            (Fail(ax), Fail(bx)) => Fail(ax.lift_f2_once(bx, f)),
        }
    }
}

impl<E, C> From<(E, C)> for GenNonEmpty<E, C> {
    fn from(value: (E, C)) -> Self {
        Self::new(value.0, value.1)
    }
}

impl<E> From<GenNonEmpty<E, Vec<E>>> for NonEmpty<E> {
    fn from(value: GenNonEmpty<E, Vec<E>>) -> Self {
        Self::from((value.head, value.tail))
    }
}

impl<X, C> Extend<X> for GenNonEmpty<X, C>
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

impl<X, C> GenNonEmpty<X, C> {
    fn collect(xs: impl IntoIterator<Item = X>) -> Option<Self>
    where
        C: Extend<X> + Default,
    {
        let mut it = xs.into_iter();
        it.by_ref().next().map(|x0| {
            let mut ret = Self::new1(x0);
            ret.extend(it);
            ret
        })
    }

    fn new1(x: X) -> Self
    where
        C: Default,
    {
        Self::new(x, C::default())
    }

    fn repack<Cf>(self) -> GenNonEmpty<X, Cf>
    where
        C: IntoNewCardinality<Cf>,
    {
        GenNonEmpty::new(self.head, self.tail.into_new_cardinality())
    }
}

//
// Non-switchable Success
//
impl<V, WC> Success<V, (), WC> {
    pub fn new_non_switchable(value: V) -> Self
    where
        WC: Default,
    {
        Self::new(value, (), WC::default())
    }

    fn set_flag<X>(self, flag: X) -> Success<V, X, WC> {
        Success::new(self.value, flag, self.warnings)
    }
}

//
// Nowarn Success
//
impl<V> Success<V, (), Nothing<()>> {
    fn nowarn_into_warn<WC: Default>(self) -> Success<V, (), WC> {
        Success::new_non_switchable(self.value)
    }

    pub(crate) fn set_warnings<WC>(self, ws: WC) -> Success<V, (), WC> {
        Success::new(self.value, (), ws)
    }

    pub(crate) fn nowarn_with_log<F, Vf, P, LWC, RWC, X, E, EC>(
        self,
        f: F,
    ) -> LogResult<Vf, P, LWC, RWC, X, E, EC>
    where
        F: FnOnce(V) -> LogResult<Vf, P, LWC, RWC, X, E, EC>,
    {
        match f(self.value) {
            Succ(s) => Succ(s),
            Fail(e) => Fail(Failure::new(e.warnings, e.errors, e.value)),
        }
    }
}

//
// Fully-generic Success
//
impl<V, X, WC> Success<V, X, WC> {
    pub fn new_flagged(value: V, flag: X) -> Self
    where
        WC: Default,
    {
        Self::new(value, flag, WC::default())
    }

    pub fn into_log<P, RWC, E, EC>(self) -> LogResult<V, P, WC, RWC, X, E, EC> {
        Succ(self)
    }

    fn remove_flag(self) -> Success<V, (), WC> {
        Success::new(self.value, (), self.warnings)
    }

    pub(crate) fn repack<WCf>(self) -> Success<V, X, WCf>
    where
        WC: IntoNewCardinality<WCf>,
    {
        Success::new(self.value, self.flag, self.warnings.into_new_cardinality())
    }

    pub(crate) fn map_warnings<F, W, Wf>(self, f: F) -> Success<V, X, Sibling1<WC, Wf>>
    where
        WC: Functor<W>,
        F: Fn(W) -> Wf,
    {
        Success::new(self.value, self.flag, self.warnings.fmap(f))
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

    pub(crate) fn with_log<F, Vf, P, E, EC>(self, f: F) -> CommutativeResult<Vf, P, WC, E, EC>
    where
        F: FnOnce(V) -> CommutativeResult<Vf, P, WC, E, EC>,
        WC: Semigroup,
    {
        match f(self.value) {
            Succ(s) => {
                let ws = self.warnings.sappend(s.warnings);
                Succ(Success::new(s.value, (), ws))
            }
            Fail(e) => {
                let ws = self.warnings.sappend(e.warnings);
                Fail(Failure::new(ws, e.errors, e.value))
            }
        }
    }

    // fn with_log_nowarn<F, Vf, Pf, E, EC>(self, f: F) -> CommutativeResult<Vf, Pf, WC, E, EC>
    // where
    //     F: FnOnce(V) -> NowarnResult<Vf, Pf, E, EC>,
    // {
    //     match f(self.value) {
    //         Succ(s) => Succ(Success::new(s.value, (), self.warnings)),
    //         Fail(e) => Fail(Failure::new(self.warnings, e.errors, e.value)),
    //     }
    // }

    pub(crate) fn with_failure<F, P, Pf, E, EC>(
        self,
        other: Failure<P, WC, E, EC>,
        f: F,
    ) -> Failure<Pf, WC, E, EC>
    where
        F: FnOnce(V, P) -> Pf,
        WC: Semigroup,
    {
        let ws = self.warnings.sappend(other.warnings);
        Failure::new(ws, other.errors, f(self.value, other.value))
    }

    pub(crate) fn fail<E, EC>(self, errors: GenNonEmpty<E, EC>) -> Failure<V, WC, E, EC> {
        Failure::new(self.warnings, errors, self.value)
    }

    /// Remove warnings while maintaining the warning type.
    ///
    /// This is useful for cases where warnings might be optionally removed
    /// so we can't just set them to `()`
    fn remove_warnings(self) -> Self
    where
        WC: Default,
    {
        Self::new_flagged(self.value, self.flag)
    }

    /// Convert warnings to errors while maintaining the warning type.
    ///
    /// This is useful for cases where warnings might be optionally converted
    /// so we can't just set them to `()`
    fn warnings_to_pure_errors<E, P, W, Fw, Fp>(
        self,
        fw: Fw,
        fp: Fp,
    ) -> IOGroupLogResult<V, P, WC, WC, X, E, ()>
    where
        Fw: Fn(W) -> E,
        Fp: FnOnce(V) -> P,
        WC: Default + IntoIterator<Item = W>,
    {
        match GenNonEmpty::<E, Vec<E>>::collect(self.warnings.into_iter().map(fw)) {
            None => Succ(Self::new_flagged(self.value, self.flag)),
            Some(es) => {
                let e = IOErrorGroup::Pure(ErrorGroup::new((), es));
                Fail(Failure::new_from_one(e, fp(self.value)))
            }
        }
    }

    pub fn resolve<F, Wres>(self, f: F) -> (V, Wres)
    where
        F: FnOnce(WC) -> Wres,
    {
        (self.value, f(self.warnings))
    }
}

//
// Nowarn Failure
//
impl<P, E, EC> Failure<P, Nothing<()>, E, EC> {
    fn nowarn_into_warn<WC>(self) -> Failure<P, WC, E, EC>
    where
        WC: Default,
    {
        Failure::new_from_many(self.errors, self.value)
    }

    fn set_warnings<WCf>(self, ws: WCf) -> Failure<P, WCf, E, EC> {
        Failure::new(ws, self.errors, self.value)
    }

    // fn nowarn_and_log<Fe, Fp, V, Pf, WC>(mut self, fp: Fp, fe: Fe) -> Failure<Pf, WC, E, EC>
    // where
    //     Fe: FnOnce(P) -> CommutativeResult<V, Pf, WC, E, EC>,
    //     Fp: FnOnce(V) -> Pf,
    //     EC: Extend<E> + IntoIterator<Item = E>,
    // {
    //     match fe(self.value) {
    //         Succ(s) => Failure::new(s.warnings, self.errors, fp(s.value)),
    //         Fail(e) => {
    //             self.errors.extend(e.errors);
    //             Failure::new(e.warnings, self.errors, e.value)
    //         }
    //     }
    // }
}

//
// Failure with single error
//
impl<P, WC, E> Failure<P, WC, E, Nothing<E>> {
    pub(crate) fn map_error<F, Ef>(self, f: F) -> Failure<P, WC, Ef, Nothing<Ef>>
    where
        F: FnOnce(E) -> Ef,
    {
        let n = GenNonEmpty::new1(f(self.errors.head));
        Failure::new(self.warnings, n, self.value)
    }
}

//
// Failure with error group
//
// impl<P, WC, E, G> Failure<P, WC, ErrorGroup<E, G>, Nothing<ErrorGroup<E, G>>> {
//     fn ungroup(self) -> Failure<P, WC, E, Vec<E>> {
//         Failure::new(self.warnings, self.errors.head.errors, self.value)
//     }
// }

//
// Failure with Anon IO error group
//
impl<P, WC, E> Failure1<P, WC, IOAnonErrorGroup<E>> {
    /// Convert warnings to non-IO errors while maintaining the warning type.
    ///
    /// Useful at code boundaries where we may want to upgrade warnings to
    /// errors based on what the user wants a given function to do.
    fn warnings_to_pure_errors<W, F>(mut self, f: F) -> Self
    where
        F: Fn(W) -> E,
        WC: IntoIterator<Item = W> + Default,
    {
        self.errors.head.extend(self.warnings.into_iter().map(f));
        Self::new_from_many(self.errors, self.value)
    }
}

//
// Fully generic Failure
//
impl<P, E, WC, EC> Failure<P, WC, E, EC> {
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

    fn repack_warnings<WCf>(self) -> Failure<P, WCf, E, EC>
    where
        WC: IntoNewCardinality<WCf>,
    {
        Failure::new(
            WC::into_new_cardinality(self.warnings),
            self.errors,
            self.value,
        )
    }

    fn repack_errors<ECf>(self) -> Failure<P, WC, E, ECf>
    where
        EC: IntoNewCardinality<ECf>,
    {
        Failure::new(self.warnings, self.errors.repack(), self.value)
    }

    fn map_warnings<F, W, Wf>(self, f: F) -> Failure<P, Sibling1<WC, Wf>, E, EC>
    where
        F: Fn(W) -> Wf,
        WC: Functor<W>,
    {
        Failure::new(self.warnings.fmap(f), self.errors, self.value)
    }

    pub(crate) fn map_errors<F, Ef>(self, f: F) -> Failure<P, WC, Ef, Sibling1<EC, Ef>>
    where
        F: Fn(E) -> Ef,
        EC: Functor<E>,
    {
        Failure::new(self.warnings, self.errors.fmap(f), self.value)
    }

    // fn push_warning<W>(&mut self, w: W)
    // where
    //     WC: Extend<W>,
    // {
    //     self.warnings.extend(iter::once(w));
    // }

    // fn eval_warning<F, W>(&mut self, f: F)
    // where
    //     F: FnOnce(&P) -> Option<W>,
    //     WC: Extend<W>,
    // {
    //     if let Some(e) = f(&self.value) {
    //         self.push_warning(e);
    //     }
    // }

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

    fn with_log<Fr, Fp, V, Pf>(mut self, fp: Fp, fr: Fr) -> Failure<Pf, WC, E, EC>
    where
        Fr: FnOnce(P) -> CommutativeResult<V, Pf, WC, E, EC>,
        Fp: FnOnce(V) -> Pf,
        WC: Semigroup,
        EC: Extend<E> + IntoIterator<Item = E>,
    {
        match fr(self.value) {
            Succ(s) => {
                let ws = self.warnings.sappend(s.warnings);
                Failure::new(ws, self.errors, fp(s.value))
            }
            Fail(e) => {
                let ws = self.warnings.sappend(e.warnings);
                self.errors.extend(e.errors);
                Failure::new(ws, self.errors, e.value)
            }
        }
    }

    fn with_log_nowarn<Fr, Fp, V, Pf>(mut self, fp: Fp, fr: Fr) -> Failure<Pf, WC, E, EC>
    where
        Fr: FnOnce(P) -> NowarnResult<V, Pf, E, EC>,
        Fp: FnOnce(V) -> Pf,
        EC: Extend<E> + IntoIterator<Item = E>,
    {
        match fr(self.value) {
            Succ(s) => Failure::new(self.warnings, self.errors, fp(s.value)),
            Fail(e) => {
                self.errors.extend(e.errors);
                Failure::new(self.warnings, self.errors, e.value)
            }
        }
    }

    fn with_success<F, V, X, PF>(self, other: Success<V, X, WC>, f: F) -> Failure<PF, WC, E, EC>
    where
        F: FnOnce(P, V) -> PF,
        WC: Monoid,
    {
        let ws = self.warnings.mappend(other.warnings);
        Failure::new(ws, self.errors, f(self.value, other.value))
    }

    fn aggregate_errors<F, Ef>(self, f: F) -> Failure<P, WC, Ef, Nothing<Ef>>
    where
        F: FnOnce(GenNonEmpty<E, EC>) -> Ef,
    {
        let es = GenNonEmpty::new1(f(self.errors));
        Failure::new(self.warnings, es, self.value)
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
}

//
// Commutative LogResult
//
impl<V, P, WC, E, EC> CommutativeResult<V, P, WC, E, EC> {
    pub(crate) fn repack_warnings<WCf>(self) -> CommutativeResult<V, P, WCf, E, EC>
    where
        WC: IntoNewCardinality<WCf>,
    {
        self.map(Success::repack).map_err(Failure::repack_warnings)
    }

    /// Map function over warnings of commutative Result
    pub(crate) fn map_commutative_warnings<F, W, Wf>(
        self,
        f: F,
    ) -> CommutativeResult<V, P, Sibling1<WC, Wf>, E, EC>
    where
        F: Fn(W) -> Wf,
        WC: Functor<W>,
    {
        self.map(|s| s.map_warnings(&f))
            .map_err(|e| e.map_warnings(f))
    }

    /// Map function over warnings of commutative Result
    pub(crate) fn map_warnings_and_errors<F, Ef>(
        self,
        f: F,
    ) -> CommutativeResult<V, P, Sibling1<WC, Ef>, Ef, Sibling1<EC, Ef>>
    where
        F: Fn(E) -> Ef,
        WC: Functor<E>,
        EC: Functor<E>,
    {
        self.map_commutative_warnings(&f).map_errors(f)
    }

    /// Add warnings to a commutative Result.
    pub(crate) fn extend_commutative_warnings<W>(&mut self, ws: impl IntoIterator<Item = W>)
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
    pub(crate) fn eval_error<Pf, Fe, Fv, Fp>(
        self,
        fv: Fv,
        fp: Fp,
        fe: Fe,
    ) -> CommutativeResult<V, Pf, WC, E, EC>
    where
        Fe: FnOnce(&V) -> Option<E>,
        Fv: FnOnce(V) -> Pf,
        Fp: FnOnce(P) -> Pf,
        EC: Extend<E> + Default,
    {
        match self {
            Succ(x) => match fe(&x.value) {
                Some(e) => Fail(x.fail(GenNonEmpty::new1(e)).fmap_once(fv)),
                None => Succ(x),
            },
            Fail(x) => Fail(x.fmap_once(fp)),
        }
    }

    // #[allow(clippy::needless_pass_by_value)]
    // pub(crate) fn eval_warning_or_error<Pf, Fe, Fv, Fp, W, M, X>(
    //     mut self,
    //     flag: X,
    //     fv: Fv,
    //     fp: Fp,
    //     fe: Fe,
    // ) -> CommutativeResult<V, Pf, WC, E, EC>
    // where
    //     X: ErrorFlag,
    //     Fv: FnOnce(V) -> Pf,
    //     Fp: FnOnce(P) -> Pf,
    //     Fe: FnOnce(&V) -> Option<M>,
    //     EC: Extend<E> + Default,
    //     WC: Extend<W>,
    //     M: Into<W> + Into<E>,
    // {
    //     if flag.is_error() {
    //         self.eval_error(fv, fp, |v| fe(v).map(Into::into))
    //     } else {
    //         self.eval_warning(|v| fe(v).map(Into::into));
    //         self.map_err_value(fp)
    //     }
    // }

    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn eval_warning_or_error3<Pf, Fe, Fv, Fp, W, M, X>(
        mut self,
        flag: X,
        fv: Fv,
        fp: Fp,
        fe: Fe,
    ) -> CommutativeResult<V, Pf, WC, E, EC>
    where
        X: TriErrorFlag,
        Fv: FnOnce(V) -> Pf,
        Fp: FnOnce(P) -> Pf,
        Fe: FnOnce(&V) -> Option<M>,
        EC: Extend<E> + Default,
        WC: Extend<W>,
        M: Into<W> + Into<E>,
    {
        match flag.is_error() {
            None => self.map_err_value(fp),
            Some(true) => self.eval_error(fv, fp, |v| fe(v).map(Into::into)),
            Some(false) => {
                self.eval_warning(|v| fe(v).map(Into::into));
                self.map_err_value(fp)
            }
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn extend_errors<Fv, Fp>(
        self,
        errors: impl IntoIterator<Item = E>,
        fv: Fv,
        fp: Fp,
    ) -> Self
    where
        Fv: FnOnce(V) -> P,
        Fp: FnOnce(P) -> P,
        EC: Extend<E> + Default,
    {
        let mut it = errors.into_iter();
        match self {
            Succ(succ) => {
                if let Some(e0) = it.by_ref().next() {
                    let mut es_ = GenNonEmpty::new1(e0);
                    es_.extend(it);
                    Fail(succ.fail(es_).fmap_once(fv))
                } else {
                    Succ(succ)
                }
            }
            Fail(mut err) => {
                err.extend_errors(it);
                Fail(err.fmap_once(fp))
            }
        }
    }

    // #[allow(clippy::needless_pass_by_value)]
    // pub(crate) fn extend_warnings_or_errors<X, M, W, Fv, Fp, Fw, Fe>(
    //     mut self,
    //     errors: impl IntoIterator<Item = M>,
    //     fv: Fv,
    //     fp: Fp,
    //     fw: Fw,
    //     fe: Fe,
    //     flag: X,
    // ) -> Self
    // where
    //     Fv: FnOnce(V) -> P,
    //     Fp: FnOnce(P) -> P,
    //     Fe: Fn(M) -> E,
    //     Fw: Fn(M) -> W,
    //     WC: Extend<W>,
    //     EC: Extend<E> + Default + SwitchableErrorContainer<Inner = E>,
    //     X: ErrorFlag,
    // {
    //     if flag.is_error() {
    //         self.extend_errors(errors.into_iter().map(fe), fv, fp)
    //     } else {
    //         self.extend_commutative_warnings(errors.into_iter().map(fw));
    //         self.map_err_value(fp)
    //     }
    // }

    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn extend_warnings_or_errors3<X, M, W, Fv, Fp, Fw, Fe>(
        mut self,
        errors: impl IntoIterator<Item = M>,
        fv: Fv,
        fp: Fp,
        fw: Fw,
        fe: Fe,
        flag: X,
    ) -> Self
    where
        Fv: FnOnce(V) -> P,
        Fp: FnOnce(P) -> P,
        Fe: Fn(M) -> E,
        Fw: Fn(M) -> W,
        WC: Extend<W>,
        EC: Extend<E> + Default + SwitchableErrorContainer<Inner = E>,
        X: TriErrorFlag,
    {
        match flag.is_error() {
            None => self.map_err_value(fp),
            Some(true) => self.extend_errors(errors.into_iter().map(fe), fv, fp),
            Some(false) => {
                self.extend_commutative_warnings(errors.into_iter().map(fw));
                self.map_err_value(fp)
            }
        }
    }

    pub(crate) fn and_commutative<F>(self, f: F) -> Self
    where
        F: FnOnce() -> CommutativeResult<(), P, WC, E, EC>,
        WC: Semigroup,
    {
        self.and_then_commutative(|v| f().map_ok_value(|()| v))
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
    pub(crate) fn and_then_commutative<F, Vf>(self, f: F) -> CommutativeResult<Vf, P, WC, E, EC>
    where
        F: FnOnce(V) -> CommutativeResult<Vf, P, WC, E, EC>,
        WC: Semigroup,
    {
        self.and_then_commutative_(|p| p, f)
    }

    pub(crate) fn and_then_commutative_<Fp, Fr, Vf, Pf>(
        self,
        fp: Fp,
        fr: Fr,
    ) -> CommutativeResult<Vf, Pf, WC, E, EC>
    where
        Fr: FnOnce(V) -> CommutativeResult<Vf, Pf, WC, E, EC>,
        Fp: FnOnce(P) -> Pf,
        WC: Semigroup,
    {
        match self {
            Succ(x) => x.with_log(fr),
            Fail(x) => Fail(x.fmap_once(fp)),
        }
    }

    // pub(crate) fn and_then_nowarn_commutative<F, Vf>(
    //     self,
    //     f: F,
    // ) -> CommutativeResult<Vf, P, WC, E, EC>
    // where
    //     F: FnOnce(V) -> NowarnResult<Vf, P, E, EC>,
    // {
    //     self.and_then_nowarn_commutative_(|p| p, f)
    // }

    // pub(crate) fn and_then_nowarn_commutative_<Fp, Fr, Vf, Pf>(
    //     self,
    //     fp: Fp,
    //     fr: Fr,
    // ) -> CommutativeResult<Vf, Pf, WC, E, EC>
    // where
    //     Fr: FnOnce(V) -> NowarnResult<Vf, Pf, E, EC>,
    //     Fp: FnOnce(P) -> Pf,
    // {
    //     match self {
    //         Succ(x) => x.with_log_nowarn(fr),
    //         Fail(x) => Fail(x.fmap_once(fp)),
    //     }
    // }

    /// Combine two commutative results.
    ///
    /// Ok values will be wrapped in a tuple. Error values if they exist will
    /// be voided.
    ///
    /// Inners for warnings and errors must be the same. The former must
    /// be a semigroup (which here means Option<T> must be converted to Vec<T>
    /// prior to calling). The latter will be converted to a Vec<T> since
    /// there could be more than one errors.
    pub(crate) fn zip_commutative<V1, P1>(
        self,
        a: CommutativeResult<V1, P1, WC, E, EC>,
    ) -> CommutativeResult<(V, V1), (), WC, E, EC>
    where
        EC: Extend<E> + IntoIterator<Item = E>,
        WC: Monoid,
    {
        match (self, a) {
            (Succ(ax), Succ(bx)) => Succ(ax.lift_f2_once(bx, |x, y| (x, y))),
            (Succ(ax), Fail(bx)) => Fail(ax.with_failure(bx, |_, _| ())),
            (Fail(ax), Succ(bx)) => Fail(ax.with_success(bx, |_, _| ())),
            (Fail(ax), Fail(bx)) => Fail(ax.lift_f2_once(bx, |_, _| ())),
        }
    }

    /// Combine three commutative results.
    pub(crate) fn zip3_commutative<V1, V2, P1, P2>(
        self,
        a: CommutativeResult<V1, P1, WC, E, EC>,
        b: CommutativeResult<V2, P2, WC, E, EC>,
    ) -> CommutativeResult<(V, V1, V2), (), WC, E, EC>
    where
        EC: Extend<E> + IntoIterator<Item = E>,
        WC: Monoid,
    {
        self.zip_commutative(a)
            .zip_commutative(b.repack())
            .map_ok_value(|((ax, bx), cx)| (ax, bx, cx))
    }

    /// Combine four commutative results.
    #[allow(clippy::type_complexity)]
    pub(crate) fn zip4_commutative<V1, V2, V3, P1, P2, P3>(
        self,
        a: CommutativeResult<V1, P1, WC, E, EC>,
        b: CommutativeResult<V2, P2, WC, E, EC>,
        c: CommutativeResult<V3, P3, WC, E, EC>,
    ) -> CommutativeResult<(V, V1, V2, V3), (), WC, E, EC>
    where
        EC: Extend<E> + IntoIterator<Item = E>,
        WC: Monoid,
    {
        self.zip3_commutative(a, b)
            .zip_commutative(c.repack())
            .map_ok_value(|((ax, bx, cx), dx)| (ax, bx, cx, dx))
    }

    /// Combine five commutative results.
    #[allow(clippy::type_complexity)]
    pub(crate) fn zip5_commutative<V1, V2, V3, V4, P1, P2, P3, P4>(
        self,
        a: CommutativeResult<V1, P1, WC, E, EC>,
        b: CommutativeResult<V2, P2, WC, E, EC>,
        c: CommutativeResult<V3, P3, WC, E, EC>,
        d: CommutativeResult<V4, P4, WC, E, EC>,
    ) -> CommutativeResult<(V, V1, V2, V3, V4), (), WC, E, EC>
    where
        EC: Extend<E> + IntoIterator<Item = E>,
        WC: Monoid,
    {
        self.zip4_commutative(a, b, c)
            .zip_commutative(d.repack())
            .map_ok_value(|((ax, bx, cx, dx), ex)| (ax, bx, cx, dx, ex))
    }

    /// Combine six commutative results.
    #[allow(clippy::type_complexity)]
    pub(crate) fn zip6_commutative<V1, V2, V3, V4, V5, P1, P2, P3, P4, P5>(
        self,
        x1: CommutativeResult<V1, P1, WC, E, EC>,
        x2: CommutativeResult<V2, P2, WC, E, EC>,
        x3: CommutativeResult<V3, P3, WC, E, EC>,
        x4: CommutativeResult<V4, P4, WC, E, EC>,
        x5: CommutativeResult<V5, P5, WC, E, EC>,
    ) -> CommutativeResult<(V, V1, V2, V3, V4, V5), (), WC, E, EC>
    where
        EC: Extend<E> + IntoIterator<Item = E>,
        WC: Monoid,
    {
        self.zip5_commutative(x1, x2, x3, x4)
            .zip_commutative(x5.repack())
            .map_ok_value(|((y0, y1, y2, y3, y4), y5)| (y0, y1, y2, y3, y4, y5))
    }
}

//
// Commutative/Resolvable LogResult
//
impl<V, WC, E> CommutativeResult<V, (), WC, E, Nothing<E>> {
    /// Resolve commutative Result with into regular Result type.
    ///
    /// Warnings will be given outside the result since commutative Results by
    /// definition allow the same warnings in both Succ and Failor branches.
    pub fn resolve_commutative<Fwarn, Ferr, WarnRes, FailRes>(
        self,
        f_warnings: Fwarn,
        f_errors: Ferr,
    ) -> (WarnRes, Result<V, FailRes>)
    where
        Fwarn: FnOnce(WC) -> WarnRes,
        Ferr: FnOnce(E) -> FailRes,
    {
        match self {
            Succ(s) => {
                let (v, warn_res) = s.resolve(f_warnings);
                (warn_res, Ok(v))
            }
            Fail(e) => (f_warnings(e.warnings), Err(f_errors(e.errors.head))),
        }
    }
}

//
// Deferred LogResult
//
impl<VP, LWC, RWC, X, E, EC> LogResult<VP, VP, LWC, RWC, X, E, EC> {
    /// Set value of deferred Result
    pub(crate) fn set_deferred_value<Vf>(self, x: Vf) -> LogResult<Vf, Vf, LWC, RWC, X, E, EC> {
        self.map_deferred_value(|_| x)
    }

    /// Map function over Succ and Failor value of result (assumed same type).
    pub(crate) fn map_deferred_value<F: FnOnce(VP) -> VPf, VPf>(
        self,
        f: F,
    ) -> LogResult<VPf, VPf, LWC, RWC, X, E, EC> {
        match self {
            Succ(s) => Succ(s.fmap_once(f)),
            Fail(s) => Fail(s.fmap_once(f)),
        }
    }
}

//
// Deferred/Commutative LogResult
//
impl<V, WC, E, EC> Deferred<V, WC, E, EC> {
    pub(crate) fn new_deferred_maybe(value: V, error: Option<E>) -> Self
    where
        WC: Default,
        EC: Default,
    {
        if let Some(e) = error {
            Fail(Failure::new_from_one(e, value))
        } else {
            Succ(Success::new_non_switchable(value))
        }
    }

    pub(crate) fn new_deferred_if(is_ok: bool, value: V, error: E) -> Self
    where
        WC: Default,
        EC: Default,
    {
        if is_ok {
            Succ(Success::new_non_switchable(value))
        } else {
            Fail(Failure::new_from_one(error, value))
        }
    }

    // /// Push a warning based on the value in a deferred Result.
    // ///
    // /// This must be a deferred result because the same value type must exist
    // /// on both Succ and Failor sides.
    // pub(crate) fn eval_def_warning<W, F>(&mut self, f: F)
    // where
    //     F: FnOnce(&V) -> Option<W>,
    //     WC: Extend<W>,
    // {
    //     match self {
    //         Succ(s) => s.eval_warning(f),
    //         Fail(e) => e.eval_warning(f),
    //     }
    // }

    /// Push an error based on the value in a deferred Result.
    ///
    /// If Result is Ok and the evaluation returns an error, the result will
    /// be converted to an error.
    ///
    /// This must be a deferred result because the same value type must exist
    /// on both Ok and Error sides.
    pub(crate) fn eval_deferred_error<F>(self, f: F) -> Self
    where
        F: FnOnce(&V) -> Option<E>,
        EC: Extend<E> + Default,
    {
        match self {
            Succ(succ) => match f(&succ.value) {
                Some(e) => Fail(succ.fail(GenNonEmpty::new1(e))),
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

    /// Push errors to a deferred Result.
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    pub(crate) fn extend_deferred_errors(self, errors: impl IntoIterator<Item = E>) -> Self
    where
        EC: Extend<E> + Default,
    {
        match self {
            Succ(succ) => {
                if let Some(es) = GenNonEmpty::collect(errors) {
                    Fail(succ.fail(es))
                } else {
                    Succ(succ)
                }
            }
            Fail(mut err) => {
                err.extend_errors(errors);
                Fail(err)
            }
        }
    }

    // /// Push switchable error to a deferred Result based on its value.
    // ///
    // /// If Result is Ok, the result will be converted to an error.
    // ///
    // /// This must be deferred because the value type will be the same
    // /// if the Result needs to flip from Ok to Error.
    // #[allow(clippy::needless_pass_by_value)]
    // pub(crate) fn eval_deferred_warning_or_error<X, M, W, F>(self, flag: X, f: F) -> Self
    // where
    //     F: FnOnce(&V) -> Option<M>,
    //     M: Into<E> + Into<W>,
    //     EC: Extend<E> + Default + SwitchableErrorContainer,
    //     WC: Extend<W>,
    //     X: ErrorFlag,
    // {
    //     self.eval_warning_or_error(flag, |v| v, |v| v, f)
    // }

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
    pub(crate) fn and_then_deferred<F, Vf>(self, f: F) -> Deferred<Vf, WC, E, EC>
    where
        F: FnOnce(V) -> Deferred<Vf, WC, E, EC>,
        WC: Semigroup,
        EC: Extend<E> + IntoIterator<Item = E>,
    {
        self.and_then_deferred_(|v| v, f)
    }

    pub(crate) fn and_then_deferred_<Fp, Fr, Vf, Pf>(
        self,
        fp: Fp,
        fr: Fr,
    ) -> CommutativeResult<Vf, Pf, WC, E, EC>
    where
        Fr: FnOnce(V) -> CommutativeResult<Vf, Pf, WC, E, EC>,
        Fp: FnOnce(Vf) -> Pf,
        WC: Semigroup,
        EC: Extend<E> + IntoIterator<Item = E>,
    {
        match self {
            Succ(s) => s.with_log(fr),
            Fail(e) => Fail(e.with_log(fp, fr)),
        }
    }

    // pub(crate) fn and_then_nowarn_deferred<F, Vf>(self, f: F) -> Deferred<Vf, WC, E, EC>
    // where
    //     F: FnOnce(V) -> NowarnResult<Vf, Vf, E, EC>,
    //     EC: Extend<E> + IntoIterator<Item = E>,
    // {
    //     self.and_then_nowarn_deferred_(|p| p, f)
    // }

    // pub(crate) fn and_then_nowarn_deferred_<Fr, Fp, Vf, Pf>(
    //     self,
    //     fp: Fp,
    //     fr: Fr,
    // ) -> CommutativeResult<Vf, Pf, WC, E, EC>
    // where
    //     Fr: FnOnce(V) -> NowarnResult<Vf, Pf, E, EC>,
    //     Fp: FnOnce(Vf) -> Pf,
    //     EC: Extend<E> + IntoIterator<Item = E>,
    // {
    //     match self {
    //         Succ(s) => s.with_log_nowarn(fr),
    //         Fail(e) => Fail(e.with_log_nowarn(fp, fr)),
    //     }
    // }

    pub(crate) fn and_then_deferred_switchable_result<F, X, Vf>(
        self,
        flag: X,
        f: F,
    ) -> Deferred<Vf, WC, E, EC>
    where
        X: ErrorFlag,
        Vf: Default,
        F: FnOnce(V) -> Result<Vf, E>,
        WC: Semigroup + Default,
        EC: Extend<E>
            + IntoIterator<Item = E>
            + SwitchableErrorContainer<Inner = E, Warn = WC>
            + Default,
    {
        self.and_then_deferred(|v| {
            f(v).into_deferred_switchable(flag)
                .switchable_into_commutative()
        })
    }
}

//
// Nowarn LogResult
//
impl<V, P, E, EC> NowarnResult<V, P, E, EC> {
    /// Lift Result with no warnings to commutative Result
    pub(crate) fn nowarn_into_warn<LWCf>(self) -> CommutativeResult<V, P, LWCf, E, EC>
    where
        LWCf: Default,
    {
        self.map_either(Success::nowarn_into_warn, |x| x)
            .non_commutative_into_commutative()
    }

    /// Set warnings in both Succ and Error sides of Result
    pub(crate) fn set_commutative_warnings<WC>(self, ws: WC) -> CommutativeResult<V, P, WC, E, EC> {
        match self {
            Succ(s) => Succ(s.set_warnings(ws)),
            Fail(e) => Fail(e.set_warnings(ws)),
        }
    }

    /// Monad-ically (kinda) chain a LogResult with no warnings.
    ///
    /// This is more general than the commutative case because there we can't
    /// assume that the warnings on either side are empty. If the function
    /// returns Fail and the input is Succ, then the warnings from the input
    /// need to be appended to those in the Fail type, which means their types
    /// need to match.
    ///
    /// If we know there are no warnings, then the function can return a
    /// non-commutative result type.
    pub(crate) fn nowarn_and_then<F, Vf, LWC, RWC, X>(
        self,
        f: F,
    ) -> LogResult<Vf, P, LWC, RWC, X, E, EC>
    where
        F: FnOnce(V) -> LogResult<Vf, P, LWC, RWC, X, E, EC>,
        RWC: Default,
    {
        self.nowarn_and_then_(|p| p, f)
    }

    pub(crate) fn nowarn_and_then_<Fp, Fr, Vf, Pf, LWC, RWC, X>(
        self,
        fp: Fp,
        fr: Fr,
    ) -> LogResult<Vf, Pf, LWC, RWC, X, E, EC>
    where
        Fr: FnOnce(V) -> LogResult<Vf, Pf, LWC, RWC, X, E, EC>,
        Fp: FnOnce(P) -> Pf,
        RWC: Default,
    {
        match self {
            Succ(x) => x.nowarn_with_log(fr),
            Fail(x) => Fail(x.nowarn_into_warn().fmap_once(fp)),
        }
    }
}

//
// Nowarn/Deferred
//
impl<V, E, EC> NowarnResult<V, V, E, EC> {
    pub(crate) fn nowarn_into_switchable<X>(self, flag: X) -> SwitchableResult<V, V, X, E, EC>
    where
        X: ErrorFlag,
        EC: SwitchableErrorContainer<Inner = E> + Default,
        EC::Warn: Default,
    {
        match self {
            Succ(x) => SwitchableResult::new_switchable_ok(x.value, flag),
            Fail(x) => {
                if flag.is_error() {
                    Fail(Failure::new_from_many(x.errors, x.value))
                } else {
                    let ws = EC::errors_to_warnings(x.errors);
                    Succ(Success::new(x.value, flag, ws))
                }
            }
        }
    }

    pub(crate) fn nowarn_into_switchable3<X>(self, flag: X) -> SwitchableResult<V, V, X, E, EC>
    where
        X: TriErrorFlag,
        EC: SwitchableErrorContainer<Inner = E> + Default,
        EC::Warn: Default,
    {
        match self {
            Succ(x) => SwitchableResult::new_switchable_ok(x.value, flag),
            Fail(x) => match flag.is_error() {
                None => SwitchableResult::new_switchable_ok(x.value, flag),
                Some(true) => Fail(Failure::new_from_many(x.errors, x.value)),
                Some(false) => {
                    let ws = EC::errors_to_warnings(x.errors);
                    Succ(Success::new(x.value, flag, ws))
                }
            },
        }
    }
}

//
// Nowarn/Deferred/Single error
//
impl<V, E> DeferredError<V, E> {
    /// Monadically chain nowarn results and replace previous error if it exists.
    ///
    /// This is a very specialized case meant to be used where a deferred result
    /// produces an error and a value, the latter of which needs to be used
    /// by another operation which produces a new deferred value with an error,
    /// where this error implies the first error.
    ///
    /// The last phrase is key because this function will throw away the first
    /// error for the case when both results are errors. In this case, this is
    /// correct because the latter error implies the first, and it is redundant
    /// to return both.
    pub(crate) fn and_then_replace<F, Vf>(self, f: F) -> DeferredError<Vf, E>
    where
        F: FnOnce(V) -> DeferredError<Vf, E>,
    {
        match self {
            Succ(x) => f(x.value),
            Fail(x) => {
                let ret = match f(x.value) {
                    Succ(y) => Failure::new(Nothing::default(), x.errors, y.value),
                    Fail(y) => Failure::new(Nothing::default(), y.errors, y.value),
                };
                Fail(ret)
            }
        }
    }
}

//
// Nowarn/Deferred/Infallible LogResult
//
impl<V, P, EC> NowarnResult<V, P, Infallible, EC> {
    pub(crate) fn infallible_nowarn_into(self) -> V {
        let Succ(ret) = self;
        ret.value
    }
}

//
// Nowarn/Resolvable LogResult
//
impl<V, E> NowarnResult<V, (), E, Nothing<E>> {
    /// Resolve Result with no warnings into regular Result type.
    pub fn resolve_nowarn(self) -> Result<V, E> {
        match self {
            Succ(s) => Ok(s.value),
            Fail(x) => Err(x.errors.head),
        }
    }
}

//
// Non-commutative LogResult
//
impl<V, P, LWC, E, EC> NonCommutativeResult<V, P, LWC, E, EC> {
    /// Lift non-commutative Result into commutative Result
    pub(crate) fn non_commutative_into_commutative(self) -> CommutativeResult<V, P, LWC, E, EC>
    where
        LWC: Default,
    {
        self.map_err(Failure::nowarn_into_warn)
    }

    /// Map function over warnings of a non-commutative Result
    pub(crate) fn map_non_commutative_warnings<F, W, Wf>(
        self,
        f: F,
    ) -> NonCommutativeResult<V, P, Sibling1<LWC, Wf>, E, EC>
    where
        F: Fn(W) -> Wf,
        LWC: Functor<W>,
    {
        self.map(|s| s.map_warnings(f))
    }
}

//
// Non-commutative/Resolveable LogResult
//
impl<V, LWC, E> NonCommutativeResult<V, (), LWC, E, Nothing<E>> {
    /// Resolve non-commutative Result with regular Result type.
    ///
    /// Warnings will be given on the Succ side since non-commutative Result's
    /// by definition cannot have warnings in the Fail branch.
    #[cfg(feature = "python")]
    pub(crate) fn resolve_non_commutative<Fwarn, Ferr, WarnRes, FailRes>(
        self,
        f_warnings: Fwarn,
        f_errors: Ferr,
    ) -> Result<(V, WarnRes), FailRes>
    where
        Fwarn: FnOnce(LWC) -> WarnRes,
        Ferr: FnOnce(E) -> FailRes,
    {
        match self {
            Succ(x) => Ok(x.resolve(f_warnings)),
            Fail(x) => Err(f_errors(x.errors.head)),
        }
    }
}

//
// Switchable LogResult
//
impl<V, P, X, WC, E, EC> LogResult<V, P, WC, Nothing<()>, X, E, EC> {
    pub(crate) fn new_switchable_ok(value: V, flag: X) -> Self
    where
        EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Default,
        EC::Warn: Default,
    {
        Succ(Success::new_flagged(value, flag))
    }

    // pub(crate) fn new_switchable(value: V, default: P, error: E, flag: X) -> Self
    // where
    //     EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Default,
    //     X: ErrorFlag,
    // {
    //     if flag.is_error() {
    //         Fail(Failure::new_from_one(error, default))
    //     } else {
    //         Succ(Success::new(value, flag, EC::error_to_warning(error)))
    //     }
    // }

    pub(crate) fn new_switchable3(value: V, default: P, error: E, flag: X) -> Self
    where
        EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Default,
        WC: Default,
        X: TriErrorFlag,
    {
        match flag.is_error() {
            None => Succ(Success::new_flagged(value, flag)),
            Some(true) => Fail(Failure::new_from_one(error, default)),
            Some(false) => Succ(Success::new(value, flag, EC::error_to_warning(error))),
        }
    }

    // pub(crate) fn new_switchable_ok_if(is_ok: bool, value: V, default: P, error: E, flag: X) -> Self
    // where
    //     EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Default,
    //     EC::Warn: Default,
    //     X: ErrorFlag,
    // {
    //     if is_ok {
    //         Self::new_switchable_ok(value, flag)
    //     } else {
    //         Self::new_switchable(value, default, error, flag)
    //     }
    // }

    pub(crate) fn new_switchable_ok_if3(
        is_ok: bool,
        value: V,
        default: P,
        error: E,
        flag: X,
    ) -> Self
    where
        EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Default,
        EC::Warn: Default,
        X: TriErrorFlag,
    {
        if is_ok {
            Self::new_switchable_ok(value, flag)
        } else {
            Self::new_switchable3(value, default, error, flag)
        }
    }

    // pub(crate) fn new_switchable_maybe(value: V, default: P, error: Option<E>, flag: X) -> Self
    // where
    //     EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Default,
    //     EC::Warn: Default,
    //     X: ErrorFlag,
    // {
    //     match error {
    //         Some(e) => Self::new_switchable(value, default, e, flag),
    //         None => Self::new_switchable_ok(value, flag),
    //     }
    // }

    pub(crate) fn new_switchable_maybe3(value: V, default: P, error: Option<E>, flag: X) -> Self
    where
        EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Default,
        EC::Warn: Default,
        X: TriErrorFlag,
    {
        match error {
            Some(e) => Self::new_switchable3(value, default, e, flag),
            None => Self::new_switchable_ok(value, flag),
        }
    }

    pub(crate) fn new_switchable_iter<I>(value: V, default: P, errors: I, flag: X) -> Self
    where
        I: IntoIterator<Item = E>,
        EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Default + Extend<E>,
        EC::Warn: Default,
        X: ErrorFlag,
    {
        match GenNonEmpty::collect(errors) {
            Some(es) => {
                if flag.is_error() {
                    Fail(Failure::new_from_many(es, default))
                } else {
                    Succ(Success::new(value, flag, EC::errors_to_warnings(es)))
                }
            }
            None => Self::new_switchable_ok(value, flag),
        }
    }

    pub(crate) fn new_switchable_iter3<I>(value: V, default: P, errors: I, flag: X) -> Self
    where
        I: IntoIterator<Item = E>,
        EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Default + Extend<E>,
        EC::Warn: Default,
        X: TriErrorFlag,
    {
        match GenNonEmpty::collect(errors) {
            Some(es) => match flag.is_error() {
                None => Self::new_switchable_ok(value, flag),
                Some(true) => Fail(Failure::new_from_many(es, default)),
                Some(false) => Succ(Success::new(value, flag, EC::errors_to_warnings(es))),
            },
            None => Self::new_switchable_ok(value, flag),
        }
    }

    /// Convert errors in non-commutative/switchable Results
    #[allow(clippy::type_complexity)]
    pub(crate) fn map_switchable_errors<F, Ef>(
        self,
        f: F,
    ) -> LogResult<V, P, Sibling1<EC::Warn, Ef>, Nothing<()>, X, Ef, Sibling1<EC, Ef>>
    where
        F: Fn(E) -> Ef,
        EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Functor<E>,
        <EC as SwitchableErrorContainer>::Warn: Functor<E>,
    {
        match self {
            Succ(s) => Succ(s.map_warnings(f)),
            Fail(e) => Fail(e.map_errors(f)),
        }
    }

    pub(crate) fn switchable_into_non_commutative(
        self,
    ) -> NonCommutativeResult<V, P, EC::Warn, E, EC>
    where
        EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Functor<E>,
    {
        self.map(Success::remove_flag)
    }

    pub(crate) fn switchable_into_commutative(self) -> CommutativeResult<V, P, EC::Warn, E, EC>
    where
        EC: SwitchableErrorContainer<Warn = WC, Inner = E>,
        EC::Warn: Default,
    {
        self.map(Success::remove_flag)
            .map_err(Failure::nowarn_into_warn)
    }
}

//
// Switchable/deferred LogResult
//
impl<T, X, WC, E, EC> LogResult<T, T, WC, Nothing<()>, X, E, EC> {
    pub(crate) fn new_deferred_switchable(value: T, error: E, flag: X) -> Self
    where
        EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Default,
        EC::Warn: Default,
        X: ErrorFlag,
    {
        if flag.is_error() {
            Fail(Failure::new_from_one(error, value))
        } else {
            Succ(Success::new(value, flag, EC::error_to_warning(error)))
        }
    }

    pub(crate) fn new_deferred_switchable3(value: T, error: E, flag: X) -> Self
    where
        EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Default,
        EC::Warn: Default,
        X: TriErrorFlag,
    {
        match flag.is_error() {
            None => Succ(Success::new_flagged(value, flag)),
            Some(true) => Fail(Failure::new_from_one(error, value)),
            Some(false) => Succ(Success::new(value, flag, EC::error_to_warning(error))),
        }
    }

    // pub(crate) fn new_deferred_switchable_ok_if(is_ok: bool, value: V, error: E, flag: X) -> Self
    // where
    //     EC: SwitchableErrorContainer<Inner = E> + Default,
    //     EC::Warn: Default,
    //     X: ErrorFlag,
    // {
    //     if is_ok {
    //         Self::new_switchable_ok(value, flag)
    //     } else {
    //         Self::new_deferred_switchable(value, error, flag)
    //     }
    // }

    // pub(crate) fn new_deferred_switchable_maybe(value: T, error: Option<E>, flag: X) -> Self
    // where
    //     EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Default,
    //     EC::Warn: Default,
    //     X: ErrorFlag,
    // {
    //     match error {
    //         Some(e) => Self::new_deferred_switchable(value, e, flag),
    //         None => Self::new_switchable_ok(value, flag),
    //     }
    // }

    pub(crate) fn new_deferred_switchable_maybe3(value: T, error: Option<E>, flag: X) -> Self
    where
        EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Default,
        EC::Warn: Default,
        X: TriErrorFlag,
    {
        match error {
            Some(e) => Self::new_deferred_switchable3(value, e, flag),
            None => Self::new_switchable_ok(value, flag),
        }
    }

    // pub(crate) fn new_deferred_switchable_iter<I>(value: T, errors: I, flag: X) -> Self
    // where
    //     I: IntoIterator<Item = E>,
    //     EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Default + Extend<E>,
    //     EC::Warn: Default,
    //     X: ErrorFlag,
    // {
    //     match GenNonEmpty::collect(errors) {
    //         Some(es) => {
    //             if flag.is_error() {
    //                 Fail(Failure::new_from_many(es, value))
    //             } else {
    //                 Succ(Success::new(value, flag, EC::errors_to_warnings(es)))
    //             }
    //         }
    //         None => Self::new_switchable_ok(value, flag),
    //     }
    // }

    pub(crate) fn new_deferred_switchable_iter3<I>(value: T, errors: I, flag: X) -> Self
    where
        I: IntoIterator<Item = E>,
        EC: SwitchableErrorContainer<Warn = WC, Inner = E> + Default + Extend<E>,
        EC::Warn: Default,
        X: TriErrorFlag,
    {
        match GenNonEmpty::collect(errors) {
            Some(es) => match flag.is_error() {
                None => Self::new_switchable_ok(value, flag),
                Some(true) => Fail(Failure::new_from_many(es, value)),
                Some(false) => Succ(Success::new(value, flag, EC::errors_to_warnings(es))),
            },
            None => Self::new_switchable_ok(value, flag),
        }
    }

    /// Push switchable errors to a deferred Result.
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    pub(crate) fn extend_deferred_switchable_errors(
        self,
        errors: impl IntoIterator<Item = E>,
    ) -> Self
    where
        EC: Extend<E> + Default + SwitchableErrorContainer<Warn = WC, Inner = E>,
        EC::Warn: Extend<E> + IntoIterator<Item = E> + Default,
        X: ErrorFlag,
    {
        match self {
            Succ(mut succ) => {
                if succ.flag.is_error() {
                    let ws = succ.warnings.into_iter().chain(errors);
                    if let Some(es) = GenNonEmpty::collect(ws) {
                        Fail(Failure::new_from_many(es, succ.value))
                    } else {
                        Succ(Success::new_flagged(succ.value, succ.flag))
                    }
                } else {
                    succ.extend_warnings(errors);
                    Succ(succ)
                }
            }
            Fail(mut fail) => {
                fail.extend_errors(errors);
                Fail(fail)
            }
        }
    }

    /// Push switchable errors to a deferred Result (tri flag version)
    ///
    /// If Result is Ok, the result will be converted to an error.
    ///
    /// This must be deferred because the value type will be the same
    /// if the Result needs to flip from Ok to Error.
    pub(crate) fn extend_deferred_switchable_errors3(
        self,
        errors: impl IntoIterator<Item = E>,
    ) -> Self
    where
        EC: Extend<E> + Default + SwitchableErrorContainer<Warn = WC, Inner = E>,
        EC::Warn: Extend<E> + IntoIterator<Item = E> + Default,
        X: TriErrorFlag,
    {
        // TODO not DRY
        match self {
            Succ(mut succ) => match succ.flag.is_error() {
                None => Succ(succ),
                Some(true) => {
                    let ws = succ.warnings.into_iter().chain(errors);
                    if let Some(es) = GenNonEmpty::collect(ws) {
                        Fail(Failure::new_from_many(es, succ.value))
                    } else {
                        Succ(Success::new_flagged(succ.value, succ.flag))
                    }
                }
                Some(false) => {
                    succ.extend_warnings(errors);
                    Succ(succ)
                }
            },
            Fail(mut fail) => {
                fail.extend_errors(errors);
                Fail(fail)
            }
        }
    }

    // pub(crate) fn eval_deferred_switchable_error<F>(self, f: F) -> Self
    // where
    //     F: FnOnce(&T) -> Option<E>,
    //     EC: Extend<E> + Default + SwitchableErrorContainer<Warn = WC>,
    //     EC::Warn: Extend<E>,
    //     X: ErrorFlag,
    // {
    //     // TODO where is the flag used?
    //     match self {
    //         Succ(succ) => {
    //             if let Some(e) = f(&succ.value) {
    //                 Fail(Failure::new_from_one(e, succ.value))
    //             } else {
    //                 Succ(succ)
    //             }
    //         }
    //         Fail(mut fail) => {
    //             if let Some(e) = f(&fail.value) {
    //                 fail.push_error(e);
    //             }
    //             Fail(fail)
    //         }
    //     }
    // }

    pub(crate) fn eval_deferred_switchable_error3<F>(self, f: F) -> Self
    where
        F: FnOnce(&T) -> Option<E>,
        EC: Extend<E> + Default + SwitchableErrorContainer<Warn = WC>,
        EC::Warn: Extend<E>,
        X: TriErrorFlag,
    {
        match self {
            Succ(mut succ) => {
                if let Some(e) = f(&succ.value) {
                    match succ.flag.is_error() {
                        None => Succ(succ),
                        Some(true) => Fail(Failure::new_from_one(e, succ.value)),
                        Some(false) => {
                            succ.extend_warnings([e]);
                            Succ(succ)
                        }
                    }
                } else {
                    Succ(succ)
                }
            }
            Fail(mut fail) => {
                if let Some(e) = f(&fail.value) {
                    fail.push_error(e);
                }
                Fail(fail)
            }
        }
    }

    pub(crate) fn and_then_switchable<Vf, F>(self, f: F) -> DeferredSwitchable<Vf, X, E, EC>
    where
        F: FnOnce(T) -> NowarnResult<Vf, Vf, E, EC>,
        EC: Extend<E> + IntoIterator<Item = E> + SwitchableErrorContainer<Warn = WC>,
    {
        self.and_then_switchable_(|v| v, f)
    }

    pub(crate) fn and_then_switchable_<Vf, Pf, Fp, Fr>(
        self,
        fp: Fp,
        fr: Fr,
    ) -> SwitchableResult<Vf, Pf, X, E, EC>
    where
        Fp: FnOnce(Vf) -> Pf,
        Fr: FnOnce(T) -> NowarnResult<Vf, Pf, E, EC>,
        EC: Extend<E> + IntoIterator<Item = E> + SwitchableErrorContainer<Warn = WC>,
    {
        match self {
            Succ(x) => fr(x.value).map(|s| s.set_warnings(x.warnings).set_flag(x.flag)),
            Fail(x) => Fail(x.with_log_nowarn(fp, fr)),
        }
    }
}

//
// LogResult with no passthru
//
impl<V, LWC, RWC, X, E, EC> LogResult<V, (), LWC, RWC, X, E, EC> {
    pub(crate) fn new_err(error: E) -> Self
    where
        RWC: Default,
        EC: Default,
    {
        Fail(Failure::new_from_one(error, ()))
    }
}

//
// Non-switchable LogResult with no passthru
//
impl<V, LWC, RWC, E, EC> LogResult<V, (), LWC, RWC, (), E, EC> {
    pub(crate) fn new_err_from_iter<I>(errors: I, default: V) -> Self
    where
        I: IntoIterator<Item = E>,
        EC: Extend<E> + Default,
        RWC: Default,
        LWC: Default,
    {
        match GenNonEmpty::collect(errors) {
            None => Self::new_ok(default),
            Some(e) => Fail(Failure::new_from_many(e, ())),
        }
    }
}

//
// Non-switchable LogResult
//
impl<V, P, LWC, RWC, E, EC> LogResult<V, P, LWC, RWC, (), E, EC> {
    pub(crate) fn new_ok(value: V) -> Self
    where
        LWC: Default,
    {
        Succ(Success::new_non_switchable(value))
    }

    pub(crate) fn new_ok_default() -> Self
    where
        V: Default,
        LWC: Default,
    {
        Self::new_ok(V::default())
    }

    pub(crate) fn new_log_if(is_ok: bool, value: V, default: P, error: E) -> Self
    where
        LWC: Default,
        RWC: Default,
        EC: Default,
    {
        if is_ok {
            Succ(Success::new_non_switchable(value))
        } else {
            Fail(Failure::new_from_one(error, default))
        }
    }

    /// Map function over errors in Result
    ///
    /// This function will work on any Result type but may change a switchable
    /// Result to non-switchable one, which is generally not a good idea.
    /// See [`map_*_fung_errors`] for functions that will map over warnings
    /// if they are the same type as errors.
    pub(crate) fn map_errors<F, Ef>(
        self,
        f: F,
    ) -> LogResult<V, P, LWC, RWC, (), Ef, Sibling1<EC, Ef>>
    where
        F: Fn(E) -> Ef,
        EC: Functor<E>,
    {
        self.map_err(|e| e.map_errors(f))
    }
}

//
// LogResult with one error
//
impl<V, P, LWC, RWC, X, E> LogResult<V, P, LWC, RWC, X, E, Nothing<E>> {
    pub(crate) fn map_error<F, Ef>(self, f: F) -> LogResult<V, P, LWC, RWC, X, Ef, Nothing<Ef>>
    where
        F: FnOnce(E) -> Ef,
    {
        self.map_err(|e| e.map_error(f))
    }
}

//
// LogResult with error group
//
// impl<V, P, LWC, RWC, X, E, G> GroupLogResult<V, P, LWC, RWC, X, E, G> {
//     pub(crate) fn ungroup(self) -> LogResult<V, P, LWC, RWC, X, E, Vec<E>> {
//         self.map_err(Failure::ungroup)
//     }
// }

//
// LogResult with IO error group
//
impl<V, P, LWC, RWC, X, E, G> IOGroupLogResult<V, P, LWC, RWC, X, E, G> {
    pub(crate) fn map_pure_errors<F, Ef>(self, f: F) -> IOGroupLogResult<V, P, LWC, RWC, X, Ef, G>
    where
        F: Fn(E) -> Ef,
    {
        self.map_error(|e| e.fmap(f))
    }
}

//
// LogResult with anon IO error group
//
impl<V, P, LWC, RWC, X, E> IOGroupLogResult<V, P, LWC, RWC, X, E, ()> {
    pub(crate) fn deanonymize_as<G>(self, g: G) -> IOGroupLogResult<V, P, LWC, RWC, X, E, G> {
        self.map_error(|e| e.deanonymize_as(g))
    }

    pub(crate) fn deanonymize<G: Default>(self) -> IOGroupLogResult<V, P, LWC, RWC, X, E, G> {
        self.deanonymize_as(G::default())
    }
}

//
// Commutative LogResult with IO error group
//
impl<V, WC, P, E> IOGroupLogResult<V, P, WC, WC, (), E, ()> {
    pub(crate) fn warnings_to_pure_errors<F, W>(
        self,
        conf: ReadSharedConfig,
        f: F,
    ) -> IOGroupLogResult<V, (), WC, WC, (), E, ()>
    where
        F: Fn(W) -> E,
        WC: IntoIterator<Item = W> + Default,
    {
        let res = self;
        if conf.warnings_are_errors {
            match res {
                Succ(s) => s.warnings_to_pure_errors(f, |_| ()),
                Fail(e) => Fail(e.warnings_to_pure_errors(f).fmap_once(|_| ())),
            }
        } else if conf.hide_warnings {
            res.map(Success::remove_warnings)
                .map_err(Failure::remove_warnings)
                .set_err_value(())
        } else {
            res.set_err_value(())
        }
    }
}

//
// Fully-generic LogResult
//
impl<V, P, LWC, RWC, X, E, EC> LogResult<V, P, LWC, RWC, X, E, EC> {
    pub(crate) fn map_either<F, G, Vf, Pf, LWCf, RWCf, Xf, Ef, ECf>(
        self,
        f: F,
        g: G,
    ) -> LogResult<Vf, Pf, LWCf, RWCf, Xf, Ef, ECf>
    where
        F: FnOnce(Success<V, X, LWC>) -> Success<Vf, Xf, LWCf>,
        G: FnOnce(Failure<P, RWC, E, EC>) -> Failure<Pf, RWCf, Ef, ECf>,
    {
        match self {
            Succ(s) => Succ(f(s)),
            Fail(e) => Fail(g(e)),
        }
    }

    pub(crate) fn map<F, Vf, Xf, LWCf>(self, f: F) -> LogResult<Vf, P, LWCf, RWC, Xf, E, EC>
    where
        F: FnOnce(Success<V, X, LWC>) -> Success<Vf, Xf, LWCf>,
    {
        self.map_either(f, |x| x)
    }

    pub(crate) fn map_err<F, Pf, RWCf, Ef, ECf>(
        self,
        f: F,
    ) -> LogResult<V, Pf, LWC, RWCf, X, Ef, ECf>
    where
        F: FnOnce(Failure<P, RWC, E, EC>) -> Failure<Pf, RWCf, Ef, ECf>,
    {
        self.map_either(|x| x, f)
    }

    /// Map function over Succ value of Result
    pub(crate) fn map_ok_value<F, Vf>(self, f: F) -> LogResult<Vf, P, LWC, RWC, X, E, EC>
    where
        F: FnOnce(V) -> Vf,
    {
        self.map(|s| s.fmap_once(f))
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
    pub(crate) fn map_err_value<F, Pf>(self, f: F) -> LogResult<V, Pf, LWC, RWC, X, E, EC>
    where
        F: FnOnce(P) -> Pf,
    {
        self.map_err(|e| e.fmap_once(f))
    }

    /// Set value of Succ Result
    pub(crate) fn set_ok_value<Vf>(self, x: Vf) -> LogResult<Vf, P, LWC, RWC, X, E, EC> {
        self.map_ok_value(|_| x)
    }

    /// Set value of Failor Result
    pub(crate) fn set_err_value<Pf>(self, x: Pf) -> LogResult<V, Pf, LWC, RWC, X, E, EC> {
        self.map_err_value(|_| x)
    }

    /// Add a member to both the Succ and Failor value, returning both as a tuple.
    ///
    /// This seems weird but is useful for cases where we need to use a non-Copy
    /// variable in two closures for both branches but one closure will "eat"
    /// (move) the value before the other can use it. This function will move
    /// the value once depending on the branch where is can be consumed by
    /// both closures as an argument.
    #[allow(clippy::type_complexity)]
    pub(crate) fn inject_value<I>(self, x: I) -> LogResult<(V, I), (P, I), LWC, RWC, X, E, EC> {
        match self {
            Succ(s) => Succ(s.fmap_once(|v| (v, x))),
            Fail(e) => Fail(e.fmap_once(|v| (v, x))),
        }
    }

    pub(crate) fn repack<LWCf, RWCf, ECf>(self) -> LogResult<V, P, LWCf, RWCf, X, E, ECf>
    where
        LWC: IntoNewCardinality<LWCf>,
        RWC: IntoNewCardinality<RWCf>,
        EC: IntoNewCardinality<ECf>,
    {
        self.repack_left_warnings()
            .repack_right_warnings()
            .repack_errors()
    }

    pub(crate) fn into_semigroup<LWCf, RWCf>(self) -> LogResult<V, P, LWCf, RWCf, X, E, Vec<E>>
    where
        LWC: IntoNewCardinality<LWCf>,
        RWC: IntoNewCardinality<RWCf>,
        EC: IntoNewCardinality<Vec<E>>,
    {
        self.repack()
    }

    pub(crate) fn repack_left_warnings<LWCf>(self) -> LogResult<V, P, LWCf, RWC, X, E, EC>
    where
        LWC: IntoNewCardinality<LWCf>,
    {
        self.map(Success::repack)
    }

    pub(crate) fn repack_right_warnings<RWCf>(self) -> LogResult<V, P, LWC, RWCf, X, E, EC>
    where
        RWC: IntoNewCardinality<RWCf>,
    {
        self.map_err(Failure::repack_warnings)
    }

    pub(crate) fn repack_errors<ECf>(self) -> LogResult<V, P, LWC, RWC, X, E, ECf>
    where
        EC: IntoNewCardinality<ECf>,
    {
        self.map_err(Failure::repack_errors)
    }

    /// Aggregate non-switchable errors into one error.
    pub(crate) fn aggregate_errors<Ef, F>(
        self,
        f: F,
    ) -> LogResult<V, P, LWC, RWC, X, Ef, Nothing<Ef>>
    where
        // NOTE pretend there is a negative trait bound for "non-switchable"
        F: FnOnce(GenNonEmpty<E, EC>) -> Ef,
    {
        self.map_err(|e| e.aggregate_errors(f))
    }

    pub fn group<G>(self) -> GroupLogResult<V, P, LWC, RWC, X, E, G>
    where
        EC: IntoNewCardinality<Vec<E>>,
        G: Default,
    {
        self.group_with(G::default())
    }

    #[allow(clippy::type_complexity)]
    pub fn group_with<G>(self, s: G) -> GroupLogResult<V, P, LWC, RWC, X, E, G>
    where
        EC: IntoNewCardinality<Vec<E>>,
    {
        self.aggregate_errors(|es| {
            let xs = GenNonEmpty::new(es.head, es.tail.into_new_cardinality());
            ErrorGroup::new(s, xs)
        })
    }

    /// Push a warning based on the Succ value of a non-deferred Result.
    ///
    /// Will only store warning on the Succ side since the value isn't present
    /// on the error side to be evaluated.
    pub(crate) fn eval_warning<W, F>(&mut self, f: F)
    where
        F: FnOnce(&V) -> Option<W>,
        LWC: Extend<W>,
    {
        if let Succ(s) = self {
            s.eval_warning(f);
        }
    }

    #[cfg(test)]
    pub(crate) fn deconstruct<W>(self) -> (Option<V>, Vec<W>, Vec<E>)
    where
        LWC: IntoIterator<Item = W>,
        RWC: IntoIterator<Item = W>,
        EC: IntoIterator<Item = E>,
    {
        match self {
            Succ(x) => {
                let ws = x.warnings.into_iter().collect();
                (Some(x.value), ws, vec![])
            }
            Fail(x) => {
                let ws = x.warnings.into_iter().collect();
                let es = x.errors.into_iter().collect();
                (None, ws, es)
            }
        }
    }

    pub(crate) fn as_ref(&self) -> Option<&V> {
        match self {
            Succ(s) => Some(&s.value),
            Fail(_) => None,
        }
    }
}

/// Split the IO error away from an impure result.
///
/// For results that have an IOErrorGroup, this will throw the entire group
/// (similar to `?`) if an IO error is present, otherwise return a pure result
/// with an `ErrorGroup` (ie no IO error).
///
/// In effect, this will short-circuit if an IO-error is present.
macro_rules! split_io {
    ($x:expr) => {
        match $x {
            Ok(x) => Ok(x),
            Err(x) => match x {
                e @ crate::logging::IOErrorGroup::IO(_, _) => {
                    return Err(type_families::Functor::fmap(e, Into::into));
                }
                crate::logging::IOErrorGroup::Pure(e) => Err(e),
            },
        }
    };
}

pub(crate) use split_io;

macro_rules! split_log {
    ($x:expr) => {
        match $x {
            crate::logging::LogResult::Succ(x) => x,
            crate::logging::LogResult::Fail(x) => {
                return crate::logging::LogResult::Fail(x);
            }
        }
    };
}

pub(crate) use split_log;

/// Lift an IO error into a LogResult with an `IOErrorGroup`.
///
/// This is effectively a replacement for `?` since we can't implement `Try`
/// on `LogResult`.
macro_rules! io_to_log {
    ($x:expr) => {
        match $x {
            Ok(x) => x,
            Err(e) => {
                return crate::logging::LogResult::new_err(IOErrorGroup::from(e));
            }
        }
    };
}

pub(crate) use io_to_log;

#[cfg(feature = "python")]
mod python {
    use super::{CommutativeResult, ErrorGroup, IOErrorGroup, NonCommutativeResult, Success};

    use crate::text::optional::Nothing;

    use fireflow_types::python::PyreflowWarning;

    use pyo3::exceptions::PyBaseExceptionGroup;
    use pyo3::prelude::*;

    use std::ffi::CString;
    use std::fmt::Display;

    impl<E, G> From<IOErrorGroup<E, G>> for PyErr
    where
        ErrorGroup<E, G>: Into<Self>,
    {
        fn from(value: IOErrorGroup<E, G>) -> Self {
            match value {
                // one OSError
                IOErrorGroup::IO(e, None) => e.into(),
                // one OSError with other non-OSErrors
                IOErrorGroup::IO(e, Some(g)) => {
                    let s = "IO error with non-IO errors";
                    let es = vec![e.into(), g.into()];
                    PyBaseExceptionGroup::new_err((s, es))
                }
                // non-OSErrors
                IOErrorGroup::Pure(e) => e.into(),
            }
        }
    }

    impl<E, S> From<ErrorGroup<E, S>> for PyErr
    where
        E: Into<Self>,
        S: Display,
    {
        // TODO check if we are on python <3.11 and do something different if so
        fn from(value: ErrorGroup<E, S>) -> Self {
            let s = value.summary.to_string();
            let es: Vec<_> = value.errors.into_iter().map(Into::into).collect();
            // NOTE this is not written in the docs or enforced; exception
            // groups take two args, the first being a string with the error
            // summary and the second being a "sequence" (ie a list/iterator
            // thing). The 'new_err' method only takes one arg, so these two
            // args need to be wrapped in a tuple. If this is incorrect then
            // python will simply produce a 'fail to normalize' exception when
            // trying to make the real exception.
            PyBaseExceptionGroup::new_err((s, es))
        }
    }

    impl<V, WC, E> CommutativeResult<V, (), WC, E, Nothing<E>> {
        pub fn py_resolve_commutative<W>(self) -> PyResult<V>
        where
            WC: IntoIterator<Item = W>,
            W: Display,
            E: Into<PyErr>,
        {
            let (warn, res) = self.resolve_commutative(emit_warnings, Into::into);
            warn?;
            res
        }
    }

    impl<V, WC, E> NonCommutativeResult<V, (), WC, E, Nothing<E>> {
        pub fn py_resolve_non_commutative<W>(self) -> PyResult<V>
        where
            WC: IntoIterator<Item = W>,
            W: Display,
            E: Into<PyErr>,
        {
            let (res, warn) = self.resolve_non_commutative(emit_warnings, Into::into)?;
            warn?;
            Ok(res)
        }
    }

    impl<V, WC> Success<V, (), WC> {
        pub fn py_resolve_warnings<W>(self) -> PyResult<V>
        where
            WC: IntoIterator<Item = W>,
            W: Display,
        {
            let (value, warn) = self.resolve(emit_warnings);
            warn?;
            Ok(value)
        }
    }

    // TODO make this work with different exception types that can be caught,
    // right now anything that has a given error type will simply become a
    // 'PyreflowWarning'
    fn emit_warnings<W>(ws: impl IntoIterator<Item = W>) -> PyResult<()>
    where
        W: Display,
    {
        Python::attach(|py| -> PyResult<()> {
            let wt = py.get_type::<PyreflowWarning>();
            for w in ws {
                let s = CString::new(w.to_string())?;
                PyErr::warn(py, &wt, &s, 0)?;
            }
            Ok(())
        })
    }
}
