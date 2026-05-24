use crate::config::{
    ConfigFlag as _, DummyTriFlag, ProcessOptionalFailure, ReadDataKeywordsConfig,
    TrimIntraValueWhitespace,
};
use crate::logging::{DeferredSwitchableError, LogResult, ResultExt as _};
use crate::validated::keys::{
    AsStdKey, DollarKey, IndexedKey, Key, NonStdKeywords, NonStdKeywordsExt as _, SpecificKey,
    StdKey, StdKeywords, TruncatedNEString,
};

use super::index::{IndexFromOne, MeasIndex};

use derive_more::{Display, From};
use derive_new::new;
use fireflow_types::nonempty_string::{NEStr, NEString};
use thiserror::Error;
use type_families::{BifunctorOnce, Sibling2, impl_functor_once, impl_kind1, impl_kind2};

use std::convert::Infallible;
use std::str::FromStr;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
    std::fmt::Display,
};

/// An error caused when parsing a required non-indexed standard key
pub type ReqKeyError<T> = ReqKeyErrorInner<<T as FromStr>::Err, T, ()>;

/// An error caused when parsing a required indexed standard key
pub type ReqIndexedKeyError<T> = ReqKeyErrorInner<<T as FromStr>::Err, T, IndexFromOne>;

/// An error caused when parsing a required indexed standard key with external state
pub type ReqIndexedStKeyError<T> = ReqKeyErrorInner<<T as FromStrWith>::Err, T, IndexFromOne>;

/// A parse key error for an optional non-indexed key.
pub type OptKeyError<T> = ParseKeyError<<T as FromStr>::Err, T, ()>;

/// A parse key error for an optional indexed key.
pub type OptIndexedKeyError<T> = ParseKeyError<<T as FromStr>::Err, T, IndexFromOne>;

/// A parse key error for an optional non-indexed key when parsing with external state.
pub type OptKeyStError<T> = ParseKeyError<<T as FromStrWith>::Err, T, ()>;

/// A parse key error for an optional indexed key when parsing with external state.
pub type OptIndexedKeyStError<T> = ParseKeyError<<T as FromStrWith>::Err, T, IndexFromOne>;

/// An error caused when parsing a required standard key
#[derive(From, Display, Debug, Error)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(E: Display))]
#[cfg_attr(feature = "python", bound(DollarKey<T, I>: Display))]
pub enum ReqKeyErrorInner<E, T, I> {
    /// Error due to parsing
    Parse(ParseKeyError<E, T, I>),

    /// Error due to absence
    Missing(MissingKeyError<T, I>),
}

/// An error caused by parsing a string incorrectly for a standard key value.
#[derive(new, Debug, Error)]
#[error("key '{key}' with value '{value}' could not be parsed: {error}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
#[cfg_attr(feature = "python", bound(ParseKeyError<E, T, I>: Display))]
pub struct ParseKeyError<E, T, I> {
    pub error: E,
    pub key: DollarKey<T, I>,
    pub value: TruncatedNEString,
}

/// An error caused by a required standard key being missing
#[derive(Debug, Error)]
#[error("missing required key: {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
#[cfg_attr(feature = "python", bound(DollarKey<T, I>: Display))]
pub struct MissingKeyError<T, I>(pub DollarKey<T, I>);

type ReqResult<T, I> = Result<T, ReqKeyErrorInner<<T as FromStr>::Err, T, I>>;

pub type Trimmed = Option<NEString>;

// TODO this is just like diagnosed keyword
/// Return value for string converted to native type that has delimiters.
#[derive(new)]
pub struct TrimmedKeyword<T> {
    /// Native rust type
    pub native: T,

    /// The original value if it was trimmed
    pub trimmed: Trimmed,
}

impl_kind1!(pub StrDelimOutputFamily, TrimmedKeyword);

impl_functor_once!(
    TrimmedKeyword,
    self,
    mut f,
    TrimmedKeyword::new(f(self.native), self.trimmed)
);

/// Return value for string that was converted with a configuration and state.
#[derive(Default, new)]
pub struct DiagnosedKeyword<T, D> {
    /// Native rust type
    pub native: T,

    /// Diagnostic info associated with this type.
    pub diagnostic: D,
}

impl<T> DiagnosedKeyword<T, ()> {
    pub(crate) fn new1(t: T) -> Self {
        Self::new(t, ())
    }

    pub(crate) fn into_native(self) -> T {
        self.native
    }
}

impl<T> DiagnosedKeyword<T, Trimmed> {
    pub(crate) fn into_root_pair(self) -> (T, Option<(StdKey, NEString)>)
    where
        T: Key,
    {
        (self.native, self.diagnostic.map(|t| (T::std(), t)))
    }

    pub(crate) fn into_indexed_pair(self, i: MeasIndex) -> (T, Option<(StdKey, NEString)>)
    where
        T: IndexedKey,
    {
        (self.native, self.diagnostic.map(|t| (T::std(i), t)))
    }
}

impl<T> DiagnosedKeyword<Option<T>, Trimmed> {
    pub(crate) fn into_opt_root_pair(self) -> (Option<T>, Option<(StdKey, NEString)>)
    where
        T: Key,
    {
        (self.native, self.diagnostic.map(|t| (T::std(), t)))
    }

    pub(crate) fn into_opt_indexed_pair(
        self,
        i: IndexFromOne,
    ) -> (Option<T>, Option<(StdKey, NEString)>)
    where
        T: IndexedKey,
    {
        (self.native, self.diagnostic.map(|t| (T::std(i), t)))
    }
}

impl_kind2!(pub DiagnosedOutputFamily, DiagnosedKeyword);

impl<A, B> BifunctorOnce<A, B> for DiagnosedKeyword<A, B> {
    fn first_once<F: FnOnce(A) -> C, C>(self, f: F) -> Sibling2<Self, C, B> {
        DiagnosedKeyword::new(f(self.native), self.diagnostic)
    }

    fn second_once<F: FnOnce(B) -> C, C>(self, f: F) -> Sibling2<Self, A, C> {
        DiagnosedKeyword::new(self.native, f(self.diagnostic))
    }
}

/// Parse a string that includes delimiters
pub trait FromStrDelim: Sized {
    type Err;
    const DELIM: char;

    fn from_iter<'a>(iter: impl Iterator<Item = &'a str>) -> Result<Self, Self::Err>;

    fn from_str_delim_diagnosed(
        s: &NEStr,
        trim_whitespace: TrimIntraValueWhitespace,
    ) -> Result<DiagnosedKeyword<Self, Trimmed>, Self::Err> {
        let (res, trimmed) = Self::from_str_delim(s, trim_whitespace);
        res.map(|x| DiagnosedKeyword::new(x, trimmed))
    }

    fn from_str_delim(
        s: &NEStr,
        trim_whitespace: TrimIntraValueWhitespace,
    ) -> (Result<Self, Self::Err>, Trimmed) {
        let it = s.as_ref().split(Self::DELIM);
        if trim_whitespace.is_set() {
            let mut was_trimmed = false;
            let res = Self::from_iter(it.map(|x| {
                let y = str::trim(x);
                was_trimmed = was_trimmed || y.len() < x.len();
                y
            }));
            (res, was_trimmed.then(|| s.to_owned()))
        } else {
            (Self::from_iter(it), None)
        }
    }
}

/// Parse a string based on external data and config
pub trait FromStrWith: Sized {
    type Err;
    type Payload<'a>;
    type Diagnostic;
    type Config;

    fn from_str_with(_: &NEStr, _: Self::Payload<'_>, _: &Self::Config) -> FromStrWithResult<Self>;
}

pub type FromStrWithResult<T> =
    Result<DiagnosedKeyword<T, <T as FromStrWith>::Diagnostic>, <T as FromStrWith>::Err>;

// this won't be necessary once rust gets specialization
macro_rules! impl_from_str_with_delim {
    ($t:path, $e:path) => {
        impl crate::text::lookup::FromStrWith for $t {
            type Err = $e;
            type Payload<'a> = ();
            type Diagnostic = Option<fireflow_types::nonempty_string::NEString>;
            type Config = crate::config::ReadStdKeywordsConfig;

            fn from_str_with(
                s: &fireflow_types::nonempty_string::NEStr,
                (): (),
                conf: &crate::config::ReadStdKeywordsConfig,
            ) -> Result<
                crate::text::lookup::DiagnosedKeyword<
                    Self,
                    Option<fireflow_types::nonempty_string::NEString>,
                >,
                Self::Err,
            > {
                let (res, trimmed) = Self::from_str_delim(s, conf.trim_intra_value_whitespace);
                res.map(|x| DiagnosedKeyword::new(x, trimmed))
            }
        }
    };
}

pub(crate) use impl_from_str_with_delim;

/// Any required key
pub(crate) trait Required: Sized {
    fn get_req<I>(
        kws: &StdKeywords,
        k: SpecificKey<Self, I>,
    ) -> Result<Self, ReqKeyErrorInner<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AsStdKey + Copy,
        Self: FromStr,
    {
        let v = Self::get_req_inner(kws, k).map_err(ReqKeyErrorInner::from)?;
        v.parse()
            .map_err(|e| ParseKeyError::new(e, k.into(), TruncatedNEString(v.to_owned())))
            .map_err(ReqKeyErrorInner::from)
    }

    #[allow(clippy::type_complexity)]
    fn get_req_with<I>(
        kws: &StdKeywords,
        k: SpecificKey<Self, I>,
        data: Self::Payload<'_>,
        conf: &Self::Config,
    ) -> Result<DiagnosedKeyword<Self, Self::Diagnostic>, ReqKeyErrorInner<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AsStdKey + Copy,
        Self: FromStrWith,
    {
        let v = Self::get_req_inner(kws, k).map_err(ReqKeyErrorInner::from)?;
        Self::from_str_with(v.as_ne_str(), data, conf)
            .map_err(|e| ParseKeyError::new(e, k.into(), TruncatedNEString(v.to_owned())))
            .map_err(ReqKeyErrorInner::from)
    }

    fn remove_req<I>(
        kws: &mut StdKeywords,
        k: SpecificKey<Self, I>,
    ) -> Result<Self, ReqKeyErrorInner<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AsStdKey + Copy,
        Self: FromStr,
    {
        let v = Self::remove_req_inner(kws, k).map_err(ReqKeyErrorInner::from)?;
        v.parse()
            .map_err(|e| ParseKeyError::new(e, k.into(), TruncatedNEString(v)))
            .map_err(ReqKeyErrorInner::from)
    }

    #[allow(clippy::type_complexity)]
    fn remove_req_with<I>(
        kws: &mut StdKeywords,
        k: SpecificKey<Self, I>,
        data: Self::Payload<'_>,
        conf: &Self::Config,
    ) -> Result<DiagnosedKeyword<Self, Self::Diagnostic>, ReqKeyErrorInner<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AsStdKey + Copy,
        Self: FromStrWith,
    {
        let v = Self::remove_req_inner(kws, k).map_err(ReqKeyErrorInner::from)?;
        Self::from_str_with(v.as_ne_str(), data, conf)
            .map_err(|e| ParseKeyError::new(e, k.into(), TruncatedNEString(v)))
            .map_err(ReqKeyErrorInner::from)
    }

    fn get_req_inner<I>(
        kws: &StdKeywords,
        k: SpecificKey<Self, I>,
    ) -> Result<&NEString, MissingKeyError<Self, I>>
    where
        SpecificKey<Self, I>: AsStdKey,
    {
        match kws.get(&k.as_std_key()) {
            Some(v) => Ok(v),
            None => Err(MissingKeyError(k.into())),
        }
    }

    fn remove_req_inner<I>(
        kws: &mut StdKeywords,
        k: SpecificKey<Self, I>,
    ) -> Result<NEString, MissingKeyError<Self, I>>
    where
        SpecificKey<Self, I>: AsStdKey,
    {
        match kws.remove(&k.as_std_key()) {
            Some(v) => Ok(v),
            None => Err(MissingKeyError(k.into())),
        }
    }
}

/// Any optional key
pub(crate) trait Optional: Sized {
    type Outer: Default + From<Self> + Into<Option<Self>>;

    fn get_opt<I>(
        kws: &StdKeywords,
        k: SpecificKey<Self, I>,
    ) -> Result<Self::Outer, ParseKeyError<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AsStdKey,
        Self: FromStr,
    {
        kws.get(&k.as_std_key())
            .map(|v| {
                v.parse()
                    .map_err(|e| ParseKeyError::new(e, k.into(), TruncatedNEString(v.to_owned())))
            })
            .transpose()
            .map(|x| x.map(Self::Outer::from).unwrap_or_default())
    }

    // #[allow(clippy::type_complexity)]
    // fn get_opt_with<I>(
    //     kws: &StdKeywords,
    //     k: SpecificKey<Self, I>,
    //     data: Self::Payload<'_>,
    //     conf: &Self::Config,
    // ) -> Result<DiagnosedKeyword<Self::Outer, Self::Diagnostic>, ParseKeyError<Self::Err, Self, I>>
    // where
    //     SpecificKey<Self, I>: AsStdKey,
    //     Self: FromStrWith,
    //     Self::Diagnostic: Default,
    // {
    //     kws.get(&k.as_std_key())
    //         .map(|v| {
    //             Self::from_str_with(v, data, conf)
    //                 .map_err(|e| ParseKeyError::new(e, k, TruncatedString(v.to_owned())))
    //         })
    //         .transpose()
    //         .map(|x| x.map_or(DiagnosedKeyword::default(), BifunctorOnce::first_into_once))
    // }

    fn get_or_ignore_opt<I>(
        kws: &StdKeywords,
        k: SpecificKey<Self, I>,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredSwitchableError<
        Self::Outer,
        ProcessOptionalFailure,
        ParseKeyError<Self::Err, Self, I>,
    >
    where
        SpecificKey<Self, I>: AsStdKey,
        Self: FromStr,
    {
        Self::get_opt(kws, k).into_deferred_switchable(conf.process_optional_failure)
    }

    fn remove_opt<I>(
        kws: &mut StdKeywords,
        k: SpecificKey<Self, I>,
    ) -> Result<Self::Outer, ParseKeyError<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AsStdKey,
        Self: FromStr,
    {
        kws.remove(&k.as_std_key())
            .map(|v| {
                v.parse()
                    .map_err(|e| ParseKeyError::new(e, k.into(), TruncatedNEString(v)))
            })
            .transpose()
            .map(|x| x.map(Self::Outer::from).unwrap_or_default())
    }

    #[allow(clippy::type_complexity)]
    fn remove_opt_with<I>(
        kws: &mut StdKeywords,
        k: SpecificKey<Self, I>,
        data: Self::Payload<'_>,
        conf: &Self::Config,
    ) -> Result<DiagnosedKeyword<Self::Outer, Self::Diagnostic>, ParseKeyError<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AsStdKey,
        Self: FromStrWith,
        Self::Diagnostic: Default,
    {
        kws.remove(&k.as_std_key())
            .map(|v| {
                Self::from_str_with(v.as_ne_str(), data, conf)
                    .map_err(|e| ParseKeyError::new(e, k.into(), TruncatedNEString(v)))
            })
            .transpose()
            .map(|x| {
                x.map_or(DiagnosedKeyword::default(), |y| {
                    y.first_once(Self::Outer::from)
                })
            })
    }

    fn remove_opt_nofail<I>(kws: &mut StdKeywords, k: SpecificKey<Self, I>) -> Self::Outer
    where
        SpecificKey<Self, I>: AsStdKey,
        Self: FromStr<Err = Infallible>,
    {
        let Ok(res) = Self::remove_opt(kws, k);
        res
    }

    fn remove_or_transfer_opt<I>(
        kws: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        dropped: &mut StdKeywords,
        k: SpecificKey<Self, I>,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredSwitchableError<Self::Outer, DummyTriFlag, ParseKeyError<Self::Err, Self, I>>
    where
        SpecificKey<Self, I>: AsStdKey + Copy,
        Self: FromStr,
    {
        let res = Self::remove_opt(kws, k);
        process_opt_key(res, k, nonstd, dropped, conf.process_optional_failure)
    }

    #[allow(clippy::type_complexity)]
    fn remove_or_transfer_opt_with<C, I>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        dropped: &mut StdKeywords,
        k: SpecificKey<Self, I>,
        data: Self::Payload<'_>,
        conf: &C,
    ) -> DeferredSwitchableError<
        DiagnosedKeyword<Self::Outer, Self::Diagnostic>,
        DummyTriFlag,
        ParseKeyError<Self::Err, Self, I>,
    >
    where
        SpecificKey<Self, I>: AsStdKey + Copy,
        Self: FromStrWith,
        Self::Diagnostic: Default,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<Self::Config>,
    {
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        let res = Self::remove_opt_with(std, k, data, conf.as_ref());
        process_opt_key(res, k, nonstd, dropped, rconf.process_optional_failure)
    }
}

/// A required metaroot key
pub(crate) trait ReqMetarootKey: Sized + Required + Key {
    fn get_metaroot_req(kws: &StdKeywords) -> ReqResult<Self, ()>
    where
        Self: FromStr,
    {
        Self::get_req(kws, SpecificKey::default())
    }

    fn remove_metaroot_req(kws: &mut StdKeywords) -> ReqResult<Self, ()>
    where
        Self: FromStr,
    {
        Self::remove_req(kws, SpecificKey::default())
    }
}

/// Any required key with one index
pub(crate) trait ReqIndexedKey: Sized + Required + IndexedKey {
    fn get_meas_req(kws: &StdKeywords, i: impl Into<IndexFromOne>) -> ReqResult<Self, IndexFromOne>
    where
        Self: FromStr,
    {
        Self::get_req(kws, SpecificKey::new_i1(i.into()))
    }

    fn remove_meas_req(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne>,
    ) -> ReqResult<Self, IndexFromOne>
    where
        Self: FromStr,
    {
        Self::remove_req(kws, SpecificKey::new_i1(i.into()))
    }

    fn remove_meas_req_with(
        kws: &mut StdKeywords,
        i: impl Into<IndexFromOne>,
        data: Self::Payload<'_>,
        conf: &Self::Config,
    ) -> Result<DiagnosedKeyword<Self, Self::Diagnostic>, ReqIndexedStKeyError<Self>>
    where
        Self: FromStrWith,
        Self::Diagnostic: Default,
    {
        Self::remove_req_with(kws, SpecificKey::new_i1(i.into()), data, conf)
    }
}

/// An optional metaroot key
pub(crate) trait OptMetarootKey: Sized + Optional + Key {
    fn get_root_opt(kws: &StdKeywords) -> Result<Self::Outer, OptKeyError<Self>>
    where
        Self: FromStr,
    {
        Self::get_opt(kws, SpecificKey::default())
    }

    fn remove_root_opt_nofail(kws: &mut StdKeywords) -> Self::Outer
    where
        Self: FromStr<Err = Infallible>,
    {
        Self::remove_opt_nofail(kws, SpecificKey::default())
    }

    fn remove_or_drop_root_opt(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        dropped: &mut StdKeywords,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredSwitchableError<Self::Outer, DummyTriFlag, OptKeyError<Self>>
    where
        Self: FromStr,
    {
        Self::remove_or_transfer_opt(std, nonstd, dropped, SpecificKey::default(), conf)
    }

    fn remove_or_drop_root_opt_with<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        dropped: &mut StdKeywords,
        data: Self::Payload<'_>,
        conf: &C,
    ) -> DeferredSwitchableError<
        DiagnosedKeyword<Self::Outer, Self::Diagnostic>,
        DummyTriFlag,
        OptKeyStError<Self>,
    >
    where
        Self: FromStrWith,
        Self::Diagnostic: Default,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<Self::Config>,
    {
        Self::remove_or_transfer_opt_with(std, nonstd, dropped, SpecificKey::default(), data, conf)
    }
}

/// Any optional key with an index
pub(crate) trait OptIndexedKey: Sized + Optional + IndexedKey {
    // fn get_meas_opt(
    //     kws: &StdKeywords,
    //     i: impl Into<IndexFromOne>,
    // ) -> Result<Self::Outer, OptIndexedKeyError<Self>>
    // where
    //     Self: FromStr,
    // {
    //     Self::get_opt(kws, SpecificKey::new_i1(i.into()))
    // }

    fn get_or_ignore_meas_opt(
        kws: &StdKeywords,
        i: impl Into<IndexFromOne>,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredSwitchableError<Self::Outer, ProcessOptionalFailure, OptIndexedKeyError<Self>>
    where
        Self: FromStr,
    {
        Self::get_or_ignore_opt(kws, SpecificKey::new_i1(i.into()), conf)
    }

    fn remove_meas_opt_nofail(kws: &mut StdKeywords, i: impl Into<IndexFromOne>) -> Self::Outer
    where
        Self: FromStr<Err = Infallible>,
    {
        Self::remove_opt_nofail(kws, SpecificKey::new_i1(i.into()))
    }

    fn remove_or_drop_meas_opt(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        dropped: &mut StdKeywords,
        i: impl Into<IndexFromOne>,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredSwitchableError<Self::Outer, DummyTriFlag, OptIndexedKeyError<Self>>
    where
        Self: FromStr,
    {
        Self::remove_or_transfer_opt(std, nonstd, dropped, SpecificKey::new_i1(i.into()), conf)
    }

    fn remove_or_drop_meas_opt_with<C>(
        std: &mut StdKeywords,
        nonstd: &mut NonStdKeywords,
        dropped: &mut StdKeywords,
        i: impl Into<IndexFromOne>,
        data: Self::Payload<'_>,
        conf: &C,
    ) -> DeferredSwitchableError<
        DiagnosedKeyword<Self::Outer, Self::Diagnostic>,
        DummyTriFlag,
        OptIndexedKeyStError<Self>,
    >
    where
        Self: FromStrWith,
        Self::Diagnostic: Default,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<Self::Config>,
    {
        Self::remove_or_transfer_opt_with(
            std,
            nonstd,
            dropped,
            SpecificKey::new_i1(i.into()),
            data,
            conf,
        )
    }
}

fn process_opt_key<E, I, K, X>(
    res: Result<X, ParseKeyError<E, K, I>>,
    k: SpecificKey<K, I>,
    nonstd: &mut NonStdKeywords,
    dropped: &mut StdKeywords,
    flag: ProcessOptionalFailure,
) -> DeferredSwitchableError<X, DummyTriFlag, ParseKeyError<E, K, I>>
where
    SpecificKey<K, I>: AsStdKey + Copy,
    X: Default,
{
    let triflag = flag.as_triflag();
    match res {
        Ok(x) => LogResult::new_switchable_ok(x, triflag),
        Err(e) => {
            match flag.is_demote_or_drop() {
                Some(true) => nonstd.insert_demoted(k.as_std_key(), e.value.0.clone()),
                Some(false) => {
                    let out = dropped.insert(k.as_std_key(), e.value.0.clone());
                    assert!(out.is_none(), "key was already dropped, {}", k.as_std_key());
                }
                None => (),
            }
            LogResult::new_deferred_switchable3(X::default(), e, triflag)
        }
    }
}
