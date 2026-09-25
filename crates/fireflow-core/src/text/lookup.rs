use crate::logging::{DeferredSwitchableError, ResultExt as _};
use crate::std_index::index::{LookupAction, StdLookupTx};
use crate::validated::keys::{DollarKey, DollarKey_, TruncatedNEString, ValueToStdKey};

use fireflow_types::config::{
    ConfigFlag as _, ProcessOptionalFailure, ReadDataKeywordsConfig, TrimIntraValueWhitespace,
};
use fireflow_types::std_key::{DollarWrap, StdKey};
use nonempty::{NEStr, NEString};

use type_families::{BifunctorOnce, Sibling2, impl_kind2};

use derive_more::{Display, From};
use derive_new::new;
use derive_where::derive_where;
use thiserror::Error;

use std::convert::Infallible;
use std::str::FromStr;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
    std::fmt::Display,
};

/// An error caused when parsing a required non-indexed standard key
pub type ReqKeyError<T> = ReqKeyErrorInner<<T as FromStr>::Err, T>;

/// An error caused when parsing a required indexed standard key with external state
pub type ReqStKeyError<T> = ReqKeyErrorInner<<T as FromStrWith>::Err, T>;

/// A parse key error for an optional non-indexed key.
pub type OptKeyError<T> = ParseKeyError<<T as FromStr>::Err, T>;

/// A parse key error for an optional non-indexed key when parsing with external state.
pub type OptStKeyError<T> = ParseKeyError<<T as FromStrWith>::Err, T>;

/// An error caused when parsing a required standard key
#[derive(From, Display, Error)]
#[derive_where(Clone, Debug, PartialEq; E, I)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
#[cfg_attr(feature = "python", bound(E: Display))]
#[cfg_attr(feature = "python", bound(DollarKey_<T, I>: Display))]
pub enum ReqKeyErrorInner_<E, T, I> {
    /// Error due to parsing
    Parse(ParseKeyError_<E, T, I>),

    /// Error due to absence
    Missing(MissingKeyError_<T, I>),
}

pub type ReqKeyErrorInner<E, T> = ReqKeyErrorInner_<E, T, <T as ValueToStdKey>::Index>;

/// An error caused by parsing a string incorrectly for a standard key value.
#[derive(new, Error)]
#[derive_where(Clone, Debug, PartialEq; E, I)]
#[error("key '{key}' with value '{value}' could not be parsed: {error}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
#[cfg_attr(feature = "python", bound(ParseKeyError_<E, T, I>: Display))]
pub struct ParseKeyError_<E, T, I> {
    pub error: E,
    pub key: DollarKey_<T, I>,
    pub value: TruncatedNEString,
}

pub type ParseKeyError<E, T> = ParseKeyError_<E, T, <T as ValueToStdKey>::Index>;

impl<E, T: ValueToStdKey> ParseKeyError<E, T> {
    pub(crate) fn new1(error: E, index: T::Index, value: NEString) -> Self {
        Self::new(error, DollarKey::new(index), TruncatedNEString(value))
    }
}

/// An error caused by a required standard key being missing
#[derive(Error, new)]
#[derive_where(Clone, Debug, PartialEq; I)]
#[error("missing required key: {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
#[cfg_attr(feature = "python", bound(DollarKey_<T, I>: Display))]
pub struct MissingKeyError_<T, I>(pub DollarKey_<T, I>);

pub type MissingKeyError<T> = MissingKeyError_<T, <T as ValueToStdKey>::Index>;

impl<T: ValueToStdKey> MissingKeyError<T> {
    pub(crate) fn new1(index: T::Index) -> Self {
        Self(DollarKey::new(index))
    }
}

type ReqResult<T> = Result<T, ReqKeyErrorInner<<T as FromStr>::Err, T>>;

pub type Trimmed = Option<NEString>;

/// A piece of data which has some additional diagnostic data with it.
#[derive(Default, new, Debug, PartialEq, Eq)]
pub struct Diagnosed<T, D> {
    /// The data itself.
    pub inner: T,

    /// Associated diagnostic data.
    pub diagnostic: D,
}

// impl<T> Diagnosed<T, ()> {
//     pub(crate) fn new1(t: T) -> Self {
//         Self::new(t, ())
//     }
// }

impl<T> Diagnosed<T, Trimmed> {
    pub(crate) fn into_root_pair(self) -> (T, Option<(StdKey, TruncatedNEString)>)
    where
        T: ValueToStdKey<Index = ()>,
    {
        let k = self.inner;
        let s = self.diagnostic.map(|t| (DollarWrap(T::std0()), t.into()));
        (k, s)
    }

    pub(crate) fn into_indexed_pair(self, i: &T::Index) -> (T, Option<(StdKey, TruncatedNEString)>)
    where
        T: ValueToStdKey,
    {
        let k = self.inner;
        let s = self.diagnostic.map(|t| (DollarWrap(T::std(i)), t.into()));
        (k, s)
    }
}

impl<T> Diagnosed<Option<T>, Trimmed> {
    pub(crate) fn into_opt_root_pair(self) -> (Option<T>, Option<(StdKey, TruncatedNEString)>)
    where
        T: ValueToStdKey<Index = ()>,
    {
        let k = self.inner;
        let s = self.diagnostic.map(|t| (DollarWrap(T::std0()), t.into()));
        (k, s)
    }

    pub(crate) fn into_opt_indexed_pair(
        self,
        i: &T::Index,
    ) -> (Option<T>, Option<(StdKey, TruncatedNEString)>)
    where
        T: ValueToStdKey,
    {
        let k = self.inner;
        let s = self.diagnostic.map(|t| (DollarWrap(T::std(i)), t.into()));
        (k, s)
    }
}

impl_kind2!(pub DiagnosedFamily, Diagnosed);

impl<A, B> BifunctorOnce<A, B> for Diagnosed<A, B> {
    fn first_once<F: FnOnce(A) -> C, C>(self, f: F) -> Sibling2<Self, C, B> {
        Diagnosed::new(f(self.inner), self.diagnostic)
    }

    fn second_once<F: FnOnce(B) -> C, C>(self, f: F) -> Sibling2<Self, A, C> {
        Diagnosed::new(self.inner, f(self.diagnostic))
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
    ) -> Result<Diagnosed<Self, Trimmed>, Self::Err> {
        let (res, trimmed) = Self::from_str_delim(s, trim_whitespace);
        res.map(|x| Diagnosed::new(x, trimmed))
    }

    fn from_str_delim(
        s: &NEStr,
        trim_whitespace: TrimIntraValueWhitespace,
    ) -> (Result<Self, Self::Err>, Trimmed) {
        let it = s.as_str().split(Self::DELIM);
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
    Result<Diagnosed<T, <T as FromStrWith>::Diagnostic>, <T as FromStrWith>::Err>;

// this won't be necessary once rust gets specialization
macro_rules! impl_from_str_with_delim {
    ($t:path, $e:path) => {
        impl crate::text::lookup::FromStrWith for $t {
            type Err = $e;
            type Payload<'a> = ();
            type Diagnostic = Option<nonempty::NEString>;
            type Config = crate::config::EvaledReadStdKeywordsConfig;

            fn from_str_with(
                s: &nonempty::NEStr,
                (): (),
                conf: &crate::config::EvaledReadStdKeywordsConfig,
            ) -> Result<crate::text::lookup::Diagnosed<Self, Option<nonempty::NEString>>, Self::Err>
            {
                let (res, trimmed) = Self::from_str_delim(s, conf.trim_intra_value_whitespace);
                res.map(|x| Diagnosed::new(x, trimmed))
            }
        }
    };
}

pub(crate) use impl_from_str_with_delim;

/// A required key
pub(crate) trait ReqValue: Sized + ValueToStdKey {
    fn get_req(kws: &StdLookupTx, i: Self::Index) -> Result<Self, ReqKeyErrorInner<Self::Err, Self>>
    where
        Self: FromStr,
        Self::Index: Copy,
    {
        let v = Self::get_req_inner(kws, i).map_err(ReqKeyErrorInner::from)?;
        v.parse()
            .map_err(|e| ParseKeyError::new1(e, i, v.to_owned()))
            .map_err(ReqKeyErrorInner::from)
    }

    // #[allow(clippy::type_complexity)]
    // fn get_req_with(
    //     kws: &StdLookupTx,
    //     i: Self::Index,
    //     data: Self::Payload<'_>,
    //     conf: &Self::Config,
    // ) -> Result<Diagnosed<Self, Self::Diagnostic>, ReqKeyErrorInner<Self::Err, Self>>
    // where
    //     Self: FromStrWith,
    //     Self::Index: Copy,
    // {
    //     let v = Self::get_req_inner(kws, i).map_err(ReqKeyErrorInner::from)?;
    //     Self::from_str_with(v, data, conf)
    //         .map_err(|e| ParseKeyError::new1(e, i, v.to_owned()))
    //         .map_err(ReqKeyErrorInner::from)
    // }

    fn remove_req(
        kws: &mut StdLookupTx,
        i: Self::Index,
    ) -> Result<Self, ReqKeyErrorInner<Self::Err, Self>>
    where
        Self: FromStr,
        Self::Index: Copy,
    {
        let v = Self::remove_req_inner(kws, i).map_err(ReqKeyErrorInner::from)?;
        v.parse()
            .map_err(|e| ParseKeyError::new1(e, i, v.to_owned()))
            .map_err(ReqKeyErrorInner::from)
    }

    #[allow(clippy::type_complexity)]
    fn remove_req_with(
        kws: &mut StdLookupTx,
        i: Self::Index,
        data: Self::Payload<'_>,
        conf: &Self::Config,
    ) -> Result<Diagnosed<Self, Self::Diagnostic>, ReqKeyErrorInner<Self::Err, Self>>
    where
        Self: FromStrWith,
        Self::Index: Copy,
    {
        let v = Self::remove_req_inner(kws, i).map_err(ReqKeyErrorInner::from)?;
        Self::from_str_with(v, data, conf)
            .map_err(|e| ParseKeyError::new1(e, i, v.to_owned()))
            .map_err(ReqKeyErrorInner::from)
    }

    fn get_req_inner<'a>(
        kws: &'a StdLookupTx,
        i: Self::Index,
    ) -> Result<&'a NEStr, MissingKeyError<Self>> {
        match kws.read::<Self>(&i) {
            Some(v) => Ok(v),
            None => Err(MissingKeyError::new1(i)),
        }
    }

    fn remove_req_inner<'a>(
        kws: &'a mut StdLookupTx,
        i: Self::Index,
    ) -> Result<&'a NEStr, MissingKeyError<Self>> {
        match kws.remove::<Self>(&i) {
            Some(v) => Ok(v),
            None => Err(MissingKeyError::new1(i)),
        }
    }

    fn get_metaroot_req(kws: &StdLookupTx) -> ReqResult<Self>
    where
        Self: ValueToStdKey<Index = ()> + FromStr,
    {
        Self::get_req(kws, ())
    }

    fn remove_metaroot_req(kws: &mut StdLookupTx) -> ReqResult<Self>
    where
        Self: ValueToStdKey<Index = ()> + FromStr,
    {
        Self::remove_req(kws, ())
    }

    fn get_meas_req(kws: &StdLookupTx, i: Self::Index) -> ReqResult<Self>
    where
        Self: FromStr,
        Self::Index: Copy,
    {
        Self::get_req(kws, i)
    }

    fn remove_meas_req(kws: &mut StdLookupTx, i: Self::Index) -> ReqResult<Self>
    where
        Self: FromStr,
        Self::Index: Copy,
    {
        Self::remove_req(kws, i)
    }

    fn remove_meas_req_with(
        kws: &mut StdLookupTx,
        i: Self::Index,
        data: Self::Payload<'_>,
        conf: &Self::Config,
    ) -> Result<Diagnosed<Self, Self::Diagnostic>, ReqStKeyError<Self>>
    where
        Self: FromStrWith,
        Self::Index: Copy,
        Self::Diagnostic: Default,
    {
        Self::remove_req_with(kws, i, data, conf)
    }
}

/// An optional key
pub(crate) trait OptValue: Sized + ValueToStdKey {
    type Outer: Default + From<Self> + Into<Option<Self>>;

    fn get_opt(
        kws: &StdLookupTx,
        k: Self::Index,
    ) -> Result<Self::Outer, ParseKeyError<Self::Err, Self>>
    where
        Self: FromStr,
    {
        kws.read::<Self>(&k)
            .map(|v| {
                v.parse()
                    .map_err(|e| ParseKeyError::new1(e, k, v.to_owned()))
            })
            .transpose()
            .map(|x| x.map(Self::Outer::from).unwrap_or_default())
    }

    // #[allow(clippy::type_complexity)]
    // fn get_opt_with<I>(
    //     kws: &StdIndexTx,
    //     k: SpecificKey<Self, Self::Index>,
    //     data: Self::Payload<'_>,
    //     conf: &Self::Config,
    // ) -> Result<DiagnosedKeyword<Self::Outer, Self::Diagnostic>, ParseKeyError<Self::Err, Self, Self::Index>>
    // where
    //     SpecificKey<Self, Self::Index>: AsStdKey,
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

    fn get_or_ignore_opt(
        kws: &StdLookupTx,
        k: Self::Index,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredSwitchableError<Self::Outer, ProcessOptionalFailure, ParseKeyError<Self::Err, Self>>
    where
        Self: FromStr,
    {
        Self::get_opt(kws, k).into_deferred_switchable3(conf.process_optional_failure)
    }

    fn remove_or_transfer_opt(
        kws: &mut StdLookupTx,
        k: Self::Index,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredSwitchableError<Self::Outer, ProcessOptionalFailure, ParseKeyError<Self::Err, Self>>
    where
        Self: FromStr,
        Self::Index: Copy,
    {
        let flag = conf.process_optional_failure;
        Self::remove_opt(kws, k, flag)
    }

    #[allow(clippy::type_complexity)]
    fn remove_or_transfer_opt_with<C>(
        kws: &mut StdLookupTx,
        k: Self::Index,
        data: Self::Payload<'_>,
        conf: &C,
    ) -> DeferredSwitchableError<
        Diagnosed<Self::Outer, Self::Diagnostic>,
        ProcessOptionalFailure,
        ParseKeyError<Self::Err, Self>,
    >
    where
        Self: FromStrWith,
        Self::Index: Copy,
        Self::Diagnostic: Default,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<Self::Config>,
    {
        let rconf: &ReadDataKeywordsConfig = conf.as_ref();
        let flag = rconf.process_optional_failure;
        Self::remove_opt_with(kws, k, data, flag, conf.as_ref())
    }

    fn get_root_opt(kws: &StdLookupTx) -> Result<Self::Outer, OptKeyError<Self>>
    where
        Self: ValueToStdKey<Index = ()> + FromStr,
    {
        Self::get_opt(kws, ())
    }

    fn remove_root_opt_nofail(kws: &mut StdLookupTx) -> Self::Outer
    where
        Self: ValueToStdKey<Index = ()> + FromStr<Err = Infallible>,
    {
        Self::remove_opt_nofail(kws, ())
    }

    fn remove_or_drop_root_opt(
        kws: &mut StdLookupTx,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredSwitchableError<Self::Outer, ProcessOptionalFailure, OptKeyError<Self>>
    where
        Self: ValueToStdKey<Index = ()> + FromStr,
    {
        Self::remove_or_transfer_opt(kws, (), conf)
    }

    fn remove_or_drop_root_opt_with<C>(
        kws: &mut StdLookupTx,
        data: Self::Payload<'_>,
        conf: &C,
    ) -> DeferredSwitchableError<
        Diagnosed<Self::Outer, Self::Diagnostic>,
        ProcessOptionalFailure,
        OptStKeyError<Self>,
    >
    where
        Self: ValueToStdKey<Index = ()> + FromStrWith,
        Self::Diagnostic: Default,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<Self::Config>,
    {
        Self::remove_or_transfer_opt_with(kws, (), data, conf)
    }

    // pub(crate) trait OptIndexedKey: Sized + Optional + Key<MeasIndex> {
    // fn get_meas_opt(
    //     kws: &StdIndexTx,
    //     i: impl Into<IndexFromOne>,
    // ) -> Result<Self::Outer, OptIndexedKeyError<Self>>
    // where
    //     Self: FromStr,
    // {
    //     Self::get_opt(kws, SpecificKey::new_i1(i.into()))
    // }

    fn get_or_ignore_meas_opt(
        std: &StdLookupTx,
        i: Self::Index,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredSwitchableError<Self::Outer, ProcessOptionalFailure, OptKeyError<Self>>
    where
        Self: FromStr,
    {
        Self::get_or_ignore_opt(std, i, conf)
    }

    fn remove_meas_opt_nofail(kws: &mut StdLookupTx, i: Self::Index) -> Self::Outer
    where
        Self: FromStr<Err = Infallible>,
    {
        Self::remove_opt_nofail(kws, i)
    }

    fn remove_or_drop_meas_opt(
        kws: &mut StdLookupTx,
        i: Self::Index,
        conf: &ReadDataKeywordsConfig,
    ) -> DeferredSwitchableError<Self::Outer, ProcessOptionalFailure, OptKeyError<Self>>
    where
        Self: FromStr,
        Self::Index: Copy,
    {
        Self::remove_or_transfer_opt(kws, i, conf)
    }

    fn remove_or_drop_meas_opt_with<C>(
        kws: &mut StdLookupTx,
        i: Self::Index,
        data: Self::Payload<'_>,
        conf: &C,
    ) -> DeferredSwitchableError<
        Diagnosed<Self::Outer, Self::Diagnostic>,
        ProcessOptionalFailure,
        OptStKeyError<Self>,
    >
    where
        Self: FromStrWith,
        Self::Index: Copy,
        Self::Diagnostic: Default,
        C: AsRef<ReadDataKeywordsConfig> + AsRef<Self::Config>,
    {
        Self::remove_or_transfer_opt_with(kws, i, data, conf)
    }

    fn remove_opt(
        kws: &mut StdLookupTx,
        k: Self::Index,
        flag: ProcessOptionalFailure,
    ) -> DeferredSwitchableError<Self::Outer, ProcessOptionalFailure, ParseKeyError<Self::Err, Self>>
    where
        Self: FromStr,
    {
        let action = LookupAction::from_flag(flag);
        kws.remove_and_parse::<_, _, Self>(&k, |v| match v.parse() {
            Ok(x) => (None, Ok(x)),
            Err(e) => (action, Err((e, v.to_owned()))),
        })
        .transpose()
        .map(|x| x.map(Self::Outer::from).unwrap_or_default())
        .map_err(|(e, v)| ParseKeyError::new1(e, k, v))
        .into_deferred_switchable3(flag)
    }

    #[allow(clippy::type_complexity)]
    fn remove_opt_with(
        kws: &mut StdLookupTx,
        i: Self::Index,
        data: Self::Payload<'_>,
        flag: ProcessOptionalFailure,
        conf: &Self::Config,
    ) -> DeferredSwitchableError<
        Diagnosed<Self::Outer, Self::Diagnostic>,
        ProcessOptionalFailure,
        ParseKeyError<Self::Err, Self>,
    >
    where
        Self: FromStrWith,
        Self::Diagnostic: Default,
    {
        let action = LookupAction::from_flag(flag);
        kws.remove_and_parse::<_, _, Self>(&i, |v| match Self::from_str_with(v, data, conf) {
            Ok(x) => (None, Ok(x)),
            Err(e) => (action, Err((e, v.to_owned()))),
        })
        .transpose()
        .map(|x| x.map_or(Diagnosed::default(), |y| y.first_once(Self::Outer::from)))
        .map_err(|(e, v)| ParseKeyError::new1(e, i, v))
        .into_deferred_switchable3(flag)
    }

    fn remove_opt_nofail(kws: &mut StdLookupTx, i: Self::Index) -> Self::Outer
    where
        Self: FromStr<Err = Infallible>,
    {
        kws.remove_and_parse::<_, _, Self>(&i, |v| {
            let Ok(x) = v.parse();
            (None, x)
        })
        .map(Self::Outer::from)
        .unwrap_or_default()
    }
}
