use crate::{
    config::{NonStdMeasPatternOpt, TimeMeasNamePattern},
    text::keywords::Cyt,
    validated::{
        datepattern::DatePattern,
        keys::{AnyKey, Key as _, ValidKeywords},
        timepattern::TimePattern,
    },
};

use fireflow_types::nonempty_string::NEString;
use fireflow_types::{ne_str, nonempty_string::NEStr};
use nonempty_collections::{NEVec, NonEmptyIterator as _, nev};

use derive_more::Display;
use derive_new::new;
use regex::Regex;
use thiserror::Error;

use std::str::FromStr;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromPyString, IntoPyString},
    fireflow_types::python as py,
};

#[derive(Clone)]
pub enum Selector<T> {
    Root(T),
    Branch(Branch<T>),
}

#[derive(Clone)]
pub enum AppendableSelector<T> {
    One(Selector<T>),
    Many(NEVec<Selector<T>>),
}

#[derive(Clone)]
pub enum Branch<T> {
    If(Box<If<T>>),
    Cond(Cond<T>),
}

#[derive(Clone, new)]
pub struct If<T> {
    pub condition: Condition,
    pub consequent: Selector<T>,
    pub alternative: Option<Selector<T>>,
}

#[derive(Clone)]
pub struct Cond<T> {
    pub forms: NEVec<(Condition, Selector<T>)>,
}

#[derive(Clone)]
pub enum Condition {
    Root(KeyTest),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Not(Box<Self>),
}

#[derive(Clone)]
pub enum KeyTest {
    HasKey(AnyKey),
    KeyIs(AnyKey, NEString),
    KeyMatches(AnyKey, ValueRegex),
}

/// A wrapper around [`regex::Regex`] to make impls cleaner.
#[derive(Clone, Display)]
#[cfg_attr(feature = "python", derive(IntoPyString, FromPyString))]
pub struct ValueRegex(Regex);

impl<T: Default> Default for Selector<T> {
    fn default() -> Self {
        Self::Root(T::default())
    }
}

impl Selector<TimeMeasNamePattern> {
    #[must_use]
    pub fn new_time_meas_pattern() -> Self {
        let hdr_tm_regex = TimeMeasNamePattern(Some(Regex::new("^HDR-T(M)$").unwrap()));
        let is_macsquant = KeyTest::KeyIs(Cyt::std().into(), ne_str!("MACSQuant").to_owned());
        let cond = Condition::Root(is_macsquant);
        Self::if_then(cond, Self::root(hdr_tm_regex))
    }
}

impl Selector<Option<TimePattern>> {
    #[must_use]
    pub fn new_time_pattern() -> Self {
        let accuri = "%H:%M:%S:%@".parse::<TimePattern>().unwrap();
        let cond = Condition::Root(KeyTest::cyt_is(ne_str!("Accuri C6")));
        Self::if_then(cond, Self::root(Some(accuri)))
    }
}

impl Selector<Option<DatePattern>> {
    #[must_use]
    pub fn new_date_pattern() -> Self {
        let mqa = "%Y-%b-%d".parse::<DatePattern>().unwrap();
        let moflo = "%d %b %Y".parse::<DatePattern>().unwrap();
        let pas = "%d-%m-%Y".parse::<DatePattern>().unwrap();
        let is_mqa = KeyTest::cyt_is(ne_str!("MACSQuant"));
        let is_moflo = KeyTest::cyt_matches("MoFlo.*").unwrap();
        let is_pas = KeyTest::cyt_is(ne_str!("partec PAS"));
        let forms = nev![
            (Condition::Root(is_moflo), Self::root(Some(moflo))),
            (Condition::Root(is_pas), Self::root(Some(pas))),
            (Condition::Root(is_mqa), Self::root(Some(mqa)))
        ];
        Self::Branch(Branch::Cond(Cond { forms }))
    }
}

impl Selector<Option<String>> {
    #[must_use]
    pub fn new_last_modified_pattern() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn new_datetime_pattern() -> Self {
        Self::default()
    }
}

impl Selector<NonStdMeasPatternOpt> {
    #[must_use]
    pub fn new_nonstandard_measurement_pattern() -> Self {
        Self::default()
    }
}

impl<T> Selector<T> {
    #[must_use]
    pub fn if_then(cond: Condition, consequent: Self) -> Self {
        Self::Branch(Branch::If(Box::new(If::new(cond, consequent, None))))
    }

    #[must_use]
    pub fn if_then_else(cond: Condition, consequent: Self, alterantive: Self) -> Self {
        Self::Branch(Branch::If(Box::new(If::new(
            cond,
            consequent,
            Some(alterantive),
        ))))
    }

    #[must_use]
    pub fn root(t: T) -> Self {
        Self::Root(t)
    }

    pub(crate) fn eval(&self, kws: &ValidKeywords) -> T
    where
        T: Clone + Default,
    {
        self.eval_inner(kws).unwrap_or_default()
    }

    fn eval_inner(&self, kws: &ValidKeywords) -> Option<T>
    where
        T: Clone,
    {
        match self {
            Self::Root(x) => Some(x.clone()),
            Self::Branch(b) => b.eval(kws),
        }
    }
}

impl<T: Default> Default for AppendableSelector<T> {
    fn default() -> Self {
        Self::One(Selector::default())
    }
}

impl<T> AppendableSelector<T> {
    pub fn root(t: T) -> Self {
        Self::One(Selector::root(t))
    }

    pub fn try_eval<E, F>(&self, kws: &ValidKeywords, f: F) -> Result<T, E>
    where
        T: Clone + Default,
        F: Fn(NEVec<T>) -> Result<T, E>,
    {
        match self {
            Self::One(s) => Ok(s.eval(kws)),
            Self::Many(ss) => {
                let xs: NEVec<_> = ss.nonempty_iter().map(|s| s.eval(kws)).collect();
                f(xs)
            }
        }
    }
}

impl<T> Branch<T> {
    fn eval(&self, kws: &ValidKeywords) -> Option<T>
    where
        T: Clone,
    {
        match self {
            Self::If(i) => i.eval(kws),
            Self::Cond(c) => c.eval(kws),
        }
    }
}

impl<T> If<T> {
    fn eval(&self, kws: &ValidKeywords) -> Option<T>
    where
        T: Clone,
    {
        if self.condition.eval(kws) {
            self.consequent.eval_inner(kws)
        } else {
            self.alternative.as_ref()?.eval_inner(kws)
        }
    }
}

impl<T> Cond<T> {
    fn eval(&self, kws: &ValidKeywords) -> Option<T>
    where
        T: Clone,
    {
        self.forms
            .iter()
            .find_map(|(c, s)| if c.eval(kws) { s.eval_inner(kws) } else { None })
    }
}

impl Condition {
    fn eval(&self, kws: &ValidKeywords) -> bool {
        match self {
            Self::Root(s) => s.eval(kws),
            Self::And(a, b) => a.eval(kws) && b.eval(kws),
            Self::Or(a, b) => a.eval(kws) || b.eval(kws),
            Self::Not(c) => !c.eval(kws),
        }
    }
}

impl KeyTest {
    #[must_use]
    pub fn cyt_is(cyt: &NEStr) -> Self {
        Self::KeyIs(Cyt::std().into(), cyt.to_owned())
    }

    pub fn cyt_matches(pat: &str) -> Result<Self, ValueRegexError> {
        Ok(Self::KeyMatches(Cyt::std().into(), pat.parse()?))
    }

    fn eval(&self, kws: &ValidKeywords) -> bool {
        match self {
            Self::HasKey(k) => kws.get_any(k).is_some(),
            Self::KeyIs(k, p) => kws.get_any(k).is_some_and(|v| v == p),
            Self::KeyMatches(k, p) => kws.get_any(k).is_some_and(|v| p.0.is_match(v.as_str())),
        }
    }
}

/// Error when parsing [`ValueRegex`] from [`String`].
#[derive(Debug, Error, PartialEq, Clone)]
#[error("error when making case-insensitive regular expression: {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct ValueRegexError(regex::Error);

impl FromStr for ValueRegex {
    type Err = ValueRegexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        regex::RegexBuilder::new(s)
            .build()
            .map(Self)
            .map_err(ValueRegexError)
    }
}

#[cfg(feature = "python")]
mod python {
    use super::{AppendableSelector, Branch, Cond, Condition, If, KeyTest, Selector, ValueRegex};

    use crate::validated::keys::AnyKey;

    use fireflow_types::nonempty_string::{NEStr, NEString};
    use fireflow_types::python as fp;

    use nonempty_collections::NEVec;
    use pyo3::{IntoPyObjectExt as _, prelude::*, types::PyTuple};

    impl<'py, T> FromPyObject<'_, 'py> for Selector<T>
    where
        for<'b> T: FromPyObject<'b, 'py, Error = PyErr>,
    {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            // Use two-stage parse to keep the errors sane. Try to parse as a
            // branch if this is a tuple that starts with one of the operators.
            // If not, parse as a root, which could be anything.
            if let Ok((op, _)) = obj.extract::<(String, Bound<PyAny>)>()
                && (op.as_str() == fp::SELECTOR_COND.as_str()
                    || op.as_str() == fp::SELECTOR_IF.as_str())
            {
                Ok(Self::Branch(obj.extract::<Branch<T>>()?))
            } else {
                Ok(Self::Root(obj.extract::<T>()?))
            }
        }
    }

    impl<'py, T> IntoPyObject<'py> for Selector<T>
    where
        T: IntoPyObject<'py>,
    {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::Root(r) => r.into_bound_py_any(py),
                Self::Branch(b) => b.into_bound_py_any(py),
            }
        }
    }

    impl<'py, T> FromPyObject<'_, 'py> for AppendableSelector<T>
    where
        for<'b> T: FromPyObject<'b, 'py, Error = PyErr>,
    {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            // Order is important to keep error messages sane
            //
            // TODO the error won't say anything if something went wrong in the
            // list-of-selector case
            if let Ok(ss) = obj.extract::<Vec<Selector<T>>>()
                && let Some(ne) = NEVec::try_from_vec(ss)
            {
                return Ok(Self::Many(ne));
            }
            Ok(Self::One(obj.extract::<Selector<T>>()?))
        }
    }

    impl<'py, T> IntoPyObject<'py> for AppendableSelector<T>
    where
        T: IntoPyObject<'py>,
    {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::One(s) => s.into_bound_py_any(py),
                Self::Many(ss) => Vec::from(ss).into_bound_py_any(py),
            }
        }
    }

    // for some reason this can't be derived because of the Box
    impl<'py, T> FromPyObject<'_, 'py> for Branch<T>
    where
        for<'b> T: FromPyObject<'b, 'py, Error = PyErr>,
    {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Ok(i) = obj.extract::<If<T>>() {
                return Ok(Self::If(Box::new(i)));
            } else if let Ok(c) = obj.extract::<Cond<T>>() {
                return Ok(Self::Cond(c));
            }
            Err(fp::ConfigError::new_err(
                "Must be an if or a cond expression",
            ))
        }
    }

    impl<'py, T> IntoPyObject<'py> for Branch<T>
    where
        T: IntoPyObject<'py>,
    {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::If(i) => i.into_bound_py_any(py),
                Self::Cond(c) => c.into_bound_py_any(py),
            }
        }
    }

    impl<'py, T> FromPyObject<'_, 'py> for If<T>
    where
        for<'b> T: FromPyObject<'b, 'py, Error = PyErr>,
    {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Ok((op, condition, consequent, alternative)) =
                obj.extract::<(String, Condition, Selector<T>, Selector<T>)>()
                && op.as_str() == fp::SELECTOR_IF.as_str()
            {
                return Ok(Self {
                    condition,
                    consequent,
                    alternative: Some(alternative),
                });
            } else if let Ok((op, condition, consequent)) =
                obj.extract::<(String, Condition, Selector<T>)>()
                && op.as_str() == fp::SELECTOR_IF.as_str()
            {
                return Ok(Self {
                    condition,
                    consequent,
                    alternative: None,
                });
            }
            Err(fp::ConfigError::new_err(format!(
                "Must be a tuple like (\"{if_}\", <condition>, <consequent>, [<alternative>]) or \
                 (\"{if_}\", <condition>, <consequent>).",
                if_ = fp::SELECTOR_IF,
            )))
        }
    }

    impl<'py, T> IntoPyObject<'py> for If<T>
    where
        T: IntoPyObject<'py>,
    {
        type Target = PyTuple;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            const OP: &str = fp::SELECTOR_IF.as_str();
            if let Some(alternative) = self.alternative {
                (OP, self.condition, self.consequent, alternative).into_pyobject(py)
            } else {
                (OP, self.condition, self.consequent).into_pyobject(py)
            }
        }
    }

    impl<'py, T> FromPyObject<'_, 'py> for Cond<T>
    where
        for<'b> T: FromPyObject<'b, 'py, Error = PyErr>,
    {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let (op, forms) = obj.extract::<(String, Vec<(Condition, Selector<T>)>)>()?;
            if let Some(ne) = NEVec::try_from_vec(forms)
                && op == fp::SELECTOR_COND.as_str()
            {
                return Ok(Self { forms: ne });
            }
            Err(fp::ConfigError::new_err(format!(
                "Must be a nested tuple like (\"{}\", (<condition0>, <predicate0>), ...).",
                fp::SELECTOR_COND,
            )))
        }
    }

    impl<'py, T> IntoPyObject<'py> for Cond<T>
    where
        T: IntoPyObject<'py>,
    {
        type Target = PyTuple;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            (fp::SELECTOR_COND.as_str(), Vec::from(self.forms)).into_pyobject(py)
        }
    }

    impl<'py> FromPyObject<'_, 'py> for Condition {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Ok(s) = obj.extract::<KeyTest>() {
                return Ok(Self::Root(s));
            } else if let Ok((op, a, b)) = obj.extract::<(String, Self, Self)>() {
                if op == fp::CONDITION_AND.as_str() {
                    return Ok(Self::And(Box::new(a), Box::new(b)));
                }
                if op == fp::CONDITION_OR.as_str() {
                    return Ok(Self::Or(Box::new(a), Box::new(b)));
                }
            } else if let Ok((op, a)) = obj.extract::<(String, Self)>()
                && op == fp::CONDITION_NOT.as_str()
            {
                return Ok(Self::Not(Box::new(a)));
            }
            Err(fp::ConfigError::new_err(format!(
                "Must be a Statement or a tuple like (\"{}\", <predicate0>, ...), \
                 (\"{}\", <predicate0>, ...), or (\"{}\", <predicate0>)",
                fp::CONDITION_AND,
                fp::CONDITION_OR,
                fp::CONDITION_NOT,
            )))
        }
    }

    impl<'py> IntoPyObject<'py> for Condition {
        type Target = PyTuple;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            let go = |a: Self, b: Self, op: &NEStr| {
                let e0 = op.as_str().into_py_any(py)?;
                let e1 = a.into_py_any(py)?;
                let e2 = b.into_py_any(py)?;
                PyTuple::new(py, [e0, e1, e2])
            };
            match self {
                Self::Root(s) => s.into_pyobject(py),
                Self::And(a, b) => go(*a, *b, fp::CONDITION_AND),
                Self::Or(a, b) => go(*a, *b, fp::CONDITION_OR),
                Self::Not(p) => {
                    let op = fp::CONDITION_NOT.as_str().into_py_any(py)?;
                    let pred = p.into_py_any(py)?;
                    PyTuple::new(py, [op, pred])
                }
            }
        }
    }

    impl<'py> FromPyObject<'_, 'py> for KeyTest {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            if let Ok((op, key)) = obj.extract::<(String, AnyKey)>()
                && op == fp::STATEMENT_HAS_KEY.as_str()
            {
                return Ok(Self::HasKey(key));
            } else if let Ok((op, key, value)) = obj.extract::<(String, AnyKey, NEString)>()
                && op == fp::STATEMENT_KEY_IS.as_str()
            {
                return Ok(Self::KeyIs(key, value));
            } else if let Ok((op, key, pat)) = obj.extract::<(String, AnyKey, ValueRegex)>()
                && op == fp::STATEMENT_KEY_MATCHES.as_str()
            {
                return Ok(Self::KeyMatches(key, pat));
            }
            Err(fp::ConfigError::new_err(format!(
                "Must be a tuple like (\"{}\", <key>), (\"{}\", <key>, <value>), \
                 or (\"{}\", <key>, <pattern>)",
                fp::STATEMENT_HAS_KEY,
                fp::STATEMENT_KEY_IS,
                fp::STATEMENT_KEY_MATCHES,
            )))
        }
    }

    impl<'py> IntoPyObject<'py> for KeyTest {
        type Target = PyTuple;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            match self {
                Self::HasKey(k) => (fp::STATEMENT_HAS_KEY.as_str(), k).into_pyobject(py),
                Self::KeyIs(k, v) => (fp::STATEMENT_KEY_IS.as_str(), k, v).into_pyobject(py),
                Self::KeyMatches(k, p) => {
                    (fp::STATEMENT_KEY_MATCHES.as_str(), k, p).into_pyobject(py)
                }
            }
        }
    }
}
