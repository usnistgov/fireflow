use crate::index::{BiMeasIndex, IndexFromOne};
use crate::std_key::{
    CsvFlagKey, DfcKey, DollarStdKey, DollarStdKey_, DollarWrap0, GateKeyId, IndexedKey,
    NonPeakMeasKeyId, PeakMeasKeyId, PseudoNonStdKey, PseudoNonStdKeyError0, RegionKeyId,
    STD_PREFIX, StdKey, StdKeyError0,
};
use crate::sub_pattern::SubPattern;

use nonempty::{IntoNonEmptyIterator as _, NEVec, NonEmptyIterator as _};

use const_format::formatcp;
use derive_more::{AsRef, Display, From};
use hashbrown::HashMap;
use itertools::Itertools as _;
use regex::{Regex, RegexBuilder};
use thiserror::Error;

use std::collections::HashSet;
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;
use std::sync::LazyLock;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
};

/// A list of patterns that match keys.
#[derive(Clone, AsRef, PartialEq)]
pub struct StdKeysOrPatterns<const HAS_PRE: bool, T>(pub HashMap<StdKeyOrPattern<HAS_PRE>, T>);

impl<const DOLLAR: bool, T> Default for StdKeysOrPatterns<DOLLAR, T> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

pub struct StdKeysMatcher<'a, const HAS_PRE: bool, T> {
    // use hashmap since we can make non-unique patterns that produce the same
    // literal keys
    pub literals: HashMap<DollarStdKey_<HAS_PRE>, &'a T>,
    pub wildcards: Vec<(&'a StdWildcard, &'a T)>,
}

/// A list of patterns that match [`crate::validated::keys::StdKey`]s.
pub type StdKeyPatterns<const HAS_PRE: bool> = StdKeysOrPatterns<HAS_PRE, ()>;

/// A list of substitutions that match [`crate::validated::keys::StdKey`]s.
pub type SubPatterns = StdKeysOrPatterns<true, SubPattern>;

#[derive(From, Clone, PartialEq, Eq, Hash, Display, Debug)]
#[display(bound(DollarWrap0<HAS_PRE, StdKey>: fmt::Display))]
#[display(bound(DollarWrap0<HAS_PRE, StdIndexPattern>: fmt::Display))]
pub enum StdKeyOrPattern<const HAS_PRE: bool> {
    Key(DollarStdKey_<HAS_PRE>),
    Pattern(StdIndexPattern<HAS_PRE>),
}

pub type StdIndexPattern<const HAS_PRE: bool> = DollarWrap0<HAS_PRE, StdIndexPattern_>;

#[derive(From, Clone, PartialEq, Eq, Hash, Display, Debug)]
#[display("{PATTERN_DELIMITER}{_0}{PATTERN_DELIMITER}")]
pub enum StdIndexPattern_ {
    Discrete(StdIndexedKeys),
    Wildcard(StdWildcard),
}

#[derive(From, Clone, PartialEq, Eq, Hash, Display, Debug)]
pub enum StdWildcard {
    #[from]
    #[display("P[*]{}", _0.suffix())]
    AnyMeas(NonPeakMeasKeyId),
    #[from]
    #[display("{}[*]", _0.prefix())]
    AnyPeak(PeakMeasKeyId),
    #[from]
    #[display("G[*]{}", _0.suffix())]
    AnyGate(GateKeyId),
    #[from]
    #[display("R[*]{}", _0.suffix())]
    AnyRegion(RegionKeyId),
    #[display("CSV[*]FLAG")]
    AnyCsvFlag,
    #[display("DFC[*]TO[*]")]
    AnyDfc,
    #[display("DFC[{_0}]TO[*]")]
    AnyDfc1(StdIndices),
    #[display("DFC[*]TO[{_0}]")]
    AnyDfc2(StdIndices),
}

#[derive(Clone, PartialEq, Eq, Hash, Display, Debug)]
pub enum StdIndexedKeys {
    #[display("P[{_0}]{}", _1.suffix())]
    Meas(StdIndices, NonPeakMeasKeyId),
    #[display("{}[{_0}]", _1.prefix())]
    Peak(StdIndices, PeakMeasKeyId),
    #[display("G[{_0}]{}", _1.suffix())]
    Gate(StdIndices, GateKeyId),
    #[display("R[{_0}]{}", _1.suffix())]
    Region(StdIndices, RegionKeyId),
    #[display("CSV[{_0}]FLAG")]
    CsvFlag(StdIndices),
    #[display("DFC[{_0}]TO[{_1}]")]
    Dfc(StdIndices, StdIndices),
}

#[derive(Clone, PartialEq, Eq, Hash, AsRef, Debug)]
// NOTE sorted interior, hence private
pub struct StdIndices(NEVec<IndexFromOne>);

/// Error when creating a new hashtable with non-unique keys.
#[derive(Debug, Error, Display, PartialEq, Clone)]
#[display(
    "the following keys were non-unique when creating new hash table: {}",
    self.0.iter().join(","),
)]
#[display(bound(T: fmt::Display))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
#[cfg_attr(feature = "python", bound(T: fmt::Display))]
pub struct NonUniqueKeyError<T>(NEVec<T>);

/// Error when parsing literal or pattern string.
#[derive(Debug, Display, PartialEq, Error, Clone, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum StdKeyOrPatternError {
    Pattern(StdIndexPatternError),
    Literal(StdKeyError0),
    DollarLiteral(PseudoNonStdKeyError0),
}

/// Error when parsing [`StdIndexPattern`] from [`String`].
#[derive(Debug, Error, PartialEq, Clone)]
#[error(
    "could make indexed standard key pattern, must be like \
     '<prefix>[x0-x1]<suffix>' or '<prefix>[x0-x1]<middle>[y0-y1]<suffix>'"
)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct StdIndexPatternError;

impl<const HAS_PRE: bool> StdKeyOrPattern<HAS_PRE> {
    pub fn put_keys<'a, T: Copy>(
        &'a self,
        literals: &mut HashMap<DollarStdKey_<HAS_PRE>, T>,
        wildcards: &mut Vec<(&'a StdWildcard, T)>,
        value: T,
    ) {
        match self {
            Self::Key(k) => {
                let _ = literals.insert(*k, value);
            }
            Self::Pattern(p) => match p.as_ref() {
                StdIndexPattern_::Discrete(ks) => ks.put_keys(literals, value),
                StdIndexPattern_::Wildcard(w) => wildcards.push((w, value)),
            },
        }
    }
}

impl StdIndexedKeys {
    fn put_keys<const HAS_PRE: bool, T: Copy>(
        &self,
        keys: &mut HashMap<DollarStdKey_<HAS_PRE>, T>,
        value: T,
    ) {
        match self {
            Self::Meas(js, i) => {
                let it = js
                    .as_ref()
                    .into_nonempty_iter()
                    .map(|j| StdKey::Meas(IndexedKey::new((*j).into(), (*i).into())))
                    .map(DollarWrap0)
                    .map(|k| (k, value));
                keys.extend(it);
            }
            Self::Peak(js, i) => {
                let it = js
                    .as_ref()
                    .into_nonempty_iter()
                    .map(|j| StdKey::Meas(IndexedKey::new((*j).into(), (*i).into())))
                    .map(DollarWrap0)
                    .map(|k| (k, value));
                keys.extend(it);
            }
            Self::Gate(js, i) => {
                let it = js
                    .as_ref()
                    .into_nonempty_iter()
                    .map(|j| StdKey::Gate(IndexedKey::new((*j).into(), *i)))
                    .map(DollarWrap0)
                    .map(|k| (k, value));
                keys.extend(it);
            }
            Self::Region(js, i) => {
                let it = js
                    .as_ref()
                    .into_nonempty_iter()
                    .map(|j| StdKey::Region(IndexedKey::new((*j).into(), *i)))
                    .map(DollarWrap0)
                    .map(|k| (k, value));
                keys.extend(it);
            }
            Self::CsvFlag(js) => {
                let it = js
                    .as_ref()
                    .into_nonempty_iter()
                    .map(|j| StdKey::CsvFlag(CsvFlagKey::new((*j).into())))
                    .map(DollarWrap0)
                    .map(|k| (k, value));
                keys.extend(it);
            }
            Self::Dfc(js, ks) => {
                let js_ = js.as_ref().as_nonempty_slice().as_ref();
                let ks_ = ks.as_ref().as_nonempty_slice().as_ref();
                let it = js_
                    .iter()
                    .flat_map(|j| {
                        ks_.iter()
                            .map(|k| BiMeasIndex::new((*j).into(), (*k).into()))
                    })
                    .map(DfcKey::new)
                    .map(StdKey::Dfc)
                    .map(DollarWrap0)
                    .map(|k| (k, value));
                keys.extend(it);
            }
        }
    }
}

impl StdWildcard {
    #[must_use]
    pub fn is_match(&self, key: &StdKey) -> bool {
        match (self, key) {
            (Self::AnyMeas(id), StdKey::Meas(k)) => k.id.split_peak().is_ok_and(|i| i == *id),
            (Self::AnyPeak(id), StdKey::Meas(k)) => k.id.split_peak().is_err_and(|i| i == *id),
            (Self::AnyGate(id), StdKey::Gate(k)) => k.id == *id,
            (Self::AnyRegion(id), StdKey::Region(k)) => k.id == *id,
            (Self::AnyCsvFlag, StdKey::CsvFlag(_)) | (Self::AnyDfc, StdKey::Dfc(_)) => true,
            (Self::AnyDfc1(xs), StdKey::Dfc(k)) => xs.as_ref().contains(&k.index.i0.into()),
            (Self::AnyDfc2(xs), StdKey::Dfc(k)) => xs.as_ref().contains(&k.index.i1.into()),
            (_, _) => false,
        }
    }
}

impl<const HAS_PRE: bool, T> StdKeysOrPatterns<HAS_PRE, T> {
    #[must_use]
    pub fn as_matcher(&self) -> StdKeysMatcher<'_, HAS_PRE, T> {
        let mut literals = HashMap::new();
        let mut wildcards = vec![];

        for (k, v) in &self.0 {
            k.put_keys(&mut literals, &mut wildcards, v);
        }

        StdKeysMatcher {
            literals,
            wildcards,
        }
    }

    pub fn from_many(
        xs: impl IntoIterator<Item = Self>,
    ) -> Result<Self, NonUniqueKeyError<StdKeyOrPattern<HAS_PRE>>> {
        checked_iter_to_hashmap(xs.into_iter().flat_map(|x| x.0.into_iter())).map(Self)
    }
}

impl<const HAS_PRE: bool> StdKeysMatcher<'_, HAS_PRE, ()> {
    #[must_use]
    pub fn is_wildcard_match(&self, key: &StdKey) -> bool {
        self.get_wildcard(key).is_some()
    }
}

impl<'a, const HAS_PRE: bool, T> StdKeysMatcher<'a, HAS_PRE, T> {
    #[must_use]
    pub fn get_wildcard(&self, key: &StdKey) -> Option<&'a T> {
        self.wildcards
            .iter()
            .find_map(|(w, v)| w.is_match(key).then_some(*v))
    }

    #[must_use]
    pub fn has_wildcards(&self) -> bool {
        !self.wildcards.is_empty()
    }
}

impl<const DOLLAR: bool> FromIterator<StdKeyOrPattern<DOLLAR>> for StdKeyPatterns<DOLLAR> {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = StdKeyOrPattern<DOLLAR>>,
    {
        Self(iter.into_iter().map(|x| (x, ())).collect())
    }
}

impl FromIterator<(StdKeyOrPattern<true>, SubPattern)> for SubPatterns {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (StdKeyOrPattern<true>, SubPattern)>,
    {
        Self(iter.into_iter().collect())
    }
}

enum Idx {
    Any,
    Discrete(StdIndices),
}

// TODO this can be cleaned up
impl FromStr for StdKeyOrPattern<true> {
    type Err = StdKeyOrPatternError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(inner) = s
            .strip_prefix(PATTERN_DELIMITER)
            .and_then(|x| x.strip_suffix(PATTERN_DELIMITER))
        {
            if let Some((b, bs)) = inner.as_bytes().split_first()
                && *b == STD_PREFIX
            {
                let ss = str::from_utf8(bs).expect("stripping prefix shouldn't break utf8");
                let p = StdIndexPattern_::from_str(ss).ok_or(StdIndexPatternError)?;
                Ok(Self::Pattern(DollarWrap0(p)))
            } else {
                Err(StdIndexPatternError.into())
            }
        } else {
            Ok(Self::Key(s.parse::<DollarStdKey>()?))
        }
    }
}

impl FromStr for StdKeyOrPattern<false> {
    type Err = StdKeyOrPatternError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(inner) = s
            .strip_prefix(PATTERN_DELIMITER)
            .and_then(|x| x.strip_suffix(PATTERN_DELIMITER))
        {
            if inner.as_bytes().first().is_some_and(|b| STD_PREFIX != *b) {
                let p = StdIndexPattern_::from_str(inner).ok_or(StdIndexPatternError)?;
                Ok(Self::Pattern(DollarWrap0(p)))
            } else {
                Err(StdIndexPatternError.into())
            }
        } else {
            Ok(Self::Key(s.parse::<PseudoNonStdKey>()?))
        }
    }
}

impl StdIndexPattern_ {
    fn from_str(s: &str) -> Option<Self> {
        static MEAS_RE: LazyLock<Regex> = LazyLock::new(|| build_regex(MEAS_PATTERN));
        static GATE_RE: LazyLock<Regex> = LazyLock::new(|| build_regex(GATE_PATTERN));
        static REGION_RE: LazyLock<Regex> = LazyLock::new(|| build_regex(REGION_PATTERN));
        static PK_RE: LazyLock<Regex> = LazyLock::new(|| build_regex(PK_PATTERN));
        static PKN_RE: LazyLock<Regex> = LazyLock::new(|| build_regex(PKN_PATTERN));
        static CSV_RE: LazyLock<Regex> = LazyLock::new(|| build_regex(CSV_FLAG_PATTERN));
        static DFC_RE: LazyLock<Regex> = LazyLock::new(|| build_regex(DFC_PATTERN));

        if let Some(cap) = MEAS_RE.captures(s) {
            let idx = cap.get(1)?.as_str();
            let id = NonPeakMeasKeyId::from_bytes(cap.get(2)?.as_str().as_bytes())?;
            Self::from_index(idx, id, StdIndexedKeys::Meas)
        } else if let Some(cap) = GATE_RE.captures(s) {
            let idx = cap.get(1)?.as_str();
            let id = GateKeyId::from_bytes(cap.get(2)?.as_str().as_bytes())?;
            Self::from_index(idx, id, StdIndexedKeys::Gate)
        } else if let Some(cap) = REGION_RE.captures(s) {
            let idx = cap.get(1)?.as_str();
            let id = RegionKeyId::from_bytes(cap.get(2)?.as_str().as_bytes())?;
            Self::from_index(idx, id, StdIndexedKeys::Region)
        } else if let Some(cap) = PK_RE.captures(s) {
            let idx = cap.get(1)?.as_str();
            Self::from_index(idx, PeakMeasKeyId::Pk, StdIndexedKeys::Peak)
        } else if let Some(cap) = PKN_RE.captures(s) {
            let idx = cap.get(1)?.as_str();
            Self::from_index(idx, PeakMeasKeyId::Pkn, StdIndexedKeys::Peak)
        } else if let Some(cap) = CSV_RE.captures(s) {
            match Idx::from_str(cap.get(1)?.as_str())? {
                Idx::Any => Some(Self::Wildcard(StdWildcard::AnyCsvFlag)),
                Idx::Discrete(xs) => Some(Self::Discrete(StdIndexedKeys::CsvFlag(xs))),
            }
        } else if let Some(cap) = DFC_RE.captures(s) {
            let i0 = Idx::from_str(cap.get(1)?.as_str());
            let i1 = Idx::from_str(cap.get(2)?.as_str());
            match i0.zip(i1)? {
                (Idx::Any, Idx::Any) => Some(Self::Wildcard(StdWildcard::AnyDfc)),
                (Idx::Discrete(xs), Idx::Any) => Some(Self::Wildcard(StdWildcard::AnyDfc1(xs))),
                (Idx::Any, Idx::Discrete(xs)) => Some(Self::Wildcard(StdWildcard::AnyDfc2(xs))),
                (Idx::Discrete(xs), Idx::Discrete(ys)) => {
                    Some(Self::Discrete(StdIndexedKeys::Dfc(xs, ys)))
                }
            }
        } else {
            None
        }
    }

    fn from_index<K, F>(s: &str, id: K, f: F) -> Option<Self>
    where
        K: Into<StdWildcard> + Copy,
        F: FnOnce(StdIndices, K) -> StdIndexedKeys,
    {
        match Idx::from_str(s)? {
            Idx::Any => Some(Self::Wildcard(id.into())),
            Idx::Discrete(xs) => Some(Self::Discrete(f(xs, id))),
        }
    }
}

impl Idx {
    fn from_str(s: &str) -> Option<Self> {
        if s == "*" {
            Some(Self::Any)
        } else {
            StdIndices::from_str(s).map(Self::Discrete)
        }
    }
}

impl StdIndices {
    fn from_str(s: &str) -> Option<Self> {
        let mut indices = vec![];
        for inner in s.split(',') {
            let xs: Vec<_> = inner.split('-').collect();
            let rng = match &xs[..] {
                [x0] => x0
                    .parse::<IndexFromOne>()
                    .ok()
                    .map(usize::from)
                    .map(|y| y..y),
                [x0, x1] => {
                    let y0 = x0.parse::<IndexFromOne>().ok()?;
                    let y1 = x1.parse::<IndexFromOne>().ok()?;
                    Some(usize::from(y0)..usize::from(y1))
                }
                _ => None,
            };
            indices.extend(rng?.map(IndexFromOne::from));
        }
        indices.sort();
        indices.dedup();
        NEVec::try_from_vec(indices).map(Self)
    }
}

impl fmt::Display for StdIndices {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let (x0, xs) = (&self.0).into_nonempty_iter().next();
        let mut start = x0;
        let mut end = x0;
        let mut ranges = vec![];
        for x in xs {
            if usize::from(*x) > usize::from(*end) + 1 {
                ranges.push((start, end));
                start = x;
            }
            end = x;
        }
        ranges.push((start, end));
        let s = ranges
            .iter()
            .map(|(a, b)| {
                if a == b {
                    a.to_string()
                } else {
                    format!("{a}-{b}")
                }
            })
            .join(",");
        f.write_str(s.as_str())
    }
}

// TODO put me somewhere useful
pub fn checked_iter_to_hashmap<K, V>(
    xs: impl IntoIterator<Item = (K, V)>,
) -> Result<HashMap<K, V>, NonUniqueKeyError<K>>
where
    K: Hash + Clone + Eq,
{
    let mut duplicated = HashSet::new();
    let mut new = HashMap::new();
    for (k, v) in xs {
        if new.contains_key(&k) {
            duplicated.insert(k);
        } else {
            new.insert(k, v);
        }
    }
    let multi: Vec<_> = duplicated.into_iter().collect();
    if let Some(ne) = NEVec::try_from_vec(multi) {
        return Err(NonUniqueKeyError(ne));
    }
    Ok(new)
}

fn build_regex(pat: &str) -> Regex {
    RegexBuilder::new(pat)
        .case_insensitive(true)
        .build()
        .unwrap()
}

pub const PATTERN_DELIMITER: char = '/';

const MEAS_PATTERN: &str = formatcp!("^P{INDEX_PATTERN}([A-Z]+)$");

const GATE_PATTERN: &str = formatcp!("^G{INDEX_PATTERN}([A-Z]+)$");

const REGION_PATTERN: &str = formatcp!("^R{INDEX_PATTERN}([A-Z]+)$");

const PK_PATTERN: &str = formatcp!("^PK{INDEX_PATTERN}$");

const PKN_PATTERN: &str = formatcp!("^PKN{INDEX_PATTERN}$");

const CSV_FLAG_PATTERN: &str = formatcp!("^CSV{INDEX_PATTERN}FLAG$");

const DFC_PATTERN: &str = formatcp!("^DFC{INDEX_PATTERN}TO{INDEX_PATTERN}$");

const INDEX_PATTERN: &str = "\\[([0-9,-\\\\*]+)\\]";

#[cfg(feature = "serde")]
mod serialize {
    use super::*;

    use serde::{Serialize, Serializer};

    impl<const DOLLAR: bool> Serialize for StdKeyPatterns<DOLLAR> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let inner: Vec<_> = self.0.iter().map(|(x, ())| x).collect();
            inner.serialize(serializer)
        }
    }

    impl Serialize for SubPatterns {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            self.0.serialize(serializer)
        }
    }

    impl<const DOLLAR: bool> Serialize for StdKeyOrPattern<DOLLAR> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.collect_str(self)
        }
    }
}

#[cfg(feature = "python")]
mod python {

    use super::{StdKeyOrPattern, StdKeyPatterns, SubPatterns};

    use hashbrown::HashMap;

    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyString};

    use std::convert::Infallible;
    use std::str::FromStr;

    impl<'py, const HAS_PRE: bool> FromPyObject<'_, 'py> for StdKeyOrPattern<HAS_PRE>
    where
        Self: FromStr,
        PyErr: From<<Self as FromStr>::Err>,
    {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            Ok(obj.extract::<String>()?.parse()?)
        }
    }

    impl<'py, const HAS_PRE: bool> IntoPyObject<'py> for StdKeyOrPattern<HAS_PRE> {
        type Target = PyString;
        type Output = Bound<'py, Self::Target>;
        type Error = Infallible;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.to_string().into_pyobject(py)
        }
    }

    impl<'py, const HAS_PRE: bool> FromPyObject<'_, 'py> for StdKeyPatterns<HAS_PRE>
    where
        for<'a> StdKeyOrPattern<HAS_PRE>: FromPyObject<'a, 'py>,
    {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            let xs: Vec<StdKeyOrPattern<HAS_PRE>> = obj.extract()?;
            Ok(Self(xs.into_iter().map(|x| (x, ())).collect()))
        }
    }

    impl<'py, const HAS_PRE: bool> IntoPyObject<'py> for StdKeyPatterns<HAS_PRE> {
        type Target = PyAny;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.0.keys().cloned().collect::<Vec<_>>().into_pyobject(py)
        }
    }

    impl<'py> FromPyObject<'_, 'py> for SubPatterns {
        type Error = PyErr;
        fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
            Ok(Self(obj.extract::<HashMap<_, _>>()?))
        }
    }

    impl<'py> IntoPyObject<'py> for SubPatterns {
        type Target = PyDict;
        type Output = Bound<'py, Self::Target>;
        type Error = PyErr;

        fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
            self.0.into_pyobject(py)
        }
    }
}
