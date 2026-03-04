#![allow(clippy::as_conversions)]

use ambassador::delegatable_trait;
use bigdecimal::BigDecimal;
use derive_more::{AsRef, Display, From, Into};
use derive_new::new;
use nonempty_collections::{
    FromNonEmptyIterator, IntoNonEmptyIterator, NESlice, NEVec, NonEmptyArrayExt,
    NonEmptyIterator as _,
};
use thiserror::Error;

use std::fmt;
use std::hash::Hash;
use std::num::{NonZeroU8, NonZeroU32};
use std::ptr::from_ref;
use std::slice;
use std::str::{FromStr, Utf8Error};
use std::{borrow::Borrow, num::NonZeroUsize};

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    crate::python as py,
    fireflow_core_proc::{DisplayAsPyErr, FromPyString},
    pyo3::prelude::*,
};

/// A string slice which can never be empty.
#[derive(AsRef, Display)]
#[repr(transparent)]
pub struct NEStr(str);

/// A string which can never be empty.
#[derive(Clone, PartialEq, Eq, Hash, Default, Display, Into, Debug, AsRef)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(IntoPyObject, FromPyString))]
#[as_ref(str)]
pub struct NEString(String);

#[derive(Clone, Copy, new)]
pub struct NEConcat<A, B>(A, B);
pub type NEConcatR<A, B> = NEConcat<A, Option<B>>;
pub type NEConcatL<A, B> = NEConcat<Option<B>, A>;

pub type NEConcat3<A, B, C> = NEConcat<NEConcat<A, B>, C>;
pub type NEConcat4<A, B, C, D> = NEConcat<NEConcat3<A, B, C>, D>;
pub type NEConcat5<A, B, C, D, E> = NEConcat<NEConcat4<A, B, C, D>, E>;

/// A [`u64`] that is 0-padded.
///
/// This only works for [`u64`] (for now) because there is no generic trait
/// for log10.
#[derive(Clone, Copy)]
pub struct PaddedU64 {
    pub pad: u32,
    pub value: u64,
}

pub enum NEAlt<A, B> {
    Left(A),
    Right(B),
}

#[derive(new)]
pub struct NEDelim<I> {
    delim: char,
    inner: I,
}

impl<A, B> NEConcat<A, B> {
    pub fn prepend<C>(self, x: C) -> NEConcat<C, Self> {
        NEConcat::new(x, self)
    }

    pub fn append<C>(self, x: C) -> NEConcat<Self, C> {
        NEConcat::new(self, x)
    }
}

impl Borrow<NEStr> for NEString {
    fn borrow(&self) -> &NEStr {
        NEStr::new_unchecked(self.0.as_str())
    }
}

impl ToOwned for NEStr {
    type Owned = NEString;
    fn to_owned(&self) -> Self::Owned {
        NEString(self.0.to_string())
    }
}

#[macro_export]
macro_rules! ne_str {
    ($s:expr) => {{
        const _: () = assert!(!$s.is_empty(), "string cannot be empty");
        $crate::nonempty_string::NEStr::try_new($s).unwrap()
    }};
}

impl FromNonEmptyIterator<char> for NEString {
    fn from_nonempty_iter<I>(iter: I) -> Self
    where
        I: IntoNonEmptyIterator<Item = char>,
    {
        let (x0, xs) = iter.into_nonempty_iter().next();
        let mut s = String::from(x0);
        s.extend(xs);
        Self(s)
    }
}

impl NEString {
    pub fn push(&mut self, c: char) {
        self.0.push(c);
    }

    pub fn push_str(&mut self, s: &str) {
        self.0.push_str(s);
    }

    #[must_use]
    pub fn len(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.0.len()).unwrap()
    }

    #[must_use]
    pub fn as_ne_str(&self) -> &NEStr {
        NEStr::new_unchecked(self.as_ref())
    }

    /// Like [`String::from_utf8_unchecked`] but requires a [`NEVec<u8>`].
    ///
    /// # Safety
    ///
    /// The user must ensure bytes are valid UTF-8.
    #[must_use]
    pub unsafe fn from_utf8_unchecked(bytes: NEVec<u8>) -> Self {
        // SAFETY: unsafe function
        let ret = unsafe { String::from_utf8_unchecked(bytes.into()) };
        Self(ret)
    }
}

impl NEStr {
    #[must_use]
    pub const fn try_new(s: &str) -> Option<&Self> {
        if s.is_empty() {
            None
        } else {
            Some(Self::new_unchecked(s))
        }
    }

    pub fn from_utf8<'a>(bytes: &'a NESlice<u8>) -> Result<&'a Self, Utf8Error> {
        Ok(Self::new_unchecked(str::from_utf8(bytes.as_ref())?))
    }

    #[must_use]
    pub const fn len(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.0.len()).unwrap()
    }

    #[must_use]
    pub const fn as_str(&self) -> &str {
        let p: *const Self = from_ref(self);
        // SAFETY: NEStr and str have same layout
        unsafe { &*(p as *const str) }
    }

    const fn new_unchecked(s: &str) -> &Self {
        let p: *const str = from_ref(s);
        // SAFETY: NEStr and str have same layout
        unsafe { &*(p as *const Self) }
    }
}

impl From<&NEStr> for NEString {
    fn from(value: &NEStr) -> Self {
        Self(value.as_str().to_owned())
    }
}

impl TryFrom<String> for NEString {
    type Error = NonEmptyStringError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            Err(NonEmptyStringError)
        } else {
            Ok(Self(value))
        }
    }
}

impl FromStr for NEString {
    type Err = NonEmptyStringError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s.to_owned())
    }
}

/// Error when parsing [`NonEmptyString`] from empty [`String`]
#[derive(Error, Debug)]
#[error("string cannot be empty")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ParseKeywordValueError))]
pub struct NonEmptyStringError;

#[delegatable_trait]
pub trait ToDisplayNE<'a> {
    type NE: DisplayNE;

    fn to_ne(&'a self) -> Self::NE;
}

/// Allows a type with [`ToDisplayNE`] to be displayed with [`DisplayNE`].
#[derive(From)]
#[repr(transparent)]
pub struct NED<T>(pub T);

impl<T> NED<T> {
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn on_inner_slice(s: NESlice<'_, T>) -> NESlice<'_, Self> {
        let n = s.len().get();
        let p = s.as_ref().as_ptr();
        // SAFETY: NED is a zero-sized type so this is a noop
        let ne = unsafe { slice::from_raw_parts(p.cast::<Self>(), n) };
        NESlice::try_from_slice(ne).unwrap()
    }

    #[allow(clippy::needless_pass_by_value)]
    fn with_ref(x: &T) -> &Self {
        let p: *const T = from_ref(x);
        // SAFETY: NEStr and str have same layout
        unsafe { &*(p.cast::<Self>()) }
    }
}

pub struct NEWrap<T>(pub T);

impl<T: DisplayNE> fmt::Display for NEWrap<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_ne(f)
    }
}

pub trait DisplayNE {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result;

    fn fmt_ne(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct CheckedFormatter<'a, 'b> {
            not_empty: bool,
            inner: &'a mut fmt::Formatter<'b>,
        }

        impl fmt::Write for CheckedFormatter<'_, '_> {
            fn write_str(&mut self, s: &str) -> fmt::Result {
                self.not_empty = !s.is_empty();
                self.inner.write_str(s)
            }
        }

        let mut xf = CheckedFormatter {
            not_empty: false,
            inner: f,
        };
        let ret = self.fmt_ne_inner(&mut xf);
        debug_assert!(xf.not_empty, "written string is empty");
        ret
    }

    fn to_ne_string(&self) -> NEString
    where
        Self: Sized,
    {
        struct DisplayWrapper<'a, T>(&'a T);

        impl<T: DisplayNE> fmt::Display for DisplayWrapper<'_, T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt_ne(f)
            }
        }

        NEString(DisplayWrapper(self).to_string())
    }
}

macro_rules! impl_to_display_ne_copy {
    ($t:ident) => {
        impl ToDisplayNE<'_> for $t {
            type NE = Self;
            fn to_ne(&self) -> Self {
                *self
            }
        }
    };
}

impl_to_display_ne_copy!(char);
impl_to_display_ne_copy!(usize);
impl_to_display_ne_copy!(u8);
impl_to_display_ne_copy!(u16);
impl_to_display_ne_copy!(u32);
impl_to_display_ne_copy!(u64);
impl_to_display_ne_copy!(NonZeroUsize);
impl_to_display_ne_copy!(NonZeroU8);
impl_to_display_ne_copy!(NonZeroU32);
impl_to_display_ne_copy!(f32);
impl_to_display_ne_copy!(f64);
impl_to_display_ne_copy!(PaddedU64);

macro_rules! impl_to_display_ne_ref {
    ($t:ident) => {
        impl<'a> ToDisplayNE<'a> for $t {
            type NE = &'a Self;
            fn to_ne(&'a self) -> Self::NE {
                self
            }
        }
    };
}

impl_to_display_ne_ref!(BigDecimal);

impl<'a, T: ToDisplayNE<'a> + ?Sized> ToDisplayNE<'a> for &T {
    type NE = T::NE;
    fn to_ne(&'a self) -> Self::NE {
        T::to_ne(*self)
    }
}

impl<'a, A, B> ToDisplayNE<'a> for NEConcat<A, B>
where
    for<'b> A: ToDisplayNE<'b> + 'a,
    for<'b> B: ToDisplayNE<'b> + 'a,
{
    type NE = NEConcat<&'a NED<A>, &'a NED<B>>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat(NED::with_ref(&self.0), NED::with_ref(&self.1))
    }
}

impl<'a, A, B> ToDisplayNE<'a> for NEAlt<A, B>
where
    for<'b> A: ToDisplayNE<'b> + 'a,
    for<'b> B: ToDisplayNE<'b> + 'a,
{
    type NE = NEAlt<&'a NED<A>, &'a NED<B>>;
    fn to_ne(&'a self) -> Self::NE {
        match self {
            Self::Left(x) => NEAlt::Left(NED::with_ref(x)),
            Self::Right(x) => NEAlt::Right(NED::with_ref(x)),
        }
    }
}

impl<'a, T> ToDisplayNE<'a> for Box<T>
where
    for<'b> T: ToDisplayNE<'b> + 'a,
{
    type NE = &'a Self;
    fn to_ne(&'a self) -> Self::NE {
        self
    }
}

impl<T> DisplayNE for Box<T>
where
    for<'b> T: ToDisplayNE<'b>,
{
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        self.as_ref().to_ne().fmt_ne_inner(f)
    }
}

impl<'a, T> ToDisplayNE<'a> for NESlice<'a, T>
where
    for<'b> T: ToDisplayNE<'b> + 'a,
{
    type NE = NESlice<'a, NED<T>>;
    fn to_ne(&'a self) -> Self::NE {
        NED::on_inner_slice(ne_slice_by_ref(self))
    }
}

impl<'a, T> ToDisplayNE<'a> for NEVec<T>
where
    for<'b> T: ToDisplayNE<'b> + 'a,
{
    type NE = NESlice<'a, NED<T>>;
    fn to_ne(&'a self) -> Self::NE {
        NED::on_inner_slice(self.as_nonempty_slice())
    }
}

impl<'a, T> ToDisplayNE<'a> for NEDelim<NEVec<T>>
where
    for<'b> T: ToDisplayNE<'b> + 'a,
{
    type NE = NEDelim<NESlice<'a, NED<T>>>;
    fn to_ne(&'a self) -> Self::NE {
        let xs = NED::on_inner_slice(self.inner.as_nonempty_slice());
        NEDelim::new(self.delim, xs)
    }
}

impl<'a, T, const LEN: usize> ToDisplayNE<'a> for NEDelim<[T; LEN]>
where
    [T; LEN]: NonEmptyArrayExt<T>,
    for<'b> T: ToDisplayNE<'b> + 'a,
{
    type NE = NEDelim<NESlice<'a, NED<T>>>;
    fn to_ne(&'a self) -> Self::NE {
        let xs = NED::on_inner_slice(self.inner.as_nonempty_slice());
        NEDelim::new(self.delim, xs)
    }
}

impl<'a, T> ToDisplayNE<'a> for NEDelim<NESlice<'a, T>>
where
    for<'b> T: ToDisplayNE<'b>,
{
    type NE = NEDelim<NESlice<'a, NED<T>>>;
    fn to_ne(&'a self) -> Self::NE {
        let xs = NED::on_inner_slice(ne_slice_by_ref(&self.inner));
        NEDelim::new(self.delim, xs)
    }
}

impl<'a> ToDisplayNE<'a> for NEString {
    type NE = &'a NEStr;
    fn to_ne(&'a self) -> Self::NE {
        self.as_ne_str()
    }
}

impl<'a> ToDisplayNE<'a> for NEStr {
    type NE = &'a Self;
    fn to_ne(&'a self) -> Self::NE {
        self
    }
}

impl<T> DisplayNE for NED<T>
where
    for<'a> T: ToDisplayNE<'a>,
{
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        self.0.to_ne().fmt_ne_inner(f)
    }
}

impl<T: DisplayNE> DisplayNE for &T {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        T::fmt_ne_inner(self, f)
    }
}

macro_rules! impl_display_ne {
    ($t:ident) => {
        impl DisplayNE for $t {
            fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
                write!(f, "{self}")
            }
        }
    };
}

impl_display_ne!(char);
impl_display_ne!(usize);
impl_display_ne!(u8);
impl_display_ne!(u16);
impl_display_ne!(u32);
impl_display_ne!(u64);
impl_display_ne!(f32);
impl_display_ne!(f64);
impl_display_ne!(NonZeroUsize);
impl_display_ne!(NonZeroU8);
impl_display_ne!(NonZeroU32);
impl_display_ne!(NEString);
impl_display_ne!(BigDecimal);

impl DisplayNE for &NEStr {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl<A: DisplayNE, B: DisplayNE> DisplayNE for NEConcat<A, B> {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        self.0.fmt_ne_inner(f)?;
        self.1.fmt_ne_inner(f)?;
        Ok(())
    }
}

impl<A: DisplayNE, B: DisplayNE> DisplayNE for NEConcatR<A, B> {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        self.0.fmt_ne_inner(f)?;
        if let Some(x) = self.1.as_ref() {
            x.fmt_ne_inner(f)?;
        }
        Ok(())
    }
}

impl<A: DisplayNE, B: DisplayNE> DisplayNE for NEConcatL<A, B> {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        if let Some(x) = self.0.as_ref() {
            x.fmt_ne_inner(f)?;
        }
        self.1.fmt_ne_inner(f)?;
        Ok(())
    }
}

impl<A: DisplayNE, B: DisplayNE> DisplayNE for NEAlt<A, B> {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        match self {
            Self::Left(x) => x.fmt_ne_inner(f),
            Self::Right(x) => x.fmt_ne_inner(f),
        }
    }
}

impl<T: DisplayNE> DisplayNE for NESlice<'_, T> {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        for x in self {
            x.fmt_ne_inner(f)?;
        }
        Ok(())
    }
}

impl<T: DisplayNE> DisplayNE for NEDelim<NESlice<'_, T>> {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        let c = self.delim;
        let (x0, xs) = self.inner.nonempty_iter().next();
        x0.fmt_ne_inner(f)?;
        for x in xs {
            write!(f, "{c}")?;
            x.fmt_ne_inner(f)?;
        }
        Ok(())
    }
}

impl<T: DisplayNE> DisplayNE for NEVec<T> {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        self.as_nonempty_slice().fmt_ne_inner(f)
    }
}

impl<T, const LEN: usize> DisplayNE for NEDelim<[T; LEN]>
where
    [T; LEN]: NonEmptyArrayExt<T>,
    T: DisplayNE,
{
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        NEDelim::new(self.delim, self.inner.as_nonempty_slice()).fmt_ne_inner(f)
    }
}

impl<T: DisplayNE> DisplayNE for NEDelim<NEVec<T>> {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        NEDelim::new(self.delim, self.inner.as_nonempty_slice()).fmt_ne_inner(f)
    }
}

// TODO testme
impl DisplayNE for PaddedU64 {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        let n_digits = self.value.checked_ilog10().unwrap_or_default() + 1;
        let n_pad = self.pad.saturating_sub(n_digits);
        for _ in 0..n_pad {
            f.write_char('0')?;
        }
        write!(f, "{}", self.value)
    }
}

#[must_use]
pub fn ne_slice_by_ref<'a, T>(s: &'a NESlice<'a, T>) -> NESlice<'a, T> {
    // TODO this should be fixed upstream
    //
    // This is the equivalent of converting &Option<T> to Option<&T> (ie
    // 'flipping' the borrow) which should be a noop and shouldn't require
    // using a failable try_* method.
    NESlice::try_from_slice(s.as_ref()).unwrap()
}
