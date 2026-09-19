use super::{NESlice, NEStr, NEString, NEVec, NonEmptyArrayExt, NonEmptyIterator as _};

use ambassador::delegatable_trait;
use bigdecimal::BigDecimal;
use derive_more::From;
use derive_new::new;

use std::{
    fmt,
    num::NonZeroUsize,
    num::{NonZeroU8, NonZeroU32},
    ptr::from_ref,
    slice,
};

use sealed::DisplayNEInner;

/// Allows a type with [`DisplayNE`] to be formatted via [`fmt::Display`].
pub struct NEWrap<T>(pub T);

impl<T: DisplayNE> fmt::Display for NEWrap<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_ne(f)
    }
}

/// Allows a type with [`ToDisplayNE`] to be displayed with [`DisplayNE`].
#[derive(From)]
#[repr(transparent)]
pub struct ToNE<T>(pub T);

impl<T> ToNE<T> {
    /// Wrap inner type on a borrowed slice.
    #[must_use]
    #[allow(clippy::needless_pass_by_value)]
    pub fn on_inner_slice(s: &NESlice<T>) -> &NESlice<Self> {
        let n = s.len().get();
        let p = s.as_ref().as_ptr();
        // SAFETY: target is a transparent type so this is a noop
        let ne = unsafe { slice::from_raw_parts(p.cast::<Self>(), n) };
        NESlice::try_from_slice(ne).unwrap()
    }

    /// Wrap a borrowed type.
    fn with_ref(x: &T) -> &Self {
        let p: *const T = from_ref(x);
        // SAFETY: Self is a transparent wrapper, layout is the same
        unsafe { &*(p.cast::<Self>()) }
    }
}

/// Combines to alternative types which implement [`DisplayNE`].
///
/// Types will be displayed like `{A}` or `{B}` depending on the variant.
///
/// Note if the inner types do not inherently implement [`DisplayNE`] but do
/// implement [`ToDisplayNE`], wrapping in [`ToNE`] will provide [`DisplayNE`].
pub enum NEAlt<A, B> {
    Left(A),
    Right(B),
}

/// Combines two types which implement [`DisplayNE`].
///
/// Types will be displayed like `{A}{B}`.
///
/// Note if the inner types do not inherently implement [`DisplayNE`] but do
/// implement [`ToDisplayNE`], wrapping in [`ToNE`] will provide [`DisplayNE`].
#[derive(Clone, Copy, new)]
pub struct NEConcat<A, B>(A, B);

impl<A, B> NEConcat<A, B> {
    pub fn prepend<C>(self, x: C) -> NEConcat<C, Self> {
        NEConcat::new(x, self)
    }

    pub fn append<C>(self, x: C) -> NEConcat<Self, C> {
        NEConcat::new(self, x)
    }
}

/// Combines two types which implement [`DisplayNE`] where the right may not exist.
pub type NEConcatR<A, B> = NEConcat<A, Option<B>>;

/// Combines two types which implement [`DisplayNE`] where the left may not exist.
pub type NEConcatL<A, B> = NEConcat<Option<B>, A>;

/// Combines three types which implement [`DisplayNE`].
pub type NEConcat3<A, B, C> = NEConcat<NEConcat<A, B>, C>;

/// Combines four types which implement [`DisplayNE`].
pub type NEConcat4<A, B, C, D> = NEConcat<NEConcat3<A, B, C>, D>;

/// Combines five types which implement [`DisplayNE`].
pub type NEConcat5<A, B, C, D, E> = NEConcat<NEConcat4<A, B, C, D>, E>;

/// A [`u64`] that is 0-padded.
///
/// This only works for [`u64`] (for now) because there is no generic trait
/// for log10.
#[derive(Clone, Copy, new)]
pub struct PaddedU64 {
    pub pad: u32,
    pub pad_char: char,
    pub value: u64,
}

/// A non-empty type which can be delimited with a character.
///
/// In practice this will be a slice-like collection.
#[derive(new)]
pub struct NEDelim<I> {
    delim: char,
    inner: I,
}

/// A type which is formatted to at least one character.
///
/// In principle, this is a wrapper around [`fmt::Display`] and is only
/// implemented for a subset of types that are guaranteed to produce one char or
/// more. Due to this restriction, the inner code is not callable or
/// implementable outside this module.
///
/// Types can be converted into other types which implement this interface via
/// [`ToDisplayNE`] which is the meant to be the only way in which this trait
/// is accessed.
pub trait DisplayNE: sealed::DisplayNEInner {
    fn to_ne_string(&self) -> NEString {
        struct DisplayWrapper<'a, T: ?Sized>(&'a T);

        impl<T: DisplayNE + ?Sized> fmt::Display for DisplayWrapper<'_, T> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt_ne(f)
            }
        }

        NEString::try_from(DisplayWrapper(self).to_string()).unwrap()
    }
}

mod sealed {
    use std::fmt;

    pub trait DisplayNEInner {
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
            assert!(xf.not_empty, "written string is empty");
            ret
        }
    }
}

/// Convert a type to one that implemements [`DisplayNE`].
#[delegatable_trait]
pub trait ToDisplayNE<'a> {
    // TODO this entire mod can probably be cleaned up once this issue is
    // solved: https://github.com/rust-lang/rust/issues/87479. This will let us
    // put a lifetime bound on NE instead of at the trait level. This in turn
    // will let us to remove all the 'for<'a> T: bla bla' bounds, which in turn
    // will likely allow us to eliminate the ToNE and put all bounds in terms of
    // ToDisplayNE (In the case of DisplayNE, the inner types can simply be
    // unwrapped using ToDisplayNE). This is basically impossible now due to
    // limitations in how rust scopes lifetime parameters. For instance, any
    // type which takes two types (NEConcat, NEAlt) will need to have two
    // lifetime params in the NE type if we were to sub ToDisplayNE::NE for each
    // inner type. These two lifetimes currently need to be set to the trait
    // level, which is overly constrained.
    type NE: DisplayNE;

    fn to_ne(&'a self) -> Self::NE;
}

/// Directly format something that has [`ToDisplay`].
pub trait DisplayableNE<'a>: Sized + ToDisplayNE<'a> {
    fn as_displayable(&'a self) -> NEWrap<Self::NE>
    where
        Self::NE: Sized,
    {
        NEWrap(self.to_ne())
    }

    fn as_string(&'a self) -> String
    where
        Self::NE: Sized,
    {
        self.as_displayable().to_string()
    }

    fn as_ne_string(&'a self) -> NEString
    where
        Self::NE: Sized,
    {
        NEString::try_from(self.as_string()).unwrap()
    }
}

impl<'a, T: ToDisplayNE<'a>> DisplayableNE<'a> for T {}

impl<T: DisplayNEInner + ?Sized> DisplayNE for T {}

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

impl_to_display_ne_ref!(NEStr);
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
    type NE = NEConcat<&'a ToNE<A>, &'a ToNE<B>>;
    fn to_ne(&'a self) -> Self::NE {
        NEConcat(ToNE::with_ref(&self.0), ToNE::with_ref(&self.1))
    }
}

impl<'a, A, B> ToDisplayNE<'a> for NEAlt<A, B>
where
    for<'b> A: ToDisplayNE<'b> + 'a,
    for<'b> B: ToDisplayNE<'b> + 'a,
{
    type NE = NEAlt<&'a ToNE<A>, &'a ToNE<B>>;
    fn to_ne(&'a self) -> Self::NE {
        match self {
            Self::Left(x) => NEAlt::Left(ToNE::with_ref(x)),
            Self::Right(x) => NEAlt::Right(ToNE::with_ref(x)),
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

impl<'a, T> ToDisplayNE<'a> for NESlice<T>
where
    for<'b> T: ToDisplayNE<'b> + 'a,
{
    type NE = &'a NESlice<ToNE<T>>;
    fn to_ne(&'a self) -> Self::NE {
        ToNE::on_inner_slice(self)
    }
}

impl<'a, T> ToDisplayNE<'a> for NEVec<T>
where
    for<'b> T: ToDisplayNE<'b> + 'a,
{
    type NE = &'a NESlice<ToNE<T>>;
    fn to_ne(&'a self) -> Self::NE {
        ToNE::on_inner_slice(self.as_nonempty_slice())
    }
}

impl<'a, T> ToDisplayNE<'a> for NEDelim<NEVec<T>>
where
    for<'b> T: ToDisplayNE<'b> + 'a,
    for<'b> <T as ToDisplayNE<'b>>::NE: Sized,
{
    type NE = NEDelim<&'a NESlice<ToNE<T>>>;
    fn to_ne(&'a self) -> Self::NE {
        let xs = ToNE::on_inner_slice(self.inner.as_nonempty_slice());
        NEDelim::new(self.delim, xs)
    }
}

impl<'a, T, const LEN: usize> ToDisplayNE<'a> for NEDelim<[T; LEN]>
where
    [T; LEN]: NonEmptyArrayExt<T>,
    for<'b> T: ToDisplayNE<'b> + 'a,
    for<'b> <T as ToDisplayNE<'b>>::NE: Sized,
{
    type NE = NEDelim<&'a NESlice<ToNE<T>>>;
    fn to_ne(&'a self) -> Self::NE {
        let xs = ToNE::on_inner_slice(self.inner.as_nonempty_slice());
        NEDelim::new(self.delim, xs)
    }
}

impl<'a, T> ToDisplayNE<'a> for NEDelim<&'a NESlice<T>>
where
    for<'b> T: ToDisplayNE<'b>,
    for<'b> <T as ToDisplayNE<'b>>::NE: Sized,
{
    type NE = NEDelim<&'a NESlice<ToNE<T>>>;
    fn to_ne(&'a self) -> Self::NE {
        let xs = ToNE::on_inner_slice(self.inner);
        NEDelim::new(self.delim, xs)
    }
}

impl<'a> ToDisplayNE<'a> for NEString {
    type NE = &'a NEStr;
    fn to_ne(&'a self) -> Self::NE {
        self.as_ne_str()
    }
}

impl<T> DisplayNEInner for ToNE<T>
where
    for<'a> T: ToDisplayNE<'a>,
    for<'b> <T as ToDisplayNE<'b>>::NE: Sized,
{
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        self.0.to_ne().fmt_ne_inner(f)
    }
}

impl<T> DisplayNEInner for Box<T>
where
    for<'b> T: ToDisplayNE<'b>,
    for<'b> <T as ToDisplayNE<'b>>::NE: Sized,
{
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        self.as_ref().to_ne().fmt_ne_inner(f)
    }
}

impl<T: DisplayNE + ?Sized> DisplayNEInner for &T {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        T::fmt_ne_inner(self, f)
    }
}

macro_rules! impl_display_ne {
    ($t:ident) => {
        impl DisplayNEInner for $t {
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

impl DisplayNEInner for &NEStr {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl<A: DisplayNE, B: DisplayNE> DisplayNEInner for NEConcat<A, B> {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        self.0.fmt_ne_inner(f)?;
        self.1.fmt_ne_inner(f)?;
        Ok(())
    }
}

impl<A: DisplayNE, B: DisplayNE> DisplayNEInner for NEConcatR<A, B> {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        self.0.fmt_ne_inner(f)?;
        if let Some(x) = self.1.as_ref() {
            x.fmt_ne_inner(f)?;
        }
        Ok(())
    }
}

impl<A: DisplayNE, B: DisplayNE> DisplayNEInner for NEConcatL<A, B> {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        if let Some(x) = self.0.as_ref() {
            x.fmt_ne_inner(f)?;
        }
        self.1.fmt_ne_inner(f)?;
        Ok(())
    }
}

impl<A: DisplayNE, B: DisplayNE> DisplayNEInner for NEAlt<A, B> {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        match self {
            Self::Left(x) => x.fmt_ne_inner(f),
            Self::Right(x) => x.fmt_ne_inner(f),
        }
    }
}

impl<T: DisplayNE> DisplayNEInner for NESlice<T> {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        for x in self {
            x.fmt_ne_inner(f)?;
        }
        Ok(())
    }
}

impl<T: DisplayNE> DisplayNEInner for NEDelim<&NESlice<T>> {
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

impl<T: DisplayNE> DisplayNEInner for NEVec<T> {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        self.as_nonempty_slice().fmt_ne_inner(f)
    }
}

impl<T, const LEN: usize> DisplayNEInner for NEDelim<[T; LEN]>
where
    [T; LEN]: NonEmptyArrayExt<T>,
    T: DisplayNE,
{
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        NEDelim::new(self.delim, self.inner.as_nonempty_slice()).fmt_ne_inner(f)
    }
}

impl<T: DisplayNE> DisplayNEInner for NEDelim<NEVec<T>> {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        NEDelim::new(self.delim, self.inner.as_nonempty_slice()).fmt_ne_inner(f)
    }
}

impl DisplayNEInner for PaddedU64 {
    fn fmt_ne_inner(&self, f: &mut impl fmt::Write) -> fmt::Result {
        let n_digits = self.value.checked_ilog10().unwrap_or_default() + 1;
        let n_pad = self.pad.saturating_sub(n_digits);
        for _ in 0..n_pad {
            f.write_char('0')?;
        }
        write!(f, "{}", self.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn padded_zero() {
        let v = PaddedU64::new(3, '0', 0).to_ne_string();
        assert_eq!(v.as_str(), "000");
    }

    #[test]
    fn padded_zero_nopad() {
        let v = PaddedU64::new(0, '0', 0).to_ne_string();
        assert_eq!(v.as_str(), "0");
    }

    #[test]
    fn padded_one() {
        let v = PaddedU64::new(3, '0', 1).to_ne_string();
        assert_eq!(v.as_str(), "001");
    }

    #[test]
    fn padded_one_nopad() {
        let v = PaddedU64::new(0, '0', 1).to_ne_string();
        assert_eq!(v.as_str(), "1");
    }

    #[test]
    fn padded_not_enough() {
        let v = PaddedU64::new(2, '0', 100).to_ne_string();
        assert_eq!(v.as_str(), "100");
    }
}
