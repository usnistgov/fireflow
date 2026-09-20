#![allow(clippy::default_numeric_fallback)]
use crate::vec::Iter;
use crate::{HasNELen, IntoNonEmptyIterator, NESlice, NEVec, NonEmptyIterator};

use core::array;
use std::fmt;
use std::num::NonZeroUsize;

/// Provides extension methods for non-empty arrays.
///
/// # Examples
///
/// Create a non-empty slice of an array:
///
/// ```
/// # use nonempty::*;
/// assert_eq!(
///     NESlice::try_from_slice(&[1, 2]),
///     Some([1, 2].as_nonempty_slice())
/// );
/// ```
///
/// Get the length of an array as a [`NonZeroUsize`]:
///
/// ```
/// # use nonempty::NonEmptyArrayExt;
/// # use std::num::NonZeroUsize;
/// assert_eq!(NonZeroUsize::MIN, [1].nonzero_len());
/// ```
///
/// Convert array into a non-empty vec:
///
/// ```
/// # use nonempty::*;
/// assert_eq!(nev![4], [4].into_nonempty_vec());
/// ```
pub trait NonEmptyArrayExt<T> {
    /// Create a `NESlice` that borrows the contents of `self`.
    fn as_nonempty_slice(&self) -> &NESlice<T>;

    /// Returns the length of this array as a [`NonZeroUsize`].
    fn nonzero_len(&self) -> NonZeroUsize;

    /// Moves `self` into a new [`crate::NEVec`].
    fn into_nonempty_vec(self) -> NEVec<T>;
}

/// Non-empty iterator for arrays with length > 0.
///
/// # Examples
///
/// Use non-zero length arrays anywhere an [`IntoNonEmptyIterator`] is expected.
///
/// ```
/// use std::num::NonZeroUsize;
///
/// use nonempty::*;
///
/// fn is_one<T>(iter: impl IntoNonEmptyIterator<Item = T>) {
///     assert_eq!(NonZeroUsize::MIN, iter.into_nonempty_iter().count());
/// }
///
/// is_one([0]);
/// ```
///
/// Only compiles for non-empty arrays:
///
/// ```compile_fail
/// use nonempty::*;
///
/// fn is_one(iter: impl IntoNonEmptyIterator<Item = usize>) {}
///
/// is_one([]); // Doesn't compile because it is empty.
/// ```
#[derive(Clone)]
pub struct ArrayNonEmptyIterator<T, const C: usize> {
    iter: array::IntoIter<T, C>,
}

impl<T, const C: usize> IntoIterator for ArrayNonEmptyIterator<T, C> {
    type Item = T;

    type IntoIter = array::IntoIter<T, C>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter
    }
}

impl<T, const C: usize> NonEmptyIterator for ArrayNonEmptyIterator<T, C> {}

impl<T: fmt::Debug, const C: usize> fmt::Debug for ArrayNonEmptyIterator<T, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.iter.fmt(f)
    }
}

macro_rules! impl_nonempty_iter_for_arrays {
    ($($i:literal),+ $(,)?) => {
        $(
            impl<T> HasNELen for [T; $i] {
                fn ne_len(&self) -> NonZeroUsize {
                    NonZeroUsize::new($i).unwrap()
                }
            }

            impl<T> IntoNonEmptyIterator for [T; $i] {
                type IntoNEIter = ArrayNonEmptyIterator<T, $i>;

                fn into_nonempty_iter(self) -> Self::IntoNEIter {
                    ArrayNonEmptyIterator {
                        iter: self.into_iter(),
                    }
                }
            }

            impl<'a, T> IntoNonEmptyIterator for &'a [T; $i] {
                type IntoNEIter = Iter<'a, T>;

                fn into_nonempty_iter(self) -> Self::IntoNEIter {
                    self.as_nonempty_slice().into_nonempty_iter()
                }
            }

            impl<T> NonEmptyArrayExt<T> for [T; $i] {
                fn as_nonempty_slice(&self) -> &NESlice<T> {
                    // This should never panic because a slice with length > 0
                    // is non-empty by definition.
                    NESlice::try_from_slice(self).unwrap()
                }

                fn nonzero_len(&self) -> NonZeroUsize {
                    // SAFETY: This should be fine because $i is always > 0.
                    unsafe { NonZeroUsize::new_unchecked($i) }
                }

                fn into_nonempty_vec(self) -> NEVec<T> {
                    self.into_nonempty_iter().collect()
                }
            }
        )+
    };
}

// NOTE 2024-04-05 This must never be implemented for 0.
//
// Also, happy birthday Dad.
impl_nonempty_iter_for_arrays!(
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
    27, 28, 29, 30, 31, 32
);

#[cfg(test)]
mod test {
    use crate::IntoNonEmptyIterator as _;
    use crate::NonEmptyIterator as _;

    #[test]
    fn iter() {
        let iter0 = [1, 2, 3, 4].into_nonempty_iter();
        let (first0, rest0) = iter0.next();
        assert_eq!(1, first0);
        assert_eq!(vec![2, 3, 4], rest0.into_iter().collect::<Vec<_>>());

        let iter1 = [1].into_nonempty_iter();
        let (first1, rest1) = iter1.next();
        assert_eq!(1, first1);
        assert_eq!(0, rest1.into_iter().count());

        assert_eq!(33, [1, -2, 33, 4].into_nonempty_iter().max());
    }

    #[test]
    fn iter_ref() {
        let iter0 = (&[1, 2, 3, 4]).into_nonempty_iter();
        let (first0, rest0) = iter0.next();
        assert_eq!(&1, first0);
        assert_eq!(vec![&2, &3, &4], rest0.into_iter().collect::<Vec<_>>());

        let iter1 = (&[1]).into_nonempty_iter();
        let (first1, rest1) = iter1.next();
        assert_eq!(&1, first1);
        assert_eq!(0, rest1.into_iter().count());

        assert_eq!(&33, (&[1, -2, 33, 4]).into_nonempty_iter().max());
    }
}
