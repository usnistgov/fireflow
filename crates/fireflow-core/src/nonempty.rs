use crate::error::{ErrorIter, MultiResult};
use crate::text::index::{IndexError, IndexFromOne};
use crate::text::optional::{ClearOptional, ClearOptionalOr};

use nonempty::NonEmpty;

pub(crate) trait NonEmptyExt {
    type X;

    fn enumerate(self) -> NonEmpty<(usize, Self::X)>;

    fn map_results<F, E, Y>(self, f: F) -> MultiResult<NonEmpty<Y>, E>
    where
        F: Fn(Self::X) -> Result<Y, E>;

    fn remove(&mut self, index: IndexFromOne) -> Result<(), ClearOptionalOr<IndexError>>;

    fn remove_nocheck(&mut self, index: IndexFromOne) -> Result<(), ClearOptional>;
}

impl<X> NonEmptyExt for NonEmpty<X> {
    type X = X;

    fn enumerate(self) -> NonEmpty<(usize, Self::X)> {
        NonEmpty::collect(self.into_iter().enumerate()).unwrap()
    }

    fn map_results<F, E, Y>(self, f: F) -> MultiResult<NonEmpty<Y>, E>
    where
        F: Fn(Self::X) -> Result<Y, E>,
    {
        self.map(f)
            .into_iter()
            .gather()
            .map(|ys| NonEmpty::from_vec(ys).unwrap())
    }

    fn remove(&mut self, index: IndexFromOne) -> Result<(), ClearOptionalOr<IndexError>> {
        index.check_index(self.len()).map_or_else(
            |e| Err(ClearOptionalOr::Error(e)),
            |i| {
                self.remove_nocheck(i.into())
                    .map_err(|_| ClearOptionalOr::Clear)
            },
        )
    }

    fn remove_nocheck(&mut self, index: IndexFromOne) -> Result<(), ClearOptional> {
        let i: usize = index.into();
        if i == 0 {
            let tail = std::mem::take(&mut self.tail);
            if let Some(xs) = NonEmpty::from_vec(tail) {
                *self = xs
            } else {
                return Err(ClearOptionalOr::Clear);
            }
        } else {
            self.tail.remove(i + 1);
        }
        Ok(())
    }
}
