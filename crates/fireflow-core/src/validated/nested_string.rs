use itertools::Itertools as _;

use std::iter::once;

#[derive(Default)]
pub struct NestedString {
    inner: Vec<u8>,
    indices: Vec<usize>,
}

impl NestedString {
    fn get(&self, i: usize) -> &str {
        let n = self.indices.len();
        assert!(i < n, "index out of bounds: {i}");
        let start = self.indices[i];
        let end = if i == n - 1 {
            self.inner.len()
        } else {
            self.indices[i + 1]
        };
        // SAFETY: this struct is validated such that each slice is a string
        unsafe { self.get_range(start, end) }
    }

    unsafe fn get_range(&self, start: usize, end: usize) -> &str {
        // SAFETY: this function is unsafe
        unsafe { str::from_utf8_unchecked(&self.inner[start..end]) }
    }

    fn init(total_len: usize, n_indices: usize) -> Self {
        Self {
            inner: Vec::with_capacity(total_len),
            indices: Vec::with_capacity(n_indices),
        }
    }

    fn extend<'a>(&mut self, ss: impl IntoIterator<Item = &'a str>) {
        let mut prev_index = self.indices.last().copied().unwrap_or(0);
        for s in ss {
            self.inner.extend(s.as_bytes());
            self.indices.push(prev_index);
            prev_index += s.len();
        }
    }

    fn push(&mut self, s: &str) {
        let prev_index = self.indices.last().copied().unwrap_or(0);
        self.inner.extend(s.as_bytes());
        self.indices.push(prev_index);
    }

    fn iter(&self) -> impl Iterator<Item = &str> {
        self.indices
            .iter()
            .copied()
            .chain(once(self.inner.len()))
            .tuple_windows()
            .map(|(start, end)| {
                // SAFETY: this struct is validated such that each slice is a string
                unsafe { self.get_range(start, end) }
            })
    }
}
