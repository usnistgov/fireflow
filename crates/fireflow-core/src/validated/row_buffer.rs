use crate::convert::{U64Ext as _, UsizeExt as _};
use crate::data::{
    AnyDatatype, AnyUint, AnyUintVec, AsciiToUintError, ColumnIsBinary as _, MixedSeries, MixedVec,
    NativeSeries, RangedVec, VariableUintSeries, ascii_to_uint,
};
use crate::logging::{IOResult, ImpureError};
use crate::text::byteord::{ArrayByteOrd, Endian};
use crate::validated::ascii_range::FixedAsciiRange;
use crate::validated::unaligned::{DstIndex, FCSRepr, SrcIndex};

use fireflow_types::config::RowBufferSize;

use derive_new::new;
use itertools::Itertools as _;
use num_traits::{FromBytes, ToBytes};

use std::convert::Infallible;
use std::io::{self, BufReader, BufWriter, Read, Write};

use super::dataframe::HasLen;

/// A cache-friendly buffer for reading and writing DATA.
///
/// Since FCS data is row-major and we want to output it in column-major, we
/// effectively need to transpose the data on-the-fly as it is being read. We
/// can't just think about this like a matrix transposition because we have
/// different data types, and we need to read into separate vectors anyways
/// since this is what polars expects to see when is makes a series.
///
/// Therefore, the idea is the read several rows at a time into an intermediate
/// row buffer from which raw bytes will be copied, possibly rearranged (in the
/// case of mixed byteord), padded (in the case of non-power-of-two integers),
/// cast as their target datatype, and finally stored in their final column
/// vectors. Once we have this row buffer, each column will be filled serially
/// which means the source buffer will be strided and the destination buffer
/// will be indexed contiguously. The row buffer will be able to store a whole
/// number of rows from the DATA segment.
///
/// Since we are only dealing with one segment of one column at the same time,
/// this means that we can adjust this size of this buffer and the one column
/// segment by extension (it will have the same length as the number of rows in
/// the buffer) to fit in the CPU's cache (ideally L1d). In practice, final
/// speed will be determined by the balance between syscall overhead for reads
/// and writes vs cache misses.
#[derive(new)]
pub(crate) struct RowBuffer<const IS_READ: bool> {
    // all values are internally validated to be non-zero and consistent
    nrows: usize,
    row_width: usize,
    rows_per_buffer: usize,
    buf_size: u64,
    bytes: Vec<u8>,
}

pub(crate) type ReadBuffer = RowBuffer<true>;

pub(crate) type WriteBuffer = RowBuffer<false>;

impl<const IS_READ: bool> RowBuffer<IS_READ> {
    pub(crate) fn init(max_size: RowBufferSize, nrows: usize, row_width: usize) -> Option<Self> {
        if nrows == 0 || row_width == 0 {
            return None;
        }
        // Max this to 1 here so that we always have at least one row we are
        // reading. If there are any machines that produce files with at least
        // 32KB rows (which would be ~1000 parameters at 32 bit column widths),
        // these will produce some lovely cache miss fireworks on most CPUs :/
        let rows_per_buffer = (usize::from(max_size) / row_width).max(1);
        let buf_size = rows_per_buffer * row_width;
        // When reading we will be pulling a stream from disk and clearing it
        // repeatedly, so it needs to start empty. When writing, we need to fill
        // the buffer with 0's up to capacity and then copy data to it, so it
        // needs to remain a fixed size.
        let bytes = if IS_READ {
            Vec::with_capacity(buf_size)
        } else {
            vec![0; buf_size]
        };
        let new = Self {
            nrows,
            rows_per_buffer,
            buf_size: buf_size.usize_to_u64(),
            row_width,
            bytes,
        };
        Some(new)
    }

    fn whole_row_number(&self) -> usize {
        self.nrows / self.rows_per_buffer
    }

    fn remainder_row_number(&self) -> usize {
        self.nrows % self.rows_per_buffer
    }

    fn remainder_bytes(&self) -> usize {
        let remainder_rows = self.remainder_row_number();
        remainder_rows * self.row_width
    }

    /// Test the input geometry to ensure that we won't read out of bounds.
    ///
    /// This is important because we don't want to use range checks in the
    /// main loop.
    fn assert_matrix_assumptions<C: HasLen>(&self, columns: &[C], value_bytes: usize) {
        let mismatch_col_lengths: Vec<_> = columns
            .iter()
            .map(HasLen::len)
            .enumerate()
            .filter(|(_, l)| *l != self.nrows)
            .map(|(i, l)| format!("({i},{l})"))
            .collect();
        assert!(
            mismatch_col_lengths.is_empty(),
            "All column lengths should be equal to given row number ({}), \
             non-equal column lengths were [{}] (index, length)",
            self.nrows,
            mismatch_col_lengths.into_iter().join(",")
        );

        let computed_row_width = columns.len() * value_bytes;
        assert!(
            computed_row_width == self.row_width,
            "Computed row bytes ({computed_row_width}) not equal to assumed row bytes ({})",
            self.row_width,
        );

        let whole_buffer_rows = self.rows_per_buffer * self.whole_row_number();
        assert!(
            whole_buffer_rows <= self.nrows,
            "number of rows in complete reads ({whole_buffer_rows}) \
             must be less than total rows ({})",
            self.nrows
        );
    }

    /// Check that we won't read out of bounds.
    ///
    /// This must be a debug assert so that there are no bounds checks (and
    /// therefore no jmp ops) in the main loop in release code.
    fn debug_assert_in_bounds(&self, idx: usize, len: usize) {
        debug_assert!(
            idx + len <= self.buf_size.u64_to_usize(),
            "need to read [{}..{}] but buffer is only {} bytes long",
            idx,
            idx + len,
            self.buf_size,
        );
    }
}

impl ReadBuffer {
    fn read_size<R: Read>(&mut self, h: &mut BufReader<R>, size: u64) -> io::Result<()> {
        self.bytes.clear();
        h.take(size).read_to_end(&mut self.bytes)?;
        Ok(())
    }

    fn read<R: Read>(&mut self, h: &mut BufReader<R>) -> io::Result<()> {
        self.read_size(h, self.buf_size)
    }

    fn read_remainder<R: Read>(&mut self, h: &mut BufReader<R>) -> io::Result<()> {
        let n = self.remainder_bytes().usize_to_u64();
        self.read_size(h, n)
    }

    fn read_columns<C, E, R, Fr, Fw>(
        &mut self,
        h: &mut BufReader<R>,
        columns: &mut [C],
        mut fread: Fr,
        fwidth: Fw,
    ) -> IOResult<(), E>
    where
        R: Read,
        Fr: FnMut(&mut C, DstIndex, &[u8], SrcIndex) -> Result<(), E>,
        Fw: Fn(usize) -> usize,
    {
        // Read groups of rows in outer loop
        let mut src_col_offset;
        let mut dst_row_offset = 0;
        for _ in 0..self.whole_row_number() {
            self.read(h)?;
            src_col_offset = 0;
            // Once we have a buffer, iterate through each column and write data
            for (ci, c) in columns.iter_mut().enumerate() {
                // Within each column, write rows, striding the row buffer and
                // indexing consecutively in the current column
                let src_width = fwidth(ci);
                for row in 0..self.rows_per_buffer {
                    let src_idx = SrcIndex(src_col_offset + self.row_width * row);
                    let dst_idx = DstIndex(dst_row_offset + row);
                    fread(c, dst_idx, &self.bytes, src_idx).map_err(ImpureError::Pure)?;
                }
                src_col_offset += src_width;
            }
            dst_row_offset += self.rows_per_buffer;
        }

        // Read remaining rows if they exist
        self.read_remainder(h)?;
        src_col_offset = 0;
        for (ci, c) in columns.iter_mut().enumerate() {
            for row in 0..self.remainder_row_number() {
                let src_idx = SrcIndex(src_col_offset + self.row_width * row);
                let dst_idx = DstIndex(dst_row_offset + row);
                fread(c, dst_idx, &self.bytes, src_idx).map_err(ImpureError::Pure)?;
            }
            src_col_offset += fwidth(ci);
        }

        Ok(())
    }

    /// Read stream of bytes using buffer where each value is the same type
    fn read_matrix<R, T, F>(
        &mut self,
        h: &mut BufReader<R>,
        columns: &mut [Vec<T>],
        from_buf: F,
    ) -> io::Result<()>
    where
        R: Read,
        F: Fn(&T::FileBuf) -> T,
        T: FCSRepr,
    {
        // This method has several nice optimizations:
        // 1. No errors on the inner two loops
        // 2. All values have the same byte layout, which means we don't need
        //    to dispatch different methods for different columns
        // 3. Using the assertions below and some unsafe code, we can remove
        //    all bounds checks on the inner loop.
        //
        // 1-3 above mean that that two inner loops have no jumps, which means
        // the compiler can unroll the loops and possibly autovectorize.
        let src_len = T::file_len();
        self.assert_matrix_assumptions(columns, src_len);

        // Read groups of rows in outer loop
        for buf_idx in 0..self.whole_row_number() {
            self.read(h)?;
            let start_row = buf_idx * self.rows_per_buffer;
            // Once we have a buffer, iterate through each column and write data
            for (ci, c) in columns.iter_mut().enumerate() {
                let src_col_offset = ci * src_len;
                // Within each column, write rows, striding the row buffer and
                // indexing consecutively in the current column
                let end_row = start_row + self.rows_per_buffer;
                let local_c = &mut c[start_row..end_row];
                for (row, value) in local_c.iter_mut().enumerate() {
                    let src_idx = SrcIndex(src_col_offset + self.row_width * row);
                    self.debug_assert_in_bounds(src_idx.0, src_len);
                    // SAFETY: src_idx given as row_width * R + C * LEN where R
                    // is row index (within the buffer) and C is column index.
                    // Both R and C must be less than the number of rows per
                    // buffer and the number of columns respectively since we
                    // are getting these via enumerate(). Therefore, the maximum
                    // that src_idx can ever be is row_width * (rows_per_buffer
                    // - 1) + (column_number - 1) * LEN. Adding LEN to the end
                    // of this exactly equals the size of the buffer itself in
                    // bytes, which means what follows can never overflow.
                    let buf = unsafe { T::array_from_slice(&self.bytes, &src_idx) };
                    *value = from_buf(&buf);
                }
            }
        }

        // Read remaining rows if they exist
        self.read_remainder(h)?;
        let remainder_rows = self.remainder_row_number();
        let dst_row_offset = self.whole_row_number() * self.rows_per_buffer;
        for (ci, c) in columns.iter_mut().enumerate() {
            let src_col_offset = ci * src_len;
            let local_c = &mut c[dst_row_offset..dst_row_offset + remainder_rows];
            for (row, value) in local_c.iter_mut().enumerate() {
                let src_idx = SrcIndex(src_col_offset + self.row_width * row);
                self.debug_assert_in_bounds(src_idx.0, src_len);
                // SAFETY: see above
                let buf = unsafe { T::array_from_slice(&self.bytes, &src_idx) };
                *value = from_buf(&buf);
            }
        }

        Ok(())
    }

    /// Read a matrix where type is an aligned big or little endian value.
    pub(crate) fn read_endian_matrix<R, T>(
        &mut self,
        h: &mut BufReader<R>,
        cols: &mut [Vec<T>],
        endian: Endian,
    ) -> io::Result<()>
    where
        R: Read,
        T: FromBytes<Bytes = T::FileBuf> + FCSRepr,
    {
        match endian {
            Endian::Big => self.read_matrix(h, cols, T::from_be_bytes),
            Endian::Little => self.read_matrix(h, cols, T::from_le_bytes),
        }
    }

    /// Read a matrix where type is an aligned big, little, or mixed endian value.
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn read_ordered_matrix<R, T, const LEN: usize>(
        &mut self,
        h: &mut BufReader<R>,
        cols: &mut [Vec<T>],
        s: ArrayByteOrd<LEN>,
    ) -> io::Result<()>
    where
        R: Read,
        T: FromBytes<Bytes = T::FileBuf> + FCSRepr<ByteOrd = [u8; LEN]>,
        T::FileBuf: AsRef<[u8]> + AsMut<[u8]> + Default,
        T::ByteOrd: AsRef<[u8]>,
        ArrayByteOrd<LEN>: AsRef<T::ByteOrd>,
    {
        if let Some(e) = s.as_endian() {
            self.read_endian_matrix(h, cols, e)
        } else {
            self.read_matrix(h, cols, |bs| T::from_ordered_bytes(bs, s.as_ref()))
        }
    }

    /// Read a matrix where input bytes characters to be read as u64
    pub(crate) fn read_char_matrix<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        cols: &mut [RangedVec<FixedAsciiRange, u64>],
    ) -> IOResult<(), AsciiToUintError> {
        // TODO this smells like something that could be cleaned up later
        let ranges: Vec<_> = cols
            .iter()
            .map(|c| usize::from(u8::from(c.range.chars())))
            .collect();
        self.read_columns(
            h,
            cols,
            |dst, dst_index, src, src_index| {
                let src_width = usize::from(u8::from(dst.range.chars()));
                let x = ascii_to_uint(&src[src_index.0..src_index.0 + src_width])?;
                dst.data[dst_index.0] = x;
                Ok(())
            },
            |i| ranges[i],
        )
    }

    /// Read a dataframe of unsigned integers with different widths
    pub(crate) fn read_any_uint_df<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        cols: &mut [AnyUintVec],
        endian: Endian,
    ) -> io::Result<()> {
        let src_widths: Vec<_> = cols
            .iter()
            .map(|c| usize::from(u8::from(c.bytes())))
            .collect();
        let res = match endian {
            Endian::Big => self.read_columns(
                h,
                cols,
                |dst, dst_index, src, src_index| {
                    dst.read_be(dst_index, src, src_index);
                    Ok(())
                },
                |i| src_widths[i],
            ),
            Endian::Little => self.read_columns(
                h,
                cols,
                |dst, dst_index, src, src_index| {
                    dst.read_le(dst_index, src, src_index);
                    Ok(())
                },
                |i| src_widths[i],
            ),
        };
        res.map_err(|e: ImpureError<Infallible>| {
            let ImpureError::IO(i) = e;
            i
        })
    }

    /// Read a dataframe of any mix of column types
    pub(crate) fn read_mixed_df<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        cols: &mut [MixedVec],
        endian: Endian,
    ) -> IOResult<(), AsciiToUintError> {
        let src_widths: Vec<_> = cols
            .iter()
            .map(|c| match c {
                MixedVec::Ascii(x) => usize::from(u8::from(x.range.chars())),
                MixedVec::Uint(x) => usize::from(u8::from(x.bytes())),
                MixedVec::F32(_) => 4,
                MixedVec::F64(_) => 8,
            })
            .collect();
        match endian {
            Endian::Big => self.read_columns(h, cols, AnyDatatype::read_be, |i| src_widths[i]),
            Endian::Little => self.read_columns(h, cols, AnyDatatype::read_le, |i| src_widths[i]),
        }
    }
}

impl WriteBuffer {
    fn write<W: Write>(&self, h: &mut BufWriter<W>) -> io::Result<()> {
        h.write_all(&self.bytes[..])
    }

    fn write_remainder<W: Write>(&self, h: &mut BufWriter<W>) -> io::Result<()> {
        let n = self.remainder_bytes();
        h.write_all(&self.bytes[..n])
    }

    fn write_columns<C, W, Fp, Fw>(
        &mut self,
        h: &mut BufWriter<W>,
        columns: &[C],
        mut fpush: Fp,
        fwidth: Fw,
    ) -> io::Result<()>
    where
        W: Write,
        Fp: FnMut(&C, SrcIndex, &mut [u8], DstIndex),
        Fw: Fn(usize) -> usize,
    {
        // Write groups of rows in outer loop
        let mut dst_col_offset;
        let mut src_row_offset = 0;
        for _ in 0..self.whole_row_number() {
            dst_col_offset = 0;
            // Once we have a buffer, iterate through each column and write data
            for (ci, c) in columns.iter().enumerate() {
                // Within each column, write rows, striding the row buffer and
                // indexing consecutively in the current column
                let src_width = fwidth(ci);
                for row in 0..self.rows_per_buffer {
                    let src_idx = SrcIndex(src_row_offset + row);
                    let dst_idx = DstIndex(dst_col_offset + self.row_width * row);
                    fpush(c, src_idx, &mut self.bytes, dst_idx);
                }
                dst_col_offset += src_width;
            }
            src_row_offset += self.rows_per_buffer;
            self.write(h)?;
        }

        // Read remaining rows if they exist
        let remainder_rows = self.remainder_row_number();
        dst_col_offset = 0;
        for (ci, c) in columns.iter().enumerate() {
            for row in 0..remainder_rows {
                let src_idx = SrcIndex(src_row_offset + row);
                let dst_idx = DstIndex(dst_col_offset + self.row_width * row);
                fpush(c, src_idx, &mut self.bytes, dst_idx);
            }
            dst_col_offset += fwidth(ci);
        }

        self.write_remainder(h)?;

        Ok(())
    }

    /// Read stream of bytes using buffer where each value is the same type
    fn write_matrix<W, T, F>(
        &mut self,
        h: &mut BufWriter<W>,
        columns: &[&[T]],
        to_buf: F,
    ) -> io::Result<()>
    where
        W: Write,
        F: Fn(&T) -> T::FileBuf,
        T: FCSRepr,
    {
        // This has similar analogous optimizations and assumptions as
        // ReadBuffer::read_matrix
        let dst_len = T::file_len();
        self.assert_matrix_assumptions(columns, dst_len);

        // Write groups of rows in outer loop
        for buf_idx in 0..self.whole_row_number() {
            let start_row = buf_idx * self.rows_per_buffer;
            // Once we have a buffer, iterate through each column and write data
            for (ci, c) in columns.iter().enumerate() {
                let dst_col_offset = ci * dst_len;
                // Within each column, write rows, striding the row buffer and
                // indexing consecutively in the current column
                let end_row = start_row + self.rows_per_buffer;
                let local_c = &c[start_row..end_row];
                for (row, value) in local_c.iter().enumerate() {
                    let dst_idx = DstIndex(dst_col_offset + self.row_width * row);
                    self.debug_assert_in_bounds(dst_idx.0, dst_len);
                    let buf = to_buf(value);
                    // SAFETY: src_idx given as row_width * R + C * LEN where R
                    // is row index (within the buffer) and C is column index.
                    // Both R and C must be less than the number of rows per
                    // buffer and the number of columns respectively since we
                    // are getting these via enumerate(). Therefore, the maximum
                    // that src_idx can ever be is row_width * (rows_per_buffer
                    // - 1) + (column_number - 1) * LEN. Adding LEN to the end
                    // of this exactly equals the size of the buffer itself in
                    // bytes, which means what follows can never overflow.
                    unsafe {
                        T::array_to_slice(&buf, &mut self.bytes, &dst_idx);
                    };
                }
            }
            self.write(h)?;
        }

        // Write remaining rows if they exist
        let remainder_rows = self.remainder_row_number();
        let dst_row_offset = self.whole_row_number() * self.rows_per_buffer;
        for (ci, c) in columns.iter().enumerate() {
            let dst_col_offset = ci * dst_len;
            let local_c = &c[dst_row_offset..dst_row_offset + remainder_rows];
            for (row, value) in local_c.iter().enumerate() {
                let dst_idx = DstIndex(dst_col_offset + self.row_width * row);
                self.debug_assert_in_bounds(dst_idx.0, dst_len);
                let buf = to_buf(value);
                // SAFETY: see above
                unsafe {
                    T::array_to_slice(&buf, &mut self.bytes, &dst_idx);
                };
            }
        }

        self.write_remainder(h)?;

        Ok(())
    }

    /// Write a matrix where type is an aligned big or little endian value.
    pub(crate) fn write_endian_matrix<W, T>(
        &mut self,
        h: &mut BufWriter<W>,
        cols: &[&[T]],
        endian: Endian,
    ) -> io::Result<()>
    where
        W: Write,
        T: ToBytes<Bytes = T::FileBuf> + FCSRepr,
    {
        match endian {
            Endian::Big => self.write_matrix(h, cols, T::to_be_bytes),
            Endian::Little => self.write_matrix(h, cols, T::to_le_bytes),
        }
    }

    /// Write a matrix where type is an aligned big, little, or mixed endian value.
    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn write_ordered_matrix<W, T, const LEN: usize>(
        &mut self,
        h: &mut BufWriter<W>,
        cols: &[&[T]],
        s: ArrayByteOrd<LEN>,
    ) -> io::Result<()>
    where
        W: Write,
        T: ToBytes<Bytes = T::FileBuf> + FCSRepr<ByteOrd = [u8; LEN]>,
        T::FileBuf: AsRef<[u8]> + AsMut<[u8]> + Default,
        T::ByteOrd: AsRef<[u8]>,
        ArrayByteOrd<LEN>: AsRef<T::ByteOrd>,
    {
        if let Some(e) = s.as_endian() {
            self.write_endian_matrix(h, cols, e)
        } else {
            self.write_matrix(h, cols, |bs| T::to_ordered_bytes(bs, s.as_ref()))
        }
    }

    /// Write a matrix where input bytes characters are to be read as u64
    pub(crate) fn write_char_matrix<W: Write>(
        &mut self,
        h: &mut BufWriter<W>,
        cols: &[NativeSeries<FixedAsciiRange>],
    ) -> io::Result<()> {
        let ranges: Vec<_> = cols
            .iter()
            .map(|c| usize::from(u8::from(c.column_schema().chars())))
            .collect();
        self.write_columns(
            h,
            cols,
            |src, src_index, dst, dst_index| {
                let v = src.as_ref()[src_index.0];
                src.column_schema().as_slice_unchecked(v, dst, &dst_index);
            },
            |i| ranges[i],
        )
    }

    /// Write a dataframe of unsigned integers with different widths
    pub(crate) fn write_any_uint_df<W: Write>(
        &mut self,
        h: &mut BufWriter<W>,
        cols: &[VariableUintSeries],
        endian: Endian,
    ) -> io::Result<()> {
        let src_widths: Vec<_> = cols
            .iter()
            .map(|c| usize::from(u8::from(c.bytes())))
            .collect();
        match endian {
            Endian::Big => self.write_columns(h, cols, AnyUint::write_be, |i| src_widths[i]),
            Endian::Little => self.write_columns(h, cols, AnyUint::write_le, |i| src_widths[i]),
        }
    }

    /// Write a dataframe of any mix of column types
    pub(crate) fn write_mixed_df<W: Write>(
        &mut self,
        h: &mut BufWriter<W>,
        cols: &[MixedSeries],
        endian: Endian,
    ) -> io::Result<()> {
        let src_widths: Vec<_> = cols
            .iter()
            .map(|c| match c {
                AnyDatatype::Ascii(x) => usize::from(u8::from(x.column_schema().chars())),
                AnyDatatype::Uint(x) => usize::from(u8::from(x.bytes())),
                AnyDatatype::F32(_) => 4,
                AnyDatatype::F64(_) => 8,
            })
            .collect();
        match endian {
            Endian::Big => self.write_columns(h, cols, AnyDatatype::write_be, |i| src_widths[i]),
            Endian::Little => self.write_columns(h, cols, AnyDatatype::write_le, |i| src_widths[i]),
        }
    }
}
