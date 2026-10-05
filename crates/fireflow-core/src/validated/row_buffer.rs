use crate::convert::UsizeExt as _;
use crate::data::{
    AnyDatatype, AnyUint, AnyUintVec, AsciiNumToUintError, ColumnIsBinary as _,
    DataAsciiNumToUintError, MixedSeries, MixedVec, NativeSeries, RangedVec, VariableUintSeries,
    numeric_ascii_to_uint,
};
use crate::logging::{IOResult, ImpureError};
use crate::text::byteord::Endian;
use crate::validated::ascii_range::FixedAsciiRange;
use crate::validated::read_state::WriteFCSDigest;
use crate::validated::unaligned::{DstIndex, SrcIndex};

use fireflow_types::config::RowBufferSize;

use derive_new::new;

use std::convert::Infallible;
use std::io::{self, BufReader, BufWriter, Read, Write};

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
    row_nbytes: usize,
    rows_per_buffer: usize,
    total_nbytes: u64,
    bytes: Vec<u8>,
}

pub(crate) type ReadBuffer = RowBuffer<true>;

pub(crate) type WriteBuffer = RowBuffer<false>;

impl<const IS_READ: bool> RowBuffer<IS_READ> {
    pub(crate) fn init(max_size: RowBufferSize, nrows: usize, row_nbytes: usize) -> Option<Self> {
        if nrows == 0 || row_nbytes == 0 {
            return None;
        }
        // Max this to 1 here so that we always have at least one row we are
        // reading. If there are any machines that produce files with at least
        // 32KB rows (which would be ~1000 parameters at 32 bit column widths),
        // these will produce some lovely cache miss fireworks on most CPUs :/
        let rows_per_buffer = (usize::from(max_size) / row_nbytes).max(1);
        let buf_size = rows_per_buffer * row_nbytes;
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
            total_nbytes: buf_size.usize_to_u64(),
            row_nbytes,
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
        remainder_rows * self.row_nbytes
    }
}

impl ReadBuffer {
    fn read_size<R: Read>(&mut self, h: &mut BufReader<R>, size: u64) -> io::Result<()> {
        self.bytes.clear();
        let taken = h.take(size).read_to_end(&mut self.bytes)?;
        assert_eq!(taken.usize_to_u64(), size, "could not read {size} bytes");
        Ok(())
    }

    fn read<R: Read>(&mut self, h: &mut BufReader<R>) -> io::Result<()> {
        self.read_size(h, self.total_nbytes)
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
        Fw: Fn(&C) -> usize,
    {
        // Read groups of rows in outer loop
        let mut src_col_offset;
        let mut dst_row_offset = 0;
        for _ in 0..self.whole_row_number() {
            self.read(h)?;
            src_col_offset = 0;
            // Once we have a buffer, iterate through each column and write data
            for c in columns.iter_mut() {
                // Within each column, write rows, striding the row buffer and
                // indexing consecutively in the current column
                let src_width = fwidth(c);
                for row in 0..self.rows_per_buffer {
                    let src_idx = SrcIndex(src_col_offset + self.row_nbytes * row);
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
        for c in columns.iter_mut() {
            for row in 0..self.remainder_row_number() {
                let src_idx = SrcIndex(src_col_offset + self.row_nbytes * row);
                let dst_idx = DstIndex(dst_row_offset + row);
                fread(c, dst_idx, &self.bytes, src_idx).map_err(ImpureError::Pure)?;
            }
            src_col_offset += fwidth(c);
        }

        Ok(())
    }

    /// Read a matrix where input bytes characters to be read as u64
    pub(crate) fn read_char_matrix<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        cols: &mut [RangedVec<FixedAsciiRange, u64>],
    ) -> IOResult<(), AsciiNumToUintError> {
        self.read_columns(
            h,
            cols,
            |dst, dst_index, src, src_index| {
                let src_width = usize::from(u8::from(dst.range.chars()));
                let x = numeric_ascii_to_uint(&src[src_index.0..src_index.0 + src_width])?;
                dst.data[dst_index.0] = x;
                Ok(())
            },
            |c| usize::from(u8::from(c.range.chars())),
        )
    }

    /// Read a dataframe of unsigned integers with different widths
    pub(crate) fn read_any_uint_df<R: Read>(
        &mut self,
        h: &mut BufReader<R>,
        cols: &mut [AnyUintVec],
        endian: Endian,
    ) -> io::Result<()> {
        let get_width = |c: &AnyUintVec| usize::from(u8::from(c.bytes()));
        let res = match endian {
            Endian::Big => self.read_columns(
                h,
                cols,
                |dst, dst_index, src, src_index| {
                    dst.read_be(dst_index, src, src_index);
                    Ok(())
                },
                get_width,
            ),
            Endian::Little => self.read_columns(
                h,
                cols,
                |dst, dst_index, src, src_index| {
                    dst.read_le(dst_index, src, src_index);
                    Ok(())
                },
                get_width,
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
    ) -> IOResult<(), DataAsciiNumToUintError> {
        let get_width = |c: &MixedVec| match c {
            MixedVec::Ascii(x) => usize::from(u8::from(x.range.chars())),
            MixedVec::Uint(x) => usize::from(u8::from(x.bytes())),
            MixedVec::F32(_) => 4,
            MixedVec::F64(_) => 8,
        };
        match endian {
            Endian::Big => self.read_columns(h, cols, AnyDatatype::read_be, get_width),
            Endian::Little => self.read_columns(h, cols, AnyDatatype::read_le, get_width),
        }
    }
}

impl WriteBuffer {
    fn write<W: Write>(&self, h: &mut BufWriter<W>, digest: &mut WriteFCSDigest) -> io::Result<()> {
        digest.update_and_write(h, &self.bytes[..])
    }

    fn write_remainder<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        digest: &mut WriteFCSDigest,
    ) -> io::Result<()> {
        let n = self.remainder_bytes();
        digest.update_and_write(h, &self.bytes[..n])
    }

    fn write_columns<C, W, Fp, Fw>(
        &mut self,
        h: &mut BufWriter<W>,
        columns: &[C],
        digest: &mut WriteFCSDigest,
        mut fpush: Fp,
        fwidth: Fw,
    ) -> io::Result<()>
    where
        W: Write,
        Fp: FnMut(&C, SrcIndex, &mut [u8], DstIndex),
        Fw: Fn(&C) -> usize,
    {
        // Write groups of rows in outer loop
        let mut dst_col_offset;
        let mut src_row_offset = 0;
        for _ in 0..self.whole_row_number() {
            dst_col_offset = 0;
            // Once we have a buffer, iterate through each column and write data
            for c in columns {
                // Within each column, write rows, striding the row buffer and
                // indexing consecutively in the current column
                let src_width = fwidth(c);
                for row in 0..self.rows_per_buffer {
                    let src_idx = SrcIndex(src_row_offset + row);
                    let dst_idx = DstIndex(dst_col_offset + self.row_nbytes * row);
                    fpush(c, src_idx, &mut self.bytes, dst_idx);
                }
                dst_col_offset += src_width;
            }
            src_row_offset += self.rows_per_buffer;
            self.write(h, digest)?;
        }

        // Read remaining rows if they exist
        let remainder_rows = self.remainder_row_number();
        dst_col_offset = 0;
        for c in columns {
            for row in 0..remainder_rows {
                let src_idx = SrcIndex(src_row_offset + row);
                let dst_idx = DstIndex(dst_col_offset + self.row_nbytes * row);
                fpush(c, src_idx, &mut self.bytes, dst_idx);
            }
            dst_col_offset += fwidth(c);
        }

        self.write_remainder(h, digest)?;

        Ok(())
    }

    /// Write a matrix where input bytes characters are to be read as u64
    pub(crate) fn write_char_matrix<W: Write>(
        &mut self,
        h: &mut BufWriter<W>,
        cols: &[NativeSeries<FixedAsciiRange>],
        digest: &mut WriteFCSDigest,
    ) -> io::Result<()> {
        self.write_columns(
            h,
            cols,
            digest,
            |src, src_index, dst, dst_index| {
                let v = src.as_ref()[src_index.0];
                src.column_schema().as_slice_unchecked(v, dst, &dst_index);
            },
            |c| usize::from(u8::from(c.column_schema().chars())),
        )
    }

    /// Write a dataframe of unsigned integers with different widths
    pub(crate) fn write_any_uint_df<W: Write>(
        &mut self,
        h: &mut BufWriter<W>,
        cols: &[VariableUintSeries],
        digest: &mut WriteFCSDigest,
        endian: Endian,
    ) -> io::Result<()> {
        let get_width = |c: &VariableUintSeries| usize::from(u8::from(c.bytes()));
        match endian {
            Endian::Big => self.write_columns(h, cols, digest, AnyUint::write_be, get_width),
            Endian::Little => self.write_columns(h, cols, digest, AnyUint::write_le, get_width),
        }
    }

    /// Write a dataframe of any mix of column types
    pub(crate) fn write_mixed_df<W: Write>(
        &mut self,
        h: &mut BufWriter<W>,
        cols: &[MixedSeries],
        digest: &mut WriteFCSDigest,
        endian: Endian,
    ) -> io::Result<()> {
        let get_width = |c: &MixedSeries| match c {
            AnyDatatype::Ascii(x) => usize::from(u8::from(x.column_schema().chars())),
            AnyDatatype::Uint(x) => usize::from(u8::from(x.bytes())),
            AnyDatatype::F32(_) => 4,
            AnyDatatype::F64(_) => 8,
        };
        match endian {
            Endian::Big => self.write_columns(h, cols, digest, AnyDatatype::write_be, get_width),
            Endian::Little => self.write_columns(h, cols, digest, AnyDatatype::write_le, get_width),
        }
    }
}
