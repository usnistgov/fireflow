use crate::convert::{U64Ext as _, UsizeExt as _};

use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;

/// A reader for an FCS file with cached file length and position.
///
/// Including the length and position can help reduce syscalls. They also make
/// the API easier to use.
pub struct BufFileReader {
    /// The reader itself.
    reader: BufReader<File>,
    /// The length of the file.
    ///
    /// Using a fixed value here assumes the file length will not change at
    /// all while being read. This program will not do this by design. All the
    /// API functions are explicitly read or write only, and there is no
    /// concurrency.
    ///
    /// If another process changes the file length, this program has no control
    /// over it but will throw an error for cases where the real length is
    /// shorter than expected (ie not enough bytes).
    file_len: u64,
    /// The current position of the cursor in the file.
    ///
    /// Used for relative seeking which can avoid syscall overhead. This is
    /// useful for cases when seeking to a new offset remains in the same
    /// buffer.
    ///
    /// This can never be equal to or greater than [`Self::file_len`].
    pos: u64,
}

impl Read for BufFileReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.reader.read(buf)?;
        self.advance_pos(n)
    }

    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        let n = self.reader.read_vectored(bufs)?;
        self.advance_pos(n)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let n = self.reader.read_to_end(buf)?;
        self.advance_pos(n)
    }

    fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        let n = self.reader.read_to_string(buf)?;
        self.advance_pos(n)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.reader.read_exact(buf)?;
        let _ = self.advance_pos(buf.len())?;
        Ok(())
    }
}

impl Seek for BufFileReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let err = io::Error::from(io::ErrorKind::InvalidInput);
        let new_pos = match pos {
            SeekFrom::Current(p) => self.pos.checked_add_signed(p).ok_or(err)?,
            SeekFrom::End(p) => self.file_len.checked_add_signed(p).ok_or(err)?,
            SeekFrom::Start(p) => p,
        };
        self.seek_to(new_pos)?;
        Ok(new_pos)
    }
}

impl BufFileReader {
    pub(crate) fn open(p: &PathBuf) -> io::Result<Self> {
        let file = File::options().read(true).open(p)?;
        let m = file.metadata()?;
        Ok(Self {
            file_len: m.len(),
            reader: BufReader::new(file),
            pos: 0,
        })
    }

    pub(crate) fn file_len(&self) -> u64 {
        self.file_len
    }

    pub(crate) fn pos(&self) -> u64 {
        self.pos
    }

    pub(crate) fn seek_to(&mut self, new_pos: u64) -> io::Result<()> {
        if let Some(delta) = new_pos.checked_signed_diff(self.pos)
            && new_pos <= self.file_len
        {
            self.reader.seek_relative(delta)?;
            self.pos = new_pos;
            Ok(())
        } else {
            // TODO uninformative error, but this should also be very rare
            Err(io::Error::from(io::ErrorKind::InvalidInput))
        }
    }

    pub(crate) fn remaining_bytes(&self) -> u64 {
        self.file_len
            .checked_sub(self.pos)
            .expect("position is always less than file length")
    }

    fn advance_pos(&mut self, n: usize) -> io::Result<usize> {
        self.pos += n.usize_to_u64();
        if self.pos > self.file_len {
            let msg = "file was changed on disk and is now longer than when initially read";
            Err(io::Error::other(msg))
        } else {
            Ok(n)
        }
    }

    pub(crate) fn read_exact_owned(&mut self, n: u64) -> io::Result<Vec<u8>> {
        read_exact_owned(self, n)
    }

    pub(crate) fn read_to_end_checked(&mut self, n: u64, buf: &mut Vec<u8>) -> io::Result<()> {
        read_to_end_checked(self, n, buf)
    }
}

/// Like [`Read::read_exact`] but returns an owned vector.
pub(crate) fn read_exact_owned<R: Read>(h: &mut R, n: u64) -> io::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(n.u64_to_usize());
    read_to_end_checked(h, n, &mut buf)?;
    Ok(buf)
}

/// Like [`Read::read_to_end`] but checks that we actually read the full amount.
pub(crate) fn read_to_end_checked<R: Read>(h: &mut R, n: u64, buf: &mut Vec<u8>) -> io::Result<()> {
    h.take(n).read_to_end(buf)?;
    if buf.len() == n.u64_to_usize() {
        Ok(())
    } else {
        Err(io::ErrorKind::UnexpectedEof.into())
    }
}
