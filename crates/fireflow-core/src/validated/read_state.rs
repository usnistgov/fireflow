use crate::{api::CRCOutput, text::keywords::Nextdata};

use crc::CRC_16_KERMIT;
use derive_more::{Display, From, Into};
use derive_new::new;
use fireflow_types::keywords::Version;
use thiserror::Error;

use std::io::{self, BufReader, Read, Seek};

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{DisplayAsPyErr, FromInnerPyObject},
    fireflow_types::python as py,
};

/// Read state after HEADER is parsed.
pub type HeaderReadState<C> = ReadDatasetState<C, ()>;

/// Read state after HEADER and TEXT are parsed.
pub type TEXTReadState<C> = ReadDatasetState<C, DatasetBounds>;

/// The length of the entire FCS file in bytes.
#[derive(From, Into, Clone, Copy, Debug, Display, PartialEq, Eq)]
pub(crate) struct FileLen(pub(crate) u64);

/// The length of the current dataset in bytes.
///
/// For files with one dataset, this will be exactly equal to the file length;
/// this is 99% of files. For files with multiple datasets via $NEXTDATA, this
/// will be the length of the current individual dataset.
#[derive(From, Into, Clone, Copy, Debug, Display, PartialEq, Eq)]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
pub struct DatasetLen(pub u64);

/// The offset of the current dataset in bytes.
///
/// This will be zero except in files with multiple datasets for all but the
/// first dataset.
#[derive(From, Into, Clone, Copy, Debug, PartialEq, Default, Display)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[cfg_attr(feature = "python", derive(FromInnerPyObject))]
pub struct DatasetOffset(pub u64);

#[derive(Error, Debug, PartialEq, Clone)]
#[error("dataset offset ({0}) exceeds file length ({1})")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct DatasetOffsetError(DatasetOffset, FileLen);

#[derive(Error, Debug, PartialEq, Clone)]
#[error("dataset offset ({0}) + new length ({1}) exceeds file length ({2})")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::ConfigError))]
pub struct DatasetLenEOFError(DatasetOffset, DatasetLen, FileLen);

/// State pertinent to reading a dataset.
#[derive(new)]
pub struct ReadDatasetState<C, D> {
    /// The length of the entire FCS file.
    file_len: FileLen,

    /// The offset of the current FCS dataset.
    ///
    /// This will almost always be zero unless there are multiple datasets in
    /// the file.
    dataset_offset: DatasetOffset,

    /// The current CRC digest of the file.
    digest: crc::Digest<'static, u16>,

    /// The length of the current dataset (if available).
    ///
    /// This will almost always be equal to `file_len`.
    ///
    /// This is only known once $NEXTDATA is read, thus this only applies after
    /// TEXT is read.
    dataset_bounds: D,

    /// A read-only configuration to be used with this state.
    conf: C,
}

/// The upper boundary of a dataset in an FCS file.
#[derive(Clone, Copy, new)]
pub struct DatasetBounds {
    pub(crate) len: DatasetLen,
    pub(crate) from_nextdata: bool,
}

/// The CRC algorithm to use for FCS files.
///
/// The standards (since 3.0) specify this must be CRC-16/CCITT, which is the
/// same thing as CRC-16/KERMIT (not the same as CRC-16/CCITT-FALSE).
const FCS_CRC: crc::Crc<u16> = crc::Crc::<u16>::new(&CRC_16_KERMIT);

impl<C> HeaderReadState<C> {
    pub(crate) fn init(
        fl: FileLen,
        dataset_offset: DatasetOffset,
        conf: C,
    ) -> Result<Self, DatasetOffsetError> {
        if u64::from(fl) < u64::from(dataset_offset) {
            let e = DatasetOffsetError(dataset_offset, fl);
            return Err(e);
        }
        let digest = FCS_CRC.digest();
        Ok(Self::new(fl, dataset_offset, digest, (), conf))
    }

    pub(crate) fn maybe_with_dataset_length(
        self,
        dataset_len: Option<DatasetLen>,
    ) -> Result<TEXTReadState<C>, DatasetLenEOFError> {
        if let Some(dl) = dataset_len {
            let f = self.file_len;
            let d = self.dataset_offset;
            if d.0 + dl.0 <= f.0 {
                Ok(self.with_dataset_length(dl, false))
            } else {
                Err(DatasetLenEOFError(d, dl, f))
            }
        } else {
            Ok(self.into_last_dataset())
        }
    }

    pub(crate) fn local_file_len(&self) -> u64 {
        let f = self.file_len;
        let d = self.dataset_offset;
        f.0.checked_sub(d.0)
            .unwrap_or_else(|| panic!("dataset offset ({d}) exceeds file length ({f})"))
    }

    pub(crate) fn with_nextdata(self, nd: Nextdata) -> TEXTReadState<C> {
        self.with_dataset_length(DatasetLen(u64::from(nd)), true)
    }

    fn with_dataset_length(self, dataset_len: DatasetLen, from_nextdata: bool) -> TEXTReadState<C> {
        let f = self.file_len;
        let d = self.dataset_offset;
        assert!(
            d.0 + dataset_len.0 <= f.0,
            "dataset offset ({d}) + dataset length ({dataset_len}), exceeds file length ({f})"
        );
        let bounds = DatasetBounds::new(dataset_len, from_nextdata);
        ReadDatasetState::new(f, d, self.digest, bounds, self.conf)
    }

    // this should only be called if $NEXTDATA is 0 or missing (if allowed)
    pub(crate) fn into_last_dataset(self) -> TEXTReadState<C> {
        let f = self.file_len;
        let d = self.dataset_offset;
        let dl =
            f.0.checked_sub(d.0)
                .expect("dataset offset should not exceed file length");
        let bounds = DatasetBounds::new(DatasetLen(dl), false);
        ReadDatasetState::new(f, d, self.digest, bounds, self.conf)
    }
}

impl<C> TEXTReadState<C> {
    pub(crate) fn read_crc<R>(
        &self,
        h: &mut BufReader<R>,
        crc_start: u64,
        version: Version,
    ) -> io::Result<Option<CRCOutput>>
    where
        R: Read + Seek,
    {
        if version == Version::FCS2_0 {
            return Ok(None);
        }
        h.seek(io::SeekFrom::Start(self.dataset_offset.0 + crc_start))?;
        let rem = self.remaining_bytes(h)?;
        if rem < 8 {
            Ok(None)
        } else {
            let mut buf = [0_u8; 8];
            h.read_exact(&mut buf)?;
            // NOTE the CRC has 8 digits but must parse to a 16-bit number.
            // It isn't clear why the CRC isn't just 5 bytes, since the max
            // u16 is ~64k.
            let ret = str::from_utf8(&buf)
                .ok()
                .and_then(|s| s.parse::<u16>().ok())
                .map_or(CRCOutput::Invalid(buf), CRCOutput::Valid);
            Ok(Some(ret))
        }
    }

    pub(crate) fn dataset_bounds(&self) -> &DatasetBounds {
        &self.dataset_bounds
    }
}

impl<C, D> ReadDatasetState<C, D> {
    pub(crate) fn conf(&self) -> &C {
        &self.conf
    }

    pub(crate) fn file_len(&self) -> FileLen {
        self.file_len
    }

    pub(crate) fn dataset_offset(&self) -> DatasetOffset {
        self.dataset_offset
    }

    pub(crate) fn remaining_bytes<R: Seek>(&self, h: &mut BufReader<R>) -> io::Result<u64> {
        let pos = h.stream_position()?;
        let remaining = u64::from(self.file_len) - pos;
        Ok(remaining)
    }

    pub(crate) fn update_digest(&mut self, bytes: &[u8]) {
        self.digest.update(bytes);
    }
}
