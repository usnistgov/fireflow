use crate::{
    api::CRCOutput,
    config::{CRCConfig, ComputeWriteCRC, ConfigFlag as _},
    logging::{IOErrorGroup, LogResult, SwitchableErrorResult, WarningAndIOGroupResult, io_to_log},
    text::keywords::Nextdata,
    validated::keys::StringOrBytes,
};

use crc_fast::{CrcAlgorithm, Digest};
use derive_more::{Display, From, Into};
use derive_new::new;
use fireflow_types::{config::ComputeCRC, keywords::Version};
use thiserror::Error;

use std::io::{self, BufReader, Read, Seek, Write as _};

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr, FromInnerPyObject},
    fireflow_types::python as py,
};

/// Read state after HEADER is parsed.
pub type HeaderReadState<C> = ReadDatasetState<C, ()>;

/// Read state after HEADER and TEXT are parsed.
pub type TEXTReadState<C> = ReadDatasetState<C, DatasetBounds>;

/// The live CRC of the file as it is being written.
pub struct WriteFCSDigest {
    digest: Digest,
    compute_digest: ComputeWriteCRC,
    is_2_0: bool,
}

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

/// Error when computing or testing the CRC.
#[derive(Debug, Display, Error, PartialEq, Clone, From)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum CRCError {
    Missing(MissingCRCError),
    Failed(FailedChecksumError),
}

/// Error when CRC word is missing from end of dataset (3.0+)
#[derive(Error, Debug, PartialEq, Clone)]
#[error("CRC word is missing at offset {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct MissingCRCError(u64);

/// Error when computed checksum of a dataset does not match the CRC word
#[derive(Error, Debug, PartialEq, Clone, new)]
#[error("dataset checksum failed, expected {file}, got {computed}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct FailedChecksumError {
    file: u16,
    computed: u16,
}

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
        Ok(Self::new(fl, dataset_offset, (), conf))
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
        ReadDatasetState::new(f, d, bounds, self.conf)
    }

    // this should only be called if $NEXTDATA is 0 or missing (if allowed)
    pub(crate) fn into_last_dataset(self) -> TEXTReadState<C> {
        let f = self.file_len;
        let d = self.dataset_offset;
        let dl =
            f.0.checked_sub(d.0)
                .expect("dataset offset should not exceed file length");
        let bounds = DatasetBounds::new(DatasetLen(dl), false);
        ReadDatasetState::new(f, d, bounds, self.conf)
    }
}

impl<C> TEXTReadState<C> {
    pub(crate) fn test_crc<R>(
        &self,
        h: &mut BufReader<R>,
        crc_start: u64,
        version: Version,
        conf: CRCConfig,
    ) -> WarningAndIOGroupResult<(Option<CRCOutput>, Option<u16>), CRCError, CRCError, ()>
    where
        R: Read + Seek,
    {
        if version == Version::FCS2_0 {
            return LogResult::new_ok((None, None));
        }
        let file_crc_out = io_to_log!(self.read_crc(h, crc_start));
        let res = match file_crc_out {
            CRCOutput::Invalid(_) => {
                let e = CRCError::from(MissingCRCError(crc_start));
                let computed_crc = if matches!(conf.compute_crc, ComputeCRC::Always) {
                    Some(io_to_log!(self.compute_crc(h, crc_start)))
                } else {
                    None
                };
                SwitchableErrorResult::new_switchable3(computed_crc, (), e, conf.allow_missing_crc)
                    .switchable_into_commutative()
            }
            CRCOutput::Valid { crc: file_crc, .. } => {
                if matches!(conf.compute_crc, ComputeCRC::Never) {
                    LogResult::new_ok(None)
                } else {
                    let computed_crc = io_to_log!(self.compute_crc(h, crc_start));
                    let e = CRCError::from(FailedChecksumError::new(file_crc, computed_crc));
                    let flag = conf.allow_mismatch_crc;
                    let crc_match = file_crc == 0 || file_crc == computed_crc;
                    let v = Some(computed_crc);
                    SwitchableErrorResult::new_switchable_ok_if3(crc_match, v, (), e, flag)
                        .switchable_into_commutative()
                }
            }
        };
        res.map_ok_value(|computed| (Some(file_crc_out), computed))
            .group()
            .map_errors(IOErrorGroup::Pure)
    }

    fn read_crc<R>(&self, h: &mut BufReader<R>, crc_start: u64) -> io::Result<CRCOutput>
    where
        R: Read + Seek,
    {
        h.seek(io::SeekFrom::Start(self.dataset_offset.0 + crc_start))?;
        let mut buf = vec![];
        h.take(8).read_to_end(&mut buf)?;
        if buf.len() == 8 {
            // NOTE the CRC has 8 digits but must parse to a 16-bit number.
            // It isn't clear why the CRC isn't just 5 bytes, since the max
            // u16 is ~64k.
            let ret = str::from_utf8(&buf)
                .ok()
                .and_then(|s| s.parse::<u16>().ok())
                .map_or(CRCOutput::Invalid(StringOrBytes::from(buf)), |crc| {
                    CRCOutput::Valid {
                        crc,
                        offset: crc_start,
                    }
                });
            Ok(ret)
        } else {
            Ok(CRCOutput::Invalid(StringOrBytes::from(buf)))
        }
    }

    fn compute_crc<R>(&self, h: &mut BufReader<R>, crc_start: u64) -> io::Result<u16>
    where
        R: Read + Seek,
    {
        h.seek(io::SeekFrom::Start(self.dataset_offset.0))?;
        let mut digest = Digest::new(FCS_CRC);
        io::copy(&mut h.take(crc_start), &mut digest)?;
        let crc = u16::try_from(digest.finalize()).expect("CRC should be 16 bit");
        Ok(crc)
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
}

impl WriteFCSDigest {
    pub(crate) fn new(compute_digest: ComputeWriteCRC, version: Version) -> Self {
        Self {
            digest: Digest::new(FCS_CRC),
            compute_digest,
            is_2_0: version == Version::FCS2_0,
        }
    }

    pub(crate) fn update_and_write<W: io::Write>(
        &mut self,
        h: &mut io::BufWriter<W>,
        bs: &[u8],
    ) -> io::Result<()> {
        self.update(bs);
        h.write_all(bs)
    }

    pub(crate) fn update(&mut self, bs: &[u8]) {
        if !self.is_2_0 && self.compute_digest.is_set() {
            self.digest.update(bs);
        }
    }

    pub(crate) fn write_final<W: io::Write>(&self, h: &mut io::BufWriter<W>) -> io::Result<()> {
        if self.is_2_0 {
            return Ok(());
        }
        let x = if self.compute_digest.is_set() {
            self.digest.finalize()
        } else {
            0
        };
        write!(h, "{x:0>8}")
    }
}

/// The CRC algorithm to use for FCS files.
///
/// CRC-16/KERMIT is the same thing as CRC-16/CCITT (but not the same as
/// CRC-16/CCITT-FALSE, which is often confused with CCITT). See
/// https://reveng.sourceforge.io/crc-catalogue/all.htm
pub(crate) const FCS_CRC: CrcAlgorithm = CrcAlgorithm::Crc16Kermit;
