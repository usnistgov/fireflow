use crate::api::{
    FlatDatasetFromKwsOutput, FlatDatasetOutput, FlatDatasetSummary, FlatTEXTOutput,
    FlatTEXTSummary, HeaderOrFlatTEXTWarning, HeaderOrFlatTextError, MultiFlatDatasetError,
    MultiFlatDatasetWarning, MultiStdDatasetError, MultiStdDatasetWarning, MultiStdTEXTError,
    MultiStdTEXTWarning, ReadFlatDatasetError, ReadFlatDatasetWarning, StdDatasetError,
    StdDatasetOutput, StdDatasetSummary, StdDatasetWarning, StdTEXTError, StdTEXTOutput,
    StdTEXTSummary, StdTEXTWarning, StringOrBytes, ValidKeywords,
};
use crate::config::{
    EvaledReadRepairKeywordsConfig, ReadFlatDatasetConfig, ReadFlatTEXTConfig,
    ReadStdDatasetConfig, ReadStdTEXTConfig, eval_repair_conf,
};
use crate::convert::{U64Ext as _, UsizeExt as _};
use crate::core::{Analysis, AnyCoreDataset, AnyCoreTEXT, Other, Others, OthersReader};
use crate::data::{
    DstIndex, ReadDelimAsciiWithoutRowsError, ReadDelimWithRowsAsciiError, SrcIndex,
    read_columns_buffered, read_columns_mmap, read_delim_with_rows_from_iter,
    read_delim_without_rows_from_iter, read_matrix_buffered, read_matrix_mmap,
};
use crate::header::autodetect_version;
use crate::logging::{
    IOAnonErrorGroup, IOErrorGroup, IOResult, ImpureError, LogResult, ResultExt as _,
    WarningsAndIOGroupResult, io_to_log, split_log,
};
use crate::segment::read::{
    AnyAnalysisOffsets, NonEmptyOffsets, Offsets, read_exact_owned, read_to_end_checked,
};
use crate::text::keywords::Tot;
use crate::validated::ascii_range::DelimAsciiRange;
use crate::validated::buf_file::BufFileReader;
use crate::validated::read_state::{DatasetOffset, HeaderReader, TEXTReader};

use fireflow_types::config::{
    ReadDataKeywordsConfig, ReadDatasetConfig, ReadHeaderAndTEXTConfig, ReadHeaderInnerConfig,
    ReadOffsetConfig, ReadSharedConfig, RowBufferSize,
};
use fireflow_types::keywords::Version;

use derive_more::AsRef;
use derive_new::new;
use memmap2::{Advice, Mmap};
use nonempty::{NESlice, NEVec};
use num_traits::FromBytes;

use std::borrow::Cow;
use std::fs::File;
use std::io::{self, Read as _, Seek as _, SeekFrom};
use std::path::PathBuf;
use std::time::Instant;

pub enum AnyReader {
    Buffered(BufFileReader),
    Mmap(Mmap),
}

#[derive(new)]
pub(crate) struct FlatTEXTOutputInner<T> {
    pub(crate) this: T,
    pub(crate) read_end: Instant,
    pub(crate) reader: TEXTReader,
}

impl AnyReader {
    pub(crate) fn read_ne_segment<I, S>(
        &mut self,
        offsets: &NonEmptyOffsets<I, S>,
    ) -> io::Result<Cow<'_, NESlice<u8>>> {
        match self {
            Self::Buffered(r) => offsets.read_contents_buffered(r).map(Cow::Owned),
            Self::Mmap(r) => Ok(Cow::Borrowed(offsets.read_contents_mmap(r))),
        }
    }

    pub(crate) fn read_segment<I, S>(
        &mut self,
        offsets: Offsets<I, S>,
    ) -> io::Result<Cow<'_, [u8]>> {
        match self {
            Self::Buffered(r) => offsets.read_contents_buffered(r).map(Cow::Owned),
            Self::Mmap(r) => Ok(Cow::Borrowed(offsets.read_contents_mmap(r))),
        }
    }

    pub(crate) fn read_analysis(&mut self, offsets: AnyAnalysisOffsets) -> io::Result<Analysis> {
        let buf = self.read_segment(offsets)?;
        Ok(Analysis(StringOrBytes::from(buf.into_owned())))
    }

    pub(crate) fn read_others(&mut self, osr: &OthersReader) -> io::Result<Others> {
        let mut others = vec![];
        for s in &osr.offsets {
            let buf = self.read_segment(s.offsets)?;
            others.push(Other(StringOrBytes::from(buf.into_owned())));
        }
        Ok(Others(others))
    }

    pub(crate) fn read_flat_text(
        self,
        dataset_offset: DatasetOffset,
        start_time: Instant,
        conf: &ReadFlatTEXTConfig,
    ) -> WarningsAndIOGroupResult<
        (FlatTEXTOutput, Self),
        HeaderOrFlatTEXTWarning,
        HeaderOrFlatTextError,
        FlatTEXTSummary,
    > {
        self.read_flat_text_inner(dataset_offset, start_time, conf)
            .map_ok_value(|out| (out.this, out.reader.into_inner()))
            .warnings_to_pure_errors(conf.shared, HeaderOrFlatTextError::from)
            .deanonymize()
    }

    pub(crate) fn read_std_text(
        self,
        dataset_offset: DatasetOffset,
        start_time: Instant,
        conf: &ReadStdTEXTConfig,
    ) -> WarningsAndIOGroupResult<
        (AnyCoreTEXT, StdTEXTOutput, Self),
        StdTEXTWarning,
        StdTEXTError,
        StdTEXTSummary,
    > {
        self.read_flat_text_inner(dataset_offset, start_time, conf)
            .map_pure_errors(StdTEXTError::from)
            .map_commutative_warnings(StdTEXTWarning::from)
            .and_then_commutative(|out| {
                out.this
                    .into_std_text(out.read_end, &out.reader, conf)
                    .map_commutative_warnings(StdTEXTWarning::from)
                    .map_errors(StdTEXTError::from)
                    .group()
                    .map_errors(IOErrorGroup::Pure)
                    .map_ok_value(|(core, std_out)| (core, std_out, out.reader.into_inner()))
            })
            .warnings_to_pure_errors(conf.shared, StdTEXTError::from)
            .deanonymize()
    }

    pub(crate) fn read_flat_dataset(
        self,
        dataset_offset: DatasetOffset,
        scan_next_dataset: bool,
        start_time: Instant,
        conf: &ReadFlatDatasetConfig,
    ) -> WarningsAndIOGroupResult<
        (FlatDatasetOutput, Self),
        ReadFlatDatasetWarning,
        ReadFlatDatasetError,
        FlatDatasetSummary,
    > {
        #[derive(AsRef)]
        struct LookupConfig {
            #[as_ref(EvaledReadRepairKeywordsConfig)]
            repair: EvaledReadRepairKeywordsConfig,
            #[as_ref(ReadDataKeywordsConfig)]
            data_kws: ReadDataKeywordsConfig,
            #[as_ref(ReadDatasetConfig)]
            dataset: ReadDatasetConfig,
            #[as_ref(ReadOffsetConfig)]
            offsets: ReadOffsetConfig,
        }

        self.read_flat_text_inner(dataset_offset, start_time, conf)
            .map_commutative_warnings(ReadFlatDatasetWarning::from)
            .map_pure_errors(ReadFlatDatasetError::from)
            .and_then_commutative(|out| {
                eval_repair_conf(conf.as_ref(), &out.this.keywords)
                    .map_ok_value(|repair| {
                        LookupConfig {
                            repair,
                            // TODO useless clone
                            data_kws: AsRef::<ReadDataKeywordsConfig>::as_ref(&conf).clone(),
                            dataset: *AsRef::<ReadDatasetConfig>::as_ref(&conf),
                            offsets: *AsRef::<ReadOffsetConfig>::as_ref(&conf),
                        }
                    })
                    .map_errors(ReadFlatDatasetError::from)
                    .nowarn_into_warn()
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .map_ok_value(|x| (x, out.reader, out.this, out.read_end))
            })
            .and_then_commutative(|(lconf, mut treader, mut flat, read_end)| {
                let mut kws = flat.keywords;
                let repair_res = kws
                    .repair(&lconf.repair)
                    .map_commutative_warnings(ReadFlatDatasetWarning::from)
                    .map_errors(ReadFlatDatasetError::from);

                let ltx = kws.std.as_transaction();
                let version = flat.flat_diagnostics.header_supp.header.version;
                let hns = &mut flat.flat_diagnostics.header_supp;
                let oride = conf.flat.version_override.as_ref();
                autodetect_version(version, &kws.std, oride)
                    .map_err(ReadFlatDatasetError::from)
                    .map_err(IOErrorGroup::new_pure_one)
                    .into_log()
                    .and_then_commutative(|(new_version, scores)| {
                        FlatDatasetFromKwsOutput::read(
                            &mut treader,
                            new_version,
                            &ltx,
                            hns,
                            scan_next_dataset,
                            read_end,
                            &lconf,
                        )
                        .map_commutative_warnings(ReadFlatDatasetWarning::from)
                        .map_pure_errors(ReadFlatDatasetError::from)
                        .map_ok_value(|x| (x, scores))
                    })
                    .zip_io_group_commutative(repair_res)
                    .map_ok_value(|((dataset, scores), repair_diag)| {
                        let final_std = ltx.commit();
                        // Rebuild final keywords object since we may have
                        // repaired them to read DATA
                        let final_kws =
                            ValidKeywords::new(final_std, kws.pnonstd, kws.pstd, kws.nonstd);
                        let ret = FlatDatasetOutput::new(
                            final_kws,
                            flat.flat_diagnostics,
                            dataset,
                            scores,
                            repair_diag,
                        );
                        (ret, treader.into_inner())
                    })
            })
            .warnings_to_pure_errors(conf.shared, ReadFlatDatasetError::from)
            .deanonymize()
    }

    pub(crate) fn read_std_dataset(
        self,
        dataset_offset: DatasetOffset,
        scan_next_dataset: bool,
        start_time: Instant,
        conf: &ReadStdDatasetConfig,
    ) -> WarningsAndIOGroupResult<
        ((AnyCoreDataset, StdDatasetOutput), Self),
        StdDatasetWarning,
        StdDatasetError,
        StdDatasetSummary,
    > {
        self.read_flat_text_inner(dataset_offset, start_time, conf)
            .map_pure_errors(StdDatasetError::from)
            .map_commutative_warnings(StdDatasetWarning::from)
            .and_then_commutative(|mut out| {
                out.this
                    .into_std_dataset(&mut out.reader, scan_next_dataset, out.read_end, conf)
                    .map_commutative_warnings(StdDatasetWarning::from)
                    .map_pure_errors(StdDatasetError::from)
                    .map_ok_value(|x| (x, out.reader.into_inner()))
            })
            .warnings_to_pure_errors(conf.shared, StdDatasetError::from)
            .deanonymize()
    }

    pub(crate) fn read_flat_texts(
        self,
        skip: Option<usize>,
        limit: Option<usize>,
        scan_next_dataset: bool,
        mut start_time: Instant,
        conf: &ReadFlatTEXTConfig,
    ) -> WarningsAndIOGroupResult<
        Vec<FlatTEXTOutput>,
        HeaderOrFlatTEXTWarning,
        HeaderOrFlatTextError,
        FlatTEXTSummary,
    > {
        let mut dataset_offset = Some(DatasetOffset::default());
        let mut count = 0_usize;
        let mut outputs = vec![];
        let mut warnings = vec![];
        let mut this_reader = self;
        while let Some(dso) = dataset_offset
            && limit.is_none_or(|x| count <= x)
        {
            let res = Self::read_flat_text(this_reader, dso, start_time, conf);
            let succ = split_log!(res);
            let ((ret, next_reader), ()) = succ.resolve(|ws| warnings.extend(ws));
            this_reader = next_reader;
            let scanned_dataset_offset = if scan_next_dataset {
                let pos = ret.flat_diagnostics.header_supp.text_other_max_end_offset();
                let abs_pos = pos + u64::from(dso);
                io_to_log!(this_reader.next_dataset_boundary(abs_pos))
            } else {
                None
            };
            let hns = &ret.flat_diagnostics.header_supp;
            let nd = hns.nextdata.map(u64::from);
            dataset_offset = scanned_dataset_offset.or_else(|| {
                let n = nd?;
                (n > 0).then_some(DatasetOffset(dso.0 + n))
            });
            if count > skip.unwrap_or_default() {
                outputs.push(ret);
            }
            count += 1;
            // Subsequent start times will be a bit shorter because the file is
            // already open.
            start_time = Instant::now();
        }
        LogResult::new_ok(outputs).set_commutative_warnings(warnings)
    }

    pub(crate) fn read_std_texts(
        self,
        skip: Option<usize>,
        limit: Option<usize>,
        scan_next_dataset: bool,
        mut start_time: Instant,
        conf: &ReadStdTEXTConfig,
    ) -> WarningsAndIOGroupResult<
        Vec<(AnyCoreTEXT, StdTEXTOutput)>,
        MultiStdTEXTWarning,
        MultiStdTEXTError,
        StdTEXTSummary,
    > {
        let mut dataset_offset = Some(DatasetOffset::default());
        let mut count = 0_usize;
        let mut outputs = vec![];
        let mut warnings = vec![];
        let mut this_reader = self;
        let rconf = ReadFlatTEXTConfig {
            header: AsRef::<ReadHeaderInnerConfig>::as_ref(conf).clone(),
            flat: AsRef::<ReadHeaderAndTEXTConfig>::as_ref(conf).clone(),
            offset: *conf.as_ref(),
            shared: *conf.as_ref(),
        };
        macro_rules! get_scanned {
            ($reader:expr, $pos:expr) => {
                if scan_next_dataset {
                    io_to_log!($reader.next_dataset_boundary($pos))
                } else {
                    None
                }
            };
        }
        while let Some(dso) = dataset_offset
            && limit.is_none_or(|x| count < x)
        {
            let nextdata_res = if skip.is_some_and(|s| count < s) {
                let res = Self::read_flat_text(this_reader, dso, start_time, &rconf)
                    .map_commutative_warnings(MultiStdTEXTWarning::from)
                    .map_pure_errors(MultiStdTEXTError::from)
                    .map_error(|e| e.set_group(StdTEXTSummary));
                let ((ret, next_reader), ()) = split_log!(res).resolve(|ws| warnings.extend(ws));
                this_reader = next_reader;
                let pos = ret.flat_diagnostics.header_supp.text_other_max_end_offset();
                let abs_pos = pos + u64::from(dso);
                let scanned_dataset_offset = get_scanned!(this_reader, abs_pos);
                let hns = ret.flat_diagnostics.header_supp;
                let nd = hns.nextdata.map(u64::from);
                dataset_offset = scanned_dataset_offset.or_else(|| {
                    let n = nd?;
                    (n > 0).then_some(DatasetOffset(dso.0 + n))
                });
                None
            } else {
                let res = Self::read_std_text(this_reader, dso, start_time, conf)
                    .map_commutative_warnings(MultiStdTEXTWarning::from)
                    .map_pure_errors(MultiStdTEXTError::from);
                let ((ret, std_out, next_reader), ()) =
                    split_log!(res).resolve(|ws| warnings.extend(ws));
                this_reader = next_reader;
                let pos = std_out
                    .flat_diagnostics
                    .header_supp
                    .text_other_max_end_offset();
                let abs_pos = pos + u64::from(dso);
                let scanned_dataset_offset = get_scanned!(this_reader, abs_pos);
                dataset_offset = scanned_dataset_offset.or_else(|| {
                    let nd = u64::from(std_out.flat_diagnostics.header_supp.nextdata?);
                    (nd > 0).then_some(DatasetOffset(dso.0 + nd))
                });
                Some((ret, std_out))
            };
            outputs.extend(nextdata_res);
            count += 1;
            // Subsequent start times will be a bit shorter because the file is
            // already open.
            start_time = Instant::now();
        }
        LogResult::new_ok(outputs).set_commutative_warnings(warnings)
    }

    pub(crate) fn read_flat_datasets(
        self,
        skip: Option<usize>,
        limit: Option<usize>,
        scan_next_dataset: bool,
        start_time: Instant,
        conf: &ReadFlatDatasetConfig,
    ) -> WarningsAndIOGroupResult<
        Vec<FlatDatasetOutput>,
        MultiFlatDatasetWarning,
        MultiFlatDatasetError,
        FlatDatasetSummary,
    > {
        self.read_nextdata_loop(
            skip,
            limit,
            scan_next_dataset,
            start_time,
            conf,
            FlatDatasetSummary,
            Self::read_flat_dataset,
            |ret| ret.dataset.dataset_diagnostics.next_dataset_offset,
        )
    }

    pub(crate) fn read_std_datasets(
        self,
        skip: Option<usize>,
        limit: Option<usize>,
        scan_next_dataset: bool,
        start_time: Instant,
        conf: &ReadStdDatasetConfig,
    ) -> WarningsAndIOGroupResult<
        Vec<(AnyCoreDataset, StdDatasetOutput)>,
        MultiStdDatasetWarning,
        MultiStdDatasetError,
        StdDatasetSummary,
    > {
        self.read_nextdata_loop(
            skip,
            limit,
            scan_next_dataset,
            start_time,
            conf,
            StdDatasetSummary,
            Self::read_std_dataset,
            |ret| ret.1.dataset.dataset_diagnostics.next_dataset_offset,
        )
    }

    fn read_flat_text_inner<C>(
        self,
        dataset_offset: DatasetOffset,
        start_time: Instant,
        conf: &C,
    ) -> WarningsAndIOGroupResult<
        FlatTEXTOutputInner<FlatTEXTOutput>,
        HeaderOrFlatTEXTWarning,
        HeaderOrFlatTextError,
        (),
    >
    where
        C: AsRef<ReadHeaderAndTEXTConfig> + AsRef<ReadHeaderInnerConfig> + AsRef<ReadOffsetConfig>,
    {
        HeaderReader::try_new(self, dataset_offset, start_time)
            .map_err(HeaderOrFlatTextError::from)
            .map_err(IOAnonErrorGroup::new_pure_one)
            .into_log()
            .and_then_commutative(|hreader| FlatTEXTOutput::read(hreader, conf))
    }

    #[allow(clippy::too_many_arguments)]
    fn read_nextdata_loop<X, W, E, Wi, Ei, G, C, Fsucc, Fnext>(
        self,
        skip: Option<usize>,
        limit: Option<usize>,
        scan_next_dataset: bool,
        mut start_time: Instant,
        conf: &C,
        g: G,
        mut f0: Fsucc,
        mut fnext: Fnext,
    ) -> WarningsAndIOGroupResult<Vec<X>, W, E, G>
    where
        Fsucc: FnMut(
            Self,
            DatasetOffset,
            bool,
            Instant,
            &C,
        ) -> WarningsAndIOGroupResult<(X, Self), Wi, Ei, G>,
        Fnext: FnMut(&X) -> Option<DatasetOffset>,
        E: From<HeaderOrFlatTextError> + From<Ei>,
        W: From<HeaderOrFlatTEXTWarning> + From<Wi>,
        C: AsRef<ReadHeaderInnerConfig>
            + AsRef<ReadHeaderAndTEXTConfig>
            + AsRef<ReadOffsetConfig>
            + AsRef<ReadSharedConfig>,
        G: Copy,
    {
        let mut dataset_offset = Some(DatasetOffset::default());
        let mut count = 0_usize;
        let mut outputs = vec![];
        let mut warnings = vec![];
        let mut this_reader = self;
        let rconf = ReadFlatTEXTConfig {
            header: AsRef::<ReadHeaderInnerConfig>::as_ref(conf).clone(),
            flat: AsRef::<ReadHeaderAndTEXTConfig>::as_ref(conf).clone(),
            offset: *conf.as_ref(),
            shared: *conf.as_ref(),
        };
        while let Some(dso) = dataset_offset
            && limit.is_none_or(|x| count < x)
        {
            let nextdata_res = if skip.is_some_and(|s| count < s) {
                let res = Self::read_flat_text(this_reader, dso, start_time, &rconf)
                    .map_commutative_warnings(W::from)
                    .map_pure_errors(E::from)
                    .map_error(|e| e.set_group(g));
                let ((ret, next_reader), ()) = split_log!(res).resolve(|ws| warnings.extend(ws));
                let pos = ret.flat_diagnostics.header_supp.text_other_max_end_offset();
                let abs_pos = pos + u64::from(dso);
                this_reader = next_reader;
                let scanned_dataset_offset = if scan_next_dataset {
                    io_to_log!(this_reader.next_dataset_boundary(abs_pos))
                } else {
                    None
                };
                let hns = ret.flat_diagnostics.header_supp;
                let nd = hns.nextdata.map(u64::from);
                dataset_offset = scanned_dataset_offset.or_else(|| {
                    let n = nd?;
                    (n > 0).then_some(DatasetOffset(dso.0 + n))
                });
                None
            } else {
                let res = f0(this_reader, dso, scan_next_dataset, start_time, conf)
                    .map_commutative_warnings(W::from)
                    .map_pure_errors(E::from);
                let ((ret, next_reader), ()) = split_log!(res).resolve(|ws| warnings.extend(ws));
                this_reader = next_reader;
                dataset_offset =
                    fnext(&ret).and_then(|next_dso| (next_dso.0 > 0).then_some(next_dso));
                Some(ret)
            };
            outputs.extend(nextdata_res);
            count += 1;
            // Subsequent start times will be a bit shorter because the file is
            // already open.
            start_time = Instant::now();
        }
        LogResult::new_ok(outputs).set_commutative_warnings(warnings)
    }

    /// Read stream of bytes using buffer where each value is the same type
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn read_matrix<const LEN: usize, T, F>(
        &mut self,
        start_pos: u64,
        nrows: usize,
        ncols: usize,
        byte_swap: F,
        buffer_nbytes: RowBufferSize,
    ) -> io::Result<Vec<Vec<T>>>
    where
        T: FromBytes<Bytes = [u8; LEN]> + Default + Clone,
        F: Fn(&mut [u8; LEN]),
    {
        if nrows == 0 || ncols == 0 {
            return Ok(vec![]);
        }

        match self {
            Self::Buffered(r) => {
                r.seek(SeekFrom::Start(start_pos))?;
                read_matrix_buffered(r, nrows, ncols, byte_swap, buffer_nbytes)
            }
            Self::Mmap(r) => {
                let p0 = start_pos.u64_to_usize();
                let cols = read_matrix_mmap(r, p0, nrows, ncols, byte_swap, buffer_nbytes);
                Ok(cols)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn read_columns<C, Rng, E, Fr, Fw, Fc>(
        &mut self,
        start_pos: u64,
        nrows: usize,
        ranges: &[Rng],
        fread: Fr,
        fwidth: Fw,
        fcol: Fc,
        buffer_nbytes: RowBufferSize,
    ) -> IOResult<Vec<C>, E>
    where
        Fr: FnMut(&mut C, DstIndex, &[u8], SrcIndex) -> Result<(), E>,
        Fw: Fn(&Rng) -> usize,
        Fc: Fn(&Rng, usize) -> C,
    {
        if ranges.is_empty() || nrows == 0 {
            return Ok(vec![]);
        }

        match self {
            Self::Buffered(r) => {
                r.seek(SeekFrom::Start(start_pos))?;
                read_columns_buffered(r, nrows, ranges, fread, fwidth, fcol, buffer_nbytes)
            }
            Self::Mmap(r) => {
                let p0 = start_pos.u64_to_usize();
                read_columns_mmap(r, p0, nrows, ranges, fread, fwidth, fcol, buffer_nbytes)
                    .map_err(ImpureError::Pure)
            }
        }
    }

    pub(crate) fn read_delim_with_rows(
        &mut self,
        start_pos: u64,
        ranges: &NESlice<DelimAsciiRange>,
        tot: Tot,
        nbytes: usize,
    ) -> IOResult<Vec<Vec<u64>>, ReadDelimWithRowsAsciiError> {
        match self {
            Self::Buffered(r) => {
                r.seek(SeekFrom::Start(start_pos))?;
                #[allow(
                    clippy::unbuffered_bytes,
                    reason = "rare use case, efficiency not a concern"
                )]
                read_delim_with_rows_from_iter(ranges, r.bytes().take(nbytes), tot)
            }
            Self::Mmap(r) => {
                let p0 = start_pos.u64_to_usize();
                let p1 = p0 + nbytes;
                let bytes = r[p0..p1].iter().copied().map(Ok);
                read_delim_with_rows_from_iter(ranges, bytes, tot)
            }
        }
    }

    pub(crate) fn read_delim_without_rows(
        &mut self,
        start_pos: u64,
        ranges: &NESlice<DelimAsciiRange>,
        nbytes: usize,
    ) -> IOResult<Vec<Vec<u64>>, ReadDelimAsciiWithoutRowsError> {
        match self {
            Self::Buffered(r) => {
                r.seek(SeekFrom::Start(start_pos))?;
                #[allow(
                    clippy::unbuffered_bytes,
                    reason = "rare use case, efficiency not a concern"
                )]
                read_delim_without_rows_from_iter(ranges, r.bytes().take(nbytes))
            }
            Self::Mmap(r) => {
                let p0 = start_pos.u64_to_usize();
                let p1 = p0 + nbytes;
                let bytes = r[p0..p1].iter().copied().map(Ok);
                read_delim_without_rows_from_iter(ranges, bytes)
            }
        }
    }

    pub(crate) fn file_len(&self) -> u64 {
        match self {
            Self::Buffered(r) => r.file_len(),
            Self::Mmap(r) => r.len().usize_to_u64(),
        }
    }

    pub(crate) fn open(path: &PathBuf, mmap: bool) -> io::Result<Self> {
        if mmap {
            let file = File::options().read(true).open(path)?;
            // SAFETY: we read the riot act to the user informing them of all
            // the terrible things that may happen if they don't use memory
            // mapping properly.
            let reader = unsafe { Mmap::map(&file)? };
            reader.advise(Advice::Sequential)?;
            Ok(Self::Mmap(reader))
        } else {
            Ok(Self::Buffered(BufFileReader::open(path)?))
        }
    }

    pub(crate) fn read_bytes(&mut self, start_pos: u64, nbytes: u64) -> io::Result<Cow<[u8]>> {
        match self {
            Self::Buffered(r) => {
                r.seek(io::SeekFrom::Start(start_pos))?;
                read_exact_owned(r, nbytes).map(Cow::Owned)
            }
            Self::Mmap(r) => {
                // TODO do these need to return OOM errors?
                let p0 = start_pos.u64_to_usize();
                let p1 = p0 + nbytes.u64_to_usize();
                Ok(Cow::Borrowed(&r[p0..p1]))
            }
        }
    }

    pub(crate) fn read_bytes_checked(
        &mut self,
        start_pos: u64,
        nbytes: u64,
    ) -> IOResult<Cow<[u8]>, u64> {
        match self {
            Self::Buffered(r) => {
                r.seek(io::SeekFrom::Start(start_pos))?;
                let remaining = r.remaining_bytes();
                if remaining < nbytes {
                    Err(ImpureError::Pure(remaining))
                } else {
                    Ok(Cow::Owned(read_exact_owned(r, nbytes)?))
                }
            }
            Self::Mmap(r) => {
                // TODO do these need to return OOM errors?
                let p0 = start_pos.u64_to_usize();
                let p1 = p0 + nbytes.u64_to_usize();
                let remaining = r.len() - p0;
                if remaining < nbytes.u64_to_usize() {
                    Err(ImpureError::Pure(remaining.usize_to_u64()))
                } else {
                    Ok(Cow::Borrowed(&r[p0..p1]))
                }
            }
        }
    }

    pub(crate) fn read_exact_checked<const LEN: usize>(
        &mut self,
        start_pos: u64,
    ) -> IOResult<[u8; LEN], u64> {
        match self {
            Self::Buffered(r) => {
                r.seek(io::SeekFrom::Start(start_pos))?;
                let remaining = r.remaining_bytes();
                if remaining.u64_to_usize() < LEN {
                    Err(ImpureError::Pure(remaining))
                } else {
                    let mut buf = [0_u8; LEN];
                    r.read_exact(&mut buf)?;
                    Ok(buf)
                }
            }
            Self::Mmap(r) => {
                let p0 = start_pos.u64_to_usize();
                let p1 = p0 + LEN;
                let remaining = r.len() - p0;
                if remaining < LEN {
                    Err(ImpureError::Pure(remaining.usize_to_u64()))
                } else {
                    Ok(r[p0..p1].try_into().unwrap())
                }
            }
        }
    }

    pub(crate) fn next_dataset_boundary(
        &mut self,
        start_pos: u64,
    ) -> io::Result<Option<DatasetOffset>> {
        match self {
            Self::Buffered(r) => {
                r.seek(io::SeekFrom::Start(start_pos))?;
                next_dataset_boundary_buffered(r)
            }
            Self::Mmap(r) => Ok(next_dataset_boundary_mmap(r, start_pos.u64_to_usize())),
        }
    }
}

pub(crate) fn next_dataset_boundary_buffered(
    reader: &mut BufFileReader,
) -> io::Result<Option<DatasetOffset>> {
    const OVERLAP_SIZE: usize = BOUNDARY_MATCH_SIZE - 1;
    const BUF_SIZE: u64 = 32000;

    // TODO this whole buffer dance probably isn't necessary since this already
    // uses a buffered reader. Not clear what method to use. .tuple_windows
    // is probably too slow since it copies each position N-1 times
    let mut buf = vec![];
    let mut file_offset = reader.pos();
    read_to_end_checked(reader, BUF_SIZE, &mut buf)?;

    while buf.len() >= BOUNDARY_MATCH_SIZE {
        for w in buf[..].array_windows() {
            if match_bytes_version(w).is_some() {
                return Ok(Some(DatasetOffset(file_offset)));
            }
            file_offset += 1;
        }
        let mut tmp = [0_u8; OVERLAP_SIZE];
        tmp.copy_from_slice(&buf[buf.len() - OVERLAP_SIZE..]);
        buf.clear();
        buf.extend(tmp);
        let overlap_nbytes = BUF_SIZE - OVERLAP_SIZE.usize_to_u64();
        read_to_end_checked(reader, overlap_nbytes, &mut buf)?;
    }

    Ok(None)
}

pub(crate) fn next_dataset_boundary_mmap(
    reader: &Mmap,
    file_offset: usize,
) -> Option<DatasetOffset> {
    for (i, w) in reader.as_ref()[file_offset..].array_windows().enumerate() {
        if match_bytes_version(w).is_some() {
            let o = (file_offset + i).usize_to_u64();
            return Some(DatasetOffset(o));
        }
    }

    None
}

pub(crate) fn match_bytes_version(xs: &[u8; BOUNDARY_MATCH_SIZE]) -> Option<Version> {
    match xs {
        b"FCS2.0    " => Some(Version::FCS2_0),
        b"FCS3.0    " => Some(Version::FCS3_0),
        b"FCS3.1    " => Some(Version::FCS3_1),
        b"FCS3.2    " => Some(Version::FCS3_2),
        _ => None,
    }
}

pub(crate) const BOUNDARY_MATCH_SIZE: usize = 10;
