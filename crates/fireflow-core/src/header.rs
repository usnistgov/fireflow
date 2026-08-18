//! Reading and writing the HEADER segment

use crate::config::{
    AppendableFlag, ConfigFlag as _, ReadHeaderInnerConfig, ReadOffsetConfig,
    SelectVersionStrategy, VersionOverride,
};
use crate::convert::InstantExt as _;
use crate::core::{DarkBytes, Other, WriteHeaderAndTextConfig};
use crate::logging::{
    IOAnonErrorGroup, IOErrorGroup, IOGroupResult, LogResult, ResultExt as _,
    WarningsAndIOGroupResult, io_to_log,
};
use crate::segment::read::{
    GuessOtherWidthError, HeaderAnalysisOffsets, HeaderDataOffsets, HeaderOffsets,
    HeaderOffsetsError, HeaderToHeaderOffsetsOverlap, IsOffsetPair as _, OriginalOffsets,
    OtherOffsetOutput, OtherOffsets20, PrimaryTextOffsets,
};
use crate::segment::write::{
    HeaderAnalysisOffsetsToWrite, HeaderDataOffsetsToWrite, OffsetsToWrite, OtherOffsetsToWrite,
    PrimaryTextOffsetsToWrite, SupplementalTextOffsetsToWrite, TEXTAnalysisOffsetsToWrite,
    TEXTDataOffsetsToWrite,
};
use crate::text::keyword_enum::{
    AnyKeyword, Escaped, Keyword0FromValue as _, NEStringKeyword0, OffsetKeyword, OptKeyword,
    OptRootKeyword, ReqKeyword, StdOrNonStdOptRootKeyword,
};
use crate::text::keywords::{
    Beginanalysis, Begindata, Beginstext, Endanalysis, Enddata, Endstext, KeywordOptimizer,
    KeywordVersionScore, Nextdata, Par,
};
use crate::text::lookup::ReqMetarootKey as _;
use crate::validated::ascii_uint::{HeaderString, Uint8DigitOverflowError, UintZeroPad20};
use crate::validated::header_offsets::{
    FinalHeaderOffsets, HEADER_LEN, HeaderOffsetsValidationError,
};
use crate::validated::keys::{DKey0, Key as _, StdKeywords};
use crate::validated::read_state::{DatasetOffset, HeaderReadState, WriteFCSDigest};
use crate::validated::textdelim::{DelimCollisionError, HasDelim as _};

use fireflow_types::config::EnumStrIter as _;
use fireflow_types::keywords::{Version, VersionFormatError};
use fireflow_types::nonempty_string::{NEStr, NEString};

use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty_collections::NEVec;
use nonempty_collections::{IntoIteratorExt as _, iter::NonEmptyIterator as _};
use num_traits::identities::Zero;
use thiserror::Error;

use std::fmt::{self, Write as _};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write as _};
use std::iter::once;
use std::time::Instant;

#[cfg(feature = "serde")]
use serde::Serialize;

#[cfg(feature = "python")]
use {
    fireflow_core_proc::{AllIntoPyErr, DisplayAsPyErr},
    fireflow_types::python as py,
};

/// The uncorrected segments from the HEADER
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct OriginalHeaderOffsets {
    pub text: OriginalOffsets,
    pub data: OriginalOffsets,
    pub analysis: OriginalOffsets,
    pub other: Vec<OriginalOffsets>,
}

/// Keyword scores for all versions generated when guessing version
///
/// Each score should sum to the same number.
pub type KeywordVersionScores = (
    KeywordVersionScore,
    KeywordVersionScore,
    KeywordVersionScore,
    KeywordVersionScore,
);

/// The segments to be written in the HEADER.
#[derive(Clone, new)]
pub(crate) struct WriteHeaderSegments<T> {
    pub(crate) text: PrimaryTextOffsetsToWrite,
    pub(crate) data: HeaderDataOffsetsToWrite,
    pub(crate) analysis: HeaderAnalysisOffsetsToWrite,
    pub(crate) other: Vec<OtherOffsetsToWrite<T>>,
}

impl<T> WriteHeaderSegments<T> {
    pub(crate) fn h_write<W: io::Write>(
        &self,
        h: &mut BufWriter<W>,
        version: Version,
        digest: &mut WriteFCSDigest,
    ) -> io::Result<()>
    where
        T: Zero + fmt::Display + Copy,
    {
        // 6+4+16+16+16 bytes
        let mut buf = String::new();
        write!(
            buf,
            "{version}    {}{}{}",
            self.text, self.data, self.analysis
        )
        .unwrap();
        for o in &self.other {
            write!(buf, "{o}").unwrap();
        }
        digest.update_and_write(h, buf.as_bytes())
    }
}

/// Output from parsing the FCS header.
///
/// Includes version and the three main segments (TEXT, DATA, ANALYSIS) plus
/// any OTHER segments after the first 58 bytes.
///
/// Only valid segments are to be put in this struct (ie begin <= end).
#[derive(Clone, PartialEq, new)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct Header {
    /// The offset in the FCS file where this HEADER appears.
    pub dataset_offset: DatasetOffset,

    /// FCS version (first 6 bytes)
    pub version: Version,

    /// The offsets as read from the header after overlap corrections.
    ///
    /// File length will not be considered, so offsets may exceed EOF.
    pub final_offsets: FinalHeaderOffsets,

    /// The offsets as originally written in the FCS file.
    pub original_offsets: OriginalHeaderOffsets,

    /// Overlaps between offsets from HEADER that were corrected.
    pub overlaps: Vec<HeaderToHeaderOffsetsOverlap>,

    /// Bytes between the end of the HEADER and the first segment.
    ///
    /// The "end of the HEADER" is the first byte after the second ANALYSIS
    /// offset or the last OTHER offset pair if it exists.
    pub dark_bytes: Option<DarkBytes>,

    /// The number of nanoseconds spent reading HEADER.
    pub read_header_ns: u128,
}

/// Result of parsing HEADER.
#[derive(new)]
pub struct ParseHeaderOutput {
    pub(crate) header: Header,
    pub(crate) read_end: Instant,
}

impl Header {
    pub fn h_read<C, R>(
        h: &mut BufReader<R>,
        st: &mut HeaderReadState<C>,
    ) -> WarningsAndIOGroupResult<ParseHeaderOutput, GuessOtherWidthError, HeaderError, ()>
    where
        C: AsRef<ReadHeaderInnerConfig> + AsRef<ReadOffsetConfig>,
        R: Read + Seek,
    {
        io_to_log!(h.seek(SeekFrom::Start(st.dataset_offset().0)));
        let req = io_to_log!(ReqHeader::h_read(h, st));

        let (text, text_orig) = req.text;
        let (data, data_orig) = req.data;
        let (analysis, analysis_orig) = req.analysis;

        let min_coord = [
            text.as_nonempty().map(|x| x.begin()),
            data.as_nonempty().map(|x| x.begin()),
            analysis.as_nonempty().map(|x| x.begin()),
        ]
        .into_iter()
        .flatten()
        .min();

        let other_res = if let Some(m) = min_coord {
            OtherOffsets20::h_read_others(h, m, st)
        } else {
            LogResult::new_ok(OtherOffsetOutput::default())
        };

        let oconf: &ReadOffsetConfig = st.conf().as_ref();

        other_res
            .map_pure_errors(HeaderError::from)
            .and_then_commutative(|other| {
                let (os_final, os_orig, dark_other) = other.finalize();
                let original =
                    OriginalHeaderOffsets::new(text_orig, data_orig, analysis_orig, os_orig);
                let limit = oconf.overlap_correction_limit;
                let d = st.dataset_offset();
                FinalHeaderOffsets::try_new_with_limit(text, data, analysis, os_final, limit)
                    .map_errors(HeaderError::from)
                    .nowarn_into_warn()
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .map_ok_value(|(final_, overlaps)| {
                        let t0 = st.start_time();
                        let t1 = Instant::now();
                        let t = t1.duration_since1(t0).as_nanos();
                        let hdr =
                            Self::new(d, req.version, final_, original, overlaps, dark_other, t);
                        ParseHeaderOutput::new(hdr, t1)
                    })
            })
    }
}

#[derive(new)]
struct ReqHeader {
    version: Version,
    text: (PrimaryTextOffsets, OriginalOffsets),
    data: (HeaderDataOffsets, OriginalOffsets),
    analysis: (HeaderAnalysisOffsets, OriginalOffsets),
}

impl ReqHeader {
    fn h_read<C, R>(
        h: &mut BufReader<R>,
        st: &HeaderReadState<C>,
    ) -> IOGroupResult<Self, HeaderError, ()>
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderInnerConfig> + AsRef<ReadOffsetConfig>,
    {
        let conf: &ReadHeaderInnerConfig = st.conf().as_ref();
        let text_cor = conf.text_correction;
        let data_cor = conf.data_correction;
        let anal_cor = conf.analysis_correction;

        #[allow(clippy::as_conversions, reason = "const not stable yet")]
        let mut buf = [0_u8; HEADER_LEN as usize];
        let remaining = st.remaining_bytes(h)?;
        if remaining < HEADER_LEN.into() {
            let e = HeaderNoBytesError(remaining).into();
            return Err(IOAnonErrorGroup::new_pure_one(e));
        }
        h.read_exact(&mut buf)?;

        let vers_res = read_version(&buf).map_err(HeaderError::from);
        let space_res = Self::read_spaces(&buf).map_err(HeaderError::from);

        let (version, ()) = vers_res
            .zip(space_res)
            .group()
            .resolve_nowarn()
            .map_err(IOErrorGroup::Pure)?;

        macro_rules! slice8 {
            ($from:expr, $to:expr) => {{
                const _: () = assert!($to - $from == 8, "slice should be 8");
                buf[$from..$to].try_into().unwrap()
            }};
        }

        let text_res = HeaderOffsets::read_primary(
            slice8!(10, 18),
            slice8!(18, 26),
            true,
            text_cor,
            version,
            st,
        );
        let data_res = HeaderOffsets::read_primary(
            slice8!(26, 34),
            slice8!(34, 42),
            false,
            data_cor,
            version,
            st,
        );
        let anal_res = HeaderOffsets::read_primary(
            slice8!(42, 50),
            slice8!(50, 58),
            false,
            anal_cor,
            version,
            st,
        );

        text_res
            .zip3_commutative(data_res, anal_res)
            .map_errors(HeaderError::from)
            .group()
            .resolve_nowarn()
            .map_err(IOErrorGroup::Pure)
            .map(|(t, d, a)| Self::new(version, t, d, a))
    }

    fn read_spaces(buf: &HeaderBuf) -> Result<(), HeaderSpacesFormatError> {
        if buf[6..10].iter().all(|x| *x == 32) {
            Ok(())
        } else {
            Err(HeaderSpacesFormatError)
        }
    }
}

fn read_version(buf: &HeaderBuf) -> Result<Version, VersionError> {
    if let Ok(s) = str::from_utf8(&buf[0..6]) {
        s.parse().map_err(VersionError::from)
    } else {
        Err(VersionNonUtf8Error(buf.to_vec()).into())
    }
}

pub(crate) fn autodetect_version(
    version: Version,
    kws: &StdKeywords,
    ver_override: Option<&VersionOverride>,
) -> Result<(Version, Option<KeywordVersionScores>), GuessVersionError> {
    match ver_override {
        None => Ok((version, None)),
        Some(VersionOverride::Force(v)) => Ok((*v, None)),
        Some(VersionOverride::AutoDetect {
            strategy,
            prioritize_current,
        }) => {
            let rank =
                |(v0, s0): &(Version, KeywordVersionScore),
                 (v1, s1): &(Version, KeywordVersionScore)| match strategy {
                    SelectVersionStrategy::Earliest => v1.cmp(v0),
                    SelectVersionStrategy::Latest => v0.cmp(v1),
                    SelectVersionStrategy::Loose => s0.good_opt.cmp(&s1.good_opt),
                    SelectVersionStrategy::Strict => s1.good_opt.cmp(&s0.good_opt),
                };
            let par = Par::get_metaroot_req(kws).map_err(|_| GuessVersionError::NoPar)?;
            let mut opt = KeywordOptimizer::default();
            for (k, v) in kws {
                opt.classify_keyword(k, v.as_ne_str(), par);
            }
            let scores = Version::ITEMS.map(|v| (v, opt.get_score(v, par)));
            let ret_scores = || Some(scores.clone().map(|(_, s)| s).into());
            if let Some(xs) = scores
                .iter()
                .filter(|(_, s)| s.is_passing(false))
                .try_into_nonempty_iter()
            {
                // Found at least one version that doesn't require dropping,
                // rank by strategy to select
                let ys: NEVec<_> = xs.collect();
                let chosen_version =
                    if ys.iter().find(|(v, _)| *v == version).is_some() && *prioritize_current {
                        version
                    } else {
                        ys.nonempty_iter().max_by(|&x, &y| rank(x, y)).0
                    };
                Ok((chosen_version, ret_scores()))
            } else if let Some(xs) = scores
                .iter()
                .filter(|(_, s)| s.is_passing(true))
                .try_into_nonempty_iter()
            {
                // No versions found that can be satisfied without dropping
                // keywords, find versions with dropping and rank using
                // strategy.
                let ret = xs.max_by(|&x, &y| {
                    if x.1.drop == y.1.drop {
                        rank(x, y)
                    } else {
                        y.1.drop.cmp(&x.1.drop)
                    }
                });
                Ok((ret.0, ret_scores()))
            } else {
                // No versions found that have valid keywords available,
                // return error
                Err(GuessVersionError::AllInvalid)
            }
        }
    }
}

/// Error when parsing HEADER segment
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum HeaderError {
    Segment(HeaderOffsetsError),
    Version(VersionError),
    Validation(HeaderOffsetsValidationError),
    Space(HeaderSpacesFormatError),
    NoBytes(HeaderNoBytesError),
}

/// Error when version is not follow by proper number of spaces in HEADER
#[derive(Debug, Error, PartialEq, Clone)]
#[error("version must be followed by 4 spaces")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct HeaderSpacesFormatError;

/// Error when HEADER could not be read because the byte stream was exhausted.
#[derive(Debug, Error, PartialEq, Clone)]
#[error("need {HEADER_LEN} bytes to read HEADER, got {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct HeaderNoBytesError(u64);

/// Error when validating segments in HEADER
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum VersionError {
    Format(VersionFormatError),
    NonUtf8(VersionNonUtf8Error),
}

/// Error when parsing FCS version
#[derive(Debug, Error, PartialEq, Clone)]
#[error("invalid bytes found when parsing version: {}", self.0.iter().join(","))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct VersionNonUtf8Error(Vec<u8>);

/// Error when trying to guess FCS version from keywords
#[derive(Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub enum GuessVersionError {
    // TODO should also say a bit more on why this is the case
    #[error("no FCS versions could be guessed from keywords")]
    AllInvalid,
    #[error("$PAR could not be found and thus FCS version could not be detected")]
    NoPar,
}

/// Error when writing HEADER or TEXT.
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum WriteTEXTHeaderError {
    Overflow(Uint8DigitOverflowError),
    DelimError(DelimCollisionError),
}

#[derive(new)]
pub(crate) struct HeaderKeywordsToWrite<T> {
    pub(crate) header: WriteHeaderSegments<T>,
    pub(crate) text: TEXTToWrite,
    pub(crate) nextdata: Nextdata,
}

/// Different configurations in which TEXT may be written.
///
/// `Combined` is meant for 2.0 only. The other two are meant for 3.0+ where
/// we need to make two strings for required and optional keywords but may
/// need to write them as one contiguous bytestream if STEXT is not required
/// (which is basically always). The alternative is to combined these strings
/// but this potentially costs lots of memory.
pub(crate) enum TEXTToWrite {
    /// Primary TEXT only (2.0)
    Combined2_0(NEString),
    /// Primary TEXT only but with two buffers for required and optional (3.0+).
    ///
    /// Both buffers are assumed to start with a delimiter.
    Combined(NEString, NEString),
    /// Primary and supplemental TEXT with required and optional (3.0+)
    ///
    /// Both buffers are assumed to start with a delimiter.
    Split(NEString, NEString),
}

impl TEXTToWrite {
    fn write_primary<W: io::Write>(
        &self,
        h: &mut BufWriter<W>,
        digest: &mut WriteFCSDigest,
    ) -> io::Result<()> {
        match self {
            Self::Combined2_0(s) => digest.update_and_write(h, s.as_str().as_bytes()),
            Self::Combined(r, o) => {
                digest.update_and_write(h, r.as_str().as_bytes())?;
                if let Some((_, o_no_delim)) = o.as_str().split_at_checked(1) {
                    digest.update_and_write(h, o_no_delim.as_bytes())?;
                }
                Ok(())
            }
            Self::Split(p, _) => digest.update_and_write(h, p.as_str().as_bytes()),
        }
    }

    fn write_supplemental<W: io::Write>(
        &self,
        h: &mut BufWriter<W>,
        digest: &mut WriteFCSDigest,
    ) -> io::Result<()> {
        if let Self::Split(_, s) = self {
            digest.update_and_write(h, s.as_str().as_bytes())
        } else {
            Ok(())
        }
    }
}

impl<T> HeaderKeywordsToWrite<T> {
    /// Create HEADER+TEXT+OTHER offsets for FCS 2.0
    pub(crate) fn new_2_0<'a>(
        kws: impl IntoIterator<Item = AnyKeyword<'a>>,
        conf: &WriteHeaderAndTextConfig<'_>,
    ) -> Result<Self, WriteTEXTHeaderError>
    where
        T: TryFrom<u64, Error = Uint8DigitOverflowError> + Copy + HeaderString + Into<u64> + Zero,
    {
        let delim = conf.delim;
        let data_len = conf.data_len;
        let anal_len = conf.analysis_len;
        let other_lens = &conf.other_lens()[..];
        let text_begin = Self::header_len(other_lens.len(), T::WIDTH);

        // Make new buffer for TEXT with first delimiter
        let mut text = NEString::from(char::from(delim));

        // Check for invalid delimiters and write non-offset keywords to buffers
        for x in kws {
            let y = if let Some(f) = conf.fil.as_ref()
                && matches!(
                    x,
                    AnyKeyword::Opt(OptKeyword::Root(StdOrNonStdOptRootKeyword::Std(
                        OptRootKeyword::Fil(_)
                    )))
                ) {
                AnyKeyword::Opt(fil_to_kw(f))
            } else {
                x
            };
            y.has_delim(delim).map_or(Ok(()), Err)?;
            Escaped::new(delim, &y).write_str(&mut text);
        }

        let text_len: u64 = u64::try_from(text.len().get()).expect("overflow") + NEXTDATA_LEN;
        let text_seg = PrimaryTextOffsetsToWrite::try_new_with_len(text_begin, text_len)?;

        let other_begin = text_seg.try_next_byte().map_or(text_begin, u64::from);
        let (other_segs, data_begin) = Self::other_segments(other_begin, other_lens)?;

        let data_seg = HeaderDataOffsetsToWrite::try_new_with_len(data_begin, data_len)?;

        let anal_begin = data_seg.try_next_byte().map_or(data_begin, u64::from);
        let anal_seg = HeaderAnalysisOffsetsToWrite::try_new_with_len(anal_begin, anal_len)?;

        let nextdata = Self::get_nextdata(anal_begin, &anal_seg, conf.has_nextdata, true);
        let nextdata_kw = OffsetKeyword::from_value(nextdata);

        Escaped::new(delim, &nextdata_kw).write_str(&mut text);

        let header = WriteHeaderSegments::new(text_seg, data_seg, anal_seg, other_segs);

        Ok(Self::new(header, TEXTToWrite::Combined2_0(text), nextdata))
    }

    /// Create HEADER+TEXT+OTHER offsets for FCS 3.0
    ///
    /// Order in which this is expected to be written is HEADER, OTHER(s), TEXT,
    /// STEXT, DATA, ANALYSIS.
    pub(crate) fn new_3_0<'a>(
        req: impl IntoIterator<Item = ReqKeyword<'a>>,
        opt: impl IntoIterator<Item = OptKeyword<'a>>,
        conf: &WriteHeaderAndTextConfig<'_>,
    ) -> Result<Self, WriteTEXTHeaderError>
    where
        T: TryFrom<u64, Error = Uint8DigitOverflowError> + Copy + HeaderString + Into<u64> + Zero,
    {
        let delim = conf.delim;
        let data_len = conf.data_len;
        let anal_len = conf.analysis_len;
        let other_lens = &conf.other_lens()[..];
        let prim_text_begin = Self::header_len(other_lens.len(), T::WIDTH);

        // TODO this might be optimized by pre-allocating (which would require
        // estimating the size of TEXT a priori) or by dumping characters into a
        // null buffer and counting them, then writing the file later. This
        // would save memory and possibly be as fast depending on how well we
        // can estimate the number of chars to be written.

        // init string buffers for primary and supp with first delim
        let mut req_text = NEString::from(char::from(delim));
        let mut opt_text = NEString::from(char::from(delim));

        // Check for invalid delimiters and write non-offset keywords to buffers
        for x in req {
            x.has_delim(delim).map_or(Ok(()), Err)?;
            Escaped::new(delim, &x).write_str(&mut req_text);
        }

        for x in opt {
            let y = if let Some(f) = conf.fil.as_ref()
                && matches!(
                    x,
                    OptKeyword::Root(StdOrNonStdOptRootKeyword::Std(OptRootKeyword::Fil(_)))
                ) {
                fil_to_kw(f)
            } else {
                x
            };
            y.has_delim(delim).map_or(Ok(()), Err)?;
            Escaped::new(delim, &y).write_str(&mut opt_text);
        }

        // Compute lengths of primary and supplemental TEXT given the length of
        // the required and optional keywords plus the offset keywords which
        // still need to be added.
        //
        // All required keywords need to go in primary TEXT. Supplemental TEXT
        // is the length of the optional keywords (plus the first delimiter)
        // since these can all be moved out of primary TEXT if it is too long.
        // Primary TEXT with optional and required keywords is the sum of both
        // these buffers MINUS ONE because the optional keyword buffer includes
        // an initial delimiter.
        let req_text_len = u64::try_from(req_text.len().get()).expect("overflow") + OFFSETS_LEN_3_0;
        let supp_text_len = u64::try_from(opt_text.len().get()).expect("overflow");
        let all_text_len = req_text_len + supp_text_len - 1;

        let make_text_seg = |len| -> Result<_, WriteTEXTHeaderError> {
            let seg = PrimaryTextOffsetsToWrite::try_new_with_len(prim_text_begin, len)?;
            let other_begin = seg.try_next_byte().map_or(prim_text_begin, u64::from);
            Ok((seg, other_begin))
        };

        // Include STEXT only if the optional keywords don't fit within the
        // first 99,999,999 bytes
        let prim_text_res = make_text_seg(all_text_len);
        let (prim_text_seg, other_segs, supp_text_seg, data_begin) =
            if let Ok((prim_text_seg, other_begin)) = prim_text_res {
                let (other_segs, next_begin) = Self::other_segments(other_begin, other_lens)?;
                let supp_text_seg = SupplementalTextOffsetsToWrite::default();
                (prim_text_seg, other_segs, supp_text_seg, next_begin)
            } else {
                let (prim_text_seg, other_begin) = make_text_seg(req_text_len)?;
                let (other_segs, supp_text_begin) = Self::other_segments(other_begin, other_lens)?;
                // NOTE this will happen because if we made it to this point,
                // req + opt is too big, and req is small enough, which means
                // opt must have something in it
                assert!(
                    supp_text_len > 1,
                    "supp TEXT should have at least one key/val pair"
                );
                let supp_text_seg =
                    SupplementalTextOffsetsToWrite::new_with_len(supp_text_begin, supp_text_len);
                let data_begin = supp_text_seg
                    .try_next_byte()
                    .map_or(supp_text_begin, u64::from);
                (prim_text_seg, other_segs, supp_text_seg, data_begin)
            };

        let data_seg = TEXTDataOffsetsToWrite::new_with_len(data_begin, data_len);

        let anal_begin = data_seg.try_next_byte().map_or(data_begin, u64::from);
        let anal_seg = TEXTAnalysisOffsetsToWrite::new_with_len(anal_begin, anal_len);

        let h_anal_seg = anal_seg.as_header();
        let h_data_seg = data_seg.as_header();

        let nextdata = Self::get_nextdata(anal_begin, &anal_seg, conf.has_nextdata, false);
        let nextdata_kw = OffsetKeyword::from_value(nextdata);

        // Add offset keywords to the end of required TEXT buffer.
        //
        // NOTE in 3.2 *DATA and *SDATA are technically optional, but it is much
        // easier just to include them in the "required" stuff regardless.
        let offset_kws = supp_text_seg
            .keywords()
            .into_iter()
            .chain(data_seg.keywords())
            .chain(anal_seg.keywords())
            .chain(once(nextdata_kw));

        for x in offset_kws {
            Escaped::new(delim, &x).write_str(&mut req_text);
        }

        let header = WriteHeaderSegments::new(prim_text_seg, h_data_seg, h_anal_seg, other_segs);

        let text = if supp_text_seg.is_empty() {
            TEXTToWrite::Combined(req_text, opt_text)
        } else {
            TEXTToWrite::Split(req_text, opt_text)
        };

        Ok(Self::new(header, text, nextdata))
    }

    pub(crate) fn h_write<W: io::Write>(
        &self,
        h: &mut BufWriter<W>,
        version: Version,
        other_segs: &[Other],
        digest: &mut WriteFCSDigest,
    ) -> io::Result<()>
    where
        T: Copy + Zero + fmt::Display,
    {
        // write HEADER
        self.header.h_write(h, version, digest)?;

        // write primary TEXT
        self.text.write_primary(h, digest)?;

        // write OTHER
        for o in other_segs {
            h.write_all(o.0.as_bytes())?;
        }

        // write supplemental TEXT
        self.text.write_supplemental(h, digest)?;
        Ok(())
    }

    fn header_len(other_n: usize, w: u8) -> u64
    where
        T: HeaderString,
    {
        let n = u64::try_from(other_n).unwrap();
        let o = n * u64::from(w) * 2;
        u64::from(HEADER_LEN) + o
    }

    #[allow(clippy::type_complexity)]
    fn other_segments(
        begin: u64,
        other_lens: &[u64],
    ) -> Result<(Vec<OtherOffsetsToWrite<T>>, u64), <u64 as TryInto<T>>::Error>
    where
        u64: TryInto<T>,
        T: Copy + Into<u64> + Zero,
    {
        let ret = other_lens
            .iter()
            .scan(begin, |b, &length| {
                let s = OtherOffsetsToWrite::try_new_with_len(*b, length);
                *b += length;
                Some(s)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let next = ret
            .iter()
            .filter_map(OffsetsToWrite::try_next_byte)
            .last()
            .map_or(begin, Into::into);
        Ok((ret, next))
    }

    // TODO by begin needed here?
    fn get_nextdata<I, S, T0>(
        offset_begin: u64,
        offsets: &OffsetsToWrite<I, S, T0>,
        flag: AppendableFlag,
        is_2_0: bool,
    ) -> Nextdata
    where
        T0: Copy + Into<u64> + Zero,
    {
        const CRC_WORD_WIDTH: u64 = 8;
        let ret = if flag.is_set() {
            let n = offsets.try_next_byte().map_or(offset_begin, u64::from);
            let c = if is_2_0 { 0 } else { CRC_WORD_WIDTH };
            UintZeroPad20(n + c)
        } else {
            UintZeroPad20(0)
        };
        Nextdata(ret)
    }
}

#[allow(clippy::as_conversions, reason = "const not stable yet")]
type HeaderBuf = [u8; HEADER_LEN as usize];

/// Length of $(BEGIN/END)(STEXT/ANALYSIS/DATA) and $NEXTDATA offset length.
///
/// This was chosen on the basis that the maximum file size is 2^64, and thus
/// the maximum offset is the number of digits in 2^64, which is 20. This will
/// "waste" very little space in TEXT and will make computing the TEXT width
/// much easier.
pub(crate) const OFFSET_VAL_LEN: u64 = 20;

/// The maximum value that may be stored in a HEADER offset.
pub(crate) const MAX_HEADER_OFFSET: u32 = 99_999_999;

/// Number of bytes consumed by $NEXTDATA keyword + value + delimiters
const NEXTDATA_LEN: u64 = std_key_len(Nextdata::C) + OFFSET_VAL_LEN + 2;

/// The number of bytes each offset is expected to take.
///
/// These are the length of each keyword + 2 since there should be two
/// delimiters counting toward its byte real estate.
const DATA_LEN: u64 = std_key_len(Begindata::C) + std_key_len(Enddata::C) + OFFSET_VAL_LEN * 2 + 4;

const ANALYSIS_LEN: u64 =
    std_key_len(Beginanalysis::C) + std_key_len(Endanalysis::C) + OFFSET_VAL_LEN * 2 + 4;

const STEXT_LEN: u64 =
    std_key_len(Beginstext::C) + std_key_len(Endstext::C) + OFFSET_VAL_LEN * 2 + 4;

/// The total number of bytes offset keywords are expected to take.
///
/// This only applies to 3.0+ since 2.0 only has NEXTDATA.
// NOTE we cheat a bit and include supp text here, even though it is optional in
// 3.2. It probably will never make a difference.
const OFFSETS_LEN_3_0: u64 = DATA_LEN + ANALYSIS_LEN + STEXT_LEN + NEXTDATA_LEN;

/// Compute the length of a standard key.
///
/// Assume key does not have '$' on the front, so add 1.
#[allow(clippy::as_conversions)]
const fn std_key_len(s: &NEStr) -> u64 {
    (s.len().get() + 1) as u64
}

fn fil_to_kw(f: &NEString) -> OptKeyword<'_> {
    let a = NEStringKeyword0::new(DKey0::default(), f.as_ne_str());
    let b = StdOrNonStdOptRootKeyword::Std(OptRootKeyword::Fil(a));
    OptKeyword::Root(b)
}
