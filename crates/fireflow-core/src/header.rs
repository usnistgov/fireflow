//! Reading and writing the HEADER segment

use crate::config::{
    AppendableFlag, ConfigFlag as _, ReadHeaderInnerConfig, ReadOffsetConfig, ReadState,
    SelectVersionStrategy, VersionOverride,
};
use crate::core::{Other, WriteHeaderAndTextConfig};
use crate::logging::{
    IOAnonErrorGroup, IOErrorGroup, IOGroupResult, LogResult, ResultExt as _,
    WarningsAndIOGroupResult, io_to_log, split_io,
};
use crate::segment::{
    GuessOtherWidthError, HeaderAnalysisOffsets, HeaderDataOffsets, HeaderOffsets,
    HeaderSegmentError, HeaderToHeaderOffsetsOverlap, Offsets, OriginalOffsets, OtherOffsets,
    OtherOffsets20, PrimaryTextOffsets, SupplementalTextOffsets, TEXTAnalysisOffsets,
    TEXTDataOffsets,
};
use crate::text::keyword_enum::{
    AnyKeyword, Escaped, Keyword0FromValue as _, OffsetKeyword, OptKeyword, ReqKeyword,
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
use crate::validated::keys::{Key as _, StdKeywords};
use crate::validated::textdelim::{DelimCollisionError, HasDelim as _};

use fireflow_types::config::EnumStrIter as _;
use fireflow_types::keywords::{Version, VersionFormatError};
use fireflow_types::nonempty_string::{NEStr, NEString};

use derive_more::{Display, From};
use derive_new::new;
use itertools::Itertools as _;
use nonempty_collections::{
    IntoIteratorExt as _, IntoNonEmptyIterator as _, NEVec, iter::NonEmptyIterator as _,
};
use num_traits::identities::Zero;
use thiserror::Error;

use std::fmt;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::iter::once;

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
    pub(crate) text: PrimaryTextOffsets,
    pub(crate) data: HeaderDataOffsets,
    pub(crate) analysis: HeaderAnalysisOffsets,
    pub(crate) other: Vec<OtherOffsets<T>>,
}

impl<T> WriteHeaderSegments<T> {
    pub(crate) fn h_write<W: Write>(&self, h: &mut BufWriter<W>, version: Version) -> io::Result<()>
    where
        T: Zero + fmt::Display + Copy,
    {
        // 6+4+16+16+16 bytes
        write!(
            h,
            "{version}    {}{}{}",
            self.text, self.data, self.analysis
        )?;
        for o in &self.other {
            write!(h, "{o}")?;
        }
        Ok(())
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
    pub version: Version,
    pub final_offsets: FinalHeaderOffsets,
    pub original_offsets: OriginalHeaderOffsets,
    pub overlaps: Vec<HeaderToHeaderOffsetsOverlap>,
}

impl Header {
    pub fn h_read<C, R>(
        h: &mut BufReader<R>,
        st: &ReadState<C>,
    ) -> WarningsAndIOGroupResult<Self, GuessOtherWidthError, HeaderError, ()>
    where
        C: AsRef<ReadHeaderInnerConfig> + AsRef<ReadOffsetConfig>,
        R: Read + Seek,
    {
        let oconf: &ReadOffsetConfig = st.conf.as_ref();
        io_to_log!(h.seek(SeekFrom::Start(st.dataset_offset.0)));
        let req = io_to_log!(ReqHeader::h_read(h, st));
        let (text, text_raw) = req.text;
        let (data, data_raw) = req.data;
        let (analysis, analysis_raw) = req.analysis;
        let coords = [text.try_coords(), data.try_coords(), analysis.try_coords()];
        let min_coord = coords.iter().flatten().map(|x| x.0).min();
        let other_res = if let Some(m) = min_coord {
            OtherOffsets20::h_read_others(h, m, st)
        } else {
            LogResult::new_ok(None)
        };
        other_res
            .map_pure_errors(HeaderError::from)
            .and_then_commutative(|other| {
                let (os, os_raw) = if let Some((os, w)) = other {
                    let (parsed, raw): (NEVec<_>, NEVec<_>) = os.into_nonempty_iter().unzip();
                    (Some((parsed, w)), Vec::from(raw))
                } else {
                    (None, vec![])
                };
                let usegs = OriginalHeaderOffsets::new(text_raw, data_raw, analysis_raw, os_raw);
                let limit = oconf.overlap_correction_limit;
                FinalHeaderOffsets::try_new_with_limit(text, data, analysis, os, limit)
                    .map_errors(HeaderError::from)
                    .nowarn_into_warn()
                    .group()
                    .map_error(IOErrorGroup::Pure)
                    .map_ok_value(|(segs, overlaps)| (segs, usegs, overlaps))
            })
            .map_ok_value(|(segs, usegs, overlaps)| Self::new(req.version, segs, usegs, overlaps))
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
    fn h_read<C, R>(h: &mut BufReader<R>, st: &ReadState<C>) -> IOGroupResult<Self, HeaderError, ()>
    where
        R: Read + Seek,
        C: AsRef<ReadHeaderInnerConfig> + AsRef<ReadOffsetConfig>,
    {
        let conf: &ReadHeaderInnerConfig = st.conf.as_ref();
        let text_cor = conf.text_correction;
        let data_cor = conf.data_correction;
        let anal_cor = conf.analysis_correction;

        let vers_res = split_io!(h_read_version(h, st))
            .ungroup()
            .map_errors(HeaderError::from);
        let space_res = split_io!(Self::h_read_spaces(h, st))
            .ungroup()
            .map_errors(HeaderError::from);

        let (version, ()) = vers_res
            .zip_commutative(space_res)
            .group()
            .resolve_nowarn()
            .map_err(IOErrorGroup::Pure)?;

        let text_res = HeaderOffsets::h_read_primary(h, true, text_cor, version, st);
        let data_res = HeaderOffsets::h_read_primary(h, false, data_cor, version, st);
        let anal_res = HeaderOffsets::h_read_primary(h, false, anal_cor, version, st);

        let pure_text_res = split_io!(text_res).ungroup();
        let pure_data_res = split_io!(data_res).ungroup();
        let pure_anal_res = split_io!(anal_res).ungroup();

        pure_text_res
            .zip3_commutative(pure_data_res, pure_anal_res)
            .map_errors(HeaderError::from)
            .group()
            .resolve_nowarn()
            .map_err(IOErrorGroup::Pure)
            .map(|(t, d, a)| Self::new(version, t, d, a))
    }

    fn h_read_spaces<R, C>(
        h: &mut BufReader<R>,
        st: &ReadState<C>,
    ) -> Result<(), IOAnonErrorGroup<HeaderSpacesError>>
    where
        R: Read + Seek,
    {
        let remaining = st.remaining_bytes(h)?;
        if remaining < 4 {
            let e = HeaderSpacesNoBytesError(remaining).into();
            return Err(IOAnonErrorGroup::new_pure_one(e));
        }
        let mut buf = [0_u8; 4];
        h.read_exact(&mut buf)?;
        if buf.iter().all(|x| *x == 32) {
            Ok(())
        } else {
            Err(IOAnonErrorGroup::new_pure_one(
                HeaderSpacesFormatError.into(),
            ))
        }
    }
}

fn h_read_version<R, C>(
    h: &mut BufReader<R>,
    st: &ReadState<C>,
) -> IOGroupResult<Version, VersionError, ()>
where
    R: Read + Seek,
{
    let remaining = st.remaining_bytes(h)?;
    if remaining < 6 {
        let e = VersionNoBytesError(remaining).into();
        return Err(IOAnonErrorGroup::new_pure_one(e));
    }
    let mut buf = [0; 6];
    h.read_exact(&mut buf)?;
    if let Ok(s) = str::from_utf8(&buf) {
        s.parse()
            .map_err(VersionError::from)
            .map_err(IOErrorGroup::new_pure_one)
    } else {
        let e = VersionNonUtf8Error(buf.to_vec());
        Err(IOErrorGroup::new_pure_one(e.into()))
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
        Some(VersionOverride::AutoDetect(strat)) => {
            let rank =
                |(v0, s0): &(Version, KeywordVersionScore),
                 (v1, s1): &(Version, KeywordVersionScore)| match strat {
                    SelectVersionStrategy::Earliest => v1.cmp(v0),
                    SelectVersionStrategy::Latest => v0.cmp(v1),
                    SelectVersionStrategy::Loose => s1.good_opt.cmp(&s0.good_opt),
                    SelectVersionStrategy::Strict => s0.good_opt.cmp(&s1.good_opt),
                };
            if let Ok(par) = Par::get_metaroot_req(kws) {
                let mut opt = KeywordOptimizer::default();
                for (k, v) in kws {
                    opt.classify_keyword(k, v.as_ne_str());
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
                    Ok((xs.max_by(|&x, &y| rank(x, y)).0, ret_scores()))
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
            } else {
                Err(GuessVersionError::NoPar)
            }
        }
    }
}

/// Error when parsing HEADER segment
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum HeaderError {
    Segment(HeaderSegmentError),
    Version(VersionError),
    Validation(HeaderOffsetsValidationError),
    Space(HeaderSpacesError),
}

/// Error when parsing spaces after FCS version
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum HeaderSpacesError {
    Format(HeaderSpacesFormatError),
    Bytes(HeaderSpacesNoBytesError),
}

/// Error when version is not follow by proper number of spaces in HEADER
#[derive(Debug, Error, PartialEq, Clone)]
#[error("version must be followed by 4 spaces")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct HeaderSpacesFormatError;

/// Error when spaces could not be read because not enough bytes were present
#[derive(Debug, Error, PartialEq, Clone)]
#[error("needed 4 bytes to read spaces after FCS version, got {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct HeaderSpacesNoBytesError(u64);

/// Error when validating segments in HEADER
#[derive(From, Display, Debug, Error, PartialEq, Clone)]
#[cfg_attr(feature = "python", derive(AllIntoPyErr))]
pub enum VersionError {
    Format(VersionFormatError),
    NonUtf8(VersionNonUtf8Error),
    Bytes(VersionNoBytesError),
}

/// Error when parsing FCS version
#[derive(Debug, Error, PartialEq, Clone)]
#[error("invalid bytes found when parsing version: {}", self.0.iter().join(","))]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct VersionNonUtf8Error(Vec<u8>);

/// Error when not enough bytes to parse version
#[derive(Debug, Error, PartialEq, Clone)]
#[error("needed 6 bytes to parse FCS version, got {0}")]
#[cfg_attr(feature = "python", derive(DisplayAsPyErr))]
#[cfg_attr(feature = "python", pyerr(py::FileLayoutError))]
pub struct VersionNoBytesError(u64);

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
    fn write_primary<W: Write>(&self, h: &mut BufWriter<W>) -> io::Result<()> {
        match self {
            Self::Combined2_0(s) => h.write_all(s.as_str().as_bytes()),
            Self::Combined(r, o) => {
                h.write_all(r.as_str().as_bytes())?;
                if let Some((_, o_no_delim)) = o.as_str().split_at_checked(1) {
                    h.write_all(o_no_delim.as_bytes())?;
                }
                Ok(())
            }
            Self::Split(p, _) => h.write_all(p.as_str().as_bytes()),
        }
    }

    fn write_supplemental<W: Write>(&self, h: &mut BufWriter<W>) -> io::Result<()> {
        if let Self::Split(_, s) = self {
            h.write_all(s.as_str().as_bytes())
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
        T: TryFrom<u64, Error = Uint8DigitOverflowError> + Copy + HeaderString + Into<u64>,
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
            x.has_delim(delim).map_or(Ok(()), Err)?;
            Escaped::new(delim, &x).write_str(&mut text);
        }

        let text_len: u64 = u64::try_from(text.len().get()).expect("overflow") + NEXTDATA_LEN;
        let text_seg = PrimaryTextOffsets::try_new_with_len(text_begin, text_len)?;

        let other_begin = text_seg.try_next_byte().map_or(text_begin, u64::from);
        let (other_segs, data_begin) = Self::other_segments(other_begin, other_lens)?;

        let data_seg = HeaderDataOffsets::try_new_with_len(data_begin, data_len)?;

        let anal_begin = data_seg.try_next_byte().map_or(data_begin, u64::from);
        let anal_seg = HeaderAnalysisOffsets::try_new_with_len(anal_begin, anal_len)?;

        let nextdata = Self::get_nextdata(anal_begin, &anal_seg, conf.has_nextdata);
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
        T: TryFrom<u64, Error = Uint8DigitOverflowError> + Copy + HeaderString + Into<u64>,
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
            x.has_delim(delim).map_or(Ok(()), Err)?;
            Escaped::new(delim, &x).write_str(&mut opt_text);
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
            let seg = PrimaryTextOffsets::try_new_with_len(prim_text_begin, len)?;
            let other_begin = seg.try_next_byte().map_or(prim_text_begin, u64::from);
            Ok((seg, other_begin))
        };

        // Include STEXT only if the optional keywords don't fit within the
        // first 99,999,999 bytes
        let prim_text_res = make_text_seg(all_text_len);
        let (prim_text_seg, other_segs, supp_text_seg, data_begin) =
            if let Ok((prim_text_seg, other_begin)) = prim_text_res {
                let (other_segs, next_begin) = Self::other_segments(other_begin, other_lens)?;
                let supp_text_seg = SupplementalTextOffsets::default();
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
                    SupplementalTextOffsets::new_with_len(supp_text_begin, supp_text_len);
                let data_begin = supp_text_seg
                    .try_next_byte()
                    .map_or(supp_text_begin, u64::from);
                (prim_text_seg, other_segs, supp_text_seg, data_begin)
            };

        let data_seg = TEXTDataOffsets::new_with_len(data_begin, data_len);

        let anal_begin = data_seg.try_next_byte().map_or(data_begin, u64::from);
        let anal_seg = TEXTAnalysisOffsets::new_with_len(anal_begin, anal_len);

        let h_anal_seg = anal_seg.as_header();
        let h_data_seg = data_seg.as_header();

        let nextdata = Self::get_nextdata(anal_begin, &anal_seg, conf.has_nextdata);
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

    pub(crate) fn h_write<W: Write>(
        &self,
        h: &mut BufWriter<W>,
        version: Version,
        other_segs: &[Other],
    ) -> io::Result<()>
    where
        T: Copy + Zero + fmt::Display,
    {
        // write HEADER
        self.header.h_write(h, version)?;

        // write primary TEXT
        self.text.write_primary(h)?;

        // write OTHER
        for o in other_segs {
            h.write_all(&o.0)?;
        }

        // write supplemental TEXT
        self.text.write_supplemental(h)?;
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
    ) -> Result<(Vec<OtherOffsets<T>>, u64), <T as TryFrom<u64>>::Error>
    where
        T: Copy + TryFrom<u64> + Into<u64>,
    {
        let ret = other_lens
            .iter()
            .scan(begin, |b, &length| {
                let s = OtherOffsets::try_new_with_len(*b, length);
                *b += length;
                Some(s)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let next = ret
            .iter()
            .filter_map(Offsets::try_next_byte)
            .last()
            .map_or(begin, Into::into);
        Ok((ret, next))
    }

    fn get_nextdata<I, S, T0>(
        seg_begin: u64,
        seg: &Offsets<I, S, T0>,
        flag: AppendableFlag,
    ) -> Nextdata
    where
        T0: Copy + Into<u64>,
    {
        let ret = if flag.is_set() {
            let n = seg.try_next_byte().map_or(seg_begin, u64::from);
            UintZeroPad20(n)
        } else {
            UintZeroPad20(0)
        };
        Nextdata(ret)
    }
}

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
