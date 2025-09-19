from ._pyreflow import fcs_read_header as fcs_read_header
from ._pyreflow import fcs_read_raw_text as fcs_read_raw_text

from ._pyreflow import Header as Header
from ._pyreflow import HeaderSegments as HeaderSegments
from ._pyreflow import RawTEXTOutput as RawTEXTOutput
from ._pyreflow import RawTEXTParseData as RawTEXTParseData

# from ._pyreflow import _api  # type: ignore
# from pyreflow.typing import (
#     ByteOrd,
#     StdKey,
#     AnyCoreTEXT,
#     AnyCoreDataset,
#     Segment,
#     FCSVersion,
#     StdKeywords,
#     NonStdKeywords,
#     OffsetCorrection,
#     AnalysisBytes,
#     OtherBytes,
#     TemporalOpticalKey,
# )
# from pathlib import Path
# from typing import Any, NamedTuple
# import polars as pl
# import textwrap


# class HeaderSegments(NamedTuple):
#     """
#     Return value containing segments in *HEADER*
#     """

#     text: Segment
#     """The segment for *TEXT*"""

#     data: Segment
#     """The segment for *DATA*"""

#     analysis: Segment
#     """The segment for *ANALYSIS*"""

#     other: list[Segment]
#     """Segments for *OTHER* in the order they appear in *HEADER* (if any)"""


# class ParseData(NamedTuple):
#     """
#     Return value containing data generated when parsing *TEXT*.
#     """

#     header_segments: HeaderSegments

#     supp_text: Segment | None
#     """
#     The segment for supplemental *TEXT* if it exists.

#     This will always be ``None`` for FCS 2.0.
#     """

#     nextdata: int | None
#     """
#     The value of *$NEXTDATA* if it exists.

#     This keywords is required for all supported FCS versions; however, not
#     including it is a non-fatal error since it only precludes reading the next
#     dataset (which in many cases doesn't exist).
#     """

#     delimiter: int
#     """The delimiter used when parsing *TEXT*."""

#     non_ascii: dict[str, str]
#     """Any key/value pairs whose key includes non-ASCII characters.

#     Such keywords are not allowed in any FCS standard but are included here
#     since in some cases they might represent useful information if found.
#     """

#     byte_pairs: dict[bytes, bytes]
#     """Any key/value pairs that contain invalid UTF-8 characters."""


# class ExtraStdKeywords(NamedTuple):
#     """
#     Keywords which were not consumed when standardizing TEXT.
#     """

#     pseudostandard: dict[StdKey, str]
#     """
#     Keywords which start with *$* but are not part of the target FCS standard.
#     """

#     unused: dict[StdKey, str]
#     """
#     Keywords which are part of the standard but were not used.
#     """


# class StdTEXTData(NamedTuple):
#     """
#     Return data from reading standardized TEXT.
#     """

#     tot: int | None
#     """
#     The value of *$TOT* if included.

#     This keyword is optional in FCS 2.0 and this attribute will be ``None`` if
#     missing.
#     """

#     data: Segment
#     """
#     Segment corresponding to *DATA*.

#     This will be the segment in *TEXT* if present and valid, otherwise it will
#     be the segment from *HEADER*.
#     """

#     analysis: Segment
#     """
#     Segment corresponding to *ANALYSIS*.

#     This will be the segment in TEXT if present and valid, otherwise it will
#     be the segment from *HEADER*.
#     """

#     extra: ExtraStdKeywords

#     parse: ParseData


# class StdDatasetData(NamedTuple):
#     """
#     Return data from reading standardized dataset.
#     """

#     parse: ParseData

#     extra: ExtraStdKeywords

#     data_seg: Segment
#     """
#     Segment used to read to *DATA*.

#     This will be the segment in *TEXT* if present and valid, otherwise it will
#     be the segment from *HEADER*.
#     """

#     analysis_seg: Segment
#     """
#     Segment used to read *ANALYSIS*.

#     This will be the segment in *TEXT* if present and valid, otherwise it will
#     be the segment from *HEADER*.
#     """


# class ReadHeaderOutput(NamedTuple):
#     """
#     Return value from reading the *HEADER* segment
#     """

#     version: FCSVersion
#     """The FCS version"""

#     segments: HeaderSegments


# # TODO use newtype wrappers for std and nonstd to prevent mixing downstream
# class ReadRawTEXTOutput(NamedTuple):
#     """
#     Return value after reading *HEADER* and raw *TEXT*.
#     """

#     version: FCSVersion
#     """The FCS version from *HEADER*"""

#     std: StdKeywords
#     """All valid key/value pairs that start with *$*"""

#     nonstd: NonStdKeywords
#     """All valid key/value pairs that do not start with *$*"""

#     parse: ParseData


# class ReadStdTEXTOutput(NamedTuple):
#     """Return value when reading standardized *TEXT*."""

#     core: AnyCoreTEXT
#     """Encodes valid keywords from *TEXT*."""

#     uncore: StdTEXTData
#     """All other data not represented in ``core`` after standardizing *TEXT*"""


# class ReadRawDatasetOutput(NamedTuple):
#     """Return value when reading raw dataset."""

#     data: pl.DataFrame
#     """The *DATA* segment as a polars dataframe."""

#     analysis: AnalysisBytes
#     """The *ANALYSIS* segment as a byte sequence."""

#     others: list[OtherBytes]
#     """*OTHER* segments in the order defined in *HEADER* as a byte sequence."""

#     data_seg: Segment
#     """
#     Segment used to read *DATA*.

#     This will be the segment in *TEXT* if present and valid, otherwise it will
#     be the segment from *HEADER*.
#     """

#     analysis_seg: Segment
#     """
#     Segment used to read *ANALYSIS*.

#     This will be the segment in *TEXT* if present and valid, otherwise it will
#     be the segment from *HEADER*.
#     """

#     text: ReadRawTEXTOutput
#     """Other data from reading raw *TEXT*."""


# class ReadStdDatasetOutput(NamedTuple):
#     """Return value when reading standardized dataset."""

#     core: AnyCoreDataset
#     """Encodes *TEXT*, *DATA*, *ANALYSIS*, and *OTHER* if present."""

#     uncore: StdDatasetData
#     """All non-essential data not included in ``core``"""


# class ReadRawDatasetFromKwsOutput(NamedTuple):
#     """Return value from reading raw dataset using known keywords."""

#     data: pl.DataFrame
#     """The *DATA* segment as a polars dataframe."""

#     analysis: AnalysisBytes
#     """The *ANALYSIS* segment as a byte sequence."""

#     others: list[OtherBytes]
#     """*OTHER* segments in the order defined in *HEADER* as a byte sequence."""

#     data_seg: Segment
#     """
#     Segment used to read *DATA*.

#     This will be the segment in *TEXT* if present and valid, otherwise it will
#     be the segment from *HEADER*.
#     """

#     analysis_seg: Segment
#     """
#     Segment used to read *ANALYSIS*.

#     This will be the segment in *TEXT* if present and valid, otherwise it will
#     be the segment from *HEADER*.
#     """


# class ReadStdDatasetFromKwsOutput(NamedTuple):
#     """Return value from reading raw dataset using known keywords."""

#     core: AnyCoreDataset
#     """Encodes *TEXT*, *DATA*, *ANALYSIS*, and *OTHER* if present."""

#     extra: ExtraStdKeywords

#     data_seg: Segment
#     """
#     Segment used to read *DATA*.

#     This will be the segment in *TEXT* if present and valid, otherwise it will
#     be the segment from *HEADER*.
#     """

#     analysis_seg: Segment
#     """
#     Segment used to read *ANALYSIS*.

#     This will be the segment in *TEXT* if present and valid, otherwise it will
#     be the segment from *HEADER*.
#     """


# def _to_parse_data(xs: dict[str, Any]) -> ParseData:
#     args: dict[str, Any] = {
#         k: HeaderSegments(**v) if k == "header_segments" else v for k, v in xs.items()
#     }
#     return ParseData(**args)


# def _to_std_text_data(xs: dict[str, Any]) -> StdTEXTData:
#     args: dict[str, Any] = {
#         k: _to_parse_data(v)
#         if k == "parse"
#         else (ExtraStdKeywords(**v) if k == "extra" else v)
#         for k, v in xs.items()
#     }
#     return StdTEXTData(**args)


# def _to_raw_output(xs: dict[str, Any]) -> ReadRawTEXTOutput:
#     return ReadRawTEXTOutput(
#         version=xs["version"],
#         **xs["keywords"],
#         parse=_to_parse_data(xs["parse"]),
#     )


# _HEADER_ARGS: dict[str, list[str]] = {
#     "text_correction": ["Offset correction to apply to *TEXT* segment."],
#     "data_correction": ["Offset correction to apply to *DATA* segment."],
#     "analysis_correction": ["Offset correction to apply to *ANALYSIS* segment."],
#     "other_corrections": ["Offset corrections to apply to *OTHER* segments."],
#     "max_other": [
#         ("Limit to number of *OTHER* segments to parse. ``None`` means 'no limit'.")
#     ],
#     "other_width": [
#         (
#             "Width to use when parsing *OTHER* segments."
#             "In 3.2 this should be 8 bytes; In older versions this was unspecified. "
#             "In practice, vendors seem to use whatever width they want, presumably to "
#             "make 'large' numbers fit. This must be an integer between 1 and "
#             "20 (corresponding to a theoretical max of 2^64)."
#         )
#     ],
#     "squish_offsets": [
#         (
#             "If ``True``, force offsets to be ``0,0`` if they are ``X,0`` where "
#             "``X`` is non-zero. "
#             "This happens if the ending offset is greater than 8 digits and "
#             "must be stored in *TEXT*. In such cases, both offsets should be "
#             "stored in *TEXT* and the analogue in *HEADER* should be ``0,0``"
#         ),
#         (
#             "Some vendors fail to do this, so setting this to ``True`` will "
#             "correctly force the *HEADER* offsets to be empty in these cases. "
#             "This only applies to FCS 3.0 and greater since FCS 2.0 does not "
#             "allow offsets larger than 8 digits."
#         ),
#     ],
#     "allow_negative": [
#         (
#             "If ``True``, set negative offsets to zero rather than throwing an error. "
#             "Since the ending offset of a segment points to the last byte "
#             "rather than the next byte, this means  that an empty segment should "
#             "be written as ``0,-1`` rather than ``0,0``. "
#             "Some vendors will do the former, which is logic but technically wrong."
#         )
#     ],
#     "truncate_offsets": [
#         (
#             "If ``True``, truncate end of offsets if they exceed the size of the file."
#             "Such files are likely corrupted, so this should be used with caution."
#         )
#     ],
# }

# _RAW_ARGS: dict[str, list[str]] = {
#     "version_override": ["Override the FCS version as seen in *HEADER*."],
#     "supp_text_correction": [
#         "Offset correction to apply to supplemental *TEXT* segment."
#     ],
#     "allow_duplicated_stext": [
#         (
#             "If ``True`` allow *sTEXT* offsets to match the *pTEXT* offsets "
#             "from *HEADER*. Some vendors will duplicate these two "
#             "segments despite *sTEXT* not being present, which is incorrect."
#         )
#     ],
#     "ignore_supp_text": ["If ``True``, ignore supplemental *TEXT* entirely."],
#     "use_literal_delims": [
#         (
#             "If ``True``, treat every delimiter as literal (turn off escaping). "
#             "Without escaping, delimiters cannot be included in keys or values, "
#             "but empty values become possible. Use this option for files where "
#             "unescaped delimiters results in the 'correct' interpretation of *TEXT*."
#         )
#     ],
#     "allow_non_ascii_delim": [
#         "If ``True`` allow non-ASCII delimiters (outside 1-126)."
#     ],
#     "allow_missing_final_delim": [
#         "If ``True`` allow *TEXT* to not end with a delimiter."
#     ],
#     "allow_nonunique": [
#         (
#             "If ``True`` allow non-unique keys in *TEXT*. In such cases, "
#             "only the first key will be used regardless of this setting; "
#         )
#     ],
#     "allow_odd": [
#         (
#             "If ``True``, allow *TEXT* to contain odd number of words. "
#             "The last 'dangling' word will be dropped independent of this flag."
#         )
#     ],
#     "allow_empty": [
#         (
#             "If ``True`` allow keys with blank values. "
#             "Only relevant if ``use_literal_delims`` is also ``True``."
#         )
#     ],
#     "allow_delim_at_boundary": [
#         (
#             "If ``True`` allow delimiters at word boundaries. "
#             "The FCS standard forbids this because it is impossible to tell if such "
#             "delimiters belong to the previous or the next word. "
#             "Consequently, delimiters at boundaries will be dropped regardless of this flag. "
#             "Setting this to ``True`` will turn this into a warning not an error. "
#             "Only relevant if ``use_literal_delims`` is ``False``."
#         )
#     ],
#     "allow_non_utf8": [
#         (
#             "If ``True`` allow non-UTF8 characters in *TEXT*. "
#             "Words with such characters will be dropped regardless; setting this to "
#             "``True`` will turn these cases into warnings not errors."
#         )
#     ],
#     "allow_non_ascii_keywords": [
#         (
#             "If ``True`` allow non-ASCII keys. "
#             "This only applies to non-standard keywords, as all standardized keywords "
#             "may only contain letters, numbers, and start with *$*. Regardless, all "
#             "compliant keys must only have ASCII. Setting this to true will emit "
#             "an error when encountering such a key. If false, the key will be kept "
#             "as a non-standard key."
#         )
#     ],
#     "allow_missing_stext": [
#         "If ``True`` allow *sTEXT* offsets to be missing from *pTEXT*."
#     ],
#     "allow_stext_own_delim": [
#         (
#             "If ``True`` allow *sTEXT* offsets to have a different "
#             "delimiter compared to *pTEXT*."
#         )
#     ],
#     "allow_missing_nextdata": [
#         (
#             "If ``True`` allow *$NEXTDATA* to be missing. "
#             "This is a required keyword in all versions. However, most files "
#             "only have one dataset in which case this keyword is meaningless."
#         )
#     ],
#     "trim_value_whitespace": [
#         (
#             "If ``True`` trim whitespace from all values. "
#             "If performed, trimming precedes all other repair steps."
#             "Any values which are entirely spaces will become blanks, in which case "
#             "it may also be sensible to enable ``allow_empty``."
#         )
#     ],
#     "ignore_standard_keys": [
#         (
#             "Remove standard keys from *TEXT*. "
#             "The leading *$* is implied so do not include it."
#         )
#     ],
#     "promote_to_standard": ["Promote nonstandard keys to standard keys in *TEXT*."],
#     "demote_from_standard": ["Denote standard keys to nonstandard keys in *TEXT*."],
#     "rename_standard_keys": [
#         (
#             "Rename standard keys in *TEXT*. "
#             "Keys matching the first part of the pair will be replaced by the second. "
#             "Comparisons are case insensitive. "
#             "The leading *$* is implied so do not include it."
#         )
#     ],
#     "replace_standard_key_values": [
#         (
#             "Replace values for standard keys in *TEXT*"
#             "Comparisons are case insensitive. "
#             "The leading *$* is implied so do not include it."
#         )
#     ],
#     "append_standard_keywords": [
#         (
#             "Append standard key/value pairs to *TEXT*."
#             "All keys and values will be included as they appear here. "
#             "The leading *$* is implied so do not include it."
#         )
#     ],
# }

# _STD_ARGS: dict[str, list[str]] = {
#     "trim_intra_value_whitespace": [
#         "If ``True`` trim whitespace between delimiters such as ``;`` and ``,``."
#     ],
#     "time_meas_pattern": [
#         (
#             "A pattern to match the *$PnN* of the time measurement. "
#             "Must be a regular expression following syntax described in "
#             "`regexp-syntax <https://docs.rs/regex-syntax/latest/regex_syntax/>`__. "
#             "If ``None``, do not try to find a time measurement."
#         )
#     ],
#     "allow_missing_time": ["If ``True`` allow time measurement to be missing."],
#     "force_time_linear": [
#         "If ``True`` force the time channel to be linear independent of *$PnE*."
#     ],
#     "ignore_time_gain": [
#         (
#             "If ``True`` ignore the *$PnG* (gain) keyword. "
#             "This keyword should not be set according to the standard. "
#             "This library will allow gain to be 1.0 since this equates to identity. "
#             "If gain is not 1.0, this is nonsense and it can be ignored with this flag."
#         )
#     ],
#     "ignore_time_optical_keys": [
#         (
#             "Ignore optical keys in temporal measurement. "
#             "These keys are nonsensical for time measurements but are not explicitly "
#             "forbidden in the the standard. "
#             'Provided keys are the string after the "Pn" in the "PnX" keywords.'
#         )
#     ],
#     "parse_indexed_spillover": [
#         (
#             "Parse $SPILLOVER with numeric indices rather than strings "
#             "(ie names or *$PnN*)"
#         )
#     ],
#     "date_pattern": [
#         (
#             "If supplied, will be used as an alternative pattern when parsing *$DATE*. "
#             "It should have specifiers for year, month, and day as outlined in "
#             "`chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`__. "
#             "If not supplied, *$DATE* will be parsed according to the standard pattern which "
#             "is ``%d-%b-%Y``."
#         )
#     ],
#     "time_pattern": [
#         (
#             "If supplied, will be used as an alternative pattern when parsing *$BTIM* and *$ETIM*. "
#             "It should have specifiers for hours, minutes, and seconds as outlined in "
#             "`chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`__. "
#             "It may optionally also have a sub-seconds specifier as shown in the same link. "
#             "Furthermore, the specifiers '%!' and %@' may be used to match 1/60 and "
#             "centiseconds respectively. "
#             "If not supplied, *$BTIM* and *$ETIM* will be parsed according to the "
#             "standard pattern which is version-specific."
#         )
#     ],
#     "allow_pseudostandard": [
#         "If ``True`` allow non-standard keywords with a leading *$*. "
#         "The presence of such keywords often means the version in *HEADER* is incorrect."
#     ],
#     "allow_unused_standard": [
#         "If ``True`` allow unused standard keywords to be present."
#     ],
#     "disallow_deprecated": [
#         "If ``True`` throw error if a deprecated key is encountered."
#     ],
#     # TODO expand upon this elsewhere
#     "fix_log_scale_offsets": ["If ``True`` fix log-scale *PnE* and *PnG* keywords."],
#     "nonstandard_measurement_pattern": [
#         (
#             "Pattern to use when matching nonstandard measurement keys. "
#             "Must be a regular expression pattern with ``%n`` which will "
#             "represent the measurement index and should not start with *$*. "
#             "Otherwise should be a normal regular expression as defined in "
#             "`regexp-syntax <https://docs.rs/regex-syntax/latest/regex_syntax/>`__. "
#         )
#     ],
# }

# _OFFSET_ARGS: dict[str, list[str]] = {
#     "text_data_correction": ["Corrections for *DATA* offsets in *TEXT*"],
#     "text_analysis_correction": ["Corrections for *ANALYSIS* offsets in *TEXT*"],
#     "ignore_text_data_offsets": ["If ``True`` ignore *DATA* offsets in *TEXT*"],
#     "ignore_text_analysis_offsets": ["If ``True`` ignore *ANALYSIS* offsets in *TEXT*"],
#     "allow_header_text_offset_mismatch": [
#         "If ``True`` allow *TEXT* and *HEADER* offsets to mismatch."
#     ],
#     "allow_missing_required_offsets": [
#         "If ``True`` allow required offsets in *TEXT* to be missing. "
#         "Only applies to *DATA* and *ANALYSIS* offsets in FCS 3.0/3.1. "
#         "If missing, fall back to offsets from *HEADER*."
#     ],
#     "truncate_text_offsets": ["If ``True`` truncate offsets that exceed end of file."],
# }

# _LAYOUT_ARGS: dict[str, list[str]] = {
#     "integer_widths_from_byteord": [
#         (
#             "If ``True`` set all *$PnB* to the number of bytes from *$BYTEORD*. "
#             "Only has an effect for FCS 2.0/3.0 where *$DATATYPE* is ``I``."
#         )
#     ],
#     "integer_byteord_override": [
#         "Override *$BYTEORD* for integer layouts in FCS 2.0/3.0."
#     ],
#     "disallow_range_truncation": [
#         (
#             "If ``True`` throw error if *$PnR* values need to be truncated "
#             "to match the number of bytes specified by *$PnB* and *$DATATYPE*."
#         )
#     ],
# }

# _DATA_ARGS: dict[str, list[str]] = {
#     "allow_uneven_event_width": [
#         (
#             "If ``True`` allow event width to not perfectly divide length of *DATA*. "
#             "Does not apply to delimited ASCII layouts. "
#         )
#     ],
#     "allow_tot_mismatch": [
#         "If ``True`` allow *$TOT* to not match number of events as "
#         "computed by the event width and length of *DATA*."
#         "Does not apply to delimited ASCII layouts."
#     ],
#     # TODO this arg is defunct
#     "allow_data_par_mismatch": [""],
# }

# _SHARED_ARGS: dict[str, list[str]] = {
#     "warnings_are_errors": ["If ``True`` all warnings will be regarded as errors."],
# }


# def _assign_args(keys: list[str], src: dict[str, Any]) -> dict[str, Any]:
#     acc: dict[str, Any] = {}
#     for k in keys:
#         acc[k] = src.pop(k)
#     return acc


# def _assign_raw_args(src: dict[str, Any]) -> dict[str, Any]:
#     raw_conf = _assign_args(list(_RAW_ARGS), src)
#     raw_conf["header"] = _assign_args(list(_HEADER_ARGS), src)
#     return raw_conf


# class KeyPatterns(NamedTuple):
#     """Patterns used to match keys in *TEXT*.

#     All comparisons will be case-insensitive.
#     """

#     literal: list[str]
#     """Strings to be matched literally and exactly."""

#     regexp: list[str]
#     """
#     Strings to be matched as regular expression patterns described in
#     `regexp-syntax <https://docs.rs/regex-syntax/latest/regex_syntax/>`__.
#     """


# DEFAULT_CORRECTION = (0, 0)
# DEFAULT_KEY_PATTERNS: KeyPatterns = KeyPatterns([], [])
# DEFAULT_OTHER_WIDTH = 8
# DEFAULT_TIME_MEAS_PATTERN = "^(TIME|Time)$"


# # def fcs_read_header(
# #     p: Path,
# #     text_correction: OffsetCorrection = DEFAULT_CORRECTION,
# #     data_correction: OffsetCorrection = DEFAULT_CORRECTION,
# #     analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
# #     other_corrections: list[OffsetCorrection] = [],
# #     max_other: int | None = None,
# #     other_width: int = DEFAULT_OTHER_WIDTH,
# #     squish_offsets: bool = False,
# #     allow_negative: bool = False,
# #     truncate_offsets: bool = False,
# # ) -> ReadHeaderOutput:
# #     args = {k: v for k, v in locals().items() if k != "p"}
# #     conf = _assign_args([*_HEADER_ARGS], args)
# #     assert len(args) == 0, False
# #     ret = _api._fcs_read_header(p, conf)
# #     return ReadHeaderOutput(
# #         version=ret["version"], segments=HeaderSegments(**ret["segments"])
# #     )


# def fcs_read_raw_text(
#     p: Path,
#     # header args
#     text_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     data_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     other_corrections: list[OffsetCorrection] = [],
#     max_other: int | None = None,
#     other_width: int = DEFAULT_OTHER_WIDTH,
#     squish_offsets: bool = False,
#     allow_negative: bool = False,
#     truncate_offsets: bool = False,
#     # raw text args
#     version_override: FCSVersion | None = None,
#     supp_text_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     allow_duplicated_stext: bool = False,
#     ignore_supp_text: bool = False,
#     use_literal_delims: bool = False,
#     allow_non_ascii_delim: bool = False,
#     allow_missing_final_delim: bool = False,
#     allow_nonunique: bool = False,
#     allow_odd: bool = False,
#     allow_empty: bool = False,
#     allow_delim_at_boundary: bool = False,
#     allow_non_utf8: bool = False,
#     allow_non_ascii_keywords: bool = False,
#     allow_missing_stext: bool = False,
#     allow_stext_own_delim: bool = False,
#     allow_missing_nextdata: bool = False,
#     trim_value_whitespace: bool = False,
#     ignore_standard_keys: KeyPatterns = DEFAULT_KEY_PATTERNS,
#     rename_standard_keys: dict[str, str] = {},
#     promote_to_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
#     demote_from_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
#     replace_standard_key_values: dict[str, str] = {},
#     append_standard_keywords: dict[str, str] = {},
#     # shared args
#     warnings_are_errors: bool = False,
# ) -> ReadRawTEXTOutput:
#     """
#     Read the HEADER and TEXT of an FCS file.
#     """
#     args = {k: v for k, v in locals().items() if k != "p"}
#     conf = {
#         "raw": _assign_raw_args(args),
#         "shared": _assign_args(list(_SHARED_ARGS), args),
#     }
#     assert len(args) == 0, False
#     ret = _api._fcs_read_raw_text(p, conf)
#     keywords = ret["keywords"]
#     return ReadRawTEXTOutput(
#         version=ret["version"],
#         **keywords,
#         parse=_to_parse_data(ret["parse"]),
#     )


# def fcs_read_std_text(
#     p: Path,
#     # header args
#     version_override: FCSVersion | None = None,
#     text_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     data_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     other_corrections: list[OffsetCorrection] = [],
#     max_other: int | None = None,
#     other_width: int = DEFAULT_OTHER_WIDTH,
#     squish_offsets: bool = False,
#     allow_negative: bool = False,
#     truncate_offsets: bool = False,
#     # raw text args
#     supp_text_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     allow_duplicated_stext: bool = False,
#     ignore_supp_text: bool = False,
#     use_literal_delims: bool = False,
#     allow_non_ascii_delim: bool = False,
#     allow_missing_final_delim: bool = False,
#     allow_nonunique: bool = False,
#     allow_odd: bool = False,
#     allow_empty: bool = False,
#     allow_delim_at_boundary: bool = False,
#     allow_non_utf8: bool = False,
#     allow_non_ascii_keywords: bool = False,
#     allow_missing_stext: bool = False,
#     allow_stext_own_delim: bool = False,
#     allow_missing_nextdata: bool = False,
#     trim_value_whitespace: bool = False,
#     ignore_standard_keys: KeyPatterns = DEFAULT_KEY_PATTERNS,
#     rename_standard_keys: dict[str, str] = {},
#     promote_to_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
#     demote_from_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
#     replace_standard_key_values: dict[str, str] = {},
#     append_standard_keywords: dict[str, str] = {},
#     # standard args
#     trim_intra_value_whitespace: bool = False,
#     time_meas_pattern: str | None = DEFAULT_TIME_MEAS_PATTERN,
#     allow_missing_time: bool = False,
#     force_time_linear: bool = False,
#     ignore_time_gain: bool = False,
#     ignore_time_optical_keys: set[TemporalOpticalKey] = set(),
#     parse_indexed_spillover: bool = False,
#     date_pattern: str | None = None,
#     time_pattern: str | None = None,
#     allow_pseudostandard: bool = False,
#     allow_unused_standard: bool = False,
#     disallow_deprecated: bool = False,
#     fix_log_scale_offsets: bool = False,
#     nonstandard_measurement_pattern: str | None = None,
#     # offset args
#     text_data_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     text_analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     ignore_text_data_offsets: bool = False,
#     ignore_text_analysis_offsets: bool = False,
#     allow_header_text_offset_mismatch: bool = False,
#     allow_missing_required_offsets: bool = False,
#     truncate_text_offsets: bool = False,
#     # layout args
#     integer_widths_from_byteord: bool = False,
#     integer_byteord_override: ByteOrd | None = None,
#     disallow_range_truncation: bool = False,
#     # shared args
#     warnings_are_errors: bool = False,
# ) -> ReadStdTEXTOutput:
#     """
#     Read the HEADER and standardized TEXT of an FCS file.
#     """
#     args = {k: v for k, v in locals().items() if k != "p"}
#     conf = {
#         "raw": _assign_raw_args(args),
#         "standard": _assign_args(list(_STD_ARGS), args),
#         "offsets": _assign_args(list(_OFFSET_ARGS), args),
#         "layout": _assign_args(list(_LAYOUT_ARGS), args),
#         "shared": _assign_args(list(_SHARED_ARGS), args),
#     }
#     assert len(args) == 0, False
#     core, uncore = _api._fcs_read_std_text(p, conf)
#     return ReadStdTEXTOutput(core=core, uncore=_to_std_text_data(uncore))


# def fcs_read_raw_dataset(
#     p: Path,
#     # header args
#     version_override: FCSVersion | None = None,
#     text_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     data_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     other_corrections: list[OffsetCorrection] = [],
#     max_other: int | None = None,
#     other_width: int = DEFAULT_OTHER_WIDTH,
#     squish_offsets: bool = False,
#     allow_negative: bool = False,
#     truncate_offsets: bool = False,
#     # raw text args
#     supp_text_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     allow_duplicated_stext: bool = False,
#     ignore_supp_text: bool = False,
#     use_literal_delims: bool = False,
#     allow_non_ascii_delim: bool = False,
#     allow_missing_final_delim: bool = False,
#     allow_nonunique: bool = False,
#     allow_odd: bool = False,
#     allow_empty: bool = False,
#     allow_delim_at_boundary: bool = False,
#     allow_non_utf8: bool = False,
#     allow_non_ascii_keywords: bool = False,
#     allow_missing_stext: bool = False,
#     allow_stext_own_delim: bool = False,
#     allow_missing_nextdata: bool = False,
#     trim_value_whitespace: bool = False,
#     ignore_standard_keys: KeyPatterns = DEFAULT_KEY_PATTERNS,
#     rename_standard_keys: dict[str, str] = {},
#     promote_to_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
#     demote_from_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
#     replace_standard_key_values: dict[str, str] = {},
#     append_standard_keywords: dict[str, str] = {},
#     # offset args
#     text_data_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     text_analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     ignore_text_data_offsets: bool = False,
#     ignore_text_analysis_offsets: bool = False,
#     allow_header_text_offset_mismatch: bool = False,
#     allow_missing_required_offsets: bool = False,
#     truncate_text_offsets: bool = False,
#     # layout args
#     integer_widths_from_byteord: bool = False,
#     integer_byteord_override: ByteOrd | None = None,
#     disallow_range_truncation: bool = False,
#     # data args
#     allow_uneven_event_width: bool = False,
#     allow_tot_mismatch: bool = False,
#     allow_data_par_mismatch: bool = False,
#     # shared args
#     warnings_are_errors: bool = False,
# ) -> ReadRawDatasetOutput:
#     """
#     Read an FCS file with standardized TEXT.
#     """
#     args = {k: v for k, v in locals().items() if k != "p"}
#     conf = {
#         "raw": _assign_raw_args(args),
#         "offsets": _assign_args(list(_OFFSET_ARGS), args),
#         "layout": _assign_args(list(_LAYOUT_ARGS), args),
#         "data": _assign_args(list(_DATA_ARGS), args),
#         "shared": _assign_args(list(_SHARED_ARGS), args),
#     }
#     assert len(args) == 0, False
#     ret = _api._fcs_read_raw_dataset(p, conf)
#     text = ret["text"]
#     return ReadRawDatasetOutput(
#         text=_to_raw_output(ret["text"]),
#         **ret["dataset"],
#     )


# def fcs_read_std_dataset(
#     p: Path,
#     # header args
#     version_override: FCSVersion | None = None,
#     text_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     data_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     other_corrections: list[OffsetCorrection] = [],
#     max_other: int | None = None,
#     other_width: int = DEFAULT_OTHER_WIDTH,
#     squish_offsets: bool = False,
#     allow_negative: bool = False,
#     truncate_offsets: bool = False,
#     # raw text args
#     supp_text_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     allow_duplicated_stext: bool = False,
#     ignore_supp_text: bool = False,
#     use_literal_delims: bool = False,
#     allow_non_ascii_delim: bool = False,
#     allow_missing_final_delim: bool = False,
#     allow_nonunique: bool = False,
#     allow_odd: bool = False,
#     allow_empty: bool = False,
#     allow_delim_at_boundary: bool = False,
#     allow_non_utf8: bool = False,
#     allow_non_ascii_keywords: bool = False,
#     allow_missing_stext: bool = False,
#     allow_stext_own_delim: bool = False,
#     allow_missing_nextdata: bool = False,
#     trim_value_whitespace: bool = False,
#     ignore_standard_keys: KeyPatterns = DEFAULT_KEY_PATTERNS,
#     rename_standard_keys: dict[str, str] = {},
#     promote_to_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
#     demote_from_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
#     replace_standard_key_values: dict[str, str] = {},
#     append_standard_keywords: dict[str, str] = {},
#     # standard args
#     trim_intra_value_whitespace: bool = False,
#     time_meas_pattern: str | None = DEFAULT_TIME_MEAS_PATTERN,
#     allow_missing_time: bool = False,
#     force_time_linear: bool = False,
#     ignore_time_gain: bool = False,
#     ignore_time_optical_keys: set[TemporalOpticalKey] = set(),
#     parse_indexed_spillover: bool = False,
#     date_pattern: str | None = None,
#     time_pattern: str | None = None,
#     allow_pseudostandard: bool = False,
#     allow_unused_standard: bool = False,
#     disallow_deprecated: bool = False,
#     fix_log_scale_offsets: bool = False,
#     nonstandard_measurement_pattern: str | None = None,
#     # offset args
#     text_data_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     text_analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     ignore_text_data_offsets: bool = False,
#     ignore_text_analysis_offsets: bool = False,
#     allow_header_text_offset_mismatch: bool = False,
#     allow_missing_required_offsets: bool = False,
#     truncate_text_offsets: bool = False,
#     # layout args
#     integer_widths_from_byteord: bool = False,
#     integer_byteord_override: ByteOrd | None = None,
#     disallow_range_truncation: bool = False,
#     # data args
#     allow_uneven_event_width: bool = False,
#     allow_tot_mismatch: bool = False,
#     allow_data_par_mismatch: bool = False,
#     # shared args
#     warnings_are_errors: bool = False,
# ) -> ReadStdDatasetOutput:
#     """
#     Read an FCS file with standardized TEXT.
#     """
#     args = {k: v for k, v in locals().items() if k != "p"}
#     conf = {
#         "raw": _assign_raw_args(args),
#         "offsets": _assign_args(list(_OFFSET_ARGS), args),
#         "layout": _assign_args(list(_LAYOUT_ARGS), args),
#         "standard": _assign_args(list(_STD_ARGS), args),
#         "data": _assign_args(list(_DATA_ARGS), args),
#         "shared": _assign_args(list(_SHARED_ARGS), args),
#     }
#     assert len(args) == 0, False
#     core, uncore = _api._fcs_read_std_dataset(p, conf)
#     return ReadStdDatasetOutput(
#         core=core,
#         uncore=StdDatasetData(
#             parse=_to_parse_data(uncore["parse"]),
#             extra=ExtraStdKeywords(**uncore["dataset"]["extra"]),
#             data_seg=uncore["dataset"]["standardized"]["data_seg"],
#             analysis_seg=uncore["dataset"]["standardized"]["analysis_seg"],
#         ),
#     )


# def fcs_read_raw_dataset_with_keywords(
#     p: Path,
#     version: FCSVersion,
#     std: dict[str, str],
#     data_seg: Segment,
#     analysis_seg: Segment,
#     other_segs: list[Segment],
#     # offset args
#     text_data_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     text_analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     ignore_text_data_offsets: bool = False,
#     ignore_text_analysis_offsets: bool = False,
#     allow_header_text_offset_mismatch: bool = False,
#     allow_missing_required_offsets: bool = False,
#     truncate_text_offsets: bool = False,
#     # layout args
#     integer_widths_from_byteord: bool = False,
#     integer_byteord_override: ByteOrd | None = None,
#     disallow_range_truncation: bool = False,
#     # data args
#     allow_uneven_event_width: bool = False,
#     allow_tot_mismatch: bool = False,
#     allow_data_par_mismatch: bool = False,
#     # shared args
#     warnings_are_errors: bool = False,
# ) -> ReadRawDatasetFromKwsOutput:
#     """
#     Read raw data from FCS file using a given set of keywords.
#     """
#     omit = ["p", "version", "std", "data_seg", "analysis_seg", "other_segs"]
#     args = {k: v for k, v in locals().items() if k not in omit}
#     conf = {
#         "offsets": _assign_args(list(_OFFSET_ARGS), args),
#         "layout": _assign_args(list(_LAYOUT_ARGS), args),
#         "data": _assign_args(list(_DATA_ARGS), args),
#         "shared": _assign_args(list(_SHARED_ARGS), args),
#     }
#     assert len(args) == 0, False
#     ret = _api._fcs_read_raw_dataset_with_keywords(
#         p, version, std, data_seg, analysis_seg, other_segs, conf
#     )
#     return ReadRawDatasetFromKwsOutput(**ret)


# def fcs_read_std_dataset_with_keywords(
#     p: Path,
#     version: FCSVersion,
#     std: dict[str, str],
#     nonstd: dict[str, str],
#     data_seg: Segment,
#     analysis_seg: Segment,
#     other_segs: list[Segment],
#     # standard args
#     trim_intra_value_whitespace: bool = False,
#     time_meas_pattern: str | None = DEFAULT_TIME_MEAS_PATTERN,
#     allow_missing_time: bool = False,
#     force_time_linear: bool = False,
#     ignore_time_gain: bool = False,
#     ignore_time_optical_keys: set[TemporalOpticalKey] = set(),
#     parse_indexed_spillover: bool = False,
#     date_pattern: str | None = None,
#     time_pattern: str | None = None,
#     allow_pseudostandard: bool = False,
#     allow_unused_standard: bool = False,
#     disallow_deprecated: bool = False,
#     fix_log_scale_offsets: bool = False,
#     nonstandard_measurement_pattern: str | None = None,
#     # offset args
#     text_data_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     text_analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
#     ignore_text_data_offsets: bool = False,
#     ignore_text_analysis_offsets: bool = False,
#     allow_header_text_offset_mismatch: bool = False,
#     allow_missing_required_offsets: bool = False,
#     truncate_text_offsets: bool = False,
#     # layout args
#     integer_widths_from_byteord: bool = False,
#     integer_byteord_override: ByteOrd | None = None,
#     disallow_range_truncation: bool = False,
#     # data args
#     allow_uneven_event_width: bool = False,
#     allow_tot_mismatch: bool = False,
#     allow_data_par_mismatch: bool = False,
#     # shared args
#     warnings_are_errors: bool = False,
# ) -> ReadStdDatasetFromKwsOutput:
#     """
#     Read standardized data from FCS file using a given set of keywords.
#     """
#     omit = ["p", "version", "std", "nonstd", "data_seg", "analysis_seg", "other_segs"]
#     args = {k: v for k, v in locals().items() if k not in omit}
#     conf = {
#         "std": _assign_args(list(_STD_ARGS), args),
#         "offsets": _assign_args(list(_OFFSET_ARGS), args),
#         "layout": _assign_args(list(_LAYOUT_ARGS), args),
#         "data": _assign_args(list(_DATA_ARGS), args),
#         "shared": _assign_args(list(_SHARED_ARGS), args),
#     }
#     assert len(args) == 0, False
#     core, uncore = _api._fcs_read_std_dataset_with_keywords(
#         p,
#         version,
#         {"std": std, "nonstd": nonstd},
#         data_seg,
#         analysis_seg,
#         other_segs,
#         conf,
#     )
#     return ReadStdDatasetFromKwsOutput(
#         core=core,
#         extra=ExtraStdKeywords(**uncore["extra"]),
#         data_seg=uncore["standardized"]["data_seg"],
#         analysis_seg=uncore["standardized"]["analysis_seg"],
#     )


# def _format_docstring(front: str, params: list[tuple[str, list[str]]]) -> str:
#     # TODO actually indent these appropriately
#     width = 76
#     indent = " " * 4

#     def format_param(key: str, value: list[str]) -> str:
#         v0 = textwrap.wrap(
#             f":param {key}: {value[0]}",
#             width=width,
#             subsequent_indent=indent,
#         )
#         vs = [
#             textwrap.wrap(v, width=76, initial_indent=indent, subsequent_indent=indent)
#             for v in value[1:]
#         ]
#         paras = ["\n".join(xs) for xs in [v0, *vs]]
#         return "\n\n".join(paras)

#     ps = "\n".join([format_param(k, v) for k, v in params])
#     return f"{front}\n\n{ps}"


# # fcs_read_header.__doc__ = _format_docstring(
# #     "Read the *HEADER* of an FCS file.",
# #     [
# #         ("p", ["path to FCS file"]),
# #         *_HEADER_ARGS.items(),
# #     ],
# # )

# fcs_read_raw_text.__doc__ = _format_docstring(
#     "Read the *TEXT* of an FCS file without standarization.",
#     [
#         ("p", ["path to FCS file"]),
#         *_HEADER_ARGS.items(),
#         *_RAW_ARGS.items(),
#         *_SHARED_ARGS.items(),
#     ],
# )

# fcs_read_std_text.__doc__ = _format_docstring(
#     "Read the *TEXT* of an FCS file with standardization.",
#     [
#         ("p", ["path to FCS file"]),
#         *_HEADER_ARGS.items(),
#         *_RAW_ARGS.items(),
#         *_STD_ARGS.items(),
#         *_OFFSET_ARGS.items(),
#         *_LAYOUT_ARGS.items(),
#         *_SHARED_ARGS.items(),
#     ],
# )

# fcs_read_raw_dataset.__doc__ = _format_docstring(
#     "Read dataset from FCS file without standardization.",
#     [
#         ("p", ["path to FCS file"]),
#         *_HEADER_ARGS.items(),
#         *_RAW_ARGS.items(),
#         *_OFFSET_ARGS.items(),
#         *_LAYOUT_ARGS.items(),
#         *_DATA_ARGS.items(),
#         *_SHARED_ARGS.items(),
#     ],
# )

# fcs_read_std_dataset.__doc__ = _format_docstring(
#     "Read dataset from FCS file with standardization.",
#     [
#         ("p", ["path to FCS file"]),
#         *_HEADER_ARGS.items(),
#         *_RAW_ARGS.items(),
#         *_STD_ARGS.items(),
#         *_OFFSET_ARGS.items(),
#         *_LAYOUT_ARGS.items(),
#         *_DATA_ARGS.items(),
#         *_SHARED_ARGS.items(),
#     ],
# )

# fcs_read_raw_dataset_with_keywords.__doc__ = _format_docstring(
#     "Read dataset from FCS file using given keywords without standardization.",
#     [
#         ("p", ["path to FCS file"]),
#         ("version", ["FCS version to use"]),
#         ("std", ["standard keywords"]),
#         ("data_seg", ["*DATA* segment from *HEADER*"]),
#         ("analysis_seg", ["*ANALYSIS* segment from *HEADER*"]),
#         ("other_segs", ["*OTHER* segments from *HEADER*"]),
#         *_OFFSET_ARGS.items(),
#         *_LAYOUT_ARGS.items(),
#         *_DATA_ARGS.items(),
#         *_SHARED_ARGS.items(),
#     ],
# )

# fcs_read_std_dataset_with_keywords.__doc__ = _format_docstring(
#     "Read dataset from FCS file using given keywords with standardization.",
#     [
#         ("p", ["path to FCS file"]),
#         ("version", ["FCS version to use"]),
#         ("std", ["standard keywords"]),
#         ("nonstd", ["non-standard keywords"]),
#         ("data_seg", ["*DATA* segment from *HEADER*"]),
#         ("analysis_seg", ["*ANALYSIS* segment from *HEADER*"]),
#         ("other_segs", ["*OTHER* segments from *HEADER*"]),
#         *_STD_ARGS.items(),
#         *_OFFSET_ARGS.items(),
#         *_LAYOUT_ARGS.items(),
#         *_DATA_ARGS.items(),
#         *_SHARED_ARGS.items(),
#     ],
# )

# del _format_docstring
