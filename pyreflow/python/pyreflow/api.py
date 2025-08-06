from ._pyreflow import _api  # type: ignore
from pyreflow.typing import (
    ByteOrd,
    StdKey,
    Timestep,
    AnyCoreTEXT,
    AnyCoreDataset,
    Segment,
    FCSVersion,
    StdKeywords,
    NonStdKeywords,
    OffsetCorrection,
    KeyPatterns,
    AnalysisBytes,
    OtherBytes,
)
from pathlib import Path
from typing import Any, NamedTuple
import polars as pl


class HeaderSegments(NamedTuple):
    """
    Return value containing segments in *HEADER*
    """

    text: Segment
    """The segment for *TEXT*"""

    data: Segment
    """The segment for *DATA*"""

    analysis: Segment
    """The segment for *ANALYSIS*"""

    other: list[Segment]
    """Segments for *OTHER* in the order they appear in *HEADER* (if any)"""


class ParseData(NamedTuple):
    """
    Return value containing data generated when parsing *TEXT*.
    """

    header_segments: HeaderSegments

    supp_text: Segment | None
    """
    The segment for supplemental *TEXT* if it exists.

    This will always be ``None`` for FCS 2.0.
    """

    nextdata: int | None
    """
    The value of *$NEXTDATA* if it exists.

    This keywords is required for all supported FCS versions; however, not
    including it is a non-fatal error since it only precludes reading the next
    dataset (which in many cases doesn't exist).
    """

    delimiter: int
    """The delimiter used when parsing *TEXT*."""

    non_ascii: dict[str, str]
    """Any key/value pairs whose key includes non-ASCII characters.

    Such keywords are not allowed in any FCS standard but are included here
    since in some cases they might represent useful information if found.
    """

    byte_pairs: dict[bytes, bytes]
    """Any key/value pairs that contain invalid UTF-8 characters."""


class StdTEXTData(NamedTuple):
    """
    Return data from reading standardized TEXT.
    """

    tot: int | None
    """
    The value of *$TOT* if included.

    This keyword is optional in FCS 2.0 and this attribute will be ``None`` if
    missing.
    """

    # TODO link CoreTEXT in doc
    timestep: Timestep | None
    """
    The value of *$TIMESTEP* if not already captured.

    This will always be ``None`` for FCS 2.0 since *$TIMESTEP* only applies to
    3.0+.

    Furthermore, this keyword will be included in the CoreTEXT object if the
    measurements contain a time measurement, which must include *$TIMESTEP*. In
    these cases, this value will also be ``None``.

    In other words, this value will not be ``None`` only if a time measurement
    was not specified when standardizing *TEXT* and *$TIMESTEP* is still
    included in the raw keywords.
    """

    data: Segment
    """
    Segment corresponding to *DATA*.

    This will be the segment in *TEXT* if present and valid, otherwise it will
    be the segment from *HEADER*.
    """

    analysis: Segment
    """
    Segment corresponding to *ANALYSIS*.

    This will be the segment in TEXT if present and valid, otherwise it will
    be the segment from *HEADER*.
    """

    pseudostandard: dict[str, str]
    """
    Keywords which start with *$* but are not part of the target FCS standard.
    """

    parse: ParseData


class StdDatasetData(NamedTuple):
    """
    Return data from reading standardized dataset.
    """

    parse: ParseData

    pseudostandard: dict[str, str]
    """
    Keywords which start with *$* but are not part of the target FCS standard.
    """

    data_seg: Segment
    """
    Segment used to read to *DATA*.

    This will be the segment in *TEXT* if present and valid, otherwise it will
    be the segment from *HEADER*.
    """

    analysis_seg: Segment
    """
    Segment used to read *ANALYSIS*.

    This will be the segment in *TEXT* if present and valid, otherwise it will
    be the segment from *HEADER*.
    """


class ReadHeaderOutput(NamedTuple):
    """
    Return value from reading the *HEADER* segment
    """

    version: FCSVersion
    """The FCS version"""

    segments: HeaderSegments


# TODO use newtype wrappers for std and nonstd to prevent mixing downstream
class ReadRawTEXTOutput(NamedTuple):
    """
    Return value after reading *HEADER* and raw *TEXT*.
    """

    version: FCSVersion
    """The FCS version from *HEADER*"""

    std: StdKeywords
    """All valid key/value pairs that start with *$*"""

    nonstd: NonStdKeywords
    """All valid key/value pairs that do not start with *$*"""

    parse: ParseData


class ReadStdTEXTOutput(NamedTuple):
    """Return value when reading standardized *TEXT*."""

    core: AnyCoreTEXT
    """Encodes valid keywords from *TEXT*."""

    uncore: StdTEXTData
    """All other data not represented in ``core`` after standardizing *TEXT*"""


class ReadRawDatasetOutput(NamedTuple):
    """Return value when reading raw dataset."""

    data: pl.DataFrame
    """The *DATA* segment as a polars dataframe."""

    analysis: AnalysisBytes
    """The *ANALYSIS* segment as a byte sequence."""

    others: list[OtherBytes]
    """*OTHER* segments in the order defined in *HEADER* as a byte sequence."""

    data_seg: Segment
    """
    Segment used to read *DATA*.

    This will be the segment in *TEXT* if present and valid, otherwise it will
    be the segment from *HEADER*.
    """

    analysis_seg: Segment
    """
    Segment used to read *ANALYSIS*.

    This will be the segment in *TEXT* if present and valid, otherwise it will
    be the segment from *HEADER*.
    """

    text: ReadRawTEXTOutput
    """Other data from reading raw *TEXT*."""


class ReadStdDatasetOutput(NamedTuple):
    """Return value when reading standardized dataset."""

    core: AnyCoreDataset
    """Encodes *TEXT*, *DATA*, *ANALYSIS*, and *OTHER* if present."""

    uncore: StdDatasetData
    """All non-essential data not included in ``core``"""


class ReadRawDatasetFromKwsOutput(NamedTuple):
    """Return value from reading raw dataset using known keywords."""

    data: pl.DataFrame
    """The *DATA* segment as a polars dataframe."""

    analysis: AnalysisBytes
    """The *ANALYSIS* segment as a byte sequence."""

    others: list[OtherBytes]
    """*OTHER* segments in the order defined in *HEADER* as a byte sequence."""

    data_seg: Segment
    """
    Segment used to read *DATA*.

    This will be the segment in *TEXT* if present and valid, otherwise it will
    be the segment from *HEADER*.
    """

    analysis_seg: Segment
    """
    Segment used to read *ANALYSIS*.

    This will be the segment in *TEXT* if present and valid, otherwise it will
    be the segment from *HEADER*.
    """


class ReadStdDatasetFromKwsOutput(NamedTuple):
    """Return value from reading raw dataset using known keywords."""

    core: AnyCoreDataset
    """Encodes *TEXT*, *DATA*, *ANALYSIS*, and *OTHER* if present."""

    pseudostandard: dict[StdKey, str]
    """
    Keywords which start with *$* but are not part of the target FCS standard.
    """

    data_seg: Segment
    """
    Segment used to read *DATA*.

    This will be the segment in *TEXT* if present and valid, otherwise it will
    be the segment from *HEADER*.
    """

    analysis_seg: Segment
    """
    Segment used to read *ANALYSIS*.

    This will be the segment in *TEXT* if present and valid, otherwise it will
    be the segment from *HEADER*.
    """


def _to_parse_data(xs: dict[str, Any]) -> ParseData:
    args: dict[str, Any] = {
        k: HeaderSegments(**v) if k == "header_segments" else v for k, v in xs.items()
    }
    return ParseData(**args)


def _to_std_text_data(xs: dict[str, Any]) -> StdTEXTData:
    args: dict[str, Any] = {
        k: _to_parse_data(v) if k == "parse" else v for k, v in xs.items()
    }
    return StdTEXTData(**args)


def _to_raw_output(xs: dict[str, Any]) -> ReadRawTEXTOutput:
    return ReadRawTEXTOutput(
        version=xs["version"],
        **xs["keywords"],
        parse=_to_parse_data(xs["parse"]),
    )


_HEADER_ARGS = {
    "text_correction": "Offset correction to apply to *TEXT* segment.",
    "data_correction": "Offset correction to apply to *DATA* segment.",
    "analysis_correction": "Offset correction to apply to *ANALYSIS* segment.",
    "other_corrections": "Offset corrections to apply to *OTHER* segments.",
    "max_other": (
        'Limit to number of *OTHER* segments to parse. ``None`` means "no limit".'
    ),
    "other_width": (
        "Width to use when parsing *OTHER* segments. "
        "The FCS standards do not specify a width for these, "
        "and some vendors will use lenghts longer than 8 bytes "
        "to hold larger offsets."
    ),
    "squish_offsets": (
        "Force offsets to be empty if the start offset is greater than "
        "zero and the end offset is zero. This sometimes happens if the ending "
        "offset is greater than 8 digits, which means it must be stored in "
        "*TEXT*. However, in such cases, both offsets should be stored in "
        "*TEXT* and the analogue in *HEADER* should be empty. Some vendors "
        "fail to do this, so setting this to `True` will correctly force the "
        "*HEADER* offsets to be empty in these cases."
    ),
    "allow_negative": (
        "Correct negative offsets to zero rather than throwing an error. "
        "Since the ending offset of a segment points to the last byte "
        "rather than the next byte, this means  that an empty segment should "
        "be written as ``0,-1`` rather than ``0,0``. "
        "Some vendors will do the former, which is incorrect according to the "
        "standard, albeit quite logical."
    ),
    "truncate_offsets": (
        "Truncate end of offsets if they exceed the size of the file."
        "Such files are likely corrupted, so this should be used with caution."
        "Setting this to ``True`` will allow such files to still be read even "
        "thought their data may be nonsense."
    ),
}

_RAW_ARGS = {
    "version_override": "",
    "supp_text_correction": "",
    "allow_duplicated_stext": "",
    "ignore_supp_text": "",
    "use_literal_delims": "",
    "allow_non_ascii_delim": "",
    "allow_missing_final_delim": "",
    "allow_nonunique": "",
    "allow_odd": "",
    "allow_empty": "",
    "allow_delim_at_boundary": "",
    "allow_non_utf8": "",
    "allow_non_ascii_keywords": "",
    "allow_missing_stext": "",
    "allow_stext_own_delim": "",
    "allow_missing_nextdata": "",
    "trim_value_whitespace": "",
    "date_pattern": "",
    "ignore_standard_keys": "",
    "rename_standard_keys": "",
    "promote_to_standard": "",
    "demote_from_standard": "",
    "replace_standard_key_values": "",
    "append_standard_keywords": "",
}

_STD_ARGS = {
    "time_pattern": "",
    "allow_missing_time": "",
    "shortname_prefix": "",
    "allow_pseudostandard": "",
    "disallow_deprecated": "",
    "fix_log_scale_offsets": "",
    "nonstandard_measurement_pattern": "",
}

_OFFSET_ARGS = {
    "text_data_correction": "",
    "text_analysis_correction": "",
    "ignore_text_data_offsets": "",
    "ignore_text_analysis_offsets": "",
    "allow_header_text_offset_mismatch": "",
    "allow_missing_required_offsets": "",
    "truncate_text_offsets": "",
}

_LAYOUT_ARGS = {
    "integer_widths_from_byteord": "",
    "integer_byteord_override": "",
    "disallow_range_truncation": "",
}

_DATA_ARGS = {
    "allow_uneven_event_width": "",
    "allow_tot_mismatch": "",
    "allow_data_par_mismatch": "",
}

_SHARED_ARGS = {
    "warnings_are_errors": "",
}


def _assign_args(keys: list[str], src: dict[str, Any]) -> dict[str, Any]:
    acc: dict[str, Any] = {}
    for k in keys:
        acc[k] = src.pop(k)
    return acc


def _assign_raw_args(src: dict[str, Any]) -> dict[str, Any]:
    raw_conf = _assign_args(list(_RAW_ARGS), src)
    raw_conf["header"] = _assign_args(list(_HEADER_ARGS), src)
    return raw_conf


DEFAULT_CORRECTION = (0, 0)
DEFAULT_KEY_PATTERNS: tuple[list[str], list[str]] = ([], [])
DEFAULT_OTHER_WIDTH = 8
DEFAULT_SHORTNAME_PREFIX = "P"
DEFAULT_TIME_PATTERN = "^(TIME|Time)$"


def fcs_read_header(
    p: Path,
    text_correction: OffsetCorrection = DEFAULT_CORRECTION,
    data_correction: OffsetCorrection = DEFAULT_CORRECTION,
    analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = DEFAULT_OTHER_WIDTH,
    squish_offsets: bool = False,
    allow_negative: bool = False,
    truncate_offsets: bool = False,
) -> ReadHeaderOutput:
    args = {k: v for k, v in locals().items() if k != "p"}
    conf = _assign_args([*_HEADER_ARGS], args)
    assert len(args) == 0, False
    ret = _api._fcs_read_header(p, conf)
    return ReadHeaderOutput(
        version=ret["version"], segments=HeaderSegments(**ret["segments"])
    )


def fcs_read_raw_text(
    p: Path,
    # header args
    text_correction: OffsetCorrection = DEFAULT_CORRECTION,
    data_correction: OffsetCorrection = DEFAULT_CORRECTION,
    analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = DEFAULT_OTHER_WIDTH,
    squish_offsets: bool = False,
    allow_negative: bool = False,
    truncate_offsets: bool = False,
    # raw text args
    version_override: FCSVersion | None = None,
    supp_text_correction: OffsetCorrection = DEFAULT_CORRECTION,
    allow_duplicated_stext: bool = False,
    ignore_supp_text: bool = False,
    use_literal_delims: bool = False,
    allow_non_ascii_delim: bool = False,
    allow_missing_final_delim: bool = False,
    allow_nonunique: bool = False,
    allow_odd: bool = False,
    allow_empty: bool = False,
    allow_delim_at_boundary: bool = False,
    allow_non_utf8: bool = False,
    allow_non_ascii_keywords: bool = False,
    allow_missing_stext: bool = False,
    allow_stext_own_delim: bool = False,
    allow_missing_nextdata: bool = False,
    trim_value_whitespace: bool = False,
    date_pattern: str | None = None,
    ignore_standard_keys: KeyPatterns = DEFAULT_KEY_PATTERNS,
    rename_standard_keys: dict[str, str] = {},
    promote_to_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
    demote_from_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    # shared args
    warnings_are_errors: bool = False,
) -> ReadRawTEXTOutput:
    """
    Read the HEADER and TEXT of an FCS file.
    """
    args = {k: v for k, v in locals().items() if k != "p"}
    conf = {
        "raw": _assign_raw_args(args),
        "shared": _assign_args(list(_SHARED_ARGS), args),
    }
    assert len(args) == 0, False
    ret = _api._fcs_read_raw_text(p, conf)
    keywords = ret["keywords"]
    return ReadRawTEXTOutput(
        version=ret["version"],
        **keywords,
        parse=_to_parse_data(ret["parse"]),
    )


def fcs_read_std_text(
    p: Path,
    # header args
    version_override: FCSVersion | None = None,
    text_correction: OffsetCorrection = DEFAULT_CORRECTION,
    data_correction: OffsetCorrection = DEFAULT_CORRECTION,
    analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = DEFAULT_OTHER_WIDTH,
    squish_offsets: bool = False,
    allow_negative: bool = False,
    truncate_offsets: bool = False,
    # raw text args
    supp_text_correction: OffsetCorrection = DEFAULT_CORRECTION,
    allow_duplicated_stext: bool = False,
    ignore_supp_text: bool = False,
    use_literal_delims: bool = False,
    allow_non_ascii_delim: bool = False,
    allow_missing_final_delim: bool = False,
    allow_nonunique: bool = False,
    allow_odd: bool = False,
    allow_empty: bool = False,
    allow_delim_at_boundary: bool = False,
    allow_non_utf8: bool = False,
    allow_non_ascii_keywords: bool = False,
    allow_missing_stext: bool = False,
    allow_stext_own_delim: bool = False,
    allow_missing_nextdata: bool = False,
    trim_value_whitespace: bool = False,
    date_pattern: str | None = None,
    ignore_standard_keys: KeyPatterns = DEFAULT_KEY_PATTERNS,
    rename_standard_keys: dict[str, str] = {},
    promote_to_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
    demote_from_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    # standard args
    time_pattern: str | None = DEFAULT_TIME_PATTERN,
    allow_missing_time: bool = False,
    shortname_prefix: str = DEFAULT_SHORTNAME_PREFIX,
    allow_pseudostandard: bool = False,
    disallow_deprecated: bool = False,
    fix_log_scale_offsets: bool = False,
    nonstandard_measurement_pattern: str | None = None,
    # offset args
    text_data_correction: OffsetCorrection = DEFAULT_CORRECTION,
    text_analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: bool = False,
    allow_missing_required_offsets: bool = False,
    truncate_text_offsets: bool = False,
    # layout args
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: bool = False,
    # shared args
    warnings_are_errors: bool = False,
) -> ReadStdTEXTOutput:
    """
    Read the HEADER and standardized TEXT of an FCS file.
    """
    args = {k: v for k, v in locals().items() if k != "p"}
    conf = {
        "raw": _assign_raw_args(args),
        "standard": _assign_args(list(_STD_ARGS), args),
        "offsets": _assign_args(list(_OFFSET_ARGS), args),
        "layout": _assign_args(list(_LAYOUT_ARGS), args),
        "shared": _assign_args(list(_SHARED_ARGS), args),
    }
    assert len(args) == 0, False
    core, uncore = _api._fcs_read_std_text(p, conf)
    return ReadStdTEXTOutput(core=core, uncore=_to_std_text_data(uncore))


def fcs_read_raw_dataset(
    p: Path,
    # header args
    version_override: FCSVersion | None = None,
    text_correction: OffsetCorrection = DEFAULT_CORRECTION,
    data_correction: OffsetCorrection = DEFAULT_CORRECTION,
    analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = DEFAULT_OTHER_WIDTH,
    squish_offsets: bool = False,
    allow_negative: bool = False,
    truncate_offsets: bool = False,
    # raw text args
    supp_text_correction: OffsetCorrection = DEFAULT_CORRECTION,
    allow_duplicated_stext: bool = False,
    ignore_supp_text: bool = False,
    use_literal_delims: bool = False,
    allow_non_ascii_delim: bool = False,
    allow_missing_final_delim: bool = False,
    allow_nonunique: bool = False,
    allow_odd: bool = False,
    allow_empty: bool = False,
    allow_delim_at_boundary: bool = False,
    allow_non_utf8: bool = False,
    allow_non_ascii_keywords: bool = False,
    allow_missing_stext: bool = False,
    allow_stext_own_delim: bool = False,
    allow_missing_nextdata: bool = False,
    trim_value_whitespace: bool = False,
    date_pattern: str | None = None,
    ignore_standard_keys: KeyPatterns = DEFAULT_KEY_PATTERNS,
    rename_standard_keys: dict[str, str] = {},
    promote_to_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
    demote_from_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    # offset args
    text_data_correction: OffsetCorrection = DEFAULT_CORRECTION,
    text_analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: bool = False,
    allow_missing_required_offsets: bool = False,
    truncate_text_offsets: bool = False,
    # layout args
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: bool = False,
    # data args
    allow_uneven_event_width: bool = False,
    allow_tot_mismatch: bool = False,
    allow_data_par_mismatch: bool = False,
    # shared args
    warnings_are_errors: bool = False,
) -> ReadRawDatasetOutput:
    """
    Read an FCS file with standardized TEXT.
    """
    args = {k: v for k, v in locals().items() if k != "p"}
    conf = {
        "raw": _assign_raw_args(args),
        "offsets": _assign_args(list(_OFFSET_ARGS), args),
        "layout": _assign_args(list(_LAYOUT_ARGS), args),
        "data": _assign_args(list(_DATA_ARGS), args),
        "shared": _assign_args(list(_SHARED_ARGS), args),
    }
    assert len(args) == 0, False
    ret = _api._fcs_read_raw_dataset(p, conf)
    text = ret["text"]
    return ReadRawDatasetOutput(
        text=_to_raw_output(ret["text"]),
        **ret["dataset"],
    )


def fcs_read_std_dataset(
    p: Path,
    # header args
    version_override: FCSVersion | None = None,
    text_correction: OffsetCorrection = DEFAULT_CORRECTION,
    data_correction: OffsetCorrection = DEFAULT_CORRECTION,
    analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = DEFAULT_OTHER_WIDTH,
    squish_offsets: bool = False,
    allow_negative: bool = False,
    truncate_offsets: bool = False,
    # raw text args
    supp_text_correction: OffsetCorrection = DEFAULT_CORRECTION,
    allow_duplicated_stext: bool = False,
    ignore_supp_text: bool = False,
    use_literal_delims: bool = False,
    allow_non_ascii_delim: bool = False,
    allow_missing_final_delim: bool = False,
    allow_nonunique: bool = False,
    allow_odd: bool = False,
    allow_empty: bool = False,
    allow_delim_at_boundary: bool = False,
    allow_non_utf8: bool = False,
    allow_non_ascii_keywords: bool = False,
    allow_missing_stext: bool = False,
    allow_stext_own_delim: bool = False,
    allow_missing_nextdata: bool = False,
    trim_value_whitespace: bool = False,
    date_pattern: str | None = None,
    ignore_standard_keys: KeyPatterns = DEFAULT_KEY_PATTERNS,
    rename_standard_keys: dict[str, str] = {},
    promote_to_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
    demote_from_standard: KeyPatterns = DEFAULT_KEY_PATTERNS,
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    # standard args
    time_pattern: str | None = DEFAULT_TIME_PATTERN,
    allow_missing_time: bool = False,
    shortname_prefix: str = DEFAULT_SHORTNAME_PREFIX,
    allow_pseudostandard: bool = False,
    disallow_deprecated: bool = False,
    fix_log_scale_offsets: bool = False,
    nonstandard_measurement_pattern: str | None = None,
    # offset args
    text_data_correction: OffsetCorrection = DEFAULT_CORRECTION,
    text_analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: bool = False,
    allow_missing_required_offsets: bool = False,
    truncate_text_offsets: bool = False,
    # layout args
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: bool = False,
    # data args
    allow_uneven_event_width: bool = False,
    allow_tot_mismatch: bool = False,
    allow_data_par_mismatch: bool = False,
    # shared args
    warnings_are_errors: bool = False,
) -> ReadStdDatasetOutput:
    """
    Read an FCS file with standardized TEXT.
    """
    args = {k: v for k, v in locals().items() if k != "p"}
    conf = {
        "raw": _assign_raw_args(args),
        "offsets": _assign_args(list(_OFFSET_ARGS), args),
        "layout": _assign_args(list(_LAYOUT_ARGS), args),
        "standard": _assign_args(list(_STD_ARGS), args),
        "data": _assign_args(list(_DATA_ARGS), args),
        "shared": _assign_args(list(_SHARED_ARGS), args),
    }
    assert len(args) == 0, False
    core, uncore = _api._fcs_read_std_dataset(p, conf)
    return ReadStdDatasetOutput(
        core=core,
        uncore=StdDatasetData(
            parse=_to_parse_data(uncore["parse"]),
            pseudostandard=uncore["dataset"]["pseudostandard"],
            data_seg=uncore["dataset"]["standardized"]["data_seg"],
            analysis_seg=uncore["dataset"]["standardized"]["analysis_seg"],
        ),
    )


def fcs_read_raw_dataset_with_keywords(
    p: Path,
    version: FCSVersion,
    std: dict[str, str],
    data_seg: Segment,
    analysis_seg: Segment,
    other_segs: list[Segment],
    # offset args
    text_data_correction: OffsetCorrection = DEFAULT_CORRECTION,
    text_analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: bool = False,
    allow_missing_required_offsets: bool = False,
    truncate_text_offsets: bool = False,
    # layout args
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: bool = False,
    # data args
    allow_uneven_event_width: bool = False,
    allow_tot_mismatch: bool = False,
    allow_data_par_mismatch: bool = False,
    # shared args
    warnings_are_errors: bool = False,
) -> ReadRawDatasetFromKwsOutput:
    """
    Read raw data from FCS file using a given set of keywords.
    """
    omit = ["p", "version", "std", "data_seg", "analysis_seg", "other_segs"]
    args = {k: v for k, v in locals().items() if k not in omit}
    conf = {
        "offsets": _assign_args(list(_OFFSET_ARGS), args),
        "layout": _assign_args(list(_LAYOUT_ARGS), args),
        "data": _assign_args(list(_DATA_ARGS), args),
        "shared": _assign_args(list(_SHARED_ARGS), args),
    }
    assert len(args) == 0, False
    ret = _api._fcs_read_raw_dataset_with_keywords(
        p, version, std, data_seg, analysis_seg, other_segs, conf
    )
    return ReadRawDatasetFromKwsOutput(**ret)


def fcs_read_std_dataset_with_keywords(
    p: Path,
    version: FCSVersion,
    std: dict[str, str],
    nonstd: dict[str, str],
    data_seg: Segment,
    analysis_seg: Segment,
    other_segs: list[Segment],
    # standard args
    time_pattern: str | None = DEFAULT_TIME_PATTERN,
    allow_missing_time: bool = False,
    shortname_prefix: str = DEFAULT_SHORTNAME_PREFIX,
    allow_pseudostandard: bool = False,
    disallow_deprecated: bool = False,
    fix_log_scale_offsets: bool = False,
    nonstandard_measurement_pattern: str | None = None,
    # offset args
    text_data_correction: OffsetCorrection = DEFAULT_CORRECTION,
    text_analysis_correction: OffsetCorrection = DEFAULT_CORRECTION,
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: bool = False,
    allow_missing_required_offsets: bool = False,
    truncate_text_offsets: bool = False,
    # layout args
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: bool = False,
    # data args
    allow_uneven_event_width: bool = False,
    allow_tot_mismatch: bool = False,
    allow_data_par_mismatch: bool = False,
    # shared args
    warnings_are_errors: bool = False,
) -> ReadStdDatasetFromKwsOutput:
    """
    Read standardized data from FCS file using a given set of keywords.
    """
    omit = ["p", "version", "std", "nonstd", "data_seg", "analysis_seg", "other_segs"]
    args = {k: v for k, v in locals().items() if k not in omit}
    conf = {
        "std": _assign_args(list(_STD_ARGS), args),
        "offsets": _assign_args(list(_OFFSET_ARGS), args),
        "layout": _assign_args(list(_LAYOUT_ARGS), args),
        "data": _assign_args(list(_DATA_ARGS), args),
        "shared": _assign_args(list(_SHARED_ARGS), args),
    }
    assert len(args) == 0, False
    core, uncore = _api._fcs_read_std_dataset_with_keywords(
        p,
        version,
        {"std": std, "nonstd": nonstd},
        data_seg,
        analysis_seg,
        other_segs,
        conf,
    )
    return ReadStdDatasetFromKwsOutput(
        core=core,
        pseudostandard=uncore["pseudostandard"],
        data_seg=uncore["standardized"]["data_seg"],
        analysis_seg=uncore["standardized"]["analysis_seg"],
    )


def _format_docstring(front: str, params: list[tuple[str, str]]) -> str:
    # TODO actually indent these appropriately
    def format_param(key: str, value: str) -> str:
        return f":param {key}: {value}"

    ps = "\n".join([format_param(k, v) for k, v in params])
    return f"{front}\n\n{ps}"


fcs_read_header.__doc__ = _format_docstring(
    "Read the *HEADER* of an FCS file.", [*_HEADER_ARGS.items()]
)

del _format_docstring
