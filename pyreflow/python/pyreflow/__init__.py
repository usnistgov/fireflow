from ._pyreflow import (  # type: ignore
    CoreTEXT2_0,
    CoreTEXT3_0,
    CoreTEXT3_1,
    CoreTEXT3_2,
    CoreDataset2_0,
    CoreDataset3_0,
    CoreDataset3_1,
    CoreDataset3_2,
    Optical2_0,
    Optical3_0,
    Optical3_1,
    Optical3_2,
    Temporal2_0,
    Temporal3_0,
    Temporal3_1,
    Temporal3_2,
    AsciiFixedLayout,
    AsciiDelimLayout,
    OrderedUint08Layout,
    OrderedUint16Layout,
    OrderedUint24Layout,
    OrderedUint32Layout,
    OrderedUint40Layout,
    OrderedUint48Layout,
    OrderedUint56Layout,
    OrderedUint64Layout,
    OrderedF32Layout,
    OrderedF64Layout,
    EndianF32Layout,
    EndianF64Layout,
    EndianUintLayout,
    MixedLayout,
)
import pyreflow._pyreflow as pf


from pathlib import Path
from typing import Literal, Any, Union, NamedTuple, NewType
import polars as pl

Calibration3_1 = tuple[float, str]
Calibration3_2 = tuple[float, float, str]

Mode = Union[Literal["L"] | Literal["C"] | Literal["U"]]

Originality = Union[
    Literal["Original"]
    | Literal["NonDataModified"]
    | Literal["Appended"]
    | Literal["DataModified"]
]

Feature = Union[Literal["Area"] | Literal["Width"] | Literal["Height"]]

FCSVersion = (
    Literal["FCS2.0"] | Literal["FCS3.0"] | Literal["FCS3.1"] | Literal["FCS3.2"]
)

FloatType = Literal["F"]
DoubleType = Literal["D"]
IntegerType = Literal["I"]
AsciiType = Literal["A"]

Datatype = Union[FloatType | DoubleType | IntegerType | AsciiType]
MixedType = Union[
    tuple[FloatType | DoubleType, float], tuple[AsciiType | IntegerType, int]
]
Trigger = tuple[str, int]
Shortname = NewType("Shortname", str)

Segment = tuple[int, int]
OffsetCorrection = tuple[int, int]
KeyPatterns = tuple[list[str], list[str]]

StdKey = NewType("StdKey", str)
NonStdKey = NewType("NonStdKey", str)

StdKeywords = dict[StdKey, str]
NonStdKeywords = dict[NonStdKey, str]


class HeaderSegments(NamedTuple):
    text: Segment
    data: Segment
    analysis: Segment
    other: list[Segment]


class ParseData(NamedTuple):
    header_segments: HeaderSegments
    supp_text: Segment | None
    nextdata: int | None
    delimiter: int
    non_ascii: dict[str, str]
    byte_pairs: dict[bytes, bytes]


class StdTEXTData(NamedTuple):
    tot: int | None
    timestep: float | None
    data: Segment
    analysis: Segment
    pseudostandard: dict[str, str]
    parse: ParseData


class StdDatasetData(NamedTuple):
    parse: ParseData
    pseudostandard: dict[str, str]
    data_seg: Segment
    analysis_seg: Segment


AnyCoreTEXT = Union[CoreTEXT2_0 | CoreTEXT3_0 | CoreTEXT3_1 | CoreTEXT3_2]

AnyCoreDataset = Union[
    CoreDataset2_0 | CoreDataset3_0 | CoreDataset3_1 | CoreDataset3_2
]

AnalysisBytes = NewType("AnalysisBytes", bytes)
OtherBytes = NewType("OtherBytes", bytes)


class ReadHeaderOutput(NamedTuple):
    version: FCSVersion
    segments: HeaderSegments


# TODO use newtype wrappers for std and nonstd to prevent mixing downstream
class ReadRawTEXTOutput(NamedTuple):
    version: FCSVersion
    std: StdKeywords
    nonstd: NonStdKeywords
    parse: ParseData


class ReadStdTEXTOutput(NamedTuple):
    core: AnyCoreTEXT
    uncore: StdTEXTData


class ReadRawDatasetOutput(NamedTuple):
    text: ReadRawTEXTOutput
    data: pl.DataFrame
    analysis: AnalysisBytes
    others: list[OtherBytes]
    data_seg: Segment
    analysis_seg: Segment


class ReadStdDatasetOutput(NamedTuple):
    core: AnyCoreDataset
    uncore: StdDatasetData


class ReadRawDatasetFromKwsOutput(NamedTuple):
    data: pl.DataFrame
    analysis: AnalysisBytes
    others: list[OtherBytes]
    data_seg: Segment
    analysis_seg: Segment


class ReadStdDatasetFromKwsOutput(NamedTuple):
    core: AnyCoreDataset
    pseudostandard: dict[str, str]
    data_seg: Segment
    analysis_seg: Segment


def to_parse_data(xs: dict[str, Any]) -> ParseData:
    args: dict[str, Any] = {
        k: HeaderSegments(**v) if k == "header_segments" else v for k, v in xs.items()
    }
    return ParseData(**args)


def to_std_text_data(xs: dict[str, Any]) -> StdTEXTData:
    args: dict[str, Any] = {
        k: to_parse_data(v) if k == "parse" else v for k, v in xs.items()
    }
    return StdTEXTData(**args)


def to_raw_output(xs: dict[str, Any]) -> ReadRawTEXTOutput:
    return ReadRawTEXTOutput(
        version=xs["version"],
        **xs["keywords"],
        parse=to_parse_data(xs["parse"]),
    )


HEADER_ARGS = [
    "version_override",
    "text_correction",
    "data_correction",
    "analysis_correction",
    "other_corrections",
    "max_other",
    "other_width",
    "squish_offsets",
    "allow_negative",
    "truncate_offsets",
]

RAW_ARGS = [
    "supp_text_correction",
    "allow_duplicated_stext",
    "ignore_supp_text",
    "use_literal_delims",
    "allow_non_ascii_delim",
    "allow_missing_final_delim",
    "allow_nonunique",
    "allow_odd",
    "allow_empty",
    "allow_delim_at_boundary",
    "allow_non_utf8",
    "allow_non_ascii_keywords",
    "allow_missing_stext",
    "allow_stext_own_delim",
    "allow_missing_nextdata",
    "trim_value_whitespace",
    "date_pattern",
    "ignore_standard_keys",
    "rename_standard_keys",
    "promote_to_standard",
    "demote_from_standard",
    "replace_standard_key_values",
    "append_standard_keywords",
]

STD_ARGS = [
    "time_pattern",
    "allow_missing_time",
    "shortname_prefix",
    "allow_pseudostandard",
    "disallow_deprecated",
    "fix_log_scale_offsets",
    "nonstandard_measurement_pattern",
]

OFFSET_ARGS = [
    "text_data_correction",
    "text_analysis_correction",
    "ignore_text_data_offsets",
    "ignore_text_analysis_offsets",
    "allow_header_text_offset_mismatch",
    "allow_missing_required_offsets",
    "truncate_text_offsets",
]

LAYOUT_ARGS = [
    "integer_widths_from_byteord",
    "integer_byteord_override",
    "disallow_range_truncation",
]

DATA_ARGS = [
    "allow_uneven_event_width",
    "allow_tot_mismatch",
    "allow_data_par_mismatch",
]

SHARED_ARGS = [
    "warnings_are_errors",
]


def _assign_args(keys: list[str], src: dict[str, Any]) -> dict[str, Any]:
    acc: dict[str, Any] = {}
    for k in keys:
        acc[k] = src.pop(k)
    return acc


def _assign_raw_args(src: dict[str, Any]) -> dict[str, Any]:
    raw_conf = _assign_args(RAW_ARGS, src)
    raw_conf["header"] = _assign_args(HEADER_ARGS, src)
    return raw_conf


DEFAULT_CORRECTION = (0, 0)
DEFAULT_KEY_PATTERNS: tuple[list[str], list[str]] = ([], [])
DEFAULT_OTHER_WIDTH = 8
DEFAULT_SHORTNAME_PREFIX = "P"
DEFAULT_TIME_PATTERN = "^(TIME|Time)$"


def fcs_read_header(
    p: Path,
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
) -> ReadHeaderOutput:
    args = {k: v for k, v in locals().items() if k != "p"}
    conf = _assign_args(HEADER_ARGS, args)
    assert len(args) == 0, False
    ret = pf._fcs_read_header(p, conf)
    return ReadHeaderOutput(
        version=ret["version"], segments=HeaderSegments(**ret["segments"])
    )


def fcs_read_raw_text(
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
    # shared args
    warnings_are_errors: bool = False,
) -> ReadRawTEXTOutput:
    args = {k: v for k, v in locals().items() if k != "p"}
    conf = {
        "raw": _assign_raw_args(args),
        "shared": _assign_args(SHARED_ARGS, args),
    }
    assert len(args) == 0, False
    ret = pf._fcs_read_raw_text(p, conf)
    keywords = ret["keywords"]
    return ReadRawTEXTOutput(
        version=ret["version"],
        **keywords,
        parse=to_parse_data(ret["parse"]),
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
    integer_byteord_override: list[int] | None = None,
    disallow_range_truncation: bool = False,
    # shared args
    warnings_are_errors: bool = False,
) -> ReadStdTEXTOutput:
    args = {k: v for k, v in locals().items() if k != "p"}
    conf = {
        "raw": _assign_raw_args(args),
        "standard": _assign_args(STD_ARGS, args),
        "offsets": _assign_args(OFFSET_ARGS, args),
        "layout": _assign_args(LAYOUT_ARGS, args),
        "shared": _assign_args(SHARED_ARGS, args),
    }
    assert len(args) == 0, False
    core, uncore = pf._fcs_read_std_text(p, conf)
    return ReadStdTEXTOutput(core=core, uncore=to_std_text_data(uncore))


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
    integer_byteord_override: list[int] | None = None,
    disallow_range_truncation: bool = False,
    # data args
    allow_uneven_event_width: bool = False,
    allow_tot_mismatch: bool = False,
    allow_data_par_mismatch: bool = False,
    # shared args
    warnings_are_errors: bool = False,
) -> ReadRawDatasetOutput:
    args = {k: v for k, v in locals().items() if k != "p"}
    conf = {
        "raw": _assign_raw_args(args),
        "offsets": _assign_args(OFFSET_ARGS, args),
        "layout": _assign_args(LAYOUT_ARGS, args),
        "data": _assign_args(DATA_ARGS, args),
        "shared": _assign_args(SHARED_ARGS, args),
    }
    assert len(args) == 0, False
    ret = pf._fcs_read_raw_dataset(p, conf)
    text = ret["text"]
    return ReadRawDatasetOutput(
        text=to_raw_output(ret["text"]),
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
    integer_byteord_override: list[int] | None = None,
    disallow_range_truncation: bool = False,
    # data args
    allow_uneven_event_width: bool = False,
    allow_tot_mismatch: bool = False,
    allow_data_par_mismatch: bool = False,
    # shared args
    warnings_are_errors: bool = False,
) -> ReadStdDatasetOutput:
    args = {k: v for k, v in locals().items() if k != "p"}
    conf = {
        "raw": _assign_raw_args(args),
        "offsets": _assign_args(OFFSET_ARGS, args),
        "layout": _assign_args(LAYOUT_ARGS, args),
        "standard": _assign_args(STD_ARGS, args),
        "data": _assign_args(DATA_ARGS, args),
        "shared": _assign_args(SHARED_ARGS, args),
    }
    assert len(args) == 0, False
    core, uncore = pf._fcs_read_std_dataset(p, conf)
    return ReadStdDatasetOutput(
        core=core,
        uncore=StdDatasetData(
            parse=to_parse_data(uncore["parse"]),
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
    integer_byteord_override: list[int] | None = None,
    disallow_range_truncation: bool = False,
    # data args
    allow_uneven_event_width: bool = False,
    allow_tot_mismatch: bool = False,
    allow_data_par_mismatch: bool = False,
    # shared args
    warnings_are_errors: bool = False,
) -> ReadRawDatasetFromKwsOutput:
    omit = ["p", "version", "std", "data_seg", "analysis_seg", "other_segs"]
    args = {k: v for k, v in locals().items() if k not in omit}
    conf = {
        "offsets": _assign_args(OFFSET_ARGS, args),
        "layout": _assign_args(LAYOUT_ARGS, args),
        "data": _assign_args(DATA_ARGS, args),
        "shared": _assign_args(SHARED_ARGS, args),
    }
    assert len(args) == 0, False
    ret = pf._fcs_read_raw_dataset_with_keywords(
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
    integer_byteord_override: list[int] | None = None,
    disallow_range_truncation: bool = False,
    # data args
    allow_uneven_event_width: bool = False,
    allow_tot_mismatch: bool = False,
    allow_data_par_mismatch: bool = False,
    # shared args
    warnings_are_errors: bool = False,
) -> ReadStdDatasetFromKwsOutput:
    omit = ["p", "version", "std", "nonstd", "data_seg", "analysis_seg", "other_segs"]
    args = {k: v for k, v in locals().items() if k not in omit}
    conf = {
        "std": _assign_args(STD_ARGS, args),
        "offsets": _assign_args(OFFSET_ARGS, args),
        "layout": _assign_args(LAYOUT_ARGS, args),
        "data": _assign_args(DATA_ARGS, args),
        "shared": _assign_args(SHARED_ARGS, args),
    }
    assert len(args) == 0, False
    core, uncore = pf._fcs_read_std_dataset_with_keywords(
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


__all__ = [
    "__version__",
    *[
        obj.__name__
        for obj in [
            CoreTEXT2_0,
            CoreTEXT3_0,
            CoreTEXT3_1,
            CoreTEXT3_2,
            CoreDataset2_0,
            CoreDataset3_0,
            CoreDataset3_1,
            CoreDataset3_2,
            Optical2_0,
            Optical3_0,
            Optical3_1,
            Optical3_2,
            Temporal2_0,
            Temporal3_0,
            Temporal3_1,
            Temporal3_2,
            AsciiFixedLayout,
            AsciiDelimLayout,
            OrderedUint08Layout,
            OrderedUint16Layout,
            OrderedUint24Layout,
            OrderedUint32Layout,
            OrderedUint40Layout,
            OrderedUint48Layout,
            OrderedUint56Layout,
            OrderedUint64Layout,
            OrderedF32Layout,
            OrderedF64Layout,
            EndianF32Layout,
            EndianF64Layout,
            EndianUintLayout,
            MixedLayout,
            fcs_read_header,
            fcs_read_raw_text,
            fcs_read_std_text,
            fcs_read_raw_dataset,
            fcs_read_std_dataset,
            fcs_read_raw_dataset_with_keywords,
            fcs_read_std_dataset_with_keywords,
        ]
    ],
]
