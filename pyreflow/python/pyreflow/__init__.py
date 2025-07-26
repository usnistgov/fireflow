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
    _fcs_read_header,
    _fcs_read_raw_text,
    _fcs_read_std_text,
    _fcs_read_std_dataset,
    _fcs_read_raw_dataset,
    _fcs_read_raw_dataset_with_keywords,
    _fcs_read_std_dataset_with_keywords,
)


from pathlib import Path
from typing import Literal, Any, TypedDict, Union
import polars as pl


FCSVersion = (
    Literal["FCS2.0"] | Literal["FCS3.0"] | Literal["FCS3.1"] | Literal["FCS3.2"]
)

Segment = tuple[int, int]
OffsetCorrection = tuple[int, int]
KeyPatterns = tuple[list[str], list[str]]

HeaderSegments = TypedDict(
    "HeaderSegments",
    {"text": Segment, "data": Segment, "analysis": Segment, "other": list[Segment]},
)

ParseData = TypedDict(
    "ParseData",
    {
        "header_segments": HeaderSegments,
        "supp_text": Segment | None,
        "nextdata": int | None,
        "delimiter": int,
        "non_ascii": dict[str, str],
        "byte_pairs": dict[bytes, bytes],
    },
)

StdTEXTOutput = TypedDict(
    "StdTEXTOutput",
    {
        "tot": int | None,
        "timestep": float | None,
        "data": Segment,
        "analysis": Segment,
        "pseudostandard": dict[str, str],
        "parse": ParseData,
    },
)

StdTEXTSegments = TypedDict(
    "StdTEXTSegments",
    {
        "data_seg": Segment,
        "analysis_seg": Segment,
    },
)

StdDatasetData = TypedDict(
    "StdDatasetData",
    {
        "standardized": StdTEXTSegments,
        "pseudostandard": dict[str, str],
    },
)

StdDatasetOutput = TypedDict(
    "StdDatasetOutput",
    {
        "dataset": StdDatasetData,
        "parse": ParseData,
    },
)

AnyCoreTEXT = Union[CoreTEXT2_0 | CoreTEXT3_0 | CoreTEXT3_1 | CoreTEXT3_2]

AnyCoreDataset = Union[
    CoreDataset2_0 | CoreDataset3_0 | CoreDataset3_1 | CoreDataset3_2
]

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


def assign_args(keys: list[str], src: dict[str, Any]) -> dict[str, Any]:
    acc: dict[str, Any] = {}
    for k in keys:
        acc[k] = src.pop(k)
    return acc


def fcs_read_header(
    p: Path,
    version_override: FCSVersion | None = None,
    text_correction: OffsetCorrection = (0, 0),
    data_correction: OffsetCorrection = (0, 0),
    analysis_correction: OffsetCorrection = (0, 0),
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    squish_offsets: bool = False,
    allow_negative: bool = False,
    truncate_offsets: bool = False,
) -> tuple[FCSVersion, HeaderSegments]:
    args = {k: v for k, v in locals().items() if k != "p"}
    conf = assign_args(HEADER_ARGS, args)
    assert len(args) == 0, False
    ret = _fcs_read_header(p, conf)
    return (ret["version"], ret["segments"])


def fcs_read_raw_text(
    p: Path,
    # header args
    version_override: FCSVersion | None = None,
    text_correction: OffsetCorrection = (0, 0),
    data_correction: OffsetCorrection = (0, 0),
    analysis_correction: OffsetCorrection = (0, 0),
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    squish_offsets: bool = False,
    allow_negative: bool = False,
    truncate_offsets: bool = False,
    # raw text args
    supp_text_correction: OffsetCorrection = (0, 0),
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
    ignore_standard_keys: KeyPatterns = ([], []),
    rename_standard_keys: dict[str, str] = {},
    promote_to_standard: KeyPatterns = ([], []),
    demote_from_standard: KeyPatterns = ([], []),
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    # shared args
    warnings_are_errors: bool = False,
) -> tuple[FCSVersion, dict[str, str], dict[str, str], ParseData]:
    args = {k: v for k, v in locals().items() if k != "p"}
    header_conf = assign_args(HEADER_ARGS, args)
    raw_conf = assign_args(RAW_ARGS, args)
    shared_conf = assign_args(SHARED_ARGS, args)
    raw_conf["header"] = header_conf
    conf = {
        "raw": raw_conf,
        "shared": shared_conf,
    }
    assert len(args) == 0, False
    ret = _fcs_read_raw_text(p, conf)
    keywords = ret["keywords"]
    return (ret["version"], keywords["std"], keywords["nonstd"], ret["parse"])


def fcs_read_std_text(
    p: Path,
    # header args
    version_override: FCSVersion | None = None,
    text_correction: OffsetCorrection = (0, 0),
    data_correction: OffsetCorrection = (0, 0),
    analysis_correction: OffsetCorrection = (0, 0),
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    squish_offsets: bool = False,
    allow_negative: bool = False,
    truncate_offsets: bool = False,
    # raw text args
    supp_text_correction: OffsetCorrection = (0, 0),
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
    ignore_standard_keys: KeyPatterns = ([], []),
    rename_standard_keys: dict[str, str] = {},
    promote_to_standard: KeyPatterns = ([], []),
    demote_from_standard: KeyPatterns = ([], []),
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    # standard args
    time_pattern: str | None = None,
    allow_missing_time: bool = False,
    shortname_prefix: str = "P",
    allow_pseudostandard: bool = False,
    disallow_deprecated: bool = False,
    fix_log_scale_offsets: bool = False,
    nonstandard_measurement_pattern: str | None = None,
    # offset args
    text_data_correction: OffsetCorrection = (0, 0),
    text_analysis_correction: OffsetCorrection = (0, 0),
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
) -> tuple[AnyCoreTEXT, StdTEXTOutput]:
    args = {k: v for k, v in locals().items() if k != "p"}
    raw_conf = assign_args(RAW_ARGS, args)
    raw_conf["header"] = assign_args(HEADER_ARGS, args)
    conf = {
        "raw": raw_conf,
        "standard": assign_args(STD_ARGS, args),
        "offsets": assign_args(OFFSET_ARGS, args),
        "layout": assign_args(LAYOUT_ARGS, args),
        "shared": assign_args(SHARED_ARGS, args),
    }
    assert len(args) == 0, False
    return _fcs_read_std_text(p, conf)


def fcs_read_raw_dataset(
    p: Path,
    # header args
    version_override: FCSVersion | None = None,
    text_correction: OffsetCorrection = (0, 0),
    data_correction: OffsetCorrection = (0, 0),
    analysis_correction: OffsetCorrection = (0, 0),
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    squish_offsets: bool = False,
    allow_negative: bool = False,
    truncate_offsets: bool = False,
    # raw text args
    supp_text_correction: OffsetCorrection = (0, 0),
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
    ignore_standard_keys: KeyPatterns = ([], []),
    rename_standard_keys: dict[str, str] = {},
    promote_to_standard: KeyPatterns = ([], []),
    demote_from_standard: KeyPatterns = ([], []),
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    # offset args
    text_data_correction: OffsetCorrection = (0, 0),
    text_analysis_correction: OffsetCorrection = (0, 0),
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
) -> tuple[
    FCSVersion,
    dict[str, str],
    dict[str, str],
    pl.DataFrame,
    bytes,
    list[bytes],
    Segment,
    Segment,
]:
    args = {k: v for k, v in locals().items() if k != "p"}
    raw_conf = assign_args(RAW_ARGS, args)
    raw_conf["header"] = assign_args(HEADER_ARGS, args)
    conf = {
        "raw": raw_conf,
        "offsets": assign_args(OFFSET_ARGS, args),
        "layout": assign_args(LAYOUT_ARGS, args),
        "data": assign_args(DATA_ARGS, args),
        "shared": assign_args(SHARED_ARGS, args),
    }
    assert len(args) == 0, False
    ret = _fcs_read_raw_dataset(p, conf)
    text = ret["text"]
    kws = text["keywords"]
    data = ret["dataset"]
    return (
        text["version"],
        kws["std"],
        kws["nonstd"],
        data["data"],
        data["analysis"],
        data["others"],
        data["data_seg"],
        data["analysis_seg"],
    )


def fcs_read_std_dataset(
    p: Path,
    # header args
    version_override: FCSVersion | None = None,
    text_correction: OffsetCorrection = (0, 0),
    data_correction: OffsetCorrection = (0, 0),
    analysis_correction: OffsetCorrection = (0, 0),
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    squish_offsets: bool = False,
    allow_negative: bool = False,
    truncate_offsets: bool = False,
    # raw text args
    supp_text_correction: OffsetCorrection = (0, 0),
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
    ignore_standard_keys: KeyPatterns = ([], []),
    rename_standard_keys: dict[str, str] = {},
    promote_to_standard: KeyPatterns = ([], []),
    demote_from_standard: KeyPatterns = ([], []),
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    # standard args
    time_pattern: str | None = None,
    allow_missing_time: bool = False,
    shortname_prefix: str = "P",
    allow_pseudostandard: bool = False,
    disallow_deprecated: bool = False,
    fix_log_scale_offsets: bool = False,
    nonstandard_measurement_pattern: str | None = None,
    # offset args
    text_data_correction: OffsetCorrection = (0, 0),
    text_analysis_correction: OffsetCorrection = (0, 0),
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
) -> tuple[AnyCoreDataset, StdDatasetOutput]:
    args = {k: v for k, v in locals().items() if k != "p"}
    raw_conf = assign_args(RAW_ARGS, args)
    raw_conf["header"] = assign_args(HEADER_ARGS, args)
    conf = {
        "raw": raw_conf,
        "offsets": assign_args(OFFSET_ARGS, args),
        "layout": assign_args(LAYOUT_ARGS, args),
        "standard": assign_args(STD_ARGS, args),
        "data": assign_args(DATA_ARGS, args),
        "shared": assign_args(SHARED_ARGS, args),
    }
    assert len(args) == 0, False
    return _fcs_read_std_dataset(p, conf)


__all__ = [
    "__version__",
    CoreTEXT2_0.__name__,
    CoreTEXT3_0.__name__,
    CoreTEXT3_1.__name__,
    CoreTEXT3_2.__name__,
    CoreDataset2_0.__name__,
    CoreDataset3_0.__name__,
    CoreDataset3_1.__name__,
    CoreDataset3_2.__name__,
    Optical2_0.__name__,
    Optical3_0.__name__,
    Optical3_1.__name__,
    Optical3_2.__name__,
    Temporal2_0.__name__,
    Temporal3_0.__name__,
    Temporal3_1.__name__,
    Temporal3_2.__name__,
    AsciiFixedLayout.__name__,
    AsciiDelimLayout.__name__,
    OrderedUint08Layout.__name__,
    OrderedUint16Layout.__name__,
    OrderedUint24Layout.__name__,
    OrderedUint32Layout.__name__,
    OrderedUint40Layout.__name__,
    OrderedUint48Layout.__name__,
    OrderedUint56Layout.__name__,
    OrderedUint64Layout.__name__,
    OrderedF32Layout.__name__,
    OrderedF64Layout.__name__,
    EndianF32Layout.__name__,
    EndianF64Layout.__name__,
    EndianUintLayout.__name__,
    MixedLayout.__name__,
    fcs_read_header.__name__,
]
