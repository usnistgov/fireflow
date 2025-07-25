from ._pyreflow import (
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
from typing import Literal

FCSVersion = (
    Literal["FCS2.0"] | Literal["FCS3.0"] | Literal["FCS3.1"] | Literal["FCS3.2"]
)

Segment = tuple[int, int]
OffsetCorrection = tuple[int, int]
KeyPatterns = tuple[list[str], list[str]]


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
) -> tuple[FCSVersion, Segment, Segment, Segment, list[Segment]]:
    conf = {
        "version_override": version_override,
        "text_correction": text_correction,
        "data_correction": data_correction,
        "analysis_correction": analysis_correction,
        "other_corrections": other_corrections,
        "max_other": max_other,
        "other_width": other_width,
        "squish_offsets": squish_offsets,
        "allow_negative": allow_negative,
        "truncate_offsets": truncate_offsets,
    }
    ret = _fcs_read_header(p, conf)
    segments = ret["segments"]
    return (
        ret["version"],
        segments["text"],
        segments["data"],
        segments["analysis"],
        segments["other"],
    )


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
) -> tuple[FCSVersion, dict[str, str], dict[str, str]]:
    header_conf = {
        "version_override": version_override,
        "text_correction": text_correction,
        "data_correction": data_correction,
        "analysis_correction": analysis_correction,
        "other_corrections": other_corrections,
        "max_other": max_other,
        "other_width": other_width,
        "squish_offsets": squish_offsets,
        "allow_negative": allow_negative,
        "truncate_offsets": truncate_offsets,
    }
    raw_conf = {
        "header": header_conf,
        "supp_text_correction": supp_text_correction,
        "allow_duplicated_stext": allow_duplicated_stext,
        "ignore_supp_text": ignore_supp_text,
        "use_literal_delims": use_literal_delims,
        "allow_non_ascii_delim": allow_non_ascii_delim,
        "allow_missing_final_delim": allow_missing_final_delim,
        "allow_nonunique": allow_nonunique,
        "allow_odd": allow_odd,
        "allow_empty": allow_empty,
        "allow_delim_at_boundary": allow_delim_at_boundary,
        "allow_non_utf8": allow_non_utf8,
        "allow_non_ascii_keywords": allow_non_ascii_keywords,
        "allow_missing_stext": allow_missing_stext,
        "allow_stext_own_delim": allow_stext_own_delim,
        "allow_missing_nextdata": allow_missing_nextdata,
        "trim_value_whitespace": trim_value_whitespace,
        "date_pattern": date_pattern,
        "ignore_standard_keys": ignore_standard_keys,
        "rename_standard_keys": rename_standard_keys,
        "promote_to_standard": promote_to_standard,
        "demote_from_standard": demote_from_standard,
        "replace_standard_key_values": replace_standard_key_values,
        "append_standard_keywords": append_standard_keywords,
    }
    shared_conf = {
        "warnings_are_errors": warnings_are_errors,
    }
    conf = {
        "raw": raw_conf,
        "shared": shared_conf,
    }
    ret = _fcs_read_raw_text(p, conf)
    keywords = ret["keywords"]
    return (
        ret["version"],
        keywords["std"],
        keywords["nonstd"],
    )


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
