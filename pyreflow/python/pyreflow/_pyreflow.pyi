from __future__ import annotations
from pathlib import Path
from decimal import Decimal
from datetime import time, date, datetime
from typing import Self, final, Any, Literal

from polars import Series, DataFrame

import pyreflow.typing as pft

@final
class CoreTEXT2_0:
    def __new__(
        cls,
        measurements: pft.Measurements2_0,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema,
        mode: pft.Mode = "L",
        cyt: str = "",
        comp: pft.Compensation | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        abrt: int | None = None,
        com: str = "",
        cells: str = "",
        exp: str = "",
        fil: str = "",
        inst: str = "",
        lost: int | None = None,
        op: str = "",
        proj: str = "",
        smno: str = "",
        src: str = "",
        sys: str = "",
        tr: pft.Trigger | None = None,
        applied_gates: pft.AppliedGates2_0 = ([], {}, None),
        pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords = {},
        nonstandard_keywords: pft.NonStdKeywords = {},
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    data_schema: (
        FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema
    )
    mode: pft.Mode
    cyt: str
    comp: pft.Compensation | None
    btim: time | None
    etim: time | None
    date: date | None
    abrt: int | None
    com: str
    cells: str
    exp: str
    fil: str
    inst: str
    lost: int | None
    op: str
    proj: str
    smno: str
    src: str
    sys: str
    tr: pft.Trigger | None
    applied_gates: pft.AppliedGates2_0
    pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords
    nonstandard_keywords: pft.NonStdKeywords
    all_detector_types: list[str | tuple[()]]
    all_detector_voltages: list[float | tuple[()] | None]
    all_filters: list[str | tuple[()]]
    all_longnames: list[str]
    all_peak_bins: list[int | None]
    all_peak_sizes: list[int | None]
    all_percents_emitted: list[float | tuple[()] | None]
    all_powers: list[float | tuple[()] | None]
    all_scales: list[pft.OpticalScale2_0 | None]
    all_shortnames_maybe: list[pft.Shortname | None]
    all_shortnames: list[pft.Shortname]
    all_wavelengths: list[float | tuple[()] | None]
    @classmethod
    def from_kws(
        cls,
        std: pft.StdKeywords,
        pnonstd: pft.PseudoNonStdKeywords,
        nonstd: pft.NonStdKeywords,
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_optical_only_keys: list[pft.OpticalOnlyKey] = [],
        process_optical_only_keys: pft.ProcessOpticalOnlyKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: pft.ProcessKeywordFailure = "error",
        process_hyper_par: pft.ProcessKeywordFailure = "error",
        process_other_version: pft.ProcessKeywordFailure = "error",
        process_extra_timestep: pft.ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        int_width_override: pft.IntWidthOverride = "never",
        byteord_override: pft.ByteordOverride = "none",
        disallow_range_truncation: pft.TriFlag = "false",
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
    ) -> tuple[CoreTEXT2_0, StdTEXTDiagnostics]: ...
    def insert_optical(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname | None,
        meas: Optical2_0,
        range: pft.Range,
        scale: pft.OpticalScale2_0 | None = (),
    ) -> None: ...
    def insert_temporal(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: Temporal2_0,
        range: pft.Range,
    ) -> None: ...
    measurements: list[Optical2_0 | Temporal2_0]
    def measurement_at(self, index: pft.MeasIndex) -> Optical2_0 | Temporal2_0: ...
    def measurement_named(self, name: pft.Shortname) -> Optical2_0 | Temporal2_0: ...
    @property
    def par(self) -> int: ...
    def push_optical(
        self,
        name: pft.Shortname | None,
        meas: Optical2_0,
        range: pft.Range,
        scale: pft.OpticalScale2_0 | None = (),
    ) -> None: ...
    def push_temporal(
        self, name: pft.Shortname, meas: Temporal2_0, range: pft.Range
    ) -> None: ...
    def set_measurements_and_data_schema(
        self,
        measurements: list[Optical2_0 | Temporal2_0],
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema,
    ) -> None: ...
    def set_named_measurements_and_data_schema(
        self,
        measurements: pft.Measurements2_0,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements(
        self,
        measurements: pft.Measurements2_0,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_temporal_at(
        self, index: pft.MeasIndex, allow_loss: pft.TriFlag = "false"
    ) -> bool: ...
    def set_temporal(
        self, name: pft.Shortname, allow_loss: pft.TriFlag = "false"
    ) -> bool: ...
    def set_trigger_threshold(self, threshold: int) -> bool: ...
    def standard_keywords(
        self, req_or_opt: pft.ReqOrOpt = "both", root_or_meas: pft.RootOrMeas = "both"
    ) -> dict[pft.NEStr, pft.NEStr]: ...
    def remove_measurement_by_index(
        self, index: pft.MeasIndex
    ) -> tuple[
        pft.Shortname | None,
        Optical2_0 | Temporal2_0,
        pft.Range,
        pft.OpticalScale2_0 | None,
    ]: ...
    def remove_measurement_by_name(
        self, name: pft.Shortname
    ) -> tuple[
        pft.MeasIndex, Optical2_0 | Temporal2_0, pft.Range, pft.OpticalScale2_0 | None
    ]: ...
    def rename_temporal(self, name: pft.Shortname) -> pft.Shortname | None: ...
    def replace_optical_at(
        self, index: pft.MeasIndex, meas: Optical2_0
    ) -> Temporal2_0 | tuple[Optical2_0, pft.OpticalScale2_0 | None]: ...
    def replace_optical_named(
        self, name: pft.Shortname, meas: Optical2_0
    ) -> Temporal2_0 | tuple[Optical2_0, pft.OpticalScale2_0 | None]: ...
    def replace_temporal_at(
        self, index: pft.MeasIndex, meas: Temporal2_0
    ) -> Temporal2_0 | tuple[Optical2_0, pft.OpticalScale2_0 | None]: ...
    def replace_temporal_named(
        self, name: pft.Shortname, meas: Temporal2_0
    ) -> Temporal2_0 | tuple[Optical2_0, pft.OpticalScale2_0 | None]: ...
    @property
    def temporal(self) -> tuple[pft.MeasIndex, pft.Shortname, Temporal2_0] | None: ...
    def to_dataset(
        self,
        data: DataFrame,
        analysis: pft.AnalysisBytes = "",
        others: list[pft.OtherBytes] = [],
    ) -> CoreDataset2_0: ...
    def to_version_3_0(self, allow_loss: pft.TriFlag = "false") -> CoreTEXT3_0: ...
    def to_version_3_1(self, allow_loss: pft.TriFlag = "false") -> CoreTEXT3_1: ...
    def to_version_3_2(self, allow_loss: pft.TriFlag = "false") -> CoreTEXT3_2: ...
    @property
    def version(self) -> pft.FCSVersion: ...
    def unset_measurements(self) -> None: ...
    def unset_temporal(self) -> bool: ...
    @classmethod
    def write_texts(
        cls,
        path: Path,
        datasets: list[CoreTEXT2_0],
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
    ) -> int | None: ...
    def write_text(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
        appendable: bool = False,
        append: bool = False,
    ) -> int: ...

@final
class CoreTEXT3_0:
    def __new__(
        cls,
        measurements: pft.Measurements3_0,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema,
        mode: pft.Mode = "L",
        cyt: str = "",
        comp: pft.Compensation | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        cytsn: str = "",
        unicode: pft.Unicode | None = None,
        csvbits: int = 0,
        cstot: int = 0,
        csvflags: pft.CsvFlags = [],
        abrt: int | None = None,
        com: str = "",
        cells: str = "",
        exp: str = "",
        fil: str = "",
        inst: str = "",
        lost: int | None = None,
        op: str = "",
        proj: str = "",
        smno: str = "",
        src: str = "",
        sys: str = "",
        tr: pft.Trigger | None = None,
        applied_gates: pft.AppliedGates3_0 = ([], {}, None),
        pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords = {},
        nonstandard_keywords: pft.NonStdKeywords = {},
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    data_schema: (
        FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema
    )
    mode: pft.Mode
    cyt: str
    comp: pft.Compensation | None
    btim: time | None
    etim: time | None
    date: date | None
    cytsn: str
    unicode: pft.Unicode | None
    csvbits: int
    cstot: int
    csvflags: pft.CsvFlags
    abrt: int | None
    com: str
    cells: str
    exp: str
    fil: str
    inst: str
    lost: int | None
    op: str
    proj: str
    smno: str
    src: str
    sys: str
    tr: pft.Trigger | None
    applied_gates: pft.AppliedGates3_0
    pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords
    nonstandard_keywords: pft.NonStdKeywords
    all_detector_types: list[str | tuple[()]]
    all_detector_voltages: list[float | tuple[()] | None]
    all_filters: list[str | tuple[()]]
    all_longnames: list[str]
    all_peak_bins: list[int | None]
    all_peak_sizes: list[int | None]
    all_percents_emitted: list[float | tuple[()] | None]
    all_powers: list[float | tuple[()] | None]
    all_scales: list[pft.OpticalScale3_0]
    all_shortnames_maybe: list[pft.Shortname | None]
    all_shortnames: list[pft.Shortname]
    all_wavelengths: list[float | tuple[()] | None]
    @classmethod
    def from_kws(
        cls,
        std: pft.StdKeywords,
        pnonstd: pft.PseudoNonStdKeywords,
        nonstd: pft.NonStdKeywords,
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_optical_only_keys: list[pft.OpticalOnlyKey] = [],
        process_optical_only_keys: pft.ProcessOpticalOnlyKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: pft.ProcessKeywordFailure = "error",
        process_hyper_par: pft.ProcessKeywordFailure = "error",
        process_other_version: pft.ProcessKeywordFailure = "error",
        process_extra_timestep: pft.ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        add_missing_timestep: pft.Timestep | None = None,
        text_data_correction: pft.OffsetCorrection = (0, 0),
        text_analysis_correction: pft.OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: pft.TriFlag = "false",
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        int_width_override: pft.IntWidthOverride = "never",
        byteord_override: pft.ByteordOverride = "none",
        disallow_range_truncation: pft.TriFlag = "false",
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
    ) -> tuple[CoreTEXT3_0, StdTEXTDiagnostics]: ...
    def insert_optical(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname | None,
        meas: Optical3_0,
        range: pft.Range,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def insert_temporal(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: Temporal3_0,
        range: pft.Range,
    ) -> None: ...
    measurements: list[Optical3_0 | Temporal3_0]
    def measurement_at(self, index: pft.MeasIndex) -> Optical3_0 | Temporal3_0: ...
    def measurement_named(self, name: pft.Shortname) -> Optical3_0 | Temporal3_0: ...
    @property
    def par(self) -> int: ...
    def push_optical(
        self,
        name: pft.Shortname | None,
        meas: Optical3_0,
        range: pft.Range,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def push_temporal(
        self, name: pft.Shortname, meas: Temporal3_0, range: pft.Range
    ) -> None: ...
    def set_measurements_and_data_schema(
        self,
        measurements: list[Optical3_0 | Temporal3_0],
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema,
    ) -> None: ...
    def set_named_measurements_and_data_schema(
        self,
        measurements: pft.Measurements3_0,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements(
        self,
        measurements: pft.Measurements3_0,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_temporal_at(
        self,
        index: pft.MeasIndex,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def set_temporal(
        self,
        name: pft.Shortname,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def set_timestep(self, timestep: pft.Timestep) -> pft.Timestep | None: ...
    def set_trigger_threshold(self, threshold: int) -> bool: ...
    def standard_keywords(
        self, req_or_opt: pft.ReqOrOpt = "both", root_or_meas: pft.RootOrMeas = "both"
    ) -> dict[pft.NEStr, pft.NEStr]: ...
    def remove_measurement_by_index(
        self, index: pft.MeasIndex
    ) -> tuple[
        pft.Shortname | None,
        Optical3_0 | Temporal3_0,
        pft.Range,
        pft.OpticalScale3_0 | None,
    ]: ...
    def remove_measurement_by_name(
        self, name: pft.Shortname
    ) -> tuple[
        pft.MeasIndex, Optical3_0 | Temporal3_0, pft.Range, pft.OpticalScale3_0 | None
    ]: ...
    def rename_temporal(self, name: pft.Shortname) -> pft.Shortname | None: ...
    def replace_optical_at(
        self, index: pft.MeasIndex, meas: Optical3_0
    ) -> Temporal3_0 | tuple[Optical3_0, pft.OpticalScale3_0]: ...
    def replace_optical_named(
        self, name: pft.Shortname, meas: Optical3_0
    ) -> Temporal3_0 | tuple[Optical3_0, pft.OpticalScale3_0]: ...
    def replace_temporal_at(
        self, index: pft.MeasIndex, meas: Temporal3_0
    ) -> Temporal3_0 | tuple[Optical3_0, pft.OpticalScale3_0]: ...
    def replace_temporal_named(
        self, name: pft.Shortname, meas: Temporal3_0
    ) -> Temporal3_0 | tuple[Optical3_0, pft.OpticalScale3_0]: ...
    @property
    def temporal(self) -> tuple[pft.MeasIndex, pft.Shortname, Temporal3_0] | None: ...
    @property
    def timestep(self) -> pft.Timestep | None: ...
    def to_dataset(
        self,
        data: DataFrame,
        analysis: pft.AnalysisBytes = "",
        others: list[pft.OtherBytes] = [],
    ) -> CoreDataset3_0: ...
    def to_version_2_0(self, allow_loss: pft.TriFlag = "false") -> CoreTEXT2_0: ...
    def to_version_3_1(self, allow_loss: pft.TriFlag = "false") -> CoreTEXT3_1: ...
    def to_version_3_2(self, allow_loss: pft.TriFlag = "false") -> CoreTEXT3_2: ...
    @property
    def version(self) -> pft.FCSVersion: ...
    def unset_measurements(self) -> None: ...
    def unset_temporal(self) -> pft.Timestep | None: ...
    @classmethod
    def write_texts(
        cls,
        path: Path,
        datasets: list[CoreTEXT3_0],
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
    ) -> int | None: ...
    def write_text(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
        appendable: bool = False,
        append: bool = False,
    ) -> int: ...

@final
class CoreTEXT3_1:
    def __new__(
        cls,
        measurements: pft.Measurements3_1,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema,
        mode: pft.Mode = "L",
        cyt: str = "",
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        cytsn: str = "",
        spillover: pft.Spillover | None = None,
        last_modifier: str = "",
        last_modified: datetime | None = None,
        originality: pft.Originality | None = None,
        plateid: str = "",
        platename: str = "",
        wellid: str = "",
        vol: float | None = None,
        csvbits: int = 0,
        cstot: int = 0,
        csvflags: pft.CsvFlags = [],
        abrt: int | None = None,
        com: str = "",
        cells: str = "",
        exp: str = "",
        fil: str = "",
        inst: str = "",
        lost: int | None = None,
        op: str = "",
        proj: str = "",
        smno: str = "",
        src: str = "",
        sys: str = "",
        tr: pft.Trigger | None = None,
        applied_gates: pft.AppliedGates3_0 = ([], {}, None),
        pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords = {},
        nonstandard_keywords: pft.NonStdKeywords = {},
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    data_schema: (
        FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema
    )
    mode: pft.Mode
    cyt: str
    btim: time | None
    etim: time | None
    date: date | None
    cytsn: str
    spillover: pft.Spillover | None
    last_modifier: str
    last_modified: datetime | None
    originality: pft.Originality | None
    plateid: str
    platename: str
    wellid: str
    vol: float | None
    csvbits: int
    cstot: int
    csvflags: pft.CsvFlags
    abrt: int | None
    com: str
    cells: str
    exp: str
    fil: str
    inst: str
    lost: int | None
    op: str
    proj: str
    smno: str
    src: str
    sys: str
    tr: pft.Trigger | None
    applied_gates: pft.AppliedGates3_0
    pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords
    nonstandard_keywords: pft.NonStdKeywords
    all_calibrations: list[pft.Calibration3_1 | tuple[()] | None]
    all_detector_types: list[str | tuple[()]]
    all_detector_voltages: list[float | tuple[()] | None]
    all_displays: list[pft.Display | None]
    all_filters: list[str | tuple[()]]
    all_longnames: list[str]
    all_peak_bins: list[int | None]
    all_peak_sizes: list[int | None]
    all_percents_emitted: list[float | tuple[()] | None]
    all_powers: list[float | tuple[()] | None]
    all_scales: list[pft.OpticalScale3_0]
    all_shortnames: list[pft.Shortname]
    all_wavelengths: list[list[float] | tuple[()]]
    @classmethod
    def from_kws(
        cls,
        std: pft.StdKeywords,
        pnonstd: pft.PseudoNonStdKeywords,
        nonstd: pft.NonStdKeywords,
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_optical_only_keys: list[pft.OpticalOnlyKey] = [],
        process_optical_only_keys: pft.ProcessOpticalOnlyKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: pft.ProcessKeywordFailure = "error",
        process_hyper_par: pft.ProcessKeywordFailure = "error",
        process_other_version: pft.ProcessKeywordFailure = "error",
        process_extra_timestep: pft.ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        add_missing_timestep: pft.Timestep | None = None,
        spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
        text_data_correction: pft.OffsetCorrection = (0, 0),
        text_analysis_correction: pft.OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: pft.TriFlag = "false",
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        disallow_range_truncation: pft.TriFlag = "false",
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
    ) -> tuple[CoreTEXT3_1, StdTEXTDiagnostics]: ...
    def insert_optical(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: Optical3_1,
        range: pft.MaybeTypedVariableBitmask,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def insert_temporal(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: Temporal3_1,
        range: pft.MaybeTypedVariableBitmask,
    ) -> None: ...
    measurements: list[Optical3_1 | Temporal3_1]
    def measurement_at(self, index: pft.MeasIndex) -> Optical3_1 | Temporal3_1: ...
    def measurement_named(self, name: pft.Shortname) -> Optical3_1 | Temporal3_1: ...
    @property
    def par(self) -> int: ...
    def push_optical(
        self,
        name: pft.Shortname,
        meas: Optical3_1,
        range: pft.MaybeTypedVariableBitmask,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def push_temporal(
        self,
        name: pft.Shortname,
        meas: Temporal3_1,
        range: pft.MaybeTypedVariableBitmask,
    ) -> None: ...
    def set_measurements_and_data_schema(
        self,
        measurements: list[Optical3_1 | Temporal3_1],
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema,
    ) -> None: ...
    def set_named_measurements_and_data_schema(
        self,
        measurements: pft.Measurements3_1,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements(
        self,
        measurements: pft.Measurements3_1,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_temporal_at(
        self,
        index: pft.MeasIndex,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def set_temporal(
        self,
        name: pft.Shortname,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def set_timestep(self, timestep: pft.Timestep) -> pft.Timestep | None: ...
    def set_trigger_threshold(self, threshold: int) -> bool: ...
    def standard_keywords(
        self, req_or_opt: pft.ReqOrOpt = "both", root_or_meas: pft.RootOrMeas = "both"
    ) -> dict[pft.NEStr, pft.NEStr]: ...
    def remove_measurement_by_index(
        self, index: pft.MeasIndex
    ) -> tuple[
        pft.Shortname,
        Optical3_1 | Temporal3_1,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.ByteWidth | None,
    ]: ...
    def remove_measurement_by_name(
        self, name: pft.Shortname
    ) -> tuple[
        pft.MeasIndex,
        Optical3_1 | Temporal3_1,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.ByteWidth | None,
    ]: ...
    def rename_temporal(self, name: pft.Shortname) -> pft.Shortname | None: ...
    def replace_optical_at(
        self, index: pft.MeasIndex, meas: Optical3_1
    ) -> Temporal3_1 | tuple[Optical3_1, pft.OpticalScale3_0]: ...
    def replace_optical_named(
        self, name: pft.Shortname, meas: Optical3_1
    ) -> Temporal3_1 | tuple[Optical3_1, pft.OpticalScale3_0]: ...
    def replace_temporal_at(
        self, index: pft.MeasIndex, meas: Temporal3_1
    ) -> Temporal3_1 | tuple[Optical3_1, pft.OpticalScale3_0]: ...
    def replace_temporal_named(
        self, name: pft.Shortname, meas: Temporal3_1
    ) -> Temporal3_1 | tuple[Optical3_1, pft.OpticalScale3_0]: ...
    @property
    def temporal(self) -> tuple[pft.MeasIndex, pft.Shortname, Temporal3_1] | None: ...
    @property
    def timestep(self) -> pft.Timestep | None: ...
    def to_dataset(
        self,
        data: DataFrame,
        analysis: pft.AnalysisBytes = "",
        others: list[pft.OtherBytes] = [],
    ) -> CoreDataset3_1: ...
    def to_version_2_0(self, allow_loss: pft.TriFlag = "false") -> CoreTEXT2_0: ...
    def to_version_3_0(self, allow_loss: pft.TriFlag = "false") -> CoreTEXT3_0: ...
    def to_version_3_2(self, allow_loss: pft.TriFlag = "false") -> CoreTEXT3_2: ...
    @property
    def version(self) -> pft.FCSVersion: ...
    def unset_measurements(self) -> None: ...
    def unset_temporal(self) -> pft.Timestep | None: ...
    @classmethod
    def write_texts(
        cls,
        path: Path,
        datasets: list[CoreTEXT3_1],
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
    ) -> int | None: ...
    def write_text(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
        appendable: bool = False,
        append: bool = False,
    ) -> int: ...

@final
class CoreTEXT3_2:
    def __new__(
        cls,
        measurements: pft.Measurements3_2,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema
        | MixedDataSchema,
        cyt: str,
        mode: pft.Mode3_2 | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        begindatetime: datetime | None = None,
        enddatetime: datetime | None = None,
        cytsn: str = "",
        spillover: pft.Spillover | None = None,
        last_modifier: str = "",
        last_modified: datetime | None = None,
        originality: pft.Originality | None = None,
        plateid: str = "",
        platename: str = "",
        wellid: str = "",
        vol: float | None = None,
        carrierid: str = "",
        carriertype: str = "",
        locationid: str = "",
        unstainedinfo: str = "",
        unstainedcenters: pft.UnstainedCenters = {},
        flowrate: str = "",
        abrt: int | None = None,
        com: str = "",
        cells: str = "",
        exp: str = "",
        fil: str = "",
        inst: str = "",
        lost: int | None = None,
        op: str = "",
        proj: str = "",
        smno: str = "",
        src: str = "",
        sys: str = "",
        tr: pft.Trigger | None = None,
        applied_gates: pft.AppliedGates3_2 = ({}, None),
        pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords = {},
        nonstandard_keywords: pft.NonStdKeywords = {},
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    data_schema: (
        FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema
        | MixedDataSchema
    )
    cyt: str
    mode: pft.Mode3_2 | None
    btim: time | None
    etim: time | None
    date: date | None
    begindatetime: datetime | None
    enddatetime: datetime | None
    cytsn: str
    spillover: pft.Spillover | None
    last_modifier: str
    last_modified: datetime | None
    originality: pft.Originality | None
    plateid: str
    platename: str
    wellid: str
    vol: float | None
    carrierid: str
    carriertype: str
    locationid: str
    unstainedinfo: str
    unstainedcenters: pft.UnstainedCenters
    flowrate: str
    abrt: int | None
    com: str
    cells: str
    exp: str
    fil: str
    inst: str
    lost: int | None
    op: str
    proj: str
    smno: str
    src: str
    sys: str
    tr: pft.Trigger | None
    applied_gates: pft.AppliedGates3_2
    pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords
    nonstandard_keywords: pft.NonStdKeywords
    all_analytes: list[str | tuple[()]]
    all_awh_features: list[Literal["Area", "Width", "Height"] | tuple[()] | None]
    all_calibrations: list[pft.Calibration3_2 | tuple[()] | None]
    all_detector_names: list[str | tuple[()]]
    all_detector_types: list[str | tuple[()]]
    all_detector_voltages: list[float | tuple[()] | None]
    all_displays: list[pft.Display | None]
    all_features: list[str | tuple[()] | None]
    all_filters: list[str | tuple[()]]
    all_longnames: list[str]
    all_measurement_types: list[str | bool]
    @property
    def all_other_features(self) -> list[str | tuple[()] | None]: ...
    all_percents_emitted: list[float | tuple[()] | None]
    all_powers: list[float | tuple[()] | None]
    all_scales: list[pft.OpticalScale3_0]
    all_shortnames: list[pft.Shortname]
    all_tags: list[str | tuple[()]]
    all_wavelengths: list[list[float] | tuple[()]]
    @classmethod
    def from_kws(
        cls,
        std: pft.StdKeywords,
        pnonstd: pft.PseudoNonStdKeywords,
        nonstd: pft.NonStdKeywords,
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_optical_only_keys: list[pft.OpticalOnlyKey] = [],
        process_optical_only_keys: pft.ProcessOpticalOnlyKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: pft.ProcessKeywordFailure = "error",
        process_hyper_par: pft.ProcessKeywordFailure = "error",
        process_other_version: pft.ProcessKeywordFailure = "error",
        process_extra_timestep: pft.ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        add_missing_timestep: pft.Timestep | None = None,
        spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
        disallow_localtime: bool = False,
        text_data_correction: pft.OffsetCorrection = (0, 0),
        text_analysis_correction: pft.OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: pft.TriFlag = "false",
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        disallow_range_truncation: pft.TriFlag = "false",
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
    ) -> tuple[CoreTEXT3_2, StdTEXTDiagnostics]: ...
    def insert_optical(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: Optical3_2,
        range: pft.MaybeTypedMixedRange,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def insert_temporal(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: Temporal3_2,
        range: pft.MaybeTypedMixedRange,
    ) -> None: ...
    measurements: list[Optical3_2 | Temporal3_2]
    def measurement_at(self, index: pft.MeasIndex) -> Optical3_2 | Temporal3_2: ...
    def measurement_named(self, name: pft.Shortname) -> Optical3_2 | Temporal3_2: ...
    @property
    def par(self) -> int: ...
    def push_optical(
        self,
        name: pft.Shortname,
        meas: Optical3_2,
        range: pft.MaybeTypedMixedRange,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def push_temporal(
        self, name: pft.Shortname, meas: Temporal3_2, range: pft.MaybeTypedMixedRange
    ) -> None: ...
    def set_measurements_and_data_schema(
        self,
        measurements: list[Optical3_2 | Temporal3_2],
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema
        | MixedDataSchema,
    ) -> None: ...
    def set_named_measurements_and_data_schema(
        self,
        measurements: pft.Measurements3_2,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema
        | MixedDataSchema,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements(
        self,
        measurements: pft.Measurements3_2,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_temporal_at(
        self,
        index: pft.MeasIndex,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def set_temporal(
        self,
        name: pft.Shortname,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def set_timestep(self, timestep: pft.Timestep) -> pft.Timestep | None: ...
    def set_trigger_threshold(self, threshold: int) -> bool: ...
    def standard_keywords(
        self, req_or_opt: pft.ReqOrOpt = "both", root_or_meas: pft.RootOrMeas = "both"
    ) -> dict[pft.NEStr, pft.NEStr]: ...
    def remove_measurement_by_index(
        self, index: pft.MeasIndex
    ) -> tuple[
        pft.Shortname,
        Optical3_2 | Temporal3_2,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.AnyType | None,
    ]: ...
    def remove_measurement_by_name(
        self, name: pft.Shortname
    ) -> tuple[
        pft.MeasIndex,
        Optical3_2 | Temporal3_2,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.AnyType | None,
    ]: ...
    def rename_temporal(self, name: pft.Shortname) -> pft.Shortname | None: ...
    def replace_optical_at(
        self, index: pft.MeasIndex, meas: Optical3_2
    ) -> Temporal3_2 | tuple[Optical3_2, pft.OpticalScale3_0]: ...
    def replace_optical_named(
        self, name: pft.Shortname, meas: Optical3_2
    ) -> Temporal3_2 | tuple[Optical3_2, pft.OpticalScale3_0]: ...
    def replace_temporal_at(
        self, index: pft.MeasIndex, meas: Temporal3_2, allow_loss: pft.TriFlag = "false"
    ) -> Temporal3_2 | tuple[Optical3_2, pft.OpticalScale3_0]: ...
    def replace_temporal_named(
        self, name: pft.Shortname, meas: Temporal3_2, allow_loss: pft.TriFlag = "false"
    ) -> Temporal3_2 | tuple[Optical3_2, pft.OpticalScale3_0]: ...
    @property
    def temporal(self) -> tuple[pft.MeasIndex, pft.Shortname, Temporal3_2] | None: ...
    @property
    def timestep(self) -> pft.Timestep | None: ...
    def to_dataset(
        self,
        data: DataFrame,
        analysis: pft.AnalysisBytes = "",
        others: list[pft.OtherBytes] = [],
    ) -> CoreDataset3_2: ...
    def to_version_2_0(self, allow_loss: pft.TriFlag = "false") -> CoreTEXT2_0: ...
    def to_version_3_0(self, allow_loss: pft.TriFlag = "false") -> CoreTEXT3_0: ...
    def to_version_3_1(self, allow_loss: pft.TriFlag = "false") -> CoreTEXT3_1: ...
    @property
    def version(self) -> pft.FCSVersion: ...
    def unset_measurements(self) -> None: ...
    def unset_temporal(
        self, allow_loss: pft.TriFlag = "false"
    ) -> pft.Timestep | None: ...
    @classmethod
    def write_texts(
        cls,
        path: Path,
        datasets: list[CoreTEXT3_2],
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
    ) -> int | None: ...
    def write_text(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
        appendable: bool = False,
        append: bool = False,
    ) -> int: ...

@final
class CoreDataset2_0:
    def __new__(
        cls,
        measurements: pft.Measurements2_0,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema,
        data: DataFrame,
        mode: pft.Mode = "L",
        cyt: str = "",
        comp: pft.Compensation | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        abrt: int | None = None,
        com: str = "",
        cells: str = "",
        exp: str = "",
        fil: str = "",
        inst: str = "",
        lost: int | None = None,
        op: str = "",
        proj: str = "",
        smno: str = "",
        src: str = "",
        sys: str = "",
        tr: pft.Trigger | None = None,
        applied_gates: pft.AppliedGates2_0 = ([], {}, None),
        pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords = {},
        nonstandard_keywords: pft.NonStdKeywords = {},
        analysis: pft.AnalysisBytes = "",
        others: list[pft.OtherBytes] = [],
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    data_schema: (
        FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema
    )
    data: DataFrame
    mode: pft.Mode
    cyt: str
    comp: pft.Compensation | None
    btim: time | None
    etim: time | None
    date: date | None
    abrt: int | None
    com: str
    cells: str
    exp: str
    fil: str
    inst: str
    lost: int | None
    op: str
    proj: str
    smno: str
    src: str
    sys: str
    tr: pft.Trigger | None
    applied_gates: pft.AppliedGates2_0
    pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords
    nonstandard_keywords: pft.NonStdKeywords
    analysis: pft.AnalysisBytes
    others: list[pft.OtherBytes]
    all_detector_types: list[str | tuple[()]]
    all_detector_voltages: list[float | tuple[()] | None]
    all_filters: list[str | tuple[()]]
    all_longnames: list[str]
    all_peak_bins: list[int | None]
    all_peak_sizes: list[int | None]
    all_percents_emitted: list[float | tuple[()] | None]
    all_powers: list[float | tuple[()] | None]
    all_scales: list[pft.OpticalScale2_0 | None]
    all_shortnames_maybe: list[pft.Shortname | None]
    all_shortnames: list[pft.Shortname]
    all_wavelengths: list[float | tuple[()] | None]
    def check_ranges(
        self,
        over_bitmask_action: pft.OverLimitAction = "trunc_warn",
        over_range_action: pft.OverLimitAction = "warn",
    ) -> list[int | None]: ...
    @classmethod
    def from_kws(
        cls,
        path: Path,
        header: HeaderAndSuppOffsets,
        std: pft.StdKeywords,
        pnonstd: pft.PseudoNonStdKeywords,
        nonstd: pft.NonStdKeywords,
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_optical_only_keys: list[pft.OpticalOnlyKey] = [],
        process_optical_only_keys: pft.ProcessOpticalOnlyKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: pft.ProcessKeywordFailure = "error",
        process_hyper_par: pft.ProcessKeywordFailure = "error",
        process_other_version: pft.ProcessKeywordFailure = "error",
        process_extra_timestep: pft.ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        int_width_override: pft.IntWidthOverride = "never",
        byteord_override: pft.ByteordOverride = "none",
        disallow_range_truncation: pft.TriFlag = "false",
        data_remainder_limit: int = 0,
        allow_uneven_event_width: pft.TriFlag = "false",
        allow_tot_mismatch: pft.TriFlag = "false",
        over_bitmask_action: pft.OverLimitAction = "trunc_warn",
        over_range_action: pft.OverLimitAction = "warn",
        read_intra_segment_dark_bytes: bool = False,
        read_post_dataset_dark_bytes: bool = False,
        row_buffer_size: int = 28000,
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
        dataset_offset: int = 0,
        dataset_len: int | None = None,
        mmap: bool = False,
    ) -> tuple[CoreDataset2_0, NewStdDatasetFromKwsOutput]: ...
    def insert_optical(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname | None,
        meas: Optical2_0,
        range: pft.Range,
        col: Series,
        scale: pft.OpticalScale2_0 | None = (),
    ) -> None: ...
    def insert_temporal(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: Temporal2_0,
        range: pft.Range,
        col: Series,
    ) -> None: ...
    measurements: list[Optical2_0 | Temporal2_0]
    def measurement_at(self, index: pft.MeasIndex) -> Optical2_0 | Temporal2_0: ...
    def measurement_named(self, name: pft.Shortname) -> Optical2_0 | Temporal2_0: ...
    @property
    def par(self) -> int: ...
    def push_optical(
        self,
        name: pft.Shortname | None,
        meas: Optical2_0,
        range: pft.Range,
        col: Series,
        scale: pft.OpticalScale2_0 | None = (),
    ) -> None: ...
    def push_temporal(
        self, name: pft.Shortname, meas: Temporal2_0, range: pft.Range, col: Series
    ) -> None: ...
    def set_measurements_and_data(
        self, measurements: list[Optical2_0 | Temporal2_0], data: DataFrame
    ) -> None: ...
    def set_measurements_and_data_schema(
        self,
        measurements: list[Optical2_0 | Temporal2_0],
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema,
    ) -> None: ...
    def set_measurements_data_schema_and_data(
        self,
        measurements: list[Optical2_0 | Temporal2_0],
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema,
        data: DataFrame,
    ) -> None: ...
    def set_named_measurements_and_data(
        self,
        measurements: pft.Measurements2_0,
        data: DataFrame,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements_and_data_schema(
        self,
        measurements: pft.Measurements2_0,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements(
        self,
        measurements: pft.Measurements2_0,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_temporal_at(
        self, index: pft.MeasIndex, allow_loss: pft.TriFlag = "false"
    ) -> bool: ...
    def set_temporal(
        self, name: pft.Shortname, allow_loss: pft.TriFlag = "false"
    ) -> bool: ...
    def set_trigger_threshold(self, threshold: int) -> bool: ...
    def standard_keywords(
        self, req_or_opt: pft.ReqOrOpt = "both", root_or_meas: pft.RootOrMeas = "both"
    ) -> dict[pft.NEStr, pft.NEStr]: ...
    def remove_measurement_by_index(
        self, index: pft.MeasIndex
    ) -> tuple[
        pft.Shortname | None,
        Optical2_0 | Temporal2_0,
        Series,
        pft.Range,
        pft.OpticalScale2_0 | None,
    ]: ...
    def remove_measurement_by_name(
        self, name: pft.Shortname
    ) -> tuple[
        pft.MeasIndex,
        Optical2_0 | Temporal2_0,
        Series,
        pft.Range,
        pft.OpticalScale2_0 | None,
    ]: ...
    def rename_temporal(self, name: pft.Shortname) -> pft.Shortname | None: ...
    def replace_optical_at(
        self, index: pft.MeasIndex, meas: Optical2_0
    ) -> Temporal2_0 | tuple[Optical2_0, pft.OpticalScale2_0 | None]: ...
    def replace_optical_named(
        self, name: pft.Shortname, meas: Optical2_0
    ) -> Temporal2_0 | tuple[Optical2_0, pft.OpticalScale2_0 | None]: ...
    def replace_temporal_at(
        self, index: pft.MeasIndex, meas: Temporal2_0
    ) -> Temporal2_0 | tuple[Optical2_0, pft.OpticalScale2_0 | None]: ...
    def replace_temporal_named(
        self, name: pft.Shortname, meas: Temporal2_0
    ) -> Temporal2_0 | tuple[Optical2_0, pft.OpticalScale2_0 | None]: ...
    @property
    def temporal(self) -> tuple[pft.MeasIndex, pft.Shortname, Temporal2_0] | None: ...
    def to_text(self) -> CoreTEXT2_0: ...
    def to_version_3_0(self, allow_loss: pft.TriFlag = "false") -> CoreDataset3_0: ...
    def to_version_3_1(self, allow_loss: pft.TriFlag = "false") -> CoreDataset3_1: ...
    def to_version_3_2(self, allow_loss: pft.TriFlag = "false") -> CoreDataset3_2: ...
    @property
    def version(self) -> pft.FCSVersion: ...
    def unset_data(self) -> None: ...
    def unset_temporal(self) -> bool: ...
    def write_text(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
        appendable: bool = False,
        append: bool = False,
    ) -> int: ...
    def write_dataset(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
        allow_over_bitmask: pft.TriFlag = "false",
        disallow_over_range: pft.TriFlag = "false",
        row_buffer_size: int = 28000,
        appendable: bool = False,
        append: bool = False,
    ) -> int: ...

@final
class CoreDataset3_0:
    def __new__(
        cls,
        measurements: pft.Measurements3_0,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema,
        data: DataFrame,
        mode: pft.Mode = "L",
        cyt: str = "",
        comp: pft.Compensation | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        cytsn: str = "",
        unicode: pft.Unicode | None = None,
        csvbits: int = 0,
        cstot: int = 0,
        csvflags: pft.CsvFlags = [],
        abrt: int | None = None,
        com: str = "",
        cells: str = "",
        exp: str = "",
        fil: str = "",
        inst: str = "",
        lost: int | None = None,
        op: str = "",
        proj: str = "",
        smno: str = "",
        src: str = "",
        sys: str = "",
        tr: pft.Trigger | None = None,
        applied_gates: pft.AppliedGates3_0 = ([], {}, None),
        pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords = {},
        nonstandard_keywords: pft.NonStdKeywords = {},
        analysis: pft.AnalysisBytes = "",
        others: list[pft.OtherBytes] = [],
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    data_schema: (
        FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema
    )
    data: DataFrame
    mode: pft.Mode
    cyt: str
    comp: pft.Compensation | None
    btim: time | None
    etim: time | None
    date: date | None
    cytsn: str
    unicode: pft.Unicode | None
    csvbits: int
    cstot: int
    csvflags: pft.CsvFlags
    abrt: int | None
    com: str
    cells: str
    exp: str
    fil: str
    inst: str
    lost: int | None
    op: str
    proj: str
    smno: str
    src: str
    sys: str
    tr: pft.Trigger | None
    applied_gates: pft.AppliedGates3_0
    pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords
    nonstandard_keywords: pft.NonStdKeywords
    analysis: pft.AnalysisBytes
    others: list[pft.OtherBytes]
    all_detector_types: list[str | tuple[()]]
    all_detector_voltages: list[float | tuple[()] | None]
    all_filters: list[str | tuple[()]]
    all_longnames: list[str]
    all_peak_bins: list[int | None]
    all_peak_sizes: list[int | None]
    all_percents_emitted: list[float | tuple[()] | None]
    all_powers: list[float | tuple[()] | None]
    all_scales: list[pft.OpticalScale3_0]
    all_shortnames_maybe: list[pft.Shortname | None]
    all_shortnames: list[pft.Shortname]
    all_wavelengths: list[float | tuple[()] | None]
    def check_ranges(
        self,
        over_bitmask_action: pft.OverLimitAction = "trunc_warn",
        over_range_action: pft.OverLimitAction = "warn",
    ) -> list[int | None]: ...
    @classmethod
    def from_kws(
        cls,
        path: Path,
        header: HeaderAndSuppOffsets,
        std: pft.StdKeywords,
        pnonstd: pft.PseudoNonStdKeywords,
        nonstd: pft.NonStdKeywords,
        allow_pseudoempty: bool = False,
        dataset_overflow_limit: int = 0,
        overlap_correction_limit: int = 0,
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_optical_only_keys: list[pft.OpticalOnlyKey] = [],
        process_optical_only_keys: pft.ProcessOpticalOnlyKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: pft.ProcessKeywordFailure = "error",
        process_hyper_par: pft.ProcessKeywordFailure = "error",
        process_other_version: pft.ProcessKeywordFailure = "error",
        process_extra_timestep: pft.ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        add_missing_timestep: pft.Timestep | None = None,
        text_data_correction: pft.OffsetCorrection = (0, 0),
        text_analysis_correction: pft.OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: pft.TriFlag = "false",
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        int_width_override: pft.IntWidthOverride = "never",
        byteord_override: pft.ByteordOverride = "none",
        disallow_range_truncation: pft.TriFlag = "false",
        data_remainder_limit: int = 0,
        allow_uneven_event_width: pft.TriFlag = "false",
        allow_tot_mismatch: pft.TriFlag = "false",
        over_bitmask_action: pft.OverLimitAction = "trunc_warn",
        over_range_action: pft.OverLimitAction = "warn",
        allow_missing_crc: pft.TriFlag = "false",
        allow_mismatch_crc: pft.TriFlag = "false",
        compute_crc: pft.ComputeCRC = "never",
        read_intra_segment_dark_bytes: bool = False,
        read_post_dataset_dark_bytes: bool = False,
        row_buffer_size: int = 28000,
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
        dataset_offset: int = 0,
        dataset_len: int | None = None,
        mmap: bool = False,
    ) -> tuple[CoreDataset3_0, NewStdDatasetFromKwsOutput]: ...
    def insert_optical(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname | None,
        meas: Optical3_0,
        range: pft.Range,
        col: Series,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def insert_temporal(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: Temporal3_0,
        range: pft.Range,
        col: Series,
    ) -> None: ...
    measurements: list[Optical3_0 | Temporal3_0]
    def measurement_at(self, index: pft.MeasIndex) -> Optical3_0 | Temporal3_0: ...
    def measurement_named(self, name: pft.Shortname) -> Optical3_0 | Temporal3_0: ...
    @property
    def par(self) -> int: ...
    def push_optical(
        self,
        name: pft.Shortname | None,
        meas: Optical3_0,
        range: pft.Range,
        col: Series,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def push_temporal(
        self, name: pft.Shortname, meas: Temporal3_0, range: pft.Range, col: Series
    ) -> None: ...
    def set_measurements_and_data(
        self, measurements: list[Optical3_0 | Temporal3_0], data: DataFrame
    ) -> None: ...
    def set_measurements_and_data_schema(
        self,
        measurements: list[Optical3_0 | Temporal3_0],
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema,
    ) -> None: ...
    def set_measurements_data_schema_and_data(
        self,
        measurements: list[Optical3_0 | Temporal3_0],
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema,
        data: DataFrame,
    ) -> None: ...
    def set_named_measurements_and_data(
        self,
        measurements: pft.Measurements3_0,
        data: DataFrame,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements_and_data_schema(
        self,
        measurements: pft.Measurements3_0,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | OrderedUintDataSchema
        | OrderedF32DataSchema
        | OrderedF64DataSchema,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements(
        self,
        measurements: pft.Measurements3_0,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_temporal_at(
        self,
        index: pft.MeasIndex,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def set_temporal(
        self,
        name: pft.Shortname,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def set_timestep(self, timestep: pft.Timestep) -> pft.Timestep | None: ...
    def set_trigger_threshold(self, threshold: int) -> bool: ...
    def standard_keywords(
        self, req_or_opt: pft.ReqOrOpt = "both", root_or_meas: pft.RootOrMeas = "both"
    ) -> dict[pft.NEStr, pft.NEStr]: ...
    def remove_measurement_by_index(
        self, index: pft.MeasIndex
    ) -> tuple[
        pft.Shortname | None,
        Optical3_0 | Temporal3_0,
        Series,
        pft.Range,
        pft.OpticalScale3_0 | None,
    ]: ...
    def remove_measurement_by_name(
        self, name: pft.Shortname
    ) -> tuple[
        pft.MeasIndex,
        Optical3_0 | Temporal3_0,
        Series,
        pft.Range,
        pft.OpticalScale3_0 | None,
    ]: ...
    def rename_temporal(self, name: pft.Shortname) -> pft.Shortname | None: ...
    def replace_optical_at(
        self, index: pft.MeasIndex, meas: Optical3_0
    ) -> Temporal3_0 | tuple[Optical3_0, pft.OpticalScale3_0]: ...
    def replace_optical_named(
        self, name: pft.Shortname, meas: Optical3_0
    ) -> Temporal3_0 | tuple[Optical3_0, pft.OpticalScale3_0]: ...
    def replace_temporal_at(
        self, index: pft.MeasIndex, meas: Temporal3_0
    ) -> Temporal3_0 | tuple[Optical3_0, pft.OpticalScale3_0]: ...
    def replace_temporal_named(
        self, name: pft.Shortname, meas: Temporal3_0
    ) -> Temporal3_0 | tuple[Optical3_0, pft.OpticalScale3_0]: ...
    @property
    def temporal(self) -> tuple[pft.MeasIndex, pft.Shortname, Temporal3_0] | None: ...
    @property
    def timestep(self) -> pft.Timestep | None: ...
    def to_text(self) -> CoreTEXT3_0: ...
    def to_version_2_0(self, allow_loss: pft.TriFlag = "false") -> CoreDataset2_0: ...
    def to_version_3_1(self, allow_loss: pft.TriFlag = "false") -> CoreDataset3_1: ...
    def to_version_3_2(self, allow_loss: pft.TriFlag = "false") -> CoreDataset3_2: ...
    @property
    def version(self) -> pft.FCSVersion: ...
    def unset_data(self) -> None: ...
    def unset_temporal(self) -> pft.Timestep | None: ...
    def write_text(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
        appendable: bool = False,
        append: bool = False,
    ) -> int: ...
    def write_dataset(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
        allow_over_bitmask: pft.TriFlag = "false",
        disallow_over_range: pft.TriFlag = "false",
        row_buffer_size: int = 28000,
        appendable: bool = False,
        append: bool = False,
    ) -> int: ...

@final
class CoreDataset3_1:
    def __new__(
        cls,
        measurements: pft.Measurements3_1,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema,
        data: DataFrame,
        mode: pft.Mode = "L",
        cyt: str = "",
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        cytsn: str = "",
        spillover: pft.Spillover | None = None,
        last_modifier: str = "",
        last_modified: datetime | None = None,
        originality: pft.Originality | None = None,
        plateid: str = "",
        platename: str = "",
        wellid: str = "",
        vol: float | None = None,
        csvbits: int = 0,
        cstot: int = 0,
        csvflags: pft.CsvFlags = [],
        abrt: int | None = None,
        com: str = "",
        cells: str = "",
        exp: str = "",
        fil: str = "",
        inst: str = "",
        lost: int | None = None,
        op: str = "",
        proj: str = "",
        smno: str = "",
        src: str = "",
        sys: str = "",
        tr: pft.Trigger | None = None,
        applied_gates: pft.AppliedGates3_0 = ([], {}, None),
        pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords = {},
        nonstandard_keywords: pft.NonStdKeywords = {},
        analysis: pft.AnalysisBytes = "",
        others: list[pft.OtherBytes] = [],
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    data_schema: (
        FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema
    )
    data: DataFrame
    mode: pft.Mode
    cyt: str
    btim: time | None
    etim: time | None
    date: date | None
    cytsn: str
    spillover: pft.Spillover | None
    last_modifier: str
    last_modified: datetime | None
    originality: pft.Originality | None
    plateid: str
    platename: str
    wellid: str
    vol: float | None
    csvbits: int
    cstot: int
    csvflags: pft.CsvFlags
    abrt: int | None
    com: str
    cells: str
    exp: str
    fil: str
    inst: str
    lost: int | None
    op: str
    proj: str
    smno: str
    src: str
    sys: str
    tr: pft.Trigger | None
    applied_gates: pft.AppliedGates3_0
    pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords
    nonstandard_keywords: pft.NonStdKeywords
    analysis: pft.AnalysisBytes
    others: list[pft.OtherBytes]
    all_calibrations: list[pft.Calibration3_1 | tuple[()] | None]
    all_detector_types: list[str | tuple[()]]
    all_detector_voltages: list[float | tuple[()] | None]
    all_displays: list[pft.Display | None]
    all_filters: list[str | tuple[()]]
    all_longnames: list[str]
    all_peak_bins: list[int | None]
    all_peak_sizes: list[int | None]
    all_percents_emitted: list[float | tuple[()] | None]
    all_powers: list[float | tuple[()] | None]
    all_scales: list[pft.OpticalScale3_0]
    all_shortnames: list[pft.Shortname]
    all_wavelengths: list[list[float] | tuple[()]]
    def check_ranges(
        self,
        over_bitmask_action: pft.OverLimitAction = "trunc_warn",
        over_range_action: pft.OverLimitAction = "warn",
    ) -> list[int | None]: ...
    @classmethod
    def from_kws(
        cls,
        path: Path,
        header: HeaderAndSuppOffsets,
        std: pft.StdKeywords,
        pnonstd: pft.PseudoNonStdKeywords,
        nonstd: pft.NonStdKeywords,
        allow_pseudoempty: bool = False,
        dataset_overflow_limit: int = 0,
        overlap_correction_limit: int = 0,
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_optical_only_keys: list[pft.OpticalOnlyKey] = [],
        process_optical_only_keys: pft.ProcessOpticalOnlyKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: pft.ProcessKeywordFailure = "error",
        process_hyper_par: pft.ProcessKeywordFailure = "error",
        process_other_version: pft.ProcessKeywordFailure = "error",
        process_extra_timestep: pft.ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        add_missing_timestep: pft.Timestep | None = None,
        spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
        text_data_correction: pft.OffsetCorrection = (0, 0),
        text_analysis_correction: pft.OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: pft.TriFlag = "false",
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        disallow_range_truncation: pft.TriFlag = "false",
        data_remainder_limit: int = 0,
        allow_uneven_event_width: pft.TriFlag = "false",
        allow_tot_mismatch: pft.TriFlag = "false",
        over_bitmask_action: pft.OverLimitAction = "trunc_warn",
        over_range_action: pft.OverLimitAction = "warn",
        allow_missing_crc: pft.TriFlag = "false",
        allow_mismatch_crc: pft.TriFlag = "false",
        compute_crc: pft.ComputeCRC = "never",
        read_intra_segment_dark_bytes: bool = False,
        read_post_dataset_dark_bytes: bool = False,
        row_buffer_size: int = 28000,
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
        dataset_offset: int = 0,
        dataset_len: int | None = None,
        mmap: bool = False,
    ) -> tuple[CoreDataset3_1, NewStdDatasetFromKwsOutput]: ...
    def insert_optical(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: Optical3_1,
        range: pft.MaybeTypedVariableBitmask,
        col: Series,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def insert_temporal(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: Temporal3_1,
        range: pft.MaybeTypedVariableBitmask,
        col: Series,
    ) -> None: ...
    measurements: list[Optical3_1 | Temporal3_1]
    def measurement_at(self, index: pft.MeasIndex) -> Optical3_1 | Temporal3_1: ...
    def measurement_named(self, name: pft.Shortname) -> Optical3_1 | Temporal3_1: ...
    @property
    def par(self) -> int: ...
    def push_optical(
        self,
        name: pft.Shortname,
        meas: Optical3_1,
        range: pft.MaybeTypedVariableBitmask,
        col: Series,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def push_temporal(
        self,
        name: pft.Shortname,
        meas: Temporal3_1,
        range: pft.MaybeTypedVariableBitmask,
        col: Series,
    ) -> None: ...
    def set_measurements_and_data(
        self, measurements: list[Optical3_1 | Temporal3_1], data: DataFrame
    ) -> None: ...
    def set_measurements_and_data_schema(
        self,
        measurements: list[Optical3_1 | Temporal3_1],
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema,
    ) -> None: ...
    def set_measurements_data_schema_and_data(
        self,
        measurements: list[Optical3_1 | Temporal3_1],
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema,
        data: DataFrame,
    ) -> None: ...
    def set_named_measurements_and_data(
        self,
        measurements: pft.Measurements3_1,
        data: DataFrame,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements_and_data_schema(
        self,
        measurements: pft.Measurements3_1,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements(
        self,
        measurements: pft.Measurements3_1,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_temporal_at(
        self,
        index: pft.MeasIndex,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def set_temporal(
        self,
        name: pft.Shortname,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def set_timestep(self, timestep: pft.Timestep) -> pft.Timestep | None: ...
    def set_trigger_threshold(self, threshold: int) -> bool: ...
    def standard_keywords(
        self, req_or_opt: pft.ReqOrOpt = "both", root_or_meas: pft.RootOrMeas = "both"
    ) -> dict[pft.NEStr, pft.NEStr]: ...
    def remove_measurement_by_index(
        self, index: pft.MeasIndex
    ) -> tuple[
        pft.Shortname,
        Optical3_1 | Temporal3_1,
        Series,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.ByteWidth | None,
    ]: ...
    def remove_measurement_by_name(
        self, name: pft.Shortname
    ) -> tuple[
        pft.MeasIndex,
        Optical3_1 | Temporal3_1,
        Series,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.ByteWidth | None,
    ]: ...
    def rename_temporal(self, name: pft.Shortname) -> pft.Shortname | None: ...
    def replace_optical_at(
        self, index: pft.MeasIndex, meas: Optical3_1
    ) -> Temporal3_1 | tuple[Optical3_1, pft.OpticalScale3_0]: ...
    def replace_optical_named(
        self, name: pft.Shortname, meas: Optical3_1
    ) -> Temporal3_1 | tuple[Optical3_1, pft.OpticalScale3_0]: ...
    def replace_temporal_at(
        self, index: pft.MeasIndex, meas: Temporal3_1
    ) -> Temporal3_1 | tuple[Optical3_1, pft.OpticalScale3_0]: ...
    def replace_temporal_named(
        self, name: pft.Shortname, meas: Temporal3_1
    ) -> Temporal3_1 | tuple[Optical3_1, pft.OpticalScale3_0]: ...
    @property
    def temporal(self) -> tuple[pft.MeasIndex, pft.Shortname, Temporal3_1] | None: ...
    @property
    def timestep(self) -> pft.Timestep | None: ...
    def to_text(self) -> CoreTEXT3_1: ...
    def to_version_2_0(self, allow_loss: pft.TriFlag = "false") -> CoreDataset2_0: ...
    def to_version_3_0(self, allow_loss: pft.TriFlag = "false") -> CoreDataset3_0: ...
    def to_version_3_2(self, allow_loss: pft.TriFlag = "false") -> CoreDataset3_2: ...
    @property
    def version(self) -> pft.FCSVersion: ...
    def unset_data(self) -> None: ...
    def unset_temporal(self) -> pft.Timestep | None: ...
    def write_text(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
        appendable: bool = False,
        append: bool = False,
    ) -> int: ...
    def write_dataset(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
        allow_over_bitmask: pft.TriFlag = "false",
        disallow_over_range: pft.TriFlag = "false",
        row_buffer_size: int = 28000,
        appendable: bool = False,
        append: bool = False,
    ) -> int: ...

@final
class CoreDataset3_2:
    def __new__(
        cls,
        measurements: pft.Measurements3_2,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema
        | MixedDataSchema,
        data: DataFrame,
        cyt: str,
        mode: pft.Mode3_2 | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        begindatetime: datetime | None = None,
        enddatetime: datetime | None = None,
        cytsn: str = "",
        spillover: pft.Spillover | None = None,
        last_modifier: str = "",
        last_modified: datetime | None = None,
        originality: pft.Originality | None = None,
        plateid: str = "",
        platename: str = "",
        wellid: str = "",
        vol: float | None = None,
        carrierid: str = "",
        carriertype: str = "",
        locationid: str = "",
        unstainedinfo: str = "",
        unstainedcenters: pft.UnstainedCenters = {},
        flowrate: str = "",
        abrt: int | None = None,
        com: str = "",
        cells: str = "",
        exp: str = "",
        fil: str = "",
        inst: str = "",
        lost: int | None = None,
        op: str = "",
        proj: str = "",
        smno: str = "",
        src: str = "",
        sys: str = "",
        tr: pft.Trigger | None = None,
        applied_gates: pft.AppliedGates3_2 = ({}, None),
        pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords = {},
        nonstandard_keywords: pft.NonStdKeywords = {},
        analysis: pft.AnalysisBytes = "",
        others: list[pft.OtherBytes] = [],
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    data_schema: (
        FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema
        | MixedDataSchema
    )
    data: DataFrame
    cyt: str
    mode: pft.Mode3_2 | None
    btim: time | None
    etim: time | None
    date: date | None
    begindatetime: datetime | None
    enddatetime: datetime | None
    cytsn: str
    spillover: pft.Spillover | None
    last_modifier: str
    last_modified: datetime | None
    originality: pft.Originality | None
    plateid: str
    platename: str
    wellid: str
    vol: float | None
    carrierid: str
    carriertype: str
    locationid: str
    unstainedinfo: str
    unstainedcenters: pft.UnstainedCenters
    flowrate: str
    abrt: int | None
    com: str
    cells: str
    exp: str
    fil: str
    inst: str
    lost: int | None
    op: str
    proj: str
    smno: str
    src: str
    sys: str
    tr: pft.Trigger | None
    applied_gates: pft.AppliedGates3_2
    pseudo_nonstandard_keywords: pft.PseudoNonStdKeywords
    nonstandard_keywords: pft.NonStdKeywords
    analysis: pft.AnalysisBytes
    others: list[pft.OtherBytes]
    all_analytes: list[str | tuple[()]]
    all_awh_features: list[Literal["Area", "Width", "Height"] | tuple[()] | None]
    all_calibrations: list[pft.Calibration3_2 | tuple[()] | None]
    all_detector_names: list[str | tuple[()]]
    all_detector_types: list[str | tuple[()]]
    all_detector_voltages: list[float | tuple[()] | None]
    all_displays: list[pft.Display | None]
    all_features: list[str | tuple[()] | None]
    all_filters: list[str | tuple[()]]
    all_longnames: list[str]
    all_measurement_types: list[str | bool]
    @property
    def all_other_features(self) -> list[str | tuple[()] | None]: ...
    all_percents_emitted: list[float | tuple[()] | None]
    all_powers: list[float | tuple[()] | None]
    all_scales: list[pft.OpticalScale3_0]
    all_shortnames: list[pft.Shortname]
    all_tags: list[str | tuple[()]]
    all_wavelengths: list[list[float] | tuple[()]]
    def check_ranges(
        self,
        over_bitmask_action: pft.OverLimitAction = "trunc_warn",
        over_range_action: pft.OverLimitAction = "warn",
    ) -> list[int | None]: ...
    @classmethod
    def from_kws(
        cls,
        path: Path,
        header: HeaderAndSuppOffsets,
        std: pft.StdKeywords,
        pnonstd: pft.PseudoNonStdKeywords,
        nonstd: pft.NonStdKeywords,
        allow_pseudoempty: bool = False,
        dataset_overflow_limit: int = 0,
        overlap_correction_limit: int = 0,
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_optical_only_keys: list[pft.OpticalOnlyKey] = [],
        process_optical_only_keys: pft.ProcessOpticalOnlyKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: pft.ProcessKeywordFailure = "error",
        process_hyper_par: pft.ProcessKeywordFailure = "error",
        process_other_version: pft.ProcessKeywordFailure = "error",
        process_extra_timestep: pft.ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        add_missing_timestep: pft.Timestep | None = None,
        spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
        disallow_localtime: bool = False,
        text_data_correction: pft.OffsetCorrection = (0, 0),
        text_analysis_correction: pft.OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: pft.TriFlag = "false",
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        disallow_range_truncation: pft.TriFlag = "false",
        data_remainder_limit: int = 0,
        allow_uneven_event_width: pft.TriFlag = "false",
        allow_tot_mismatch: pft.TriFlag = "false",
        over_bitmask_action: pft.OverLimitAction = "trunc_warn",
        over_range_action: pft.OverLimitAction = "warn",
        allow_missing_crc: pft.TriFlag = "false",
        allow_mismatch_crc: pft.TriFlag = "false",
        compute_crc: pft.ComputeCRC = "never",
        read_intra_segment_dark_bytes: bool = False,
        read_post_dataset_dark_bytes: bool = False,
        row_buffer_size: int = 28000,
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
        dataset_offset: int = 0,
        dataset_len: int | None = None,
        mmap: bool = False,
    ) -> tuple[CoreDataset3_2, NewStdDatasetFromKwsOutput]: ...
    def insert_optical(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: Optical3_2,
        range: pft.MaybeTypedMixedRange,
        col: Series,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def insert_temporal(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: Temporal3_2,
        range: pft.MaybeTypedMixedRange,
        col: Series,
    ) -> None: ...
    measurements: list[Optical3_2 | Temporal3_2]
    def measurement_at(self, index: pft.MeasIndex) -> Optical3_2 | Temporal3_2: ...
    def measurement_named(self, name: pft.Shortname) -> Optical3_2 | Temporal3_2: ...
    @property
    def par(self) -> int: ...
    def push_optical(
        self,
        name: pft.Shortname,
        meas: Optical3_2,
        range: pft.MaybeTypedMixedRange,
        col: Series,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def push_temporal(
        self,
        name: pft.Shortname,
        meas: Temporal3_2,
        range: pft.MaybeTypedMixedRange,
        col: Series,
    ) -> None: ...
    def set_measurements_and_data(
        self, measurements: list[Optical3_2 | Temporal3_2], data: DataFrame
    ) -> None: ...
    def set_measurements_and_data_schema(
        self,
        measurements: list[Optical3_2 | Temporal3_2],
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema
        | MixedDataSchema,
    ) -> None: ...
    def set_measurements_data_schema_and_data(
        self,
        measurements: list[Optical3_2 | Temporal3_2],
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema
        | MixedDataSchema,
        data: DataFrame,
    ) -> None: ...
    def set_named_measurements_and_data(
        self,
        measurements: pft.Measurements3_2,
        data: DataFrame,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements_and_data_schema(
        self,
        measurements: pft.Measurements3_2,
        data_schema: FixedAsciiDataSchema
        | DelimAsciiDataSchema
        | VariableUintDataSchema
        | SingleUintDataSchema
        | BigLittleF32DataSchema
        | BigLittleF64DataSchema
        | MixedDataSchema,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements(
        self,
        measurements: pft.Measurements3_2,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_temporal_at(
        self,
        index: pft.MeasIndex,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def set_temporal(
        self,
        name: pft.Shortname,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def set_timestep(self, timestep: pft.Timestep) -> pft.Timestep | None: ...
    def set_trigger_threshold(self, threshold: int) -> bool: ...
    def standard_keywords(
        self, req_or_opt: pft.ReqOrOpt = "both", root_or_meas: pft.RootOrMeas = "both"
    ) -> dict[pft.NEStr, pft.NEStr]: ...
    def remove_measurement_by_index(
        self, index: pft.MeasIndex
    ) -> tuple[
        pft.Shortname,
        Optical3_2 | Temporal3_2,
        Series,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.AnyType | None,
    ]: ...
    def remove_measurement_by_name(
        self, name: pft.Shortname
    ) -> tuple[
        pft.MeasIndex,
        Optical3_2 | Temporal3_2,
        Series,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.AnyType | None,
    ]: ...
    def rename_temporal(self, name: pft.Shortname) -> pft.Shortname | None: ...
    def replace_optical_at(
        self, index: pft.MeasIndex, meas: Optical3_2
    ) -> Temporal3_2 | tuple[Optical3_2, pft.OpticalScale3_0]: ...
    def replace_optical_named(
        self, name: pft.Shortname, meas: Optical3_2
    ) -> Temporal3_2 | tuple[Optical3_2, pft.OpticalScale3_0]: ...
    def replace_temporal_at(
        self, index: pft.MeasIndex, meas: Temporal3_2, allow_loss: pft.TriFlag = "false"
    ) -> Temporal3_2 | tuple[Optical3_2, pft.OpticalScale3_0]: ...
    def replace_temporal_named(
        self, name: pft.Shortname, meas: Temporal3_2, allow_loss: pft.TriFlag = "false"
    ) -> Temporal3_2 | tuple[Optical3_2, pft.OpticalScale3_0]: ...
    @property
    def temporal(self) -> tuple[pft.MeasIndex, pft.Shortname, Temporal3_2] | None: ...
    @property
    def timestep(self) -> pft.Timestep | None: ...
    def to_text(self) -> CoreTEXT3_2: ...
    def to_version_2_0(self, allow_loss: pft.TriFlag = "false") -> CoreDataset2_0: ...
    def to_version_3_0(self, allow_loss: pft.TriFlag = "false") -> CoreDataset3_0: ...
    def to_version_3_1(self, allow_loss: pft.TriFlag = "false") -> CoreDataset3_1: ...
    @property
    def version(self) -> pft.FCSVersion: ...
    def unset_data(self) -> None: ...
    def unset_temporal(
        self, allow_loss: pft.TriFlag = "false"
    ) -> pft.Timestep | None: ...
    def write_text(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
        appendable: bool = False,
        append: bool = False,
    ) -> int: ...
    def write_dataset(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
        allow_over_bitmask: pft.TriFlag = "false",
        disallow_over_range: pft.TriFlag = "false",
        row_buffer_size: int = 28000,
        appendable: bool = False,
        append: bool = False,
    ) -> int: ...

@final
class Optical2_0:
    def __new__(
        cls,
        wavelength: float | None = None,
        bin: int | None = None,
        size: int | None = None,
        filter: str = "",
        power: float | None = None,
        detector_type: str = "",
        percent_emitted: float | None = None,
        detector_voltage: float | None = None,
        longname: str = "",
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    wavelength: float | None
    bin: int | None
    size: int | None
    filter: str
    power: float | None
    detector_type: str
    percent_emitted: float | None
    detector_voltage: float | None
    longname: str

@final
class Optical3_0:
    def __new__(
        cls,
        wavelength: float | None = None,
        bin: int | None = None,
        size: int | None = None,
        filter: str = "",
        power: float | None = None,
        detector_type: str = "",
        percent_emitted: float | None = None,
        detector_voltage: float | None = None,
        longname: str = "",
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    wavelength: float | None
    bin: int | None
    size: int | None
    filter: str
    power: float | None
    detector_type: str
    percent_emitted: float | None
    detector_voltage: float | None
    longname: str

@final
class Optical3_1:
    def __new__(
        cls,
        wavelengths: list[float] = [],
        calibration: pft.Calibration3_1 | None = None,
        display: pft.Display | None = None,
        bin: int | None = None,
        size: int | None = None,
        filter: str = "",
        power: float | None = None,
        detector_type: str = "",
        percent_emitted: float | None = None,
        detector_voltage: float | None = None,
        longname: str = "",
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    wavelengths: list[float]
    calibration: pft.Calibration3_1 | None
    display: pft.Display | None
    bin: int | None
    size: int | None
    filter: str
    power: float | None
    detector_type: str
    percent_emitted: float | None
    detector_voltage: float | None
    longname: str

@final
class Optical3_2:
    def __new__(
        cls,
        wavelengths: list[float] = [],
        calibration: pft.Calibration3_2 | None = None,
        display: pft.Display | None = None,
        analyte: str = "",
        feature: str | None = None,
        tag: str = "",
        measurement_type: str = "",
        detector_name: str = "",
        filter: str = "",
        power: float | None = None,
        detector_type: str = "",
        percent_emitted: float | None = None,
        detector_voltage: float | None = None,
        longname: str = "",
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    wavelengths: list[float]
    calibration: pft.Calibration3_2 | None
    display: pft.Display | None
    analyte: str
    feature: str | None
    tag: str
    measurement_type: str
    detector_name: str
    filter: str
    power: float | None
    detector_type: str
    percent_emitted: float | None
    detector_voltage: float | None
    longname: str
    awh_feature: Literal["Area", "Width", "Height"] | None

@final
class Temporal2_0:
    def __new__(
        cls, bin: int | None = None, size: int | None = None, longname: str = ""
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    bin: int | None
    size: int | None
    longname: str

@final
class Temporal3_0:
    def __new__(
        cls,
        timestep: pft.Timestep,
        bin: int | None = None,
        size: int | None = None,
        longname: str = "",
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    timestep: pft.Timestep
    bin: int | None
    size: int | None
    longname: str

@final
class Temporal3_1:
    def __new__(
        cls,
        timestep: pft.Timestep,
        display: pft.Display | None = None,
        bin: int | None = None,
        size: int | None = None,
        longname: str = "",
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    timestep: pft.Timestep
    display: pft.Display | None
    bin: int | None
    size: int | None
    longname: str

@final
class Temporal3_2:
    def __new__(
        cls,
        timestep: pft.Timestep,
        display: pft.Display | None = None,
        has_type: bool = False,
        longname: str = "",
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    timestep: pft.Timestep
    display: pft.Display | None
    has_type: bool
    longname: str

@final
class FixedAsciiDataSchema(pft.AsciiDataSchema):
    def __new__(cls, ranges: list[pft.IntRange]) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def ranges(self) -> list[pft.IntRange]: ...
    @property
    def datatype(self) -> pft.Datatype: ...
    @property
    def char_widths(self) -> list[int]: ...

@final
class DelimAsciiDataSchema(pft.AsciiDataSchema):
    def __new__(cls, ranges: list[pft.IntRange]) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def ranges(self) -> list[pft.IntRange]: ...
    @property
    def datatype(self) -> pft.Datatype: ...

@final
class OrderedUintDataSchema(pft.OrderedDataSchema):
    def __new__(
        cls,
        ranges: list[pft.IntRange],
        byte_width: pft.ByteWidth = 4,
        byteord: pft.ByteOrd = "little",
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def ranges(self) -> list[pft.IntRange]: ...
    @property
    def byte_width(self) -> pft.ByteWidth: ...
    @property
    def byteord(self) -> pft.ByteOrd: ...
    @property
    def datatype(self) -> pft.Datatype: ...
    @property
    def is_float(self) -> bool: ...

@final
class OrderedF32DataSchema(pft.OrderedDataSchema):
    def __new__(
        cls, ranges: list[pft.FloatRange], byteord: pft.ByteOrd = "little"
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def ranges(self) -> list[pft.FloatRange]: ...
    @property
    def byteord(self) -> pft.ByteOrd: ...
    @property
    def datatype(self) -> pft.Datatype: ...
    @property
    def is_float(self) -> bool: ...
    @property
    def byte_width(self) -> pft.ByteWidth: ...

@final
class OrderedF64DataSchema(pft.OrderedDataSchema):
    def __new__(
        cls, ranges: list[pft.FloatRange], byteord: pft.ByteOrd = "little"
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def ranges(self) -> list[pft.FloatRange]: ...
    @property
    def byteord(self) -> pft.ByteOrd: ...
    @property
    def datatype(self) -> pft.Datatype: ...
    @property
    def is_float(self) -> bool: ...
    @property
    def byte_width(self) -> pft.ByteWidth: ...

@final
class SingleUintDataSchema(pft.BigLittleDataSchema, pft.MatrixDataSchema):
    def __new__(
        cls,
        ranges: list[pft.IntRange],
        byte_width: pft.ByteWidth = 4,
        endian: pft.Endian = "little",
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def ranges(self) -> list[pft.IntRange]: ...
    @property
    def byte_width(self) -> pft.ByteWidth: ...
    @property
    def endian(self) -> pft.Endian: ...
    @property
    def datatype(self) -> pft.Datatype: ...
    @property
    def is_float(self) -> bool: ...

@final
class BigLittleF32DataSchema(pft.BigLittleDataSchema):
    def __new__(
        cls, ranges: list[pft.FloatRange], endian: pft.Endian = "little"
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def ranges(self) -> list[pft.FloatRange]: ...
    @property
    def endian(self) -> pft.Endian: ...
    @property
    def datatype(self) -> pft.Datatype: ...
    @property
    def is_float(self) -> bool: ...
    @property
    def byte_width(self) -> pft.ByteWidth: ...

@final
class BigLittleF64DataSchema(pft.BigLittleDataSchema):
    def __new__(
        cls, ranges: list[pft.FloatRange], endian: pft.Endian = "little"
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def ranges(self) -> list[pft.FloatRange]: ...
    @property
    def endian(self) -> pft.Endian: ...
    @property
    def datatype(self) -> pft.Datatype: ...
    @property
    def is_float(self) -> bool: ...
    @property
    def byte_width(self) -> pft.ByteWidth: ...

@final
class VariableUintDataSchema(pft.BigLittleDataSchema, pft.NumericDataSchema):
    def __new__(
        cls, ranges: list[pft.VariableBitmask], endian: pft.Endian = "little"
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def ranges(self) -> list[pft.VariableBitmask]: ...
    @property
    def endian(self) -> pft.Endian: ...
    @property
    def is_float(self) -> bool: ...
    @property
    def datatype(self) -> pft.Datatype: ...
    @property
    def byte_widths(self) -> list[pft.ByteWidth]: ...

@final
class MixedDataSchema(pft.BigLittleDataSchema):
    def __new__(
        cls, typed_ranges: list[pft.MixedRange], endian: pft.Endian = "little"
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def typed_ranges(self) -> list[pft.MixedRange]: ...
    @property
    def endian(self) -> pft.Endian: ...
    @property
    def byte_widths(self) -> list[int]: ...

@final
class UnivariateRegion2_0:
    def __new__(cls, index: int, gate: tuple[Decimal, Decimal]) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def index(self) -> int: ...
    @property
    def gate(self) -> tuple[Decimal, Decimal]: ...

@final
class UnivariateRegion3_0:
    def __new__(
        cls, index: int, gate: tuple[Decimal, Decimal], is_gate: bool = False
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def index(self) -> int: ...
    @property
    def gate(self) -> tuple[Decimal, Decimal]: ...
    @property
    def is_gate(self) -> bool: ...

@final
class UnivariateRegion3_2:
    def __new__(cls, index: int, gate: tuple[Decimal, Decimal]) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def index(self) -> int: ...
    @property
    def gate(self) -> tuple[Decimal, Decimal]: ...

@final
class BivariateRegion2_0:
    def __new__(
        cls, index: tuple[int, int], vertices: list[tuple[Decimal, Decimal]]
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def index(self) -> tuple[int, int]: ...
    @property
    def vertices(self) -> list[tuple[Decimal, Decimal]]: ...

@final
class BivariateRegion3_0:
    def __new__(
        cls,
        x_index: int,
        y_index: int,
        vertices: list[tuple[Decimal, Decimal]],
        x_is_gate: bool = False,
        y_is_gate: bool = False,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def x_index(self) -> int: ...
    @property
    def y_index(self) -> int: ...
    @property
    def vertices(self) -> list[tuple[Decimal, Decimal]]: ...
    @property
    def x_is_gate(self) -> bool: ...
    @property
    def y_is_gate(self) -> bool: ...

@final
class BivariateRegion3_2:
    def __new__(
        cls, index: tuple[int, int], vertices: list[tuple[Decimal, Decimal]]
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def index(self) -> tuple[int, int]: ...
    @property
    def vertices(self) -> list[tuple[Decimal, Decimal]]: ...

@final
class GatedMeasurement:
    def __new__(
        cls,
        scale: tuple[()] | tuple[float, float] | None = None,
        filter: str = "",
        shortname: pft.Shortname | None = None,
        percent_emitted: float | None = None,
        range: float | None = None,
        longname: str = "",
        detector_type: str = "",
        detector_voltage: float | None = None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    scale: tuple[()] | tuple[float, float] | None
    filter: str
    shortname: pft.Shortname | None
    percent_emitted: float | None
    range: float | None
    longname: str
    detector_type: str
    detector_voltage: float | None

#
def fcs_read_header(
    path: Path,
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    dataset_offset: int = 0,
) -> Header: ...

#
def fcs_read_flat_text(
    path: Path,
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    process_supp_text: pft.ProcessSuppText = "asis",
    delim_escape_mode: pft.DelimEscapeMode = "escaped",
    allow_non_ascii_delim: pft.TriFlag = "false",
    allow_nonunique: pft.TriFlag = "false",
    allow_even_delims: pft.TriFlag = "false",
    allow_odd_tokens: pft.TriFlag = "false",
    allow_empty_keys: pft.TriFlag = "false",
    allow_delim_at_boundary: pft.TriFlag = "false",
    use_encoding: pft.UseEncoding = "utf8",
    allow_non_ascii_keys: pft.TriFlag = "false",
    allow_non_utf8_values: pft.TriFlag = "false",
    allow_missing_supp_text: pft.TriFlag = "false",
    allow_supp_text_own_delim: pft.TriFlag = "false",
    allow_missing_nextdata: pft.TriFlag = "false",
    trim_value_whitespace: pft.TrimValueWhitespace = "notrim",
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    dataset_offset: int = 0,
    mmap: bool = False,
) -> FlatTEXTOutput: ...

#
def fcs_read_flat_texts(
    path: Path,
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    process_supp_text: pft.ProcessSuppText = "asis",
    delim_escape_mode: pft.DelimEscapeMode = "escaped",
    allow_non_ascii_delim: pft.TriFlag = "false",
    allow_nonunique: pft.TriFlag = "false",
    allow_even_delims: pft.TriFlag = "false",
    allow_odd_tokens: pft.TriFlag = "false",
    allow_empty_keys: pft.TriFlag = "false",
    allow_delim_at_boundary: pft.TriFlag = "false",
    use_encoding: pft.UseEncoding = "utf8",
    allow_non_ascii_keys: pft.TriFlag = "false",
    allow_non_utf8_values: pft.TriFlag = "false",
    allow_missing_supp_text: pft.TriFlag = "false",
    allow_supp_text_own_delim: pft.TriFlag = "false",
    allow_missing_nextdata: pft.TriFlag = "false",
    trim_value_whitespace: pft.TrimValueWhitespace = "notrim",
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    skip: int | None = None,
    limit: int | None = None,
    scan: bool = False,
    mmap: bool = False,
) -> list[FlatTEXTOutput]: ...

#
def fcs_read_std_text(
    path: Path,
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    process_supp_text: pft.ProcessSuppText = "asis",
    delim_escape_mode: pft.DelimEscapeMode = "escaped",
    allow_non_ascii_delim: pft.TriFlag = "false",
    allow_nonunique: pft.TriFlag = "false",
    allow_even_delims: pft.TriFlag = "false",
    allow_odd_tokens: pft.TriFlag = "false",
    allow_empty_keys: pft.TriFlag = "false",
    allow_delim_at_boundary: pft.TriFlag = "false",
    use_encoding: pft.UseEncoding = "utf8",
    allow_non_ascii_keys: pft.TriFlag = "false",
    allow_non_utf8_values: pft.TriFlag = "false",
    allow_missing_supp_text: pft.TriFlag = "false",
    allow_supp_text_own_delim: pft.TriFlag = "false",
    allow_missing_nextdata: pft.TriFlag = "false",
    trim_value_whitespace: pft.TrimValueWhitespace = "notrim",
    dedup_measurement_names: bool = False,
    trim_intra_value_whitespace: bool = False,
    time_meas_pattern: pft.Selector[str] = "^(TIME|Time)$",
    allow_missing_time: pft.TriFlag = "false",
    force_linear_scale: pft.ForceLinearScale = "none",
    ignore_optical_only_keys: list[pft.OpticalOnlyKey] = [],
    process_optical_only_keys: pft.ProcessOpticalOnlyKeys = "demote_warn",
    date_pattern: pft.Selector[str | None] = None,
    time_pattern: pft.Selector[str | None] = None,
    datetime_pattern: pft.Selector[str | None] = None,
    last_modified_pattern: pft.Selector[str | None] = None,
    allow_other_feature: bool = False,
    process_pseudostandard: pft.ProcessKeywordFailure = "error",
    process_hyper_par: pft.ProcessKeywordFailure = "error",
    process_other_version: pft.ProcessKeywordFailure = "error",
    process_extra_timestep: pft.ProcessKeywordFailure = "error",
    fix_log_scale_offsets: bool = False,
    add_missing_timestep: pft.Timestep | None = None,
    spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
    disallow_localtime: bool = False,
    ignore_standard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    promote_nonstandard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    demote_standard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    rename_standard_keys: pft.AppendableSelector[pft.KeyPairs] = {},
    replace_standard_key_values: pft.AppendableSelector[pft.StdKeyValues] = {},
    append_standard_keywords: pft.AppendableSelector[pft.StdKeyValues] = {},
    substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
    promote_all_nonstandard_keys: bool = False,
    allow_repair_non_unique: pft.TriFlag = "false",
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    int_width_override: pft.IntWidthOverride = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    dataset_offset: int = 0,
    mmap: bool = False,
) -> tuple[CoreTEXT2_0 | CoreTEXT3_0 | CoreTEXT3_1 | CoreTEXT3_2, StdTEXTOutput]: ...

#
def fcs_read_std_texts(
    path: Path,
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    process_supp_text: pft.ProcessSuppText = "asis",
    delim_escape_mode: pft.DelimEscapeMode = "escaped",
    allow_non_ascii_delim: pft.TriFlag = "false",
    allow_nonunique: pft.TriFlag = "false",
    allow_even_delims: pft.TriFlag = "false",
    allow_odd_tokens: pft.TriFlag = "false",
    allow_empty_keys: pft.TriFlag = "false",
    allow_delim_at_boundary: pft.TriFlag = "false",
    use_encoding: pft.UseEncoding = "utf8",
    allow_non_ascii_keys: pft.TriFlag = "false",
    allow_non_utf8_values: pft.TriFlag = "false",
    allow_missing_supp_text: pft.TriFlag = "false",
    allow_supp_text_own_delim: pft.TriFlag = "false",
    allow_missing_nextdata: pft.TriFlag = "false",
    trim_value_whitespace: pft.TrimValueWhitespace = "notrim",
    dedup_measurement_names: bool = False,
    trim_intra_value_whitespace: bool = False,
    time_meas_pattern: pft.Selector[str] = "^(TIME|Time)$",
    allow_missing_time: pft.TriFlag = "false",
    force_linear_scale: pft.ForceLinearScale = "none",
    ignore_optical_only_keys: list[pft.OpticalOnlyKey] = [],
    process_optical_only_keys: pft.ProcessOpticalOnlyKeys = "demote_warn",
    date_pattern: pft.Selector[str | None] = None,
    time_pattern: pft.Selector[str | None] = None,
    datetime_pattern: pft.Selector[str | None] = None,
    last_modified_pattern: pft.Selector[str | None] = None,
    allow_other_feature: bool = False,
    process_pseudostandard: pft.ProcessKeywordFailure = "error",
    process_hyper_par: pft.ProcessKeywordFailure = "error",
    process_other_version: pft.ProcessKeywordFailure = "error",
    process_extra_timestep: pft.ProcessKeywordFailure = "error",
    fix_log_scale_offsets: bool = False,
    add_missing_timestep: pft.Timestep | None = None,
    spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
    disallow_localtime: bool = False,
    ignore_standard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    promote_nonstandard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    demote_standard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    rename_standard_keys: pft.AppendableSelector[pft.KeyPairs] = {},
    replace_standard_key_values: pft.AppendableSelector[pft.StdKeyValues] = {},
    append_standard_keywords: pft.AppendableSelector[pft.StdKeyValues] = {},
    substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
    promote_all_nonstandard_keys: bool = False,
    allow_repair_non_unique: pft.TriFlag = "false",
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    int_width_override: pft.IntWidthOverride = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    skip: int | None = None,
    limit: int | None = None,
    scan: bool = False,
    mmap: bool = False,
) -> list[
    tuple[CoreTEXT2_0 | CoreTEXT3_0 | CoreTEXT3_1 | CoreTEXT3_2, StdTEXTOutput]
]: ...

#
def fcs_read_flat_dataset(
    path: Path,
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    process_supp_text: pft.ProcessSuppText = "asis",
    delim_escape_mode: pft.DelimEscapeMode = "escaped",
    allow_non_ascii_delim: pft.TriFlag = "false",
    allow_nonunique: pft.TriFlag = "false",
    allow_even_delims: pft.TriFlag = "false",
    allow_odd_tokens: pft.TriFlag = "false",
    allow_empty_keys: pft.TriFlag = "false",
    allow_delim_at_boundary: pft.TriFlag = "false",
    use_encoding: pft.UseEncoding = "utf8",
    allow_non_ascii_keys: pft.TriFlag = "false",
    allow_non_utf8_values: pft.TriFlag = "false",
    allow_missing_supp_text: pft.TriFlag = "false",
    allow_supp_text_own_delim: pft.TriFlag = "false",
    allow_missing_nextdata: pft.TriFlag = "false",
    trim_value_whitespace: pft.TrimValueWhitespace = "notrim",
    ignore_standard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    promote_nonstandard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    demote_standard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    rename_standard_keys: pft.AppendableSelector[pft.KeyPairs] = {},
    replace_standard_key_values: pft.AppendableSelector[pft.StdKeyValues] = {},
    append_standard_keywords: pft.AppendableSelector[pft.StdKeyValues] = {},
    substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
    promote_all_nonstandard_keys: bool = False,
    allow_repair_non_unique: pft.TriFlag = "false",
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    int_width_override: pft.IntWidthOverride = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    data_remainder_limit: int = 0,
    allow_uneven_event_width: pft.TriFlag = "false",
    allow_tot_mismatch: pft.TriFlag = "false",
    over_bitmask_action: pft.OverLimitAction = "trunc_warn",
    over_range_action: pft.OverLimitAction = "warn",
    allow_missing_crc: pft.TriFlag = "false",
    allow_mismatch_crc: pft.TriFlag = "false",
    compute_crc: pft.ComputeCRC = "never",
    read_intra_segment_dark_bytes: bool = False,
    read_post_dataset_dark_bytes: bool = False,
    row_buffer_size: int = 28000,
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    dataset_offset: int = 0,
    scan: bool = False,
    mmap: bool = False,
) -> FlatDatasetOutput: ...

#
def fcs_read_flat_datasets(
    path: Path,
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    process_supp_text: pft.ProcessSuppText = "asis",
    delim_escape_mode: pft.DelimEscapeMode = "escaped",
    allow_non_ascii_delim: pft.TriFlag = "false",
    allow_nonunique: pft.TriFlag = "false",
    allow_even_delims: pft.TriFlag = "false",
    allow_odd_tokens: pft.TriFlag = "false",
    allow_empty_keys: pft.TriFlag = "false",
    allow_delim_at_boundary: pft.TriFlag = "false",
    use_encoding: pft.UseEncoding = "utf8",
    allow_non_ascii_keys: pft.TriFlag = "false",
    allow_non_utf8_values: pft.TriFlag = "false",
    allow_missing_supp_text: pft.TriFlag = "false",
    allow_supp_text_own_delim: pft.TriFlag = "false",
    allow_missing_nextdata: pft.TriFlag = "false",
    trim_value_whitespace: pft.TrimValueWhitespace = "notrim",
    ignore_standard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    promote_nonstandard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    demote_standard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    rename_standard_keys: pft.AppendableSelector[pft.KeyPairs] = {},
    replace_standard_key_values: pft.AppendableSelector[pft.StdKeyValues] = {},
    append_standard_keywords: pft.AppendableSelector[pft.StdKeyValues] = {},
    substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
    promote_all_nonstandard_keys: bool = False,
    allow_repair_non_unique: pft.TriFlag = "false",
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    int_width_override: pft.IntWidthOverride = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    data_remainder_limit: int = 0,
    allow_uneven_event_width: pft.TriFlag = "false",
    allow_tot_mismatch: pft.TriFlag = "false",
    over_bitmask_action: pft.OverLimitAction = "trunc_warn",
    over_range_action: pft.OverLimitAction = "warn",
    allow_missing_crc: pft.TriFlag = "false",
    allow_mismatch_crc: pft.TriFlag = "false",
    compute_crc: pft.ComputeCRC = "never",
    read_intra_segment_dark_bytes: bool = False,
    read_post_dataset_dark_bytes: bool = False,
    row_buffer_size: int = 28000,
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    skip: int | None = None,
    limit: int | None = None,
    scan: bool = False,
    mmap: bool = False,
) -> list[FlatDatasetOutput]: ...

#
def fcs_read_std_dataset(
    path: Path,
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    process_supp_text: pft.ProcessSuppText = "asis",
    delim_escape_mode: pft.DelimEscapeMode = "escaped",
    allow_non_ascii_delim: pft.TriFlag = "false",
    allow_nonunique: pft.TriFlag = "false",
    allow_even_delims: pft.TriFlag = "false",
    allow_odd_tokens: pft.TriFlag = "false",
    allow_empty_keys: pft.TriFlag = "false",
    allow_delim_at_boundary: pft.TriFlag = "false",
    use_encoding: pft.UseEncoding = "utf8",
    allow_non_ascii_keys: pft.TriFlag = "false",
    allow_non_utf8_values: pft.TriFlag = "false",
    allow_missing_supp_text: pft.TriFlag = "false",
    allow_supp_text_own_delim: pft.TriFlag = "false",
    allow_missing_nextdata: pft.TriFlag = "false",
    trim_value_whitespace: pft.TrimValueWhitespace = "notrim",
    dedup_measurement_names: bool = False,
    trim_intra_value_whitespace: bool = False,
    time_meas_pattern: pft.Selector[str] = "^(TIME|Time)$",
    allow_missing_time: pft.TriFlag = "false",
    force_linear_scale: pft.ForceLinearScale = "none",
    ignore_optical_only_keys: list[pft.OpticalOnlyKey] = [],
    process_optical_only_keys: pft.ProcessOpticalOnlyKeys = "demote_warn",
    date_pattern: pft.Selector[str | None] = None,
    time_pattern: pft.Selector[str | None] = None,
    datetime_pattern: pft.Selector[str | None] = None,
    last_modified_pattern: pft.Selector[str | None] = None,
    allow_other_feature: bool = False,
    process_pseudostandard: pft.ProcessKeywordFailure = "error",
    process_hyper_par: pft.ProcessKeywordFailure = "error",
    process_other_version: pft.ProcessKeywordFailure = "error",
    process_extra_timestep: pft.ProcessKeywordFailure = "error",
    fix_log_scale_offsets: bool = False,
    add_missing_timestep: pft.Timestep | None = None,
    spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
    disallow_localtime: bool = False,
    ignore_standard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    promote_nonstandard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    demote_standard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    rename_standard_keys: pft.AppendableSelector[pft.KeyPairs] = {},
    replace_standard_key_values: pft.AppendableSelector[pft.StdKeyValues] = {},
    append_standard_keywords: pft.AppendableSelector[pft.StdKeyValues] = {},
    substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
    promote_all_nonstandard_keys: bool = False,
    allow_repair_non_unique: pft.TriFlag = "false",
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    int_width_override: pft.IntWidthOverride = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    data_remainder_limit: int = 0,
    allow_uneven_event_width: pft.TriFlag = "false",
    allow_tot_mismatch: pft.TriFlag = "false",
    over_bitmask_action: pft.OverLimitAction = "trunc_warn",
    over_range_action: pft.OverLimitAction = "warn",
    allow_missing_crc: pft.TriFlag = "false",
    allow_mismatch_crc: pft.TriFlag = "false",
    compute_crc: pft.ComputeCRC = "never",
    read_intra_segment_dark_bytes: bool = False,
    read_post_dataset_dark_bytes: bool = False,
    row_buffer_size: int = 28000,
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    dataset_offset: int = 0,
    scan: bool = False,
    mmap: bool = False,
) -> tuple[
    CoreDataset2_0 | CoreDataset3_0 | CoreDataset3_1 | CoreDataset3_2, StdDatasetOutput
]: ...

#
def fcs_read_std_datasets(
    path: Path,
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    process_supp_text: pft.ProcessSuppText = "asis",
    delim_escape_mode: pft.DelimEscapeMode = "escaped",
    allow_non_ascii_delim: pft.TriFlag = "false",
    allow_nonunique: pft.TriFlag = "false",
    allow_even_delims: pft.TriFlag = "false",
    allow_odd_tokens: pft.TriFlag = "false",
    allow_empty_keys: pft.TriFlag = "false",
    allow_delim_at_boundary: pft.TriFlag = "false",
    use_encoding: pft.UseEncoding = "utf8",
    allow_non_ascii_keys: pft.TriFlag = "false",
    allow_non_utf8_values: pft.TriFlag = "false",
    allow_missing_supp_text: pft.TriFlag = "false",
    allow_supp_text_own_delim: pft.TriFlag = "false",
    allow_missing_nextdata: pft.TriFlag = "false",
    trim_value_whitespace: pft.TrimValueWhitespace = "notrim",
    dedup_measurement_names: bool = False,
    trim_intra_value_whitespace: bool = False,
    time_meas_pattern: pft.Selector[str] = "^(TIME|Time)$",
    allow_missing_time: pft.TriFlag = "false",
    force_linear_scale: pft.ForceLinearScale = "none",
    ignore_optical_only_keys: list[pft.OpticalOnlyKey] = [],
    process_optical_only_keys: pft.ProcessOpticalOnlyKeys = "demote_warn",
    date_pattern: pft.Selector[str | None] = None,
    time_pattern: pft.Selector[str | None] = None,
    datetime_pattern: pft.Selector[str | None] = None,
    last_modified_pattern: pft.Selector[str | None] = None,
    allow_other_feature: bool = False,
    process_pseudostandard: pft.ProcessKeywordFailure = "error",
    process_hyper_par: pft.ProcessKeywordFailure = "error",
    process_other_version: pft.ProcessKeywordFailure = "error",
    process_extra_timestep: pft.ProcessKeywordFailure = "error",
    fix_log_scale_offsets: bool = False,
    add_missing_timestep: pft.Timestep | None = None,
    spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
    disallow_localtime: bool = False,
    ignore_standard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    promote_nonstandard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    demote_standard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    rename_standard_keys: pft.AppendableSelector[pft.KeyPairs] = {},
    replace_standard_key_values: pft.AppendableSelector[pft.StdKeyValues] = {},
    append_standard_keywords: pft.AppendableSelector[pft.StdKeyValues] = {},
    substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
    promote_all_nonstandard_keys: bool = False,
    allow_repair_non_unique: pft.TriFlag = "false",
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    int_width_override: pft.IntWidthOverride = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    data_remainder_limit: int = 0,
    allow_uneven_event_width: pft.TriFlag = "false",
    allow_tot_mismatch: pft.TriFlag = "false",
    over_bitmask_action: pft.OverLimitAction = "trunc_warn",
    over_range_action: pft.OverLimitAction = "warn",
    allow_missing_crc: pft.TriFlag = "false",
    allow_mismatch_crc: pft.TriFlag = "false",
    compute_crc: pft.ComputeCRC = "never",
    read_intra_segment_dark_bytes: bool = False,
    read_post_dataset_dark_bytes: bool = False,
    row_buffer_size: int = 28000,
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    skip: int | None = None,
    limit: int | None = None,
    scan: bool = False,
    mmap: bool = False,
) -> list[
    tuple[
        CoreDataset2_0 | CoreDataset3_0 | CoreDataset3_1 | CoreDataset3_2,
        StdDatasetOutput,
    ]
]: ...

#
def fcs_read_flat_dataset_with_keywords(
    path: Path,
    header: HeaderAndSuppOffsets,
    std: pft.StdKeywords,
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    int_width_override: pft.IntWidthOverride = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    data_remainder_limit: int = 0,
    allow_uneven_event_width: pft.TriFlag = "false",
    allow_tot_mismatch: pft.TriFlag = "false",
    over_bitmask_action: pft.OverLimitAction = "trunc_warn",
    over_range_action: pft.OverLimitAction = "warn",
    allow_missing_crc: pft.TriFlag = "false",
    allow_mismatch_crc: pft.TriFlag = "false",
    compute_crc: pft.ComputeCRC = "never",
    read_intra_segment_dark_bytes: bool = False,
    read_post_dataset_dark_bytes: bool = False,
    row_buffer_size: int = 28000,
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    dataset_offset: int = 0,
    dataset_len: int | None = None,
    mmap: bool = False,
) -> NewFlatDatasetFromKwsOutput: ...

#
def fcs_summarize(
    path: Path,
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    process_supp_text: pft.ProcessSuppText = "asis",
    delim_escape_mode: pft.DelimEscapeMode = "escaped",
    allow_non_ascii_delim: pft.TriFlag = "false",
    allow_nonunique: pft.TriFlag = "false",
    allow_even_delims: pft.TriFlag = "false",
    allow_odd_tokens: pft.TriFlag = "false",
    allow_empty_keys: pft.TriFlag = "false",
    allow_delim_at_boundary: pft.TriFlag = "false",
    use_encoding: pft.UseEncoding = "utf8",
    allow_non_ascii_keys: pft.TriFlag = "false",
    allow_non_utf8_values: pft.TriFlag = "false",
    allow_missing_supp_text: pft.TriFlag = "false",
    allow_supp_text_own_delim: pft.TriFlag = "false",
    allow_missing_nextdata: pft.TriFlag = "false",
    trim_value_whitespace: pft.TrimValueWhitespace = "notrim",
    ignore_standard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    promote_nonstandard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    demote_standard_keys: pft.AppendableSelector[pft.StdKeyPatterns] = [],
    rename_standard_keys: pft.AppendableSelector[pft.KeyPairs] = {},
    replace_standard_key_values: pft.AppendableSelector[pft.StdKeyValues] = {},
    append_standard_keywords: pft.AppendableSelector[pft.StdKeyValues] = {},
    substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
    promote_all_nonstandard_keys: bool = False,
    allow_repair_non_unique: pft.TriFlag = "false",
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    int_width_override: pft.IntWidthOverride = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    data_remainder_limit: int = 0,
    allow_uneven_event_width: pft.TriFlag = "false",
    allow_tot_mismatch: pft.TriFlag = "false",
    over_bitmask_action: pft.OverLimitAction = "trunc_warn",
    over_range_action: pft.OverLimitAction = "warn",
    allow_missing_crc: pft.TriFlag = "false",
    allow_mismatch_crc: pft.TriFlag = "false",
    compute_crc: pft.ComputeCRC = "never",
    read_intra_segment_dark_bytes: bool = False,
    read_post_dataset_dark_bytes: bool = False,
    row_buffer_size: int = 28000,
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    skip: int | None = None,
    limit: int | None = None,
    scan: bool = False,
    mmap: bool = False,
) -> list[DatasetSummary]: ...

#
def fcs_write_datasets(
    path: Path,
    datasets: list[CoreDataset2_0 | CoreDataset3_0 | CoreDataset3_1 | CoreDataset3_2],
    delim: int = 30,
    big_other: bool = False,
    compute_crc: bool = False,
    override_fil: bool = False,
    allow_over_bitmask: pft.TriFlag = "false",
    disallow_over_range: pft.TriFlag = "false",
    row_buffer_size: int = 28000,
) -> int | None: ...

#
def fcs_scan_dataset_boundaries(
    path: Path, mmap: bool = False
) -> list[tuple[pft.FCSVersion, int]]: ...
@final
class Header:
    def __new__(
        cls,
        dataset_offset: int,
        version: pft.FCSVersion,
        final_offsets: FinalHeaderOffsets,
        original_offsets: OriginalHeaderOffsets,
        overlaps: list[HeaderToHeaderOffsetsOverlap],
        dark_bytes: pft.DarkBytes | None,
        read_header_ns: int,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def dataset_offset(self) -> int: ...
    @property
    def version(self) -> pft.FCSVersion: ...
    @property
    def final_offsets(self) -> FinalHeaderOffsets: ...
    @property
    def original_offsets(self) -> OriginalHeaderOffsets: ...
    @property
    def overlaps(self) -> list[HeaderToHeaderOffsetsOverlap]: ...
    @property
    def dark_bytes(self) -> pft.DarkBytes | None: ...
    @property
    def read_header_ns(self) -> int: ...

@final
class FinalHeaderOffsets:
    def __new__(
        cls,
        text: pft.FinalOffsets,
        data: pft.FinalOffsets,
        analysis: pft.FinalOffsets,
        others: pft.FinalOtherOffsets | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def text(self) -> pft.FinalOffsets: ...
    @property
    def data(self) -> pft.FinalOffsets: ...
    @property
    def analysis(self) -> pft.FinalOffsets: ...
    @property
    def others(self) -> pft.FinalOtherOffsets | None: ...

@final
class OriginalHeaderOffsets:
    def __new__(
        cls,
        text: pft.OriginalOffsets,
        data: pft.OriginalOffsets,
        analysis: pft.OriginalOffsets,
        others: list[pft.OriginalOffsets],
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def text(self) -> pft.OriginalOffsets: ...
    @property
    def data(self) -> pft.OriginalOffsets: ...
    @property
    def analysis(self) -> pft.OriginalOffsets: ...
    @property
    def others(self) -> list[pft.OriginalOffsets]: ...

@final
class ValidKeywords:
    def __new__(
        cls,
        std: pft.StdKeywords,
        pnonstd: pft.PseudoNonStdKeywords,
        pstd: pft.PseudoStdKeywords,
        nonstd: pft.NonStdKeywords,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def std(self) -> pft.StdKeywords: ...
    @property
    def pnonstd(self) -> pft.PseudoNonStdKeywords: ...
    @property
    def pstd(self) -> pft.PseudoStdKeywords: ...
    @property
    def nonstd(self) -> pft.NonStdKeywords: ...

@final
class StdTEXTDiagnostics:
    def __new__(
        cls,
        optional: pft.DroppedStdKeywords,
        hyper_par: pft.DroppedStdKeywords,
        hyper_gate: pft.DroppedStdKeywords,
        other_version: pft.DroppedStdKeywords,
        timestep: pft.NEStr | None,
        undemoted_pseudostandard: list[tuple[pft.PseudoStdKey, pft.NEStr]],
        dedup_names: list[pft.Shortname | None],
        scale: list[pft.MeasScaleDiagnostic],
        gate_scale: list[pft.GateScaleDiagnostic],
        trimmed: list[tuple[pft.StdKey, pft.NEStr]],
        temporal_optical_pairs: list[tuple[pft.StdKey, pft.NEStr]],
        timestep_added: bool,
        spillover_was_indexed: bool | None,
        btim_pattern: str | None,
        etim_pattern: str | None,
        date_pattern: str | None,
        begindatetime_pattern: str | None,
        enddatetime_pattern: str | None,
        begindatetime_used_localtime: bool | None,
        enddatetime_used_localtime: bool | None,
        last_modified_pattern: str | None,
        read_std_ns: int,
        schema_diagnostics: DataSchemaDiagnostics,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def optional(self) -> pft.DroppedStdKeywords: ...
    @property
    def hyper_par(self) -> pft.DroppedStdKeywords: ...
    @property
    def hyper_gate(self) -> pft.DroppedStdKeywords: ...
    @property
    def other_version(self) -> pft.DroppedStdKeywords: ...
    @property
    def timestep(self) -> pft.NEStr | None: ...
    @property
    def undemoted_pseudostandard(self) -> list[tuple[pft.PseudoStdKey, pft.NEStr]]: ...
    @property
    def dedup_names(self) -> list[pft.Shortname | None]: ...
    @property
    def scale(self) -> list[pft.MeasScaleDiagnostic]: ...
    @property
    def gate_scale(self) -> list[pft.GateScaleDiagnostic]: ...
    @property
    def trimmed(self) -> list[tuple[pft.StdKey, pft.NEStr]]: ...
    @property
    def temporal_optical_pairs(self) -> list[tuple[pft.StdKey, pft.NEStr]]: ...
    @property
    def timestep_added(self) -> bool: ...
    @property
    def spillover_was_indexed(self) -> bool | None: ...
    @property
    def btim_pattern(self) -> str | None: ...
    @property
    def etim_pattern(self) -> str | None: ...
    @property
    def date_pattern(self) -> str | None: ...
    @property
    def begindatetime_pattern(self) -> str | None: ...
    @property
    def enddatetime_pattern(self) -> str | None: ...
    @property
    def begindatetime_used_localtime(self) -> bool | None: ...
    @property
    def enddatetime_used_localtime(self) -> bool | None: ...
    @property
    def last_modified_pattern(self) -> str | None: ...
    @property
    def read_std_ns(self) -> int: ...
    @property
    def schema_diagnostics(self) -> DataSchemaDiagnostics: ...

@final
class DataSchemaDiagnostics:
    def __new__(
        cls,
        truncated_columns: list[float | None],
        original_int_width: int | None,
        original_byteord: list[int] | None,
        read_schema_ns: int,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def truncated_columns(self) -> list[float | None]: ...
    @property
    def original_int_width(self) -> int | None: ...
    @property
    def original_byteord(self) -> list[int] | None: ...
    @property
    def read_schema_ns(self) -> int: ...

@final
class DatasetOffsets:
    def __new__(
        cls,
        final_data_offsets: pft.FinalOffsets,
        final_analysis_offsets: pft.FinalOffsets,
        data_origin: TEXTOffsetsOrigin,
        analysis_origin: TEXTOffsetsOrigin,
        data_analysis_overlap: int | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def final_data_offsets(self) -> pft.FinalOffsets: ...
    @property
    def final_analysis_offsets(self) -> pft.FinalOffsets: ...
    @property
    def data_origin(self) -> TEXTOffsetsOrigin: ...
    @property
    def analysis_origin(self) -> TEXTOffsetsOrigin: ...
    @property
    def data_analysis_overlap(self) -> int | None: ...

@final
class FlatTEXTOutput:
    def __new__(
        cls, keywords: ValidKeywords, flat_diagnostics: FlatTEXTDiagnostics
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def keywords(self) -> ValidKeywords: ...
    @property
    def flat_diagnostics(self) -> FlatTEXTDiagnostics: ...

@final
class HeaderAndSuppOffsets:
    def __new__(
        cls, header: Header, supp_text: SuppTEXTOffsetsOutput, nextdata: int | None
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def header(self) -> Header: ...
    @property
    def supp_text(self) -> SuppTEXTOffsetsOutput: ...
    @property
    def nextdata(self) -> int | None: ...

@final
class FlatDatasetOutput:
    def __new__(
        cls,
        keywords: ValidKeywords,
        flat_diagnostics: FlatTEXTDiagnostics,
        dataset: FlatDatasetFromKwsOutput,
        version_scores: pft.KeywordVersionScores | None,
        repair_diagnostics: RepairDiagnostics,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def keywords(self) -> ValidKeywords: ...
    @property
    def flat_diagnostics(self) -> FlatTEXTDiagnostics: ...
    @property
    def dataset(self) -> FlatDatasetFromKwsOutput: ...
    @property
    def version_scores(self) -> pft.KeywordVersionScores | None: ...
    @property
    def repair_diagnostics(self) -> RepairDiagnostics: ...

@final
class FlatTEXTDiagnostics:
    def __new__(
        cls,
        header_supp: HeaderAndSuppOffsets,
        primary_text_overflow: int,
        header_overflows: list[HeaderOffsetsOverflow],
        read_text_ns: int,
        primary_split: SplitTEXTDiagnostics,
        supp_split: SplitTEXTDiagnostics | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def header_supp(self) -> HeaderAndSuppOffsets: ...
    @property
    def primary_text_overflow(self) -> int: ...
    @property
    def header_overflows(self) -> list[HeaderOffsetsOverflow]: ...
    @property
    def read_text_ns(self) -> int: ...
    @property
    def primary_split(self) -> SplitTEXTDiagnostics: ...
    @property
    def supp_split(self) -> SplitTEXTDiagnostics | None: ...

@final
class SplitTEXTDiagnostics:
    def __new__(
        cls,
        delimiter: int,
        escaped: bool,
        keys_with_non_utf8_values: list[tuple[pft.AnyKey, pft.NEBytes]],
        values_with_non_ascii_keys: list[tuple[pft.NEBytes, pft.NEStr]],
        byte_pairs: list[tuple[pft.NEBytes, pft.NEBytes]],
        non_unique_std_keywords: list[tuple[pft.StdKey, pft.NEStr]],
        non_unique_pstd_keywords: list[tuple[pft.PseudoStdKey, pft.NEStr]],
        non_unique_pnonstd_keywords: list[tuple[pft.PseudoNonStdKey, pft.NEStr]],
        non_unique_nonstd_keywords: list[tuple[pft.NonStdKey, pft.NEStr]],
        keys_with_empty_trimmed_values: list[tuple[pft.KeyOrBytes, pft.NEStr]],
        keys_with_trimmed_values: list[tuple[pft.KeyOrBytes, pft.NEStr]],
        keys_with_blank_values: list[pft.NEStrOrBytes],
        values_with_blank_keys: list[pft.NEStrOrBytes],
        empty_pairs: int,
        tokens_with_boundary_delims: list[pft.NEStrOrBytes],
        last_odd_token: str | bytes,
        has_even_delims: bool,
        extra_leading_delims: int,
        multibyte_encoded: bool,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def delimiter(self) -> int: ...
    @property
    def escaped(self) -> bool: ...
    @property
    def keys_with_non_utf8_values(self) -> list[tuple[pft.AnyKey, pft.NEBytes]]: ...
    @property
    def values_with_non_ascii_keys(self) -> list[tuple[pft.NEBytes, pft.NEStr]]: ...
    @property
    def byte_pairs(self) -> list[tuple[pft.NEBytes, pft.NEBytes]]: ...
    @property
    def non_unique_std_keywords(self) -> list[tuple[pft.StdKey, pft.NEStr]]: ...
    @property
    def non_unique_pstd_keywords(self) -> list[tuple[pft.PseudoStdKey, pft.NEStr]]: ...
    @property
    def non_unique_pnonstd_keywords(
        self,
    ) -> list[tuple[pft.PseudoNonStdKey, pft.NEStr]]: ...
    @property
    def non_unique_nonstd_keywords(self) -> list[tuple[pft.NonStdKey, pft.NEStr]]: ...
    @property
    def keys_with_empty_trimmed_values(
        self,
    ) -> list[tuple[pft.KeyOrBytes, pft.NEStr]]: ...
    @property
    def keys_with_trimmed_values(self) -> list[tuple[pft.KeyOrBytes, pft.NEStr]]: ...
    @property
    def keys_with_blank_values(self) -> list[pft.NEStrOrBytes]: ...
    @property
    def values_with_blank_keys(self) -> list[pft.NEStrOrBytes]: ...
    @property
    def empty_pairs(self) -> int: ...
    @property
    def tokens_with_boundary_delims(self) -> list[pft.NEStrOrBytes]: ...
    @property
    def last_odd_token(self) -> str | bytes: ...
    @property
    def has_even_delims(self) -> bool: ...
    @property
    def extra_leading_delims(self) -> int: ...
    @property
    def multibyte_encoded(self) -> bool: ...

@final
class FlatDatasetFromKwsOutput:
    def __new__(
        cls,
        data: DataFrame,
        analysis: pft.AnalysisBytes,
        others: list[pft.OtherBytes],
        dataset_offsets: DatasetOffsets,
        schema_diagnostics: DataSchemaDiagnostics,
        dataset_diagnostics: DatasetDiagnostics,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def data(self) -> DataFrame: ...
    @property
    def analysis(self) -> pft.AnalysisBytes: ...
    @property
    def others(self) -> list[pft.OtherBytes]: ...
    @property
    def dataset_offsets(self) -> DatasetOffsets: ...
    @property
    def schema_diagnostics(self) -> DataSchemaDiagnostics: ...
    @property
    def dataset_diagnostics(self) -> DatasetDiagnostics: ...

@final
class NewFlatDatasetFromKwsOutput:
    def __new__(
        cls, dataset: FlatDatasetFromKwsOutput, header: FinalHeaderOffsets
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def dataset(self) -> FlatDatasetFromKwsOutput: ...
    @property
    def header(self) -> FinalHeaderOffsets: ...

@final
class SuppTEXTOffsetsOutput:
    def __new__(
        cls,
        origin_type: pft.SuppTEXTOffsetsOriginType,
        final_offsets: pft.FinalOffsets | None,
        original_offsets: pft.OriginalOffsets | None,
        other_index: int | None,
        overlaps: list[SuppToHeaderOffsetsOverlap],
        overflow: SuppOffsetsOverflow | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def origin_type(self) -> pft.SuppTEXTOffsetsOriginType: ...
    @property
    def final_offsets(self) -> pft.FinalOffsets | None: ...
    @property
    def original_offsets(self) -> pft.OriginalOffsets | None: ...
    @property
    def other_index(self) -> int | None: ...
    @property
    def overlaps(self) -> list[SuppToHeaderOffsetsOverlap]: ...
    @property
    def overflow(self) -> SuppOffsetsOverflow | None: ...

@final
class TEXTOffsetsOrigin:
    def __new__(
        cls,
        origin_type: pft.TEXTOffsetsOriginType,
        original_offsets: pft.OriginalOffsets | None,
        overlaps: list[TextToHeaderOrSuppOffsetsOverlap],
        overflow: TextOffsetsOverflow | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def origin_type(self) -> pft.TEXTOffsetsOriginType: ...
    @property
    def original_offsets(self) -> pft.OriginalOffsets | None: ...
    @property
    def overlaps(self) -> list[TextToHeaderOrSuppOffsetsOverlap]: ...
    @property
    def overflow(self) -> TextOffsetsOverflow | None: ...

@final
class RepairDiagnostics:
    def __new__(
        cls,
        demoted: list[pft.StdKey],
        promoted: list[pft.PseudoNonStdKey],
        subbed: list[tuple[pft.StdKey, pft.NEStr]],
        replaced: list[tuple[pft.StdKey, pft.NEStr]],
        renamed: list[tuple[pft.AnyKey, pft.AnyKey]],
        renamed_non_unique: list[tuple[pft.AnyKey, pft.AnyKey]],
        ignored: list[tuple[pft.StdKey, pft.NEStr]],
        removed: list[tuple[pft.StdKey, pft.NEStr]],
        promoted_non_unique: list[tuple[pft.PseudoNonStdKey, pft.NEStr]],
        appended_non_unique: list[tuple[pft.StdKey, pft.NEStr]],
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def demoted(self) -> list[pft.StdKey]: ...
    @property
    def promoted(self) -> list[pft.PseudoNonStdKey]: ...
    @property
    def subbed(self) -> list[tuple[pft.StdKey, pft.NEStr]]: ...
    @property
    def replaced(self) -> list[tuple[pft.StdKey, pft.NEStr]]: ...
    @property
    def renamed(self) -> list[tuple[pft.AnyKey, pft.AnyKey]]: ...
    @property
    def renamed_non_unique(self) -> list[tuple[pft.AnyKey, pft.AnyKey]]: ...
    @property
    def ignored(self) -> list[tuple[pft.StdKey, pft.NEStr]]: ...
    @property
    def removed(self) -> list[tuple[pft.StdKey, pft.NEStr]]: ...
    @property
    def promoted_non_unique(self) -> list[tuple[pft.PseudoNonStdKey, pft.NEStr]]: ...
    @property
    def appended_non_unique(self) -> list[tuple[pft.StdKey, pft.NEStr]]: ...

@final
class DatasetDiagnostics:
    def __new__(
        cls,
        event_width: int | None,
        event_data_remainder: int | None,
        tot_event_mismatch: bool | None,
        overrange_columns: list[tuple[int, bool] | None],
        intra_segment_dark_bytes: list[IntraSegmentDarkBytes],
        post_dataset_dark_bytes: pft.DarkBytes | None,
        file_crc: pft.CRCOutput,
        computed_crc: int | None,
        dataset_len: int,
        next_dataset_offset: int | None,
        next_dataset_manually_scanned: bool,
        read_data_ns: int,
        check_range_ns: int,
        read_other_analysis_ns: int,
        read_crc_ns: int,
        read_dark_bytes_ns: int,
        scan_next_ns: int,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def event_width(self) -> int | None: ...
    @property
    def event_data_remainder(self) -> int | None: ...
    @property
    def tot_event_mismatch(self) -> bool | None: ...
    @property
    def overrange_columns(self) -> list[tuple[int, bool] | None]: ...
    @property
    def intra_segment_dark_bytes(self) -> list[IntraSegmentDarkBytes]: ...
    @property
    def post_dataset_dark_bytes(self) -> pft.DarkBytes | None: ...
    @property
    def file_crc(self) -> pft.CRCOutput: ...
    @property
    def computed_crc(self) -> int | None: ...
    @property
    def dataset_len(self) -> int: ...
    @property
    def next_dataset_offset(self) -> int | None: ...
    @property
    def next_dataset_manually_scanned(self) -> bool: ...
    @property
    def read_data_ns(self) -> int: ...
    @property
    def check_range_ns(self) -> int: ...
    @property
    def read_other_analysis_ns(self) -> int: ...
    @property
    def read_crc_ns(self) -> int: ...
    @property
    def read_dark_bytes_ns(self) -> int: ...
    @property
    def scan_next_ns(self) -> int: ...

@final
class IntraSegmentDarkBytes:
    def __new__(
        cls,
        prev: pft.FlankingSegmentName,
        next: pft.FlankingSegmentName,
        start: int,
        end: int,
        bytes: pft.DarkBytes,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def prev(self) -> pft.FlankingSegmentName: ...
    @property
    def next(self) -> pft.FlankingSegmentName: ...
    @property
    def start(self) -> int: ...
    @property
    def end(self) -> int: ...
    @property
    def bytes(self) -> pft.DarkBytes: ...

@final
class KeywordVersionScore:
    def __new__(
        cls,
        good_req: int,
        good_opt: int,
        drop: int,
        missing_opt: int,
        missing_req: int,
        missing_absent: int,
        incompatible_widths: bool,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def good_req(self) -> int: ...
    @property
    def good_opt(self) -> int: ...
    @property
    def drop(self) -> int: ...
    @property
    def missing_opt(self) -> int: ...
    @property
    def missing_req(self) -> int: ...
    @property
    def missing_absent(self) -> int: ...
    @property
    def incompatible_widths(self) -> bool: ...

@final
class StdTEXTOutput:
    def __new__(
        cls,
        tot: int | None,
        dataset_offsets: DatasetOffsets,
        repair_diagnostics: RepairDiagnostics,
        std_diagnostics: StdTEXTDiagnostics,
        flat_diagnostics: FlatTEXTDiagnostics,
        version_scores: pft.KeywordVersionScores | None,
        pseudostandard: pft.PseudoStdKeywords,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def tot(self) -> int | None: ...
    @property
    def dataset_offsets(self) -> DatasetOffsets: ...
    @property
    def repair_diagnostics(self) -> RepairDiagnostics: ...
    @property
    def std_diagnostics(self) -> StdTEXTDiagnostics: ...
    @property
    def flat_diagnostics(self) -> FlatTEXTDiagnostics: ...
    @property
    def version_scores(self) -> pft.KeywordVersionScores | None: ...
    @property
    def pseudostandard(self) -> pft.PseudoStdKeywords: ...

@final
class StdDatasetOutput:
    def __new__(
        cls,
        dataset: StdDatasetFromKwsOutput,
        flat_diagnostics: FlatTEXTDiagnostics,
        version_scores: pft.KeywordVersionScores | None,
        repair_diagnostics: RepairDiagnostics,
        pseudostandard: pft.PseudoStdKeywords,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def dataset(self) -> StdDatasetFromKwsOutput: ...
    @property
    def flat_diagnostics(self) -> FlatTEXTDiagnostics: ...
    @property
    def version_scores(self) -> pft.KeywordVersionScores | None: ...
    @property
    def repair_diagnostics(self) -> RepairDiagnostics: ...
    @property
    def pseudostandard(self) -> pft.PseudoStdKeywords: ...

@final
class StdDatasetFromKwsOutput:
    def __new__(
        cls,
        dataset_offsets: DatasetOffsets,
        std_diagnostics: StdTEXTDiagnostics,
        dataset_diagnostics: DatasetDiagnostics,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def dataset_offsets(self) -> DatasetOffsets: ...
    @property
    def std_diagnostics(self) -> StdTEXTDiagnostics: ...
    @property
    def dataset_diagnostics(self) -> DatasetDiagnostics: ...

@final
class NewStdDatasetFromKwsOutput:
    def __new__(
        cls, dataset: StdDatasetFromKwsOutput, header: FinalHeaderOffsets
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def dataset(self) -> StdDatasetFromKwsOutput: ...
    @property
    def header(self) -> FinalHeaderOffsets: ...

@final
class DatasetSummary:
    def __new__(
        cls,
        version: pft.FCSVersion,
        text_len: int,
        data_len: int,
        analysis_len: int,
        n_events: int,
        n_measurements: int,
        n_other: int,
        others_len: int,
        datatype: pft.Datatype | None,
        dataset_offset: int,
        file_crc: pft.CRCOutput,
        computed_crc: int | None,
        read_header_ns: int,
        read_text_ns: int,
        read_schema_ns: int,
        read_data_ns: int,
        check_range_ns: int,
        read_other_analysis_ns: int,
        read_crc_ns: int,
        read_dark_bytes_ns: int,
        scan_next_ns: int,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def version(self) -> pft.FCSVersion: ...
    @property
    def text_len(self) -> int: ...
    @property
    def data_len(self) -> int: ...
    @property
    def analysis_len(self) -> int: ...
    @property
    def n_events(self) -> int: ...
    @property
    def n_measurements(self) -> int: ...
    @property
    def n_other(self) -> int: ...
    @property
    def others_len(self) -> int: ...
    @property
    def datatype(self) -> pft.Datatype | None: ...
    @property
    def dataset_offset(self) -> int: ...
    @property
    def file_crc(self) -> pft.CRCOutput: ...
    @property
    def computed_crc(self) -> int | None: ...
    @property
    def read_header_ns(self) -> int: ...
    @property
    def read_text_ns(self) -> int: ...
    @property
    def read_schema_ns(self) -> int: ...
    @property
    def read_data_ns(self) -> int: ...
    @property
    def check_range_ns(self) -> int: ...
    @property
    def read_other_analysis_ns(self) -> int: ...
    @property
    def read_crc_ns(self) -> int: ...
    @property
    def read_dark_bytes_ns(self) -> int: ...
    @property
    def scan_next_ns(self) -> int: ...

@final
class BuildInfo:
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def version(self) -> str: ...
    @property
    def commit_hash(self) -> str | None: ...
    @property
    def build_date(self) -> str: ...
    @property
    def rustc_version(self) -> str: ...
    @property
    def target(self) -> str: ...
    @property
    def is_debug(self) -> bool: ...
    @property
    def opt_level(self) -> str: ...
    @property
    def target_features(self) -> str: ...

@final
class HeaderOffsetsOverflow:
    def __new__(
        cls,
        offsets: pft.HeaderNamedOffsets,
        overflow: int,
        dataset_len: int,
        bound_is_nextdata: bool,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def offsets(self) -> pft.HeaderNamedOffsets: ...
    @property
    def overflow(self) -> int: ...
    @property
    def dataset_len(self) -> int: ...
    @property
    def bound_is_nextdata(self) -> bool: ...

@final
class TextOffsetsOverflow:
    def __new__(
        cls,
        offsets: pft.TextNamedOffsets,
        overflow: int,
        dataset_len: int,
        bound_is_nextdata: bool,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def offsets(self) -> pft.TextNamedOffsets: ...
    @property
    def overflow(self) -> int: ...
    @property
    def dataset_len(self) -> int: ...
    @property
    def bound_is_nextdata(self) -> bool: ...

@final
class SuppOffsetsOverflow:
    def __new__(
        cls,
        offsets: pft.SuppTEXTNamedOffsets,
        overflow: int,
        dataset_len: int,
        bound_is_nextdata: bool,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def offsets(self) -> pft.SuppTEXTNamedOffsets: ...
    @property
    def overflow(self) -> int: ...
    @property
    def dataset_len(self) -> int: ...
    @property
    def bound_is_nextdata(self) -> bool: ...

@final
class HeaderToHeaderOffsetsOverlap:
    def __new__(
        cls,
        offsets0: pft.HeaderNamedOffsets,
        offsets1: pft.HeaderNamedOffsets,
        overlap: int,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def offsets0(self) -> pft.HeaderNamedOffsets: ...
    @property
    def offsets1(self) -> pft.HeaderNamedOffsets: ...
    @property
    def overlap(self) -> int: ...

@final
class TextToHeaderOffsetsOverlap:
    def __new__(
        cls,
        offsets0: pft.TextNamedOffsets,
        offsets1: pft.HeaderNamedOffsets,
        overlap: int,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def offsets0(self) -> pft.TextNamedOffsets: ...
    @property
    def offsets1(self) -> pft.HeaderNamedOffsets: ...
    @property
    def overlap(self) -> int: ...

@final
class SuppToHeaderOffsetsOverlap:
    def __new__(
        cls,
        offsets0: pft.SuppTEXTNamedOffsets,
        offsets1: pft.HeaderNamedOffsets,
        overlap: int,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def offsets0(self) -> pft.SuppTEXTNamedOffsets: ...
    @property
    def offsets1(self) -> pft.HeaderNamedOffsets: ...
    @property
    def overlap(self) -> int: ...

@final
class TextToHeaderOrSuppOffsetsOverlap:
    def __new__(
        cls,
        offsets0: pft.TextNamedOffsets,
        offsets1: pft.HeaderOrSuppNamedOffsets,
        overlap: int,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def offsets0(self) -> pft.TextNamedOffsets: ...
    @property
    def offsets1(self) -> pft.HeaderOrSuppNamedOffsets: ...
    @property
    def overlap(self) -> int: ...

class PyreflowError(Exception): ...
class FileLayoutError(PyreflowError): ...
class ParseKeyError(PyreflowError): ...
class ParseKeywordValueError(PyreflowError): ...
class InvalidKeywordValueError(PyreflowError): ...
class ExtraKeywordError(PyreflowError): ...
class ConversionError(PyreflowError): ...
class RelationalError(PyreflowError): ...
class EventDataError(PyreflowError): ...
class DataLossError(PyreflowError): ...
class ConfigError(PyreflowError): ...
class WriteFCSError(PyreflowError): ...
class PyreflowWarning(Warning): ...

class _ConfigCommon:
    @classmethod
    def strict(cls) -> dict[str, Any]: ...
    @classmethod
    def scalpel(cls) -> dict[str, Any]: ...
    @classmethod
    def sledgehammer(cls) -> dict[str, Any]: ...

@final
class ReadHeaderConfig(_ConfigCommon):
    pass

@final
class ReadFlatTEXTConfig(_ConfigCommon):
    pass

@final
class ReadStdTEXTConfig(_ConfigCommon):
    pass

@final
class ReadFlatDatasetConfig(_ConfigCommon):
    pass

@final
class ReadStdDatasetConfig(_ConfigCommon):
    pass

@final
class ReadFlatDatasetFromKeywordsConfig(_ConfigCommon):
    pass

class _ConfigCommonNewCore:
    @classmethod
    def strict(cls, version: pft.FCSVersion) -> dict[str, Any]: ...
    @classmethod
    def scalpel(cls, version: pft.FCSVersion) -> dict[str, Any]: ...
    @classmethod
    def sledgehammer(cls, version: pft.FCSVersion) -> dict[str, Any]: ...

@final
class NewCoreTEXTConfig(_ConfigCommonNewCore):
    pass

@final
class NewCoreDatasetConfig(_ConfigCommonNewCore):
    pass

__version__: str

__all__ = [
    "__version__",
    "PyreflowError",
    "FileLayoutError",
    "ParseKeyError",
    "ParseKeywordValueError",
    "InvalidKeywordValueError",
    "ExtraKeywordError",
    "ConversionError",
    "RelationalError",
    "EventDataError",
    "DataLossError",
    "ConfigError",
    "WriteFCSError",
    "PyreflowWarning",
    "CoreTEXT2_0",
    "CoreTEXT3_0",
    "CoreTEXT3_1",
    "CoreTEXT3_2",
    "CoreDataset2_0",
    "CoreDataset3_0",
    "CoreDataset3_1",
    "CoreDataset3_2",
    "Optical2_0",
    "Optical3_0",
    "Optical3_1",
    "Optical3_2",
    "Temporal2_0",
    "Temporal3_0",
    "Temporal3_1",
    "Temporal3_2",
    "UnivariateRegion2_0",
    "UnivariateRegion3_0",
    "UnivariateRegion3_2",
    "BivariateRegion2_0",
    "BivariateRegion3_0",
    "BivariateRegion3_2",
    "GatedMeasurement",
    "FixedAsciiDataSchema",
    "DelimAsciiDataSchema",
    "OrderedUintDataSchema",
    "OrderedF32DataSchema",
    "OrderedF64DataSchema",
    "BigLittleF32DataSchema",
    "BigLittleF64DataSchema",
    "SingleUintDataSchema",
    "VariableUintDataSchema",
    "MixedDataSchema",
    "Header",
    "FinalHeaderOffsets",
    "HeaderToHeaderOffsetsOverlap",
    "TextToHeaderOffsetsOverlap",
    "SuppToHeaderOffsetsOverlap",
    "TextToHeaderOrSuppOffsetsOverlap",
    "HeaderOffsetsOverflow",
    "TextOffsetsOverflow",
    "SuppOffsetsOverflow",
    "SuppTEXTOffsetsOutput",
    "TEXTOffsetsOrigin",
    "HeaderAndSuppOffsets",
    "OriginalHeaderOffsets",
    "FlatTEXTOutput",
    "FlatDatasetOutput",
    "FlatDatasetFromKwsOutput",
    "NewFlatDatasetFromKwsOutput",
    "FlatTEXTDiagnostics",
    "StdTEXTOutput",
    "StdDatasetOutput",
    "StdDatasetFromKwsOutput",
    "NewStdDatasetFromKwsOutput",
    "StdTEXTDiagnostics",
    "RepairDiagnostics",
    "DataSchemaDiagnostics",
    "ValidKeywords",
    "DatasetOffsets",
    "SplitTEXTDiagnostics",
    "DatasetDiagnostics",
    "IntraSegmentDarkBytes",
    "KeywordVersionScore",
    "DatasetSummary",
    "ReadHeaderConfig",
    "ReadFlatTEXTConfig",
    "ReadStdTEXTConfig",
    "ReadFlatDatasetConfig",
    "ReadStdDatasetConfig",
    "ReadFlatDatasetFromKeywordsConfig",
    "NewCoreTEXTConfig",
    "NewCoreDatasetConfig",
    "BuildInfo",
    "fcs_read_header",
    "fcs_read_flat_text",
    "fcs_read_std_text",
    "fcs_read_flat_dataset",
    "fcs_read_std_dataset",
    "fcs_read_flat_texts",
    "fcs_read_std_texts",
    "fcs_read_flat_datasets",
    "fcs_read_std_datasets",
    "fcs_read_flat_dataset_with_keywords",
    "fcs_summarize",
    "fcs_write_datasets",
    "fcs_scan_dataset_boundaries",
]
