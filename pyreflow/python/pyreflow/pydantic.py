import pyreflow.typing as pft
import pyreflow._defaults as pfd
import pyreflow.api as pfa
from pathlib import Path
from importlib.util import find_spec

if find_spec("pydantic") is not None:
    from pydantic import BaseModel as BaseModel_
    from pydantic import ConfigDict
else:
    raise ImportError(
        "This feature requires the optional dependency 'pydantic'. "
        "Install it with: pip install pyreflow[pydantic]"
    )


class BaseModel(BaseModel_):
    model_config = ConfigDict(frozen=True, extra="forbid")


class PyreflowHeaderConfig(BaseModel):
    text_correction: tuple[int, int] = pfd._DEFAULT_CORRECTION
    data_correction: tuple[int, int] = pfd._DEFAULT_CORRECTION
    other_corrections: list[pft.OffsetCorrection] = []
    max_other: int | None = None
    other_width: int = pfd._DEFAULT_OTHER_WIDTH
    squish_offsets: bool = False
    allow_negative: bool = False
    truncate_offsets: bool = False

    def read_header(self, path: Path, dataset_offset: int = 0) -> pfa.Header:
        """Wrapper for :func:`~pyreflow.api.fcs_read_header`."""
        return pfa.fcs_read_header(
            path, dataset_offset=dataset_offset, **self.model_dump()
        )


class _ReadFlatTEXTConfig(BaseModel):
    version_override: pft.FCSVersion | None = None
    supp_text_correction: pft.OffsetCorrection = pfd._DEFAULT_CORRECTION
    allow_overlapping_supp_text: bool = False
    ignore_supp_text: bool = False
    use_literal_delims: bool = False
    allow_non_ascii_delim: bool = False
    allow_missing_final_delim: bool = False
    allow_nonunique: bool = False
    allow_odd: bool = False
    allow_empty: bool = False
    allow_delim_at_boundary: bool = False
    allow_non_utf8: bool = False
    use_latin1: bool = False
    allow_non_ascii_keywords: bool = False
    allow_missing_supp_text: bool = False
    allow_supp_text_own_delim: bool = False
    allow_missing_nextdata: bool = False
    trim_value_whitespace: bool = False
    trim_trailing_whitespace: bool = False
    ignore_standard_keys: pft.KeyPatterns = pfd._DEFAULT_KEY_PATTERNS
    promote_to_standard: pft.KeyPatterns = pfd._DEFAULT_KEY_PATTERNS
    demote_from_standard: pft.KeyPatterns = pfd._DEFAULT_KEY_PATTERNS
    rename_standard_keys: dict[str, str] = {}
    replace_standard_key_values: dict[str, str] = {}
    append_standard_keywords: dict[str, str] = {}
    substitute_standard_key_values: pft.SubPatterns = ({}, {})


class _ReadStdKeywordsConfig(BaseModel):
    dedup_measurement_names: bool = False
    trim_intra_value_whitespace: bool = False
    time_meas_pattern: str | None = pfd._DEFAULT_TIME_MEAS_PATTERN
    allow_missing_time: bool = False
    force_time_linear: bool = False
    ignore_time_optical_keys: list[pft.TemporalOpticalKey] = []
    date_pattern: str | None = None
    time_pattern: str | None = None
    datetime_pattern: str | None = None
    last_modified_pattern: str | None = None
    allow_other_feature: bool = False
    allow_pseudostandard: bool = False
    allow_unused_standard: bool = False
    disallow_deprecated: bool = False
    fix_log_scale_offsets: bool = False
    nonstandard_measurement_pattern: str | None = pfd._DEFAULT_NS_MEAS_PATTERN
    ignore_time_gain: bool = False
    parse_indexed_spillover: bool = False
    disallow_localtime: bool = False


class _ReadDataKeywordsConfig(BaseModel):
    text_data_correction: pft.OffsetCorrection = pfd._DEFAULT_CORRECTION
    text_analysis_correction: pft.OffsetCorrection = pfd._DEFAULT_CORRECTION
    ignore_text_data_offsets: bool = False
    ignore_text_analysis_offsets: bool = False
    allow_header_text_offset_mismatch: bool = False
    allow_missing_required_offsets: bool = False
    truncate_text_offsets: bool = False
    allow_optional_dropping: bool = False
    transfer_dropped_optional: bool = False
    integer_widths_from_byteord: bool = False
    integer_byteord_override: pft.ByteOrd | None = None
    disallow_range_truncation: bool = False


class _ReadEventsConfig(BaseModel):
    allow_uneven_event_width: bool = False
    allow_tot_mismatch: bool = False
    truncate_event_values: pft.TruncateEventValues = "int_only"
    disallow_over_range: bool = False


class _ReadSharedConfig(BaseModel):
    warnings_are_errors: bool = False
    hide_warnings: bool = False


class PyreflowReadFlatTEXTConfig(
    PyreflowHeaderConfig, _ReadFlatTEXTConfig, _ReadSharedConfig
):
    def read_flat_text(
        self,
        path: Path,
        dataset_offset: int = 0,
    ) -> pfa.FlatTEXTOutput:
        """Wrapper for :func:`~pyreflow.api.fcs_read_flat_text`."""
        return pfa.fcs_read_flat_text(
            path, dataset_offset=dataset_offset, **self.model_dump()
        )

    def read_flat_texts(
        self,
        path: Path,
        skip: int | None = None,
        limit: int | None = None,
    ) -> list[pfa.FlatTEXTOutput]:
        """Wrapper for :func:`~pyreflow.api.fcs_read_flat_texts`."""
        return pfa.fcs_read_flat_texts(path, skip, limit, **self.model_dump())


class PyreflowReadStdTEXTConfig(
    PyreflowHeaderConfig,
    _ReadFlatTEXTConfig,
    _ReadDataKeywordsConfig,
    _ReadSharedConfig,
):
    def read_std_text(
        self,
        path: Path,
        dataset_offset: int = 0,
    ) -> tuple[pft.AnyCoreTEXT, pfa.StdTEXTOutput]:
        """Wrapper for :func:`~pyreflow.api.fcs_read_std_text`."""
        return pfa.fcs_read_std_text(
            path, dataset_offset=dataset_offset, **self.model_dump()
        )

    def read_std_texts(
        self,
        path: Path,
        skip: int | None = None,
        limit: int | None = None,
    ) -> list[tuple[pft.AnyCoreTEXT, pfa.StdTEXTOutput]]:
        """Wrapper for :func:`~pyreflow.api.fcs_read_std_texts`."""
        return pfa.fcs_read_std_texts(path, skip, limit, **self.model_dump())


class PyreflowReadFlatDatasetConfig(
    PyreflowHeaderConfig,
    _ReadFlatTEXTConfig,
    _ReadDataKeywordsConfig,
    _ReadEventsConfig,
    _ReadSharedConfig,
):
    def read_flat_dataset(
        self, path: Path, dataset_offset: int = 0
    ) -> pfa.FlatDatasetOutput:
        """Wrapper for :func:`~pyreflow.api.fcs_read_flat_dataset`."""
        return pfa.fcs_read_flat_dataset(
            path, dataset_offset=dataset_offset, **self.model_dump()
        )

    def read_flat_datasets(
        self,
        path: Path,
        skip: int | None = None,
        limit: int | None = None,
    ) -> list[pfa.FlatDatasetOutput]:
        """Wrapper for :func:`~pyreflow.api.fcs_read_flat_datasets`."""
        return pfa.fcs_read_flat_datasets(path, skip, limit, **self.model_dump())

    def summarize(
        self,
        path: Path,
        skip: int | None = None,
        limit: int | None = None,
    ) -> list[pfa.DatasetSummary]:
        """Wrapper for :func:`~pyreflow.api.fcs_summarize`."""
        return pfa.fcs_summarize(path, skip, limit, **self.model_dump())


class PyreflowReadStdDatasetConfig(
    PyreflowHeaderConfig,
    _ReadFlatTEXTConfig,
    _ReadStdKeywordsConfig,
    _ReadDataKeywordsConfig,
    _ReadEventsConfig,
    _ReadSharedConfig,
):
    def read_std_dataset(
        self,
        path: Path,
        dataset_offset: int = 0,
    ) -> tuple[pft.AnyCoreDataset, pfa.StdDatasetOutput]:
        """Wrapper for :func:`~pyreflow.api.fcs_read_std_dataset`."""
        return pfa.fcs_read_std_dataset(
            path, dataset_offset=dataset_offset, **self.model_dump()
        )

    def read_std_datasets(
        self,
        path: Path,
        skip: int | None = None,
        limit: int | None = None,
    ) -> list[tuple[pft.AnyCoreDataset, pfa.StdDatasetOutput]]:
        """Wrapper for :func:`~pyreflow.api.fcs_read_std_datasets`."""
        return pfa.fcs_read_std_datasets(path, skip, limit, **self.model_dump())


class PyreflowReadFlatDatasetFromKeywordsConfig(
    _ReadDataKeywordsConfig,
    _ReadEventsConfig,
    _ReadSharedConfig,
):
    def read_flat_dataset_with_keywords(
        self,
        path: Path,
        dataset_offset: int = 0,
    ) -> pfa.FlatDatasetWithKwsOutput:
        """Wrapper for :func:`~pyreflow.api.fcs_read_flat_dataset_with_keywords`."""
        return pfa.fcs_read_flat_dataset_with_keywords(
            path, dataset_offset=dataset_offset, **self.model_dump()
        )
