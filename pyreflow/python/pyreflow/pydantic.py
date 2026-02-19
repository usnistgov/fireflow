from __future__ import annotations
import pyreflow.typing as pft
import pyreflow.api as pfa
from pathlib import Path
from importlib.util import find_spec
from typing import TypeVar, Type, Self

if find_spec("pydantic") is not None:
    from pydantic import BaseModel as BaseModel_
    from pydantic import ConfigDict
else:
    raise ImportError(
        "This feature requires the optional dependency 'pydantic'. "
        "Install it with: pip install pyreflow[pydantic]"
    )

M = TypeVar("M", bound="BaseModel")

_DEFAULT_CORRECTION = (0, 0)
_DEFAULT_KEY_PATTERNS: pft.KeyPatterns = []
_DEFAULT_TRIFLAG: pft.TriFlag = "false"


class BaseModel(BaseModel_):
    model_config = ConfigDict(extra="forbid")

    def to_parent(self, parent: Type[M]) -> M:
        return parent.model_validate(
            {
                k: v
                for k, v in self.model_dump().items()
                if k in parent.model_fields.keys()
            }
        )


class _HeaderConfig(BaseModel):
    text_correction: pft.OffsetCorrection = _DEFAULT_CORRECTION
    data_correction: pft.OffsetCorrection = _DEFAULT_CORRECTION
    analysis_correction: pft.OffsetCorrection = _DEFAULT_CORRECTION
    other_corrections: list[pft.OffsetCorrection] = []
    max_other: int | None = None
    other_width: int = 8
    guess_other_width: pft.GuessOtherWidth = "none"
    squish_offsets: bool = False


class _OffsetConfig(BaseModel):
    allow_pseudoempty: bool = False
    truncate_offset_limit: int = 0
    overlap_correction_limit: int = 0


class _ReadFlatTEXTConfig(BaseModel):
    version_override: pft.VersionOverride | None = None
    supp_text_correction: pft.OffsetCorrection = _DEFAULT_CORRECTION
    nextdata_correction: int = 0
    allow_duplicated_supp_text: pft.TriFlag = _DEFAULT_TRIFLAG
    ignore_supp_text: bool = False
    delim_escape_mode: pft.DelimEscapeMode = "escaped"
    allow_non_ascii_delim: pft.TriFlag = _DEFAULT_TRIFLAG
    allow_missing_final_delim: pft.TriFlag = _DEFAULT_TRIFLAG
    allow_nonunique: pft.TriFlag = _DEFAULT_TRIFLAG
    allow_odd: pft.TriFlag = _DEFAULT_TRIFLAG
    allow_empty_keys: pft.TriFlag = _DEFAULT_TRIFLAG
    allow_delim_at_boundary: pft.TriFlag = _DEFAULT_TRIFLAG
    use_latin1: bool = False
    allow_non_ascii_keys: pft.TriFlag = _DEFAULT_TRIFLAG
    allow_non_utf8_values: pft.TriFlag = _DEFAULT_TRIFLAG
    allow_missing_supp_text: pft.TriFlag = _DEFAULT_TRIFLAG
    allow_supp_text_own_delim: pft.TriFlag = _DEFAULT_TRIFLAG
    allow_missing_nextdata: pft.TriFlag = _DEFAULT_TRIFLAG
    trim_value_whitespace: pft.TrimValueWhitespace = "notrim"
    trim_text_end: bool = False
    ignore_standard_keys: pft.KeyPatterns = _DEFAULT_KEY_PATTERNS
    promote_to_standard: pft.KeyPatterns = _DEFAULT_KEY_PATTERNS
    demote_from_standard: pft.KeyPatterns = _DEFAULT_KEY_PATTERNS
    rename_standard_keys: dict[str, str] = {}
    replace_standard_key_values: dict[str, str] = {}
    append_standard_keywords: dict[str, str] = {}
    substitute_standard_key_values: pft.SubPatterns = {}


class _ReadStdKeywordsConfig(BaseModel):
    dedup_measurement_names: bool = False
    trim_intra_value_whitespace: bool = False
    time_meas_pattern: str | None = "^(TIME|Time)$"
    allow_missing_time: pft.TriFlag = _DEFAULT_TRIFLAG
    force_linear_scale: pft.ForceLinearScale = "none"
    ignore_time_optical_keys: list[pft.TemporalOpticalKey] = []
    process_time_optical_keys: pft.ProcessTimeOpticalKeys = "demote_warn"
    date_pattern: str | None = None
    time_pattern: str | None = None
    datetime_pattern: str | None = None
    last_modified_pattern: str | None = None
    allow_other_feature: bool = False
    process_pseudostandard: pft.ProcessKeywordFailure = "error"
    process_hyper_par: pft.ProcessKeywordFailure = "error"
    process_other_version: pft.ProcessKeywordFailure = "error"
    process_extra_timestep: pft.ProcessKeywordFailure = "error"
    disallow_deprecated: pft.TriFlag = _DEFAULT_TRIFLAG
    fix_log_scale_offsets: bool = False
    nonstandard_measurement_pattern: str | None = "^P%n"
    spillover_measurement_mode: pft.SpilloverMeasurementMode = "named"
    disallow_localtime: bool = False


class _ReadDataKeywordsConfig(BaseModel):
    text_data_correction: pft.OffsetCorrection = _DEFAULT_CORRECTION
    text_analysis_correction: pft.OffsetCorrection = _DEFAULT_CORRECTION
    ignore_text_data_offsets: bool = False
    ignore_text_analysis_offsets: bool = False
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error"
    allow_missing_required_offsets: pft.TriFlag = _DEFAULT_TRIFLAG
    process_optional_failure: pft.ProcessKeywordFailure = "error"
    integer_widths_from_byteord: bool = False
    integer_byteord_override: pft.ByteOrd | None = None
    disallow_range_truncation: pft.TriFlag = _DEFAULT_TRIFLAG


class _ReadEventsConfig(BaseModel):
    data_remainder_limit: int = 0
    allow_uneven_event_width: pft.TriFlag = _DEFAULT_TRIFLAG
    allow_tot_mismatch: pft.TriFlag = _DEFAULT_TRIFLAG
    truncate_event_values: pft.TruncateEventValues = "int_only"
    disallow_over_range: pft.TriFlag = _DEFAULT_TRIFLAG


class _ReadSharedConfig(BaseModel):
    warnings_are_errors: bool = False
    hide_warnings: bool = False


class _HeaderMethods(BaseModel):
    def to_header_config(self) -> PyreflowReadHeaderConfig:
        """Project this model to :py:class:`~PyreflowReadHeaderConfig`."""
        return self.to_parent(PyreflowReadHeaderConfig)


class _FlatTEXTMethods(BaseModel):
    def to_flat_text_config(self) -> PyreflowReadFlatTEXTConfig:
        """Project this model to :py:class:`~PyreflowReadFlatTEXTConfig`."""
        return self.to_parent(PyreflowReadFlatTEXTConfig)


class _StdTEXTMethods(BaseModel):
    def to_std_text_config(self) -> PyreflowReadStdTEXTConfig:
        """Project this model to :py:class:`~PyreflowReadStdTEXTConfig`."""
        return self.to_parent(PyreflowReadStdTEXTConfig)


class _FlatDatasetMethods(BaseModel):
    def to_flat_dataset_config(self) -> PyreflowReadFlatDatasetConfig:
        """Project this model to :py:class:`~PyreflowReadFlatDatasetConfig`."""
        return self.to_parent(PyreflowReadFlatDatasetConfig)


# NOTE order of _*Config classes is important to preserve order of parameters
# in docs (for some reason its in reverse order)
class PyreflowReadHeaderConfig(_OffsetConfig, _HeaderConfig):
    def read_header(self, path: Path, dataset_offset: int = 0) -> pfa.Header:
        """Wrapper for :func:`~pyreflow.api.fcs_read_header`."""
        return pfa.fcs_read_header(
            path, dataset_offset=dataset_offset, **self.model_dump()
        )

    @classmethod
    def new_scalpal(cls) -> Self:
        """Init to read non-compliant files without data loss."""
        return cls(**pfa.ReadHeaderConfig.scalpal())

    @classmethod
    def new_sledgehammer(cls) -> Self:
        """Init to read non-compliant files maybe with possible metadata loss."""
        return cls(**pfa.ReadHeaderConfig.sledgehammer())


class PyreflowReadFlatTEXTConfig(
    _HeaderMethods,
    _ReadSharedConfig,
    _ReadFlatTEXTConfig,
    _OffsetConfig,
    _HeaderConfig,
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

    @classmethod
    def new_scalpal(cls) -> Self:
        """Init to read non-compliant files without data loss."""
        return cls(**pfa.ReadFlatTEXTConfig.scalpal())

    @classmethod
    def new_sledgehammer(cls) -> Self:
        """Init to read non-compliant files maybe with possible metadata loss."""
        return cls(**pfa.ReadFlatTEXTConfig.sledgehammer())


class PyreflowReadStdTEXTConfig(
    _HeaderMethods,
    _FlatTEXTMethods,
    _ReadSharedConfig,
    _ReadDataKeywordsConfig,
    _ReadStdKeywordsConfig,
    _ReadFlatTEXTConfig,
    _OffsetConfig,
    _HeaderConfig,
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

    @classmethod
    def new_scalpal(cls) -> Self:
        """Init to read non-compliant files without data loss."""
        return cls(**pfa.ReadStdTEXTConfig.scalpal())

    @classmethod
    def new_sledgehammer(cls) -> Self:
        """Init to read non-compliant files maybe with possible metadata loss."""
        return cls(**pfa.ReadStdTEXTConfig.sledgehammer())


class PyreflowReadFlatDatasetConfig(
    _HeaderMethods,
    _FlatTEXTMethods,
    _ReadSharedConfig,
    _ReadEventsConfig,
    _ReadDataKeywordsConfig,
    _ReadFlatTEXTConfig,
    _OffsetConfig,
    _HeaderConfig,
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

    @classmethod
    def new_scalpal(cls) -> Self:
        """Init to read non-compliant files without data loss."""
        return cls(**pfa.ReadFlatDatasetConfig.scalpal())

    @classmethod
    def new_sledgehammer(cls) -> Self:
        """Init to read non-compliant files maybe with possible metadata loss."""
        return cls(**pfa.ReadFlatDatasetConfig.sledgehammer())


class PyreflowReadStdDatasetConfig(
    _HeaderMethods,
    _FlatTEXTMethods,
    _StdTEXTMethods,
    _FlatDatasetMethods,
    _ReadSharedConfig,
    _ReadEventsConfig,
    _ReadDataKeywordsConfig,
    _ReadStdKeywordsConfig,
    _ReadFlatTEXTConfig,
    _OffsetConfig,
    _HeaderConfig,
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

    @classmethod
    def new_scalpal(cls) -> Self:
        """Init to read non-compliant files without data loss."""
        return cls(**pfa.ReadStdDatasetConfig.scalpal())

    @classmethod
    def new_sledgehammer(cls) -> Self:
        """Init to read non-compliant files maybe with possible metadata loss."""
        return cls(**pfa.ReadStdDatasetConfig.sledgehammer())


class PyreflowReadFlatDatasetFromKeywordsConfig(
    _ReadSharedConfig,
    _ReadEventsConfig,
    _ReadDataKeywordsConfig,
    _OffsetConfig,
):
    def read_flat_dataset_with_keywords(
        self,
        path: Path,
        dataset_offset: int = 0,
    ) -> pfa.FlatDatasetFromKwsOutput:
        """Wrapper for :func:`~pyreflow.api.fcs_read_flat_dataset_with_keywords`."""
        return pfa.fcs_read_flat_dataset_with_keywords(
            path, dataset_offset=dataset_offset, **self.model_dump()
        )

    @classmethod
    def new_scalpal(cls) -> Self:
        """Init to read non-compliant files without data loss."""
        return cls(**pfa.ReadFlatDatasetFromKeywordsConfig.scalpal())

    @classmethod
    def new_sledgehammer(cls) -> Self:
        """Init to read non-compliant files maybe with possible metadata loss."""
        return cls(**pfa.ReadFlatDatasetFromKeywordsConfig.sledgehammer())
