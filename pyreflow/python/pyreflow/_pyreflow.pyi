from __future__ import annotations
from pathlib import Path
from datetime import time, date, datetime
from typing import TypeVar, Self, Generic, Union, final, Any

from polars import Series, DataFrame
import numpy as np
import numpy.typing as npt

from pyreflow.typing import (
    MeasIndex,
    Range,
    ByteOrd,
    Endian,
    IntRange,
    FloatRange,
    Timestep,
    Display,
    Scale,
    ScaleTransform,
    Mode,
    Mode3_2,
    Trigger,
    Shortname,
    StdKeywords,
    NonStdKeywords,
    AnalysisBytes,
    OtherBytes,
    Datatype,
    MixedType,
    Originality,
    Feature,
    Calibration3_1,
    Calibration3_2,
    AppliedGates2_0,
    AppliedGates3_0,
    AppliedGates3_2,
    Unicode,
    CsvFlags,
    Compensation,
    Spillover,
    UnstainedCenters,
    FCSVersion,
    TemporalOpticalKey,
    Segment,
    OffsetCorrection,
    KeyPatterns,
    AnyCoreTEXT,
    AnyCoreDataset,
    SubPatterns,
    ReqOrOpt,
    RootOrMeas,
    TruncateEventValues,
    DelimEscapeMode,
    VersionOverride,
    ProcessKeywordFailure,
    TriFlag,
    ForceLinearScale,
    MeasScaleDiagnostic,
    GateScaleDiagnostic,
    TrimValueWhitespace,
    SpilloverMeasurementMode,
    ProcessTimeOpticalKeys,
    KeywordVersionScores,
    GuessOtherWidth,
    OtherSegments,
    AllowHeaderTextOffsetMismatch,
)

_X = TypeVar("_X")
_Y = TypeVar("_Y")
_C = TypeVar("_C")
_N = TypeVar("_N")
_L = TypeVar("_L")

_OpticalKeyVals = list[_X | tuple[()] | None]

class _LayoutUnmixedCommon:
    @property
    def datatype(self) -> Datatype: ...
    def __deepcopy__(self, memo: Any) -> Self: ...

class _LayoutEndianCommon:
    @property
    def endian(self) -> Endian: ...
    def __deepcopy__(self, memo: Any) -> Self: ...

class _LayoutAsciiCommon(_LayoutUnmixedCommon):
    def __new__(cls, ranges: list[IntRange]) -> Self: ...

class _LayoutOrderedUintCommon(_LayoutUnmixedCommon):
    def __new__(cls, ranges: list[IntRange], byteord: ByteOrd = "little") -> Self: ...
    @property
    def ranges(self) -> list[IntRange]: ...
    @property
    def byteord(self) -> ByteOrd: ...
    @property
    def byte_width(self) -> int: ...

class _LayoutOrderedFloatCommon(_LayoutUnmixedCommon):
    def __new__(cls, ranges: list[FloatRange], byteord: ByteOrd = "little") -> Self: ...
    @property
    def ranges(self) -> list[FloatRange]: ...
    @property
    def byteord(self) -> ByteOrd: ...
    @property
    def byte_width(self) -> int: ...

class _LayoutEndianFloatCommon(_LayoutUnmixedCommon):
    def __new__(cls, ranges: list[FloatRange], endian: Endian = "little") -> Self: ...
    @property
    def ranges(self) -> list[FloatRange]: ...
    @property
    def endian(self) -> Endian: ...
    @property
    def byte_width(self) -> int: ...

@final
class FixedAsciiLayout(
    _LayoutAsciiCommon,
    _LayoutUnmixedCommon,
):
    def __new__(cls, ranges: list[IntRange]) -> Self: ...
    @property
    def ranges(self) -> list[FloatRange]: ...
    @property
    def char_widths(self) -> list[int | float]: ...

@final
class DelimAsciiLayout(_LayoutAsciiCommon, _LayoutUnmixedCommon):
    def __new__(cls, ranges: list[IntRange]) -> Self: ...
    @property
    def ranges(self) -> list[IntRange]: ...

@final
class OrderedUint08Layout(_LayoutUnmixedCommon):
    def __new__(cls, ranges: list[IntRange]) -> Self: ...
    @property
    def ranges(self) -> list[FloatRange]: ...
    @property
    def byte_width(self) -> int: ...

@final
class OrderedUint16Layout(_LayoutUnmixedCommon):
    def __new__(cls, ranges: list[IntRange], endian: Endian = "little") -> Self: ...
    @property
    def ranges(self) -> list[FloatRange]: ...
    @property
    def endian(self) -> Endian: ...
    @property
    def byte_width(self) -> int: ...

@final
class OrderedUint24Layout(_LayoutOrderedUintCommon): ...

@final
class OrderedUint32Layout(_LayoutOrderedUintCommon): ...

@final
class OrderedUint40Layout(_LayoutOrderedUintCommon): ...

@final
class OrderedUint48Layout(_LayoutOrderedUintCommon): ...

@final
class OrderedUint56Layout(_LayoutOrderedUintCommon): ...

@final
class OrderedUint64Layout(_LayoutOrderedUintCommon): ...

@final
class OrderedF32Layout(_LayoutOrderedFloatCommon): ...

@final
class OrderedF64Layout(_LayoutOrderedFloatCommon): ...

@final
class EndianF32Layout(_LayoutEndianCommon, _LayoutEndianFloatCommon): ...

@final
class EndianF64Layout(_LayoutEndianCommon, _LayoutEndianFloatCommon): ...

@final
class EndianUintLayout(_LayoutEndianCommon, _LayoutUnmixedCommon):
    def __new__(cls, ranges: list[IntRange], endian: Endian = "little") -> Self: ...
    @property
    def ranges(self) -> list[IntRange]: ...
    @property
    def endian(self) -> Endian: ...
    @property
    def byte_widths(self) -> list[int]: ...

@final
class MixedLayout(_LayoutEndianCommon):
    def __new__(
        cls, typed_ranges: list[MixedType], endian: Endian = "little"
    ) -> Self: ...
    @property
    def typed_ranges(self) -> list[MixedType]: ...
    @property
    def endian(self) -> Endian: ...
    @property
    def byte_widths(self) -> list[int]: ...

_AnyOrderedLayout = Union[
    FixedAsciiLayout
    | DelimAsciiLayout
    | OrderedUint08Layout
    | OrderedUint16Layout
    | OrderedUint24Layout
    | OrderedUint32Layout
    | OrderedUint40Layout
    | OrderedUint48Layout
    | OrderedUint56Layout
    | OrderedUint64Layout
    | OrderedF32Layout
    | OrderedF64Layout
]

_AnyNonMixedLayout = Union[
    FixedAsciiLayout
    | DelimAsciiLayout
    | EndianF32Layout
    | EndianF64Layout
    | EndianUintLayout
]

_AnyMixedLayout = Union[
    FixedAsciiLayout
    | DelimAsciiLayout
    | EndianF32Layout
    | EndianF64Layout
    | EndianUintLayout
    | MixedLayout
]

class _MeasCommon:
    nonstandard_keywords: NonStdKeywords
    longname: str

    def __deepcopy__(self, memo: Any) -> Self: ...

class _OpticalWavelength:
    wavelength: float | None

class _OpticalWavelengths:
    wavelengths: list[float]

class _MeasDisplay:
    display: Display

class _PeakCommon:
    size: int
    bin: int

class _OpticalCommon:
    filter: str
    detector_type: str
    detector_voltage: float | None
    power: float | None
    percent_emitted: float | None

class _OpticalScaleTransform:
    transform: ScaleTransform

class _TemporalTimestep:
    timestep: Timestep

@final
class Optical2_0(_MeasCommon, _OpticalCommon, _OpticalWavelength, _PeakCommon):
    scale: Scale | None

    def __new__(
        cls,
        scale: Scale | None = None,
        wavelength: float | None = None,
        bin: int | None = None,
        size: int | None = None,
        filter: str = "",
        power: float | None = None,
        detector_type: str = "",
        percent_emitted: float | None = None,
        detector_voltage: float | None = None,
        longname: str = "",
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...

@final
class Optical3_0(
    _MeasCommon, _OpticalCommon, _OpticalScaleTransform, _OpticalWavelength, _PeakCommon
):
    def __new__(
        cls,
        transform: ScaleTransform,
        wavelength: float | None = None,
        bin: int | None = None,
        size: int | None = None,
        filter: str = "",
        power: float | None = None,
        detector_type: str = "",
        percent_emitted: float | None = None,
        detector_voltage: float | None = None,
        longname: str = "",
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...

@final
class Optical3_1(
    _MeasCommon,
    _OpticalCommon,
    _OpticalScaleTransform,
    _OpticalWavelengths,
    _MeasDisplay,
    _PeakCommon,
):
    calibration: Calibration3_1 | None

    def __new__(
        cls,
        transform: ScaleTransform,
        wavelengths: list[float] = [],
        calibration: Calibration3_1 | None = None,
        display: Display | None = None,
        bin: int | None = None,
        size: int | None = None,
        filter: str = "",
        power: float | None = None,
        detector_type: str = "",
        percent_emitted: float | None = None,
        detector_voltage: float | None = None,
        longname: str = "",
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...

@final
class Optical3_2(
    _MeasCommon,
    _OpticalCommon,
    _OpticalScaleTransform,
    _OpticalWavelengths,
    _MeasDisplay,
):
    calibration: Calibration3_2 | None
    detector_name: str | None
    tag: str | None
    measurement_type: str
    feature: str | None
    awh_feature: Feature | None
    analyte: str | None

    def __new__(
        cls,
        transform: ScaleTransform,
        wavelengths: list[float] = [],
        calibration: Calibration3_2 | None = None,
        display: Display | None = None,
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
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...

@final
class Temporal2_0(_MeasCommon, _PeakCommon):
    def __new__(
        cls,
        has_scale: bool = False,
        bin: int | None = None,
        size: int | None = None,
        longname: str = "",
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...

    has_scale: bool

@final
class Temporal3_0(_MeasCommon, _TemporalTimestep, _PeakCommon):
    def __new__(
        cls,
        timestep: float,
        bin: int | None = None,
        size: int | None = None,
        longname: str = "",
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...

@final
class Temporal3_1(_MeasCommon, _MeasDisplay, _TemporalTimestep, _PeakCommon):
    def __new__(
        cls,
        timestep: float,
        display: Display | None = None,
        bin: int | None = None,
        size: int | None = None,
        longname: str = "",
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...

@final
class Temporal3_2(_MeasCommon, _MeasDisplay, _TemporalTimestep):
    has_type: bool

    def __new__(
        cls,
        timestep: float,
        display: Display | None = None,
        has_type: bool = False,
        longname: str = "",
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...

_T = TypeVar("_T", bound=Temporal2_0 | Temporal3_0 | Temporal3_1 | Temporal3_2)
_O = TypeVar("_O", bound=Optical2_0 | Optical3_0 | Optical3_1 | Optical3_2)

_FlatInput = list[tuple[_N, _O] | tuple[Shortname, _T]]

@final
class GatedMeasurement:
    def __new__(
        cls,
        scale: tuple[()] | tuple[float, float] | None = None,
        filter: str = "",
        shortname: str | None = None,
        percent_emitted: float | None = None,
        range: float | None = None,
        longname: str = "",
        detector_type: str = "",
        detector_voltage: float | None = None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    scale: tuple[()] | tuple[float, float] | None
    filter: str
    shortname: str | None
    percent_emitted: float | None
    range: float | None
    longname: str
    detector_type: str
    detector_voltage: float | None

class _UnivariateRegion(Generic[_X]):
    def __new__(
        cls,
        index: _X,
        gate: tuple[float, float],
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def index(self) -> _X: ...
    @property
    def gate(self) -> tuple[float, float]: ...

class _BivariateRegion(Generic[_X]):
    def __new__(
        cls,
        index: tuple[_X, _X],
        vertices: list[tuple[float, float]],
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def index(self) -> tuple[_X, _X]: ...
    @property
    def vertices(self) -> list[tuple[float, float]]: ...

@final
class UnivariateRegion2_0(_UnivariateRegion[int]):
    pass

@final
class UnivariateRegion3_0(_UnivariateRegion[str]):
    pass

@final
class UnivariateRegion3_2(_UnivariateRegion[int]):
    pass

@final
class BivariateRegion2_0(_BivariateRegion[int]):
    pass

@final
class BivariateRegion3_0(_BivariateRegion[str]):
    pass

@final
class BivariateRegion3_2(_BivariateRegion[int]):
    pass

class _CoreCommon:
    abrt: int | None
    cells: str | None
    com: str | None
    exp: str | None
    fil: str | None
    inst: str | None
    lost: int | None
    op: str | None
    proj: str | None
    smno: str | None
    src: str | None
    sys: str | None
    btim: time | None
    etim: time | None
    date: date | None
    tr: Trigger | None

    all_shortnames: list[Shortname]
    all_longnames: list[str | None]

    all_filters: _OpticalKeyVals[str]
    all_powers: _OpticalKeyVals[float]
    all_percents_emitted: _OpticalKeyVals[int]
    all_detector_types: _OpticalKeyVals[str]
    all_detector_voltages: _OpticalKeyVals[float]
    all_meas_nonstandard_keywords: list[NonStdKeywords]

    nonstandard_keywords: NonStdKeywords
    def standard_keywords(
        self,
        req_or_opt: ReqOrOpt,
        root_or_meas: RootOrMeas,
    ) -> dict[str, str]: ...
    @property
    def par(self) -> int: ...
    def set_trigger_threshold(self, threshold: int) -> bool: ...
    def write_text(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        appendable: bool = False,
        append: bool = False,
    ) -> None: ...
    @classmethod
    def write_texts(
        cls,
        path: Path,
        datasets: list[Self],
        delim: int = 30,
        big_other: bool = False,
    ) -> None: ...
    @property
    def version(self) -> FCSVersion: ...
    def __deepcopy__(self, memo: Any) -> Self: ...

class _CoreDatasetCommon:
    def write_dataset(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        skip_conversion_check: bool = False,
        appendable: bool = False,
        append: bool = False,
    ) -> None: ...
    @classmethod
    def write_datasets(
        cls,
        path: Path,
        datasets: list[Self],
        delim: int = 30,
        big_other: bool = False,
        skip_conversion_check: bool = False,
    ) -> None: ...

class _CoreShortnamesMaybe:
    all_shortnames_maybe: list[Shortname | None]

class _CoreTemporal2_0:
    def set_temporal(self, name: Shortname, allow_loss: TriFlag = "false") -> bool: ...
    def set_temporal_at(
        self, index: MeasIndex, allow_loss: TriFlag = "false"
    ) -> bool: ...
    def unset_temporal(self) -> bool: ...

class _CoreTemporal3_0:
    def set_temporal(
        self,
        name: Shortname,
        timestep: Timestep,
        allow_loss: TriFlag = "false",
    ) -> bool: ...
    def set_temporal_at(
        self,
        index: MeasIndex,
        timestep: Timestep,
        allow_loss: TriFlag = "false",
    ) -> bool: ...
    def unset_temporal(self) -> float | None: ...

class _CoreTemporal3_2:
    def set_temporal(
        self,
        name: Shortname,
        timestep: Timestep,
        allow_loss: TriFlag = "false",
    ) -> bool: ...
    def set_temporal_at(
        self, index: MeasIndex, timestep: Timestep, allow_loss: TriFlag = "false"
    ) -> bool: ...
    def unset_temporal(self, allow_loss: TriFlag = "false") -> float | None: ...

class _CoreGetSetMeas(Generic[_N, _O, _T]):
    @property
    def temporal(self) -> tuple[MeasIndex, Shortname, _T] | None: ...
    @property
    def measurements(self) -> list[_O | _T]: ...
    def measurement_at(self, index: MeasIndex) -> _O | _T: ...
    def measurement_named(self, name: Shortname) -> _O | _T: ...
    def replace_optical_at(self, index: MeasIndex, meas: _O) -> _O | _T: ...
    def replace_optical_named(self, name: Shortname, meas: _O) -> _O | _T | None: ...
    def rename_temporal(self, name: Shortname) -> Shortname | None: ...

class _CoreTEXTRemove(Generic[_N, _O, _T]):
    def remove_measurement_by_name(
        self, name: Shortname
    ) -> tuple[MeasIndex, _O | _T, Range]: ...
    def remove_measurement_by_index(
        self, index: MeasIndex
    ) -> tuple[_N, _O | _T, Range]: ...

class _CoreDatasetRemove(Generic[_N, _O, _T]):
    def remove_measurement_by_name(
        self, name: Shortname
    ) -> tuple[MeasIndex, _O | _T, Series, Range]: ...
    def remove_measurement_by_index(
        self, index: MeasIndex
    ) -> tuple[_N, _O | _T, Series, Range]: ...

class _CoreReplaceTemporal2_0(Generic[_N, _O, _T]):
    def replace_temporal_at(self, index: MeasIndex, meas: _T) -> _O | _T: ...
    def replace_temporal_named(self, name: Shortname, meas: _T) -> _O | _T | None: ...

class _CoreReplaceTemporal3_2:
    def replace_temporal_at(
        self,
        index: MeasIndex,
        meas: Temporal3_2,
        allow_loss: TriFlag = "false",
    ) -> Optical3_2 | Temporal3_2: ...
    def replace_temporal_named(
        self,
        name: Shortname,
        meas: Temporal3_2,
        allow_loss: TriFlag = "false",
    ) -> Optical3_2 | Temporal3_2 | None: ...

class _CoreTEXTGetSetMeas(Generic[_N, _T, _O]):
    def push_optical(
        self, name: _N, meas: _O, range: Range, disallow_trunc: TriFlag = "false"
    ) -> None: ...
    def insert_optical(
        self,
        index: MeasIndex,
        name: _N,
        meas: _O,
        range: Range,
        disallow_trunc: TriFlag = "false",
    ) -> None: ...
    def push_temporal(
        self,
        name: Shortname,
        meas: _T,
        range: Range,
        disallow_trunc: TriFlag = "false",
    ) -> None: ...
    def insert_temporal(
        self,
        index: MeasIndex,
        name: Shortname,
        meas: _T,
        range: Range,
        disallow_trunc: TriFlag = "false",
    ) -> None: ...
    def unset_measurements(self) -> None: ...

class _CoreDatasetGetSetMeas(Generic[_N, _T, _O]):
    analysis: AnalysisBytes
    others: list[OtherBytes]

    def push_optical(
        self,
        name: _N,
        meas: _O,
        col: Series,
        range: Range,
        disallow_trunc: TriFlag = "false",
    ) -> None: ...
    def insert_optical(
        self,
        index: MeasIndex,
        name: _N,
        meas: _O,
        col: Series,
        range: Range,
        disallow_trunc: TriFlag = "false",
    ) -> None: ...
    def push_temporal(
        self,
        name: Shortname,
        meas: _T,
        col: Series,
        range: Range,
        disallow_trunc: TriFlag = "false",
    ) -> None: ...
    def insert_temporal(
        self,
        index: MeasIndex,
        name: Shortname,
        meas: _T,
        col: Series,
        range: Range,
        disallow_trunc: TriFlag = "false",
    ) -> None: ...
    def unset_data(self) -> None: ...
    def truncate_data(self, skip_conv_check: bool = False) -> None: ...
    data: DataFrame
    def set_measurements_and_data(
        self,
        measurements: list[_O | _T],
        data: DataFrame,
    ) -> None: ...

class _CoreGetSetMeasOrdered(Generic[_O, _T]):
    layout: _AnyOrderedLayout

    def set_named_measurements(
        self,
        measurements: _FlatInput[Shortname | None, _O, _T],
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements_and_layout(
        self,
        measurements: _FlatInput[Shortname | None, _O, _T],
        layout: _AnyOrderedLayout,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_measurements_and_layout(
        self,
        measurements: list[_O | _T],
        layout: _AnyOrderedLayout,
    ) -> None: ...

class _CoreGetSetMeasEndian(Generic[_L, _O, _T]):
    layout: _L

    def set_named_measurements(
        self,
        measurements: _FlatInput[Shortname, _O, _T],
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements_and_layout(
        self,
        measurements: _FlatInput[Shortname, _O, _T],
        layout: _L,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_measurements_and_layout(
        self,
        measurements: list[_O | _T],
        layout: _L,
    ) -> None: ...

class _CoreDatasetGetSetMeasOrdered(Generic[_O, _T]):
    def set_named_measurements_and_data(
        self,
        measurements: _FlatInput[Shortname | None, _O, _T],
        data: DataFrame,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_measurements_layout_and_data(
        self,
        measurements: list[_O | _T],
        layout: _AnyOrderedLayout,
        data: DataFrame,
    ) -> None: ...

class _CoreDatasetGetSetMeasEndian(Generic[_O, _T, _L]):
    def set_named_measurements_and_data(
        self,
        measurements: _FlatInput[Shortname, _O, _T],
        data: DataFrame,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_measurements_layout_and_data(
        self,
        measurements: list[_O | _T],
        layout: _L,
        data: DataFrame,
    ) -> None: ...

class _CoreSetShortnamesMaybe:
    def set_measurement_shortnames_maybe(
        self, names: list[Shortname | None]
    ) -> None: ...

class _CoreScaleMethods:
    all_scales: list[Scale | None]

class _CoreScaleTransformMethods:
    all_scale_transforms: list[ScaleTransform]

class _CoreTimestepMethods:
    @property
    def timestep(self) -> Timestep | None: ...
    def set_timestep(self, timestep: Timestep) -> Timestep | None: ...

class _CoreGates(Generic[_X]):
    applied_gates: _X

class _CoreSubset:
    @property
    def cstot(self) -> int: ...
    @property
    def csvbits(self) -> int: ...
    @property
    def csvflags(self) -> CsvFlags: ...

class _CoreModified:
    originality: Originality | None
    last_modified: datetime | None
    last_modifier: str | None

class _CorePlate:
    platename: str | None
    plateid: str | None
    wellid: str | None

class _CoreCompensation:
    compensation: npt.NDArray[np.float32] | None

class _CoreSpillover:
    spillover: Spillover | None

class _CoreUnicode:
    unicode: Unicode | None

class _CoreVol:
    vol: float | None

class _CoreCytsn:
    cytsn: str | None

class _CorePeak:
    all_peak_bins: list[int]
    all_peak_sizes: list[int]

class _CoreMeasWavelength:
    all_wavelengths: _OpticalKeyVals[float]

class _CoreMeasWavelengths:
    all_wavelengths: _OpticalKeyVals[list[float]]

class _CoreMeasDisplay:
    all_displays: list[Display | None]

class _CorePre3_1:
    comp: npt.NDArray[np.float32] | None

class _CorePre3_2:
    mode: Mode
    cyt: str | None

class _Core3_2:
    mode: Mode3_2 | None
    flowrate: str | None
    cyt: str
    unstainedinfo: str | None
    unstainedcenters: dict[Shortname, float]
    carriertype: str | None
    carrierid: str | None
    locationid: str | None
    begindatetime: datetime | None
    enddatetime: datetime | None

    all_detector_names: _OpticalKeyVals[str]
    all_tags: _OpticalKeyVals[str]
    all_features: _OpticalKeyVals[str]
    all_awh_features: _OpticalKeyVals[Feature]
    all_other_features: _OpticalKeyVals[str]
    all_analytes: _OpticalKeyVals[str]
    all_measurement_types: list[str | bool]

class _CoreMeasCalibration(Generic[_C]):
    all_calibrations: _OpticalKeyVals[_C]

class _CoreToDataset(Generic[_X]):
    def to_dataset(
        self,
        data: DataFrame,
        analysis: AnalysisBytes = b"",
        others: list[OtherBytes] = [],
    ) -> _X: ...

class _CoreTo2_0(Generic[_X]):
    def to_version_2_0(self, allow_loss: TriFlag = "false") -> _X: ...

class _CoreTo3_0(Generic[_X]):
    def to_version_3_0(self, allow_loss: TriFlag = "false") -> _X: ...

class _CoreTo3_1(Generic[_X]):
    def to_version_3_1(self, allow_loss: TriFlag = "false") -> _X: ...

class _CoreTo3_2(Generic[_X]):
    def to_version_3_2(self, allow_loss: TriFlag = "false") -> _X: ...

@final
class CoreTEXT2_0(
    _CoreCommon,
    _CorePre3_1,
    _CorePre3_2,
    _CoreTemporal2_0,
    _CoreShortnamesMaybe,
    _CoreGetSetMeas[Shortname | None, Optical2_0, Temporal2_0],
    _CoreTEXTRemove[Shortname | None, Optical2_0, Temporal2_0],
    _CoreReplaceTemporal2_0[Shortname | None, Optical2_0, Temporal2_0],
    _CoreTEXTGetSetMeas[Shortname | None, Temporal2_0, Optical2_0],
    _CoreGetSetMeasOrdered[Optical2_0, Temporal2_0],
    _CoreSetShortnamesMaybe,
    _CoreScaleMethods,
    _CoreToDataset[CoreDataset2_0],
    _CoreCompensation,
    _CoreMeasWavelength,
    _CorePeak,
    _CoreGates[AppliedGates2_0],
    _CoreTo3_0[CoreTEXT3_0],
    _CoreTo3_1[CoreTEXT3_1],
    _CoreTo3_2[CoreTEXT3_2],
):
    def __new__(
        cls,
        measurements: list[tuple[Shortname | None, Optical2_0 | Temporal2_0]],
        layout: _AnyOrderedLayout,
        mode: Mode = "L",
        cyt: str = "",
        comp: npt.NDArray[np.float32] | None = None,
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
        tr: Trigger | None = None,
        applied_gates: AppliedGates2_0 = ([], {}, None),
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        std: StdKeywords,
        nonstd: NonStdKeywords,
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = "^(TIME|Time)$",
        allow_missing_time: TriFlag = "false",
        force_linear_scale: ForceLinearScale = "none",
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        process_time_optical_keys: ProcessTimeOpticalKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: ProcessKeywordFailure = "error",
        process_hyper_par: ProcessKeywordFailure = "error",
        process_other_version: ProcessKeywordFailure = "error",
        process_extra_timestep: ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = "^P%n",
        process_optional_failure: ProcessKeywordFailure = "error",
        integer_widths_from_byteord: bool = False,
        integer_byteord_override: list[int] | None = None,
        disallow_range_truncation: TriFlag = "false",
        disallow_deprecated: TriFlag = "false",
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
    ) -> Self: ...

@final
class CoreTEXT3_0(
    _CoreCommon,
    _CorePre3_1,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreShortnamesMaybe,
    _CoreGetSetMeas[Shortname | None, Optical3_0, Temporal3_0],
    _CoreTEXTRemove[Shortname | None, Optical3_0, Temporal3_0],
    _CoreReplaceTemporal2_0[Shortname | None, Optical2_0, Temporal2_0],
    _CoreTEXTGetSetMeas[Shortname | None, Temporal3_0, Optical3_0],
    _CoreGetSetMeasOrdered[Optical3_0, Temporal3_0],
    _CoreSetShortnamesMaybe,
    _CoreScaleTransformMethods,
    _CoreTimestepMethods,
    _CoreToDataset[CoreDataset3_0],
    _CoreCompensation,
    _CoreUnicode,
    _CoreCytsn,
    _CoreMeasWavelength,
    _CorePeak,
    _CoreSubset,
    _CoreGates[AppliedGates3_0],
    _CoreTo2_0[CoreTEXT2_0],
    _CoreTo3_1[CoreTEXT3_1],
    _CoreTo3_2[CoreTEXT3_2],
):
    def __new__(
        cls,
        measurements: list[tuple[Shortname | None, Optical3_0 | Temporal3_0]],
        layout: _AnyOrderedLayout,
        mode: Mode = "L",
        cyt: str = "",
        comp: Compensation | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        cytsn: str = "",
        unicode: Unicode | None = None,
        csvbits: int = 0,
        cstot: int = 0,
        csvflags: CsvFlags = [],
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
        tr: Trigger | None = None,
        applied_gates: AppliedGates3_0 = ([], {}, None),
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        std: StdKeywords,
        nonstd: NonStdKeywords,
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = "^(TIME|Time)$",
        allow_missing_time: TriFlag = "false",
        force_linear_scale: ForceLinearScale = "none",
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        process_time_optical_keys: ProcessTimeOpticalKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: ProcessKeywordFailure = "error",
        process_hyper_par: ProcessKeywordFailure = "error",
        process_other_version: ProcessKeywordFailure = "error",
        process_extra_timestep: ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = "^P%n",
        add_missing_timestep: float | None = None,
        text_data_correction: OffsetCorrection = (0, 0),
        text_analysis_correction: OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: TriFlag = "false",
        process_optional_failure: ProcessKeywordFailure = "error",
        integer_widths_from_byteord: bool = False,
        integer_byteord_override: list[int] | None = None,
        disallow_range_truncation: TriFlag = "false",
        disallow_deprecated: TriFlag = "false",
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
    ) -> Self: ...

@final
class CoreTEXT3_1(
    _CoreCommon,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_1, Temporal3_1],
    _CoreTEXTRemove[Shortname | None, Optical3_1, Temporal3_1],
    _CoreReplaceTemporal2_0[Shortname | None, Optical2_0, Temporal2_0],
    _CoreTEXTGetSetMeas[Shortname, Temporal3_1, Optical3_1],
    _CoreGetSetMeasEndian[_AnyNonMixedLayout, Optical3_1, Temporal3_1],
    _CoreScaleTransformMethods,
    _CoreTimestepMethods,
    _CoreToDataset[CoreDataset3_1],
    _CoreSubset,
    _CoreModified,
    _CorePlate,
    _CoreSpillover,
    _CoreVol,
    _CoreCytsn,
    _CoreMeasWavelengths,
    _CorePeak,
    _CoreMeasDisplay,
    _CoreMeasCalibration[Calibration3_1],
    _CoreGates[AppliedGates3_0],
    _CoreTo2_0[CoreTEXT2_0],
    _CoreTo3_0[CoreTEXT3_0],
    _CoreTo3_2[CoreTEXT3_2],
):
    def __new__(
        cls,
        measurements: list[tuple[Shortname, Optical3_1 | Temporal3_1]],
        layout: _AnyNonMixedLayout,
        mode: Mode = "L",
        cyt: str = "",
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        cytsn: str = "",
        spillover: Spillover | None = None,
        last_modifier: str = "",
        last_modified: datetime | None = None,
        originality: Originality | None = None,
        plateid: str = "",
        platename: str = "",
        wellid: str = "",
        vol: float | None = None,
        csvbits: int = 0,
        cstot: int = 0,
        csvflags: CsvFlags = [],
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
        tr: Trigger | None = None,
        applied_gates: AppliedGates3_0 = ([], {}, None),
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        std: StdKeywords,
        nonstd: NonStdKeywords,
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = "^(TIME|Time)$",
        allow_missing_time: TriFlag = "false",
        force_linear_scale: ForceLinearScale = "none",
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        process_time_optical_keys: ProcessTimeOpticalKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: ProcessKeywordFailure = "error",
        process_hyper_par: ProcessKeywordFailure = "error",
        process_other_version: ProcessKeywordFailure = "error",
        process_extra_timestep: ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = "^P%n",
        add_missing_timestep: float | None = None,
        spillover_measurement_mode: SpilloverMeasurementMode = "named",
        text_data_correction: OffsetCorrection = (0, 0),
        text_analysis_correction: OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: TriFlag = "false",
        process_optional_failure: ProcessKeywordFailure = "error",
        disallow_range_truncation: TriFlag = "false",
        disallow_deprecated: TriFlag = "false",
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
    ) -> Self: ...

@final
class CoreTEXT3_2(
    _CoreCommon,
    _Core3_2,
    _CoreTemporal3_2,
    _CoreGetSetMeas[Shortname, Optical3_2, Temporal3_2],
    _CoreTEXTRemove[Shortname | None, Optical3_2, Temporal3_2],
    _CoreReplaceTemporal3_2,
    _CoreTEXTGetSetMeas[Shortname, Temporal3_2, Optical3_2],
    _CoreGetSetMeasEndian[_AnyMixedLayout, Optical3_2, Temporal3_2],
    _CoreScaleTransformMethods,
    _CoreTimestepMethods,
    _CoreToDataset[CoreDataset3_2],
    _CoreModified,
    _CorePlate,
    _CoreSpillover,
    _CoreVol,
    _CoreCytsn,
    _CoreMeasWavelengths,
    _CoreMeasDisplay,
    _CoreMeasCalibration[Calibration3_2],
    _CoreGates[AppliedGates3_2],
    _CoreTo2_0[CoreTEXT2_0],
    _CoreTo3_0[CoreTEXT3_0],
    _CoreTo3_1[CoreTEXT3_1],
):
    def __new__(
        cls,
        measurements: list[tuple[Shortname, Optical3_2 | Temporal3_2]],
        layout: _AnyMixedLayout,
        cyt: str,
        mode: Mode3_2 | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        begindatetime: datetime | None = None,
        enddatetime: datetime | None = None,
        cytsn: str = "",
        spillover: Spillover | None = None,
        last_modifier: str = "",
        last_modified: datetime | None = None,
        originality: Originality | None = None,
        plateid: str = "",
        platename: str = "",
        wellid: str = "",
        vol: float | None = None,
        carrierid: str = "",
        carriertype: str = "",
        locationid: str = "",
        unstainedinfo: str = "",
        unstainedcenters: UnstainedCenters = {},
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
        tr: Trigger | None = None,
        applied_gates: AppliedGates3_2 = ({}, None),
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        std: StdKeywords,
        nonstd: NonStdKeywords,
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = "^(TIME|Time)$",
        allow_missing_time: TriFlag = "false",
        force_linear_scale: ForceLinearScale = "none",
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        process_time_optical_keys: ProcessTimeOpticalKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: ProcessKeywordFailure = "error",
        process_hyper_par: ProcessKeywordFailure = "error",
        process_other_version: ProcessKeywordFailure = "error",
        process_extra_timestep: ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = "^P%n",
        add_missing_timestep: float | None = None,
        spillover_measurement_mode: SpilloverMeasurementMode = "named",
        disallow_localtime: bool = False,
        text_data_correction: OffsetCorrection = (0, 0),
        text_analysis_correction: OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: TriFlag = "false",
        process_optional_failure: ProcessKeywordFailure = "error",
        disallow_range_truncation: TriFlag = "false",
        disallow_deprecated: TriFlag = "false",
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
    ) -> Self: ...

@final
class CoreDataset2_0(
    _CoreCommon,
    _CorePre3_1,
    _CorePre3_2,
    _CoreTemporal2_0,
    _CoreShortnamesMaybe,
    _CoreGetSetMeas[Shortname | None, Optical2_0, Temporal2_0],
    _CoreDatasetRemove[Shortname | None, Optical2_0, Temporal2_0],
    _CoreReplaceTemporal2_0[Shortname | None, Optical2_0, Temporal2_0],
    _CoreDatasetGetSetMeas[Shortname | None, Temporal2_0, Optical2_0],
    _CoreGetSetMeasOrdered[Optical2_0, Temporal2_0],
    _CoreDatasetGetSetMeasOrdered[Optical2_0, Temporal2_0],
    _CoreScaleMethods,
    _CoreSetShortnamesMaybe,
    _CoreCompensation,
    _CoreMeasWavelength,
    _CorePeak,
    _CoreGates[AppliedGates2_0],
    _CoreTo3_0[CoreDataset3_0],
    _CoreTo3_1[CoreDataset3_1],
    _CoreTo3_2[CoreDataset3_2],
    _CoreDatasetCommon,
):
    def __new__(
        cls,
        measurements: list[tuple[Shortname | None, Optical2_0 | Temporal2_0]],
        layout: _AnyOrderedLayout,
        data: DataFrame,
        mode: Mode = "L",
        cyt: str = "",
        comp: npt.NDArray[np.float32] | None = None,
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
        tr: Trigger | None = None,
        applied_gates: AppliedGates2_0 = ([], {}, None),
        nonstandard_keywords: NonStdKeywords = {},
        analysis: bytes = b"",
        others: list[bytes] = [],
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        path: Path,
        header: Header,
        std: StdKeywords,
        nonstd: NonStdKeywords,
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = "^(TIME|Time)$",
        allow_missing_time: TriFlag = "false",
        force_linear_scale: ForceLinearScale = "none",
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        process_time_optical_keys: ProcessTimeOpticalKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: ProcessKeywordFailure = "error",
        process_hyper_par: ProcessKeywordFailure = "error",
        process_other_version: ProcessKeywordFailure = "error",
        process_extra_timestep: ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = "^P%n",
        process_optional_failure: ProcessKeywordFailure = "error",
        integer_widths_from_byteord: bool = False,
        integer_byteord_override: list[int] | None = None,
        disallow_range_truncation: TriFlag = "false",
        disallow_deprecated: TriFlag = "false",
        # data args
        data_remainder_limit: int = 0,
        allow_uneven_event_width: TriFlag = "false",
        allow_tot_mismatch: TriFlag = "false",
        truncate_event_values: TruncateEventValues = "int_only",
        disallow_over_range: TriFlag = "false",
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
        dataset_offset: int = 0,
    ) -> Self: ...

@final
class CoreDataset3_0(
    _CoreCommon,
    _CorePre3_1,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreShortnamesMaybe,
    _CoreGetSetMeas[Shortname | None, Optical3_0, Temporal3_0],
    _CoreDatasetRemove[Shortname | None, Optical3_0, Temporal3_0],
    _CoreReplaceTemporal2_0[Shortname | None, Optical2_0, Temporal2_0],
    _CoreDatasetGetSetMeas[Shortname | None, Temporal3_0, Optical3_0],
    _CoreGetSetMeasOrdered[Optical3_0, Temporal3_0],
    _CoreDatasetGetSetMeasOrdered[Optical3_0, Temporal3_0],
    _CoreSetShortnamesMaybe,
    _CoreScaleTransformMethods,
    _CoreTimestepMethods,
    _CoreCompensation,
    _CoreUnicode,
    _CoreCytsn,
    _CoreMeasWavelength,
    _CorePeak,
    _CoreSubset,
    _CoreGates[AppliedGates3_0],
    _CoreTo2_0[CoreDataset2_0],
    _CoreTo3_1[CoreDataset3_1],
    _CoreTo3_2[CoreDataset3_2],
    _CoreDatasetCommon,
):
    def __new__(
        cls,
        measurements: list[tuple[Shortname | None, Optical3_0 | Temporal3_0]],
        layout: _AnyOrderedLayout,
        data: DataFrame,
        mode: Mode = "L",
        cyt: str = "",
        comp: Compensation | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        cytsn: str = "",
        unicode: Unicode | None = None,
        csvbits: int = 0,
        cstot: int = 0,
        csvflags: CsvFlags = [],
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
        tr: Trigger | None = None,
        applied_gates: AppliedGates3_0 = ([], {}, None),
        nonstandard_keywords: NonStdKeywords = {},
        analysis: bytes = b"",
        others: list[bytes] = [],
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        path: Path,
        header: Header,
        std: StdKeywords,
        nonstd: NonStdKeywords,
        # offset args
        allow_pseudoempty: bool = False,
        truncate_offset_limit: int = 0,
        overlap_correction_limit: int = 0,
        # std args
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = "^(TIME|Time)$",
        allow_missing_time: TriFlag = "false",
        force_linear_scale: ForceLinearScale = "none",
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        process_time_optical_keys: ProcessTimeOpticalKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: ProcessKeywordFailure = "error",
        process_hyper_par: ProcessKeywordFailure = "error",
        process_other_version: ProcessKeywordFailure = "error",
        process_extra_timestep: ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = "^P%n",
        add_missing_timestep: float | None = None,
        # layout args
        text_data_correction: OffsetCorrection = (0, 0),
        text_analysis_correction: OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: TriFlag = "false",
        process_optional_failure: ProcessKeywordFailure = "error",
        integer_widths_from_byteord: bool = False,
        integer_byteord_override: list[int] | None = None,
        disallow_range_truncation: TriFlag = "false",
        disallow_deprecated: TriFlag = "false",
        # data args
        data_remainder_limit: int = 0,
        allow_uneven_event_width: TriFlag = "false",
        allow_tot_mismatch: TriFlag = "false",
        truncate_event_values: TruncateEventValues = "int_only",
        disallow_over_range: TriFlag = "false",
        # shared args
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
        dataset_offset: int = 0,
    ) -> Self: ...

@final
class CoreDataset3_1(
    _CoreCommon,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_1, Temporal3_1],
    _CoreDatasetRemove[Shortname | None, Optical3_1, Temporal3_1],
    _CoreReplaceTemporal2_0[Shortname | None, Optical2_0, Temporal2_0],
    _CoreDatasetGetSetMeas[Shortname, Temporal3_1, Optical3_1],
    _CoreGetSetMeasEndian[_AnyNonMixedLayout, Optical3_1, Temporal3_1],
    _CoreDatasetGetSetMeasEndian[Optical3_1, Temporal3_1, _AnyNonMixedLayout],
    _CoreScaleTransformMethods,
    _CoreTimestepMethods,
    _CoreSubset,
    _CoreModified,
    _CorePlate,
    _CoreSpillover,
    _CoreVol,
    _CoreCytsn,
    _CoreMeasWavelengths,
    _CorePeak,
    _CoreMeasDisplay,
    _CoreMeasCalibration[Calibration3_1],
    _CoreGates[AppliedGates3_0],
    _CoreTo2_0[CoreDataset2_0],
    _CoreTo3_0[CoreDataset3_0],
    _CoreTo3_2[CoreDataset3_2],
    _CoreDatasetCommon,
):
    def __new__(
        cls,
        measurements: list[tuple[Shortname, Optical3_1 | Temporal3_1]],
        layout: _AnyNonMixedLayout,
        data: DataFrame,
        mode: Mode = "L",
        cyt: str = "",
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        cytsn: str = "",
        spillover: Spillover | None = None,
        last_modifier: str = "",
        last_modified: datetime | None = None,
        originality: Originality | None = None,
        plateid: str = "",
        platename: str = "",
        wellid: str = "",
        vol: float | None = None,
        csvbits: int = 0,
        cstot: int = 0,
        csvflags: CsvFlags = [],
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
        tr: Trigger | None = None,
        applied_gates: AppliedGates3_0 = ([], {}, None),
        nonstandard_keywords: NonStdKeywords = {},
        analysis: bytes = b"",
        others: list[bytes] = [],
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        path: Path,
        header: Header,
        std: StdKeywords,
        nonstd: NonStdKeywords,
        # offset args
        allow_pseudoempty: bool = False,
        truncate_offset_limit: int = 0,
        overlap_correction_limit: int = 0,
        # std args
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = "^(TIME|Time)$",
        allow_missing_time: TriFlag = "false",
        force_linear_scale: ForceLinearScale = "none",
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        process_time_optical_keys: ProcessTimeOpticalKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: ProcessKeywordFailure = "error",
        process_hyper_par: ProcessKeywordFailure = "error",
        process_other_version: ProcessKeywordFailure = "error",
        process_extra_timestep: ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = "^P%n",
        add_missing_timestep: float | None = None,
        spillover_measurement_mode: SpilloverMeasurementMode = "named",
        # layout args
        text_data_correction: OffsetCorrection = (0, 0),
        text_analysis_correction: OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: TriFlag = "false",
        process_optional_failure: ProcessKeywordFailure = "error",
        disallow_range_truncation: TriFlag = "false",
        disallow_deprecated: TriFlag = "false",
        # data args
        data_remainder_limit: int = 0,
        allow_uneven_event_width: TriFlag = "false",
        allow_tot_mismatch: TriFlag = "false",
        truncate_event_values: TruncateEventValues = "int_only",
        disallow_over_range: TriFlag = "false",
        # shared args
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
        dataset_offset: int = 0,
    ) -> Self: ...

@final
class CoreDataset3_2(
    _CoreCommon,
    _Core3_2,
    _CoreTemporal3_2,
    _CoreGetSetMeas[Shortname, Optical3_2, Temporal3_2],
    _CoreDatasetRemove[Shortname | None, Optical3_2, Temporal3_2],
    _CoreReplaceTemporal3_2,
    _CoreDatasetGetSetMeas[Shortname, Temporal3_2, Optical3_2],
    _CoreGetSetMeasEndian[_AnyMixedLayout, Optical3_2, Temporal3_2],
    _CoreDatasetGetSetMeasEndian[Optical3_2, Temporal3_2, _AnyMixedLayout],
    _CoreScaleTransformMethods,
    _CoreTimestepMethods,
    _CoreModified,
    _CorePlate,
    _CoreSpillover,
    _CoreVol,
    _CoreCytsn,
    _CoreMeasWavelengths,
    _CoreMeasDisplay,
    _CoreMeasCalibration[Calibration3_2],
    _CoreGates[AppliedGates3_2],
    _CoreTo2_0[CoreDataset2_0],
    _CoreTo3_0[CoreDataset3_0],
    _CoreTo3_1[CoreDataset3_1],
    _CoreDatasetCommon,
):
    def __new__(
        cls,
        measurements: list[tuple[Shortname, Optical3_2 | Temporal3_2]],
        layout: _AnyMixedLayout,
        data: DataFrame,
        cyt: str,
        mode: Mode3_2 | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        begindatetime: datetime | None = None,
        enddatetime: datetime | None = None,
        cytsn: str = "",
        spillover: Spillover | None = None,
        last_modifier: str = "",
        last_modified: datetime | None = None,
        originality: Originality | None = None,
        plateid: str = "",
        platename: str = "",
        wellid: str = "",
        vol: float | None = None,
        carrierid: str = "",
        carriertype: str = "",
        locationid: str = "",
        unstainedinfo: str = "",
        unstainedcenters: UnstainedCenters = {},
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
        tr: Trigger | None = None,
        applied_gates: AppliedGates3_2 = ({}, None),
        nonstandard_keywords: NonStdKeywords = {},
        analysis: bytes = b"",
        others: list[bytes] = [],
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        path: Path,
        header: Header,
        std: StdKeywords,
        nonstd: NonStdKeywords,
        # offset args
        allow_pseudoempty: bool = False,
        truncate_offset_limit: int = 0,
        overlap_correction_limit: int = 0,
        # std args
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = "^(TIME|Time)$",
        allow_missing_time: TriFlag = "false",
        force_linear_scale: ForceLinearScale = "none",
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        process_time_optical_keys: ProcessTimeOpticalKeys = "demote_warn",
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        datetime_pattern: str | None = None,
        last_modified_pattern: str | None = None,
        allow_other_feature: bool = False,
        process_pseudostandard: ProcessKeywordFailure = "error",
        process_hyper_par: ProcessKeywordFailure = "error",
        process_other_version: ProcessKeywordFailure = "error",
        process_extra_timestep: ProcessKeywordFailure = "error",
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = "^P%n",
        add_missing_timestep: float | None = None,
        spillover_measurement_mode: SpilloverMeasurementMode = "named",
        disallow_localtime: bool = False,
        # layout args
        text_data_correction: OffsetCorrection = (0, 0),
        text_analysis_correction: OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: TriFlag = "false",
        process_optional_failure: ProcessKeywordFailure = "error",
        disallow_range_truncation: TriFlag = "false",
        disallow_deprecated: TriFlag = "false",
        # data args
        data_remainder_limit: int = 0,
        allow_uneven_event_width: TriFlag = "false",
        allow_tot_mismatch: TriFlag = "false",
        truncate_event_values: TruncateEventValues = "int_only",
        disallow_over_range: TriFlag = "false",
        # shared args
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
        dataset_offset: int = 0,
    ) -> Self: ...

class PyreflowError(Exception): ...
class FileLayoutError(PyreflowError): ...
class ParseKeyError(PyreflowError): ...
class ParseKeywordValueError(PyreflowError): ...
class InvalidKeywordValueError(PyreflowError): ...
class ExtraKeywordError(PyreflowError): ...
class FCSDeprecatedError(PyreflowError): ...
class ConversionError(PyreflowError): ...
class RelationalError(PyreflowError): ...
class EventDataError(PyreflowError): ...
class DataLossError(PyreflowError): ...
class ConfigError(PyreflowError): ...
class PyreflowWarning(Warning): ...

@final
class ParsedHeaderSegments:
    def __new__(
        cls,
        text_seg: Segment,
        data_seg: Segment,
        analysis_seg: Segment,
        other_segs: OtherSegments,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def text_seg(self) -> Segment: ...
    @property
    def data_seg(self) -> Segment: ...
    @property
    def analysis_seg(self) -> Segment: ...
    @property
    def other_segs(self) -> OtherSegments: ...

@final
class UncorrectedHeaderSegments:
    def __new__(
        cls,
        text_seg: Segment,
        data_seg: Segment,
        analysis_seg: Segment,
        other_segs: list[Segment],
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def text_seg(self) -> Segment: ...
    @property
    def data_seg(self) -> Segment: ...
    @property
    def analysis_seg(self) -> Segment: ...
    @property
    def other_segs(self) -> list[Segment]: ...

@final
class Header:
    def __new__(
        cls,
        version: FCSVersion,
        segments: ParsedHeaderSegments,
        uncorrected_segments: UncorrectedHeaderSegments,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def version(self) -> FCSVersion: ...
    @property
    def segments(self) -> ParsedHeaderSegments: ...
    @property
    def uncorrected_segments(self) -> UncorrectedHeaderSegments: ...

@final
class ValidKeywords:
    def __new__(cls, std: StdKeywords, nonstd: NonStdKeywords) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def std(self) -> StdKeywords: ...
    @property
    def nonstd(self) -> NonStdKeywords: ...

@final
class StdTEXTDiagnostics:
    def __new__(
        cls,
        pseudostandard: StdKeywords,
        hyper_par: StdKeywords,
        hyper_gate: StdKeywords,
        other_version: StdKeywords,
        timestep: str | None,
        original_names: list[Shortname | None],
        scale: list[MeasScaleDiagnostic],
        gate_scale: list[GateScaleDiagnostic],
        trimmed: list[tuple[str, str]],
        temporal_optical_pairs: list[tuple[str, str]],
        timestep_added: bool,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def pseudostandard(self) -> StdKeywords: ...
    @property
    def hyper_par(self) -> StdKeywords: ...
    @property
    def hyper_gate(self) -> StdKeywords: ...
    @property
    def other_version(self) -> StdKeywords: ...
    @property
    def timestep(self) -> str | None: ...
    @property
    def original_names(self) -> list[Shortname | None]: ...
    @property
    def scale(self) -> list[MeasScaleDiagnostic]: ...
    @property
    def gate_scale(self) -> list[GateScaleDiagnostic]: ...
    @property
    def trimmed(self) -> list[tuple[str, str]]: ...
    @property
    def temporal_optical_pairs(self) -> list[tuple[str, str]]: ...
    @property
    def timestep_added(self) -> bool: ...

@final
class DatasetSegments:
    def __new__(
        cls,
        data_seg: Segment,
        analysis_seg: Segment,
        data_seg_uncorrected: Segment | None,
        analysis_seg_uncorrected: Segment | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def data_seg(self) -> Segment: ...
    @property
    def analysis_seg(self) -> Segment: ...
    @property
    def data_seg_uncorrected(self) -> Segment | None: ...
    @property
    def analysis_seg_uncorrected(self) -> Segment | None: ...

@final
class SplitTEXTDiagnostics:
    def __new__(
        cls,
        delimiter: int,
        escaped: bool,
        keys_with_blank_values: list[bytes | str],
        values_with_blank_keys: list[bytes | str],
        tokens_with_boundary_delims: list[bytes | str],
        last_odd_token: bytes | str,
        missing_final_delim: bool,
        has_extra_delim: bool,
        trailing_bytes: bytes,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def delimiter(self) -> int: ...
    @property
    def escaped(self) -> bool: ...
    @property
    def keys_with_blank_values(self) -> list[bytes | str]: ...
    @property
    def values_with_blank_keys(self) -> list[bytes | str]: ...
    @property
    def tokens_with_boundary_delims(self) -> list[bytes | str]: ...
    @property
    def last_odd_token(self) -> bytes | str: ...
    @property
    def missing_final_delim(self) -> bool: ...
    @property
    def has_extra_delim(self) -> bool: ...
    @property
    def trailing_bytes(self) -> bytes: ...

@final
class HeaderAndSuppOffsets:
    def __new__(
        cls,
        header: Header,
        supp_text: tuple[Segment | None, Segment] | None,
        nextdata: int | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def header(self) -> Header: ...
    @property
    def supp_text(self) -> tuple[Segment | None, Segment] | None: ...
    @property
    def nextdata(self) -> int | None: ...

@final
class FlatTEXTDiagnostics:
    def __new__(
        cls,
        header_supp: HeaderAndSuppOffsets,
        byte_pairs: list[tuple[bytes | str, bytes | str]],
        non_unique_std_keywords: list[tuple[str, str]],
        non_unique_nonstd_keywords: list[tuple[str, str]],
        ignored_standard_keywords: list[tuple[str, bytes | str]],
        keys_with_empty_trimmed_values: list[bytes | str],
        keys_with_trimmed_values: list[tuple[bytes | str, bytes | str]],
        primary_split: SplitTEXTDiagnostics,
        supp_split: SplitTEXTDiagnostics | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def header_supp(self) -> HeaderAndSuppOffsets: ...
    @property
    def byte_pairs(self) -> list[tuple[bytes | str, bytes | str]]: ...
    @property
    def non_unique_std_keywords(self) -> list[tuple[str, str]]: ...
    @property
    def non_unique_nonstd_keywords(self) -> list[tuple[str, str]]: ...
    @property
    def ignored_standard_keywords(self) -> list[tuple[str, bytes | str]]: ...
    @property
    def keys_with_empty_trimmed_values(self) -> list[bytes | str]: ...
    @property
    def keys_with_trimmed_values(self) -> list[tuple[bytes | str, bytes | str]]: ...
    @property
    def primary_split(self) -> SplitTEXTDiagnostics: ...
    @property
    def supp_split(self) -> SplitTEXTDiagnostics | None: ...

@final
class FlatTEXTOutput:
    def __new__(
        cls,
        kws: ValidKeywords,
        flat_diagnostics: FlatTEXTDiagnostics,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def kws(self) -> ValidKeywords: ...
    @property
    def flat_diagnostics(self) -> FlatTEXTDiagnostics: ...

@final
class EventsDiagnostics:
    def __new__(
        cls,
        event_width: int | None,
        event_data_remainder: int | None,
        tot_event_mismatch: bool | None,
        overrange_columns: list[tuple[int, bool] | None],
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

@final
class FlatDatasetFromKwsOutput:
    def __new__(
        cls,
        data: DataFrame,
        analysis: bytes,
        others: list[bytes],
        dataset_segs: DatasetSegments,
        events_diagnostics: EventsDiagnostics,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def data(self) -> DataFrame: ...
    @property
    def analysis(self) -> bytes: ...
    @property
    def others(self) -> list[bytes]: ...
    @property
    def dataset_segs(self) -> DatasetSegments: ...
    @property
    def events_diagnostics(self) -> EventsDiagnostics: ...

@final
class NewFlatDatasetFromKwsOutput:
    def __new__(
        cls,
        dataset: FlatDatasetFromKwsOutput,
        header: ParsedHeaderSegments,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def dataset(self) -> FlatDatasetFromKwsOutput: ...
    @property
    def header(self) -> ParsedHeaderSegments: ...

@final
class FlatDatasetOutput:
    def __new__(
        cls,
        text: FlatTEXTOutput,
        dataset: FlatDatasetFromKwsOutput,
        version_scores: KeywordVersionScores | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def text(self) -> FlatTEXTOutput: ...
    @property
    def dataset(self) -> FlatDatasetFromKwsOutput: ...
    @property
    def version_scores(self) -> KeywordVersionScores | None: ...

@final
class StdTEXTOutput:
    def __new__(
        cls,
        tot: int | None,
        dataset_segs: DatasetSegments,
        std_diagnostics: StdTEXTDiagnostics,
        flat_diagnostics: FlatTEXTDiagnostics,
        version_scores: KeywordVersionScores | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def tot(self) -> int | None: ...
    @property
    def dataset_segs(self) -> DatasetSegments: ...
    @property
    def std_diagnostics(self) -> StdTEXTDiagnostics: ...
    @property
    def flat_diagnostics(self) -> FlatTEXTDiagnostics: ...
    @property
    def version_scores(self) -> KeywordVersionScores | None: ...

@final
class StdDatasetFromKwsOutput:
    def __new__(
        cls,
        dataset_segs: DatasetSegments,
        std_diagnostics: StdTEXTDiagnostics,
        events_diagnostics: EventsDiagnostics,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def dataset_segs(self) -> DatasetSegments: ...
    @property
    def std_diagnostics(self) -> StdTEXTDiagnostics: ...
    @property
    def events_diagnostics(self) -> EventsDiagnostics: ...

@final
class NewStdDatasetFromKwsOutput:
    def __new__(
        cls,
        dataset: StdDatasetFromKwsOutput,
        header: ParsedHeaderSegments,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def dataset(self) -> StdDatasetFromKwsOutput: ...
    @property
    def header(self) -> ParsedHeaderSegments: ...

@final
class StdDatasetOutput:
    def __new__(
        cls,
        dataset: StdDatasetFromKwsOutput,
        flat_diagnostics: FlatTEXTDiagnostics,
        version_scores: KeywordVersionScores | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def dataset(self) -> StdDatasetFromKwsOutput: ...
    @property
    def flat_diagnostics(self) -> FlatTEXTDiagnostics: ...
    @property
    def version_scores(self) -> KeywordVersionScores | None: ...

@final
class DatasetSummary:
    def __new__(
        cls,
        version: FCSVersion,
        text_len: int,
        data_len: int,
        analysis_len: int,
        n_events: int,
        n_measurements: int,
        n_other: int,
        others_len: int,
        datatype: Datatype,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def version(self) -> FCSVersion: ...
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
    def datatype(self) -> Datatype: ...

class _ConfigCommon:
    @classmethod
    def strict(cls) -> dict[str, Any]: ...
    @classmethod
    def scalpal(cls) -> dict[str, Any]: ...
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

@final
class NewCoreTEXTConfig(_ConfigCommon):
    pass

@final
class NewCoreDatasetConfig(_ConfigCommon):
    pass

def fcs_read_header(
    path: Path,
    # header args
    text_correction: OffsetCorrection = (0, 0),
    data_correction: OffsetCorrection = (0, 0),
    analysis_correction: OffsetCorrection = (0, 0),
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: GuessOtherWidth = "none",
    squish_offsets: bool = False,
    allow_pseudoempty: bool = False,
    # offset args
    truncate_offset_limit: int = 0,
    overlap_correction_limit: int = 0,
    dataset_offset: int = 0,
) -> Header: ...

#
def fcs_read_flat_text(
    path: Path,
    # header args
    text_correction: OffsetCorrection = (0, 0),
    data_correction: OffsetCorrection = (0, 0),
    analysis_correction: OffsetCorrection = (0, 0),
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    truncate_offset_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: VersionOverride | None = None,
    supp_text_correction: OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: TriFlag = "false",
    ignore_supp_text: bool = False,
    delim_escape_mode: DelimEscapeMode = "escaped",
    allow_non_ascii_delim: TriFlag = "false",
    allow_missing_final_delim: TriFlag = "false",
    allow_nonunique: TriFlag = "false",
    allow_odd: TriFlag = "false",
    allow_empty_keys: TriFlag = "false",
    allow_delim_at_boundary: TriFlag = "false",
    use_latin1: bool = False,
    allow_non_ascii_keys: TriFlag = "false",
    allow_non_utf8_values: TriFlag = "false",
    allow_missing_supp_text: TriFlag = "false",
    allow_supp_text_own_delim: TriFlag = "false",
    allow_missing_nextdata: TriFlag = "false",
    trim_value_whitespace: TrimValueWhitespace = "notrim",
    trim_text_end: bool = False,
    ignore_standard_keys: KeyPatterns = [],
    promote_to_standard: KeyPatterns = [],
    demote_from_standard: KeyPatterns = [],
    rename_standard_keys: dict[str, str] = {},
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    substitute_standard_key_values: SubPatterns = {},
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    dataset_offset: int = 0,
) -> FlatTEXTOutput: ...

#
def fcs_read_std_text(
    path: Path,
    # header args
    text_correction: OffsetCorrection = (0, 0),
    data_correction: OffsetCorrection = (0, 0),
    analysis_correction: OffsetCorrection = (0, 0),
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    truncate_offset_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: VersionOverride | None = None,
    supp_text_correction: OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: TriFlag = "false",
    ignore_supp_text: bool = False,
    delim_escape_mode: DelimEscapeMode = "escaped",
    allow_non_ascii_delim: TriFlag = "false",
    allow_missing_final_delim: TriFlag = "false",
    allow_nonunique: TriFlag = "false",
    allow_odd: TriFlag = "false",
    allow_empty_keys: TriFlag = "false",
    allow_delim_at_boundary: TriFlag = "false",
    use_latin1: bool = False,
    allow_non_ascii_keys: TriFlag = "false",
    allow_non_utf8_values: TriFlag = "false",
    allow_missing_supp_text: TriFlag = "false",
    allow_supp_text_own_delim: TriFlag = "false",
    allow_missing_nextdata: TriFlag = "false",
    trim_value_whitespace: TrimValueWhitespace = "notrim",
    trim_text_end: bool = False,
    ignore_standard_keys: KeyPatterns = [],
    promote_to_standard: KeyPatterns = [],
    demote_from_standard: KeyPatterns = [],
    rename_standard_keys: dict[str, str] = {},
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    substitute_standard_key_values: SubPatterns = {},
    # standard args
    dedup_measurement_names: bool = False,
    trim_intra_value_whitespace: bool = False,
    time_meas_pattern: str | None = "^(TIME|Time)$",
    allow_missing_time: TriFlag = "false",
    force_linear_scale: ForceLinearScale = "none",
    ignore_time_optical_keys: list[TemporalOpticalKey] = [],
    process_time_optical_keys: ProcessTimeOpticalKeys = "demote_warn",
    date_pattern: str | None = None,
    time_pattern: str | None = None,
    datetime_pattern: str | None = None,
    last_modified_pattern: str | None = None,
    allow_other_feature: bool = False,
    process_pseudostandard: ProcessKeywordFailure = "error",
    process_hyper_par: ProcessKeywordFailure = "error",
    process_other_version: ProcessKeywordFailure = "error",
    process_extra_timestep: ProcessKeywordFailure = "error",
    fix_log_scale_offsets: bool = False,
    nonstandard_measurement_pattern: str | None = "^P%n",
    add_missing_timestep: float | None = None,
    spillover_measurement_mode: SpilloverMeasurementMode = "named",
    disallow_localtime: bool = False,
    # layout args
    text_data_correction: OffsetCorrection = (0, 0),
    text_analysis_correction: OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: TriFlag = "false",
    process_optional_failure: ProcessKeywordFailure = "error",
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: TriFlag = "false",
    disallow_deprecated: TriFlag = "false",
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    dataset_offset: int = 0,
) -> tuple[AnyCoreTEXT, StdTEXTOutput]: ...

#
def fcs_read_flat_dataset(
    path: Path,
    # header args
    text_correction: OffsetCorrection = (0, 0),
    data_correction: OffsetCorrection = (0, 0),
    analysis_correction: OffsetCorrection = (0, 0),
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    truncate_offset_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: VersionOverride | None = None,
    supp_text_correction: OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: TriFlag = "false",
    ignore_supp_text: bool = False,
    delim_escape_mode: DelimEscapeMode = "escaped",
    allow_non_ascii_delim: TriFlag = "false",
    allow_missing_final_delim: TriFlag = "false",
    allow_nonunique: TriFlag = "false",
    allow_odd: TriFlag = "false",
    allow_empty_keys: TriFlag = "false",
    allow_delim_at_boundary: TriFlag = "false",
    use_latin1: bool = False,
    allow_non_ascii_keys: TriFlag = "false",
    allow_non_utf8_values: TriFlag = "false",
    allow_missing_supp_text: TriFlag = "false",
    allow_supp_text_own_delim: TriFlag = "false",
    allow_missing_nextdata: TriFlag = "false",
    trim_value_whitespace: TrimValueWhitespace = "notrim",
    trim_text_end: bool = False,
    ignore_standard_keys: KeyPatterns = [],
    promote_to_standard: KeyPatterns = [],
    demote_from_standard: KeyPatterns = [],
    rename_standard_keys: dict[str, str] = {},
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    substitute_standard_key_values: SubPatterns = {},
    # layout args
    text_data_correction: OffsetCorrection = (0, 0),
    text_analysis_correction: OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: TriFlag = "false",
    process_optional_failure: ProcessKeywordFailure = "error",
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: TriFlag = "false",
    disallow_deprecated: TriFlag = "false",
    # data args
    data_remainder_limit: int = 0,
    allow_uneven_event_width: TriFlag = "false",
    allow_tot_mismatch: TriFlag = "false",
    truncate_event_values: TruncateEventValues = "int_only",
    disallow_over_range: TriFlag = "false",
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    dataset_offset: int = 0,
) -> FlatDatasetOutput: ...

#
def fcs_read_std_dataset(
    path: Path,
    # header args
    text_correction: OffsetCorrection = (0, 0),
    data_correction: OffsetCorrection = (0, 0),
    analysis_correction: OffsetCorrection = (0, 0),
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    truncate_offset_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: VersionOverride | None = None,
    supp_text_correction: OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: TriFlag = "false",
    ignore_supp_text: bool = False,
    delim_escape_mode: DelimEscapeMode = "escaped",
    allow_non_ascii_delim: TriFlag = "false",
    allow_missing_final_delim: TriFlag = "false",
    allow_nonunique: TriFlag = "false",
    allow_odd: TriFlag = "false",
    allow_empty_keys: TriFlag = "false",
    allow_delim_at_boundary: TriFlag = "false",
    use_latin1: bool = False,
    allow_non_ascii_keys: TriFlag = "false",
    allow_non_utf8_values: TriFlag = "false",
    allow_missing_supp_text: TriFlag = "false",
    allow_supp_text_own_delim: TriFlag = "false",
    allow_missing_nextdata: TriFlag = "false",
    trim_value_whitespace: TrimValueWhitespace = "notrim",
    trim_text_end: bool = False,
    ignore_standard_keys: KeyPatterns = [],
    promote_to_standard: KeyPatterns = [],
    demote_from_standard: KeyPatterns = [],
    rename_standard_keys: dict[str, str] = {},
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    substitute_standard_key_values: SubPatterns = {},
    # standard args
    dedup_measurement_names: bool = False,
    trim_intra_value_whitespace: bool = False,
    time_meas_pattern: str | None = "^(TIME|Time)$",
    allow_missing_time: TriFlag = "false",
    force_linear_scale: ForceLinearScale = "none",
    ignore_time_optical_keys: list[TemporalOpticalKey] = [],
    process_time_optical_keys: ProcessTimeOpticalKeys = "demote_warn",
    date_pattern: str | None = None,
    time_pattern: str | None = None,
    datetime_pattern: str | None = None,
    last_modified_pattern: str | None = None,
    allow_other_feature: bool = False,
    process_pseudostandard: ProcessKeywordFailure = "error",
    process_hyper_par: ProcessKeywordFailure = "error",
    process_other_version: ProcessKeywordFailure = "error",
    process_extra_timestep: ProcessKeywordFailure = "error",
    fix_log_scale_offsets: bool = False,
    nonstandard_measurement_pattern: str | None = "^P%n",
    add_missing_timestep: float | None = None,
    spillover_measurement_mode: SpilloverMeasurementMode = "named",
    disallow_localtime: bool = False,
    # layout args
    text_data_correction: OffsetCorrection = (0, 0),
    text_analysis_correction: OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: TriFlag = "false",
    process_optional_failure: ProcessKeywordFailure = "error",
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: TriFlag = "false",
    disallow_deprecated: TriFlag = "false",
    # data args
    data_remainder_limit: int = 0,
    allow_uneven_event_width: TriFlag = "false",
    allow_tot_mismatch: TriFlag = "false",
    truncate_event_values: TruncateEventValues = "int_only",
    disallow_over_range: TriFlag = "false",
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    dataset_offset: int = 0,
) -> tuple[AnyCoreDataset, StdDatasetOutput]: ...

#
def fcs_read_flat_texts(
    path: Path,
    skip: int | None = None,
    limit: int | None = None,
    # header args
    text_correction: OffsetCorrection = (0, 0),
    data_correction: OffsetCorrection = (0, 0),
    analysis_correction: OffsetCorrection = (0, 0),
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    truncate_offset_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: VersionOverride | None = None,
    supp_text_correction: OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: TriFlag = "false",
    ignore_supp_text: bool = False,
    delim_escape_mode: DelimEscapeMode = "escaped",
    allow_non_ascii_delim: TriFlag = "false",
    allow_missing_final_delim: TriFlag = "false",
    allow_nonunique: TriFlag = "false",
    allow_odd: TriFlag = "false",
    allow_empty_keys: TriFlag = "false",
    allow_delim_at_boundary: TriFlag = "false",
    use_latin1: bool = False,
    allow_non_ascii_keys: TriFlag = "false",
    allow_non_utf8_values: TriFlag = "false",
    allow_missing_supp_text: TriFlag = "false",
    allow_supp_text_own_delim: TriFlag = "false",
    allow_missing_nextdata: TriFlag = "false",
    trim_value_whitespace: TrimValueWhitespace = "notrim",
    trim_text_end: bool = False,
    ignore_standard_keys: KeyPatterns = [],
    promote_to_standard: KeyPatterns = [],
    demote_from_standard: KeyPatterns = [],
    rename_standard_keys: dict[str, str] = {},
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    substitute_standard_key_values: SubPatterns = {},
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
) -> list[FlatTEXTOutput]: ...

#
def fcs_read_std_texts(
    path: Path,
    skip: int | None = None,
    limit: int | None = None,
    # header args
    text_correction: OffsetCorrection = (0, 0),
    data_correction: OffsetCorrection = (0, 0),
    analysis_correction: OffsetCorrection = (0, 0),
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    truncate_offset_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: VersionOverride | None = None,
    supp_text_correction: OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: TriFlag = "false",
    ignore_supp_text: bool = False,
    delim_escape_mode: DelimEscapeMode = "escaped",
    allow_non_ascii_delim: TriFlag = "false",
    allow_missing_final_delim: TriFlag = "false",
    allow_nonunique: TriFlag = "false",
    allow_odd: TriFlag = "false",
    allow_empty_keys: TriFlag = "false",
    allow_delim_at_boundary: TriFlag = "false",
    use_latin1: bool = False,
    allow_non_ascii_keys: TriFlag = "false",
    allow_non_utf8_values: TriFlag = "false",
    allow_missing_supp_text: TriFlag = "false",
    allow_supp_text_own_delim: TriFlag = "false",
    allow_missing_nextdata: TriFlag = "false",
    trim_value_whitespace: TrimValueWhitespace = "notrim",
    trim_text_end: bool = False,
    ignore_standard_keys: KeyPatterns = [],
    promote_to_standard: KeyPatterns = [],
    demote_from_standard: KeyPatterns = [],
    rename_standard_keys: dict[str, str] = {},
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    substitute_standard_key_values: SubPatterns = {},
    # standard args
    dedup_measurement_names: bool = False,
    trim_intra_value_whitespace: bool = False,
    time_meas_pattern: str | None = "^(TIME|Time)$",
    allow_missing_time: TriFlag = "false",
    force_linear_scale: ForceLinearScale = "none",
    ignore_time_optical_keys: list[TemporalOpticalKey] = [],
    process_time_optical_keys: ProcessTimeOpticalKeys = "demote_warn",
    date_pattern: str | None = None,
    time_pattern: str | None = None,
    datetime_pattern: str | None = None,
    last_modified_pattern: str | None = None,
    allow_other_feature: bool = False,
    process_pseudostandard: ProcessKeywordFailure = "error",
    process_hyper_par: ProcessKeywordFailure = "error",
    process_other_version: ProcessKeywordFailure = "error",
    process_extra_timestep: ProcessKeywordFailure = "error",
    fix_log_scale_offsets: bool = False,
    nonstandard_measurement_pattern: str | None = "^P%n",
    add_missing_timestep: float | None = None,
    spillover_measurement_mode: SpilloverMeasurementMode = "named",
    disallow_localtime: bool = False,
    # layout args
    text_data_correction: OffsetCorrection = (0, 0),
    text_analysis_correction: OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: TriFlag = "false",
    process_optional_failure: ProcessKeywordFailure = "error",
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: TriFlag = "false",
    disallow_deprecated: TriFlag = "false",
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
) -> list[tuple[AnyCoreTEXT, StdTEXTOutput]]: ...

#
def fcs_read_flat_datasets(
    path: Path,
    skip: int | None = None,
    limit: int | None = None,
    # header args
    text_correction: OffsetCorrection = (0, 0),
    data_correction: OffsetCorrection = (0, 0),
    analysis_correction: OffsetCorrection = (0, 0),
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    truncate_offset_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: VersionOverride | None = None,
    supp_text_correction: OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: TriFlag = "false",
    ignore_supp_text: bool = False,
    delim_escape_mode: DelimEscapeMode = "escaped",
    allow_non_ascii_delim: TriFlag = "false",
    allow_missing_final_delim: TriFlag = "false",
    allow_nonunique: TriFlag = "false",
    allow_odd: TriFlag = "false",
    allow_empty_keys: TriFlag = "false",
    allow_delim_at_boundary: TriFlag = "false",
    use_latin1: bool = False,
    allow_non_ascii_keys: TriFlag = "false",
    allow_non_utf8_values: TriFlag = "false",
    allow_missing_supp_text: TriFlag = "false",
    allow_supp_text_own_delim: TriFlag = "false",
    allow_missing_nextdata: TriFlag = "false",
    trim_value_whitespace: TrimValueWhitespace = "notrim",
    trim_text_end: bool = False,
    ignore_standard_keys: KeyPatterns = [],
    promote_to_standard: KeyPatterns = [],
    demote_from_standard: KeyPatterns = [],
    rename_standard_keys: dict[str, str] = {},
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    substitute_standard_key_values: SubPatterns = {},
    # layout args
    text_data_correction: OffsetCorrection = (0, 0),
    text_analysis_correction: OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: TriFlag = "false",
    process_optional_failure: ProcessKeywordFailure = "error",
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: TriFlag = "false",
    disallow_deprecated: TriFlag = "false",
    # data args
    data_remainder_limit: int = 0,
    allow_uneven_event_width: TriFlag = "false",
    allow_tot_mismatch: TriFlag = "false",
    truncate_event_values: TruncateEventValues = "int_only",
    disallow_over_range: TriFlag = "false",
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
) -> list[FlatDatasetOutput]: ...

#
def fcs_read_std_datasets(
    path: Path,
    skip: int | None = None,
    limit: int | None = None,
    # header args
    text_correction: OffsetCorrection = (0, 0),
    data_correction: OffsetCorrection = (0, 0),
    analysis_correction: OffsetCorrection = (0, 0),
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    truncate_offset_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: VersionOverride | None = None,
    supp_text_correction: OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: TriFlag = "false",
    ignore_supp_text: bool = False,
    delim_escape_mode: DelimEscapeMode = "escaped",
    allow_non_ascii_delim: TriFlag = "false",
    allow_missing_final_delim: TriFlag = "false",
    allow_nonunique: TriFlag = "false",
    allow_odd: TriFlag = "false",
    allow_empty_keys: TriFlag = "false",
    allow_delim_at_boundary: TriFlag = "false",
    use_latin1: bool = False,
    allow_non_ascii_keys: TriFlag = "false",
    allow_non_utf8_values: TriFlag = "false",
    allow_missing_supp_text: TriFlag = "false",
    allow_supp_text_own_delim: TriFlag = "false",
    allow_missing_nextdata: TriFlag = "false",
    trim_value_whitespace: TrimValueWhitespace = "notrim",
    trim_text_end: bool = False,
    ignore_standard_keys: KeyPatterns = [],
    promote_to_standard: KeyPatterns = [],
    demote_from_standard: KeyPatterns = [],
    rename_standard_keys: dict[str, str] = {},
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    substitute_standard_key_values: SubPatterns = {},
    # standard args
    dedup_measurement_names: bool = False,
    trim_intra_value_whitespace: bool = False,
    time_meas_pattern: str | None = "^(TIME|Time)$",
    allow_missing_time: TriFlag = "false",
    force_linear_scale: ForceLinearScale = "none",
    ignore_time_optical_keys: list[TemporalOpticalKey] = [],
    process_time_optical_keys: ProcessTimeOpticalKeys = "demote_warn",
    date_pattern: str | None = None,
    time_pattern: str | None = None,
    datetime_pattern: str | None = None,
    last_modified_pattern: str | None = None,
    allow_other_feature: bool = False,
    process_pseudostandard: ProcessKeywordFailure = "error",
    process_hyper_par: ProcessKeywordFailure = "error",
    process_other_version: ProcessKeywordFailure = "error",
    process_extra_timestep: ProcessKeywordFailure = "error",
    fix_log_scale_offsets: bool = False,
    nonstandard_measurement_pattern: str | None = "^P%n",
    add_missing_timestep: float | None = None,
    spillover_measurement_mode: SpilloverMeasurementMode = "named",
    disallow_localtime: bool = False,
    # layout args
    text_data_correction: OffsetCorrection = (0, 0),
    text_analysis_correction: OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: TriFlag = "false",
    process_optional_failure: ProcessKeywordFailure = "error",
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: TriFlag = "false",
    disallow_deprecated: TriFlag = "false",
    # data args
    data_remainder_limit: int = 0,
    allow_uneven_event_width: TriFlag = "false",
    allow_tot_mismatch: TriFlag = "false",
    truncate_event_values: TruncateEventValues = "int_only",
    disallow_over_range: TriFlag = "false",
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
) -> list[tuple[AnyCoreDataset, StdDatasetOutput]]: ...

#
def fcs_read_flat_dataset_with_keywords(
    path: Path,
    header: Header,
    std: dict[str, str],
    # offset args
    allow_pseudoempty: bool = False,
    truncate_offset_limit: int = 0,
    overlap_correction_limit: int = 0,
    # layout args
    text_data_correction: OffsetCorrection = (0, 0),
    text_analysis_correction: OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: TriFlag = "false",
    process_optional_failure: ProcessKeywordFailure = "error",
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: TriFlag = "false",
    disallow_deprecated: TriFlag = "false",
    # data args
    data_remainder_limit: int = 0,
    allow_uneven_event_width: TriFlag = "false",
    allow_tot_mismatch: TriFlag = "false",
    truncate_event_values: TruncateEventValues = "int_only",
    disallow_over_range: TriFlag = "false",
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    dataset_offset: int = 0,
) -> FlatDatasetFromKwsOutput: ...

#
def fcs_summarize(
    path: Path,
    skip: int | None = None,
    limit: int | None = None,
    # header args
    text_correction: OffsetCorrection = (0, 0),
    data_correction: OffsetCorrection = (0, 0),
    analysis_correction: OffsetCorrection = (0, 0),
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    truncate_offset_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: VersionOverride | None = None,
    supp_text_correction: OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: TriFlag = "false",
    ignore_supp_text: bool = False,
    delim_escape_mode: DelimEscapeMode = "escaped",
    allow_non_ascii_delim: TriFlag = "false",
    allow_missing_final_delim: TriFlag = "false",
    allow_nonunique: TriFlag = "false",
    allow_odd: TriFlag = "false",
    allow_empty_keys: TriFlag = "false",
    allow_delim_at_boundary: TriFlag = "false",
    use_latin1: bool = False,
    allow_non_ascii_keys: TriFlag = "false",
    allow_non_utf8_values: TriFlag = "false",
    allow_missing_supp_text: TriFlag = "false",
    allow_supp_text_own_delim: TriFlag = "false",
    allow_missing_nextdata: TriFlag = "false",
    trim_value_whitespace: TrimValueWhitespace = "notrim",
    trim_text_end: bool = False,
    ignore_standard_keys: KeyPatterns = [],
    promote_to_standard: KeyPatterns = [],
    demote_from_standard: KeyPatterns = [],
    rename_standard_keys: dict[str, str] = {},
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    substitute_standard_key_values: SubPatterns = {},
    # layout args
    text_data_correction: OffsetCorrection = (0, 0),
    text_analysis_correction: OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: TriFlag = "false",
    process_optional_failure: ProcessKeywordFailure = "error",
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: TriFlag = "false",
    disallow_deprecated: TriFlag = "false",
    # data args
    data_remainder_limit: int = 0,
    allow_uneven_event_width: TriFlag = "false",
    allow_tot_mismatch: TriFlag = "false",
    truncate_event_values: TruncateEventValues = "int_only",
    disallow_over_range: TriFlag = "false",
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
) -> list[DatasetSummary]: ...

__version__: str

__all__ = [
    "__version__",
    "PyreflowError",
    "FileLayoutError",
    "ParseKeyError",
    "ParseKeywordValueError",
    "InvalidKeywordValueError",
    "ExtraKeywordError",
    "FCSDeprecatedError",
    "ConversionError",
    "RelationalError",
    "EventDataError",
    "DataLossError",
    "ConfigError",
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
    "FixedAsciiLayout",
    "DelimAsciiLayout",
    "OrderedUint08Layout",
    "OrderedUint16Layout",
    "OrderedUint24Layout",
    "OrderedUint32Layout",
    "OrderedUint40Layout",
    "OrderedUint48Layout",
    "OrderedUint56Layout",
    "OrderedUint64Layout",
    "OrderedF32Layout",
    "OrderedF64Layout",
    "EndianF32Layout",
    "EndianF64Layout",
    "EndianUintLayout",
    "MixedLayout",
    "Header",
    "ParsedHeaderSegments",
    "HeaderAndSuppOffsets",
    "UncorrectedHeaderSegments",
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
    "ValidKeywords",
    "DatasetSegments",
    "SplitTEXTDiagnostics",
    "EventsDiagnostics",
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
]
