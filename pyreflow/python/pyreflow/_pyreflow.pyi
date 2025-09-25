from __future__ import annotations
from pathlib import Path
from datetime import time, date, datetime
from typing import TypeVar, Self, Generic, Union, final

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
)

_X = TypeVar("_X")
_Y = TypeVar("_Y")
_C = TypeVar("_C")
_N = TypeVar("_N")
_L = TypeVar("_L")

_OpticalKeyVals = list[_X | tuple[()] | None]

_DEFAULT_SEGMENT = (0, 0)
_DEFAULT_CORRECTION = (0, 0)
_DEFAULT_OTHER_WIDTH = 8
_DEFAULT_KEY_PATTERNS: KeyPatterns = ([], [])
_DEFAULT_TIME_MEAS_PATTERN = "^(TIME|Time)$"

class _LayoutUnmixedCommon:
    @property
    def datatype(self) -> Datatype: ...

class _LayoutEndianCommon:
    @property
    def endian(self) -> Endian: ...

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
    longname: str | None

class _OpticalWavelength:
    wavelength: float | None

class _OpticalWavelengths:
    wavelengths: list[float] | None

class _MeasDisplay:
    display: Display

class _PeakCommon:
    size: int
    bin: int

class _OpticalCommon:
    filter: str | None
    detector_type: str | None
    detector_voltage: float | None
    power: float | None
    percent_emitted: str | None

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
        filter: str | None = None,
        power: float | None = None,
        detector_type: str | None = None,
        percent_emitted: str | None = None,
        detector_voltage: float | None = None,
        longname: str | None = None,
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
        filter: str | None = None,
        power: float | None = None,
        detector_type: str | None = None,
        percent_emitted: str | None = None,
        detector_voltage: float | None = None,
        longname: str | None = None,
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
        wavelengths: list[float] | None = None,
        calibration: Calibration3_1 | None = None,
        display: Display | None = None,
        bin: int | None = None,
        size: int | None = None,
        filter: str | None = None,
        power: float | None = None,
        detector_type: str | None = None,
        percent_emitted: str | None = None,
        detector_voltage: float | None = None,
        longname: str | None = None,
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
    # TODO literal string
    measurement_type: str | None
    feature: Feature | None
    analyte: str | None

    def __new__(
        cls,
        transform: ScaleTransform,
        wavelengths: list[float] | None = None,
        calibration: Calibration3_2 | None = None,
        display: Display | None = None,
        analyte: str | None = None,
        feature: Feature | None = None,
        tag: str | None = None,
        measurement_type: str | None = None,
        detector_name: str | None = None,
        filter: str | None = None,
        power: float | None = None,
        detector_type: str | None = None,
        percent_emitted: str | None = None,
        detector_voltage: float | None = None,
        longname: str | None = None,
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...

@final
class Temporal2_0(_MeasCommon, _PeakCommon):
    def __new__(
        cls,
        has_scale: bool = False,
        bin: int | None = None,
        size: int | None = None,
        longname: str | None = None,
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
        longname: str | None = None,
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
        longname: str | None = None,
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
        longname: str | None = None,
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...

_T = TypeVar("_T", bound=Temporal2_0 | Temporal3_0 | Temporal3_1 | Temporal3_2)
_O = TypeVar("_O", bound=Optical2_0 | Optical3_0 | Optical3_1 | Optical3_2)

_RawInput = list[tuple[_N, _O] | tuple[Shortname, _T]]

@final
class GatedMeasurement:
    def __new__(
        cls,
        scale: tuple[()] | tuple[float, float] | None = None,
        filter: str | None = None,
        shortname: str | None = None,
        percent_emitted: str | None = None,
        range: float | None = None,
        longname: str | None = None,
        detector_type: str | None = None,
        detector_voltage: float | None = None,
    ) -> Self: ...
    scale: tuple[()] | tuple[float, float] | None
    filter: str | None
    shortname: str | None
    percent_emitted: str | None
    range: float | None
    longname: str | None
    detector_type: str | None
    detector_voltage: float | None

class _UnivariateRegion(Generic[_X]):
    def __new__(
        cls,
        index: _X,
        gate: tuple[float, float],
    ) -> Self: ...
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
        exclude_req_root: bool = False,
        exclude_opt_root: bool = False,
        exclude_req_meas: bool = False,
        exclude_opt_meas: bool = False,
    ) -> dict[str, str]: ...
    @property
    def par(self) -> int: ...
    def set_trigger_threshold(self, threshold: int) -> bool: ...
    def write_text(
        self, path: Path, delim: int = 30, big_other: bool = False
    ) -> None: ...
    @property
    def version(self) -> FCSVersion: ...

class _CoreDatasetCommon:
    def write_dataset(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        skip_conversion_check: bool = False,
    ) -> None: ...

class _CoreShortnamesMaybe:
    all_shortnames_maybe: list[Shortname | None]

class _CoreTemporal2_0:
    def set_temporal(self, name: Shortname, force: bool = False) -> bool: ...
    def set_temporal_at(self, index: MeasIndex, force: bool = False) -> bool: ...
    def unset_temporal(self) -> bool: ...

class _CoreTemporal3_0:
    def set_temporal(
        self,
        name: Shortname,
        timestep: Timestep,
        force: bool = False,
    ) -> bool: ...
    def set_temporal_at(
        self,
        index: MeasIndex,
        timestep: Timestep,
        force: bool = False,
    ) -> bool: ...
    def unset_temporal(self) -> float | None: ...

class _CoreTemporal3_2:
    def set_temporal(
        self,
        name: Shortname,
        timestep: Timestep,
        force: bool = False,
    ) -> bool: ...
    def set_temporal_at(
        self, index: MeasIndex, timestep: Timestep, force: bool = False
    ) -> bool: ...
    def unset_temporal(self, force: bool = False) -> float | None: ...

class _CoreGetSetMeas(Generic[_N, _O, _T]):
    @property
    def temporal(self) -> tuple[MeasIndex, Shortname, _T] | None: ...
    @property
    def measurements(self) -> list[_O | _T]: ...
    def remove_measurement_by_name(
        self, name: Shortname
    ) -> tuple[MeasIndex, _O | _T]: ...
    def remove_measurement_by_index(self, index: MeasIndex) -> tuple[_N, _O | _T]: ...
    def measurement_at(self, index: MeasIndex) -> _O | _T: ...
    def replace_optical_at(self, index: MeasIndex, meas: _O) -> _O | _T: ...
    def replace_optical_named(self, name: Shortname, meas: _O) -> _O | _T | None: ...
    def rename_temporal(self, name: Shortname) -> Shortname | None: ...

class _CoreReplaceTemporal2_0(Generic[_N, _O, _T]):
    def replace_temporal_at(self, index: MeasIndex, meas: _T) -> _O | _T: ...
    def replace_temporal_named(self, name: Shortname, meas: _T) -> _O | _T | None: ...

class _CoreReplaceTemporal3_2:
    def replace_temporal_at(
        self,
        index: MeasIndex,
        meas: Temporal3_2,
        force: bool = False,
    ) -> Optical3_2 | Temporal3_2: ...
    def replace_temporal_named(
        self,
        name: Shortname,
        meas: Temporal3_2,
        force: bool = False,
    ) -> Optical3_2 | Temporal3_2 | None: ...

class _CoreTEXTGetSetMeas(Generic[_N, _T, _O]):
    def push_optical(
        self, name: _N, meas: _O, range: Range, notrunc: bool = False
    ) -> None: ...
    def insert_optical(
        self, index: MeasIndex, name: _N, meas: _O, range: Range, notrunc: bool = False
    ) -> None: ...
    def push_temporal(
        self, name: Shortname, meas: _T, range: Range, notrunc: bool = False
    ) -> None: ...
    def insert_temporal(
        self,
        index: MeasIndex,
        name: Shortname,
        meas: _T,
        range: Range,
        notrunc: bool = False,
    ) -> None: ...
    def unset_measurements(self) -> None: ...

class _CoreDatasetGetSetMeas(Generic[_N, _T, _O]):
    analysis: AnalysisBytes
    others: list[OtherBytes]

    def push_optical(
        self, name: _N, meas: _O, col: Series, range: Range, notrunc: bool = False
    ) -> None: ...
    def insert_optical(
        self,
        index: MeasIndex,
        name: _N,
        meas: _O,
        col: Series,
        range: Range,
        notrunc: bool = False,
    ) -> None: ...
    def push_temporal(
        self,
        name: Shortname,
        meas: _T,
        col: Series,
        range: Range,
        notrunc: bool = False,
    ) -> None: ...
    def insert_temporal(
        self,
        index: MeasIndex,
        name: Shortname,
        meas: _T,
        col: Series,
        range: Range,
        notrunc: bool = False,
    ) -> None: ...
    def unset_data(self) -> None: ...
    def truncate_data(self, skip_conv_check: bool = False) -> None: ...
    data: DataFrame

class _CoreGetSetMeasOrdered(Generic[_O, _T]):
    layout: _AnyOrderedLayout

    def set_measurements(
        self,
        measurements: _RawInput[Shortname | None, _O, _T],
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_measurements_and_layout(
        self,
        measurements: _RawInput[Shortname | None, _O, _T],
        layout: _AnyOrderedLayout,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...

class _CoreGetSetMeasEndian(Generic[_L, _O, _T]):
    layout: _L

    def set_measurements(
        self,
        measurements: _RawInput[Shortname, _O, _T],
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_measurements_and_layout(
        self,
        measurements: _RawInput[Shortname, _O, _T],
        layout: _L,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...

class _CoreDatasetGetSetMeasOrdered(Generic[_O, _T]):
    def set_measurements_and_data(
        self,
        measurements: _RawInput[Shortname | None, _O, _T],
        data: DataFrame,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...

class _CoreDatasetGetSetMeasEndian(Generic[_O, _T]):
    def set_measurements_and_data(
        self,
        measurements: _RawInput[Shortname, _O, _T],
        data: DataFrame,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
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
    def cstot(self) -> int | None: ...
    @property
    def csvbits(self) -> int | None: ...
    @property
    def csvflags(self) -> CsvFlags | None: ...

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
    spillover: Spillover

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
    unstainedcenters: dict[Shortname, float] | None
    carriertype: str | None
    carrierid: str | None
    locationid: str | None
    begindatetime: datetime | None
    enddatetime: datetime | None

    all_detector_names: _OpticalKeyVals[str]
    all_tags: _OpticalKeyVals[str]
    all_features: _OpticalKeyVals[Feature]
    all_analytes: _OpticalKeyVals[str]
    # TODO this can return a list of all types including the time channel since
    # they share the same kw
    all_measurement_types: _OpticalKeyVals[str]

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
    def to_version_2_0(self, force: bool = False) -> _X: ...

class _CoreTo3_0(Generic[_X]):
    def to_version_3_0(self, force: bool = False) -> _X: ...

class _CoreTo3_1(Generic[_X]):
    def to_version_3_1(self, force: bool = False) -> _X: ...

class _CoreTo3_2(Generic[_X]):
    def to_version_3_2(self, force: bool = False) -> _X: ...

@final
class CoreTEXT2_0(
    _CoreCommon,
    _CorePre3_1,
    _CorePre3_2,
    _CoreTemporal2_0,
    _CoreShortnamesMaybe,
    _CoreGetSetMeas[Shortname | None, Optical2_0, Temporal2_0],
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
        cyt: str | None = None,
        comp: npt.NDArray[np.float32] | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        abrt: int | None = None,
        com: str | None = None,
        cells: str | None = None,
        exp: str | None = None,
        fil: str | None = None,
        inst: str | None = None,
        lost: int | None = None,
        op: str | None = None,
        proj: str | None = None,
        smno: str | None = None,
        src: str | None = None,
        sys: str | None = None,
        tr: Trigger | None = None,
        applied_gates: AppliedGates2_0 = ([], {}, None),
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        std: StdKeywords,
        nonstd: NonStdKeywords,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = _DEFAULT_TIME_MEAS_PATTERN,
        allow_missing_time: bool = False,
        force_time_linear: bool = False,
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        allow_pseudostandard: bool = False,
        allow_unused_standard: bool = False,
        disallow_deprecated: bool = False,
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = None,
        integer_widths_from_byteord: bool = False,
        integer_byteord_override: list[int] | None = None,
        disallow_range_truncation: bool = False,
        warnings_are_errors: bool = False,
    ) -> Self: ...

@final
class CoreTEXT3_0(
    _CoreCommon,
    _CorePre3_1,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreShortnamesMaybe,
    _CoreGetSetMeas[Shortname | None, Optical3_0, Temporal3_0],
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
        cyt: str | None = None,
        comp: Compensation | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        cytsn: str | None = None,
        unicode: Unicode | None = None,
        csvbits: int | None = None,
        cstot: int | None = None,
        csvflags: CsvFlags | None = None,
        abrt: int | None = None,
        com: str | None = None,
        cells: str | None = None,
        exp: str | None = None,
        fil: str | None = None,
        inst: str | None = None,
        lost: int | None = None,
        op: str | None = None,
        proj: str | None = None,
        smno: str | None = None,
        src: str | None = None,
        sys: str | None = None,
        tr: Trigger | None = None,
        applied_gates: AppliedGates3_0 = ([], {}, None),
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        std: StdKeywords,
        nonstd: NonStdKeywords,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = _DEFAULT_TIME_MEAS_PATTERN,
        allow_missing_time: bool = False,
        force_time_linear: bool = False,
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        allow_pseudostandard: bool = False,
        allow_unused_standard: bool = False,
        disallow_deprecated: bool = False,
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = None,
        ignore_time_gain: bool = False,
        integer_widths_from_byteord: bool = False,
        integer_byteord_override: list[int] | None = None,
        disallow_range_truncation: bool = False,
        warnings_are_errors: bool = False,
    ) -> Self: ...

@final
class CoreTEXT3_1(
    _CoreCommon,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_1, Temporal3_1],
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
        cyt: str | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        cytsn: str | None = None,
        spillover: Spillover | None = None,
        last_modifier: str | None = None,
        last_modified: datetime | None = None,
        originality: Originality | None = None,
        plateid: str | None = None,
        platename: str | None = None,
        wellid: str | None = None,
        vol: float | None = None,
        csvbits: int | None = None,
        cstot: int | None = None,
        csvflags: CsvFlags | None = None,
        abrt: int | None = None,
        com: str | None = None,
        cells: str | None = None,
        exp: str | None = None,
        fil: str | None = None,
        inst: str | None = None,
        lost: int | None = None,
        op: str | None = None,
        proj: str | None = None,
        smno: str | None = None,
        src: str | None = None,
        sys: str | None = None,
        tr: Trigger | None = None,
        applied_gates: AppliedGates3_0 = ([], {}, None),
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        std: StdKeywords,
        nonstd: NonStdKeywords,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = _DEFAULT_TIME_MEAS_PATTERN,
        allow_missing_time: bool = False,
        force_time_linear: bool = False,
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        allow_pseudostandard: bool = False,
        allow_unused_standard: bool = False,
        disallow_deprecated: bool = False,
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = None,
        ignore_time_gain: bool = False,
        parse_indexed_spillover: bool = False,
        disallow_range_truncation: bool = False,
        warnings_are_errors: bool = False,
    ) -> Self: ...

@final
class CoreTEXT3_2(
    _CoreCommon,
    _Core3_2,
    _CoreTemporal3_2,
    _CoreGetSetMeas[Shortname, Optical3_2, Temporal3_2],
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
        cytsn: str | None = None,
        spillover: Spillover | None = None,
        last_modifier: str | None = None,
        last_modified: datetime | None = None,
        originality: Originality | None = None,
        plateid: str | None = None,
        platename: str | None = None,
        wellid: str | None = None,
        vol: float | None = None,
        carrierid: str | None = None,
        carriertype: str | None = None,
        locationid: str | None = None,
        unstainedinfo: str | None = None,
        unstainedcenters: UnstainedCenters | None = None,
        flowrate: str | None = None,
        abrt: int | None = None,
        com: str | None = None,
        cells: str | None = None,
        exp: str | None = None,
        fil: str | None = None,
        inst: str | None = None,
        lost: int | None = None,
        op: str | None = None,
        proj: str | None = None,
        smno: str | None = None,
        src: str | None = None,
        sys: str | None = None,
        tr: Trigger | None = None,
        applied_gates: AppliedGates3_2 = ({}, None),
        nonstandard_keywords: NonStdKeywords = {},
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        std: StdKeywords,
        nonstd: NonStdKeywords,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = _DEFAULT_TIME_MEAS_PATTERN,
        allow_missing_time: bool = False,
        force_time_linear: bool = False,
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        allow_pseudostandard: bool = False,
        allow_unused_standard: bool = False,
        disallow_deprecated: bool = False,
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = None,
        ignore_time_gain: bool = False,
        parse_indexed_spillover: bool = False,
        disallow_range_truncation: bool = False,
        warnings_are_errors: bool = False,
    ) -> Self: ...

@final
class CoreDataset2_0(
    _CoreCommon,
    _CorePre3_1,
    _CorePre3_2,
    _CoreTemporal2_0,
    _CoreShortnamesMaybe,
    _CoreGetSetMeas[Shortname | None, Optical2_0, Temporal2_0],
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
        cyt: str | None = None,
        comp: npt.NDArray[np.float32] | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        abrt: int | None = None,
        com: str | None = None,
        cells: str | None = None,
        exp: str | None = None,
        fil: str | None = None,
        inst: str | None = None,
        lost: int | None = None,
        op: str | None = None,
        proj: str | None = None,
        smno: str | None = None,
        src: str | None = None,
        sys: str | None = None,
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
        std: StdKeywords,
        nonstd: NonStdKeywords,
        data_seg: Segment,
        analysis_seg: Segment = _DEFAULT_SEGMENT,
        other_segs: list[Segment] = [],
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = _DEFAULT_TIME_MEAS_PATTERN,
        allow_missing_time: bool = False,
        force_time_linear: bool = False,
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        allow_pseudostandard: bool = False,
        allow_unused_standard: bool = False,
        disallow_deprecated: bool = False,
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = None,
        integer_widths_from_byteord: bool = False,
        integer_byteord_override: list[int] | None = None,
        disallow_range_truncation: bool = False,
        allow_uneven_event_width: bool = False,
        allow_tot_mismatch: bool = False,
        warnings_are_errors: bool = False,
    ) -> Self: ...

@final
class CoreDataset3_0(
    _CoreCommon,
    _CorePre3_1,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreShortnamesMaybe,
    _CoreGetSetMeas[Shortname | None, Optical3_0, Temporal3_0],
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
        cyt: str | None = None,
        comp: Compensation | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        cytsn: str | None = None,
        unicode: Unicode | None = None,
        csvbits: int | None = None,
        cstot: int | None = None,
        csvflags: CsvFlags | None = None,
        abrt: int | None = None,
        com: str | None = None,
        cells: str | None = None,
        exp: str | None = None,
        fil: str | None = None,
        inst: str | None = None,
        lost: int | None = None,
        op: str | None = None,
        proj: str | None = None,
        smno: str | None = None,
        src: str | None = None,
        sys: str | None = None,
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
        std: StdKeywords,
        nonstd: NonStdKeywords,
        data_seg: Segment,
        analysis_seg: Segment = _DEFAULT_SEGMENT,
        other_segs: list[Segment] = [],
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = _DEFAULT_TIME_MEAS_PATTERN,
        allow_missing_time: bool = False,
        force_time_linear: bool = False,
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        allow_pseudostandard: bool = False,
        allow_unused_standard: bool = False,
        disallow_deprecated: bool = False,
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = None,
        ignore_time_gain: bool = False,
        integer_widths_from_byteord: bool = False,
        integer_byteord_override: list[int] | None = None,
        disallow_range_truncation: bool = False,
        text_data_correction: OffsetCorrection = _DEFAULT_CORRECTION,
        text_analysis_correction: OffsetCorrection = _DEFAULT_CORRECTION,
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_missing_required_offsets: bool = False,
        allow_header_text_offset_mismatch: bool = False,
        truncate_text_offsets: bool = False,
        allow_uneven_event_width: bool = False,
        allow_tot_mismatch: bool = False,
        warnings_are_errors: bool = False,
    ) -> Self: ...

@final
class CoreDataset3_1(
    _CoreCommon,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_1, Temporal3_1],
    _CoreReplaceTemporal2_0[Shortname | None, Optical2_0, Temporal2_0],
    _CoreDatasetGetSetMeas[Shortname, Temporal3_1, Optical3_1],
    _CoreGetSetMeasEndian[_AnyNonMixedLayout, Optical3_1, Temporal3_1],
    _CoreDatasetGetSetMeasEndian[Optical3_1, Temporal3_1],
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
        cyt: str | None = None,
        btim: time | None = None,
        etim: time | None = None,
        date: date | None = None,
        cytsn: str | None = None,
        spillover: Spillover | None = None,
        last_modifier: str | None = None,
        last_modified: datetime | None = None,
        originality: Originality | None = None,
        plateid: str | None = None,
        platename: str | None = None,
        wellid: str | None = None,
        vol: float | None = None,
        csvbits: int | None = None,
        cstot: int | None = None,
        csvflags: CsvFlags | None = None,
        abrt: int | None = None,
        com: str | None = None,
        cells: str | None = None,
        exp: str | None = None,
        fil: str | None = None,
        inst: str | None = None,
        lost: int | None = None,
        op: str | None = None,
        proj: str | None = None,
        smno: str | None = None,
        src: str | None = None,
        sys: str | None = None,
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
        std: StdKeywords,
        nonstd: NonStdKeywords,
        data_seg: Segment,
        analysis_seg: Segment = _DEFAULT_SEGMENT,
        other_segs: list[Segment] = [],
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = _DEFAULT_TIME_MEAS_PATTERN,
        allow_missing_time: bool = False,
        force_time_linear: bool = False,
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        allow_pseudostandard: bool = False,
        allow_unused_standard: bool = False,
        disallow_deprecated: bool = False,
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = None,
        ignore_time_gain: bool = False,
        parse_indexed_spillover: bool = False,
        disallow_range_truncation: bool = False,
        text_data_correction: OffsetCorrection = _DEFAULT_CORRECTION,
        text_analysis_correction: OffsetCorrection = _DEFAULT_CORRECTION,
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_missing_required_offsets: bool = False,
        allow_header_text_offset_mismatch: bool = False,
        truncate_text_offsets: bool = False,
        allow_uneven_event_width: bool = False,
        allow_tot_mismatch: bool = False,
        warnings_are_errors: bool = False,
    ) -> Self: ...

@final
class CoreDataset3_2(
    _CoreCommon,
    _Core3_2,
    _CoreTemporal3_2,
    _CoreGetSetMeas[Shortname, Optical3_2, Temporal3_2],
    _CoreReplaceTemporal3_2,
    _CoreDatasetGetSetMeas[Shortname, Temporal3_2, Optical3_2],
    _CoreGetSetMeasEndian[_AnyMixedLayout, Optical3_2, Temporal3_2],
    _CoreDatasetGetSetMeasEndian[Optical3_2, Temporal3_2],
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
        cytsn: str | None = None,
        spillover: Spillover | None = None,
        last_modifier: str | None = None,
        last_modified: datetime | None = None,
        originality: Originality | None = None,
        plateid: str | None = None,
        platename: str | None = None,
        wellid: str | None = None,
        vol: float | None = None,
        carrierid: str | None = None,
        carriertype: str | None = None,
        locationid: str | None = None,
        unstainedinfo: str | None = None,
        unstainedcenters: UnstainedCenters | None = None,
        flowrate: str | None = None,
        abrt: int | None = None,
        com: str | None = None,
        cells: str | None = None,
        exp: str | None = None,
        fil: str | None = None,
        inst: str | None = None,
        lost: int | None = None,
        op: str | None = None,
        proj: str | None = None,
        smno: str | None = None,
        src: str | None = None,
        sys: str | None = None,
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
        std: StdKeywords,
        nonstd: NonStdKeywords,
        data_seg: Segment,
        analysis_seg: Segment = _DEFAULT_SEGMENT,
        other_segs: list[Segment] = [],
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: str | None = _DEFAULT_TIME_MEAS_PATTERN,
        allow_missing_time: bool = False,
        force_time_linear: bool = False,
        ignore_time_optical_keys: list[TemporalOpticalKey] = [],
        date_pattern: str | None = None,
        time_pattern: str | None = None,
        allow_pseudostandard: bool = False,
        allow_unused_standard: bool = False,
        disallow_deprecated: bool = False,
        fix_log_scale_offsets: bool = False,
        nonstandard_measurement_pattern: str | None = None,
        ignore_time_gain: bool = False,
        parse_indexed_spillover: bool = False,
        disallow_range_truncation: bool = False,
        text_data_correction: OffsetCorrection = _DEFAULT_CORRECTION,
        text_analysis_correction: OffsetCorrection = _DEFAULT_CORRECTION,
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_missing_required_offsets: bool = False,
        allow_header_text_offset_mismatch: bool = False,
        truncate_text_offsets: bool = False,
        allow_uneven_event_width: bool = False,
        allow_tot_mismatch: bool = False,
        warnings_are_errors: bool = False,
    ) -> Self: ...

class PyreflowException(Exception): ...
class PyreflowWarning(Exception): ...

@final
class HeaderSegments:
    def __new__(
        cls,
        text_seg: Segment,
        data_seg: Segment,
        analysis_seg: Segment,
        other_segs: list[Segment],
    ) -> Self: ...
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
    def __new__(cls, version: FCSVersion, segments: HeaderSegments) -> Self: ...
    @property
    def version(self) -> FCSVersion: ...
    @property
    def segments(self) -> HeaderSegments: ...

@final
class ValidKeywords:
    def __new__(cls, std: StdKeywords, nonstd: NonStdKeywords) -> Self: ...
    @property
    def std(self) -> StdKeywords: ...
    @property
    def nonstd(self) -> NonStdKeywords: ...

@final
class ExtraStdKeywords:
    def __new__(cls, pseudostandard: StdKeywords, unused: StdKeywords) -> Self: ...
    @property
    def pseudostandard(self) -> StdKeywords: ...
    @property
    def unused(self) -> StdKeywords: ...

@final
class DatasetSegments:
    def __new__(
        cls,
        data_seg: Segment,
        analysis_seg: Segment,
    ) -> Self: ...
    @property
    def data_seg(self) -> Segment: ...
    @property
    def analysis_seg(self) -> Segment: ...

@final
class RawTEXTParseData:
    def __new__(
        cls,
        header_segments: HeaderSegments,
        supp_text: Segment | None,
        nextdata: int | None,
        delimiter: int,
        non_ascii: list[tuple[str, str]],
        byte_pairs: list[tuple[bytes, bytes]],
    ) -> Self: ...
    @property
    def header_segments(self) -> HeaderSegments: ...
    @property
    def supp_text(self) -> Segment | None: ...
    @property
    def nextdata(self) -> int | None: ...
    @property
    def delimiter(self) -> int: ...
    @property
    def non_ascii(self) -> list[tuple[str, str]]: ...
    @property
    def byte_pairs(self) -> list[tuple[bytes, bytes]]: ...

@final
class RawTEXTOutput:
    def __new__(
        cls,
        version: FCSVersion,
        kws: ValidKeywords,
        parse: RawTEXTParseData,
    ) -> Self: ...
    @property
    def version(self) -> FCSVersion: ...
    @property
    def kws(self) -> ValidKeywords: ...
    @property
    def parse(self) -> RawTEXTParseData: ...

@final
class RawDatasetWithKwsOutput:
    def __new__(
        cls,
        data: DataFrame,
        analysis: bytes,
        others: list[bytes],
        dataset_segs: DatasetSegments,
    ) -> Self: ...
    @property
    def data(self) -> DataFrame: ...
    @property
    def analysis(self) -> bytes: ...
    @property
    def others(self) -> list[bytes]: ...
    @property
    def dataset_segs(self) -> DatasetSegments: ...

@final
class RawDatasetOutput:
    def __new__(
        cls,
        text: RawTEXTOutput,
        dataset: RawDatasetWithKwsOutput,
    ) -> Self: ...
    @property
    def text(self) -> RawTEXTOutput: ...
    @property
    def dataset(self) -> RawDatasetWithKwsOutput: ...

@final
class StdTEXTOutput:
    def __new__(
        cls,
        tot: int | None,
        dataset_segs: DatasetSegments,
        extra: ExtraStdKeywords,
        parse: RawTEXTParseData,
    ) -> Self: ...
    @property
    def tot(self) -> int | None: ...
    @property
    def dataset_segs(self) -> DatasetSegments: ...
    @property
    def extra(self) -> ExtraStdKeywords: ...
    @property
    def parse(self) -> RawTEXTParseData: ...

@final
class StdDatasetWithKwsOutput:
    def __new__(
        cls,
        dataset_segs: DatasetSegments,
        extra: ExtraStdKeywords,
    ) -> Self: ...
    @property
    def dataset_segs(self) -> DatasetSegments: ...
    @property
    def extra(self) -> ExtraStdKeywords: ...

@final
class StdDatasetOutput:
    def __new__(
        cls,
        dataset: StdDatasetWithKwsOutput,
        parse: RawTEXTParseData,
    ) -> Self: ...
    @property
    def dataset(self) -> StdDatasetWithKwsOutput: ...
    @property
    def parse(self) -> RawTEXTParseData: ...

def fcs_read_header(
    path: Path,
    text_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    data_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    analysis_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = _DEFAULT_OTHER_WIDTH,
    squish_offsets: bool = False,
    allow_negative: bool = False,
    truncate_offsets: bool = False,
) -> Header: ...

#
def fcs_read_raw_text(
    path: Path,
    # header args
    text_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    data_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    analysis_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = _DEFAULT_OTHER_WIDTH,
    squish_offsets: bool = False,
    allow_negative: bool = False,
    truncate_offsets: bool = False,
    # raw args
    version_override: FCSVersion | None = None,
    supp_text_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    allow_duplicated_supp_text: bool = False,
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
    allow_missing_supp_text: bool = False,
    allow_supp_text_own_delim: bool = False,
    allow_missing_nextdata: bool = False,
    trim_value_whitespace: bool = False,
    ignore_standard_keys: KeyPatterns = _DEFAULT_KEY_PATTERNS,
    promote_to_standard: KeyPatterns = _DEFAULT_KEY_PATTERNS,
    demote_from_standard: KeyPatterns = _DEFAULT_KEY_PATTERNS,
    rename_standard_keys: dict[str, str] = {},
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    # shared args
    warnings_are_errors: bool = False,
) -> RawTEXTOutput: ...

#
def fcs_read_std_text(
    path: Path,
    # header args
    text_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    data_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    analysis_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = _DEFAULT_OTHER_WIDTH,
    squish_offsets: bool = False,
    allow_negative: bool = False,
    truncate_offsets: bool = False,
    # raw args
    version_override: FCSVersion | None = None,
    supp_text_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    allow_duplicated_supp_text: bool = False,
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
    allow_missing_supp_text: bool = False,
    allow_supp_text_own_delim: bool = False,
    allow_missing_nextdata: bool = False,
    trim_value_whitespace: bool = False,
    ignore_standard_keys: KeyPatterns = _DEFAULT_KEY_PATTERNS,
    promote_to_standard: KeyPatterns = _DEFAULT_KEY_PATTERNS,
    demote_from_standard: KeyPatterns = _DEFAULT_KEY_PATTERNS,
    rename_standard_keys: dict[str, str] = {},
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    # standard args
    trim_intra_value_whitespace: bool = False,
    time_meas_pattern: str | None = _DEFAULT_TIME_MEAS_PATTERN,
    allow_missing_time: bool = False,
    force_time_linear: bool = False,
    ignore_time_optical_keys: list[TemporalOpticalKey] = [],
    date_pattern: str | None = None,
    time_pattern: str | None = None,
    allow_pseudostandard: bool = False,
    allow_unused_standard: bool = False,
    disallow_deprecated: bool = False,
    fix_log_scale_offsets: bool = False,
    nonstandard_measurement_pattern: str | None = None,
    ignore_time_gain: bool = False,
    parse_indexed_spillover: bool = False,
    # offset args
    text_data_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    text_analysis_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_missing_required_offsets: bool = False,
    allow_header_text_offset_mismatch: bool = False,
    truncate_text_offsets: bool = False,
    # layout args
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: bool = False,
    # shared args
    warnings_are_errors: bool = False,
) -> tuple[AnyCoreTEXT, StdTEXTOutput]: ...

#
def fcs_read_raw_dataset(
    path: Path,
    # header args
    text_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    data_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    analysis_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = _DEFAULT_OTHER_WIDTH,
    squish_offsets: bool = False,
    allow_negative: bool = False,
    truncate_offsets: bool = False,
    # raw args
    version_override: FCSVersion | None = None,
    supp_text_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    allow_duplicated_supp_text: bool = False,
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
    allow_missing_supp_text: bool = False,
    allow_supp_text_own_delim: bool = False,
    allow_missing_nextdata: bool = False,
    trim_value_whitespace: bool = False,
    ignore_standard_keys: KeyPatterns = _DEFAULT_KEY_PATTERNS,
    promote_to_standard: KeyPatterns = _DEFAULT_KEY_PATTERNS,
    demote_from_standard: KeyPatterns = _DEFAULT_KEY_PATTERNS,
    rename_standard_keys: dict[str, str] = {},
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    # offset args
    text_data_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    text_analysis_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_missing_required_offsets: bool = False,
    allow_header_text_offset_mismatch: bool = False,
    truncate_text_offsets: bool = False,
    # layout args
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: bool = False,
    # data args
    allow_uneven_event_width: bool = False,
    allow_tot_mismatch: bool = False,
    # shared args
    warnings_are_errors: bool = False,
) -> RawTEXTOutput: ...

#
def fcs_read_std_dataset(
    path: Path,
    # header args
    text_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    data_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    analysis_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    other_corrections: list[OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = _DEFAULT_OTHER_WIDTH,
    squish_offsets: bool = False,
    allow_negative: bool = False,
    truncate_offsets: bool = False,
    # raw args
    version_override: FCSVersion | None = None,
    supp_text_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    allow_duplicated_supp_text: bool = False,
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
    allow_missing_supp_text: bool = False,
    allow_supp_text_own_delim: bool = False,
    allow_missing_nextdata: bool = False,
    trim_value_whitespace: bool = False,
    ignore_standard_keys: KeyPatterns = _DEFAULT_KEY_PATTERNS,
    promote_to_standard: KeyPatterns = _DEFAULT_KEY_PATTERNS,
    demote_from_standard: KeyPatterns = _DEFAULT_KEY_PATTERNS,
    rename_standard_keys: dict[str, str] = {},
    replace_standard_key_values: dict[str, str] = {},
    append_standard_keywords: dict[str, str] = {},
    # standard args
    trim_intra_value_whitespace: bool = False,
    time_meas_pattern: str | None = _DEFAULT_TIME_MEAS_PATTERN,
    allow_missing_time: bool = False,
    force_time_linear: bool = False,
    ignore_time_optical_keys: list[TemporalOpticalKey] = [],
    date_pattern: str | None = None,
    time_pattern: str | None = None,
    allow_pseudostandard: bool = False,
    allow_unused_standard: bool = False,
    disallow_deprecated: bool = False,
    fix_log_scale_offsets: bool = False,
    nonstandard_measurement_pattern: str | None = None,
    ignore_time_gain: bool = False,
    parse_indexed_spillover: bool = False,
    # offset args
    text_data_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    text_analysis_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_missing_required_offsets: bool = False,
    allow_header_text_offset_mismatch: bool = False,
    truncate_text_offsets: bool = False,
    # layout args
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: bool = False,
    # data args
    allow_uneven_event_width: bool = False,
    allow_tot_mismatch: bool = False,
    # shared args
    warnings_are_errors: bool = False,
) -> tuple[AnyCoreDataset, StdDatasetOutput]: ...

#
def fcs_read_raw_dataset_with_keywords(
    path: Path,
    version: FCSVersion,
    std: dict[str, str],
    data_seg: Segment,
    analysis_seg: Segment = _DEFAULT_SEGMENT,
    other_segs: list[Segment] = [],
    # offset args
    text_data_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    text_analysis_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_missing_required_offsets: bool = False,
    allow_header_text_offset_mismatch: bool = False,
    truncate_text_offsets: bool = False,
    # layout args
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: bool = False,
    # data args
    allow_uneven_event_width: bool = False,
    allow_tot_mismatch: bool = False,
    # shared args
    warnings_are_errors: bool = False,
) -> RawDatasetWithKwsOutput: ...

#
def fcs_read_std_dataset_with_keywords(
    path: Path,
    version: FCSVersion,
    std: dict[str, str],
    nonstd: dict[str, str],
    data_seg: Segment,
    analysis_seg: Segment = _DEFAULT_SEGMENT,
    other_segs: list[Segment] = [],
    # standard args
    trim_intra_value_whitespace: bool = False,
    time_meas_pattern: str | None = _DEFAULT_TIME_MEAS_PATTERN,
    allow_missing_time: bool = False,
    force_time_linear: bool = False,
    ignore_time_optical_keys: list[TemporalOpticalKey] = [],
    date_pattern: str | None = None,
    time_pattern: str | None = None,
    allow_pseudostandard: bool = False,
    allow_unused_standard: bool = False,
    disallow_deprecated: bool = False,
    fix_log_scale_offsets: bool = False,
    nonstandard_measurement_pattern: str | None = None,
    ignore_time_gain: bool = False,
    parse_indexed_spillover: bool = False,
    # offset args
    text_data_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    text_analysis_correction: OffsetCorrection = _DEFAULT_CORRECTION,
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_missing_required_offsets: bool = False,
    allow_header_text_offset_mismatch: bool = False,
    truncate_text_offsets: bool = False,
    # layout args
    integer_widths_from_byteord: bool = False,
    integer_byteord_override: ByteOrd | None = None,
    disallow_range_truncation: bool = False,
    # data args
    allow_uneven_event_width: bool = False,
    allow_tot_mismatch: bool = False,
    # shared args
    warnings_are_errors: bool = False,
) -> StdDatasetWithKwsOutput: ...

__version__: str

__all__ = [
    "__version__",
    "PyreflowWarning",
    "PyreflowException",
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
    "HeaderSegments",
    "RawTEXTOutput",
    "RawDatasetOutput",
    "RawDatasetWithKwsOutput",
    "RawTEXTParseData",
    "StdTEXTOutput",
    "StdDatasetOutput",
    "StdDatasetWithKwsOutput",
    "ExtraStdKeywords",
    "ValidKeywords",
    "DatasetSegments",
    "fcs_read_header",
    "fcs_read_raw_text",
    "fcs_read_std_text",
    "fcs_read_raw_dataset",
    "fcs_read_std_dataset",
    "fcs_read_raw_dataset_with_keywords",
    "fcs_read_std_dataset_with_keywords",
]
