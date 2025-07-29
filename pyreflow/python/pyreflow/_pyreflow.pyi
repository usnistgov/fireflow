from __future__ import annotations
from pyreflow import (
    NonStdKey,
    Mode,
    Trigger,
    Shortname,
    NonStdKeywords,
    AnalysisBytes,
    OtherBytes,
    Datatype,
    MixedType,
    Originality,
    Feature,
    Calibration3_1,
    Calibration3_2,
)
from datetime import time, date, datetime
from typing import TypeVar, Self, Generic, Union
from polars import Series, DataFrame
import numpy as np
import numpy.typing as npt

X = TypeVar("X")
C = TypeVar("C")
N = TypeVar("N")
L = TypeVar("L")

OpticalKeyVals = list[tuple[int, X | None]]
Scale = tuple[float, float] | tuple[()]
ScaleTransform = tuple[float, float] | tuple[float]
Display = tuple[bool, float, float]

class _LayoutCommon:
    @property
    def widths(self) -> list[int]: ...
    @property
    def ranges(self) -> list[float]: ...
    @property
    def datatype(self) -> Datatype: ...
    @property
    def datatypes(self) -> list[Datatype]: ...

class _LayoutOrderedCommon:
    @property
    def byte_order(self) -> list[int]: ...
    @property
    def is_big_endian(self) -> bool | None: ...

class _LayoutEndianCommon:
    @property
    def is_big_endian(self) -> bool: ...

class _LayoutAsciiCommon:
    def __new__(cls, ranges: list[int]) -> Self: ...

class _LayoutOrderedUintCommon:
    def __new__(cls, ranges: list[int], is_big: bool) -> Self: ...
    def new_ordered(cls, ranges: list[int], byteord: list[int]) -> Self: ...

class _LayoutOrderedFloatCommon:
    def __new__(cls, ranges: list[float], is_big: bool) -> Self: ...
    def new_ordered(cls, ranges: list[float], byteord: list[int]) -> Self: ...

class _LayoutEndianFloatCommon:
    def __new__(cls, ranges: list[float], is_big: bool) -> Self: ...

class AsciiFixedLayout(_LayoutCommon, _LayoutAsciiCommon): ...
class AsciiDelimLayout(_LayoutCommon, _LayoutAsciiCommon): ...
class OrderedUint08Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...
class OrderedUint16Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...
class OrderedUint24Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...
class OrderedUint32Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...
class OrderedUint40Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...
class OrderedUint48Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...
class OrderedUint56Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...
class OrderedUint64Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...
class OrderedF32Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedFloatCommon
): ...
class OrderedF64Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedFloatCommon
): ...
class EndianF32Layout(_LayoutCommon, _LayoutEndianCommon, _LayoutEndianFloatCommon): ...
class EndianF64Layout(_LayoutCommon, _LayoutEndianCommon, _LayoutEndianFloatCommon): ...

class EndianUintLayout(_LayoutCommon, _LayoutEndianCommon):
    def __new__(cls, ranges: list[int], is_big: bool) -> Self: ...

class MixedLayout(_LayoutCommon, _LayoutEndianCommon):
    def __new__(cls, ranges: list[MixedType], is_big: bool) -> Self: ...

AnyOrderedLayout = Union[
    AsciiFixedLayout
    | AsciiDelimLayout
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

AnyNonMixedLayout = Union[
    AsciiFixedLayout
    | AsciiDelimLayout
    | EndianF32Layout
    | EndianF64Layout
    | EndianUintLayout
]

AnyMixedLayout = Union[
    AsciiFixedLayout
    | AsciiDelimLayout
    | EndianF32Layout
    | EndianF64Layout
    | EndianUintLayout
    | MixedLayout
]

class _MeasCommon:
    nonstandard_keywords: NonStdKeywords

    def nonstandard_insert(self, key: NonStdKey, value: str) -> str | None: ...
    def nonstandard_get(self, key: NonStdKey) -> str | None: ...
    def nonstandard_remove(self, key: NonStdKey) -> str | None: ...

class _OpticalWavelength:
    wavelength: float | None

class _OpticalWavelengths:
    wavelengths: list[float] | None

class _MeasDisplay:
    display: Display

class _OpticalCommon:
    filter: str | None
    detector_type: str | None
    detector_voltage: float | None
    power: float | None
    percent_emitted: str | None

class _OpticalScaleTransform:
    transform: ScaleTransform

    def __new__(cls, scale: Scale) -> Self: ...

class _TemporalTimestep:
    timestep: float

    def __new__(cls, timestep: float) -> Self: ...

class Optical2_0(_MeasCommon, _OpticalCommon, _OpticalWavelength):
    scale: Scale | None

    def __new__(cls) -> Self: ...

class Optical3_0(
    _MeasCommon, _OpticalCommon, _OpticalScaleTransform, _OpticalWavelength
): ...

class Optical3_1(
    _MeasCommon,
    _OpticalCommon,
    _OpticalScaleTransform,
    _OpticalWavelengths,
    _MeasDisplay,
):
    calibration: Calibration3_1 | None

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
    feature: str | None
    analyte: str | None

class Temporal2_0(_MeasCommon):
    def __new__(cls) -> Self: ...

class Temporal3_0(_MeasCommon, _TemporalTimestep): ...
class Temporal3_1(_MeasCommon, _MeasDisplay, _TemporalTimestep): ...

class Temporal3_2(_MeasCommon, _MeasDisplay, _TemporalTimestep):
    measurement_type: bool

T = TypeVar("T", bound=Temporal2_0 | Temporal3_0 | Temporal3_1 | Temporal3_2)
O = TypeVar("O", bound=Optical2_0 | Optical3_0 | Optical3_1 | Optical3_2)

RawInput = list[tuple[N, O] | tuple[Shortname, T]]

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
    trigger: Trigger | None

    all_shortnames: list[Shortname]
    longnames: list[str | None]

    filters: OpticalKeyVals[str]
    powers: OpticalKeyVals[float]
    percents_emitted: OpticalKeyVals[int]
    detector_types: OpticalKeyVals[str]
    detector_voltages: OpticalKeyVals[float]

    @property
    def shortnames_maybe(self) -> list[Shortname | None]: ...
    def insert_nonstandard(self, key: NonStdKey, value: str) -> str | None: ...
    def remove_nonstandard(self, key: NonStdKey) -> str | None: ...
    def get_nonstandard(self, key: NonStdKey) -> str | None: ...
    def raw_keywords(self, want_req: bool, want_meta: bool) -> dict[str, str]: ...
    @property
    def par(self) -> int: ...
    def set_trigger_treshold(self, threshold: int) -> bool: ...

class _CoreTemporal2_0:
    def set_temporal(self, name: Shortname, force: bool) -> bool: ...
    def set_temporal_at(self, index: int, force: bool) -> bool: ...
    def unset_temporal(self) -> bool: ...

class _CoreTemporal3_0:
    def set_temporal(self, name: Shortname, timestep: float, force: bool) -> bool: ...
    def set_temporal_at(self, index: int, timestep: float, force: bool) -> bool: ...
    def unset_temporal(self) -> float | None: ...

class _CoreGetSetMeas(Generic[N, O, T]):
    def remove_measurement_by_name(
        self, name: Shortname
    ) -> tuple[int, O | T] | None: ...
    def measurement_at(self, index: int) -> O | T: ...
    def replace_optical_at(self, index: int, meas: O) -> O | T: ...
    def replace_optical_named(self, name: Shortname, meas: O) -> O | T | None: ...
    def rename_temporal(self, name: Shortname) -> Shortname | None: ...
    def replace_temporal_at(self, index: int, meas: T, force: bool) -> O | T: ...
    def replace_temporal_named(
        self, name: Shortname, meas: O, force: bool
    ) -> O | T | None: ...
    @property
    def measurements(self) -> list[O | T]: ...
    def remove_measurement_by_index(self, index: int) -> tuple[N, O | T] | None: ...

class _CoreTEXTGetSetMeas(Generic[N, T, O]):
    def push_optical(self, name: N, meas: O, range: float, notrunc: bool): ...
    def insert_optical(
        self, index: int, meas: O, name: N, range: float, notrunc: bool
    ): ...
    def push_temporal(self, meas: T, name: Shortname, range: float, notrunc: bool): ...
    def insert_temporal(
        self, index: int, name: Shortname, meas: T, range: float, notrunc: bool
    ): ...
    def unset_measurements(self): ...

class _CoreDatasetGetSetMeas(Generic[T]):
    analysis: AnalysisBytes
    others: list[OtherBytes]

    def push_temporal(
        self, name: Shortname, temp: T, col: Series, range: float, notrunc: bool
    ): ...
    def insert_temporal(
        self, index: int, temp: T, col: Series, range: float, notrunc: bool
    ): ...
    def unset_data(self): ...
    @property
    def data(self) -> DataFrame: ...

class _CoreGetSetMeasOrdered(Generic[O, T]):
    @property
    def layout(self) -> AnyOrderedLayout | None: ...
    def set_layout(self, layout: AnyOrderedLayout): ...
    def set_measurements(
        self, raw_input: RawInput[Shortname | None, O, T], prefix: str
    ): ...
    def set_measurements_and_layout(
        self,
        raw_input: RawInput[Shortname | None, O, T],
        layout: AnyOrderedLayout,
        prefix: str,
    ): ...

class _CoreGetSetMeasEndian(Generic[L, O, T]):
    @property
    def layout(self) -> L | None: ...
    def set_layout(self, layout: L): ...
    def set_measurements(self, raw_input: RawInput[Shortname, O, T]): ...
    def set_measurements_and_layout(
        self, raw_input: RawInput[Shortname, O, T], layout: L
    ): ...

class _CoreDatasetGetSetMeasOrdered(Generic[O, T]):
    def set_measurements_and_data(
        self,
        raw_input: RawInput[Shortname | None, O, T],
        cols: list[Series],
        prefix: str,
    ): ...

class _CoreDatasetGetSetMeasEndian(Generic[O, T]):
    def set_measurements_and_data(
        self,
        raw_input: RawInput[Shortname | None, O, T],
        cols: list[Series],
        prefix: str,
    ): ...

class _CoreSetShortnamesMaybe:
    def set_measurement_shortnames_maybe(self, names: list[Shortname | None]): ...

class _CoreScaleMethods:
    scales: list[Scale | None]

    @property
    def all_scales(self) -> list[Scale | None]: ...

class _CoreScaleTransformMethods:
    scale_transforms: list[ScaleTransform]

    @property
    def all_scale_transforms(self) -> list[ScaleTransform]: ...

class _CoreTimestepMethods:
    @property
    def get_timestep(self) -> float | None: ...
    def set_timestep(self, float) -> bool: ...

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
    @property
    def spillover_matrix(self) -> npt.NDArray[np.float32]: ...
    @property
    def spillover_names(self) -> list[Shortname]: ...
    def set_spillover(
        self, names: list[Shortname], matrix: npt.NDArray[np.float32]
    ): ...
    def unset_spillover(self): ...

class _CoreUnicode:
    unicode: tuple[int, list[str]] | None

class _CoreVol:
    vol: float | None

class _CoreCytsn:
    cytsn: str | None

class _CoreMeasWavelength:
    wavelength: OpticalKeyVals[float]

class _CoreMeasWavelengths:
    wavelengths: OpticalKeyVals[list[float]]

class _CoreMeasDisplay:
    display: list[Display | None]

class _CorePre3_2:
    cyt: str | None

    def __new__(cls, mode: Mode) -> Self: ...

class _Core3_2:
    flowrate: str | None
    cyt: str
    detector_names: OpticalKeyVals[str]
    tags: OpticalKeyVals[str]
    feature: OpticalKeyVals[Feature]
    unstainedinfo: str | None
    carriertype: str | None
    carrierid: str | None
    locationid: str | None
    begindatetime: datetime | None
    enddatetime: datetime | None

    def __new__(cls, cyt: str) -> Self: ...
    @property
    def unstained_centers(self) -> dict[Shortname, float] | None: ...
    def insert_unstained_center(self, name: Shortname, value: float): ...
    def remove_unstained_center(self, name: Shortname) -> float | None: ...
    def clear_unstained_centers(self): ...

class _CoreMeasCalibration(Generic[C]):
    calibrations: OpticalKeyVals[C]

class _CoreToDataset(Generic[X]):
    def to_dataset(
        self, cols: list[Series], analysis: AnalysisBytes, others: list[OtherBytes]
    ) -> X: ...

class _CoreTo2_0(Generic[X]):
    def version_2_0(self, lossless: bool) -> X: ...

class _CoreTo3_0(Generic[X]):
    def version_3_0(self, lossless: bool) -> X: ...

class _CoreTo3_1(Generic[X]):
    def version_3_1(self, lossless: bool) -> X: ...

class _CoreTo3_2(Generic[X]):
    def version_3_2(self, lossless: bool) -> X: ...

class CoreTEXT2_0(
    _CoreCommon,
    _CorePre3_2,
    _CoreTemporal2_0,
    _CoreGetSetMeas[Shortname | None, Optical2_0, Temporal2_0],
    _CoreTEXTGetSetMeas[Shortname | None, Temporal2_0, Optical2_0],
    _CoreGetSetMeasOrdered[Optical2_0, Temporal2_0],
    _CoreSetShortnamesMaybe,
    _CoreScaleMethods,
    _CoreToDataset[CoreDataset2_0],
    _CoreCompensation,
    _CoreMeasWavelength,
    _CoreTo3_0[CoreTEXT3_0],
    _CoreTo3_1[CoreTEXT3_1],
    _CoreTo3_2[CoreTEXT3_2],
): ...
class CoreTEXT3_0(
    _CoreCommon,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname | None, Optical3_0, Temporal3_0],
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
    _CoreTo2_0[CoreTEXT2_0],
    _CoreTo3_1[CoreTEXT3_1],
    _CoreTo3_2[CoreTEXT3_2],
): ...
class CoreTEXT3_1(
    _CoreCommon,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_1, Temporal3_1],
    _CoreTEXTGetSetMeas[Shortname, Temporal3_1, Optical3_1],
    _CoreGetSetMeasEndian[AnyNonMixedLayout, Optical3_1, Temporal3_1],
    _CoreScaleTransformMethods,
    _CoreTimestepMethods,
    _CoreToDataset[CoreDataset3_1],
    _CoreModified,
    _CorePlate,
    _CoreSpillover,
    _CoreVol,
    _CoreCytsn,
    _CoreMeasWavelengths,
    _CoreMeasDisplay,
    _CoreMeasCalibration[Calibration3_1],
    _CoreTo2_0[CoreTEXT2_0],
    _CoreTo3_0[CoreTEXT3_0],
    _CoreTo3_2[CoreTEXT3_2],
): ...
class CoreTEXT3_2(
    _CoreCommon,
    _Core3_2,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_2, Temporal3_2],
    _CoreTEXTGetSetMeas[Shortname, Temporal3_2, Optical3_2],
    _CoreGetSetMeasEndian[AnyMixedLayout, Optical3_2, Temporal3_2],
    _CoreScaleMethods,
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
    _CoreTo2_0[CoreTEXT2_0],
    _CoreTo3_0[CoreTEXT3_0],
    _CoreTo3_1[CoreTEXT3_1],
): ...
class CoreDataset2_0(
    _CoreCommon,
    _CorePre3_2,
    _CoreTemporal2_0,
    _CoreGetSetMeas[Shortname | None, Optical2_0, Temporal2_0],
    _CoreDatasetGetSetMeas[Temporal2_0],
    _CoreGetSetMeasOrdered[Optical2_0, Temporal2_0],
    _CoreDatasetGetSetMeasOrdered[Optical2_0, Temporal2_0],
    _CoreSetShortnamesMaybe,
    _CoreCompensation,
    _CoreMeasWavelength,
    _CoreTo3_0[CoreDataset3_0],
    _CoreTo3_1[CoreDataset3_1],
    _CoreTo3_2[CoreDataset3_2],
): ...
class CoreDataset3_0(
    _CoreCommon,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname | None, Optical3_0, Temporal3_0],
    _CoreDatasetGetSetMeas[Temporal3_0],
    _CoreGetSetMeasOrdered[Optical3_0, Temporal3_0],
    _CoreDatasetGetSetMeasOrdered[Optical3_0, Temporal3_0],
    _CoreSetShortnamesMaybe,
    _CoreScaleTransformMethods,
    _CoreTimestepMethods,
    _CoreCompensation,
    _CoreUnicode,
    _CoreCytsn,
    _CoreMeasWavelength,
    _CoreTo2_0[CoreDataset2_0],
    _CoreTo3_1[CoreDataset3_1],
    _CoreTo3_2[CoreDataset3_2],
): ...
class CoreDataset3_1(
    _CoreCommon,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_1, Temporal3_1],
    _CoreDatasetGetSetMeas[Temporal3_1],
    _CoreGetSetMeasEndian[AnyNonMixedLayout, Optical3_1, Temporal3_1],
    _CoreDatasetGetSetMeasEndian[Optical3_1, Temporal3_1],
    _CoreScaleTransformMethods,
    _CoreTimestepMethods,
    _CoreModified,
    _CorePlate,
    _CoreSpillover,
    _CoreVol,
    _CoreCytsn,
    _CoreMeasWavelengths,
    _CoreMeasDisplay,
    _CoreMeasCalibration[Calibration3_1],
    _CoreTo2_0[CoreDataset2_0],
    _CoreTo3_0[CoreDataset3_0],
    _CoreTo3_2[CoreDataset3_2],
): ...
class CoreDataset3_2(
    _CoreCommon,
    _Core3_2,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_2, Temporal3_2],
    _CoreDatasetGetSetMeas[Temporal3_2],
    _CoreGetSetMeasEndian[AnyMixedLayout, Optical3_2, Temporal3_2],
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
    _CoreTo2_0[CoreDataset2_0],
    _CoreTo3_0[CoreDataset3_0],
    _CoreTo3_1[CoreDataset3_1],
): ...
