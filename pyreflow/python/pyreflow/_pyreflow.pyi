from __future__ import annotations
from pyreflow import (
    PyreflowWarning,
    PyreflowException,
)
from pyreflow.typing import (
    Range,
    IntRange,
    FloatRange,
    Timestep,
    Display,
    Scale,
    ScaleTransform,
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
from typing import TypeVar, Self, Generic, Union, final
from polars import Series, DataFrame
import numpy as np
import numpy.typing as npt

_X = TypeVar("_X")
_C = TypeVar("_C")
_N = TypeVar("_N")
_L = TypeVar("_L")

_OpticalKeyVals = list[tuple[int, _X | None]]

class _LayoutCommon:
    @property
    def widths(self) -> list[int]: ...
    @property
    def ranges(self) -> list[Range]: ...
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
    def __new__(cls, ranges: list[IntRange]) -> Self: ...

class _LayoutOrderedUintCommon:
    def __new__(cls, ranges: list[IntRange], is_big: bool) -> Self: ...
    @classmethod
    def new_ordered(cls, ranges: list[IntRange], byteord: list[int]) -> Self: ...

class _LayoutOrderedFloatCommon:
    def __new__(cls, ranges: list[FloatRange], is_big: bool) -> Self: ...
    @classmethod
    def new_ordered(cls, ranges: list[FloatRange], byteord: list[int]) -> Self: ...

class _LayoutEndianFloatCommon:
    def __new__(cls, ranges: list[FloatRange], is_big: bool) -> Self: ...

@final
class AsciiFixedLayout(_LayoutCommon, _LayoutAsciiCommon): ...

@final
class AsciiDelimLayout(_LayoutCommon, _LayoutAsciiCommon): ...

@final
class OrderedUint08Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...

@final
class OrderedUint16Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...

@final
class OrderedUint24Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...

@final
class OrderedUint32Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...

@final
class OrderedUint40Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...

@final
class OrderedUint48Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...

@final
class OrderedUint56Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...

@final
class OrderedUint64Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedUintCommon
): ...

@final
class OrderedF32Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedFloatCommon
): ...

@final
class OrderedF64Layout(
    _LayoutCommon, _LayoutOrderedCommon, _LayoutOrderedFloatCommon
): ...

@final
class EndianF32Layout(_LayoutCommon, _LayoutEndianCommon, _LayoutEndianFloatCommon): ...

@final
class EndianF64Layout(_LayoutCommon, _LayoutEndianCommon, _LayoutEndianFloatCommon): ...

@final
class EndianUintLayout(_LayoutCommon, _LayoutEndianCommon):
    def __new__(cls, ranges: list[IntRange], is_big: bool) -> Self: ...

@final
class MixedLayout(_LayoutCommon, _LayoutEndianCommon):
    def __new__(cls, ranges: list[MixedType], is_big: bool) -> Self: ...

_AnyOrderedLayout = Union[
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

_AnyNonMixedLayout = Union[
    AsciiFixedLayout
    | AsciiDelimLayout
    | EndianF32Layout
    | EndianF64Layout
    | EndianUintLayout
]

_AnyMixedLayout = Union[
    AsciiFixedLayout
    | AsciiDelimLayout
    | EndianF32Layout
    | EndianF64Layout
    | EndianUintLayout
    | MixedLayout
]

class _MeasCommon:
    nonstandard_keywords: NonStdKeywords
    longname: str | None

    def nonstandard_insert(self, key: NonStdKey, value: str) -> str | None: ...
    def nonstandard_get(self, key: NonStdKey) -> str | None: ...
    def nonstandard_remove(self, key: NonStdKey) -> str | None: ...

class _OpticalWavelength:
    wavelength: float | None

class _OpticalWavelengths:
    wavelength: list[float] | None

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
    timestep: Timestep

    def __new__(cls, timestep: Timestep) -> Self: ...

@final
class Optical2_0(_MeasCommon, _OpticalCommon, _OpticalWavelength):
    scale: Scale | None

    def __new__(cls) -> Self: ...

@final
class Optical3_0(
    _MeasCommon, _OpticalCommon, _OpticalScaleTransform, _OpticalWavelength
): ...

@final
class Optical3_1(
    _MeasCommon,
    _OpticalCommon,
    _OpticalScaleTransform,
    _OpticalWavelengths,
    _MeasDisplay,
):
    calibration: Calibration3_1 | None

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
    feature: str | None
    analyte: str | None

@final
class Temporal2_0(_MeasCommon):
    def __new__(cls) -> Self: ...

@final
class Temporal3_0(_MeasCommon, _TemporalTimestep): ...

@final
class Temporal3_1(_MeasCommon, _MeasDisplay, _TemporalTimestep): ...

@final
class Temporal3_2(_MeasCommon, _MeasDisplay, _TemporalTimestep):
    measurement_type: bool

_T = TypeVar("_T", bound=Temporal2_0 | Temporal3_0 | Temporal3_1 | Temporal3_2)
_O = TypeVar("_O", bound=Optical2_0 | Optical3_0 | Optical3_1 | Optical3_2)

_RawInput = list[tuple[_N, _O] | tuple[Shortname, _T]]

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

    filters: _OpticalKeyVals[str]
    powers: _OpticalKeyVals[float]
    percents_emitted: _OpticalKeyVals[int]
    detector_types: _OpticalKeyVals[str]
    detector_voltages: _OpticalKeyVals[float]

    @property
    def shortnames_maybe(self) -> list[Shortname | None]: ...
    def insert_nonstandard(self, key: NonStdKey, value: str) -> str | None: ...
    def remove_nonstandard(self, key: NonStdKey) -> str | None: ...
    def get_nonstandard(self, key: NonStdKey) -> str | None: ...
    def raw_keywords(
        self,
        want_req: bool | None = None,
        want_meta: bool | None = None,
    ) -> dict[str, str]: ...
    @property
    def par(self) -> int: ...
    def set_trigger_threshold(self, threshold: int) -> bool: ...

class _CoreTemporal2_0:
    def set_temporal(self, name: Shortname, force: bool) -> bool: ...
    def set_temporal_at(self, index: int, force: bool) -> bool: ...
    def unset_temporal(self, force: bool) -> bool: ...

class _CoreTemporal3_0:
    def set_temporal(
        self, name: Shortname, timestep: Timestep, force: bool
    ) -> bool: ...
    def set_temporal_at(self, index: int, timestep: Timestep, force: bool) -> bool: ...
    def unset_temporal(self, force: bool) -> float | None: ...

class _CoreGetSetMeas(Generic[_N, _O, _T]):
    def remove_measurement_by_name(
        self, name: Shortname
    ) -> tuple[int, _O | _T] | None: ...
    def measurement_at(self, index: int) -> _O | _T: ...
    def replace_optical_at(self, index: int, meas: _O) -> _O | _T: ...
    def replace_optical_named(self, name: Shortname, meas: _O) -> _O | _T | None: ...
    def rename_temporal(self, name: Shortname) -> Shortname | None: ...
    def replace_temporal_at(self, index: int, meas: _T, force: bool) -> _O | _T: ...
    def replace_temporal_named(
        self, name: Shortname, meas: _O, force: bool
    ) -> _O | _T | None: ...
    @property
    def measurements(self) -> list[_O | _T]: ...
    def remove_measurement_by_index(self, index: int) -> tuple[_N, _O | _T] | None: ...

class _CoreTEXTGetSetMeas(Generic[_N, _T, _O]):
    def push_optical(self, name: _N, meas: _O, range: Range, notrunc: bool): ...
    def insert_optical(
        self, index: int, meas: _O, name: _N, range: Range, notrunc: bool
    ): ...
    def push_temporal(self, meas: _T, name: Shortname, range: Range, notrunc: bool): ...
    def insert_temporal(
        self, index: int, name: Shortname, meas: _T, range: Range, notrunc: bool
    ): ...
    def unset_measurements(self): ...

class _CoreDatasetGetSetMeas(Generic[_T]):
    analysis: AnalysisBytes
    others: list[OtherBytes]

    def push_temporal(
        self, name: Shortname, meas: _T, col: Series, range: Range, notrunc: bool
    ): ...
    def insert_temporal(
        self,
        index: int,
        name: Shortname,
        meas: _T,
        col: Series,
        range: Range,
        notrunc: bool,
    ): ...
    def unset_data(self): ...
    @property
    def data(self) -> DataFrame: ...

class _CoreGetSetMeasOrdered(Generic[_O, _T]):
    @property
    def layout(self) -> _AnyOrderedLayout | None: ...
    def set_layout(self, layout: _AnyOrderedLayout): ...
    def set_measurements(
        self, measurements: _RawInput[Shortname | None, _O, _T], prefix: str
    ): ...
    def set_measurements_and_layout(
        self,
        measurements: _RawInput[Shortname | None, _O, _T],
        layout: _AnyOrderedLayout,
        prefix: str,
    ): ...

class _CoreGetSetMeasEndian(Generic[_L, _O, _T]):
    @property
    def layout(self) -> _L | None: ...
    def set_layout(self, layout: _L): ...
    def set_measurements(self, measurements: _RawInput[Shortname, _O, _T]): ...
    def set_measurements_and_layout(
        self, measurements: _RawInput[Shortname, _O, _T], layout: _L
    ): ...

class _CoreDatasetGetSetMeasOrdered(Generic[_O, _T]):
    def set_measurements_and_data(
        self,
        measurements: _RawInput[Shortname | None, _O, _T],
        cols: list[Series],
        prefix: str,
    ): ...

class _CoreDatasetGetSetMeasEndian(Generic[_O, _T]):
    def set_measurements_and_data(
        self,
        measurements: _RawInput[Shortname | None, _O, _T],
        cols: list[Series],
    ): ...

class _CoreSetShortnamesMaybe:
    def set_measurement_shortnames_maybe(self, names: list[Shortname | None]): ...

class _CoreScaleMethods:
    scales: list[Scale | None]

    @property
    def all_scales(self) -> list[Scale | None]: ...

class _CoreScaleTransformMethods:
    transforms: list[ScaleTransform]

    @property
    def all_transforms(self) -> list[ScaleTransform]: ...

class _CoreTimestepMethods:
    @property
    def timestep(self) -> Timestep | None: ...
    def set_timestep(self, timestep: Timestep) -> bool: ...

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
    wavelengths: _OpticalKeyVals[float]

class _CoreMeasWavelengths:
    wavelengths: _OpticalKeyVals[list[float]]

class _CoreMeasDisplay:
    displays: list[Display | None]

class _CorePre3_2:
    cyt: str | None

    def __new__(cls, mode: Mode) -> Self: ...

class _Core3_2:
    flowrate: str | None
    cyt: str
    unstainedinfo: str | None
    carriertype: str | None
    carrierid: str | None
    locationid: str | None
    begindatetime: datetime | None
    enddatetime: datetime | None

    detector_names: _OpticalKeyVals[str]
    tags: _OpticalKeyVals[str]
    features: _OpticalKeyVals[Feature]
    analytes: _OpticalKeyVals[str]
    measurement_types: _OpticalKeyVals[str]

    def __new__(cls, cyt: str) -> Self: ...
    @property
    def unstained_centers(self) -> dict[Shortname, float] | None: ...
    def insert_unstained_center(self, name: Shortname, value: float): ...
    def remove_unstained_center(self, name: Shortname) -> float | None: ...
    def clear_unstained_centers(self): ...

class _CoreMeasCalibration(Generic[_C]):
    calibrations: _OpticalKeyVals[_C]

class _CoreToDataset(Generic[_X]):
    def to_dataset(
        self, cols: list[Series], analysis: AnalysisBytes, others: list[OtherBytes]
    ) -> _X: ...

class _CoreTo2_0(Generic[_X]):
    def version_2_0(self, lossless: bool) -> _X: ...

class _CoreTo3_0(Generic[_X]):
    def version_3_0(self, lossless: bool) -> _X: ...

class _CoreTo3_1(Generic[_X]):
    def version_3_1(self, lossless: bool) -> _X: ...

class _CoreTo3_2(Generic[_X]):
    def version_3_2(self, lossless: bool) -> _X: ...

@final
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

@final
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

@final
class CoreTEXT3_1(
    _CoreCommon,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_1, Temporal3_1],
    _CoreTEXTGetSetMeas[Shortname, Temporal3_1, Optical3_1],
    _CoreGetSetMeasEndian[_AnyNonMixedLayout, Optical3_1, Temporal3_1],
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

@final
class CoreTEXT3_2(
    _CoreCommon,
    _Core3_2,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_2, Temporal3_2],
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
    _CoreTo2_0[CoreTEXT2_0],
    _CoreTo3_0[CoreTEXT3_0],
    _CoreTo3_1[CoreTEXT3_1],
): ...

@final
class CoreDataset2_0(
    _CoreCommon,
    _CorePre3_2,
    _CoreTemporal2_0,
    _CoreGetSetMeas[Shortname | None, Optical2_0, Temporal2_0],
    _CoreDatasetGetSetMeas[Temporal2_0],
    _CoreGetSetMeasOrdered[Optical2_0, Temporal2_0],
    _CoreDatasetGetSetMeasOrdered[Optical2_0, Temporal2_0],
    _CoreScaleMethods,
    _CoreSetShortnamesMaybe,
    _CoreCompensation,
    _CoreMeasWavelength,
    _CoreTo3_0[CoreDataset3_0],
    _CoreTo3_1[CoreDataset3_1],
    _CoreTo3_2[CoreDataset3_2],
): ...

@final
class CoreDataset3_0(
    _CoreCommon,
    _CorePre3_2,
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

@final
class CoreDataset3_1(
    _CoreCommon,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_1, Temporal3_1],
    _CoreDatasetGetSetMeas[Temporal3_1],
    _CoreGetSetMeasEndian[_AnyNonMixedLayout, Optical3_1, Temporal3_1],
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

@final
class CoreDataset3_2(
    _CoreCommon,
    _Core3_2,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_2, Temporal3_2],
    _CoreDatasetGetSetMeas[Temporal3_2],
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
    _CoreTo2_0[CoreDataset2_0],
    _CoreTo3_0[CoreDataset3_0],
    _CoreTo3_1[CoreDataset3_1],
): ...

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
    "AsciiFixedLayout",
    "AsciiDelimLayout",
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
]
