from __future__ import annotations
from pathlib import Path
from datetime import time, date, datetime
from typing import TypeVar, Self, Generic, Union, final

from polars import Series, DataFrame
import numpy as np
import numpy.typing as npt

# TODO not sure why mypy complains about this
from pyreflow import PyreflowWarning, PyreflowException  # type: ignore
from pyreflow.typing import (
    MeasIndex,
    Range,
    ByteOrd,
    IntRange,
    FloatRange,
    Timestep,
    Display,
    Scale,
    ScaleTransform,
    NonStdKey,
    Mode,
    Mode3_2,
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

_X = TypeVar("_X")
_Y = TypeVar("_Y")
_C = TypeVar("_C")
_N = TypeVar("_N")
_L = TypeVar("_L")

_OpticalKeyVals = list[_X | tuple[()] | None]

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
    def byte_order(self) -> ByteOrd: ...
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
    def new_ordered(cls, ranges: list[IntRange], byteord: ByteOrd) -> Self: ...

class _LayoutOrderedFloatCommon:
    def __new__(cls, ranges: list[FloatRange], is_big: bool) -> Self: ...
    @classmethod
    def new_ordered(cls, ranges: list[FloatRange], byteord: ByteOrd) -> Self: ...

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
    feature: Feature | None
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

class GatedMeasurement:
    

class _UnivariateRegion(Generic[_X]):
    @property
    def index(self) -> _X: ...
    @property
    def gate(self) -> tuple[float, float]: ...

class _BivariateRegion(Generic[_X]):
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

class _Regions(Generic[_X, _Y]):
    @property
    def regions(self) -> dict[int, _X | _Y]: ...
    @property
    def scheme(self) -> str | None: ...

class _GatedMeasurements:
    @property
    def gated_measurements(self) -> list[GatedMeasurement]: ...

@final
class AppliedGates2_0(
    _Regions[UnivariateRegion2_0, BivariateRegion2_0], _GatedMeasurements
):
    pass

@final
class AppliedGates3_0(
    _Regions[UnivariateRegion3_0, BivariateRegion3_0], _GatedMeasurements
):
    pass

@final
class AppliedGates3_2(_Regions[UnivariateRegion3_2, BivariateRegion3_2]):
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
    trigger: Trigger | None

    all_pnn: list[Shortname]
    all_pns: list[str | None]

    all_pnf: _OpticalKeyVals[str]
    all_pno: _OpticalKeyVals[float]
    all_pnp: _OpticalKeyVals[int]
    all_pnt: _OpticalKeyVals[str]
    all_pnv: _OpticalKeyVals[float]

    def insert_nonstandard(self, key: NonStdKey, value: str) -> str | None: ...
    def remove_nonstandard(self, key: NonStdKey) -> str | None: ...
    def get_nonstandard(self, key: NonStdKey) -> str | None: ...
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
    def write_text(self, path: Path, delim: int = ...) -> None: ...

class _CoreDatasetCommon:
    def write_dataset(
        self,
        path: Path,
        delim: int = ...,
        skip_conversion_check: bool = False,
    ) -> None: ...

class _CoreShortnamesMaybe:
    all_pnn_maybe: list[Shortname | None]

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
        self, meas: _O, name: _N, range: Range, notrunc: bool = False
    ) -> None: ...
    def insert_optical(
        self, index: MeasIndex, meas: _O, name: _N, range: Range, notrunc: bool = False
    ) -> None: ...
    def push_temporal(
        self, meas: _T, name: Shortname, range: Range, notrunc: bool = False
    ) -> None: ...
    def insert_temporal(
        self,
        index: MeasIndex,
        meas: _T,
        name: Shortname,
        range: Range,
        notrunc: bool = False,
    ) -> None: ...
    def unset_measurements(self) -> None: ...

class _CoreDatasetGetSetMeas(Generic[_N, _T, _O]):
    analysis: AnalysisBytes
    others: list[OtherBytes]

    def push_optical(
        self, meas: _O, col: Series, name: _N, range: Range, notrunc: bool = False
    ) -> None: ...
    def insert_optical(
        self,
        index: MeasIndex,
        meas: _O,
        col: Series,
        name: _N,
        range: Range,
        notrunc: bool = False,
    ) -> None: ...
    def push_temporal(
        self,
        meas: _T,
        col: Series,
        name: Shortname,
        range: Range,
        notrunc: bool = False,
    ) -> None: ...
    def insert_temporal(
        self,
        index: MeasIndex,
        meas: _T,
        col: Series,
        name: Shortname,
        range: Range,
        notrunc: bool = False,
    ) -> None: ...
    def unset_data(self) -> None: ...
    data: DataFrame

class _CoreGetSetMeasOrdered(Generic[_O, _T]):
    layout: _AnyOrderedLayout

    def set_measurements(
        self,
        measurements: _RawInput[Shortname | None, _O, _T],
        prefix: str,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> None: ...
    def set_measurements_and_layout(
        self,
        measurements: _RawInput[Shortname | None, _O, _T],
        layout: _AnyOrderedLayout,
        prefix: str,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> None: ...

class _CoreGetSetMeasEndian(Generic[_L, _O, _T]):
    layout: _L

    def set_measurements(
        self,
        measurements: _RawInput[Shortname, _O, _T],
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> None: ...
    def set_measurements_and_layout(
        self,
        measurements: _RawInput[Shortname, _O, _T],
        layout: _L,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> None: ...

class _CoreDatasetGetSetMeasOrdered(Generic[_O, _T]):
    def set_measurements_and_data(
        self,
        measurements: _RawInput[Shortname | None, _O, _T],
        df: DataFrame,
        prefix: str,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> None: ...

class _CoreDatasetGetSetMeasEndian(Generic[_O, _T]):
    def set_measurements_and_data(
        self,
        measurements: _RawInput[Shortname, _O, _T],
        df: DataFrame,
        allow_shared_names: bool,
        skip_index_check: bool,
    ) -> None: ...

class _CoreSetShortnamesMaybe:
    def set_measurement_shortnames_maybe(
        self, names: list[Shortname | None]
    ) -> None: ...

class _CoreScaleMethods:
    scales: list[Scale | None]

class _CoreScaleTransformMethods:
    transforms: list[ScaleTransform]

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
    def csvflags(self) -> list[int | None] | None: ...

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
    ) -> None: ...
    def unset_spillover(self) -> None: ...

class _CoreUnicode:
    unicode: tuple[int, list[str]] | None

class _CoreVol:
    vol: float | None

class _CoreCytsn:
    cytsn: str | None

class _CorePeak:
    all_pkn: list[int]
    all_pknn: list[int]

class _CoreMeasWavelength:
    all_pnl: _OpticalKeyVals[float]

class _CoreMeasWavelengths:
    all_pnl: _OpticalKeyVals[list[float]]

class _CoreMeasDisplay:
    all_pnd: list[Display | None]

class _CorePre3_2:
    mode: Mode
    cyt: str | None

    def __new__(cls, mode: Mode, datatype: Datatype) -> Self: ...

class _Core3_2:
    def __new__(cls, cyt: str, datatype: Datatype) -> Self: ...

    mode: Mode3_2 | None
    flowrate: str | None
    cyt: str
    unstainedinfo: str | None
    unstained_centers: dict[Shortname, float] | None
    carriertype: str | None
    carrierid: str | None
    locationid: str | None
    begindatetime: datetime | None
    enddatetime: datetime | None

    all_pndet: _OpticalKeyVals[str]
    all_pntag: _OpticalKeyVals[str]
    all_pnfeature: _OpticalKeyVals[Feature]
    all_pnanalyte: _OpticalKeyVals[str]
    # TODO this can return a list of all types including the time channel since
    # they share the same kw
    all_pntype: _OpticalKeyVals[str]

class _CoreMeasCalibration(Generic[_C]):
    all_pncalibration: _OpticalKeyVals[_C]

class _CoreToDataset(Generic[_X]):
    def to_dataset(
        self,
        df: DataFrame,
        analysis: AnalysisBytes = ...,
        others: list[OtherBytes] = ...,
    ) -> _X: ...

class _CoreTo2_0(Generic[_X]):
    def version_2_0(self, force: bool = False) -> _X: ...

class _CoreTo3_0(Generic[_X]):
    def version_3_0(self, force: bool = False) -> _X: ...

class _CoreTo3_1(Generic[_X]):
    def version_3_1(self, force: bool = False) -> _X: ...

class _CoreTo3_2(Generic[_X]):
    def version_3_2(self, force: bool = False) -> _X: ...

@final
class CoreTEXT2_0(
    _CoreCommon,
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
): ...

@final
class CoreTEXT3_0(
    _CoreCommon,
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
): ...

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
): ...

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
): ...

@final
class CoreDataset2_0(
    _CoreCommon,
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
): ...

@final
class CoreDataset3_0(
    _CoreCommon,
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
): ...

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
): ...

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
    "UnivariateRegion2_0",
    "UnivariateRegion3_0",
    "UnivariateRegion3_2",
    "BivariateRegion2_0",
    "BivariateRegion3_0",
    "BivariateRegion3_2",
    "AppliedGates2_0",
    "AppliedGates3_0",
    "AppliedGates3_2",
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
