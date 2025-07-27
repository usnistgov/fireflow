from pyreflow import (
    NonStdKey,
    Trigger,
    Shortname,
    NonStdKeywords,
    AnalysisBytes,
    OtherBytes,
)
from datetime import time, date
from typing import TypeVar, Self, Generic, Union
from polars import Series, DataFrame

X = TypeVar("X")
N = TypeVar("N")
L = TypeVar("L")

OpticalKeyVals = list[tuple[int, X | None]]
Scale = tuple[float, float] | tuple[()]
ScaleTransform = tuple[float, float] | tuple[float]
Display = tuple[bool, float, float]

class AsciiFixedLayout: ...
class AsciiDelimLayout: ...
class OrderedUint08Layout: ...
class OrderedUint16Layout: ...
class OrderedUint24Layout: ...
class OrderedUint32Layout: ...
class OrderedUint40Layout: ...
class OrderedUint48Layout: ...
class OrderedUint56Layout: ...
class OrderedUint64Layout: ...
class OrderedF32Layout: ...
class OrderedF64Layout: ...
class EndianF32Layout: ...
class EndianF64Layout: ...
class EndianUintLayout: ...
class MixedLayout: ...

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
    calibration: tuple[float, str] | None

class Optical3_2(
    _MeasCommon,
    _OpticalCommon,
    _OpticalScaleTransform,
    _OpticalWavelengths,
    _MeasDisplay,
):
    calibration: tuple[float, float, str] | None
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

# class _CoreDatasetGetSetMeasOrdered(Generic[O, T]):
#     def set_measurements_and_data(
#         self,
#         raw_input: RawInput[Shortname | None, O, T],
#         layout: AnyOrderedLayout,
#         prefix: str,
#     ): ...

class CoreTEXT2_0(
    _CoreCommon,
    _CoreTemporal2_0,
    _CoreGetSetMeas[Shortname | None, Optical2_0, Temporal2_0],
    _CoreTEXTGetSetMeas[Shortname | None, Temporal2_0, Optical2_0],
    _CoreGetSetMeasOrdered[Optical2_0, Temporal2_0],
): ...
class CoreTEXT3_0(
    _CoreCommon,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname | None, Optical3_0, Temporal3_0],
    _CoreTEXTGetSetMeas[Shortname | None, Temporal3_0, Optical3_0],
    _CoreGetSetMeasOrdered[Optical3_0, Temporal3_0],
): ...
class CoreTEXT3_1(
    _CoreCommon,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_1, Temporal3_1],
    _CoreTEXTGetSetMeas[Shortname, Temporal3_1, Optical3_1],
    _CoreGetSetMeasEndian[AnyNonMixedLayout, Optical3_1, Temporal3_1],
): ...
class CoreTEXT3_2(
    _CoreCommon,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_2, Temporal3_2],
    _CoreTEXTGetSetMeas[Shortname, Temporal3_2, Optical3_2],
    _CoreGetSetMeasEndian[AnyMixedLayout, Optical3_2, Temporal3_2],
): ...
class CoreDataset2_0(
    _CoreCommon,
    _CoreTemporal2_0,
    _CoreGetSetMeas[Shortname | None, Optical2_0, Temporal2_0],
    _CoreDatasetGetSetMeas[Temporal2_0],
    _CoreGetSetMeasOrdered[Optical2_0, Temporal2_0],
): ...
class CoreDataset3_0(
    _CoreCommon,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname | None, Optical3_0, Temporal3_0],
    _CoreDatasetGetSetMeas[Temporal3_0],
    _CoreGetSetMeasOrdered[Optical3_0, Temporal3_0],
): ...
class CoreDataset3_1(
    _CoreCommon,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_1, Temporal3_1],
    _CoreDatasetGetSetMeas[Temporal3_1],
    _CoreGetSetMeasEndian[AnyNonMixedLayout, Optical3_1, Temporal3_1],
): ...
class CoreDataset3_2(
    _CoreCommon,
    _CoreTemporal3_0,
    _CoreGetSetMeas[Shortname, Optical3_2, Temporal3_2],
    _CoreDatasetGetSetMeas[Temporal3_2],
    _CoreGetSetMeasEndian[AnyMixedLayout, Optical3_2, Temporal3_2],
): ...
