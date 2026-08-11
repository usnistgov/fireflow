from __future__ import annotations
from pathlib import Path
from decimal import Decimal
from datetime import time, date, datetime
from typing import Self, Union, final, Any

from polars import Series, DataFrame
import numpy as np
import numpy.typing as npt

import pyreflow.typing as pft

class _DataSchemaOrderedFloatCommon(pft.OrderedDataSchema):
    def __new__(
        cls, ranges: list[pft.FloatRange], byteord: pft.ByteOrd = "little"
    ) -> Self: ...
    @property
    def ranges(self) -> list[pft.FloatRange]: ...
    @property
    def byteord(self) -> pft.ByteOrd: ...
    @property
    def datatype(self) -> pft.Datatype: ...
    @property
    def byte_width(self) -> pft.ByteWidth: ...
    @property
    def is_float(self) -> bool: ...
    def __deepcopy__(self, memo: Any) -> Self: ...

class _DataSchemaEndianFloatCommon(pft.BigLittleDataSchema):
    def __new__(
        cls, ranges: list[pft.FloatRange], endian: pft.Endian = "little"
    ) -> Self: ...
    @property
    def ranges(self) -> list[pft.FloatRange]: ...
    @property
    def endian(self) -> pft.Endian: ...
    @property
    def datatype(self) -> pft.Datatype: ...
    @property
    def byte_width(self) -> pft.ByteWidth: ...
    @property
    def is_float(self) -> bool: ...
    def __deepcopy__(self, memo: Any) -> Self: ...

@final
class FixedAsciiDataSchema(pft.AsciiDataSchema):
    def __new__(cls, ranges: list[pft.IntRange]) -> Self: ...
    @property
    def ranges(self) -> list[pft.FloatRange]: ...
    @property
    def char_widths(self) -> list[int]: ...
    @property
    def datatype(self) -> pft.Datatype: ...
    def __deepcopy__(self, memo: Any) -> Self: ...

@final
class DelimAsciiDataSchema(pft.AsciiDataSchema):
    def __new__(cls, ranges: list[pft.IntRange]) -> Self: ...
    @property
    def ranges(self) -> list[pft.IntRange]: ...
    @property
    def datatype(self) -> pft.Datatype: ...
    def __deepcopy__(self, memo: Any) -> Self: ...

@final
class OrderedUintDataSchema(pft.OrderedDataSchema):
    def __new__(
        cls,
        ranges: list[pft.IntRange],
        byte_width: pft.ByteWidth = 4,
        byteord: pft.ByteOrd = "little",
    ) -> Self: ...
    @property
    def ranges(self) -> list[pft.FloatRange]: ...
    @property
    def byteord(self) -> pft.ByteOrd: ...
    @property
    def datatype(self) -> pft.Datatype: ...
    @property
    def byte_width(self) -> pft.ByteWidth: ...
    @property
    def is_float(self) -> bool: ...
    def __deepcopy__(self, memo: Any) -> Self: ...

@final
class OrderedF32DataSchema(_DataSchemaOrderedFloatCommon): ...

@final
class OrderedF64DataSchema(_DataSchemaOrderedFloatCommon): ...

@final
class BigLittleF32DataSchema(_DataSchemaEndianFloatCommon): ...

@final
class BigLittleF64DataSchema(_DataSchemaEndianFloatCommon): ...

@final
class SingleUintDataSchema(pft.BigLittleDataSchema, pft.MatrixDataSchema):
    def __new__(
        cls,
        ranges: list[pft.IntRange],
        byte_width: pft.ByteWidth = 4,
        endian: pft.Endian = "little",
    ) -> Self: ...
    @property
    def ranges(self) -> list[pft.IntRange]: ...
    @property
    def datatype(self) -> pft.Datatype: ...
    @property
    def byte_width(self) -> pft.ByteWidth: ...
    @property
    def endian(self) -> pft.Endian: ...
    @property
    def is_float(self) -> bool: ...
    def __deepcopy__(self, memo: Any) -> Self: ...

@final
class VariableUintDataSchema(pft.BigLittleDataSchema, pft.NumericDataSchema):
    def __new__(
        cls, ranges: list[pft.VariableBitmask], endian: pft.Endian = "little"
    ) -> Self: ...
    @property
    def ranges(self) -> list[pft.VariableBitmask]: ...
    @property
    def endian(self) -> pft.Endian: ...
    @property
    def byte_widths(self) -> list[int]: ...
    @property
    def datatype(self) -> pft.Datatype: ...
    @property
    def is_float(self) -> bool: ...
    def __deepcopy__(self, memo: Any) -> Self: ...

@final
class MixedDataSchema(pft.BigLittleDataSchema):
    def __new__(
        cls, typed_ranges: list[pft.MixedRange], endian: pft.Endian = "little"
    ) -> Self: ...
    @property
    def typed_ranges(self) -> list[pft.MixedRange]: ...
    @property
    def endian(self) -> pft.Endian: ...
    @property
    def byte_widths(self) -> list[int]: ...
    def __deepcopy__(self, memo: Any) -> Self: ...

_AnyOrderedDataSchema = Union[
    FixedAsciiDataSchema
    | DelimAsciiDataSchema
    | OrderedUintDataSchema
    | OrderedF32DataSchema
    | OrderedF64DataSchema
]

_AnyNonMixedDataSchema = Union[
    FixedAsciiDataSchema
    | DelimAsciiDataSchema
    | BigLittleF32DataSchema
    | BigLittleF64DataSchema
    | SingleUintDataSchema
    | VariableUintDataSchema
]

_AnyMixedDataSchema = Union[
    pft.BigLittleDataSchema
    | pft.AsciiDataSchema
    | VariableUintDataSchema
    | MixedDataSchema
]

class _MeasCommon:
    longname: str

    def __deepcopy__(self, memo: Any) -> Self: ...

class _OpticalWavelength:
    wavelength: float | None

class _OpticalWavelengths:
    wavelengths: list[float]

class _MeasDisplay:
    display: pft.Display

class _PeakCommon:
    size: int
    bin: int

class _OpticalCommon:
    filter: str
    detector_type: str
    detector_voltage: float | None
    power: float | None
    percent_emitted: float | None

class _TemporalTimestep:
    timestep: pft.Timestep

@final
class Optical2_0(_MeasCommon, _OpticalCommon, _OpticalWavelength, _PeakCommon):
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

@final
class Optical3_0(_MeasCommon, _OpticalCommon, _OpticalWavelength, _PeakCommon):
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

@final
class Optical3_1(
    _MeasCommon,
    _OpticalCommon,
    _OpticalWavelengths,
    _MeasDisplay,
    _PeakCommon,
):
    calibration: pft.Calibration3_1 | None

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

@final
class Optical3_2(
    _MeasCommon,
    _OpticalCommon,
    _OpticalWavelengths,
    _MeasDisplay,
):
    calibration: pft.Calibration3_2 | None
    detector_name: str | None
    tag: str | None
    measurement_type: str
    feature: str | None
    awh_feature: pft.Feature | None
    analyte: str | None

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

@final
class Temporal2_0(_MeasCommon, _PeakCommon):
    def __new__(
        cls,
        bin: int | None = None,
        size: int | None = None,
        longname: str = "",
    ) -> Self: ...

@final
class Temporal3_0(_MeasCommon, _TemporalTimestep, _PeakCommon):
    def __new__(
        cls,
        timestep: float,
        bin: int | None = None,
        size: int | None = None,
        longname: str = "",
    ) -> Self: ...

@final
class Temporal3_1(_MeasCommon, _MeasDisplay, _TemporalTimestep, _PeakCommon):
    def __new__(
        cls,
        timestep: float,
        display: pft.Display | None = None,
        bin: int | None = None,
        size: int | None = None,
        longname: str = "",
    ) -> Self: ...

@final
class Temporal3_2(_MeasCommon, _MeasDisplay, _TemporalTimestep):
    has_type: bool

    def __new__(
        cls,
        timestep: float,
        display: pft.Display | None = None,
        has_type: bool = False,
        longname: str = "",
    ) -> Self: ...

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

class _UnivariateRegion[X: int | str]:
    def __new__(
        cls,
        index: X,
        gate: tuple[float, float],
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def index(self) -> X: ...
    @property
    def gate(self) -> tuple[float, float]: ...

class _BivariateRegion[X: int | str]:
    def __new__(
        cls,
        index: tuple[X, X],
        vertices: list[tuple[float, float]],
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def index(self) -> tuple[X, X]: ...
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
    tr: pft.Trigger | None

    all_shortnames: list[pft.Shortname]
    all_longnames: list[str | None]

    all_filters: pft.OpticalKeyVals[str]
    all_powers: pft.OpticalKeyVals[float]
    all_percents_emitted: pft.OpticalKeyVals[int]
    all_detector_types: pft.OpticalKeyVals[str]
    all_detector_voltages: pft.OpticalKeyVals[float]

    nonstandard_keywords: pft.NonStdKeywords
    def standard_keywords(
        self,
        req_or_opt: pft.ReqOrOpt,
        root_or_meas: pft.RootOrMeas,
    ) -> dict[str, str]: ...
    @property
    def par(self) -> int: ...
    def set_trigger_threshold(self, threshold: int) -> bool: ...
    def write_text(
        self,
        path: Path,
        delim: int = 30,
        big_other: bool = False,
        compute_crc: bool = False,
        override_fil: bool = False,
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
        compute_crc: bool = False,
        override_fil: bool = False,
    ) -> None: ...
    @property
    def version(self) -> pft.FCSVersion: ...
    def __deepcopy__(self, memo: Any) -> Self: ...

class _CoreDatasetCommon:
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
    ) -> None: ...

class _CoreShortnamesMaybe:
    all_shortnames_maybe: list[pft.Shortname | None]

class _CoreTemporal2_0:
    def set_temporal(
        self, name: pft.Shortname, allow_loss: pft.TriFlag = "false"
    ) -> bool: ...
    def set_temporal_at(
        self, index: pft.MeasIndex, allow_loss: pft.TriFlag = "false"
    ) -> bool: ...
    def unset_temporal(self) -> bool: ...

class _CoreTemporal3_0:
    def set_temporal(
        self,
        name: pft.Shortname,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def set_temporal_at(
        self,
        index: pft.MeasIndex,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def unset_temporal(self) -> float | None: ...

class _CoreTemporal3_2:
    def set_temporal(
        self,
        name: pft.Shortname,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def set_temporal_at(
        self,
        index: pft.MeasIndex,
        timestep: pft.Timestep,
        allow_loss: pft.TriFlag = "false",
    ) -> bool: ...
    def unset_temporal(self, allow_loss: pft.TriFlag = "false") -> float | None: ...

class _CoreGetSetMeas[N, O, T, S]:
    @property
    def temporal(self) -> tuple[pft.MeasIndex, pft.Shortname, T] | None: ...
    @property
    def measurements(self) -> list[O | T]: ...
    def measurement_at(self, index: pft.MeasIndex) -> O | T: ...
    def measurement_named(self, name: pft.Shortname) -> O | T: ...
    def replace_optical_at(self, index: pft.MeasIndex, meas: O) -> tuple[O, S] | T: ...
    def replace_optical_named(
        self, name: pft.Shortname, meas: O
    ) -> tuple[O, S] | T | None: ...
    def rename_temporal(self, name: pft.Shortname) -> pft.Shortname | None: ...

class _CoreTEXTRemove2_0:
    def remove_measurement_by_name(
        self, name: pft.Shortname
    ) -> tuple[
        pft.MeasIndex,
        Optical2_0 | Temporal2_0,
        pft.Range,
        pft.OpticalScale2_0,
        pft.IntegerWidth | None,
    ]: ...
    def remove_measurement_by_index(
        self, index: pft.MeasIndex
    ) -> tuple[
        pft.Shortname | None,
        Optical2_0 | Temporal2_0,
        pft.Range,
        pft.OpticalScale2_0,
        pft.IntegerWidth | None,
    ]: ...

class _CoreTEXTRemove3_0:
    def remove_measurement_by_name(
        self, name: pft.Shortname
    ) -> tuple[
        pft.MeasIndex,
        Optical3_0 | Temporal3_0,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.IntegerWidth | None,
    ]: ...
    def remove_measurement_by_index(
        self, index: pft.MeasIndex
    ) -> tuple[
        pft.Shortname | None,
        Optical3_0 | Temporal3_0,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.IntegerWidth | None,
    ]: ...

class _CoreTEXTRemove3_1:
    def remove_measurement_by_name(
        self, name: pft.Shortname
    ) -> tuple[
        pft.MeasIndex,
        Optical3_1 | Temporal3_1,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.IntegerWidth | None,
    ]: ...
    def remove_measurement_by_index(
        self, index: pft.MeasIndex
    ) -> tuple[
        pft.Shortname,
        Optical3_1 | Temporal3_1,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.IntegerWidth | None,
    ]: ...

class _CoreTEXTRemove3_2:
    def remove_measurement_by_name(
        self, name: pft.Shortname
    ) -> tuple[
        pft.MeasIndex,
        Optical3_2 | Temporal3_2,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.AnyType | None,
    ]: ...
    def remove_measurement_by_index(
        self, index: pft.MeasIndex
    ) -> tuple[
        pft.Shortname,
        Optical3_2 | Temporal3_2,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.AnyType | None,
    ]: ...

class _CoreDatasetRemove2_0:
    def remove_measurement_by_name(
        self, name: pft.Shortname
    ) -> tuple[
        pft.MeasIndex,
        Optical2_0 | Temporal2_0,
        Series,
        pft.OpticalScale2_0,
        pft.Range,
    ]: ...
    def remove_measurement_by_index(
        self, index: pft.MeasIndex
    ) -> tuple[
        pft.Shortname | None,
        Optical2_0 | Temporal2_0,
        Series,
        pft.OpticalScale2_0,
        pft.Range,
    ]: ...

class _CoreDatasetRemove3_0:
    def remove_measurement_by_name(
        self, name: pft.Shortname
    ) -> tuple[
        pft.MeasIndex,
        Optical3_0 | Temporal3_0,
        Series,
        pft.OpticalScale3_0 | None,
        pft.Range,
    ]: ...
    def remove_measurement_by_index(
        self, index: pft.MeasIndex
    ) -> tuple[
        pft.Shortname | None,
        Optical3_0 | Temporal3_0,
        Series,
        pft.OpticalScale3_0 | None,
        pft.Range,
    ]: ...

class _CoreDatasetRemove3_1:
    def remove_measurement_by_name(
        self, name: pft.Shortname
    ) -> tuple[
        pft.MeasIndex,
        Optical3_1 | Temporal3_1,
        Series,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.IntegerWidth | None,
    ]: ...
    def remove_measurement_by_index(
        self, index: pft.MeasIndex
    ) -> tuple[
        pft.Shortname,
        Optical3_1 | Temporal3_1,
        Series,
        pft.Range,
        pft.OpticalScale3_0 | None,
        pft.IntegerWidth | None,
    ]: ...

class _CoreDatasetRemove3_2:
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

class _CoreReplaceTemporal2_0[N, O, T, S]:
    def replace_temporal_at(self, index: pft.MeasIndex, meas: T) -> tuple[O, S] | T: ...
    def replace_temporal_named(
        self, name: pft.Shortname, meas: T
    ) -> tuple[O, S] | T | None: ...

class _CoreReplaceTemporal3_2:
    def replace_temporal_at(
        self,
        index: pft.MeasIndex,
        meas: Temporal3_2,
        allow_loss: pft.TriFlag = "false",
    ) -> tuple[Optical3_2, pft.OpticalScale3_0] | Temporal3_2: ...
    def replace_temporal_named(
        self,
        name: pft.Shortname,
        meas: Temporal3_2,
        allow_loss: pft.TriFlag = "false",
    ) -> tuple[Optical3_2, pft.OpticalScale2_0] | Temporal3_2 | None: ...

class _CoreTEXTInsertMeas2_0:
    def push_optical(
        self,
        name: pft.Shortname | None,
        meas: Optical2_0,
        range: pft.Range,
        scale: pft.OpticalScale2_0 = (),
    ) -> None: ...
    def insert_optical(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname | None,
        meas: Optical2_0,
        range: pft.Range,
        scale: pft.OpticalScale2_0 = (),
    ) -> None: ...
    def push_temporal(
        self,
        name: pft.Shortname,
        meas: Temporal2_0,
        range: pft.Range,
    ) -> None: ...
    def insert_temporal(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: Temporal2_0,
        range: pft.Range,
    ) -> None: ...

class _CoreTEXTInsertMeas3_0[
    N,
    T: Temporal3_0 | Temporal3_1 | Temporal3_2,
    O: Optical3_0 | Optical3_1 | Optical3_2,
    R: pft.Range | pft.MaybeTypedVariableBitmask | pft.MaybeTypedMixedRange,
]:
    def push_optical(
        self,
        name: N,
        meas: O,
        range: R,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def insert_optical(
        self,
        index: N,
        name: N,
        meas: O,
        range: R,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def push_temporal(
        self,
        name: pft.Shortname,
        meas: T,
        range: R,
    ) -> None: ...
    def insert_temporal(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: T,
        range: R,
    ) -> None: ...

class _CoreDatasetInsertMeas2_0:
    def push_optical(
        self,
        name: pft.Shortname | None,
        meas: Optical2_0,
        range: pft.Range,
        col: Series,
        scale: pft.OpticalScale2_0 = (),
    ) -> None: ...
    def insert_optical(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname | None,
        meas: Optical2_0,
        range: pft.Range,
        col: Series,
        scale: pft.OpticalScale2_0 = (),
    ) -> None: ...
    def push_temporal(
        self,
        name: pft.Shortname,
        meas: Temporal2_0,
        range: pft.Range,
        col: Series,
    ) -> None: ...
    def insert_temporal(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: Temporal2_0,
        range: pft.Range,
        col: Series,
    ) -> None: ...

class _CoreDatasetInsertMeas3_0[
    N,
    T: Temporal3_0 | Temporal3_1 | Temporal3_2,
    O: Optical3_0 | Optical3_1 | Optical3_2,
    R: pft.Range | pft.MaybeTypedVariableBitmask | pft.MaybeTypedMixedRange,
]:
    def push_optical(
        self,
        name: N,
        meas: O,
        range: R,
        col: Series,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def insert_optical(
        self,
        index: pft.MeasIndex,
        name: N,
        meas: O,
        range: R,
        col: Series,
        scale: pft.OpticalScale3_0 = 1.0,
    ) -> None: ...
    def push_temporal(
        self,
        name: pft.Shortname,
        meas: T,
        range: R,
        col: Series,
    ) -> None: ...
    def insert_temporal(
        self,
        index: pft.MeasIndex,
        name: pft.Shortname,
        meas: T,
        range: R,
        col: Series,
    ) -> None: ...

class _CoreTEXTGetSetMeas:
    def unset_measurements(self) -> None: ...

class _CoreDatasetGetSetMeas[T, O]:
    analysis: pft.AnalysisBytes
    others: list[pft.OtherBytes]

    def unset_data(self) -> None: ...
    def check_ranges(
        self,
        over_bitmask_action: pft.OverLimitAction = "trunc_warn",
        over_range_action: pft.OverLimitAction = "warn",
    ) -> list[int | None]: ...
    data: DataFrame
    def set_measurements_and_data(
        self,
        measurements: list[O | T],
        data: DataFrame,
    ) -> None: ...

class _CoreGetSetMeasOrdered[
    T: pft.AnyTemporal,
    O: pft.AnyOptical,
    S: pft.OpticalScale2_0 | pft.OpticalScale3_0,
]:
    data_schema: _AnyOrderedDataSchema

    def set_named_measurements(
        self,
        measurements: pft.Measurements[pft.Shortname | None, T, O, S],
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements_and_data_schema(
        self,
        measurements: pft.Measurements[pft.Shortname | None, T, O, S],
        data_schema: _AnyOrderedDataSchema,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_measurements_and_data_schema(
        self,
        measurements: list[O | T],
        data_schema: _AnyOrderedDataSchema,
    ) -> None: ...

class _CoreGetSetMeasEndian[
    L,
    T: pft.AnyTemporal,
    O: pft.AnyOptical,
    S: pft.OpticalScale2_0 | pft.OpticalScale3_0,
]:
    data_schema: L

    def set_named_measurements(
        self,
        measurements: pft.Measurements[pft.Shortname, T, O, S],
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_named_measurements_and_data_schema(
        self,
        measurements: pft.Measurements[pft.Shortname, T, O, S],
        data_schema: L,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_measurements_and_data_schema(
        self,
        measurements: list[O | T],
        data_schema: L,
    ) -> None: ...

class _CoreDatasetGetSetMeasOrdered[
    T: pft.AnyTemporal,
    O: pft.AnyOptical,
    S: pft.OpticalScale2_0 | pft.OpticalScale3_0,
]:
    def set_named_measurements_and_data(
        self,
        measurements: pft.Measurements[pft.Shortname | None, T, O, S],
        data: DataFrame,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_measurements_data_schema_and_data(
        self,
        measurements: list[O | T],
        data_schema: _AnyOrderedDataSchema,
        data: DataFrame,
    ) -> None: ...

class _CoreDatasetGetSetMeasEndian[
    L,
    T: pft.AnyTemporal,
    O: pft.AnyOptical,
    S: pft.OpticalScale2_0 | pft.OpticalScale3_0,
]:
    def set_named_measurements_and_data(
        self,
        measurements: pft.Measurements[pft.Shortname, T, O, S],
        data: DataFrame,
        allow_shared_names: bool = False,
        skip_index_check: bool = False,
    ) -> None: ...
    def set_measurements_data_schema_and_data(
        self,
        measurements: list[O | T],
        data_schema: L,
        data: DataFrame,
    ) -> None: ...

class _CoreSetShortnamesMaybe:
    def set_measurement_shortnames_maybe(
        self, names: list[pft.Shortname | None]
    ) -> None: ...

class _CoreScaleMethods:
    all_scales: list[pft.OpticalScale2_0]

class _CoreScaleTransformMethods:
    all_scales: list[pft.OpticalScale3_0]

class _CoreTimestepMethods:
    @property
    def timestep(self) -> pft.Timestep | None: ...
    def set_timestep(self, timestep: pft.Timestep) -> pft.Timestep | None: ...

class _CoreGates[X]:
    applied_gates: X

class _CoreSubset:
    @property
    def cstot(self) -> int: ...
    @property
    def csvbits(self) -> int: ...
    @property
    def csvflags(self) -> pft.CsvFlags: ...

class _CoreModified:
    originality: pft.Originality | None
    last_modified: datetime | None
    last_modifier: str | None

class _CorePlate:
    platename: str | None
    plateid: str | None
    wellid: str | None

class _CoreCompensation:
    compensation: npt.NDArray[np.float32] | None

class _CoreSpillover:
    spillover: pft.Spillover | None

class _CoreUnicode:
    unicode: pft.Unicode | None

class _CoreVol:
    vol: float | None

class _CoreCytsn:
    cytsn: str | None

class _CorePeak:
    all_peak_bins: list[int]
    all_peak_sizes: list[int]

class _CoreMeasWavelength:
    all_wavelengths: pft.OpticalKeyVals[float]

class _CoreMeasWavelengths:
    all_wavelengths: pft.OpticalKeyVals[list[float]]

class _CoreMeasDisplay:
    all_displays: list[pft.Display | None]

class _CorePre3_1:
    comp: npt.NDArray[np.float32] | None

class _CorePre3_2:
    mode: pft.Mode
    cyt: str | None

class _Core3_2:
    mode: pft.Mode3_2 | None
    flowrate: str | None
    cyt: str
    unstainedinfo: str | None
    unstainedcenters: dict[pft.Shortname, float]
    carriertype: str | None
    carrierid: str | None
    locationid: str | None
    begindatetime: datetime | None
    enddatetime: datetime | None

    all_detector_names: pft.OpticalKeyVals[str]
    all_tags: pft.OpticalKeyVals[str]
    all_features: pft.OpticalKeyVals[str]
    all_awh_features: pft.OpticalKeyVals[pft.Feature]
    all_other_features: pft.OpticalKeyVals[str]
    all_analytes: pft.OpticalKeyVals[str]
    all_measurement_types: list[str | bool]

class _CoreMeasCalibration[C: pft.Calibration3_1 | pft.Calibration3_2]:
    all_calibrations: pft.OpticalKeyVals[C]

class _CoreToDataset[
    X: CoreDataset2_0 | CoreDataset3_0 | CoreDataset3_1 | CoreDataset3_2
]:
    def to_dataset(
        self,
        data: DataFrame,
        analysis: pft.AnalysisBytes = "",
        others: list[pft.OtherBytes] = [],
    ) -> X: ...

class _CoreTo2_0[X]:
    def to_version_2_0(self, allow_loss: pft.TriFlag = "false") -> X: ...

class _CoreTo3_0[X]:
    def to_version_3_0(self, allow_loss: pft.TriFlag = "false") -> X: ...

class _CoreTo3_1[X]:
    def to_version_3_1(self, allow_loss: pft.TriFlag = "false") -> X: ...

class _CoreTo3_2[X]:
    def to_version_3_2(self, allow_loss: pft.TriFlag = "false") -> X: ...

@final
class CoreTEXT2_0(
    _CoreCommon,
    _CorePre3_1,
    _CorePre3_2,
    _CoreTemporal2_0,
    _CoreShortnamesMaybe,
    _CoreGetSetMeas[pft.Shortname | None, Optical2_0, Temporal2_0, pft.OpticalScale2_0],
    _CoreTEXTRemove2_0,
    _CoreReplaceTemporal2_0[
        pft.Shortname | None, Optical2_0, Temporal2_0, pft.OpticalScale2_0
    ],
    _CoreTEXTInsertMeas2_0,
    _CoreTEXTGetSetMeas,
    _CoreGetSetMeasOrdered[Temporal2_0, Optical2_0, pft.OpticalScale2_0],
    _CoreSetShortnamesMaybe,
    _CoreScaleMethods,
    _CoreToDataset[CoreDataset2_0],
    _CoreCompensation,
    _CoreMeasWavelength,
    _CorePeak,
    _CoreGates[pft.AppliedGates2_0],
    _CoreTo3_0[CoreTEXT3_0],
    _CoreTo3_1[CoreTEXT3_1],
    _CoreTo3_2[CoreTEXT3_2],
):
    def __new__(
        cls,
        measurements: pft.Measurements2_0,
        data_schema: _AnyOrderedDataSchema,
        mode: pft.Mode = "L",
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
        tr: pft.Trigger | None = None,
        applied_gates: pft.AppliedGates2_0 = ([], {}, None),
        nonstandard_keywords: pft.NonStdKeywords = {},
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        std: pft.StdKeywords,
        nonstd: pft.NonStdKeywords,
        # std args
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: pft.Selector[str | None] = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_time_optical_keys: list[pft.TemporalOpticalKey] = [],
        process_time_optical_keys: pft.ProcessTimeOpticalKeys = "demote_warn",
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
        # layout args
        ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
        promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
        replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
        append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
        substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
        allow_repair_non_unique: pft.TriFlag = "false",
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        fix_int_widths: pft.FixIntWidths = "never",
        byteord_override: pft.ByteordOverride = "none",
        disallow_range_truncation: pft.TriFlag = "false",
        # shared args
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
    _CoreGetSetMeas[pft.Shortname | None, Optical3_0, Temporal3_0, pft.OpticalScale3_0],
    _CoreTEXTRemove3_0,
    _CoreReplaceTemporal2_0[
        pft.Shortname | None, Optical2_0, Temporal2_0, pft.OpticalScale3_0
    ],
    _CoreTEXTInsertMeas3_0[pft.Shortname | None, Temporal3_0, Optical3_0, pft.Range],
    _CoreTEXTGetSetMeas,
    _CoreGetSetMeasOrdered[Temporal3_0, Optical3_0, pft.OpticalScale3_0],
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
    _CoreGates[pft.AppliedGates3_0],
    _CoreTo2_0[CoreTEXT2_0],
    _CoreTo3_1[CoreTEXT3_1],
    _CoreTo3_2[CoreTEXT3_2],
):
    def __new__(
        cls,
        measurements: pft.Measurements3_0,
        data_schema: _AnyOrderedDataSchema,
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
        nonstandard_keywords: pft.NonStdKeywords = {},
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        std: pft.StdKeywords,
        nonstd: pft.NonStdKeywords,
        # std args
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: pft.Selector[str | None] = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_time_optical_keys: list[pft.TemporalOpticalKey] = [],
        process_time_optical_keys: pft.ProcessTimeOpticalKeys = "demote_warn",
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
        add_missing_timestep: float | None = None,
        # layout args
        ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
        promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
        replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
        append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
        substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
        allow_repair_non_unique: pft.TriFlag = "false",
        text_data_correction: pft.OffsetCorrection = (0, 0),
        text_analysis_correction: pft.OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: pft.TriFlag = "false",
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        fix_int_widths: pft.FixIntWidths = "never",
        byteord_override: pft.ByteordOverride = "none",
        disallow_range_truncation: pft.TriFlag = "false",
        # shared args
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
    ) -> Self: ...

@final
class CoreTEXT3_1(
    _CoreCommon,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreGetSetMeas[pft.Shortname, Optical3_1, Temporal3_1, pft.OpticalScale3_0],
    _CoreTEXTRemove3_1,
    _CoreReplaceTemporal2_0[
        pft.Shortname | None, Optical2_0, Temporal2_0, pft.OpticalScale3_0
    ],
    _CoreTEXTInsertMeas3_0[
        pft.Shortname,
        Temporal3_1,
        Optical3_1,
        pft.MaybeTypedVariableBitmask,
    ],
    _CoreTEXTGetSetMeas,
    _CoreGetSetMeasEndian[
        _AnyNonMixedDataSchema, Temporal3_1, Optical3_1, pft.OpticalScale3_0
    ],
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
    _CoreMeasCalibration[pft.Calibration3_1],
    _CoreGates[pft.AppliedGates3_0],
    _CoreTo2_0[CoreTEXT2_0],
    _CoreTo3_0[CoreTEXT3_0],
    _CoreTo3_2[CoreTEXT3_2],
):
    def __new__(
        cls,
        measurements: pft.Measurements3_1,
        data_schema: _AnyNonMixedDataSchema,
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
        nonstandard_keywords: pft.NonStdKeywords = {},
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        std: pft.StdKeywords,
        nonstd: pft.NonStdKeywords,
        # std args
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: pft.Selector[str | None] = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_time_optical_keys: list[pft.TemporalOpticalKey] = [],
        process_time_optical_keys: pft.ProcessTimeOpticalKeys = "demote_warn",
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
        add_missing_timestep: float | None = None,
        spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
        # layout args
        ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
        promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
        replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
        append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
        substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
        allow_repair_non_unique: pft.TriFlag = "false",
        text_data_correction: pft.OffsetCorrection = (0, 0),
        text_analysis_correction: pft.OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: pft.TriFlag = "false",
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        disallow_range_truncation: pft.TriFlag = "false",
        # shared args
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
    ) -> Self: ...

@final
class CoreTEXT3_2(
    _CoreCommon,
    _Core3_2,
    _CoreTemporal3_2,
    _CoreGetSetMeas[pft.Shortname, Optical3_2, Temporal3_2, pft.OpticalScale3_0],
    _CoreTEXTRemove3_2,
    _CoreReplaceTemporal3_2,
    _CoreTEXTInsertMeas3_0[
        pft.Shortname,
        Temporal3_2,
        Optical3_2,
        pft.MaybeTypedMixedRange,
    ],
    _CoreTEXTGetSetMeas,
    _CoreGetSetMeasEndian[
        _AnyMixedDataSchema, Temporal3_2, Optical3_2, pft.OpticalScale3_0
    ],
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
    _CoreMeasCalibration[pft.Calibration3_2],
    _CoreGates[pft.AppliedGates3_2],
    _CoreTo2_0[CoreTEXT2_0],
    _CoreTo3_0[CoreTEXT3_0],
    _CoreTo3_1[CoreTEXT3_1],
):
    def __new__(
        cls,
        measurements: pft.Measurements3_2,
        data_schema: _AnyMixedDataSchema,
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
        nonstandard_keywords: pft.NonStdKeywords = {},
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        std: pft.StdKeywords,
        nonstd: pft.NonStdKeywords,
        # std args
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: pft.Selector[str | None] = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_time_optical_keys: list[pft.TemporalOpticalKey] = [],
        process_time_optical_keys: pft.ProcessTimeOpticalKeys = "demote_warn",
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
        add_missing_timestep: float | None = None,
        spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
        disallow_localtime: bool = False,
        # layout args
        ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
        promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
        replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
        append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
        substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
        allow_repair_non_unique: pft.TriFlag = "false",
        text_data_correction: pft.OffsetCorrection = (0, 0),
        text_analysis_correction: pft.OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: pft.TriFlag = "false",
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        disallow_range_truncation: pft.TriFlag = "false",
        # shared args
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
    _CoreGetSetMeas[pft.Shortname | None, Optical2_0, Temporal2_0, pft.OpticalScale2_0],
    _CoreDatasetRemove2_0,
    _CoreReplaceTemporal2_0[
        pft.Shortname | None, Optical2_0, Temporal2_0, pft.OpticalScale2_0
    ],
    _CoreDatasetInsertMeas2_0,
    _CoreDatasetGetSetMeas[Temporal2_0, Optical2_0],
    _CoreGetSetMeasOrdered[Temporal2_0, Optical2_0, pft.OpticalScale2_0],
    _CoreDatasetGetSetMeasOrdered[Temporal2_0, Optical2_0, pft.OpticalScale2_0],
    _CoreScaleMethods,
    _CoreSetShortnamesMaybe,
    _CoreCompensation,
    _CoreMeasWavelength,
    _CorePeak,
    _CoreGates[pft.AppliedGates2_0],
    _CoreTo3_0[CoreDataset3_0],
    _CoreTo3_1[CoreDataset3_1],
    _CoreTo3_2[CoreDataset3_2],
    _CoreDatasetCommon,
):
    def __new__(
        cls,
        measurements: pft.Measurements2_0,
        data_schema: _AnyOrderedDataSchema,
        data: DataFrame,
        mode: pft.Mode = "L",
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
        tr: pft.Trigger | None = None,
        applied_gates: pft.AppliedGates2_0 = ([], {}, None),
        nonstandard_keywords: pft.NonStdKeywords = {},
        analysis: pft.AnalysisBytes = "",
        others: list[bytes] = [],
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        path: Path,
        header: Header,
        std: pft.StdKeywords,
        nonstd: pft.NonStdKeywords,
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: pft.Selector[str | None] = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_time_optical_keys: list[pft.TemporalOpticalKey] = [],
        process_time_optical_keys: pft.ProcessTimeOpticalKeys = "demote_warn",
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
        # layout args
        ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
        promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
        replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
        append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
        substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
        allow_repair_non_unique: pft.TriFlag = "false",
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        fix_int_widths: pft.FixIntWidths = "never",
        byteord_override: pft.ByteordOverride = "none",
        disallow_range_truncation: pft.TriFlag = "false",
        # data args
        data_remainder_limit: int = 0,
        allow_uneven_event_width: pft.TriFlag = "false",
        allow_tot_mismatch: pft.TriFlag = "false",
        over_bitmask_action: pft.OverLimitAction = "trunc_warn",
        over_range_action: pft.OverLimitAction = "warn",
        read_intra_segment_dark_bytes: bool = False,
        read_post_dataset_dark_bytes: bool = False,
        row_buffer_size: int = 28000,
        # shared args
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
        dataset_offset: int = 0,
        dataset_len: int | None = None,
    ) -> Self: ...

@final
class CoreDataset3_0(
    _CoreCommon,
    _CorePre3_1,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreShortnamesMaybe,
    _CoreGetSetMeas[pft.Shortname | None, Optical3_0, Temporal3_0, pft.OpticalScale3_0],
    _CoreDatasetRemove3_0,
    _CoreReplaceTemporal2_0[
        pft.Shortname | None, Optical2_0, Temporal2_0, pft.OpticalScale3_0
    ],
    _CoreDatasetInsertMeas3_0[pft.Shortname | None, Temporal3_0, Optical3_0, pft.Range],
    _CoreDatasetGetSetMeas[Temporal3_0, Optical3_0],
    _CoreGetSetMeasOrdered[Temporal3_0, Optical3_0, pft.OpticalScale3_0],
    _CoreDatasetGetSetMeasOrdered[Temporal3_0, Optical3_0, pft.OpticalScale3_0],
    _CoreSetShortnamesMaybe,
    _CoreScaleTransformMethods,
    _CoreTimestepMethods,
    _CoreCompensation,
    _CoreUnicode,
    _CoreCytsn,
    _CoreMeasWavelength,
    _CorePeak,
    _CoreSubset,
    _CoreGates[pft.AppliedGates3_0],
    _CoreTo2_0[CoreDataset2_0],
    _CoreTo3_1[CoreDataset3_1],
    _CoreTo3_2[CoreDataset3_2],
    _CoreDatasetCommon,
):
    def __new__(
        cls,
        measurements: pft.Measurements3_0,
        data_schema: _AnyOrderedDataSchema,
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
        nonstandard_keywords: pft.NonStdKeywords = {},
        analysis: pft.AnalysisBytes = "",
        others: list[bytes] = [],
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        path: Path,
        header: Header,
        std: pft.StdKeywords,
        nonstd: pft.NonStdKeywords,
        # offset args
        allow_pseudoempty: bool = False,
        dataset_overflow_limit: int = 0,
        overlap_correction_limit: int = 0,
        # std args
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: pft.Selector[str | None] = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_time_optical_keys: list[pft.TemporalOpticalKey] = [],
        process_time_optical_keys: pft.ProcessTimeOpticalKeys = "demote_warn",
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
        add_missing_timestep: float | None = None,
        # layout args
        ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
        promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
        replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
        append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
        substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
        allow_repair_non_unique: pft.TriFlag = "false",
        text_data_correction: pft.OffsetCorrection = (0, 0),
        text_analysis_correction: pft.OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: pft.TriFlag = "false",
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        fix_int_widths: pft.FixIntWidths = "never",
        byteord_override: pft.ByteordOverride = "none",
        disallow_range_truncation: pft.TriFlag = "false",
        # data args
        data_remainder_limit: int = 0,
        allow_uneven_event_width: pft.TriFlag = "false",
        allow_tot_mismatch: pft.TriFlag = "false",
        over_bitmask_action: pft.OverLimitAction = "trunc_warn",
        over_range_action: pft.OverLimitAction = "warn",
        allow_missing_crc: pft.TriFlag = "false",
        allow_mismatch_crc: pft.TriFlag = "false",
        compute_crc: pft.ComputeReadCRC = "never",
        read_intra_segment_dark_bytes: bool = False,
        read_post_dataset_dark_bytes: bool = False,
        row_buffer_size: int = 28000,
        # shared args
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
        dataset_offset: int = 0,
        dataset_len: int | None = None,
    ) -> Self: ...

@final
class CoreDataset3_1(
    _CoreCommon,
    _CorePre3_2,
    _CoreTemporal3_0,
    _CoreGetSetMeas[pft.Shortname, Optical3_1, Temporal3_1, pft.OpticalScale3_0],
    _CoreDatasetRemove3_1,
    _CoreReplaceTemporal2_0[
        pft.Shortname | None, Optical2_0, Temporal2_0, pft.OpticalScale3_0
    ],
    _CoreDatasetInsertMeas3_0[
        pft.Shortname,
        Temporal3_1,
        Optical3_1,
        pft.MaybeTypedVariableBitmask,
    ],
    _CoreDatasetGetSetMeas[Temporal3_1, Optical3_1],
    _CoreGetSetMeasEndian[
        _AnyNonMixedDataSchema, Temporal3_1, Optical3_1, pft.OpticalScale3_0
    ],
    _CoreDatasetGetSetMeasEndian[
        _AnyNonMixedDataSchema, Temporal3_1, Optical3_1, pft.OpticalScale3_0
    ],
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
    _CoreMeasCalibration[pft.Calibration3_1],
    _CoreGates[pft.AppliedGates3_0],
    _CoreTo2_0[CoreDataset2_0],
    _CoreTo3_0[CoreDataset3_0],
    _CoreTo3_2[CoreDataset3_2],
    _CoreDatasetCommon,
):
    def __new__(
        cls,
        measurements: pft.Measurements3_1,
        data_schema: _AnyNonMixedDataSchema,
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
        nonstandard_keywords: pft.NonStdKeywords = {},
        analysis: pft.AnalysisBytes = "",
        others: list[bytes] = [],
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        path: Path,
        header: Header,
        std: pft.StdKeywords,
        nonstd: pft.NonStdKeywords,
        # offset args
        allow_pseudoempty: bool = False,
        dataset_overflow_limit: int = 0,
        overlap_correction_limit: int = 0,
        # std args
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: pft.Selector[str | None] = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_time_optical_keys: list[pft.TemporalOpticalKey] = [],
        process_time_optical_keys: pft.ProcessTimeOpticalKeys = "demote_warn",
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
        add_missing_timestep: float | None = None,
        spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
        # layout args
        ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
        promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
        replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
        append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
        substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
        allow_repair_non_unique: pft.TriFlag = "false",
        text_data_correction: pft.OffsetCorrection = (0, 0),
        text_analysis_correction: pft.OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: pft.TriFlag = "false",
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        disallow_range_truncation: pft.TriFlag = "false",
        # data args
        data_remainder_limit: int = 0,
        allow_uneven_event_width: pft.TriFlag = "false",
        allow_tot_mismatch: pft.TriFlag = "false",
        over_bitmask_action: pft.OverLimitAction = "trunc_warn",
        over_range_action: pft.OverLimitAction = "warn",
        allow_missing_crc: pft.TriFlag = "false",
        allow_mismatch_crc: pft.TriFlag = "false",
        compute_crc: pft.ComputeReadCRC = "never",
        read_intra_segment_dark_bytes: bool = False,
        read_post_dataset_dark_bytes: bool = False,
        row_buffer_size: int = 28000,
        # shared args
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
        dataset_offset: int = 0,
        dataset_len: int | None = None,
    ) -> Self: ...

@final
class CoreDataset3_2(
    _CoreCommon,
    _Core3_2,
    _CoreTemporal3_2,
    _CoreGetSetMeas[pft.Shortname, Optical3_2, Temporal3_2, pft.OpticalScale3_0],
    _CoreDatasetRemove3_2,
    _CoreReplaceTemporal3_2,
    _CoreDatasetInsertMeas3_0[
        pft.Shortname,
        Temporal3_2,
        Optical3_2,
        pft.MaybeTypedMixedRange,
    ],
    _CoreDatasetGetSetMeas[Temporal3_2, Optical3_2],
    _CoreGetSetMeasEndian[
        _AnyMixedDataSchema, Temporal3_2, Optical3_2, pft.OpticalScale3_0
    ],
    _CoreDatasetGetSetMeasEndian[
        _AnyMixedDataSchema, Temporal3_2, Optical3_2, pft.OpticalScale3_0
    ],
    _CoreScaleTransformMethods,
    _CoreTimestepMethods,
    _CoreModified,
    _CorePlate,
    _CoreSpillover,
    _CoreVol,
    _CoreCytsn,
    _CoreMeasWavelengths,
    _CoreMeasDisplay,
    _CoreMeasCalibration[pft.Calibration3_2],
    _CoreGates[pft.AppliedGates3_2],
    _CoreTo2_0[CoreDataset2_0],
    _CoreTo3_0[CoreDataset3_0],
    _CoreTo3_1[CoreDataset3_1],
    _CoreDatasetCommon,
):
    def __new__(
        cls,
        measurements: pft.Measurements3_2,
        data_schema: _AnyMixedDataSchema,
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
        nonstandard_keywords: pft.NonStdKeywords = {},
        analysis: pft.AnalysisBytes = "",
        others: list[bytes] = [],
    ) -> Self: ...
    @classmethod
    def from_kws(
        cls,
        path: Path,
        header: Header,
        std: pft.StdKeywords,
        nonstd: pft.NonStdKeywords,
        # offset args
        allow_pseudoempty: bool = False,
        dataset_overflow_limit: int = 0,
        overlap_correction_limit: int = 0,
        # std args
        dedup_measurement_names: bool = False,
        trim_intra_value_whitespace: bool = False,
        time_meas_pattern: pft.Selector[str | None] = "^(TIME|Time)$",
        allow_missing_time: pft.TriFlag = "false",
        force_linear_scale: pft.ForceLinearScale = "none",
        ignore_time_optical_keys: list[pft.TemporalOpticalKey] = [],
        process_time_optical_keys: pft.ProcessTimeOpticalKeys = "demote_warn",
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
        add_missing_timestep: float | None = None,
        spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
        disallow_localtime: bool = False,
        # layout args
        ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
        promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
        rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
        replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
        append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
        substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
        allow_repair_non_unique: pft.TriFlag = "false",
        text_data_correction: pft.OffsetCorrection = (0, 0),
        text_analysis_correction: pft.OffsetCorrection = (0, 0),
        ignore_text_data_offsets: bool = False,
        ignore_text_analysis_offsets: bool = False,
        allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
        allow_missing_required_offsets: pft.TriFlag = "false",
        process_optional_failure: pft.ProcessKeywordFailure = "error",
        disallow_range_truncation: pft.TriFlag = "false",
        # data args
        data_remainder_limit: int = 0,
        allow_uneven_event_width: pft.TriFlag = "false",
        allow_tot_mismatch: pft.TriFlag = "false",
        over_bitmask_action: pft.OverLimitAction = "trunc_warn",
        over_range_action: pft.OverLimitAction = "warn",
        allow_missing_crc: pft.TriFlag = "false",
        allow_mismatch_crc: pft.TriFlag = "false",
        compute_crc: pft.ComputeReadCRC = "never",
        read_intra_segment_dark_bytes: bool = False,
        read_post_dataset_dark_bytes: bool = False,
        row_buffer_size: int = 28000,
        # shared args
        warnings_are_errors: bool = False,
        hide_warnings: bool = False,
        dataset_offset: int = 0,
        dataset_len: int | None = None,
    ) -> Self: ...

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

@final
class FinalHeaderOffsets:
    def __new__(
        cls,
        text: pft.Offsets,
        data: pft.Offsets,
        analysis: pft.Offsets,
        others: pft.OtherOffsets,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def text(self) -> pft.Offsets: ...
    @property
    def data(self) -> pft.Offsets: ...
    @property
    def analysis(self) -> pft.Offsets: ...
    @property
    def others(self) -> pft.OtherOffsets: ...

@final
class OriginalHeaderOffsets:
    def __new__(
        cls,
        text: pft.Offsets,
        data: pft.Offsets,
        analysis: pft.Offsets,
        others: list[pft.Offsets],
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def text(self) -> pft.Offsets: ...
    @property
    def data(self) -> pft.Offsets: ...
    @property
    def analysis(self) -> pft.Offsets: ...
    @property
    def others(self) -> list[pft.Offsets]: ...

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

@final
class ValidKeywords:
    def __new__(cls, std: pft.StdKeywords, nonstd: pft.NonStdKeywords) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def std(self) -> pft.StdKeywords: ...
    @property
    def nonstd(self) -> pft.NonStdKeywords: ...

@final
class RepairDiagnostics:
    def __new__(
        cls,
        non_unique_std: list[tuple[str, str]],
        non_unique_nonstd: list[tuple[str, str]],
        demoted: list[str],
        promoted: list[str],
        subbed: list[tuple[str, str]],
        replaced: list[tuple[str, str]],
        renamed: list[tuple[str, str]],
        ignored: list[tuple[str, str]],
        removed: list[tuple[str, str]],
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def non_unique_std(self) -> list[tuple[str, str]]: ...
    @property
    def non_unique_nonstd(self) -> list[tuple[str, str]]: ...
    @property
    def demoted(self) -> list[str]: ...
    @property
    def promoted(self) -> list[str]: ...
    @property
    def subbed(self) -> list[tuple[str, str]]: ...
    @property
    def replaced(self) -> list[tuple[str, str]]: ...
    @property
    def renamed(self) -> list[tuple[str, str]]: ...
    @property
    def ignored(self) -> list[tuple[str, str]]: ...
    @property
    def removed(self) -> list[tuple[str, str]]: ...

@final
class DataSchemaDiagnostics:
    def __new__(
        cls,
        truncated_columns: list[Decimal | None],
        original_int_width: int | None,
        original_byteord: list[int] | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def truncated_columns(self) -> list[Decimal | None]: ...
    @property
    def original_int_width(self) -> int | None: ...
    @property
    def original_byteord(self) -> list[int] | None: ...

@final
class StdTEXTDiagnostics:
    def __new__(
        cls,
        optional: pft.StdKeywords,
        pseudostandard: pft.StdKeywords,
        hyper_par: pft.StdKeywords,
        hyper_gate: pft.StdKeywords,
        other_version: pft.StdKeywords,
        timestep: str | None,
        dedup_names: list[pft.Shortname | None],
        scale: list[pft.MeasScaleDiagnostic],
        gate_scale: list[pft.GateScaleDiagnostic],
        trimmed: list[tuple[str, str]],
        temporal_optical_pairs: list[tuple[str, str]],
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
        schema_diagnostics: DataSchemaDiagnostics,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def optional(self) -> pft.StdKeywords: ...
    @property
    def pseudostandard(self) -> pft.StdKeywords: ...
    @property
    def hyper_par(self) -> pft.StdKeywords: ...
    @property
    def hyper_gate(self) -> pft.StdKeywords: ...
    @property
    def other_version(self) -> pft.StdKeywords: ...
    @property
    def timestep(self) -> str | None: ...
    @property
    def dedup_names(self) -> list[pft.Shortname | None]: ...
    @property
    def scale(self) -> list[pft.MeasScaleDiagnostic]: ...
    @property
    def gate_scale(self) -> list[pft.GateScaleDiagnostic]: ...
    @property
    def trimmed(self) -> list[tuple[str, str]]: ...
    @property
    def temporal_optical_pairs(self) -> list[tuple[str, str]]: ...
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
    def schema_diagnostics(self) -> DataSchemaDiagnostics: ...

@final
class DatasetOffsets:
    def __new__(
        cls,
        final_data_offsets: pft.Offsets,
        final_analysis_offsets: pft.Offsets,
        data_origin: TEXTOffsetsOrigin,
        analysis_origin: TEXTOffsetsOrigin,
        data_analysis_overlap: int | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def final_data_offsets(self) -> pft.Offsets: ...
    @property
    def final_analysis_offsets(self) -> pft.Offsets: ...
    @property
    def data_origin(self) -> TEXTOffsetsOrigin: ...
    @property
    def analysis_origin(self) -> TEXTOffsetsOrigin: ...
    @property
    def data_analysis_overlap(self) -> int | None: ...

@final
class SplitTEXTDiagnostics:
    def __new__(
        cls,
        delimiter: int,
        escaped: bool,
        keys_with_blank_values: list[bytes | str],
        values_with_blank_keys: list[bytes | str],
        skipped_pairs: int,
        tokens_with_boundary_delims: list[bytes | str],
        last_odd_token: bytes | str,
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
    def keys_with_blank_values(self) -> list[bytes | str]: ...
    @property
    def values_with_blank_keys(self) -> list[bytes | str]: ...
    @property
    def skipped_pairs(self) -> int: ...
    @property
    def tokens_with_boundary_delims(self) -> list[bytes | str]: ...
    @property
    def last_odd_token(self) -> bytes | str: ...
    @property
    def has_even_delims(self) -> bool: ...
    @property
    def extra_leading_delims(self) -> int: ...
    @property
    def multibyte_encoded(self) -> bool: ...

class _OffsetsOverlap[N0, N1]:
    def __new__(
        cls,
        offsets0: pft.NamedOffsets[N0],
        offsets1: pft.NamedOffsets[N1],
        overlap: int,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def offsets0(self) -> pft.NamedOffsets[N0]: ...
    @property
    def offsets1(self) -> pft.NamedOffsets[N1]: ...
    @property
    def overlap(self) -> int: ...

class _OffsetsOverflow[N0]:
    def __new__(
        cls,
        offsets: pft.NamedOffsets[N0],
        overflow: int,
        dataset_len: int,
        bound_is_nextdata: bool,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def offsets(self) -> pft.NamedOffsets[N0]: ...
    @property
    def overflow(self) -> int: ...
    @property
    def dataset_len(self) -> int: ...
    @property
    def bound_is_nextdata(self) -> bool: ...

@final
class HeaderToHeaderOffsetsOverlap(
    _OffsetsOverlap[pft.HeaderOffsetsName, pft.HeaderOffsetsName]
):
    pass

@final
class TextToHeaderOffsetsOverlap(
    _OffsetsOverlap[pft.TextOffsetsName, pft.HeaderOffsetsName]
):
    pass

@final
class SuppToHeaderOffsetsOverlap(
    _OffsetsOverlap[pft.SuppTextOffsetsName, pft.HeaderOffsetsName]
):
    pass

@final
class TextToHeaderOrSuppOffsetsOverlap(
    _OffsetsOverlap[pft.TextOffsetsName, pft.HeaderOrSuppOffsetsName]
):
    pass

@final
class HeaderOffsetsOverflow(_OffsetsOverflow[pft.HeaderOffsetsName]):
    pass

@final
class TextOffsetsOverflow(_OffsetsOverflow[pft.TextOffsetsName]):
    pass

@final
class SuppOffsetsOverflow(_OffsetsOverflow[pft.SuppTextOffsetsName]):
    pass

@final
class SuppTEXTOffsetsOutput:
    def __new__(
        cls,
        origin_type: pft.SuppTEXTOffsetsOriginType,
        final_offsets: pft.Offsets | None,
        original_offsets: pft.Offsets | None,
        other_index: int | None,
        overlaps: list[SuppToHeaderOffsetsOverlap],
        overflow: SuppOffsetsOverflow | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def origin_type(self) -> pft.SuppTEXTOffsetsOriginType: ...
    @property
    def final_offsets(self) -> pft.Offsets: ...
    @property
    def original_offsets(self) -> pft.Offsets | None: ...
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
        original_offsets: pft.Offsets | None,
        overlaps: list[TextToHeaderOrSuppOffsetsOverlap],
        overflow: TextOffsetsOverflow | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def origin_type(self) -> pft.TEXTOffsetsOriginType: ...
    @property
    def original_offsets(self) -> pft.Offsets | None: ...
    @property
    def overlaps(self) -> list[TextToHeaderOrSuppOffsetsOverlap]: ...
    @property
    def overflow(self) -> TextOffsetsOverflow | None: ...

@final
class HeaderAndSuppOffsets:
    def __new__(
        cls,
        header: Header,
        supp_text: SuppTEXTOffsetsOutput,
        nextdata: int | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def header(self) -> Header: ...
    @property
    def supp_text(self) -> SuppTEXTOffsetsOutput: ...
    @property
    def nextdata(self) -> int | None: ...

@final
class FlatTEXTDiagnostics:
    def __new__(
        cls,
        header_supp: HeaderAndSuppOffsets,
        primary_text_overflow: int,
        header_overflows: list[HeaderOffsetsOverflow],
        byte_pairs: list[tuple[bytes | str, bytes | str]],
        non_unique_std_keywords: list[tuple[str, str]],
        non_unique_nonstd_keywords: list[tuple[str, str]],
        keys_with_empty_trimmed_values: list[bytes | str],
        keys_with_trimmed_values: list[tuple[bytes | str, str]],
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
    def byte_pairs(self) -> list[tuple[bytes | str, bytes | str]]: ...
    @property
    def non_unique_std_keywords(self) -> list[tuple[str, str]]: ...
    @property
    def non_unique_nonstd_keywords(self) -> list[tuple[str, str]]: ...
    @property
    def keys_with_empty_trimmed_values(self) -> list[bytes | str]: ...
    @property
    def keys_with_trimmed_values(self) -> list[tuple[bytes | str, str]]: ...
    @property
    def primary_split(self) -> SplitTEXTDiagnostics: ...
    @property
    def supp_split(self) -> SplitTEXTDiagnostics | None: ...

@final
class FlatTEXTOutput:
    def __new__(
        cls,
        keywords: ValidKeywords,
        flat_diagnostics: FlatTEXTDiagnostics,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def keywords(self) -> ValidKeywords: ...
    @property
    def flat_diagnostics(self) -> FlatTEXTDiagnostics: ...

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
class FlatDatasetFromKwsOutput:
    def __new__(
        cls,
        data: DataFrame,
        analysis: bytes,
        others: list[bytes],
        dataset_offsets: DatasetOffsets,
        repair_diagnostics: RepairDiagnostics,
        schema_diagnostics: DataSchemaDiagnostics,
        dataset_diagnostics: DatasetDiagnostics,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def data(self) -> DataFrame: ...
    @property
    def analysis(self) -> bytes: ...
    @property
    def others(self) -> list[bytes]: ...
    @property
    def dataset_offsets(self) -> DatasetOffsets: ...
    @property
    def repair_diagnostics(self) -> RepairDiagnostics: ...
    @property
    def schema_diagnostics(self) -> DataSchemaDiagnostics: ...
    @property
    def dataset_diagnostics(self) -> DatasetDiagnostics: ...

@final
class NewFlatDatasetFromKwsOutput:
    def __new__(
        cls,
        dataset: FlatDatasetFromKwsOutput,
        header: FinalHeaderOffsets,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def dataset(self) -> FlatDatasetFromKwsOutput: ...
    @property
    def header(self) -> FinalHeaderOffsets: ...

@final
class FlatDatasetOutput:
    def __new__(
        cls,
        keywords: ValidKeywords,
        flat_diagnostics: FlatTEXTDiagnostics,
        dataset: FlatDatasetFromKwsOutput,
        version_scores: pft.KeywordVersionScores | None,
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
class StdDatasetFromKwsOutput:
    def __new__(
        cls,
        dataset_offsets: DatasetOffsets,
        repair_diagnostics: RepairDiagnostics,
        std_diagnostics: StdTEXTDiagnostics,
        dataset_diagnostics: DatasetDiagnostics,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def dataset_offsets(self) -> DatasetOffsets: ...
    @property
    def repair_diagnostics(self) -> RepairDiagnostics: ...
    @property
    def std_diagnostics(self) -> StdTEXTDiagnostics: ...
    @property
    def dataset_diagnostics(self) -> DatasetDiagnostics: ...

@final
class NewStdDatasetFromKwsOutput:
    def __new__(
        cls,
        dataset: StdDatasetFromKwsOutput,
        header: FinalHeaderOffsets,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def dataset(self) -> StdDatasetFromKwsOutput: ...
    @property
    def header(self) -> FinalHeaderOffsets: ...

@final
class StdDatasetOutput:
    def __new__(
        cls,
        dataset: StdDatasetFromKwsOutput,
        flat_diagnostics: FlatTEXTDiagnostics,
        version_scores: pft.KeywordVersionScores | None,
    ) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def dataset(self) -> StdDatasetFromKwsOutput: ...
    @property
    def flat_diagnostics(self) -> FlatTEXTDiagnostics: ...
    @property
    def version_scores(self) -> pft.KeywordVersionScores | None: ...

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
        datatype: pft.Datatype,
        dataset_offset: int,
        file_crc: pft.CRCOutput,
        computed_crc: int | None,
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
    def datatype(self) -> pft.Datatype: ...
    @property
    def dataset_offset(self) -> int: ...
    @property
    def file_crc(self) -> pft.CRCOutput: ...
    @property
    def computed_crc(self) -> int | None: ...

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

@final
class BuildInfo:
    def __new__(cls) -> Self: ...
    def __deepcopy__(self, memo: Any) -> Self: ...
    @property
    def version(self) -> str: ...
    @property
    def commit_hash(self) -> str: ...
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

def fcs_read_header(
    path: Path,
    # header args
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    allow_pseudoempty: bool = False,
    # offset args
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    dataset_offset: int = 0,
) -> Header: ...

#
def fcs_read_flat_text(
    path: Path,
    # header args
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    ignore_supp_text: bool = False,
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
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    dataset_offset: int = 0,
) -> FlatTEXTOutput: ...

#
def fcs_read_std_text(
    path: Path,
    # header args
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    ignore_supp_text: bool = False,
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
    # standard args
    dedup_measurement_names: bool = False,
    trim_intra_value_whitespace: bool = False,
    time_meas_pattern: pft.Selector[str | None] = "^(TIME|Time)$",
    allow_missing_time: pft.TriFlag = "false",
    force_linear_scale: pft.ForceLinearScale = "none",
    ignore_time_optical_keys: list[pft.TemporalOpticalKey] = [],
    process_time_optical_keys: pft.ProcessTimeOpticalKeys = "demote_warn",
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
    add_missing_timestep: float | None = None,
    spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
    disallow_localtime: bool = False,
    # layout args
    ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
    promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
    replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
    append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
    substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
    allow_repair_non_unique: pft.TriFlag = "false",
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    fix_int_widths: pft.FixIntWidths = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    dataset_offset: int = 0,
) -> tuple[pft.AnyCoreTEXT, StdTEXTOutput]: ...

#
def fcs_read_flat_dataset(
    path: Path,
    # header args
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    ignore_supp_text: bool = False,
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
    # layout args
    ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
    promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
    replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
    append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
    substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
    allow_repair_non_unique: pft.TriFlag = "false",
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    fix_int_widths: pft.FixIntWidths = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    # data args
    data_remainder_limit: int = 0,
    allow_uneven_event_width: pft.TriFlag = "false",
    allow_tot_mismatch: pft.TriFlag = "false",
    over_bitmask_action: pft.OverLimitAction = "trunc_warn",
    over_range_action: pft.OverLimitAction = "warn",
    allow_missing_crc: pft.TriFlag = "false",
    allow_mismatch_crc: pft.TriFlag = "false",
    compute_crc: pft.ComputeReadCRC = "never",
    read_intra_segment_dark_bytes: bool = False,
    read_post_dataset_dark_bytes: bool = False,
    row_buffer_size: int = 28000,
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    dataset_offset: int = 0,
    scan: bool = False,
) -> FlatDatasetOutput: ...

#
def fcs_read_std_dataset(
    path: Path,
    # header args
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    ignore_supp_text: bool = False,
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
    # standard args
    dedup_measurement_names: bool = False,
    trim_intra_value_whitespace: bool = False,
    time_meas_pattern: pft.Selector[str | None] = "^(TIME|Time)$",
    allow_missing_time: pft.TriFlag = "false",
    force_linear_scale: pft.ForceLinearScale = "none",
    ignore_time_optical_keys: list[pft.TemporalOpticalKey] = [],
    process_time_optical_keys: pft.ProcessTimeOpticalKeys = "demote_warn",
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
    add_missing_timestep: float | None = None,
    spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
    disallow_localtime: bool = False,
    # layout args
    ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
    promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
    replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
    append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
    substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
    allow_repair_non_unique: pft.TriFlag = "false",
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    fix_int_widths: pft.FixIntWidths = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    # data args
    data_remainder_limit: int = 0,
    allow_uneven_event_width: pft.TriFlag = "false",
    allow_tot_mismatch: pft.TriFlag = "false",
    over_bitmask_action: pft.OverLimitAction = "trunc_warn",
    over_range_action: pft.OverLimitAction = "warn",
    allow_missing_crc: pft.TriFlag = "false",
    allow_mismatch_crc: pft.TriFlag = "false",
    compute_crc: pft.ComputeReadCRC = "never",
    read_intra_segment_dark_bytes: bool = False,
    read_post_dataset_dark_bytes: bool = False,
    row_buffer_size: int = 28000,
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    dataset_offset: int = 0,
    scan: bool = False,
) -> tuple[pft.AnyCoreDataset, StdDatasetOutput]: ...

#
def fcs_read_flat_texts(
    path: Path,
    skip: int | None = None,
    limit: int | None = None,
    scan: bool = False,
    # header args
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    ignore_supp_text: bool = False,
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
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
) -> list[FlatTEXTOutput]: ...

#
def fcs_read_std_texts(
    path: Path,
    skip: int | None = None,
    limit: int | None = None,
    scan: bool = False,
    # header args
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    ignore_supp_text: bool = False,
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
    # standard args
    dedup_measurement_names: bool = False,
    trim_intra_value_whitespace: bool = False,
    time_meas_pattern: pft.Selector[str | None] = "^(TIME|Time)$",
    allow_missing_time: pft.TriFlag = "false",
    force_linear_scale: pft.ForceLinearScale = "none",
    ignore_time_optical_keys: list[pft.TemporalOpticalKey] = [],
    process_time_optical_keys: pft.ProcessTimeOpticalKeys = "demote_warn",
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
    add_missing_timestep: float | None = None,
    spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
    disallow_localtime: bool = False,
    # layout args
    ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
    promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
    replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
    append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
    substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
    allow_repair_non_unique: pft.TriFlag = "false",
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    fix_int_widths: pft.FixIntWidths = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
) -> list[tuple[pft.AnyCoreTEXT, StdTEXTOutput]]: ...

#
def fcs_read_flat_datasets(
    path: Path,
    skip: int | None = None,
    limit: int | None = None,
    scan: bool = False,
    # header args
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    ignore_supp_text: bool = False,
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
    # layout args
    ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
    promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
    replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
    append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
    substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
    allow_repair_non_unique: pft.TriFlag = "false",
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    fix_int_widths: pft.FixIntWidths = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    # data args
    data_remainder_limit: int = 0,
    allow_uneven_event_width: pft.TriFlag = "false",
    allow_tot_mismatch: pft.TriFlag = "false",
    over_bitmask_action: pft.OverLimitAction = "trunc_warn",
    over_range_action: pft.OverLimitAction = "warn",
    allow_missing_crc: pft.TriFlag = "false",
    allow_mismatch_crc: pft.TriFlag = "false",
    compute_crc: pft.ComputeReadCRC = "never",
    read_intra_segment_dark_bytes: bool = False,
    read_post_dataset_dark_bytes: bool = False,
    row_buffer_size: int = 28000,
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
) -> list[FlatDatasetOutput]: ...

#
def fcs_read_std_datasets(
    path: Path,
    skip: int | None = None,
    limit: int | None = None,
    scan: bool = False,
    # header args
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    ignore_supp_text: bool = False,
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
    # standard args
    dedup_measurement_names: bool = False,
    trim_intra_value_whitespace: bool = False,
    time_meas_pattern: pft.Selector[str | None] = "^(TIME|Time)$",
    allow_missing_time: pft.TriFlag = "false",
    force_linear_scale: pft.ForceLinearScale = "none",
    ignore_time_optical_keys: list[pft.TemporalOpticalKey] = [],
    process_time_optical_keys: pft.ProcessTimeOpticalKeys = "demote_warn",
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
    add_missing_timestep: float | None = None,
    spillover_measurement_mode: pft.SpilloverMeasurementMode = "named",
    disallow_localtime: bool = False,
    # layout args
    ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
    promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
    replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
    append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
    substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
    allow_repair_non_unique: pft.TriFlag = "false",
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    fix_int_widths: pft.FixIntWidths = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    # data args
    data_remainder_limit: int = 0,
    allow_uneven_event_width: pft.TriFlag = "false",
    allow_tot_mismatch: pft.TriFlag = "false",
    over_bitmask_action: pft.OverLimitAction = "trunc_warn",
    over_range_action: pft.OverLimitAction = "warn",
    allow_missing_crc: pft.TriFlag = "false",
    allow_mismatch_crc: pft.TriFlag = "false",
    compute_crc: pft.ComputeReadCRC = "never",
    read_intra_segment_dark_bytes: bool = False,
    read_post_dataset_dark_bytes: bool = False,
    row_buffer_size: int = 28000,
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
) -> list[tuple[pft.AnyCoreDataset, StdDatasetOutput]]: ...

#
def fcs_read_flat_dataset_with_keywords(
    path: Path,
    header: Header,
    kws: ValidKeywords,
    # offset args
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    # layout args
    ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
    promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
    replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
    append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
    substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
    allow_repair_non_unique: pft.TriFlag = "false",
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    fix_int_widths: pft.FixIntWidths = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    # data args
    data_remainder_limit: int = 0,
    allow_uneven_event_width: pft.TriFlag = "false",
    allow_tot_mismatch: pft.TriFlag = "false",
    over_bitmask_action: pft.OverLimitAction = "trunc_warn",
    over_range_action: pft.OverLimitAction = "warn",
    allow_missing_crc: pft.TriFlag = "false",
    allow_mismatch_crc: pft.TriFlag = "false",
    compute_crc: pft.ComputeReadCRC = "never",
    read_intra_segment_dark_bytes: bool = False,
    read_post_dataset_dark_bytes: bool = False,
    row_buffer_size: int = 28000,
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
    dataset_offset: int = 0,
    dataset_len: int | None = None,
) -> FlatDatasetFromKwsOutput: ...

#
def fcs_summarize(
    path: Path,
    skip: int | None = None,
    limit: int | None = None,
    scan: bool = False,
    # header args
    text_correction: pft.OffsetCorrection = (0, 0),
    data_correction: pft.OffsetCorrection = (0, 0),
    analysis_correction: pft.OffsetCorrection = (0, 0),
    other_corrections: list[pft.OffsetCorrection] = [],
    max_other: int | None = None,
    other_width: int = 8,
    guess_other_width: pft.GuessOtherWidth = "none",
    squish_offsets: bool = False,
    # offset args
    allow_pseudoempty: bool = False,
    dataset_overflow_limit: int = 0,
    overlap_correction_limit: int = 0,
    # flat args
    version_override: pft.VersionOverride | None = None,
    supp_text_correction: pft.OffsetCorrection = (0, 0),
    nextdata_correction: int = 0,
    allow_duplicated_supp_text: pft.TriFlag = "false",
    ignore_supp_text: bool = False,
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
    # layout args
    ignore_standard_keys: pft.AppendableSelector[pft.KeyPatterns] = [],
    promote_to_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    demote_from_standard: pft.AppendableSelector[pft.KeyPatterns] = [],
    rename_standard_keys: pft.AppendableSelector[dict[str, str]] = {},
    replace_standard_key_values: pft.AppendableSelector[dict[str, str]] = {},
    append_standard_keywords: pft.AppendableSelector[dict[str, str]] = {},
    substitute_standard_key_values: pft.AppendableSelector[pft.SubPatterns] = {},
    allow_repair_non_unique: pft.TriFlag = "false",
    text_data_correction: pft.OffsetCorrection = (0, 0),
    text_analysis_correction: pft.OffsetCorrection = (0, 0),
    ignore_text_data_offsets: bool = False,
    ignore_text_analysis_offsets: bool = False,
    allow_header_text_offset_mismatch: pft.AllowHeaderTextOffsetMismatch = "error",
    allow_missing_required_offsets: pft.TriFlag = "false",
    process_optional_failure: pft.ProcessKeywordFailure = "error",
    fix_int_widths: pft.FixIntWidths = "never",
    byteord_override: pft.ByteordOverride = "none",
    disallow_range_truncation: pft.TriFlag = "false",
    # data args
    data_remainder_limit: int = 0,
    allow_uneven_event_width: pft.TriFlag = "false",
    allow_tot_mismatch: pft.TriFlag = "false",
    over_bitmask_action: pft.OverLimitAction = "trunc_warn",
    over_range_action: pft.OverLimitAction = "warn",
    allow_missing_crc: pft.TriFlag = "false",
    allow_mismatch_crc: pft.TriFlag = "false",
    compute_crc: pft.ComputeReadCRC = "never",
    read_intra_segment_dark_bytes: bool = False,
    read_post_dataset_dark_bytes: bool = False,
    row_buffer_size: int = 28000,
    # shared args
    warnings_are_errors: bool = False,
    hide_warnings: bool = False,
) -> list[DatasetSummary]: ...

#
def fcs_write_datasets(
    path: Path,
    datasets: list[pft.AnyCoreDataset],
    delim: int = 30,
    big_other: bool = False,
    compute_crc: bool = False,
    override_fil: bool = False,
    allow_over_bitmask: pft.TriFlag = "false",
    disallow_over_range: pft.TriFlag = "false",
    row_buffer_size: int = 28000,
) -> int | None: ...

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
]
