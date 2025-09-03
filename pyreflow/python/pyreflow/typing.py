import pyreflow._pyreflow as pf
from typing import Literal, TypeAlias
from decimal import Decimal
import numpy as np
import numpy.typing as npt

MeasIndex: TypeAlias = int

Endian: TypeAlias = Literal["big", "little"]

ByteOrd: TypeAlias = list[int] | Endian

Range: TypeAlias = float

FloatRange: TypeAlias = Decimal

IntRange: TypeAlias = int

Shortname: TypeAlias = str

Timestep: TypeAlias = float

StdKey: TypeAlias = str

NonStdKey: TypeAlias = str

AnalysisBytes: TypeAlias = bytes

OtherBytes: TypeAlias = bytes

Trigger: TypeAlias = tuple[Shortname, int]

Unicode: TypeAlias = tuple[int, list[str]]

CsvFlags: TypeAlias = list[int | None]

Compensation: TypeAlias = npt.NDArray[np.float32]

Spillover: TypeAlias = tuple[list[str], npt.NDArray[np.float32]]

UnstainedCenters: TypeAlias = dict[StdKey, float]

Segment: TypeAlias = tuple[int, int]

OffsetCorrection: TypeAlias = tuple[int, int]

StdKeywords: TypeAlias = dict[StdKey, str]
NonStdKeywords: TypeAlias = dict[NonStdKey, str]

Calibration3_1: TypeAlias = tuple[float, str]
Calibration3_2: TypeAlias = tuple[float, float, str]

Scale: TypeAlias = tuple[float, float] | tuple[()]
ScaleTransform: TypeAlias = tuple[float, float] | float

Display: TypeAlias = tuple[bool, float, float]

Mode = Literal["L", "C", "U"]

Mode3_2 = Literal["L"]

Originality = Literal["Original", "NonDataModified", "Appended", "DataModified"]

Feature = Literal["Area", "Width", "Height"]

FCSVersion = Literal["FCS2.0", "FCS3.0", "FCS3.1", "FCS3.2"]

FloatType = Literal["F"]
DoubleType = Literal["D"]
IntegerType = Literal["I"]
AsciiType = Literal["A"]

Datatype: TypeAlias = FloatType | DoubleType | IntegerType | AsciiType
MixedType: TypeAlias = (
    tuple[FloatType | DoubleType, FloatRange] | tuple[AsciiType | IntegerType, IntRange]
)

TemporalOpticalKey = Literal[
    "F",
    "L",
    "O",
    "T",
    "P",
    "V",
    "CALIBRATION",
    "DET",
    "TAG",
    "FEATURE",
    "ANALYTE",
]

AnyCoreTEXT: TypeAlias = (
    pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreTEXT3_1 | pf.CoreTEXT3_2
)

AnyCoreDataset: TypeAlias = (
    pf.CoreDataset2_0 | pf.CoreDataset3_0 | pf.CoreDataset3_1 | pf.CoreDataset3_2
)

AnyCore: TypeAlias = AnyCoreTEXT | AnyCoreDataset


AnyOptical: TypeAlias = pf.Optical2_0 | pf.Optical3_0 | pf.Optical3_1 | pf.Optical3_2

AnyTemporal: TypeAlias = (
    pf.Temporal2_0 | pf.Temporal3_0 | pf.Temporal3_1 | pf.Temporal3_2
)

AnyMeas: TypeAlias = AnyOptical | AnyTemporal

AppliedGates2_0: TypeAlias = tuple[
    list[pf.GatedMeasurement],
    dict[int, pf.UnivariateRegion2_0 | pf.BivariateRegion2_0],
    str | None,
]

AppliedGates3_0: TypeAlias = tuple[
    list[pf.GatedMeasurement],
    dict[int, pf.UnivariateRegion3_0 | pf.BivariateRegion3_0],
    str | None,
]

AppliedGates3_2: TypeAlias = tuple[
    dict[int, pf.UnivariateRegion3_2 | pf.BivariateRegion3_2],
    str | None,
]
