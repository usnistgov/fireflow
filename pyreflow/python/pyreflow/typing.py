from ._pyreflow import (
    CoreTEXT2_0,
    CoreTEXT3_0,
    CoreTEXT3_1,
    CoreTEXT3_2,
    CoreDataset2_0,
    CoreDataset3_0,
    CoreDataset3_1,
    CoreDataset3_2,
)
from typing import Literal, NewType, TypeAlias

MeasIndex = NewType("MeasIndex", int)

ByteOrd = NewType("ByteOrd", list[int])

Range = NewType("Range", float)

FloatRange = NewType("FloatRange", float)

IntRange = NewType("IntRange", int)

Shortname = NewType("Shortname", str)

Timestep = NewType("Timestep", float)

StdKey = NewType("StdKey", str)

NonStdKey = NewType("NonStdKey", str)

AnalysisBytes = NewType("AnalysisBytes", bytes)

OtherBytes = NewType("OtherBytes", bytes)

Trigger: TypeAlias = tuple[Shortname, int]

Segment: TypeAlias = tuple[int, int]

OffsetCorrection: TypeAlias = tuple[int, int]

KeyPatterns: TypeAlias = tuple[list[str], list[str]]

StdKeywords: TypeAlias = dict[StdKey, str]
NonStdKeywords: TypeAlias = dict[NonStdKey, str]

Calibration3_1: TypeAlias = tuple[float, str]
Calibration3_2: TypeAlias = tuple[float, float, str]

Scale: TypeAlias = tuple[float, float] | tuple[()]
ScaleTransform: TypeAlias = tuple[float, float] | tuple[float]

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

AnyCoreTEXT: TypeAlias = CoreTEXT2_0 | CoreTEXT3_0 | CoreTEXT3_1 | CoreTEXT3_2

AnyCoreDataset: TypeAlias = (
    CoreDataset2_0 | CoreDataset3_0 | CoreDataset3_1 | CoreDataset3_2
)

AnyCore: TypeAlias = AnyCoreTEXT | AnyCoreDataset
