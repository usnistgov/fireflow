import pyreflow._pyreflow as pf
from typing import Literal, TypeAlias

MeasIndex: TypeAlias = int

ByteOrd: TypeAlias = list[int]

Range: TypeAlias = float

FloatRange: TypeAlias = float

IntRange: TypeAlias = int

Shortname: TypeAlias = str

Timestep: TypeAlias = float

StdKey: TypeAlias = str

NonStdKey: TypeAlias = str

AnalysisBytes: TypeAlias = bytes

OtherBytes: TypeAlias = bytes

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
