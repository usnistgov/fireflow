from typing import Literal, NewType, TypeAlias

Shortname = NewType("Shortname", str)

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

Originality = Literal["Original", "NonDataModified", "Appended", "DataModified"]

Feature = Literal["Area", "Width", "Height"]

FCSVersion = Literal["FCS2.0", "FCS3.0", "FCS3.1", "FCS3.2"]

FloatType = Literal["F"]
DoubleType = Literal["D"]
IntegerType = Literal["I"]
AsciiType = Literal["A"]

Datatype: TypeAlias = FloatType | DoubleType | IntegerType | AsciiType
MixedType: TypeAlias = (
    tuple[FloatType | DoubleType, float] | tuple[AsciiType | IntegerType, int]
)
