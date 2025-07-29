from typing import Literal, Union, NewType

Calibration3_1 = tuple[float, str]
Calibration3_2 = tuple[float, float, str]

Mode = Union[Literal["L"] | Literal["C"] | Literal["U"]]

Originality = Union[
    Literal["Original"]
    | Literal["NonDataModified"]
    | Literal["Appended"]
    | Literal["DataModified"]
]

Feature = Union[Literal["Area"] | Literal["Width"] | Literal["Height"]]

FCSVersion = (
    Literal["FCS2.0"] | Literal["FCS3.0"] | Literal["FCS3.1"] | Literal["FCS3.2"]
)

FloatType = Literal["F"]
DoubleType = Literal["D"]
IntegerType = Literal["I"]
AsciiType = Literal["A"]

Datatype = Union[FloatType | DoubleType | IntegerType | AsciiType]
MixedType = Union[
    tuple[FloatType | DoubleType, float], tuple[AsciiType | IntegerType, int]
]
Trigger = tuple[str, int]
Shortname = NewType("Shortname", str)

Segment = tuple[int, int]
OffsetCorrection = tuple[int, int]
KeyPatterns = tuple[list[str], list[str]]

StdKey = NewType("StdKey", str)
NonStdKey = NewType("NonStdKey", str)

StdKeywords = dict[StdKey, str]
NonStdKeywords = dict[NonStdKey, str]

AnalysisBytes = NewType("AnalysisBytes", bytes)
OtherBytes = NewType("OtherBytes", bytes)
