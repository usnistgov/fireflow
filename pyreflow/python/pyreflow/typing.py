import pyreflow._pyreflow as pf
from typing import Literal, TypeAlias, TypeVar
import numpy as np
import numpy.typing as npt

MeasIndex: TypeAlias = int

Endian: TypeAlias = Literal["big", "little"]

ByteOrd: TypeAlias = list[int] | Endian

Range: TypeAlias = float | int

FloatRange: TypeAlias = float

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

Offsets: TypeAlias = tuple[int, int]

OffsetCorrection: TypeAlias = tuple[int, int]

StdKeywords: TypeAlias = dict[StdKey, str]
NonStdKeywords: TypeAlias = dict[NonStdKey, str]

Calibration3_1: TypeAlias = tuple[float, str]
Calibration3_2: TypeAlias = tuple[float, float, str]

OpticalScale2_0: TypeAlias = tuple[float, float] | tuple[()] | None
OpticalScale3_0: TypeAlias = tuple[float, float] | float

Display: TypeAlias = tuple[bool, float, float]

Mode = Literal["L", "C", "U"]

Mode3_2 = Literal["L"]

Originality = Literal["Original", "NonDataModified", "Appended", "DataModified"]

Feature = Literal["Area", "Width", "Height"]

FCSVersion = Literal["FCS2.0", "FCS3.0", "FCS3.1", "FCS3.2"]

VersionOverride = Literal[
    "FCS2.0",
    "FCS3.0",
    "FCS3.1",
    "FCS3.2",
    "latest",
    "earliest",
    "loose",
    "strict",
]

FloatType = Literal["F"]
DoubleType = Literal["D"]
IntegerType = Literal["I"]
AsciiType = Literal["A"]
Datatype: TypeAlias = FloatType | DoubleType | IntegerType | AsciiType

F32Type = Literal["F32"]
F64Type = Literal["F64"]
IntegerWidth = Literal["U08", "U16", "U24", "U32", "U40", "U48", "U56", "U64"]
AnyType: TypeAlias = F32Type | F64Type | AsciiType | IntegerWidth
VariableBitmask: TypeAlias = tuple[IntegerWidth, IntRange]
MixedRange: TypeAlias = (
    tuple[F32Type | F64Type, Range] | tuple[AsciiType | IntegerWidth, IntRange]
)

MaybeTypedVariableBitmask: TypeAlias = IntRange | VariableBitmask
MaybeTypedMixedRange: TypeAlias = Range | MixedRange

TemporalOpticalKey = Literal[
    "G",
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

TemporalType: TypeAlias = Literal["Time"]

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

AnyAsciiDataSchema: TypeAlias = pf.DelimAsciiDataSchema | pf.FixedAsciiDataSchema

AnyDataSchema2_0: TypeAlias = (
    AnyAsciiDataSchema
    | pf.OrderedUintDataSchema
    | pf.OrderedF32DataSchema
    | pf.OrderedF64DataSchema
)

AnyDataSchema3_0: TypeAlias = AnyDataSchema2_0

AnyDataSchema3_1: TypeAlias = (
    AnyAsciiDataSchema
    | pf.SingleUintDataSchema
    | pf.VariableUintDataSchema
    | pf.BigLittleF32DataSchema
    | pf.BigLittleF64DataSchema
)

AnyDataSchema3_2: TypeAlias = AnyDataSchema3_1 | pf.MixedDataSchema

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

KeyPatterns: TypeAlias = list[str]

SubPattern: TypeAlias = tuple[str, str, bool]

SubPatterns: TypeAlias = dict[str, SubPattern]

DelimEscapeMode: TypeAlias = Literal[
    "escaped",
    "unescaped",
    "guess_escaped",
    "guess_unescaped",
]


ReqOrOpt: TypeAlias = Literal["req_only", "opt_only", "both"]

RootOrMeas: TypeAlias = Literal["root_only", "meas_only", "both"]

ProcessKeywordFailure: TypeAlias = Literal[
    "error", "demote_warn", "demote_silent", "drop_warn", "drop_silent"
]

ProcessTimeOpticalKeys: TypeAlias = Literal[
    "demote_warn", "demote_silent", "drop_warn", "drop_silent"
]

TriFlag: TypeAlias = Literal["false", "true", "silent"]

ForceLinearScale: TypeAlias = Literal["none", "time_only", "all_non_int", "all"]

MeasScaleDiagnostic: TypeAlias = (
    tuple[str, Literal["forced", "log", "trimmed", "trimmed_log"]] | None
)

GateScaleDiagnostic: TypeAlias = (
    tuple[str, Literal["log", "trimmed", "trimmed_log"]] | None
)

TrimValueWhitespace: TypeAlias = Literal[
    "notrim", "trim", "trim_blank_warn", "trim_blank_silent"
]

SpilloverMeasurementMode: TypeAlias = Literal["named", "indexed", "guess"]

KeywordVersionScores: TypeAlias = tuple[
    pf.KeywordVersionScore,
    pf.KeywordVersionScore,
    pf.KeywordVersionScore,
    pf.KeywordVersionScore,
]

UseEncoding: TypeAlias = Literal["single", "utf8", "guess"]

GuessOtherWidth: TypeAlias = Literal["none", "error", "warn", "silent"]

OtherOffsets: TypeAlias = tuple[list[tuple[int, Offsets]], int]

AllowHeaderTextOffsetMismatch: TypeAlias = Literal[
    "error", "header_warn", "header_silent", "text_warn", "text_silent"
]

CheckedRangeDatatypes: TypeAlias = Literal["bitmask_only", "int_only", "all", "none"]

OverLimitAction: TypeAlias = Literal[
    "error", "warn", "silent", "trunc_warn", "trunc_silent", "none"
]

FixIntWidths: TypeAlias = int | Literal["next_byte", "never"]

ByteordOverride: TypeAlias = list[int] | Literal["endian", "none"]

HeaderOffsetsName: TypeAlias = Literal["text", "data", "header"]
SuppTextOffsetsName: TypeAlias = Literal["supp_text"]
TextOffsetsName: TypeAlias = Literal["data", "header"]
HeaderOrSuppOffsetsName: TypeAlias = HeaderOffsetsName | SuppTextOffsetsName

N = TypeVar("N")

NamedOffsets: TypeAlias = tuple[N, int, int]

HeaderNamedOffsets: TypeAlias = NamedOffsets[HeaderOffsetsName]
SuppTEXTNamedOffsets: TypeAlias = NamedOffsets[SuppTextOffsetsName]
TextNamedOffsets: TypeAlias = NamedOffsets[TextOffsetsName]
HeaderOrSuppNamedOffsets: TypeAlias = NamedOffsets[HeaderOrSuppOffsetsName]

SuppTEXTOffsetsOriginType: TypeAlias = Literal[
    "empty",
    "unparsed",
    "malformed",
    "dup_ptext",
    "dup_analysis",
    "ignored",
    "dup_other",
    "valid",
]

TEXTOffsetsOriginType: TypeAlias = Literal[
    "empty_text",
    "ignored",
    "unparsed",
    "malformed",
    "match",
    "mismatch_header",
    "mismatch_text",
    "empty_header",
]

CRCOutput: TypeAlias = bytes | str | tuple[int, int] | None

ComputeReadCRC: TypeAlias = Literal["never", "always", "test"]

FlankingSegmentName: TypeAlias = Literal["text", "stext", "data", "analysis"] | int
