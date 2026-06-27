from __future__ import annotations
import pyreflow._pyreflow as pf
from typing import Literal
import numpy as np
import numpy.typing as npt

type MeasIndex = int

type Endian = Literal["big", "little"]

type ByteOrd = list[int] | Endian

type Range = float | int

type FloatRange = float

type IntRange = int

type Shortname = str

type Timestep = float

type StdKey = str

type NonStdKey = str

type AnalysisBytes = bytes

type OtherBytes = bytes

type Trigger = tuple[Shortname, int]

type Unicode = tuple[int, list[str]]

type CsvFlags = list[int | None]

type Compensation = npt.NDArray[np.float32]

type Spillover = tuple[list[str], npt.NDArray[np.float32]]

type UnstainedCenters = dict[StdKey, float]

type Offsets = tuple[int, int]

type OffsetCorrection = tuple[int, int]

type StdKeywords = dict[StdKey, str]
type NonStdKeywords = dict[NonStdKey, str]

type Calibration3_1 = tuple[float, str]
type Calibration3_2 = tuple[float, float, str]

type OpticalScale2_0 = tuple[float, float] | tuple[()] | None
type OpticalScale3_0 = tuple[float, float] | float

type Display = tuple[bool, float, float]

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
type Datatype = FloatType | DoubleType | IntegerType | AsciiType

F32Type = Literal["F32"]
F64Type = Literal["F64"]
IntegerWidth = Literal["U08", "U16", "U24", "U32", "U40", "U48", "U56", "U64"]
type AnyType = F32Type | F64Type | AsciiType | IntegerWidth
type VariableBitmask = tuple[IntegerWidth, IntRange]
type MixedRange = (
    tuple[F32Type | F64Type, Range] | tuple[AsciiType | IntegerWidth, IntRange]
)

type MaybeTypedVariableBitmask = IntRange | VariableBitmask
type MaybeTypedMixedRange = Range | MixedRange

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

type TemporalType = Literal["Time"]

type AnyCoreTEXT = pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreTEXT3_1 | pf.CoreTEXT3_2

type AnyCoreDataset = (
    pf.CoreDataset2_0 | pf.CoreDataset3_0 | pf.CoreDataset3_1 | pf.CoreDataset3_2
)

type AnyCore = AnyCoreTEXT | AnyCoreDataset


type AnyOptical = pf.Optical2_0 | pf.Optical3_0 | pf.Optical3_1 | pf.Optical3_2

type AnyTemporal = pf.Temporal2_0 | pf.Temporal3_0 | pf.Temporal3_1 | pf.Temporal3_2

type AnyAsciiDataSchema = pf.DelimAsciiDataSchema | pf.FixedAsciiDataSchema

type AnyDataSchema2_0 = (
    AnyAsciiDataSchema
    | pf.OrderedUintDataSchema
    | pf.OrderedF32DataSchema
    | pf.OrderedF64DataSchema
)

type AnyDataSchema3_0 = AnyDataSchema2_0

type AnyDataSchema3_1 = (
    AnyAsciiDataSchema
    | pf.SingleUintDataSchema
    | pf.VariableUintDataSchema
    | pf.BigLittleF32DataSchema
    | pf.BigLittleF64DataSchema
)

type AnyDataSchema3_2 = AnyDataSchema3_1 | pf.MixedDataSchema

type AnyMeas = AnyOptical | AnyTemporal

type AppliedGates2_0 = tuple[
    list[pf.GatedMeasurement],
    dict[int, pf.UnivariateRegion2_0 | pf.BivariateRegion2_0],
    str | None,
]

type AppliedGates3_0 = tuple[
    list[pf.GatedMeasurement],
    dict[int, pf.UnivariateRegion3_0 | pf.BivariateRegion3_0],
    str | None,
]

type AppliedGates3_2 = tuple[
    dict[int, pf.UnivariateRegion3_2 | pf.BivariateRegion3_2],
    str | None,
]

type KeyPatterns = list[str]

type SubPattern = tuple[str, str, bool]

type SubPatterns = dict[str, SubPattern]

type DelimEscapeMode = Literal[
    "escaped",
    "unescaped",
    "guess_escaped",
    "guess_unescaped",
]


type ReqOrOpt = Literal["req_only", "opt_only", "both"]

type RootOrMeas = Literal["root_only", "meas_only", "both"]

type ProcessKeywordFailure = Literal[
    "error", "demote_warn", "demote_silent", "drop_warn", "drop_silent"
]

type ProcessTimeOpticalKeys = Literal[
    "demote_warn", "demote_silent", "drop_warn", "drop_silent"
]

type TriFlag = Literal["false", "true", "silent"]

type ForceLinearScale = Literal["none", "time_only", "all_non_int", "all"]

type MeasScaleDiagnostic = (
    tuple[str, Literal["forced", "log", "trimmed", "trimmed_log"]] | None
)

type GateScaleDiagnostic = tuple[str, Literal["log", "trimmed", "trimmed_log"]] | None

type TrimValueWhitespace = Literal[
    "notrim", "trim", "trim_blank_warn", "trim_blank_silent"
]

type SpilloverMeasurementMode = Literal["named", "indexed", "guess"]

type KeywordVersionScores = tuple[
    pf.KeywordVersionScore,
    pf.KeywordVersionScore,
    pf.KeywordVersionScore,
    pf.KeywordVersionScore,
]

type UseEncoding = Literal["single", "utf8", "guess"]

type GuessOtherWidth = Literal["none", "error", "warn", "silent"]

type OtherOffsets = tuple[list[tuple[int, Offsets]], int]

type AllowHeaderTextOffsetMismatch = Literal[
    "error", "header_warn", "header_silent", "text_warn", "text_silent"
]

type CheckedRangeDatatypes = Literal["bitmask_only", "int_only", "all", "none"]

type OverLimitAction = Literal[
    "error", "warn", "silent", "trunc_warn", "trunc_silent", "none"
]

type FixIntWidths = int | Literal["next_byte", "never"]

type ByteordOverride = list[int] | Literal["endian", "none"]

type HeaderOffsetsName = Literal["text", "data", "header"]
type SuppTextOffsetsName = Literal["supp_text"]
type TextOffsetsName = Literal["data", "header"]
type HeaderOrSuppOffsetsName = HeaderOffsetsName | SuppTextOffsetsName

type NamedOffsets[N] = tuple[N, int, int]

type HeaderNamedOffsets = NamedOffsets[HeaderOffsetsName]
type SuppTEXTNamedOffsets = NamedOffsets[SuppTextOffsetsName]
type TextNamedOffsets = NamedOffsets[TextOffsetsName]
type HeaderOrSuppNamedOffsets = NamedOffsets[HeaderOrSuppOffsetsName]

type SuppTEXTOffsetsOriginType = Literal[
    "empty",
    "unparsed",
    "malformed",
    "dup_ptext",
    "dup_analysis",
    "ignored",
    "dup_other",
    "valid",
]

type TEXTOffsetsOriginType = Literal[
    "empty_text",
    "ignored",
    "unparsed",
    "malformed",
    "match",
    "mismatch_header",
    "mismatch_text",
    "empty_header",
]

type CRCOutput = bytes | str | tuple[int, int] | None

type ComputeReadCRC = Literal["never", "always", "test"]

type FlankingSegmentName = Literal["text", "stext", "data", "analysis"] | int

type DarkBytes = str | bytes | tuple[int, int]

#: A dynamic selector for a type based on contents of an FCS file.
#:
#: This is used to select certain configuration options based on the keywords
#: of an FCS file.
#:
#: The type can be included simply by itself, in which case no selection will
#: occur.
#:
#: Alternatively, the type can be embedded in a series of Lisp-like conditional
#: statements represented as Python tuples.
#:
#: If the tuple's first element is ``"if"``, the second must be a condition, the
#: third must be a another selector (the same as this type) which will be
#: evaluated if the condition is true, and the fourth must be another selector
#: or ``None`` which will be evaluated if the condition is false.
#:
#: If the tuple's first element is ``"cond"``, each subsequent element must be a
#: tuple pair with a condition and and a statement to be evaluated if the
#: condition is true. These conditions will be evaluated in series until the
#: the first true case.
#:
#: If this evaluates to ``None``, the default for the underlying type ``T`` will
#: be chosen.
type Selector[T] = (
    T
    | tuple[Literal["if"], Condition, Selector[T], Selector[T] | None]
    | tuple[Literal["cond"], Cond[T]]
    | tuple[Literal["cond"], Cond[T], Cond[T]]
    | tuple[Literal["cond"], Cond[T], Cond[T], Cond[T]]
    | tuple[Literal["cond"], Cond[T], Cond[T], Cond[T], Cond[T]]
    | tuple[Literal["cond"], Cond[T], Cond[T], Cond[T], Cond[T], Cond[T]]
    | tuple[Literal["cond"], Cond[T], Cond[T], Cond[T], Cond[T], Cond[T], Cond[T]]
)

#: A conditional rule to be used in :py:class:`~pyreflow.typing.Selector`.
type Cond[T] = tuple[Condition, Selector[T]]

#: An expression which evaluates to true or false depending on FCS keywords.
#:
#: This is a List-like set of tuples that represent conditional logic. The first
#: element in each tuple is a "logical function" which will evaluate to true or
#: false depending on the boolean outputs of the arguments that follow.
type Condition = (
    tuple[Literal["and"], Statement, Statement]
    | tuple[Literal["or"], Statement, Statement]
    | tuple[Literal["not"], Statement]
)

#: Evaluates to true of false depending on the value of an FCS file keyword.
#:
#: These are Lisp-like expressions represented as Python tuples where the first
#: element is a "function" which is run with the arguments that follow.
#:
#: If ``"has_key"``, return true if the indicated key is present.
#:
#: If ``"key_is"``, return true if the indicated key (first argument) has a
#: value exactly equal to the second argument.
#:
#: If ``"key_matches"``, return true if the indicated key (first argument) has a
#: value which matches the regular expression (second argument). The regexp must
#: follow the syntax of the rust `regexp crate <https://docs.rs/regex-syntax/latest/regex_syntax/>`__.
#:
#: Keys can either be standard (start with ``"$"``) or non-standard (no ``"$"``).
type Statement = (
    tuple[Literal["has_key"], str]
    | tuple[Literal["key_is"], str, str]
    | tuple[Literal["key_matches"], str, str]
)
