from __future__ import annotations
from abc import ABC, abstractmethod
import pyreflow._pyreflow as pf
from typing import Literal
import numpy as np
import numpy.typing as npt


type MeasIndex = int

#: The endian-ness of values in the *DATA* segment.
#:
#: Corresponds to the value of *$BYTEORD* for FCS 3.1/3.2.
type Endian = Literal["big", "little"]

#: The order of bytes to encode the values in the *DATA* segment.
#:
#: Corresponds to the value of *$BYTEORD* for FCS 2.0/3.0.
type ByteOrd = list[int] | Endian

type Range = float | int

type FloatRange = float

type IntRange = int

type Shortname = str

type Timestep = float

type StdKey = str

type NonStdKey = str

type AnalysisBytes = str | bytes

type OtherBytes = str | bytes

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

type Mode = Literal["L", "C", "U"]

type Mode3_2 = Literal["L"]

type Originality = Literal["Original", "NonDataModified", "Appended", "DataModified"]

type Feature = Literal["Area", "Width", "Height"]

type FCSVersion = Literal["FCS2.0", "FCS3.0", "FCS3.1", "FCS3.2"]

type VersionOverride = (
    FCSVersion
    | Literal[
        "latest",
        "earliest",
        "loose",
        "strict",
        "current_or_latest",
        "current_or_earliest",
        "current_or_loose",
        "current_or_strict",
    ]
)

type ByteWidth = Literal[1, 2, 3, 4, 5, 6, 7, 8]

#: Any value value for the *$DATATYPE* keyword.
type Datatype = FloatType | DoubleType | IntegerType | AsciiType

#: Value when *$DATATYPE* corresponds to 32-bit float.
type FloatType = Literal["F"]

#: Value when *$DATATYPE* corresponds to 64-bit float.
type DoubleType = Literal["D"]

#: Value when *$DATATYPE* corresponds to an unsigned integer.
type IntegerType = Literal["I"]

#: Value when *$DATATYPE* corresponds to ASCII-encoded values.
type AsciiType = Literal["A"]

type F32Type = Literal["F32"]
type F64Type = Literal["F64"]
type IntegerWidth = Literal["U08", "U16", "U24", "U32", "U40", "U48", "U56", "U64"]
type AnyType = F32Type | F64Type | AsciiType | IntegerWidth
type VariableBitmask = tuple[IntegerWidth, IntRange]
type MixedRange = (
    tuple[F32Type | F64Type, Range] | tuple[AsciiType | IntegerWidth, IntRange]
)

type MaybeTypedVariableBitmask = IntRange | VariableBitmask
type MaybeTypedMixedRange = Range | MixedRange

type TemporalOpticalKey = Literal[
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

type Measurement[
    N,
    T: AnyTemporal,
    O: AnyOptical,
    S: OpticalScale2_0 | OpticalScale3_0,
] = tuple[N, O, S] | tuple[Shortname, T]

type Measurements[
    N,
    T: AnyTemporal,
    O: AnyOptical,
    S: OpticalScale2_0 | OpticalScale3_0,
] = list[Measurement[N, T, O, S]]

type Measurement2_0 = Measurement[
    Shortname | None, pf.Temporal2_0, pf.Optical2_0, OpticalScale2_0
]

type Measurement3_0 = Measurement[
    Shortname | None, pf.Temporal3_0, pf.Optical3_0, OpticalScale3_0
]

type Measurement3_1 = Measurement[
    Shortname, pf.Temporal3_1, pf.Optical3_1, OpticalScale3_0
]

type Measurement3_2 = Measurement[
    Shortname, pf.Temporal3_2, pf.Optical3_2, OpticalScale3_0
]

type Measurements2_0 = Measurements[
    Shortname | None, pf.Temporal2_0, pf.Optical2_0, OpticalScale2_0
]

type Measurements3_0 = Measurements[
    Shortname | None, pf.Temporal3_0, pf.Optical3_0, OpticalScale3_0
]

type Measurements3_1 = Measurements[
    Shortname, pf.Temporal3_1, pf.Optical3_1, OpticalScale3_0
]

type Measurements3_2 = Measurements[
    Shortname, pf.Temporal3_2, pf.Optical3_2, OpticalScale3_0
]

type OpticalKeyVals[X] = list[X | tuple[()] | None]


type AnyDataSchema3_2 = (
    BigLittleDataSchema
    | AsciiDataSchema
    | pf.VariableUintDataSchema
    | pf.MixedDataSchema
)

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
    | tuple[Literal["if"], Condition, Selector[T]]
    | tuple[Literal["if"], Condition, Selector[T], Selector[T]]
    | tuple[Literal["cond"], list[tuple[Condition, Selector[T]]]]
)

#: Like a :py:class:`~pyreflow.typing.Selector` but can also include a list.
#:
#: The values of the results of each individual selector will be concatenated.
type AppendableSelector[T] = Selector[T] | list[Selector[T]]

#: An expression which evaluates to true or false depending on FCS keywords.
#:
#: This is either a bare :py:class:`~pyreflow.typing.KeyTest` which will
#: evaluated directly or a List-like set of tuples that represent conditional
#: logic. The first element in each tuple is a "logical function" which will
#: evaluate to true or false depending on the boolean outputs of the arguments
#: that follow.
type Condition = (
    KeyTest
    | tuple[Literal["and"], KeyTest, KeyTest]
    | tuple[Literal["or"], KeyTest, KeyTest]
    | tuple[Literal["not"], KeyTest]
)

#: Evaluates to true or false depending on the value of an FCS file keyword.
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
type KeyTest = (
    tuple[Literal["has_key"], str]
    | tuple[Literal["key_is"], str, str]
    | tuple[Literal["key_matches"], str, str]
)


class SingleTypedDataSchema(ABC):
    """A data schema defined by a single *$DATATYPE* value."""

    @property
    @abstractmethod
    def datatype(self) -> Datatype:
        """The value of the *$DATATYPE* keyword."""
        ...


class AsciiDataSchema(SingleTypedDataSchema):
    """A data schema which uses ASCII for the underlying encoding."""

    pass


class NumericDataSchema(ABC):
    """A data schema which uses binary-encoded numbers."""

    @property
    @abstractmethod
    def is_float(self) -> bool:
        """``True`` if the numeric type is a floating point."""
        ...


class MatrixDataSchema(SingleTypedDataSchema, NumericDataSchema):
    """A data schema which has only one numeric value type."""

    @property
    @abstractmethod
    def byte_width(self) -> ByteWidth:
        """The width of each value in bytes.

        This is the same as *$PnB* divided by 8 which should be the same for
        all measurement columns.
        """
        ...


class OrderedDataSchema(MatrixDataSchema):
    """A data schema which can be encoded with any byte order."""

    @property
    @abstractmethod
    def byteord(self) -> ByteOrd:
        """The order of bytes for each encoded value.

        Corresponds to the value of the *$BYTEORD* keyword.

        Only applies to non-ASCII schemas for FCS 2.0 and 3.0.
        """
        ...


class BigLittleDataSchema(ABC):
    """A data schema which can be either big or little endian."""

    @property
    @abstractmethod
    def endian(self) -> Endian:
        """The endian-ness of the encoded numeric values.

        Corresponds to the value of the *$BYTEORD* keyword.

        Only applies to non-ASCII schemas for FCS 3.1 and 3.2.
        """
        ...


_ABC_MAP: dict[type, list[type[ABC]]] = {
    pf.OrderedUintDataSchema: [OrderedDataSchema],
    pf.OrderedF32DataSchema: [OrderedDataSchema],
    pf.OrderedF64DataSchema: [OrderedDataSchema],
    pf.SingleUintDataSchema: [MatrixDataSchema, BigLittleDataSchema],
    pf.VariableUintDataSchema: [BigLittleDataSchema, NumericDataSchema],
    pf.BigLittleF32DataSchema: [MatrixDataSchema, BigLittleDataSchema],
    pf.BigLittleF64DataSchema: [MatrixDataSchema, BigLittleDataSchema],
    pf.MixedDataSchema: [BigLittleDataSchema],
    pf.FixedAsciiDataSchema: [AsciiDataSchema],
    pf.DelimAsciiDataSchema: [AsciiDataSchema],
}
