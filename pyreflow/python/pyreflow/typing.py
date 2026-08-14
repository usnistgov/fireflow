from __future__ import annotations
from abc import ABC, abstractmethod
import pyreflow._pyreflow as pf
from typing import Literal
import numpy as np
import numpy.typing as npt

#
# Config flags and values
#

type OffsetCorrection = tuple[int, int]
"""
Correction for segment offset pair.

Each number will be added to the two offsets in the pair respectively.
"""

type FCSVersion = Literal["FCS2.0", "FCS3.0", "FCS3.1", "FCS3.2"]
"""Any of the supported FCS version strings.

One of these must always be in first six bytes of an FCS dataset.

"""

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
"""Flag to denote how version should be overridden.

Supplying a literal FCS version will directly override the version. It will be
as if the version in *HEADER* does not exist.

If an explicit version is not supplied, this flag will trigger an algorithm to
guess the version based on the keywords present in *TEXT*. A given version will
"match" if it all of its required keywords are satisfied based on *TEXT*. If no
versions match, the algorithm fails and throw an error. If one version matches,
the algorithm returns that version.

The remaining levels in this flag only matter if more than one version matches,
in which case they describe the criteria by which to rank and choose the "best
match":

* ``"latest"``: choose the latest version (ie FCS3.2 > FCS3.1)
* ``"earliest"``: choose the earliest version (ie FCS3.2 < FCS3.1)
* ``"strict"``: choose the version with the least  optional keywords
* ``"loose"``: choose the version with the most optional keywords
* ``"current_or_*"``: like each of the above levels but choose the version
  listed in *HEADER* above all others if this matches

There are some non-obvious edge cases for the version-matching algorithm:

* The *$DFCmTOn* keywords in FCS2.0 are collectively counted as one optional
  keyword. This is because the equivalent in FCS3.0 is *$COMP* which is just one
  keyword.
* The *$PnB* keywords are required in all versions, but starting in FCS3.1 they
  are allowed to differ. Therefore if multiple *$PnB* values are found, FCS2.0
  and FCS3.0 automatically are deemed invalid matches.
* If *$BYTEORD* is monotonic, the version must be FCS2.0 or FCS3.0.
* If *$MODE* is ``"U"`` or ``"C"``, the version cannot be FCS3.2.
* If *$PnL* is a comma-separated list of numbers, the version must be FCS3.1 or
  FCS3.2.

"""

type DelimEscapeMode = Literal[
    "escaped",
    "unescaped",
    "guess_escaped",
    "guess_unescaped",
]
"""Flag to determine how to escape delims in *TEXT*.

If ``"escaped"`` or ``"unescaped"``, escape or do not escape delimiters
respectively. If ``"guess_escaped"`` or ``"guess_unescaped"``, attempt
to guess how delimiters should be treated, falling back to escaped or
unescaped mode respectively if the choice is ambiguous.

"""

type KeyPattern = NEStr
"""A pattern which matches standard or nonstandard key values.

This may be either a literal keyword value or a regular expression
pattern.

A literal pattern must match the target keyword exactly (case insensitive). This
is less flexible than a regular expression but is much faster.

A regular expression is denoted by prefixing and suffixing with ``"/"`` (ie like
``"/<pattern>/"``). The value of ``"<pattern>"`` must follow the syntax outlined
in `regexp-syntax <https://docs.rs/regex/latest/regex/#syntax>`__.

"""

type SubPattern = tuple[NEStr, str, bool]
"""A sed-like pattern which substitutes values in a string.

Each element corresponds to a regular expression, replacement pattern, and
global flag respectively (ie roughly analogous to
``s/<pattern>/<replacement>[/g]`` in the ``sed`` command).

The regular expression must follow the syntax outlined in `regexp-syntax
<https://docs.rs/regex/latest/regex/#syntax>`__. It may also contain capture
expressions which must be matched exactly in the replacement string. Any
references in replacement string must be given with surrounding brackets like
``"${1}"`` (equivalent to ``\\1`` in ``sed``) or ``"${cygnus}"`` which
corresponds to the Rust regexp syntax.

If the global flag is ``True``, replace all found matches, otherwise only
replace the first.

"""

type KeyPatterns = list[KeyPattern]
"""A list of patterns which match standard or nonstandard key values."""

type SubPatterns = dict[KeyPattern, SubPattern]
"""Substitution patterns which may be used to modify keywords.

The key is matched using :py:type:`~pyreflow.typing.KeyPattern`, and the
the value of the key is modified via :py:type:`~pyreflow.typing.SubPattern`.

"""

type KeyStringPairs = dict[KeyString, KeyString]
"""Mapping between names of keys from *TEXT*.

All values must be unique and no value can match its own key.
"""

type KeyStringValues = dict[KeyString, NEStr]
"""Mapping between a key from *TEXT* and a value."""

type ProcessKeywordFailure = Literal[
    "error",
    "demote_warn",
    "demote_silent",
    "drop_warn",
    "drop_silent",
]
"""Flag denoting what should happen if a keyword cannot be parsed.

Levels are as follows:

* ``"error"``: throw error
* ``"demote_warn"``: demote to non-standard with warning
* ``"demote_silent"``: demote to non-standard without warning
* ``"drop_warn"``: drop with warning
* ``"drop_silent"``: drop without warning

"""

type OpticalOnlyKey = Literal[
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
"""A key which should only be used for optical measurements."""

type ProcessOpticalOnlyKeys = Literal[
    "demote_warn", "demote_silent", "drop_warn", "drop_silent"
]
"""Flag denoting how to handle optical keywords found in a temporal measurement."""

type TriFlag = Literal["false", "true", "silent"]
"""Flag which may be in three states.

The meaning of ``"true"`` and ``"false"`` depends on context.
``"silent"`` means the behavior controlled by the flag will not emit any
errors or warnings.

"""

type ForceLinearScale = Literal["none", "time_only", "all_non_int", "all"]
"""Flag denoting where to fix *$PnE* values that should be linear.

Levels are as follows:

* ``"time_only"``: only change the temporal measurement
* ``"all_non_int"``: change non-integer and temporal measurements
* ``"all"``: change all measurements
* ``"none"``: change no measurements 

"""

type TrimValueWhitespace = Literal[
    "notrim", "trim", "trim_blank_warn", "trim_blank_silent"
]
"""Flag denoting how to trim whitespace around keyword values in *TEXT*.

Levels are as follows:

* ``"notrim"``: do not trim at all
* ``"trim"``: trim and throw error if result is blank
* ``"trim_blank_warn"``: trim and throw warning if result is blank
* ``"trim_blank_silent"`` trim and do nothing if result is blank

"""

type SpilloverMeasurementMode = Literal["named", "indexed", "guess"]
"""Flag denoting how to interpret names for *$SPILLOVER* keyword.

The "names" are the sequence of identifiers after the first integer (the
size of the matrix) and before the values of the matrix itself.

Levels are as follows:

* ``"named"``: interpret as names which link to *$PnN*
* ``"indexed"``: interpret as 1-indices which point to measurements
* ``"guess"``: automatically choose the prior two modes

"""

type UseEncoding = Literal["single", "utf8", "guess"]
"""Flag denoting how bytes in *TEXT* should be interpreted.

Levels are as follows:

* ``"single"``: interpret bytes as IANA ISO/IEC-8859-1 (aka Latin-1)
* ``"utf8"``: interpret bytes as UTF-8
* ``"guess"``: interpret bytes as UTF-8 and fall back to to IANA
  ISO/IEC-8859-1 on failure

"""

type GuessOtherWidth = Literal["none", "error", "warn", "silent"]
"""Flag to denote how *OTHER* width fields should be guessed.

Levels are as follows:

* ``"none"``: do not guess
* ``"error"``: guess and throw error on failure
* ``"warn"``: guess and throw warning on failure
* ``"silent"`` guess and do nothing on failure

"""

type AllowHeaderTextOffsetMismatch = Literal[
    "error", "header_warn", "header_silent", "text_warn", "text_silent"
]
"""Flag denoting what to do if offsets from *HEADER* and *TEXT* mismatch.

Levels are as follows:

* ``"error"``: throw error
* ``"header_warn"``: choose *HEADER* and throw warning
* ``"header_silent"``: choose *HEADER* and do nothing
* ``"text_warn"``: choose *TEXT* and throw warning
* ``"text_silent"``: choose *TEXT* and do nothing

"""

type OverLimitAction = Literal[
    "error", "warn", "silent", "trunc_warn", "trunc_silent", "none"
]
"""Flag to denote what should happen if a value out of range.

Levels are as follows:

* ``"error"``: emit error
* ``"warn"``: emit warning
* ``"silent"``: do nothing
* ``"trunc_warn"`` truncate and emit warning
* ``"trunc_silent"``: truncate with no warning

"""

type IntWidthOverride = int | Literal["next_byte", "never"]
"""Fix *$PnB* if incorrect.

Set to ``"next_byte"`` or ``"never"`` to round up to next multiple of 8
or do nothing respectively.

Set to an integer 1-8 to override all *$PnB* explicitly.

"""

type ByteordOverride = list[int] | Literal["endian", "none"]
"""Flag denoting how/when to override *$BYTEORD* if it is broken.

Set to ``"none"`` or ``"endian"`` to do nothing or interpret *$BYTEORD*
based on its endian-ness (ie without its length) respectively. Set to an
explicit integer sequence to set *$BYTEORD* directly.

"""

type ComputeCRC = Literal["never", "always", "test"]
"""Flag denoting when to compute the CRC.

Levels are as follows:

* ``"never"``: never compute CRC
* ``"always"``: always compute CRC
* ``"test"``: only compute CRC when a CRC word was found at the end of
  the dataset to which the computed CRC should be compared

"""

type Selector[T] = (
    T
    | tuple[Literal["if"], Condition, Selector[T]]
    | tuple[Literal["if"], Condition, Selector[T], Selector[T]]
    | tuple[Literal["cond"], list[tuple[Condition, Selector[T]]]]
)
"""A dynamic selector for a type based on contents of an FCS file.

This is used to select certain configuration options based on the
keywords of an FCS file.

The type can be included simply by itself, in which case no selection
will occur.

Alternatively, the type can be embedded in a series of Lisp-like
conditional statements represented as Python tuples.

If the tuple's first element is ``"if"``, the second must be a
condition, the third must be a another selector (the same as this type)
which will be evaluated if the condition is true, and the fourth must be
another selector or ``None`` which will be evaluated if the condition is
false.

If the tuple's first element is ``"cond"``, each subsequent element must
be a tuple pair with a condition and and a statement to be evaluated if
the condition is true. These conditions will be evaluated in series
until the the first true case.

If this evaluates to ``None``, the default for the underlying type ``T``
will be chosen.

"""

type AppendableSelector[T] = Selector[T] | list[Selector[T]]
"""Like a :py:class:`~pyreflow.typing.Selector` but can include a list.

The values of the results of each individual selector will be
concatenated.

"""

type Condition = (
    KeyTest
    | tuple[Literal["and"], KeyTest, KeyTest]
    | tuple[Literal["or"], KeyTest, KeyTest]
    | tuple[Literal["not"], KeyTest]
)
"""An expression which evaluates to true/false depending on FCS keywords.

This is either a bare :py:class:`~pyreflow.typing.KeyTest` which will
evaluated directly or a List-like set of tuples that represent
conditional logic. The first element in each tuple is a "logical
function" which will evaluate to true or false depending on the boolean
outputs of the arguments that follow.

"""

type KeyTest = (
    tuple[Literal["has_key"], str]
    | tuple[Literal["key_is"], str, str]
    | tuple[Literal["key_matches"], str, str]
)
"""Evaluates to true/false depending on the value of an FCS keyword.

These are Lisp-like expressions represented as Python tuples where the
first element is a "function" which is run with the arguments that
follow.

If ``"has_key"``, return true if the indicated key is present.

If ``"key_is"``, return true if the indicated key (first argument) has a
value exactly equal to the second argument.

If ``"key_matches"``, return true if the indicated key (first argument)
has a value which matches the regular expression (second argument). The
regexp must follow the syntax of the rust `regexp crate
<https://docs.rs/regex-syntax/latest/regex_syntax/>`__.

Keys can either be standard (start with ``"$"``) or non-standard (no
``"$"``).

"""

type ReqOrOpt = Literal["req_only", "opt_only", "both"]
"""A filter denoting required and/or optional keywords."""

type RootOrMeas = Literal["root_only", "meas_only", "both"]
"""A filter denoting root and/or measurement keywords."""

#
# Keyword value aliases
#

type NEStr = str
"""A string which cannot be empty."""

type NEStrOrBytes = str | bytes
"""A string or bytestring which cannot be empty."""

type KeyOrBytes = StdKey | NonStdKey | bytes
"""A valid key from *TEXT* or a bytestring."""

type KeyString = NEStr
"""A standard or nonstandard key depending on context.

If referring to a standard key, the leading ``"$"`` is implied.

Only printable ASCII characters are allowed.

"""

type StdKey = NEStr
"""The value of a standard key (ie starts with ``"$"``).

Only printable ASCII characters are allowed.

"""

type NonStdKey = NEStr
"""The value of a non-standard key (ie does not start with ``"$"``).

Only printable ASCII characters are allowed.

"""

type StdKeywords = dict[StdKey, NEStr]
"""All standard keywords and their serialized values."""

type NonStdKeywords = dict[NonStdKey, NEStr]
"""All non-standard keywords and their serialized values."""

type MeasIndex = int
"""The index for a measurement in a dataset (starting at 0)."""

type Endian = Literal["big", "little"]
"""The endian-ness of values in the *DATA* segment.

Corresponds to the value of *$BYTEORD* for FCS 3.1/3.2.
"""

type ByteOrd = Endian | list[int]
"""The order of bytes to encode the values in the *DATA* segment.

Corresponds to the value of *$BYTEORD* for FCS 2.0/3.0.
"""

type Range = FloatRange | IntRange
"""The value of *$PnR* (data schema agnostic)."""

type FloatRange = float
"""The value of *$PnR* for numeric float layouts."""

type IntRange = int
"""The value of *$PnR* for integer layouts (including ASCII)."""

type Shortname = NEStr
"""The value of *$PnN*.

This must not contain commas..
"""

type Timestep = float
"""The value of *$Timestep*.

This must be greater than ``"0.0"``.
"""

type Trigger = tuple[Shortname, int]
"""The value of the *$TR* keyword.

The first element is the measurement name (ie a *$PnN*) and the second
is the trigger threshold value. When serialized in an FCS file this will
be like ``"<name>,<threshold>"``.

"""

type Unicode = tuple[int, list[NEStr]]
"""The value of the *$UNICODE* keyword.

The first element is the page code and the second is is a list of
keywords. When written to an FCS file this will be serialized as a
comma-separated list.

"""

type CsvFlags = list[int | None]
"""Combined values for *$CSVnFLAG* and *$CSMODE*.

Each element in the list corresponds to $CSVnFLAG and the length of the
list corresponds to $CSMODE.

"""

type Compensation = npt.NDArray[np.float32]
"""The value of the compensation matrix."""

type Spillover = tuple[list[Shortname], npt.NDArray[np.float32]]
"""The value of *$SPILLOVER* for FCS 3.1/3.2.

The first element of the tuple corresponds to the row and column
names of the matrix. Each must match existing *$PnN* keywords and must
be unique.

The second element is the matrix itself. This must be square and have
the same width/height as the list of names in the first element. It also
must be at least 2x2.

"""

type UnstainedCenters = dict[StdKey, float]
"""The value of *$UNSTAINEDCENTERS*.

Keys correspond to *$PnN*.
"""

type Calibration3_1 = tuple[float, NEStr]
"""The value of *$PnCALIBRATION* (FCS 3.1).

The first element corresponds to the calibration factor (ie slope).

The second element corresponds to the calibration unit (ie ERF).
"""

type Calibration3_2 = tuple[float, float, NEStr]
"""The value of *$PnCALIBRATION* (FCS 3.2).

The first element corresponds to the calibration factor (ie slope).

The second element corresponds to the calibration offset.

The third element corresponds to the calibration unit (ie ERF).
"""

type OpticalScale2_0 = tuple[float, float] | tuple[()] | None
"""The value of *$PnE* (FCS 2.0 only).

The first variant corresponds to log-scaled values where both values
must be positive.

The second variant corresponds to linear scaling, which in FCS is
written as ``"0,0"``.

*$PnE* in FCS2.0 is optional so ``None`` can be given if *$PnE* does
not exist for a measurement.

"""


type OpticalScale3_0 = tuple[float, float] | float
"""The combined value of *$PnE* and *$PnG* (FCS 3.0 and up).

The first variant corresponds to log-scaled values where both values
must be non-zero. This corresponds to a *$PnE* of
``"<value1>,<value2>"`` and either a *$PnG* value or ``1.0`` (both of
which are assumed to be noop).

The second variant corresponds to linear scaling, possibly with fixed
gain; this means *$PnE* is ``"0,0"`` and *$PnG* is a positive number.

"""

type Display = tuple[bool, float, float]
"""The value of *$PnD*.

The first element is ``False`` if the display is linear, ``True`` if logarithmic.

The second and third elements correspond to either lower and upper bound
(linear) or decades and offset (logarithmic). In the latter case, both numbers
must be positive.

"""

type Mode = Literal["L", "C", "U"]
"""The allowed values of *$MODE* (up to FCS 3.1)."""

type Mode3_2 = Literal["L"]
"""The allowed values of *$MODE* (FCS 3.2)."""

type Originality = Literal[
    "Original",
    "NonDataModified",
    "Appended",
    "DataModified",
]
"""The allowed values of *$ORIGINALITY*."""

type Feature = Literal["Area", "Width", "Height"]

type ByteWidth = Literal[1, 2, 3, 4, 5, 6, 7, 8]
"""A valid width for a numeric data type.

Technically floats can only be 4 or 8 bytes wide, but this type variable
represents the union of this and all possible values for integer widths,
which is 1-8 bytes.
"""

type Datatype = Literal["F", "D", "I", "A"]
"""The value of the *$DATATYPE* keyword."""

type AsciiType = Literal["A"]
"""Value when *$DATATYPE* corresponds to ASCII-encoded values."""

type AnyType = AnyFloatType | AnyIntegerType | AsciiType
"""Any numeric datatype supported in the *DATA* segment.

This is not the same as the allowed values for *$DATATYPE* since it
also needs to include integer widths.
"""

type VariableBitmask = tuple[ByteWidth, IntRange]
"""The width and range for a column in :py:class:`~pyreflow.VariableUintDataSchema`.

Both width and range are necessary since starting in FCS3.1, integer measurement
column widths are no longer restricted by *$BYTEORD* and thus are allowed to be
different.

Each element corresponds to the byte width and range for a given
measurement. In the FCS file, this is *$PnB* divided by 8 and *$PnR*
less one respectively.

"""


type MixedRange = (
    tuple[AnyFloatType, Range] | tuple[AsciiType | AnyIntegerType, IntRange]
)
"""The data type and range for a column in a mixed-type data schema.

Each variant tuple is like ``(<type>, <range>)`` where ``type`` is one
of ``"A"``, ``"I**"``, or ``"F**"`` corresponding to Ascii, unsigned
integer, or float datatypes respectively. For integers and floats, the
``"**"`` encode the size, which must be 08-64 (in multiples of 8) and
32/64 respectively.

``type`` corresponds to *$DATATYPE*, *$PnB*, and *$PnDATATYPE* (if it
exists for this particular measurement column). ``range`` corresponds to
*$PnR*.

"""

type AnyFloatType = Literal["F32", "F64"]
"""An identifier corresponding to floating point numeric types."""

type AnyIntegerType = Literal["U08", "U16", "U24", "U32", "U40", "U48", "U56", "U64"]
"""An identifier corresponding to integer data types of any supported width."""

type MaybeTypedVariableBitmask = IntRange | VariableBitmask
"""A range which may or may not have a width.

If emitted from a data schema with exactly one byte width, only the
range (ie the value of *$PnR*) will be given.

If emitted from a data schema with multiple integer widths, the value of
*$PnB* will be also be returned to disambiguate the meaning of *$PnR*.

This is necessary for FCS3.1 which may have data schemas that include
multiple integer widths.

"""

type MaybeTypedMixedRange = Range | MixedRange
"""A range which may or may not have a datatype.

If emitted from a data schema with exactly one data type, only the
range (ie the value of *$PnR*) will be given.

If emitted from a data schema with multiple data types, the data type
(ie the combined value of *$DATATYPE*, *$PnB*, and *$PnDATATYPE* if
applicable) will be also be returned to disambiguate the meaning of
*$PnR*.

This is necessary for FCS3.2 which may have data schemas that include
multiple data types.

"""

type AppliedGates2_0 = tuple[
    list[pf.GatedMeasurement],
    dict[int, pf.UnivariateRegion2_0 | pf.BivariateRegion2_0],
    str | None,
]
"""Value for *$Gm\\*/$Rn\\*/$GATING/$GATE* keywords.

The first element corresponds to the *$Gm\\** keywords, where ``m`` is
given by position in the list.

The second element corresponds to the *$RnI* and *$RnW* keywords and is
a mapping of regions and windows to be used in gating scheme. Keys in
dictionary are the region indices (the ``n`` in *$Rn\\**). The values in
the dictionary are either univariate or bivariate gates and must
correspond to an index in the list in the first element.

The third element corresponds to the *$GATING* keyword. All "Rn" in this
string must reference a key in the dict of the second member.

"""

type AppliedGates3_0 = tuple[
    list[pf.GatedMeasurement],
    dict[int, pf.UnivariateRegion3_0 | pf.BivariateRegion3_0],
    str | None,
]
"""Value for *$Gm\\*/$Rn\\*/$GATING/$GATE keywords*.

The first element corresponds to the *$Gm\\** keywords, where ``m`` is
given by position in the list.

The second element corresponds to the *$RnI* and *$RnW* keywords and is
a mapping of regions and windows to be used in gating scheme. Keys in
dictionary are the region indices (the ``n`` in *$Rn\\**). The values in
the dictionary are either univariate or bivariate gates and must
correspond to an index in the list in the first element or a physical
measurement.

The third element corresponds to the *$GATING* keyword. All "Rn" in this
string must reference a key in the dict of the second member.

"""

type AppliedGates3_2 = tuple[
    dict[int, pf.UnivariateRegion3_2 | pf.BivariateRegion3_2],
    str | None,
]
"""Value for *$Rn\\*/$GATING* keywords.

The first element corresponds to the *$RnI* and *$RnW* keywords and is a
mapping of regions and windows to be used in gating scheme. Keys in
dictionary are the region indices (the n in *$Rn\\**). The values in the
dictionary are either univariate or bivariate gates and must correspond
to a physical measurement.

The second element corresponds to the *$GATING* keyword. All "Rn" in
this string must reference a key in the dict of the first member.

"""

#
# Class union aliases
#

type AnyCoreTEXT = pf.CoreTEXT2_0 | pf.CoreTEXT3_0 | pf.CoreTEXT3_1 | pf.CoreTEXT3_2
"""A standardized FCS *TEXT* segment from any version."""

type AnyCoreDataset = (
    pf.CoreDataset2_0 | pf.CoreDataset3_0 | pf.CoreDataset3_1 | pf.CoreDataset3_2
)
"""A standardized FCS dataset from any version."""

type AnyCore = AnyCoreTEXT | AnyCoreDataset
"""A standardized FCS output from any version (may or may not have *DATA*)."""

type AnyOptical = pf.Optical2_0 | pf.Optical3_0 | pf.Optical3_1 | pf.Optical3_2
"""Standardized optical keywords from any FCS version."""

type AnyTemporal = pf.Temporal2_0 | pf.Temporal3_0 | pf.Temporal3_1 | pf.Temporal3_2
"""Standardized temporal keywords from any FCS version."""

type AnyMeas = AnyOptical | AnyTemporal
"""Standardized measurement keywords (temporal or optical)."""

type Measurement[
    N,
    T: AnyTemporal,
    O: AnyOptical,
    S: OpticalScale2_0 | OpticalScale3_0,
] = tuple[N, O, S] | tuple[Shortname, T]
"""All keywords associated with a measurement (optical or temporal).

Generic aliases are as follows:

* ``N``: type corresponding to *$PnN*.
* ``T``: type corresponding to standardized temporal keywords.
* ``O``: type corresponding to standardized optical keywords.
* ``S``: type corresponding to *$PnE* and *$PnG* for FCS 3.0 and up.

This is a union type with two variants that represent an optical
measurement or temporal measurement respectively.

The optical tuple has a generic name parameter (``N``) because for FCS
2.0 and 3.0 this is optional. For temporal measurements the name is not
optional.

The optical tuple further requires a type (``S``) for *$PnE* (and *$PnG*
if applicable) since this may change for each version. Temporal
measurement have no scaling so this parameter is meaningless.

"""

type Measurements[
    N,
    T: AnyTemporal,
    O: AnyOptical,
    S: OpticalScale2_0 | OpticalScale3_0,
] = list[Measurement[N, T, O, S]]
"""A list of :py:type:`~pyreflow.typing.Measurement` values."""

type Measurement2_0 = Measurement[
    Shortname | None, pf.Temporal2_0, pf.Optical2_0, OpticalScale2_0
]
"""Standardized measurement keywords for FCS 2.0."""

type Measurement3_0 = Measurement[
    Shortname | None, pf.Temporal3_0, pf.Optical3_0, OpticalScale3_0
]
"""Standardized measurement keywords for FCS 3.0."""

type Measurement3_1 = Measurement[
    Shortname, pf.Temporal3_1, pf.Optical3_1, OpticalScale3_0
]
"""Standardized measurement keywords for FCS 3.1."""

type Measurement3_2 = Measurement[
    Shortname, pf.Temporal3_2, pf.Optical3_2, OpticalScale3_0
]
"""Standardized measurement keywords for FCS 3.2."""

type Measurements2_0 = Measurements[
    Shortname | None, pf.Temporal2_0, pf.Optical2_0, OpticalScale2_0
]
"""List of standardized measurements for FCS 2.0"""

type Measurements3_0 = Measurements[
    Shortname | None, pf.Temporal3_0, pf.Optical3_0, OpticalScale3_0
]
"""List of standardized measurements for FCS 3.0"""

type Measurements3_1 = Measurements[
    Shortname, pf.Temporal3_1, pf.Optical3_1, OpticalScale3_0
]
"""List of standardized measurements for FCS 3.1"""

type Measurements3_2 = Measurements[
    Shortname, pf.Temporal3_2, pf.Optical3_2, OpticalScale3_0
]
"""List of standardized measurements for FCS 3.2"""

type OpticalKeyVals[X] = list[X | tuple[()] | None]
"""All the values of an optional, optical-only keyword for a dataset.

This exists because it is convenient to return lists of keyword values
that are equal to the number of columns in a dataset.

However, temporal keywords will not have a value for these since by
definition they are optical-only. For these cases, ``()`` is returned.

For all other cases, either ``X`` is returned (which represents the
optical keyword type in question) or ``None`` if no value is assigned.

"""

type AnyDataSchema3_2 = (
    BigLittleDataSchema
    | AsciiDataSchema
    | pf.VariableUintDataSchema
    | pf.MixedDataSchema
)
"""A data schema from FCS 3.2."""


#
# Output value aliases
#

type AnalysisBytes = str | bytes

"""The contents of the *ANALYSIS* segment."""

type OtherBytes = str | bytes
"""The contents of one *OTHER* segment."""

type MeasScaleDiagnostic = (
    tuple[str, Literal["forced", "log", "trimmed", "trimmed_log"]] | None
)
"""Diagnostic output from correcting *$PnE* keywords.

This will be ``None`` if the keyword value was valid and not changed.

Otherwise, it is a pair where the first element is the original keywords
value and the second is an identifier describing what was wrong with it.

Each level is as follows:

* ``"forced"``: value was not linear (ie ``"0,0"``), was required by
  context to be linear, and was forced to be linear.

* ``"log"``: value was something like ``"<value>,0"`` which represents
  logarithmic scaling with an offset of 0 (ie total nonsense); this
  flag indicates the ``"0"`` was changed to a ``"1"``.

* ``"trimmed"``: the comma-separated pair had space after the comma that
  was trimmed away.

* ``"trimmed_log"``: a combination of ``"trimmed"`` and ``"log"``.

"""

type GateScaleDiagnostic = tuple[NEStr, Literal["log", "trimmed", "trimmed_log"]] | None
"""Diagnostic output from correcting *$GmE* keywords.

This will be ``None`` if the keyword value was valid and not changed.

Otherwise, it is a pair where the first element is the original keywords
value and the second is an identifier describing what was wrong with it.

Each level is as follows:

* ``"log"``: value was something like ``"<value>,0"`` which represents
  logarithmic scaling with an offset of 0 (ie total nonsense); this
  flag indicates the ``"0"`` was changed to a ``"1"``.

* ``"trimmed"``: the comma-separated pair had space after the comma that
  was trimmed away.

* ``"trimmed_log"``: a combination of ``"trimmed"`` and ``"log"``.

"""

type KeywordVersionScores = tuple[
    pf.KeywordVersionScore,
    pf.KeywordVersionScore,
    pf.KeywordVersionScore,
    pf.KeywordVersionScore,
]
"""The score results used to guess FCS version based on keywords.

Each element of the tuple corresponds to an FCS version (2.0, 3.0, 3.1,
and 3.2 respectively).

"""

type HeaderOffsetsName = Literal["text", "data", "header"]
"""Identifier for a segment whose offsets are given in *HEADER*."""

type SuppTextOffsetsName = Literal["supp_text"]
"""Identifier for the supplemental *TEXT* offsets."""

type TextOffsetsName = Literal["data", "header"]
"""Identifier for a segment whose offsets are given in *TEXT*. (no supp *TEXT*)"""

type HeaderOrSuppOffsetsName = HeaderOffsetsName | SuppTextOffsetsName
"""Identifiers for segments in *HEADER* or the supplemented *TEXT*."""

type NamedOffsets[N] = tuple[N, int, int]
"""Offsets for a given segment.

``N`` will encode an identifier for the offsets.

NOTE, unlike FCS files, the second offset in the pair will correspond
to the next byte after the segment and not the last byte in the segment.

"""

type HeaderNamedOffsets = NamedOffsets[HeaderOffsetsName]
"""Segment offsets from *HEADER*."""

type SuppTEXTNamedOffsets = NamedOffsets[SuppTextOffsetsName]
"""Supplemental *TEXT* offsets."""

type TextNamedOffsets = NamedOffsets[TextOffsetsName]
"""Segment offsets from *TEXT*."""

type HeaderOrSuppNamedOffsets = NamedOffsets[HeaderOrSuppOffsetsName]
"""Segment offsets from *HEADER* or the supplemented *TEXT* offsets"""


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
"""The provenance of the supplemental *TEXT* offsets.

The meaning of each level is further described in
:py:class:`~pyreflow.api.SuppTEXTOffsetsOutput` where they are tightly
coupled with other adjacent values in this class.

"""

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
"""The provenance of offsets encoded in the *TEXT* segment.

The meaning of each level is further described in
:py:class:`~pyreflow.api.TEXTOffsetsOrigin` where they are tightly
coupled with other adjacent values in this class.

"""

type FinalOffsets = tuple[int, int]
"""The offsets used to parse a segment in an FCS file.

The final offset points to the next byte after the segment (not the last
as is the case for offsets in the FCS file).

The offsets will always point to a valid region in the original FCS file and
will never be empty.

"""

type FinalOtherOffsets = tuple[list[tuple[int, FinalOffsets]], int]
"""Output from parsing *OTHER* offsets from an FCS file.

The first element corresponds to the offsets themselves. Each tuple pair in the
list encodes the offset pair index in the *HEADER* and the offset values
respectively. Empty offsets will not be returned.

The second element corresponds to the width (in bytes/ASCII characters)
used to parse the offsets.

"""

type OriginalOffsets = tuple[int, int]
"""An offset pair as literally written in an FCS file."""

type CRCOutput = bytes | str | tuple[int, int] | None
"""The result of parsing the CRC word at the end of a dataset.

Will be a tuple pair if a valid CRC was found, where the first integer
is the CRC and the second is its offset in the dataset.

Will be up to an 8-character string or bytestring if the CRC could not
be parsed to a number.

Will be ``None`` if not found at all.

For FCS 2.0 this will always be ``None``.

"""

type FlankingSegmentName = Literal["text", "stext", "data", "analysis"] | int
"""Denotes the identity of a segment on either side of a dark bytes region.

Each level is as follows:

* ``"text"``: primary *TEXT*

* ``"stext"``: supplemental *TEXT*

* ``"data"``: *DATA*

* ``"analysis"``: *ANALYSIS*

* an :py:class:`int`: an *OTHER* segment, indexed in the order they
  appear in *HEADER*


"""

type DarkBytes = str | bytes | tuple[int, int]
"""A region in an FCS file which is not part of a segment.

If a tuple, the first element is a byte and the second is the number of
times it was repeated (ie padding). This is expected to be common.

If a :py:class:str, the region is an arbitrary sequence of UTF-8
characters.

If a :py:class:bytes, the region is an arbitrary sequence of non-UTF-8
bytes.

"""

#
# Abstract classes for data schemas
#


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


# mapping for how ABC classes should be assigned to concrete classes; used for
# registration and for documentation since ABCs aren't "real" subclasses and
# therefore sphinx will miss them
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
