Type Aliases
============

Configuration Flags
-------------------

These are types used to controlled how ``pyreflow`` parses FCS files.

.. autotype:: pyreflow.typing.OffsetCorrection
.. autotype:: pyreflow.typing.VersionOverride
.. autotype:: pyreflow.typing.DelimEscapeMode
.. autotype:: pyreflow.typing.KeyPattern
.. autotype:: pyreflow.typing.SubPattern
.. autotype:: pyreflow.typing.KeyPatterns
.. autotype:: pyreflow.typing.SubPatterns
.. autotype:: pyreflow.typing.KeyStringPairs
.. autotype:: pyreflow.typing.KeyStringValues
.. autotype:: pyreflow.typing.ProcessKeywordFailure
.. autotype:: pyreflow.typing.OpticalOnlyKey
.. autotype:: pyreflow.typing.ProcessOpticalOnlyKeys
.. autotype:: pyreflow.typing.TriFlag
.. autotype:: pyreflow.typing.ForceLinearScale
.. autotype:: pyreflow.typing.TrimValueWhitespace
.. autotype:: pyreflow.typing.SpilloverMeasurementMode
.. autotype:: pyreflow.typing.UseEncoding
.. autotype:: pyreflow.typing.GuessOtherWidth
.. autotype:: pyreflow.typing.AllowHeaderTextOffsetMismatch
.. autotype:: pyreflow.typing.OverLimitAction
.. autotype:: pyreflow.typing.IntWidthOverride
.. autotype:: pyreflow.typing.ByteordOverride
.. autotype:: pyreflow.typing.ComputeCRC

Keyword types
-------------

Types to refer to keywords generally.

.. autotype:: pyreflow.typing.KeyOrBytes
.. autotype:: pyreflow.typing.KeyString
.. autotype:: pyreflow.typing.StdKey
.. autotype:: pyreflow.typing.NonStdKey
.. autotype:: pyreflow.typing.StdKeywords
.. autotype:: pyreflow.typing.NonStdKeywords

Standardized keyword values
---------------------------

Types to refer to keyword values after they are standardized/parsed.

.. autotype:: pyreflow.typing.Endian
.. autotype:: pyreflow.typing.ByteOrd

.. autotype:: pyreflow.typing.ByteWidth

.. autotype:: pyreflow.typing.Range
.. autotype:: pyreflow.typing.FloatRange
.. autotype:: pyreflow.typing.IntRange
.. autotype:: pyreflow.typing.VariableBitmask
.. autotype:: pyreflow.typing.MixedRange
.. autotype:: pyreflow.typing.MaybeTypedVariableBitmask
.. autotype:: pyreflow.typing.MaybeTypedMixedRange

.. autotype:: pyreflow.typing.Shortname
.. autotype:: pyreflow.typing.Timestep
.. autotype:: pyreflow.typing.Trigger
.. autotype:: pyreflow.typing.Unicode
.. autotype:: pyreflow.typing.CsvFlags
.. autotype:: pyreflow.typing.Compensation
.. autotype:: pyreflow.typing.Spillover
.. autotype:: pyreflow.typing.UnstainedCenters
.. autotype:: pyreflow.typing.Calibration3_1
.. autotype:: pyreflow.typing.Calibration3_2
.. autotype:: pyreflow.typing.OpticalScale2_0
.. autotype:: pyreflow.typing.OpticalScale3_0
.. autotype:: pyreflow.typing.Display
.. autotype:: pyreflow.typing.Mode
.. autotype:: pyreflow.typing.Mode3_2
.. autotype:: pyreflow.typing.Originality
.. autotype:: pyreflow.typing.Feature

.. autotype:: pyreflow.typing.Datatype
.. autotype:: pyreflow.typing.AsciiType

.. autotype:: pyreflow.typing.AnyType
.. autotype:: pyreflow.typing.AnyFloatType
.. autotype:: pyreflow.typing.AnyIntegerType

.. autotype:: pyreflow.typing.AppliedGates2_0
.. autotype:: pyreflow.typing.AppliedGates3_0
.. autotype:: pyreflow.typing.AppliedGates3_2


Aggregates and Unions
---------------------

Types which represent many similar subtypes under one alias.

.. autotype:: pyreflow.typing.AnyCoreTEXT
.. autotype:: pyreflow.typing.AnyCoreDataset
.. autotype:: pyreflow.typing.AnyCore
.. autotype:: pyreflow.typing.AnyOptical
.. autotype:: pyreflow.typing.AnyTemporal
.. autotype:: pyreflow.typing.AnyMeas

.. autotype:: pyreflow.typing.Measurement
.. autotype:: pyreflow.typing.Measurements

.. autotype:: pyreflow.typing.Measurement2_0
.. autotype:: pyreflow.typing.Measurement3_0
.. autotype:: pyreflow.typing.Measurement3_1
.. autotype:: pyreflow.typing.Measurement3_2

.. autotype:: pyreflow.typing.Measurements2_0
.. autotype:: pyreflow.typing.Measurements3_0
.. autotype:: pyreflow.typing.Measurements3_1
.. autotype:: pyreflow.typing.Measurements3_2

.. autotype:: pyreflow.typing.OpticalKeyVals

.. autotype:: pyreflow.typing.AnyDataSchema3_2

Output Values
-------------

Types which are used to encode output after parsing an FCS file.

.. autotype:: pyreflow.typing.AnalysisBytes
.. autotype:: pyreflow.typing.OtherBytes
.. autotype:: pyreflow.typing.MeasScaleDiagnostic
.. autotype:: pyreflow.typing.GateScaleDiagnostic
.. autotype:: pyreflow.typing.KeywordVersionScores
.. autotype:: pyreflow.typing.HeaderOffsetsName
.. autotype:: pyreflow.typing.SuppTextOffsetsName
.. autotype:: pyreflow.typing.TextOffsetsName
.. autotype:: pyreflow.typing.HeaderOrSuppOffsetsName
.. autotype:: pyreflow.typing.NamedOffsets
.. autotype:: pyreflow.typing.HeaderNamedOffsets
.. autotype:: pyreflow.typing.SuppTEXTNamedOffsets
.. autotype:: pyreflow.typing.TextNamedOffsets
.. autotype:: pyreflow.typing.HeaderOrSuppNamedOffsets
.. autotype:: pyreflow.typing.SuppTEXTOffsetsOriginType
.. autotype:: pyreflow.typing.TEXTOffsetsOriginType
.. autotype:: pyreflow.typing.FinalOffsets
.. autotype:: pyreflow.typing.FinalOtherOffsets
.. autotype:: pyreflow.typing.OriginalOffsets
.. autotype:: pyreflow.typing.CRCOutput
.. autotype:: pyreflow.typing.FlankingSegmentName
.. autotype:: pyreflow.typing.DarkBytes

Misc Types
----------

.. autotype:: pyreflow.typing.MeasIndex
.. autotype:: pyreflow.typing.FCSVersion
.. autotype:: pyreflow.typing.NEStr
.. autotype:: pyreflow.typing.NEStrOrBytes
.. autotype:: pyreflow.typing.ReqOrOpt
.. autotype:: pyreflow.typing.RootOrMeas
