# Standard Implementation

`fireflow` aims to support the FCS standards as written as best as possible.
However, there are a few instances where this library deviates from the
standards in order to achieve practicality.

The following is a general overview of how `fireflow` adheres to the standards
and where it deviates. Users are encouraged to read the standards themselves as
this documentation assumes a general level of familiarity. References to the
standards will be given as sectional citations (ie §X.Y.Z, referring to headers)
when necessary and applicable.

By default, `fireflow` will only **read** fully-compliant files as outlined
here. `fireflow` has many ways to deal with "non-compliant" errors commonly
found in FCS files which are covered elsewhere; this is meant to provide an
overview of how `fireflow` implements the standards themselves.

`fireflow` will only **write** fully-compliant files which follow the standards
and any exceptions outlined here. Any deviation from this is a bug.

## HEADER

The first 58 bytes of *HEADER* are supported as-is. *OTHER* segments are also
supported.

FCS 3.2 is the only version that specifies that the *OTHER* segment offsets must
be 8 bytes long (§3.2). By default, `fireflow` will use 8 bytes for the width of
each *OTHER* offset when reading and writing (both are configurable).

## TEXT

All standard keywords (those that start with *$*) for each FCS version are
generally supported in `fireflow`. For many keywords this means that that key
and value will be read exactly as specified in each standard. There are some
important exceptions outlined below.

### Offsets

All offset keywords (*$NEXTDATA*, *$BEGINDATA*, *$ENDDATA*, *$BEGINSTEXT*,
*$ENDSTEXT*, *$BEGINANALYSIS*, *$ENDANALYSIS*) are restricted to be 20 digits
long. This reflects `fireflow`'s internal limitation of 64-bit offsets, which
should be more than enough for all use cases.

Furthermore, `fireflow` will write new FCS files with each of these left-padded
with zeros up to 20 characters. This makes the length of *TEXT* predictable,
which in turn allows computing the value of each offset.

### *$TR*

For all versions, `fireflow` will ensure the measurement name in this keyword
matches a *$PnN*.

### *$UNSTAINEDCENTERS*

For FCS 3.2, `fireflow` will ensure all measurement names in this keyword
match a *$PnN*.

### Timestamp keywords

`fireflow` will check that the "start time" occurs before the "end time".

For the keywords *$BTIM*, *$ETIM* and *$DATE*, this requires all three to be
present since it is possible for a run to start on one day and end at an earlier
wall clock time on a subsequent day.

For *$BEGINDATETIME* and *$ENDDATETIME* this comparison is performed directly if
both are present.

No version explicitly mandates this check, but it makes sense to do.

### Spillover/Compensation

For *$DFCnTOm* and *$COMP* `fireflow` will only accept matrices that are 2x2 or
larger despite not being written into the standard. This restriction is
specified for *$SPILLOVER* starting in FCS 3.1.

Furthermore, `fireflow` will ensure that each matrix "matches" the measurements
specified in *TEXT*. For *$DFCnTOM* and *$COMP* this means that the length/width
of the matrix should match *$PAR* and each measurement name in *$SPILLOVER* must
match a *$PnN*.

### Subset keywords

`fireflow` will parse *$CSMODE*, *$CSVBITS*, *$CSTOT*, and *$CSVnFLAG* as-is.
No other processing will be done with these despite FCS 3.0 and FCS 3.1 (see
§3.4 in both) outlining their use in parsing the *ANALYSIS* segment.

### *$PnB*

As of FCS 3.2, this keyword is "recommended" to be multiples of 8 (§3.3.38) and
anything otherwise is deprecated.

`fireflow` outright forbids anything that is not a multiple of 8 for any version
**for numeric data**. Specifically, *$PnB* must be a multiple of 8 between 8 and
64 (inclusive) for numeric data, and an integer between 1 and 20 for ASCII data
(inclusive).

In reality, non-multiples of 8 are rarely seen (if ever) and implementing
support for these would make the internal logic of `fireflow` much more complex.

Furthermore, the *$BYTEORD* keyword in FCS 2.0 and 3.0 effectively forbids
*$PnB* to be anything other than a multiple of 8 since its length is measured in
bytes rather than bits. Even when *$BYTEORD* was changed to merely describe
endianness in FCS 3.1, it is still not clear how one would interpret a binary
string that is anything other than a multiple of 8 bits long since "endianness"
refers to the order of bytes rather than bits.

### *$PnR*

In all FCS versions, this keywords is supposed to be an unsigned integer.

However, it makes practical sense to allow this number to match the type of the
measurement it describes; if *$DATATYPE* is a `F` then *$PnR* should be a 32-bit
float.

This also applies to *$GnR*.

`fireflow` implements this by parsing each *$PnR* as a decimal (ie an integer
with precision describing the decimal point, **not** a float to avoid rounding
errors). For cases where *$PnR* is needed to process data, it will be converted
to the appropriate type (and the user will be alerted if this fails). As of FCS
version 3.2, the only case where this is done is for unsigned integer
measurements (*$DATATYPE* or *$PnDATATYPE* are `I`) where *$PnR* is used to
make the bitmask that will be applied to data when read/written.

### *$PnE* and *$PnG*

As of FCS 3.2, *$PnG* shall not be used when *$PnE* is set to log scaling
(§3.3.46).

`fireflow` slightly loosens this restriction to allow *$PnG* to be `1.0` in the
case of log scaling. Since *$PnG* is simply a multiplier, setting it to `1.0`
equates to a no-op, and thus no harm should come from this while also allowing
many machines that "erroneously" set *$PnG* to `1.0`.

In all other cases, the standard is enforced as written, including the
restriction of log scaling to integer data only. Note that ASCII values are not
counted as integer values here despite being processed as 64-bit integers in
`fireflow`.

### *$PnL*, *$PnO*, and *$PnV*

These keywords are specified to be written as integers in some/all FCS versions.
However, `fireflow` will interpret these as 32-bit floats. This generally should
not be a problem unless users wish to use gigantic integers that will lose
precision if stored in a 32-bit float (which is probably inappropriate for these
keywords anyways).

Additionally, *$PnL* and *$PnV* may only use positive floats and *$PnO* may only
use non-negative floats.

Specifically, *$PnL* is supposed to be an integer in all versions (although FCS
3.1 allowed this to include multiple wavelengths), *PnO* was specified as an
integer through FCS 3.1, and *$PnV* was specified to be an integer through FCS
3.0. Note that FCS 2.0 does not list the type of these but integer is implied
via the examples given.

Everything that applies to *$PnV* above also applies to *$GnV* where applicable.

### *PnFEATURE*

`fireflow` restricts these to be one of `Area`, `Width` or `Height`. It is not
clear if FCS 3.2 (§3.3.45) allows other other values to be used; however, these
make sense when specifying measurements that are optical in nature which have a
signal vs time curve.

### *PnTYPE*

`fireflow` allows any string to be used for this keyword. It is not clear
if only the values given in FCS 3.2 (§3.3.55) are allowed. However, it can be
assumed that many other "types" of measurements not in this list might be used,
especially as new machines are developed.

### Temporal Measurements

`fireflow` makes a clear distinction between "temporal" and "optical"
measurements. "Temporal" measurements describe the channel often labeled "Time".
"Optical" describe everything else and includes actual optical measurements with
lasers, filters, and detectors but could include other types which as of FCS 3.2
are not well-described.

As of FCS 3.2, the restrictions applied to temporal measurements are:

1. shall only have linear scaling (§3.3.43)
2. shall not have *$PnG* set (§3.3.46)
3. *$PnN* shall be set to "Time" (§3.3.48)
4. if provided, *$TIMESTEP* should also be present (§3.3.64)

`fireflow` enforces (1) and (2) according to the logic outlined
[previously](#pne-and-png). (3) is enforced by default but in a
case-insensitive manner as outlined in (§3.3.64). (4) is enforced for all
versions except FCS 2.0 where *$TIMESTEP* did not exist.

Additionally, `fireflow` adds the following restrictions to temporal
measurements:

1. there can only be zero or one temporal measurement per dataset
2. the temporal measurement cannot be referenced by *$SPILLOVER*, *$TR*, or
   *$UNSTAINEDCENTERS* as each of these pertain to optical data
3. *PnTYPE* must be `Time` for FCS 3.2
4. *$TIMESTEP* must be greater than zero
5. Only *$PnN*, *$PnS*, *$PnD*, *$PnTYPE*, *$PKn*, and *PKNn* are allowed since
   all other keywords describe some aspect of an optical measurement
   
For (1), some vendors store data in 32-bit but will "hack" a 64-bit time
measurement by spreading the 64-bit values across two 32-bit measurements.
`fireflow` can read these without issue, but only one will be considered the
temporal measurement (assuming all other restrictions are met as described
above).

## DATA

The internal encoding of the *DATA* segment is controlled by *$DATATYPE*,
*$BYTEORD*, *$PnB*, *$PnR* (integers and ASCII only), *$PnDATATYPE* (FCS 3.2
only), and *$TOT* (delimited ASCII only). Only certain combinations of these
keywords are allowed and their implications are clarified here.

### Layout summary

All possible data layouts in `fireflow` (in terms of their keywords) are:

| Name              | $DATATYPE       | $BYTEORD | $PnB   | $PnR   | $PnDATATYPE | Versions |
|-------------------|-----------------|----------|--------|--------|-------------|----------|
| Ascii (delimited) | `A`             | Note 1   | `*`    | x<2^64 | n/a         | all      |
| Ascii (fixed)     | `A`             | Note 1   | 0<x<21 | x<2^64 | n/a         | all      |
| Int8 (mixed)      | `I`             | `1`      | `8`    | x<2^8  | n/a         | 2.0/3.0  |
| Int16 (mixed)     | `I`             | {1..2}   | `16`   | x<2^16 | n/a         | 2.0/3.0  |
| Int24 (mixed)     | `I`             | {1..3}   | `24`   | x<2^24 | n/a         | 2.0/3.0  |
| Int32 (mixed)     | `I`             | {1..4}   | `32`   | x<2^32 | n/a         | 2.0/3.0  |
| Int40 (mixed)     | `I`             | {1..5}   | `40`   | x<2^40 | n/a         | 2.0/3.0  |
| Int48 (mixed)     | `I`             | {1..6}   | `48`   | x<2^48 | n/a         | 2.0/3.0  |
| Int56 (mixed)     | `I`             | {1..7}   | `56`   | x<2^56 | n/a         | 2.0/3.0  |
| Int64 (mixed)     | `I`             | {1..8}   | `64`   | x<2^64 | n/a         | 2.0/3.0  |
| F32 (mixed)       | `F`             | {1..4}   | `32`   | f32    | n/a         | 2.0/3.0  |
| F64 (mixed)       | `D`             | {1..8}   | `32`   | f64    | n/a         | 2.0/3.0  |
| F32               | `F`             | endian   | `32`   | f32    | n/a         | 3.1/3.2  |
| F64               | `D`             | endian   | `64`   | f64    | n/a         | 3.1/3.2  |
| Int (any width)   | `I`             | endian   | octet  | Note 2 | n/a         | 3.1/3.2  |
| Mixed-Type        | `A`/`I`/`F`/`D` | endian   | Note 3 | Note 3 | `I`/`F`/`D` | 3.2      |

Legend:
* `X`: literal value
* {X..Y}: comma-separated integers between X and Y (inclusive) in any order
* endian: `1,2,3,4` or `4,3,2,1`
* octet: any multiple of 8 between 8 and 64 (inclusive)
* f32: any f32 value except +/- infinity and NaN
* f64: any f64 value except +/- infinity and NaN

Notes:
1. For ASCII, *$BYTEORD* does not matter. `fireflow` will still read it to
   ensure validity; beyond this its value has no impact on the layout.
2. These must be less than 2^`$PnB`.
3. Must match whatever is specified by *$DATATYPE* and *$PnDATATYPE*. *$PnR*
   must be within [1, 20] and can never be `*` if *$DATATYPE* is `A` and
   *$PnDATATYPE* is not set (the only way to specify an ASCII column in this
   layout). If the measurement is an integer, *$PnR* must be less than 2^*$PnB*.
   If measurement is a float, *$PnR* must be a valid representation of a float
   (either 32 or 64 bit depending on datatype).
   
### Fixed layouts and *$TOT*

For all but delimited ASCII, the *$TOT* keyword is overspecified since the
number of events can be computed using the length of *DATA* and the sum of all
*$PnB*. `fireflow` will check if *$TOT* matches this precomputed event number
and alert the user for a discrepancy.

For delimited ASCII, *$TOT* is necessary since there is no fixed event width.
For FCS 2.0, *$TOT* is optional, and if not given `fireflow` will simply read
until the end of *DATA*.

### Variable-width Integer layouts

One of the implications of changing *$BYTEORD* to only mean "big or little
endian" in FCS 3.1 was that it became possible to make *$PnB* different across
measurements; this is because previously *$PnB* was implied in *$BYTEORD*.

As a consequence, integer layouts can express any width from 8 to 64 bits (in
multiples of 8) starting in FCS 3.1. It wasn't clear if this was intended, but
`fireflow` nonetheless supports this layout.

In practice, many vendors still seem to use a single width for integer data
in FCS 3.1+ despite the added flexibility.

### ASCII integers

`fireflow` treats ASCII values as unsigned 64-bit integers. It isn't clear from
FCS 3.2 §3.3.14 if signed integers are allowed).

## ANALYSIS and OTHER

`fireflow` will read *ANALYSIS* and *OTHER* segments as an unprocessed
bytestream. Nothing else will be done. It is up to the user to process these
as needed.

FCS 3.0 and 3.1 provide guidance for parsing *ANALYSIS* in §3.4 but this is not
implemented in `fireflow`. Furthermore, the *$CS\** keywords are available from
`fireflow`'s *TEXT* API which may be used in this process as required.

## Additional checks

### Non-overlapping offsets

Each segment as specified in *HEADER* and/or *TEXT* should not overlap with any
other segment. Additionally, each offset must be contained within the file.

Together, these checks will flag many common offset issues (off-by-one) as well
as any truncated files that were not fully written.

## Segment Order

When writing, `fireflow` will arrange segments in the following order:

* *HEADER*
* *TEXT*
* *OTHER* (if they exist, in the order listed in *HEADER*)
* supplemental *TEXT*
* *DATA*
* *ANALYSIS*

*TEXT* must come directly after *HEADER* to make it obvious when to stop parsing
*OTHER* offsets. *OTHER* comes after *TEXT* to maximize likelihood these will
fit within the first 99,999,999 bytes. Everything else can go in any order
since they can be addressed using "large" offsets.

## Other unsupported features

* Multiple datasets, although this is planned
* cyclic redundancy check (CRC)
