FCS files can have many issues that make them "non-compliant."

By default, `fireflow` will error when it encounters any non-conforming data.
However, it has support for on-the-fly repair of many common issues, so in
practice many files can be rescued without directly editing the files
themselves.

The following is an overview of such common issues and how `fireflow` can fix
them. The `fireflow` flags specified under each issue are written in terms of
the configuration as defined in [config.rs](crates/fireflow-core/src/config.rs)
but have identical or near-identical analogues in `fireflow`'s various APIs.

# Offset Issues

Offsets denote where data is located in an FCS file. Unfortunately these are
often wrong for various reasons.

Incorrect offsets can either be corrected, or in some cases, ignored entirely.

Within this section, "offsets" (plural) or "offset pair" denotes the beginning
and end offset used to describe a segment. *$NEXTDATA* also is an offset but it
only uses one value; this will be denoted as *$NEXTDATA* explicitly.

## Offset Pairs

### Overlap Correction

In practice, the first value of an offset pair is almost never wrong, and the
second value is likely to be too large. This can result in offsets that overlap
each other.

Overlaps can automatically be fixed by setting `overlap_correction_limit` to
a value greater than zero as appropriate. This limit controls how many bytes may
be subtracted from the second offset to correct the overlap. Usually `1` is
enough since most offset errors are off by one.

### "Split" Offsets in *HEADER*

This refers to offsets in the *HEADER* which set the first value to a non-zero
integer and the second to zero. This can happen if the end offset is greater
than 8 digits, which presumably means it (and the beginning offset) are stored
in *TEXT* where the 8-digit limit does not apply.

The standards specify that **both** offsets should be moved to *TEXT* in cases
like this and that the *HEADER* offsets should be set to `0,0`.

Enable `squish_offsets` to treat such offsets as `0,0`. This only applies to
the *DATA* and *ANALYSIS* offsets since *TEXT* must fit within the first
99,999,999 bytes. This should also only happen in FCS 3.0+ files.

### Pseudoempty Offsets

Empty offsets should always be written as `0,0` according to the standard.

However, the standard also says that the offsets point to the first and second
bytes of the segment, which means `0,0` is actually one byte (both at offset 0).

Consequently, some vendors write something like `0,-1` or `1000,999` to mean
'empty'. This is totally logical, and unfortunately is not what the standard
says to do.

Such offsets are called 'pseudoempty'. To allow them, set
`allow_pseudoempty`; they will be treated as empty offsets.

### Missing Required *TEXT* Offsets

In FCS 3.0 and 3.1, all *TEXT* offsets are required even if empty.

Pass `allow_missing_required_offsets` to permit missing offsets, in which
case they will be assumed either empty or taken from *HEADER* if possible.

### Duplicated Supplemental *TEXT*

Often, if supplemental *TEXT* is included, it will exactly match the offsets
from *TEXT*. 

Use `allow_duplicated_supp_text` to allow such cases (which will simply
pretend the supplemental *TEXT* offsets do not exist).

### Truncated Offset Pairs

Some files are incompletely written. In these cases, offsets will often point
beyond the last byte of the file. These files are probably screwed up and likely
should not be used.

The *DATA* offset can also point beyond the end of the file if *DATA* is the
last segment and the ending offset for *DATA* is one greater than it is supposed
to be.

In either case, set `dataset_overflow_limit` to a value greater than zero
as appropriate. This limit sets the number of bytes which will be subtracted off
the end of the *DATA* segment in order to match the length of the file.

### Manual Correction

All offsets can be overriden. For *HEADER* these options are:

* `text_correction`
* `data_correction`
* `analysis_correction`
* `other_corrections`

For *TEXT* these options are:

* `text_data_correction`
* `text_analysis_correction`
* `supp_text_correction`

`fireflow` requires that offsets in *HEADER* and *TEXT* which describe the same
segment are equal unless one is empty. This only applies to *DATA* and
*ANALYSIS*. This means that correcting these offsets usually requires editing
both the *HEADER* and *TEXT*. To control what happens on mismatch, specify
`allow_header_text_offset_mismatch`.

Offsets in *TEXT* can be ignored entirely with:
* `ignore_text_data_offsets`
* `ignore_text_analysis_offsets`
* `ignore_supp_text`

In practice, supplemental *TEXT* can probably safely be ignored as it is often
never used. One can also set `allow_missing_supp_text` to ignore these
offsets if they are missing but try to use them if they are present.

## *$NEXTDATA*

### Missing

Per the standard, *$NEXTDATA* is a required keyword. In practice, almost no FCS
file has multiple datasets, so this keyword does nothing.

If *$NEXTDATA* is missing, enable `allow_missing_nextdata` to permit this
error.

### Manual Correction

Use `nextdata_correction` to adjust the value of *$NEXTDATA*. This should only
be used for simple cases where *$NEXTDATA* is off by a small fixed amount and
one suspects there is only one dataset.

### Automatic Correction

A more flexible (but expensive) option is to use the `scan` argument which
is available on many of `fireflow`'s read methods. This will totally ignore
*$NEXTDATA* as written in the file, and instead search for the pattern
`"FCSX.Y    "` to determine the offset of the next dataset.

Some files include multiple datasets in the whole file and "hide" all but the
first by setting *$NEXTDATA* for the first dataset to `0`. For example, the file
`Millipore - easyCyte 6HT-2L - InCyte.fcs` from Flow Repository ID `FR-FCM-ZZZ4`
appears to only have one dataset according to the first dataset's *$NEXTDATA*
value (0); in actuality it has 95 (ninety-five) datasets.

This option is the easiest way to read such files.

# Standard Key Issues

Many files are either missing standard keys or have keys that appear to be
standard (ie they start with *$*) but are not part of the standard given in
*HEADER*. In the latter case, this often means the file is lying about its
version and is actually a later version than what it claims.

There are various solutions to this.

## Incorrect Version

Use `version_override` to either guess the version based on keywords or
force a different version of the user's choice.

## Ignoring or Demoting Extra Keys

In the case where extra keys are given, use `ignore_standard_keys` or
`demote_standard_keys` to "remove" these keys from the standard key list.
The former will drop these keys entirely, and the latter will remove the `$`
from the front.

## Adding Additional Keys

If a non-standard key should actually be a standard key, it can be "promoted"
using `promote_nonstandard_keys`, which will add `$` to the front of the key.

Entirely missing keys can be given with `append_standard_keywords`.

## Wrongly Named Keys

Standard keys can be renamed with `rename_standard_keys`.

## Permitting Malformed Optional Keys

Use `process_optional_failure` to control how optional keywords are handled
if they cannot be parsed.

## Permitting Extra Keys

As a last resort, extra standard keys can simply be permitted. The following
flags control how these keys are to be handled:

* `process_pseudostandard`: pertains to keys that start with *$* but are
  not in any standard
* `process_hyper_par`: pertains to measurement keys in the standard but
  outside the range of the *$PAR* keyword.
* `process_other_version`: pertains to keys from a different FCS version.
* `process_extra_timestep`: pertains to the *$TIMESTEP* keyword if it was
  not used
  
# Issues with Standard Key Values

Even if a standard key is present, its value may not be parsable. There are a
variety of solutions to this.

Only standard keys can be corrected on the fly. Non-standard keys are available
after parsing for the user to do as they wish. 

## Duplicated *$PnN*

Sometimes *$PnN* are repeated. In FCS 2.0 and 3.0, this was not explicitly
forbidden, although it does not make much sense to do. `fireflow` requires all
names to be unique, so such files will result in an error.

Use `dedup_measurement_names` to append a string to the end of such names
which will make them unique.

## Indexed *$SPILLOVER*

The *$SPILLOVER* keyword should use *$PnN* to link the rows/columns of the
matrix to measurements. In practice, some files use numbers to specify
measurement indices.

Enable `spillover_measurement_mode` to guess or specify how these
names/indices should be interpreted.

## Invalid Dates/Times

Keywords for date and time must follow a specified pattern. For instance,
*$DATE* should be formatted like `dd-mmm-yyyy`.

Use the following options to control the parse format for the indicated keys:

* `date_pattern`: *$DATE*
* `time_pattern`: *$BTIM* and *$ETIM*
* `datetime_pattern`: *$BEGINDATETIME* and *$ENDDATETIME*
* `last_modified_pattern` *$LAST_MODIFIED*

## *$PnE* and *$DATATYPE* Mismatch

Some files store floats in *DATA* but specify log-scaled *$PnE* which is not
allowed by the standard.

Use `force_linear_scale` to force the *$PnE* in such files to be linear.

## Log-Zero-Offset *$PnE*

One common error for *$PnE* is specifying `X,0.0` where `X` is non-zero. This is
incorrect because it means "log(0) = linear value of 0" (ie total nonsense).

Enable `fix_log_scale_offsets` to convert `X,0,0` to `X,1.0`.

## *$PnB* and *$BYTEORD* Mismatch

For FCS 2.0 and 3.0, *$PnB* must match *$BYTEORD*. If this isn't true, enable
`byteord_override` to force all *$PnB* to match *$BYTEORD*. For
example, a *$BYTEORD* value of `1,2,3` would result in all *$PnB* being set to
`24` (bits).

This is often seen in files which either have a byte-width other than 32 or 64
or confuse the *$PnB* with a bitmask (such as setting it to a value of `10` when
the actual numbers are 16-bit).

Alternatively, if *$PnB* are correct and *$BYTEORD* is wrong, override the
latter with `int_width_override`.

## Large *$PnR* Values

Some machines will set *$PnR* to be an absurdly huge number, presumably to mean
"infinity." In practice, `fireflow` will coerce *$PnR* to be the type of the
column. Sometimes (especially in the case of "large values") this will truncate
*$PnR*.

For a perfectly specified file, truncation should not happen, but it is probably
harmless if it does. Enable `disallow_range_truncation` to permit this
truncation.

## Invalid *$PnFEATURE*

Only `"Area"`, `"Width"`, or `"Height"` is allowed for this keyword. However,
some machines use non-optical measurements (ie imaging) which (understandably)
set this to something else.

Use `allow_other_feature` to allow such cases.

## Extra Whitespace

### Left and Right

Some values contain whitespace at the start or end of the string. There are
various reasons for this. This is commonly observed within the offset keywords
(*$BEGIN/ENDSTEXT*, etc) in order to make them a fixed length which in turn
makes the length of *TEXT* easier to compute. This is a problem since the string
`" 1"` cannot be parsed as a number (technically it should be `"001"`).

Use `trim_value_whitespace` to remove whitespace from the beginning and end
of all values in *TEXT*. This option can further specify how to treat empty
values created by trimming, which will often happen.

### In Between Commas

Composite values which are represented as comma-separated lists (ie
*$SPILLOVER*) sometimes have whitespace between the commas. Most of these
separated values are numbers, which cannot be parsed with space around them.

Use `trim_intra_value_whitespace` to remove this whitespace.

## Direct Override

Values for standard keys can be totally overriden with
`replace_standard_key_values`. In practice, there may be other options which
are more specific to the error which are more robust. 

Individual values can be edited directly using
`substitute_standard_key_values` which uses sed-like grammer to substitute
patterns in matching keys.

These options are intended as a last resort since they require manually
specifying each key and value.

# Issues with Time Measurement

## Non-Standard Name

The time measurement should have a *$PnN* with the value `Time`. The standard
slightly loosens this restriction by suggesting this should be matched
case-insensitively.

Some vendors use something like `T1` or `HDR-T` for time. Specify
`time_meas_pattern` with a pattern to match the *$PnN* of the time
measurement in these cases.

## Missing Time

Files should include the time measurement. Enable `allow_missing_time` to
permit the time measurement to be missing.

## Missing *$TIMESTEP*

Files with a time measurement should include *$TIMESTEP*. It may be missing for
various reasons.

Use `add_missing_timestep` to add *$TIMESTEP* to the file of a given value.

## Optical keywords

The time measurement should not have any keywords which describe an optical
property (ie *$PnL*) or a detector (ie *PnV*).

Use `ignore_optical_only_keys` to ignore these keys if present. Also specify
`process_optical_only_keys` to control what will happen to such keys when
found.

# *HEADER* Parsing Issues

## Ambiguous *OTHER* Offsets

Only FCS 3.2 specifies that the *OTHER* offset pairs should be 8 bytes long.
No version provides a way to indicate how many offset pairs are present. This
makes parsing these offset pairs inherently ambiguous. In practice, many vendors
will use a width other than 8 (often much longer) and may include ~10 offset
pairs.

The flag `guess_other_width` can be used to infer the width of OTHER
segments. This is not failsafe, so the flag `other_width` allows one to
specify this width manually; this will likely require prior knowledge or manual
inspection.

*OTHER* segments can be ignored entirely by setting `max_other` to `0`. This
will bypass parsing of *OTHER* segments entirely.

# *TEXT* Parsing Issues

## Delimiter Issues

Delimiters can have multiple failure modes, some of which have a tendency to
occur together.

### Odd Token Number

The number of tokens (keys or values) in *TEXT* should be even; they must be
paired as keys and values. If this isn't the case, the file might have an issue
with either delimiter escaping or the final offset. In many cases *TEXT* will
end with whitespace after the final delimiter.

Use `allow_odd_tokens` to allow *TEXT* to contain an odd number of tokens.

### Even Delimiters Number

The number of delimiters in *TEXT* must always be odd (regardless of escape
mode). If the number is even, it often means the final offset is incorrect or a
value has an unescaped delimiter.

Use `allow_even_delims` to allow *TEXT* to contain an even number of
delimiters.

### Escaping

Delimiters are supposed to be "escapable" which means the delimiter can be
included in a keyword value if it doesn't appear at the beginning/end and if it
is preceded by another delimiter (escaped). This precludes empty key values,
which are forbidden by the standard.

Some FCS files use literal delimiters, presumably to allow empty keyword values.

Use `delim_escape_mode` to either guess or manually specify how delimiters
should be escaped. Guessing is often reliable.

### Non-ASCII Delimiters

Delimiters should be an ASCII character (value between 1 and 126). For files
which do not follow this, enable `allow_non_ascii_delim`.

### Boundary Delimiters

Delimiters (even when escaped) should not appear at word boundaries as it is
ambiguous if the delimiter is part of the previous or next word.

Enable `allow_delim_at_boundary` to permit this. Such delimiters will be
discarded as they cannot be accurately interpreted.

### Primary and Supplemental Delimiter Mismatch

The delimiter for primary and supplemental *TEXT* should be the same. In
practice this isn't necessary since the delimiter can be interpreted as the
first character of the segment. Enable `allow_supp_text_own_delim` to permit
this error.

## Keyword Issues

### Non-unique

All keywords should be unique. Using `allow_nonunique` will permit
non-unique keywords to exist without triggering an error (`fireflow` will only
use the first).

Non-unique keys may also be generated when promoting, adding, or renaming standard
keys. Collision behavior is controlled with `allow_repair_non_unique`.

### Empty Keys

Empty keys are not permitted. If these are present, it likely means the
delimiter escape mode is incorrect. This is also rare. In practice it is much
more likely that values will be empty.

Use `allow_empty_keys` to ignore empty keys.

### Non-UTF8/Non-ASCII characters

The *TEXT* segment should be composed of UTF-8 text; additionally, keys must
only be ASCII.

Enable `allow_non_ascii_keys` and `allow_non_utf8_values` to permit
non-ASCII and non-UTF8 characters to be encountered while parsing keys and
values respectively.

Alternatively, use `use_encoding` to specify or guess the encoding of
*TEXT*; single-byte mode will interpret all bytes according to Latin-1 (aka
ISO/IEC 8859-1).

Technically, FCS2.0 and FCS3.0 (by default) only permit ASCII characters in
*TEXT*. `fireflow` (being written in a modern language) uses UTF-8 for all
strings by default so it does not explicitly forbid valid UTF-8 but non-ASCII
characters for 2.0/3.0.

# *DATA* Parsing Issues

## *$TOT* and *DATA* Length Mismatch

Unless a file uses delimited ASCII (very rare), the number of events can be
computed by dividing the length of *DATA* over the event width (the sum of
*$PnB* in bytes). *$TOT* is unnecessary in this case and leaves the possibility
for a mismatch.

In case of such a mismatch, enable `allow_tot_mismatch` to ignore the error.

## Event Width and *DATA* Mismatch

For non-delimited ASCII layouts, the event width (sum of *$PnB*) should evenly
divide *DATA*. If this is not the case, the offsets for *DATA* are likely wrong.

In some cases, this will likely be corrected using `overlap_correction_limit`
for the offsets themselves. If this does not fix the problem, set
`data_remainder_limit` to a number greater than zero to trim the remainder off
the end of *DATA* such that event width perfectly divides it.

This error can also be totally ignored using `allow_uneven_event_width`.

## Bitmask Truncation

By default, integer values should be truncated such that they fit within the
bitmask implied by *$PnR*.

However, some files store "extra" data in these higher bits. Truncation to
*$PnR* for integers using a bitmask can be controlled via
`over_bitmask_action`.

In contrast, values can be truncated to the literal range of *$PnR* for any
datatype (not just integers) using `{over_range_action}`. By default this is not
set to trigger an error if an overrange value is encountered since it is not
clear what *$PnR* means if the data is unmixed or compensated.

# Misc Issues

## Cyclic Redundancy Check (CRC)

### Missing

For FCS3.0 and up, the last 8 bytes of a dataset should be a CRC value. If not
given, these 8 bytes should be set to `0`.

Many files do not include these 8 bytes. Use `allow_missing_crc` to permit
this error.

### Mismatch

CRC is not computed by default since it is non-trival to compute and most files
do not include non-zero CRC values at the end of the dataset. This can be
controlled with `compute_crc`.

If the CRC is computed and a CRC value is found at the end of the dataset, they
will throw an error if they do not match. Use `allow_mismatch_crc` to permit
mismatch in this case.

## Dark Bytes

So-called "dark bytes" are bytes that are not a segment or the CRC value as
defined in the FCS standard. Some machines store/hide vendor-specific data in
these regions.

Reading "dark bytes" is disabled by default since it is expensive to perform.

Set `read_intra_segment_dark_bytes` and `read_post_dataset_dark_bytes`
to read data in between segments (former) and after the last segment (latter).
These do not trigger errors.
