FCS files can have many issues that make them "non-compliant."

By default, `fireflow` will error when it encounters any non-conforming data.
However, it has support for on-the-fly repair of many common issues, so in
practice many files can be rescued without directly editing the files
themselves.

The following is an overview of such common issues and how `fireflow` can fix
them. The `fireflow` flags specified under each issue are written in terms of
the configuration as defined in [config.rs](crates/fireflow-core/src/config.rs)
but have identical or near-identical analogues in `fireflow`'s various APIs.

# TLDR

Since this can get complicated quite quickly, `fireflow` has several built-in
modes to handle these issues automatically for *most* files. The way these are
invoked is API-specific, but the names of these two modes which can be search in
the appropriate docs are:

* scalpal: parse file carefullly, trying to preserve as much as possible without
  destroying metadata
* sledgehammer: parse file with the aim of reading *DATA* and skip reading
  metadata as necessary

These two options will set many of the flags outlined below. This is probably
what most users want. It is quite fast, but the fastest performance may be
achevied by manually specifying these flags using a priori knowledge.

# Offset issues

The offsets throughout the FCS file are often wrong. Usually, the end is one
greater than it should be (but not always). This is likely because the end
offset (somewhat confusingly) is supposed to point to the last byte rather than
the next byte (as is common in many programming languages when specifying
intervals, slices, etc)

Incorrect offsets can either be corrected, or in some cases, ignored entirely.

## Overlap correction

In practice, if the offsets are wrong, it is usually the ending offset, and it
errs too large. This means segment often overlap with each if an offset is
incorrect.

Overlaps can automatically be fixed by setting `overlap_correction_limit` to
a value greater than zero as appropriate. Usually `1` is enough since most
offset errors are off by one.

## Correcting bad offsets manually

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
both the *HEADER* and *TEXT*. To allow them to mismatch, use
`allow_header_text_offset_mismatch` which will preferentially use *TEXT* offsets
upon mismatch.

Offsets in *TEXT* can be ignored entirely with:
* `ignore_text_data_offsets`
* `ignore_text_analysis_offsets`
* `ignore_supp_text`

In practice, supplemental *TEXT* can probably safely be ignored since this is
often never used. If the offsets are not in the file, one can pass
`allow_missing_supp_text` to permit this.

## Truncated offsets

Some files are incompletely written. In these cases, offsets will often point
beyond the last byte of the file. These files are probably screwed up and likely
should not be used.

The *DATA* offset can also point beyond the end of the file if *DATA* is the
last segment and the ending offset for *DATA* is one greater than it is supposed
to be.

To read these files, set `truncate_offset_limit` to a value greater than zero
as appropriate.

## "Split" offsets in *HEADER*

This refers to offsets in the *HEADER* which set their start to a non-zero
integer and their end to zero. This can happen if the end offset is greater
than 8 digits, which presumably means it (and the beginning offset) are stored
in *TEXT* where the 8-digit limit does not apply.

The standards specify that **both** offsets should be moved to *TEXT* in cases
like this and that the *HEADER* offsets should be set to `0,0`.

Enable `squish_offsets` to treat such offsets as `0,0`. Note this only applies
to the *DATA* and *ANALYSIS* offsets since *TEXT* must fit within the first
99,999,999 bytes. This should also only happen in FCS 3.0+ files.

## Pseudoempty offsets

Empty offsets should always be written as `0,0` according to the standard.

However, the standard also says that the offsets point to the first and second
bytes of the segment, which means `0,0` is actually one byte (both at offset 0).

Consequently, some vendors write something like `0,-1` or `1000,999` to mean
'empty'. This is totally logical, and unfortunately is not what the standard
says to do.

Such offsets are called 'pseudoempty'. To allow them, set `allow_pseudoempty`.

## Missing required *TEXT* offsets

In FCS 3.0 and 3.1, all *TEXT* offsets are required even if empty. Pass 
`allow_missing_required_offsets` to permit missing offsets, in which case they
will be assumed either empty or taken from *HEADER* if possible.

## Missing *$NEXTDATA*

Per the standard, *$NEXTDATA* is a required keyword. In practice, almost no FCS
file has multiple datasets, so this keyword does nothing. If *$NEXTDATA* is
missing, enable `allow_missing_nextdata` to permit this error.

## Incorrect *$NEXTDATA*

Use `nextdata_correction` to adjust the value of *$NEXTDATA*. This should only
be used for simple cases where *$NEXTDATA* is off by a small fixed amount and
one suspects there is only one dataset.

A more flexible (but expensive) option is to use the `scan_next_dataset`
argument which is available on many of `fireflow`'s read methods. This will
totally ignore *$NEXTDATA* as written in the file, and instead search for the
string pattern `"FCSX.Y    "` in the file itself to determine the offset of the
next dataset.

For some files, this is the easiest way to completely read them. One (somewhat
rare) pattern is to include multiple datasets in the whole file and "hide" all
but the first by setting *$NEXTDATA* for the first dataset to 0.

For example, the file `Millipore - easyCyte 6HT-2L - InCyte.fcs` from Flow
Repository ID `FR-FCM-ZZZ4` appears to only have one dataset according to the
first dataset's *$NEXTDATA* value (0); in actuality it has 95 (ninety-five)
datasets.

## Duplicated Supplemental *TEXT*

Often, if supplemental *TEXT* is included, it will exactly match the offsets
from *TEXT*. Use `allow_duplicated_supp_text` to allow such cases (which will
simply pretent the supplemental *TEXT* offsets do not exist).

# Issues with standard keys

Many files are either missing standard keys or have extra standard keys given
their indicated version. In the latter case, this often means the file is lying
about its version and is actually a later version than what it claims.

There are various solutions to this.

## Incorrect version

Use `version_override` to either guess the version based on keywords or force a
different version of the user's choice.

## Ignoring or demoting extra keys

In the case where extra keys are given, use `ignore_standard_keys` or
`demote_from_standard` to "remove" these keys from the standard key list. The
former will drop these keys entirely, and the latter will remove the `$` from
the front.

## Including additional keys

If a non-standard key should actually be a standard key, it can be "promoted"
using `promote_to_standard`, which will add a *$* to the front of the key.
Entirely missing keys can also be given with `append_standard_keywords`.

## Wrongly named keys

If a standard key is misnamed, this can be fixed with `rename_standard_keys`.

## Permitting optional keys that produce errors

Use `process_optional_failure` to control how optional keywords are handled if
they cannot be parsed.

## Permitting extra keys

As a last resort, extra keys can simply be permitted. The following flags
control how these keys are to be handled (dropped, throw error, warn, etc):

* `process_pseudostandard`: pertains to keys not in any standard
* `process_hyper_par`: pertains to measurement keys in the standard but outside
  the range of the *$PAR* keywrod.
* `process_other_version`: pertains to keys from a different FCS version.
* `process_extra_timestep`: pertains to the $TIMESTEP keyword if it was not used

# Issues with standard keyword values

Even if a standard key is present, its value may not be parsable. There are a
variety of solutions to this.

Note, only standard keys can be corrected on the fly. This is because `fireflow`
provides an API for reading non-standard keys after standardization is performed
which permits infinite flexibility for the user while keeping the API simple.
These options are for cases where standardization fails due to a value being
incorrect.

## Duplicated $PnN

Sometimes $PnN are repeated. In FCS 2.0 and 3.0, this was not explicitly
forbidden, although it doesn't make much sense to do. `fireflow` requires all
names to be unique, so such files will result in an error.

Use `dedup_measurement_names` to append a string to the end of such names which
will make them unique.

## *$SPILLOVER* with indexed measurements

The *$SPILLOVER* keyword should use *$PnN* to link the rows/columns of the
matrix to measurements. In practice, some files use numbers to specify
measurement indices.

Enable `spillover_measurement_mode` to guess or specify how these names/indices
should be interepreted.

## Invalid dates/times

These keywords should follow a specified pattern. Use `date_pattern` (for
*$DATE*), `time_pattern` (for *$BTIM* and *$ETIM*), `datetime_pattern` (for
*$BEGINDATETIME* and *$ENDDATETIME*), or `last_modified_pattern` (for
*$LAST_MODIFIED*) to supply a custom pattern for parsing this field in the case
of files who do not format their dates and times correctly.

## *$PnE* and *$DATATYPE* mismatch

Some files use floats but specify log-scaled *$PnE* which is not allowed by the
standard.

Use `force_linear_scale` to force the *$PnE* in such files to be linear.

## Incorrect *$PnE* log offset value

One common error for *$PnE* is specifying `X,0.0` where `X` is non-zero. This is
incorrect because it means "log(0) = linear value of 0".

Enable `fix_log_scale_offsets` to convert `X,0,0` to `X,1.0`.

## Mismatching *$PnB* and *$BYTEORD*

For FCS 2.0 and 3.0, *$PnB* must match *$BYTEORD*. If this isn't true, enable
`integer_widths_from_byteord` to force all *$PnB* to match *$BYTEORD*. For
example, a *$BYTEORD* value of `1,2,3` would result in all *$PnB* being set to
`24` (bits).

This is often seen in files which either have a byte-width other than 32 or 64
or confuse the *$PnB* with a bitmask (such as setting it to a value of `10` when
the actual numbers are 16-bit).

Alternatively, if *$PnB* are correct and *$BYTEORD* is wrong, override the
latter with `integer_byteord_override`.

## Large *$PnR* values

Some machines will set *$PnR* to be an absurdly huge number, presumably to mean
"infinity." In practice, `fireflow` will coerce *$PnR* to be the type of the
column. Sometimes (especially in the case of "large values") this will truncate
*$PnR*.

For a perfectly specified file, truncation should not happen, but it is probably
harmless if it does. Enable `disallow_range_truncation` to permit this
truncation.

Note that *$PnR* floats which are actually in integer columns will also be
truncated. This may or may not indicate an issue with the file.

## Invalid $PnFEATURE

Only `"Area"`, `"Width"`, or `"Height"` is allowed for this keyword. However,
some machines use non-optical measurements (ie imaging) which (understandably)
set this to something else.

Use `allow_other_feature` to allow such cases.

## Extra whitespace

Some values contain whitespace around them. There are various reasons for this.
This is commonly observed within the offset keywords (*$BEGIN/ENDSTEXT*, etc) in
order to make them a fixed length which in turn makes the length of *TEXT*
easier to compute. This can be a problem since the string `"  1"` cannot be
parsed as a number (technically it should be `"001"`).

Enable `trim_value_whitespace` to remove whitespace from the beginning and end
of all values in *TEXT*. This option can further specify how to treat empty
values created by trimming, which will often happen.

## Extra whitespace in comma-separated values

Composite values which are represented as comma-separated lists (*$SPILLOVER*
for example) sometimes have whitespace between the commas. Most of these
separated values are numbers, which cannot be parsed with space around them.

Enable `trim_intra_value_whitespace` to remove this whitespace.

## Direct override

Values for standard keys can be totally overriden with
`replace_standard_key_values`. In practice, there may be other options which are
more specific to the error which are more robust. This should be used as a last
resort since it requires manually specifying each key and value.

# Issues with time measurement

## Non-standard name

The time measurement should have a *$PnN* with the value `Time`. The standard
slightly loosens this restriction and says this should be matched
case-insensitively.

Some vendors use something like `T1` or `HDR-T` for time. Specify
`time_meas_pattern` with a pattern to match the *$PnN* of the time measurement
in these cases.

## Missing time

Files should include the time measurement. Enable `allow_missing_time` to permit
the time measurement to be missing.

## Missing $TIMESTEP

Files with a time measurement should include $TIMESTEP. It may be missing for
various reasons.

Use `add_missing_timestep` to add $TIMESTEP to the file of a given value.

## Non-identity *$PnG*

*$PnG* for the time measurement should not be present. In practice, a value of
`1.0` should be fine since this amounts to an identity operation.

Some files will set this to a non-unit value. Enable `ignore_time_gain` to
ignore *$PnG* for the time measurement.

## Optical keywords

The time measurement should not have any keywords which describe an optical
property (ie *$PnL*) or a detector (ie *PnV*).

Use `ignore_time_optical_keys` to ignore these keys if present. Also specify
`process_time_optical_keys` to control what will happen to such keys when found.

# Issues parsing *HEADER*

## *OTHER* offsets with unspecified width

Only FCS 3.2 specifies that the *OTHER* segment offsets should be 8 bytes long.
Consequently, some vendors will use a different (often much longer) width when 
writing *OTHER* segments, presumably to break past the limit imposed by only
allowing 8 digits.

The flag `guess_other_width` can be used to infer the width of OTHER segments.
This is not failsafe, so the flag `other_width` allows one to specify this width
manually (which will likely require prior knowledge or manual file inspection).

*OTHER* segments can be ignored entirely by setting `max_other` to `0`. This
will bypass parsing of *OTHER* segments entirely.

# Issues parsing *TEXT*

## Delimiter issues

Delimiters can have multiple failure modes, some of which have a tendency to
occur together.

### Whitespace after *TEXT*

Some vendors will add extra "padding" (usually spaces) after the last delimiter
in *TEXT* and up until the ending offset for *TEXT* indicated in *HEADER*. The
reason for this probably has to do with the fact that there is a circular
dependency between the number of digits in offset keywords in *TEXT*
(*$BEGINDATA*, *$ENDDATA*, etc) and the length of *TEXT*. Padding the end of
*TEXT* up to a certain length could eliminate this problem by making the length
of *TEXT* predictable.

Regardless of the root cause, the standard requires that *TEXT* end with a
delimiter, so this behavior is not allowed. This can be fixed with
`trim_trailing_whitespace`.

### Odd number of tokens

The number of tokens (keys and values) in TEXT should be even; they must be
paired as keys and values. If this isn't the case, the file might have an issue
with either delimiter escaping or the final offset. In many cases TEXT will end
with whitespace after the final delimiter.

Use `allow_odd_tokens` to allow TEXT to contain an odd number of tokens.

### Even number of delimiters

The number of delimiters in TEXT must always be odd (regardless of escape mode).
If the number is even, it often means the final offset is incorrect or a value
has an unescaped delimiter.

Use `allow_even_delims` to allow TEXT to contain an even number of delimiters.

### Escaping

Delimiters are supposed to be "escapable" which means the delimiter can be
included in a keyword value if it doesn't appear at the beginning/end and if it
is preceded by another delimiter (escaped). This precludes empty key values,
which are forbidden by the standard.

Some FCS files use literal delimiters, presumably to allow empty keyword values.

Use `delim_escape_mode` to either guess or manually specify how delimiters
should be escaped. Guessing is often reliable.

### Non-ASCII delimiters

Delimiters should be an ASCII character (value between 1 and 126). For files
which do not follow this, enable `allow_non_ascii_delim`.

### Boundary delimiters

Delimiters (even when escaped) should not appear at word boundaries as it is
ambiguous if the delimiter is part of the previous or next word.

Enable `allow_delim_at_boundary` to permit this. Such delimiters will be
discarded as they cannot be accurately interpreted.

### Different delimiter between primary and supplemental

The delimiter for primary and supplemental *TEXT* should be the same. In
practice this isn't necessary since the delimiter can be interpreted as the
first character of the segment. Enable `allow_supp_text_own_delim` to permit
this error.

## Issues with keywords

### Non-unique

All keywords should be unique. Enabling `allow_nonunique` will permit non-unique
keywords to exist without triggering an error (`fireflow` will only use the
first).

### Empty keys

Empty keys are not permitted. If these are present, it likely means the
delimiter escape mode is incorrect. This is also rare. In practice it is much
more likely that values will be empty.

Use `allow_empty_keys` to ignore empty keys.

### Non-UTF8/Non-ASCII characters

The *TEXT* segment should be composed of UTF-8 text; additionally, keys must
only be ASCII.

Enable `allow_non_ascii_keys` and `allow_non_utf8_values` to permit non-ASCII
and non-UTF8 characters to be encountered while parsing keys and values
respectively.

Alternatively, enable `use_latin1` to interpret each character in *TEXT* as
Latin-1 aka ISO/IEC 8859-1 if these need to be salvaged.

Technically, FCS2.0 and FCS3.0 (by default) only permit ASCII characters in
*TEXT*. `fireflow` (being written in a modern language) uses UTF-8 for all
strings by default so it does not explicitly forbid valid UTF-8 but non-ASCII
characters for 2.0/3.0.

### Non-ASCII keys

All keys (standard or not) should be composed of ASCII characters. Enable
`allow_non_ascii_keywords` to permit these. This only applies to non-standard
keywords in standardized mode, since `fireflow` will search for standard keys
using hardcoded strings which only contain ASCII.

# Issues parsing *DATA*

## Mismatch between *$TOT* and length of *DATA*

Unless a file uses delimited ASCII (which is rare if it ever happens), the
number of events can be computed by dividing the length of *DATA* over the event
width (the sum of *$PnB* in bytes). *$TOT* is unnecessary in this case and
leaves the possibility for a mismatch.

In case of such a mismatch, enable `allow_tot_mismatch` to ignore the error.

## Mismatch between event width and lenth of *DATA*

For non-delimited ASCII layouts, the event width (sum of *$PnB*) should evenly
divide *DATA*. If this is not the case, this probably means the offsets for
*DATA* are wrong.

In some cases, this will likely be corrected using `overlap_correction_limit`
for the offsets themselves. If this does not fix the problem, set
`data_remainder_limit` to a number greater than zero to trim the remainder off
the end of *DATA* such that event width perfectly divides it.

This error can also be totally ignored using `allow_uneven_event_width`.

## Range truncation

By default, integer values should be truncated such that they fit within the
bitmask implied by *$PnR*.

However, some files store "extra" data in these higher bits. Truncation to
*$PnR* can be controlled via `checked_range_datatypes` and `over_range_action`.

