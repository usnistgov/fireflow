FCS files can have any number of formatting issues that makes them
"non-compliant".

By default, `fireflow` will error when it encounters any non-conforming data.
However, it has support for on-the-fly repair of many common issues, so in
practice many files can be rescued without directly editing the files
themselves.

The following is an overview of such common issues and how `fireflow` can fix
them. The `fireflow` flags specified under each issue are written in terms of
the configuration as defined in [config.rs](crates/fireflow-core/src/config.rs)
but have identical or near-identical analogues in `fireflow`'s various APIs.

# General offset issues

The offsets throughout the FCS file are often wrong. Usually, the end is one
greater than it should be (but not always). This is likely because the end
offset (somewhat confusingly) is supposed to point to the last byte rather than
the next byte (as is common in many programming languages when specifying
intervals, slices, etc)

Incorrect offsets can either be corrected, or in some cases, ignored entirely.

## Correcting bad offsets

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

To read these files, use `truncate_offsets` (for *HEADER* offsets) or
`truncate_text_offsets` (for *TEXT*) to force all ending offsets to point to the
last byte if they exceed the file's length.

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

## Negative offsets in *HEADER*

In practice, the most common case of this seems to be `0,-1` which some vendors
(quite logically) interpret to be an "empty" segment.

This stems from the fact that the ending offset refers to the last byte of the
segment, so `0,0` is actually a 1-byte segment (not empty).

Set `allow_negative` to set any negative numbers up to 0, in which the above
case to become `0,0`.

## Missing required *TEXT* offsets

In FCS 3.0 and 3.1, all *TEXT* offsets are required even if empty. Pass 
`allow_missing_required_offsets` to permit missing offsets, in which case they
will be assumed either empty or taken from *HEADER* if possible.

## Missing *$NEXTDATA*

Per the standard, *$NEXTDATA* is a required keyword. In practice, almost no FCS
file has multiple datasets, so this keyword does nothing. If *$NEXTDATA* is
missing, enable `allow_missing_nextdata` to permit this error.

# Issues with standard keys

Many files are either missing standard keys or have extra standard keys given
their indicated version. In the latter case, this often means the file is lying
about its version and is actually a later version than what it claims.

There are various solutions to this.

## Incorrect version

Use `version_override` to force the file to be read with a different version. It
will be as if this version were written in the first 6 bytes.

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

## Permitting extra keys

As a last resort, extra keys can simply be permitted.

Enable `allow_pseudostandard` to allow keys which are not part of the indicated
standard to be included.

Enable `allow_unused_standard` to allow keys which are part of the standard but
not used to be included (for example *$TIMESTEP* without a time measurement).

# Issues with standard keyword values

Even if a standard key is present, its value may not be parsable. There are a
variety of solutions to this.

Note, only standard keys can be corrected on the fly. This is because `fireflow`
provides an API for reading non-standard keys after standardization is performed
which permits infinite flexibility for the user while keeping the API simple.
These options are for cases where standardization fails due to a value being
incorrect.

## *$SPILLOVER* with indexed measurements

The *$SPILLOVER* keyword should use *$PnN* to link the rows/columns of the
matrix to measurements. In practice, some files use numbers to specify
measurement indices. Enable `parse_indexed_spillover` to interpret the
measurements as indices rather than names.

## Invalid *$DATE*, *$BTIM*, and *$ETIM*

These keywords should follow a specified pattern. Use `date_pattern` (for
*$DATE*) or `time_pattern` (for *$BTIM* and *$ETIM*) to supply a custom pattern
for parsing this field in the case of files who do not format their dates and
times correctly.

## Incorrect *$PnE* log offset value

One common error for *$PnE* is specifying `X,0.0` where `X` is non-zero. This is
incorrect because this says "log(0) = linear value of 0".

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

## Extra whitespace

Some values will contain extra whitespace around them. There are various reasons
(probably?) for this, but one common place this is observed is within the offset
keywords (*$BEGIN/ENDSTEXT*, etc) in order to make them a fixed length which in
turn makes the length of *TEXT* easier to compute. This can be a problem since
the string `"     1"` cannot be parsed as a number (technically it should be
`"000001"`).

Enable `trim_value_whitespace` to remove whitespace from the beginning and end
of all values in *TEXT*. This will likely create empty values, in which case
`allow_empty` can also be used.

## Extra whitespace in comma-separated values

Composite values which are represented as comma-separated lists (*$SPILLOVER*
for example) sometimes have whitespace between the commas. Most of these
separated values are numbers, which cannot be parsed with space around them.

Enable `trim_intra_value_whitespace` to remove this whitespace.

## Direct override

Values for standard keys can be totally overriden with
`replace_standard_key_values`. In practice, there may be other options which are
more specific to the error which are more robust. This should be used as a last
resort.

# Issues with time measurement

## Non-standard name

The time measurement should have a *$PnN* with the value `Time`. In practice,
the standard slightly loosens this restriction and says this should be matched
case-insensitively.

Some vendors use something like `T1` or `HDR-T` for time. Specify
`time_meas_pattern` with a pattern to match the *$PnN* of the time measurement
in these cases.

## Missing time

Files should include the time measurement. Enable `allow_missing_time` to permit
the time measurement to be missing.

## Non-identity *$PnG*

*$PnG* for the time measurement should not be present. In practice, a value of
`1.0` should be fine since this amounts to an identity operation.

Some files will set this to a non-unit value. Enable `ignore_time_gain` to
ignore *$PnG* for the time measurement.

## Optical keywords

The time measurement should not have any keywords which describe an optical
property (ie *$PnL*) or a detector (ie *PnV*). Use `ignore_time_optical_keys` to
ignore these keys if present.

# Issues parsing *HEADER*

## *OTHER* offsets with unspecified width

Only FCS 3.2 specifies that the *OTHER* segment offsets should be 8 bytes long.
Consequently, some vendors will use a different (often much longer) width when 
writing *OTHER* segments, presumably to break past the limit imposed by only
allowing 8 digits.

The flag `other_width` allows one to specify this width. Unfortunately it will
likely require trial-and-error and/or direct inspection of the file to figure
out what this should be.

*OTHER* segments can be ignored entirely by setting `max_other` to `0`. This
will bypass parsing of *OTHER* segments entirely.

# Issues parsing *TEXT*

## Delimiter issues

Delimiters can have multiple failure modes, some of which have a tendency to
occur together.

### Escaping

Delimiters are supposed to be "escapable" which means the delimiter can be
included in a keyword value if it doesn't appear the the begin/end and if it is
escaped (preceded by another delimiter). This precludes empty key values, which
are forbidden by the standard.

Some FCS files use literal delimiters, presumably to allow empty keyword values.
In this case, enable the flag `use_literal_delims`.

### Non-ASCII delimiters

Delimiters should be an ASCII character (value between 1 and 126). For files
which do not follow this, pass `allow_non_ascii_delim`.

### Final delimiter

The *TEXT* segment should start and end with a delimiter. If this isn't the
case, enabling `allow_missing_final_delim` can solve this.

However, this likely means that the boundaries of the *TEXT* segment are
incorrect and should be fixed by changing the offsets.

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

### Non-even word number

Keys and values come in pairs, so if the number of words in *TEXT* is odd, the
last key does not have a value. Enable `allow_odd` to permit this. The last
key will be ignored.

### Empty values

Empty keyword values are not permitted. This is implied when delimiter escaping
is used. To allow empty values, enable `allow_empty`. Keys with empty values
will be dropped.

### Non-UTF8 characters

The *TEXT* segment should be composed of UTF-8 text. Enable `allow_non_utf8` to
permit non-UTF8 characters to be encountered while parsing. Such keys and/or
values will be dropped.

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

Unless a file uses delimited ASCII (which is rare if it ever happens), the
number of events can be computed by dividing the length of *DATA* over the event
width (the sum of *$PnB* in bytes). This means *$TOT* is unnecessary for this
cases and leaves the possibility for a mismatch.

In case of such a mismatch, enable `allow_tot_mismatch` to ignore the error.

There is also the possibility that the event width does not evenly divide
*DATA*. In this case, enable `allow_uneven_event_width` to permit the file to be
read without error.
