FCS files can have any number of formatting issues that makes them
"non-compliant".

By default, `fireflow` will error when it counters any non-conforming data.
However, it has support for on-the-fly repair of many common issues, so in
practice many files can be rescued without directly editing the files
themselves.

The following is an overview of such common issues and how `fireflow` can fix
them. The `fireflow` flags specified under each issue are written in terms of
the configuration as defined in [config.rs](crates/fireflow-core/src/config.rs)
but have identical or near-identical analogues in `fireflow` various APIs.

# Issues with *HEADER*

## Incorrect offsets

In most cases, the end offset is one greater than it should be. This is likely
because the end offset (somewhat confusingly) is supposed to point to the last
byte rather than the next byte (as is common in many programming languages when
specifying intervals, slices, etc).

Each offset in the *HEADER* may be fixed individually using the following flags:
* `text_correction`
* `data_correction`
* `analysis_correction`
* `other_corrections`

These allow correcting both the beginning and ending offsets, but in the most
common case outlined above only the end will need to be corrected (with a `-1`).

## *OTHER* segments with unspecified offset width

Only FCS 3.2 specifies that the *OTHER* segment offsets should be 8 bytes long.
Consequently, some vendors will use a different (often much longer) width when 
writing *OTHER* segments, presumably to break past the limit imposed by only
allowing 8 digits.

The flag `other_width` allows one to specify this width. Unfortunately it will
likely require trial-and-error and/or direct inspection of the file to figure
out what this should be.

## "Split" offsets

This refers to offsets in the HEADER which set their start to a non-zero
interger and their end to zero. This can happen if the end offset is greater
than 8 digits, which presumably means it (and the beginning offset) are stored
in *TEXT* where the 8-digit limit does not apply.

The standards specify that **both** offsets should be moved to *TEXT* in cases
like this and that the *HEADER* offsets should be set to `0,0`.

Set the flag `squish_offsets` to treat such offsets as `0,0`. Note this only
applies to the *DATA* and *ANALYSIS* offsets since *TEXT* must fit within the
first 99,999,999 bytes. This should also only happen with FCS 3.0+ files.

## Negative offsets

In practice, the most common case of this seems to be `0,-1` which some people
(quite logically) interpret to be an "empty" segment.

This stems from the fact that the ending offset refers to the last byte of the
segment, so `0,0` is actually a 1-byte segment (not an empty segment).

Set `allow_negative` to round any negative numbers up to 0, which will force the
above case to become `0,0`.

## Truncated Files

Some files are incompletely written. In these cases, the offsets will often
point beyond the last byte of the files.

These files are probably screwed up in some way and likely should not be used.
If you still wish to read them, use `truncate_offsets` which will force all
ending offsets in the *HEADER* to point to the last byte if they exceed the
file's length.

# Issues with *TEXT*

## Incorrect version

Many files will incorrectly specify their version in the first 6 bytes of the
file. This is a problem, since `fireflow` will make many assumptions about the
file based on this version, and could try to parse something that does not exist
if the version is incorrect.

Most often this will be obvious because a standard keyword will be included that
belongs to a different (often newer) version.

Use `version_override` to force the file to be read with a differnt version. It
will be as if this version were written in the first 6 bytes.

## Incorrect Supplemental TEXT offsets

Some files incorrectly specify the supplemental TEXT segment. This has several
forms:
* supplemental and primary TEXT offsets are the same
* supplemental offsets are non-empty but overlap some other segment
* supplemental offsets are not given despite their keywords being required

The vast majority of files do not use/need supplemental TEXT which makes this
error particularly frustrating.

One can adjust the supplemental text using `supp_text_correction` which is
analogous to the other offset correction keywords above.

However, it just effective and likely safe to totally ignore supplemental TEXT
using `ignore_supp_text`.
