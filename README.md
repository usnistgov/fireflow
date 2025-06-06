## Fireflow: a library to read and write standards-compliant FCS files

FCS (flow cytometry standard) files are the canonical format for storing data
produced by flow cytometers.

Supported FCS versions:

* 2.0
* 3.0
* 3.1
* 3.2

Key features:

* Is written in Rust (reliable and fast)
* Implements/enforces the FCS standard[^1]
* Can convert between FCS versions
* Can repair non-compliant FCS files
* Has API for command line, Python, R (planned)

[^1]: see unsupported/planned features 

### Overview

#### Reading files

Generally, `fireflow` can read FCS files in two modes: "raw" and "standard".
These primarily differ in how they process the TEXT segment.

##### Raw mode

This will interpret the absolute minimum (`$BYTEORD`, `$DATATYPE`, etc) in
order to parse the file up to and including the DATA and ANALYSIS segments. The
TEXT is internally represented and returned as a flat list of key/value pairs.

This can be fast and easy way of processing FCS files, especially those that
don't follow the standard. However, this does not "check" the majority of the
keywords to ensure they follow the claimed FCS version or even if they exist. It
also makes downstream processing difficult, flat key/value lists have almost no
structure or predictibility.

##### Standard mode

In this mode, the file must follow the claimed FCS standard.

The degree to which this is enforced is configurable; at its strictest every
non-standard component will trigger an error. Some leniency can be given to
issues which are recoverable, such as badly-formatted optional keywords. Some
errors (such as misconfigured offsets) can also be fixed on the fly if known _a
priori_. It is also possible to read TEXT as a flat list, fix any errors
externally, and then parse DATA and ANALYSIS in using updated keywords.

The advantage of this approach is that keywords can be returned as hierarchy of
primitive datatypes (floats, ints, lists, etc) rather than a flat list of string
pairs. This hierarchy can then be read/manipulated using a well-defined API,
which makes downstream analyses more robust. This also allows near-trivial
conversions to different FCS versions.

#### Writing files

Unlike reading files, there is no "raw mode"; only standards-compliant files can
be written with `fireflow`.

### Quick start

TODO

#### Future additions

* `OTHER` segments
* CRC (read/write/check)
* Support for multiple datasets

#### Limitations

* `$PnB` can only be in multiples of 8 when parsing numeric data
