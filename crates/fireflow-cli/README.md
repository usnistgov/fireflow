This crate provides the command line interface for `fireflow`.

It currently only supports a subset of features provided by the full Rust
interface (and other wrappers such as Python).

Supported features:

* reading *HEADER* and *TEXT* as JSON blobs
* exporting *DATA* as a TSV stream
* raw and standard mode
* most repair flags
* dumping the spillover or compensation matrices
* dumping the measurement keywords as a table

# Installation

This is not yet available on crates.io so it must be built locally. Ensure
`cargo` is installed and run the following:

``` bash
clone https://github.com/usnistgov/fireflow
cd fireflow/crates/fireflow-cli
cargo install --path .
```

Ensure `$PATH` includes `$CARGO_HOME/bin`.

Now `fireflow` can be invoked from any shell.

# Examples

`fireflow` by default will only read an FCS file if it follows the standard
perfectly.

However, flags may be provided which can repair most deviations. Pass `--help`
for any subcommand (`header`, `raw`, `std`, etc) depicted below to see available
flags.

## Show the header

For a pristine file:

``` bash
fireflow header -i <path/to/file>
```

For a file with a *DATA* segment which is has an end offset one greater than it
should be (which will be beyond the length of the file and thus trigger an
error):

``` bash
fireflow header -i <path/to/file> --data-correction-end=-1
```

## Show the *TEXT* segment in raw mode

For a pristine file:

``` bash
fireflow raw -i <path/to/file>
```

For a file which doesn't use escaped delimiters (which also likely means some
keyword values are empty):

``` bash
fireflow raw -i <path/to/file> --use-literal-delims --allow-empty
```

If successful, this will output the keywords (split by standard and
non-standard) as written in *TEXT*, along with some miscellaneous data from
parsing (such as the delimiter).

## Show the *TEXT* segment in standardized mode

For a pristine file:

``` bash
fireflow std -i <path/to/file>
```

For a file which "lies" about its version (*HEADER* says 3.0 but should say
3.1).

``` bash
fireflow std -i <path/to/file> --version-override=3.1
```

This will output a JSON object with the keywords neatly decomposed into
primitive types and arranged hierarchically (for example, *$TR* will have two
fields for name and threshold, measurements will be arranged in an array, the
data layout will be a separate array, etc). The exact structure is
version-specific.

## Show the *DATA* segment

For now, this requires reading *TEXT* in standardized mode (in theory raw is
also possible but not supported yet).

For a pristine file:

``` bash
fireflow data -i <path/to/file>
```

For a file which has the wrong value for *$TOT*:

``` bash
fireflow data -i <path/to/file> --allow-tot-mismatch
```

This will output the *DATA* segment as a tab-separated value stream to stdout
with *$PnN* as the headers.

## Show the measurements as a table

This will print all measurement keywords as a table, where each row is a
measurement and each column is a measurement keyword (ie *$Pn\**).

``` bash
fireflow measurements -i <path/to/file>
```

## Show compensation or spillover matrix

Depending on version, this will print the value(s) for the *$DFCmTOn* keywords
(2.0), the *$COMP* keyword (3.0), or the *$SPILLOVER* keyword (3.1+).


``` bash
fireflow spillover -i <path/to/file>
```
