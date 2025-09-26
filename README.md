[![CI](https://github.com/usnistgov/fireflow/actions/workflows/ci.yml/badge.svg)](https://github.com/usnistgov/fireflow/actions/workflows/ci.yml)
[![Documentation](https://github.com/usnistgov/fireflow/actions/workflows/Docs4NIST.yml/badge.svg)](https://github.com/usnistgov/fireflow/actions/workflows/Docs4NIST.yml)

# Fireflow: a library to read and write standards-compliant FCS files

FCS (flow cytometry standard) files are the canonical format for storing data
produced by flow cytometers.

Supported FCS versions:

* 2.0
* 3.0
* 3.1
* 3.2

Key features:

* Is written in Rust (reliable and fast)
* Implements/enforces the FCS standard (see [implementation details](STANDARD.md))
* Can convert between FCS versions
* Can repair non-compliant FCS files
* Has API for command line, Python, R (planned)

## Status

Pre-release

## Python API

The documentation is currently not hosted, so it needs to be built locally.

Run the following (assuming you have `make` installed):

```
make docs
```

Then open the resulting index file in a web browser: 
`pyreflow/docs/build/html/index.html`
