[![CI](https://github.com/usnistgov/fireflow/actions/workflows/ci.yml/badge.svg)](https://github.com/usnistgov/fireflow/actions/workflows/ci.yml)
[![Documentation](https://github.com/usnistgov/fireflow/actions/workflows/Docs4NIST.yml/badge.svg)](https://github.com/usnistgov/fireflow/actions/workflows/Docs4NIST.yml)

# Fireflow: a library to read and write standards-compliant FCS files

FCS (flow cytometry standard) files are the canonical format for storing data
produced by flow cytometers.

`fireflow` aims to losslessly read and write FCS files produced from *any*
machine with the following versions:

* FCS2.0
* FCS3.0
* FCS3.1
* FCS3.2

Key features:

* Is written in Rust (reliable and fast)
* Fully implements/validates the FCS standards (see [implementation
  details](STANDARD.md))
* Can convert between FCS versions
* Can repair non-compliant FCS files while minimizing unreadable data (see
  [common issues](COMMON_ISSUES.md))
* Has API for command line, Python, R (planned)

## Status

Pre-release

## Requirements

* Rust version: 1.89+
* Operating systems: Windows, MacOS, Linux

## API support

In addition to the core Rust library, `fireflow` has several wrappers.

### Python

Tested Python versions:

* CPython 3.11
* CPython 3.12
* CPython 3.13
* CPython 3.14

The python API is documented [here](https://pages.nist.gov/fireflow/).

The Python package/module for `fireflow` is `pyreflow`.

`pyreflow` is not yet available via PyPI or other package respositories so it
must be built from source:

Install using pip/git:

``` bash
pip install git+https://github.com/usnistgov/fireflow.git#subdirectory=pyreflow
```

This will build and install the master branch into the currently active
environment.

Alternatively, install into a `conda` environment.

Example `env.yml`:

``` yaml
channels:
  - conda-forge
dependencies:
  - maturin=1.8.7
  - pip:
    - git+https://github.com/usnistgov/fireflow.git#subdirectory=pyreflow
```

### Command Line Interface

See [here](crates/fireflow-cli/README.md).

### R

Coming soon

## Development and Support

This library was developed as part of the ongoing efforts of the [NIST Flow
Cytometry Standards
Consortium](https://www.nist.gov/programs-projects/nist-flow-cytometry-standards-consortium).

Please submit code-related issues to the issue tracker in this repository.
Please send general inquiries to njd2@nist.gov.
