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
* Can repair non-compliant FCS files (see [common issues](COMMON_ISSUES.md))
* Has API for command line, Python, R (planned)

## Status

Pre-release

## API support

In addition to the core Rust library, `fireflow` has several wrappers.

### Python

The Python wrapper for `fireflow` is `pyreflow`.

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

The documentation is currently not hosted, so it needs to be built locally.

Run the following (assuming you have `make` installed):

```
make docs
```

Then open the resulting index file in a web browser: 
`pyreflow/docs/build/html/index.html`

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
