Welcome to pyreflow
===================

``pyreflow`` is a library aiming to provide a standards-compliant Python API for
Flow Cytometry Standards (FCS) files. It is a wrapper around the Rust
`fireflow`_ library.

It supports the following FCS versions:

* 2.0
* 3.0
* 3.1
* 3.2

Key features:

* Effectively-complete support of FCS standards (with minor `caveats`_)
* Reading/writing files from disk to/from memory
* API to manipulate metadata using validated classes and methods
* Support for manipulating data using `polars`_ dataframes
* Upgrading and downgrading versions
* Numerous tools for reparing non-compliant files

``pyreflow`` is **not** an "analysis" library. Its main goal is to parse and
manipulate FCS files. It provides no methods to transform data (compensation,
scaling), gate populations, compute statistics, etc. These are the purview of
other tools such as `flowCore`_ (R) and `flowkit`_ (Python).

To understand the intended use-cases, a good analogy is that `fireflow`_ is
analogous to other "file-format libraries" such as `libyaml`_, `libpng`_, and
`libjpeg`_. Each of these provide an interface to a given file type and are
written in a "fast" language. Their intention is to be at the bottom of a
software stack. Many of these have wrappers in "nice" languages such as Python,
and this is what ``pyreflow`` aims to be.

.. _fireflow: https://github.com/usnistgov/fireflow
.. _caveats: https://github.com/usnistgov/fireflow/blob/master/STANDARD.md
.. _polars: https://docs.pola.rs/api/python/stable/reference/dataframe/index.html
.. _flowcore: https://github.com/RGLab/flowCore
.. _flowkit: https://github.com/whitews/FlowKit
.. _libyaml: https://github.com/yaml/libyaml
.. _libpng: https://github.com/pnggroup/libpng
.. _libjpeg: https://github.com/winlibs/libjpeg

User Guide
----------

.. toctree::
   :maxdepth: 3

   install
   quickstart
   workflow
   terminology
   api
