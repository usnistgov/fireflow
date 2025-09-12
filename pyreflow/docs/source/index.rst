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

A good analogy is that ``fireflow`` and ``pyreflow`` are like ``libyaml`` and
``pylibyaml`` respectively; their main purpose is to provide an interface for a
certain file type, and the latter is a python wrapper for the former, written in
a "fast" language.

.. _fireflow: https://github.com/usnistgov/fireflow
.. _caveats: https://github.com/usnistgov/fireflow/blob/master/STANDARD.md
.. _polars: https://docs.pola.rs/api/python/stable/reference/dataframe/index.html
.. _flowcore: https://github.com/RGLab/flowCore
.. _flowkit: https://github.com/whitews/FlowKit

User Guide
----------

.. toctree::
   :maxdepth: 2

   install
   quickstart
   workflow
   terminology

API Reference
-------------

.. toctree::
   :maxdepth: 1

   reader
   core
   measurement
   gating
   layout

