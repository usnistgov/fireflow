pyreflow documentation
======================

``pyreflow`` is a library aiming to provide a standards-compliant Python API for
Flow Cytometry Standards (FCS) files. It is a wrapper around the Rust
``fireflow`` library.

It supports the following FCS versions:

* 2.0
* 3.0
* 3.1
* 3.2

Key features:

* Effectively-complete support of FCS standards.
* Reading/writing files from disk to/from memory
* API to manipulate metadata using validated classes and methods
* Support for manipulating data using `polars`_ dataframes
* Upgrading and downgrading versions
* Numerous tools for reparing non-compliant files

.. _polars: https://docs.pola.rs/api/python/stable/reference/dataframe/index.html


User Guide
----------

.. toctree::
   :maxdepth: 2

   install

API Reference
-------------

.. toctree::
   :maxdepth: 2

   fcs_read
   core
   measurement
   gating
   layout

