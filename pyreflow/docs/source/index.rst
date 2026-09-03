pyreflow
========

A library providing a standards-compliant Python API for Flow Cytometry
Standards (FCS) files. It is a wrapper around the Rust `fireflow`_ library.

Key Features
------------

* **FCS Standards Support**: Complete support for FCS 2.0 through FCS 3.2
  (with minor `caveats`_).
* **Read/Write Support**: Parse FCS files and write back to disk.
* **Multiple Datasets**: Read/write FCS files which have multiple datasets (ie
  .LMD and others).
* **Metadata API**: View and edit FCS keywords in an organized hierarchy (see
  :doc:`data_struct/core`).
* **High-Performance**: Faster than other FCS libraries for many operations
  (see `benchmark`_).
* **Modern DataFrames**: Manipulate *DATA* using `polars`_ dataframes.
* **Upgrade/Downgrade Support**: Convert FCS files to a different FCS version.
* **Error Correction**: Support for fixing FCS formatting errors
  (see :ref:`example <read_malformed>` and `common issues
  <https://github.com/usnistgov/fireflow/blob/master/COMMON_ISSUES.md>`_).

.. note::

   Some familiarity with the FCS standards will likely be helpful in
   understanding this libary (particularly the metadata-related API).
   

User Guide
----------

.. toctree::
   :maxdepth: 2

   intro
   install
   examples
   api

.. _fireflow: https://github.com/usnistgov/fireflow
.. _caveats: https://github.com/usnistgov/fireflow/blob/master/STANDARD.md
.. _polars: https://docs.pola.rs/api/python/stable/reference/dataframe/index.html
.. _benchmark: https://github.com/usnistgov/fireflow/blob/master/pyreflow/bench/README.md
.. _issues: 
