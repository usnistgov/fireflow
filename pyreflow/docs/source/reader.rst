Reader Functions
================

The following are function which read various components of FCS files.

Reading *HEADER*
----------------

.. autofunction:: pyreflow.api.fcs_read_header


..                  
  Reading *TEXT*
  --------------
  
  This will necessarily also read *HEADER* to get offset and version information.
  
  This comes in two flavors: raw and standardizied.
  
  Raw mode will return *TEXT* as a flat list split out by standard and non-standard keywords.
  
  Standardized mode will return a Python class that encoded the data in *TEXT* and
  allows easy access/manipulation.
  
  .. autofunction:: pyreflow.api.fcs_read_raw_text
  
  .. autofunction:: pyreflow.api.fcs_read_std_text
  
  Reading entire datasets
  -----------------------
  
  These functions are largely analogous to those in the previous section.
  
  The main difference is that these will also read *DATA*, *ANALYSIS*, and *OTHER*
  (if applicable) after reading *HEADER* and *TEXT*. *TEXT* may or may not be
  standardized depending on which function is used. In the latter case, the
  returned Python class will also include any data from *DATA*, *ANALYSIS* and
  *OTHER* segments and will provide accessors for these in addition to the
  keywords in *TEXT*.
  
  .. autofunction:: pyreflow.api.fcs_read_raw_dataset
  
  .. autofunction:: pyreflow.api.fcs_read_std_dataset
  
  Reading entire datasets from a given set of keywords
  ----------------------------------------------------
  
  Sometimes, the flags provided by :func:`pyreflow.api.fcs_read_raw_dataset` and
  :func:`pyreflow.api.fcs_read_std_dataset` are not enough to repair any issues in
  *TEXT* that might make a file unreadable.
  
  In these cases, one can read *TEXT* in raw mode using
  :func:`pyreflow.api.fcs_read_raw_text`, repair the keywords and/or offsets
  out-of-band, and then feed these into one of the following functions below.
  
  .. autofunction:: pyreflow.api.fcs_read_raw_dataset_with_keywords
  
  .. autofunction:: pyreflow.api.fcs_read_std_dataset_with_keywords
  
  
  Inputs
  ------
  
  Input types used in the functions above
  
  .. autonamedtuple:: pyreflow.api.KeyPatterns
  
Outputs
-------

These are neatly bundled tuples of data returned by each of the ``fcs_*``
functions above.

.. autoclass:: pyreflow.api.Header
   :members:

..
  .. autonamedtuple:: pyreflow.api.ReadRawTEXTOutput
  
  .. autonamedtuple:: pyreflow.api.ReadStdTEXTOutput
  
  .. autonamedtuple:: pyreflow.api.ReadRawDatasetOutput
  
  .. autonamedtuple:: pyreflow.api.ReadStdDatasetOutput
  
  .. autonamedtuple:: pyreflow.api.ReadRawDatasetFromKwsOutput
  
  .. autonamedtuple:: pyreflow.api.ReadStdDatasetFromKwsOutput
  
  
Common outputs
--------------

The following are named tuples which are reused when returning data from the
above functions.

.. autoclass:: pyreflow.api.HeaderSegments
   :members:

..
  .. autonamedtuple:: pyreflow.api.ParseData
  
  .. autonamedtuple:: pyreflow.api.StdTEXTData
  
  .. autonamedtuple:: pyreflow.api.StdDatasetData
