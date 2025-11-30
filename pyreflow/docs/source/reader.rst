Reader Functions
================

The following are function which read various components of FCS files.

Dataset parsing
---------------

The majority of the functions in this section are intended to read *TEXT*,
possibly with accompanying data, possibly from multiple datasets in an FCS file.

These are summarized below:

.. list-table::
   :header-rows: 1

   * - Function
     - Parse Mode
     - Includes Data
     - Dataset Number
   * - :func:`~pyreflow.api.fcs_read_flat_text`
     - flat
     - no
     - singular
   * - :func:`~pyreflow.api.fcs_read_std_text`
     - standard
     - no
     - singular
   * - :func:`~pyreflow.api.fcs_read_flat_dataset`
     - flat
     - yes
     - singular
   * - :func:`~pyreflow.api.fcs_read_std_dataset`
     - standard
     - yes
     - singular
   * - :func:`~pyreflow.api.fcs_read_flat_texts`
     - flat
     - no
     - plural
   * - :func:`~pyreflow.api.fcs_read_std_texts`
     - standard
     - no
     - plural
   * - :func:`~pyreflow.api.fcs_read_flat_datasets`
     - flat
     - yes
     - plural
   * - :func:`~pyreflow.api.fcs_read_std_datasets`
     - standard
     - yes
     - plural

Each column denotes the category to which each function belongs and its intended
purpose:

*Parse Mode:*

This refers to the method used to parse *TEXT*. "Flat" mode treats *TEXT* as a
flat list of keywords and does not further processing. "Standard" mode attempts
to collect this flat list into a well-defined data structure which in `pyreflow`
is a version-specific python class (see :ref:`coretext` and :ref:`coredataset`).

"Standard" mode requires that *TEXT* first be parsed in "flat" mode, which
implies the latter is more lenient with regard to deviations from the FCS
standard.

*Includes Data:*

If "yes", the function will include *DATA*, *ANALYSIS*, and *OTHER* segments in
the returned object. Otherwise it will just include the *TEXT* segment.

*Dataset Number:*

This refers to the number of datasets in an FCS file that can be parsed by the
function. If a function is "singular", it can only parse the first dataset.
Otherwise it can parse multiple datasets from a file, and returns these in a
list rather than a single object.

The vast majority of FCS files only have one dataset, so the singular functions
are simpler to use for many cases since they do not require any flags to be set
to read one dataset.

Singular functions optionally take an ``dataset_offset`` argument which can be
used to "jump" to any dataset in a file (assuming obviously one knows where it
is).

Plural functions take ``skip`` and ``limit`` arguments. The former will skip the
first ``n`` datasets when returning the final list (although the *TEXT* for all
datasets will still be read to get *$NEXTDATA*). ``limit`` will stop the parser
after ``n`` datasets have been parsed. The defaults for these are both ``None``
which will tell the parser to exhaustively read all datasets.

*HEADER* parsing
----------------

:func:`~pyreflow.api.fcs_read_header` merely reads the first *HEADER* in an FSC
file.

There is no plural (multi-dataset) version of this function since reading
multiple datasets requires *TEXT* to be parsed to obtain *NEXTDATA*

This function also takes a ``dataset_offset`` argument, so one can theoretically
read any *HEADER* in the file if one knows its offset.

Offline keyword repair
----------------------

:func:`~pyreflow.api.fcs_read_flat_dataset_with_keywords` can be used to parse
a flat list of keyword pairs into a dataset.

Sometimes, the flags provided by :func:`~pyreflow.api.fcs_read_flat_dataset` are
not enough to repair any issues in *TEXT* that might make a file unreadable.

In these cases, one can read *TEXT* in flat mode using
:func:`~pyreflow.api.fcs_read_flat_text`, repair the keywords and/or offsets
out-of-band, and then feed these into
:func:`~pyreflow.api.fcs_read_flat_dataset_with_keywords`.

This only applies to flat mode. For the standardized analogue, see the
``from_kws`` methods in :ref:`coretext` and :ref:`coredataset`.


All functions
-------------

.. autofunction:: pyreflow.api.fcs_read_flat_text

.. autofunction:: pyreflow.api.fcs_read_std_text

.. autofunction:: pyreflow.api.fcs_read_flat_dataset

.. autofunction:: pyreflow.api.fcs_read_std_dataset

.. autofunction:: pyreflow.api.fcs_read_flat_texts

.. autofunction:: pyreflow.api.fcs_read_std_texts

.. autofunction:: pyreflow.api.fcs_read_flat_datasets

.. autofunction:: pyreflow.api.fcs_read_std_datasets

.. autofunction:: pyreflow.api.fcs_read_header

.. autofunction:: pyreflow.api.fcs_read_flat_dataset_with_keywords

Outputs
-------

These are neatly bundled classes of data returned by each of the functions
above.

.. autoclass:: pyreflow.api.Header
   :members:

.. autoclass:: pyreflow.api.RawTEXTOutput
   :members:
  
.. autoclass:: pyreflow.api.StdTEXTOutput
   :members:

.. autoclass:: pyreflow.api.RawDatasetOutput
   :members:

.. autoclass:: pyreflow.api.StdDatasetOutput
   :members:

.. autoclass:: pyreflow.api.RawDatasetWithKwsOutput
   :members:

.. autoclass:: pyreflow.api.StdDatasetWithKwsOutput
   :members:
  
  
Common outputs
--------------

These are which are reused when returning data from the above functions.

.. autoclass:: pyreflow.api.HeaderSegments
   :members:

.. autoclass:: pyreflow.api.RawTEXTParseData
   :members:

.. autoclass:: pyreflow.api.ValidKeywords
   :members:

.. autoclass:: pyreflow.api.ExtraStdKeywords
   :members:

.. autoclass:: pyreflow.api.DatasetSegments
   :members:
