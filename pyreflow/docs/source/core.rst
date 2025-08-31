Core\* Classes
==============

Each of these classes is an in-memory, non-redundant data structure to represent
and manipulate an FCS file (hence the name ``Core*``).

These can be regarded as database-like entities in that they can be represented
by a hierarchical 'schema', maintain an internally-consistent state, and update
this state atomically.

Each structure corresponds to an FCS version, and each version takes into
account its *TEXT* keywords, data layout, and other pecularities. When
applicable, keywords are stored in a "simple" datatype (``int``, ``float``,
``tuple``, etc) corresponding to its version specification, rather than a raw
string as written in the *TEXT* of an FCS file. When applicable, these data
structures can also store *DATA* (as a polars ``DataFrame``), *ANALYSIS*, and
*OTHER* segments (as ``bytes``).

The following data is **not** explicitly stored in ``Core*``:

* *$TOT* since this implied by the length of the dataframe
* *$PAR* since this is implied by the length of the measurement vector
* any offsets, since these can be computed from the contained data itself
* *$NEXTDATA* for similar reasons as offsets
* any pseudostandard keywords since this would allow for "invalid" data

Class relationships
===================

.. list-table::
   :header-rows: 1

   * - Version
     - CoreTEXT\*
     - CoreDataset\*
     - Optical\*
     - Temporal\*
   * - FCS2.0
     - :py:class:`~pyreflow.CoreTEXT2_0`
     - :py:class:`~pyreflow.CoreDataset2_0`
     - :py:class:`~pyreflow.Optical2_0`
     - :py:class:`~pyreflow.Temporal2_0`
   * - FCS3.0
     - :py:class:`~pyreflow.CoreTEXT3_0`
     - :py:class:`~pyreflow.CoreDataset3_0`
     - :py:class:`~pyreflow.Optical3_0`
     - :py:class:`~pyreflow.Temporal3_0`
   * - FCS3.1
     - :py:class:`~pyreflow.CoreTEXT3_1`
     - :py:class:`~pyreflow.CoreDataset3_1`
     - :py:class:`~pyreflow.Optical3_1`
     - :py:class:`~pyreflow.Temporal3_1`
   * - FCS3.2
     - :py:class:`~pyreflow.CoreTEXT3_2`
     - :py:class:`~pyreflow.CoreDataset3_2`
     - :py:class:`~pyreflow.Optical3_2`
     - :py:class:`~pyreflow.Temporal3_2`


.. list-table::
   :header-rows: 1

   * - Layout
     - FCS2.0
     - FCS3.0
     - FCS3.1
     - FCS3.2
   * - :py:class:`~pyreflow.FixedAsciiLayout`
     - X
     - X
     - X
     - X
   * - :py:class:`~pyreflow.DelimAsciiLayout`
     - X
     - X
     - X
     - X
   * - :py:class:`~pyreflow.OrderedUint08Layout`
     - X
     - X
     - 
     - 
   * - :py:class:`~pyreflow.OrderedUint16Layout`
     - X
     - X
     - 
     - 
   * - :py:class:`~pyreflow.OrderedUint24Layout`
     - X
     - X
     - 
     - 
   * - :py:class:`~pyreflow.OrderedUint32Layout`
     - X
     - X
     - 
     - 
   * - :py:class:`~pyreflow.OrderedUint40Layout`
     - X
     - X
     - 
     - 
   * - :py:class:`~pyreflow.OrderedUint48Layout`
     - X
     - X
     - 
     - 
   * - :py:class:`~pyreflow.OrderedUint56Layout`
     - X
     - X
     - 
     - 
   * - :py:class:`~pyreflow.OrderedUint64Layout`
     - X
     - X
     - 
     - 
   * - :py:class:`~pyreflow.OrderedF32Layout`
     - X
     - X
     - 
     - 
   * - :py:class:`~pyreflow.OrderedF64Layout`
     - X
     - X
     - 
     - 
   * - :py:class:`~pyreflow.EndianF32Layout`
     - 
     - 
     - X
     - X
   * - :py:class:`~pyreflow.EndianF64Layout`
     - 
     - 
     - X
     - X
   * - :py:class:`~pyreflow.EndianUintLayout`
     - 
     - 
     - X
     - X
   * - :py:class:`~pyreflow.MixedLayout`
     - 
     - 
     - 
     - X

CoreTEXT\*
==========

Represents *HEADER* and *TEXT*.

One of these will be created when using :func:pyreflow.api.fcs_read_std_text
depending on the FCS version. They can also be constructed from scratch as
one would expect in Python.

In general, the following manipulations are possible:

* modifying the values of all keywords (within the constraints of the FCS
  standards)
* adding/removing measurements
* converting measurements to/from temporal and optical types
* upgrading/downgrading the FCS version
* converting to ``CoreDataset*`` by supplying a ``DataFrame`` and/or byte
  segments for *ANALYSIS* and *OTHER*.

These may also be written to an FCS file on disk, in which case the file will be
an "empty" FCS file with a single dataset and no events (ie only *HEADER* and
*TEXT*).

.. autoclass:: pyreflow.CoreTEXT2_0
   :members:

.. autoclass:: pyreflow.CoreTEXT3_0
   :members:
      
.. autoclass:: pyreflow.CoreTEXT3_1
   :members:

.. autoclass:: pyreflow.CoreTEXT3_2
   :members:

CoreDataset*
============

Represents one dataset from an FCS file (*HEADER* + *TEXT* + *DATA* +
*ANALYSIS* + *OTHER*).

One of these will be created when using the following functions:

* :func:pyreflow.api.fcs_read_std_dataset
* :func:pyreflow.api.fcs_read_std_dataset_with_keywords

In addition to all the minipulations offered by ``CoreTEXT*``, these
additionally allow:

* modifying the ``DataFrame`` corresponding to *DATA*
* modifying the byte segments corresponding to *ANALYSIS and/or *OTHER*
* removing all data by converting to ``CoreTEXT*``

When written, these will result in an FCS file a single dataset reflecting its
contents.

.. autoclass:: pyreflow.CoreDataset2_0
   :members:

.. autoclass:: pyreflow.CoreDataset3_0
   :members:

.. autoclass:: pyreflow.CoreDataset3_1
   :members:

.. autoclass:: pyreflow.CoreDataset3_2
   :members:
