Core\* Classes
==============

Each of these classes is a version-specific, in-memory, non-redundant data
structure to represent and manipulate an FCS file.

These can be regarded as database-like entities that encode a hierarchical
'schema', maintain an internally-consistent state, and update this state
atomically.

Each class corresponds to an FCS version. When applicable, keywords are stored
in a "native" datatype (:py:class:`int`, :py:class:`float`, :py:class:`tuple`,
etc) corresponding to its version specification, rather than a raw string as
written in the *TEXT* of an FCS file. CoreDataset* classes additionally store
*DATA* (as a `polars`_ ``DataFrame``), *ANALYSIS*, and *OTHER* segments (as
:py:class:`bytes`).

Each ``Core*`` class is further composed of other version-specific classes as
follows:

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

Some keywords are redundant and therefore **not** stored in any ``Core*``
classes:

* *$TOT* since this implied by the length of the dataframe
* *$PAR* since this is implied by the length of the measurement vector
* any offsets (*$(BEGIN|END)(DATA|ANALYSIS|STEXT)* and *$NEXTDATA*), since these
  can be computed by serializing the class's contents
* any pseudostandard keywords since this would allow the FCS standard to be
  violated when writing to a new file

.. _polars: https://docs.pola.rs/api/python/stable/reference/dataframe/index.html

.. _coretext:

CoreTEXT\*
----------

Represents *HEADER* and *TEXT*.

These can be created by:

* calling :func:`pyreflow.api.fcs_read_std_text`
* calling ``from_kws`` (described below)
* calling ``__new__``

In general, the following manipulations are possible:

* modifying the values of all keywords (within the constraints of the FCS
  standards)
* adding/removing measurements
* converting measurements to/from temporal and optical types
* upgrading/downgrading the FCS version
* converting to :ref:`coredataset` by supplying a ``DataFrame`` and/or byte
  segments for *ANALYSIS* and *OTHER*.
* writing to disk; the file will be an "empty" FCS file with a single dataset
  and no events (ie only *HEADER* and *TEXT*).

.. autoclass:: pyreflow.CoreTEXT2_0
   :members:

.. autoclass:: pyreflow.CoreTEXT3_0
   :members:
      
.. autoclass:: pyreflow.CoreTEXT3_1
   :members:

.. autoclass:: pyreflow.CoreTEXT3_2
   :members:

.. _coredataset:

CoreDataset*
------------

Represents one dataset from an FCS file (*HEADER* + *TEXT* + *DATA* +
*ANALYSIS* + *OTHER*).

These can be created by:

* calling :func:`pyreflow.api.fcs_read_std_dataset`
* calling :func:`pyreflow.api.fcs_read_std_dataset_with_keywords`
* calling ``from_kws`` (described below)
* calling ``__new__``

In addition to all the minipulations offered by ``CoreTEXT*``, these
additionally allow:

* modifying the ``DataFrame`` corresponding to *DATA*
* modifying the byte segments corresponding to *ANALYSIS and/or *OTHER*
* removing all data by converting to :ref:`coretext`

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
