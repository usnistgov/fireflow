Data Schema Classes
===================

The following classes encode keywords comprising the schema for the
*DATA* segment (*$DATATYPE*, *$BYTEORD*, *$PnB*, *$PnR*, *$PnDATATYPE*).

Only certain data schema are valid for a given FCS version, summarized below:

.. list-table::
   :header-rows: 1

   * - Data Schema
     - FCS2.0
     - FCS3.0
     - FCS3.1
     - FCS3.2
   * - :py:class:`~pyreflow.OrderedUintDataSchema`
     - X
     - X
     -
     -
   * - :py:class:`~pyreflow.OrderedF32DataSchema`
     - X
     - X
     -
     -
   * - :py:class:`~pyreflow.OrderedF64DataSchema`
     - X
     - X
     -
     -
   * - :py:class:`~pyreflow.BigLittleF32DataSchema`
     -
     -
     - X
     - X
   * - :py:class:`~pyreflow.BigLittleF64DataSchema`
     -
     -
     - X
     - X
   * - :py:class:`~pyreflow.SingleUintDataSchema`
     -
     -
     - X
     - X
   * - :py:class:`~pyreflow.VariableUintDataSchema`
     -
     -
     - X
     - X
   * - :py:class:`~pyreflow.MixedDataSchema`
     -
     -
     -
     - X
   * - :py:class:`~pyreflow.FixedAsciiDataSchema`
     - X
     - X
     - X
     - X
   * - :py:class:`~pyreflow.DelimAsciiDataSchema`
     - X
     - X
     - X
     - X

Ordered Numeric Data Schema
---------------------------

Data schema encoded using numeric binary types (unsigned integer or float) using
any byte order. Used for FCS 2.0 and 3.0.

.. autoclass:: pyreflow.OrderedUintDataSchema
   :members:

.. autoclass:: pyreflow.OrderedF32DataSchema
   :members:

.. autoclass:: pyreflow.OrderedF64DataSchema
   :members:

Big/Little Numeric Data Schema
------------------------------

Data schema encoded using numeric binary types (unsigned integer or float) using
either big or little endian. Used for FCS 3.1 and 3.2.

.. autoclass:: pyreflow.BigLittleF32DataSchema
   :members:

.. autoclass:: pyreflow.BigLittleF64DataSchema
   :members:

.. autoclass:: pyreflow.SingleUintDataSchema
   :members:

.. autoclass:: pyreflow.VariableUintDataSchema
   :members:

Mixed Data Schema
-----------------

Data schema which can include any type (character, float, unsigned integer).
Only for FCS 3.2.

.. autoclass:: pyreflow.MixedDataSchema
   :members:

ASCII data schema
-----------------

Data schema encoded using ASCII characters. Available in all FCS versions.

.. autoclass:: pyreflow.FixedAsciiDataSchema
   :members:

.. autoclass:: pyreflow.DelimAsciiDataSchema
   :members:
