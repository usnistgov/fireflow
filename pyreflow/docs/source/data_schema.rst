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

Concrete Data Schema Class
++++++++++++++++++++++++++

Ordered Numeric Data
--------------------

Data schema encoded using numeric binary types (unsigned integer or float) using
any byte order. Used for FCS 2.0 and 3.0.

.. autoclass:: pyreflow.OrderedUintDataSchema
   :members:
   :show-inheritance:

.. autoclass:: pyreflow.OrderedF32DataSchema
   :members:
   :show-inheritance:

.. autoclass:: pyreflow.OrderedF64DataSchema
   :members:
   :show-inheritance:

Big/Little Numeric Data
-----------------------

Data schema encoded using numeric binary types (unsigned integer or float) using
either big or little endian. Used for FCS 3.1 and 3.2.

Note that :py:func:`~pyreflow.VariableUintDataSchema` is special in that it has
only datatype (according to the *$DATATYPE* keyword) but it is not considered a
matrix data schema (ie does not subclass
:py:func:`pyreflow.typing.MatrixDataSchema`) because its columns may be
different widths and thus may be different primitive data types (ie ``u16`` vs
``u32``).

.. autoclass:: pyreflow.BigLittleF32DataSchema
   :members:
   :show-inheritance:

.. autoclass:: pyreflow.BigLittleF64DataSchema
   :members:
   :show-inheritance:

.. autoclass:: pyreflow.SingleUintDataSchema
   :members:
   :show-inheritance:

.. autoclass:: pyreflow.VariableUintDataSchema
   :members:
   :show-inheritance:

Mixed Data Schema
-----------------

Data schema which can include any type (character, float, unsigned integer).
Only for FCS 3.2.

.. autoclass:: pyreflow.MixedDataSchema
   :members:
   :show-inheritance:

ASCII data schema
-----------------

Data schema encoded using ASCII characters. Available in all FCS versions.

.. autoclass:: pyreflow.FixedAsciiDataSchema
   :members:
   :show-inheritance:

.. autoclass:: pyreflow.DelimAsciiDataSchema
   :members:
   :show-inheritance:

Abstract Superclasses
+++++++++++++++++++++

The following classes are abstract interfaces for the concrete classes listed
above. Their primary purpose is to provide a convenient way to filter each class
by :py:func:`isinstance`.

.. autoclass:: pyreflow.typing.BigLittleDataSchema
   :members:
   :show-inheritance:

.. autoclass:: pyreflow.typing.SingleTypedDataSchema
   :members:
   :show-inheritance:

.. autoclass:: pyreflow.typing.AsciiDataSchema
   :members:
   :show-inheritance:

.. autoclass:: pyreflow.typing.MatrixDataSchema
   :members:
   :show-inheritance:

.. autoclass:: pyreflow.typing.OrderedDataSchema
   :members:
   :show-inheritance:
