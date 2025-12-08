Layout Classes
==============

The following classes encode keywords that describe the byte-layout of the
*DATA* segment (*$DATATYPE*, *$BYTEORD*, *$PnB*, *$PnR*, *$PnDATATYPE*).

Only certain layouts are valid for a given FCS version, summarized below:

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

ASCII layouts
-------------

Layouts encoded using ASCII characters. Available in all FCS versions.

.. autoclass:: pyreflow.FixedAsciiLayout
   :members:

.. autoclass:: pyreflow.DelimAsciiLayout
   :members:

Ordered Numeric Layouts
-----------------------

Layouts encoded using numeric binary types (unsigned integer or float) using
any byte order. Used for FCS 2.0 and 3.0.
      
.. autoclass:: pyreflow.OrderedUint08Layout
   :members:

.. autoclass:: pyreflow.OrderedUint16Layout
   :members:

.. autoclass:: pyreflow.OrderedUint24Layout
   :members:

.. autoclass:: pyreflow.OrderedUint32Layout
   :members:

.. autoclass:: pyreflow.OrderedUint40Layout
   :members:

.. autoclass:: pyreflow.OrderedUint48Layout
   :members:

.. autoclass:: pyreflow.OrderedUint56Layout
   :members:

.. autoclass:: pyreflow.OrderedUint64Layout
   :members:

.. autoclass:: pyreflow.OrderedF32Layout
   :members:

.. autoclass:: pyreflow.OrderedF64Layout
   :members:

Endian Numeric Layouts
----------------------

Layouts encoded using numeric binary types (unsigned integer or float) using
either big or little endian. Used for FCS 3.1 and 3.2.

.. autoclass:: pyreflow.EndianF32Layout
   :members:

.. autoclass:: pyreflow.EndianF64Layout
   :members:

.. autoclass:: pyreflow.EndianUintLayout
   :members:

Mixed Layouts
-------------

Layouts which can include any type (character, float, unsigned integer). Only
for FCS 3.2.

.. autoclass:: pyreflow.MixedLayout
   :members:
