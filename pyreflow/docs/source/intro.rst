Introduction
============

``pyreflow`` is intended to be a file manipulation library.

A good analogy is that ``pyreflow`` is to FCS files as `pylibyaml`_ is to YAML
files. They can read/write their respective formats from/to disk and provide a
minimal API for manipulating these formats in memory. Furthermore, they are both
wrappers around compiled libraries written in a "fast" language (`fireflow`_ and
`libyaml`_ respectively).

.. important::

   ``pyreflow`` is **not** an "analysis" library; it provides no methods for
   scaling, compensation, unmixing, gating, plotting, or statistical
   calculation. It is expected that other libraries will build on top of
   ``pyreflow`` to provide these functions.

Comparison to Other FCS Libraries
---------------------------------

There are other libraries that can read and/or write FCS files. ``pyreflow``
has some important differences:

* `flowio`_ (Python): This is the most directly-comparable library to
  ``pyreflow``. It can read and write FCS files and provides and API for viewing
  their contents as-is in memory. It does not itself provide analysis-level
  functionality such as compensation or gating; this is provided by a
  higher-level wrapper (`flowkit`_).
* `flowcore`_ (R): This is like ``pyreflow`` except it has functionality for
  compensation, transformation and gating in addition to reading/writing FCS
  files.
* `fcsparser`_ (Python): This fulfills a similar purpose to both
  ``pyreflow`` and `flowio`_ except that it can only read FCS files.

Importantly, each of the above libraries is limited compared to ``pyreflow``
in the following ways:

* Limited ability to handle FCS compliance errors
* No higher-level API for manipulating FCS metadata (aside from a flat list or
  dictionary, see :ref:`flat_vs_standard`).

Workflow Overview
-----------------

``pyreflow`` supports the following general workflow as shown below:

.. graphviz::

   digraph G {
       "FCS File" -> HEADER [label = "1"];
       "FCS File" -> "HEADER+TEXT\n(flat keywords)" [label = "2"];
       "FCS File" -> CoreTEXT [label = "3"];
       "FCS File" -> "All segments\n(flat keywords)" [label = "4"];
       "FCS File" -> CoreDataset [label = "5"];

       "HEADER+TEXT\n(flat keywords)" -> "All segments\n(flat keywords)" [label = "6"];
       "HEADER+TEXT\n(flat keywords)" -> CoreTEXT [label = "7"];
       "HEADER+TEXT\n(flat keywords)" -> CoreDataset [label = "8"];

       CoreTEXT -> CoreDataset [label = "9"];

       CoreTEXT -> "FCS File" [label = "10"];
       CoreDataset -> "FCS File" [label = "11"];

       CoreTEXT -> CoreTEXT [label = "12"];
       CoreDataset -> CoreDataset [label = "13"];
       CoreDataset -> CoreTEXT [label = "14"];

       "HEADER+TEXT\n(flat keywords)" -> "offline\nmanipulation";
       "offline\nmanipulation" -> "HEADER+TEXT\n(flat keywords)";
   }

Legend:

1. :py:func:`~pyreflow.api.fcs_read_header`
2. :py:func:`~pyreflow.api.fcs_read_flat_text`
3. :py:func:`~pyreflow.api.fcs_read_std_text`
4. :py:func:`~pyreflow.api.fcs_read_flat_dataset`
5. :py:func:`~pyreflow.api.fcs_read_std_dataset`
6. :py:func:`~pyreflow.api.fcs_read_flat_dataset_with_keywords`
7. ``CoreTEXT*.from_kws()`` (see :ref:`coretext`)
8. ``CoreDataset*.from_kws()`` (see :ref:`coredataset`)
9. ``CoreTEXT*.to_dataset()`` (see :ref:`coretext`)
10. ``CoreTEXT*.write_text()`` (see :ref:`coretext`)
11. ``CoreDataset*.write_dataset()`` (see :ref:`coredataset`)
12. ``CoreTEXT*.version_*()`` (see :ref:`coretext`)
13. ``CoreDataset*.version_*()`` (see :ref:`coredataset`)
14. ``CoreDataset*.to_text()`` (see :ref:`coredataset`)

.. _flat_vs_standard:

Flat vs Standardized
++++++++++++++++++++

`Flat mode` refers to parsing *TEXT* as a flat list of key/value pairs (both
strings) with no further processing. Only when parsing *DATA* will a subset of
keywords be interpreted (*$DATATYPE*, *$PnB*, etc); everything else will be left
as-is, and no further checks for standards compliance will be performed.

`Flat mode` is similar to how other FCS libraries (`flowCore`_, `flowio`_,
`fcsparser`_) parse FCS files.

In `standard` or `standardized mode` (abbreviated `std` in function names
above), each keyword will be parsed and stored in a class called :ref:`coretext`
(for *TEXT*) or :ref:`coredataset` (for all segments) where the ``*`` indicates
FCS version. These are internally validated, thus non-conforming keywords will
trigger an error if present. These classes themselves have an API
which allows reading/writing internal elements of an FCS file. They can also be
written back to disk.

`Flat mode` has the advantage of being faster (see `benchmark`_), while
`standard mode` has the advantage of compliant parsing and type-safe data
manipulation

.. _fireflow: https://github.com/usnistgov/fireflow
.. _flowcore: https://github.com/RGLab/flowCore
.. _flowkit: https://github.com/whitews/FlowKit
.. _flowio: https://github.com/whitews/FlowIO
.. _fcsparser: https://github.com/eyurtsev/fcsparser
.. _pylibyaml: https://github.com/philsphicas/pylibyaml
.. _libyaml: https://github.com/yaml/libyaml
.. _benchmark: https://github.com/usnistgov/fireflow/blob/master/pyreflow/bench/README.md
