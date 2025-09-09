Workflow
========

``pyreflow`` supports the following general workflow as shown below:

.. graphviz::

   digraph G {
       "FCS File" -> HEADER [label = "1"];
       "FCS File" -> "HEADER+TEXT\n(flat keywords)" [label = "2"];
       "FCS File" -> CoreTEXT [label = "3"];
       "FCS File" -> "All segments\n(flat keywords)" [label = "4"];
       "FCS File" -> CoreDataset [label = "5"];

       "HEADER+TEXT\n(flat keywords)" -> "All segments\n(flat keywords)" [label = "6"];
       "HEADER+TEXT\n(flat keywords)" -> CoreDataset [label = "7"];

       CoreTEXT -> CoreDataset [label = "8"];

       CoreTEXT -> "FCS File" [label = "9"];
       CoreDataset -> "FCS File" [label = "10"];

       CoreTEXT -> CoreTEXT [label = "11"];
       CoreDataset -> CoreDataset [label = "12"];

       "HEADER+TEXT\n(flat keywords)" -> "offline\nmanipulation";
       "offline\nmanipulation" -> "HEADER+TEXT\n(flat keywords)";
   }

Legend:

1. :py:func:`~pyreflow.api.fcs_read_header`
2. :py:func:`~pyreflow.api.fcs_read_raw_text`
3. :py:func:`~pyreflow.api.fcs_read_std_text`
4. :py:func:`~pyreflow.api.fcs_read_raw_dataset`
5. :py:func:`~pyreflow.api.fcs_read_std_dataset`
6. :py:func:`~pyreflow.api.fcs_read_raw_dataset_with_keywords`
7. :py:func:`~pyreflow.api.fcs_read_std_dataset_with_keywords`
8. :py:meth:`CoreTEXT*.to_dataset` (see :ref:`coretext`)
9. :py:meth:`CoreTEXT*.write_text` (see :ref:`coretext`)
10. :py:meth:`CoreDataset*.write_dataset` (see :ref:`coredataset`)
11. :py:meth:`CoreTEXT*.version_*` (see :ref:`coretext`)
12. :py:meth:`CoreDataset*.version_*` (see :ref:`coredataset`)

Raw vs standardized mode
------------------------

"Raw mode" refers to parsing an FCS file while minimally checking the keywords
for standards compliance. In this mode, *TEXT* will be kept as a flat list (ie a
dictionary in Python). Only when parsing *DATA* will a subset of keywords be
interpreted (*$DATATYPE*, *$PnB*, etc); everything else will be left as-is.

"Raw mode" is similar to how many other FCS libraries (flowCore et al) parse FCS
files.

In "standard" or "standardized mode" (abbreviated "std" in function names
above), each keyword will be parsed and stored in a class called CoreTEXT (for
*TEXT*) or CoreDataset (for all segments). These are internally validated, thus
it non-conforming keywords will trigger an error if present upon creation. These
classes themselves have an API which allows reading/writing internal elements of
an FCS file. They can also be written back to disk.

The Core* classes are explained further in :doc:`core`.

"Raw mode" has the advantage of being slightly faster, while "standard mode" has
the advantage of compliant parsing and manipulation.

.. _polars: https://docs.pola.rs/api/python/stable/reference/dataframe/index.html

Repairing Files
---------------

Many FCS files do not conform to the standards they claim to follow.
``pyreflow`` offers several ways to deal with these.

First, see the arguments for the functions in :doc:`toplevel`; most of these
are flags or other options to control parsing, alter keywords, fix offsets, etc.
These should address most needs.

For extreme cases where these flags are not enough, the recommended strategy is
to first use :func:`~pyreflow.api.fcs_read_raw_text` to get a keyword
dictionary. These can then be fixed using arbitrary python code ("offline
manipulation" above). Finally, these can be parsed again using
:func:`~pyreflow.api.fcs_read_raw_dataset_with_keywords` or
:func:`~pyreflow.api.fcs_read_std_dataset_with_keywords`.
