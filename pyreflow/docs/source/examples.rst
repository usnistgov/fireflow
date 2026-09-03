Examples
========

The following are high-level toy examples to show how ``pyreflow`` can be used.

Reading FCS Files
+++++++++++++++++

Reading an FCS file can be done in stages, starting with *HEADER*, proceeding to
*TEXT*, optionally standardizing *TEXT* into a Python class, then finally
reading *DATA*, *ANALYSIS* and *OTHER* segments.

Each of the functions in these examples corresponds to those described in
:doc:`read/functions`.

Read *HEADER*
-------------

Reading *HEADER* is straightforward.

The returned object will be a :py:class:`~pyreflow.api.Header` object which
contains the version and offsets.

This will also parse the *OTHER* offsets, although they are not shown in this
example.

.. testcode:: python

   import pyreflow as pf
   from tempfile import NamedTemporaryFile

   # Create data for FCS file.
   #
   # It does not matter than the TEXT offsets point to nowhere since we
   # are only reading the first 58 bytes in this example.
   header = "FCS2.0          58     100       0       0       0       0"

   with NamedTemporaryFile(mode = "wt") as f:
       # Write file
       f.write(header)
       f.flush()

       # Read HEADER
       out = pf.api.fcs_read_header(f.name)

       # Show version as writtin in the first 6 bytes.
       assert out.version == "FCS2.0"

       # Show final offsets of the file.
       fo = out.final_offsets
       assert fo.text == (58, 101)
       assert fo.data == (0, 0)
       assert fo.analysis == (0, 0)

       # Show original offset of the file. NOTE: the end offset will be one
       # less than what is in the file if non-empty
       oo = out.original_offsets
       assert oo.text == (58, 100)
       assert oo.data == (0, 0)
       assert oo.analysis == (0, 0)

.. _read_flat_text:

Read *TEXT* (flat mode)
-----------------------

After reading *HEADER*, *TEXT* can be parsed as a flat list.

This will return a :py:class:`~pyreflow.api.FlatTEXTOutput` which will contain
the :py:class:`~pyreflow.api.Header` from the previous section, the keywords,
and diagnostic data pertaining to the parse process (delimiter used, escape mode
used, malformed keywords, etc). The keywords will be split into standard and
nonstandard dictionaries, differentiated by those which start with a dollar
sign.

.. testcode:: python

   import pyreflow as pf
   from tempfile import NamedTemporaryFile

   # Create data for FCS file
   text = "/$PAR/0/$MODE/L/$DATATYPE/I/$BYTEORD/1,2,3,4/$TOT/0/$NEXTDATA/0/"
   text_end = len(text) + 58 - 1
   header = f"FCS2.0          58{text_end:>8}       0       0       0       0"

   with NamedTemporaryFile(mode = "wt") as f:
       # Write file
       f.write(header + text)
       f.flush()

       # Read HEADER + TEXT (as dictionary)
       out = pf.api.fcs_read_flat_text(f.name)

       kws = out.keywords

       # All standard keywords should be present in a dictionary as strings
       assert kws.std["$PAR"] == "0"
       assert kws.std["$MODE"] == "L"
       assert kws.std["$DATATYPE"] == "I"
       assert kws.std["$BYTEORD"] == "1,2,3,4"
       assert kws.std["$TOT"] == "0"
       assert kws.std["$NEXTDATA"] == "0"
       assert len(kws.std) == 6

       # Nonstandard keywords are not in this file.
       assert len(kws.nonstd) == 0

       # Output includes diagnostic data pertaining to how file was parsed. We
       # can see what delimiter was used (for example)
       assert out.flat_diagnostics.primary_split.delimiter == 47

.. _read_std_text:

Read *TEXT* (standardized mode)
-------------------------------

This will parse *HEADER* and *TEXT* as done in the previous two sections, but
additionally will parse the values of the standard keywords and construct a
python class corresponding to the *TEXT* segment. Rather that dictionary values,
the data in *TEXT* will be accessible via attibutes and methods using native
Python types (ie numbers will be :py:class:`int`, dates will be
:py:class:`~datetime.date`, etc).

This will return a :py:type:`~pyreflow.typing.AnyCoreTEXT` and a
:py:class:`~pyreflow.api.StdTEXTOutput` corresponding to standardized *TEXT* and
diagnostics output from the parse process respectively. The latter will have all
the data accumulated from parsing *HEADER* and *TEXT* as described in the
previous sections (:py:class:`~pyreflow.api.Header`, malformed keywords, etc).

.. testcode:: python

   import pyreflow as pf
   from tempfile import NamedTemporaryFile

   # Create data for FCS file
   text0 = "/$PAR/1/$MODE/L/$DATATYPE/I/$BYTEORD/1,2,3,4/$TOT/0/$NEXTDATA/0/$CYT/Kerby/"
   text1 = "$P1N/Time/$P1B/32/$P1R/4294967296/"
   text = text0 + text1
   text_end = len(text) + 58 - 1
   header = f"FCS2.0          58{text_end:>8}       0       0       0       0"

   with NamedTemporaryFile(mode = "wt") as f:
       # Write file
       f.write(header + text)
       f.flush()

       # Read HEADER + TEXT (as standardized Python class).
       #
       # 'core' is a data structure corresponding to TEXT. 'uncore' is a
       # secondary data structure containing diagnostic data recorded during
       # the parse process.
       core, uncore = pf.api.fcs_read_std_text(f.name)

       # Show the version
       assert core.version == "FCS2.0"

       # Show the $CYT keyword
       assert core.cyt == "Kerby"

       # Show all $PnN keywords
       assert core.all_shortnames == ["Time"]

       # Show the data schema. In this case it should be a little-endian 32-bit
       # integer schema.
       ds = core.data_schema
       assert isinstance(ds, pf.OrderedUintDataSchema)
       assert ds.byte_width == 4
       assert ds.byteord == "little"
       # NOTE: This is one less than what is in the file.
       assert ds.ranges == [4294967295] 

       # Show $TOT. 
       assert uncore.tot == 0


Read entire dataset (flat mode)
-------------------------------

This will read *HEADER* + *TEXT* as described in :ref:`read_flat_text` and
additionally parse *DATA*, *ANALYSIS*, and *OTHER* segments. Only the keywords
needed to parse these additional segments will be parsed and interpreted. This
will return a :py:class:`~pyreflow.api.FlatDatasetOutput` which also contains
:py:class:`~pyreflow.api.FlatTEXTOutput` from the previous sections.

.. testcode:: python

   import pyreflow as pf
   from tempfile import NamedTemporaryFile

   # Create data for FCS file
   text0 = b"/$PAR/1/$MODE/L/$DATATYPE/I/$BYTEORD/1,2,3,4/$TOT/3/$NEXTDATA/0/$CYT/Kerby/"
   text1 = b"$P1N/Time/$P1B/32/$P1R/4294967296/"
   text = text0 + text1
   data = b"\x00\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00" # 0,1,2 as LE u32
   text_end = len(text) + 58 - 1
   data_begin = text_end + 1
   data_end = data_begin + len(data) - 1
   header = f"FCS2.0          58{text_end:>8}{data_begin:>8}{data_end:>8}       0       0"

   with NamedTemporaryFile(mode = "wb") as f:
       # Write file
       f.write(header.encode() + text + data)
       f.flush()

       # Read HEADER + TEXT + DATA (no standardization).
       out = pf.api.fcs_read_flat_dataset(f.name)

       ds = out.dataset

       # Show DATA as a polars dataframe
       assert ds.data.columns == ["X0"] # NOTE: columns won't be named after $PnN
       assert ds.data.get_column("X0").to_list() == [0, 1, 2]

       # Show ANALYSIS (blank for this file)
       assert ds.analysis == ""

       # Show OTHER segments (none for this file)
       assert ds.others == []


Read entire dataset (standardized mode)
---------------------------------------

This is the standardized analogue to :ref:`read_flat_text`.

Before reading, *DATA*, *ANALYSIS*, and *OTHER* segments, the *TEXT* segment
will also be standardized as described in :ref:`read_std_text`. Will return a
:py:type:`pyreflow.typing.AnyCoreDataset` and
:py:class:`~pyreflow.api.StdDatasetOutput` which have analogous meanings.

.. testcode:: python

   import pyreflow as pf
   from tempfile import NamedTemporaryFile

   # Create data for FCS file
   text0 = b"/$PAR/1/$MODE/L/$DATATYPE/I/$BYTEORD/1,2,3,4/$TOT/3/$NEXTDATA/0/$CYT/Kerby/"
   text1 = b"$P1N/Time/$P1B/32/$P1R/4294967296/"
   text = text0 + text1
   data = b"\x00\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00" # 0,1,2 as LE u32
   text_end = len(text) + 58 - 1
   data_begin = text_end + 1
   data_end = data_begin + len(data) - 1
   header = f"FCS2.0          58{text_end:>8}{data_begin:>8}{data_end:>8}       0       0"

   with NamedTemporaryFile(mode = "wb") as f:
       # Write file
       f.write(header.encode() + text + data)
       f.flush()

       # Read HEADER + TEXT + DATA (with standardization).
       #
       # 'core' is a data structure corresponding to TEXT + DATA. 'uncore' is a
       # secondary data structure containing diagnostic data recorded during
       # the parse process.
       core, uncore = pf.api.fcs_read_std_dataset(f.name)

       # Show DATA as a polars dataframe
       assert core.data.columns == ["Time"]
       assert core.data.get_column("Time").to_list() == [0, 1, 2]

       # Show ANALYSIS (blank for this file)
       assert core.analysis == ""

       # Show OTHER segments (none for this file)
       assert core.others == []

Read malformed FCS file
-----------------------

Thus far we have only tried to parse files which are perfectly valid FCS files.

In reality, most FCS files are not perfect and will need to be repaired.

``pyreflow`` will only read perfect files by default. However, it has the
capability to repair non-standard FCS files on-the-fly if configured properly.
The easiest way to do this is to use the
:py:class:`~pyreflow.pydantic.PyreflowReadStdDatasetConfig` class (part of
`pyreflow`'s `Pydantic
<https://pydantic.dev/docs/validation/latest/get-started/>`__ interface) and
call the
:py:func:`~pyreflow.pydantic.PyreflowReadStdDatasetConfig.new_scalpel()`
method followed by a call to the desired reader method (in this case
:py:func:`~pyreflow.pydantic.PyreflowReadStdDatasetConfig.read_std_dataset()`).
This will configure ``pyreflow`` to use a set of heuristics that are likely to
work on most files and preserve as much metadata as possible. **Most users will
likely want this.** This can also be set to ``strategy="sledgehammer"`` to
prioritize reading *DATA* at the expense of metadata. Usually this is not
necessary.

The details of how ``pyreflow`` repairs FCS files are extremely complex. Without
using the ``strategy`` argument, all heuristics and repair flags need to be
manually specified, which will be tedious for many users and will become
annoying when parsing many files in bulk. Those wishing to know more should
consult the full argument list for :py:func:`~pyreflow.api.fcs_read_std_dataset`
as well as :doc:`_generated/strategies` which explains how each strategy sets
these arguments. See also `common issues
<https://github.com/usnistgov/fireflow/blob/master/COMMON_ISSUES.md>`__ for how
these flags apply to a given FCS error modality.

.. testcode:: python

   import pyreflow as pf
   from tempfile import NamedTemporaryFile

   # Create data for FCS file
   #
   # This file has two errors:
   # - $BYTEORD should be "1,2,3,4"
   # - $TOT should not be repeated
   text0 = b"/$PAR/1/$MODE/L/$DATATYPE/I/$BYTEORD/1,2,3/$TOT/3/$TOT/4/$NEXTDATA/0/$CYT/Kerby/"
   text1 = b"$P1N/Time/$P1B/32/$P1R/4294967296/"
   text = text0 + text1
   data = b"\x00\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00" # 0,1,2 as LE u32
   text_end = len(text) + 58 - 1
   data_begin = text_end + 1
   data_end = data_begin + len(data) - 1
   header = f"FCS2.0          58{text_end:>8}{data_begin:>8}{data_end:>8}       0       0"

   with NamedTemporaryFile(mode = "wb") as f:
       # Write file
       f.write(header.encode() + text + data)
       f.flush()

       # Read dataset and fix it on-the-fly
       conf = pf.pydantic.PyreflowReadStdDatasetConfig.new_scalpel()
       core, uncore = conf.read_std_dataset(f.name)

       # Show DATA (it is intact)
       assert core.data.columns == ["Time"]
       assert core.data.get_column("Time").to_list() == [0, 1, 2]

       # The extra $TOT keyword was removed and recorded via diagnostic output.
       #
       # Extra keywords are simply dropped to create a valid file.
       assert uncore.flat_diagnostics.non_unique_std_keywords == [("$TOT", "4")]

       # The original $BYTEORD was also corrected and recorded.
       #
       # For this file, the order was interpreted as little endian and then
       # proper length was inferred from $PnB (32)
       assert uncore.dataset.std_diagnostics.schema_diagnostics.original_byteord == [1, 2, 3]

Writing FCS Files
+++++++++++++++++

Writing FCS files with ``pyreflow`` is much simpler than reading. It is only
possible to write a standards-compliant FCS file with ``pyreflow``. The API for
reading is complex due to the number of options available for repairing FCS
errors; these are not necessary for writing due.

.. testcode:: python

   import pyreflow as pf
   import polars as pl
   from tempfile import NamedTemporaryFile

   # DATA will be one column with three rows. The name of the column need not
   # match the $PnN defined below.
   data = pl.DataFrame([1, 2, 3], schema = {"X1": pl.UInt32})

   # Add one measurement, in this case for time. The first part of the tuple is
   # the $PnN ("Time") and the second is the object representing the other
   # keywords. We can also set $PnS (longname) and other keywords from here.
   meas = [("Time", pf.Temporal3_1(timestep = 1.0, longname = "Run Time"))]

   # Make the data schema, in this case an integer layout. 32-bit little endian
   # is the default. This needs to match the schema in the dataframe defined
   # above.
   schema = pf.SingleUintDataSchema(ranges = [4294967295])

   # Combine the above into a a complete dataset. We can also set
   # non-measurement keywords here (in this case $CYT and $INST).
   core = pf.CoreDataset3_1(meas, schema, data, cyt = "Kerby", inst = "Cyberdyne")

   # We can now write this object to a file. If we read it back, it should be
   # the same as the original. We don't need anything special for reading since
   # the file on disk should be standards-compliant.
   with NamedTemporaryFile(mode = "wb") as f:
       core.write_dataset(f.name)
       core0, _ = pf.api.fcs_read_std_dataset(f.name)
       assert core == core0

   # We can also only write HEADER and TEXT. Just like above, we should be
   # able to read this back and equate it with the original.
   with NamedTemporaryFile(mode = "wb") as f:
       core.write_text(f.name)
       core0, _ = pf.api.fcs_read_std_text(f.name)
       assert core.to_text() == core0

Repairing FCS Files
+++++++++++++++++++

Reading and writing from the examples above can be combined to "repair" a file.

.. testcode:: python

   import pyreflow as pf
   from tempfile import NamedTemporaryFile

   # Create data for FCS file
   #
   # This file has two errors:
   # - $BYTEORD should be "1,2,3,4"
   # - $TOT should not be repeated
   text0 = b"/$PAR/1/$MODE/L/$DATATYPE/I/$BYTEORD/1,2,3/$TOT/3/$TOT/4/$NEXTDATA/0/$CYT/Kerby/"
   text1 = b"$P1N/Time/$P1B/32/$P1R/4294967296/"
   text = text0 + text1
   data = b"\x00\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00" # 0,1,2 as LE u32
   text_end = len(text) + 58 - 1
   data_begin = text_end + 1
   data_end = data_begin + len(data) - 1
   header = f"FCS2.0          58{text_end:>8}{data_begin:>8}{data_end:>8}       0       0"

   with NamedTemporaryFile(mode = "wb") as f:
       # Write file
       f.write(header.encode() + text + data)
       f.flush()

       # Read dataset and fix it on-the-fly
       conf = pf.pydantic.PyreflowReadStdDatasetConfig.new_scalpel()
       core, _ = conf.read_std_dataset(f.name)

       # Save file back to disk.
       core.write_dataset(f.name)

       # We can now read back this file without any special configuration since
       # it is perfectly compliant. It should be the same as what we just wrote.
       core1, _ = pf.api.fcs_read_std_dataset(f.name)

       assert core == core1

