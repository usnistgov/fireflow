Quickstart
==========

Follow the :doc:`install` instructions to set up an environment. Then try these
examples to learn what `pyreflow` can do.

Read *HEADER*:

.. code-block:: python

   from pyreflow.api import fcs_read_header

   out = fcs_read_header("t_cells.fcs")

   # show version
   out.version

   # show TEXT segment offsets
   out.segments.text

Read *TEXT* (raw mode):

.. code-block:: python

   from pyreflow.api import fcs_read_raw_text

   out = fcs_read_raw_text("tea_cells.fcs")

   # standard keywords as a dict
   out.std

   # non-standard keywords as a dict
   out.nonstd

   # header segments
   out.parse.header

   # delimiter
   out.parse.delimiter

Read *TEXT* (standardized mode):

.. code-block:: python

   from pyreflow.api import fcs_read_std_text

   # "core" is a class which encodes fully standards-compliant TEXT; "uncore"
   # has data that is overspecified or non-compliant relative to "core"
   core, uncore = fcs_read_std_text("tea_sells.fcs")

   # show version
   core.version

   # show the $CYT keyword
   core.cyt

   # set the $PROJ keyword
   core.proj = "platypus immune dynamics"

   # show all $PnN keywords
   core.all_shortnames

   # set all $PnS keywords (length must match number of measurements)
   core.all_longnames = ["FSC-A", "SSC-A", "FL1-A", "FL2-A"]

   # write HEADER+TEXT to file
   core.write_text("bee_sells.fcs")

   # show $TOT
   uncore.tot

   # show "pseudostandard" keywords
   uncore.pseudostandard

Read dataset (raw mode):

.. code-block:: python

   from pyreflow.api import fcs_read_raw_dataset

   out = fcs_read_raw_dataset("tea_smells.fcs")

   # show DATA as dataframe
   out.data

   # show ANALYSIS as bytes
   out.analysis

   # show standard keywords
   out.text.std

Read dataset (standardized mode):

.. code-block:: python

   from pyreflow.api import fcs_read_std_dataset

   # "core" and "uncore" are roughly analogous to those from fcs_read_std_text
   core, uncore = fcs_read_std_dataset("tea_sales.fcs")

   # show DATA
   core.data

   # show ANALYSIS
   core.analysis

   # remove all DATA and measurements
   core.unset_data()

   # write to file
   core.write_dataset("bee_sales.fcs")

   # show pseudostandard keywords
   uncore.pseudostandard

Read non-compliant dataset:

.. code-block:: python

   from pyreflow.api import fcs_read_std_dataset

   path = "whiskey_tango_foxtrot.fcs"

   # This might be a typical example of a file which says its version is 2.0
   # but is actually 3.0 due to the presence of 3.0 keywords. The default is
   # to only accept compliant files, so this will fail:
   core, uncore = fcs_read_std_dataset(path)

   # "version_override" will force this file to be read as FCS 3.0. It
   # doesn't have $TIMESTEP so we need to add this as well using
   # "append_standard_keywords" (note the value is a string and the key does
   # not start with "$"). Finally, some keyword values have extra whitespace
   # which prevents them from being parsed. We can fix with
   # "trim_value_whitespace". Since this often leads to blank values (which
   # are not allowed) we can also ignore those using "allow_empty".
   core, uncore = fcs_read_std_dataset(
       path,
       version_override = "FCS3.0",
       append_standard_keywords = {"TIMESTEP": "1.0"},
       trim_value_whitespace = True,
       allow_empty = True,
   )

Read (extremely) non-compliant dataset:

.. code-block:: python

   from pyreflow.api import (
       fcs_read_raw_text,
       fcs_read_std_dataset_with_keywords,
   )

   path = "foobar.fcs"

   # This file is so far gone that pyreflow does not have the flags to fix it.
   # Therefore we need to correct "offline". Start by parsing TEXT in raw mode:
   out = fcs_read_raw_text(path)

   # Pull the standard keywords as a dict. These are what we need to fix.
   to_fix = out.std

   # ... do stuff with "to_fix" using raw python code

   # After fixing, continue parsing the file with corrected keywords:
   let hs = out.parse.header_segments
   better = fcs_read_std_dataset_with_keywords(
       path,
       out.version,
       to_fix,
       out.nonstd,
       hs.data,
       hs.analysis,
       hs.other,
   )

   # Celebrate by showing DATA
   better.core.data
