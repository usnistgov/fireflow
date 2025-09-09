These are terms used throughout `fireflow`'s code and documentation. Some of
these are derived from the standards themselves and some are unique to this
library.

* data layout: The arrangement of bytes in *DATA*. This is composed of several
  keywords: *$DATATYPE*, *$BYTEORD*, *$PnB*, *$PnR*, and *$PnDATATYPE* (FCS3.2)
* measurement: A single vector of data (in *DATA*) along with its metadata (in
  *TEXT*). This is also used in later versions of the FCS standards. Other names
  often used include "column," "parameter," and "feature."
* optical: Describes any measurement which is not a temporal measurement. These
  often are "real" optical measurements in the sense they pertain to light (but
  not always)
* pseudostandard: a keyword which is not part of the indicated standard but has
  leading *$*.
* "raw mode": reading an FCS file while doing minimal validation of *TEXT*
* "standardized mode": reading an FCS file and checking to ensure that all
  keywords in *TEXT* conform to the indicated FCS standard.
* temporal: Describes a measurement which represents the time dimension.
