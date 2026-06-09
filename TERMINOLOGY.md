These are terms used throughout `fireflow`'s code and documentation. Some of
these are derived from the standards themselves and some are unique to this
library.

* data layout: The arrangement of bytes in *DATA*. This is composed of several
  keywords: *$DATATYPE*, *$BYTEORD*, *$PnB*, *$PnR*, and *$PnDATATYPE* (FCS3.2)
* measurement: A single vector of data (in *DATA*) along with its metadata (in
  *TEXT*). This is also used in later versions of the FCS standards. Other names
  often used include "column," "parameter," and "feature."
* offsets: two offsets that denote the location of segment in an FCS file. This
  is used interchangably with "offset pair". It is and must be kept distinct
  from `segment` (which refers to the bytes themselves) since offsets for one
  segment can be duplicated in multiple locations throughout an FCS file.
* optical: Describes any measurement which is not a temporal measurement. These
  often are "real" optical measurements in the sense they pertain to light (but
  not always)
* pseudoempty: a segment offset pair where the second offset is one less than
  the first (ie `0,-1` or `1000,999`).
* pseudostandard: a keyword which is not part of the indicated standard but has
  leading *$*.
* "flat mode": reading an FCS file while leaving keywords as a list of 
  key/value pairs (hence "flat") and doing minimal validation of *TEXT*
* segment: a stretch of bytes in an FCS file that has a defined purpose 
  (*HEADER*, *TEXT*, etc).
* "standardized mode": reading an FCS file and checking to ensure that all
  keywords in *TEXT* conform to the indicated FCS standard.
* temporal: Describes a measurement which represents the time dimension.
