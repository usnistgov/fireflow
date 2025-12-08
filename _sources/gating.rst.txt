Gating Classes
==============

The following classes encode the gating keywords (*$Gn\**, *$Rn\**, *$GATING*,
and *$GATE*).

These apply to each FCS version as follows (note that ``3_0`` is reused for
both FCS 3.0 and FCS 3.1):

.. list-table::
   :header-rows: 1

   * - Version
     - UnivariateRegion\*
     - BivariateRegion\*
   * - FCS2.0
     - :py:class:`~pyreflow.UnivariateRegion2_0`
     - :py:class:`~pyreflow.BivariateRegion2_0`
   * - FCS3.0
     - :py:class:`~pyreflow.UnivariateRegion3_0`
     - :py:class:`~pyreflow.BivariateRegion3_0`
   * - FCS3.1
     - :py:class:`~pyreflow.UnivariateRegion3_0`
     - :py:class:`~pyreflow.BivariateRegion3_0`
   * - FCS3.2
     - :py:class:`~pyreflow.UnivariateRegion3_2`
     - :py:class:`~pyreflow.BivariateRegion3_2`

Univariate Regions
------------------

These encode one pair of *$RnI* and *$RnW* where each has a scaler value
corresponding to a univariate gate.

.. autoclass:: pyreflow.UnivariateRegion2_0
   :members:

.. autoclass:: pyreflow.UnivariateRegion3_0
   :members:

.. autoclass:: pyreflow.UnivariateRegion3_2
   :members:

Bivariate Regions
-----------------

These encode one pair of *$RnI* and *$RnW* where each has a pair of values
corresponding to a bivariate gate.

.. autoclass:: pyreflow.BivariateRegion2_0
   :members:

.. autoclass:: pyreflow.BivariateRegion3_0
   :members:

.. autoclass:: pyreflow.BivariateRegion3_2
   :members:


Gated Measurements
------------------

This encodes one "gated measurement" described by the *$Gn\** keywords for a
given index ``n``.

.. autoclass:: pyreflow.GatedMeasurement
   :members:
