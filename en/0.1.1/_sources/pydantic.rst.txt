Configuration Interface
=======================

.. _overview:

Overview
++++++++

``pyreflow`` by default will only read fully standards compliant FCS files.

However, since most files are not standards compliant, ``pyreflow`` offers
several general configuration strategies in order to deal with the real world.

These are summarized as follows:

* **Scalpal strategy**: This will attempt to fix as many mistakes as possible in
  an FCS file while preserving non-trivial metadata. Trivial metadata includes,
  whitespace, keys with blank values, and repeated keys. If non-trivial data
  cannot be preserved, this strategy will fail.

* **Sledgehammer strategy**: This is optimized to read *DATA* at the expense of
  metadata. Non standard keywords (unless they are required for parsing *DATA*)
  will be dropped. Segments such as *ANALYSIS* and *OTHER* may not be read.

These strategies only apply to reading FCS files. ``pyreflow`` will only write
compliant files so in this case these strategies are irrelevent.

Dictionary Configuration
++++++++++++++++++++++++

Each of these strategies is implemented in ``pyreflow`` via default
configuration dictionaries which can be splatted as function arguments using
``**``.

Since these are just dictionaries, they can be modified after creating to
fine-tune the options in a given strategy.

.. autoclass:: pyreflow.api.ReadHeaderConfig
   :members:

.. autoclass:: pyreflow.api.ReadFlatTEXTConfig
   :members:
      
.. autoclass:: pyreflow.api.ReadStdTEXTConfig
   :members:

.. autoclass:: pyreflow.api.ReadFlatDatasetConfig
   :members:

.. autoclass:: pyreflow.api.ReadStdDatasetConfig
   :members:

.. autoclass:: pyreflow.api.ReadFlatDatasetFromKeywordsConfig
   :members:
      
.. autoclass:: pyreflow.api.NewCoreTEXTConfig
   :members:

.. autoclass:: pyreflow.api.NewCoreDatasetConfig
   :members:


Pydantic
++++++++

``pyreflow`` has an optional `pydantic <https://docs.pydantic.dev/>`_ interface
for configuration options. Configurations may be parsed from a yaml or JSON file
and validated using these classes. Using this requires ``pydantic`` to be
installed, which is not the case by default.

Methods on these classes are wrappers for functions defined in :doc:`reader`.
For any method, the corresponding function in :doc:`reader` is
``fcs_<method_name>``. See there for in-depth explanation for every argument,
parameter, and exception.

This may be useful in large pipelines where one has many files with different
configurations that one wishes to process in a type-safe manner.

Each class also has methods corresponding to the different read strategies as
outlined in ref:`overview`. These can be used to initialize a class with the
default configuration for a given strategy and then modified as needed.

.. autoclass:: pyreflow.pydantic.PyreflowReadHeaderConfig
   :members:
   :inherited-members: BaseModel
   :exclude-members: model_config

.. autoclass:: pyreflow.pydantic.PyreflowReadFlatTEXTConfig
   :members:
   :inherited-members: BaseModel
   :exclude-members: model_config

.. autoclass:: pyreflow.pydantic.PyreflowReadStdTEXTConfig
   :members:
   :inherited-members: BaseModel
   :exclude-members: model_config

.. autoclass:: pyreflow.pydantic.PyreflowReadFlatDatasetConfig
   :members:
   :inherited-members: BaseModel
   :exclude-members: model_config

.. autoclass:: pyreflow.pydantic.PyreflowReadStdDatasetConfig
   :members:
   :inherited-members: BaseModel
   :exclude-members: model_config

.. autoclass:: pyreflow.pydantic.PyreflowReadFlatDatasetFromKeywordsConfig
   :members:
   :inherited-members: BaseModel
   :exclude-members: model_config
