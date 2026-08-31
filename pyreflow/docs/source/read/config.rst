.. _config_interfaces:

Configuration
=============

All functions and methods for reading FCS files have a wide range of options to
control their behavior.

Some users may find it easier to handle these options using the following
interfaces, which handle these options in bulk. They also offer a conveneint way
to use strategies (see :ref:`strategies`), including overriding values as needed.

Dictionary Arguments
++++++++++++++++++++

This interface provides dictionaries of options and their values which may be
splatted into functions using ``**``. Different methods are provided for each
read strategy (see :ref:`strategies`).

Since these are just dictionaries, they can be modified after creation to
fine-tune.

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

Methods on these classes are wrappers for functions defined in :doc:`functions`.
For any method, the corresponding function in :doc:`functions` is
``fcs_<method_name>``. See there for in-depth explanation for every argument,
parameter, and exception.

This may be useful in large pipelines where one has many files with different
configurations that one wishes to process in a type-safe manner.

Each class also has methods corresponding to the different read strategies as
outlined in :ref:`strategies`. These can be used to initialize a class with the
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
