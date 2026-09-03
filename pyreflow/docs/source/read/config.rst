.. _config_interfaces:

Configuration
=============

All functions and methods for reading FCS files have a wide range of options to
control their behavior.

Many users may find it easier to handle these options using the following
interfaces, which handle these options in bulk. They also offer a convenient way
to use strategies (see :ref:`strategies`), including overriding values as
needed.

.. _config_dict:

Dictionary Arguments
++++++++++++++++++++

This interface provides dictionaries of options and their values which may be
splatted into functions using ``**``. Different methods are provided for each
read strategy (see :ref:`strategies`).

.. tip::

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

.. note::

   ``pydantic`` must be installed for this interface to work

``pyreflow`` has an optional `pydantic <https://docs.pydantic.dev/>`_ interface
for configuration options.

Methods on these classes are wrappers for functions defined in :doc:`functions`.
For any method here, the corresponding function in :doc:`functions` is
``fcs_<method_name>``. See there for in-depth explanation for every argument,
parameter, and exception.

Each class also has methods corresponding to the different read strategies as
outlined in :ref:`strategies`. These can be used to initialize a class with the
default configuration for a given strategy and then modified as needed.

.. tip::

   Configurations can be stored in JSON or YAML and then parsed with this
   interface. This may be useful in large pipelines which need multiple
   configurations for many FCS files.

   Consult the ``pydantic`` docs for how to parse these files.

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
