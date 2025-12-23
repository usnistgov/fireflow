Pydantic Interface
==================

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

.. autoclass:: pyreflow.pydantic.PyreflowHeaderConfig
   :members:
   :exclude-members: model_config

.. autoclass:: pyreflow.pydantic.PyreflowReadFlatTEXTConfig
   :members:
   :exclude-members: model_config

.. autoclass:: pyreflow.pydantic.PyreflowReadStdTEXTConfig
   :members:
   :exclude-members: model_config

.. autoclass:: pyreflow.pydantic.PyreflowReadFlatDatasetConfig
   :members:
   :exclude-members: model_config

.. autoclass:: pyreflow.pydantic.PyreflowReadStdDatasetConfig
   :members:
   :exclude-members: model_config

.. autoclass:: pyreflow.pydantic.PyreflowReadFlatDatasetFromKeywordsConfig
   :members:
   :exclude-members: model_config
