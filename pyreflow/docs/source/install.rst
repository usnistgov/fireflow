Installation
============

`pyreflow` is not yet available via PyPI or other package respositories so it
must be obtained using git and built.

Install using pip/git:

.. code-block:: sh

    pip install git+https://github.com/njd2/fireflow.git#subdirectory=pyreflow

This will build and install the master branch into the currently active
environment.

Alternatively, install into a `conda` environment.

Example `env.yml`:

.. code-block:: yaml

    channels:
      - conda-forge
    dependencies:
      - maturin=1.8.7
      - pip:
        - git+https://github.com/njd2/fireflow.git#subdirectory=pyreflow
