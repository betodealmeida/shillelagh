.. _postgres:

================
Postgres backend
================

Since version 1.3.0 Shillelagh ships with an experimental backend that uses Postgres instead of SQLite. The backend implements a custom [pyscopg2](https://pypi.org/project/psycopg2/) cursor that automatically registers a foreign data wrapper (FDW) whenever a supported table is accessed. It's based on the [multicorn2](http://multicorn2.org/) extension and Python package.

To use the backend you need to:

1. Install the [Multicorn2](http://multicorn2.org/) extension.
2. Install the multicorn2 Python package in the machine running Postgres. Note that this is not the "multicorn" package available on PyPI. You need to download the source and install it manually.
3. Install Shillelagh in the machine running Postgres.

Note that you need to install Python packages in a way that they are available to the process running Postgres. You can either install them globally, or install them in a virtual environment and have it activated in the process that starts Postgres.

The ``postgres/`` directory has a Docker configuration that can be used to test the backend, or as a basis for installation. To run it, execute:

.. code-block:: bash

   docker compose -f postgres/docker-compose.yml up

You should then be able to run the example script in `examples/postgres.py`_ to test that everything works.
