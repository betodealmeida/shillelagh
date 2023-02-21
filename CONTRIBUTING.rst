============
Contributing
============

Thanks for wanting to contribute to the project! Here's a quick rundown of how to get it running for development.

Install ``pyenv``
=================

You don't *need* ``pyenv``, but development will be easier with it. Follow `these instructions <https://github.com/pyenv/pyenv#installation>`_ to install it, and then create a virtual environment. Shillelagh is tested with Python 3.8-3.11, so make sure to install one of those versions.

.. code-block:: bash

    $ pyenv install 3.10.3
    $ pyenv virtualenv 3.10.3 shillelagh
    $ cd /path/to/shillelagh/
    $ pyenv local shillelagh

Install dependencies
====================

You want to install the package in developer mode (``-e``) with all the dependencies needed for testing:

.. code-block:: bash

    $ pip install -e ".[testing]"

Install pre-commit hooks
========================

Shillelagh uses a lot of pre-commit hooks.

.. code-block:: bash

    $ pre-commit install

Now, you can run ``pre-commit run`` to check that the files you modified will pass CI.

Running tests
=============

To run tests:

.. code-block:: bash

    $ pytest --cov=src/shillelagh -vv tests/ --doctest-modules src/shillelagh --without-integration --without-slow-integration

Or, if you're using ``pyenv`` and created a virtual environment called ``shillelagh`` you can use the ``Makefile``:

.. code-block:: bash

    $ make test

Shillelagh has unit and integration tests. Don't worry about integration tests: they require credentials in order to run, so you won't be able to run them locally, and the CI tests will fail when you create your PR.
