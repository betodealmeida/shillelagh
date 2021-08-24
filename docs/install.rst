.. _install:

============
Installation
============

Install Shillelagh with ``pip``:

.. code-block:: bash

    $ pip install 'shillelagh'

This will install an unofficial APSW package from the `Python package index <https://pypi.org/project/apsw/>`_. It's highly recommend to install a newer version:

.. code-block:: bash

    $ pip install https://github.com/rogerbinns/apsw/releases/download/3.36.0-r1/apsw-3.36.0-r1.zip \
    --global-option=fetch --global-option=--version --global-option=3.36.0 --global-option=--all \
    --global-option=build --global-option=--enable-all-extensions
