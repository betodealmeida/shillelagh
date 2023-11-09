.. _install:

============
Installation
============

Install Shillelagh with ``pip``:

.. code-block:: bash

    $ pip install 'shillelagh'

You also need to install optional dependencies, depending on the adapter you want to use:

.. code-block:: bash

    $ pip install 'shillelagh[console]'       # to use the CLI
    $ pip install 'shillelagh[githubapi]'     # for GitHub
    $ pip install 'shillelagh[gsheetsapi]'    # for GSheets
    $ pip install 'shillelagh[htmltableapi]'  # for HTML tables
    $ pip install 'shillelagh[pandasmemory]'  # for Pandas in memory
    $ pip install 'shillelagh[s3selectapi]'   # for S3 files
    $ pip install 'shillelagh[systemapi]'     # for CPU information

Alternatively, you can install everything with:

.. code-block:: bash

    $ pip install 'shillelagh[all]'
~
