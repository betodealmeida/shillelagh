.. _sqlglot:

===============
sqlglot backend
===============

Since version 1.4.0 Shillelagh ships with an experimental backend that uses `sqlglot <https://sqlglot.com/sqlglot.html>`_ instead of SQLite. The backend uses the `Python executor <https://sqlglot.com/sqlglot/executor.html#executing>`_, after traversing the generated AST to collect predicates for each virtual table. For now the backend uses the SQLite dialect, but in the future we could imagine supporting other dialects as well.

The backend can be used by using the ``shillelagh+sqlglot://`` URI scheme, or importing the ``connect()`` function:

.. code-block:: python

   from shillelagh.backends.sqlglot.db import connect

   connection = connect()
   cursor = connection.cursor()
