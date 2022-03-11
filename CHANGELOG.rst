=========
Changelog
=========

Version 1.0.8 - 2022-03-11
==========================

- Do not try to import non-specified adapters
- Fix for querying datetime/date/time via SQLAlchemy

Version 1.0.7 - 2022-03-03
==========================

- Add support for using Google Application Default Credentials for Google Sheets
- Make package PEP 561 compatible
- Add ``requests`` as a dependency
- Documentation fixes

Version 1.0.6 - 2021-12-30
==========================

- Add an adapter for system resources (CPU usage for now)
- Improve PEP 249 compatibility

Version 1.0.5 - 2021-12-02
==========================

- Implement ``do_ping`` for GSheets dialect
- Create a ``cookiecutter`` template for new adapters
- Add a ``StringDuration`` field
- Add GitHub adapter
- Handle arbitrary number formats in Gsheets

Version 1.0.4 - 2021-08-30
==========================

- Add pattern parser/formatter for GSheets

Version 1.0.3 - 2021-08-24
==========================

- Add optional dependencies for Datasette

Version 1.0.2 - 2021-08-24
==========================

- Fix Datasette by always using ``LIMIT`` with ``OFFSET``
- More operators: ``LIKE``, ``IS NULL``, ``IS NOT NULL`` and ``!=``

Version 1.0.1 - 2021-08-23
==========================

- Add cost estimation to all adapters
- Add Datasette adapter
- Remove ``csv://`` and ``datasette+`` prefixes to simply URIs
- Add ``has_table`` method to dialects

Version 1.0.0 - 2021-08-18
==========================

- Move config to ``~/.config/shillelagh/``
- Add function ``get_available_adapters`` to list installed adapters
- Developer and user docs `added <https://shillelagh.readthedocs.io/>`_
- Small fixes

Version 0.8.1 - 2021-07-11
==========================

- Add integration tests
- Fix couple bugs on GSheets while adding integration tests

Version 0.8.0 - 2021-07-08
==========================

- Refactor fields
- Change GSheets to use formatted values
- Fix bug in GSheets DML with datime/date/time
- Return naive objects when no timezone specified

Version 0.7.4 - 2021-07-03
==========================

- Fix DML bug in GSheets with a middle nameless column

Version 0.7.3 - 2021-07-01
==========================

- Relax ``google-auth`` dependency

Version 0.7.2 - 2021-07-01
==========================

- Fix ``SELECT`` bug in GSheets with a middle nameless column

Version 0.7.1 - 2021-07-01
==========================

- Relax SQLAlchemy dependency

Version 0.7.0 - 2021-07-01
==========================

- Add support for DML to the GSheets adapter
- GSheets dialect now return "main" as its schema
- Schema prefix can now be used on table names
- GSheets now supports defining a catalog of spreadsheets
- Improve many small bugs in the type conversion system
- Add ``sleep``, ``version``, and ``get_metadata`` functions
- Add REPL command-line utility (``shillelagh``)
- Remove ``adapter_args``, use only ``adapter_kwargs`` now

Version 0.6.1 - 2021-06-22
==========================

- Parse bindings in ``execute``, allowing native Python types
- Allow configuring adapters via kwargs in addition to args

Version 0.6.0 - 2021-06-17
==========================

- Handle type conversion via fields
- Fix Socrata, mapping ``calendar_date`` to ``Date``

Version 0.5.2 - 2021-06-03
==========================

- Adapter for Socrata

Version 0.5.1 - 2021-05-24
==========================

- Better error handling in the GSheets dialect
- Use GSheets URL parameters on ``get_table_names``

Version 0.5.0 - 2021-05-22
==========================

- Use new GSheets API v4
- Implement ``get_table_names`` for GSheets dialect
- Allow passing parameters to GSheets dialect via URL query

Version 0.4.3 - 2021-04-20
==========================

- Import ``Literal`` from ``typing_extensions`` for Python 3.7 compatibility

Version 0.4.2 - 2021-04-18
==========================

- Fix for some Google sheets where headers are not picked up

Version 0.4.1 - 2021-04-12
==========================

- Make ``parse_uri`` signature more generic

Version 0.4 - 2021-04-10
========================

- Allow adapters to return complex types (eg, datetime)
- Implement ``Order.ANY`` for columns that can be sorted by the adapter
- Add all columns to the weatherapi.com adapter

Version 0.3.1 - 2021-03-19
==========================

- Add safe mode through ``shillelagh+safe://``
- Fix isolation levels for apsw

Version 0.3.0 - 2021-03-18
==========================

- Handle conversion of datetime objects (time, date, datetime) natively

Version 0.2.1 - 2021-03-15
==========================

- Ignore empty columns in gsheets

Version 0.2 - 2021-02-17
========================

- Add DB API 2.0 layer
- Add SQLAlchemy dialect
- Add GSheets adapter
- Add drop-in replacement for ``gsheets://`` dialect

Version 0.1 - 2020-10-26
========================

- Initial release
