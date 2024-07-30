=========
Changelog
=========

Next
====

Version 1.2.22 - 2024-07-30
===========================

- Handle array-only responses in the generic JSON adapter (#461)

Version 1.2.21 - 2024-07-12
===========================

- Fix for invalid pattern in GSheets (#458)
- dbt MetricFlow adapter (#459)

Version 1.2.20 - 2024-07-10
===========================

- Add ``import_dbapi()`` to supress warning (#452)
- Remove ``LIMIT`` and ``OFFSET`` from GitHub adapter (#456)

Version 1.2.19 - 2024-04-03
===========================

- Relax ``tabulate`` dependency for Apache Superset (#443)

Version 1.2.18 - 2024-03-27
===========================

- Fix OAuth2 flow in GSheets (#438)

Version 1.2.17 - 2024-02-23
===========================

- Add support for GitHub issues (#433)

Version 1.2.16 - 2024-02-22
===========================

- Allow for custom expiration time in the generic JSON/XML adapters (#429)
- Use a different JSONPath library that handles root better (#431)

Version 1.2.15 - 2024-02-13
===========================

- Preset adapter now handles pagination, offset and limit (#427)

Version 1.2.14 - 2024-01-05
===========================

- Preset adapter can now query workspaces (#423)

Version 1.2.13 - 2024-01-04
===========================

- Add custom adapter for the Preset API (#421)

Version 1.2.12 - 2023-12-05
===========================

- ``DROP TABLE`` now handles comments in query (#417)

Version 1.2.11 - 2023-11-27
===========================

- Relax ``holidays`` dependency for Apache Superset (#416)

Version 1.2.10 - 2023-11-08
===========================

- Add missing ``requests-cache`` dependency (#413)

Version 1.2.9 - 2023-11-08
==========================

- Improve Datasette detection (#399)
- Fix generic JSON handling of null values (#399)
- Add holidays adapter (#409)
- Add support for decimals (#410)

Version 1.2.8 - 2023-10-21
==========================

- Add new cost model ``NetworkAPICostModel`` (#381)
- Add a generic XML adapter (#391)

Version 1.2.7 - 2023-08-14
==========================

- Fix unneeded error when an operators is not supported by adapter (#378)
- Fix comparison to empty strings in GSheets (#379)

Version 1.2.6 - 2023-07-20
==========================

- Add support for querying durations in Google Sheets (#374)

Version 1.2.5 - 2023-07-14
==========================

- System adapter now supports memory (virtual/swap) queries (#369 and #372)

Version 1.2.4 - 2023-05-15
==========================

- Relax dependency for ``requests-cache`` correctly (#362)

Version 1.2.3 - 2023-05-15
==========================

- Add ``yarl`` dependency to the generic JSON adapter (#355)
- Only warn of errors when loading adapters if they are explicitly requested (#360)
- Relax dependency for ``requests-cache`` (#361)

Version 1.2.2 - 2023-04-17
==========================

- Allow passing request headers to the generic JSON adapter via query arguments (#354)

Version 1.2.1 - 2023-04-14
==========================

- Allow specifying custom request headers when using the generic JSON adapter (#337)
- Fix for escaping identifiers correctly (#340)
- Support for S3-compatible storage (#343)
- Adapters can now know which columns were requested (#345)
- Python 3.11 officially supported (#334)
- Fix for error when an adapter can't be loaded (#346)
- Fix for ``BestIndexObject`` (#350)
- Fix for empty dataframes (#351)

Version 1.2.0 - 2023-02-17
==========================

- Use ``marshal`` instead of ``pickle`` for adapter argument serde (#321)
- Support SQLAlchemy 2.0 (and 1.4) (#331)
- ``s3_select`` can now use credentials from the environment or config files

Version 1.1.5 - 2022-12-08
==========================

- Handle dataframes without column names (#319)
- Support booleans when inferring types from data (#318)

Version 1.1.4 - 2022-12-06
==========================

- Support JSON files in the S3 Select adapter (#314)

Version 1.1.3 - 2022-11-17
==========================

- Improve generic JSON adapter to handle nested fields (#309)

Version 1.1.2 - 2022-11-01
==========================

- Fix preventing loading of non-requested adapters (#303)
- New generic JSON adapter (#304)

Version 1.1.1 - 2022-10-26
==========================

- Add support for CSV files over HTTP(S) (#296)
- Fix for fraction parsing in GSheets (#298)
- Fix for negative dollar format in GSheets (#299)
- Other small fixes and typos.

Version 1.1.0 - 2022-07-28
==========================

- CLI now supports multi-line statements (#205)
- Add new adapter for CSV/JSON/Parquet files stored in S3 (#254)
- Add support for ``DROP TABLE`` (#258)
- Add new adapter for scraping data from HTML tables (#259)
- Add registry for adapters (#268)
- Adapters can implement ``LIMIT`` and ``OFSET`` (#270)
- Add support for polymorphic fields (#280)
- Add docs on architecture (#273), dialects (#278)
- Improve docs on custom fields (#275)
- Configuration directory is now system dependent (#283)
- Row updates should no longer raise errors in SQLAlchemy (#284)

Version 1.0.16 - 2022-07-15
===========================

- Better serialize/deserialize for virtual table arguments, supporting bytes and other types

Version 1.0.15 - 2022-07-13
===========================

- Represent integers as strings in SQLite to prevent overflow
- Add S3 Select adapter

Version 1.0.14 - 2022-05-25
===========================

- Fix for column names with double quotes

Version 1.0.13 - 2022-05-02
===========================

- Remove support for Python 3.7
- Remove upper bounds from dependencies

Version 1.0.12 - 2022-04-28
===========================

- Allow adapters/dialects to pass custom parameters to ``apsw.Connection``

Version 1.0.11 - 2022-04-14
===========================

- Read SQLite constants from ``apsw``
- Fix description returned in the case of empty set
- Change cost to be a float
- Fix GSheets ``do_ping``
- Small fixes and typos.

Version 1.0.10 - 2022-03-14
===========================

- Relax ``requests`` dependency
- Enable ``supports_statement_cache``

Version 1.0.9 - 2022-03-12
==========================

- Fix for GSheets where the first row is not detected as column names

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
