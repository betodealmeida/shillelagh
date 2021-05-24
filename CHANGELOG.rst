=========
Changelog
=========

Version 0.5.1 - 2021-05-24
==========================

- Better error handling in the GSheets dialect
- Use GSheets URL parameters on get_table_names

Version 0.5.0 - 2021-05-22
==========================

- Use new Google Sheets API v4
- Implement ``get_table_names`` for GSheets dialect
- Allow passing parameters to GSheets dialect via URL query

Version 0.4.3 - 2021-04-20
==========================

- Import Literal from typing_extensions for Python 3.7 compatibility

Version 0.4.2 - 2021-04-18
==========================

- Fix for some Google sheets where headers are not picked up

Version 0.4.1 - 2021-04-12
==========================

- Make parse_uri signature more generic

Version 0.4 - 2021-04-10
========================

- Allow adapters to return complex types (eg, datetime)
- Implement Order.ANY for columns that can be sorted by the adapter
- Add all columns to the weatherapi.com adapter

Version 0.3.1 - 2021-03-19
==========================

- Add safe mode through shillelagh+safe://
- Fix isolation levels for apsw

Version 0.3.0 - 2021-03-18
==========================

- Handle conversion of datetime objects (time, date, datetime) natively

Version 0.2.1 - 2021-03-15
==========================

- Ignore empty columns in gsheets

Version 0.2 - 2021-02-17
========================

- Added DB API 2.0 layer
- Added SQLAlchemy dialect
- Added Google Spreadsheets adapter
- Added drop-in replacement for gsheets:// dialect

Version 0.1 - 2020-10-26
========================

- Initial release
