"""Exceptions for DB API 2.0"""


class Warning(Exception):
    """
    Important warnings like data truncations while inserting.

    Exception raised for important warnings like data truncations
    while inserting, etc. It must be a subclass of the Python
    exceptions.StandardError (defined in the module exceptions).
    """


class Error(Exception):
    """
    Base class of all other error exceptions.

    Exception that is the base class of all other error exceptions.
    You can use this to catch all errors with one single except
    statement. Warnings are not considered errors and thus should
    not use this class as base. It must be a subclass of the Python
    StandardError (defined in the module exceptions).
    """


class InterfaceError(Error):
    """
    Errors that are related to the database interface.

    Exception raised for errors that are related to the database
    interface rather than the database itself. It must be a subclass
    of Error.
    """


class DatabaseError(Error):
    """
    Errors that are related to the database.

    Exception raised for errors that are related to the database.
    It must be a subclass of Error.
    """


class DataError(DatabaseError):
    """
    Errors that are due to problems with the processed data.

    Exception raised for errors that are due to problems with the
    processed data like division by zero, numeric value out of range,
    etc. It must be a subclass of DatabaseError.
    """


class OperationalError(DatabaseError):
    """
    Errors that are related to the database's operation.

    Exception raised for errors that are related to the database's
    operation and not necessarily under the control of the programmer,
    e.g. an unexpected disconnect occurs, the data source name is not
    found, a transaction could not be processed, a memory allocation
    error occurred during processing, etc. It must be a subclass of
    DatabaseError.
    """


class IntegrityError(DatabaseError):
    """
    Raised when the relational integrity of the database is affected.

    Exception raised when the relational integrity of the database is
    affected, e.g. a foreign key check fails. It must be a subclass of
    DatabaseError.
    """


class InternalError(DatabaseError):
    """
    Raised when the database encounters an internal error.

    Exception raised when the database encounters an internal error,
    e.g. the cursor is not valid anymore, the transaction is out of
    sync, etc. It must be a subclass of DatabaseError.
    """


class ProgrammingError(DatabaseError):
    """
    Raised for programming errors.

    Exception raised for programming errors, e.g. table not found or
    already exists, syntax error in the SQL statement, wrong number
    of parameters specified, etc. It must be a subclass of DatabaseError.
    """


class NotSupportedError(DatabaseError):
    """
    Raised in case a method or database API is not supported.

    Exception raised in case a method or database API was used which is
    not supported by the database, e.g. requesting a .rollback() on a
    connection that does not support transaction or has transactions
    turned off. It must be a subclass of DatabaseError.
    """
