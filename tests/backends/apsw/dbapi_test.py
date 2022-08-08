"""
Test compliance with PEP 249 (https://www.python.org/dev/peps/pep-0249/).
"""

from inspect import isfunction, ismethod

import pytest

from shillelagh.backends.apsw import db as dbapi


def test_module_interface() -> None:
    """
    Test constructors, globals, and exceptions.
    """
    # constructors
    assert isfunction(dbapi.connect)

    # globals
    assert dbapi.apilevel == "2.0"
    assert dbapi.threadsafety == 2
    assert dbapi.paramstyle == "qmark"

    # exceptions
    assert issubclass(dbapi.Warning, Exception)
    assert issubclass(dbapi.Error, Exception)
    assert issubclass(dbapi.InterfaceError, dbapi.Error)
    assert issubclass(dbapi.DatabaseError, dbapi.Error)
    assert issubclass(dbapi.DataError, dbapi.DatabaseError)
    assert issubclass(dbapi.OperationalError, dbapi.DatabaseError)
    assert issubclass(dbapi.IntegrityError, dbapi.DatabaseError)
    assert issubclass(dbapi.InternalError, dbapi.DatabaseError)
    assert issubclass(dbapi.ProgrammingError, dbapi.DatabaseError)
    assert issubclass(dbapi.NotSupportedError, dbapi.DatabaseError)


def test_connection() -> None:
    """
    Test that the connection object implements required methods.
    """
    connection = dbapi.connect(":memory:")

    assert ismethod(connection.close)
    assert ismethod(connection.commit)
    assert ismethod(connection.rollback)  # optional
    assert ismethod(connection.cursor)


def test_cursor() -> None:
    """
    Test that the cursor implements required methods.
    """
    connection = dbapi.connect(":memory:")
    cursor = connection.cursor()

    # attributes
    assert cursor.description is None
    assert cursor.rowcount == -1

    cursor.execute("SELECT 1, 'test'")
    assert cursor.description
    assert len(cursor.description) == 2
    assert all(len(sequence) == 7 for sequence in cursor.description)
    assert cursor.rowcount == 1

    # methods
    assert ismethod(cursor.close)
    cursor.close()
    with pytest.raises(dbapi.Error) as excinfo:
        cursor.execute("SELECT 1")
    assert str(excinfo.value) == "Cursor already closed"

    assert ismethod(cursor.execute)
    assert ismethod(cursor.executemany)
    assert ismethod(cursor.fetchone)
    assert ismethod(cursor.fetchmany)
    assert ismethod(cursor.fetchall)
    assert cursor.arraysize == 1
    cursor.arraysize = 2
    assert cursor.arraysize == 2
    assert ismethod(cursor.setinputsizes)
    assert ismethod(cursor.setoutputsizes)


def test_type_objects_and_constructors() -> None:
    """
    Test type objects and constructors.
    """
    assert isfunction(dbapi.Date)
    assert isfunction(dbapi.Time)
    assert isfunction(dbapi.Timestamp)
    assert isfunction(dbapi.DateFromTicks)
    assert isfunction(dbapi.TimeFromTicks)
    assert isfunction(dbapi.TimestampFromTicks)
    assert isfunction(dbapi.Binary)
    assert dbapi.STRING
    assert dbapi.BINARY
    assert dbapi.NUMBER
    assert dbapi.DATETIME
    assert dbapi.ROWID
