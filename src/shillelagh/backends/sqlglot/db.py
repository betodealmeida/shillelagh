# pylint: disable=invalid-name, c-extension-no-member, unused-import
"""
A DB API 2.0 wrapper based on sqlglot.
"""

import logging
from collections.abc import Iterator
from typing import Any, Optional

import sqlglot
from sqlglot import exp
from sqlglot.executor import execute
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.schema import MappingSchema

from shillelagh.adapters.base import Adapter
from shillelagh.adapters.registry import registry
from shillelagh.backends.apsw.db import DEFAULT_SCHEMA
from shillelagh.backends.apsw.db import Connection as APSWConnection
from shillelagh.backends.apsw.db import Cursor as APSWCursor
from shillelagh.backends.apsw.db import apilevel, check_closed, paramstyle, threadsafety
from shillelagh.exceptions import (  # nopycln: import; pylint: disable=redefined-builtin
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    OperationalError,
    Warning,
)
from shillelagh.fields import Boolean, DateTime, Field, Integer, String
from shillelagh.filters import (
    Equal,
    Filter,
    IsNotNull,
    IsNull,
    Like,
    NotEqual,
    Operator,
    Range,
)
from shillelagh.lib import find_adapter
from shillelagh.types import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    Binary,
    Date,
    DateFromTicks,
    Time,
    TimeFromTicks,
    Timestamp,
    TimestampFromTicks,
)
from shillelagh.typing import Description

__all__ = [
    "DatabaseError",
    "DataError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "OperationalError",
    "BINARY",
    "DATETIME",
    "NUMBER",
    "ROWID",
    "STRING",
    "Binary",
    "Date",
    "DateFromTicks",
    "Time",
    "TimeFromTicks",
    "Timestamp",
    "TimestampFromTicks",
    "Warning",
    "apilevel",
    "threadsafety",
    "paramstyle",
]


_logger = logging.getLogger(__name__)


type_map: dict[exp.DataType.Type, type[Field]] = {
    exp.DataType.Type.INT: Integer,
    exp.DataType.Type.TEXT: String,
    exp.DataType.Type.BOOLEAN: Boolean,
    exp.DataType.Type.TIMESTAMP: DateTime,
}

DEFAULT_TYPE = String


def remove_anded_parentheses(node: exp.Expression) -> exp.Expression:
    """
    Remove unnecessary parentheses around `AND` expressions.
    """
    return (
        node.this.transform(remove_anded_parentheses)
        if isinstance(node, exp.Paren) and isinstance(node.this, exp.And)
        else node
    )


class Cursor(APSWCursor):  # pylint: disable=too-many-instance-attributes
    """
    Connection cursor.
    """

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        adapters: list[type[Adapter]],
        adapter_kwargs: dict[str, dict[str, Any]],
        schema: str = DEFAULT_SCHEMA,
    ):
        self._adapters = adapters
        self._adapter_kwargs = adapter_kwargs

        self.schema = schema

        # This read/write attribute specifies the number of rows to fetch at a
        # time with .fetchmany(). It defaults to 1 meaning to fetch a single
        # row at a time.
        self.arraysize = 1

        self.closed = False

        # this is updated only after a query
        self.description: Description = None

        # this is set to an iterator of rows after a successful query
        self._results: Optional[Iterator[tuple[Any, ...]]] = None
        self._rowcount = -1

        self.operation: Optional[str] = None

    @check_closed
    def execute(
        self,
        operation: str,
        parameters: Optional[tuple[Any, ...]] = None,
    ) -> "Cursor":
        """
        Execute a query using the cursor.
        """
        self.description = None
        self._rowcount = -1

        # store current SQL in the cursor
        self.operation = operation

        ast = sqlglot.parse_one(operation)

        # drop table?
        if uri := self._drop_table_uri(ast):
            adapter = self._get_adapter_instance(uri)
            adapter.drop_table()
            return self

        if not isinstance(ast, exp.Select):
            # we only support SELECT queries
            raise InterfaceError("Only `DROP TABLE` and `SELECT` queries are supported")

        # run query
        if parameters:
            ast.replace_placeholders(ast, **parameters)

        # qualify query so we can push down predicates to adapters
        schema = self._get_schema(ast)
        qualified = qualify(ast, dialect="sqlite", schema=schema)

        # annotate query with types
        annotated = annotate_types(qualified, schema=schema)

        # build tables for sqlglot
        tables = self._get_tables(qualified)

        # and execute query
        table = execute(qualified, tables=tables)
        self._results = (reader.row for reader in table)

        # store description
        self.description = [
            (
                name,
                type_map.get(expression.type.this, DEFAULT_TYPE),
                None,
                None,
                None,
                None,
                True,
            )
            for name, expression in zip(table.columns, annotated.expressions)
        ]

        return self

    def _get_adapter_instance(self, uri: str) -> Adapter:
        """
        Return an adapter instance for the given URI.
        """
        adapter, args, kwargs = find_adapter(uri, self._adapter_kwargs, self._adapters)

        return adapter(*args, **kwargs)

    def _drop_table_uri(self, ast: exp.Expression) -> Optional[str]:
        """
        Extract table being dropped, if any
        """
        if not isinstance(ast, exp.Drop):
            return None

        return ast.find(exp.Table).name if ast.find(exp.Table) else None

    def _get_schema(self, ast: exp.Select) -> MappingSchema:
        """
        Return the schema of all referenced tables.
        """
        schema = MappingSchema(dialect="sqlite")

        for relation in self._get_relations(ast):
            uri = relation.name
            adapter = self._get_adapter_instance(uri)
            column_mapping = {
                name: field.type for name, field in adapter.get_columns().items()
            }
            schema.add_table(
                table=exp.Table(this=exp.Identifier(this=uri, quoted=True)),
                column_mapping=column_mapping,
            )

        return schema

    def _get_relations(self, ast: exp.Select) -> set[exp.Table]:
        """
        Return the relations referenced in the query.
        """
        return {
            source
            for scope in traverse_scope(ast)
            for source in scope.sources.values()
            if isinstance(source, exp.Table)
        }

    def _get_tables(self, ast: exp.Select) -> dict[str, list[dict[str, Any]]]:
        """
        Build the tables needed for the sqlglot executor.
        """
        modified = ast.copy()

        # first replace all table names with a dummy subquery, so we can push predicates
        # more easily
        for relation in self._get_relations(modified):
            identifier = exp.Identifier(this=relation.name, quoted=True)
            relation.replace(
                exp.Subquery(
                    this=exp.Select(
                        expressions=[exp.Star()],
                        **{"from": exp.From(this=exp.Table(this=identifier))},
                    ),
                    alias=exp.TableAlias(this=identifier),
                ),
            )

        # now push predicates
        modified = pushdown_predicates(modified)

        # finally build a table for each subquery
        tables = {}
        for subquery in modified.find_all(exp.Subquery):
            if not isinstance(subquery.this.args["from"].this, exp.Table):
                continue

            uri = subquery.alias
            adapter = self._get_adapter_instance(uri)
            bounds = self._get_bounds(subquery)
            tables[uri] = list(adapter.get_data(bounds, order=[]))

        return tables

    def _get_bounds(self, ast: exp.Subquery) -> dict[str, Filter]:
        if "where" not in ast.this.args:
            return {}

        where = ast.this.args["where"]
        where = where.transform(remove_anded_parentheses)

        predicates: list[exp.Expression] = []
        predicate = where.this
        while isinstance(predicate, exp.And):
            predicates.append(predicate.expression)
            predicate = predicate.this
        predicates.append(predicate)

        bounds = {}
        for predicate in predicates:
            if self._is_valid_column_predicate(predicate) and isinstance(
                predicate,
                (exp.GTE, exp.GT, exp.LTE, exp.LT),
            ):
                operators = {
                    exp.GTE: (Operator.GE, Operator.LT),
                    exp.GT: (Operator.GT, Operator.LE),
                    exp.LTE: (Operator.LE, Operator.GT),
                    exp.LT: (Operator.LT, Operator.GE),
                }[type(predicate)]
                if isinstance(predicate.this, exp.Column):
                    bounds[predicate.this.name] = Range.build(
                        {(operators[0], predicate.expression.to_py())},
                    )
                else:
                    bounds[predicate.expression.name] = Range.build(
                        {(operators[1], predicate.this.to_py())},
                    )

            elif self._is_valid_column_predicate(predicate) and isinstance(
                predicate,
                (exp.EQ, exp.NEQ, exp.Like),
            ):
                filter_ = {
                    exp.EQ: Equal,
                    exp.NEQ: NotEqual,
                    exp.Like: Like,
                }[type(predicate)]
                if isinstance(predicate.this, exp.Column):
                    bounds[predicate.this.name] = filter_(predicate.expression.to_py())
                else:
                    bounds[predicate.expression.name] = filter_(predicate.this.to_py())
            elif isinstance(predicate, exp.Column):
                bounds[predicate.name] = Equal(True)
            elif isinstance(predicate, exp.Is) and predicate.expression == exp.Null():
                bounds[predicate.this.name] = IsNull()
            elif (
                isinstance(predicate, exp.Not)
                and isinstance(predicate.this, exp.Is)
                and predicate.this.expression == exp.Null()
            ):
                bounds[predicate.this.name] = IsNotNull()

        return bounds

    def _is_valid_column_predicate(self, predicate: exp.Expression) -> bool:
        return (
            isinstance(predicate.this, exp.Column)
            and isinstance(predicate.expression, exp.Literal)
        ) or (
            isinstance(predicate.this, exp.Literal)
            and isinstance(predicate.expression, exp.Column)
        )


class Connection(APSWConnection):  # pylint: disable=too-many-instance-attributes
    """Connection."""

    def __init__(  # pylint: disable=too-many-arguments, too-many-positional-arguments
        self,
        adapters: list[type[Adapter]],
        adapter_kwargs: dict[str, dict[str, Any]],
        schema: str = DEFAULT_SCHEMA,
        safe: bool = False,
    ):
        self.schema = schema
        self.safe = safe

        self._adapters = adapters
        self._adapter_kwargs = adapter_kwargs

        self.closed = False
        self.cursors: list[APSWCursor] = []

    @check_closed
    def commit(self) -> None:
        """Commit any pending transaction to the database."""

    @check_closed
    def rollback(self) -> None:
        """Rollback any transactions."""

    @check_closed
    def cursor(self) -> Cursor:
        """Return a new Cursor Object using the connection."""
        cursor = Cursor(self._adapters, self._adapter_kwargs, self.schema)
        self.cursors.append(cursor)

        return cursor


def connect(  # pylint: disable=too-many-arguments, too-many-positional-arguments
    adapters: Optional[list[str]] = None,
    adapter_kwargs: Optional[dict[str, dict[str, Any]]] = None,
    safe: bool = False,
    schema: str = DEFAULT_SCHEMA,
) -> Connection:
    """
    Constructor for creating a connection to the database.
    """
    adapter_kwargs = adapter_kwargs or {}
    enabled_adapters = registry.load_all(adapters, safe)

    # replace entry point names with class names
    mapping = {
        name: adapter.__name__.lower() for name, adapter in enabled_adapters.items()
    }
    adapter_kwargs = {mapping[k]: v for k, v in adapter_kwargs.items() if k in mapping}

    return Connection(
        list(enabled_adapters.values()),
        adapter_kwargs,
        schema,
        safe,
    )
