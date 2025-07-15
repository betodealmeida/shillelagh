# pylint: disable=invalid-name, c-extension-no-member, unused-import
"""
A DB API 2.0 wrapper based on sqlglot.
"""

import datetime
import logging
from collections import defaultdict
from typing import Any, DefaultDict, Optional

import sqlglot
from sqlglot import exp
from sqlglot.executor import execute
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.schema import MappingSchema

from shillelagh.adapters.base import Adapter
from shillelagh.adapters.registry import registry
from shillelagh.backends.apsw.db import DEFAULT_SCHEMA
from shillelagh.db import (
    Connection,
    Cursor,
    apilevel,
    check_closed,
    paramstyle,
    threadsafety,
)
from shillelagh.exceptions import (  # nopycln: import; pylint: disable=redefined-builtin
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    OperationalError,
    ProgrammingError,
    Warning,
)
from shillelagh.fields import Boolean, DateTime, Field, Integer, String
from shillelagh.filters import Operator
from shillelagh.lib import find_adapter, get_bounds
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


def to_py(ast: exp.Expression) -> Any:
    """
    Convert an expression to a Python value.
    """
    if isinstance(ast, exp.TimeStrToTime):
        return datetime.datetime.fromisoformat(ast.this.to_py())

    if isinstance(ast, exp.DateStrToDate):
        return datetime.date.fromisoformat(ast.this.to_py())

    if (
        # remove once https://github.com/tobymao/sqlglot/pull/5409 is released
        hasattr(exp, "DateStrToTime") and isinstance(ast, exp.DateStrToTime)  # pylint: disable=no-member
    ):
        return datetime.time.fromisoformat(ast.this.to_py())  # pragma: no cover

    return ast.to_py()


class SQLGlotCursor(Cursor):  # pylint: disable=too-many-instance-attributes
    """
    Connection cursor.
    """

    @check_closed
    def execute(
        self,
        operation: str,
        parameters: Optional[tuple[Any, ...]] = None,
    ) -> "SQLGlotCursor":
        """
        Execute a query using the cursor.
        """
        self.description = None
        self._rowcount = -1

        # store current SQL in the cursor
        self.operation = operation
        try:
            ast = sqlglot.parse_one(operation, "sqlite")
        except sqlglot.errors.ParseError as exc:
            raise ProgrammingError("Invalid SQL query") from exc

        # drop table?
        if uri := self._drop_table_uri(ast):
            adapter = self._get_adapter_instance(uri)
            adapter.drop_table()
            return self

        if not isinstance(ast, exp.Select):
            raise InterfaceError("Only `DROP TABLE` and `SELECT` queries are supported")

        # run query
        if parameters:
            ast = exp.replace_placeholders(ast, *parameters)

        # qualify query so we can push down predicates to adapters
        schema = self._get_schema(ast)
        qualified = qualify_columns(ast, schema=schema)
        annotated = annotate_types(qualified, schema=schema)
        tables = self._get_tables(annotated)

        # and execute query
        table = execute(annotated, schema=schema, tables=tables)
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
            columns = adapter.get_columns()
            all_bounds = self._get_all_bounds(columns, subquery)
            bounds = get_bounds(columns, all_bounds)
            table = exp.Table(this=exp.Identifier(this=uri, quoted=True))
            tables[table] = list(adapter.get_rows(bounds, order=[]))

        return tables

    def _get_all_bounds(  # pylint: disable=too-many-branches
        self,
        columns: dict[str, Field],
        ast: exp.Subquery,
    ) -> DefaultDict[str, set[tuple[Operator, Any]]]:
        """
        Convert predicates to bounds whenever possible.
        """
        all_bounds: DefaultDict[str, set[tuple[Operator, Any]]] = defaultdict(set)

        if "where" not in ast.this.args:
            return all_bounds

        where = ast.this.args["where"]
        where = where.transform(remove_anded_parentheses)

        predicates: list[exp.Expression] = []
        predicate = where.this
        while isinstance(predicate, exp.And):
            predicates.append(predicate.expression)
            predicate = predicate.this
        predicates.append(predicate)

        for predicate in predicates:
            if self._is_valid_column_predicate(predicate) and isinstance(
                predicate,
                (exp.GTE, exp.GT, exp.LTE, exp.LT, exp.EQ, exp.NEQ, exp.Like),
            ):
                operator = {
                    exp.GTE: Operator.GE,
                    exp.GT: Operator.GT,
                    exp.LTE: Operator.LE,
                    exp.LT: Operator.LT,
                    exp.EQ: Operator.EQ,
                    exp.NEQ: Operator.NE,
                    exp.Like: Operator.LIKE,
                }[type(predicate)]
                all_bounds[predicate.this.name].add(
                    (operator, to_py(predicate.expression)),
                )
            elif isinstance(predicate, exp.Column):
                all_bounds[predicate.name].add((Operator.EQ, True))
            elif isinstance(predicate, exp.Is) and predicate.expression == exp.Null():
                all_bounds[predicate.this.name].add((Operator.IS_NULL, True))
            elif (
                isinstance(predicate, exp.Not)
                and isinstance(predicate.this, exp.Is)
                and predicate.this.expression == exp.Null()
            ):
                all_bounds[predicate.this.this.name].add((Operator.IS_NOT_NULL, True))

        # convert values to types expected by the adapter
        for column_name, operators in all_bounds.items():
            column_type = columns[column_name]
            all_bounds[column_name] = {
                (operator, column_type.format(value)) for operator, value in operators
            }

        return all_bounds

    def _is_valid_column_predicate(self, predicate: exp.Expression) -> bool:
        # sqlglot moves the column to the left side of the operator
        return isinstance(predicate.this, exp.Column) and not isinstance(
            predicate.expression,
            exp.Column,
        )


class SQLGlotConnection(Connection[SQLGlotCursor]):
    """Connection."""

    @check_closed
    def cursor(self) -> SQLGlotCursor:
        """Return a new Cursor Object using the connection."""
        cursor = SQLGlotCursor(self._adapters, self._adapter_kwargs, self.schema)
        self.cursors.append(cursor)

        return cursor


def connect(  # pylint: disable=too-many-arguments, too-many-positional-arguments
    adapters: Optional[list[str]] = None,
    adapter_kwargs: Optional[dict[str, dict[str, Any]]] = None,
    safe: bool = False,
    schema: str = DEFAULT_SCHEMA,
) -> SQLGlotConnection:
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

    return SQLGlotConnection(
        list(enabled_adapters.values()),
        adapter_kwargs,
        schema,
        safe,
    )
