# pylint: disable=invalid-name, c-extension-no-member, unused-import
"""
An async DB API 2.0 wrapper based on sqlglot.

This module provides async support for the SQLGlot backend, enabling concurrent
fetching of data from multiple adapters. This is particularly beneficial for
network-based adapters (e.g., Google Sheets, BigQuery) where I/O is the bottleneck.
"""

import asyncio
import logging
from collections import defaultdict
from typing import Any, DefaultDict, Optional

import sqlglot
from sqlglot import exp
from sqlglot.executor import execute
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.schema import MappingSchema

from shillelagh.adapters.base import Adapter
from shillelagh.adapters.registry import registry
from shillelagh.backends.apsw.db import DEFAULT_SCHEMA
from shillelagh.backends.sqlglot.db import (
    SQLGlotConnection,
    SQLGlotCursor,
    remove_anded_parentheses,
    to_py,
    type_map,
)
from shillelagh.db import check_closed
from shillelagh.exceptions import InterfaceError, ProgrammingError
from shillelagh.fields import Field
from shillelagh.filters import Operator
from shillelagh.lib import find_adapter, get_bounds

__all__ = [
    "SQLGlotAsyncCursor",
    "SQLGlotAsyncConnection",
    "connect_async",
]


_logger = logging.getLogger(__name__)


class SQLGlotAsyncCursor(SQLGlotCursor):  # pylint: disable=too-many-instance-attributes
    """
    Async connection cursor.

    This cursor extends SQLGlotCursor to provide async query execution.
    The primary benefit is concurrent fetching of data from multiple tables,
    which can provide significant performance improvements for queries involving
    multiple network-based data sources.
    """

    @check_closed
    async def execute_async(
        self,
        operation: str,
        parameters: Optional[tuple[Any, ...]] = None,
    ) -> "SQLGlotAsyncCursor":
        """
        Execute a query asynchronously using the cursor.

        This method is similar to execute() but fetches data from all tables
        concurrently using asyncio, which can provide 2-5x speedup for queries
        involving multiple network-based adapters.
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
            # Run drop_table in executor since it might do I/O
            await asyncio.get_event_loop().run_in_executor(None, adapter.drop_table)
            return self

        if not isinstance(ast, exp.Select):
            raise InterfaceError("Only `DROP TABLE` and `SELECT` queries are supported")

        # run query
        if parameters:
            ast = exp.replace_placeholders(ast, *parameters)

        # qualify query so we can push down predicates to adapters
        schema = await self._get_schema_async(ast)
        qualified = qualify_columns(ast, schema=schema)
        annotated = annotate_types(qualified, schema=schema)
        tables = await self._get_tables_async(annotated)

        # and execute query (sqlglot executor is synchronous, processes in-memory data)
        table = execute(annotated, schema=schema, tables=tables)
        self._results = (reader.row for reader in table)

        # store description
        self.description = [
            (
                name,
                type_map.get(expression.type.this, type_map.get(exp.DataType.Type.TEXT)),
                None,
                None,
                None,
                None,
                True,
            )
            for name, expression in zip(table.columns, annotated.expressions)
        ]

        return self

    async def _get_schema_async(self, ast: exp.Select) -> MappingSchema:
        """
        Return the schema of all referenced tables asynchronously.

        This method fetches column metadata from all adapters concurrently.
        """
        schema = MappingSchema(dialect="sqlite")

        async def fetch_schema_for_table(relation: exp.Table) -> tuple[exp.Table, dict[str, str]]:
            """Fetch schema for a single table."""
            uri = relation.name
            adapter = self._get_adapter_instance(uri)

            # get_columns() might do I/O (e.g., HTTP request for schema discovery)
            # so we run it in an executor
            loop = asyncio.get_event_loop()
            columns = await loop.run_in_executor(None, adapter.get_columns)

            column_mapping = {name: field.type for name, field in columns.items()}
            table = exp.Table(this=exp.Identifier(this=uri, quoted=True))
            return table, column_mapping

        # Fetch all table schemas concurrently
        relations = self._get_relations(ast)
        tasks = [fetch_schema_for_table(relation) for relation in relations]
        results = await asyncio.gather(*tasks)

        # Add all tables to schema
        for table, column_mapping in results:
            schema.add_table(table=table, column_mapping=column_mapping)

        return schema

    async def _get_tables_async(self, ast: exp.Select) -> dict[str, list[dict[str, Any]]]:
        """
        Build the tables needed for the sqlglot executor asynchronously.

        This is the key optimization: instead of fetching tables sequentially,
        we fetch all tables concurrently using asyncio.gather().

        For a query with N tables that each take T seconds to fetch:
        - Synchronous: N * T seconds
        - Asynchronous: max(T) seconds (all fetched in parallel)
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

        # Build a list of tasks for fetching each table
        async def fetch_table(
            subquery: exp.Subquery,
        ) -> tuple[exp.Table, list[dict[str, Any]]]:
            """Fetch data for a single table."""
            uri = subquery.alias
            adapter = self._get_adapter_instance(uri)
            columns = adapter.get_columns()
            all_bounds = self._get_all_bounds(columns, subquery)
            bounds = get_bounds(columns, all_bounds)

            # Run the blocking get_rows() in a thread pool executor
            # This prevents blocking the event loop
            loop = asyncio.get_event_loop()
            rows = await loop.run_in_executor(
                None,
                lambda: list(adapter.get_rows(bounds, order=[])),
            )

            table = exp.Table(this=exp.Identifier(this=uri, quoted=True))
            return table, rows

        # Collect all subqueries that need to be fetched
        subqueries_to_fetch = [
            subquery
            for subquery in modified.find_all(exp.Subquery)
            if isinstance(subquery.this.args.get("from", {}).this, exp.Table)
        ]

        # Fetch all tables concurrently
        _logger.info(
            "Fetching %d tables concurrently using async execution",
            len(subqueries_to_fetch),
        )
        tasks = [fetch_table(subquery) for subquery in subqueries_to_fetch]
        results = await asyncio.gather(*tasks)

        # Convert results to the expected format
        tables = dict(results)

        return tables


class SQLGlotAsyncConnection(SQLGlotConnection):
    """Async connection for SQLGlot backend."""

    @check_closed
    def cursor_async(self) -> SQLGlotAsyncCursor:
        """Return a new async Cursor Object using the connection."""
        cursor = SQLGlotAsyncCursor(self._adapters, self._adapter_kwargs, self.schema)
        self.cursors.append(cursor)

        return cursor


def connect_async(  # pylint: disable=too-many-arguments, too-many-positional-arguments
    adapters: Optional[list[str]] = None,
    adapter_kwargs: Optional[dict[str, dict[str, Any]]] = None,
    safe: bool = False,
    schema: str = DEFAULT_SCHEMA,
) -> SQLGlotAsyncConnection:
    """
    Constructor for creating an async connection to the database.

    This function creates a connection that supports async query execution,
    allowing concurrent fetching of data from multiple adapters.

    Example usage:
        >>> import asyncio
        >>> from shillelagh.backends.sqlglot.db_async import connect_async
        >>>
        >>> async def main():
        ...     conn = connect_async()
        ...     cursor = conn.cursor_async()
        ...     await cursor.execute_async(
        ...         "SELECT * FROM 'gs://sheet1' JOIN 'gs://sheet2' ON ..."
        ...     )
        ...     results = cursor.fetchall()
        ...
        >>> asyncio.run(main())

    Args:
        adapters: List of adapter names to enable (None = all available)
        adapter_kwargs: Additional arguments for adapters
        safe: If True, only load safe adapters
        schema: Schema name (default: 'main')

    Returns:
        SQLGlotAsyncConnection: An async-enabled connection
    """
    adapter_kwargs = adapter_kwargs or {}
    enabled_adapters = registry.load_all(adapters, safe)

    # replace entry point names with class names
    mapping = {
        name: adapter.__name__.lower() for name, adapter in enabled_adapters.items()
    }
    adapter_kwargs = {mapping[k]: v for k, v in adapter_kwargs.items() if k in mapping}

    return SQLGlotAsyncConnection(
        list(enabled_adapters.values()),
        adapter_kwargs,
        schema,
        safe,
    )
