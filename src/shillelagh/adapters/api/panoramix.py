import datetime
import operator
import urllib.parse
from typing import Any
from typing import Dict
from typing import Iterator
from typing import Optional
from typing import Tuple
from typing import Type

from flask import g
from flask_login import current_user
from shillelagh.adapters.base import Adapter
from shillelagh.exceptions import ProgrammingError
from shillelagh.fields import Blob
from shillelagh.fields import Boolean
from shillelagh.fields import Date
from shillelagh.fields import DateTime
from shillelagh.fields import Field
from shillelagh.fields import Float
from shillelagh.fields import Integer
from shillelagh.fields import Order
from shillelagh.fields import String
from shillelagh.fields import Time
from shillelagh.filters import Equal
from shillelagh.filters import Filter
from shillelagh.filters import Range
from shillelagh.types import Row
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy.sql import Select
from sqlalchemy.sql import select
from superset import db
from superset import security_manager
from superset import sql_parse
from superset.models.core import Database


type_map: Dict[Any, Type[Field]] = {
    bool: Boolean,
    float: Float,
    int: Integer,
    str: String,
    datetime.date: Date,
    datetime.datetime: DateTime,
    datetime.time: Time,
}


def get_field(python_type: Any) -> Field:
    class_ = type_map.get(python_type, Blob)
    return class_(
        filters=[Equal, Range],
        order=Order.NONE,  # XXX implement Order.ANY
        exact=True,
    )


class SupersetAPI(Adapter):
    @staticmethod
    def supports(uri: str) -> bool:
        parsed = urllib.parse.urlparse(uri)
        parts = parsed.path.split(".")
        return len(parts) in {3, 4, 5} and parts[0] == "superset"

    @staticmethod
    def parse_uri(uri: str) -> Tuple[str, Optional[str], Optional[str], str]:
        parsed = urllib.parse.urlparse(uri)
        parts = parsed.path.split(".")
        if len(parts) == 3:
            return parts[1], None, None, parts[2]
        if len(parts) == 4:
            return parts[1], None, parts[2], parts[3]
        return tuple(parts[1:])  # type: ignore

    def __init__(
        self,
        database: str,
        catalog: Optional[str],
        schema: Optional[str],
        table: str,
    ):
        self.database = database
        self.catalog = catalog
        self.schema = schema
        self.table = table

        self._set_columns()

    def _set_columns(self) -> None:
        database = (
            db.session.query(Database).filter_by(database_name=self.database).one()
        )
        self._check_permission(database)
        self.engine = database.get_sqla_engine()
        metadata = MetaData()
        self._table = Table(
            self.table,
            metadata,
            autoload=True,
            autoload_with=self.engine,
        )

        self.columns = {
            column.name: get_field(column.type.python_type) for column in self._table.c
        }

    def _check_permission(self, database: Database) -> None:
        g.user = current_user
        table = sql_parse.Table(self.table, self.schema, self.catalog)
        security_manager.raise_for_access(database=database, table=table)

    def get_columns(self) -> Dict[str, Field]:
        return self.columns

    def _build_sql(self, bounds: Dict[str, Filter]) -> Select:
        query = select([self._table])

        for column_name, filter_ in bounds.items():
            column = self._table.c[column_name]
            if isinstance(filter_, Equal):
                query = query.where(column == filter_.value)
            elif isinstance(filter_, Range):
                if filter_.start is not None:
                    op = operator.ge if filter_.include_start else operator.gt
                    query = query.where(op(column, filter_.start))
                if filter_.end is not None:
                    op = operator.le if filter_.include_end else operator.ge
                    query = query.where(op(column, filter_.end))
            else:
                raise ProgrammingError(f"Invalid filter: {filter_}")

        return query

    def get_data(self, bounds: Dict[str, Filter]) -> Iterator[Row]:
        query = self._build_sql(bounds)

        connection = self.engine.connect()
        rows = connection.execute(query)
        for i, row in enumerate(rows):
            data = dict(zip(self.columns, row))
            data["rowid"] = i
            yield data
