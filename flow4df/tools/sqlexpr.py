from pyspark.sql import Row
import datetime
from numbers import Number
from typing import Any
import datetime as dt

r = Row(event_year=2025, event_date=datetime.date(2025, 10, 31))
r = Row(event_year=2025, event_date=datetime.date(2025, 10, 31), planet=None)
r = Row(event_year=2025, event_date=datetime.date(2025, 10, 31), planet='Earth')
r = Row(
    event_year=2025,
    event_date=datetime.datetime(2025, 10, 31, 1, 2, 10, tzinfo=datetime.UTC),
    planet='Earth'
)
tz1 = datetime.timezone(datetime.timedelta(hours=3))
r = Row(
    event_year=2025,
    event_date=datetime.datetime(2025, 10, 31, 1, 2, 10, tzinfo=tz1),
    planet='Earth'
)


def sql_condition(column_name: str, value: Any) -> str:
    """TODO: only simple data types should be supported!"""
    _v = value
    if value is None:
        _v = 'NULL'
    elif isinstance(value, dt.datetime):
        unix_ts = value.timestamp()
        ts = dt.datetime.fromtimestamp(unix_ts, tz=dt.UTC)
        _v = f'"{ts.isoformat()}"'
    elif not isinstance(value, Number):
        _v = f'"{value}"'

    return f'{column_name} <=> {_v}'


def row_to_sql_filter(row: Row) -> str:
    conditions = [
        sql_condition(column_name=k, value=v) for k, v in row.asDict().items()
    ]
    return ' AND '.join(conditions)


# row_to_sql_filter(r)
