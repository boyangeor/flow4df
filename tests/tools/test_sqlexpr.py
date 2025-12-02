import pytest
import datetime as dt
from pyspark.sql import Row
from flow4df.tools import sqlexpr


expression_tests = [
    (
        Row(planet='Earth', year=2025),
        'planet <=> "Earth" AND year <=> 2025',
    ),
    (
        Row(planet='Earth', x=3.14),
        'planet <=> "Earth" AND x <=> 3.14',
    ),
    (
        Row(planet='Earth', year=None),
        'planet <=> "Earth" AND year <=> NULL',
    ),
    (
        Row(event_year=2025, event_date=dt.date(2025, 1, 1)),
        'event_year <=> 2025 AND event_date <=> "2025-01-01"',
    ),
    (
        Row(event_year=None, event_date=None),
        'event_year <=> NULL AND event_date <=> NULL',
    ),
    (
        Row(
            event_date=dt.date(2025, 1, 1),
            event_hour=dt.datetime(2025, 1, 1, 1, tzinfo=dt.UTC),
        ),
        (
            'event_date <=> "2025-01-01" AND '
            'event_hour <=> "2025-01-01T01:00:00+00:00"'
        ),
    ),
]


@pytest.mark.parametrize('test_args', expression_tests)
def test_row_to_sql_filter(test_args: tuple[Row, str]) -> None:
    row, expected = test_args
    actual = sqlexpr.row_to_sql_filter(row)
    assert actual == expected
