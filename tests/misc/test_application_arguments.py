import pytest
import datetime as dt
from flow4df.misc import entry_point_main
from flow4df.misc.application_arguments import ApplicationArguments
from flow4df import DataInterval

serialization_tests = [
    {
        'main': entry_point_main.f1,
        'data_interval': None,
    },
    {
        'main': entry_point_main.f1,
        'data_interval': DataInterval(
            start=dt.datetime(2026, 1, 1, 10, tzinfo=dt.UTC),
            end=dt.datetime(2026, 1, 1, 19, tzinfo=dt.UTC),
        ),
    },
    {
        'main': entry_point_main.f1,
        'active_tables': [],
        'tables': [],
        'data_interval': DataInterval(
            start=dt.datetime(2026, 1, 1, 10, tzinfo=dt.UTC),
            end=dt.datetime(2026, 1, 29, 10, tzinfo=dt.UTC),
        ),
    }
]


@pytest.mark.parametrize('init_args', serialization_tests)
def test_app_args_serialization_1(init_args: dict) -> None:
    app_args = ApplicationArguments(**init_args)
    raw_json = app_args.to_json()

    parsed = ApplicationArguments.from_json(raw_json, None)  # type: ignore
    raw_json2 = parsed.to_json()
    assert raw_json == raw_json2
