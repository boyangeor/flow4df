from __future__ import annotations
import json
import inspect
import importlib
from dataclasses import dataclass, field
from collections.abc import Callable
from flow4df import Table, DataInterval
from flow4df.table import TableIndex

active_tables_default = field(default_factory=list, repr=False)


@dataclass(frozen=False, kw_only=True)
class ApplicationArguments:
    main: Callable[..., None]
    active_tables: list[Table] = active_tables_default
    tables: list[Table] = field(default_factory=list, repr=False)
    data_interval: DataInterval | None = None

    def as_dict(self) -> dict:
        module = inspect.getmodule(self.main)
        _m = f'Cannot find module of {self.main}'
        assert module is not None, _m
        main_dict_repr = {
            'module_name': module.__name__,
            'function_name': self.main.__name__
        }
        serialized_active_tables = [
            table.table_identifier.semantic_dict
            for table in self.active_tables
        ]
        serialized_tables = [
            table.table_identifier.as_dict()
            for table in self.tables
        ]
        data_interval_dict = None
        if self.data_interval is not None:
            data_interval_dict = self.data_interval.as_dict()

        return {
            'main': main_dict_repr,
            'active_tables': serialized_active_tables,
            'tables': serialized_tables,
            'data_interval': data_interval_dict,
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), default=lambda x: x.isoformat())

    @staticmethod
    def from_json(
        json_string: str, table_index: TableIndex
    ) -> ApplicationArguments:
        d = json.loads(json_string)
        main_module = importlib.import_module(d['main']['module_name'])
        main_function = getattr(main_module, d['main']['function_name'])

        active_tables = [
            table_index.get_active_table(**table_args)
            for table_args in d.get('active_tables', [])
        ]
        tables = [
            table_index.get_table(**table_args)
            for table_args in d.get('tables', [])
        ]
        data_interval_dict = d.get('data_interval')
        data_interval = data_interval_dict
        if data_interval_dict is not None:
            data_interval = DataInterval.from_iso_formatted_timestamps(
                start_iso_timestamp=data_interval_dict['start'],
                end_iso_timestamp=data_interval_dict['end']
            )

        return ApplicationArguments(
            main=main_function,
            active_tables=active_tables,
            tables=tables,
            data_interval=data_interval
        )
