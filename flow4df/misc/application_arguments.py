from __future__ import annotations
import json
import inspect
import importlib
from dataclasses import dataclass, field
from collections.abc import Callable
from flow4df import Table
from flow4df.table_index import TableIndex

list_field_args = {'default_factory': list, 'repr': False}


@dataclass(frozen=False, kw_only=True)
class ApplicationArguments:
    main: Callable[..., None]
    active_tables: list[Table] = field(**list_field_args)
    tables: list[Table] = field(**list_field_args)

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
        return {
            'main': main_dict_repr,
            'active_tables': serialized_active_tables,
            'tables': serialized_tables
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict())

    @staticmethod
    def from_json(
        json_string: str, table_index: TableIndex
    ) -> ApplicationArguments:
        d = json.loads(json_string)
        main_module = importlib.import_module(d['main']['module_name'])
        main_function = getattr(main_module, d['main']['function_name'])

        active_tables = [
            table_index.get_active_table(**table_args)
            for table_args in d['active_tables']
        ]
        tables = [
            table_index.get_table(**table_args)
            for table_args in d['tables']
        ]
        return ApplicationArguments(
            main=main_function, active_tables=active_tables, tables=tables,
        )
