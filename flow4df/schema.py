from __future__ import annotations
from dataclasses import dataclass, field
from collections import Counter
import difflib
import flow4df


@dataclass(frozen=False, kw_only=True)
class Schema:
    tables: list[flow4df.Table]
    name: str = field(init=False, repr=True)

    def __post_init__(self) -> None:
        assert len(self.tables) > 0, 'At least 1 Table required.'
        statuses = [t.is_active for t in self.tables]
        _m = 'Only active Table instances required.'
        assert all(statuses), _m
        self._assert_unique_tables()
        self._assert_same_schema()
        self.name = self.tables[0].table_identifier.schema
        self._table_dict = {
            t.table_identifier.name: t for t in self.tables
        }

    def _assert_unique_tables(self) -> None:
        table_names = [t.table_identifier.name for t in self.tables]
        counted_names = Counter(table_names)
        most_common_name = counted_names.most_common(1)
        name, n = most_common_name[0]
        _m = (
            f'Cannot have {n} active versions of `{name}`. '
            'Make sure there is a single active version per Table.'
        )
        assert n == 1, _m
        return None

    def _assert_same_schema(self) -> None:
        distinct_schemas = {t.table_identifier.schema for t in self.tables}
        _m = 'All Tables must have the same schema!'
        assert len(distinct_schemas) == 1, _m
        return None

    def __getitem__(self, name: str) -> flow4df.Table:
        if name not in self._table_dict:
            closest = difflib.get_close_matches(
                word=name, possibilities=self._table_dict.keys(), n=3, cutoff=0
            )
            _m = f'Cannot find table `{name}`. Did you mean one of {closest}?'
            raise ValueError(_m)
        return self._table_dict[name]

    @staticmethod
    def from_package(package: str) -> Schema:
        modules = flow4df.tools.module.list_modules(package=package)
        _tables = [flow4df.Table.find_table_in_module(m) for m in modules]
        tables = [t for t in _tables if t is not None]
        active_tables = [t for t in tables if t.is_active]
        return Schema(tables=active_tables)
