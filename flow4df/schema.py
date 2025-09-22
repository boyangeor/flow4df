import flow4df
import difflib
from dataclasses import dataclass


@dataclass(frozen=False, kw_only=True)
class Schema:
    package: str

    def list_tables(self) -> list[flow4df.Table]:
        modules = flow4df.tools.module.list_modules(package=self.package)
        _tables = [flow4df.Table.find_table_in_module(m) for m in modules]
        tables = [t for t in _tables if t is not None]
        active_tables = [t for t in tables if t.is_active]
        return active_tables

    def get_table(self, table_name: str) -> flow4df.Table:
        active_tables = self.list_tables()
        for table in active_tables:
            if table.table_identifier.name == table_name:
                return table

        possibilities = [t.table_identifier.name for t in active_tables]
        closest = difflib.get_close_matches(
            word=table_name, possibilities=possibilities, n=3, cutoff=0
        )
        _m = (
            f'Cannot find table `{table_name}`. '
            f'Did you mean one of {closest}?'
        )
        raise ValueError(_m)
