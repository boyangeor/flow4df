import flow4df
import difflib
import functools
from collections import Counter
from dataclasses import dataclass


@dataclass(frozen=True, kw_only=True)
class TableIndex:
    """TODO doc."""
    package: str

    @staticmethod
    def assert_single_active_table(active_tables: list[flow4df.Table]) -> None:
        versionless_names = [
            t.table_identifier.versionless_name for t in active_tables
        ]
        counter = Counter(versionless_names)
        with_multiple_versions = [
            name for name, count in counter.items() if count > 1
        ]
        _m = f'Multiple versions for: {with_multiple_versions} !'
        assert len(with_multiple_versions) == 0, _m
        return None

    @property  # Doesn't work if cached!
    def all_tables(self) -> list[flow4df.Table]:
        modules = flow4df.tools.module.list_modules(package=self.package)
        _tables = [flow4df.Table.find_table_in_module(m) for m in modules]
        return [t for t in _tables if t is not None]

    def raise_no_matching(
        identifier: tuple[str, ...], tables: list[flow4df.Table]
    ) -> None:
        possibilities = [
            t.table_identifier.versionless_name for t in tables
        ]
        table_name = '.'.join(identifier)
        closest = difflib.get_close_matches(
            word=table_name, possibilities=possibilities, n=3, cutoff=0
        )
        _m = (
            f'Cannot find table `{table_name}`. '
            f'Did you mean one of {closest}?'
        )
        raise ValueError(_m)

    @functools.cache
    def get_active_table(
        self, catalog: str, schema: str, name: str
    ) -> flow4df.Table:
        active_tables = [t for t in self.all_tables if t.is_active]
        TableIndex.assert_single_active_table(active_tables)
        for at in active_tables:
            is_same = at.table_identifier.is_semantically_equivalent(
                catalog=catalog, schema=schema, name=name,
            )
            if is_same:
                return at

        self.raise_no_matching(
            identifier=(catalog, schema, name),
            tables=active_tables
        )

    @functools.cache
    def get_table(
        self, catalog: str, schema: str, name: str, version: str
    ) -> flow4df.Table:
        all_tables = self.all_tables
        requested_identifier = flow4df.TableIdentifier(
            catalog=catalog, schema=schema, name=name, version=version
        )
        for table in self.all_tables:
            if table.table_identifier == requested_identifier:
                return table

        self.raise_no_matching(
            identifier=(catalog, schema, name, version),
            tables=all_tables
        )
