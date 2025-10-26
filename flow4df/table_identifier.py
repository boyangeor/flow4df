from __future__ import annotations
import re
import functools
from dataclasses import dataclass, asdict

table_module_pattern = re.compile(
    r'(?P<catalog>\w+)\.(?P<schema>\w+)\.'
    r'(?P<name>\w+)_v(?P<version>\w+)(?=\.|\b)'
)
PATTERN = '<catalog>.<schema>.<table_name>_v<version>'


@dataclass(kw_only=True)
class TableIdentifier:
    catalog: str
    schema: str
    name: str
    version: str

    @staticmethod
    def from_module_name(module_name: str) -> TableIdentifier:
        _m = (
            f'Cannot build TableIdentifier from `{module_name}`'
            f'Make sure the `module_name` contains this pattern:\n {PATTERN}'
            f'\n\n e.g. `myproject.catalogX.schemaY.fct_tableZ_v10`'
        )
        search_res = re.search(table_module_pattern, module_name)
        assert search_res is not None, _m
        return TableIdentifier(**search_res.groupdict())

    def as_dict(self) -> dict[str, str]:
        return asdict(self)

    @functools.cached_property
    def table_id(self) -> str:
        return f'{self.catalog}.{self.schema}.{self.name}_v{self.version}'

    @functools.cached_property
    def full_name(self) -> str:
        return f'{self.catalog}.{self.schema}.{self.name}_v{self.version}'

    @functools.cached_property
    def versionless_name(self) -> str:
        return f'{self.catalog}.{self.schema}.{self.name}'

    @functools.cached_property
    def semantic_identifier(self) -> tuple[str, str, str]:
        return (self.catalog, self.schema, self.name)

    @functools.cached_property
    def semantic_dict(self) -> dict[str, str]:
        return {
            'catalog': self.catalog, 'schema': self.schema, 'name': self.name
        }

    def is_semantically_equivalent(
        self, catalog: str, schema: str, name: str
    ) -> bool:
        return all([
            self.catalog == catalog, self.schema == schema, self.name == name
        ])
