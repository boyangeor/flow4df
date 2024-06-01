import functools
from dataclasses import dataclass


@dataclass(kw_only=True, frozen=True)
class TableIdentifier:
    database: str
    name: str
    version: int

    @functools.cached_property
    def table_id(self) -> str:
        return f'{self.database}.{self.name}_v{self.version}'
