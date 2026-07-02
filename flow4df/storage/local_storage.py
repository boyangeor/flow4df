from pathlib import Path
from dataclasses import dataclass
from flow4df import TableIdentifier
from flow4df.storage.storage import Storage


@dataclass(frozen=True, kw_only=True)
class LocalStorage(Storage):
    prefix: str

    def _build_table_component(self, table_identifier: TableIdentifier) -> str:
        return f'{table_identifier.name}_v{table_identifier.version}'

    def build_location(self, table_identifier: TableIdentifier) -> str:
        table_component = self._build_table_component(table_identifier)
        path = Path(
            self.prefix,
            table_identifier.catalog,
            table_identifier.schema,
            table_component
        )
        return path.as_posix()

    def build_canonical_location(
        self, table_identifier: TableIdentifier
    ) -> str:
        return self.build_location(table_identifier)

    def build_checkpoint_location(
        self,
        table_identifier: TableIdentifier,
        checkpoint_dir: str = '_checkpoint',
    ) -> str:
        table_component = self._build_table_component(table_identifier)
        path = Path(
            self.prefix,
            table_identifier.catalog,
            table_identifier.schema,
            table_component,
            checkpoint_dir
        )
        return path.as_posix()

    def build_catalog_location(self, table_identifier: TableIdentifier) -> str:
        path = Path(self.prefix, table_identifier.catalog)
        return path.as_posix()
