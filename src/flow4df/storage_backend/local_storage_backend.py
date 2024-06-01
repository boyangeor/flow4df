from pathlib import Path
from dataclasses import dataclass
from flow4df.table_identifier import TableIdentifier
from flow4df.storage_backend import StorageBackend


@dataclass(frozen=True, kw_only=True)
class LocalStorageBackend(StorageBackend):
    prefix: str

    def _build_table_component(
        self,
        table_identifier: TableIdentifier,
        table_suffix: str | None = None
    ) -> str:
        table_component = (
            f'{table_identifier.name}_v{table_identifier.version}'
        )
        if table_suffix is not None:
            table_component = f'{table_component}.{table_suffix}'

        return table_component

    def build_location(
        self,
        table_identifier: TableIdentifier,
        table_suffix: str | None = None
    ) -> str:
        table_component = self._build_table_component(
            table_identifier=table_identifier, table_suffix=table_suffix
        )
        path = Path(self.prefix, table_identifier.database, table_component)
        return path.as_uri()

    def build_checkpoint_location(
        self,
        table_identifier: TableIdentifier,
        checkpoint_dir: str,
        table_suffix: str | None = None
    ) -> str:
        table_component = self._build_table_component(
            table_identifier=table_identifier, table_suffix=table_suffix
        )
        db = table_identifier.database
        path = Path(self.prefix, db, table_component, checkpoint_dir)
        return path.as_uri()
