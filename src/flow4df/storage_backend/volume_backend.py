from pathlib import Path
from dataclasses import dataclass
from flow4df.table_identifier import TableIdentifier
from flow4df.storage_backend import StorageBackend


@dataclass(frozen=False, kw_only=True)
class VolumeBackend(StorageBackend):
    catalog_name: str
    volume_name: str

    def build_location(
        self,
        table_identifier: TableIdentifier,
        table_suffix: str | None = None
    ) -> str:
        del table_suffix
        p = Path(
            '/Volumes',
            self.catalog_name,
            table_identifier.database,
            self.volume_name,
            table_identifier.table_path_component
        )
        return p.as_posix()

    def build_checkpoint_location(
        self,
        table_identifier: TableIdentifier,
        checkpoint_dir: str,
        table_suffix: str | None = None
    ) -> str:
        location = self.build_location(
            table_identifier=table_identifier, table_suffix=table_suffix
        )
        return f'{location}/{checkpoint_dir}'
