from typing import Protocol
from flow4df.table_identifier import TableIdentifier


class StorageBackend(Protocol):
    def build_location(
        self,
        table_identifier: TableIdentifier,
        table_suffix: str | None = None,
    ) -> str:
        ...

    def build_checkpoint_location(
        self,
        table_identifier: TableIdentifier,
        checkpoint_dir: str,
        table_suffix: str | None = None,
    ) -> str:
        ...
