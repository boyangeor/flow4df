from typing import Protocol
from flow4df.table_identifier import TableIdentifier


class Storage(Protocol):
    def build_location(self, table_identifier: TableIdentifier) -> str:
        ...

    def build_checkpoint_location(
        self, table_identifier: TableIdentifier, checkpoint_dir: str,
    ) -> str:
        ...
