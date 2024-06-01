from dataclasses import dataclass
from flow4df.storage import Storage


@dataclass(frozen=True, kw_only=False)
class UpstreamStorages:
    storages: list[Storage]

    def find_storage(self, table_query: str) -> Storage:
        # TODO: add docstring, decide on fuzzy matching
        matched = [
            e for e in self.storages if e.table_identifier.name == table_query
        ]
        assert len(matched) == 1
        return matched[0]
