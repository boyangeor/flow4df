from enum import Enum
from dataclasses import dataclass
from flow4df.table_identifier import TableIdentifier
from flow4df.storage_backend import StorageBackend

class S3Client(Enum):
    s3 = 's3'
    s3a = 's3a'
    s3n = 's3n'


@dataclass(frozen=False, kw_only=True)
class S3StorageBackend(StorageBackend):
    bucket_name: str
    s3_client: S3Client = S3Client.s3
    prefix: str | None = None

    def build_location(
        self,
        table_identifier: TableIdentifier,
        table_suffix: str | None = None
    ) -> str:
        table_component = table_identifier.table_path_component
        if table_suffix is not None:
            table_component = f'{table_component}.{table_suffix}'

        _pref = f'{self.s3_client.value}://{self.bucket_name}/{self.prefix}'
        db = table_identifier.database
        return f'{_pref}/{db}/{table_component}'

    def build_checkpoint_location(
        self,
        table_identifier: TableIdentifier,
        checkpoint_dir: str,
        table_suffix: str | None = None
    ) -> str:
        location = self.build_location(
            table_identifier=table_identifier,
            table_suffix=table_suffix
        )
        return f'{location}/{checkpoint_dir}'
