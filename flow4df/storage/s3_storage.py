from typing import Literal
from dataclasses import dataclass
from flow4df.table_identifier import TableIdentifier
from flow4df.storage.storage import Storage

S3Client = Literal['s3', 's3a', 's3n']


@dataclass(frozen=True, kw_only=True)
class S3Storage(Storage):
    bucket_name: str
    s3_client: S3Client = 's3'
    prefix: str = 'dwh'

    def build_location(
        self,
        table_identifier: TableIdentifier,
        table_suffix: str | None = None
    ) -> str:
        table_component = (
            f'{table_identifier.name}_v{table_identifier.version}'
        )
        if table_suffix is not None:
            table_component = f'{table_component}.{table_suffix}'

        _pref = f'{self.s3_client}://{self.bucket_name}/{self.prefix}'
        components = [
            _pref,
            table_identifier.catalog,
            table_identifier.schema,
            table_component
        ]
        return '/'.join(components)

    def build_checkpoint_location(
        self,
        table_identifier: TableIdentifier,
        checkpoint_dir: str = '_checkpoint',
        table_suffix: str | None = None
    ) -> str:
        location = self.build_location(
            table_identifier=table_identifier,
            table_suffix=table_suffix
        )
        return f'{location}/{checkpoint_dir}'

    def build_catalog_location(self, table_identifier: TableIdentifier) -> str:
        return f'{self.s3_client}://{self.bucket_name}/{self.prefix}'
