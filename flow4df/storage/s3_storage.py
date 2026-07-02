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

    def build_location(self, table_identifier: TableIdentifier) -> str:
        return self._build_location(
            table_identifier=table_identifier, s3_client=self.s3_client
        )

    def build_canonical_location(
        self, table_identifier: TableIdentifier
    ) -> str:
        return self._build_location(
            table_identifier=table_identifier, s3_client='s3',
        )

    def build_checkpoint_location(
        self,
        table_identifier: TableIdentifier,
        checkpoint_dir: str = '_checkpoint',
    ) -> str:
        location = self.build_location(
            table_identifier=table_identifier,
        )
        return f'{location}/{checkpoint_dir}'

    def build_catalog_location(self, table_identifier: TableIdentifier) -> str:
        del table_identifier
        return f'{self.s3_client}://{self.bucket_name}/{self.prefix}'


    def _build_location(
        self,
        table_identifier: TableIdentifier,
        s3_client: S3Client,
    ) -> str:
        table_component = (
            f'{table_identifier.name}_v{table_identifier.version}'
        )
        _pref = f'{s3_client}://{self.bucket_name}/{self.prefix}'
        components = [
            _pref,
            table_identifier.catalog,
            table_identifier.schema,
            table_component
        ]
        return '/'.join(components)
