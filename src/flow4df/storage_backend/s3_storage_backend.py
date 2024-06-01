# @dataclass(frozen=False, kw_only=True)
# class S3Storage(StorageBackend):
#     bucket_name: str

#     def build_location(
#         self,
#         database: str,
#         table_name: str,
#         version: str,
#         table_suffix: str | None = None
#     ) -> str:
#         table_component = f'{table_name}_v{version}'
#         if table_suffix is not None:
#             table_component = f'{table_component}.{table_suffix}'

#         _bucket = CloudPath(f's3://{self.bucket_name}')
#         path = _bucket.joinpath(database, table_component)
#         return path.as_uri()

#     def build_checkpoint_location(
#         self,
#         database: str,
#         table_name: str,
#         version: str,
#         table_suffix: str | None = None
#     ) -> str:
#         table_component = f'{table_name}_v{version}'
#         if table_suffix is not None:
#             table_component = f'{table_component}.{table_suffix}'

#         _bucket = CloudPath(f's3://{self.bucket_name}')
#         path = _bucket.joinpath(database, table_component, '_checkpoint')
#         return path.as_uri()
