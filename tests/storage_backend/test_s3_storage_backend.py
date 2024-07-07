from flow4df.table_identifier import TableIdentifier
from flow4df.storage_backend.s3_storage_backend import S3Client
from flow4df.storage_backend.s3_storage_backend import S3StorageBackend

table_identifier = TableIdentifier(
    database='mydb',
    name='trf_event',
    version=2
)

def test_s3_storage_backend_1() -> None:
    backend = S3StorageBackend(
        bucket_name='thisisbucket',
        s3_client=S3Client.s3a,
        prefix='databases'
    )
    actual = backend.build_location(
        table_identifier=table_identifier,
        table_suffix='delta'
    )
    expected = 's3a://thisisbucket/databases/mydb/trf_event_v2.delta'
    assert actual == expected

    backend_s3 = S3StorageBackend(
        bucket_name='thisisbucket',
        s3_client=S3Client.s3,
        prefix='databases'
    )
    cp_actual = backend_s3.build_checkpoint_location(
        table_identifier=table_identifier,
        checkpoint_dir='_checkpoint',
        table_suffix='delta'
    )
    cp_expected = (
        's3://thisisbucket/databases/mydb/trf_event_v2.delta/_checkpoint'
    )
    assert cp_actual == cp_expected
