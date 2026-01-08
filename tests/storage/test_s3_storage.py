import pytest
from flow4df import TableIdentifier
from flow4df import S3Storage

location_tests = [
    (
        TableIdentifier(
            catalog='catalogX',
            schema='schemaY',
            name='fct_tableZ',
            version='1'
        ),
        S3Storage(
            bucket_name='some_secret_bucket',
            s3_client='s3',
            prefix='dwh'
        ),
        's3://some_secret_bucket/dwh/catalogX/schemaY/fct_tableZ_v1',
        's3://some_secret_bucket/dwh/catalogX/schemaY/fct_tableZ_v1/_checkpoint',
    ),
    (
        TableIdentifier(
            catalog='catalogX',
            schema='schemaY',
            name='dim_city',
            version='2'
        ),
        S3Storage(
            bucket_name='other_secret_bucket',
            s3_client='s3a',
            prefix='dwh'
        ),
        's3a://other_secret_bucket/dwh/catalogX/schemaY/dim_city_v2',
        's3a://other_secret_bucket/dwh/catalogX/schemaY/dim_city_v2/_checkpoint',
    ),
]


@pytest.mark.parametrize('test_args', location_tests)
def test_build_location(
    test_args: tuple[TableIdentifier, S3Storage, str, str]
) -> None:
    ti, storage, expected_location, expected_cp_location = test_args
    created_location = storage.build_location(ti)
    created_cp_location = storage.build_checkpoint_location(ti)
    assert created_location == expected_location
    assert created_cp_location == expected_cp_location
