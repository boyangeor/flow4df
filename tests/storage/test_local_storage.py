import pytest
from flow4df import TableIdentifier
from flow4df import LocalStorage

location_tests = [
    (
        TableIdentifier(
            catalog='catalogX',
            schema='schemaY',
            name='fct_tableZ',
            version='1'
        ),
        'file:///tmp/mydir/catalogX/schemaY/fct_tableZ_v1',
        'file:///tmp/mydir/catalogX/schemaY/fct_tableZ_v1/_checkpoint',
    ),
    (
        TableIdentifier(
            catalog='my_catalog',
            schema='my_schema',
            name='dim_city',
            version='2'
        ),
        'file:///tmp/mydir/my_catalog/my_schema/dim_city_v2',
        'file:///tmp/mydir/my_catalog/my_schema/dim_city_v2/_checkpoint',
    ),
]


@pytest.mark.parametrize('test_args', location_tests)
def test_build_location(
    test_args: tuple[TableIdentifier, str, str]
) -> None:
    ti, expected_location, expected_cp_location = test_args
    storage = LocalStorage(prefix='/tmp/mydir')
    created_location = storage.build_location(ti)
    created_cp_location = storage.build_checkpoint_location(ti)
    assert created_location == expected_location
    assert created_cp_location == expected_cp_location


def test_custom_cp_location() -> None:
    storage = LocalStorage(prefix='/tmp/mydir')
    identifier = TableIdentifier(
        catalog='catalogX',
        schema='schemaY',
        name='fct_tableZ',
        version='1'
    )
    expected = (
        'file:///tmp/mydir/catalogX/schemaY/fct_tableZ_v1/_custom_checkpoint'
    )
    actual = storage.build_checkpoint_location(
        table_identifier=identifier,
        checkpoint_dir='_custom_checkpoint'
    )
    assert actual == expected
