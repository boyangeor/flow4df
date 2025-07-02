import pytest
from flow4df.table_identifier import TableIdentifier

init_tests = [
    (
        'myproject.catalogX.schemaY.fct_tableZ_v1.table',
        TableIdentifier(
            catalog='catalogX',
            schema='schemaY',
            name='fct_tableZ',
            version='1'
        ),
    ),
    (
        'myproject.my_catalog.my_schema.dim_city_v2',
        TableIdentifier(
            catalog='my_catalog',
            schema='my_schema',
            name='dim_city',
            version='2'
        ),
    ),
    (
        'dwhproject.MyCatalog.MySchema.dim_city_v22',
        TableIdentifier(
            catalog='MyCatalog',
            schema='MySchema',
            name='dim_city',
            version='22'
        ),
    ),
]


@pytest.mark.parametrize('mod_name_expected', init_tests)
def test_table_identifier_init(
    mod_name_expected: tuple[str, TableIdentifier]
) -> None:
    mod_name, expected = mod_name_expected
    created = TableIdentifier.from_module_name(mod_name)
    assert created == expected
