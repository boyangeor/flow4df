from flow4df.table_identifier import TableIdentifier


def test_table_identifier_init() -> None:
    table_identifier = TableIdentifier(
        database='dummydb',
        name='fct_event',
        version=1
    )
    assert table_identifier.table_id == 'dummydb.fct_event_v1'
