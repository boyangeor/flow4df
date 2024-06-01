from flow4df import table_identifier
from flow4df.table_identifier import TableIdentifier
from flow4df.storage_backend.local_storage_backend import LocalStorageBackend

def test_local_storage_backend_1() -> None:
    table_identifier = TableIdentifier(
        database='dummydb',
        name='trf_event',
        version=1
    )
    backend = LocalStorageBackend(prefix='/tmp')
    location = backend.build_location(
        table_identifier=table_identifier, table_suffix='delta'
    )
    assert location == 'file:///tmp/dummydb/trf_event_v1.delta'


def test_local_storage_backend_2() -> None:
    table_identifier = TableIdentifier(
        database='dummydb',
        name='trf_event',
        version=1
    )
    backend = LocalStorageBackend(prefix='/tmp')
    cp_location = backend.build_checkpoint_location(
        table_identifier=table_identifier,
        table_suffix='delta',
        checkpoint_dir='_checkpoint'
    )
    expected = 'file:///tmp/dummydb/trf_event_v1.delta/_checkpoint'
    assert cp_location == expected
