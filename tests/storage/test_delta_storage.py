import pytest
from pyspark.sql import functions as F
from flow4df import table_identifier

from flow4df.table_identifier import TableIdentifier
from flow4df.storage.delta_storage import DeltaStorage
from flow4df.storage_backend.local_storage_backend import LocalStorageBackend
from flow4df.partitioning import Partitioning
from flow4df.common import NamedColumn


def test_init_1() -> None:
    t1 = TableIdentifier(
        database='dummydb',
        name='fct_event',
        version=1
    )
    backend = LocalStorageBackend(prefix='/tmp')
    time_monotonic = [
        NamedColumn('my_date', lambda: F.to_date('timestamp')),
    ]
    partitioning = Partitioning(
        time_non_monotonic=[],
        time_monotonic_increasing=time_monotonic
    )
    delta_storage = DeltaStorage(
        table_identifier=t1,
        storage_backend=backend,
        partitioning=partitioning,
        stateful_query_source=True,
    )
    expected = 'file:///tmp/dummydb/fct_event_v1.delta/_checkpoint'
    returned = delta_storage.build_checkpoint_location(
        checkpoint_dir='_checkpoint'
    )
    assert expected == returned


def test_init_2() -> None:
    t1 = TableIdentifier(
        database='dummydb',
        name='fct_event',
        version=1
    )
    backend = LocalStorageBackend(prefix='/tmp')
    # Stateful query sources require at least 1 column in
    # `time_monotonic_increasing`
    partitioning = Partitioning(
        time_non_monotonic=[],
        time_monotonic_increasing=[]
    )
    with pytest.raises(AssertionError):
        DeltaStorage(
            table_identifier=t1,
            storage_backend=backend,
            partitioning=partitioning,
            stateful_query_source=True,
        )
