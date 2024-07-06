import flow4df
import functools
from pathlib import Path
from pyspark.sql import types as T, functions as F
from pyspark.sql import SparkSession

from flow4df.common import NamedColumn

module_path = Path(__file__)
cases_path = module_path.parent.parent
stubs_path = cases_path.joinpath('stubs')

schema = T.StructType([
    T.StructField('id', T.LongType(), True),
    T.StructField('n', T.LongType(), True),
    T.StructField('group', T.StringType(), True),
    # T.StructField('date', T.DateType(), True)
])


def execute(
    spark: SparkSession,
    this_storage: flow4df.Storage,
    upstream_storages: flow4df.UpstreamStorages,
    trigger: flow4df.Trigger | None,
    data_interval: flow4df.DataInterval | None
) -> None:
    del upstream_storages, trigger, data_interval
    source_df = spark.range(10)
    source_df = source_df.withColumns({
        'n': F.lit(0).cast('long'),
        'group': F.lit('some_group')
    })
    assert isinstance(this_storage, flow4df.DeltaStorage)
    current_dt = this_storage.build_delta_table(spark=spark)

    merge_builder = current_dt.alias('current').merge(
        source=source_df.alias('source'),
        condition='current.id = source.id'
    )
    column_updates = {
        'n': F.col('current.n') + F.lit(1)
    }
    merge_builder = (
        merge_builder
        .whenMatchedUpdate(set=column_updates)  # type: ignore
        .whenNotMatchedInsertAll()
    )
    merge_builder.execute()
    return None


transformation = flow4df.GenericTransformation(
    generic_execute=execute,
)
table_identifier = flow4df.TableIdentifier(
    database='dummydb',
    name='trf_generic_event',
    version=1
)
partitioning = flow4df.Partitioning(
    time_non_monotonic=[
        flow4df.NamedColumn(
            name='group', column_thunk=lambda: F.lit('some_group')
        )
    ],
    time_monotonic_increasing=[]
)
build_storage = functools.partial(
    flow4df.DeltaStorage,
    table_identifier=table_identifier,
    partitioning=partitioning,
    stateful_query_source=False,
)
storage = build_storage(
    storage_backend=flow4df.LocalStorageBackend(prefix='/tmp')
)
storage_stub = build_storage(
    storage_backend=flow4df.LocalStorageBackend(prefix=stubs_path.as_posix())
)

trf_generic_event_v1 = flow4df.TableNode(
    schema=schema,
    upstream_table_nodes=[],
    transformation=transformation,
    storage=storage,
    storage_stub=storage_stub,
)
