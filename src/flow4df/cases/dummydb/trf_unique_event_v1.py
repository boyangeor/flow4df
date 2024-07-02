import flow4df
import functools
from pathlib import Path
from pyspark.sql import types as T, functions as F
from pyspark.sql import SparkSession, DataFrame
from flow4df.cases import dummydb

module_path = Path(__file__)
cases_path = module_path.parent.parent
stubs_path = cases_path.joinpath('stubs')

schema = T.StructType([
    T.StructField('timestamp', T.TimestampType(), True),
    T.StructField('value', T.LongType(), True),
    T.StructField('value_group', T.ByteType(), True),
    T.StructField('date', T.DateType(), True)
])


def build_streaming_df(
    spark: SparkSession, upstream_storages: flow4df.UpstreamStorages
) -> DataFrame:
    trf_event_storage = upstream_storages.find_storage(table_query='trf_event')
    trf_event_df = trf_event_storage.build_streaming_df(spark=spark)
    return trf_event_df


def foreach_batch_execute(
    spark: SparkSession,
    this_storage: flow4df.Storage,
    batch_df: DataFrame,
    epoch_id: int
) -> None:
    del epoch_id
    assert isinstance(this_storage, flow4df.DeltaStorage)
    unique_event_dt = this_storage.build_delta_table(spark=spark)

    batch_df.cache()
    min_date = batch_df.select(F.min('date')).collect()[0][0]

    condition = ' AND '.join([
        'event.value = source.value',
        # To restrict the search space for the MERGE
        f'event.date >= "{min_date.isoformat()}"'
    ])
    merge_builder = unique_event_dt.alias('event').merge(
        source=batch_df.alias('source'),
        condition=condition,
    )
    merge_builder = merge_builder.whenNotMatchedInsertAll()
    merge_builder.execute()
    batch_df.unpersist()


transformation = flow4df.ForeachBatchStreamingTransformation(
    build_streaming_df=build_streaming_df,
    foreach_batch_execute=foreach_batch_execute,
    default_trigger={'availableNow': True}
)
table_identifier = flow4df.TableIdentifier(
    database='dummydb',
    name='trf_unique_event',
    version=1
)
partitioning = flow4df.Partitioning(
    time_non_monotonic=[
        flow4df.NamedColumn(
            name='value_group',
            column_thunk=lambda: (F.col('value') % 3).cast(T.ByteType())
        )
    ],
    time_monotonic_increasing=[
        flow4df.NamedColumn(
            name='date', column_thunk=lambda: F.to_date('timestamp')
        )
    ]
)
build_storage = functools.partial(
    flow4df.DeltaStorage,
    table_identifier=table_identifier,
    partitioning=partitioning,
    stateful_query_source=True,
    z_order_by='value'
)
storage = build_storage(
    storage_backend=flow4df.LocalStorageBackend(prefix='/tmp')
)
storage_stub = build_storage(
    storage_backend=flow4df.LocalStorageBackend(prefix=stubs_path.as_posix())
)

trf_unique_event_v1 = flow4df.TableNode(
    schema=schema,
    upstream_table_nodes=[dummydb.trf_event_v1],
    transformation=transformation,
    storage=storage,
    storage_stub=storage_stub,
)
