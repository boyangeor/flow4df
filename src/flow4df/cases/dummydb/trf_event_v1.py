import flow4df
import functools
from pathlib import Path
from pyspark.sql import types as T, functions as F
from pyspark.sql import SparkSession, DataFrame

module_path = Path(__file__)
cases_path = module_path.parent.parent
stubs_path = cases_path.joinpath('stubs')

schema = T.StructType([
    T.StructField('timestamp', T.TimestampType(), True),
    T.StructField('value', T.LongType(), True),
    T.StructField('value_group', T.ByteType(), True),
    T.StructField('date', T.DateType(), True)
])


def my_transform(
    spark: SparkSession, upstream_storages: flow4df.UpstreamStorages
) -> DataFrame:
    _ = upstream_storages
    df = (
        spark
        .readStream
        .format('rate-micro-batch')
        .options(**{
            'rowsPerBatch': 20,
            'advanceMillisPerBatch': 14 * 3600 * 1000,  # 14 hours
        })
        .load()
    )
    return df


transformation = flow4df.StructuredStreamingTransformation(
    transform=my_transform,
    output_mode=flow4df.OutputMode.append,
    default_trigger={'processingTime': '15 seconds'}
)

table_identifier = flow4df.TableIdentifier(
    database='dummydb',
    name='trf_event',
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
    overwrite_partition=True,
    z_order_by='value',
)
storage = build_storage(
    storage_backend=flow4df.LocalStorageBackend(prefix='/tmp')
)
storage_stub = build_storage(
    storage_backend=flow4df.LocalStorageBackend(prefix=stubs_path.as_posix())
)

trf_event_v1 = flow4df.TableNode(
    schema=schema,
    upstream_table_nodes=[],
    transformation=transformation,
    storage=storage,
    storage_stub=storage_stub,
)
