import flow4df
import functools
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, TimestampType, LongType
from pyspark.sql.types import DateType
from flow4df.cases import dummydb

module_path = Path(__file__)
cases_path = module_path.parent.parent
stubs_path = cases_path.joinpath('stubs')

window_type = StructType([
    StructField('start', TimestampType(), True),
    StructField('end', TimestampType(), True),
])
schema = StructType([
    StructField('window', window_type, False),
    StructField('n', LongType(), False),
    StructField('month', DateType(), False),
])


def count_hourly(
    spark: SparkSession, upstream_storages: flow4df.UpstreamStorages
) -> DataFrame:
    # Create DataFrame from the upstream TableNode
    trf_event_storage = upstream_storages.find_storage(table_query='trf_event')
    trf_event_df = trf_event_storage.build_streaming_df(spark=spark)

    window_1d = F.window(timeColumn='timestamp', windowDuration='1 HOUR')
    agg_cols = [
        F.count('*').alias('n'),
    ]
    agf_1d_event = trf_event_df.groupBy(window_1d).agg(*agg_cols)

    return agf_1d_event


transformation = flow4df.StructuredStreamingTransformation(
    transform=count_hourly,
    output_mode=flow4df.OutputMode.append,
    default_trigger={'processingTime': '30 seconds'}
)

table_identifier = flow4df.TableIdentifier(
    database='dummydb',
    name='agf_1h_event',
    version=1
)


def build_month_col() -> Column:
    w_start = F.col('window').getField('start')
    return F.date_trunc('month', w_start).cast(DateType())


partitioning = flow4df.Partitioning(
    time_non_monotonic=[],
    time_monotonic_increasing=[
        flow4df.NamedColumn(name='month', column_thunk=build_month_col)
    ]
)

build_storage = functools.partial(
    flow4df.DeltaStorage,
    table_identifier=table_identifier,
    partitioning=partitioning,
    stateful_query_source=True,
)
storage = build_storage(
    storage_backend=flow4df.LocalStorageBackend(prefix='/tmp')
)
storage_stub = build_storage(
    storage_backend=flow4df.LocalStorageBackend(prefix=stubs_path.as_posix())
)

agf_1h_event_v1 = flow4df.TableNode(
    schema=schema,
    upstream_table_nodes=[dummydb.trf_event_v1],
    transformation=transformation,
    storage=storage,
    storage_stub=storage_stub,
)
