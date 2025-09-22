import flow4df
import datetime as dt
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

from flow4df.transformation.incremental_batch_transformation import (
    IncrementalBatchTransformation
)
from flow4df.example.catalog1 import bronze_schema


def transform(
    spark: SparkSession,
    this_table: flow4df.Table,
    data_interval: flow4df.DataInterval
) -> DataFrame:
    upstream_table = this_table.get_upstream_table(name='trf_raw_measurement')
    batch_df = (
        upstream_table.as_batch_df(spark)
        .where(F.col('timestamp') >= F.lit(data_interval.start))
        .where(F.col('timestamp') < F.lit(data_interval.end))
    )
    w = F.window('timestamp', '1 HOUR', '1 HOUR')
    return (
        batch_df
        .groupBy(w)
        .count()
        .select(
            F.col('window.start').alias('window_start'),
            F.col('window.end').alias('window_end'),
            F.col('count').alias('row_count')
        )
    )


def get_upstream_watermark(
    spark: SparkSession,
    upstream_tables: list[flow4df.Table],
) -> dt.datetime:
    # there is only 1 source table
    source_table = upstream_tables[0]
    timestamp_stats = source_table.get_column_stats(
        column_name='timestamp', spark=spark,
    )
    watermark_delay = dt.timedelta(hours=1)
    upstream_max = timestamp_stats.max_value.replace(tzinfo=dt.UTC)
    return upstream_max - watermark_delay


transformation = IncrementalBatchTransformation(
    transform=transform,
    output_mode=flow4df.enums.OutputMode.append,
    get_upstream_watermark=get_upstream_watermark,
    start=dt.datetime(1970, 1, 1, 0, tzinfo=dt.UTC),
    min_interval_length=dt.timedelta(hours=1),
    max_interval_length=dt.timedelta(hours=2),
)
part_spec = flow4df.PartitionSpec(
    time_non_monotonic=[],
    time_monotonic_increasing=[],
)
identifier = flow4df.TableIdentifier.from_module_name(__name__)

table_schema = T.StructType([
    T.StructField('window_start', T.TimestampType(), True),
    T.StructField('window_end', T.TimestampType(), True),
    T.StructField('row_count', T.LongType(), False)
])

table = flow4df.Table(
    table_schema=table_schema,
    table_identifier=identifier,
    upstream_tables=[
        bronze_schema.get_table('trf_raw_measurement'),
    ],
    transformation=transformation,
    table_format=flow4df.DeltaTableFormat(
        stateful_query_source=False, merge_schema=True,
    ),
    storage=flow4df.LocalStorage(prefix='/tmp'),
    storage_stub=flow4df.LocalStorage(prefix='/tmp/stubs'),
    partition_spec=part_spec,
    is_active=True,
)
