from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

from flow4df import Table, UpstreamTables
from flow4df import TableIdentifier
from flow4df import StructuredStreamingTransformation
from flow4df import DeltaTableFormat
from flow4df import LocalStorage
from flow4df import PartitionSpec
from flow4df.enums import OutputMode


table_schema = T.StructType([
    T.StructField('timestamp', T.TimestampType(), True),
    T.StructField('value', T.LongType(), True)
])


def transform(
    spark: SparkSession, upstream_tables: UpstreamTables
) -> DataFrame:
    del upstream_tables
    df = (
        spark.readStream.format('rate-micro-batch')
        .option('rowsPerBatch', 5)
        .load()
    )
    df = df.withColumn('event_date', F.to_date('timestamp'))
    return df.repartition(1)


transformation = StructuredStreamingTransformation(
    transform=transform,
    output_mode=OutputMode.append,
    default_trigger={'availableNow': True},
    checkpoint_dir='_checkpoint',
)

part_spec = PartitionSpec(
    time_non_monotonic=[],
    time_monotonic_increasing=['event_date'],
)

table = Table(
    schema=table_schema,
    table_identifier=TableIdentifier.from_module_name(__name__),
    upstream_tables=[],
    transformation=transformation,
    table_format=DeltaTableFormat(merge_schema=True),
    storage=LocalStorage(prefix='/tmp'),
    storage_stub=LocalStorage(prefix='/tmp'),
    partition_spec=part_spec,
)
