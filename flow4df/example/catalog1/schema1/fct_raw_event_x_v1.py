import flow4df
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

table_schema = T.StructType([
    T.StructField('timestamp', T.TimestampType(), True),
    T.StructField('value', T.LongType(), True),
    T.StructField('planet', T.StringType(), True),
    T.StructField('event_date', T.DateType(), True),
])


def transform(
    spark: SparkSession, this_table: flow4df.Table
) -> DataFrame:
    del this_table
    df = (
        spark.readStream.format('rate-micro-batch')
        .option('rowsPerBatch', 5)
        .option('advanceMsPerBatch', 3600 * 1000)
        .load()
    )
    df = df.withColumns({
        'planet': F.lit('Earth'),
        'event_date': F.to_date('timestamp'),
    })
    return df.repartition(1)


transformation = flow4df.StructuredStreamingTransformation(
    transform=transform,
    output_mode=flow4df.enums.OutputMode.append,
    default_trigger={'availableNow': True},
    checkpoint_dir='_checkpoint',
)
part_spec = flow4df.PartitionSpec(
    time_non_monotonic=['planet'],
    time_monotonic_increasing=['event_date'],
)
identifier = flow4df.TableIdentifier.from_module_name(__name__)
table = flow4df.Table(
    table_schema=table_schema,
    table_identifier=identifier,
    upstream_tables=[],
    transformation=transformation,
    table_format=flow4df.DeltaTableFormat(
        stateful_query_source=True, merge_schema=True,
    ),
    storage=flow4df.LocalStorage(prefix='/tmp'),
    storage_stub=flow4df.LocalStorage(prefix='/tmp/stubs'),
    partition_spec=part_spec,
    is_active=True,
)
