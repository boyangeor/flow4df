import flow4df
import datetime as dt
from pyspark.sql.types import StructType, StructField, TimestampType
from pyspark.sql.types import LongType, IntegerType, DateType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

table_schema = StructType([
    StructField('value', LongType(), True),
    StructField('timestamp', TimestampType(), True),
    StructField('event_ts', TimestampType(), True),
    StructField('event_year', IntegerType(), True),
    StructField('event_date', DateType(), True)
])


def build_late_arriving_df(spark: SparkSession) -> DataFrame:
    _data = [
        (0, dt.datetime(2025, 10, 31), ),
        (1, dt.datetime(2025, 11, 1), ),
        (2, dt.datetime(2025, 11, 2), ),
        (3, dt.datetime(2025, 11, 3), ),
        (4, dt.datetime(2025, 11, 4), ),
        (5, dt.datetime(2025, 10, 25), ),
        (6, dt.datetime(2025, 10, 26), ),
        (7, dt.datetime(2025, 10, 27), ),
        (8, dt.datetime(2025, 10, 28), ),
        (9, dt.datetime(2025, 10, 29), ),
        (10, None,),
        (11, dt.datetime(2025, 11, 10), ),
    ]
    _schema = ['value', 'event_ts']
    df = spark.createDataFrame(_data, _schema)
    return df.withColumns({
        'event_year': F.year('event_ts'),
        'event_date': F.to_date('event_ts'),
    })


def transform(
    spark: SparkSession, this_table: flow4df.Table
) -> DataFrame:
    del this_table

    la_df = build_late_arriving_df(spark)
    df = (
        spark.readStream.format('rate-micro-batch')
        .option('rowsPerBatch', 1)
        .option('advanceMillisPerBatch', 3600 * 1000)
        .load()
    )
    df2 = df.join(other=la_df, on='value', how='left_outer')
    return df2.repartition(1)


transformation = flow4df.StructuredStreamingTransformation(
    transform=transform,
    output_mode=flow4df.enums.OutputMode.append,
    default_trigger={'availableNow': True},
    checkpoint_dir='_checkpoint',
)
part_spec = flow4df.PartitionSpec(
    time_non_monotonic=[],
    time_monotonic_increasing=['event_year', 'event_date'],
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
