import flow4df
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from flow4df.example_dwh import table_index


def transform(
    spark: SparkSession, this_table: flow4df.Table
) -> DataFrame:
    trf_raw_measurement = this_table.get_upstream_table('trf_raw_measurement')
    trf_raw_measurement_df = trf_raw_measurement.as_streaming_df(spark)
    return trf_raw_measurement_df.withColumns({
        'galaxy': F.lit('Milky Way')
    })


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

table_schema = T.StructType([
    T.StructField('timestamp', T.TimestampType(), True),
    T.StructField('value', T.LongType(), True),
    T.StructField('planet', T.StringType(), True),
    T.StructField('event_date', T.DateType(), True),
    T.StructField('galaxy', T.StringType(), True),
])
upstream_table = table_index.get_active_table(
    catalog='catalog1',
    schema='bronze',
    name='trf_raw_measurement',
)
table = flow4df.Table(
    table_schema=table_schema,
    table_identifier=identifier,
    upstream_tables=[
        # active_tables.get_table(
        #     catalog='catalog1',
        #     schema='bronze',
        #     name='trf_raw_measurement',
        # )
        upstream_table
    ],
    transformation=transformation,
    table_format=flow4df.DeltaTableFormat(
        stateful_query_source=True, merge_schema=True,
    ),
    storage=flow4df.LocalStorage(prefix='/tmp'),
    storage_stub=flow4df.LocalStorage(prefix='/tmp/stubs'),
    partition_spec=part_spec,
    is_active=True,
)
