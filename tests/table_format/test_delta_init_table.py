import flow4df
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame


def build_table() -> flow4df.Table:
    table_schema = T.StructType([
        T.StructField('timestamp', T.TimestampType(), True),
        T.StructField('value', T.LongType(), True),
        T.StructField('event_date', T.DateType(), True)
    ])

    def transform(
        spark: SparkSession, upstream_tables: flow4df.UpstreamTables
    ) -> DataFrame:
        del upstream_tables
        df = (
            spark.readStream.format('rate-micro-batch')
            .option('rowsPerBatch', 5)
            .load()
        )
        df = df.withColumn('event_date', F.to_date('timestamp'))
        return df.repartition(1)

    transformation = flow4df.StructuredStreamingTransformation(
        transform=transform,
        output_mode=flow4df.enums.OutputMode.append,
        default_trigger={'availableNow': True},
        checkpoint_dir='_checkpoint',
    )
    part_spec = flow4df.PartitionSpec(
        time_non_monotonic=[],
        time_monotonic_increasing=['event_date'],
    )
    table_identifier = flow4df.TableIdentifier(
        catalog='some_catalog',
        schema='bronze',
        name='test_init_table',
        version='2'
    )
    return flow4df.Table(
        schema=table_schema,
        table_identifier=table_identifier,
        upstream_tables=[],
        transformation=transformation,
        table_format=flow4df.DeltaTableFormat(
            merge_schema=True, stateful_query_source=True
        ),
        storage=flow4df.LocalStorage(prefix='/tmp'),
        storage_stub=flow4df.LocalStorage(prefix='/tmp'),
        partition_spec=part_spec,
    )


def test_init_table(spark: SparkSession) -> None:
    table = build_table()
    table.init_table(spark=spark)

    # Read it
    df = table.as_batch_df(spark)
    assert df.count() == 0
