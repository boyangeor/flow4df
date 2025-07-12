import pytest
import flow4df
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

ROWS_PER_BATCH = 10

def build_table() -> flow4df.Table:
    table_schema = T.StructType([
        T.StructField('timestamp', T.TimestampType(), True),
        T.StructField('value', T.LongType(), True)
    ])

    def transform(
        spark: SparkSession, upstream_tables: flow4df.UpstreamTables
    ) -> DataFrame:
        del upstream_tables
        df = (
            spark.readStream.format('rate-micro-batch')
            .option('rowsPerBatch', ROWS_PER_BATCH)
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
        name='fct_event_1',
        version='1',
    )
    return flow4df.Table(
        schema=table_schema,
        table_identifier=table_identifier,
        upstream_tables=[],
        transformation=transformation,
        table_format=flow4df.DeltaTableFormat(merge_schema=True),
        storage=flow4df.LocalStorage(prefix='/dev/shm'),
        # storage=flow4df.LocalStorage(prefix='/tmp'),
        storage_stub=flow4df.LocalStorage(prefix='/tmp'),
        partition_spec=part_spec,
    )

@pytest.mark.slow
def test_rmb_table_1(spark: SparkSession) -> None:
    table = build_table()
    handle = table.run(spark=spark, trigger={'availableNow': True})
    assert handle is not None
    handle.awaitTermination()

    df = table.as_batch_df(spark)
    assert df.count() >= ROWS_PER_BATCH
