import pytest
import flow4df
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

ROWS_PER_BATCH = 10


def build_table(temp_dir: str) -> flow4df.Table:
    table_schema = T.StructType([
        T.StructField('timestamp', T.TimestampType(), True),
        T.StructField('value', T.LongType(), True),
        T.StructField('event_date', T.DateType(), True),
    ])

    def transform(
        spark: SparkSession, this_table: flow4df.Table
    ) -> DataFrame:
        df = (
            spark.readStream
            .format('rate-micro-batch')
            .option('rowsPerBatch', ROWS_PER_BATCH)
            .option('advanceMsPerBatch', 3600 * 1000)
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
        catalog='catalog1',
        schema=__name__.split('.')[-1],
        name='fct_event_1',
        version='1',
    )
    table_format = flow4df.DeltaTableFormat(
        stateful_query_source=True,
        merge_schema=True
    )
    return flow4df.Table(
        table_schema=table_schema,
        table_identifier=table_identifier,
        upstream_tables=[],
        transformation=transformation,
        table_format=table_format,
        storage=flow4df.LocalStorage(prefix=temp_dir),
        storage_stub=flow4df.LocalStorage(prefix=temp_dir),
        partition_spec=part_spec,
        is_active=True,
    )


@pytest.mark.slow
def test_run_transformation(spark: SparkSession, temp_dir: str) -> None:
    table = build_table(temp_dir=temp_dir)
    handle = table.run(spark=spark, trigger={'availableNow': True})
    assert handle is not None
    handle.awaitTermination()
    df = table.as_batch_df(spark)
    assert df.count() >= ROWS_PER_BATCH


@pytest.mark.slow
def test_test_transformation(spark: SparkSession, temp_dir: str) -> None:
    table = build_table(temp_dir=temp_dir)
    table.test_transformation(spark=spark)
