import pytest
import flow4df
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame


def build_table(temp_dir: str, table_name: str) -> flow4df.Table:
    table_schema = T.StructType([
        T.StructField('timestamp', T.TimestampType(), True),
        T.StructField('value', T.LongType(), True),
        T.StructField('event_date', T.DateType(), True)
    ])

    def transform(
        spark: SparkSession, this_table: flow4df.Table
    ) -> DataFrame:
        del this_table
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
        catalog='catalog1',
        schema=__name__.split('.')[-1],
        name=table_name,
        version='1'
    )
    return flow4df.Table(
        table_schema=table_schema,
        table_identifier=table_identifier,
        upstream_tables=[],
        transformation=transformation,
        table_format=flow4df.DeltaTableFormat(
            merge_schema=True, stateful_query_source=True
        ),
        storage=flow4df.LocalStorage(prefix=temp_dir),
        storage_stub=flow4df.LocalStorage(prefix=temp_dir),
        partition_spec=part_spec,
        is_active=True
    )


@pytest.mark.slow
def test_init_table(spark: SparkSession, temp_dir: str) -> None:
    table = build_table(temp_dir=temp_dir, table_name='init_table')
    table.init_table(spark=spark)

    # Read it
    df = table.as_batch_df(spark)
    assert df.count() == 0


@pytest.mark.slow
def test_delta_maintenance(spark: SparkSession, temp_dir: str) -> None:
    table = build_table(
        temp_dir=temp_dir, table_name='maintenance_test'
    )
    table.init_table(spark=spark)
    # Trigger it few times
    for i in range(2):
        handle = table.run(spark, trigger={'availableNow': True})
        assert handle is not None
        handle.awaitTermination()

    stats_before = table.calculate_table_stats(spark)
    # Run the maintenance
    table.run_table_maintenance(spark, run_for=None)

    stats_after = table.calculate_table_stats(spark)
    # The row count should be the same
    _m = 'Maintenance changed the row count!'
    assert stats_before.row_count == stats_after.row_count, _m
    # The file count should be smaller
    _m = 'Maintenance did not reduce the file count'
    assert stats_before.file_count > stats_after.file_count, _m
    return None
