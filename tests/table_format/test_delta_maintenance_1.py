import pytest
import flow4df
import datetime as dt
from pyspark.sql.types import StructType, StructField, TimestampType
from pyspark.sql.types import LongType, IntegerType, DateType, StringType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame


def build_table(temp_dir: str) -> flow4df.Table:
    table_schema = StructType([
        StructField('value', LongType(), True),
        StructField('timestamp', TimestampType(), True),
        StructField('event_ts', TimestampType(), True),
        StructField('event_year', IntegerType(), True),
        StructField('event_date', DateType(), True),
        StructField('planet', StringType(), True),
    ])

    def build_late_arriving_df(spark: SparkSession) -> DataFrame:
        _data = [
            (0, dt.datetime(2025, 1, 1, tzinfo=dt.UTC), None),
            (1, None, 'Earth'),
            (2, dt.datetime(2025, 1, 10, tzinfo=dt.UTC), 'Earth'),
            (3, dt.datetime(2025, 1, 7, tzinfo=dt.UTC), 'Earth'),
            (4, dt.datetime(2025, 1, 4, tzinfo=dt.UTC), 'Earth'),
            (5, dt.datetime(2025, 1, 14, tzinfo=dt.UTC), 'Earth'),
        ]
        _schema = ['value', 'event_ts', 'planet']
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
        time_non_monotonic=['planet'],
        time_monotonic_increasing=['event_year', 'event_date'],
    )
    identifier = flow4df.TableIdentifier(
        catalog='catalog1',
        schema=__name__.split('.')[-1],
        name='table_with_null_partitions',
        version='1',
    )
    return flow4df.Table(
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


@pytest.mark.slow
def test_delta_maintenance_1(spark: SparkSession, temp_dir: dir) -> None:
    table = build_table(temp_dir=temp_dir)
    table.init_table(spark=spark)
    # Trigger it 6 times
    for _ in range(6):
        handle = table.run(spark)
        assert handle is not None
        handle.awaitTermination()

    stats_before = table.calculate_table_stats(spark)
    # Run the maintenance
    table.run_table_maintenance(spark, run_for=None)

    stats_after = table.calculate_table_stats(spark)
    # The row count should be the same
    _m = 'Maintenance changed the row count!'
    assert stats_before.row_count == stats_after.row_count, _m
    return None
