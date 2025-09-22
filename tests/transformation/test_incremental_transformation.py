import pytest
import flow4df
import datetime as dt
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from flow4df.transformation.incremental_batch_transformation import (
    IncrementalBatchTransformation
)


def build_table_1(temp_dir: str) -> flow4df.Table:
    table_schema = T.StructType([
        T.StructField('id', T.LongType(), False),
        T.StructField('timestamp', T.TimestampType(), True)
    ])

    def transform(
        spark: SparkSession,
        this_table: flow4df.Table,
        data_interval: flow4df.DataInterval,
    ) -> DataFrame:
        start = F.lit('2025-01-01').cast('timestamp')
        timedelta = F.col('id') * F.lit(dt.timedelta(minutes=20))
        return (
            spark.range(20)
            .withColumn('timestamp', start + timedelta)
        )

    transformation = flow4df.BatchTransformation(
        transform=transform,
        output_mode=flow4df.enums.OutputMode.append,
    )
    part_spec = flow4df.PartitionSpec(
        time_non_monotonic=[],
        time_monotonic_increasing=[],
    )
    table_identifier = flow4df.TableIdentifier(
        catalog='catalog1',
        schema=__name__.split('.')[-1],
        name='trf_event',
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


def build_table_2(temp_dir: str) -> flow4df.Table:
    table_schema = T.StructType([
        T.StructField('id', T.LongType(), False),
        T.StructField('timestamp', T.TimestampType(), True)
    ])

    def transform(
        spark: SparkSession,
        this_table: flow4df.Table,
        data_interval: flow4df.DataInterval
    ) -> DataFrame:
        upstream_table = this_table.get_upstream_table(name='trf_event')
        batch_df = (
            upstream_table.as_batch_df(spark)
            .where(F.col('timestamp') >= F.lit(data_interval.start))
            .where(F.col('timestamp') < F.lit(data_interval.end))
        )
        return batch_df

    def get_upstream_watermark(
        spark: SparkSession,
        upstream_tables: list[flow4df.Table],
    ) -> dt.datetime:
        # there is only 1 source table
        source_table = upstream_tables[0]
        timestamp_stats = source_table.get_column_stats(
            column_name='timestamp', spark=spark,
        )
        watermark_delay = dt.timedelta(hours=0)
        print(timestamp_stats.max_value)
        upstream_max = timestamp_stats.max_value.replace(tzinfo=dt.UTC)
        upstream_wm = upstream_max - watermark_delay
        print(upstream_wm)
        return upstream_wm

    transformation = IncrementalBatchTransformation(
        transform=transform,
        output_mode=flow4df.enums.OutputMode.append,
        get_upstream_watermark=get_upstream_watermark,
        start=dt.datetime(2025, 1, 1, 0, tzinfo=dt.UTC),
        min_interval_length=dt.timedelta(minutes=30),
        max_interval_length=dt.timedelta(minutes=60),
    )
    part_spec = flow4df.PartitionSpec(
        time_non_monotonic=[],
        time_monotonic_increasing=[],
    )
    table_identifier = flow4df.TableIdentifier(
        catalog='catalog1',
        schema=__name__.split('.')[-1],
        name='trf_event2',
        version='1',
    )
    table_format = flow4df.DeltaTableFormat(
        stateful_query_source=True,
        merge_schema=True
    )
    return flow4df.Table(
        table_schema=table_schema,
        table_identifier=table_identifier,
        upstream_tables=[
            build_table_1(temp_dir=temp_dir)
        ],
        transformation=transformation,
        table_format=table_format,
        storage=flow4df.LocalStorage(prefix=temp_dir),
        storage_stub=flow4df.LocalStorage(prefix=temp_dir),
        partition_spec=part_spec,
        is_active=True,
    )


@pytest.mark.slow
def test_incremental_transformation(spark: SparkSession, temp_dir: str) -> None:
    t1 = build_table_1(temp_dir=temp_dir)
    t1.init_table_stub(spark)
    test_interval = flow4df.DataInterval(
        start=dt.datetime(2025, 1, 1, tzinfo=dt.UTC),
        end=dt.datetime(2025, 1, 2, tzinfo=dt.UTC),
    )
    t1.run(spark, data_interval=test_interval)

    t2 = build_table_2(temp_dir=temp_dir)
    t2.init_table(spark)
    next_data_interval = t2.build_next_data_interval(spark)
    while next_data_interval is not None:
        print(next_data_interval)
        t2.run(spark, data_interval=next_data_interval)
        next_data_interval = t2.build_next_data_interval(spark)

    t1_count = t1.as_batch_df(spark).count()
    t2_count = t2.as_batch_df(spark).count()
    assert t2_count == 18, 'Did not process all available!'
    assert (t1_count - t2_count) == 2, 'Processed more than available!'
    return None
