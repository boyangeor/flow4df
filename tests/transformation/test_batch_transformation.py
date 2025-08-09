import pytest
import flow4df
import datetime as dt
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

ROWS_PER_BATCH = 10


def build_table_1(temp_dir: str) -> flow4df.Table:
    table_schema = T.StructType([
        T.StructField('timestamp', T.TimestampType(), True),
        T.StructField('value', T.LongType(), True),
        T.StructField('event_date', T.DateType(), True),
    ])

    def transform(
        spark: SparkSession, this_table: flow4df.Table
    ) -> DataFrame:
        del this_table
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
        name='fct_event_x',
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
        T.StructField('timestamp', T.TimestampType(), True),
        T.StructField('value', T.LongType(), True),
        T.StructField('event_date', T.DateType(), True),
    ])

    def transform(
        spark: SparkSession,
        this_table: flow4df.Table,
        data_interval: flow4df.DataInterval,
    ) -> DataFrame:
        fct_event_x = this_table.get_upstream_table(name='fct_event_x')
        fct_event_x_df = fct_event_x.as_batch_df(spark=spark)
        return (
            fct_event_x_df
            .where(F.col('timestamp') >= F.lit(data_interval.start))
            .where(F.col('timestamp') < F.lit(data_interval.end))
        )

    transformation = flow4df.BatchTransformation(
        transform=transform,
        output_mode=flow4df.enums.OutputMode.append,
    )
    part_spec = flow4df.PartitionSpec(
        time_non_monotonic=[],
        time_monotonic_increasing=['event_date'],
    )
    table_identifier = flow4df.TableIdentifier(
        catalog='catalog1',
        schema=__name__.split('.')[-1],
        name='fct_event_y',
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
            build_table_1(temp_dir=temp_dir),
        ],
        transformation=transformation,
        table_format=table_format,
        storage=flow4df.LocalStorage(prefix=temp_dir),
        storage_stub=flow4df.LocalStorage(prefix=temp_dir),
        partition_spec=part_spec,
        is_active=True,
    )


@pytest.mark.slow
def test_run_transformation(spark: SparkSession, temp_dir: str) -> None:
    t1 = build_table_1(temp_dir=temp_dir)
    # Run t1 for 1 Batch
    handle = t1.run(spark=spark, trigger={'availableNow': True})
    handle.awaitTermination()

    t2 = build_table_2(temp_dir=temp_dir)
    test_interval = flow4df.DataInterval(
        start=dt.datetime(1970, 1, 1, tzinfo=dt.UTC),
        end=dt.datetime(1970, 1, 2, tzinfo=dt.UTC),
    )
    t2.run(spark=spark, data_interval=test_interval)

    df = t2.as_batch_df(spark)
    assert df.count() > 0
    return None



@pytest.mark.slow
def test_test_transformation(spark: SparkSession, temp_dir: str) -> None:
    t1 = build_table_1(temp_dir=temp_dir)
    t1.init_table_stub(spark)

    t2 = build_table_2(temp_dir=temp_dir)
    test_interval = flow4df.DataInterval(
        start=dt.datetime(1970, 1, 1, tzinfo=dt.UTC),
        end=dt.datetime(1970, 1, 2, tzinfo=dt.UTC),
    )
    t2.test_transformation(spark, data_interval=test_interval)
    return None
