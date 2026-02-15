import pytest
import flow4df
import datetime as dt
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrameWriter

ROW_COUNT = 10


def build_table(temp_dir: str) -> flow4df.Table:

    def execute(spark: SparkSession, this_table: flow4df.Table,) -> None:
        df = (
            spark.range(ROW_COUNT)
            .withColumn('event_date', F.lit(dt.date(2026, 1, 1)))
        )
        writer = df.write.mode('overwrite')
        writer = this_table.table_format.configure_writer(
            writer,
            location=this_table.location,
            data_interval=None
        )
        assert isinstance(writer, DataFrameWriter)
        writer.save()
        return None

    transformation = flow4df.GenericTransformation(generic_execute=execute)
    part_spec = flow4df.PartitionSpec(
        time_non_monotonic=[],
        time_monotonic_increasing=['event_date'],
    )
    table_identifier = flow4df.TableIdentifier(
        catalog='catalog1',
        schema=__name__.split('.')[-1],
        name='fct_dummy_event_2',
        version='1',
    )
    table_format = flow4df.DeltaTableFormat(
        stateful_query_source=True, merge_schema=True
    )
    table_schema = T.StructType([
        T.StructField('id', T.LongType(), True),
        T.StructField('event_date', T.DateType(), True),
    ])
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
    # Init and run
    table.init_table(spark=spark)
    table.run(spark=spark)
    # Read and compare row count
    df = table.as_batch_df(spark)
    assert df.count() == ROW_COUNT
    return None


@pytest.mark.slow
def test_test_transformation(spark: SparkSession, temp_dir: str) -> None:
    table = build_table(temp_dir=temp_dir)
    table.test_transformation(spark=spark)
    return None
