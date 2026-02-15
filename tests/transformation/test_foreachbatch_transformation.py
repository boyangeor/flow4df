import pytest
import flow4df
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame

ROWS_PER_BATCH = 10


def build_table(temp_dir: str) -> flow4df.Table:
    def streaming_transform(
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
        return df

    def foreach_batch_execute(
        spark: SparkSession,
        this_table: flow4df.Table,
        batch_df: DataFrame,
        epoch_id: int
    ) -> None:
        del spark, this_table, epoch_id
        disp0 = batch_df.select(F.count('*').alias('row_count'))
        disp0.show(10, False)
        return None

    transformation = flow4df.ForeachBatchStreamingTransformation(
        streaming_transform=streaming_transform,
        foreach_batch_execute=foreach_batch_execute,
        default_trigger={'availableNow': True},
        checkpoint_dir='_checkpoint',
    )
    part_spec = flow4df.PartitionSpec(
        time_non_monotonic=[],
        time_monotonic_increasing=[],
    )
    table_identifier = flow4df.TableIdentifier(
        catalog='catalog1',
        schema=__name__.split('.')[-1],
        name='fct_dummy_event_1',
        version='1',
    )
    table_format = flow4df.DeltaTableFormat(
        stateful_query_source=True,
        merge_schema=True,
    )
    # It's a hollow table, it's location has only the checkpoint
    dummy_schema = T.StructType([
        T.StructField('dummy_column', T.StringType(), True),
    ])
    return flow4df.Table(
        table_schema=dummy_schema,
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
    for _ in range(2):
        handle = table.run(spark=spark, trigger={'availableNow': True})
        assert handle is not None
        handle.awaitTermination()

    return None
