import pytest
import tempfile
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

from flow4df import Table
from flow4df import TableIdentifier
from flow4df import StructuredStreamingTransformation
from flow4df import DeltaTableFormat
from flow4df import LocalStorage
from flow4df import PartitionSpec
import flow4df
from flow4df.enums import OutputMode


@pytest.fixture
def spark():
    _catalog = 'org.apache.spark.sql.delta.catalog.DeltaCatalog'
    conf_map = {
        'spark.sql.shuffle.partitions': 4,
        'spark.driver.memory': '10g',
        'spark.sql.session.timeZone': 'UTC',
        'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
        'spark.sql.catalog.spark_catalog': _catalog,
        'spark.databricks.delta.retentionDurationCheck.enabled': 'false',
        'spark.databricks.delta.snapshotPartitions': 1,
        'spark.databricks.delta.stalenessLimit': 24 * 3600 * 1000,
    }
    builder = SparkSession.builder.config(map=conf_map).master('local[2]')
    configured_builder = configure_spark_with_delta_pip(
        builder, extra_packages=[]
    )
    return configured_builder.getOrCreate()


@pytest.fixture
def example_table_1():
    table_schema = T.StructType([
        T.StructField('timestamp', T.TimestampType(), True),
        T.StructField('value', T.LongType(), True)
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

    transformation = StructuredStreamingTransformation(
        transform=transform,
        output_mode=OutputMode.append,
        default_trigger={'availableNow': True},
        checkpoint_dir='_checkpoint',
    )
    part_spec = PartitionSpec(
        time_non_monotonic=[],
        time_monotonic_increasing=['event_date'],
    )
    identifier = TableIdentifier(
        catalog='catalog1',
        schema='schema1',
        name='example_table_1',
        version='1'
    )
    table_format = DeltaTableFormat(
        stateful_query_source=True,
        merge_schema=True,
    )
    return Table(
        table_schema=table_schema,
        table_identifier=identifier,
        upstream_tables=[],
        transformation=transformation,
        table_format=table_format,
        storage=LocalStorage(prefix='/tmp'),
        storage_stub=LocalStorage(prefix='/tmp2'),
        partition_spec=part_spec,
        is_active=True,
    )


@pytest.fixture
def temp_dir() -> str:
    temp_dir = tempfile.TemporaryDirectory()
    yield temp_dir.name
    temp_dir.cleanup()
