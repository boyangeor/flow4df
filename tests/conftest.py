import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


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
