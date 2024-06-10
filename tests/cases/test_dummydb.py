import pytest
import flow4df
import pendulum
from pyspark.sql import SparkSession
from flow4df.cases import dummydb

table_nodes = flow4df.TableNode.list_table_nodes(dummydb)


@pytest.mark.parametrize('table_node', table_nodes)
def test_dummydb_table_nodes(
    spark: SparkSession, table_node: flow4df.TableNode
) -> None:
    fake_interval = None
    if isinstance(table_node.transformation, flow4df.BatchTransformation):
        fake_interval = flow4df.DataInterval(
            start=pendulum.datetime(2024, 6, 1),
            end=pendulum.datetime(2024, 6, 2),
        )

    table_node.dry_run(spark=spark, data_interval=fake_interval)
