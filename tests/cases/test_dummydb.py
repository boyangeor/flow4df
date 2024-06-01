import pytest
from pyspark.sql import SparkSession
from flow4df import TableNode
from flow4df.cases import dummydb

table_nodes = TableNode.list_table_nodes(dummydb)


@pytest.mark.parametrize('table_node', table_nodes)
def test_dummydb_table_nodes(
    spark: SparkSession, table_node: TableNode
) -> None:
    table_node.dry_run(spark=spark)
