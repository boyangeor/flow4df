import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from flow4df.tools import schema


def test_assert_schemas_equivalent(spark: SparkSession) -> None:
    actual = T.StructType([
        T.StructField('a', T.IntegerType(), True),
        T.StructField('b', T.StringType(), True),
    ])
    expected = T.StructType([
        T.StructField('a', T.IntegerType(), True),
        T.StructField('b', T.StringType(), True),
    ])
    schema.assert_schemas_equivalent(spark, actual, expected)


def test_assert_raises(spark: SparkSession) -> None:
    s1 = T.StructType([
        T.StructField('a', T.IntegerType(), True),
        T.StructField('b', T.StringType(), True),
    ])
    s2 = T.StructType([
        T.StructField('a', T.IntegerType(), True),
        T.StructField('c', T.StringType(), True),
    ])
    with pytest.raises(AssertionError):
        schema.assert_schemas_equivalent(spark, s1, s2)
