from pyspark.sql.types import StructType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame, Column


def assert_schemas_equivalent(
    spark: SparkSession, actual: StructType, expected: StructType
) -> None:
    columns = ['field_name', 'field_type']

    def create_df(schema: StructType, source: Column) -> DataFrame:
        _data = [(f.name, f.dataType.typeName()) for f in schema]
        df = spark.createDataFrame(_data, schema=columns)
        return df.withColumn('source', source)

    df1 = create_df(actual, source=F.lit('actual'))
    df2 = create_df(expected, source=F.lit('expected'))
    combined_df = df1.union(df2)

    grouped = combined_df.groupBy(*columns)
    summary_df = (
        grouped
        .agg(
            F.count_distinct('source').alias('source_count'),
            F.collect_set('source').alias('sources'),
        )
        .orderBy(F.col('source_count').asc(), 'sources')
    )
    is_in_both = F.col('source_count') == F.lit(2)
    different_count = summary_df.where(~is_in_both).count()
    if different_count > 0:
        summary_df.where(~is_in_both).show(50, False)
        raise AssertionError('Schemas not nequivalent!')

    return None
