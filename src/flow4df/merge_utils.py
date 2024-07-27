import operator
import functools
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as F
from delta.tables import DeltaTable


def merge_nested_scd_2(
    spark: SparkSession,
    location: str,
    source_df: DataFrame,
    key_columns: list[str],
    value_columns: list[str]
) -> None:
    """`key_columns` must be primary key for `source_df`!"""

    _m = '`source_df` must have `valid_from` timestamp column.'
    assert 'valid_from' in source_df.columns, _m

    change_struct = F.struct(*value_columns, 'valid_from')
    source_df = source_df.withColumns({
        'changes': F.array(change_struct),
        'n_changes': F.lit(1),
    })
    target_dt = DeltaTable.forPath(sparkSession=spark, path=location)

    conditions = [f'target.{c} = source.{c}' for c in key_columns]
    merge_condition = ' AND '.join(conditions)
    merge_builder = target_dt.alias('target').merge(
        source=source_df.alias('source'),
        condition=merge_condition,
    )

    value_comparisons = [
        F.col(f'target.{c}') != F.col(f'source.{c}')
        for c in value_columns
    ]
    any_value_change_pred = functools.reduce(operator.or_, value_comparisons)
    is_more_recent = F.col('target.valid_from') < F.col('source.valid_from')
    new_value_pred = any_value_change_pred & is_more_recent

    # Create the update expressions
    def value_update_column(value_column: str) -> Column:
        return (
            F.when(new_value_pred, F.col(f'source.{value_column}'))
            .otherwise(F.col(f'target.{value_column}'))
        )

    update_expressions: dict[str, str | Column] = {
        column: value_update_column(column)
        for column in value_columns + ['valid_from']
    }
    # `changes`
    new_change = F.struct(*[
        F.col(f'source.{column}').alias(column)
        for column in value_columns + ['valid_from']
    ])
    new_change_array = F.array_append(F.col('target.changes'), new_change)
    update_expressions['changes'] = (
        F.when(new_value_pred, new_change_array)
        .otherwise(F.col('target.changes'))
    )
    # `n_changes`
    update_expressions['n_changes'] = (
        F.when(new_value_pred, F.col('target.n_changes') + F.lit(1))
        .otherwise(F.col('target.n_changes'))
    )
    _ = (
        merge_builder
        .whenNotMatchedInsertAll()
        .whenMatchedUpdate(set=update_expressions)
        .execute()
    )
    return None
