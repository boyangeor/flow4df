import re
import json
import math
import logging
import operator
import functools
import datetime as dt
from typing import Literal, TypedDict
from dataclasses import dataclass, field
from pyspark.sql import DataFrameWriter, SparkSession, DataFrame, Column
from pyspark.sql import Window, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import LongType, StructType
from pyspark.ml.feature import QuantileDiscretizer

from flow4df import type_annotations, enums
from flow4df.table_format.table_format import TableFormat
from flow4df.table_format.table_format import TableStats
from flow4df import DataInterval, PartitionSpec, TableIdentifier
from flow4df.column_stats import ColumnStats
from flow4df.tools import sqlexpr

log = logging.getLogger()
TABLE_FORMAT_NAME = 'delta'

bool_literal = Literal['true', 'false']
# https://docs.delta.io/table-properties/
DeltaTableProperties = TypedDict(
    'DeltaTableProperties',
    {
        'delta.appendOnly': bool_literal,  # false
        'delta.checkpoint.writeStatsAsJson': bool_literal,  # true
        'delta.checkpoint.writeStatsAsStruct': bool_literal,
        'delta.compatibility.symlinkFormatManifest.enabled': bool_literal,
        'delta.dataSkippingNumIndexedCols': int,  # 32
        'delta.deletedFileRetentionDuration': str,  # INTERVAL 1 WEEK
        'delta.enableChangeDataFeed': bool_literal,  # false
        'delta.logRetentionDuration': str,  # INTERVAL 30 DAYS
        'delta.minReaderVersion': int,
        'delta.minWriterVersion': int,
        'delta.setTransactionRetentionDuration': str,
        'delta.checkpointPolicy': str,  # classic
    },
    total=False,
)


@dataclass(frozen=True, kw_only=True)
class Constraint:
    name: str
    expression: str


@dataclass(frozen=True, kw_only=True)
class DeltaTableFormat(TableFormat):
    stateful_query_source: bool
    merge_schema: bool = True
    idempotency_from_data_interval: bool = True
    constraints: list[Constraint] = field(default_factory=list)
    table_properties: DeltaTableProperties = field(default_factory=dict)  # type: ignore
    target_rows_per_file: int | None = None

    def build_batch_writer(
        self,
        df: DataFrame,
        table_identifier: TableIdentifier,
        output_mode: enums.OutputMode,
        partition_spec: PartitionSpec,
    ) -> type_annotations.Writer:
        del table_identifier
        writer = (
            df
            .write
            .mode(output_mode.name)
            .partitionBy(*partition_spec.partition_by)
        )
        return writer

    def configure_reader(
        self, reader: type_annotations.Reader, location: str
    ) -> type_annotations.Reader:
        return (
            reader
            .format(TABLE_FORMAT_NAME)
            .option('path', location)
        )

    def configure_writer(
        self,
        writer: type_annotations.Writer,
        location: str,
        data_interval: DataInterval | None
    ) -> type_annotations.Writer:
        """Configures the given Writer and returns it.

        Sets:
            - .format('delta')
            - .option('path', '<location>')
            - .option('mergeSchema', True|False)
            For idempotency of batch writes:
            - .option('txnAppId', 'flow4df_run')
            - .option('txnVersion', <data_interval.start_unix_ts_seconds>)
        """
        # assert isinstance(writer, DataFrameWriter)
        conf_writer = (
            writer
            .format(TABLE_FORMAT_NAME)
            .option('path', location)
            .option('mergeSchema', self.merge_schema)
        )
        if data_interval is not None and self.idempotency_from_data_interval:
            conf_writer = (
                conf_writer
                .option('txnAppId', 'flow4df_run')
                .option('txnVersion', data_interval.start_unix_ts_seconds)
            )

        if data_interval is not None:
            user_md = {'data_interval': data_interval.as_dict()}
            user_md_json = json.dumps(
                user_md, separators=(',', ':'), default=lambda x: x.isoformat()
            )
            conf_writer = (
                conf_writer
                .option('userMetadata', user_md_json)
            )

        return conf_writer

    def init_table(
        self,
        spark: SparkSession,
        location: str,
        table_identifier: TableIdentifier,
        table_schema: StructType,
        partition_spec: PartitionSpec,
    ) -> None:
        del table_identifier
        from delta import DeltaTable
        # Issue when setting file:///tmp/blah as location
        # `CREATE TABLE contains two different locations:`
        # `file:///... vs file:/...`
        # To investigate further, mb a bug? For now just remove it
        adjusted_location = re.sub('^file://', '', location)

        builder = (
            DeltaTable
            .createIfNotExists(sparkSession=spark)
            .location(adjusted_location)
            .addColumns(table_schema)
            .partitionedBy(*partition_spec.partition_by)
        )
        _time_bucket_not_in_schema = '_time_bucket' not in table_schema.names
        with_time_bucketing = partition_spec.time_bucketing_column is not None
        if with_time_bucketing and _time_bucket_not_in_schema:
            builder = builder.addColumn('_time_bucket', T.IntegerType())

        for c in self.constraints:
            _key = f'delta.constraints.{c.name}'
            builder = builder.property(key=_key, value=c.expression)

        for prop_key, prop_value in self.table_properties.items():
            builder = builder.property(key=prop_key, value=str(prop_value))

        builder.execute()
        return None

    def run_table_maintenance(
        self,
        spark: SparkSession,
        location: str,
        partition_spec: PartitionSpec,
        column_types: dict[str, T.DataType],
        run_for: dt.timedelta | None = None,
    ) -> None:
        from delta import DeltaTable
        as_delta_table = DeltaTable.forPath(sparkSession=spark, path=location)
        if not self.stateful_query_source:
            as_delta_table.optimize().executeCompaction()
            return None

        start = dt.datetime.now(dt.UTC)
        while True:
            part_to_compact = self.find_partition_to_compact(
                spark=spark,
                location=location,
                partition_spec=partition_spec,
                column_types=column_types,
                target_rows_per_file=self.target_rows_per_file,
            )
            if part_to_compact is None:
                break

            since_start = dt.datetime.now(dt.UTC) - start
            max_runtime_reached = (
                False if run_for is None else since_start > run_for
            )
            if max_runtime_reached:
                break

            log.info(f'Compacting {location}: {part_to_compact}')
            if partition_spec.time_bucketing_column is None:
                self.compact_partition(
                    spark=spark,
                    location=location,
                    partition_predicate=part_to_compact,
                )
            else:
                self.compact_partition_with_bucketing(
                    spark=spark,
                    location=location,
                    partition_predicate=part_to_compact,
                    time_bucketing_column=partition_spec.time_bucketing_column,
                    target_rows_per_file=self.target_rows_per_file,
                )

        return None

    def configure_session(
        self,
        spark: SparkSession,
        table_identifier: TableIdentifier,
        catalog_location: str
    ) -> None:
        del spark, catalog_location, table_identifier
        return None

    def calculate_table_stats(
        self, spark: SparkSession, location: str
    ) -> TableStats:
        raw_log_snapshot = DeltaTableFormat.build_log_snapshot_df(
            spark=spark, location=location
        )
        file_row_count = F.get_json_object('stats', '$.numRecords')
        size_gib = F.sum('size') / F.lit(1_073_741_824)
        agg_cols = [
            F.count('*').alias('file_count'),
            F.sum(file_row_count.cast(LongType())).alias('row_count'),
            size_gib.alias('size_gib'),
        ]
        stats_df = raw_log_snapshot.select(agg_cols)
        stats_row = stats_df.collect()[0]
        return TableStats(**stats_row.asDict())

    def get_column_stats(
        self,
        spark: SparkSession,
        location: str,
        column_types: dict[str, T.DataType],
        column_name: str,
        table_identifier: TableIdentifier,
    ) -> ColumnStats:
        """TODO: Provide non Spark based implementation too."""
        del table_identifier
        raw_log_snapshot = DeltaTableFormat.build_log_snapshot_df(
            spark=spark, location=location
        )
        column_type = column_types[column_name]

        part_value = (
            F.col('partitionValues').getItem(column_name).cast(column_type)
        )
        f_min_value = F.coalesce(
            F.get_json_object('stats', f'$.minValues.{column_name}'),
            part_value
        )
        f_max_value = F.coalesce(
            F.get_json_object('stats', f'$.maxValues.{column_name}'),
            part_value
        )
        f_min_value = f_min_value.cast(column_type)
        f_max_value = f_max_value.cast(column_type)
        if column_type == T.TimestampType():
            f_min_value = F.unix_seconds(f_min_value)
            f_max_value = F.unix_seconds(f_max_value)

        f_null_count = F.get_json_object('stats', f'$.nullCount.{column_name}')
        f_row_count = F.get_json_object('stats', '$.numRecords')
        agg_cols = [
            F.min(f_min_value).alias('min_value'),
            F.max(f_max_value).alias('max_value'),
            F.sum(f_row_count).alias('row_count'),
            F.sum(f_null_count).alias('null_count'),
        ]
        raw_log_snapshot.select('stats').show(10, False)
        column_stats_df = raw_log_snapshot.select(agg_cols)
        column_stats_row = column_stats_df.collect()[0]
        column_stats_dict = column_stats_row.asDict()
        if column_type == T.TimestampType():
            for k in ['min_value', 'max_value']:
                column_stats_dict[k] = dt.datetime.fromtimestamp(
                    column_stats_row[k], tz=dt.UTC
                )

        return ColumnStats(column_name=column_name, **column_stats_dict)

    def is_initialized_only(
        self,
        spark: SparkSession,
        location: str,
        table_identifier: TableIdentifier,
    ) -> bool:
        del table_identifier
        from delta import DeltaTable
        as_delta_table = DeltaTable.forPath(sparkSession=spark, path=location)
        last_operation = as_delta_table.history(limit=1).collect()[0]
        return last_operation['operation'] == 'CREATE TABLE'

    def get_last_append_operation(
        self,
        spark: SparkSession,
        location: str,
        look_back_limit: int,
    ) -> Row | None:
        """TODO: Provide non Spark based implementation too."""
        from delta import DeltaTable
        as_delta_table = DeltaTable.forPath(sparkSession=spark, path=location)
        history_df = as_delta_table.history(limit=look_back_limit)
        last_operation_rows = (
            history_df
            .where(F.col('operation') == F.lit('WRITE'))
            .orderBy(F.col('version').desc())
            .take(1)
        )
        last_operation = None
        if len(last_operation_rows) == 1:
            last_operation = last_operation_rows[0]

        return last_operation

    def get_last_batch_data_interval(
        self,
        spark: SparkSession,
        location: str,
        table_identifier: TableIdentifier,
    ) -> DataInterval:
        del table_identifier
        limits = [2, 8, 64, None]
        last_append_operation = None
        for limit in limits:
            last_append_operation = self.get_last_append_operation(
                spark=spark, location=location, look_back_limit=limit
            )
            if last_append_operation is not None:
                break

        _m = f'Cannot find last append operation in Delta log: {location}'
        assert last_append_operation is not None, _m

        parsed_md = json.loads(last_append_operation['userMetadata'])
        raw_data_interval = parsed_md['data_interval']
        return DataInterval.from_iso_formatted_timestamps(
            start_iso_timestamp=raw_data_interval['start'],
            end_iso_timestamp=raw_data_interval['end']
        )

    @staticmethod
    def build_log_snapshot_df(spark: SparkSession, location: str) -> DataFrame:
        j_logs = [
            # OSS Delta
            spark._jvm.org.apache.spark.sql.delta.DeltaLog,  # type: ignore
            # Databricks Delta
            spark._jvm.com.databricks.sql.transaction.tahoe.DeltaLog  # type: ignore
        ]
        for j_log in j_logs:
            try:
                delta_log = j_log.forTable(  # type: ignore
                    spark._jsparkSession, location
                )
                if delta_log is not None:
                    break
            except Exception:
                delta_log = None

        _m = 'Cannot build DeltaLog snapshot!'
        assert delta_log is not None, _m  # type: ignore
        jvm_table_files = (
            delta_log.snapshot().allFiles().toDF()  # type: ignore
        )
        log_snapshot_df = DataFrame(jvm_table_files, spark)
        return log_snapshot_df.withColumn(
            'file_added_ts', F.timestamp_millis('modificationTime')
        )

    @staticmethod
    def add_part_struct(
        log_snapshot_df: DataFrame,
        partition_spec: PartitionSpec,
        column_types: dict[str, T.DataType]
    ) -> DataFrame:
        _m = 'Table must have partitioning columns!'
        assert len(partition_spec.partition_columns), _m
        pv_map = F.col('partitionValues')
        part_struct = F.struct([
            pv_map.getItem(c).cast(column_types[c]).alias(c)
            for c in partition_spec.partition_columns
        ])
        _is_null_conds = [
            part_struct.getField(name).isNull()
            for name in partition_spec.partition_columns
        ]
        has_null_partitions = functools.reduce(operator.or_, _is_null_conds)
        file_row_count = F.get_json_object('stats', '$.numRecords')
        return log_snapshot_df.withColumns({
            'part_struct': part_struct,
            'has_null_partitions': has_null_partitions,
            'row_count': file_row_count.cast(LongType()),
        })

    @staticmethod
    def add_partitioning_info(
        log_snapshot_df: DataFrame,
        partition_spec: PartitionSpec,
        column_types: dict[str, T.DataType]
    ) -> DataFrame:
        log_snapshot_df = DeltaTableFormat.add_part_struct(
            log_snapshot_df, partition_spec, column_types
        )
        part_struct = F.col('part_struct')
        non_increasing: list[Column] = [
            part_struct.getField(e)
            for e in partition_spec.time_non_monotonic
        ]
        w1 = Window.partitionBy(F.lit(True), *non_increasing).orderBy(
            part_struct
        )
        w2 = Window.partitionBy(part_struct).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing
        )
        df = (
            log_snapshot_df
            # Must filter before the window functions!
            .where(~F.col('has_null_partitions'))
            .withColumns({
                'prev_mod_time': F.lag('modificationTime').over(w1),
                'n_files': F.count('*').over(w2),
                'partition_row_count': F.sum('row_count').over(w2),
            })
        )
        return df

    @staticmethod
    def find_partition_to_compact(
        spark: SparkSession,
        location: str,
        partition_spec: PartitionSpec,
        column_types: dict[str, T.DataType],
        target_rows_per_file: int | None,
    ) -> str | None:
        raw_log_snapshot_df = DeltaTableFormat.build_log_snapshot_df(
            spark=spark, location=location
        )
        log_snapshot_df = DeltaTableFormat.add_partitioning_info(
            log_snapshot_df=raw_log_snapshot_df,
            partition_spec=partition_spec,
            column_types=column_types
        )
        target_file_count = F.lit(1)
        if partition_spec.time_bucketing_column is not None:
            _m = (
                'Must set `target_rows_per_file` for PartitionSpec with '
                '`time_bucketing_column`.'
            )
            assert target_rows_per_file is not None, _m
            _count = operator.truediv(
                F.col('partition_row_count'),
                F.lit(target_rows_per_file)
            )
            target_file_count = F.ceil(_count).cast(LongType())

        preds = [
            F.col('n_files') > target_file_count,
            F.col('prev_mod_time') > F.col('modificationTime')
        ]
        predicate = functools.reduce(operator.or_, preds)
        part_to_compact = log_snapshot_df.where(predicate).select(
            F.min('part_struct').alias('part_struct')
        ).collect()[0]['part_struct']

        unordered_partition = None
        if part_to_compact is not None:
            unordered_partition = sqlexpr.row_to_sql_filter(part_to_compact)

        return unordered_partition

    @staticmethod
    def compact_partition(
        spark: SparkSession, location: str, partition_predicate: str
    ) -> None:
        as_data_frame = (
            spark.read
            .format(TABLE_FORMAT_NAME)
            .option('path', location)
            .load()
        )
        part_df = as_data_frame.where(partition_predicate).repartition(1)
        writer = (
            part_df.write
            .format(TABLE_FORMAT_NAME)
            .mode('overwrite')
            .option('path', location)
            .option('dataChange', False)
            # .option('partitionOverwriteMode', 'dynamic')
            .option('replaceWhere', partition_predicate)
        )
        writer.save()
        return None

    @staticmethod
    def compact_partition_with_bucketing(
        spark: SparkSession,
        location: str,
        partition_predicate: str,
        time_bucketing_column: str,
        target_rows_per_file: int,
    ) -> None:
        _m = '`target_rows_per_file` must be positive!'
        assert target_rows_per_file > 0, _m

        as_data_frame = (
            spark
            .read
            .format(TABLE_FORMAT_NAME)
            .option('path', location)
            .load()
        )
        part_df = (
            as_data_frame
            .where(partition_predicate)
            .withColumn('_unix_ts', F.unix_seconds(time_bucketing_column))
            .drop(F.col('_time_bucket'))
        )
        part_row_count = part_df.count()
        bucket_count = math.ceil(part_row_count / target_rows_per_file)

        bucketized_part_df = part_df.withColumn('_time_bucket', F.lit(0))
        if bucket_count > 1:
            qds = QuantileDiscretizer(
                numBuckets=bucket_count,
                inputCol='_unix_ts',
                outputCol='_time_bucket'
            )
            qds_fitted = qds.fit(part_df)
            bucketized_part_df = (
                qds_fitted.transform(part_df)
                .withColumn('_time_bucket', F.col('_time_bucket').cast('int'))
            )

        writer = (
            bucketized_part_df
            .drop(F.col('_unix_ts'))
            .repartition(1)
            .write
            .format(TABLE_FORMAT_NAME)
            .mode('overwrite')
            .option('path', location)
            .option('dataChange', False)
            .option('replaceWhere', partition_predicate)
        )
        writer.save()
        return None
