import re
import logging
import operator
import functools
import datetime as dt
from dataclasses import dataclass, field
from pyspark.sql import SparkSession, DataFrame, Column, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType

from flow4df import types
from flow4df import TableFormat
from flow4df import DataInterval, PartitionSpec

log = logging.getLogger()
TABLE_FORMAT_NAME = 'delta'


@dataclass(frozen=True, kw_only=True)
class Constraint:
    name: str
    expression: str


@dataclass(frozen=True, kw_only=True)
class DeltaTableFormat(TableFormat):
    stateful_query_source: bool
    merge_schema: bool = True
    constraints: list[Constraint] = field(default_factory=list)

    def configure_reader(
        self, reader: types.Reader, location: str
    ) -> types.Reader:
        return (
            reader
            .format(TABLE_FORMAT_NAME)
            .option('path', location)
        )

    def configure_writer(
        self, writer: types.Writer, data_interval: DataInterval, location: str,
    ) -> types.Writer:
        del data_interval
        return (
            writer
            .format(TABLE_FORMAT_NAME)
            .option('path', location)
            .option('mergeSchema', self.merge_schema)
        )

    def init_table(
        self,
        spark: SparkSession,
        location: str,
        schema: StructType,
        partition_spec: PartitionSpec,
    ) -> None:
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
            .addColumns(schema)
            .partitionedBy(*partition_spec.columns)
        )
        for c in self.constraints:
            builder = builder.property(
                key=f'delta.constraints.{c.name}', value=c.expression
            )

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
                column_types=column_types
            )
            if part_to_compact is None:
                break

            since_start = dt.datetime.now(dt.UTC) - start
            max_runtime_reached = (
                False if run_for is None else since_start > run_for
            )
            if max_runtime_reached:
                break

            log.info(f'Compacting: {location}\n{part_to_compact}')
            self.compact_partition(
                spark=spark,
                location=location,
                partition_predicate=part_to_compact,
            )

        return None

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
        return log_snapshot_df

    @staticmethod
    def add_partitioning_info(
        log_snapshot_df: DataFrame,
        partition_spec: PartitionSpec,
        column_types: dict[str, T.DataType]
    ) -> DataFrame:
        pv_map = F.col('partitionValues')
        pred_map = F.transform_values(
            col=pv_map, f=lambda k, v: F.format_string('%s = "%s"', k, v)
        )
        part_expression = F.array_join(
            F.map_values(pred_map), delimiter=' AND '
        )
        part_struct = F.lit(None)
        if partition_spec.column_count > 0:
            part_struct = F.struct([
                pv_map.getItem(c).cast(column_types[c]).alias(c)
                for c in partition_spec.columns
            ])

        partition_spec.time_non_monotonic
        non_increasing: list[Column] = [
            part_struct.getField(e.name)
            for e in partition_spec.time_non_monotonic
        ]
        w1 = Window.partitionBy(*non_increasing).orderBy(part_struct)
        w2 = Window.partitionBy(part_struct).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing
        )
        df = log_snapshot_df.withColumns({
            'part_expression': part_expression,
            'part_struct': part_struct,
            'prev_mod_time': F.lag('modificationTime').over(w1),
            'n_files': F.count('*').over(w2),
        })
        return df.withColumns({
            'file_added_ts': F.timestamp_millis('modificationTime')
        })

    @staticmethod
    def find_partition_to_compact(
        spark: SparkSession,
        location: str,
        partition_spec: PartitionSpec,
        column_types: dict[str, T.DataType]
    ) -> str | None:
        raw_log_snapshot_df = DeltaTableFormat.build_log_snapshot_df(
            spark=spark, location=location
        )
        log_snapshot_df = DeltaTableFormat.add_partitioning_info(
            log_snapshot_df=raw_log_snapshot_df,
            partition_spec=partition_spec,
            column_types=column_types
        )
        preds = [
            F.col('n_files') > F.lit(1),
            F.col('prev_mod_time') > F.col('modificationTime')
        ]
        predicate = functools.reduce(operator.or_, preds)
        _pe = 'part_expression'
        part_to_compact = log_snapshot_df.where(predicate).select(
            F.min_by(_pe, 'part_struct').alias(_pe)
        ).collect()

        unordered_partition = None
        if len(part_to_compact) > 0:
            unordered_partition = part_to_compact[0][_pe]

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
            .option('replaceWhere', partition_predicate)
        )
        writer.save()
        return None
