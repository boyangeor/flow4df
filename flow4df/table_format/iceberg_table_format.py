import re
import json
import logging
import operator
import functools
import datetime as dt
from dataclasses import dataclass, field
from pyspark.sql import SparkSession, DataFrame, Column, Window, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import LongType, StructType
from pyspark.sql.streaming.readwriter import DataStreamWriter

from flow4df import type_annotations, enums
from flow4df.table_format.table_format import TableFormat
from flow4df.table_format.table_format import TableStats
from flow4df import DataInterval, PartitionSpec, TableIdentifier
from flow4df.column_stats import ColumnStats

TABLE_FORMAT_NAME = 'iceberg'


@dataclass(frozen=True, kw_only=True)
class IcebergTableFormat(TableFormat):

    def init_table(
        self,
        spark: SparkSession,
        location: str,
        table_identifier: TableIdentifier,
        table_schema: StructType,
        partition_spec: PartitionSpec,
    ) -> None:
        del location
        table_exists = spark.catalog.tableExists(table_identifier.full_name)
        if not table_exists:
            empty_df = spark.createDataFrame([], schema=table_schema)
            part_cols = [F.col(c) for c in partition_spec.columns]
            writer_v2 = (
                empty_df
                .writeTo(table_identifier.full_name)
                .partitionedBy(*part_cols)
            )
            writer_v2.create()

        return None

    def build_batch_writer(
        self,
        df: DataFrame,
        table_identifier: TableIdentifier,
        output_mode: enums.OutputMode,
        partition_spec: PartitionSpec,
    ) -> type_annotations.Writer:
        del output_mode
        part_cols = [F.col(c) for c in partition_spec.columns]
        writer = (
            df
            .writeTo(table_identifier.full_name)
            .partitionedBy(*part_cols)
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
        del data_interval
        if isinstance(writer, DataStreamWriter):
            return (
                writer
                .format(TABLE_FORMAT_NAME)
                .option('path', location)
            )
        # It's a batch V2 writer
        # Add txnVersion equivalent
        # Add userMetadata equivalent
        return writer

    def configure_session(
        self,
        spark: SparkSession,
        table_identifier: TableIdentifier,
        catalog_location: str,
    ) -> None:
        catalog_impl = 'org.apache.iceberg.spark.SparkCatalog'
        catalog_config = f'spark.sql.catalog.{table_identifier.catalog}'
        spark.conf.set(catalog_config, catalog_impl)
        spark.conf.set(f'{catalog_config}.type', 'hadoop')
        spark.conf.set(f'{catalog_config}.warehouse', catalog_location)
        return None
