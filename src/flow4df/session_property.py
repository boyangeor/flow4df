"""
Spark configuration properties, useful for autocomplete.
"""
from typing import Literal

SessionProperty = Literal[
    'spark.log.level',
    'spark.driver.memory',
    'spark.sql.session.timeZone',
    'spark.sql.shuffle.partitions',
    'spark.sql.extensions',
    'spark.sql.catalog.spark_catalog',
    'spark.jars.packages',
    'spark.submit.pyFiles',
    'spark.files.maxPartitionBytes',
    'spark.files.ignoreCorruptFiles',
    # https://books.japila.pl/spark-structured-streaming-internals/configuration-properties/#statestoremaintenanceinterval
    'spark.sql.streaming.stateStore.maintenanceInterval',
    # https://books.japila.pl/spark-structured-streaming-internals/configuration-properties/#minbatchestoretain
    'spark.sql.streaming.minBatchesToRetain',
    # https://books.japila.pl/spark-structured-streaming-internals/configuration-properties/#statestoreproviderclass
    'spark.sql.streaming.stateStore.providerClass',
]

DeltaProperty = Literal[
    'spark.databricks.delta.retentionDurationCheck.enabled',
    'spark.databricks.delta.snapshotPartitions',
    'spark.databricks.delta.stalenessLimit',
    'spark.databricks.delta.schema.autoMerge.enabled',
]

