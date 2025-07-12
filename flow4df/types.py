from typing import TypeAlias, Union
from pyspark.sql import DataFrameReader, DataFrameWriter
from pyspark.sql.streaming.readwriter import DataStreamReader, DataStreamWriter

Reader: TypeAlias = Union[DataFrameReader, DataStreamReader]
Writer: TypeAlias = Union[DataFrameWriter, DataStreamWriter]
