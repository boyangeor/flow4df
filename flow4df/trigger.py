from typing import TypedDict


class Trigger(TypedDict, total=False):
    """Trigger for a Structured Streaming query.

    https://spark.apache.org/docs/latest/streaming/apis-on-dataframes-and-datasets.html#triggers  # noqa
    """
    availableNow: bool
    processingTime: str
