from enum import Enum


class OutputMode(Enum):
    append = 1     # streaming and batch
    update = 2     # streaming
    complete = 3   # streaming
    error = 4      # batch
    overwrite = 5  # batch
    ignore = 6     # batch
