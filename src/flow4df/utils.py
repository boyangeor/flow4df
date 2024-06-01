"""
Commonly used functions.
"""
from pyspark.sql import types as T
from black import format_str, FileMode


def prettify_schema(schema: T.StructType) -> str:
    _pad = max([len(c) for c in schema.names])
    prefixes = [
        f'{i + 1}. {field.name}' for i, field in enumerate(schema.fields)
    ]
    _pad = max([len(c) for c in prefixes])
    rows = ['Schema:']
    rows.extend([
        f'{p.ljust(_pad)} {field.dataType.typeName().upper()}'
        for p, field in zip(prefixes, schema.fields)
    ])
    return '\n'.join(rows)


def format_schema(schema: T.StructType, single_quotes: bool = True) -> None:
    fixed = format_str(str(schema), mode=FileMode())
    if single_quotes:
        return fixed.replace('"', "'")
    else:
        return fixed
