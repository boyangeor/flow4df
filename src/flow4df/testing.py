#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Using `assertSchemaEqual` from pyspark.testing.utils requires `pandas` ...
It's fixed in pyspark > 3.5.1 but not released yet.
"""
import difflib
from typing import Any
from itertools import zip_longest
from pyspark.sql.types import StructType, StructField
from pyspark.errors import PySparkAssertionError


def assertSchemaEqual(actual: StructType, expected: StructType):
    r"""
    A util function to assert equality between DataFrame schemas `actual` and `expected`.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    actual : StructType
        The DataFrame schema that is being compared or tested.
    expected : StructType
        The expected schema, for comparison with the actual schema.

    Notes
    -----
    When assertSchemaEqual fails, the error message uses the Python `difflib` library to display
    a diff log of the `actual` and `expected` schemas.

    Examples
    --------
    >>> from pyspark.sql.types import StructType, StructField, ArrayType, IntegerType, DoubleType
    >>> s1 = StructType([StructField("names", ArrayType(DoubleType(), True), True)])
    >>> s2 = StructType([StructField("names", ArrayType(DoubleType(), True), True)])
    >>> assertSchemaEqual(s1, s2)  # pass, schemas are identical

    >>> df1 = spark.createDataFrame(data=[(1, 1000), (2, 3000)], schema=["id", "number"])
    >>> df2 = spark.createDataFrame(data=[("1", 1000), ("2", 5000)], schema=["id", "amount"])
    >>> assertSchemaEqual(df1.schema, df2.schema)  # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
    ...
    PySparkAssertionError: [DIFFERENT_SCHEMA] Schemas do not match.
    --- actual
    +++ expected
    - StructType([StructField('id', LongType(), True), StructField('number', LongType(), True)])
    ?                               ^^                               ^^^^^
    + StructType([StructField('id', StringType(), True), StructField('amount', LongType(), True)])
    ?                               ^^^^                              ++++ ^
    """
    if not isinstance(actual, StructType):
        raise PySparkAssertionError(
            error_class="UNSUPPORTED_DATA_TYPE",
            message_parameters={"data_type": type(actual)},
        )
    if not isinstance(expected, StructType):
        raise PySparkAssertionError(
            error_class="UNSUPPORTED_DATA_TYPE",
            message_parameters={"data_type": type(expected)},
        )

    def compare_schemas_ignore_nullable(s1: StructType, s2: StructType):
        if len(s1) != len(s2):
            return False
        zipped = zip_longest(s1, s2)
        for sf1, sf2 in zipped:
            if not compare_structfields_ignore_nullable(sf1, sf2):
                return False
        return True

    def compare_structfields_ignore_nullable(actualSF: StructField, expectedSF: StructField):
        if actualSF is None and expectedSF is None:
            return True
        elif actualSF is None or expectedSF is None:
            return False
        if actualSF.name != expectedSF.name:
            return False
        else:
            return compare_datatypes_ignore_nullable(actualSF.dataType, expectedSF.dataType)

    def compare_datatypes_ignore_nullable(dt1: Any, dt2: Any):
        # checks datatype equality, using recursion to ignore nullable
        if dt1.typeName() == dt2.typeName():
            if dt1.typeName() == "array":
                return compare_datatypes_ignore_nullable(dt1.elementType, dt2.elementType)
            elif dt1.typeName() == "struct":
                return compare_schemas_ignore_nullable(dt1, dt2)
            else:
                return True
        else:
            return False

    # ignore nullable flag by default
    if not compare_schemas_ignore_nullable(actual, expected):
        generated_diff = difflib.ndiff(str(actual).splitlines(), str(expected).splitlines())

        error_msg = "\n".join(generated_diff)

        raise PySparkAssertionError(
            error_class="DIFFERENT_SCHEMA",
            message_parameters={"error_msg": error_msg},
        )
