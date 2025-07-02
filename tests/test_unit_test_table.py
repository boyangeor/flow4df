from flow4df import Table
from flow4df.table import UnitTestTable


def test_unit_test_table(example_table_1: Table) -> None:
    utt = UnitTestTable.from_table(example_table_1)
    assert utt.storage == example_table_1.storage_stub
    assert utt.storage_stub == example_table_1.storage_stub
