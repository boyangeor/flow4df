from flow4df import PartitionSpec


def test_init() -> None:
    ps1 = PartitionSpec(
        time_non_monotonic=['planet'],
        time_monotonic_increasing=['event_year', 'event_date'],
        time_bucketing_column=['event_ts'],
    )
    e1 = ['planet', 'event_year', 'event_date']
    assert ps1.partition_columns == e1

    e2 = ['planet', 'event_year', 'event_date', '_time_bucket']
    assert ps1.partition_by == e2
    return None
