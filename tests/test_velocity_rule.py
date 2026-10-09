from datetime import datetime, timedelta, timezone

from scoring.rules import is_velocity_anomalous

NOW = datetime(2026, 10, 8, 10, 0, 0, tzinfo=timezone.utc)


def ts(seconds_ago):
    return (NOW - timedelta(seconds=seconds_ago)).isoformat()


def test_eleven_transactions_in_a_minute_is_anomalous():
    past = [ts(s) for s in range(1, 11)]  # 10 past, at 1..10 seconds ago
    assert is_velocity_anomalous(NOW.isoformat(), past) is True

def test_ten_total_in_window_is_not_anomalous():
    past = [ts(s) for s in range(1, 10)]  # 9 past + current = 10, not over the limit
    assert is_velocity_anomalous(NOW.isoformat(), past) is False


def test_old_transactions_outside_window_are_not_counted():
    past = [ts(s) for s in range(61, 71)]  # 10 past, all older than 60s
    assert is_velocity_anomalous(NOW.isoformat(), past) is False


def test_transaction_exactly_60s_ago_is_outside_window():
    past = [ts(s) for s in range(1, 10)] + [ts(60)]  # 9 inside + 1 at the edge
    assert is_velocity_anomalous(NOW.isoformat(), past) is False