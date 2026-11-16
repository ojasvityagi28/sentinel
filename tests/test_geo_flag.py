from scoring.rules import should_flag_geo,initial_geo_baseline
from datetime import datetime, timezone

T = datetime(2026, 10, 16, 10, 0, 0, tzinfo=timezone.utc)
EARLIER = datetime(2026, 10, 16, 9, 0, 0, tzinfo=timezone.utc)

def txn(location, timestamp):
    return {"location": location, "timestamp": timestamp}


def test_no_trusted_location_is_not_flagged():
    current = txn("London", "2026-10-16T10:00:00+00:00")
    assert should_flag_geo(None, current) is False


def test_first_transaction_away_is_flagged_against_seeded_home():
    # seed: home city at the time of the first transaction
    seeded_home = txn("London", "2026-10-16T10:00:00+00:00")
    current = txn("Delhi", "2026-10-16T10:00:00+00:00")
    assert should_flag_geo(seeded_home, current) is True


def test_first_transaction_at_home_is_not_flagged():
    seeded_home = txn("London", "2026-10-16T10:00:00+00:00")
    current = txn("London", "2026-10-16T10:00:00+00:00")
    assert should_flag_geo(seeded_home, current) is False


def test_away_trip_is_flagged():
    trusted = txn("London", "2026-10-16T10:00:00+00:00")
    current = txn("Delhi", "2026-10-16T10:00:20+00:00")
    assert should_flag_geo(trusted, current) is True


def test_second_away_transaction_is_also_flagged():
    # the first Delhi was flagged, so trusted is still London
    trusted = txn("London", "2026-10-16T10:00:00+00:00")
    current = txn("Delhi", "2026-10-16T10:00:40+00:00")
    assert should_flag_geo(trusted, current) is True


def test_return_home_is_not_flagged():
    trusted = txn("London", "2026-10-16T10:00:00+00:00")
    current = txn("London", "2026-10-16T10:00:40+00:00")
    assert should_flag_geo(trusted, current) is False


def test_genuine_trip_is_not_flagged():
    # about 6,700 km in 9 hours is about 745 km/h, under 900
    trusted = txn("London", "2026-10-16T10:00:00+00:00")
    current = txn("Delhi", "2026-10-16T19:00:00+00:00")
    assert should_flag_geo(trusted, current) is False


def test_uses_latest_unflagged_transaction_when_there_is_one():
    assert initial_geo_baseline(("London", EARLIER), "London", T) == ("London", EARLIER)


def test_seeds_home_city_at_first_transaction_time_when_no_history():
    assert initial_geo_baseline(None, "London", T) == ("London", T)


def test_no_history_and_no_home_city_gives_no_baseline():
    assert initial_geo_baseline(None, None, T) is None


def test_alternating_sequence_flags_only_the_away_transactions():
    # trusted moves only when a transaction is not flagged (the consumer's rule)
    seq = [
        txn("Mumbai", "2026-10-16T10:00:00+00:00"),
        txn("London", "2026-10-16T10:00:20+00:00"),
        txn("London", "2026-10-16T10:00:40+00:00"),
        txn("Mumbai", "2026-10-16T10:01:00+00:00"),
        txn("Mumbai", "2026-10-16T10:01:20+00:00"),
    ]
    trusted = txn("Mumbai", "2026-10-16T10:00:00+00:00")   # seeded home
    flags = []
    for current in seq:
        flagged = should_flag_geo(trusted, current)
        flags.append(flagged)
        if not flagged:
            trusted = current
    assert flags == [False, True, True, False, False]