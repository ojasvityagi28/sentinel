from scoring.rules import should_flag_geo


def txn(location, timestamp):
    return {"location": location, "timestamp": timestamp}


def test_user_recovers_when_trusted_baseline_was_an_away_city():
    trusted = txn("Delhi", "2026-10-16T10:00:00+00:00")     # first-ever transaction was away
    previous = txn("London", "2026-10-16T10:00:20+00:00")   # flagged earlier, user is really in London
    current = txn("London", "2026-10-16T10:00:40+00:00")
    assert should_flag_geo(trusted, previous, current) is False

def test_away_trip_is_flagged():
    home = txn("London", "2026-10-16T10:00:00+00:00")
    current = txn("Delhi", "2026-10-16T10:00:20+00:00")
    assert should_flag_geo(home, home, current) is True


def test_return_home_is_not_flagged():
    home = txn("London", "2026-10-16T10:00:00+00:00")
    away = txn("Delhi", "2026-10-16T10:00:20+00:00")      # previous was the flagged away trip
    current = txn("London", "2026-10-16T10:00:40+00:00")
    assert should_flag_geo(home, away, current) is False


def test_first_transaction_has_nothing_to_compare():
    current = txn("London", "2026-10-16T10:00:00+00:00")
    assert should_flag_geo(None, None, current) is False