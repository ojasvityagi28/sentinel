from scoring.rules import is_geo_impossible


def make_txn(location, timestamp):
    return {
        "transaction_id": "t",
        "user_id": 1,
        "amount_cents": 1000,
        "merchant": "Amazon",
        "location": location,
        "timestamp": timestamp,
    }

def test_delhi_to_london_in_one_hour_is_impossible():
    prev = make_txn("Delhi", "2026-10-08T10:00:00+00:00")
    curr = make_txn("London", "2026-10-08T11:00:00+00:00")
    assert is_geo_impossible(prev, curr) is True

def test_delhi_to_mumbai_in_ten_hours_is_possible():
    prev = make_txn("Delhi", "2026-10-08T10:00:00+00:00")
    curr = make_txn("Mumbai", "2026-10-08T20:00:00+00:00")
    assert is_geo_impossible(prev, curr) is False

def test_same_city_same_instant_is_possible():
    prev = make_txn("Delhi", "2026-10-08T10:00:00+00:00")
    curr = make_txn("Delhi", "2026-10-08T10:00:00+00:00")
    assert is_geo_impossible(prev, curr) is False


def test_different_cities_same_instant_is_impossible():
    prev = make_txn("Delhi", "2026-10-08T10:00:00+00:00")
    curr = make_txn("Mumbai", "2026-10-08T10:00:00+00:00")
    assert is_geo_impossible(prev, curr) is True