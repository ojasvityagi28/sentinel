import math
from datetime import datetime

CITY_COORDS = {
    "Delhi": (28.6139, 77.2090),
    "Mumbai": (19.0760, 72.8777),
    "Bangalore": (12.9716, 77.5946),
    "London": (51.5074, -0.1278),
    "Dubai": (25.2048, 55.2708),
}

EARTH_RADIUS_KM = 6371
MAX_SPEED_KMH = 900  # roughly airliner speed


def haversine_km(city_a, city_b):
    lat1, lon1 = CITY_COORDS[city_a]
    lat2, lon2 = CITY_COORDS[city_b]

    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def is_geo_impossible(prev_txn, curr_txn):
    prev_time = datetime.fromisoformat(prev_txn["timestamp"])
    curr_time = datetime.fromisoformat(curr_txn["timestamp"])

    hours = (curr_time - prev_time).total_seconds() / 3600
    distance_km = haversine_km(prev_txn["location"], curr_txn["location"])

    if distance_km == 0:
        return False          # same city, always possible

    if hours <= 0:
        return True           # different cities at the same instant (or out of order)

    speed = distance_km / hours
    return speed > MAX_SPEED_KMH


MIN_HISTORY = 10
Z_THRESHOLD = 3


def is_amount_anomalous(amount_cents, past_amounts):
    n = len(past_amounts)

    if n < MIN_HISTORY:
        return False          # not enough history to judge

    mean = sum(past_amounts) / n

    sum_sq = 0
    for a in past_amounts:
        sum_sq += (a - mean) ** 2
    std_dev = math.sqrt(sum_sq / (n - 1))    # sample std dev

    if std_dev == 0:
        return amount_cents != mean          # identical history: any change is unusual

    z = (amount_cents - mean) / std_dev
    return abs(z) > Z_THRESHOLD


def is_velocity_anomalous(curr_timestamp, past_timestamps, window_seconds=60, max_txns=10):
    curr_time = datetime.fromisoformat(curr_timestamp)

    count = 1                      # the current transaction counts
    for ts_str in past_timestamps:
        past_time = datetime.fromisoformat(ts_str)
        gap = (curr_time - past_time).total_seconds()
        if 0 <= gap < window_seconds:
            count += 1

    return count > max_txns


def should_flag_geo(trusted, previous, current):
    if trusted is None or previous is None:
        return False                      # no history yet: nothing to compare with
    return is_geo_impossible(trusted, current) and is_geo_impossible(previous, current)