from scoring.rules import is_amount_anomalous

PAST = [1000, 1200, 800, 1000, 1000, 1100, 900, 1000, 1050, 950]  # mean 1000, std ~100

SAME = [1000] * 10      # 10 identical amounts, so std dev is 0


def test_identical_history_flags_any_different_amount():
    assert is_amount_anomalous(2000, SAME) is True

def test_identical_history_does_not_flag_same_amount():
    assert is_amount_anomalous(1000, SAME) is False
    
def test_amount_far_above_history_is_anomalous():
    assert is_amount_anomalous(5000, PAST) is True

def test_normal_amount_is_not_anomalous():
    assert is_amount_anomalous(1100, PAST) is False

def test_too_little_history_is_not_flagged():
    assert is_amount_anomalous(5000, PAST[:5]) is False

def test_amount_far_below_history_is_anomalous():
    assert is_amount_anomalous(600, PAST) is True