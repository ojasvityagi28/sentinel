from scoring.review import should_dismiss_amount_alert, alerts_to_dismiss

LONG = [700, 1300] * 100      # 200 amounts, mean 1000, sample std about 301 (cutoff near 1,900)
def test_borderline_amount_is_dismissed_with_long_history():
    assert should_dismiss_amount_alert(1500, LONG) is True

def test_genuinely_large_amount_stays_open():
    assert should_dismiss_amount_alert(5000, LONG) is False


def test_too_little_history_stays_open():
    assert should_dismiss_amount_alert(1500, LONG[:5]) is False

def test_alerts_to_dismiss_groups_and_applies_rule():
    rows = (
        [(1, 100, 100)] * 11
        + [(2, 500, 100)] * 11
        + [(3, 1000, 50)] * 5
    )

    result = alerts_to_dismiss(rows)

    assert result == {1}