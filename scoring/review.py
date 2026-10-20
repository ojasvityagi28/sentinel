
from scoring.rules import is_amount_anomalous

MIN_HISTORY = 10

def should_dismiss_amount_alert(flagged_amount, earlier_amounts):
    if len(earlier_amounts) < MIN_HISTORY:
        return False                      # can't re-check with so little data: leave it open

    return not is_amount_anomalous(flagged_amount, earlier_amounts)


def alerts_to_dismiss(rows):
    grouped = {}
    for alert_id, flagged_amount, history_amount in rows:
        if alert_id not in grouped:
            grouped[alert_id] = (flagged_amount, [])
        grouped[alert_id][1].append(history_amount)

    dismiss = set()
    for alert_id, (flagged_amount, history) in grouped.items():
        if should_dismiss_amount_alert(flagged_amount, history):
            dismiss.add(alert_id)

    return dismiss


   