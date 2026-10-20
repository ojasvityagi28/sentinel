from scoring.review import alerts_to_dismiss

def review_amount_alerts(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                a.alert_id,
                t.amount_cents AS flagged_amount,
                h.amount_cents AS history_amount
            FROM alerts a
            JOIN transactions t ON t.transaction_id = a.transaction_id
            JOIN LATERAL (
                SELECT h.amount_cents
                FROM transactions h
                WHERE h.user_id = t.user_id
                  AND h.timestamp < t.timestamp
                  AND NOT EXISTS (
                      SELECT 1 FROM alerts ha
                      WHERE ha.transaction_id = h.transaction_id
                        AND ha.rule = 'amount'
                        AND ha.status = 'open'
                  )
                ORDER BY h.timestamp DESC
                LIMIT 200
            ) h ON true
            WHERE a.rule = 'amount'
              AND a.status = 'open';
        """)
        rows = cur.fetchall()

        dismiss_ids = alerts_to_dismiss(rows)

        if dismiss_ids:
            cur.execute(
                "UPDATE alerts SET status = 'dismissed' WHERE alert_id = ANY(%s)",
                (list(dismiss_ids),)
            )

        conn.commit()

    return dismiss_ids



def compute_risk_profiles(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT t.user_id,
                   COUNT(DISTINCT t.transaction_id) AS transaction_count,
                   COUNT(DISTINCT a.transaction_id) AS flagged_transactions
            FROM transactions t
            LEFT JOIN alerts a
              ON a.transaction_id = t.transaction_id AND a.status = 'open'
            GROUP BY t.user_id;
        """)
        rows = cur.fetchall()

        for user_id, tx_count, flagged in rows:
            cur.execute("""
                INSERT INTO risk_profiles (user_id, transaction_count, flagged_transactions, updated_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (user_id)
                DO UPDATE SET transaction_count = EXCLUDED.transaction_count,
                              flagged_transactions = EXCLUDED.flagged_transactions,
                              updated_at = now();
            """, (user_id, tx_count, flagged))

        conn.commit()
    return rows

import csv


def generate_report(conn, out_dir, run_date):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT user_id, transaction_count, flagged_transactions,
                   round(flagged_transactions::numeric / transaction_count, 4) AS alert_rate
            FROM risk_profiles
            WHERE transaction_count >= 10
            ORDER BY alert_rate DESC, flagged_transactions DESC
            LIMIT 10;
        """)
        rows = cur.fetchall()

    path = f"{out_dir}/risk_report_{run_date}.csv"
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "transaction_count", "flagged_transactions", "alert_rate"])
        writer.writerows(rows)
    return path