# Sentinel

A real-time transaction risk engine. Simulated transactions flow through Kafka, are scored against three anomaly rules as they arrive, and flagged ones are stored in Postgres. A nightly Airflow job reviews borderline alerts, recomputes per-user risk profiles and writes a report. Everything is exposed through an authenticated REST API.

Stack: Python, Kafka, PostgreSQL, FastAPI, Airflow, Docker Compose, pytest (rules written test-first).

## Status

| Part | State |
|---|---|
| Kafka producer (simulated transactions) and consumer | Built |
| Three scoring rules, written test-first | Built |
| Postgres storage, alerts, risk profiles | Built |
| FastAPI endpoints with JWT auth | Built |
| Nightly Airflow DAG (review alerts, risk profiles, report) | Built, verified on schedule |
| Producer, consumer, API, Kafka, Postgres and Airflow in Docker Compose | Built, verified from a fresh clone |
| Latency measurement | Built (SQL query, results below) |
| Kubernetes manifests (consumer, API, Service, Secret placeholders) | Written, not deployed |

## Architecture

```text
 producer ──► Kafka topic "transactions" ──► consumer ──► Postgres
              (keyed by user_id,              │           ├─ transactions
               3 partitions)                  │           ├─ alerts  (status: open / dismissed)
                                              ▼           ├─ risk_profiles
                                    scoring/ (pure rules) ├─ operators
                                    + per-user in-memory cache └─ users (registered home city)

 Airflow (nightly): review amount alerts ──► compute risk profiles ──► CSV report
                    (reads and writes Postgres)

 operator ──► FastAPI (JWT) ──► Postgres
```

- Messages are keyed by `user_id`, so one user's transactions always land in one partition and are processed in order.
- For each message the consumer reads the user's history from an in-memory cache (loaded from Postgres on first sight, with the geo baseline seeded from the user's registered home city when there is no history), runs the rules, inserts the transaction, and writes one `alerts` row per rule that fired.
- Inserts use `ON CONFLICT DO NOTHING` on `transaction_id`, so Kafka's at-least-once redelivery cannot create duplicate rows, duplicate alerts or duplicate cache entries.
- The producer, consumer and API share one Docker image; each service gets its own start command in `docker-compose.yml`.

## Scoring rules

Each rule is a plain function with no Kafka or database access, which is what makes it testable in milliseconds.

| Rule | Fires when | Parameters |
|---|---|---|
| `geo` | The implied travel speed (haversine distance / elapsed time) from the user's trusted location is above the limit | 900 km/h; 5-city coordinate table |
| `amount` | The amount is more than 3 sample standard deviations from the mean of the user's recent amounts, high or low | needs at least 10 past amounts; window of 50 |
| `velocity` | More than 10 of the user's transactions (current one included) fall in the sliding 60 seconds ending at this one | 60 s window, limit 10 |

Baselines are chosen per rule:
- `geo` keeps a *trusted* location: the last transaction that did not itself fire `geo`. When a user has none yet, it starts from their registered home city (`users` table), timed at their first transaction. Every away transaction is flagged, including several in a row, and the return home is not.
- `amount` keeps flagged amounts out of its baseline, so one spike does not inflate the standard deviation for later comparisons.
- `velocity` counts every transaction, flagged or not, since a burst is exactly what it measures.

### A bug found in a fresh clone, and two fixes

The first version compared against the trusted location only, and trusted a user's first transaction blindly. In a fresh clone, one user's report showed 982 `geo` alerts on 991 transactions: that user's first-ever transaction was in an away city, so it became trusted, every later home-city transaction was flagged against it, and a flagged transaction never became the new baseline.

The first fix flagged only if a transaction was impossible against both the trusted location and the previous transaction. That recovered stuck users, but a second away transaction in a row matched the previous one, passed, became trusted, and the real return home was then flagged.

The final fix corrects the starting point instead: `initial_geo_baseline` uses the user's newest transaction without a `geo` alert, or else their registered home city timed at their first transaction, and the rule compares against the trusted location only. Both changes were written test-first; one test replays Mumbai, London, London, Mumbai, Mumbai (seconds apart) and expects flags on the two London transactions only. In a 10-minute end-to-end run (680 transactions, 10 away), all 10 away transactions were flagged and no home-city transaction was.

The producer's idea of where a user lives (`home_city()` in the producer) and the bank's record (`users`) are separate copies, written once with the same values, like an address taken at account opening. The consumer only reads the bank's copy.

## Nightly Airflow job

DAG `sentinel_nightly`, scheduled at 02:00 UTC, three tasks in order:

1. **`review_amount_alerts`**: for each open `amount` alert, re-checks the flagged amount against the user's last 200 earlier amounts (excluding amounts that themselves fired `amount`). If it is no longer anomalous against that larger history, the alert's status becomes `dismissed`. It needs at least 10 earlier amounts, otherwise the alert stays open. `geo` and `velocity` alerts are not reviewed.
2. **`compute_risk_profiles`**: upserts one row per user into `risk_profiles` (transaction count, flagged transactions counting only open alerts). Rerunning gives the same result, and users whose alerts were all dismissed are reset to zero.
3. **`generate_report`**: writes `reports/risk_report_<date>.csv`, the top 10 users by alert rate (users with at least 10 transactions).

"Dismissed" means the data no longer looks suspicious against a larger history. It does not mean the transaction was proven legitimate, and nothing is deleted. Detecting a lasting change in a user's spending (as opposed to noise) is not built.

Airflow runs as a single container in standalone mode with its own SQLite metadata, which is enough for one nightly DAG. It only runs while the host machine is on.

## Simulated data

The producer gives each user a home city and a typical amount. Most transactions are normal; a small share are away-city (2%), amount spikes at 10x (1%), or a 12-transaction burst from one user (0.5% chance per second). This makes the three rules fire at low rates instead of constantly.

## Data model

- `transactions`: `transaction_id` (PK), `user_id`, `amount_cents`, `merchant`, `location`, `timestamp`, `processed_at` (set by Postgres at insert time with `clock_timestamp()`). Indexed on `(user_id, timestamp DESC)` and `(timestamp DESC)`.
- `alerts`: `alert_id`, `transaction_id` (FK), `rule`, `status` (`open` or `dismissed`), `created_at`. One row per rule fired.
- `risk_profiles`: `user_id` (PK), `transaction_count`, `flagged_transactions`, `updated_at`.
- `operators`: API users, with bcrypt-hashed passwords.
- `users`: `user_id` (PK), `home_city`. Each simulated user's registered home city.

## API

All routes except `/health` and `/auth/login` require a JWT bearer token. Operators are created with a script; there is no public registration.

| Route | Purpose |
|---|---|
| `POST /auth/login` | Exchange operator credentials for a token |
| `GET /transactions` | Keyset-paginated list (`user_id`, `before` + `before_id` cursor, `limit` up to 100) |
| `GET /alerts` | Alerts joined to their transactions; filter by `user_id`, `rule`, `status`; cursor on `alert_id` |
| `GET /users/{id}/risk-profile` | Transaction count, flagged transactions, alert rate, open alerts per rule, last alert time |
| `GET /health` | Database reachability check |

Interactive docs are served at `/docs`. Queries use parameter placeholders throughout.

## Latency

Measured as `processed_at - producer timestamp`: the time from the producer creating a transaction to its row being written in Postgres. It includes Kafka transit, the history lookup, and the three rules. Alert rows and the commit follow just after.

Conditions: about 1 message per second, all services on one machine, first minute of each run discarded, consumer caught up before measuring. These runs were made with the producer and consumer running directly on the host, before containerisation and before the final geo-rule change, and were not repeated afterwards.

| Run | Samples | p50 | p95 | max |
|---|---|---|---|---|
| Postgres history lookup on every message | 537 | 5.4 ms | 8.8 ms | 19.8 ms |
| In-memory per-user cache | 312 | 4.1 ms | 5.2 ms | 8.1 ms |

One run each, so treat the improvement as indicative, not precise. p95 rests on a small number of slow rows. The numbers say nothing about throughput at higher load.

## Tests

29 tests cover the rules, the geo starting point and the alert-review decision, including opposite cases and boundaries (same city at the same instant, exactly 60 seconds old, identical amount history, too little history to review an alert, a first transaction in an away city, two away transactions in a row). Line coverage of `scoring/` is 100% (78 statements). The consumer, API and Airflow tasks are not covered by automated tests; they were checked by hand, including a full run from a fresh clone, so the coverage figure does not describe the whole project.

```bash
pip install -r requirements-dev.txt
pytest --cov=scoring --cov-report=term-missing
```

## Running it

1. Copy the environment file and fill it in:
   ```bash
   cp .env.example .env
   ```
   Set `SECRET_KEY` (for example `python3 -c "import secrets; print(secrets.token_hex(32))"`), choose a `POSTGRES_PASSWORD`, and set `AIRFLOW_UID` to the output of `id -u` (a wrong value makes the Airflow container unable to write the report).
2. Build and start everything:
   ```bash
   docker compose up -d --build
   ```
   A fresh database is created from `db/init.sql`. A database created before the `users` table existed needs the migration once:
   ```bash
   docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < migrations/001_users.sql
   ```
3. Create an operator for the API:
   ```bash
   docker compose exec api python -m scripts.create_operator
   ```
4. Open `http://localhost:8000/docs`, log in with `POST /auth/login`, and use **Authorize** with the returned token.
5. Airflow's UI is at `http://localhost:8080` (the generated login is in `docker logs sentinel-airflow`). To run the nightly job now instead of waiting for 02:00 UTC:
   ```bash
   docker compose exec airflow airflow dags unpause sentinel_nightly
   docker compose exec airflow airflow dags trigger sentinel_nightly
   ```
   The report appears in `reports/`.

Services run inside the Compose network: Kafka advertises its internal address, so code running outside Docker cannot connect to it.

## Known limitations

- `geo` is a speed rule only: activity in a far city is flagged until enough real time has passed for the trip to be possible (London to Delhi: about 7.5 hours), then accepted. After the system has been off, a user's trusted location can be hours old, so an away transaction right after the pause can pass. It also only knows five cities.
- Fixed thresholds are tuned to this simulation. Real systems tune per customer or segment.
- The z-score estimates spread from a small history. In one run, 4 of 7 `amount` alerts were ordinary amounts just past the cutoff, and amounts are not normally distributed, so a low-side flag can be impossible for users with wide spread. The nightly review recovers some of these, but not all.
- Re-running the review can dismiss further alerts, because dismissed amounts rejoin other alerts' baselines; the job is safe to repeat but not strictly idempotent.
- A lasting change in a user's spending keeps producing `amount` flags until it is absorbed by the baseline; detecting such shifts is not built.
- The cache lives in the consumer's memory (about 5 KB per user measured, so roughly 5 GB per million users). It is not shared between consumers and goes stale if an alert is dismissed, until the consumer restarts.
- Airflow runs on the same machine as everything else, so nightly runs only happen while that machine is on. A real deployment would run it on an always-on host.
- `/transactions` uses a two-field cursor (`before`, `before_id`); a single opaque token would be cleaner.
- Alerts record which rule fired but not the triggering value (speed, z-score, count).
- `passlib` is unmaintained and requires `bcrypt<5`, which is pinned in `requirements.txt`.

## Kubernetes (design only)

`k8s/` holds manifests for the consumer and API Deployments, an API Service with a readiness check on `/health`, and a Secret with placeholder values. They describe how Sentinel would run on a managed cluster such as GKE and have not been applied to one; the image name would first need to point at a container registry. The API could also run on Cloud Run, but the consumer needs an always-on process, which suits a Deployment better.