# Alert rules (rules_version 2) — what is verified and what is not

Alert rules run only when `OPENFARM_ALERT_RULES_ENABLED=true` (default **false**; AgroMind keeps it false
in every environment). Processing and the periodic refresh never depend on it.

Logic: `services/api/app/core/alert_rules.py` (pure, no I/O). Parameters: `AlertDefaults` in
`services/api/app/tasks/indices.py` (`None` = rule not configured, never evaluated). Soil rules:
`evaluate_soil_alerts` in `app/core/soil_intelligence.py`, persisted by `_create_soil_alerts` in
`app/tasks/soil.py`. Tests: `services/api/tests/test_alert_rules.py`.

## Status per rule

| Rule | Status | Parameters | Needs before enabling for users |
|---|---|---|---|
| `ndvi_threshold` | Logic verified | mean < 0.30 (high < 0.15) | Calibration per crop / phenological stage (fallow and emergence are legitimately below 0.30) |
| `evi_threshold` | Logic verified | mean < 0.20 (high < 0.10) | Same; EVI also saturates at 1.0 on dense canopies (clip in the formula) |
| `savi_threshold` | Logic verified | mean < 0.25 (high < 0.10) | Same; SAVI L=0.5 range differs from NDVI |
| `ndvi_drop`, `evi_drop`, `savi_drop` | Logic verified | drop ≥ 15 % vs. mean of the last 4 prior observations of the same index; severity low / medium ≥ 20 % / high ≥ 30 % | Calibration: 15 % and a 4-scene window are not validated against crop cycles (harvest is a legitimate drop) |
| `ndwi_threshold`, `ndwi_drop` | **Not configured** | — | A validated water-stress index and rule. McFeeters NDWI (green−NIR) measures open water and is negative over vegetation by construction; a 0 threshold fired on 35/35 real scenes |
| `soil_ph_*`, `soil_soc_*`, `soil_sand_high`, `soil_cec_low`, `soil_compaction`, `soil_waterlogging` | Persistence fixed and tested; thresholds unchanged | see `SOIL_ALERT_THRESHOLDS` / `evaluate_soil_alerts` | Agronomic validation of the thresholds for the region; product decision on whether static soil facts are "alerts" |

"Logic verified" = direction, scale, missing/invalid data, reference and idempotency are covered by tests;
it does **not** mean the threshold is agronomically right. No threshold value was changed in v2.

## Semantics (v2)

* **Threshold**: compares the field's zonal mean of the observed scene with the index's threshold; equal is
  not below. Invalid values (None/NaN/inf) never alert.
* **Drop**: reference = mean of the last `drop_window` (4) valid observations of the **same index**,
  dated **strictly before** the observed date (the observed value, or an older copy of the same date when a
  scene is reprocessed, is never part of its own reference). Order-independent: history is sorted by date.
  A date with two different values is ambiguous and ignored. Fewer than 4 valid prior observations, or a
  reference ≤ 0, ⇒ the rule abstains.
* **Traceability** (`rule_params_json`): `rules_version`, configured `threshold`/`threshold_high` or
  `drop_pct`/`window` (same keys as v1), `observed`, `observed_drop_pct`, `reference_mean`,
  `reference_dates`, `calibration`. `weather_context` is the stored weather (null when absent);
  `soil_context` for soil alerts holds the measured value(s), threshold and depth as produced by the rule.

## Identity, re-runs and managed alerts

Policy (v2): **the first detection is kept**. A re-evaluation never overwrites the original evidence
nor the management state of an existing alert, open or closed. A changed or vanished result is not
reflected automatically.

Enforced by the database (migration 0015), not only by the code:

| Index | Columns | Applies to | Effect |
|---|---|---|---|
| `uq_alerts_field_rule_date` | `(field_id, rule_name, date)` | every rule | one alert per field, rule and observation date; index rules can fire again on a **new** date |
| `uq_alerts_soil_field_rule` | `(field_id, rule_name) WHERE left(rule_name,5)='soil_'` | soil rules | one alert per field and soil rule, whatever the refresh date |

* `alerts.date` is `NOT NULL` since migration 0001 and both writers always set it, so PostgreSQL's
  default `NULLS DISTINCT` (PostgreSQL 16.4 in prod and dev) can never let a duplicate through; inserting a
  NULL date is rejected (tested).
* Writers use `INSERT … ON CONFLICT DO NOTHING` (soil: no conflict target, so either index applies). Plain
  inserts that bypass it are rejected with `duplicate key value violates unique constraint`. Concurrent
  writers cannot race past the indexes.
* Historical alerts: 0015 changes no row. It refuses to run (explicit error, nothing deleted) if the table
  already holds duplicates of either identity; prod and dev had 0 alerts on 2026-10-14.

## Start-up, migrations and health

* The API container runs `alembic upgrade head` before uvicorn (`services/api/Dockerfile` `CMD`): up to 10
  attempts, 3 s apart. **If every attempt fails, uvicorn still starts** (the loop ends on a successful
  `sleep`) and the API serves with the older schema. The logs only say `Waiting for DB... attempt N`; the
  real cause is printed just above each line. Unchanged on purpose: failing the start would turn a refused
  migration into an API crash loop (all reads used by AgroMind down) and the image is shared with the
  upstream compose files.
* `/healthz` now tells the truth: it answers **HTTP 503** when the database or Redis fails **or** when the
  schema is behind the code (`schema: database at 0014; code requires 0015`); 200 with
  `{"status":"ok","schema":"schema at 0015"}` otherwise. It used to answer 200 with `status: unhealthy`,
  which the container healthcheck (`curl -f`) accepted. A schema *ahead* of the code (after a code
  rollback) is reported but not a failure.
* The processor/beat do not run migrations. With the schema behind, index/soil alert inserts would fail
  (their `ON CONFLICT` needs the 0015 indexes) - only relevant if `OPENFARM_ALERT_RULES_ENABLED=true`, which
  must therefore never be enabled while `/healthz` reports the schema behind.
* Known, out of scope: with Redis unreachable `/healthz` answers 500 (the rate limiter fails before the
  handler) - already non-2xx, so the healthcheck fails as it should.

## Deploying migration 0015 (controlled procedure)

1. Keep `OPENFARM_ALERT_RULES_ENABLED=false` (CORE and EDGE).
2. Read-only pre-checks on the target database:
   `SELECT version_num FROM alembic_version` (expect 0014);
   `SELECT field_id, rule_name, date, count(*) FROM alerts GROUP BY 1,2,3 HAVING count(*)>1` (expect 0 rows);
   `SELECT field_id, rule_name, count(*) FROM alerts WHERE left(rule_name,5)='soil_' GROUP BY 1,2 HAVING count(*)>1`
   (expect 0 rows). Any row: stop, review the duplicates with the product owner; never delete blindly.
3. Fresh `pg_dump -Fc` of the engine database.
4. Deploy **EDGE first** (its API applies the migration), then CORE (processor + beat, same commit).
5. Verify on EDGE: logs contain `Running upgrade 0014 -> 0015` and no `Waiting for DB`;
   `docker exec <api> curl -s -w ' %{http_code}' localhost:8000/healthz` → `200 … schema at 0015`;
   both `uq_alerts_*` indexes exist (`pg_indexes`). From the AgroMind backend: API and tiler via aliases 200.
6. If `/healthz` answers 503 with `schema: …`: the migration did not apply. The API keeps serving reads with
   the old schema (no outage for AgroMind); do not enable alerts; read the error above `Waiting for DB` in
   the API logs, fix the cause, then restart the API container (it re-runs `alembic upgrade head`).
7. Rollback: run `alembic downgrade 0014` from the new API container **before** deploying the previous
   commit; it only drops the two indexes (no data change). Without the downgrade the previous code
   (`7ea72e9`) still serves on a 0015 schema, but its start-up first fails 10 times
   (`Can't locate revision identified by '0015'`, ~30 s) and its `/healthz` does not check the schema
   (verified on the disposable database).
