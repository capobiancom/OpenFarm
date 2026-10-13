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

* An alert is identified by `(field_id, rule_name, date)` — unique index `uq_alerts_field_rule_date`
  (migration 0015). Inserts use `ON CONFLICT DO NOTHING`: reprocessing a scene, retries and concurrent
  writers never duplicate it.
* The **first** detection wins and is never overwritten: if a re-evaluation produces different numbers, or
  no longer triggers, the existing alert (open or closed by a user) and its evidence are kept unchanged.
  Status/assignment history is therefore never lost; a changed result is not reflected automatically.
* Soil: a rule already raised for a field (open or closed) is not raised again on later soil refreshes.
* Migration 0015 refuses to run (explicit error) if duplicated identities already exist; it never deletes.
