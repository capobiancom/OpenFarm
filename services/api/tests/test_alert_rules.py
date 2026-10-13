"""Alert rules (rules_version 2): index separation, reference without the
current value, NDWI not configured, insufficient evidence, idempotency,
soil rules, kill switch, compatibility with the periodic refresh and the
AgroMind contracts.

Pure-logic tests always run; tests marked ``db`` need the disposable PostGIS
of conftest.py (DATABASE_URL_SYNC, migrated to head) and are skipped
otherwise. Numbers are the field means the pipeline computes from fake
reflectances: NDVI = (B08-B04)/(B08+B04), e.g. B04 0.3 / B08 0.7 -> 0.4.
"""

from __future__ import annotations

import math
import os
import random
from datetime import date, timedelta

import pytest

from app.core.alert_rules import clean_history, evaluate_drop, evaluate_index_rules, evaluate_threshold
from app.tasks.indices import INDEX_REGISTRY, get_index

needs_db = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL_SYNC"), reason="needs a test PostGIS database"
)

NDVI, EVI, SAVI, NDWI = (get_index(k) for k in ("ndvi", "evi", "savi", "ndwi"))
D = date(2026, 9, 28)


def _hist(*pairs):
    return [(D - timedelta(days=d), v) for d, v in pairs]


# ── 1. NDWI: no crop water-stress alert from a negative value ────────


def test_ndwi_negative_value_never_alerts():
    for value in (-0.05, -0.5, -0.9):
        assert evaluate_index_rules(NDWI, D, value, _hist((40, 0.1), (30, 0.2), (20, 0.1), (10, 0.2))) == []


def test_ndwi_rules_are_explicitly_not_configured():
    cfg = NDWI.alerts
    assert cfg.threshold is None and cfg.threshold_high is None and cfg.drop_pct is None
    assert cfg.calibration.startswith("not configured")


# ── 3/4/5. reference, ordering, insufficient or invalid data ─────────


def test_current_value_is_not_part_of_its_reference():
    # An older copy of the same date (reprocessing) and a later date are ignored.
    history = _hist((40, 0.6), (30, 0.6), (20, 0.6), (10, 0.6), (0, 0.1), (-5, 0.9))
    c = evaluate_drop(NDVI, D, 0.42, history)
    assert c is not None
    assert c.params["reference_mean"] == pytest.approx(0.6)
    assert D.isoformat() not in c.params["reference_dates"]
    assert c.params["observed_drop_pct"] == pytest.approx(30.0)


def test_result_does_not_depend_on_input_order():
    history = _hist((50, 0.7), (40, 0.6), (30, 0.65), (20, 0.62), (10, 0.58))
    expected = evaluate_index_rules(NDVI, D, 0.4, history)
    assert [c.rule_name for c in expected] == ["ndvi_drop"]
    rng = random.Random(7)
    for _ in range(50):
        shuffled = history[:]
        rng.shuffle(shuffled)
        assert evaluate_index_rules(NDVI, D, 0.4, shuffled) == expected


@pytest.mark.parametrize(
    "history",
    [
        _hist((30, 0.6), (20, 0.6), (10, 0.6)),  # only 3 prior observations
        _hist((40, None), (30, 0.6), (20, float("nan")), (10, 0.6), (5, float("inf"))),
        _hist((40, 0.6), (30, 0.6), (20, 0.6), (10, 0.6), (10, 0.2)),  # ambiguous date dropped
        [],
    ],
    ids=["too-few", "invalid-values", "ambiguous-date", "empty"],
)
def test_insufficient_or_invalid_history_abstains(history):
    assert evaluate_drop(NDVI, D, 0.1, history) is None


def test_invalid_observation_never_alerts():
    good = _hist((40, 0.6), (30, 0.6), (20, 0.6), (10, 0.6))
    for value in (None, float("nan"), float("inf"), "x"):
        assert evaluate_index_rules(NDVI, D, value, good) == []


def test_non_positive_reference_abstains_from_relative_drop():
    assert evaluate_drop(NDVI, D, -0.3, _hist((40, -0.1), (30, -0.1), (20, 0.0), (10, -0.2))) is None


def test_clean_history_dedupes_identical_values_and_sorts():
    h = clean_history([(D - timedelta(days=1), 0.5), (D - timedelta(days=3), 0.4),
                       (D - timedelta(days=1), 0.5), (D, 0.1)], before=D)
    assert h == [(D - timedelta(days=3), 0.4), (D - timedelta(days=1), 0.5)]


# ── 9. valid cases still alert, with full traceability ───────────────


@pytest.mark.parametrize(
    "idx,value,severity",
    [(NDVI, 0.2, "medium"), (NDVI, 0.1, "high"), (EVI, 0.15, "medium"), (SAVI, 0.05, "high")],
)
def test_threshold_alerts_below_configured_value(idx, value, severity):
    c = evaluate_threshold(idx, D, value)
    assert c.rule_name == f"{idx.key}_threshold" and c.severity == severity
    assert c.index_type == idx.key and c.observed_date == D
    assert c.params["threshold"] == idx.alerts.threshold
    assert c.params["observed"] == pytest.approx(value)
    assert c.params["comparison"] == "below" and c.params["rules_version"] == 2
    assert evaluate_threshold(idx, D, idx.alerts.threshold) is None  # boundary: not below


def test_drop_severity_bands_and_params():
    h = _hist((40, 0.6), (30, 0.6), (20, 0.6), (10, 0.6))
    assert evaluate_drop(NDVI, D, 0.5, h).severity == "low"      # 16.7 %
    assert evaluate_drop(NDVI, D, 0.47, h).severity == "medium"  # 21.7 %
    c = evaluate_drop(NDVI, D, 0.3, h)                           # 50 %
    assert c.severity == "high"
    assert c.params["drop_pct"] == 15 and c.params["window"] == 4  # compatible keys
    assert c.params["reference_dates"] == [(D - timedelta(days=d)).isoformat() for d in (40, 30, 20, 10)]
    assert evaluate_drop(NDVI, D, 0.52, h) is None                # 13.3 % < 15 %


def test_every_index_declares_its_calibration_status():
    for idx in INDEX_REGISTRY.values():
        assert idx.alerts.calibration


# ── pipeline integration (isolated PostGIS) ──────────────────────────


def _add_history(db, field, label, points):
    from app.models.tables import FieldStat, RasterLayer

    for day, mean in points:
        layer = RasterLayer(org_id=field.org_id, field_id=field.id, layer_type=label, date=day,
                            cog_uri=f"s3://openfarm/cogs/x/{field.id}/{day}/{label.lower()}.tif",
                            params_json={"date_from": "2026-01-01", "date_to": "2026-03-31", "is_backfill": True},
                            provenance_json={"scene_id": f"ORIG_{day:%Y%m%d}", "processed_at": "2026-01-02T00:00:00+00:00"})
        db.add(layer)
        db.flush()
        db.add(FieldStat(org_id=field.org_id, field_id=field.id, layer_id=layer.id, date=day, mean=mean))
    db.commit()


@pytest.fixture()
def rules_on(monkeypatch):
    from app.core.config import settings

    monkeypatch.setattr(settings, "openfarm_alert_rules_enabled", True)


def _alerts(db, field):
    from sqlalchemy import select

    from app.models.tables import Alert

    db.expire_all()
    return db.execute(select(Alert).where(Alert.field_id == field.id).order_by(Alert.rule_name)).scalars().all()


@needs_db
def test_ndvi_drop_uses_only_ndvi_history(db, world, fake_sentinel, rules_on):
    """2 + 9: NDVI history 0.6 -> scene 0.4 = 33 % drop. The other indices'
    history (EVI 0.9, SAVI 0.8, NDWI -0.5) must not leak into the reference."""
    from conftest import fake_scene, run_index_job

    f = world["add_field"]("lot")
    days = [D - timedelta(days=d) for d in (40, 30, 20, 10)]
    _add_history(db, f, "NDVI", [(d, 0.6) for d in days])
    _add_history(db, f, "EVI", [(d, 0.9) for d in days])
    _add_history(db, f, "SAVI", [(d, 0.8) for d in days])
    _add_history(db, f, "NDWI", [(d, -0.5) for d in days])
    fake_sentinel.append(fake_scene(D, {"B04": 0.3, "B08": 0.7}))

    job, _ = run_index_job(db, f, world["user"], "ndvi", D - timedelta(days=5), D)

    assert job.status == "completed"
    [drop] = [a for a in _alerts(db, f) if a.rule_name == "ndvi_drop"]
    assert drop.rule_params_json["reference_mean"] == pytest.approx(0.6)
    assert drop.rule_params_json["reference_dates"] == [d.isoformat() for d in days]
    assert drop.rule_params_json["observed"] == pytest.approx(0.4, abs=1e-4)
    assert drop.severity == "high" and drop.date == D and drop.index_type == "ndvi"
    assert not [a for a in _alerts(db, f) if a.rule_name == "ndvi_threshold"]  # 0.4 >= 0.3


@needs_db
def test_ndwi_job_with_rules_enabled_creates_no_alert(db, world, fake_sentinel, rules_on):
    """1 (end to end): NDWI -0.5 used to raise ndwi_threshold + ndwi_drop."""
    from conftest import fake_scene, run_index_job

    f = world["add_field"]("lot")
    _add_history(db, f, "NDWI", [(D - timedelta(days=d), -0.5) for d in (40, 30, 20, 10)])
    _add_history(db, f, "NDVI", [(D - timedelta(days=d), 0.6) for d in (40, 30, 20, 10)])
    fake_sentinel.append(fake_scene(D, {"B03": 0.1, "B08": 0.3}))
    job, _ = run_index_job(db, f, world["user"], "ndwi", D - timedelta(days=5), D)
    assert job.status == "completed" and _alerts(db, f) == []


@needs_db
def test_reprocessing_same_scene_is_idempotent_and_respects_management(db, world, fake_sentinel, rules_on):
    """6 + 7: the same scene twice -> one alert; an alert closed in between
    stays closed with its original evidence; raster provenance is kept."""
    from sqlalchemy import select

    from app.models.tables import Alert, RasterLayer
    from conftest import fake_scene, run_index_job

    f = world["add_field"]("lot")
    _add_history(db, f, "NDVI", [(D - timedelta(days=d), 0.6) for d in (40, 30, 20, 10)])
    fake_sentinel.append(fake_scene(D, {"B04": 0.45, "B08": 0.55}))  # NDVI 0.1

    run_index_job(db, f, world["user"], "ndvi", D, D)
    first = {a.rule_name: (a.id, dict(a.rule_params_json)) for a in _alerts(db, f)}
    assert set(first) == {"ndvi_threshold", "ndvi_drop"}

    db.execute(Alert.__table__.update().where(Alert.field_id == f.id).values(status="closed"))
    db.commit()
    fake_sentinel[:] = [fake_scene(D, {"B04": 0.5, "B08": 0.55})]  # different value, same scene date
    run_index_job(db, f, world["user"], "ndvi", D, D)

    again = _alerts(db, f)
    assert len(again) == 2
    for a in again:
        assert a.status == "closed"
        assert (a.id, a.rule_params_json) == first[a.rule_name]
    layer = db.execute(select(RasterLayer).where(RasterLayer.field_id == f.id,
                                                 RasterLayer.layer_type == "NDVI",
                                                 RasterLayer.date == D)).scalar_one()
    assert "last_reprocess" in layer.params_json


@needs_db
def test_database_enforces_alert_identity_under_concurrent_writers(db, world):
    """6 (DB level): a second writer with the same identity is a no-op, and a
    plain insert that bypasses ON CONFLICT is rejected by the unique index."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from sqlalchemy.exc import IntegrityError

    from app.core.database_sync import SyncSession
    from app.models.tables import Alert

    f = world["add_field"]("lot")
    row = dict(org_id=f.org_id, field_id=f.id, date=D, severity="high", rule_name="ndvi_threshold",
               message="m", status="open", index_type="ndvi")
    s1, s2 = SyncSession(), SyncSession()
    try:
        stmt = pg_insert(Alert).values(**row).on_conflict_do_nothing(
            index_elements=["field_id", "rule_name", "date"])
        assert s1.execute(stmt).rowcount == 1
        s1.commit()
        assert s2.execute(stmt).rowcount == 0
        s2.commit()
        s2.add(Alert(**row))
        with pytest.raises(IntegrityError):
            s2.commit()
    finally:
        s1.close()
        s2.rollback()
        s2.close()
    assert len(_alerts(db, f)) == 1


@needs_db
def test_disabled_rules_create_no_alerts_even_with_triggering_data(db, world, fake_sentinel, monkeypatch):
    """10: the same triggering scene with the switch off -> no alerts."""
    from app.core.config import settings
    from conftest import fake_scene, run_index_job

    monkeypatch.setattr(settings, "openfarm_alert_rules_enabled", False)
    f = world["add_field"]("lot")
    _add_history(db, f, "NDVI", [(D - timedelta(days=d), 0.6) for d in (40, 30, 20, 10)])
    fake_sentinel.append(fake_scene(D, {"B04": 0.45, "B08": 0.55}))
    job, _ = run_index_job(db, f, world["user"], "ndvi", D, D)
    assert job.status == "completed" and _alerts(db, f) == []


@needs_db
def test_weekly_scheduled_job_flows_through_new_rules(db, world, sent, fake_sentinel, rules_on):
    """11: a job created by the weekly dispatcher runs unchanged through the
    pipeline; with an NDVI history the new drop rule fires on its scene."""
    from sqlalchemy import select

    from app.models.tables import Job
    from app.tasks.backfill import schedule_weekly_index_compute
    from app.tasks.ndvi import process_ndvi
    from conftest import TODAY, fake_scene

    f = world["add_field"]("lot")
    _add_history(db, f, "NDVI", [(TODAY - timedelta(days=d), 0.6) for d in (50, 40, 30, 20)])
    assert schedule_weekly_index_compute()["jobs_dispatched"] == 4
    job = db.execute(select(Job).where(Job.field_id == f.id, Job.type == "ndvi")).scalar_one()
    scene_day = TODAY - timedelta(days=3)
    fake_sentinel.append(fake_scene(scene_day, {"B04": 0.3, "B08": 0.7}))
    process_ndvi(str(job.id))
    [drop] = [a for a in _alerts(db, f) if a.rule_name == "ndvi_drop"]
    assert drop.date == scene_day and drop.rule_params_json["reference_mean"] == pytest.approx(0.6)


# ── 8. soil rules ────────────────────────────────────────────────────


SOIL_SUMMARY_BAD = {"avg_ph": 4.2, "compaction_risk": 0.3, "waterlogging_risk": 0.2, "drainage_class": "well"}
SOIL_LAYERS_BAD = [{"depth_top_cm": 0, "soc_g_kg": 4.0, "sand_pct": 50.0, "cec_cmol_kg": 12.0}]


def test_soil_candidates_carry_value_and_threshold_in_context():
    from app.core.soil_intelligence import evaluate_soil_alerts

    cands = {c.rule_name: c for c in evaluate_soil_alerts(SOIL_SUMMARY_BAD, SOIL_LAYERS_BAD)}
    assert set(cands) == {"soil_ph_very_low", "soil_soc_very_low"}
    assert cands["soil_ph_very_low"].soil_context == {"ph": 4.2, "threshold": 4.5}
    # The attributes the old soil task read never existed on the candidate:
    assert not hasattr(cands["soil_ph_very_low"], "trigger_value")
    assert evaluate_soil_alerts({}, []) == []


@needs_db
def test_soil_alerts_are_created_without_attribute_errors_and_once(db, world, rules_on):
    from app.tasks.soil import _create_soil_alerts

    f = world["add_field"]("lot")
    assert _create_soil_alerts(db, f, SOIL_SUMMARY_BAD, SOIL_LAYERS_BAD) == 2
    alerts = {a.rule_name: a for a in _alerts(db, f)}
    ph = alerts["soil_ph_very_low"]
    assert ph.soil_context == {"ph": 4.2, "threshold": 4.5}
    assert ph.rule_params_json["threshold"] == 4.5 and ph.index_type is None
    # Next soil refresh (any day): the static condition is not raised again.
    assert _create_soil_alerts(db, f, SOIL_SUMMARY_BAD, SOIL_LAYERS_BAD) == 0
    assert _create_soil_alerts(db, f, {}, []) == 0  # missing data: nothing, no error
    assert len(_alerts(db, f)) == 2


@needs_db
def test_soil_rules_disabled_create_nothing(db, world, monkeypatch):
    from app.core.config import settings
    from app.tasks.soil import _create_soil_alerts

    monkeypatch.setattr(settings, "openfarm_alert_rules_enabled", False)
    f = world["add_field"]("lot")
    assert _create_soil_alerts(db, f, SOIL_SUMMARY_BAD, SOIL_LAYERS_BAD) == 0
    assert _alerts(db, f) == []


# ── 12. contracts used by AgroMind ───────────────────────────────────


def test_routes_used_by_agromind_are_unchanged():
    from app.main import app

    # FastAPI >= 0.14x wraps included routers (_IncludedRouter), so read the
    # served contract from the OpenAPI schema instead of app.routes.
    routes = {(m.upper(), path) for path, ops in app.openapi()["paths"].items() for m in ops}
    for method, path in [
        ("POST", "/v1/orgs"), ("POST", "/v1/farms"), ("POST", "/v1/fields"),
        ("GET", "/v1/fields/{field_id}"), ("DELETE", "/v1/fields/{field_id}"),
        ("POST", "/v1/fields/{field_id}/restore"), ("GET", "/v1/fields/{field_id}/stats"),
        ("GET", "/v1/fields/{field_id}/weather"), ("GET", "/v1/fields/{field_id}/soil"),
        ("GET", "/v1/fields/{field_id}/soil/summary"),
        ("GET", "/v1/alerts"), ("GET", "/v1/alerts/summary"), ("PATCH", "/v1/alerts/{alert_id}"),
    ]:
        assert (method, path) in routes, (method, path)


def test_alert_response_schema_accepts_new_params():
    import uuid
    from datetime import datetime, timezone

    from app.schemas.monitoring import AlertOut

    c = evaluate_drop(NDVI, D, 0.3, _hist((40, 0.6), (30, 0.6), (20, 0.6), (10, 0.6)))
    out = AlertOut(id=uuid.uuid4(), field_id=uuid.uuid4(), date=c.observed_date, severity=c.severity,
                   rule_name=c.rule_name, rule_params_json=c.params, index_type=c.index_type,
                   message=c.message, status="open", created_at=datetime.now(timezone.utc))
    assert out.rule_params_json["drop_pct"] == 15 and math.isclose(out.rule_params_json["observed"], 0.3)
