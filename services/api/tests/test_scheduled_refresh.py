"""Periodic refresh (Celery Beat) - eligibility, windows, idempotency, alerts off.

These tests need a disposable PostGIS database migrated to head; they are
skipped otherwise. DATABASE_URL_SYNC must point to it before import, e.g.:

    DATABASE_URL_SYNC=postgresql+psycopg2://u:p@host/db alembic upgrade head
    DATABASE_URL_SYNC=... python -m pytest tests/test_scheduled_refresh.py

Network calls (STAC, Sentinel-2 COGs, MinIO, Open-Meteo) are replaced with
fakes; everything else (pipeline, upserts, dispatchers, SQL) is real.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pytest
import yaml

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL_SYNC"), reason="needs a test PostGIS database"
)

from sqlalchemy import delete, func, select, text  # noqa: E402

TODAY = datetime.now(timezone.utc).date()
GEOM = (
    "SRID=4326;POLYGON((-62.760 -17.640,-62.750 -17.640,"
    "-62.750 -17.650,-62.760 -17.650,-62.760 -17.640))"
)
REPO = Path(__file__).resolve().parents[3]


# ── fixtures ─────────────────────────────────────────────────────────


@pytest.fixture()
def db():
    from app.core.database_sync import SyncSession
    from app.models import tables as t

    s = SyncSession()
    for model in (t.Alert, t.FieldStat, t.RasterLayer, t.WeatherDaily, t.Job, t.Field, t.Farm):
        s.execute(delete(model))
    s.execute(delete(t.OrgMember)) if hasattr(t, "OrgMember") else None
    s.execute(delete(t.Org))
    s.execute(delete(t.User))
    s.commit()
    yield s
    s.rollback()
    s.close()


@pytest.fixture()
def world(db):
    """One user/org/farm and a helper to add fields with scene history."""
    from app.models.tables import Farm, Field, Org, RasterLayer, FieldStat, User

    user = User(email=f"svc-{uuid.uuid4().hex[:6]}@test.local", name="svc")
    db.add(user)
    db.flush()
    org = Org(name="org", created_by=user.id)
    db.add(org)
    db.flush()
    farm = Farm(org_id=org.id, name="farm")
    db.add(farm)
    db.flush()

    def add_field(name, latest=None, deleted=False, in_org=None, backfill_meta=False):
        o = in_org or org
        f = Field(
            org_id=o.id,
            farm_id=farm.id if o is org else _farm_for(o),
            name=name,
            created_by=user.id,
            geom=GEOM,
            deleted_at=datetime.now(timezone.utc) if deleted else None,
        )
        db.add(f)
        db.flush()
        if latest:
            for label in ("NDVI", "EVI", "SAVI", "NDWI"):
                layer = RasterLayer(
                    org_id=o.id,
                    field_id=f.id,
                    layer_type=label,
                    date=latest,
                    cog_uri=f"s3://openfarm/cogs/{o.id}/{f.id}/{latest}/{label.lower()}.tif",
                    params_json=(
                        {"date_from": "2026-01-01", "date_to": "2026-03-31", "is_backfill": True}
                        if backfill_meta
                        else {"date_from": str(latest), "date_to": str(latest)}
                    ),
                    provenance_json={"scene_id": "ORIG", "processed_at": "2026-01-02T00:00:00+00:00"},
                )
                db.add(layer)
                db.flush()
                db.add(FieldStat(org_id=o.id, field_id=f.id, layer_id=layer.id, date=latest, mean=0.5))
        db.commit()
        return f

    def _farm_for(o):
        fm = Farm(org_id=o.id, name="other")
        db.add(fm)
        db.flush()
        return fm.id

    def new_org(deleted=False):
        o = Org(name="other-org", created_by=user.id,
                deleted_at=datetime.now(timezone.utc) if deleted else None)
        db.add(o)
        db.commit()
        return o

    return {"user": user, "org": org, "farm": farm, "add_field": add_field, "new_org": new_org}


@pytest.fixture()
def sent(monkeypatch):
    """Capture Celery dispatches; assert each job is already committed."""
    from app.core.database_sync import SyncSession
    from app.models.tables import Job
    from app.worker import celery_app

    calls = []

    def fake_send_task(name, args=None, countdown=None, **kw):
        probe = SyncSession()
        try:
            assert probe.get(Job, uuid.UUID(args[0])) is not None, "job sent before commit"
        finally:
            probe.close()
        calls.append((name, args[0], countdown))

    monkeypatch.setattr(celery_app, "send_task", fake_send_task)
    return calls


# ── Beat schedule, compose and configuration ─────────────────────────


def test_beat_schedule_is_utc_weekly_monday_and_daily():
    from app.worker import celery_app

    conf = celery_app.conf
    assert conf.timezone == "UTC" and conf.enable_utc is True
    weekly = conf.beat_schedule["compute-indices-weekly"]
    daily = conf.beat_schedule["fetch-weather-daily"]
    assert weekly["task"] == "app.tasks.backfill.schedule_weekly_index_compute"
    assert weekly["schedule"].day_of_week == {1}
    assert weekly["schedule"].hour == {6} and weekly["schedule"].minute == {0}
    assert daily["task"] == "app.tasks.weather.schedule_daily_weather_fetch"
    assert daily["schedule"].hour == {8} and daily["schedule"].minute == {0}
    assert daily["schedule"].day_of_week == set(range(7))
    assert weekly["options"]["expires"] == daily["options"]["expires"] == 6 * 3600
    celery_app.loader.import_default_modules()
    for entry in (weekly, daily):
        assert entry["task"] in celery_app.tasks


def test_alert_rules_disabled_by_default_and_by_env(monkeypatch):
    from app.core.config import Settings

    monkeypatch.delenv("OPENFARM_ALERT_RULES_ENABLED", raising=False)
    assert Settings(_env_file=None).openfarm_alert_rules_enabled is False
    monkeypatch.setenv("OPENFARM_ALERT_RULES_ENABLED", "false")
    assert Settings(_env_file=None).openfarm_alert_rules_enabled is False
    monkeypatch.setenv("OPENFARM_ALERT_RULES_ENABLED", "true")
    assert Settings(_env_file=None).openfarm_alert_rules_enabled is True


def test_core_compose_runs_single_beat_and_no_embedded_beat():
    compose = yaml.safe_load((REPO / "deploy/coolify/core/docker-compose.yml").read_text())
    svc = compose["services"]
    sched = svc["openfarm-scheduler"]
    assert "celery -A app.worker beat" in sched["command"]
    assert " worker" not in sched["command"]
    assert sched["deploy"]["replicas"] == "${OPENFARM_SCHEDULER_REPLICAS:-0}"
    assert "ports" not in sched and list(sched["networks"]) == ["openfarm"]
    assert sched["restart"] == "unless-stopped" and "healthcheck" in sched
    assert " -B" not in svc["openfarm-processor"]["command"]
    assert "beat" not in svc["openfarm-processor"]["command"]
    for name, s in svc.items():
        assert "ports" not in s, name


def test_api_processor_and_beat_share_one_codebase():
    import app.main  # noqa: F401  - the API imports cleanly with the new settings
    from app.tasks.backfill import schedule_weekly_index_compute
    from app.tasks.weather import schedule_daily_weather_fetch
    from app.worker import celery_app

    names = {e["task"] for e in celery_app.conf.beat_schedule.values()}
    assert names == {schedule_weekly_index_compute.name, schedule_daily_weather_fetch.name}


def test_scheduler_healthcheck(tmp_path):
    from app import scheduler_health

    path = str(tmp_path / "celerybeat-schedule")
    assert scheduler_health.schedule_age(path) is None
    Path(path + ".db").write_text("x")
    assert scheduler_health.schedule_age(path) < 5
    old = datetime.now().timestamp() - 2000
    os.utime(path + ".db", (old, old))
    assert scheduler_health.schedule_age(path) > 900


# ── Windows ──────────────────────────────────────────────────────────


def test_weekly_window_fresh_stale_capped_and_new():
    from app.tasks.scheduling import weekly_index_window

    assert weekly_index_window(TODAY - timedelta(days=3), TODAY) is None
    assert weekly_index_window(TODAY - timedelta(days=15), TODAY) == (
        TODAY - timedelta(days=14), TODAY)
    assert weekly_index_window(TODAY - timedelta(days=400), TODAY) == (
        TODAY - timedelta(days=60), TODAY)
    assert weekly_index_window(None, TODAY) == (TODAY - timedelta(days=7), TODAY)


def test_daily_weather_window_fills_gap_and_is_capped():
    from app.tasks.scheduling import daily_weather_window

    assert daily_weather_window(None, TODAY) == (TODAY - timedelta(days=1), TODAY)
    assert daily_weather_window(TODAY, TODAY) == (TODAY - timedelta(days=1), TODAY)
    assert daily_weather_window(TODAY - timedelta(days=3), TODAY) == (TODAY - timedelta(days=3), TODAY)
    assert daily_weather_window(TODAY - timedelta(days=200), TODAY) == (TODAY - timedelta(days=30), TODAY)


# ── Eligibility and weekly dispatch ──────────────────────────────────


def test_only_live_fields_in_live_orgs_are_eligible(db, world):
    from app.tasks.scheduling import eligible_fields

    live = world["add_field"]("live")
    world["add_field"]("archived", deleted=True)
    world["add_field"]("in-deleted-org", in_org=world["new_org"](deleted=True))
    assert [f[0] for f in eligible_fields(db)] == [live.id]


def test_weekly_dispatch_only_stale_eligible_fields_without_backfill(db, world, sent):
    from app.models.tables import Job
    from app.tasks.backfill import schedule_weekly_index_compute

    stale = world["add_field"]("stale", latest=TODAY - timedelta(days=15))
    world["add_field"]("fresh", latest=TODAY - timedelta(days=2))
    world["add_field"]("archived", latest=TODAY - timedelta(days=30), deleted=True)
    world["add_field"]("dead-org", latest=TODAY - timedelta(days=30),
                       in_org=world["new_org"](deleted=True))

    result = schedule_weekly_index_compute()

    assert result["fields_dispatched"] == 1 and result["jobs_dispatched"] == 4
    jobs = db.execute(select(Job)).scalars().all()
    assert {j.field_id for j in jobs} == {stale.id}
    assert sorted(j.type for j in jobs) == ["evi", "ndvi", "ndwi", "savi"]
    for j in jobs:
        assert "is_backfill" not in j.params_json
        assert j.params_json["trigger"] == "scheduled_weekly"
        assert j.params_json["date_from"] == str(TODAY - timedelta(days=14))
        assert j.params_json["date_to"] == str(TODAY)
    assert len(sent) == 4 and {c[1] for c in sent} == {str(j.id) for j in jobs}


def test_weekly_dispatch_is_idempotent(db, world, sent):
    from app.models.tables import Job
    from app.tasks.backfill import schedule_weekly_index_compute

    world["add_field"]("stale", latest=TODAY - timedelta(days=15))
    assert schedule_weekly_index_compute()["jobs_dispatched"] == 4
    # Beat restart / redelivery / manual re-run: no new jobs.
    assert schedule_weekly_index_compute()["jobs_dispatched"] == 0
    # Even after the first jobs finished, the scheduled run is not repeated.
    db.execute(Job.__table__.update().values(status="completed"))
    db.commit()
    assert schedule_weekly_index_compute()["jobs_dispatched"] == 0
    assert db.execute(select(func.count()).select_from(Job)).scalar() == 4
    assert len(sent) == 4


def test_weekly_dispatch_skips_fields_with_work_in_flight(db, world, sent):
    from app.models.tables import Job
    from app.tasks.backfill import schedule_weekly_index_compute

    f = world["add_field"]("busy", latest=TODAY - timedelta(days=15))
    db.add(Job(org_id=f.org_id, field_id=f.id, type="ndvi", status="running",
               params_json={"is_backfill": True}, created_by=world["user"].id))
    db.commit()
    assert schedule_weekly_index_compute()["jobs_dispatched"] == 0


def test_weekly_dispatch_respects_advisory_lock(db, world, sent):
    from app.core.database_sync import SyncSession
    from app.tasks.backfill import schedule_weekly_index_compute
    from app.tasks.scheduling import WEEKLY_INDEX_LOCK_KEY

    world["add_field"]("stale", latest=TODAY - timedelta(days=15))
    holder = SyncSession()
    try:
        holder.execute(text("SELECT pg_advisory_lock(:k)"), {"k": WEEKLY_INDEX_LOCK_KEY})
        assert schedule_weekly_index_compute()["status"] == "skipped"
    finally:
        holder.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": WEEKLY_INDEX_LOCK_KEY})
        holder.close()
    assert sent == []


# ── Pipeline: alerts off, no scenes, reprocessing ────────────────────


def _fake_scene(day):
    return {
        "id": f"S2X_TEST_{day:%Y%m%d}",
        "date": day,
        "cloud_cover": 1.0,
        "band_hrefs": {"B03": "https://example/B03.tif", "B08": "https://example/B08.tif"},
    }


@pytest.fixture()
def fake_sentinel(monkeypatch):
    """No network: fixed scenes, bands with NDWI ~ -0.5, COG write skipped."""
    import app.tasks.pipeline as pipeline
    import app.tasks.vegetation as vegetation

    scenes: list[dict] = []
    monkeypatch.setattr(vegetation, "search_scenes", lambda *a, **k: list(scenes))

    def fake_band(href, bounds, target_shape, target_transform):
        value = 0.1 if href.endswith("B03.tif") else 0.3
        return np.full(target_shape, value, dtype="float32")

    monkeypatch.setattr(pipeline, "read_band_windowed", fake_band)
    monkeypatch.setattr(
        pipeline, "write_cog",
        lambda data, tr, crs, org, field, d, key: f"s3://openfarm/cogs/{org}/{field}/{d}/{key}.tif",
    )
    return scenes


def _run_ndwi_job(db, field, user, date_from, date_to, extra=None):
    from app.models.tables import Job
    from app.tasks.vegetation import process_ndwi

    job = Job(org_id=field.org_id, field_id=field.id, type="ndwi", status="pending",
              params_json={"date_from": str(date_from), "date_to": str(date_to), **(extra or {})},
              created_by=user.id)
    db.add(job)
    db.commit()
    result = process_ndwi(str(job.id))
    db.expire_all()
    return db.get(Job, job.id), result


def test_normal_processing_creates_no_alerts_when_rules_disabled(db, world, fake_sentinel, monkeypatch):
    from app.core.config import settings
    from app.models.tables import Alert, FieldStat, RasterLayer

    monkeypatch.setattr(settings, "openfarm_alert_rules_enabled", False)
    f = world["add_field"]("lot", latest=TODAY - timedelta(days=20))
    fake_sentinel.append(_fake_scene(TODAY - timedelta(days=5)))

    job, _ = _run_ndwi_job(db, f, world["user"], TODAY - timedelta(days=19), TODAY,
                           {"trigger": "scheduled_weekly"})

    assert job.status == "completed"
    stat = db.execute(
        select(FieldStat.mean).join(RasterLayer, RasterLayer.id == FieldStat.layer_id)
        .where(RasterLayer.field_id == f.id, RasterLayer.date == TODAY - timedelta(days=5))
    ).scalar_one()
    assert float(stat) == pytest.approx(-0.5, abs=1e-3)  # NDWI stored ...
    assert db.execute(select(func.count()).select_from(Alert)).scalar() == 0  # ... no alerts


def test_same_run_creates_alerts_only_if_rules_enabled(db, world, fake_sentinel, monkeypatch):
    """Control: proves the flag (not the data) is what suppresses alerts."""
    from app.core.config import settings
    from app.models.tables import Alert

    monkeypatch.setattr(settings, "openfarm_alert_rules_enabled", True)
    f = world["add_field"]("lot", latest=TODAY - timedelta(days=20))
    fake_sentinel.append(_fake_scene(TODAY - timedelta(days=5)))
    _run_ndwi_job(db, f, world["user"], TODAY - timedelta(days=19), TODAY)
    assert db.execute(select(func.count()).select_from(Alert)).scalar() > 0


def test_no_new_scenes_completes_without_touching_layers(db, world, fake_sentinel):
    from app.models.tables import RasterLayer

    f = world["add_field"]("lot", latest=TODAY - timedelta(days=20))
    before = db.execute(select(func.count()).select_from(RasterLayer)).scalar()

    job, result = _run_ndwi_job(db, f, world["user"], TODAY - timedelta(days=19), TODAY)

    assert job.status == "completed" and result["scenes"] == 0
    assert job.progress_json["message"] == "No cloud-free scenes found for the date range."
    assert db.execute(select(func.count()).select_from(RasterLayer)).scalar() == before


def test_reprocessing_keeps_original_provenance_and_does_not_duplicate(db, world, fake_sentinel):
    from app.models.tables import FieldStat, RasterLayer

    day = TODAY - timedelta(days=20)
    f = world["add_field"]("lot", latest=day, backfill_meta=True)
    fake_sentinel.append(_fake_scene(day))

    _run_ndwi_job(db, f, world["user"], day, day)

    layers = db.execute(
        select(RasterLayer).where(RasterLayer.field_id == f.id, RasterLayer.layer_type == "NDWI")
    ).scalars().all()
    assert len(layers) == 1
    layer = layers[0]
    assert layer.params_json["is_backfill"] is True
    assert layer.params_json["date_from"] == "2026-01-01"
    assert layer.params_json["last_reprocess"]["is_backfill"] is False
    assert layer.provenance_json["processed_at"] == "2026-01-02T00:00:00+00:00"
    assert layer.provenance_json["scene_id"] == "ORIG"
    assert layer.provenance_json["last_reprocess_scene_id"] == f"S2X_TEST_{day:%Y%m%d}"
    stats = db.execute(select(FieldStat).where(FieldStat.layer_id == layer.id)).scalars().all()
    assert len(stats) == 1 and float(stats[0].mean) == pytest.approx(-0.5, abs=1e-3)


# ── Weather ──────────────────────────────────────────────────────────


def _fake_open_meteo(calls):
    def fake(lat, lon, start, end, *, use_archive=False):
        calls.append((start, end, use_archive))
        days = [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]
        return {"daily": {"time": days,
                          "temperature_2m_max": [30.0] * len(days),
                          "temperature_2m_min": [10.0] * len(days)}}
    return fake


def test_daily_weather_fills_gap_without_double_counting_gdd(db, world, monkeypatch):
    import app.tasks.weather as weather
    from app.models.tables import WeatherDaily

    f = world["add_field"]("lot")
    for i in (6, 5, 4):  # stored up to TODAY-4, GDD 10/day
        d = TODAY - timedelta(days=i)
        db.add(WeatherDaily(org_id=f.org_id, field_id=f.id, date=d, latitude=-17.6, longitude=-62.7,
                            gdd_daily=10, gdd_cumulative=10 * (7 - i)))
    db.commit()
    calls = []
    monkeypatch.setattr(weather, "_fetch_open_meteo", _fake_open_meteo(calls))

    result = weather.fetch_weather_for_field(str(f.id))

    assert result["status"] == "success"
    assert calls == [(TODAY - timedelta(days=4), TODAY, False)]
    rows = db.execute(
        select(WeatherDaily.date, WeatherDaily.gdd_cumulative)
        .where(WeatherDaily.field_id == f.id).order_by(WeatherDaily.date)
    ).all()
    assert [r[0] for r in rows] == [TODAY - timedelta(days=i) for i in range(6, -1, -1)]
    # (30+10)/2-10 = 10 GDD per day, continuous: 10,20,30,...,70
    assert [float(r[1]) for r in rows] == [10.0 * n for n in range(1, 8)]


def test_daily_weather_dispatch_covers_only_eligible_fields(db, world, monkeypatch):
    import app.tasks.weather as weather

    live = world["add_field"]("live")
    world["add_field"]("archived", deleted=True)
    dispatched = []

    class FakeGroup:
        def __init__(self, sigs):
            self.sigs = list(sigs)

        def apply_async(self):
            dispatched.extend(s.args[0] for s in self.sigs)

    monkeypatch.setattr(weather, "group", FakeGroup)
    assert weather.schedule_daily_weather_fetch()["fields"] == 1
    assert dispatched == [str(live.id)]


def test_provider_errors_are_logged_without_secrets(db, world, monkeypatch):
    import httpx

    import app.tasks.weather as weather

    f = world["add_field"]("lot")

    def boom(*a, **k):
        raise httpx.ConnectError("failed https://api.open-meteo.com/v1/forecast?apikey=TOPSECRET&x=1")

    monkeypatch.setattr(weather, "_fetch_open_meteo", boom)
    result = weather.fetch_weather_for_field(str(f.id))
    assert result["status"] == "failed"
    assert "TOPSECRET" not in result["error"] and "apikey=***" in result["error"]
    assert weather._safe_url("https://h/v1/forecast?apikey=TOPSECRET") == "https://h/v1/forecast"
