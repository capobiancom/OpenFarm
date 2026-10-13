"""Database-level alert identity and start-up schema safety (migration 0015).

* The database itself must reject duplicate alerts - not only the code:
  one per (field, rule, date) for every rule, one per (field, rule) for
  soil rules, ``date`` NOT NULL (so NULLS DISTINCT can never let a duplicate
  through), also with concurrent writers.
* A failed migration at start-up must not be reported as healthy.

Needs the disposable PostGIS of conftest.py (DATABASE_URL and
DATABASE_URL_SYNC, migrated to head). Some tests run ``alembic`` against
that database and always leave it back at head.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
from datetime import date, timedelta
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABASE_URL_SYNC"), reason="needs a test PostGIS database"
)

D = date(2026, 9, 28)
API_DIR = Path(__file__).resolve().parents[1]


def _row(field, rule="ndvi_threshold", day=D):
    return dict(org_id=field.org_id, field_id=field.id, date=day, severity="high",
                rule_name=rule, message="m", status="open")


def _insert(session, values, on_conflict=False):
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    from app.models.tables import Alert

    stmt = pg_insert(Alert).values(**values)
    if on_conflict:
        stmt = stmt.on_conflict_do_nothing()
    return session.execute(stmt).rowcount


def _count(db, field):
    from sqlalchemy import func, select

    from app.models.tables import Alert

    db.expire_all()
    return db.execute(select(func.count()).select_from(Alert).where(Alert.field_id == field.id)).scalar()


# ── identity enforced by the database ────────────────────────────────


def test_identity_indexes_exist_as_designed(db):
    from sqlalchemy import text

    defs = dict(db.execute(text(
        "SELECT indexname, indexdef FROM pg_indexes WHERE tablename='alerts' AND indexname LIKE 'uq_alerts_%'"
    )).all())
    assert "UNIQUE INDEX uq_alerts_field_rule_date ON public.alerts USING btree (field_id, rule_name, date)" in defs["uq_alerts_field_rule_date"]
    soil = defs["uq_alerts_soil_field_rule"]
    assert "UNIQUE INDEX" in soil and "(field_id, rule_name)" in soil and "WHERE (\"left\"(rule_name, 5) = 'soil_'::text)" in soil
    nullable = db.execute(text(
        "SELECT is_nullable FROM information_schema.columns WHERE table_name='alerts' AND column_name='date'"
    )).scalar()
    assert nullable == "NO"


def test_null_date_is_rejected_so_nulls_distinct_cannot_apply(db, world):
    from sqlalchemy.exc import IntegrityError

    f = world["add_field"]("lot")
    for rule in ("ndvi_threshold", "soil_ph_low"):
        with pytest.raises(IntegrityError, match="not-null|null value"):
            _insert(db, _row(f, rule, day=None))
            db.commit()
        db.rollback()
    assert _count(db, f) == 0


def test_index_rule_unique_per_day_but_allowed_on_other_days(db, world):
    from sqlalchemy.exc import IntegrityError

    f = world["add_field"]("lot")
    assert _insert(db, _row(f, "ndvi_drop", D)) == 1
    db.commit()
    with pytest.raises(IntegrityError, match="uq_alerts_field_rule_date"):
        _insert(db, _row(f, "ndvi_drop", D))
        db.commit()
    db.rollback()
    assert _insert(db, _row(f, "ndvi_drop", D), on_conflict=True) == 0
    assert _insert(db, _row(f, "ndvi_drop", D + timedelta(days=7))) == 1  # a new observation date
    db.commit()
    assert _count(db, f) == 2


def test_soil_rule_unique_per_field_whatever_the_date(db, world):
    from sqlalchemy.exc import IntegrityError

    f, other = world["add_field"]("lot"), world["add_field"]("other")
    assert _insert(db, _row(f, "soil_ph_low", D)) == 1
    db.commit()
    with pytest.raises(IntegrityError, match="uq_alerts_soil_field_rule"):
        _insert(db, _row(f, "soil_ph_low", D + timedelta(days=30)))
        db.commit()
    db.rollback()
    assert _insert(db, _row(f, "soil_ph_low", D + timedelta(days=30)), on_conflict=True) == 0
    assert _insert(db, _row(f, "soil_soc_low", D + timedelta(days=30))) == 1  # another soil rule
    assert _insert(db, _row(other, "soil_ph_low", D + timedelta(days=30))) == 1  # another field
    db.commit()
    assert _count(db, f) == 2 and _count(db, other) == 1


def test_concurrent_soil_writers_cannot_duplicate(db, world):
    from app.core.database_sync import SyncSession

    f = world["add_field"]("lot")
    s1, s2 = SyncSession(), SyncSession()
    try:
        assert _insert(s1, _row(f, "soil_compaction", D), on_conflict=True) == 1
        s1.commit()
        # A refresh on another day, in another session, after the first one.
        assert _insert(s2, _row(f, "soil_compaction", D + timedelta(days=1)), on_conflict=True) == 0
        s2.commit()
    finally:
        s1.close()
        s2.close()
    assert _count(db, f) == 1


def test_soil_task_relies_on_db_identity_across_refresh_days(db, world, monkeypatch):
    import app.tasks.soil as soil
    from app.core.config import settings

    monkeypatch.setattr(settings, "openfarm_alert_rules_enabled", True)
    f = world["add_field"]("lot")
    bad = ({"avg_ph": 4.2}, [{"depth_top_cm": 0, "soc_g_kg": 4.0}])
    assert soil._create_soil_alerts(db, f, *bad) == 2

    class Tomorrow:  # the next soil refresh happens on another day
        @staticmethod
        def now(tz=None):
            from datetime import datetime, timezone
            return datetime.now(timezone.utc) + timedelta(days=1)

    monkeypatch.setattr(soil, "datetime", Tomorrow)
    assert soil._create_soil_alerts(db, f, *bad) == 0
    assert _count(db, f) == 2


# ── schema state, health and start-up ───────────────────────────────


class _FakeRedis:
    async def ping(self):
        return True

    async def aclose(self):
        return None


def _health(monkeypatch):
    from app import main
    from app.core.database import engine

    monkeypatch.setattr(main.aioredis, "from_url", lambda url: _FakeRedis())

    async def run():
        try:
            return await main.healthz()
        finally:
            await engine.dispose()

    r = asyncio.run(run())
    if isinstance(r, dict):
        return 200, r
    return r.status_code, json.loads(r.body)


def _alembic(*args):
    return subprocess.run(["alembic", *args], cwd=API_DIR, capture_output=True, text=True)


def test_classify_schema_revisions():
    from app.core.schema_status import classify, code_heads

    assert code_heads() == ("0015",)
    assert classify("0015")[0] == "ok"
    assert classify("0014") == ("behind", "database at 0014; code requires 0015")
    assert classify(None)[0] == "behind"
    assert classify("9999_future")[0] == "ahead"


def test_health_ok_at_head(monkeypatch):
    status, body = _health(monkeypatch)
    assert status == 200 and body == {"status": "ok", "schema": "schema at 0015"}


def test_failed_migration_is_reported_unhealthy_and_recovers(db, world, monkeypatch):
    """Migration path end to end on the disposable database:
    0014 + duplicates -> upgrade fails explicitly, nothing deleted, health 503;
    duplicates resolved -> upgrade succeeds, health 200."""
    from sqlalchemy import text

    f = world["add_field"]("lot")
    try:
        assert _alembic("downgrade", "0014").returncode == 0
        status, body = _health(monkeypatch)
        assert status == 503 and body["errors"] == ["schema: database at 0014; code requires 0015"]

        for _ in range(2):  # without the index nothing stops duplicates
            _insert(db, _row(f, "ndvi_threshold", D))
        db.commit()
        failed = _alembic("upgrade", "head")
        assert failed.returncode != 0
        assert "duplicated (field_id, rule_name, date) groups; review them before 0015" in failed.stderr
        assert _count(db, f) == 2  # nothing deleted
        assert db.execute(text("SELECT version_num FROM alembic_version")).scalar() == "0014"
        assert _health(monkeypatch)[0] == 503

        db.execute(text("DELETE FROM alerts WHERE field_id = :f"), {"f": f.id})  # disposable test DB only
        db.commit()
        assert _alembic("upgrade", "head").returncode == 0
        assert _health(monkeypatch) == (200, {"status": "ok", "schema": "schema at 0015"})
    finally:
        db.rollback()
        db.execute(text("DELETE FROM alerts WHERE field_id = :f"), {"f": f.id})
        db.commit()
        _alembic("upgrade", "head")


def test_dockerfile_start_command_still_starts_api_after_failed_migrations():
    """Documents the start-up limitation (unchanged on purpose): with alembic
    failing every attempt, the loop exits successfully and the server starts.
    That is why /healthz must report the schema (test above)."""
    dockerfile = (API_DIR / "Dockerfile").read_text()
    cmd = json.loads(re.search(r"^CMD (\[.*\])$", dockerfile, re.M).group(1))
    assert cmd[:2] == ["sh", "-c"]
    script = (cmd[2].replace("alembic upgrade head", "false")
              .replace("sleep 3", "true")
              .replace("uvicorn app.main:app --host 0.0.0.0 --port 8000", "echo SERVER_STARTED"))
    out = subprocess.run(["sh", "-c", script], capture_output=True, text=True)
    assert out.returncode == 0
    assert out.stdout.count("Waiting for DB") == 10 and "SERVER_STARTED" in out.stdout
