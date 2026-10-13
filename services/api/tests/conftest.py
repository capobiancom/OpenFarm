"""Shared fixtures: disposable PostGIS (DATABASE_URL_SYNC, migrated to head),
test world (user/org/farm/fields with scene history) and network fakes.

Tests that need the database are skipped when DATABASE_URL_SYNC is unset.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from urllib.parse import parse_qs, urlparse

import numpy as np
import pytest
from sqlalchemy import delete

TODAY = datetime.now(timezone.utc).date()
GEOM = (
    "SRID=4326;POLYGON((-62.760 -17.640,-62.750 -17.640,"
    "-62.750 -17.650,-62.760 -17.650,-62.760 -17.640))"
)


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



def fake_scene(day, bands=None):
    """A Sentinel-2-like scene; ``bands`` = {band: reflectance} (defaults:
    B03 0.1, everything else 0.3 -> NDWI -0.5, NDVI/SAVI/EVI ~0)."""
    bands = bands or {"B03": 0.1, "B08": 0.3}
    return {
        "id": f"S2X_TEST_{day:%Y%m%d}",
        "date": day,
        "cloud_cover": 1.0,
        "band_hrefs": {b: f"https://example/{day:%Y%m%d}/{b}.tif?v={v}" for b, v in bands.items()},
    }


@pytest.fixture()
def fake_sentinel(monkeypatch):
    """No network: fixed scenes, bands from the scene href, COG write skipped."""
    import app.tasks.ndvi as ndvi
    import app.tasks.pipeline as pipeline
    import app.tasks.vegetation as vegetation

    scenes: list[dict] = []
    monkeypatch.setattr(vegetation, "search_scenes", lambda *a, **k: list(scenes))
    monkeypatch.setattr(ndvi, "search_scenes", lambda *a, **k: list(scenes))

    def fake_band(href, bounds, target_shape, target_transform):
        value = float(parse_qs(urlparse(href).query)["v"][0])
        return np.full(target_shape, value, dtype="float32")

    monkeypatch.setattr(pipeline, "read_band_windowed", fake_band)
    monkeypatch.setattr(
        pipeline, "write_cog",
        lambda data, tr, crs, org, field, d, key: f"s3://openfarm/cogs/{org}/{field}/{d}/{key}.tif",
    )
    return scenes


def run_index_job(db, field, user, index, date_from, date_to, extra=None):
    """Create a normal job for ``index`` and run its Celery task inline."""
    from app.models.tables import Job
    from app.tasks.ndvi import process_ndvi
    from app.tasks.vegetation import process_evi, process_ndwi, process_savi

    task = {"ndvi": process_ndvi, "evi": process_evi, "savi": process_savi, "ndwi": process_ndwi}[index]
    job = Job(org_id=field.org_id, field_id=field.id, type=index, status="pending",
              params_json={"date_from": str(date_from), "date_to": str(date_to), **(extra or {})},
              created_by=user.id)
    db.add(job)
    db.commit()
    result = task(str(job.id))
    db.expire_all()
    return db.get(Job, job.id), result
