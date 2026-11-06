"""Reprocess stored index layers with the current pipeline version.

Used once after a pipeline change that alters stored values (3.0.0:
reflectance conversion, nodata and SCL masking, new quality_score) and to
add a new index (NDMI) for every date that already has layers.

Unlike the backfill (which skips dates that already have data) and the
weekly refresh (which never reprocesses stored dates), this re-reads the
exact Sentinel-2 scene stored in each layer's provenance, so the dates and
scenes of the series do not change. Writes go through the normal pipeline:
the raster_layers row is upserted on (field_id, date, layer_type), its
field_stats row is updated in place, and the COG is overwritten at the
same object key (cogs/<org>/<field>/<date>/<index>.tif).

Plan only (no writes):

    python -m app.tasks.reprocess                 # all live fields, JSON
    python -m app.tasks.reprocess <field_id> ...

Dispatch (from a processor/api container):

    celery -A app.worker call app.tasks.reprocess.reprocess_stored_layers
    celery -A app.worker call app.tasks.reprocess.reprocess_stored_layers \
        --kwargs '{"field_ids": ["<uuid>"]}'
"""

from __future__ import annotations

import json
import sys
import uuid
from collections import defaultdict
from datetime import date

import structlog
from sqlalchemy import func, select

from app.tasks.indices import INDEX_REGISTRY, INDEX_TASK_MAP
from app.tasks.pipeline import PIPELINE_VERSION, get_db_session
from app.worker import celery_app

logger = structlog.get_logger()

REPROCESS_TRIGGER = f"reprocess_{PIPELINE_VERSION}"
MAX_SCENES_PER_JOB = 40  # ~5 s per scene and index: far below the 1500 s soft limit


def _layer_version(params: dict | None, provenance: dict | None) -> str | None:
    """Pipeline version that produced the data currently stored in a layer."""
    params = params or {}
    provenance = provenance or {}
    return (
        params.get("pipeline_version")
        or provenance.get("last_reprocess_pipeline_version")
        or provenance.get("pipeline_version")
    )


def _layer_scene(provenance: dict | None) -> str | None:
    provenance = provenance or {}
    return provenance.get("last_reprocess_scene_id") or provenance.get("scene_id")


def plan_reprocess(
    session,
    field_ids: list[str] | None = None,
    index_keys: list[str] | None = None,
    target_version: str = PIPELINE_VERSION,
) -> list[dict]:
    """One entry per (field, index, chunk) that needs (re)processing.

    A (field, date) needs work for an index when that index has no layer on
    the date (e.g. NDMI) or its layer was produced by another pipeline
    version. The scene for a date is the one stored in its layers.
    """
    from app.models.tables import RasterLayer
    from app.tasks.scheduling import eligible_fields

    keys = sorted(k.lower() for k in (index_keys or INDEX_REGISTRY))
    for k in keys:
        if k not in INDEX_REGISTRY:
            raise ValueError(f"unknown index {k}")
    wanted = {uuid.UUID(f) for f in field_ids} if field_ids else None

    plans: list[dict] = []
    for field_id, org_id, created_by in eligible_fields(session):
        if wanted is not None and field_id not in wanted:
            continue
        rows = session.execute(
            select(
                RasterLayer.date,
                RasterLayer.layer_type,
                RasterLayer.params_json,
                RasterLayer.provenance_json,
            ).where(RasterLayer.field_id == field_id)
        ).all()
        scene_by_date: dict[date, str] = {}
        version: dict[tuple[date, str], str | None] = {}
        for d, layer_type, params, prov in rows:
            sid = _layer_scene(prov)
            if sid and d not in scene_by_date:
                scene_by_date[d] = sid
            version[(d, layer_type)] = _layer_version(params, prov)

        for key in keys:
            label = INDEX_REGISTRY[key].label
            todo = [
                d
                for d in sorted(scene_by_date)
                if version.get((d, label), None) != target_version
            ]
            for i in range(0, len(todo), MAX_SCENES_PER_JOB):
                chunk = todo[i : i + MAX_SCENES_PER_JOB]
                plans.append(
                    {
                        "field_id": field_id,
                        "org_id": org_id,
                        "created_by": created_by,
                        "index": key,
                        "date_from": chunk[0],
                        "date_to": chunk[-1],
                        "scene_ids": [scene_by_date[d] for d in chunk],
                        "new_layers": sum(
                            1 for d in chunk if (d, label) not in version
                        ),
                    }
                )
    return plans


def _fields_with_jobs_in_flight(session, field_ids) -> set:
    from app.models.tables import Job

    if not field_ids:
        return set()
    return set(
        session.execute(
            select(Job.field_id)
            .where(Job.field_id.in_(field_ids), Job.status.in_(("pending", "running")))
            .group_by(Job.field_id)
            .having(func.count() > 0)
        ).scalars()
    )


@celery_app.task(
    name="app.tasks.reprocess.reprocess_stored_layers",
    bind=True,
    max_retries=0,
    time_limit=300,
    soft_time_limit=240,
)
def reprocess_stored_layers(
    self,
    field_ids: list[str] | None = None,
    index_keys: list[str] | None = None,
    dry_run: bool = False,
    stagger_seconds: int = 20,
) -> dict:
    """Create and dispatch the reprocess jobs from ``plan_reprocess``.

    Jobs are flagged ``is_backfill`` (alert rules are never evaluated for
    them) and carry ``trigger = reprocess_<version>``. Fields with a pending
    or running job are skipped; run again later for them. Jobs are committed
    before their tasks are sent.
    """
    from app.models.tables import Job

    session = get_db_session()
    try:
        plans = plan_reprocess(session, field_ids, index_keys)
        busy = _fields_with_jobs_in_flight(session, {p["field_id"] for p in plans})
        plans_run = [p for p in plans if p["field_id"] not in busy]
        summary = {
            "pipeline_version": PIPELINE_VERSION,
            "jobs": len(plans_run),
            "scenes": sum(len(p["scene_ids"]) for p in plans_run),
            "new_layers": sum(p["new_layers"] for p in plans_run),
            "skipped_busy_fields": sorted(str(f) for f in busy),
            "dry_run": dry_run,
        }
        if dry_run:
            session.rollback()
            return summary

        to_send: list[tuple[str, str, int]] = []
        for n, p in enumerate(plans_run):
            job = Job(
                org_id=p["org_id"],
                field_id=p["field_id"],
                type=p["index"],
                status="pending",
                params_json={
                    "date_from": p["date_from"].isoformat(),
                    "date_to": p["date_to"].isoformat(),
                    "scene_ids": p["scene_ids"],
                    "is_backfill": True,
                    "trigger": REPROCESS_TRIGGER,
                },
                created_by=p["created_by"],
            )
            session.add(job)
            session.flush()
            to_send.append(
                (INDEX_TASK_MAP[p["index"]], str(job.id), n * stagger_seconds)
            )
        session.commit()
        for task_name, job_id, countdown in to_send:
            celery_app.send_task(task_name, args=[job_id], countdown=countdown)
        logger.info("reprocess_dispatched", **summary)
        return summary
    except Exception as e:
        logger.error("reprocess_dispatch_failed", error=str(e))
        session.rollback()
        raise
    finally:
        session.close()


def main(argv: list[str]) -> None:  # pragma: no cover - thin CLI wrapper
    session = get_db_session()
    try:
        plans = plan_reprocess(session, argv or None)
        per_field: dict[str, dict] = defaultdict(lambda: {"jobs": 0, "scenes": 0})
        for p in plans:
            f = per_field[str(p["field_id"])]
            f["jobs"] += 1
            f["scenes"] += len(p["scene_ids"])
        print(
            json.dumps(
                {
                    "pipeline_version": PIPELINE_VERSION,
                    "jobs": len(plans),
                    "scene_index_runs": sum(len(p["scene_ids"]) for p in plans),
                    "new_layers": sum(p["new_layers"] for p in plans),
                    "per_field": per_field,
                    "plans": [
                        {k: v for k, v in p.items() if k != "scene_ids"}
                        | {"scenes": len(p["scene_ids"])}
                        for p in plans
                    ],
                },
                default=str,
                indent=2,
            )
        )
    finally:
        session.rollback()
        session.close()


if __name__ == "__main__":
    main(sys.argv[1:])
