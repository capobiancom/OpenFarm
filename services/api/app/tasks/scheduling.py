"""Planning for the periodic refresh (Celery Beat) - eligibility and windows.

Shared by the scheduled tasks and by a read-only CLI that prints what the
next run would dispatch, so the plan can be checked before Beat is enabled:

    python -m app.tasks.scheduling            # plan for today (UTC), JSON

A field is eligible when it is live (``deleted_at IS NULL``) and its org is
live. In the AgroMind integration that is exactly "analysis enabled": AgroMind
archives the engine field when analysis is disabled, the lot is deleted or
its geometry is superseded.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import func, select, text

from app.core.config import settings

# pg_try_advisory_xact_lock keys: one per periodic dispatcher, so two
# dispatches of the same schedule (Beat restart, redelivery, a second Beat
# by mistake) can never run concurrently.
WEEKLY_INDEX_LOCK_KEY = 0x0F_A2_01
DAILY_WEATHER_LOCK_KEY = 0x0F_A2_02

SCHEDULED_WEEKLY_TRIGGER = "scheduled_weekly"


@dataclass(frozen=True)
class WeeklyIndexPlan:
    field_id: uuid.UUID
    org_id: uuid.UUID
    created_by: uuid.UUID
    latest_scene: date | None
    date_from: date
    date_to: date


def try_lock(session, key: int) -> bool:
    """Transaction-scoped advisory lock; released on commit/rollback."""
    return bool(
        session.execute(
            text("SELECT pg_try_advisory_xact_lock(:k)"), {"k": key}
        ).scalar()
    )


def eligible_fields(session) -> list[tuple[uuid.UUID, uuid.UUID, uuid.UUID]]:
    """(field_id, org_id, created_by) of live fields in live orgs."""
    from app.models.tables import Field, Org

    return [
        tuple(r)
        for r in session.execute(
            select(Field.id, Field.org_id, Field.created_by)
            .join(Org, Org.id == Field.org_id)
            .where(Field.deleted_at.is_(None), Org.deleted_at.is_(None))
            .order_by(Field.created_at, Field.id)
        ).all()
    ]


def weekly_index_window(
    latest_scene: date | None, today: date
) -> tuple[date, date] | None:
    """Window for the weekly refresh, or None when the field is fresh.

    Fresh = latest scene within the last 7 days. Otherwise the window starts
    the day after the latest stored scene (stored dates are never
    reprocessed) and is capped to ``index_weekly_max_window_days``: the
    weekly refresh is never a backfill.
    """
    if latest_scene is not None and latest_scene > today - timedelta(days=7):
        return None
    floor = today - timedelta(days=settings.index_weekly_max_window_days)
    if latest_scene is None:
        start = today - timedelta(days=7)
    else:
        start = max(latest_scene + timedelta(days=1), floor)
    if start >= today:
        return None
    return start, today


def _field_has_guarded_job(session, field_id: uuid.UUID, now: datetime) -> bool:
    """True when the field already has work in flight or was just scheduled."""
    from app.models.tables import Job

    since = now - timedelta(hours=settings.scheduler_job_guard_hours)
    return (
        session.execute(
            select(func.count())
            .select_from(Job)
            .where(
                Job.field_id == field_id,
                Job.created_at >= since,
                (Job.status.in_(("pending", "running")))
                | (Job.params_json["trigger"].astext == SCHEDULED_WEEKLY_TRIGGER),
            )
        ).scalar()
        or 0
    ) > 0


def plan_weekly_index_jobs(session, today: date | None = None) -> list[WeeklyIndexPlan]:
    """Fields (and windows) the weekly index refresh would dispatch."""
    from app.models.tables import RasterLayer

    today = today or datetime.now(timezone.utc).date()
    now = datetime.now(timezone.utc)
    plans: list[WeeklyIndexPlan] = []
    for field_id, org_id, created_by in eligible_fields(session):
        latest = session.execute(
            select(func.max(RasterLayer.date)).where(RasterLayer.field_id == field_id)
        ).scalar_one_or_none()
        window = weekly_index_window(latest, today)
        if window is None:
            continue
        if _field_has_guarded_job(session, field_id, now):
            continue
        plans.append(
            WeeklyIndexPlan(field_id, org_id, created_by, latest, window[0], window[1])
        )
    return plans


def daily_weather_window(last_stored: date | None, today: date) -> tuple[date, date]:
    """Window for the daily weather refresh.

    Re-fetches from the last stored day (it may hold partial data) up to
    today, so days missed while the scheduler was off are filled in, but
    never further back than ``weather_daily_max_gap_days``. With no stored
    data it fetches yesterday and today, like the original daily task.
    """
    floor = today - timedelta(days=settings.weather_daily_max_gap_days)
    yesterday = today - timedelta(days=1)
    if last_stored is None:
        return yesterday, today
    return max(min(last_stored, yesterday), floor), today


def plan_daily_weather(session, today: date | None = None) -> list[dict]:
    from app.models.tables import WeatherDaily

    today = today or datetime.now(timezone.utc).date()
    out = []
    for field_id, _org_id, _created_by in eligible_fields(session):
        last = session.execute(
            select(func.max(WeatherDaily.date)).where(WeatherDaily.field_id == field_id)
        ).scalar_one_or_none()
        start, end = daily_weather_window(last, today)
        out.append(
            {"field_id": str(field_id), "last_stored": last, "start": start, "end": end}
        )
    return out


def main() -> None:  # pragma: no cover - thin CLI wrapper
    from app.tasks.indices import INDEX_REGISTRY
    from app.tasks.pipeline import get_db_session

    session = get_db_session()
    try:
        today = datetime.now(timezone.utc).date()
        weekly = plan_weekly_index_jobs(session, today)
        out = {
            "today_utc": today,
            "alert_rules_enabled": settings.openfarm_alert_rules_enabled,
            "eligible_fields": [str(f[0]) for f in eligible_fields(session)],
            "weekly_index": {
                "fields": [asdict(p) for p in weekly],
                "jobs": len(weekly) * len(INDEX_REGISTRY),
            },
            "daily_weather": plan_daily_weather(session, today),
        }
        print(json.dumps(out, default=str, indent=2))
    finally:
        session.rollback()
        session.close()


if __name__ == "__main__":
    main()
