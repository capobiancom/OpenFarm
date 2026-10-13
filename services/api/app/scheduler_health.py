"""Healthcheck for the Celery Beat service (openfarm-scheduler).

Beat persists its schedule state to a shelve file and re-syncs it at least
every few minutes while its loop is alive (tick <= beat_max_loop_interval,
sync every 3 min). A stale or missing file means the loop is stuck or dead.

    python -m app.scheduler_health [schedule_path] [max_age_seconds]
"""

from __future__ import annotations

import glob
import os
import sys
import time

DEFAULT_PATH = "/tmp/celerybeat-schedule"
DEFAULT_MAX_AGE = 900


def schedule_age(path: str = DEFAULT_PATH) -> float | None:
    """Seconds since the schedule file (any shelve suffix) was last written."""
    files = glob.glob(path + "*")
    if not files:
        return None
    return time.time() - max(os.path.getmtime(f) for f in files)


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PATH
    max_age = float(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_MAX_AGE
    age = schedule_age(path)
    if age is None:
        print(f"unhealthy: no schedule file at {path}*")
        return 1
    if age > max_age:
        print(f"unhealthy: schedule file not synced for {age:.0f}s")
        return 1
    print(f"healthy: schedule synced {age:.0f}s ago")
    return 0


if __name__ == "__main__":
    sys.exit(main())
