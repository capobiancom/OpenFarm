"""NDVI processing task - backward-compatible entry point.

Delegates to the shared vegetation-index pipeline. The Celery task name
``app.tasks.ndvi.process_ndvi`` is preserved so existing jobs, imports, and
the ``POST /jobs/ndvi`` endpoint keep working.
"""

from __future__ import annotations

from app.worker import celery_app


@celery_app.task(
    name="app.tasks.ndvi.process_ndvi",
    bind=True,
    max_retries=3,
    time_limit=1800,
    soft_time_limit=1500,
)
def process_ndvi(self, job_id: str) -> dict:
    """Process NDVI for a field - delegates to the shared pipeline."""
    from app.tasks.vegetation import _run_index_pipeline

    return _run_index_pipeline(self, job_id, "ndvi")
