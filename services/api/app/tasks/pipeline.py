"""Shared vegetation-index pipeline helpers.

Extracted from the original ``ndvi.py`` so that every index task
(NDVI, EVI, SAVI, NDWI) reuses the same STAC search, band download,
COG write, zonal-stats, and alert-evaluation logic.
"""

from __future__ import annotations

import os
import tempfile
from datetime import date, datetime, timezone
from typing import Any

import numpy as np
import rasterio
from rasterio.features import geometry_mask
from rasterio.transform import from_bounds
from rasterio.warp import Resampling, reproject, transform_bounds
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from minio import Minio
from pystac_client import Client as STACClient
from sqlalchemy import func, literal, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm.attributes import flag_modified

import structlog

from app.core.config import settings
from app.tasks.indices import IndexDef

logger = structlog.get_logger()

# ── Configuration (same as ndvi.py) ──────────────────────────────────

STAC_API_URL = os.environ.get(
    "STAC_API_URL", "https://earth-search.aws.element84.com/v1"
)
STAC_COLLECTION = "sentinel-2-l2a"
MAX_CLOUD_COVER = 20
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "openfarm")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "openfarm_dev_secret")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "openfarm")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"

# GDAL environment for reading remote COGs
os.environ.setdefault("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
os.environ.setdefault("CPL_VSIL_CURL_ALLOWED_EXTENSIONS", ".tif,.TIF,.tiff")
os.environ.setdefault("GDAL_HTTP_MERGE_CONSECUTIVE_RANGES", "YES")
os.environ.setdefault("GDAL_HTTP_MULTIPLEX", "YES")
os.environ.setdefault("VSI_CACHE", "TRUE")
os.environ.setdefault("VSI_CACHE_SIZE", "5000000")

RETRY_DELAYS = [60, 300, 900]  # Per PRD Section 7.4


# ── MinIO / DB helpers ───────────────────────────────────────────────


def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def get_db_session():
    from app.core.database_sync import SyncSession

    return SyncSession()


# ── Job progress helpers ─────────────────────────────────────────────


def update_job_progress(session, job, step: str, details: dict | None = None):
    progress = job.progress_json or {}
    progress["current_step"] = step
    progress.setdefault("steps", {})[step] = {
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    if details:
        progress["steps"][step].update(details)
    job.progress_json = progress
    flag_modified(job, "progress_json")
    session.commit()


def complete_step(session, job, step: str, details: dict | None = None):
    progress = job.progress_json or {}
    if step in progress.get("steps", {}):
        progress["steps"][step]["status"] = "completed"
        progress["steps"][step]["finished_at"] = datetime.now(timezone.utc).isoformat()
        if details:
            progress["steps"][step].update(details)
    flag_modified(job, "progress_json")
    session.commit()


# ── STAC scene search ────────────────────────────────────────────────


def search_scenes(
    field_geom_geojson: dict,
    date_from: date,
    date_to: date,
    index_def: IndexDef,
) -> list[dict]:
    """Search Element84 STAC and resolve per-band HREFs for the given index."""
    catalog = STACClient.open(STAC_API_URL)
    search = catalog.search(
        collections=[STAC_COLLECTION],
        intersects=field_geom_geojson,
        datetime=f"{date_from.isoformat()}/{date_to.isoformat()}",
        query={"eo:cloud_cover": {"lt": MAX_CLOUD_COVER}},
        max_items=100,
    )
    items = list(search.items())
    logger.info(
        "stac_search_results",
        count=len(items),
        index=index_def.key,
        date_from=str(date_from),
        date_to=str(date_to),
    )
    if not items:
        return []

    # Group by week, pick lowest cloud cover per week
    weekly: dict[str, Any] = {}
    for item in items:
        item_date = item.datetime.date() if item.datetime else date_from
        week_key = item_date.isocalendar()[:2]
        week_str = f"{week_key[0]}-W{week_key[1]:02d}"
        cloud = item.properties.get("eo:cloud_cover", 100)
        if week_str not in weekly or cloud < weekly[week_str]["cloud"]:
            weekly[week_str] = {"item": item, "cloud": cloud, "date": item_date}

    scenes = []
    for week_str in sorted(weekly.keys()):
        entry = weekly[week_str]
        item = entry["item"]

        # Resolve band HREFs using the index's stac_asset_map
        band_hrefs: dict[str, str | None] = {}
        for band_key in index_def.bands:
            href = None
            for asset_name in index_def.stac_asset_map.get(band_key, (band_key,)):
                asset = item.assets.get(asset_name)
                if asset:
                    href = asset.href
                    break
            band_hrefs[band_key] = href

        # Only include scene if all required bands resolved
        if all(band_hrefs.values()):
            scenes.append(
                {
                    "id": item.id,
                    "date": entry["date"],
                    "cloud_cover": entry["cloud"],
                    "band_hrefs": band_hrefs,
                }
            )
        else:
            missing = [k for k, v in band_hrefs.items() if v is None]
            logger.warning(
                "missing_bands",
                scene_id=item.id,
                index=index_def.key,
                missing_bands=missing,
            )

    return scenes


# ── Band reading ─────────────────────────────────────────────────────


def read_band_windowed(
    href: str, bounds: tuple, target_shape: tuple, target_transform
) -> np.ndarray:
    """Read a band from a remote COG, windowed to field extent."""
    with rasterio.open(href) as src:
        src_bounds = transform_bounds("EPSG:4326", src.crs, *bounds)
        window = rasterio.windows.from_bounds(*src_bounds, transform=src.transform)
        data = src.read(1, window=window, boundless=True, fill_value=0)

        dst = np.zeros(target_shape, dtype=np.float32)
        reproject(
            source=data.astype(np.float32),
            destination=dst,
            src_transform=rasterio.windows.transform(window, src.transform),
            src_crs=src.crs,
            dst_transform=target_transform,
            dst_crs="EPSG:4326",
            resampling=Resampling.bilinear,
        )
        return dst


# ── COG writing ──────────────────────────────────────────────────────


def write_cog(
    data: np.ndarray,
    transform,
    crs: str,
    org_id: str,
    field_id: str,
    scene_date: date,
    index_key: str,
) -> str:
    """Write an index array as COG to MinIO. Returns the ``cog_uri``."""
    object_key = f"cogs/{org_id}/{field_id}/{scene_date.isoformat()}/{index_key}.tif"
    tmp_src_path = tempfile.mktemp(suffix="_src.tif")
    tmp_dst_path = tempfile.mktemp(suffix="_cog.tif")

    try:
        profile = {
            "driver": "GTiff",
            "dtype": "float32",
            "width": data.shape[1],
            "height": data.shape[0],
            "count": 1,
            "crs": crs,
            "transform": transform,
            "nodata": np.nan,
        }
        with rasterio.open(tmp_src_path, "w", **profile) as dst:
            dst.write(data, 1)

        output_profile = cog_profiles.get("deflate")
        cog_translate(
            tmp_src_path, tmp_dst_path, output_profile, overview_level=2, quiet=True
        )

        get_minio_client().fput_object(
            MINIO_BUCKET, object_key, tmp_dst_path, content_type="image/tiff"
        )
        logger.info("cog_uploaded", object_key=object_key, index=index_key)
        return f"s3://{MINIO_BUCKET}/{object_key}"
    finally:
        for p in [tmp_src_path, tmp_dst_path]:
            try:
                os.unlink(p)
            except OSError:
                pass


# ── Zonal statistics ─────────────────────────────────────────────────


def compute_zonal_stats(data: np.ndarray) -> dict:
    """Compute zonal statistics over the valid (finite) pixels."""
    valid = data[np.isfinite(data)]
    if len(valid) == 0:
        return {
            "mean": None,
            "median": None,
            "min": None,
            "max": None,
            "stddev": None,
            "p10": None,
            "p90": None,
            "quality_score": 0.0,
        }
    total_pixels = data.size
    return {
        "mean": float(np.nanmean(valid)),
        "median": float(np.nanmedian(valid)),
        "min": float(np.nanmin(valid)),
        "max": float(np.nanmax(valid)),
        "stddev": float(np.nanstd(valid)),
        "p10": float(np.nanpercentile(valid, 10)),
        "p90": float(np.nanpercentile(valid, 90)),
        "quality_score": round(len(valid) / total_pixels, 4) if total_pixels else 0.0,
    }


# ── Alert evaluation ────────────────────────────────────────────────


def _get_weather_context(session, field_id, alert_date: date) -> dict | None:
    """Query recent weather data to build context JSONB for alert enrichment."""
    from datetime import timedelta

    from sqlalchemy import select

    from app.models.tables import WeatherDaily

    start = alert_date - timedelta(days=7)
    rows = (
        session.execute(
            select(WeatherDaily)
            .where(
                WeatherDaily.field_id == field_id,
                WeatherDaily.date >= start,
                WeatherDaily.date <= alert_date,
            )
            .order_by(WeatherDaily.date.desc())
        )
        .scalars()
        .all()
    )
    if not rows:
        return None

    latest = rows[0]
    precip_7d = sum(float(r.precipitation_sum or 0) for r in rows)
    et0_7d = sum(float(r.et0_fao_mm or 0) for r in rows)

    ctx: dict = {
        "period": f"{start.isoformat()} to {alert_date.isoformat()}",
        "precipitation_7d_mm": round(precip_7d, 1),
        "et0_7d_mm": round(et0_7d, 1),
    }
    if latest.water_balance_30d_mm is not None:
        ctx["water_deficit_mm"] = round(float(latest.water_balance_30d_mm), 1)
    if latest.soil_moisture_0_1cm is not None:
        ctx["soil_moisture_top"] = round(float(latest.soil_moisture_0_1cm), 3)
    if latest.gdd_cumulative is not None:
        ctx["gdd_cumulative"] = round(float(latest.gdd_cumulative), 1)
    if latest.drought_index is not None:
        ctx["drought_index"] = round(float(latest.drought_index), 2)
    return ctx


def run_alerts(
    session,
    field_id,
    org_id,
    scene_date: date,
    stats: dict,
    history: list[tuple[date, float | None]],
    index_def: IndexDef,
) -> int:
    """Evaluate the index alert rules for one observation; returns alerts inserted.

    ``history`` holds prior (date, mean) observations of the SAME index (see
    app.core.alert_rules for how it is cleaned). Inserts are idempotent: an
    alert is identified by (field_id, rule_name, date), enforced by the
    unique index uq_alerts_field_rule_date, and an existing one - open or
    already managed (closed) - is never duplicated or overwritten.
    """
    from app.core.alert_rules import evaluate_index_rules
    from app.models.tables import Alert

    if not settings.openfarm_alert_rules_enabled:
        return 0

    candidates = evaluate_index_rules(index_def, scene_date, stats.get("mean"), history)
    if not candidates:
        return 0

    # Fetch recent weather context for alert enrichment (None if absent)
    weather_ctx = _get_weather_context(session, field_id, scene_date)

    inserted = 0
    for c in candidates:
        result = session.execute(
            pg_insert(Alert)
            .values(
                org_id=org_id,
                field_id=field_id,
                date=c.observed_date,
                severity=c.severity,
                rule_name=c.rule_name,
                rule_params_json=c.params,
                message=c.message,
                status="open",
                index_type=c.index_type,
                weather_context=weather_ctx,
            )
            .on_conflict_do_nothing(index_elements=["field_id", "rule_name", "date"])
        )
        inserted += result.rowcount or 0
    session.commit()
    logger.info(
        "alerts_evaluated",
        field_id=str(field_id),
        index=index_def.key,
        date=str(scene_date),
        candidates=[c.rule_name for c in candidates],
        inserted=inserted,
    )
    return inserted


# ── Grid / mask helpers ──────────────────────────────────────────────


def compute_target_grid(field_bounds: tuple, field_geom):
    """Return (target_transform, target_shape, field_mask, expanded_bounds)."""
    minx, miny, maxx, maxy = field_bounds
    buf = 0.001
    minx -= buf
    miny -= buf
    maxx += buf
    maxy += buf
    pixel_size = 0.0001
    width = max(int((maxx - minx) / pixel_size), 1)
    height = max(int((maxy - miny) / pixel_size), 1)
    max_dim = 5000
    if width > max_dim or height > max_dim:
        scale = max_dim / max(width, height)
        width = max(int(width * scale), 1)
        height = max(int(height * scale), 1)

    target_transform = from_bounds(minx, miny, maxx, maxy, width, height)
    target_shape = (height, width)
    field_mask = geometry_mask(
        [field_geom],
        out_shape=target_shape,
        transform=target_transform,
        invert=True,
    )
    return target_transform, target_shape, field_mask, (minx, miny, maxx, maxy)


# ── Full per-scene processing ────────────────────────────────────────


def process_scene(
    session,
    job,
    scene: dict,
    scene_idx: int,
    total_scenes: int,
    index_def: IndexDef,
    target_transform,
    target_shape: tuple,
    field_mask: np.ndarray,
    bounds: tuple,
    org_id_str: str,
    field_id_str: str,
    date_from: date,
    date_to: date,
    historical_means: list[tuple[date, float | None]],
    extra_params: dict | None = None,
):
    """Download bands, compute index, write COG, stats, alerts for one scene.

    Returns the stats dict on success, ``None`` on failure.
    """
    from app.models.tables import RasterLayer, FieldStat

    scene_id = scene["id"]
    scene_date = scene["date"]
    band_hrefs = scene["band_hrefs"]
    index_key = index_def.key
    compute_step = f"compute_{index_key}"

    # -- download bands --
    update_job_progress(
        session,
        job,
        "download_bands",
        {"scene": scene_idx + 1, "total_scenes": total_scenes, "scene_id": scene_id},
    )
    bands: dict[str, np.ndarray] = {}
    for band_key, href in band_hrefs.items():
        bands[band_key] = read_band_windowed(
            href, bounds, target_shape, target_transform
        )
    complete_step(session, job, "download_bands")

    # -- compute index --
    update_job_progress(session, job, compute_step)
    kwargs = extra_params or {}
    index_data = index_def.formula(bands, **kwargs)
    index_data[~np.isfinite(index_data)] = np.nan
    index_data[~field_mask] = np.nan
    complete_step(session, job, compute_step)

    # -- write COG --
    update_job_progress(session, job, "write_cog")
    cog_uri = write_cog(
        index_data,
        target_transform,
        "EPSG:4326",
        org_id_str,
        field_id_str,
        scene_date,
        index_key,
    )
    complete_step(session, job, "write_cog")

    # -- compute stats --
    update_job_progress(session, job, "compute_stats")
    stats = compute_zonal_stats(index_data)
    valid = index_data[np.isfinite(index_data)]
    data_min = float(np.nanmin(valid)) if len(valid) > 0 else None
    data_max = float(np.nanmax(valid)) if len(valid) > 0 else None

    # -- upsert RasterLayer (field_id + date + layer_type) --
    layer_values = dict(
        org_id=job.org_id,
        field_id=job.field_id,
        layer_type=index_def.label,
        satellite="S2",
        date=scene_date,
        cog_uri=cog_uri,
        min=data_min,
        max=data_max,
        params_json={
            "date_from": str(date_from),
            "date_to": str(date_to),
            "cloud_cover": scene["cloud_cover"],
            **kwargs,
        },
        provenance_json={
            "scene_id": scene_id,
            "bands": band_hrefs,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "pipeline_version": "2.0.0",
        },
    )
    # Reprocessing an existing date keeps the original params/provenance
    # (e.g. is_backfill and the backfill window) and records the latest run
    # under its own keys, so the origin of the data is never lost.
    reprocess_params = {
        **layer_values["params_json"],
        "job_id": str(job.id),
        "is_backfill": bool((job.params_json or {}).get("is_backfill", False)),
    }
    stmt = (
        pg_insert(RasterLayer)
        .values(**layer_values)
        .on_conflict_do_update(
            constraint="uq_raster_field_date_type",
            set_={
                "cog_uri": cog_uri,
                "min": data_min,
                "max": data_max,
                "params_json": func.coalesce(
                    RasterLayer.__table__.c.params_json, literal({}, type_=JSONB)
                ).op("||")(
                    literal({"last_reprocess": reprocess_params}, type_=JSONB)
                ),
                "provenance_json": func.coalesce(
                    RasterLayer.__table__.c.provenance_json, literal({}, type_=JSONB)
                ).op("||")(
                    literal(
                        {
                            "last_reprocessed_at": layer_values["provenance_json"][
                                "processed_at"
                            ],
                            "last_reprocess_scene_id": scene_id,
                            "last_reprocess_pipeline_version": "2.0.0",
                        },
                        type_=JSONB,
                    )
                ),
            },
        )
        .returning(RasterLayer.id)
    )
    layer_id = session.execute(stmt).scalar_one()
    session.flush()

    # -- upsert FieldStat (via layer_id which is now stable) --
    existing_stat = session.execute(
        select(FieldStat.id).where(
            FieldStat.field_id == job.field_id,
            FieldStat.date == scene_date,
            FieldStat.layer_id == layer_id,
        )
    ).scalar_one_or_none()

    stat_values = dict(
        mean=stats["mean"],
        median=stats["median"],
        min=stats["min"],
        max=stats["max"],
        p10=stats["p10"],
        p90=stats["p90"],
        stddev=stats["stddev"],
        quality_score=stats["quality_score"],
    )
    if existing_stat:
        session.execute(
            FieldStat.__table__.update()
            .where(FieldStat.id == existing_stat)
            .values(**stat_values)
        )
    else:
        field_stat = FieldStat(
            org_id=job.org_id,
            field_id=job.field_id,
            layer_id=layer_id,
            date=scene_date,
            **stat_values,
        )
        session.add(field_stat)
    session.commit()
    complete_step(session, job, "compute_stats")

    # -- run alerts (skip for backfill jobs to avoid flooding, and whenever
    # the alert rules are disabled: indices are still stored above) --
    is_backfill = (job.params_json or {}).get("is_backfill", False)
    update_job_progress(session, job, "run_alerts")
    # The rules read only observations dated before this scene; it joins the
    # history afterwards so later scenes of the same job can use it.
    history_before = list(historical_means)
    if stats["mean"] is not None:
        historical_means.append((scene_date, stats["mean"]))
    if not settings.openfarm_alert_rules_enabled:
        logger.info(
            "alert_rules_disabled",
            job_id=str(job.id),
            field_id=str(job.field_id),
            index=index_key,
            date=str(scene_date),
        )
    elif not is_backfill:
        run_alerts(
            session,
            job.field_id,
            job.org_id,
            scene_date,
            stats,
            history_before,
            index_def,
        )
    complete_step(session, job, "run_alerts")

    logger.info(
        "scene_processed",
        scene_id=scene_id,
        index=index_key,
        date=str(scene_date),
        mean=stats["mean"],
    )
    return stats
