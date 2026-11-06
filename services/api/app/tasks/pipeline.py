"""Shared vegetation-index pipeline helpers.

Extracted from the original ``ndvi.py`` so that every index task
(NDVI, EVI, SAVI, NDWI, NDMI) reuses the same STAC search, band download,
COG write, zonal-stats, and alert-evaluation logic.

Pipeline 3.0.0 (radiometry and masking):

* Bands are converted from L2A digital numbers to surface reflectance with
  the per-item STAC metadata before any formula (``reflectance_params``).
* Band nodata (DN 0) is NaN, never 0. Continuous bands are resampled
  bilinearly with nodata excluded, and a validity mask resampled with
  nearest neighbour drops any target pixel whose source pixel is nodata.
* The Scene Classification Layer (SCL, 20 m, nearest) masks no data,
  saturated/defective, cloud shadow, cloud medium/high, thin cirrus and
  snow (``SCL_MASKED_CLASSES``).
* ``quality_score`` is the clear-and-valid fraction of the pixels inside the
  field polygon; per-layer cloud/nodata fractions go to params_json.
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
STAC_COLLECTION = os.environ.get("STAC_COLLECTION", "sentinel-2-l2a")
MAX_CLOUD_COVER = 20
PIPELINE_VERSION = "3.0.0"

# Sentinel-2 L2A Scene Classification (SCL) classes masked out of every index.
#   0 no data, 1 saturated or defective, 3 cloud shadow,
#   8 cloud medium probability, 9 cloud high probability, 10 thin cirrus,
#   11 snow or ice.
# Kept as valid: 2 dark area pixels (topographic shadow; masking it removes
# dark but valid soil), 4 vegetation, 5 not vegetated, 6 water,
# 7 unclassified (often field edges or haze-free mixed pixels).
SCL_ASSET = "scl"
SCL_NODATA_CLASSES = frozenset({0})
SCL_CLOUD_CLASSES = frozenset({3, 8, 9, 10})
SCL_OTHER_MASKED_CLASSES = frozenset({1, 11})
SCL_MASKED_CLASSES = SCL_NODATA_CLASSES | SCL_CLOUD_CLASSES | SCL_OTHER_MASKED_CLASSES

# L2A quantification value (ESA product spec): reflectance = DN / 10000.
DEFAULT_REFLECTANCE_SCALE = 1e-4

# What to do with an item whose processing baseline is >= 04.00 (ESA
# BOA_ADD_OFFSET = -1000) but which does not state
# earthsearch:boa_offset_applied = true. Pixel comparisons against the ESA
# product (Planetary Computer, same scene) showed Element84 COGs already
# hold DN - 1000 for such items too, so the default is not to subtract the
# offset again. "apply_stac_offset" uses raster:bands offset literally,
# "skip" leaves the scene out.
S2_AMBIGUOUS_OFFSET_POLICY = os.environ.get(
    "S2_AMBIGUOUS_OFFSET_POLICY", "assume_applied"
)
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

# Job params that steer the run and must not be copied into layer params.
_NON_LAYER_PARAMS = frozenset({"date_from", "date_to", "scene_ids"})


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


def _baseline_at_least_4(baseline: Any) -> bool | None:
    try:
        return float(str(baseline)) >= 4.0
    except (TypeError, ValueError):
        return None


def reflectance_params(
    item_properties: dict, raster_band: dict | None
) -> tuple[float, float, str] | None:
    """(scale, offset, source) turning one item's DN into surface reflectance.

    ``reflectance = DN * scale + offset``. Decided from the item metadata:

    * scale: ``raster:bands[0].scale`` (L2A quantification 1e-4 if absent).
    * ``earthsearch:boa_offset_applied`` true: the provider already applied
      BOA_ADD_OFFSET to the pixel values, so the offset is 0 (the
      raster:bands offset of -0.1 describes the original ESA product and
      must not be applied a second time).
    * processing baseline < 04.00: no BOA offset exists; raster:bands offset
      (0 for these items).
    * baseline >= 04.00 without the flag: ambiguous metadata, resolved by
      ``S2_AMBIGUOUS_OFFSET_POLICY``. Returns None when the policy is "skip".
    """
    rb = raster_band or {}
    scale = rb.get("scale")
    scale = float(scale) if scale not in (None, 0) else DEFAULT_REFLECTANCE_SCALE
    stac_offset = float(rb.get("offset") or 0.0)
    applied = item_properties.get("earthsearch:boa_offset_applied")
    new_baseline = _baseline_at_least_4(item_properties.get("s2:processing_baseline"))

    if applied is True:
        return scale, 0.0, "boa_offset_applied_by_provider"
    if new_baseline is False:
        return scale, stac_offset, "baseline_lt_04_raster_bands"
    policy = S2_AMBIGUOUS_OFFSET_POLICY
    if policy == "apply_stac_offset":
        return scale, stac_offset, "ambiguous_applied_raster_bands_offset"
    if policy == "skip":
        return None
    return scale, 0.0, "ambiguous_assumed_applied"


def scene_from_item(
    item, index_def: IndexDef, item_date: date | None = None
) -> dict | None:
    """Scene dict (hrefs, reflectance params, SCL) for one STAC item.

    Returns None (and logs) when a required asset or its radiometric
    metadata is missing.
    """
    band_hrefs: dict[str, str | None] = {}
    reflectance: dict[str, dict] = {}
    for band_key in index_def.bands:
        asset = None
        for asset_name in index_def.stac_asset_map.get(band_key, (band_key,)):
            asset = item.assets.get(asset_name)
            if asset:
                break
        band_hrefs[band_key] = asset.href if asset else None
        if asset:
            rbs = asset.extra_fields.get("raster:bands") or [{}]
            params = reflectance_params(item.properties, rbs[0])
            if params is None:
                logger.warning(
                    "ambiguous_reflectance_offset_skipped",
                    scene_id=item.id,
                    baseline=item.properties.get("s2:processing_baseline"),
                )
                return None
            scale, offset, source = params
            reflectance[band_key] = {
                "scale": scale,
                "offset": offset,
                "source": source,
                "nodata": rbs[0].get("nodata", 0),
            }
    scl = item.assets.get(SCL_ASSET)
    missing = [k for k, v in band_hrefs.items() if v is None]
    if scl is None:
        missing.append("SCL")
    if missing:
        logger.warning(
            "missing_bands",
            scene_id=item.id,
            index=index_def.key,
            missing_bands=missing,
        )
        return None
    return {
        "id": item.id,
        "date": item_date or (item.datetime.date() if item.datetime else None),
        "cloud_cover": item.properties.get("eo:cloud_cover"),
        "band_hrefs": band_hrefs,
        "scl_href": scl.href,
        "reflectance": reflectance,
        "processing_baseline": item.properties.get("s2:processing_baseline"),
        "boa_offset_applied": item.properties.get("earthsearch:boa_offset_applied"),
    }


def scenes_by_ids(scene_ids: list[str], index_def: IndexDef) -> list[dict]:
    """Resolve stored scene ids (reprocessing) to scene dicts, sorted by date."""
    catalog = STACClient.open(STAC_API_URL)
    scenes: list[dict] = []
    ids = list(dict.fromkeys(scene_ids))
    for i in range(0, len(ids), 50):
        search = catalog.search(
            collections=[STAC_COLLECTION], ids=ids[i : i + 50], max_items=100
        )
        for item in search.items():
            scene = scene_from_item(item, index_def)
            if scene is not None:
                scenes.append(scene)
    found = {s["id"] for s in scenes}
    for sid in ids:
        if sid not in found:
            logger.warning(
                "reprocess_scene_unavailable", scene_id=sid, index=index_def.key
            )
    # One scene per date (the stored layers are unique per field/date/type).
    by_date: dict[date, dict] = {}
    for sc in scenes:
        by_date.setdefault(sc["date"], sc)
    return [by_date[d] for d in sorted(by_date)]


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
        # Only include the scene if all required bands (and SCL) resolved
        scene = scene_from_item(entry["item"], index_def, entry["date"])
        if scene is not None:
            scene["cloud_cover"] = entry["cloud"]
            scenes.append(scene)

    return scenes


# ── Band reading ─────────────────────────────────────────────────────


def read_band_windowed(
    href: str,
    bounds: tuple,
    target_shape: tuple,
    target_transform,
    *,
    categorical: bool = False,
    nodata: float = 0,
) -> tuple[np.ndarray, np.ndarray]:
    """Read one band from a remote COG onto the target grid.

    Returns ``(values, valid)``. Pixels outside the scene footprint or equal
    to the band nodata value are never turned into data:

    * continuous bands (``categorical=False``): bilinear resampling with the
      nodata value excluded from the kernel, output NaN where invalid;
    * ``valid`` is the nodata mask resampled with nearest neighbour, so a
      target pixel whose nearest source pixel is nodata is invalid even if
      bilinear mixing found valid neighbours;
    * categorical bands (SCL): nearest resampling, nodata stays ``nodata``.
    """
    with rasterio.open(href) as src:
        src_bounds = transform_bounds("EPSG:4326", src.crs, *bounds)
        window = rasterio.windows.from_bounds(*src_bounds, transform=src.transform)
        data = src.read(1, window=window, boundless=True, fill_value=nodata)
        src_transform = rasterio.windows.transform(window, src.transform)
        src_crs = src.crs

    src_valid = (data != nodata).astype(np.uint8)
    valid_u8 = np.zeros(target_shape, dtype=np.uint8)
    reproject(
        source=src_valid,
        destination=valid_u8,
        src_transform=src_transform,
        src_crs=src_crs,
        dst_transform=target_transform,
        dst_crs="EPSG:4326",
        resampling=Resampling.nearest,
    )
    valid = valid_u8.astype(bool)

    if categorical:
        dst = np.full(target_shape, nodata, dtype=data.dtype)
        reproject(
            source=data,
            destination=dst,
            src_transform=src_transform,
            src_crs=src_crs,
            dst_transform=target_transform,
            dst_crs="EPSG:4326",
            src_nodata=nodata,
            dst_nodata=nodata,
            resampling=Resampling.nearest,
        )
        return dst, valid & (dst != nodata)

    dst = np.full(target_shape, np.nan, dtype=np.float32)
    reproject(
        source=data.astype(np.float32),
        destination=dst,
        src_transform=src_transform,
        src_crs=src_crs,
        dst_transform=target_transform,
        dst_crs="EPSG:4326",
        src_nodata=float(nodata),
        dst_nodata=np.nan,
        resampling=Resampling.bilinear,
    )
    valid &= np.isfinite(dst)
    dst[~valid] = np.nan
    return dst, valid


def to_reflectance(dn: np.ndarray, scale: float, offset: float) -> np.ndarray:
    """DN to surface reflectance; NaN (nodata) stays NaN."""
    return (dn.astype(np.float32) * np.float32(scale) + np.float32(offset)).astype(
        np.float32
    )


def scl_masks(scl: np.ndarray, scl_valid: np.ndarray) -> dict[str, np.ndarray]:
    """Boolean masks from the SCL band: nodata, cloud, other masked, clear."""
    nodata = ~scl_valid | np.isin(scl, list(SCL_NODATA_CLASSES))
    cloud = ~nodata & np.isin(scl, list(SCL_CLOUD_CLASSES))
    other = ~nodata & np.isin(scl, list(SCL_OTHER_MASKED_CLASSES))
    return {
        "nodata": nodata,
        "cloud": cloud,
        "other": other,
        "clear": ~(nodata | cloud | other),
    }


def read_scene_bands(
    scene: dict, bounds: tuple, target_shape: tuple, target_transform
) -> tuple[dict[str, np.ndarray], dict[str, np.ndarray]]:
    """Reflectance bands (NaN where nodata or SCL-masked) and pixel masks.

    Returns ``(bands, masks)`` where ``masks`` has boolean arrays
    ``band_nodata`` (any required band is nodata), ``scl_nodata``, ``cloud``,
    ``other`` (saturated/defective, snow) and ``clear`` (usable pixel).
    """
    reflectance = scene.get("reflectance")
    if not reflectance:
        raise ValueError(f"scene {scene.get('id')} has no reflectance metadata")
    bands: dict[str, np.ndarray] = {}
    band_nodata = np.zeros(target_shape, dtype=bool)
    for band_key, href in scene["band_hrefs"].items():
        meta = reflectance[band_key]
        dn, valid = read_band_windowed(
            href,
            bounds,
            target_shape,
            target_transform,
            nodata=meta.get("nodata", 0) or 0,
        )
        band_nodata |= ~valid
        bands[band_key] = to_reflectance(dn, meta["scale"], meta["offset"])

    scl, scl_valid = read_band_windowed(
        scene["scl_href"], bounds, target_shape, target_transform, categorical=True
    )
    m = scl_masks(scl, scl_valid)
    clear = m["clear"] & ~band_nodata
    for arr in bands.values():
        arr[~clear] = np.nan
    masks = {
        "band_nodata": band_nodata,
        "scl_nodata": m["nodata"],
        "cloud": m["cloud"],
        "other": m["other"],
        "clear": clear,
    }
    return bands, masks


def field_quality(
    masks: dict[str, np.ndarray], field_mask: np.ndarray, index_data: np.ndarray
) -> dict:
    """Per-field quality fractions over the pixels inside the polygon.

    * ``field_nodata_fraction``: no data in any band or SCL class 0.
    * ``field_cloud_fraction``: SCL cloud shadow/medium/high/cirrus among
      pixels with data.
    * ``field_other_masked_fraction``: saturated/defective or snow.
    * ``quality_score``: clear pixels with a finite index value.
    """
    n = int(field_mask.sum())
    if n == 0:
        return {
            "field_pixels": 0,
            "field_nodata_fraction": 1.0,
            "field_cloud_fraction": 0.0,
            "field_other_masked_fraction": 0.0,
            "quality_score": 0.0,
        }
    nodata = (masks["band_nodata"] | masks["scl_nodata"]) & field_mask
    cloud = masks["cloud"] & ~nodata & field_mask
    other = masks["other"] & ~nodata & ~cloud & field_mask
    usable = field_mask & np.isfinite(index_data)
    return {
        "field_pixels": n,
        "field_nodata_fraction": round(float(nodata.sum()) / n, 4),
        "field_cloud_fraction": round(float(cloud.sum()) / n, 4),
        "field_other_masked_fraction": round(float(other.sum()) / n, 4),
        "quality_score": round(float(usable.sum()) / n, 4),
    }


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


def compute_zonal_stats(data: np.ndarray, field_mask: np.ndarray | None = None) -> dict:
    """Zonal statistics over the finite pixels.

    ``quality_score`` = finite pixels / pixels inside ``field_mask`` (the
    polygon). Without a mask it falls back to the whole array, which is the
    pre-3.0.0 bounding-box definition kept only for callers that have no
    polygon.
    """
    if field_mask is not None:
        data = np.where(field_mask, data, np.nan)
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
    total_pixels = int(field_mask.sum()) if field_mask is not None else data.size
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
    historical_means: list[float],
    index_def: IndexDef,
):
    """Evaluate threshold and drop alert rules for any index type."""
    from app.models.tables import Alert

    if not settings.openfarm_alert_rules_enabled:
        return
    if index_def.alerts is None:  # e.g. NDWI (open water) or NDMI
        return

    current_mean = stats.get("mean")
    if current_mean is None:
        return

    # Fetch recent weather context for alert enrichment
    weather_ctx = _get_weather_context(session, field_id, scene_date)

    alert_cfg = index_def.alerts
    label = index_def.label

    # threshold rule
    if current_mean < alert_cfg.threshold:
        severity = "high" if current_mean < alert_cfg.threshold_high else "medium"
        session.add(
            Alert(
                org_id=org_id,
                field_id=field_id,
                date=scene_date,
                severity=severity,
                rule_name=f"{index_def.key}_threshold",
                rule_params_json={"threshold": alert_cfg.threshold},
                message=(
                    f"{label} mean ({current_mean:.3f}) below threshold "
                    f"({alert_cfg.threshold}). Consider scouting."
                ),
                status="open",
                index_type=index_def.key,
                weather_context=weather_ctx,
            )
        )

    # drop rule
    if len(historical_means) >= 2:
        window = historical_means[-alert_cfg.drop_window :]
        rolling_avg = sum(window) / len(window)
        if rolling_avg > 0:
            drop_pct = ((rolling_avg - current_mean) / rolling_avg) * 100
            if drop_pct >= alert_cfg.drop_pct:
                severity = (
                    "high" if drop_pct >= 30 else "medium" if drop_pct >= 20 else "low"
                )
                session.add(
                    Alert(
                        org_id=org_id,
                        field_id=field_id,
                        date=scene_date,
                        severity=severity,
                        rule_name=f"{index_def.key}_drop",
                        rule_params_json={
                            "drop_pct": alert_cfg.drop_pct,
                            "window": alert_cfg.drop_window,
                        },
                        message=(
                            f"{label} dropped {drop_pct:.1f}% "
                            f"(from avg {rolling_avg:.3f} to {current_mean:.3f}). "
                            f"Investigate crop stress."
                        ),
                        status="open",
                        index_type=index_def.key,
                        weather_context=weather_ctx,
                    )
                )
    session.commit()


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
    historical_means: list[float],
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

    # -- download bands (reflectance, nodata NaN, SCL-masked) --
    update_job_progress(
        session,
        job,
        "download_bands",
        {"scene": scene_idx + 1, "total_scenes": total_scenes, "scene_id": scene_id},
    )
    bands, masks = read_scene_bands(scene, bounds, target_shape, target_transform)
    complete_step(session, job, "download_bands")

    # -- compute index --
    update_job_progress(session, job, compute_step)
    kwargs = {
        k: v for k, v in (extra_params or {}).items() if k not in _NON_LAYER_PARAMS
    }
    formula_kwargs = {k: v for k, v in kwargs.items() if k == "savi_l"}
    index_data = index_def.formula(bands, **formula_kwargs).astype(np.float32)
    index_data[~np.isfinite(index_data)] = np.nan
    index_data[~masks["clear"]] = np.nan
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
    stats = compute_zonal_stats(index_data, field_mask)
    quality = field_quality(masks, field_mask, index_data)
    stats["quality_score"] = quality["quality_score"]
    valid = index_data[np.isfinite(index_data)]
    data_min = float(np.nanmin(valid)) if len(valid) > 0 else None
    data_max = float(np.nanmax(valid)) if len(valid) > 0 else None

    # Describes the data now stored (COG + stats); rewritten on every run.
    layer_quality = {
        "pipeline_version": PIPELINE_VERSION,
        "cloud_cover": scene.get("cloud_cover"),
        "field_cloud_fraction": quality["field_cloud_fraction"],
        "field_nodata_fraction": quality["field_nodata_fraction"],
        "field_other_masked_fraction": quality["field_other_masked_fraction"],
        "field_pixels": quality["field_pixels"],
        "quality_score": quality["quality_score"],
        "scl_masked": True,
        "scl_masked_classes": sorted(SCL_MASKED_CLASSES),
        "reflectance": {
            b: {"scale": m["scale"], "offset": m["offset"], "source": m["source"]}
            for b, m in scene["reflectance"].items()
        },
        "processing_baseline": scene.get("processing_baseline"),
        "boa_offset_applied": scene.get("boa_offset_applied"),
    }

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
            **kwargs,
            **layer_quality,
        },
        provenance_json={
            "scene_id": scene_id,
            "bands": band_hrefs,
            "scl": scene.get("scl_href"),
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "pipeline_version": PIPELINE_VERSION,
        },
    )
    # Reprocessing an existing date keeps the original params/provenance
    # (e.g. is_backfill and the backfill window) and records the latest run
    # under its own keys, so the origin of the data is never lost.
    # The quality keys describe the current COG/stats, so they overwrite the
    # previous run's values at the top level as well.
    reprocess_params = {
        "date_from": str(date_from),
        "date_to": str(date_to),
        **kwargs,
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
                    literal(
                        {"last_reprocess": reprocess_params, **layer_quality},
                        type_=JSONB,
                    )
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
                            "last_reprocess_bands": band_hrefs,
                            "last_reprocess_pipeline_version": PIPELINE_VERSION,
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
    if stats["mean"] is not None:
        historical_means.append(stats["mean"])
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
            historical_means,
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
