"""Vegetation index registry - formulas, bands, colormaps, alert defaults.

Every Celery task, API endpoint, and tile URL builder references this
registry so index behaviour is defined in exactly one place.

Formulas receive surface reflectance (0..1, float) with NaN for invalid
pixels: ``pipeline.read_scene_bands`` converts Sentinel-2 L2A digital
numbers with the per-item scale/offset and applies the nodata and SCL
masks before any formula runs. EVI and SAVI carry additive constants
(+1 and +L), so they are only meaningful on reflectance, never on raw DN.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import numpy as np


# ── Formula functions ────────────────────────────────────────────────


def _ndvi(bands: dict[str, np.ndarray], **_kw) -> np.ndarray:
    nir, red = bands["B08"], bands["B04"]
    with np.errstate(divide="ignore", invalid="ignore"):
        result = (nir - red) / (nir + red)
    return np.clip(result, -1.0, 1.0)


def _evi(bands: dict[str, np.ndarray], **_kw) -> np.ndarray:
    nir, red, blue = bands["B08"], bands["B04"], bands["B02"]
    with np.errstate(divide="ignore", invalid="ignore"):
        result = 2.5 * (nir - red) / (nir + 6.0 * red - 7.5 * blue + 1.0)
    return np.clip(result, -1.0, 1.0)


def _savi(bands: dict[str, np.ndarray], **_kw) -> np.ndarray:
    nir, red = bands["B08"], bands["B04"]
    L = _kw.get("savi_l", 0.5)
    with np.errstate(divide="ignore", invalid="ignore"):
        result = ((nir - red) / (nir + red + L)) * (1.0 + L)
    return np.clip(result, -1.5, 1.5)


def _ndwi(bands: dict[str, np.ndarray], **_kw) -> np.ndarray:
    """McFeeters (1996) NDWI: open surface water, not canopy water content."""
    green, nir = bands["B03"], bands["B08"]
    with np.errstate(divide="ignore", invalid="ignore"):
        result = (green - nir) / (green + nir)
    return np.clip(result, -1.0, 1.0)


def _ndmi(bands: dict[str, np.ndarray], **_kw) -> np.ndarray:
    """NDMI (Gao 1996 NDWI, NIR/SWIR1): vegetation water content."""
    nir, swir1 = bands["B08"], bands["B11"]
    with np.errstate(divide="ignore", invalid="ignore"):
        result = (nir - swir1) / (nir + swir1)
    return np.clip(result, -1.0, 1.0)


# ── Alert configuration ─────────────────────────────────────────────


@dataclass(frozen=True)
class AlertDefaults:
    threshold: float
    threshold_high: float  # below this → high severity (else medium)
    drop_pct: float
    drop_window: int = 4


# ── Index definition ─────────────────────────────────────────────────


@dataclass(frozen=True)
class IndexDef:
    """Complete definition for one vegetation index."""

    key: str  # lowercase, used in COG paths and job types
    label: str  # uppercase, stored in raster_layers.layer_type
    bands: tuple[str, ...]  # Sentinel-2 asset keys required
    formula: Callable[[dict[str, np.ndarray]], np.ndarray]
    colormap: str  # TiTiler colormap name
    rescale: tuple[float, float]  # min, max for tile rendering
    # None = no threshold/drop alert rule is ever evaluated for this index
    # (e.g. NDWI measures open water, a low value is not crop stress).
    alerts: AlertDefaults | None
    stac_asset_map: dict[str, tuple[str, ...]] = field(default_factory=dict)
    # stac_asset_map: band_key → (primary_asset, fallback_asset, ...)
    name: str = ""  # human-readable name for UIs and API metadata
    description: str = ""  # what the index measures


# ── Registry ─────────────────────────────────────────────────────────

INDEX_REGISTRY: dict[str, IndexDef] = {}


def _register(idx: IndexDef) -> None:
    INDEX_REGISTRY[idx.key] = idx


_register(
    IndexDef(
        key="ndvi",
        label="NDVI",
        name="Vegetation vigor (NDVI)",
        description="Normalized Difference Vegetation Index, (B08 - B04) / (B08 + B04).",
        bands=("B04", "B08"),
        formula=_ndvi,
        colormap="rdylgn",
        rescale=(-0.2, 0.9),
        alerts=AlertDefaults(threshold=0.3, threshold_high=0.15, drop_pct=15),
        stac_asset_map={
            "B04": ("red", "B04"),
            "B08": ("nir", "B08"),
        },
    )
)

_register(
    IndexDef(
        key="evi",
        label="EVI",
        name="Enhanced vegetation index (EVI)",
        description=(
            "2.5 (B08 - B04) / (B08 + 6 B04 - 7.5 B02 + 1) on surface reflectance; "
            "less saturated than NDVI over dense canopies."
        ),
        bands=("B02", "B04", "B08"),
        formula=_evi,
        colormap="rdylgn",
        rescale=(-0.2, 0.8),
        alerts=AlertDefaults(threshold=0.2, threshold_high=0.1, drop_pct=15),
        stac_asset_map={
            "B02": ("blue", "B02"),
            "B04": ("red", "B04"),
            "B08": ("nir", "B08"),
        },
    )
)

_register(
    IndexDef(
        key="savi",
        label="SAVI",
        name="Soil-adjusted vegetation index (SAVI)",
        description="(1 + L) (B08 - B04) / (B08 + B04 + L) on surface reflectance, L = 0.5.",
        bands=("B04", "B08"),
        formula=_savi,
        colormap="rdylgn",
        rescale=(-0.2, 0.8),
        alerts=AlertDefaults(threshold=0.25, threshold_high=0.1, drop_pct=15),
        stac_asset_map={
            "B04": ("red", "B04"),
            "B08": ("nir", "B08"),
        },
    )
)

_register(
    IndexDef(
        key="ndwi",
        label="NDWI",
        name="Surface water (McFeeters NDWI)",
        description=(
            "McFeeters NDWI, (B03 - B08) / (B03 + B08): open surface water and "
            "flooding (> 0 = water). It is negative over any vegetation and is "
            "not a measure of crop water content; use NDMI for that."
        ),
        bands=("B03", "B08"),
        formula=_ndwi,
        colormap="rdbu",
        rescale=(-1.0, 1.0),
        # No alert rule: the former defaults (threshold 0.0, high -0.2, drop
        # 20 %) read a low NDWI as crop stress, which is wrong for McFeeters
        # NDWI (healthy vegetation is always < 0).
        alerts=None,
        stac_asset_map={
            "B03": ("green", "B03"),
            "B08": ("nir", "B08"),
        },
    )
)

_register(
    IndexDef(
        key="ndmi",
        label="NDMI",
        name="Vegetation moisture (NDMI)",
        description=(
            "Normalized Difference Moisture Index, (B08 - B11) / (B08 + B11), "
            "B11 = SWIR 1610 nm at 20 m: canopy water content. Typical values: "
            "bare or dry soil -0.2..0.1, stressed or sparse crop 0.1..0.3, "
            "well-watered dense canopy 0.3..0.6."
        ),
        bands=("B08", "B11"),
        formula=_ndmi,
        colormap="brbg",
        rescale=(-0.2, 0.6),
        # No alert rule until thresholds are calibrated (rules are off anyway).
        alerts=None,
        stac_asset_map={
            "B08": ("nir", "B08"),
            "B11": ("swir16", "B11"),
        },
    )
)


def get_index(key: str) -> IndexDef:
    """Look up an index definition; raises ValueError if unknown."""
    try:
        return INDEX_REGISTRY[key.lower()]
    except KeyError:
        valid = ", ".join(sorted(INDEX_REGISTRY))
        raise ValueError(f"Unknown index '{key}'. Valid: {valid}")


VALID_INDEX_KEYS = tuple(sorted(INDEX_REGISTRY.keys()))

# Derive task names from the index registry - single source of truth.
# The ndvi task lives in its own legacy module; others share the vegetation module.
INDEX_TASK_MAP: dict[str, str] = {
    key: f"app.tasks.{'ndvi' if key == 'ndvi' else 'vegetation'}.process_{key}"
    for key in INDEX_REGISTRY
}
