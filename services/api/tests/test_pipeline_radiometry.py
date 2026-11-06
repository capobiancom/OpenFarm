"""Pipeline 3.0.0: reflectance, nodata, SCL masking, quality_score, NDMI.

Pure numpy / in-memory rasterio: no database, no network.

Run: python -m pytest services/api/tests/test_pipeline_radiometry.py
"""

from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds, from_origin

import app.tasks.pipeline as pipeline
from app.tasks.indices import INDEX_REGISTRY, INDEX_TASK_MAP, get_index


# ── scale / offset from STAC metadata ────────────────────────────────


RB = {"nodata": 0, "data_type": "uint16", "scale": 0.0001, "offset": -0.1}


def test_offset_not_applied_twice_when_provider_applied_it():
    props = {"s2:processing_baseline": "05.11", "earthsearch:boa_offset_applied": True}
    assert pipeline.reflectance_params(props, RB) == (
        0.0001,
        0.0,
        "boa_offset_applied_by_provider",
    )


def test_old_baseline_uses_raster_bands_offset():
    props = {"s2:processing_baseline": "03.01", "earthsearch:boa_offset_applied": False}
    assert pipeline.reflectance_params(props, {"scale": 0.0001, "offset": 0}) == (
        0.0001,
        0.0,
        "baseline_lt_04_raster_bands",
    )


@pytest.mark.parametrize(
    "policy, expected",
    [
        ("assume_applied", (0.0001, 0.0, "ambiguous_assumed_applied")),
        ("apply_stac_offset", (0.0001, -0.1, "ambiguous_applied_raster_bands_offset")),
        ("skip", None),
    ],
)
def test_ambiguous_items_follow_policy(monkeypatch, policy, expected):
    monkeypatch.setattr(pipeline, "S2_AMBIGUOUS_OFFSET_POLICY", policy)
    props = {"s2:processing_baseline": "05.11", "earthsearch:boa_offset_applied": False}
    assert pipeline.reflectance_params(props, RB) == expected


def test_missing_scale_defaults_to_l2a_quantification():
    props = {"s2:processing_baseline": "05.11", "earthsearch:boa_offset_applied": True}
    assert pipeline.reflectance_params(props, {})[0] == pytest.approx(1e-4)


def test_to_reflectance_scales_and_keeps_nan():
    dn = np.array([0.0, 1000.0, 2500.0, np.nan], dtype=np.float32)
    out = pipeline.to_reflectance(dn, 1e-4, 0.0)
    assert out[:3] == pytest.approx([0.0, 0.1, 0.25])
    assert np.isnan(out[3])
    assert pipeline.to_reflectance(np.array([2000.0]), 1e-4, -0.1)[0] == pytest.approx(
        0.1
    )


# ── formulas on reflectance ──────────────────────────────────────────


def _bands(**v):
    return {k: np.array([x], dtype=np.float32) for k, x in v.items()}


def test_savi_and_evi_are_physical_on_reflectance():
    # Moderate canopy: red 0.05, nir 0.30, blue 0.03
    b = _bands(B02=0.03, B04=0.05, B08=0.30)
    ndvi = get_index("ndvi").formula(b)[0]
    savi = get_index("savi").formula(b)[0]
    evi = get_index("evi").formula(b)[0]
    assert ndvi == pytest.approx(0.25 / 0.35)
    assert savi == pytest.approx(1.5 * 0.25 / 0.85)
    assert evi == pytest.approx(2.5 * 0.25 / (0.30 + 0.30 - 0.225 + 1.0))
    assert savi < ndvi < 1.5 * ndvi
    assert evi < ndvi


def test_raw_dn_reproduces_the_old_bias():
    """On DN the +L and +1 constants vanish: SAVI = 1.5 NDVI, EVI saturates."""
    dn = _bands(B02=300.0, B04=500.0, B08=3000.0)
    ndvi = get_index("ndvi").formula(dn)[0]
    assert get_index("savi").formula(dn)[0] == pytest.approx(1.5 * ndvi, rel=1e-3)
    assert get_index("evi").formula(dn)[0] == pytest.approx(1.0)  # clipped


def test_ndmi_formula_and_registration():
    idx = get_index("ndmi")
    assert idx.label == "NDMI" and idx.bands == ("B08", "B11")
    assert idx.stac_asset_map["B11"][0] == "swir16"
    assert idx.rescale == (-0.2, 0.6)
    assert idx.alerts is None
    v = idx.formula(_bands(B08=0.30, B11=0.15))[0]
    assert v == pytest.approx(0.15 / 0.45)
    assert INDEX_TASK_MAP["ndmi"] == "app.tasks.vegetation.process_ndmi"
    assert "ndmi" in INDEX_REGISTRY


def test_ndmi_is_accepted_by_the_jobs_endpoint_schema():
    from app.schemas.monitoring import JobCreateIndex

    body = JobCreateIndex(
        index_type="ndmi", date_from="2026-01-01", date_to="2026-02-01"
    )
    assert body.index_type == "ndmi"


def test_ndwi_is_surface_water_and_has_no_alert_rule():
    idx = get_index("ndwi")
    assert idx.label == "NDWI"  # stored layer_type unchanged
    assert "McFeeters" in idx.name and "surface water" in idx.name.lower()
    assert idx.alerts is None


def test_run_alerts_never_evaluates_ndwi_even_if_rules_enabled(monkeypatch):
    from app.core.config import settings

    monkeypatch.setattr(settings, "openfarm_alert_rules_enabled", True)

    class Boom:
        def __getattr__(self, name):
            raise AssertionError("session must not be used")

    for key in ("ndwi", "ndmi"):
        pipeline.run_alerts(
            Boom(), None, None, None, {"mean": -0.9}, [0.5, 0.5, 0.5], get_index(key)
        )


def test_new_task_is_registered_with_celery():
    import app.tasks.ndvi  # noqa: F401
    import app.tasks.reprocess  # noqa: F401
    import app.tasks.vegetation  # noqa: F401
    from app.worker import celery_app

    for name in INDEX_TASK_MAP.values():
        assert name in celery_app.tasks
    assert "app.tasks.reprocess.reprocess_stored_layers" in celery_app.tasks


# ── nodata as NaN, nearest validity mask ─────────────────────────────


def _write_tif(path, data, transform, crs="EPSG:32720", nodata=0):
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        width=data.shape[1],
        height=data.shape[0],
        count=1,
        dtype=data.dtype,
        crs=crs,
        transform=transform,
        nodata=nodata,
    ) as dst:
        dst.write(data, 1)


def _grid_over(src_transform, shape, crs="EPSG:32720"):
    from rasterio.warp import transform_bounds

    left, top = src_transform.c, src_transform.f
    right = left + src_transform.a * shape[1]
    bottom = top + src_transform.e * shape[0]
    b = transform_bounds(crs, "EPSG:4326", left, bottom, right, top)
    # inset so the target grid lies inside the source raster
    dx, dy = (b[2] - b[0]) * 0.05, (b[3] - b[1]) * 0.05
    bounds = (b[0] + dx, b[1] + dy, b[2] - dx, b[3] - dy)
    tshape = (60, 60)
    return bounds, tshape, from_bounds(*bounds, tshape[1], tshape[0])


def test_nodata_becomes_nan_and_never_leaks_into_valid_pixels(tmp_path):
    data = np.full((80, 80), 2000, dtype=np.uint16)
    data[:, :40] = 0  # left half: no data (outside the scene footprint)
    tr = from_origin(600000, 8000000, 10, 10)
    path = tmp_path / "b.tif"
    _write_tif(path, data, tr)
    bounds, tshape, ttr = _grid_over(tr, data.shape)

    values, valid = pipeline.read_band_windowed(str(path), bounds, tshape, ttr)

    assert valid.any() and (~valid).any()
    assert np.all(np.isnan(values[~valid]))
    # Every valid pixel is exactly the valid DN: bilinear never mixed in a 0.
    assert np.allclose(values[valid], 2000.0)
    assert not np.any((values > 0) & (values < 2000))


def test_categorical_band_uses_nearest(tmp_path):
    scl = np.zeros((40, 40), dtype=np.uint8)
    scl[:, :20] = 4
    scl[:, 20:] = 9
    tr = from_origin(600000, 8000000, 20, 20)
    path = tmp_path / "scl.tif"
    _write_tif(path, scl, tr)
    bounds, tshape, ttr = _grid_over(tr, scl.shape)

    out, valid = pipeline.read_band_windowed(
        str(path), bounds, tshape, ttr, categorical=True
    )
    # no interpolated classes; corners outside the (rotated) footprint are
    # nodata (0) and flagged invalid
    assert set(np.unique(out[valid])) == {4, 9}
    assert np.all(out[~valid] == 0)
    assert valid.mean() > 0.9


# ── SCL masking and quality ──────────────────────────────────────────


def test_scl_masked_classes_are_the_documented_ones():
    assert pipeline.SCL_MASKED_CLASSES == {0, 1, 3, 8, 9, 10, 11}
    scl = np.arange(12, dtype=np.uint8)
    m = pipeline.scl_masks(scl, scl != 0)
    assert list(np.where(m["clear"])[0]) == [2, 4, 5, 6, 7]
    assert list(np.where(m["cloud"])[0]) == [3, 8, 9, 10]
    assert list(np.where(m["other"])[0]) == [1, 11]
    assert list(np.where(m["nodata"])[0]) == [0]


def _fake_scene():
    meta = {
        "scale": 1e-4,
        "offset": 0.0,
        "source": "boa_offset_applied_by_provider",
        "nodata": 0,
    }
    return {
        "id": "S2X_TEST",
        "band_hrefs": {"B04": "red", "B08": "nir"},
        "scl_href": "scl",
        "reflectance": {"B04": meta, "B08": meta},
    }


def test_read_scene_bands_masks_clouds_and_nodata(monkeypatch):
    shape = (4, 5)
    # columns: 0 nodata in red, 1 cloud (9), 2 shadow (3), 3-4 vegetation (4)
    scl = np.array([[4, 9, 3, 4, 4]] * 4, dtype=np.uint8)
    red = np.full(shape, 500.0, dtype=np.float32)
    red[:, 0] = np.nan
    nir = np.full(shape, 3000.0, dtype=np.float32)

    def fake(
        href, bounds, target_shape, target_transform, *, categorical=False, nodata=0
    ):
        if categorical:
            return scl, scl != 0
        arr = {"red": red, "nir": nir}[href].copy()
        return arr, np.isfinite(arr)

    monkeypatch.setattr(pipeline, "read_band_windowed", fake)
    bands, masks = pipeline.read_scene_bands(_fake_scene(), None, shape, None)

    assert np.all(np.isnan(bands["B04"][:, :3])) and np.all(
        np.isnan(bands["B08"][:, :3])
    )
    assert bands["B04"][0, 3] == pytest.approx(0.05)
    assert bands["B08"][0, 4] == pytest.approx(0.30)
    assert masks["band_nodata"][:, 0].all() and not masks["band_nodata"][:, 1:].any()
    assert masks["cloud"][:, 1:3].all()
    assert masks["clear"][:, 3:].all() and not masks["clear"][:, :3].any()


def test_quality_score_is_clear_fraction_inside_polygon():
    shape = (4, 5)
    field = np.zeros(shape, dtype=bool)
    field[:, 1:] = True  # 16 pixels in the polygon; column 0 is outside
    band_nodata = np.zeros(shape, dtype=bool)
    band_nodata[0, :] = True  # 4 field pixels nodata
    cloud = np.zeros(shape, dtype=bool)
    cloud[1, :] = True  # 4 field pixels cloud
    other = np.zeros(shape, dtype=bool)
    clear = ~(band_nodata | cloud | other)
    masks = {
        "band_nodata": band_nodata,
        "scl_nodata": np.zeros(shape, dtype=bool),
        "cloud": cloud,
        "other": other,
        "clear": clear,
    }
    index = np.where(clear & field, 0.6, np.nan).astype(np.float32)

    q = pipeline.field_quality(masks, field, index)
    assert q["field_pixels"] == 16
    assert q["field_nodata_fraction"] == pytest.approx(0.25)
    assert q["field_cloud_fraction"] == pytest.approx(0.25)
    assert q["quality_score"] == pytest.approx(0.5)

    stats = pipeline.compute_zonal_stats(index, field)
    assert stats["quality_score"] == pytest.approx(0.5)
    assert stats["mean"] == pytest.approx(0.6)


def test_quality_score_ignores_bbox_pixels_outside_polygon():
    field = np.zeros((10, 10), dtype=bool)
    field[2:8, 2:8] = True
    index = np.where(field, 0.5, np.nan).astype(np.float32)
    assert pipeline.compute_zonal_stats(index, field)["quality_score"] == 1.0
    # the old definition (bounding box) would have reported 0.36
    assert pipeline.compute_zonal_stats(index)["quality_score"] == pytest.approx(0.36)


# ── STAC item → scene ────────────────────────────────────────────────


def _asset(href, **rb):
    return SimpleNamespace(href=href, extra_fields={"raster:bands": [rb]} if rb else {})


def _item(assets, **props):
    base = {
        "s2:processing_baseline": "05.11",
        "earthsearch:boa_offset_applied": True,
        "eo:cloud_cover": 3.2,
    }
    base.update(props)
    from datetime import datetime, timezone

    return SimpleNamespace(
        id="S2B_20KMG_20261021_0_L2A",
        properties=base,
        assets=assets,
        datetime=datetime(2026, 10, 21, 14, 37, tzinfo=timezone.utc),
    )


def test_scene_from_item_records_reflectance_and_scl():
    item = _item(
        {
            "nir": _asset("https://x/B08.tif", **RB),
            "swir16": _asset("https://x/B11.tif", **RB),
            "scl": _asset("https://x/SCL.tif", nodata=0, data_type="uint8"),
        }
    )
    sc = pipeline.scene_from_item(item, get_index("ndmi"))
    assert sc["band_hrefs"] == {"B08": "https://x/B08.tif", "B11": "https://x/B11.tif"}
    assert sc["scl_href"] == "https://x/SCL.tif"
    assert sc["reflectance"]["B11"] == {
        "scale": 0.0001,
        "offset": 0.0,
        "source": "boa_offset_applied_by_provider",
        "nodata": 0,
    }
    assert str(sc["date"]) == "2026-10-21"


def test_scene_without_scl_is_skipped():
    item = _item({"nir": _asset("n", **RB), "red": _asset("r", **RB)})
    assert pipeline.scene_from_item(item, get_index("ndvi")) is None
