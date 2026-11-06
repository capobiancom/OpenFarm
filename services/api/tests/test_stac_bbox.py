"""STAC privacy: outward-rounded bbox, local footprint filter, weekly pick.

No network: the STAC client is replaced by a fake that records the search.

Run: python -m pytest services/api/tests/test_stac_bbox.py
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest
from shapely.geometry import box, mapping, shape

import app.tasks.pipeline as pipeline
from app.core.config import Settings, settings
from app.tasks.indices import get_index

# Real field shapes (Bolivia: negative lon and lat).
LAS_MINAS = {
    "type": "MultiPolygon",
    "coordinates": [[[
        [-62.394791, -18.114692], [-62.374363, -18.142426],
        [-62.453327, -18.190705], [-62.474613, -18.163305],
        [-62.394791, -18.114692],
    ]]],
}
LOTE_NORTE = {
    "type": "MultiPolygon",
    "coordinates": [[[
        [-63.15, -17.2], [-63.13872, -17.2], [-63.13872, -17.19096],
        [-63.15, -17.19096], [-63.15, -17.2],
    ]]],
}


# ── outward rounding ─────────────────────────────────────────────────


def test_outward_bbox_negative_coordinates():
    g = shape(LAS_MINAS)
    assert pipeline.outward_bbox(g.bounds, 2) == [-62.48, -18.2, -62.37, -18.11]


def test_outward_bbox_value_on_grid_is_kept():
    # -63.15 * 100 is -6315.000000000001 in floats: must stay -63.15.
    g = shape(LOTE_NORTE)
    assert pipeline.outward_bbox(g.bounds, 2) == [-63.15, -17.2, -63.13, -17.19]


@pytest.mark.parametrize("decimals", [0, 1, 2, 3, 4])
@pytest.mark.parametrize("geojson", [LAS_MINAS, LOTE_NORTE])
def test_outward_bbox_contains_polygon(geojson, decimals):
    g = shape(geojson)
    bb = pipeline.outward_bbox(g.bounds, decimals)
    assert box(*bb).contains(g)
    assert bb[0] <= g.bounds[0] and bb[1] <= g.bounds[1]
    assert bb[2] >= g.bounds[2] and bb[3] >= g.bounds[3]
    # no more precision than requested
    for v in bb:
        assert round(v, decimals) == v


@pytest.mark.parametrize(
    "bounds",
    [
        (-63.141234567, -17.198765432, -63.139876543, -17.191234567),
        (-0.001, -0.001, 0.001, 0.001),
        (62.123456789, 17.987654321, 62.5, 18.0000001),
        (-62.999999999, -18.000000001, -62.990000001, -17.99),
    ],
)
def test_outward_bbox_each_side_is_outward(bounds):
    bb = pipeline.outward_bbox(bounds, 2)
    assert bb[0] <= bounds[0] and bb[1] <= bounds[1]
    assert bb[2] >= bounds[2] and bb[3] >= bounds[3]
    assert bb[2] - bb[0] <= (bounds[2] - bounds[0]) + 0.02 + 1e-9


def test_outward_bbox_clamped_to_world_and_validated():
    assert pipeline.outward_bbox((-179.999, -89.999, 179.999, 89.999), 0) == [
        -180.0, -90.0, 180.0, 90.0,
    ]
    with pytest.raises(ValueError):
        pipeline.outward_bbox((0, 0, 1, 1), 7)
    with pytest.raises(ValueError):
        pipeline.outward_bbox((1, 0, 0, 1), 2)


def test_setting_default_and_bounds():
    assert Settings().stac_bbox_decimals == 2
    with pytest.raises(Exception):
        Settings(stac_bbox_decimals=9)


# ── local footprint filter and weekly selection ──────────────────────


def _item(item_id, day, cloud, footprint, **extra_props):
    return SimpleNamespace(
        id=item_id,
        geometry=mapping(footprint) if footprint is not None else None,
        datetime=datetime(day.year, day.month, day.day, 14, tzinfo=timezone.utc),
        properties={"eo:cloud_cover": cloud, **extra_props},
        assets={},
    )


FIELD = box(-62.47, -18.19, -62.37, -18.11)
FULL = box(-63.0, -18.5, -62.0, -17.5)  # covers the field
PART = box(-62.45, -18.5, -62.0, -17.5)  # covers 80 % of it
APART = box(-62.369, -18.5, -62.0, -17.5)  # in the bbox, misses the field


def test_footprint_coverage():
    assert pipeline.footprint_coverage(mapping(FULL), FIELD) == pytest.approx(1.0)
    assert pipeline.footprint_coverage(mapping(PART), FIELD) == pytest.approx(0.8)
    assert pipeline.footprint_coverage(mapping(APART), FIELD) == 0.0
    assert pipeline.footprint_coverage(None, FIELD) == 0.0


def test_items_not_touching_the_field_are_dropped():
    items = [_item("APART", date(2026, 5, 4), 0.0, APART), _item("NOGEOM", date(2026, 5, 4), 0.0, None)]
    assert pipeline.select_weekly_items(items, FIELD, date(2026, 5, 1)) == []


def test_week_prefers_coverage_over_cloud():
    items = [
        _item("KNF", date(2026, 5, 1), 9.51, PART),
        _item("KNE", date(2026, 5, 1), 19.56, FULL),
        _item("APART", date(2026, 5, 1), 0.0, APART),
    ]
    (pick,) = pipeline.select_weekly_items(items, FIELD, date(2026, 5, 1))
    assert pick["item"].id == "KNE"
    assert pick["coverage"] == pytest.approx(1.0)


def test_equal_coverage_falls_back_to_cloud_then_date_then_id():
    w = [
        _item("B", date(2026, 6, 3), 5.0, FULL),
        _item("A", date(2026, 6, 4), 1.0, FULL),
        _item("C", date(2026, 6, 5), 1.0, FULL),
    ]
    (pick,) = pipeline.select_weekly_items(w, FIELD, date(2026, 6, 1))
    assert pick["item"].id == "A"
    tie = [_item("Z", date(2026, 6, 4), 1.0, FULL), _item("Y", date(2026, 6, 4), 1.0, FULL)]
    assert pipeline.select_weekly_items(tie, FIELD, date(2026, 6, 1))[0]["item"].id == "Y"


def test_partial_item_is_kept_when_it_is_the_only_one():
    items = [_item("KNF", date(2026, 5, 11), 18.21, PART)]
    (pick,) = pipeline.select_weekly_items(items, FIELD, date(2026, 5, 1))
    assert pick["item"].id == "KNF" and pick["coverage"] == pytest.approx(0.8)


def test_one_item_per_iso_week_sorted():
    items = [
        _item("W2", date(2026, 1, 13), 3.0, FULL),
        _item("W1a", date(2026, 1, 6), 3.0, FULL),
        _item("W1b", date(2026, 1, 8), 1.0, FULL),
        _item("Wnull", date(2026, 1, 21), None, FULL),
    ]
    picks = pipeline.select_weekly_items(items, FIELD, date(2026, 1, 1))
    assert [p["item"].id for p in picks] == ["W1b", "W2", "Wnull"]
    assert picks[-1]["cloud"] == 100


# ── search_scenes sends only the rounded bbox ────────────────────────


class _FakeCatalog:
    def __init__(self, items):
        self._items = items
        self.kwargs = None

    def search(self, **kwargs):
        self.kwargs = kwargs
        return SimpleNamespace(items=lambda: iter(self._items))


def test_search_scenes_sends_bbox_not_polygon(monkeypatch):
    items = [
        _item("KNF", date(2026, 6, 5), 0.0, PART),
        _item("KNE", date(2026, 6, 5), 5.48, FULL),
        _item("APART", date(2026, 6, 5), 0.0, APART),
    ]
    fake = _FakeCatalog(items)
    monkeypatch.setattr(pipeline.STACClient, "open", staticmethod(lambda url: fake))
    monkeypatch.setattr(
        pipeline,
        "scene_from_item",
        lambda item, index_def, item_date=None: {"id": item.id, "date": item_date},
    )
    scenes = pipeline.search_scenes(
        mapping(FIELD), date(2026, 6, 1), date(2026, 6, 30), get_index("ndvi")
    )
    assert "intersects" not in fake.kwargs
    assert fake.kwargs["bbox"] == [-62.47, -18.19, -62.37, -18.11]
    assert fake.kwargs["max_items"] == pipeline.STAC_MAX_ITEMS
    assert [s["id"] for s in scenes] == ["KNE"]
    assert scenes[0]["footprint_coverage"] == 1.0
    assert scenes[0]["cloud_cover"] == 5.48


def test_search_scenes_respects_configured_decimals(monkeypatch):
    fake = _FakeCatalog([])
    monkeypatch.setattr(pipeline.STACClient, "open", staticmethod(lambda url: fake))
    monkeypatch.setattr(settings, "stac_bbox_decimals", 1)
    pipeline.search_scenes(LAS_MINAS, date(2026, 6, 1), date(2026, 6, 30), get_index("ndvi"))
    assert fake.kwargs["bbox"] == [-62.5, -18.2, -62.3, -18.1]
