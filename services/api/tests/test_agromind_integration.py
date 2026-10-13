"""Schema-level tests for the AgroMind integration additions.

The behavioural side (idempotent create, restore, bounded backfill window)
is exercised against a live staging stack; these tests pin the request
contracts so a refactor cannot silently drop them.

Run: python -m pytest services/api/tests/test_agromind_integration.py
"""

import uuid

import pytest
from pydantic import ValidationError

from app.schemas.auth import OrgCreate
from app.schemas.farm import FarmCreate, FieldCreate

GEOM = {
    "type": "Polygon",
    "coordinates": [[[-62.80, -17.70], [-62.79, -17.70], [-62.79, -17.71], [-62.80, -17.70]]],
}


def _field(**overrides):
    base = {"farm_id": uuid.uuid4(), "name": "lote", "geom": GEOM}
    base.update(overrides)
    return FieldCreate(**base)


def test_field_create_id_and_backfill_months_are_optional():
    body = _field()
    assert body.id is None
    assert body.backfill_months is None


def test_field_create_accepts_client_id_and_window():
    fid = uuid.uuid4()
    body = _field(id=fid, backfill_months=12)
    assert body.id == fid
    assert body.backfill_months == 12


@pytest.mark.parametrize("months", [0, -1, 25, 120])
def test_field_create_rejects_out_of_range_window(months):
    with pytest.raises(ValidationError):
        _field(backfill_months=months)


def test_org_create_accepts_optional_client_id():
    assert OrgCreate(name="org").id is None
    oid = uuid.uuid4()
    assert OrgCreate(name="org", id=oid).id == oid


def test_farm_create_accepts_optional_client_id():
    assert FarmCreate(name="finca").id is None
    fid = uuid.uuid4()
    assert FarmCreate(name="finca", id=fid).id == fid
