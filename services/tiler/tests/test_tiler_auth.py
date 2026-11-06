"""Tiler authorization: secrets (incl. rotation), COG allowlist, org scope.

Run from services/tiler: python -m pytest tests
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from jose import jwt

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tiler_auth import AuthError, authorize, parse_cog_url  # noqa: E402

NEW, OLD = "tiler-new-test", "tiler-old-test"
ORG, OTHER = str(uuid.uuid4()), str(uuid.uuid4())
FIELD = str(uuid.uuid4())
COG = f"s3://openfarm/cogs/{ORG}/{FIELD}/2026-10-21/ndvi.tif"


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("OPENFARM_JWT_SECRET", NEW)
    monkeypatch.delenv("OPENFARM_JWT_SECRET_PREVIOUS", raising=False)
    monkeypatch.delenv("TILER_REQUIRE_ORG_CLAIM", raising=False)
    monkeypatch.setenv("MINIO_BUCKET", "openfarm")
    return monkeypatch


def tok(secret=NEW, minutes=5, **claims):
    p = {"sub": "svc", "exp": datetime.now(timezone.utc) + timedelta(minutes=minutes)}
    p.update(claims)
    return jwt.encode(p, secret, algorithm="HS256")


def _status(fn):
    with pytest.raises(AuthError) as e:
        fn()
    return e.value.status_code


def test_valid_token_and_engine_cog_pass():
    assert authorize(tok(), COG)["sub"] == "svc"
    assert authorize(tok(), COG.replace("s3://", "/vsis3/"))["sub"] == "svc"


def test_missing_bad_or_expired_token_is_401():
    assert _status(lambda: authorize(None, COG)) == 401
    assert _status(lambda: authorize(tok("wrong"), COG)) == 401
    assert _status(lambda: authorize(tok(minutes=-1), COG)) == 401
    assert _status(lambda: authorize(tok(sub=""), COG)) == 401


def test_previous_secret_accepted_only_when_configured(env):
    assert _status(lambda: authorize(tok(OLD), COG)) == 401
    env.setenv("OPENFARM_JWT_SECRET_PREVIOUS", OLD)
    assert authorize(tok(OLD), COG)["sub"] == "svc"
    assert authorize(tok(NEW), COG)["sub"] == "svc"


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/x.tif",
        "/vsicurl/http://169.254.169.254/latest",
        "file:///etc/passwd",
        "/etc/passwd",
        f"s3://other-bucket/cogs/{ORG}/{FIELD}/2026-10-21/ndvi.tif",
        f"s3://openfarm/uploads/{ORG}/{FIELD}/2026-10-21/ndvi.tif",
        f"s3://openfarm/cogs/{ORG}/../{FIELD}/2026-10-21/ndvi.tif",
        f"s3://openfarm/cogs/{ORG}/{FIELD}/2026-10-21/ndvi.tif?x=1",
        f"s3://openfarm/cogs/not-a-uuid/{FIELD}/2026-10-21/ndvi.tif",
        "",
    ],
)
def test_non_engine_urls_are_403(url):
    assert _status(lambda: authorize(tok(), url)) == 403


def test_org_claim_restricts_to_listed_orgs():
    assert authorize(tok(org_ids=[ORG]), COG)
    assert _status(lambda: authorize(tok(org_ids=[OTHER]), COG)) == 403


def test_org_claim_can_be_required(env):
    env.setenv("TILER_REQUIRE_ORG_CLAIM", "true")
    assert _status(lambda: authorize(tok(), COG)) == 403
    assert authorize(tok(org_ids=[ORG]), COG)


def test_parse_cog_url_parts():
    parts = parse_cog_url(COG)
    assert parts == {"org": ORG, "field": FIELD, "date": "2026-10-21", "index": "ndvi"}
