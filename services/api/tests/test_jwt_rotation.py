"""JWT secret rotation: tokens signed with the previous secret stay valid.

Run: python -m pytest services/api/tests/test_jwt_rotation.py
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from jose import JWTError, jwt

from app.core.config import settings
from app.core.jwt_keys import decode_token, verification_secrets
from app.middleware.auth import get_current_user

NEW = "new-secret-for-tests-only"
OLD = "old-secret-for-tests-only"


def _tok(secret, minutes=5, **claims):
    payload = {
        "sub": str(uuid.uuid4()),
        "exp": datetime.now(timezone.utc) + timedelta(minutes=minutes),
    }
    payload.update(claims)
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture()
def secrets(monkeypatch):
    monkeypatch.setattr(settings, "openfarm_jwt_secret", NEW)
    monkeypatch.setattr(settings, "openfarm_jwt_secret_previous", "")
    return monkeypatch


def test_without_previous_only_current_is_accepted(secrets):
    assert verification_secrets() == [NEW]
    assert decode_token(_tok(NEW))["sub"]
    with pytest.raises(JWTError):
        decode_token(_tok(OLD))


def test_previous_secret_is_accepted_during_rotation(secrets):
    secrets.setattr(settings, "openfarm_jwt_secret_previous", OLD)
    assert verification_secrets() == [NEW, OLD]
    assert decode_token(_tok(OLD))["sub"]
    assert decode_token(_tok(NEW))["sub"]
    with pytest.raises(JWTError):
        decode_token(_tok("someone-else"))


def test_expired_token_is_rejected_with_either_secret(secrets):
    secrets.setattr(settings, "openfarm_jwt_secret_previous", OLD)
    for s in (NEW, OLD):
        with pytest.raises(JWTError):
            decode_token(_tok(s, minutes=-1))


def test_previous_equal_to_current_is_ignored(secrets):
    secrets.setattr(settings, "openfarm_jwt_secret_previous", NEW)
    assert verification_secrets() == [NEW]


def test_auth_dependency_accepts_previous_and_rejects_unknown(secrets):
    secrets.setattr(settings, "openfarm_jwt_secret_previous", OLD)
    user = asyncio.run(get_current_user(f"Bearer {_tok(OLD, email='svc@x')}"))
    assert user.email == "svc@x"
    with pytest.raises(HTTPException) as e:
        asyncio.run(get_current_user(f"Bearer {_tok('unknown')}"))
    assert e.value.status_code == 401


def test_share_proxy_signs_with_current_secret_and_scopes_org(secrets):
    from app.routers.share import _mint_service_jwt

    secrets.setattr(settings, "openfarm_jwt_secret_previous", OLD)
    org = uuid.uuid4()
    tok = _mint_service_jwt(org)
    payload = jwt.decode(tok, NEW, algorithms=["HS256"])  # never the previous one
    assert payload["org_ids"] == [str(org)]
