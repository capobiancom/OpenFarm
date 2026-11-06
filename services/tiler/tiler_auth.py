"""Token verification and dataset authorization for the OpenFarm tiler.

Kept free of TiTiler imports so it can be unit tested on its own.

A tile request carries the dataset as ``?url=`` (the COG path) and a JWT.
The tiler accepts the request only when:

1. the JWT verifies with OPENFARM_JWT_SECRET, or with
   OPENFARM_JWT_SECRET_PREVIOUS during a secret rotation, and has a ``sub``;
2. the URL is an engine COG in the MinIO bucket,
   ``s3://<bucket>/cogs/<org-uuid>/<field-uuid>/<YYYY-MM-DD>/<index>.tif``
   (or the same path under ``/vsis3/``). Anything else (http, file paths,
   other buckets, ``..``) is refused before GDAL opens it;
3. when the token carries an ``org_ids`` claim, the COG's org is in it.
   With TILER_REQUIRE_ORG_CLAIM=true a token without the claim is refused.

The tiler has no database: it cannot check org membership itself. Tokens
minted by AgroMind's backend use a service user that belongs to every
engine org, so tenant isolation for AgroMind users is enforced in Laravel
(it builds the COG path from the lot the user is allowed to see).
"""

from __future__ import annotations

import os
import re

from jose import JWTError, jwt


class AuthError(Exception):
    def __init__(self, status_code: int, detail: str):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def _env_bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes")


def verification_secrets() -> list[str]:
    current = os.environ.get("OPENFARM_JWT_SECRET", "change-me")
    previous = os.environ.get("OPENFARM_JWT_SECRET_PREVIOUS", "")
    out = [current]
    if previous and previous != current:
        out.append(previous)
    return out


def decode_token(raw_token: str) -> dict:
    algorithm = os.environ.get("JWT_ALGORITHM", "HS256")
    for secret in verification_secrets():
        try:
            return jwt.decode(raw_token, secret, algorithms=[algorithm])
        except JWTError:
            continue
    raise AuthError(401, "Invalid or expired token")


_UUID = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"


def _cog_pattern(bucket: str) -> re.Pattern:
    b = re.escape(bucket)
    return re.compile(
        rf"^(?:s3://|/vsis3/){b}/cogs/(?P<org>{_UUID})/(?P<field>{_UUID})/"
        r"(?P<date>\d{4}-\d{2}-\d{2})/(?P<index>[a-z0-9_]{1,32})\.tif$"
    )


def parse_cog_url(url: str) -> dict:
    """Return the path parts of an engine COG URL or raise AuthError(403)."""
    bucket = os.environ.get("MINIO_BUCKET", "openfarm")
    m = _cog_pattern(bucket).match(url or "")
    if not m:
        raise AuthError(403, "Dataset not allowed")
    return m.groupdict()


def authorize(raw_token: str | None, url: str) -> dict:
    """Verify the token and the dataset URL; returns the token payload."""
    if not raw_token:
        raise AuthError(401, "Missing authentication token")
    payload = decode_token(raw_token)
    if not payload.get("sub"):
        raise AuthError(401, "Token missing subject")

    parts = parse_cog_url(url)
    org_ids = payload.get("org_ids")
    if org_ids is None:
        if _env_bool("TILER_REQUIRE_ORG_CLAIM"):
            raise AuthError(403, "Token is not scoped to an org")
    else:
        allowed = {
            str(o).lower()
            for o in (org_ids if isinstance(org_ids, list) else [org_ids])
        }
        if parts["org"].lower() not in allowed:
            raise AuthError(403, "Dataset not allowed for this token")
    return payload
