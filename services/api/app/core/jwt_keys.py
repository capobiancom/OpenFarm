"""HS256 verification against the current and (optionally) previous secret."""

from __future__ import annotations

from jose import JWTError, jwt

from app.core.config import settings


def verification_secrets() -> list[str]:
    """Secrets accepted for verification, current first."""
    secrets = [settings.openfarm_jwt_secret]
    prev = settings.openfarm_jwt_secret_previous
    if prev and prev != settings.openfarm_jwt_secret:
        secrets.append(prev)
    return secrets


def decode_token(token: str) -> dict:
    """Decode and verify ``token``; raises ``JWTError`` if no secret verifies it.

    Expiry and the other registered claims are checked by python-jose for
    each candidate. Signing always uses ``settings.openfarm_jwt_secret``.
    """
    last_error: JWTError | None = None
    for secret in verification_secrets():
        try:
            return jwt.decode(token, secret, algorithms=[settings.jwt_algorithm])
        except JWTError as e:  # wrong signature, expired, malformed
            last_error = e
    raise last_error or JWTError("no verification secret configured")
