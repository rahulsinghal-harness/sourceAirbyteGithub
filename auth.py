"""Shared GitHub App authentication utilities.

Centralises JWT creation, installation-token exchange, and related constants
so that both the sync (requests) and async (aiohttp) clients use identical logic.
"""

import base64
import re
import time

import jwt
import requests
from cryptography.hazmat.primitives.serialization import load_der_private_key

JWT_IAT_DRIFT = 60
JWT_EXP_SECONDS = 600  # 10 min — GitHub's max for App JWTs
TOKEN_CACHE_TTL = 3500  # ~58 min, well within the 1 h GitHub limit


def _normalize_github_app_private_key_pem(private_key: str) -> str:
    """Airbyte/UI often store PEM as one line with literal \\n; PyJWT needs real newlines."""
    key = private_key.strip()
    if "\\n" in key:
        key = key.replace("\\n", "\n")
    return key


def _load_github_app_signing_key(private_key: str):
    """Return PEM string or a cryptography private key for PyJWT RS256.

    Accepts standard PEM, or bare base64-encoded PKCS#1/PKCS#8 DER (whitespace ignored).
    """
    normalized = _normalize_github_app_private_key_pem(private_key)
    if "-----BEGIN" in normalized:
        return normalized
    der_b64 = re.sub(r"\s+", "", normalized)
    pad = (-len(der_b64)) % 4
    if pad:
        der_b64 += "=" * pad
    try:
        der = base64.b64decode(der_b64, validate=True)
    except Exception as e:
        raise ValueError(
            "github_app_private_key must be PEM (with BEGIN/END lines) or valid base64 DER"
        ) from e
    return load_der_private_key(der, password=None)

INSTALLATION_TOKEN_URL = (
    "https://api.github.com/app/installations/{installation_id}/access_tokens"
)
INSTALLATION_TOKEN_HEADERS = {"Accept": "application/vnd.github+json"}


def create_app_jwt(app_id: str, private_key: str) -> str:
    """Build and sign a short-lived JWT for GitHub App authentication.

    Pure function — no IO, no HTTP.
    """
    key = _load_github_app_signing_key(private_key)
    now = int(time.time())
    payload = {
        "iat": now - JWT_IAT_DRIFT,
        "exp": now + JWT_EXP_SECONDS,
        "iss": app_id,
    }
    return jwt.encode(payload, key, algorithm="RS256")


def create_installation_token_sync(
    app_id: str, private_key: str, installation_id: str
) -> str:
    """Exchange a GitHub App JWT for an installation access token (sync/requests)."""
    jwt_token = create_app_jwt(app_id, private_key)
    resp = requests.post(
        INSTALLATION_TOKEN_URL.format(installation_id=installation_id),
        headers={
            "Authorization": f"Bearer {jwt_token}",
            **INSTALLATION_TOKEN_HEADERS,
        },
    )
    resp.raise_for_status()
    return resp.json()["token"]
