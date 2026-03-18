"""Shared GitHub App authentication utilities.

Centralises JWT creation, installation-token exchange, and related constants
so that both the sync (requests) and async (aiohttp) clients use identical logic.
"""

import time

import jwt
import requests

JWT_IAT_DRIFT = 60
JWT_EXP_SECONDS = 600  # 10 min — GitHub's max for App JWTs
TOKEN_CACHE_TTL = 3500  # ~58 min, well within the 1 h GitHub limit

INSTALLATION_TOKEN_URL = (
    "https://api.github.com/app/installations/{installation_id}/access_tokens"
)
INSTALLATION_TOKEN_HEADERS = {"Accept": "application/vnd.github+json"}


def create_app_jwt(app_id: str, private_key: str) -> str:
    """Build and sign a short-lived JWT for GitHub App authentication.

    Pure function — no IO, no HTTP.
    """
    now = int(time.time())
    payload = {
        "iat": now - JWT_IAT_DRIFT,
        "exp": now + JWT_EXP_SECONDS,
        "iss": app_id,
    }
    return jwt.encode(payload, private_key, algorithm="RS256")


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
