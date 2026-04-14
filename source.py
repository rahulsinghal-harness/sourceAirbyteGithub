"""GitHub source — combines org repo/release/team streams with AI asset discovery."""

import logging
import time
from typing import Any, List, Mapping, Tuple, Optional

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from auth import create_installation_token_sync, TOKEN_CACHE_TTL
from streams.repo_details import RepoDetailsStream
from streams.releases_details import ReleasesDetailsStream
from streams.team_repositories import TeamRepositoriesStream
from streams.issues import IssuesStream
from streams.pull_requests import PullRequestsStream
from streams.commits import CommitsStream
from streams.repo_stats import RepoStatsStream
from streams.ai_asset import AIAssetStream, AIAssetScanCache, STREAM_TYPE_MAP

logger = logging.getLogger(__name__)


class _InstallationTokenCache:
    """Caches a GitHub App installation token so multiple sync streams share one."""

    def __init__(self):
        self._token: str | None = None
        self._expires_at: float = 0

    def get_token(self, app_id: str, private_key: str, installation_id: str) -> str:
        if self._token and time.time() < self._expires_at:
            return self._token

        self._token = create_installation_token_sync(app_id, private_key, installation_id)
        self._expires_at = time.time() + TOKEN_CACHE_TTL
        logger.info("Created new GitHub App installation token (expires in ~58m)")
        return self._token


_token_cache = _InstallationTokenCache()


def _resolve_token(config: Mapping[str, Any]) -> str:
    """Resolve a usable token from config (token or GitHub App).

    For GitHub App auth, the installation token is cached and reused across
    all sync streams and check_connection calls within the same process.
    """
    auth_type = config.get("auth_type", "token")
    if auth_type == "token":
        return config["access_token"]

    return _token_cache.get_token(
        app_id=config["github_app_id"],
        private_key=config["github_app_private_key"],
        installation_id=config["github_app_installation_id"],
    )


class GitHubSource(AbstractSource):
    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        if not config.get("org_name") and not config.get("repos"):
            return False, "At least one of 'org_name' or 'repos' must be provided"
        try:
            token = _resolve_token(config)
            resp = requests.post(
                "https://api.github.com/graphql",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"query": "query { viewer { login } }"},
            )
            resp.raise_for_status()
            body = resp.json()
            if "errors" in body:
                return False, f"GraphQL errors: {body['errors']}"
            login = body.get("data", {}).get("viewer", {}).get("login")
            logger.info("Connection check OK — authenticated as %s", login)
            return True, None
        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        result: List[Stream] = []

        if config.get("org_name"):
            result.extend([
                RepoDetailsStream(config=config),
                ReleasesDetailsStream(config=config),
                TeamRepositoriesStream(config=config),
                IssuesStream(config=config),
                PullRequestsStream(config=config),
                CommitsStream(config=config),
                RepoStatsStream(config=config),
            ])

        scan_cache = AIAssetScanCache()
        result.extend(
            AIAssetStream(asset_type=asset_type, config=config, scan_cache=scan_cache)
            for asset_type in STREAM_TYPE_MAP.values()
        )
        return result

    def spec(self, logger: logging.Logger) -> Any:
        from airbyte_cdk.models import ConnectorSpecification
        return ConnectorSpecification(
            connectionSpecification={
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
                "required": [],
                "properties": {
                    "auth_type": {
                        "type": "string",
                        "enum": ["token", "github_app"],
                        "default": "token",
                    },
                    "access_token": {
                        "type": "string",
                        "airbyte_secret": True,
                        "description": "Personal access token (when auth_type=token)",
                    },
                    "github_app_id": {
                        "type": "string",
                        "description": "GitHub App ID (when auth_type=github_app)",
                    },
                    "github_app_private_key": {
                        "type": "string",
                        "airbyte_secret": True,
                        "description": (
                            "GitHub App private key: full PEM from the downloaded .pem, or bare base64 "
                            "DER (PKCS#1/PKCS#8) with optional whitespace. Not the in-app 'Public key'."
                        ),
                    },
                    "github_app_installation_id": {
                        "type": "string",
                        "description": "GitHub App installation ID (when auth_type=github_app)",
                    },
                    "org_name": {
                        "type": "string",
                        "description": "GitHub organization — scans all non-archived repos",
                    },
                    "repos": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Repositories to scan (owner/repo format)",
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Only include data on or after this date (ISO 8601). Defaults to epoch.",
                        "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$",
                        "examples": ["2025-01-01T00:00:00Z"],
                    },
                },
                "additionalProperties": True,
            }
        )
