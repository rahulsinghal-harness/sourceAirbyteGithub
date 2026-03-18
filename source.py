"""GitHub source — combines org repo/release/team streams with AI asset discovery."""

import logging
from typing import Any, List, Mapping, Tuple, Optional

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from streams.repo_details import RepoDetailsStream
from streams.releases_details import ReleasesDetailsStream
from streams.team_repositories import TeamRepositoriesStream
from streams.ai_asset import AIAssetStream, AIAssetScanCache, STREAM_TYPE_MAP

logger = logging.getLogger(__name__)


def _resolve_token(config: Mapping[str, Any]) -> str:
    """Resolve a usable token from config (token or GitHub App)."""
    auth_type = config.get("auth_type", "token")
    if auth_type == "token":
        return config["access_token"]
    # For GitHub App auth, the async client handles JWT/installation token itself.
    # For sync streams (repo_details, releases, teams), we create an installation token here.
    import time
    import jwt
    import requests

    app_id = config["github_app_id"]
    private_key = config["github_app_private_key"]
    installation_id = config["github_app_installation_id"]

    now = int(time.time())
    payload = {"iat": now - 60, "exp": now + 600, "iss": app_id}
    jwt_token = jwt.encode(payload, private_key, algorithm="RS256")

    resp = requests.post(
        f"https://api.github.com/app/installations/{installation_id}/access_tokens",
        headers={"Authorization": f"Bearer {jwt_token}", "Accept": "application/vnd.github+json"},
    )
    resp.raise_for_status()
    return resp.json()["token"]


class GitHubSource(AbstractSource):
    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        if not config.get("org_name") and not config.get("repos"):
            return False, "At least one of 'org_name' or 'repos' must be provided"
        try:
            import requests
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
                        "description": "GitHub App private key PEM (when auth_type=github_app)",
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
