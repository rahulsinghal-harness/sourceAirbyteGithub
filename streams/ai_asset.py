"""AI asset discovery streams — one Stream subclass per asset type."""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Iterable, List, Mapping, Optional

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream

from ai_asset_auto_discovery.github import GitHubGraphQLClient
from ai_asset_auto_discovery.scanner import scan_repo

logger = logging.getLogger(__name__)

STREAM_TYPE_MAP = {
    "aiasset_plugin": "plugin",
    "aiasset_skill": "skill",
    "aiasset_agent": "agent",
    "aiasset_command": "command",
}

_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "asset_id": {"type": "string"},
        "name": {"type": "string"},
        "type": {"type": "string"},
        "version": {"type": ["string", "null"]},
        "provider": {"type": "string"},
        "category": {"type": "string"},
        "confidence": {"type": "number"},
        "source_repo": {"type": "string"},
        "source_file": {"type": "string"},
        "source_location": {"type": ["string", "null"]},
        "discovered_at": {"type": "string"},
        "description": {"type": ["string", "null"]},
        "asset_class": {"type": "string"},
        "parent_asset_id": {"type": ["string", "null"]},
        "relationship": {"type": "string"},
        "external_ref": {"type": ["string", "null"]},
        "author": {"type": ["string", "null"]},
        "license": {"type": ["string", "null"]},
    },
    "required": ["asset_id", "name", "type", "provider", "category"],
    "additionalProperties": True,
}


class AIAssetScanCache:
    """Runs the scan once and caches results for all 4 stream instances."""

    def __init__(self):
        self._assets = None

    def get_assets(self, config: Mapping[str, Any]) -> list:
        if self._assets is not None:
            return self._assets
        self._assets = asyncio.run(self._scan(config))
        return self._assets

    async def _scan(self, config: Mapping[str, Any]) -> list:
        auth_type = config.get("auth_type", "token")
        if auth_type == "github_app":
            client = GitHubGraphQLClient(
                app_id=config["github_app_id"],
                private_key=config["github_app_private_key"],
                installation_id=config["github_app_installation_id"],
            )
        else:
            client = GitHubGraphQLClient(token=config["access_token"])
        try:
            org = config["org_name"]
            repos_config = config.get("repos")

            if repos_config:
                repo_files_list = await client.scan_repos(repos_config)
            else:
                repo_files_list = await client.scan_org(org)

            all_assets = []
            for repo_files in repo_files_list:
                repo_name = f"{repo_files.owner}/{repo_files.name}"
                assets = scan_repo(repo_name, repo_files.files, repo_files.branch)
                all_assets.extend(assets)
                logger.info("%s: %d assets found", repo_name, len(assets))

            logger.info("AI asset scan complete: %d assets, %d API calls", len(all_assets), client.api_call_count)
            return all_assets
        finally:
            await client.close()


class AIAssetStream(Stream):
    primary_key = "asset_id"

    def __init__(self, asset_type: str, config: Mapping[str, Any], scan_cache: AIAssetScanCache, **kwargs: Any):
        super().__init__(**kwargs)
        self._asset_type = asset_type
        self._config = config
        self._scan_cache = scan_cache

    @property
    def name(self) -> str:
        return f"aiasset_{self._asset_type}"

    def get_json_schema(self) -> Mapping[str, Any]:
        return _SCHEMA

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        all_assets = self._scan_cache.get_assets(self._config)
        scanned_at = datetime.now(timezone.utc).isoformat()

        for asset in all_assets:
            if asset.asset_type != self._asset_type:
                continue

            source_location = (
                f"https://github.com/{asset.source_repo}/blob/{asset.source_branch}/{asset.source_file}"
                if asset.relationship == "definition" else ""
            )

            yield {
                "asset_id": asset.asset_id,
                "name": asset.name,
                "type": asset.asset_type,
                "version": asset.version,
                "provider": asset.provider,
                "category": asset.category,
                "confidence": asset.confidence,
                "source_repo": asset.source_repo,
                "source_file": asset.source_file,
                "source_location": source_location,
                "discovered_at": scanned_at,
                "description": asset.description,
                "asset_class": "ai_asset",
                "parent_asset_id": asset.parent_id,
                "relationship": asset.relationship,
                "external_ref": asset.external_ref,
                "author": asset.metadata.get("author", ""),
                "license": asset.metadata.get("license", ""),
            }
