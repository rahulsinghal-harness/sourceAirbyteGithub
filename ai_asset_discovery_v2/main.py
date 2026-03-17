"""Airbyte connector entrypoint: spec, discover, read."""

import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timezone

from airbyte.models import (
    AirbyteCatalog,
    AirbyteMessage,
    AirbyteMessageType,
    AirbyteRecord,
    AirbyteSpec,
    AirbyteStateMessage,
    AirbyteStreamDescriptor,
    AirbyteStreamState,
    ConfiguredAirbyteCatalog,
)
from airbyte.streams import AIASSET_TYPES, get_all_streams
from config import CONNECTION_SPEC, ConnectorConfig
from github import GitHubGraphQLClient
from scanner import scan_repo

logger = logging.getLogger(__name__)


def _emit(msg: AirbyteMessage) -> None:
    sys.stdout.write(msg.to_json_line() + "\n")
    sys.stdout.flush()


def handle_spec() -> None:
    logger.info("Running spec")
    _emit(AirbyteMessage(
        type=AirbyteMessageType.SPEC,
        spec=AirbyteSpec(connectionSpecification=CONNECTION_SPEC),
    ))


def handle_discover(config_path: str) -> None:
    logger.info("Running discover")
    _read_json(config_path)
    _emit(AirbyteMessage(
        type=AirbyteMessageType.CATALOG,
        catalog=AirbyteCatalog(streams=get_all_streams()),
    ))


def handle_read(config_path: str, catalog_path: str, state_path: str | None = None) -> None:
    logger.info("Running read")
    raw_config = _read_json(config_path)
    catalog_data = _read_json(catalog_path)
    config = ConnectorConfig.from_dict(raw_config)
    logger.info(f"Connector {config=}")
    configured_catalog = ConfiguredAirbyteCatalog(**catalog_data)
    selected_streams = {s.stream.name for s in configured_catalog.streams}

    if not config.repos and not config.github_org:
        logger.error("No repos or github_org specified")
        return

    client = _build_client(config)
    all_assets = asyncio.run(_scan(client, config))

    now_ms = int(time.time() * 1000)
    scanned_at = datetime.now(timezone.utc).isoformat()
    record_count = 0

    for stream_name in selected_streams:
        stream_assets = [a for a in all_assets if a.asset_type in AIASSET_TYPES] \
            if stream_name == "aiasset" else []

        for asset in stream_assets:
            source_location = f"https://github.com/{asset.source_repo}/blob/{asset.source_branch}/{asset.source_file}"
            record = {
                "asset_id": asset.asset_id,
                "name": asset.name,
                "type": asset.asset_type,
                "version": asset.version,
                "provider": asset.provider,
                "category": asset.category,
                "confidence": asset.confidence,
                "source_repo": asset.source_repo,
                "source_file": asset.source_file,
                "source_location": source_location if asset.relationship == "definition" else "",
                "discovered_at": scanned_at,
                "description": asset.description,
                "asset_class": "ai_asset",
                "parent_asset_id": asset.parent_id,
                "relationship": asset.relationship,
                "external_ref": asset.external_ref,
                "author": asset.metadata.get("author", ""),
                "license": asset.metadata.get("license", ""),
            }
            _emit(AirbyteMessage(
                type=AirbyteMessageType.RECORD,
                record=AirbyteRecord(stream=stream_name, data=record, emitted_at=now_ms),
            ))
            record_count += 1

        _emit(AirbyteMessage(
            type=AirbyteMessageType.STATE,
            state=AirbyteStateMessage(
                type="STREAM",
                stream=AirbyteStreamState(
                    stream_descriptor=AirbyteStreamDescriptor(name=stream_name),
                    stream_state={"completed": True, "scanned_at": scanned_at, "record_count": record_count},
                ),
            ),
        ))

    logger.info(f"Emitted {record_count} records across {len(selected_streams)} streams")


async def _scan(client: GitHubGraphQLClient, config: ConnectorConfig):
    try:
        all_assets = []
        if config.github_org:
            repo_files_list = await client.scan_org(config.github_org)
        else:
            repo_files_list = await client.scan_repos(config.repos)

        for repo_files in repo_files_list:
            repo_name = f"{repo_files.owner}/{repo_files.name}"
            assets = scan_repo(repo_name, repo_files.files, repo_files.branch)
            all_assets.extend(assets)
            logger.info(f"{repo_name}: {len(assets)} assets found")

        logger.info(f"Total: {len(all_assets)} assets, {client.api_call_count} API calls")
        return all_assets
    finally:
        await client.close()


def _build_client(config: ConnectorConfig) -> GitHubGraphQLClient:
    if config.auth_type == "github_app":
        return GitHubGraphQLClient(
            app_id=config.github_app_id,
            private_key=config.github_app_private_key,
            installation_id=config.github_app_installation_id,
        )
    return GitHubGraphQLClient(token=config.github_token)


def _read_json(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def main():
    from logging_config import setup_logging
    setup_logging(airbyte_mode=True)

    args = sys.argv[1:]
    if not args:
        print("Usage: main.py <spec|discover|read> [--config FILE] [--catalog FILE] [--state FILE]",
              file=sys.stderr)
        sys.exit(1)

    command = args[0]
    flags = {}
    i = 1
    while i < len(args):
        if args[i].startswith("--") and i + 1 < len(args):
            flags[args[i][2:]] = args[i + 1]
            i += 2
        else:
            i += 1

    if command == "spec":
        handle_spec()
    elif command == "discover":
        handle_discover(flags["config"])
    elif command == "read":
        handle_read(flags["config"], flags["catalog"], flags.get("state"))
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
