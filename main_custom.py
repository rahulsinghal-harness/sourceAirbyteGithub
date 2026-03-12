"""Custom entry point that extends ManifestDeclarativeSource with TeamRepositoriesStream."""

import logging
import sys
from pathlib import Path
from typing import List, Mapping, Optional

import yaml
from airbyte_cdk.entrypoint import launch
from airbyte_cdk.models import AirbyteMessage, Type as MessageType
from airbyte_cdk.sources.declarative.manifest_declarative_source import (
    ManifestDeclarativeSource,
)
from source_declarative_manifest.components import TeamRepositoriesStream

MANIFEST_PATH = Path("/airbyte/integration_code/source_declarative_manifest/manifest.yaml")

_logger = logging.getLogger("custom_github_source")


def _flatten_languages(record: dict) -> None:
    """Convert languages.edges[{size, node:{name}}] to [{name, size}]."""
    langs = record.get("languages")
    if isinstance(langs, dict) and "edges" in langs:
        record["languages"] = [
            {"name": e["node"]["name"], "size": e["size"]}
            for e in (langs.get("edges") or [])
        ]


def _safe_get(obj, key):
    """Extract a key from a dict, Pydantic model, or any dict-like object."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(key)
    if hasattr(obj, key):
        return getattr(obj, key)
    if hasattr(obj, "__getitem__"):
        try:
            return obj[key]
        except (KeyError, IndexError, TypeError):
            return None
    return None


def _extract_cursor_from_state(
    state: Optional[List], stream_name: str, cursor_field: str,
) -> Optional[str]:
    """Pull the cursor value for a stream out of the Airbyte state list.

    State entries may be plain dicts (from JSON) or AirbyteStateMessage objects
    (Pydantic models) depending on CDK version.  The nested stream_state may be
    an AirbyteStateBlob (Pydantic), a plain dict, or another dict-like object.
    """
    if not state:
        return None
    for entry in state:
        try:
            if isinstance(entry, dict):
                desc = (entry.get("stream") or {}).get("stream_descriptor") or {}
                if desc.get("name") == stream_name:
                    return (entry["stream"].get("stream_state") or {}).get(cursor_field)
            else:
                stream_obj = getattr(entry, "stream", None)
                if stream_obj is None:
                    continue
                desc = getattr(stream_obj, "stream_descriptor", None)
                name = _safe_get(desc, "name") if desc else None
                if name == stream_name:
                    ss = getattr(stream_obj, "stream_state", None)
                    return _safe_get(ss, cursor_field)
        except Exception:
            continue
    return None


class CustomGitHubSource(ManifestDeclarativeSource):
    def streams(self, config):
        streams = super().streams(config)
        streams.append(TeamRepositoriesStream(config=config))
        return streams

    def read(self, logger, config, catalog, state=None):
        if state:
            _logger.info("incoming state has %d entries", len(state))
            for i, entry in enumerate(state):
                _logger.info(
                    "state[%d] type=%s repr=%.500s", i, type(entry).__name__, repr(entry),
                )
        else:
            _logger.info("incoming state is empty/None")

        cursor = _extract_cursor_from_state(state, "repo_details", "updatedAt")
        _logger.info("extracted cursor for repo_details: %s", cursor)
        emitted = 0
        skipped = 0

        for message in super().read(logger, config, catalog, state):
            if (
                message.type == MessageType.RECORD
                and message.record
                and message.record.stream == "repo_details"
            ):
                _flatten_languages(message.record.data)

                if cursor and message.record.data.get("updatedAt", "") < cursor:
                    skipped += 1
                    continue
                emitted += 1

            yield message

        if cursor:
            _logger.info(
                "repo_details incremental filter: cursor=%s emitted=%d skipped=%d",
                cursor, emitted, skipped,
            )


def run():
    with open(MANIFEST_PATH) as f:
        source_config = yaml.safe_load(f)
    source = CustomGitHubSource(source_config=source_config)
    launch(source, sys.argv[1:])


if __name__ == "__main__":
    run()
