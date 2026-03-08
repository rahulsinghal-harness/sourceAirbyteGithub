"""Custom entry point that extends ManifestDeclarativeSource with TeamRepositoriesStream."""

import sys
from pathlib import Path

import yaml
from airbyte_cdk.entrypoint import launch
from airbyte_cdk.models import AirbyteMessage, Type as MessageType
from airbyte_cdk.sources.declarative.manifest_declarative_source import (
    ManifestDeclarativeSource,
)
from source_declarative_manifest.components import TeamRepositoriesStream

MANIFEST_PATH = Path("/airbyte/integration_code/source_declarative_manifest/manifest.yaml")


def _flatten_languages(record: dict) -> None:
    """Convert languages.edges[{size, node:{name}}] to [{name, size}]."""
    langs = record.get("languages")
    if isinstance(langs, dict) and "edges" in langs:
        record["languages"] = [
            {"name": e["node"]["name"], "size": e["size"]}
            for e in (langs.get("edges") or [])
        ]


class CustomGitHubSource(ManifestDeclarativeSource):
    def streams(self, config):
        streams = super().streams(config)
        streams.append(TeamRepositoriesStream(config=config))
        return streams

    def read(self, logger, config, catalog, state=None):
        for message in super().read(logger, config, catalog, state):
            if (
                message.type == MessageType.RECORD
                and message.record
                and message.record.stream == "repo_details"
            ):
                _flatten_languages(message.record.data)
            yield message


def run():
    with open(MANIFEST_PATH) as f:
        source_config = yaml.safe_load(f)
    source = CustomGitHubSource(source_config=source_config)
    launch(source, sys.argv[1:])


if __name__ == "__main__":
    run()
