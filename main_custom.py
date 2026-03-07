"""Custom entry point that extends ManifestDeclarativeSource with TeamRepositoriesStream."""

import sys
from pathlib import Path

import yaml
from airbyte_cdk.entrypoint import launch
from airbyte_cdk.sources.declarative.manifest_declarative_source import (
    ManifestDeclarativeSource,
)
from source_declarative_manifest.components import TeamRepositoriesStream

MANIFEST_PATH = Path("/airbyte/integration_code/source_declarative_manifest/manifest.yaml")


class CustomGitHubSource(ManifestDeclarativeSource):
    def streams(self, config):
        streams = super().streams(config)
        streams.append(TeamRepositoriesStream(config=config))
        return streams


def run():
    with open(MANIFEST_PATH) as f:
        source_config = yaml.safe_load(f)
    source = CustomGitHubSource(source_config=source_config)
    launch(source, sys.argv[1:])


if __name__ == "__main__":
    run()
