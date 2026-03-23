"""Connector entrypoint."""

import sys

from airbyte_cdk.entrypoint import launch

from source import GitHubSource


def main():
    source = GitHubSource()
    launch(source, sys.argv[1:])


if __name__ == "__main__":
    main()
