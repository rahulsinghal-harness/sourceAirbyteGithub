"""Releases details stream — repos with their recent releases."""

import logging
from typing import Any, Iterable, List, Mapping, Optional

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream

from streams.github_graphql import GitHubGraphQLMixin

logger = logging.getLogger(__name__)

QUERY = """
query($orgName: String!, $after: String) {
  organization(login: $orgName) {
    repositories(first: 100, after: $after) {
      nodes {
        id name
        releases(first: 10, orderBy: {field: CREATED_AT, direction: DESC}) {
          nodes { name tagName isPrerelease isDraft publishedAt description url }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "releases": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": ["string", "null"]},
                    "tagName": {"type": "string"},
                    "isPrerelease": {"type": ["boolean", "null"]},
                    "isDraft": {"type": ["boolean", "null"]},
                    "publishedAt": {"type": ["string", "null"]},
                    "description": {"type": ["string", "null"]},
                    "url": {"type": "string"},
                },
            },
        },
    },
}


class ReleasesDetailsStream(GitHubGraphQLMixin, Stream):
    primary_key = "id"

    def __init__(self, config: Mapping[str, Any], **kwargs: Any):
        super().__init__(**kwargs)
        self._config = config
        self._init_session(config)

    @property
    def name(self) -> str:
        return "releases_details"

    def get_json_schema(self) -> Mapping[str, Any]:
        return SCHEMA

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        org = self._config["org_name"]
        start_date = self._config.get("start_date", "1970-01-01T00:00:00Z")
        after = None
        has_next = True

        while has_next:
            resp = self._graphql(QUERY, {"orgName": org, "after": after})
            repos_data = resp["data"]["organization"]["repositories"]

            for repo in repos_data["nodes"]:
                releases = repo.get("releases", {}).get("nodes", [])
                if not releases:
                    continue
                dated = [r for r in releases if r.get("publishedAt")]
                if dated and max(r["publishedAt"] for r in dated) < start_date:
                    continue
                yield {"id": repo["id"], "name": repo["name"], "releases": releases}

            has_next = repos_data["pageInfo"]["hasNextPage"]
            after = repos_data["pageInfo"]["endCursor"]
