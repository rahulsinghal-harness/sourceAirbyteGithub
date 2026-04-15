"""Repo details stream — paginated org repos with incremental sync on updatedAt."""

import logging
from typing import Any, Iterable, List, Mapping, Optional

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream

from streams.github_graphql import GitHubGraphQLMixin

logger = logging.getLogger(__name__)

_QUERY_TEMPLATE = """
query($orgName: String!, $after: String) {{
  organization(login: $orgName) {{
    repositories(first: 50, after: $after) {{
      nodes {{
        id name nameWithOwner url homepageUrl sshUrl
        createdAt updatedAt pushedAt description
        isPrivate isFork isArchived
        primaryLanguage {{ name }}
        latestRelease {{ name tagName publishedAt }}
        languages(first: 20) {{ edges {{ size node {{ name }} }} }}
        readme: object(expression: "HEAD:README.md") {{ ... on Blob {{ text byteSize isBinary }} }}
        codeowners: object(expression: "HEAD:CODEOWNERS") {{ ... on Blob {{ text byteSize isBinary }} }}
        hasAgentsFile: object(expression: "HEAD:{agent_file_name}") {{ id }}
      }}
      pageInfo {{ hasNextPage endCursor }}
    }}
  }}
}}
"""

SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "nameWithOwner": {"type": "string"},
        "url": {"type": "string"},
        "homepageUrl": {"type": ["string", "null"]},
        "sshUrl": {"type": "string"},
        "createdAt": {"type": "string"},
        "updatedAt": {"type": "string"},
        "pushedAt": {"type": ["string", "null"]},
        "description": {"type": ["string", "null"]},
        "isPrivate": {"type": "boolean"},
        "isFork": {"type": "boolean"},
        "isArchived": {"type": "boolean"},
        "primaryLanguage": {
            "type": ["object", "null"],
            "properties": {"name": {"type": "string"}},
        },
        "latestRelease": {
            "type": ["object", "null"],
            "properties": {
                "name": {"type": ["string", "null"]},
                "tagName": {"type": ["string", "null"]},
                "publishedAt": {"type": ["string", "null"]},
            },
        },
        "readme": {
            "type": ["object", "null"],
            "properties": {
                "text": {"type": ["string", "null"]},
                "byteSize": {"type": ["integer", "null"]},
                "isBinary": {"type": ["boolean", "null"]},
            },
        },
        "codeowners": {
            "type": ["object", "null"],
            "properties": {
                "text": {"type": ["string", "null"]},
                "byteSize": {"type": ["integer", "null"]},
                "isBinary": {"type": ["boolean", "null"]},
            },
        },
        "hasAgentsFile": {
            "type": ["object", "null"],
            "properties": {"id": {"type": "string"}},
        },
        "languages": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"name": {"type": "string"}, "size": {"type": "integer"}},
            },
        },
    },
}


def _flatten_languages(record: dict) -> None:
    langs = record.get("languages")
    if isinstance(langs, dict) and "edges" in langs:
        record["languages"] = [
            {"name": e["node"]["name"], "size": e["size"]}
            for e in (langs.get("edges") or [])
        ]


class RepoDetailsStream(GitHubGraphQLMixin, Stream):
    primary_key = "id"
    cursor_field = "updatedAt"

    def __init__(self, config: Mapping[str, Any], **kwargs: Any):
        super().__init__(**kwargs)
        self._config = config
        self._cursor_value: str = ""
        agent_file_name = config.get("agent_file_name", "AGENTS.md")
        self._query = _QUERY_TEMPLATE.format(agent_file_name=agent_file_name)
        self._init_session(config)

    @property
    def name(self) -> str:
        return "repo_details"

    def get_json_schema(self) -> Mapping[str, Any]:
        return SCHEMA

    def get_updated_state(
        self,
        current_stream_state: Mapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        current = current_stream_state.get("updatedAt", "")
        latest = latest_record.get("updatedAt", "")
        return {"updatedAt": max(current, latest)}

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        org = self._config["org_name"]
        start_date = self._config.get("start_date", "1970-01-01T00:00:00Z")

        cursor_value = None
        if sync_mode == SyncMode.incremental and stream_state:
            cursor_value = stream_state.get("updatedAt")

        effective_cursor = cursor_value or start_date
        after = None
        has_next = True
        emitted = 0

        while has_next:
            resp = self._graphql(self._query, {"orgName": org, "after": after})
            repos_data = resp["data"]["organization"]["repositories"]

            for repo in repos_data["nodes"]:
                _flatten_languages(repo)
                if repo.get("updatedAt", "") < effective_cursor:
                    continue
                self._cursor_value = max(self._cursor_value, repo.get("updatedAt", ""))
                yield repo
                emitted += 1

            has_next = repos_data["pageInfo"]["hasNextPage"]
            after = repos_data["pageInfo"]["endCursor"]

        logger.info("repo_details: emitted %d records (cursor=%s)", emitted, effective_cursor)
