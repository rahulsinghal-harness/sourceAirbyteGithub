"""Repo stats stream — bulk repository issue/PR metrics via a single GraphQL org query."""

import logging
from typing import Any, Iterable, List, Mapping, Optional

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream

from streams.github_graphql import GitHubGraphQLMixin

logger = logging.getLogger(__name__)

QUERY = """
query($orgName: String!, $after: String) {
  organization(login: $orgName) {
    repositories(first: 50, after: $after) {
      nodes {
        id
        name
        issues { totalCount }
        openIssues: issues(states: OPEN) { totalCount }
        closedIssues: issues(states: CLOSED) { totalCount }
        pullRequests { totalCount }
        openPRs: pullRequests(states: OPEN) { totalCount }
        mergedPRs: pullRequests(states: MERGED) { totalCount }
        closedPRs: pullRequests(states: CLOSED) { totalCount }
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
        "totalIssueCount": {"type": "integer"},
        "openIssueCount": {"type": "integer"},
        "closedIssueCount": {"type": "integer"},
        "totalPullRequestCount": {"type": "integer"},
        "openPRCount": {"type": "integer"},
        "mergedPRCount": {"type": "integer"},
        "closedPRCount": {"type": "integer"},
    },
}


def _flatten(repo: dict) -> dict:
    """Flatten nested GraphQL response into the output schema."""
    return {
        "id": repo["id"],
        "name": repo["name"],
        "totalIssueCount": (repo.get("issues") or {}).get("totalCount", 0),
        "openIssueCount": (repo.get("openIssues") or {}).get("totalCount", 0),
        "closedIssueCount": (repo.get("closedIssues") or {}).get("totalCount", 0),
        "totalPullRequestCount": (repo.get("pullRequests") or {}).get("totalCount", 0),
        "openPRCount": (repo.get("openPRs") or {}).get("totalCount", 0),
        "mergedPRCount": (repo.get("mergedPRs") or {}).get("totalCount", 0),
        "closedPRCount": (repo.get("closedPRs") or {}).get("totalCount", 0),
    }


class RepoStatsStream(GitHubGraphQLMixin, Stream):
    primary_key = "id"

    def __init__(self, config: Mapping[str, Any], **kwargs: Any):
        super().__init__(**kwargs)
        self._config = config
        self._init_session(config)

    @property
    def name(self) -> str:
        return "repo_stats"

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
        after = None
        has_next = True
        emitted = 0

        while has_next:
            resp = self._graphql(QUERY, {"orgName": org, "after": after})
            repos_data = resp["data"]["organization"]["repositories"]

            for repo in repos_data["nodes"]:
                yield _flatten(repo)
                emitted += 1

            has_next = repos_data["pageInfo"]["hasNextPage"]
            after = repos_data["pageInfo"]["endCursor"]

        logger.info("repo_stats: emitted %d records", emitted)
