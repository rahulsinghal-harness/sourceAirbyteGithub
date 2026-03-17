"""Team repositories stream — one record per team with nested repo list."""

from typing import Any, Iterable, List, Mapping, Optional

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream

from streams.github_graphql import GitHubGraphQLMixin

BULK_QUERY = """
query($orgName: String!, $teamCursor: String) {
  organization(login: $orgName) {
    teams(first: 100, after: $teamCursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id name slug combinedSlug description createdAt updatedAt
        avatarUrl privacy notificationSetting url membersUrl
        reviewRequestDelegationEnabled reviewRequestDelegationAlgorithm
        reviewRequestDelegationMemberCount reviewRequestDelegationNotifyTeam
        viewerCanAdminister
        repositories(first: 100) {
          pageInfo { hasNextPage endCursor }
          edges {
            permission
            node { name url isPrivate }
          }
        }
      }
    }
  }
}
"""

FOLLOWUP_QUERY = """
query($orgName: String!, $teamSlug: String!, $after: String) {
  organization(login: $orgName) {
    team(slug: $teamSlug) {
      repositories(first: 100, after: $after) {
        pageInfo { hasNextPage endCursor }
        edges {
          permission
          node { name url isPrivate }
        }
      }
    }
  }
}
"""

TEAM_FIELDS = [
    "id", "name", "slug", "combinedSlug", "description",
    "createdAt", "updatedAt", "avatarUrl", "privacy",
    "notificationSetting", "url", "membersUrl",
    "reviewRequestDelegationEnabled", "reviewRequestDelegationAlgorithm",
    "reviewRequestDelegationMemberCount", "reviewRequestDelegationNotifyTeam",
    "viewerCanAdminister",
]


class TeamRepositoriesStream(GitHubGraphQLMixin, Stream):
    primary_key = "slug"

    def __init__(self, config: Mapping[str, Any], **kwargs: Any):
        super().__init__(**kwargs)
        self._config = config
        self._init_session(config)

    @property
    def name(self) -> str:
        return "team_repositories"

    def get_json_schema(self) -> Mapping[str, Any]:
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "id": {"type": "string"},
                "name": {"type": "string"},
                "slug": {"type": "string"},
                "combinedSlug": {"type": ["string", "null"]},
                "description": {"type": ["string", "null"]},
                "createdAt": {"type": ["string", "null"]},
                "updatedAt": {"type": ["string", "null"]},
                "avatarUrl": {"type": ["string", "null"]},
                "privacy": {"type": ["string", "null"]},
                "notificationSetting": {"type": ["string", "null"]},
                "url": {"type": ["string", "null"]},
                "membersUrl": {"type": ["string", "null"]},
                "reviewRequestDelegationEnabled": {"type": ["boolean", "null"]},
                "reviewRequestDelegationAlgorithm": {"type": ["string", "null"]},
                "reviewRequestDelegationMemberCount": {"type": ["integer", "null"]},
                "reviewRequestDelegationNotifyTeam": {"type": ["boolean", "null"]},
                "viewerCanAdminister": {"type": ["boolean", "null"]},
                "repositories": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "permission": {"type": "string"},
                            "name": {"type": "string"},
                            "url": {"type": "string"},
                            "isPrivate": {"type": "boolean"},
                        },
                    },
                },
            },
        }

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        org = self._config["org_name"]
        start_date = self._config.get("start_date", "1970-01-01T00:00:00Z")
        team_cursor = None
        has_next_teams = True

        while has_next_teams:
            resp = self._graphql(BULK_QUERY, {"orgName": org, "teamCursor": team_cursor})
            teams_data = resp["data"]["organization"]["teams"]

            for team in teams_data["nodes"]:
                if team.get("updatedAt") and team["updatedAt"] < start_date:
                    continue
                repos = self._extract_repos(team["repositories"]["edges"])

                if team["repositories"]["pageInfo"]["hasNextPage"]:
                    repos.extend(self._fetch_remaining_repos(
                        org, team["slug"], team["repositories"]["pageInfo"]["endCursor"],
                    ))

                record = {f: team.get(f) for f in TEAM_FIELDS}
                record["repositories"] = repos
                yield record

            has_next_teams = teams_data["pageInfo"]["hasNextPage"]
            team_cursor = teams_data["pageInfo"]["endCursor"]

    def _fetch_remaining_repos(self, org: str, team_slug: str, cursor: str) -> List[Mapping[str, Any]]:
        repos: list = []
        has_next = True
        while has_next:
            resp = self._graphql(FOLLOWUP_QUERY, {"orgName": org, "teamSlug": team_slug, "after": cursor})
            repo_data = resp["data"]["organization"]["team"]["repositories"]
            repos.extend(self._extract_repos(repo_data["edges"]))
            has_next = repo_data["pageInfo"]["hasNextPage"]
            cursor = repo_data["pageInfo"]["endCursor"]
        return repos

    @staticmethod
    def _extract_repos(edges: list) -> List[Mapping[str, Any]]:
        return [
            {"permission": e["permission"], "name": e["node"]["name"],
             "url": e["node"]["url"], "isPrivate": e["node"]["isPrivate"]}
            for e in edges
        ]
