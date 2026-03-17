"""Shared GraphQL helper for GitHub streams that use sync requests."""

import requests

GRAPHQL_URL = "https://api.github.com/graphql"


class GitHubGraphQLMixin:
    """Mixin providing a reusable sync GraphQL client for Stream subclasses."""

    def _init_session(self, config: dict):
        from source import _resolve_token
        token = _resolve_token(config)
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

    def _graphql(self, query: str, variables: dict) -> dict:
        resp = self._session.post(GRAPHQL_URL, json={"query": query, "variables": variables})
        resp.raise_for_status()
        body = resp.json()
        if "errors" in body:
            raise RuntimeError(f"GraphQL errors: {body['errors']}")
        return body
