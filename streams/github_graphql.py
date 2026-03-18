"""Shared GraphQL helper for GitHub streams that use sync requests."""

import logging
import time

import requests

GRAPHQL_URL = "https://api.github.com/graphql"

logger = logging.getLogger(__name__)


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
        for attempt in range(3):
            resp = self._session.post(GRAPHQL_URL, json={"query": query, "variables": variables})
            if resp.status_code in (502, 503) and attempt < 2:
                wait = 2 ** attempt
                logger.warning("GraphQL %d, retrying in %ds (attempt %d/3)", resp.status_code, wait, attempt + 1)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            body = resp.json()
            if "errors" in body:
                raise RuntimeError(f"GraphQL errors: {body['errors']}")
            return body
        raise RuntimeError("GraphQL request failed after 3 attempts")
