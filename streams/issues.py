"""Org issues via GitHub GraphQL search — rolling window, incremental on updatedAt, cap 1000."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, List, Mapping, Optional

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream

from streams.github_graphql import GitHubGraphQLMixin

logger = logging.getLogger(__name__)

PAGE_SIZE = 50
MAX_ISSUES = 1000
LOOKBACK_DAYS = 14
_DEFAULT_START = "1970-01-01T00:00:00Z"

QUERY = """
query($query: String!, $after: String, $first: Int!) {
  search(query: $query, type: ISSUE, first: $first, after: $after) {
    nodes {
      ... on Issue {
        id
        number
        createdAt
        updatedAt
        closedAt
        url
        resourcePath
        title
        body
        author {
          login
        }
        state
        issueType {
          name
        }
        repository {
          id
          name
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

SCHEMA: Mapping[str, Any] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "id": {"type": "string"},
        "number": {"type": "integer"},
        "createdAt": {"type": "string"},
        "updatedAt": {"type": "string"},
        "closedAt": {"type": ["string", "null"]},
        "url": {"type": "string"},
        "resourcePath": {"type": "string"},
        "title": {"type": "string"},
        "body": {"type": ["string", "null"]},
        "author": {
            "type": ["object", "null"],
            "properties": {"login": {"type": "string"}},
        },
        "state": {"type": "string"},
        "issueType": {
            "type": ["object", "null"],
            "properties": {"name": {"type": "string"}},
        },
        "repository": {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "name": {"type": "string"},
            },
        },
    },
}


def _utc_date_from_iso_z(iso_z: str) -> date:
    if iso_z.endswith("Z"):
        dt = datetime.fromisoformat(iso_z.replace("Z", "+00:00"))
    else:
        dt = datetime.fromisoformat(iso_z)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).date()


def _parse_github_dt(iso_z: str) -> datetime:
    if iso_z.endswith("Z"):
        dt = datetime.fromisoformat(iso_z.replace("Z", "+00:00"))
    else:
        dt = datetime.fromisoformat(iso_z)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _build_search_query(org: str, updated_from: date) -> str:
    safe = org.replace("\\", "\\\\").replace('"', '\\"')
    return (
        f'org:"{safe}" is:issue '
        f"updated:>={updated_from.isoformat()} "
        "sort:updated-desc"
    )


class IssuesStream(GitHubGraphQLMixin, Stream):
    primary_key = "id"
    cursor_field = "updatedAt"

    def __init__(self, config: Mapping[str, Any], **kwargs: Any):
        super().__init__(**kwargs)
        self._config = config
        self._init_session(config)

    @property
    def name(self) -> str:
        return "issues"

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
        start_raw = self._config.get("start_date", _DEFAULT_START)
        start_date_utc = _utc_date_from_iso_z(start_raw)
        today_utc = datetime.now(timezone.utc).date()
        rolling_floor = today_utc - timedelta(days=LOOKBACK_DAYS)
        first_run_from = max(start_date_utc, rolling_floor)

        cursor_iso = None
        if sync_mode == SyncMode.incremental and stream_state:
            cursor_iso = stream_state.get("updatedAt")

        if sync_mode == SyncMode.full_refresh:
            updated_from_date = first_run_from
            cursor_dt: Optional[datetime] = None
        elif cursor_iso:
            cursor_dt = _parse_github_dt(cursor_iso)
            cursor_day = cursor_dt.date()
            updated_from_date = max(start_date_utc, cursor_day)
        else:
            updated_from_date = first_run_from
            cursor_dt = None

        search_q = _build_search_query(org, updated_from_date)
        after: Optional[str] = None
        emitted = 0
        truncated = False
        stop_paging = False

        while not stop_paging:
            first = min(PAGE_SIZE, MAX_ISSUES - emitted)
            if first <= 0:
                break

            resp = self._graphql(
                QUERY,
                {"query": search_q, "after": after, "first": first},
            )
            search_data = resp["data"]["search"]
            nodes = search_data.get("nodes") or []
            page_info = search_data["pageInfo"]
            has_next = page_info["hasNextPage"]

            for node in nodes:
                if not isinstance(node, dict) or not node.get("id"):
                    continue
                updated_at = node.get("updatedAt")
                if not updated_at:
                    continue
                try:
                    node_dt = _parse_github_dt(updated_at)
                except ValueError:
                    logger.warning("issues: skip row with bad updatedAt=%r", updated_at)
                    continue

                # Match repo_details: include updatedAt == cursor (skip only strictly older).
                if cursor_dt is not None and node_dt < cursor_dt:
                    stop_paging = True
                    break

                yield node
                emitted += 1
                if emitted >= MAX_ISSUES:
                    truncated = True
                    stop_paging = True
                    break

            if stop_paging:
                if truncated and has_next:
                    logger.warning(
                        "issues: stopped at %s records (latest by updated); more pages exist",
                        MAX_ISSUES,
                    )
                break

            if not has_next:
                break

            after = page_info.get("endCursor")
            if not after:
                break

        logger.info(
            "issues: emitted %s records (sync_mode=%s, updated_from=%s, incremental_cursor=%s)",
            emitted,
            sync_mode,
            updated_from_date,
            cursor_iso,
        )
