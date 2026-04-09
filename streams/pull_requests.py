
"""Org pull requests via nested org → repos → pullRequests — incremental on updatedAt.

Two-phase per-repo sync:
  Phase 1 (backfill): DESC from top, store endCursor, resume next sync, 5000 cap/repo/sync.
  Phase 2 (incremental): DESC from top, stop when updatedAt <= stored cursor.

No full_refresh support — incremental only.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream

from streams.github_graphql import GitHubGraphQLMixin

logger = logging.getLogger(__name__)

PAGE_SIZE_REPOS = 50
PAGE_SIZE_PRS_INLINE = 10
PAGE_SIZE_PRS_FOLLOWUP = 50
MAX_PRS_PER_REPO_PER_SYNC = 5000
_LOOKBACK_DAYS = 30

# ── GraphQL queries ──────────────────────────────────────────────────────────

_PR_FIELDS = """
    id number createdAt updatedAt closedAt mergedAt publishedAt
    url resourcePath permalink title state isDraft closed locked
    merged lastEditedAt
    author { login }
    mergedBy { login }
    repository { id name }
"""

BULK_QUERY = f"""
query($orgName: String!, $repoCursor: String) {{
  organization(login: $orgName) {{
    repositories(first: {PAGE_SIZE_REPOS}, after: $repoCursor) {{
      pageInfo {{ hasNextPage endCursor }}
      nodes {{
        id
        name
        pullRequests(first: {PAGE_SIZE_PRS_INLINE}, orderBy: {{field: UPDATED_AT, direction: DESC}}) {{
          pageInfo {{ hasNextPage endCursor }}
          nodes {{
            {_PR_FIELDS}
          }}
        }}
      }}
    }}
  }}
}}
"""

FOLLOWUP_QUERY = f"""
query($orgName: String!, $repoName: String!, $prCursor: String) {{
  organization(login: $orgName) {{
    repository(name: $repoName) {{
      pullRequests(first: {PAGE_SIZE_PRS_FOLLOWUP}, after: $prCursor, orderBy: {{field: UPDATED_AT, direction: DESC}}) {{
        pageInfo {{ hasNextPage endCursor }}
        nodes {{
          {_PR_FIELDS}
        }}
      }}
    }}
  }}
}}
"""

# ── Schema ───────────────────────────────────────────────────────────────────

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
        "mergedAt": {"type": ["string", "null"]},
        "publishedAt": {"type": ["string", "null"]},
        "url": {"type": "string"},
        "resourcePath": {"type": "string"},
        "permalink": {"type": "string"},
        "title": {"type": "string"},
        "state": {"type": "string"},
        "isDraft": {"type": "boolean"},
        "closed": {"type": "boolean"},
        "locked": {"type": "boolean"},
        "merged": {"type": "boolean"},
        "lastEditedAt": {"type": ["string", "null"]},
        "author": {
            "type": ["object", "null"],
            "properties": {"login": {"type": "string"}},
        },
        "mergedBy": {
            "type": ["object", "null"],
            "properties": {"login": {"type": "string"}},
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

# ── Helpers ──────────────────────────────────────────────────────────────────


def _parse_github_dt(iso_z: str) -> datetime:
    if iso_z.endswith("Z"):
        dt = datetime.fromisoformat(iso_z.replace("Z", "+00:00"))
    else:
        dt = datetime.fromisoformat(iso_z)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# ── Stream ───────────────────────────────────────────────────────────────────


class PullRequestsStream(GitHubGraphQLMixin, Stream):
    primary_key = "id"
    cursor_field = "updatedAt"

    def __init__(self, config: Mapping[str, Any], **kwargs: Any):
        super().__init__(**kwargs)
        self._config = config
        self._init_session(config)

    @property
    def name(self) -> str:
        return "pull_requests"

    def get_json_schema(self) -> Mapping[str, Any]:
        return SCHEMA

    @property
    def supports_incremental(self) -> bool:
        return True

    def get_updated_state(
        self,
        current_stream_state: Mapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        # State is managed manually in read_records; CDK calls this per-record
        # but we handle state ourselves via repo_cursors.
        return current_stream_state

    # ── read_records ─────────────────────────────────────────────────────

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        org = self._config["org_name"]
        start_dt = datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)

        state = dict(stream_state) if stream_state else {}
        repo_cursors: Dict[str, Dict[str, Any]] = dict(state.get("repo_cursors", {}))

        repo_cursor: Optional[str] = None
        has_next_repos = True
        total_emitted = 0

        try:
            while has_next_repos:
                resp = self._graphql(
                    BULK_QUERY, {"orgName": org, "repoCursor": repo_cursor}
                )
                repos_data = resp["data"]["organization"]["repositories"]

                for repo_node in repos_data["nodes"]:
                    if not repo_node:
                        continue
                    repo_id = repo_node["id"]
                    repo_name = repo_node["name"]
                    pr_conn = repo_node.get("pullRequests")
                    if not pr_conn:
                        continue

                    emitted = yield from self._process_repo(
                        org, repo_id, repo_name, pr_conn,
                        repo_cursors, start_dt,
                    )
                    total_emitted += emitted

                has_next_repos = repos_data["pageInfo"]["hasNextPage"]
                repo_cursor = repos_data["pageInfo"].get("endCursor")
                if not repo_cursor:
                    break

        except Exception as exc:
            logger.warning(
                "pull_requests: stopping due to error (emitted %d so far): %s",
                total_emitted, exc,
            )

        # Update state with all completed repo cursors
        state["repo_cursors"] = repo_cursors
        # Emit a synthetic state message by setting _state on self
        self._state = state

        logger.info("pull_requests: emitted %d records total", total_emitted)

    # ── Per-repo processing ──────────────────────────────────────────────

    def _process_repo(
        self,
        org: str,
        repo_id: str,
        repo_name: str,
        pr_conn: Dict[str, Any],
        repo_cursors: Dict[str, Dict[str, Any]],
        start_dt: datetime,
    ) -> int:
        """Process PRs for a single repo. Returns count of emitted records."""
        cursor_entry = repo_cursors.get(repo_id, {})
        stored_updated_at = cursor_entry.get("updatedAt")
        backfill_cursor = cursor_entry.get("backfill_cursor")

        if backfill_cursor is not None:
            # Phase 1 still in progress — first do a quick incremental check
            # from top to catch PRs updated since last sync, then resume backfill.
            emitted = yield from self._phase1_with_incremental_check(
                org, repo_id, repo_name, pr_conn,
                repo_cursors, cursor_entry, start_dt,
            )
        elif stored_updated_at:
            # Phase 2 — fully synced, incremental from top
            emitted = yield from self._phase2_incremental(
                org, repo_id, repo_name, pr_conn,
                repo_cursors, cursor_entry,
            )
        else:
            # First run for this repo — start backfill
            emitted = yield from self._first_run(
                org, repo_id, repo_name, pr_conn,
                repo_cursors, start_dt,
            )

        return emitted

    # ── First run (no prior state for this repo) ─────────────────────────

    def _first_run(
        self,
        org: str,
        repo_id: str,
        repo_name: str,
        pr_conn: Dict[str, Any],
        repo_cursors: Dict[str, Dict[str, Any]],
        start_dt: datetime,
    ) -> int:
        """First run: DESC from top, cap 5000, save state."""
        nodes = pr_conn.get("nodes") or []
        page_info = pr_conn.get("pageInfo") or {}
        has_next = page_info.get("hasNextPage", False)
        end_cursor = page_info.get("endCursor")

        emitted = 0
        max_updated: Optional[str] = None
        min_updated: Optional[str] = None
        reached_start = False

        # Process inline nodes
        for node in nodes:
            rec, updated_iso = self._prepare_record(node)
            if rec is None:
                continue
            node_dt = _parse_github_dt(updated_iso)
            if node_dt < start_dt:
                reached_start = True
                break
            yield rec
            emitted += 1
            if max_updated is None or updated_iso > max_updated:
                max_updated = updated_iso
            min_updated = updated_iso

        # Paginate for more PRs if needed
        if not reached_start and has_next and end_cursor and emitted < MAX_PRS_PER_REPO_PER_SYNC:
            result = yield from self._fetch_more_prs(
                org, repo_name, end_cursor,
                start_dt, MAX_PRS_PER_REPO_PER_SYNC - emitted,
                stop_at_updated=None,
            )
            emitted += result["emitted"]
            if result["max_updated"]:
                if max_updated is None or result["max_updated"] > max_updated:
                    max_updated = result["max_updated"]
            if result["min_updated"]:
                min_updated = result["min_updated"]
            reached_start = result["reached_start"]
            has_next = result["has_next"]
            end_cursor = result["end_cursor"]

        # Save state
        entry: Dict[str, Any] = {}
        if max_updated:
            entry["updatedAt"] = max_updated
        if not reached_start and has_next and end_cursor:
            # Backfill not complete — save cursor for resumption
            entry["backfill_cursor"] = end_cursor
            if min_updated:
                entry["backfill_low"] = min_updated
        repo_cursors[repo_id] = entry

        logger.info(
            "pull_requests: repo %s first run emitted %d (backfill_complete=%s)",
            repo_name, emitted, reached_start or not has_next,
        )
        return emitted

    # ── Phase 1: Backfill with incremental check ────────────────────────

    def _phase1_with_incremental_check(
        self,
        org: str,
        repo_id: str,
        repo_name: str,
        pr_conn: Dict[str, Any],
        repo_cursors: Dict[str, Dict[str, Any]],
        cursor_entry: Dict[str, Any],
        start_dt: datetime,
    ) -> int:
        """Phase 1: quick incremental check from top + resume backfill."""
        stored_updated_at = cursor_entry.get("updatedAt", "")
        backfill_cursor = cursor_entry["backfill_cursor"]
        stored_updated_dt = _parse_github_dt(stored_updated_at) if stored_updated_at else None

        emitted = 0
        max_updated = stored_updated_at or None

        # Step 1: Quick incremental check from inline nodes (DESC from top)
        # Emit new/updated PRs until we hit stored_updated_at
        nodes = pr_conn.get("nodes") or []
        page_info = pr_conn.get("pageInfo") or {}
        inline_has_next = page_info.get("hasNextPage", False)
        inline_end_cursor = page_info.get("endCursor")
        need_more_incremental = True

        for node in nodes:
            rec, updated_iso = self._prepare_record(node)
            if rec is None:
                continue
            node_dt = _parse_github_dt(updated_iso)
            if stored_updated_dt and node_dt <= stored_updated_dt:
                need_more_incremental = False
                break
            yield rec
            emitted += 1
            if max_updated is None or updated_iso > max_updated:
                max_updated = updated_iso

        # If inline wasn't enough to reach stored cursor, paginate more
        if need_more_incremental and inline_has_next and inline_end_cursor:
            result = yield from self._fetch_more_prs(
                org, repo_name, inline_end_cursor,
                start_dt=None,
                max_records=MAX_PRS_PER_REPO_PER_SYNC - emitted,
                stop_at_updated=stored_updated_dt,
            )
            emitted += result["emitted"]
            if result["max_updated"] and (max_updated is None or result["max_updated"] > max_updated):
                max_updated = result["max_updated"]

        # Step 2: Resume backfill from stored backfill_cursor
        backfill_result = yield from self._fetch_more_prs(
            org, repo_name, backfill_cursor,
            start_dt=start_dt,
            max_records=MAX_PRS_PER_REPO_PER_SYNC - emitted,
            stop_at_updated=None,
        )
        emitted += backfill_result["emitted"]
        if backfill_result["max_updated"] and (max_updated is None or backfill_result["max_updated"] > max_updated):
            max_updated = backfill_result["max_updated"]

        # Update state
        entry: Dict[str, Any] = {}
        if max_updated:
            entry["updatedAt"] = max_updated

        reached_start = backfill_result["reached_start"]
        bf_has_next = backfill_result["has_next"]
        bf_end_cursor = backfill_result["end_cursor"]

        if not reached_start and bf_has_next and bf_end_cursor:
            entry["backfill_cursor"] = bf_end_cursor
            if backfill_result["min_updated"]:
                entry["backfill_low"] = backfill_result["min_updated"]
            elif cursor_entry.get("backfill_low"):
                entry["backfill_low"] = cursor_entry["backfill_low"]
        # else: backfill complete — no backfill_cursor

        repo_cursors[repo_id] = entry

        logger.info(
            "pull_requests: repo %s phase1 emitted %d (backfill_complete=%s)",
            repo_name, emitted, reached_start or not bf_has_next,
        )
        return emitted

    # ── Phase 2: Incremental (fully synced) ──────────────────────────────

    def _phase2_incremental(
        self,
        org: str,
        repo_id: str,
        repo_name: str,
        pr_conn: Dict[str, Any],
        repo_cursors: Dict[str, Dict[str, Any]],
        cursor_entry: Dict[str, Any],
    ) -> int:
        """Phase 2: DESC from top, stop when updatedAt <= stored cursor."""
        stored_updated_at = cursor_entry["updatedAt"]
        stored_updated_dt = _parse_github_dt(stored_updated_at)

        nodes = pr_conn.get("nodes") or []
        page_info = pr_conn.get("pageInfo") or {}
        has_next = page_info.get("hasNextPage", False)
        end_cursor = page_info.get("endCursor")

        emitted = 0
        max_updated = stored_updated_at
        hit_cursor = False

        for node in nodes:
            rec, updated_iso = self._prepare_record(node)
            if rec is None:
                continue
            node_dt = _parse_github_dt(updated_iso)
            if node_dt <= stored_updated_dt:
                hit_cursor = True
                break
            yield rec
            emitted += 1
            if updated_iso > max_updated:
                max_updated = updated_iso

        # Paginate if inline wasn't enough to reach cursor
        if not hit_cursor and has_next and end_cursor:
            result = yield from self._fetch_more_prs(
                org, repo_name, end_cursor,
                start_dt=None,
                max_records=MAX_PRS_PER_REPO_PER_SYNC - emitted,
                stop_at_updated=stored_updated_dt,
            )
            emitted += result["emitted"]
            if result["max_updated"] and result["max_updated"] > max_updated:
                max_updated = result["max_updated"]

        # Update cursor
        repo_cursors[repo_id] = {"updatedAt": max_updated}

        if emitted > 0:
            logger.info(
                "pull_requests: repo %s phase2 emitted %d new PRs",
                repo_name, emitted,
            )
        return emitted

    # ── Shared pagination helper ─────────────────────────────────────────

    def _fetch_more_prs(
        self,
        org: str,
        repo_name: str,
        after_cursor: str,
        start_dt: Optional[datetime],
        max_records: int,
        stop_at_updated: Optional[datetime],
    ) -> Dict[str, Any]:
        """Paginate PRs via FOLLOWUP_QUERY."""
        emitted = 0
        max_updated: Optional[str] = None
        min_updated: Optional[str] = None
        reached_start = False
        pr_cursor = after_cursor
        has_next = True

        while has_next and emitted < max_records:
            try:
                resp = self._graphql(
                    FOLLOWUP_QUERY,
                    {"orgName": org, "repoName": repo_name, "prCursor": pr_cursor},
                )
            except Exception as exc:
                logger.warning(
                    "pull_requests: repo %s pagination error (emitted %d): %s",
                    repo_name, emitted, exc,
                )
                has_next = False
                break

            org_data = resp.get("data", {}).get("organization") or {}
            repo_data = org_data.get("repository")
            if not repo_data:
                break
            pr_data = repo_data.get("pullRequests")
            if not pr_data:
                break

            nodes = pr_data.get("nodes") or []
            page_info = pr_data.get("pageInfo") or {}
            has_next = page_info.get("hasNextPage", False)
            end_cursor_val = page_info.get("endCursor")

            for node in nodes:
                rec, updated_iso = self._prepare_record(node)
                if rec is None:
                    continue
                node_dt = _parse_github_dt(updated_iso)

                # Stop at start_date (backfill boundary)
                if start_dt and node_dt < start_dt:
                    reached_start = True
                    has_next = False
                    break

                # Stop at stored cursor (incremental boundary)
                if stop_at_updated and node_dt <= stop_at_updated:
                    has_next = False
                    break

                yield rec
                emitted += 1
                if max_updated is None or updated_iso > max_updated:
                    max_updated = updated_iso
                min_updated = updated_iso

                if emitted >= max_records:
                    break

            if not end_cursor_val:
                break
            pr_cursor = end_cursor_val

        return {
            "emitted": emitted,
            "max_updated": max_updated,
            "min_updated": min_updated,
            "reached_start": reached_start,
            "has_next": has_next,
            "end_cursor": pr_cursor,
        }

    # ── Record preparation ───────────────────────────────────────────────

    @staticmethod
    def _prepare_record(node: Any) -> tuple:
        if not isinstance(node, dict) or not node.get("id"):
            return None, None
        updated_at = node.get("updatedAt")
        if not updated_at:
            return None, None
        return node, updated_at
