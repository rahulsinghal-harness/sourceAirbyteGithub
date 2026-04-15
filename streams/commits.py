
"""Org commits via nested org → repos → defaultBranchRef → history — incremental on OID checkpoints.

Per-repo sync with OID checkpoint strategy:
  - Store 4 checkpoint OIDs per repo at positions 1, 25, 50, 100.
  - First run: fetch up to 100 most recent commits per repo (no backfill of older history).
  - Incremental: fetch all new commits since last checkpoint (no per-repo cap).
  - Backfill cursor only saved on API errors during first run.
  - Global cap of 5000 commits per sync across all repos.

Default branch only. Incremental only (no full_refresh).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set, Tuple

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream

from streams.github_graphql import GitHubGraphQLMixin

logger = logging.getLogger(__name__)

PAGE_SIZE_REPOS = 10
PAGE_SIZE_COMMITS_INLINE = 10
PAGE_SIZE_COMMITS_FOLLOWUP = 50
MAX_COMMITS_PER_REPO_PER_SYNC = 100
MAX_COMMITS_TOTAL_PER_SYNC = 5000
_CHECKPOINT_POSITIONS = (1, 25, 50, 100)

# ── GraphQL queries ──────────────────────────────────────────────────────────

_COMMIT_FIELDS = """
    oid
    id
    url
    message
    messageHeadline
    messageBody
    author {
      name
      email
      date
      user { login }
    }
    committer {
      name
      email
      date
      user { login }
    }
    authoredDate
    committedDate
    additions
    deletions
    changedFilesIfAvailable
    tarballUrl
    zipballUrl
    statusCheckRollup { state }
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
        defaultBranchRef {{
          target {{
            ... on Commit {{
              oid
              history(first: {PAGE_SIZE_COMMITS_INLINE}) {{
                pageInfo {{ hasNextPage endCursor }}
                nodes {{
                  {_COMMIT_FIELDS}
                }}
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

FOLLOWUP_QUERY = f"""
query($orgName: String!, $repoName: String!, $commitCursor: String) {{
  organization(login: $orgName) {{
    repository(name: $repoName) {{
      defaultBranchRef {{
        target {{
          ... on Commit {{
            history(first: {PAGE_SIZE_COMMITS_FOLLOWUP}, after: $commitCursor) {{
              pageInfo {{ hasNextPage endCursor }}
              nodes {{
                {_COMMIT_FIELDS}
              }}
            }}
          }}
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
        "oid": {"type": "string"},
        "id": {"type": "string"},
        "url": {"type": "string"},
        "message": {"type": "string"},
        "messageHeadline": {"type": "string"},
        "messageBody": {"type": ["string", "null"]},
        "author": {
            "type": ["object", "null"],
            "properties": {
                "name": {"type": ["string", "null"]},
                "email": {"type": ["string", "null"]},
                "date": {"type": ["string", "null"]},
                "user": {
                    "type": ["object", "null"],
                    "properties": {"login": {"type": "string"}},
                },
            },
        },
        "committer": {
            "type": ["object", "null"],
            "properties": {
                "name": {"type": ["string", "null"]},
                "email": {"type": ["string", "null"]},
                "date": {"type": ["string", "null"]},
                "user": {
                    "type": ["object", "null"],
                    "properties": {"login": {"type": "string"}},
                },
            },
        },
        "authoredDate": {"type": "string"},
        "committedDate": {"type": "string"},
        "additions": {"type": "integer"},
        "deletions": {"type": "integer"},
        "changedFilesIfAvailable": {"type": ["integer", "null"]},
        "tarballUrl": {"type": "string"},
        "zipballUrl": {"type": "string"},
        "statusCheckRollup": {
            "type": ["object", "null"],
            "properties": {"state": {"type": "string"}},
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


def _collect_checkpoint(position: int, oid: str, checkpoints: Dict[int, str]):
    """Store OID if position is a checkpoint position."""
    if position in _CHECKPOINT_POSITIONS:
        checkpoints[position] = oid


def _rebuild_checkpoints(
    new_checkpoints: Dict[int, str],
    old_checkpoints: List[str],
    matched_index: Optional[int],
) -> List[str]:
    """Rebuild checkpoint_oids list after a sync.

    1. Fill positions from new checkpoints (collected this sync).
    2. For unfilled positions, carry forward old checkpoints deeper than
       the matched one (discard shallower — likely rewritten).
    """
    result: List[str] = []
    # Positions filled from new commits
    for pos in _CHECKPOINT_POSITIONS:
        if pos in new_checkpoints:
            result.append(new_checkpoints[pos])

    # Carry forward old checkpoints deeper than matched index
    if old_checkpoints and matched_index is not None:
        deeper_old = old_checkpoints[matched_index + 1:]
        needed = 4 - len(result)
        if needed > 0 and deeper_old:
            result.extend(deeper_old[:needed])
    elif old_checkpoints and matched_index is None:
        # No match (force push / first run interrupted backfill with no backfill_checkpoint_oids)
        # Don't carry forward — old checkpoints are unreliable
        pass

    return result


# ── Stream ───────────────────────────────────────────────────────────────────


class CommitsStream(GitHubGraphQLMixin, Stream):
    primary_key = "id"
    cursor_field = "committedDate"

    def __init__(self, config: Mapping[str, Any], **kwargs: Any):
        super().__init__(**kwargs)
        self._config = config
        self._state: Dict[str, Any] = {}
        self._init_session(config)

    @property
    def name(self) -> str:
        return "commits"

    def get_json_schema(self) -> Mapping[str, Any]:
        return SCHEMA

    @property
    def state(self) -> Mapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: Mapping[str, Any]) -> None:
        self._state = dict(value) if value else {}

    # ── read_records ─────────────────────────────────────────────────────

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        org = self._config["org_name"]

        state = dict(stream_state) if stream_state else {}
        repo_cursors: Dict[str, Dict[str, Any]] = dict(state.get("repo_cursors", {}))

        # Debug: log whether incremental state was received
        repos_with_checkpoints = sum(
            1 for v in repo_cursors.values() if v.get("checkpoint_oids")
        )
        logger.info(
            "commits: starting sync — %d repos in state (%d with checkpoint_oids)",
            len(repo_cursors), repos_with_checkpoints,
        )

        # Point self._state at the live repo_cursors dict so CDK picks up
        # progressive state via the state property as records are yielded.
        self._state = {"repo_cursors": repo_cursors}

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
                    # Check global cap before processing next repo
                    remaining_budget = MAX_COMMITS_TOTAL_PER_SYNC - total_emitted
                    if remaining_budget <= 0:
                        logger.info(
                            "commits: hit global cap of %d, stopping",
                            MAX_COMMITS_TOTAL_PER_SYNC,
                        )
                        has_next_repos = False
                        break

                    if not repo_node:
                        continue
                    repo_id = repo_node["id"]
                    repo_name = repo_node["name"]

                    # Skip repos with no default branch (empty repos)
                    branch_ref = repo_node.get("defaultBranchRef")
                    if not branch_ref:
                        continue
                    target = branch_ref.get("target")
                    if not target:
                        continue
                    tip_oid = target.get("oid")
                    history_conn = target.get("history")
                    if not history_conn or not tip_oid:
                        continue

                    emitted = yield from self._process_repo(
                        org, repo_id, repo_name, tip_oid, history_conn,
                        repo_cursors, remaining_budget,
                    )
                    total_emitted += emitted

                has_next_repos = repos_data["pageInfo"]["hasNextPage"]
                repo_cursor = repos_data["pageInfo"].get("endCursor")
                if not repo_cursor:
                    break

        except Exception as exc:
            logger.warning(
                "commits: stopping due to error (emitted %d so far): %s",
                total_emitted, exc,
            )

        # self._state already points at repo_cursors (live reference set before loop)

        logger.info("commits: emitted %d records total", total_emitted)

    # ── Per-repo processing ──────────────────────────────────────────────

    def _process_repo(
        self,
        org: str,
        repo_id: str,
        repo_name: str,
        tip_oid: str,
        history_conn: Dict[str, Any],
        repo_cursors: Dict[str, Dict[str, Any]],
        remaining_budget: int,
    ) -> int:
        cursor_entry = repo_cursors.get(repo_id, {})
        checkpoint_oids = cursor_entry.get("checkpoint_oids")
        backfill_cursor = cursor_entry.get("backfill_cursor")

        if backfill_cursor is not None:
            # Backfill resume: cap at per-repo limit (finishing initial 100)
            repo_budget = min(MAX_COMMITS_PER_REPO_PER_SYNC, remaining_budget)
            emitted = yield from self._phase1_backfill_resume(
                org, repo_id, repo_name, tip_oid, history_conn,
                repo_cursors, cursor_entry, repo_budget,
            )
        elif checkpoint_oids:
            # Incremental: no per-repo cap, only global budget
            repo_budget = remaining_budget
            emitted = yield from self._phase2_incremental(
                org, repo_id, repo_name, tip_oid, history_conn,
                repo_cursors, cursor_entry, repo_budget,
            )
        else:
            # First run: cap at per-repo limit
            repo_budget = min(MAX_COMMITS_PER_REPO_PER_SYNC, remaining_budget)
            emitted = yield from self._first_run(
                org, repo_id, repo_name, tip_oid, history_conn,
                repo_cursors, repo_budget,
            )

        return emitted

    # ── First run (no prior state) ───────────────────────────────────────

    def _first_run(
        self,
        org: str,
        repo_id: str,
        repo_name: str,
        tip_oid: str,
        history_conn: Dict[str, Any],
        repo_cursors: Dict[str, Dict[str, Any]],
        repo_budget: int,
    ) -> int:
        nodes = history_conn.get("nodes") or []
        page_info = history_conn.get("pageInfo") or {}
        has_next = page_info.get("hasNextPage", False)
        end_cursor = page_info.get("endCursor")

        emitted = 0
        checkpoints: Dict[int, str] = {}

        for node in nodes:
            rec, oid = self._prepare_record(node)
            if rec is None:
                continue
            yield rec
            emitted += 1
            _collect_checkpoint(emitted, oid, checkpoints)
            if emitted >= repo_budget:
                break

        # Paginate for more commits if needed
        error_occurred = False
        if emitted < repo_budget and has_next and end_cursor:
            try:
                result = yield from self._fetch_more_commits(
                    org, repo_name, end_cursor,
                    stop_at_oids=set(),
                    max_records=repo_budget - emitted,
                    position_offset=emitted,
                )
                emitted += result["emitted"]
                checkpoints.update(result["checkpoints"])
                has_next = result["has_next"]
                end_cursor = result["end_cursor"]
            except Exception as exc:
                logger.warning(
                    "commits: repo %s first run pagination error (emitted %d): %s",
                    repo_name, emitted, exc,
                )
                error_occurred = True

        # Save state
        entry: Dict[str, Any] = {}
        ckpt_list = _rebuild_checkpoints(checkpoints, [], None)
        if ckpt_list:
            entry["checkpoint_oids"] = ckpt_list
        # Only save backfill_cursor on API error (cap hit is intentional — no backfill needed)
        if error_occurred and end_cursor:
            entry["backfill_cursor"] = end_cursor
        repo_cursors[repo_id] = entry

        logger.info(
            "commits: repo %s first run emitted %d",
            repo_name, emitted,
        )
        return emitted

    # ── Phase 1: Resume backfill (has backfill_cursor) ───────────────────

    def _phase1_backfill_resume(
        self,
        org: str,
        repo_id: str,
        repo_name: str,
        tip_oid: str,
        history_conn: Dict[str, Any],
        repo_cursors: Dict[str, Dict[str, Any]],
        cursor_entry: Dict[str, Any],
        repo_budget: int,
    ) -> int:
        checkpoint_oids = cursor_entry.get("checkpoint_oids", [])
        backfill_cursor = cursor_entry["backfill_cursor"]
        backfill_checkpoint_oids = cursor_entry.get("backfill_checkpoint_oids", [])

        checkpoint_set: Set[str] = set(checkpoint_oids)
        backfill_checkpoint_set: Set[str] = set(backfill_checkpoint_oids)

        emitted = 0
        new_checkpoints: Dict[int, str] = {}
        matched_index: Optional[int] = None

        # Step A: Incremental from tip — stop at checkpoint_oids
        nodes = history_conn.get("nodes") or []
        page_info = history_conn.get("pageInfo") or {}
        inline_has_next = page_info.get("hasNextPage", False)
        inline_end_cursor = page_info.get("endCursor")
        hit_checkpoint = False

        for node in nodes:
            rec, oid = self._prepare_record(node)
            if rec is None:
                continue
            if oid in checkpoint_set:
                hit_checkpoint = True
                matched_index = _find_checkpoint_index(oid, checkpoint_oids)
                break
            yield rec
            emitted += 1
            _collect_checkpoint(emitted, oid, new_checkpoints)
            if emitted >= repo_budget:
                break

        # Paginate incremental if needed
        if not hit_checkpoint and emitted < repo_budget and inline_has_next and inline_end_cursor:
            try:
                result = yield from self._fetch_more_commits(
                    org, repo_name, inline_end_cursor,
                    stop_at_oids=checkpoint_set,
                    max_records=repo_budget - emitted,
                    position_offset=emitted,
                )
                emitted += result["emitted"]
                new_checkpoints.update(result["checkpoints"])
                if result["hit_checkpoint"]:
                    hit_checkpoint = True
                    matched_index = _find_checkpoint_index(result["matched_oid"], checkpoint_oids)
            except Exception as exc:
                logger.warning(
                    "commits: repo %s phase1 incremental error (emitted %d): %s",
                    repo_name, emitted, exc,
                )

        # Step B: Resume backfill from saved cursor
        backfill_emitted = 0
        backfill_error = False
        backfill_end_cursor = backfill_cursor
        backfill_has_next = True
        backfill_matched_index: Optional[int] = None

        if emitted < repo_budget:
            try:
                result = yield from self._fetch_more_commits(
                    org, repo_name, backfill_cursor,
                    stop_at_oids=backfill_checkpoint_set,
                    max_records=repo_budget - emitted,
                    position_offset=emitted,
                )
                backfill_emitted = result["emitted"]
                emitted += backfill_emitted
                new_checkpoints.update(result["checkpoints"])
                backfill_has_next = result["has_next"]
                backfill_end_cursor = result["end_cursor"]
                if result["hit_checkpoint"]:
                    backfill_matched_index = _find_checkpoint_index(
                        result["matched_oid"], backfill_checkpoint_oids
                    )
            except Exception as exc:
                logger.warning(
                    "commits: repo %s phase1 backfill error (emitted %d): %s",
                    repo_name, emitted, exc,
                )
                backfill_error = True

        # Determine if backfill is complete
        backfill_complete = (
            not backfill_error
            and (backfill_matched_index is not None or not backfill_has_next)
        )

        # Save state
        entry: Dict[str, Any] = {}

        # Rebuild checkpoints from new commits + carry forward old deeper ones
        ckpt_list = _rebuild_checkpoints(new_checkpoints, checkpoint_oids, matched_index)
        if ckpt_list:
            entry["checkpoint_oids"] = ckpt_list

        if not backfill_complete:
            entry["backfill_cursor"] = backfill_end_cursor
            # Preserve backfill_checkpoint_oids so next resume knows where to stop
            if backfill_checkpoint_oids:
                entry["backfill_checkpoint_oids"] = backfill_checkpoint_oids

        repo_cursors[repo_id] = entry

        logger.info(
            "commits: repo %s phase1 emitted %d (backfill_complete=%s)",
            repo_name, emitted, backfill_complete,
        )
        return emitted

    # ── Phase 2: Incremental (fully synced, no backfill) ─────────────────

    def _phase2_incremental(
        self,
        org: str,
        repo_id: str,
        repo_name: str,
        tip_oid: str,
        history_conn: Dict[str, Any],
        repo_cursors: Dict[str, Dict[str, Any]],
        cursor_entry: Dict[str, Any],
        repo_budget: int,
    ) -> int:
        checkpoint_oids = cursor_entry["checkpoint_oids"]
        checkpoint_set: Set[str] = set(checkpoint_oids)

        nodes = history_conn.get("nodes") or []
        page_info = history_conn.get("pageInfo") or {}
        has_next = page_info.get("hasNextPage", False)
        end_cursor = page_info.get("endCursor")

        emitted = 0
        new_checkpoints: Dict[int, str] = {}
        hit_checkpoint = False
        matched_index: Optional[int] = None
        error_occurred = False
        anchor_emitted = False

        for node in nodes:
            rec, oid = self._prepare_record(node)
            if rec is None:
                continue
            if oid in checkpoint_set:
                hit_checkpoint = True
                matched_index = _find_checkpoint_index(oid, checkpoint_oids)
                # Re-emit the checkpoint-boundary record when nothing new,
                # so recordCount > 0 and K8s agent persists our state.
                if emitted == 0 and rec is not None:
                    yield rec
                    emitted += 1
                    anchor_emitted = True
                break
            yield rec
            emitted += 1
            _collect_checkpoint(emitted, oid, new_checkpoints)
            if emitted >= repo_budget:
                break

        # Paginate if inline wasn't enough (no per-repo cap in incremental, only global budget)
        if not hit_checkpoint and emitted < repo_budget and has_next and end_cursor:
            try:
                result = yield from self._fetch_more_commits(
                    org, repo_name, end_cursor,
                    stop_at_oids=checkpoint_set,
                    max_records=repo_budget - emitted,
                    position_offset=emitted,
                )
                emitted += result["emitted"]
                new_checkpoints.update(result["checkpoints"])
                has_next = result["has_next"]
                end_cursor = result["end_cursor"]
                if result["hit_checkpoint"]:
                    hit_checkpoint = True
                    matched_index = _find_checkpoint_index(result["matched_oid"], checkpoint_oids)
            except Exception as exc:
                logger.warning(
                    "commits: repo %s phase2 pagination error (emitted %d): %s",
                    repo_name, emitted, exc,
                )
                error_occurred = True

        # Save state
        entry: Dict[str, Any] = {}
        if anchor_emitted and hit_checkpoint:
            # No new commits — preserve existing checkpoints as-is
            entry["checkpoint_oids"] = checkpoint_oids
        else:
            ckpt_list = _rebuild_checkpoints(new_checkpoints, checkpoint_oids, matched_index)
            if ckpt_list:
                entry["checkpoint_oids"] = ckpt_list

        if error_occurred and not hit_checkpoint:
            # Interrupted before finding checkpoint — save backfill state
            if end_cursor:
                entry["backfill_cursor"] = end_cursor
                entry["backfill_checkpoint_oids"] = checkpoint_oids

        repo_cursors[repo_id] = entry

        if emitted > 0 and not anchor_emitted:
            logger.info(
                "commits: repo %s phase2 emitted %d new commits",
                repo_name, emitted,
            )
        return emitted

    # ── Shared pagination helper ─────────────────────────────────────────

    def _fetch_more_commits(
        self,
        org: str,
        repo_name: str,
        after_cursor: str,
        stop_at_oids: Set[str],
        max_records: int,
        position_offset: int,
    ) -> Dict[str, Any]:
        """Paginate commits via FOLLOWUP_QUERY."""
        emitted = 0
        hit_checkpoint = False
        matched_oid: Optional[str] = None
        commit_cursor = after_cursor
        has_next = True
        checkpoints: Dict[int, str] = {}

        while has_next and emitted < max_records:
            resp = self._graphql(
                FOLLOWUP_QUERY,
                {"orgName": org, "repoName": repo_name, "commitCursor": commit_cursor},
            )

            org_data = resp.get("data", {}).get("organization") or {}
            repo_data = org_data.get("repository")
            if not repo_data:
                break
            branch_ref = repo_data.get("defaultBranchRef")
            if not branch_ref:
                break
            target = branch_ref.get("target")
            if not target:
                break
            history_data = target.get("history")
            if not history_data:
                break

            nodes = history_data.get("nodes") or []
            page_info = history_data.get("pageInfo") or {}
            has_next = page_info.get("hasNextPage", False)
            end_cursor_val = page_info.get("endCursor")

            for node in nodes:
                rec, oid = self._prepare_record(node)
                if rec is None:
                    continue

                if stop_at_oids and oid in stop_at_oids:
                    hit_checkpoint = True
                    matched_oid = oid
                    has_next = False
                    break

                yield rec
                emitted += 1
                _collect_checkpoint(position_offset + emitted, oid, checkpoints)

                if emitted >= max_records:
                    break

            if not end_cursor_val:
                break
            commit_cursor = end_cursor_val

        return {
            "emitted": emitted,
            "hit_checkpoint": hit_checkpoint,
            "matched_oid": matched_oid,
            "has_next": has_next,
            "end_cursor": commit_cursor,
            "checkpoints": checkpoints,
        }

    # ── Record preparation ───────────────────────────────────────────────

    @staticmethod
    def _prepare_record(node: Any) -> Tuple[Any, Optional[str]]:
        if not isinstance(node, dict) or not node.get("oid"):
            return None, None
        return node, node["oid"]


# ── Module-level helpers ─────────────────────────────────────────────────────


def _find_checkpoint_index(oid: str, checkpoint_oids: List[str]) -> Optional[int]:
    """Find the index of an OID in the checkpoint list."""
    try:
        return checkpoint_oids.index(oid)
    except ValueError:
        return None
