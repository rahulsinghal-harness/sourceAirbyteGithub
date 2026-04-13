# Commits Stream — PRD & Implementation Plan

---

## 1. PRD — Product Requirements Document

### 1.1 Overview

Add a **commits** stream to the GitHub Airbyte source connector that syncs commit history from the **default branch** of every repository in an organization. The stream supports **incremental sync** with resilience against force pushes, rebases, and API interruptions.

### 1.2 Background & Motivation

The connector already syncs PRs, issues, repo details, releases, teams, and AI assets. Commits are a critical missing piece for engineering analytics — they provide insight into development velocity, contributor activity, code change frequency, and merge patterns. Teams need commit data to power dashboards, audit trails, and developer productivity metrics.

### 1.3 Why Commits Are Different

Unlike PRs and issues, Git commits are **immutable objects** with **no server-managed `updatedAt` timestamp**. The available timestamps (`committedDate`, `authoredDate`, `pushedDate`) are all unreliable as incremental cursors:

| Timestamp | Problem |
|-----------|---------|
| `committedDate` | Set by developer's local clock. Offline work, clock skew, rebases make it non-monotonic. |
| `authoredDate` | Preserved across rebases — even less reliable than `committedDate`. |
| `pushedDate` | Deprecated by GitHub, nullable, may be removed without notice. |

This rules out the timestamp-based cursor approach used by PRs/issues.

### 1.4 Solution: OID Checkpoint Strategy

Store **4 checkpoint OIDs** (commit SHAs) per repository at positions 1, 25, 50, 100 from commits ingested during each sync. On subsequent syncs, paginate from the branch tip and stop when hitting any checkpoint OID.

**Why 4 checkpoints at these positions:**
- Position 1 (tip): handles normal incremental sync (most common case)
- Position 25: survives small rebases/squashes (<25 commits rewritten)
- Position 50: survives medium rewrites
- Position 100: survives large rewrites

If no checkpoint is found (complete history rewrite), falls back to a 100-commit cap — effectively a cold re-sync for that repo.

### 1.5 Scope

| In Scope | Out of Scope |
|----------|-------------|
| Default branch commits only | Non-default branch commits |
| Incremental sync mode only | full_refresh (removed — commits are append-only) |
| Per-repo state with checkpoint OIDs | Cross-repo deduplication |
| Resumable backfill on API errors | Backfill of older history beyond 100 most recent |
| Up to 100 commits per repo on first run | Webhook-based real-time sync |
| 5,000 global cap per sync across all repos | Configurable per-repo caps |
| Anchor record for K8s state persistence | |

### 1.6 Data Fields

| Field | Type | Description |
|-------|------|-------------|
| `oid` | string | Commit SHA |
| `id` | string | GitHub node ID (primary key) |
| `url` | string | GitHub URL for the commit |
| `message` | string | Full commit message |
| `messageHeadline` | string | First line of commit message |
| `messageBody` | string | Commit message body (without headline) |
| `author` | object/null | `{name, email, date, user: {login}}` (GitActor) |
| `committer` | object/null | `{name, email, date, user: {login}}` (GitActor) |
| `authoredDate` | string | Author timestamp (ISO 8601) |
| `committedDate` | string | Committer timestamp (ISO 8601) |
| `additions` | integer | Lines added |
| `deletions` | integer | Lines deleted |
| `changedFilesIfAvailable` | integer/null | Files changed |
| `tarballUrl` | string | Tarball download URL |
| `zipballUrl` | string | Zipball download URL |
| `statusCheckRollup` | object/null | `{state}` — CI/check status |
| `repository` | object | `{id, name}` — directly from GraphQL |

### 1.7 State Model

```json
{
  "repo_cursors": {
    "<repo_id>": {
      "checkpoint_oids": ["tip_sha", "pos25_sha", "pos50_sha", "pos100_sha"],
      "backfill_cursor": "graphql_pagination_cursor",
      "backfill_checkpoint_oids": ["old_tip", "old_pos25", "old_pos50", "old_pos100"]
    }
  }
}
```

| Field | Purpose | Present When |
|-------|---------|-------------|
| `checkpoint_oids` | Stop condition for incremental sync from tip | Always (after first successful fetch) |
| `backfill_cursor` | Resume point after API error/interruption | Only during interrupted syncs |
| `backfill_checkpoint_oids` | Old checkpoints used as stop for backfill resumption | Only when incremental was interrupted (preserves old checkpoints so backfill knows where to stop) |

### 1.8 Sync Behavior Summary

| Scenario | Behavior | Per-repo Cap | Global Cap |
|----------|----------|-------------|------------|
| First run | Fetch up to 100 most recent commits per repo from tip | 100 | 5,000 |
| Incremental (normal) | Fetch from tip, stop at checkpoint OID | Uncapped | 5,000 |
| Backfill resume (API error during first run) | Resume to finish getting 100 | 100 | 5,000 |
| Force push (<100 commits rewritten) | Deeper checkpoint survives, minimal re-fetch | Uncapped | 5,000 |
| Force push (>100 commits rewritten) | No checkpoint found, cold re-sync | Uncapped | 5,000 |
| API error mid-incremental | Save backfill_cursor + backfill_checkpoint_oids | Uncapped | 5,000 |
| No new commits | Emit 1 anchor record (K8s state persistence) | N/A | N/A |
| Empty repo | Skip (no defaultBranchRef) | N/A | N/A |
| Global cap hit | Stop processing repos, remaining picked up next sync | N/A | 5,000 |

**Key design decisions:**
- `backfill_cursor` only saved on API errors, NOT on cap hit — hitting 100/repo is intentional (older history not needed)
- Incremental (phase2) has no per-repo cap — new commits between syncs are typically 0-5
- Anchor record: re-emit last checkpoint commit when 0 new commits, so `recordCount >= 1` and K8s agent persists state

### 1.9 API Cost

- **Bulk query:** 10 repos x 10 inline commits = 1 GraphQL request per 10 repos
- **Follow-up pagination:** 50 commits per page, max 2 pages per repo (100 cap on first run)
- **Typical incremental sync:** Most repos have few new commits → no follow-up needed (stop on inline page)
- **Steady state:** ~18 bulk queries only (180 repos / 10 per page), ~0 follow-ups
- **Rate limit profile:** Same as PRs/issues. Default branch commit rate ≈ PR merge rate, well within 5000 points/hour.
- **Note:** `PAGE_SIZE_REPOS` is set to 10 (not 50 like PRs/issues) because the full commit field set (17 fields including nested objects) is too heavy for GitHub's GraphQL at higher counts, causing 502 errors on large orgs.

---

## 2. Implementation Plan

### 2.1 Processing Logic

```
_process_repo routing:
├── backfill_cursor exists     → _phase1()  (incremental from tip + resume backfill, 100/repo cap)
├── checkpoint_oids exists     → _phase2()  (incremental from tip, uncapped per-repo)
└── neither                    → _first_run() (100/repo cap from tip)
```

**Checkpoint rebuild rule after each sync:**
1. Collect OIDs at positions 1, 25, 50, 100 from commits fetched this sync
2. Unfilled positions → carry forward old checkpoints that are **deeper** than the matched checkpoint (discard shallower ones — likely rewritten)

### 2.2 `_first_run` (no prior state)

1. Paginate from tip, emit up to 100 commits (or `repo_budget` if global cap is closer)
2. Collect checkpoint OIDs at positions 1, 25, 50, 100
3. If completed or cap hit: save `{checkpoint_oids}` — NO `backfill_cursor` (cap hit is intentional)
4. If error mid-fetch: save `{checkpoint_oids (from fetched), backfill_cursor}` — no `backfill_checkpoint_oids` (first run has no prior state)

### 2.3 `_phase1` (has `backfill_cursor` — resuming after API error)

1. **Step A — Incremental from tip:** Emit new commits, stop at `checkpoint_oids`. Collect new checkpoints.
2. **Step B — Resume backfill from `backfill_cursor`:** Stop at `backfill_checkpoint_oids` (if exists) or `repo_budget`.
3. Total capped at `repo_budget` (min of 100, global remaining).
4. If completed: rebuild `checkpoint_oids`, clear `backfill_*` fields.
5. If error in Step B: save `{checkpoint_oids (new from Step A), backfill_checkpoint_oids (preserved), backfill_cursor (updated to where Step B stopped)}`.

### 2.4 `_phase2` (incremental, no backfill)

1. Paginate from tip, stop at `checkpoint_oids` or `repo_budget` (global remaining — no per-repo cap).
2. If 0 new commits: emit 1 anchor record (last checkpoint commit) for K8s state persistence.
3. If completed: rebuild `checkpoint_oids`.
4. If error mid-fetch: save `{checkpoint_oids (new from fetched), backfill_checkpoint_oids (old checkpoint_oids), backfill_cursor (where we stopped)}`.

### 2.5 `_fetch_more_commits` — shared pagination helper

Same pattern as `_fetch_more_prs` / `_fetch_more_issues`:
- `stop_at_oids: set` parameter for checkpoint matching
- Tracks position counter for checkpoint collection
- Returns: `{emitted, hit_checkpoint, has_next, end_cursor, checkpoints_collected}`

### 2.6 `_prepare_record`

- Validate node has `oid`
- Return `(record, oid)`

### 2.7 Constants

```python
PAGE_SIZE_REPOS = 10
PAGE_SIZE_COMMITS_INLINE = 10
PAGE_SIZE_COMMITS_FOLLOWUP = 50
MAX_COMMITS_PER_REPO_PER_SYNC = 100      # First run only
MAX_COMMITS_TOTAL_PER_SYNC = 5000        # Global cap across all repos
_CHECKPOINT_POSITIONS = (1, 25, 50, 100)
```

### 2.8 GraphQL Queries

**Commit fields (shared between both queries):**
```graphql
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
```

**BULK_QUERY** (org → repos → inline commits):
```graphql
query($orgName: String!, $repoCursor: String) {
  organization(login: $orgName) {
    repositories(first: 10, after: $repoCursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        name
        defaultBranchRef {
          target {
            ... on Commit {
              oid
              history(first: 10) {
                pageInfo { hasNextPage endCursor }
                nodes {
                  <commit fields>
                }
              }
            }
          }
        }
      }
    }
  }
}
```

Note: `oid` at the `target` level (outside `history`) gives us the current tip SHA for short-circuit check (Case 9: no new commits).

**FOLLOWUP_QUERY** (single repo pagination):
```graphql
query($orgName: String!, $repoName: String!, $commitCursor: String) {
  organization(login: $orgName) {
    repository(name: $repoName) {
      defaultBranchRef {
        target {
          ... on Commit {
            history(first: 50, after: $commitCursor) {
              pageInfo { hasNextPage endCursor }
              nodes {
                <commit fields>
              }
            }
          }
        }
      }
    }
  }
}
```

### 2.9 Schema

JSON Schema dict inline (same pattern as PR/issues):

```python
SCHEMA = {
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
```

### 2.10 Stream Class

- `CommitsStream(GitHubGraphQLMixin, Stream)`
- `primary_key = "id"` (GitHub node ID, consistent with PRs/issues)
- `cursor_field = "committedDate"` (CDK compatibility)
- `supports_incremental = True`
- Checkpoint strategy uses `oid` (SHA) internally in state — separate from primary key

### 2.11 Register in `source.py`

Import + add `CommitsStream(config=config)` in `streams()` method.

### 2.12 Catalog Entry

```json
{
  "stream": {
    "name": "commits",
    "json_schema": { ... },
    "supported_sync_modes": ["incremental"],
    "source_defined_cursor": true,
    "default_cursor_field": ["committedDate"],
    "source_defined_primary_key": [["id"]],
    "is_resumable": true
  },
  "sync_mode": "incremental",
  "destination_sync_mode": "append"
}
```

---

## 3. Test Cases

### Case 1: First Run — Large Repo

**Setup:** Repo has 800 commits `C800 → ... → C1`. No state.

**Sync:**
- Paginate from tip, emit `C800..C701` = 100 (per-repo cap hit)
- Checkpoints: pos1=`C800`, pos25=`C776`, pos50=`C751`, pos100=`C701`
- **No backfill_cursor** — cap hit is intentional, older history not needed

**State:** `{checkpoint_oids: [C800, C776, C751, C701]}`
**Emitted:** 100

---

### Case 2: First Run — Small Repo

**Setup:** Repo has 30 commits `C30 → ... → C1`. No state.

**Sync:**
- Fetch all 30 (under 100 cap)
- Checkpoints: pos1=`C30`, pos25=`C6` (no pos 50/100 — fewer than 50 commits)

**State:** `{checkpoint_oids: [C30, C6]}`
**Emitted:** 30

---

### Case 3: First Run — Interrupted by API Error

**Setup:** Repo has 200 commits. No state. API error after 50 commits.

**Sync:**
- Fetches `C200..C151` = 50 commits, then 502 error
- Checkpoints from 50: pos1=`C200`, pos25=`C176`, pos50=`C151`
- No prior checkpoints (first run) → no `backfill_checkpoint_oids`

**State:** `{checkpoint_oids: [C200, C176, C151], backfill_cursor: cursor_at_C151}`
**Emitted:** 50

---

### Case 4: Resumption After First Run Interrupted (follows Case 3)

**Setup:** State has `checkpoint_oids: [C200, C176, C151]`, `backfill_cursor: cursor_at_C151`. No `backfill_checkpoint_oids`. 5 new commits `C205..C201` merged since.

**Phase 1:**
- **Step A (incremental):** `C205, C204, C203, C202, C201, C200` → hits `C200` (checkpoint) → emits 5
- **Step B (backfill from cursor_at_C151):** No `backfill_checkpoint_oids` → stop at 100 repo cap. Fetches `C150..C106` = 45 commits (5 + 45 = 50, remaining budget to finish initial 100)

**Checkpoint rebuild:**
pos1=`C205`, pos25=`C181`, pos50=`C156`

**State:** `{checkpoint_oids: [C205, C181, C156]}` — `backfill_*` cleared
**Emitted:** 50

---

### Case 5: Normal Incremental — Few New Commits

**Setup:** State `checkpoint_oids: [C800, C776, C751, C701]`. 3 new commits: `C803 → C802 → C801 → C800`.

**Phase 2:**
- Inline: `C803, C802, C801, C800, ...` → hits `C800` (checkpoint pos 1) → emits 3

**Checkpoint rebuild:**
- From 3 new: pos1=`C803` (only fillable position)
- Unfilled → carry forward old (deeper than matched `C800`): `C776, C751, C701`

**State:** `{checkpoint_oids: [C803, C776, C751, C701]}`
**Emitted:** 3

---

### Case 6: Normal Incremental — Many New Commits (60)

**Setup:** State `checkpoint_oids: [C800, C776, C751, C701]`. 60 new commits: `C860 → ... → C801 → C800`.

**Phase 2:**
- Paginate from tip, hits `C800` (checkpoint pos 1) after 60 commits → emits 60

**Checkpoint rebuild:**
- From 60 new: pos1=`C860`, pos25=`C836`, pos50=`C811`
- Unfilled pos 100 → carry forward deepest old: `C701`

**State:** `{checkpoint_oids: [C860, C836, C811, C701]}`
**Emitted:** 60

---

### Case 7: Force Push — Small Rebase (<25 commits rewritten)

**Setup:** State `checkpoint_oids: [C800, C776, C751, C701]`. Last 10 commits squashed into 1: `C791' → C790 → ... → C776`.

**Phase 2:**
- `C791'` not in checkpoints, `C790..C777` not in checkpoints
- Hits `C776` (checkpoint pos 25) → emits 15 commits

**Checkpoint rebuild:**
- From 15 new: pos1=`C791'`
- Carry forward old **deeper** than matched `C776`: `C751, C701`
- Discard `C800` (shallower than match — rewritten, no longer in history)

**State:** `{checkpoint_oids: [C791', C751, C701]}`
**Emitted:** 15 (some overlap with previously synced data — no data loss)

---

### Case 8: Force Push — Large Rewrite (>100 commits)

**Setup:** State `checkpoint_oids: [C800, C776, C751, C701]`. Entire history rewritten (e.g., default branch switched).

**Phase 2:**
- Paginate from new tip, never hits any checkpoint
- Hits global budget → cold re-sync (fetches as many as budget allows)

**Checkpoint rebuild:** From fetched commits (like first run)

**State:** `{checkpoint_oids: [new_tip, new_pos25, new_pos50, new_pos100]}`
**Emitted:** Up to global budget remaining

---

### Case 9: No New Commits (Anchor Record)

**Setup:** State `checkpoint_oids: [C800, C776, C751, C701]`. Repo unchanged, tip still `C800`.

**Phase 2:**
- First commit `C800` matches checkpoint pos 1 → no new commits
- Emit `C800` as anchor record (duplicate) so `recordCount >= 1` and K8s agent persists state

**State:** Unchanged `{checkpoint_oids: [C800, C776, C751, C701]}`
**Emitted:** 1 (anchor — harmless duplicate, does not advance cursor)

---

### Case 10: Empty Repo

**Setup:** Repo exists but has no commits. `defaultBranchRef: null`.

**Sync:** Skip repo. No state saved.
**Emitted:** 0

---

### Case 11: New Repo Added to Org

**Setup:** New repo appears in org, no state entry.

**Sync:** Treated as Case 1 (first run). Fetch up to 100, save checkpoints.

---

### Case 12: Repo Deleted from Org

**Setup:** State has checkpoints for this repo, but repo no longer in org.

**Sync:** Repo not in org repository list. Stale state entry never read. Harmless.

---

### Case 13: Incremental — Interrupted by Error

**Setup:** State `checkpoint_oids: [C800, C776, C751, C701]`. 200 new commits. Error after 100.

**Phase 2:**
- Fetches `C1000..C901` = 100, then error
- Haven't reached any checkpoint (no per-repo cap in incremental, but error stopped us)

**Checkpoint rebuild from 100 fetched:** pos1=`C1000`, pos25=`C976`, pos50=`C951`, pos100=`C901`

**State:**
```json
{
  "checkpoint_oids": ["C1000", "C976", "C951", "C901"],
  "backfill_checkpoint_oids": ["C800", "C776", "C751", "C701"],
  "backfill_cursor": "cursor_at_C901"
}
```
**Emitted:** 100

---

### Case 14: Resumption After Incremental Interrupted (follows Case 13)

**Setup:** State has `checkpoint_oids: [C1000, C976, C951, C901]`, `backfill_checkpoint_oids: [C800, C776, C751, C701]`, `backfill_cursor: cursor_at_C901`. 3 new commits since.

**Phase 1:**
- **Step A (incremental):** `C1003, C1002, C1001, C1000` → hits `C1000` (checkpoint) → emits 3
- **Step B (backfill from cursor_at_C901):** Uses `backfill_checkpoint_oids` as stop. Fetches `C900..C800` → hits `C800` → emits 100

**Checkpoint rebuild from 103 commits this sync:**
- pos1=`C1003`, pos25=`C979`, pos50=`C954`
- pos 100 → carry forward deepest old (deeper than matched `C800`): `C701`

**State:** `{checkpoint_oids: [C1003, C979, C954, C701]}` — `backfill_*` cleared
**Emitted:** 103

---

### Case 15: Backfill Also Interrupted (follows Case 13 — error during resumption)

**Setup:** Same as Case 14 start state.

**Phase 1:**
- **Step A:** 3 new commits, hits `C1000` → emits 3
- **Step B:** Resume from `cursor_at_C901`, fetches `C900..C851` = 50, then error again

**State:**
```json
{
  "checkpoint_oids": ["C1003", "C976", "C951", "C901"],
  "backfill_checkpoint_oids": ["C800", "C776", "C751", "C701"],
  "backfill_cursor": "cursor_at_C851"
}
```
`checkpoint_oids` rebuilt from Step A. `backfill_checkpoint_oids` preserved (still needed). `backfill_cursor` updated to Step B stop point.

**Emitted:** 53

---

### Case 16: Global Cap Hit Mid-Sync

**Setup:** 60 repos, all first run. Global cap 5000.

**Sync:**
- First 50 repos: 100 each = 5000 → global cap hit
- Repos 51-60: not processed, no state entry
- Next sync: repos 1-50 go incremental (anchor records), repos 51-60 get first run

**Emitted:** 5000

---

## 4. Files to Create/Modify

| File | Action |
|------|--------|
| `streams/commits.py` | **Create** — new stream (~400 lines) |
| `source.py` | **Edit** — add import + registration (2 lines) |
| `secrets/configured_catalog.json` | **Edit** — add commits stream entry |
| `secrets/configured_catalog_all.json` | **Edit** — add commits stream entry |
| `docs/commits_stream_plan.md` | **Create** — PRD + test cases (this document) |

## 5. Reuse

- `GitHubGraphQLMixin` from `streams/github_graphql.py` — auth + retry logic
- `_parse_github_dt` helper (copy inline) — same as PR/issues
- Same class structure, `read_records` outer loop, error handling, state emission (`self._state`) pattern

## 6. Verification

1. Run sync with no state — verify max 100 commits per repo, max 5000 total
2. Verify repos with >100 commits do NOT have `backfill_cursor` (intentional stop)
3. Run again with state — verify incremental fetches all new commits (no per-repo cap)
4. Verify anchor records emitted for repos with 0 new commits
5. Verify global 5000 cap works across all modes
6. Verify `full_refresh` removed from catalog `supported_sync_modes`
7. Test with an empty repo (no `defaultBranchRef`) — skips gracefully
8. Verify state contains `checkpoint_oids` per repo
9. After 2-3 syncs, all repos should have checkpoints and steady state should emit ~200-300 records

## 7. Test Results (harness org)

Tested against the `harness` GitHub organization (180+ repos).

### Sync 1 — First Run (no state)

```
=======================================================
  SYNC ANALYTICS — COMMITS
=======================================================
  Total time:          201.49s
  Total records:       5000 (global cap hit)
  Total data size:     6346.54 KB
  Repos processed:     91 / 180+
  Per-repo max:        100
  Repos at cap:        30
  Backfill cursors:    0
  Records/sec:         24.81
  State saved:         91 repos with checkpoint_oids
=======================================================
```

### Sync 2 — Incremental (state from sync 1)

```
=======================================================
  SYNC ANALYTICS — COMMITS
=======================================================
  Total time:          176.69s
  Total records:       3790 (under global cap)
  Total data size:     4626.04 KB
  Repos processed:     159 (92 existing + 67 new)
  Anchor records:      92 (1 duplicate each, no new commits)
  New repo first-runs: 67
  Repos at cap:        20
  Backfill cursors:    0
  Records/sec:         21.45
  State saved:         159 repos with checkpoint_oids
=======================================================
```

### Before vs After

| Metric | Before (v0.0.33, 200/repo, no global cap) | After (v0.0.35, 100/repo, 5000 global) |
|--------|-------------------------------------------|----------------------------------------|
| Sync 1 records | 18,000+ | **5,000** |
| Sync 1 payload | ~28 MB | **6.3 MB** (~78% reduction) |
| Steady state records | 18,000+ | **~200-300** (~98% reduction) |
| Steady state API calls | ~200+ | **~18** (~91% reduction) |

### Sample Record

```json
{
  "oid": "c1f56a0e5e0777b3d515c531050105eb8543a749",
  "id": "C_kwDOAP1qmtoAKGMxZjU2YTBlNWUwNzc3YjNkNTE1YzUzMTA1MDEwNWViODU0M2E3NDk",
  "url": "https://github.com/harness/harness/commit/c1f56a0e5e0777b3d515c531050105eb8543a749",
  "message": "chore: [CODE-5159] Stop using session/local storage for auth tokens (#5170)...",
  "messageHeadline": "chore: [CODE-5159] Stop using session/local storage for auth tokens (…",
  "messageBody": "…#5170)...",
  "author": {
    "name": "Ritik Kapoor",
    "email": "ritik.kapoor@harness.io",
    "date": "2026-04-10T09:45:19Z",
    "user": { "login": "rkapoor10" }
  },
  "committer": {
    "name": "Harness",
    "email": "noreply@harness.io",
    "date": "2026-04-10T09:45:19Z",
    "user": null
  },
  "authoredDate": "2026-04-10T09:45:19Z",
  "committedDate": "2026-04-10T09:45:19Z",
  "additions": 11,
  "deletions": 52,
  "changedFilesIfAvailable": 7,
  "tarballUrl": "https://codeload.github.com/harness/harness/legacy.tar.gz/c1f56a0e...",
  "zipballUrl": "https://codeload.github.com/harness/harness/legacy.zip/c1f56a0e...",
  "statusCheckRollup": { "state": "FAILURE" },
  "repository": { "id": "MDEwOlJlcG9zaXRvcnkxNjYwNzg5OA==", "name": "harness" }
}
```
