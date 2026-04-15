# Plan: `issues` Stream (Nested Repo Approach)

## Context

The connector needs an issues stream. The initial approach used GitHub's GraphQL `search` API, but `search(type: ISSUE)` has a hard 1000-result cap per query — for an org like `harness` (600+ repos), this causes data gaps. The final approach uses nested `organization → repositories → issues`, which has no org-wide cap — each repo's `issues` connection paginates fully.

**Key requirement: zero data gaps within the lookback window.** If a repo has 6000 issues updated in 30 days, we must eventually get all of them across multiple syncs.

## Why Not Search API?

GitHub's `search(type: ISSUE)` returns a maximum of 1000 results per query, even with pagination. `endCursor` beyond 1000 returns 0 nodes. For an org with 600+ repos, 1000 issues can easily be updated in a single day, so the search API cannot guarantee complete data.

## Strategy: DESC Order, Per-Repo Cursor, Two Phases

Each repo goes through two phases:

### Phase 1: Backfill (repo not fully synced to 30-day boundary)

Fetch issues in DESC order (newest first). Cap at 5000 per repo per sync. Store `endCursor` for resumption.

```
Sync 1: Repo A — fetch 5000 issues DESC from top
         Save: backfill_cursor (endCursor after 5000th issue)
         Save: updatedAt = MAX(updatedAt) from batch
         Save: backfill_low = oldest updatedAt in batch
         backfill_low > 30-day boundary → still backfilling

Sync 2: Repo A — quick incremental check from top (catch newly updated issues)
         Then resume backfill from backfill_cursor
         Fetch next 5000 issues
         If reach 30-day boundary → backfill complete, clear backfill_cursor

Sync 3+: Same pattern until backfill reaches 30-day boundary
```

### Phase 2: Incremental (repo fully synced)

Once backfill is complete, fetch issues DESC from the top. Stop when `updatedAt <= stored updatedAt`. These are all new/updated issues since last sync.

```
Sync N: Repo A — fetch issues DESC from top
         Stop when updatedAt <= stored updatedAt (early stop)
         Update updatedAt cursor
         If no new issues: 0 emitted (efficient — stops on first inline page)
```

## State Model

Per-repo state, keyed by repo node ID (survives renames):

```json
{
  "repo_cursors": {
    "R_kgDOG1abc": {
      "updatedAt": "2026-04-08T10:00:00Z",
      "backfill_cursor": "Y3Vyc29yOjEwMDA=",
      "backfill_low": "2024-03-15T14:30:00Z"
    }
  }
}
```

- `updatedAt`: highest updatedAt seen (incremental stop condition in phase 2)
- `backfill_cursor`: endCursor for resuming backfill (absent when backfill complete)
- `backfill_low`: lowest updatedAt seen during backfill (to know progress toward 30-day boundary)

## GraphQL Queries

### Bulk Query (50 repos, 10 issues inline per repo)

```graphql
query($orgName: String!, $repoCursor: String) {
  organization(login: $orgName) {
    repositories(first: 50, after: $repoCursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id name
        issues(first: 10, orderBy: {field: UPDATED_AT, direction: DESC}) {
          pageInfo { hasNextPage endCursor }
          nodes {
            id number createdAt updatedAt closedAt
            url resourcePath title
            author { login }
            state
            issueType { name }
            repository { id name }
          }
        }
      }
    }
  }
}
```

- 10 issues inline (not 25) because 50 repos × 25 issues exceeds GitHub's response size limit
- `repository { id name }` fetched inside the issue node itself (not from parent)

### Follow-up Query (single repo, 50 issues per page)

```graphql
query($orgName: String!, $repoName: String!, $issueCursor: String) {
  organization(login: $orgName) {
    repository(name: $repoName) {
      issues(first: 50, after: $issueCursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
        pageInfo { hasNextPage endCursor }
        nodes { ...same issue fields including repository { id name }... }
      }
    }
  }
}
```

## Fields

```
id, number, createdAt, updatedAt, closedAt,
url, resourcePath, title,
author { login }, state, issueType { name },
repository { id, name }
```

`body` field intentionally excluded.

## Sync Mode

**Incremental only** — full_refresh not supported for this stream.

## Rate Limit Handling

- All GraphQL calls wrapped in try/except
- On error: stop processing, yield whatever was fetched so far
- Save cursors for repos fully processed before the error
- Partially processed repos: cursor NOT updated (re-processed next sync)
- Idempotent — downstream upserts on issue `id`

## Edge Cases

| Case | Handling |
|------|----------|
| Repo with >5000 issues since 30-day boundary | Backfill across multiple syncs via backfill_cursor |
| New repo added to org | No cursor → treated as first run |
| Repo deleted/archived | Stale cursor, never read |
| No new issues in a repo | Phase 2 stops on first inline page, 0 emitted |
| Rate limit mid-sync | Save completed repos, resume next sync |
| Issue updated during backfill | Caught by incremental check at start of each phase 1 sync |
| endCursor drift between syncs (DESC) | No duplicates, no gaps. Cursor is value-based (encodes `updatedAt` + node ID), so position shifts don't affect it. Issues that move up (got updated) are caught by the incremental check at the start of each sync. |
| Back-to-back sync (no changes) | 0 issues re-emitted |

## Constants

| Constant | Value | Notes |
|----------|-------|-------|
| `PAGE_SIZE_REPOS` | 50 | Same as PR stream |
| `PAGE_SIZE_ISSUES_INLINE` | 10 | Same as PR stream (10 inline) |
| `PAGE_SIZE_ISSUES_FOLLOWUP` | 50 | Reduced from 100 for smaller responses |
| `MAX_ISSUES_PER_REPO_PER_SYNC` | 5000 | Same as PR stream |
| `_LOOKBACK_DAYS` | 30 | Same as PR stream |

## Files Changed

- **`streams/issues.py`** — full rewrite (nested repo approach)
- **`source.py`** — no changes needed (already registered)

## Before vs After Comparison

| Scenario | Before (search API) | After (nested) |
|----------|---------------------|----------------|
| Org has 2000 issues in 14 days | 1000 emitted, 1000 missed | All 2000 emitted |
| Back-to-back sync, no changes | 1+ re-emitted (cursor == record) | 0 re-emitted |
| Rate limit mid-sync | Entire sync fails | Partial data saved |
| Repo with 6000 issues in 30 days | N/A (search cap) | 5000 sync 1, 1000 sync 2 |
| Lookback window | 14 days | 30 days |
| `start_date` config | Used as floor | Ignored — always 30 days back |
| `body` field | Included | Removed |
| `repository` field | From search node | From query node (same as PR stream) |

## Tasks

### Implementation
- [x] Rewrite `streams/issues.py` — nested repo approach (copy PR stream pattern)
- [x] BULK_QUERY: 50 repos, 10 issues inline, all fields including `repository { id name }`
- [x] FOLLOWUP_QUERY: single repo, 50 issues per page
- [x] Per-repo cursor state model (updatedAt + backfill_cursor + backfill_low)
- [x] Incremental only (no full_refresh)
- [x] First run: DESC, 30-day lookback, cap 5000/repo
- [x] Phase 1: backfill resumption + incremental check from top
- [x] Phase 2: early stop on updatedAt
- [x] Error recovery: two-layer try/except, partial state saved
- [x] Remove `body` field from schema and query

### Manual Testing (run against real GitHub org: harness)

#### Test 1: Incremental First Run — 30-Day Lookback

**What it tests:** First-ever sync with 30-day lookback where all repos have fewer than 5000 issues. Every repo should fully sync in one pass — no backfill_cursor should remain.

**Config:** `org=harness, sync_mode=incremental, stream_state={}`

**Results:**
- [x] 21 issues emitted across 5 repos
- [x] 603 repos scanned, all fully synced (backfill_cursor absent for all)
- [x] All fields present: id, number, createdAt, updatedAt, closedAt, url, resourcePath, title, author, state, issueType, repository
- [x] `repository { id, name }` present on all records — comes from query node
- [x] No `body` field in any record
- [x] Sync time: ~2 minutes

**Why it matters:** Confirms the happy path — bulk query paginates all repos, inline issues are sufficient for small repos, state is saved correctly with only `updatedAt` (no backfill_cursor).

---

#### Test 2: Back-to-Back Incremental — No New Data

**What it tests:** Running a second sync immediately after the first, with no new issues created in between. Phase 2 logic should detect that the top issue's `updatedAt` is <= the stored cursor and emit nothing.

**Config:** `org=harness, sync_mode=incremental, stream_state=<state from Test 1>`

**Results:**
- [x] 0 issues re-emitted
- [x] Sync time: ~2 minutes (scans all repos but stops on first inline page for each)

**Why it matters:** Confirms that incremental sync is efficient when no data has changed. Each repo's inline 10 issues are enough to detect "nothing new" without any follow-up queries.

---

#### Test 3: Validation — Ordering, Duplicates, Fields, State

**What it tests:** Data quality checks on the first run output.

**Results:**
- [x] No duplicate issue IDs
- [x] DESC ordering per repo (updatedAt)
- [x] State consistency: updatedAt = MAX(updatedAt) per repo
- [x] All required fields present on every record
- [x] No `body` field on any record

---

#### Test 4: Bad Token — Graceful Error

**What it tests:** Error handling when authentication fails.

**Results:**
- [x] 0 records emitted
- [x] No crash — graceful error handling via try/except
