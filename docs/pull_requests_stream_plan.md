# Plan: `pull_requests` Stream (v3 — Nested Repo Approach)

## Context

The connector needs a PR stream. The initial approach used GitHub's GraphQL `search` API, but `search(type: ISSUE)` has a hard 1000-result cap per query — for an org like `harness` (600+ repos), this causes data gaps. The final approach uses nested `organization → repositories → pullRequests`, which has no org-wide cap — each repo's `pullRequests` connection paginates fully.

**Key requirement: zero data gaps from start_date.** If a repo has 11,000 PRs since start_date, we must eventually get all of them across multiple syncs.

## Why Not Search API?

GitHub's `search(type: ISSUE)` returns a maximum of 1000 results per query, even with pagination. `endCursor` beyond 1000 returns 0 nodes. For an org with 300+ repos, 1000 PRs can easily be created in a single day, so the search API cannot guarantee complete data.

## Strategy: DESC Order, Per-Repo Cursor, Two Phases

Each repo goes through two phases:

### Phase 1: Backfill (repo not fully synced to start_date)

Fetch PRs in DESC order (newest first). Cap at 5000 per repo per sync. Store `endCursor` for resumption.

```
Sync 1: Repo A — fetch 5000 PRs DESC from top
         Save: backfill_cursor (endCursor after 5000th PR)
         Save: updatedAt = MAX(updatedAt) from batch
         Save: backfill_low = oldest updatedAt in batch
         backfill_low > start_date → still backfilling

Sync 2: Repo A — quick incremental check from top (catch newly updated PRs)
         Then resume backfill from backfill_cursor
         Fetch next 5000 PRs
         If reach start_date → backfill complete, clear backfill_cursor

Sync 3+: Same pattern until backfill reaches start_date
```

### Phase 2: Incremental (repo fully synced)

Once backfill is complete, fetch PRs DESC from the top. Stop when `updatedAt <= stored updatedAt`. These are all new/updated PRs since last sync.

```
Sync N: Repo A — fetch PRs DESC from top
         Stop when updatedAt <= stored updatedAt (early stop)
         Update updatedAt cursor
         If no new PRs: 0 emitted (efficient — stops on first inline page)
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
- `backfill_low`: lowest updatedAt seen during backfill (to know progress toward start_date)

## GraphQL Queries

### Bulk Query (50 repos, 10 PRs inline per repo)

```graphql
query($orgName: String!, $repoCursor: String) {
  organization(login: $orgName) {
    repositories(first: 50, after: $repoCursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id name
        pullRequests(first: 10, orderBy: {field: UPDATED_AT, direction: DESC}) {
          pageInfo { hasNextPage endCursor }
          nodes {
            id number createdAt updatedAt closedAt mergedAt publishedAt
            url resourcePath permalink title state isDraft closed locked
            merged lastEditedAt
            author { login }
            mergedBy { login }
          }
        }
      }
    }
  }
}
```

- 10 PRs inline (not 25) because 50 repos × 25 PRs exceeds GitHub's response size limit
- Repo `id` and `name` come from parent node — attached to each PR record in code

### Follow-up Query (single repo, 100 PRs per page)

```graphql
query($orgName: String!, $repoName: String!, $prCursor: String) {
  organization(login: $orgName) {
    repository(name: $repoName) {
      pullRequests(first: 100, after: $prCursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
        pageInfo { hasNextPage endCursor }
        nodes { ...same PR fields... }
      }
    }
  }
}
```

## Sync Mode

**Incremental only** — full_refresh not supported for this stream.

## Rate Limit Handling

- All GraphQL calls wrapped in try/except
- On error: stop processing, yield whatever was fetched so far
- Save cursors for repos fully processed before the error
- Partially processed repos: cursor NOT updated (re-processed next sync)
- Idempotent — downstream upserts on PR `id`

## Edge Cases

| Case | Handling |
|------|----------|
| Repo with >5000 PRs since start_date | Backfill across multiple syncs via backfill_cursor |
| New repo added to org | No cursor → treated as first run |
| Repo deleted/archived | Stale cursor, never read |
| No new PRs in a repo | Phase 2 stops on first inline page, 0 emitted |
| Rate limit mid-sync | Save completed repos, resume next sync |
| PR updated during backfill | Caught by incremental check at start of each phase 1 sync |
| endCursor drift between syncs (DESC) | No duplicates, no gaps. Cursor is value-based (encodes `updatedAt` + node ID), so position shifts don't affect it. PRs that move up (got updated) are caught by the incremental check at the start of each sync. |
| Back-to-back sync (no changes) | 0 PRs re-emitted |

## Files Changed

- **`streams/pull_requests.py`** — full rewrite (nested repo approach)
- **`source.py`** — no changes needed (already registered)

## Tasks

### Implementation
- [x] Write BULK_QUERY (50 repos, 10 PRs inline)
- [x] Write FOLLOWUP_QUERY (single repo, 100 PRs per page)
- [x] Implement per-repo cursor state model (updatedAt + backfill_cursor + backfill_low)
- [x] Incremental only (no full_refresh support)
- [x] Implement first run (DESC from top, cap 5000, save state)
- [x] Implement phase 1 — backfill resumption from backfill_cursor + incremental check from top
- [x] Implement phase 2 — early stop on updatedAt
- [x] Attach repo id/name to PR records from parent node
- [x] Handle rate limit — yield partial, save completed repo cursors
- [x] Update this doc with final design

### Manual Testing (run against real GitHub org: harness)

#### Test 1: Incremental First Run — Recent Start Date

**What it tests:** First-ever sync with a recent start_date where all repos have fewer than 5000 PRs. Every repo should fully sync in one pass — no backfill_cursor should remain.

**Config:** `org=harness, start_date=2026-04-01, sync_mode=incremental, stream_state={}`

**Results:**
- [x] 76 PRs emitted across 603 repos
- [x] All 603 repos fully synced (backfill_cursor absent for all)
- [x] All fields present in records (id, number, updatedAt, state, author, mergedBy, repository, etc.)
- [x] Top repos: mcp-server (19), helm-charts (15), Salesforce-Production-00 (7)
- [x] Sync time: ~2 minutes

**Why it matters:** Confirms the happy path — bulk query paginates all repos, inline PRs are sufficient for small repos, state is saved correctly with only `updatedAt` (no backfill_cursor).

---

#### Test 2: Back-to-Back Incremental — No New Data

**What it tests:** Running a second sync immediately after the first, with no new PRs created in between. Phase 2 logic should detect that the top PR's `updatedAt` is <= the stored cursor and emit nothing.

**Config:** `org=harness, start_date=2026-04-01, sync_mode=incremental, stream_state=<state from Test 1>`

**Results:**
- [x] 0 PRs re-emitted
- [x] Sync time: ~2 minutes (scans all repos but stops on first inline page for each)

**Why it matters:** Confirms that incremental sync is efficient when no data has changed. Each repo's inline 10 PRs are enough to detect "nothing new" without any follow-up queries.

---

#### Test 3: Incremental First Run — Old Start Date (Backfill Scenario)

**What it tests:** First-ever sync with start_date=2022-02-01. Some repos have many thousands of PRs since then and will hit the 5000 cap. These repos should save backfill_cursor for resumption in the next sync.

**Config:** `org=harness, start_date=2022-02-01, sync_mode=incremental, stream_state={}`

**Results:**
- [x] 40,183 PRs emitted total
- [x] 600 repos fully synced (all PRs since start_date fetched in one pass)
- [x] 3 repos still backfilling (hit 5000 cap):

| Repo | PRs emitted | backfill_low reached | Total PRs in repo |
|------|-------------|---------------------|--------------------|
| harness-core | 5,000 | 2023-12-18 | large |
| harness-core-ui | 5,000 | 2023-11-30 | large |
| developer-hub | 5,000 | 2024-06-10 | 11,844 |

- [x] Top repos: harness-core (5000), harness-core-ui (5000), developer-hub (5000), hce-saas (3246), canary (2033)
- [x] Sync time: ~5-6 minutes

**Why it matters:** Confirms that the 5000 cap works correctly for large repos. The 3 backfilling repos have `backfill_cursor` and `backfill_low` saved — they'll resume in subsequent syncs until they reach start_date. The other 600 repos completed fully.

---

#### Test 4: Backfill Resumption (Phase 1 with Existing Cursor)

**What it tests:** Simulates a second sync for a repo that was mid-backfill. Uses developer-hub with a synthetic state (backfill_cursor after 10 PRs, backfill_low=2026-03-19). The sync should: (1) do a quick incremental check from the top to catch newly updated PRs, then (2) resume backfill from the stored endCursor.

**Config:** `org=harness, start_date=2022-02-01, sync_mode=incremental, stream_state=<synthetic state with developer-hub backfill_cursor>`

**Results:**
- [x] developer-hub emitted 5,000 more PRs (resumed from cursor)
- [x] backfill_low moved from 2026-03-19 → 2024-06-07 (progress toward start_date)
- [x] backfill_cursor still present (developer-hub has 11,844 PRs — needs ~3 syncs total to reach 2022-02-01)
- [x] 40,183 total PRs across all repos
- [x] 3 repos still backfilling
- [x] Sync time: ~5-6 minutes

**Why it matters:** Confirms that backfill resumption works — the endCursor picks up where it left off, and backfill_low progresses toward start_date with each sync. Also confirms the incremental check at the start of phase 1 catches any PRs updated between syncs.
