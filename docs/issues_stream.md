# `issues` stream

Org-scoped stream that loads **GitHub issues** via the GraphQL **`search`** API (`type: ISSUE`). It is only registered when **`org_name`** is set in the connector config.

## Search query (built at runtime)

- Shape: `org:"<org_name>" is:issue updated:>=YYYY-MM-DD sort:updated-desc`
- **`updated:`** uses a **UTC calendar date** (GitHub search is date-granular, not sub-day).
- **`sort:updated-desc`**: newest-updated issues first so the **1000-record cap** means the **1000 most recently updated** matches in the window, not an arbitrary slice.
- Org is embedded as `org:"…"` with quotes escaped inside the org name if needed.

## Time window

- **`start_date`** (default `1970-01-01T00:00:00Z`, same as other streams): parsed to a **UTC date**.
- **`rolling_floor`**: today (UTC) minus **14 days** (fixed in code, not configurable).
- **Lower bound for `updated:>=`**:
  - **Full refresh**, or **incremental with no `updatedAt` in state** (first incremental run):  
    `max(start_date_utc_date, rolling_floor)` — at most the last 14 days, but not before `start_date` when that date is *newer* than `rolling_floor`.
  - **Incremental with state**:  
    `max(start_date_utc_date, cursor_utc_date)` where `cursor_utc_date` is the UTC **date** part of `stream_state["updatedAt"]`.

## Sync modes

- **`full_refresh`**: Re-queries the bounded window above; does **not** use stream state for the search lower bound.
- **`incremental`**: Cursor field **`updatedAt`**. State is advanced with `max` of previous and latest emitted `updatedAt` (same idea as `repo_details`).

### Client-side filter on incremental (aligned with `repo_details`)

Search `updated:>=DATE` is date-granular; the stream still applies a **timestamp** filter like **`repo_details`**: emit when **`updatedAt` is not strictly before the cursor** — i.e. **`updatedAt` ≥ cursor** (equality included).

1. Relies on **`sort:updated-desc`** so results are newest-first.
2. **Stops pagination** the first time it sees an issue with **`updatedAt` < cursor** (everything remaining in the list is older).
3. **Emits** every issue with **`updatedAt` ≥ cursor** while incremental state is present (same idea as `repo_details` skipping only `updatedAt < effective_cursor`).

## Pagination and limits

- GraphQL **`first`**: **50** per request (`min(50, 1000 - already_emitted)` on the tail).
- **Hard cap**: **1000** emitted records per sync.
- **Warnings**: If the sync stops at **1000** while **`hasNextPage`** is still true (truncation).

## Record shape (GraphQL → Airbyte)

Per issue: `id`, `number`, `createdAt`, `updatedAt`, `closedAt`, `url`, `resourcePath`, `title`, `body` (markdown, may be null or empty), `author` (`{ login }`), `state`, `issueType` (`{ name }`, may be null), `repository` (`id`, `name`).

`publishedAt` is intentionally **not** requested (not reliably available on the `Issue` type in the public schema).

## Operational notes

- **GitHub search** is limited to roughly the **first 1000** matches per query; this stream aligns with that by design.
- Very large gaps between incremental runs can still produce many candidates before the early-stop condition; the **1000** cap always applies.
