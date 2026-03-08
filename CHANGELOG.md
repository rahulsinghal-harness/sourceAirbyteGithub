# Changelog

All notable changes to this connector are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.0.2] - Unreleased

### Added

- **start_date config:** Optional `start_date` (ISO 8601, e.g. `2025-01-01T00:00:00Z`) applied across all streams. Defaults to `1970-01-01T00:00:00Z` (epoch) when omitted.
  - **repo_details:** First sync only emits repos with `updatedAt >= start_date`; subsequent syncs use cursor state.
  - **releases_details:** Only repo+releases records that have at least one release with `publishedAt >= start_date` are emitted.
  - **team_repositories:** Only teams with `updatedAt >= start_date` are emitted.

### Changed

- **Incremental sync fix:** Upgraded base image from `airbyte/source-declarative-manifest:6.36.3` to `6.54.6` so `repo_details` incremental sync correctly filters by cursor. Subsequent syncs now emit only repos updated since the last run instead of all repos.
- **Output shape (clean nested):**
  - **repo_details:** Nested objects kept for `primaryLanguage`, `latestRelease`, `readme`, `codeowners`. `languages` is now an array of `{ name, size }` (no `edges`/`node`).
  - **team_repositories:** `repositories` array entries now use `name`, `url`, `isPrivate` (Option A) instead of `repo_name`, `repo_url`, `is_private`.
- **releases_details:** Unchanged; still `id`, `name`, `releases: [ { name, tagName, publishedAt, description, url } ]`.

### Breaking

- Schema and record shape changes for **repo_details** and **team_repositories**. Consumers that relied on flat fields (e.g. `primary_language_name`, `readme_text`, `latest_release_name`) or old property names (`repo_name`, `repo_url`, `is_private`) must update to the new nested shape and property names.

## [0.0.1] - Initial release

- Streams: `repo_details` (incremental), `releases_details` (full_refresh), `team_repositories` (full_refresh, grouped by team).
- Config: `access_token`, `org_name`.
