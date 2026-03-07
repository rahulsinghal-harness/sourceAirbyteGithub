# Source GitHub Organization (GraphQL)

Custom Airbyte connector that fetches GitHub organization data via the GraphQL API v4.

## Streams

### `repo_details` (incremental)

Full repository metadata for every repo in the org. Supports incremental sync on `updatedAt` (client-side filtering).

**Fields:** `id`, `name`, `nameWithOwner`, `url`, `homepageUrl`, `sshUrl`, `createdAt`, `updatedAt`, `pushedAt`, `description`, `isPrivate`, `isFork`, `isArchived`, `primary_language_name`, `latest_release_name`, `latest_release_tag`, `latest_release_published_at`, `readme_text`, `readme_byte_size`, `codeowners_text`, `codeowners_byte_size`, `languages` (array of `{size, node: {name}}`)

**Pagination:** Cursor-based on `organization.repositories`, page size 100.

**Cost:** ~105 points/page.

### `releases_details` (full_refresh)

Latest 10 releases per repo. Repos with zero releases are filtered out.

**Fields:** `id`, `name`, `releases` (array of `{name, tagName, publishedAt, description, url}`)

**Pagination:** Cursor-based on `organization.repositories`, page size 100.

**Cost:** ~11 points/page.

### `team_repositories` (full_refresh, grouped)

One record per team with all team metadata and a `repositories` array containing every repo the team has access to, with permission levels.

**Team fields:** `id`, `name`, `slug`, `combinedSlug`, `description`, `createdAt`, `updatedAt`, `avatarUrl`, `privacy`, `notificationSetting`, `url`, `membersUrl`, `reviewRequestDelegationEnabled`, `reviewRequestDelegationAlgorithm`, `reviewRequestDelegationMemberCount`, `reviewRequestDelegationNotifyTeam`, `viewerCanAdminister`

**Repo fields (per entry):** `permission`, `repo_name`, `repo_url`, `is_private`

**Implementation:** Custom Python stream with hybrid approach -- one bulk nested query fetches teams + first 100 repos per team, then conditional follow-up pagination only for teams with >100 repos.

**Cost:** ~101 points for up to 100 teams in a single call, plus ~1 point per follow-up call.

## Configuration

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `access_token` | string (secret) | Yes | GitHub PAT with `read:org`, `repo` scopes |
| `org_name` | string | Yes | GitHub organization login name |

## Rate Limits

GitHub GraphQL: 5,000 points/hour per PAT. Point cost = `max(1, total_requested_nodes / 100)`.

For a 500-repo, 50-team org: ~631 total points (12.6% of hourly budget).

## Build and Run

```bash
# Build
docker build -t rahulsinghal-harness/sourceairbytegithub:0.0.1 .

# Spec
docker run --rm rahulsinghal-harness/sourceairbytegithub:0.0.1 spec

# Discover
docker run --rm -v $(pwd)/secrets:/secrets \
  rahulsinghal-harness/sourceairbytegithub:0.0.1 discover \
  --config /secrets/config.json

# Read
docker run --rm -v $(pwd)/secrets:/secrets \
  rahulsinghal-harness/sourceairbytegithub:0.0.1 read \
  --config /secrets/config.json \
  --catalog /secrets/catalog.json
```

**secrets/config.json:**
```json
{
  "access_token": "ghp_xxxxxxxxxxxx",
  "org_name": "your-github-org"
}
```

## k8s-agent Integration

### Step 1: Discover Catalog

```
POST /api/v1/catalog
```
```json
{
  "source_connector_image": "rahulsinghal-harness/sourceairbytegithub",
  "source_connector_tag": "0.0.1",
  "config": {
    "access_token": "ghp_xxxxxxxxxxxx",
    "org_name": "your-github-org"
  }
}
```

### Step 2: Create Integration

Use the `json_schema` from the discover response in each stream:

```
POST /api/v1/integrations
```
```json
{
  "identifier": "github-org-data",
  "airbyte_connector_image": "rahulsinghal-harness/sourceairbytegithub",
  "airbyte_connector_tag": "0.0.1",
  "airbyte_connector_config": {
    "access_token": "ghp_xxxxxxxxxxxx",
    "org_name": "your-github-org"
  },
  "airbyte_connector_catalog": {
    "streams": [
      {
        "stream": {
          "name": "repo_details",
          "json_schema": "<FROM STEP 1 DISCOVERY>",
          "supported_sync_modes": ["full_refresh", "incremental"],
          "source_defined_cursor": true,
          "default_cursor_field": ["updatedAt"]
        },
        "sync_mode": "incremental",
        "destination_sync_mode": "append",
        "cursor_field": ["updatedAt"]
      },
      {
        "stream": {
          "name": "releases_details",
          "json_schema": "<FROM STEP 1 DISCOVERY>",
          "supported_sync_modes": ["full_refresh"]
        },
        "sync_mode": "full_refresh",
        "destination_sync_mode": "overwrite"
      },
      {
        "stream": {
          "name": "team_repositories",
          "json_schema": "<FROM STEP 1 DISCOVERY>",
          "supported_sync_modes": ["full_refresh"]
        },
        "sync_mode": "full_refresh",
        "destination_sync_mode": "overwrite"
      }
    ]
  },
  "airbyte_connector_state": [],
  "enabled": true,
  "sync_interval_in_mins": 60
}
```

## Architecture

- **Base image:** `airbyte/source-declarative-manifest:6.36.3`
- **Declarative streams:** `repo_details`, `releases_details` (YAML manifest)
- **Custom Python stream:** `team_repositories` (hybrid nested query + pagination)
- **Docker image:** `rahulsinghal-harness/sourceairbytegithub:0.0.1`
