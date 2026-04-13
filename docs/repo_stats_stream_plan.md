# PRD: `repo_stats` Stream — Repository Metrics (GraphQL Only)

## Goal

Add a `repo_stats` full_refresh stream that captures repository-level metrics via a **single bulk GraphQL org→repos query** — no per-repo REST or follow-up calls. Covers activity, community health, counts, and infrastructure metrics.

## Constraint

**GraphQL only, bulk query only.** Every metric must be fetchable inline in the paginated `organization.repositories` query. No per-repo API calls.

---

## Metrics We Can Get (All Inline in One Query)

### Popularity & Engagement
| Metric | GraphQL Field | Type |
|--------|--------------|------|
| Stars | `stargazerCount` | integer |
| Forks | `forkCount` | integer |
| Watchers | `watchers { totalCount }` | integer |

### Commit Activity
| Metric | GraphQL Field | Type | Notes |
|--------|--------------|------|-------|
| All-time commits | `allTimeCommits: history { totalCount }` | integer | Default branch only |
| Commits in last 14 days | `recentCommits: history(since: "...") { totalCount }` | integer | Uses aliased `history` with `since` param |

### Issue & PR Counts
| Metric | GraphQL Field | Type |
|--------|--------------|------|
| Total issues | `issues { totalCount }` | integer |
| Open issues | `openIssues: issues(states: OPEN) { totalCount }` | integer |
| Closed issues | `closedIssues: issues(states: CLOSED) { totalCount }` | integer |
| Total PRs | `pullRequests { totalCount }` | integer |
| Open PRs | `openPRs: pullRequests(states: OPEN) { totalCount }` | integer |
| Merged PRs | `mergedPRs: pullRequests(states: MERGED) { totalCount }` | integer |
| Closed PRs | `closedPRs: pullRequests(states: CLOSED) { totalCount }` | integer |

### Branches, Tags & Releases
| Metric | GraphQL Field | Type |
|--------|--------------|------|
| Branch count | `branches: refs(refPrefix: "refs/heads/") { totalCount }` | integer |
| Tag count | `tags: refs(refPrefix: "refs/tags/") { totalCount }` | integer |
| Release count | `releases { totalCount }` | integer |
| Default branch name | `defaultBranchRef { name }` | string/null |

### Community Health
| Metric | GraphQL Field | Type |
|--------|--------------|------|
| Issues enabled | `hasIssuesEnabled` | boolean |
| Wiki enabled | `hasWikiEnabled` | boolean |
| Projects enabled | `hasProjectsEnabled` | boolean |
| Discussions enabled | `hasDiscussionsEnabled` | boolean |
| Security policy enabled | `isSecurityPolicyEnabled` | boolean |
| Code of conduct | `codeOfConduct { key name }` | object/null |
| License | `licenseInfo { name spdxId }` | object/null |
| Topics | `repositoryTopics(first: 20)` | string array |

### Infrastructure & CI/CD
| Metric | GraphQL Field | Type | Notes |
|--------|--------------|------|-------|
| Deployment count | `deployments { totalCount }` | integer | |
| Environment count | `environments { totalCount }` | integer | |
| Package count | `packages { totalCount }` | integer | |
| Projects V2 count | `projectsV2 { totalCount }` | integer | |
| Discussion count | `discussions { totalCount }` | integer | Requires discussions enabled |
| Milestone count | `milestones { totalCount }` | integer | |
| Label count | `labels { totalCount }` | integer | |
| Disk usage | `diskUsage` | integer/null | KB; may need push access |

### Security (may require elevated permissions)
| Metric | GraphQL Field | Type | Notes |
|--------|--------------|------|-------|
| Vulnerability alert count | `vulnerabilityAlerts { totalCount }` | integer/null | Needs admin — **will exclude** to avoid crash |
| Dependency manifest count | `dependencyGraphManifests { totalCount }` | integer/null | May need admin — **will exclude** to avoid crash |

---

## What We CANNOT Get via Bulk GraphQL

| Metric | Why Not | Alternative |
|--------|---------|-------------|
| **Line additions/deletions** | Requires paginating commit nodes per repo | REST `/stats/code_frequency` or GraphQL follow-up |
| **Unique contributors in period** | Requires paginating commit nodes per repo | REST `/stats/contributors` |
| **Traffic (views/clones)** | REST only, write access required | REST `/traffic/views`, `/traffic/clones` |
| **Actions workflow runs/performance** | No GraphQL fields on Repository | REST `/actions/runs` |
| **Network graph** | Visual only, no API | None |
| **Code frequency (weekly adds/dels)** | REST only | REST `/stats/code_frequency` |
| **Punch card** | REST only | REST `/stats/punch_card` |

---

## Final GraphQL Query

```graphql
query($orgName: String!, $after: String, $since: GitTimestamp!) {
  organization(login: $orgName) {
    repositories(first: 50, after: $after) {
      nodes {
        id
        name

        # Popularity
        stargazerCount
        forkCount
        watchers { totalCount }

        # Commits
        defaultBranchRef {
          name
          target {
            ... on Commit {
              allTimeCommits: history { totalCount }
              recentCommits: history(since: $since) { totalCount }
            }
          }
        }

        # Issues & PRs
        issues { totalCount }
        openIssues: issues(states: OPEN) { totalCount }
        closedIssues: issues(states: CLOSED) { totalCount }
        pullRequests { totalCount }
        openPRs: pullRequests(states: OPEN) { totalCount }
        mergedPRs: pullRequests(states: MERGED) { totalCount }
        closedPRs: pullRequests(states: CLOSED) { totalCount }

        # Branches, Tags, Releases
        branches: refs(refPrefix: "refs/heads/", first: 0) { totalCount }
        tags: refs(refPrefix: "refs/tags/", first: 0) { totalCount }
        releases { totalCount }

        # Community Health
        hasIssuesEnabled
        hasWikiEnabled
        hasProjectsEnabled
        hasDiscussionsEnabled
        isSecurityPolicyEnabled
        codeOfConduct { key name }
        licenseInfo { name spdxId }
        repositoryTopics(first: 20) { nodes { topic { name } } }

        # Infrastructure
        deployments { totalCount }
        environments { totalCount }
        packages { totalCount }
        projectsV2 { totalCount }
        discussions { totalCount }
        milestones { totalCount }
        labels { totalCount }
        diskUsage
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
```

**Variables:** `orgName`, `after` (cursor), `since` (ISO 8601, computed as `now - 14 days`)

---

## Output Schema (Flattened)

```
id                      string        Primary key (GitHub node ID)
name                    string        Repo name

# Popularity
stargazerCount          integer
forkCount               integer
watcherCount            integer

# Commits
defaultBranchName       string/null
allTimeCommitCount      integer       0 if no default branch
recentCommitCount       integer       Commits in last 14 days; 0 if no default branch

# Issues & PRs
issueCount              integer
openIssueCount          integer
closedIssueCount        integer
pullRequestCount        integer
openPRCount             integer
mergedPRCount           integer
closedPRCount           integer

# Branches, Tags, Releases
branchCount             integer
tagCount                integer
releaseCount            integer

# Community Health
hasIssuesEnabled        boolean
hasWikiEnabled          boolean
hasProjectsEnabled      boolean
hasDiscussionsEnabled   boolean
isSecurityPolicyEnabled boolean
codeOfConductKey        string/null
codeOfConductName       string/null
licenseName             string/null
licenseSpdxId           string/null
topics                  array[string]

# Infrastructure
deploymentCount         integer
environmentCount        integer
packageCount            integer
projectV2Count          integer
discussionCount         integer
milestoneCount          integer
labelCount              integer
diskUsage               integer/null  (KB)
```

---

## API Cost

- **~10 pages** for 500-repo org (50 repos/page)
- **~10-15 GraphQL points** total (all inline, no follow-ups)
- Well within 5,000 points/hour rate limit

---

## Files to Modify

| File | Action |
|------|--------|
| `streams/repo_stats.py` | **Create** — query, schema, flatten, stream class |
| `source.py` | **Edit** — import + register in `org_name` block |

## Edge Cases

- **Empty repos** (no default branch): `allTimeCommitCount` = 0, `recentCommitCount` = 0, `defaultBranchName` = null
- **Discussions disabled**: `discussionCount` may return 0 or error — need to test; may exclude if it crashes
- **`diskUsage` null**: may require push access on some repos
- **`environments`/`packages`/`projectsV2`**: may return 0 or error if feature not enabled — need to test

## Task List

- [ ] Finalize which fields survive testing (some may error on certain repos)
- [ ] Create `streams/repo_stats.py`
- [ ] Register stream in `source.py`
- [ ] Test with real org — verify all fields return without errors
- [ ] Remove any fields that cause GraphQL errors
