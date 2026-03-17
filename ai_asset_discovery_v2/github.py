import asyncio
import json
import logging
import time
from dataclasses import dataclass, field

import aiohttp

logger = logging.getLogger(__name__)

GRAPHQL_URL = "https://api.github.com/graphql"

# Blob/tree fragments reused across all queries
_BLOB_FIELDS = "... on Blob { text }"
_TREE_FIELDS = "... on Tree { entries { name type } }"

# Probes 3 fixed files + 6 directory listings per repo.
# For non-plugin repos: .claude/ subdirs have standalone definitions.
# For plugin repos: root-level skills/agents/commands hold plugin children.
_REPO_PROBE_BODY = """
    name
    owner { login }
    defaultBranchRef { name }
    settings: object(expression: "HEAD:.claude/settings.json") { %(blob)s }
    pluginJson: object(expression: "HEAD:.claude-plugin/plugin.json") { %(blob)s }
    marketplaceJson: object(expression: "HEAD:.claude-plugin/marketplace.json") { %(blob)s }
    claudeCommands: object(expression: "HEAD:.claude/commands") { %(tree)s }
    claudeSkills: object(expression: "HEAD:.claude/skills") { %(tree)s }
    claudeAgents: object(expression: "HEAD:.claude/agents") { %(tree)s }
    rootSkills: object(expression: "HEAD:skills") { %(tree)s }
    rootAgents: object(expression: "HEAD:agents") { %(tree)s }
    rootCommands: object(expression: "HEAD:commands") { %(tree)s }
""" % {"blob": _BLOB_FIELDS, "tree": _TREE_FIELDS}

_ORG_PROBE_QUERY = """
query($org: String!, $cursor: String) {
  organization(login: $org) {
    repositories(first: 20, after: $cursor, isArchived: false) {
      nodes { %s }
      pageInfo { hasNextPage endCursor }
    }
  }
}
""" % _REPO_PROBE_BODY


@dataclass
class RepoFiles:
    owner: str
    name: str
    branch: str
    files: dict[str, str] = field(default_factory=dict)


class GitHubGraphQLClient:

    def __init__(
        self,
        token: str | None = None,
        app_id: str | None = None,
        private_key: str | None = None,
        installation_id: str | None = None,
    ):
        self._token = token
        self._app_id = app_id
        self._private_key = private_key
        self._installation_id = installation_id
        self._installation_token: str | None = None
        self._installation_token_expires: float = 0
        self._session: aiohttp.ClientSession | None = None
        self._calls = 0

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            token = await self._resolve_token()
            self._session = aiohttp.ClientSession(headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            })
        return self._session

    async def _resolve_token(self) -> str:
        if self._token:
            return self._token
        if self._installation_token and time.time() < self._installation_token_expires:
            return self._installation_token
        return await self._create_installation_token()

    async def _create_installation_token(self) -> str:
        import jwt

        now = int(time.time())
        payload = {"iat": now - 60, "exp": now + 600, "iss": self._app_id}
        jwt_token = jwt.encode(payload, self._private_key, algorithm="RS256")

        async with aiohttp.ClientSession() as session:
            url = f"https://api.github.com/app/installations/{self._installation_id}/access_tokens"
            async with session.post(url, headers={
                "Authorization": f"Bearer {jwt_token}",
                "Accept": "application/vnd.github+json",
            }) as resp:
                resp.raise_for_status()
                data = await resp.json()

        self._installation_token = data["token"]
        self._installation_token_expires = time.time() + 3500
        return self._installation_token

    async def scan_org(self, org: str) -> list[RepoFiles]:
        logger.info(f"Scanning {org=}")
        probes = await self._probe_org(org)
        return await self._resolve_probes(probes)

    async def scan_repos(self, repos: list[str]) -> list[RepoFiles]:
        logger.info(f"Scanning {repos=}")
        parsed = []
        for r in repos:
            owner, name = _parse_repo_spec(r)
            if owner and name:
                parsed.append((owner, name))
        probes = await self._probe_repos(parsed)
        return await self._resolve_probes(probes)

    async def _probe_org(self, org: str) -> list[dict]:
        nodes = []
        cursor = None
        while True:
            data = await self._query(_ORG_PROBE_QUERY, {"org": org, "cursor": cursor})
            repos_data = data["organization"]["repositories"]
            nodes.extend(repos_data["nodes"])
            page_info = repos_data["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            cursor = page_info["endCursor"]
            logger.info(f"Probing org {cursor=}")
        logger.info(f"Probed {len(nodes)} repos in org '{org}'")
        return nodes

    async def _probe_repos(self, repos: list[tuple[str, str]]) -> list[dict]:
        """Probe a list of repos by batching into single GraphQL queries."""
        nodes = []
        # Batch ~10 repos per query to stay under complexity limits
        for i in range(0, len(repos), 10):
            batch = repos[i:i + 10]
            aliases = []
            for j, (owner, name) in enumerate(batch):
                aliases.append(f'repo_{j}: repository(owner: "{owner}", name: "{name}") {{ {_REPO_PROBE_BODY} }}')
            query = "query {\n" + "\n".join(aliases) + "\n}"
            data = await self._query(query)
            for j in range(len(batch)):
                node = data.get(f"repo_{j}")
                if node:
                    nodes.append(node)
        return nodes

    async def _resolve_probes(self, probes: list[dict]) -> list[RepoFiles]:
        results = []
        logger.info(probes)
        for node in probes:
            owner = node["owner"]["login"]
            name = node["name"]
            branch_ref = node.get("defaultBranchRef")
            branch = branch_ref["name"] if branch_ref else "main"

            files: dict[str, str] = {}
            pending_paths: list[str] = []

            is_plugin = node.get("pluginJson") is not None
            is_marketplace = node.get("marketplaceJson") is not None

            # Extract fixed file contents from probe
            for alias, path in [
                ("settings", ".claude/settings.json"),
                ("pluginJson", ".claude-plugin/plugin.json"),
                ("marketplaceJson", ".claude-plugin/marketplace.json"),
            ]:
                blob = node.get(alias)
                if blob and "text" in blob:
                    files[path] = blob["text"]

            # Standalone definitions under .claude/ (only when NOT a plugin repo)
            if not is_plugin and not is_marketplace:
                self._collect_dir_entries(node, "claudeCommands", ".claude/commands",
                                         pending_paths, blob_suffix=".md")
                self._collect_skill_entries(node, "claudeSkills", ".claude/skills", pending_paths)
                self._collect_dir_entries(node, "claudeAgents", ".claude/agents",
                                         pending_paths, blob_suffix=".md")

            # Plugin children at root level
            if is_plugin:
                self._collect_skill_entries(node, "rootSkills", "skills", pending_paths)
                self._collect_dir_entries(node, "rootAgents", "agents",
                                         pending_paths, blob_suffix=".md")
                self._collect_dir_entries(node, "rootCommands", "commands",
                                         pending_paths, blob_suffix=".md")

            # Marketplace: parse to discover plugin subdirs, probe them in follow-up
            if is_marketplace:
                marketplace_paths = self._marketplace_pending_paths(files[".claude-plugin/marketplace.json"])
                pending_paths.extend(marketplace_paths)

            if not files and not pending_paths:
                continue

            if pending_paths:
                fetched = await self._fetch_files(owner, name, branch, pending_paths)
                files.update(fetched)

                # For marketplace repos, after fetching plugin.json files from subdirs,
                # we may need another round to get skills/agents/commands in each subdir
                if is_marketplace:
                    extra = self._marketplace_follow_up_paths(files)
                    if extra:
                        fetched2 = await self._fetch_files(owner, name, branch, extra)
                        files.update(fetched2)

            results.append(RepoFiles(owner=owner, name=name, branch=branch, files=files))

        logger.info(f"Resolved {len(results)} repos with AI asset files")
        return results

    def _collect_dir_entries(self, node: dict, alias: str, dir_path: str,
                            pending: list[str], blob_suffix: str = "") -> None:
        tree = node.get(alias)
        if not tree or "entries" not in tree:
            return
        for entry in tree["entries"]:
            if entry["type"] == "blob" and (not blob_suffix or entry["name"].endswith(blob_suffix)):
                pending.append(f"{dir_path}/{entry['name']}")

    def _collect_skill_entries(self, node: dict, alias: str, dir_path: str,
                               pending: list[str]) -> None:
        tree = node.get(alias)
        if not tree or "entries" not in tree:
            return
        for entry in tree["entries"]:
            if entry["type"] == "tree":
                pending.append(f"{dir_path}/{entry['name']}/SKILL.md")

    def _marketplace_pending_paths(self, marketplace_content: str) -> list[str]:
        """Parse marketplace.json and return paths to probe for each plugin subdir."""
        try:
            data = json.loads(marketplace_content)
        except json.JSONDecodeError:
            return []
        paths = []
        for plugin_entry in data.get("plugins", []):
            source = plugin_entry.get("source", "")
            if not source or source.startswith("http") or source.startswith("npm:"):
                continue
            source = source.strip("/")
            paths.append(f"{source}/.claude-plugin/plugin.json")
            # We'll need dir listings too, but GraphQL fetch_files only gets blobs.
            # So we request the dirs as separate probes in the follow-up step.
        return paths

    def _marketplace_follow_up_paths(self, files: dict[str, str]) -> list[str]:
        """After fetching plugin.json for marketplace subdirs, build skill/agent/command paths.

        Since we can't list directories via the blob-fetch query, we re-probe
        the marketplace.json to discover subdirs and request their contents.
        For now, this fetches plugin.json contents — actual dir listing for
        children requires a separate tree query if the marketplace has many plugins.
        """
        # For simplicity, assume marketplace plugin subdirs may have a standard structure.
        # A full implementation would do a tree query per subdir here.
        # TODO: implement tree listing for marketplace plugin subdirs when needed
        return []

    async def _fetch_files(self, owner: str, name: str, branch: str,
                           paths: list[str]) -> dict[str, str]:
        """Batch-fetch file contents via aliased GraphQL object expressions."""
        result: dict[str, str] = {}
        # Batch ~30 files per query to stay under GraphQL complexity limits
        for i in range(0, len(paths), 30):
            batch = paths[i:i + 30]
            aliases = []
            alias_map: dict[str, str] = {}
            for j, path in enumerate(batch):
                alias = f"f_{j}"
                alias_map[alias] = path
                aliases.append(f'{alias}: object(expression: "{branch}:{path}") {{ {_BLOB_FIELDS} }}')

            query = (
                f'query {{ repository(owner: "{owner}", name: "{name}") {{\n'
                + "\n".join(aliases)
                + "\n}}"
            )
            data = await self._query(query)
            repo_data = data.get("repository", {})
            for alias, path in alias_map.items():
                blob = repo_data.get(alias)
                if blob and "text" in blob:
                    result[path] = blob["text"]

        return result

    async def _query(self, query: str, variables: dict | None = None) -> dict:
        session = await self._ensure_session()
        body: dict = {"query": query}
        if variables:
            body["variables"] = variables

        self._calls += 1

        for attempt in range(3):
            async with session.post(
                GRAPHQL_URL,
                json=body,
                timeout=aiohttp.ClientTimeout(total=60),
            ) as resp:
                if resp.status in (502, 503) and attempt < 2:
                    wait = 2 ** attempt
                    logger.warning(f"GraphQL {resp.status}, retrying in {wait}s (attempt {attempt + 1}/3)")
                    await asyncio.sleep(wait)
                    continue
                resp.raise_for_status()
                result = await resp.json()

            if "errors" in result:
                for err in result["errors"]:
                    logger.warning(f"GraphQL error: {err.get('message', err)}")
            return result.get("data", {})

        return {}

    @property
    def api_call_count(self) -> int:
        return self._calls

    async def close(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None


def _parse_repo_spec(spec: str) -> tuple[str, str]:
    """Parse 'owner/repo', 'https://github.com/owner/repo', or 'owner/repo.git' into (owner, name)."""
    spec = spec.strip().rstrip("/")
    if spec.endswith(".git"):
        spec = spec[:-4]
    if spec.startswith(("https://", "http://")):
        from urllib.parse import urlparse
        path = urlparse(spec).path.strip("/")
        parts = path.split("/")
        if len(parts) >= 2:
            return parts[0], parts[1]
        return "", ""
    if "/" in spec:
        owner, name = spec.split("/", 1)
        return owner, name
    return "", ""
