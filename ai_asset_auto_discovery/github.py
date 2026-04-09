import asyncio
import json
import logging
import time
from dataclasses import dataclass, field

import aiohttp

from auth import create_app_jwt, INSTALLATION_TOKEN_URL, INSTALLATION_TOKEN_HEADERS, TOKEN_CACHE_TTL

logger = logging.getLogger(__name__)

GRAPHQL_URL = "https://api.github.com/graphql"

# Blob/tree fragments reused across all queries
_BLOB_FIELDS = "... on Blob { text }"
_TREE_FIELDS = "... on Tree { entries { name type } }"

# Probes 3 fixed files + 3 directory listings per repo.
# For non-plugin repos: .claude/ subdirs have standalone definitions.
# For plugin/marketplace repos: follow-up calls discover children via _probe_plugin_children.
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
        jwt_token = create_app_jwt(self._app_id, self._private_key)

        async with aiohttp.ClientSession() as session:
            url = INSTALLATION_TOKEN_URL.format(installation_id=self._installation_id)
            async with session.post(url, headers={
                "Authorization": f"Bearer {jwt_token}",
                **INSTALLATION_TOKEN_HEADERS,
            }) as resp:
                resp.raise_for_status()
                data = await resp.json()

        self._installation_token = data["token"]
        self._installation_token_expires = time.time() + TOKEN_CACHE_TTL
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

            # Marketplace: parse to discover plugin subdirs, probe them in follow-up
            if is_marketplace:
                marketplace_paths = self._marketplace_pending_paths(files[".claude-plugin/marketplace.json"])
                pending_paths.extend(marketplace_paths)

            if not files and not pending_paths:
                continue

            if pending_paths:
                fetched = await self._fetch_files(owner, name, branch, pending_paths, include_history=True)
                files.update(fetched)

            # For standalone plugin repos, probe skills/agents/commands (respecting custom paths)
            if is_plugin and not is_marketplace:
                plugin_content = files.get(".claude-plugin/plugin.json", "")
                child_paths = await self._probe_plugin_children(
                    owner, name, branch, "", plugin_content
                )
                if child_paths:
                    fetched2 = await self._fetch_files(owner, name, branch, child_paths, include_history=True)
                    files.update(fetched2)

            # For marketplace repos, probe plugin subdirs for skills/agents/commands
            if is_marketplace:
                marketplace_content = files.get(".claude-plugin/marketplace.json", "")
                child_paths = await self._probe_marketplace_children(
                    owner, name, branch, marketplace_content, files
                )
                if child_paths:
                    fetched3 = await self._fetch_files(owner, name, branch, child_paths, include_history=True)
                    files.update(fetched3)

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

    @staticmethod
    def _normalize_source(source: str) -> str:
        """Strip './' prefix and trailing '/' from marketplace plugin source paths."""
        if source.startswith("./"):
            source = source[2:]
        return source.strip("/")

    def _parse_marketplace_sources(self, marketplace_content: str) -> list[str]:
        """Parse marketplace.json and return normalized source paths."""
        try:
            data = json.loads(marketplace_content)
        except json.JSONDecodeError:
            return []
        sources = []
        for plugin_entry in data.get("plugins", []):
            source = plugin_entry.get("source", "")
            if not source or source.startswith("http") or source.startswith("npm:"):
                continue
            sources.append(self._normalize_source(source))
        return sources

    def _marketplace_pending_paths(self, marketplace_content: str) -> list[str]:
        """Parse marketplace.json and return plugin.json paths to probe for each plugin subdir."""
        sources = self._parse_marketplace_sources(marketplace_content)
        return [f"{source}/.claude-plugin/plugin.json" for source in sources]

    @staticmethod
    def _parse_custom_dirs(plugin_content: str, prefix: str) -> dict[str, list[str]]:
        """Parse plugin.json and return custom directory paths per component type.

        Returns a dict like {"skills": ["custom/skills/"], "agents": ["agents/feature-dev/"], ...}.
        If a field is absent, it won't be in the dict (caller should fall back to defaults).
        If a field is present, only the declared paths are returned (replaces defaults).
        """
        if not plugin_content:
            return {}
        try:
            data = json.loads(plugin_content)
        except json.JSONDecodeError:
            return {}

        result: dict[str, list[str]] = {}
        for field in ("skills", "agents", "commands"):
            explicit = data.get(field)
            if explicit is None:
                continue
            if isinstance(explicit, str):
                explicit = [explicit]
            if not isinstance(explicit, list):
                continue
            dirs: list[str] = []
            for entry in explicit:
                entry = str(entry)
                if entry.startswith("./"):
                    entry = entry[2:]
                full = f"{prefix}/{entry}" if prefix else entry
                if full.endswith(".md"):
                    # Explicit file path — we'll fetch it directly, not via tree listing
                    # These are handled separately
                    continue
                if not full.endswith("/"):
                    full += "/"
                dirs.append(full)
            if dirs:
                result[field] = dirs
        return result

    @staticmethod
    def _collect_explicit_files(plugin_content: str, prefix: str) -> list[str]:
        """Extract explicitly declared .md file paths from plugin.json (not directories)."""
        if not plugin_content:
            return []
        try:
            data = json.loads(plugin_content)
        except json.JSONDecodeError:
            return []

        paths: list[str] = []
        for field in ("skills", "agents", "commands"):
            explicit = data.get(field)
            if explicit is None:
                continue
            if isinstance(explicit, str):
                explicit = [explicit]
            if not isinstance(explicit, list):
                continue
            for entry in explicit:
                entry = str(entry)
                if entry.startswith("./"):
                    entry = entry[2:]
                full = f"{prefix}/{entry}" if prefix else entry
                if full.endswith(".md"):
                    paths.append(full)
        return paths

    async def _probe_plugin_children(
        self, owner: str, name: str, branch: str, prefix: str, plugin_content: str
    ) -> list[str]:
        """Probe skills/agents/commands for a single plugin, respecting custom paths from plugin.json.

        Also checks for README.md at the plugin root.
        """
        custom_dirs = self._parse_custom_dirs(plugin_content, prefix)
        explicit_files = self._collect_explicit_files(plugin_content, prefix)

        # Build tree listing aliases for directories we need to scan
        aliases = []
        alias_map: dict[str, tuple[str, str]] = {}  # alias → (dir_path, component_type)
        alias_idx = 0

        for comp_type in ("skills", "agents", "commands"):
            if comp_type in custom_dirs:
                dirs = custom_dirs[comp_type]
            else:
                # Default directory
                default = f"{prefix}/{comp_type}" if prefix else comp_type
                dirs = [default + "/"]

            for dir_path in dirs:
                clean = dir_path.rstrip("/")
                alias = f"pc_{alias_idx}"
                alias_idx += 1
                alias_map[alias] = (clean, comp_type)
                aliases.append(
                    f'{alias}: object(expression: "{branch}:{clean}") {{ {_TREE_FIELDS} }}'
                )

        # Also check README.md at plugin root
        readme_path = f"{prefix}/README.md" if prefix else "README.md"
        readme_alias = f"pc_{alias_idx}"
        aliases.append(
            f'{readme_alias}: object(expression: "{branch}:{readme_path}") {{ {_BLOB_FIELDS} }}'
        )

        if not aliases:
            return explicit_files

        query = (
            f'query {{ repository(owner: "{owner}", name: "{name}") {{\n'
            + "\n".join(aliases)
            + "\n}}"
        )
        data = await self._query(query)
        repo_data = data.get("repository", {})

        pending: list[str] = list(explicit_files)  # start with explicit .md files

        for alias, (dir_path, comp_type) in alias_map.items():
            tree = repo_data.get(alias)
            if not tree or "entries" not in tree:
                continue
            for entry in tree["entries"]:
                if comp_type == "skills" and entry["type"] == "tree":
                    pending.append(f"{dir_path}/{entry['name']}/SKILL.md")
                elif comp_type != "skills" and entry["type"] == "blob" and entry["name"].endswith(".md"):
                    pending.append(f"{dir_path}/{entry['name']}")

        # If README.md exists, include it so scanner can record the path
        readme_blob = repo_data.get(readme_alias)
        if readme_blob and "text" in readme_blob:
            pending.append(readme_path)

        logger.info("Plugin children to fetch (%s): %s", prefix or "root", pending)
        return pending

    async def _probe_marketplace_children(
        self, owner: str, name: str, branch: str, marketplace_content: str,
        files: dict[str, str] | None = None,
    ) -> list[str]:
        """List skills/agents/commands for each marketplace plugin, respecting custom paths from plugin.json."""
        sources = self._parse_marketplace_sources(marketplace_content)
        if not sources:
            return []

        files = files or {}

        # Build all tree listing aliases across all plugins in one batched query
        aliases = []
        alias_map: dict[str, tuple[str, str, str]] = {}  # alias → (source, dir_path, comp_type)
        readme_aliases: dict[str, str] = {}  # alias → readme_path
        alias_idx = 0

        for source in sources:
            plugin_json_path = f"{source}/.claude-plugin/plugin.json"
            plugin_content = files.get(plugin_json_path, "")
            custom_dirs = self._parse_custom_dirs(plugin_content, source)
            explicit_files = self._collect_explicit_files(plugin_content, source)

            # Stash explicit files to collect later
            if explicit_files:
                # Store in alias_map under a special marker so we can collect them
                for ef in explicit_files:
                    alias = f"mp_{alias_idx}"
                    alias_idx += 1
                    alias_map[alias] = (source, ef, "__explicit__")

            for comp_type in ("skills", "agents", "commands"):
                if comp_type in custom_dirs:
                    dirs = custom_dirs[comp_type]
                else:
                    default = f"{source}/{comp_type}"
                    dirs = [default + "/"]

                for dir_path in dirs:
                    clean = dir_path.rstrip("/")
                    alias = f"mp_{alias_idx}"
                    alias_idx += 1
                    alias_map[alias] = (source, clean, comp_type)
                    aliases.append(
                        f'{alias}: object(expression: "{branch}:{clean}") {{ {_TREE_FIELDS} }}'
                    )

            # README check per plugin
            readme_path = f"{source}/README.md"
            readme_alias = f"mp_{alias_idx}"
            alias_idx += 1
            readme_aliases[readme_alias] = readme_path
            aliases.append(
                f'{readme_alias}: object(expression: "{branch}:{readme_path}") {{ {_BLOB_FIELDS} }}'
            )

        if not aliases:
            return []

        query = (
            f'query {{ repository(owner: "{owner}", name: "{name}") {{\n'
            + "\n".join(aliases)
            + "\n}}"
        )
        data = await self._query(query)
        repo_data = data.get("repository", {})

        pending: list[str] = []

        for alias, (source, dir_path, comp_type) in alias_map.items():
            if comp_type == "__explicit__":
                # dir_path is actually the explicit file path
                pending.append(dir_path)
                continue
            tree = repo_data.get(alias)
            if not tree or "entries" not in tree:
                continue
            for entry in tree["entries"]:
                if comp_type == "skills" and entry["type"] == "tree":
                    pending.append(f"{dir_path}/{entry['name']}/SKILL.md")
                elif comp_type != "skills" and entry["type"] == "blob" and entry["name"].endswith(".md"):
                    pending.append(f"{dir_path}/{entry['name']}")

        # Include README.md files that exist
        for alias, readme_path in readme_aliases.items():
            readme_blob = repo_data.get(alias)
            if readme_blob and "text" in readme_blob:
                pending.append(readme_path)

        logger.info("Marketplace children to fetch: %s", pending)
        return pending

    async def _fetch_files(
        self, owner: str, name: str, branch: str, paths: list[str],
        include_history: bool = False,
    ) -> dict[str, str]:
        """Batch-fetch file contents via aliased GraphQL object expressions.

        If include_history is True, also fetches last commit metadata per file.
        History data is stored under a special key: ``__history__/<path>``.
        """
        result: dict[str, str] = {}
        # Reduce batch size when fetching history to stay under complexity limits
        batch_size = 15 if include_history else 30
        for i in range(0, len(paths), batch_size):
            batch = paths[i:i + batch_size]
            aliases = []
            alias_map: dict[str, str] = {}
            for j, path in enumerate(batch):
                alias = f"f_{j}"
                alias_map[alias] = path
                aliases.append(f'{alias}: object(expression: "{branch}:{path}") {{ {_BLOB_FIELDS} }}')

            if include_history:
                # Piggyback git history queries using aliased ref lookups
                for j, path in enumerate(batch):
                    aliases.append(
                        f'h_{j}: ref(qualifiedName: "refs/heads/{branch}") {{'
                        f'  target {{'
                        f'    ... on Commit {{'
                        f'      history(first: 1, path: "{path}") {{'
                        f'        nodes {{ author {{ name email }} committedDate message }}'
                        f'      }}'
                        f'    }}'
                        f'  }}'
                        f'}}'
                    )

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

            if include_history:
                for j, path in enumerate(batch):
                    hist_data = repo_data.get(f"h_{j}")
                    if hist_data:
                        target = hist_data.get("target", {})
                        nodes = target.get("history", {}).get("nodes", [])
                        if nodes:
                            commit = nodes[0]
                            result[f"__history__/{path}"] = json.dumps({
                                "author_name": commit.get("author", {}).get("name", ""),
                                "author_email": commit.get("author", {}).get("email", ""),
                                "committed_date": commit.get("committedDate", ""),
                                "message": commit.get("message", ""),
                            })

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
