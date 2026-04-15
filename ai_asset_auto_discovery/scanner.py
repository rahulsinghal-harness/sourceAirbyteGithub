"""Pure detection logic: given file contents for a repo, emit AI assets.

No I/O — receives dict[path, content], returns list[AIAsset].
"""

import json
import logging
import os
import re

import yaml

from ai_asset_auto_discovery.schema import AIAsset

logger = logging.getLogger(__name__)

_FRONTMATTER_RE = re.compile(r"^---\s*\n(.*?)\n---\s*\n", re.DOTALL)


def _extract_history(files: dict[str, str]) -> dict[str, dict]:
    """Extract git history metadata from __history__/ pseudo-paths."""
    history: dict[str, dict] = {}
    for key, value in files.items():
        if key.startswith("__history__/"):
            real_path = key[len("__history__/"):]
            try:
                history[real_path] = json.loads(value)
            except json.JSONDecodeError:
                pass
    return history


def _enrich_with_history(assets: list[AIAsset], history: dict[str, dict]) -> None:
    """Add git commit metadata to asset metadata from history data."""
    for asset in assets:
        hist = history.get(asset.source_file)
        if not hist:
            continue
        if hist.get("author_name"):
            asset.metadata["file_author"] = hist["author_name"]
        if hist.get("author_email"):
            asset.metadata["author_email"] = hist["author_email"]
        if hist.get("committed_date"):
            asset.metadata["last_committed_at"] = hist["committed_date"]
        if hist.get("message"):
            asset.metadata["last_commit_message"] = hist["message"]


def scan_repo(repo_name: str, files: dict[str, str], branch: str = "main") -> list[AIAsset]:
    assets: list[AIAsset] = []
    is_plugin = ".claude-plugin/plugin.json" in files
    is_marketplace = ".claude-plugin/marketplace.json" in files

    if ".claude/settings.json" in files:
        assets.extend(_parse_settings(repo_name, files[".claude/settings.json"], branch))

    if not is_plugin and not is_marketplace:
        for path, content in files.items():
            if path.startswith(".claude/commands/") and path.endswith(".md"):
                assets.extend(_parse_tool_file(repo_name, path, content, "command", branch))
            elif path.startswith(".claude/skills/") and path.endswith("/SKILL.md"):
                assets.extend(_parse_tool_file(repo_name, path, content, "skill", branch))
            elif path.startswith(".claude/agents/") and path.endswith(".md"):
                assets.extend(_parse_tool_file(repo_name, path, content, "agent", branch))

    if is_marketplace:
        _scan_marketplace(repo_name, files, assets, branch)
    elif is_plugin:
        _scan_plugin_subtree(repo_name, "", files, assets, branch=branch)

    # Dedup: if a plugin appears as both definition and usage in the same repo, keep definition only
    definition_names: set[str] = set()
    for a in assets:
        if a.asset_type == "plugin" and a.relationship == "definition":
            definition_names.add(a.name)
    if definition_names:
        assets = [a for a in assets if not (
            a.asset_type == "plugin" and a.relationship == "usage" and a.name in definition_names
        )]

    # Enrich assets with git commit metadata (if __history__/ pseudo-paths are present)
    history = _extract_history(files)
    if history:
        _enrich_with_history(assets, history)

    return assets


def _parse_settings(repo_name: str, content: str, branch: str = "main") -> list[AIAsset]:
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return []

    enabled_plugins = data.get("enabledPlugins", {})
    if not isinstance(enabled_plugins, dict):
        return []

    results = []
    for plugin_ref, enabled in enabled_plugins.items():
        if not enabled:
            continue
        if "@" in plugin_ref:
            name, marketplace = plugin_ref.rsplit("@", 1)
        else:
            name = plugin_ref
            marketplace = "unknown"

        results.append(AIAsset(
            asset_type="plugin",
            name=name,
            provider=marketplace,
            category="claude_code",
            confidence=0.95,
            relationship="usage",
            source_repo=repo_name,
            source_file=".claude/settings.json",
            source_branch=branch,
            external_ref=plugin_ref,
            metadata={"marketplace": marketplace},
        ))
    return results


def _scan_marketplace(repo_name: str, files: dict[str, str], assets: list[AIAsset], branch: str = "main") -> None:
    content = files.get(".claude-plugin/marketplace.json", "")
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return

    for plugin_entry in data.get("plugins", []):
        source = plugin_entry.get("source", "")
        if not source or source.startswith("http") or source.startswith("npm:"):
            continue
        if source.startswith("./"):
            source = source[2:]
        source = source.strip("/")
        _scan_plugin_subtree(repo_name, source, files, assets, marketplace_meta=plugin_entry, branch=branch)


def _scan_plugin_subtree(
    repo_name: str,
    prefix: str,
    files: dict[str, str],
    assets: list[AIAsset],
    marketplace_meta: dict | None = None,
    branch: str = "main",
) -> None:
    plugin_json_path = f"{prefix}/.claude-plugin/plugin.json" if prefix else ".claude-plugin/plugin.json"
    plugin_content = files.get(plugin_json_path)

    plugin_name = None
    plugin_id = None
    plugin_asset = None
    plugin_data: dict = {}
    if plugin_content:
        plugin_asset, plugin_data = _parse_plugin_json(repo_name, plugin_json_path, plugin_content, branch)
        if plugin_asset:
            if marketplace_meta:
                plugin_asset.metadata.update({
                    k: v for k, v in marketplace_meta.items()
                    if k in ("author", "description", "keywords", "license") and v
                })
            assets.append(plugin_asset)
            plugin_name = plugin_asset.name
            plugin_id = plugin_asset.asset_id
    if not plugin_asset and marketplace_meta:
        name = marketplace_meta.get("name", os.path.basename(prefix) if prefix else "unknown")
        plugin_asset = AIAsset(
            asset_type="plugin",
            name=name,
            provider="Anthropic",
            category="claude_code",
            confidence=0.90,
            relationship="definition",
            source_repo=repo_name,
            source_file=".claude-plugin/marketplace.json",
            source_branch=branch,
            version=marketplace_meta.get("version"),
            description=marketplace_meta.get("description", ""),
            metadata={"source": "marketplace_entry_only"},
        )
        assets.append(plugin_asset)
        plugin_name = name
        plugin_id = plugin_asset.asset_id

    # Resolve component paths — respects plugin.json custom paths, falls back to defaults
    path_set = set(files.keys())
    skill_paths = _resolve_component_paths(prefix, path_set, plugin_data, "skills", "skills", plugin_name)
    agent_paths = _resolve_component_paths(prefix, path_set, plugin_data, "agents", "agents", plugin_name)
    cmd_paths = _resolve_component_paths(prefix, path_set, plugin_data, "commands", "commands", plugin_name)

    for comp_type, comp_paths in [("skill", skill_paths), ("agent", agent_paths), ("command", cmd_paths)]:
        for path in comp_paths:
            content = files.get(path)
            if not content:
                continue
            for asset in _parse_tool_file(repo_name, path, content, comp_type, branch):
                asset.parent_id = plugin_id
                asset.parent_name = plugin_name
                assets.append(asset)

    # Record README.md path in plugin metadata if it exists in fetched files
    if plugin_asset:
        readme_path = f"{prefix}/README.md" if prefix else "README.md"
        if readme_path in files:
            plugin_asset.metadata["readme_path"] = readme_path


def _discover_dir(dir_prefix: str, component_type: str, path_set: set[str]) -> list[str]:
    """Find component files under a directory prefix with depth restrictions.

    Skills: {dir_prefix}<name>/SKILL.md  (one level deep)
    Agents/Commands: {dir_prefix}<name>.md  (flat files only)
    """
    if not dir_prefix.endswith("/"):
        dir_prefix += "/"
    found: list[str] = []
    for path in path_set:
        if not path.startswith(dir_prefix) or not path.endswith(".md"):
            continue
        parts = path[len(dir_prefix):].split("/")
        if component_type == "skills":
            if len(parts) == 2 and parts[1] == "SKILL.md":
                found.append(path)
        else:
            if len(parts) == 1:
                found.append(path)
    return found


def _resolve_component_paths(
    prefix: str,
    path_set: set[str],
    plugin_data: dict,
    field: str,
    default_dir: str,
    plugin_name: str | None,
) -> list[str]:
    """Resolve component file paths from plugin.json custom paths or default directory.

    If plugin.json declares explicit paths for this field, use those.
    Otherwise fall back to scanning the default directory.
    """
    explicit = plugin_data.get(field)
    if explicit is not None:
        if isinstance(explicit, str):
            explicit = [explicit]
        if not isinstance(explicit, list):
            return []
        resolved: list[str] = []
        for entry in explicit:
            entry = str(entry)
            if entry.startswith("./"):
                entry = entry[2:]
            full = f"{prefix}/{entry}" if prefix else entry
            # If entry looks like a directory, discover files within it
            if full.endswith("/") or not full.endswith(".md"):
                dir_pfx = full if full.endswith("/") else full + "/"
                resolved.extend(_discover_dir(dir_pfx, field, path_set))
            elif full in path_set:
                resolved.append(full)
            else:
                logger.debug("Custom %s path not found in file tree: %s (plugin=%s)", field, full, plugin_name)
        return resolved

    # No explicit paths — fall back to default directory
    default_pfx = f"{prefix}/{default_dir}" if prefix else default_dir
    return _discover_dir(default_pfx, field, path_set)


def _parse_plugin_json(repo_name: str, path: str, content: str, branch: str = "main") -> tuple[AIAsset | None, dict]:
    """Parse plugin.json content into (asset, raw_data).

    Returns the raw JSON data so callers can inspect skills/agents/commands fields.
    """
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return None, {}

    name = data.get("name") or _name_from_path(path)
    metadata = {}
    for key in ("author", "description", "keywords", "homepage", "license", "repository"):
        if data.get(key):
            metadata[key] = data[key] if not isinstance(data[key], str) else data[key][:500]

    asset = AIAsset(
        asset_type="plugin",
        name=name,
        provider="Anthropic",
        category="claude_code",
        confidence=0.95,
        relationship="definition",
        source_repo=repo_name,
        source_file=path,
        source_branch=branch,
        version=data.get("version"),
        description=str(metadata.get("description", "")),
        content=content,
        metadata=metadata,
    )
    return asset, data


def _parse_tool_file(repo_name: str, path: str, content: str, asset_type: str, branch: str = "main") -> list[AIAsset]:
    frontmatter, body = _parse_frontmatter(content)

    if asset_type == "command":
        name = _name_from_path(path)
    else:
        name = frontmatter.get("name") or _name_from_path(path)

    confidence = 0.95 if frontmatter else 0.85

    metadata = {"content_preview": body[:200].strip()}
    if frontmatter.get("description"):
        metadata["description"] = str(frontmatter["description"])[:500]
    if frontmatter.get("model"):
        metadata["model"] = str(frontmatter["model"])
    if frontmatter.get("allowed-tools"):
        metadata["allowed_tools"] = frontmatter["allowed-tools"]

    return [AIAsset(
        asset_type=asset_type,
        name=name,
        provider="Anthropic",
        category="claude_code",
        confidence=confidence,
        relationship="definition",
        source_repo=repo_name,
        source_file=path,
        source_branch=branch,
        version=frontmatter.get("version"),
        description=str(metadata.get("description", "")),
        content=content,
        metadata=metadata,
    )]


def _parse_frontmatter(content: str) -> tuple[dict, str]:
    match = _FRONTMATTER_RE.match(content)
    if not match:
        return {}, content
    try:
        fm = yaml.safe_load(match.group(1))
        if not isinstance(fm, dict):
            return {}, content
        return fm, content[match.end():]
    except yaml.YAMLError:
        return {}, content


def _name_from_path(path: str) -> str:
    base = os.path.basename(path)
    name, _ = os.path.splitext(base)
    if name in ("SKILL", "plugin"):
        parts = path.rstrip("/").split("/")
        if len(parts) >= 2:
            return parts[-2]
    return name
