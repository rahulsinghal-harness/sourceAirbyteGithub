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


def scan_repo(repo_name: str, files: dict[str, str], branch: str = "main") -> list[AIAsset]:
    assets: list[AIAsset] = []
    is_plugin = ".claude-plugin/plugin.json" in file
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
    if plugin_content:
        plugin_asset = _parse_plugin_json(repo_name, plugin_json_path, plugin_content, branch)
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

    # Scan children relative to plugin prefix
    skills_prefix = f"{prefix}/skills/" if prefix else "skills/"
    agents_prefix = f"{prefix}/agents/" if prefix else "agents/"
    commands_prefix = f"{prefix}/commands/" if prefix else "commands/"

    for path, content in files.items():
        if path.startswith(skills_prefix) and path.endswith("/SKILL.md"):
            for asset in _parse_tool_file(repo_name, path, content, "skill", branch):
                asset.parent_id = plugin_id
                asset.parent_name = plugin_name
                assets.append(asset)
        elif path.startswith(agents_prefix) and path.endswith(".md"):
            for asset in _parse_tool_file(repo_name, path, content, "agent", branch):
                asset.parent_id = plugin_id
                asset.parent_name = plugin_name
                assets.append(asset)
        elif path.startswith(commands_prefix) and path.endswith(".md"):
            for asset in _parse_tool_file(repo_name, path, content, "command", branch):
                asset.parent_id = plugin_id
                asset.parent_name = plugin_name
                assets.append(asset)


def _parse_plugin_json(repo_name: str, path: str, content: str, branch: str = "main") -> AIAsset | None:
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return None

    name = data.get("name") or _name_from_path(path)
    metadata = {}
    for key in ("author", "description", "keywords", "homepage", "license", "repository"):
        if data.get(key):
            metadata[key] = data[key] if not isinstance(data[key], str) else data[key][:500]

    return AIAsset(
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
        metadata=metadata,
    )


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
