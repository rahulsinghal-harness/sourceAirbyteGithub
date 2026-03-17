from dataclasses import dataclass, field

CONNECTION_SPEC = {
    "type": "object",
    "title": "AI Asset Discovery Configuration",
    "properties": {
        "auth_type": {
            "type": "string",
            "enum": ["token", "github_app"],
            "default": "token",
        },
        "github_token": {
            "type": "string",
            "airbyte_secret": True,
            "description": "Personal access token (when auth_type=token)",
        },
        "github_app_id": {
            "type": "string",
            "description": "GitHub App ID (when auth_type=github_app)",
        },
        "github_app_private_key": {
            "type": "string",
            "airbyte_secret": True,
            "description": "GitHub App private key PEM (when auth_type=github_app)",
        },
        "github_app_installation_id": {
            "type": "string",
            "description": "GitHub App installation ID (when auth_type=github_app)",
        },
        "repos": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Repositories to scan (owner/repo format)",
        },
        "github_org": {
            "type": "string",
            "description": "GitHub organization — scans all non-archived repos",
        },
    },
    "required": ["auth_type"],
}


@dataclass
class ConnectorConfig:
    auth_type: str = "token"
    github_token: str = ""
    github_app_id: str = ""
    github_app_private_key: str = ""
    github_app_installation_id: str = ""
    repos: list[str] = field(default_factory=list)
    github_org: str = ""

    @classmethod
    def from_dict(cls, raw: dict) -> "ConnectorConfig":
        # Support IM format (provider/credentials/repos) and direct format
        if "credentials" in raw:
            creds = raw["credentials"]
            repos = raw.get("repos", [])
            return cls(
                auth_type=creds.get("auth_type", "token"),
                github_token=creds.get("token", ""),
                github_app_id=creds.get("app_id", ""),
                github_app_private_key=creds.get("private_key", ""),
                github_app_installation_id=creds.get("installation_id", ""),
                repos=repos,
                github_org=raw.get("github_org", ""),
            )

        return cls(
            auth_type=raw.get("auth_type", "token"),
            github_token=raw.get("github_token", ""),
            github_app_id=raw.get("github_app_id", ""),
            github_app_private_key=raw.get("github_app_private_key", ""),
            github_app_installation_id=raw.get("github_app_installation_id", ""),
            repos=raw.get("repos", []),
            github_org=raw.get("github_org", ""),
        )
