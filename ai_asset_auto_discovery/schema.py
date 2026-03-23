from dataclasses import dataclass, field
from hashlib import sha256
from typing import Optional


@dataclass
class AIAsset:
    asset_type: str
    name: str
    provider: str
    category: str
    confidence: float
    relationship: str
    source_repo: str
    source_file: str
    source_branch: str = "main"
    version: Optional[str] = None
    external_ref: Optional[str] = None
    parent_id: Optional[str] = None
    parent_name: Optional[str] = None
    description: str = ""
    metadata: dict = field(default_factory=dict)

    @property
    def asset_id(self) -> str:
        # Type prefix mapping - ensures ID starts with a letter
        type_prefixes = {
            "plugin": "p",
            "skill": "s",
            "agent": "a",
            "command": "c",
            "library": "l",
            "model": "m",
            "endpoint": "e",
            "mcp_server": "mcp",
            "agent_instructions": "ai"
        }

        # Build natural key based on asset type
        if self.asset_type == "plugin":
            natural_key = f"plugin:{self.name}:{self.provider}:{self.version or 'unknown'}"
        elif self.asset_type in ("skill", "agent", "command"):
            parent = self.parent_id or "standalone"
            natural_key = f"{self.asset_type}:{self.name}:{self.provider}:{parent}"
        elif self.asset_type == "library":
            natural_key = f"library:{self.name}:{self.version or 'unknown'}"
        elif self.asset_type == "model":
            natural_key = f"model:{self.provider}:{self.name}"
        elif self.asset_type == "endpoint":
            natural_key = f"endpoint:{self.provider}:{self.name}"
        elif self.asset_type == "mcp_server":
            natural_key = f"mcp_server:{self.name}"
        elif self.asset_type == "agent_instructions":
            natural_key = f"agent_instructions:{self.name}:{self.provider}"
        else:
            natural_key = f"{self.asset_type}:{self.name}:{self.provider}"

        # Generate hash and prepend type prefix
        # Using full SHA256 hash (64 hex chars = 256 bits) for maximum collision resistance:
        # - Zero practical collision risk (would need 2^128 entities for 50% collision)
        # - Total ID length: 66-68 chars (well under 128 char IDP limit)
        hash_part = sha256(natural_key.encode()).hexdigest()
        prefix = type_prefixes.get(self.asset_type, "x")
        return f"{prefix}_{hash_part}"