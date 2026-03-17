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
        key = f"{self.asset_type}:{self.name}:{self.provider}"
        if self.version:
            key += f":{self.version}"
        return sha256(key.encode()).hexdigest()[:16]
