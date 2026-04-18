"""Pydantic models for the Airbyte protocol.

These match what the K8s Agent parser expects:
  - AirbyteMessage with type field (SPEC/CATALOG/RECORD/STATE/LOG)
  - AirbyteRecord with stream and data fields
  - AirbyteState with type, stream descriptor, and stream state
"""

import json
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel


class AirbyteMessageType(str, Enum):
    SPEC = "SPEC"
    CATALOG = "CATALOG"
    RECORD = "RECORD"
    STATE = "STATE"
    LOG = "LOG"


class AirbyteLogLevel(str, Enum):
    FATAL = "FATAL"
    ERROR = "ERROR"
    WARN = "WARN"
    INFO = "INFO"
    DEBUG = "DEBUG"
    TRACE = "TRACE"


# --- Spec ---

class AirbyteSpec(BaseModel):
    connectionSpecification: dict[str, Any]


# --- Catalog ---

class AirbyteSyncMode(str, Enum):
    full_refresh = "full_refresh"
    incremental = "incremental"


class AirbyteStream(BaseModel):
    name: str
    json_schema: dict[str, Any]
    supported_sync_modes: list[AirbyteSyncMode] = [AirbyteSyncMode.full_refresh]
    source_defined_primary_key: list[list[str]] = [["asset_id"]]


class AirbyteCatalog(BaseModel):
    streams: list[AirbyteStream]


class ConfiguredAirbyteStream(BaseModel):
    stream: AirbyteStream
    sync_mode: AirbyteSyncMode = AirbyteSyncMode.full_refresh


class ConfiguredAirbyteCatalog(BaseModel):
    streams: list[ConfiguredAirbyteStream]


# --- Record ---

class AirbyteRecord(BaseModel):
    stream: str
    data: dict[str, Any]
    emitted_at: int = 0


# --- State ---

class AirbyteStreamDescriptor(BaseModel):
    name: str


class AirbyteStreamState(BaseModel):
    stream_descriptor: AirbyteStreamDescriptor
    stream_state: dict[str, Any]


class AirbyteStateMessage(BaseModel):
    type: str = "STREAM"
    stream: AirbyteStreamState


# --- Log ---

class AirbyteLogMessage(BaseModel):
    level: AirbyteLogLevel
    message: str


# --- Top-level message ---

class AirbyteMessage(BaseModel):
    type: AirbyteMessageType
    spec: Optional[AirbyteSpec] = None
    catalog: Optional[AirbyteCatalog] = None
    record: Optional[AirbyteRecord] = None
    state: Optional[AirbyteStateMessage] = None
    log: Optional[AirbyteLogMessage] = None

    def to_json_line(self) -> str:
        """Serialize to a single NDJSON line, excluding None fields."""
        return self.model_dump_json(exclude_none=True)
