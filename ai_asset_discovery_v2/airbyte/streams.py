from airbyte.models import AirbyteStream, AirbyteSyncMode

_COMMON_PROPERTIES = {
    "asset_id": {"type": "string"},
    "name": {"type": "string"},
    "type": {"type": "string"},
    "version": {"type": ["string", "null"]},
    "provider": {"type": "string"},
    "category": {"type": "string"},
    "confidence": {"type": "number", "minimum": 0, "maximum": 1},
    "source_repo": {"type": "string"},
    "source_file": {"type": "string"},
    "source_location": {"type": ["string", "null"]},
    "discovered_at": {"type": "string"},
    "description": {"type": ["string", "null"]},
    "asset_class": {"type": "string"},
    "parent_asset_id": {"type": ["string", "null"]},
    "relationship": {"type": "string"},
    "external_ref": {"type": ["string", "null"]},
}

AIASSET_TYPES = {"plugin", "skill", "agent", "command"}


AIDEPENDENCY_TYPES = {"library", "model", "endpoint"}


def get_all_streams() -> list[AirbyteStream]:
    return [
        AirbyteStream(
            name="aiasset",
            json_schema={
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "title": "AIAsset",
                "type": "object",
                "properties": {
                    **_COMMON_PROPERTIES,
                    "author": {"type": ["string", "null"]},
                    "license": {"type": ["string", "null"]},
                },
                "required": ["asset_id", "name", "type", "provider", "category"],
                "additionalProperties": True,
            },
            supported_sync_modes=[AirbyteSyncMode.full_refresh],
        ),
        AirbyteStream(
            name="aidependency",
            json_schema={
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "title": "AIDependency",
                "type": "object",
                "properties": {**_COMMON_PROPERTIES},
                "required": ["asset_id", "name", "type", "provider", "category"],
                "additionalProperties": True,
            },
            supported_sync_modes=[AirbyteSyncMode.full_refresh],
        ),
    ]
