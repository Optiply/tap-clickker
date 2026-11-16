"""Clickker tap class."""

from __future__ import annotations

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

from tap_clickker.streams import STREAM_TYPES


class TapClickker(Tap):
    """Singer tap for the Clickker API (Exact Globe/Synergy)."""

    name = "tap-clickker"
    alerting_level = AlertingLevel.WARNING

    config_jsonschema = {
        "type": "object",
        "properties": {
            "api_url": {
                "type": "string",
                "minLength": 1,
                "description": "Clickker API base URL, e.g. https://demo-api.clickker.nl.",
            },
            "api_key": {
                "type": "string",
                "minLength": 1,
                "secret": True,
                "description": "Clickker application key, sent as the X-ApiKey header.",
            },
            "administration": {
                "type": "string",
                "description": "Exact administration (SQL database name), sent as the "
                "`administration` header. Omit for the default administration.",
            },
            "start_date": {
                "type": "string",
                "format": "date-time",
                "description": "Lower bound for incremental streams on the first run, "
                "read as Clickker local time (any offset is ignored).",
            },
            "lookback_minutes": {
                "type": "integer",
                "minimum": 0,
                "default": 60,
                "description": "Minutes before the bookmark that each incremental run re-reads. "
                "Must exceed the longest sync, to catch records changed while it ran.",
            },
        },
        "required": ["api_url", "api_key", "start_date"],
    }

    def discover_streams(self) -> list[Stream]:
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapClickker.cli()
