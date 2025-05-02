import json
from datetime import datetime
from typing import Any

from fastapi.responses import JSONResponse


def datetime_handler(obj):
    """Handle datetime serialization."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class BaseJSONResponse(JSONResponse):
    """Base JSON response with datetime handling."""

    def render(self, content: Any) -> bytes:
        """Render the response with datetime handling."""
        return json.dumps(
            content,
            default=datetime_handler,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
        ).encode("utf-8")


class JSONPResponse(BaseJSONResponse):
    """Custom response class for JSONP support."""

    media_type = "application/javascript"

    def __init__(self, content: Any, callback: str = "callback", **kwargs) -> None:
        """Initialize JSONP response with content and callback name."""
        self.callback = callback
        super().__init__(content, **kwargs)

    def render(self, content: Any) -> bytes:
        """Render the JSONP response."""
        json_str = super().render(content).decode()
        jsonp = f"{self.callback}({json_str})"
        return jsonp.encode("utf-8")
