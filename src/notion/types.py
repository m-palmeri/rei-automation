"""Type definitions and constants for Notion API."""

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, TypedDict

PropertyType = Literal["title", "url", "rich_text"]


class PropertyValue(TypedDict, total=False):
    """A Notion property value."""

    type: PropertyType
    title: List[Dict[str, Any]]  # For title properties
    url: str  # For URL properties
    rich_text: List[Dict[str, Any]]  # For rich_text properties


@dataclass
class PageSummary:
    """Summary information about a Notion page."""

    id: str
    title: str
    last_edited_time: str
