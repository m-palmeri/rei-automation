"""Notion Database operations."""

import json
import pathlib
from typing import Dict, List, Optional, Tuple

from loguru import logger

from .client import NotionClient
from .types import PageSummary


class NotionDatabase:
    """A Notion database with query and cursor management."""

    def __init__(
        self,
        client: NotionClient,
        database_id: Optional[str] = None,
        state_dir: Optional[pathlib.Path] = None,
    ) -> None:
        """Initialize a database.

        Args:
            client: The NotionClient instance to use for API calls
            database_id: The ID of the database. Default to NOTION_DB_ID env var.
            state_dir: Directory for cursor state. Defaults to .state in current dir.
        """
        import os

        self.client = client
        self.database_id = database_id or os.getenv("NOTION_DB_ID")
        if not self.database_id:
            raise RuntimeError("NOTION_DB_ID not set")

        # Set up cursor state management
        self.state_dir = state_dir or pathlib.Path(".state")
        self.state_dir.mkdir(exist_ok=True)
        self.cursor_file = self.state_dir / f"notion_cursor_{self.database_id}.json"

    def load_cursor(self) -> str:
        """Load the last cursor timestamp."""
        if self.cursor_file.exists():
            try:
                return json.loads(self.cursor_file.read_text()).get(
                    "cursor_ts", "1970-01-01T00:00:00Z"
                )
            except Exception:
                logger.warning("cursor file unreadable, resetting to epoch")
        return "1970-01-01T00:00:00Z"

    def save_cursor(self, ts: str) -> None:
        """Save a cursor timestamp."""
        self.cursor_file.write_text(json.dumps({"cursor_ts": ts}))

    def query_since(self, after_iso: str, page_size: int = 50) -> Tuple[List[Dict], str]:
        """Query for pages edited after a timestamp.

        Args:
            after_iso: ISO timestamp to query from
            page_size: Number of results per page

        Returns:
            Tuple of (list of pages, maximum timestamp seen)
        """
        url = f"databases/{self.database_id}/query"
        payload = {
            "filter": {"timestamp": "last_edited_time", "last_edited_time": {"after": after_iso}},
            "sorts": [{"timestamp": "last_edited_time", "direction": "ascending"}],
            "page_size": page_size,
        }

        pages, max_ts = [], after_iso
        while True:
            data = self.client.post(url, payload)
            batch = data.get("results", [])
            pages.extend(batch)

            # Update max timestamp seen
            for p in batch:
                ts = p.get("last_edited_time", after_iso)
                if ts > max_ts:
                    max_ts = ts

            # Handle pagination
            if not data.get("has_more"):
                break
            payload["start_cursor"] = data.get("next_cursor")

        return pages, max_ts

    def summarize_pages(self, pages: List[Dict]) -> List[PageSummary]:
        """Create summaries of pages for debugging."""
        summaries = []
        for p in pages:
            pid = p["id"]
            ets = p["last_edited_time"]
            # Extract title
            title = ""
            props = p.get("properties", {})
            for prop in props.values():
                if prop.get("type") == "title":
                    title = self.client.extract_plain_text(prop)
                    break
            summaries.append(PageSummary(id=pid, title=title, last_edited_time=ets))
        return summaries
