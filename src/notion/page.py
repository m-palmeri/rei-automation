"""Notion Page operations."""

from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

from loguru import logger

from .client import NotionClient
from .types import PropertyType, PropertyValue


class NotionPage:
    """A Notion page with its properties and operations."""

    def __init__(self, client: NotionClient, page_id: str) -> None:
        """Initialize a page.

        Args:
            client: The NotionClient instance to use for API calls
            page_id: The ID of the page
        """
        self.client = client
        self.id = page_id
        self._data: Optional[Dict[str, Any]] = None
        self._pending_updates: Dict[str, Dict[str, Any]] = {}

    def refresh(self) -> None:
        """Refresh the page data from Notion."""
        self._data = self.client.get(f"pages/{self.id}")

    @property
    def data(self) -> Dict[str, Any]:
        """Get the page data, fetching it if not already loaded."""
        if self._data is None:
            self.refresh()
        return self._data or {}

    def get_title(self) -> str:
        """Extract the page title from properties."""
        props = self.data.get("properties", {})
        for prop in props.values():
            if prop.get("type") == "title":
                return self.client.extract_plain_text(prop)
        return ""

    def get_property(self, name: str) -> Optional[PropertyValue]:
        """Get a property by name."""
        return self.data.get("properties", {}).get(name)

    def queue_property_update(self, name: str, prop_type: PropertyType, value: str) -> None:
        """Queue a property update without making an API call.

        Args:
            name: The name of the property
            prop_type: The type of the property ('url' or 'rich_text')
            value: The value to set
        """
        prop = self.get_property(name)
        if not prop:
            raise RuntimeError(f"Property {name!r} not found on page")

        ptype = prop.get("type")
        if ptype != prop_type:
            raise RuntimeError(f"Property {name!r} is type {ptype!r}; expected {prop_type!r}")

        # Check if value is already set
        if self.client.extract_plain_text(prop) == value:
            logger.debug(f"[notion] {name!r} already set for {self.id}, skipping")
            return

        # Prepare the update based on property type
        if prop_type == "url":
            prop_value = {"url": value}
        elif prop_type == "rich_text":
            prop_value = {"rich_text": [{"type": "text", "text": {"content": value}}]}
        else:
            raise ValueError(f"Unsupported property type: {prop_type}")

        # Queue the update
        self._pending_updates[name] = prop_value
        logger.debug(f"[notion] queued update for {name!r} on {self.id}")

    def update_property(self, name: str, prop_type: PropertyType, value: str) -> None:
        """Update a page property immediately.

        Args:
            name: The name of the property
            prop_type: The type of the property ('url' or 'rich_text')
            value: The value to set
        """
        self.queue_property_update(name, prop_type, value)
        self.commit_updates()

    def commit_updates(self) -> None:
        """Apply all pending property updates in a single API call."""
        if not self._pending_updates:
            return

        # Make a single PATCH request with all updates
        self.client.patch(f"pages/{self.id}", {"properties": self._pending_updates})
        logger.info(f"[notion] updated {len(self._pending_updates)} properties for {self.id}")

        # Clear pending updates and invalidate cache
        self._pending_updates.clear()
        self._data = None

    @contextmanager
    def batch_update(self) -> Generator[None, None, None]:
        """Context manager for batching property updates.

        Example:
            with page.batch_update():
                page.queue_property_update("Property1", "url", "http://...")
                page.queue_property_update("Property2", "rich_text", "value")
                # Updates are committed at the end of the with block
        """
        try:
            yield
            if self._pending_updates:
                self.commit_updates()
        except Exception:
            self._pending_updates.clear()
            raise
