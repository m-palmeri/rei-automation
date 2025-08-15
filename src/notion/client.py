"""Base client for Notion API interactions."""

import os
from typing import Any, Dict, Optional

import httpx

from .types import PropertyValue


class NotionClient:
    """Base client for Notion API interactions."""

    API_BASE = "https://api.notion.com/v1"
    API_VERSION = "2022-06-28"  # stable version

    def __init__(self, token: Optional[str] = None) -> None:
        """Initialize the client.

        Args:
            token: Notion API token. If not provided, will look for NOTION_TOKEN env var.
        """
        self.token = token or os.getenv("NOTION_TOKEN")
        if not self.token:
            raise RuntimeError("NOTION_TOKEN not set")

    def _headers(self) -> Dict[str, str]:
        """Get the headers required for Notion API requests."""
        return {
            "Authorization": f"Bearer {self.token}",
            "Notion-Version": self.API_VERSION,
            "Content-Type": "application/json",
        }

    def _url(self, path: str) -> str:
        """Construct a full URL from a path."""
        return f"{self.API_BASE}/{path.lstrip('/')}"

    def get(self, path: str) -> Dict[str, Any]:
        """Make a GET request to the Notion API."""
        with httpx.Client(timeout=30) as client:
            r = client.get(self._url(path), headers=self._headers())
            try:
                r.raise_for_status()
                return r.json()
            except httpx.HTTPStatusError as e:
                detail = self._extract_error_detail(r)
                raise RuntimeError(f"Notion API error ({r.status_code}): {detail}") from e

    def post(self, path: str, json: Dict[str, Any]) -> Dict[str, Any]:
        """Make a POST request to the Notion API."""
        with httpx.Client(timeout=30) as client:
            r = client.post(self._url(path), headers=self._headers(), json=json)
            try:
                r.raise_for_status()
                return r.json()
            except httpx.HTTPStatusError as e:
                detail = self._extract_error_detail(r)
                raise RuntimeError(f"Notion API error ({r.status_code}): {detail}") from e

    def patch(self, path: str, json: Dict[str, Any]) -> Dict[str, Any]:
        """Make a PATCH request to the Notion API."""
        with httpx.Client(timeout=30) as client:
            r = client.patch(self._url(path), headers=self._headers(), json=json)
            try:
                r.raise_for_status()
                return r.json()
            except httpx.HTTPStatusError as e:
                detail = self._extract_error_detail(r)
                raise RuntimeError(f"Notion API error ({r.status_code}): {detail}") from e

    @staticmethod
    def _extract_error_detail(response: httpx.Response) -> str:
        """Extract error detail from a Notion API response."""
        try:
            return response.json().get("message", "")
        except Exception:
            return "No details available"

    @staticmethod
    def extract_plain_text(prop: PropertyValue) -> str:
        """Extract plain text from a property value."""
        ptype = prop.get("type")
        if ptype == "url":
            return prop.get("url", "")
        if ptype in ("title", "rich_text"):
            text_list = prop.get(ptype, [])
            return "".join(t.get("plain_text", "") for t in text_list).strip()
        return ""
