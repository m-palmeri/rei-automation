import os
import pathlib
from typing import Any, Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from loguru import logger

from .folder import GoogleDriveFolder


class GoogleDriveClient:
    """Client for interacting with Google Drive API.

    Handles authentication and provides high-level operations for working with Google Drive.
    """

    SCOPES = ["https://www.googleapis.com/auth/drive"]
    TOKEN_PATH = pathlib.Path(".secrets/google_token.json")

    def __init__(self, root_folder_id: Optional[str] = None):
        """Initialize the Google Drive client.

        Args:
            root_folder_id: Optional ID of a root folder to use as the default parent.
                          If not provided, will use GDRIVE_ROOT_FOLDER_ID from environment.
        """
        self.root_folder_id = (
            root_folder_id or os.getenv("GDRIVE_ROOT_FOLDER_ID", "").strip() or None
        )
        self._service = None

    @property
    def service(self) -> Any:
        """Get or create the Drive API service."""
        if self._service is None:
            self._service = build("drive", "v3", credentials=self._load_credentials())
        return self._service

    def _load_credentials(self) -> Credentials:
        """Load and refresh Google credentials from the token file.

        Returns:
            Valid Google credentials object.

        Raises:
            RuntimeError: If token file is missing.
        """
        if not self.TOKEN_PATH.exists():
            raise RuntimeError("Google token missing. Run: python scripts/google_auth.py")

        creds = Credentials.from_authorized_user_file(str(self.TOKEN_PATH), self.SCOPES)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            self.TOKEN_PATH.write_text(creds.to_json())
        return creds

    def create_folder(
        self, name: str, parent_id: Optional[str] = None, anyone_with_link: bool = True
    ) -> GoogleDriveFolder:
        """Create a new folder in Google Drive.

        Args:
            name: Name of the folder to create
            parent_id: ID of the parent folder. If not provided, uses the client's root_folder_id
            anyone_with_link: If True, makes the folder viewable by anyone with the link

        Returns:
            GoogleDriveFolder object representing the created folder

        Raises:
            HttpError: If the API request fails
        """
        parent = parent_id or self.root_folder_id
        body = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
        if parent:
            body["parents"] = [parent]  # type: ignore[assignment]

        try:
            folder = self.service.files().create(body=body, fields="id").execute()
            folder_id = folder["id"]

            if anyone_with_link:
                perm_body = {"role": "reader", "type": "anyone"}
                self.service.permissions().create(fileId=folder_id, body=perm_body).execute()

            meta = (
                self.service.files().get(fileId=folder_id, fields="id, name, webViewLink").execute()
            )

            # Return a new folder object
            return GoogleDriveFolder(
                self, folder_id=meta["id"], name=meta["name"], web_link=meta.get("webViewLink")
            )

        except HttpError as e:
            logger.exception(f"Drive API error: {e}")
            raise

    def search(
        self,
        query: str,
        parent_id: Optional[str] = None,
        recursive: bool = False,
        exact_match: bool = True,
        folders_only: bool = True,
        limit: Optional[int] = None,
    ) -> list[GoogleDriveFolder]:
        """Search for items in Google Drive.

        Args:
            query: The search query (name to match)
            parent_id: ID of the parent folder to search in. If not provided, uses root_folder_id
            recursive: If True, searches in all subfolders
            exact_match: If True, looks for exact name matches, otherwise uses contains
            folders_only: If True, only returns folders (not files)
            limit: Maximum number of results to return. If None, returns all matches

        Returns:
            List of matching GoogleDriveFolder objects. Empty list if nothing found.
        """
        # Build search query
        query_parts = []

        # Type constraint
        if folders_only:
            query_parts.append("mimeType = 'application/vnd.google-apps.folder'")

        # Name constraint
        if exact_match:
            query_parts.append(f"name = '{query}'")
        else:
            query_parts.append(f"name contains '{query}'")

        # Add parent constraint if specified and not searching recursively
        if not recursive:
            search_parent = parent_id or self.root_folder_id
            if search_parent:
                query_parts.append(f"'{search_parent}' in parents")
            else:
                query_parts.append("'root' in parents")

        try:
            # Search for matching items
            results = (
                self.service.files()
                .list(
                    q=" and ".join(query_parts),
                    spaces="drive",
                    fields="files(id, name, webViewLink)",
                    pageSize=limit,
                )
                .execute()
            )

            # Convert results to folder objects
            return [
                GoogleDriveFolder(
                    self, folder_id=item["id"], name=item["name"], web_link=item.get("webViewLink")
                )
                for item in results.get("files", [])
            ]

        except HttpError as e:
            logger.exception(f"Drive API error: {e}")
            raise

    def find_folder(
        self, name: str, parent_id: Optional[str] = None, recursive: bool = False
    ) -> Optional[GoogleDriveFolder]:
        """Find a single folder by exact name match. Convenience wrapper around search().

        Args:
            name: Name of the folder to find (exact match)
            parent_id: ID of the parent folder to search in. If not provided, uses root_folder_id
            recursive: If True, searches in all subfolders

        Returns:
            GoogleDriveFolder if found, None otherwise
        """
        results = self.search(
            query=name,
            parent_id=parent_id,
            recursive=recursive,
            exact_match=True,
            folders_only=True,
            limit=1,
        )
        return results[0] if results else None

    def get_folder(self, folder_id: str) -> GoogleDriveFolder:
        """Get a folder object by its ID.

        Args:
            folder_id: The ID of the folder

        Returns:
            GoogleDriveFolder object for the specified folder

        Raises:
            HttpError: If the folder doesn't exist or can't be accessed
        """
        try:
            meta = (
                self.service.files().get(fileId=folder_id, fields="id, name, webViewLink").execute()
            )

            return GoogleDriveFolder(
                self, folder_id=meta["id"], name=meta["name"], web_link=meta.get("webViewLink")
            )

        except HttpError as e:
            logger.exception(f"Drive API error: {e}")
            raise
