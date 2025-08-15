"""Database interface for the application."""

from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from drive.client import GoogleDriveClient

from .models.page_state import PageState

# Global instance for get_or_create_folder
_page_state = PageState()


def init_db() -> None:
    """Initialize database tables."""
    # Initialize both tables
    from .models.dlq import DLQ

    _page_state = PageState()
    _dlq = DLQ()
    logger.info("DB initialized (page_state, dlq).")


def get_or_create_folder(
    drive_client: "GoogleDriveClient", page_id: str, title: str
) -> tuple[str, str]:
    """Get existing folder or create a new one, with DB recovery.

    Args:
        drive_client: The GoogleDriveClient instance
        page_id: The Notion page ID
        title: The folder name to search for/create

    Returns:
        Tuple of (folder_id, web_link)
    """
    # Try DB first
    folder_id, link = _page_state.get_drive_info(page_id)
    if folder_id and link:
        # Verify folder still exists in Drive
        try:
            folder = drive_client.get_folder(folder_id)
            return folder_id, folder.web_link
        except Exception:
            # Folder doesn't exist anymore, clear DB entry by setting to None
            # This will be overwritten if we find/create a new folder
            logger.warning(f"Folder {folder_id} not found in Drive, clearing DB entry")
            _page_state.set_drive_info(page_id, "", "")

    # Try to find existing folder by name
    maybe_folder = drive_client.find_folder(title)
    if maybe_folder is not None:
        # Found it! Update DB and return
        _page_state.set_drive_info(page_id, maybe_folder.id, maybe_folder.web_link)
        return maybe_folder.id, maybe_folder.web_link

    # No folder found, create new one
    folder = drive_client.create_folder(title)
    _page_state.set_drive_info(page_id, folder.id, folder.web_link)
    return folder.id, folder.web_link
