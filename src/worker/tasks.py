from datetime import datetime, timezone

from app.db import get_or_create_folder
from app.models.dlq import DLQ
from app.models.page_state import PageState
from drive.client import GoogleDriveClient
from loguru import logger
from notion import NotionClient, NotionPage

# Initialize clients
drive_client = GoogleDriveClient()
notion_client = NotionClient()
_page_state = PageState()
_dlq = DLQ()

NOTION_DRIVE_PROP = "Google Drive Link"  # name of the property to update


def process_page(payload: dict) -> dict:
    page_id = payload["page_id"]
    edit_ts = payload.get("edit_ts")

    if edit_ts is None:
        # If no edit timestamp provided, use current time in ISO format
        edit_ts = datetime.now(timezone.utc).isoformat()

    # If we've already fully processed this exact edit, skip
    if _page_state.already_processed(page_id, edit_ts):
        logger.info(f"[process_page] SKIP id={page_id} edit_ts={edit_ts}")
        return {"status": "skip", "page_id": page_id}

    try:
        # Load page and get title
        page = NotionPage(notion_client, page_id)
        title = page.get_title() or f"notion-page-{page_id[-6:]}"

        # Avoid duplicate folder creation if we've done it before
        folder_id, web_link = get_or_create_folder(drive_client, page_id, title)
        logger.info(f"[process_page] Drive folder ready id={page_id} folder={folder_id}")

        # Update the Notion page property with the Drive link
        prop = page.get_property(NOTION_DRIVE_PROP)
        if not prop:
            raise RuntimeError(
                f"Property {NOTION_DRIVE_PROP!r} not found on page. "
                "Create it in the DB (URL or rich_text)."
            )
        # Use the existing property type, defaulting to URL if somehow not set
        prop_type = prop.get("type", "url")
        page.update_property(NOTION_DRIVE_PROP, prop_type, web_link)

        _page_state.mark_processed(page_id, edit_ts)
        return {
            "status": "ok",
            "page_id": page_id,
            "title": title,
            "drive_folder_id": folder_id,
            "drive_link": web_link,
        }

    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        _dlq.put(page_id, edit_ts, err)
        logger.exception(f"[process_page] ERROR id={page_id} -> {err}")
        raise
