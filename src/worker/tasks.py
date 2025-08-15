import os
from datetime import datetime, timezone

import httpx
from app.db import get_or_create_folder
from app.models.dlq import DLQ
from app.models.page_state import PageState
from drive.client import GoogleDriveClient
from loguru import logger

drive_client = GoogleDriveClient()
_page_state = PageState()
_dlq = DLQ()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}
NOTION_DRIVE_PROP = "Google Drive Link"  # name of the property to update


def _patch_page_properties(page_id: str, props: dict) -> None:
    """PATCH /v1/pages/{page_id} with given properties dict."""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    with httpx.Client(timeout=30) as client:
        r = client.patch(url, headers=NOTION_HEADERS, json={"properties": props})
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            # Surface the Notion error detail for quick debugging
            detail = ""
            try:
                detail = r.json().get("message", "")
            except Exception:
                pass
            raise RuntimeError(f"Notion update failed ({r.status_code}): {detail}") from e


def _extract_prop_text(prop: dict) -> str:
    """Return current human-readable value for URL or rich_text property."""
    t = prop.get("type")
    if t == "url":
        return prop.get("url") or ""
    if t == "rich_text":
        return "".join([s.get("plain_text", "") for s in prop.get("rich_text", [])]).strip()
    return ""


def _set_drive_link(page: dict, page_id: str, link: str) -> None:
    """Update the configured property with the Drive link.
    Supports URL or rich_text properties. Raises on wrong/missing type.
    """
    props = page.get("properties", {})
    prop = props.get(NOTION_DRIVE_PROP)
    if not prop:
        raise RuntimeError(
            f"Property {NOTION_DRIVE_PROP!r} not found on page. "
            "Create it in the DB (URL or rich_text)."
        )

    ptype = prop.get("type")
    current = _extract_prop_text(prop)
    if current == link:
        logger.info(f"[notion] drive link already set for {page_id}")
        return  # nothing to do

    if ptype == "url":
        _patch_page_properties(page_id, {NOTION_DRIVE_PROP: {"url": link}})
    elif ptype == "rich_text":
        _patch_page_properties(
            page_id,
            {NOTION_DRIVE_PROP: {"rich_text": [{"type": "text", "text": {"content": link}}]}},
        )
    else:
        raise RuntimeError(f"Property {NOTION_DRIVE_PROP!r} is type {ptype!r}; expected 'url'.")
    logger.info(f"[notion] updated {NOTION_DRIVE_PROP!r} for {page_id}")


def _fetch_page(page_id: str) -> dict:
    url = f"https://api.notion.com/v1/pages/{page_id}"
    with httpx.Client(timeout=30) as client:
        r = client.get(url, headers=NOTION_HEADERS)
        r.raise_for_status()
        return r.json()


def _page_title_from_obj(p: dict) -> str:
    props = p.get("properties", {})
    for _, prop in props.items():
        if prop.get("type") == "title":
            return "".join([t.get("plain_text", "") for t in prop.get("title", [])]).strip()
    return ""


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
        # Load page (for title → use as folder name placeholder)
        page = _fetch_page(page_id)
        title = _page_title_from_obj(page) or f"notion-page-{page_id[-6:]}"

        # Avoid duplicate folder creation if we’ve done it before
        folder_id, web_link = get_or_create_folder(drive_client, page_id, title)
        logger.info(f"[process_page] Drive folder ready id={page_id} folder={folder_id}")

        # Update the Notion page property with the Drive link
        _set_drive_link(page, page_id, web_link)

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
