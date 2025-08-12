from loguru import logger
import os, httpx
from app.db import already_processed, mark_processed, dlq_put

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

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

def process_page(payload: dict):
    page_id = payload["page_id"]
    edit_ts = payload.get("edit_ts")

    # Idempotency check
    if already_processed(page_id, edit_ts):
        logger.info(f"[process_page] SKIP id={page_id} edit_ts={edit_ts} (already processed)")
        return {"status": "skip", "page_id": page_id}

    try:
        # --- Real work starts here ---
        page = _fetch_page(page_id)
        title = _page_title_from_obj(page)
        logger.info(f"[process_page] RUN id={page_id} title={title!r} edit_ts={edit_ts}")
        # TODO: call external APIs, analysis, Drive upload, Notion updates
        # --------------------------------

        # Mark success
        mark_processed(page_id, edit_ts)
        return {"status": "ok", "page_id": page_id, "title": title}

    except Exception as e:
        # Record failure to a DLQ table (and re-raise for RQ retry/failed jobs)
        err = f"{type(e).__name__}: {e}"
        dlq_put(page_id, edit_ts, err)
        logger.exception(f"[process_page] ERROR id={page_id} edit_ts={edit_ts} -> {err}")
        raise