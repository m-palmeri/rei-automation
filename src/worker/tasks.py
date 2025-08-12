from loguru import logger
import os, httpx

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

def _page_title_from_obj(p: dict) -> str:
    # Works with the page object returned by DB query
    props = p.get("properties", {})
    for name, prop in props.items():
        if prop.get("type") == "title":
            return "".join([t.get("plain_text", "") for t in prop.get("title", [])]).strip()
    return ""

def _fetch_page(page_id: str) -> dict:
    # Optional: fetch full page if you want more props than query returned
    url = f"https://api.notion.com/v1/pages/{page_id}"
    with httpx.Client(timeout=30) as client:
        r = client.get(url, headers=NOTION_HEADERS)
        r.raise_for_status()
        return r.json()

def process_page(payload: dict):
    page_id = payload["page_id"]
    edit_ts = payload.get("edit_ts")
    # For richer debug, load the page to get its current properties
    try:
        page = _fetch_page(page_id)
        title = _page_title_from_obj(page)
    except Exception as e:
        logger.warning(f"failed to fetch page {page_id}: {e}")
        title = "(unknown)"

    logger.info(f"[process_page] id={page_id} title={title!r} edit_ts={edit_ts}")
    # TODO: do real work here
    return {"status": "ok", "page_id": page_id, "title": title}