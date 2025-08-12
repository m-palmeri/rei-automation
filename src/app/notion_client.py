import json, os, pathlib
from typing import List, Tuple
import httpx
from loguru import logger

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
NOTION_DB_ID = os.getenv("NOTION_DB_ID")
NOTION_VER = "2022-06-28"  # stable

STATE_DIR = pathlib.Path(".state")
STATE_DIR.mkdir(exist_ok=True)
CURSOR_FILE = STATE_DIR / f"notion_cursor_{NOTION_DB_ID or 'unknown'}.json"

def load_cursor() -> str:
    if CURSOR_FILE.exists():
        try:
            return json.loads(CURSOR_FILE.read_text()).get("cursor_ts", "1970-01-01T00:00:00Z")
        except Exception:
            logger.warning("cursor file unreadable, resetting to epoch")
    return "1970-01-01T00:00:00Z"

def save_cursor(ts: str) -> None:
    CURSOR_FILE.write_text(json.dumps({"cursor_ts": ts}))

def _headers():
    if not NOTION_TOKEN:
        raise RuntimeError("NOTION_TOKEN not set")
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VER,
        "Content-Type": "application/json",
    }

def query_db_since(after_iso: str) -> Tuple[List[dict], str]:
    """Return pages edited after `after_iso` and the max last_edited_time seen."""
    if not NOTION_DB_ID:
        raise RuntimeError("NOTION_DB_ID not set")
    url = f"https://api.notion.com/v1/databases/{NOTION_DB_ID}/query"
    payload = {
        "filter": {
            "timestamp": "last_edited_time",
            "last_edited_time": {"after": after_iso}
        },
        "sorts": [{"timestamp": "last_edited_time", "direction": "ascending"}],
        "page_size": 50
    }
    pages, max_ts = [], after_iso
    with httpx.Client(timeout=30) as client:
        while True:
            r = client.post(url, headers=_headers(), json=payload)
            r.raise_for_status()
            data = r.json()
            batch = data.get("results", [])
            pages.extend(batch)
            for p in batch:
                ts = p.get("last_edited_time", after_iso)
                if ts > max_ts:
                    max_ts = ts
            if not data.get("has_more"):
                break
            # Notion pagination uses next_cursor
            payload["start_cursor"] = data.get("next_cursor")
    return pages, max_ts