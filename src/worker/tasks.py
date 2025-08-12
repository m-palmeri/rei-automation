from loguru import logger

def process_page(payload: dict):
    """MVP stub task: prove the queue works end-to-end."""
    page_id = payload["page_id"]
    edit_ts = payload.get("edit_ts")
    logger.info(f"[process_page] page_id={page_id} edit_ts={edit_ts}")
    # TODO: call Notion, APIs, Drive, etc.
    return {"status": "ok", "page_id": page_id}