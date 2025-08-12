from fastapi import FastAPI
from loguru import logger
from redis import Redis
from rq import Queue, Retry
from .settings import settings

app = FastAPI(title="Notion Automation MVP")
redis = Redis.from_url(settings.redis_url)
q = Queue("default", connection=redis)

@app.get("/health")
def health():
    return {"ok": True}

@app.on_event("startup")
def _startup():
    # create tables if missing
    from app.db import init_db
    init_db()

@app.post("/enqueue-test")
def enqueue_test(page_id: str):
    from worker.tasks import process_page
    job = q.enqueue(process_page, {"page_id": page_id, "edit_ts": "demo"},
                    retry=Retry(max=3, interval=[10, 30, 60]))
    logger.info(f"enqueued {job.id} for {page_id}")
    return {"job_id": job.id}

@app.post("/poll")
def poll_notion(debug: bool = False):
    from app.notion_client import load_cursor, save_cursor, query_db_since
    cursor = load_cursor()
    pages, max_ts = query_db_since(cursor)

    summaries = []
    for p in pages:
        pid = p["id"]
        ets = p["last_edited_time"]
        # extract title (debug)
        title = ""
        props = p.get("properties", {})
        for _, prop in props.items():
            if prop.get("type") == "title":
                title = "".join([t.get("plain_text", "") for t in prop.get("title", [])]).strip()
                break

        q.enqueue("worker.tasks.process_page", {"page_id": pid, "edit_ts": ets},
                  retry=Retry(max=3, interval=[10, 30, 60]))
        if debug:
            summaries.append({"id": pid, "title": title, "last_edited_time": ets})

    save_cursor(max_ts)
    out = {"enqueued": len(pages), "cursor": max_ts}
    if debug:
        out["pages"] = summaries
    logger.info(f"poll: enqueued={len(pages)} new_cursor={max_ts}")
    return out