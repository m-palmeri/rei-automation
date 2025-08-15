from fastapi import FastAPI
from loguru import logger
from redis import Redis
from rq import Queue, Retry

from .settings import settings

app = FastAPI(title="Notion Automation MVP")
redis = Redis.from_url(settings.redis_url)
q = Queue("default", connection=redis)


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.on_event("startup")
def _startup() -> None:
    # create tables if missing
    from app.db import init_db

    init_db()


@app.post("/enqueue-test")
def enqueue_test(page_id: str) -> dict:
    from worker.tasks import process_page

    job = q.enqueue(
        process_page,
        {"page_id": page_id, "edit_ts": "demo"},
        retry=Retry(max=3, interval=[10, 30, 60]),
    )
    logger.info(f"enqueued {job.id} for {page_id}")
    return {"job_id": job.id}


@app.post("/poll")
def poll_notion(debug: bool = False) -> dict:
    from notion import NotionClient, NotionDatabase, PageSummary

    # Initialize client and database
    client = NotionClient()
    db = NotionDatabase(client)

    # Query for updated pages
    cursor = db.load_cursor()
    pages, max_ts = db.query_since(cursor)
    db.save_cursor(max_ts)

    # Process pages
    for page in pages:
        page_id = page["id"]
        edit_ts = page["last_edited_time"]
        q.enqueue(
            "worker.tasks.process_page",
            {"page_id": page_id, "edit_ts": edit_ts},
            retry=Retry(max=3, interval=[10, 30, 60]),
        )

    # Prepare response
    out = {"enqueued": len(pages), "cursor": max_ts}
    if debug:
        out["pages"] = [
            PageSummary(
                id=p["id"],
                title=NotionClient.extract_plain_text(p["properties"].get("title", {})),
                last_edited_time=p["last_edited_time"],
            )
            for p in pages
        ]
    logger.info(f"poll: enqueued={len(pages)} new_cursor={max_ts}")
    return out
