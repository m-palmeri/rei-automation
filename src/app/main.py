from fastapi import FastAPI
from loguru import logger
from redis import Redis
from rq import Queue
from .settings import settings

app = FastAPI(title="Notion Automation MVP")

redis = Redis.from_url(settings.redis_url)
q = Queue("default", connection=redis)

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/enqueue-test")
def enqueue_test(page_id: str):
    # local import to avoid circulars during tooling
    from ..worker.tasks import process_page
    job = q.enqueue(process_page, {"page_id": page_id, "edit_ts": "demo"})
    logger.info(f"enqueued {job.id} for {page_id}")
    return {"job_id": job.id}