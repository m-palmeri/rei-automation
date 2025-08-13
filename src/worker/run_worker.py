from redis import Redis
from rq import Worker, Queue, Connection, SimpleWorker
import os
from dotenv import load_dotenv

load_dotenv()  # so .env works in debug too

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
listen = ["default"]

if __name__ == "__main__":
    conn = Redis.from_url(redis_url)
    use_simple = os.getenv("RQ_SIMPLE", "0") == "1"
    worker_cls = SimpleWorker if use_simple else Worker
    with Connection(conn):
        worker = worker_cls(list(map(Queue, listen)))
        worker.work(with_scheduler=True)