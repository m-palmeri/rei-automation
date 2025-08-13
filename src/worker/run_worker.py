from redis import Redis
from rq import Worker, Queue, Connection, SimpleWorker
import os, time
from dotenv import load_dotenv

load_dotenv()

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
listen = ["default"]

def wait_for_redis(url: str, timeout: int = 60) -> Redis:
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            conn = Redis.from_url(url)
            conn.ping()  # force a real connection
            return conn
        except Exception as e:
            last_err = e
            time.sleep(0.5)
    raise RuntimeError(f"Redis not ready at {url}: {last_err}")

if __name__ == "__main__":
    use_simple = os.getenv("RQ_SIMPLE", "0") == "1"
    worker_cls = SimpleWorker if use_simple else Worker
    conn = wait_for_redis(redis_url, timeout=int(os.getenv("REDIS_WAIT_SECS", "60")))
    with Connection(conn):
        worker = worker_cls(list(map(Queue, listen)))
        worker.work(with_scheduler=True)