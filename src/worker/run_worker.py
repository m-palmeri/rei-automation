from redis import Redis
from rq import Worker, Queue, Connection
import os

redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
listen = ["default"]

if __name__ == "__main__":
    conn = Redis.from_url(redis_url)
    with Connection(conn):
        worker = Worker(list(map(Queue, listen)))
        worker.work(with_scheduler=True)