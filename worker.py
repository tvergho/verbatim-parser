import os

import redis
import multiprocessing
from rq import Worker, Queue, Connection
from rq.registry import ScheduledJobRegistry

listen = ['high', 'default', 'low']

redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379')

conn = redis.from_url(redis_url)
q = Queue(connection=conn, default_timeout=7200)

def start_worker():
  with Connection(conn):
    worker = Worker([q])
    worker.work()

if __name__ == '__main__':
  processes = []
  for i in range(4):
    p = multiprocessing.Process(target=start_worker)
    p.start()
  
  for p in processes:
    p.join()

registry = ScheduledJobRegistry('default', connection=conn)