import os

import redis
import multiprocessing
from rq import Worker, Queue, Connection
from rq.registry import ScheduledJobRegistry
from dotenv import load_dotenv

load_dotenv()
listen = ['high', 'default', 'low']

redis_host = os.getenv('REDIS_HOST', 'redis')
conn = redis.Redis(host=redis_host, port=6379, decode_responses=False)
q = Queue(connection=conn, default_timeout=7200)

def start_worker():
  with Connection(conn):
    worker = Worker([q])
    worker.work()

if __name__ == '__main__':
  processes = []
  for i in range(8):
    p = multiprocessing.Process(target=start_worker)
    p.start()
  
  for p in processes:
    p.join()

registry = ScheduledJobRegistry('default', connection=conn)