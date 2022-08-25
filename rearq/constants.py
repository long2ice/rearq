import os

QUEUE_KEY_PREFIX = "rearq:queue:"
DEFAULT_QUEUE: str = f"{QUEUE_KEY_PREFIX}default"
DELAY_QUEUE = f"{QUEUE_KEY_PREFIX}delay"
TASK_KEY = "rearq:task"
WORKER_KEY = "rearq:worker"
WORKER_KEY_LOCK = "rearq:worker:lock"
WORKER_KEY_TIMER_LOCK = "rearq:timer:lock"
WORKER_TASK_LOCK = "rearq:task:lock:{task}"
WORKER_HEARTBEAT_SECONDS = 10
STATIC_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKER_DIR = os.getcwd()
JOB_TIMEOUT_UNLIMITED = object()
CHANNEL = "rearq:channel"
