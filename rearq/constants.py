import os

JOB_KEY_PREFIX = "rearq:job:"
IN_PROGRESS_KEY_PREFIX = "rearq:in-progress:"
RESULT_KEY_PREFIX = "rearq:result:"
RETRY_KEY_PREFIX = "rearq:retry:"
QUEUE_KEY_PREFIX = "rearq:queue:"
HEALTH_CHECK_KEY_SUFFIX = "rearq:health-check:"
DEFAULT_QUEUE: str = f"{QUEUE_KEY_PREFIX}default"
DELAY_QUEUE = f"{QUEUE_KEY_PREFIX}delay"
WORKER_KEY = "rearq:worker"
WORKER_KEY_LOCK = "rearq:worker:lock"
WORKER_HEARTBEAT_SECONDS = 10
TASK_LAST_TIME = "rearq:task_last_time"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
