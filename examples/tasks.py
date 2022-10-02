import asyncio

from loguru import logger

from examples import settings
from rearq import JOB_TIMEOUT_UNLIMITED, ReArq

rearq = ReArq(
    db_url=settings.DB_URL,
    cluster_nodes=settings.REDIS_CLUSTER_NODES.split(","),
    delay_queue_num=2,
    keep_job_days=7,
    expire=60,
    job_retry=0,
    trace_exception=True,
)


@rearq.on_shutdown
async def on_shutdown():
    logger.debug("rearq is shutdown")


@rearq.on_startup
async def on_startup():
    logger.debug("rearq is startup")


@rearq.task(run_at_start=(1, 1))
async def add(a, b):
    return a + b


@rearq.task(run_with_lock=True)
async def run_with_lock():
    await asyncio.sleep(10)
    return "run_with_lock"


@rearq.task(job_timeout=JOB_TIMEOUT_UNLIMITED)
async def sleep(time: float):
    return await asyncio.sleep(time)


@rearq.task(cron="0 * * * *")
async def timer_add():
    return "timer"
