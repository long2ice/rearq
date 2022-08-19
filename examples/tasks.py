import asyncio

from loguru import logger

from examples import settings
from rearq import ReArq

rearq = ReArq(
    db_url=settings.DB_URL,
    redis_url=settings.REDIS_URL,
    delay_queue_num=2,
    keep_job_days=7,
    expire=60,
    trace_exception=True,
)


@rearq.on_shutdown
async def on_shutdown():
    logger.debug("rearq is shutdown")


@rearq.on_startup
async def on_startup():
    logger.debug("rearq is startup")


@rearq.task()
async def add(a, b):
    return a + b


@rearq.task(run_with_lock=True)
async def run_with_lock():
    await asyncio.sleep(5)
    return "run_with_lock"


@rearq.task()
async def sleep(time: float):
    return await asyncio.sleep(time)


@rearq.task(cron="0 * * * *", run_at_start=True)
async def timer_add():
    return "timer"
