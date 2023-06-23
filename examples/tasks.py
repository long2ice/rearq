import asyncio

from loguru import logger
from tortoise import Tortoise

from examples import settings
from rearq import JOB_TIMEOUT_UNLIMITED, ReArq

rearq = ReArq(
    redis_url=settings.REDIS_URL,
    delay_queue_num=2,
    keep_job_days=30,
    expire=60,
    job_retry=0,
    raise_job_error=True,
)


@rearq.on_shutdown
async def on_shutdown():
    logger.debug("rearq is shutdown")


@rearq.on_startup
async def on_startup():
    logger.debug("rearq is startup")
    await Tortoise.init(
        db_url=settings.DB_URL,
        modules={"rearq": ["rearq.server.models"]},
    )


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
