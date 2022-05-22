import asyncio
import os

from loguru import logger

from rearq import ReArq

rearq = ReArq(
    db_url=f"mysql://root:{os.getenv('MYSQL_PASS')}@localhost:3306/rearq",
    redis_url=f"redis://:{os.getenv('REDIS_PASS')}@localhost:6379/0",
    delay_queue_num=2,
    keep_job_days=7,
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


@rearq.task()
async def sleep(time: float):
    return await asyncio.sleep(time)


@rearq.task(cron="0 * * * *", run_at_start=True)
async def timer_add():
    return "timer"
