import asyncio
import os

from loguru import logger
from tortoise import Tortoise

from rearq import ReArq, Task
from rearq.server import models

rearq = ReArq(delay_queue_num=2, keep_job_days=7)


@rearq.on_shutdown
async def on_shutdown():
    logger.debug("rearq is shutdown")
    await Tortoise.close_connections()


@rearq.on_startup
async def on_startup():
    await Tortoise.init(
        db_url=f"mysql://root:{os.getenv('MYSQL_PASS') or '123456'}@127.0.0.1:3306/rearq",
        modules={"models": [models]},
    )
    logger.debug("rearq is startup")


@rearq.task()
async def add(self: Task, a, b):
    return a + b


@rearq.task()
async def sleep(self: Task, time: float):
    return await asyncio.sleep(time)


@rearq.task(cron="0 * * * *", run_at_start=True)
async def timer_add(self: Task):
    return "timer"
