import asyncio
import os

from loguru import logger

from rearq import ReArq, Task

rearq = ReArq(db_url=f"mysql://root:{os.getenv('MYSQL_PASS') or '123456'}@127.0.0.1:3306/test")


@rearq.on_shutdown
async def on_shutdown():
    logger.debug("rearq is shutdown")


@rearq.on_startup
async def on_startup():
    logger.debug("rearq is startup")


@rearq.task()
async def add(self: Task, a, b):
    return a + b


@rearq.task()
async def sleep(self: Task, time: float):
    return await asyncio.sleep(time)


@rearq.task(cron="*/5 * * * * * *")
async def timer_add(self: Task):
    return "timer"
