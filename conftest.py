import asyncio

import pytest

from rearq import ReArq, Task
from rearq.log import init_logging

rearq = ReArq()


@rearq.on_shutdown
async def on_shutdown():
    print("shutdown")


@rearq.on_startup
async def on_startup():
    print("startup")


@rearq.task()
async def add(self: Task, a, b):
    return a + b


@rearq.task(cron="*/5 * * * * * *")
async def timer_add(self: Task):
    return "timer"


@pytest.fixture(scope="session")
def loop():
    loop = asyncio.get_event_loop()
    return loop


@pytest.fixture(scope="session", autouse=True)
def initialize_tests(loop, request):
    init_logging(True)
    loop.run_until_complete(rearq.init())
