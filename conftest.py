import asyncio

import pytest

from rearq import ReArq, Task

rearq = ReArq(db_url="mysql://root:123456@127.0.0.1:3306/rearq")


@rearq.on_shutdown
async def on_shutdown():
    print("shutdown")


@rearq.on_startup
async def on_startup():
    print("startup")


@rearq.task()
async def add(self: Task, a, b):
    return a + b


@rearq.task()
async def sleep(self: Task, time: float):
    return await asyncio.sleep(time)


@rearq.task(cron="*/5 * * * * * *")
async def timer_add(self: Task):
    return "timer"


@pytest.fixture(scope="session")
def loop():
    loop = asyncio.get_event_loop()
    return loop


@pytest.fixture(scope="session", autouse=True)
def initialize_tests(loop, request):
    loop.run_until_complete(rearq.init())
