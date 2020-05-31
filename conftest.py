import asyncio

import pytest
from crontab import CronTab

from rearq import ReArq
from rearq.log import init_logging

rearq = ReArq()


@rearq.on_shutdown
async def on_shutdown():
    print('shutdown')


@rearq.on_startup
async def on_startup():
    print('startup')


@rearq.task()
async def add(a, b):
    return a + b


# @rearq.task(cron=CronTab('*/5 * * * * * *'))
# async def timer_add(a, b):
#     return a + b


@pytest.fixture(scope="session")
def loop():
    loop = asyncio.get_event_loop()
    return loop


@pytest.fixture(scope="session", autouse=True)
def initialize_tests(loop, request):
    init_logging(True)
    loop.run_until_complete(rearq.init())
