import asyncio

import pytest

from rearq import ReArq

rearq = ReArq()


@rearq.on_shutdown
async def on_shutdown():
    print('shutdown')


@rearq.on_startup
async def on_startup():
    print('startup')


@pytest.fixture(scope="session")
def loop():
    loop = asyncio.get_event_loop()
    return loop


@pytest.fixture(scope="session", autouse=True)
def initialize_tests(loop, request):
    loop.run_until_complete(rearq.init())
