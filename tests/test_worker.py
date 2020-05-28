import pytest

from conftest import rearq
from rearq.worker import Worker


@pytest.mark.asyncio
async def test_run():
    worker = Worker(rearq, queues=['rearq:queue:default'])
    await worker.async_run()


def test_start_up():
    @rearq.on_shutdown
    async def on_shutdown():
        print('shutdown')


def test_shutdown():
    @rearq.on_shutdown
    async def on_shutdown():
        print('shutdown')
