import pytest

from conftest import rearq
from rearq.worker import TimerWorker, Worker


@pytest.mark.asyncio
async def test_worker_run():
    worker = Worker(rearq)
    await worker.async_run()


@pytest.mark.asyncio
async def test_timer_worker_run():
    worker = TimerWorker(rearq)
    await worker.async_run()
