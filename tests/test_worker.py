import pytest

from conftest import rearq
from rearq import Worker


@pytest.mark.asyncio
async def test_run():
    worker = Worker(rearq, queues=['rearq:queue:default'])
    await worker.async_run()
