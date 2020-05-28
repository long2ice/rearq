import pytest

from conftest import rearq


@rearq.task()
async def add(a, b):
    return a + b


@pytest.mark.asyncio
async def test_add_job():
    job = await add.delay(1, 2)
    print(job)
