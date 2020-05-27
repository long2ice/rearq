import pytest

from rearq import ReArq


@pytest.mark.asyncio
async def test_task_decorator():
    @ReArq.task(queue="queue")
    async def mytask(a, b):
        return a + b

    job = await mytask.delay(1, 2)
    print(job)
