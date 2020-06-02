import pytest

from conftest import add


@pytest.mark.asyncio
async def test_add_job():
    job = await add.delay(args=(1, 2))
    print(job)
