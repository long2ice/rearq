import time

import pytest

from conftest import add


@pytest.mark.asyncio
async def test_add_job():
    job_id = str(time.time())
    job = await add.delay(job_id=job_id, args=(1, 2))
    info = await job.info()
    assert (
        info.function == "add"
        and info.args == (1, 2)
        and info.job_retry == 3
        and info.job_id == job_id
        and info.queue == "rearq:queue:default"
    )
