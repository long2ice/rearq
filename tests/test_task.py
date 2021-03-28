import time

import pytest

from examples.tasks import add


@pytest.mark.asyncio
async def test_add_job():
    job_id = str(time.time())
    job = await add.delay(job_id=job_id, args=(1, 2))
    assert job.task == "add" and job.args == (1, 2) and job.job_retry == 3 and job.job_id == job_id
