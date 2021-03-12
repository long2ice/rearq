from fastapi import Depends, FastAPI, HTTPException
from starlette.status import HTTP_404_NOT_FOUND

from .. import ReArq
from ..job import Job
from .depends import get_rearq
from .schemas import AddJobIn

app = FastAPI()


@app.get("/job")
async def get_job(job_id: str, queue: str = 'default', rearq: ReArq = Depends(get_rearq)):
    job = Job(rearq.get_redis(), job_id, queue)
    return await job.info()


@app.get("/job/result")
async def get_job_result(job_id: str, queue: str = 'default', rearq: ReArq = Depends(get_rearq)):
    job = Job(rearq.get_redis(), job_id, queue)
    return await job.result_info()


@app.post("/job")
async def add_job(add_job_in: AddJobIn, rearq: ReArq = Depends(get_rearq)):
    task = rearq.get_task_map().get(add_job_in.task)
    if not task:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail='No such task')
    job = await task.delay(args=add_job_in.args, kwargs=add_job_in.kwargs, job_id=add_job_in.job_id)
    return await job.info()


@app.delete("/job")
async def cancel_job(job_id: str, rearq: ReArq = Depends(get_rearq)):
    ret = await rearq.cancel(job_id)
    return ret
