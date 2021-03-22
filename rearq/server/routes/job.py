from fastapi import APIRouter, Depends, HTTPException
from starlette.status import HTTP_404_NOT_FOUND

from rearq import ReArq
from rearq.job import Job
from rearq.server.depends import get_rearq
from rearq.server.schemas import AddJobIn

router = APIRouter()


@router.get("")
async def get_job(job_id: str, queue: str = "default", rearq: ReArq = Depends(get_rearq)):
    job = Job(rearq.get_redis, job_id, queue)
    return await job.info()


@router.get("/result")
async def get_job_result(job_id: str, queue: str = "default", rearq: ReArq = Depends(get_rearq)):
    job = Job(rearq.get_redis, job_id, queue)
    return await job.result_info()


@router.post("")
async def add_job(add_job_in: AddJobIn, rearq: ReArq = Depends(get_rearq)):
    task = rearq.task_map.get(add_job_in.task)
    if not task:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="No such task")
    job = await task.delay(**add_job_in.dict(exclude={"task"}))
    return await job.info()


@router.delete("")
async def cancel_job(job_id: str, rearq: ReArq = Depends(get_rearq)):
    ret = await rearq.cancel(job_id)
    return ret
