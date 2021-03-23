from fastapi import APIRouter, Depends

from rearq import ReArq
from rearq.job import Job
from rearq.server.depends import get_rearq

router = APIRouter()


@router.get("")
async def get_job(job_id: str, queue: str = "default", rearq: ReArq = Depends(get_rearq)):
    job = Job(rearq.get_redis, job_id, queue)
    return await job.info()


@router.get("/result")
async def get_job_result(job_id: str, queue: str = "default", rearq: ReArq = Depends(get_rearq)):
    job = Job(rearq.get_redis, job_id, queue)
    return await job.result_info()


@router.delete("")
async def cancel_job(job_id: str, rearq: ReArq = Depends(get_rearq)):
    ret = await rearq.cancel(job_id)
    return ret
