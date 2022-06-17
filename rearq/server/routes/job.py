from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from redis.asyncio.client import Redis
from starlette.requests import Request
from starlette.status import HTTP_404_NOT_FOUND, HTTP_409_CONFLICT

from rearq import ReArq, constants
from rearq.enums import JobStatus
from rearq.server import templates
from rearq.server.depends import get_pager, get_rearq, get_redis
from rearq.server.models import Job, JobResult
from rearq.server.responses import JobListOut, JobOut
from rearq.server.schemas import AddJobIn, TaskStatus, UpdateJobIn

router = APIRouter()


@router.get("", include_in_schema=False)
async def job_page(
    request: Request, redis: Redis = Depends(get_redis), rearq: ReArq = Depends(get_rearq)
):
    workers_info = await redis.hgetall(constants.WORKER_KEY)
    return templates.TemplateResponse(
        "job.html",
        {
            "request": request,
            "page_title": "job",
            "tasks": rearq.task_map.keys(),
            "workers": workers_info.keys(),
            "job_status": JobStatus,
        },
    )


@router.get("/data", response_model=JobListOut)
async def get_results(
    task: Optional[str] = None,
    job_id: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    status: Optional[str] = None,
    pager=Depends(get_pager),
):
    qs = Job.all()
    if task:
        qs = qs.filter(task=task)
    if job_id:
        qs = qs.filter(job_id=job_id)
    if start_time:
        qs = qs.filter(enqueue_time__gte=start_time)
    if end_time:
        qs = qs.filter(enqueue_time__lte=end_time)
    if status:
        qs = qs.filter(status=status)
    results = await qs.limit(pager[0]).offset(pager[1])
    return {"rows": results, "total": await qs.count()}


@router.get("/result")
async def get_job_result(job_id: str):
    return await JobResult.get_or_none(job_id=job_id)


@router.put("")
async def update_job(update_job_in: UpdateJobIn):
    job = await Job.get_or_none(job_id=update_job_in.job_id)
    if not job:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="Can't find job")
    if job.status in [JobStatus.queued, JobStatus.deferred]:
        await job.update_from_dict(update_job_in.dict(exclude_unset=True)).save()
    else:
        raise HTTPException(status_code=HTTP_409_CONFLICT, detail="Can't update job")


@router.delete("")
async def delete_job(ids: str):
    return await Job.filter(id__in=ids.split(",")).delete()


@router.post("", response_model=JobOut)
async def add_job(add_job_in: AddJobIn, rearq: ReArq = Depends(get_rearq)):
    task = rearq.task_map.get(add_job_in.task)
    if await rearq.redis.hget(constants.TASK_KEY, add_job_in.task) == TaskStatus.disabled:
        raise HTTPException(status_code=HTTP_409_CONFLICT, detail="Task is disabled")
    if not task:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="No such task")
    job = await task.delay(**add_job_in.dict(exclude={"task"}))
    return job
