from typing import Optional

from fastapi import APIRouter, Depends
from redis.asyncio.client import Redis
from starlette.requests import Request

from rearq import ReArq, constants
from rearq.server import templates
from rearq.server.depends import get_pager, get_rearq, get_redis
from rearq.server.models import JobResult
from rearq.server.responses import JobResultListOut

router = APIRouter()


@router.get("/data", response_model=JobResultListOut)
async def get_results(
    task: Optional[str] = None,
    job_id: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    worker: Optional[str] = None,
    success: Optional[str] = None,
    pager=Depends(get_pager),
):
    qs = JobResult.all().select_related("job")
    if task:
        qs = qs.filter(job__task=task)
    if job_id:
        qs = qs.filter(job__job_id=job_id)
    if start_time:
        qs = qs.filter(start_time__gte=start_time)
    if end_time:
        qs = qs.filter(start_time__lte=end_time)
    if worker:
        qs = qs.filter(worker=worker)
    if success:
        qs = qs.filter(success=success == "1")
    results = await qs.limit(pager[0]).offset(pager[1])
    return {"rows": results, "total": await qs.count()}


@router.delete("")
async def delete_result(ids: str):
    return await JobResult.filter(id__in=ids.split(",")).delete()


@router.get("", include_in_schema=False)
async def result(
    request: Request,
    redis: Redis = Depends(get_redis),
    rearq: ReArq = Depends(get_rearq),
):
    workers_info = await redis.hgetall(constants.WORKER_KEY)

    return templates.TemplateResponse(
        "result.html",
        {
            "request": request,
            "page_title": "result",
            "tasks": rearq.task_map.keys(),
            "workers": workers_info.keys(),
        },
    )
