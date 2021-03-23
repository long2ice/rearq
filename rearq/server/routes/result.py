from typing import Optional

from fastapi import APIRouter, Depends
from starlette.requests import Request

from rearq import ReArq
from rearq.server import templates
from rearq.server.depends import get_pager, get_rearq
from rearq.server.models import Result

router = APIRouter()


@router.get("/data")
async def get_results(
    task: Optional[str] = None,
    job_id: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    pager=Depends(get_pager),
):
    limit = pager[0]
    offset = pager[1]
    qs = Result.all()
    if task:
        qs = qs.filter(task=task)
    if job_id:
        qs = qs.filter(job_id=job_id)
    if start_time:
        qs = qs.filter(start_time__gte=start_time)
    if end_time:
        qs = qs.filter(end_time__lte=end_time)
    results = await qs.limit(limit).offset(offset)
    return {"rows": results, "total": await qs.count()}


@router.get("")
async def result(
    request: Request, rearq: ReArq = Depends(get_rearq),
):
    return templates.TemplateResponse(
        "result.html",
        {"request": request, "page_title": "result", "tasks": rearq.task_map.keys(),},
    )
