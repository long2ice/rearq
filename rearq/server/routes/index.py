from fastapi import APIRouter, Depends
from pypika.functions import Date
from starlette.requests import Request
from tortoise.functions import Count, Function

from rearq import ReArq, constants
from rearq.server import templates
from rearq.server.depends import get_rearq, get_redis
from rearq.server.models import Job, JobResult

router = APIRouter()


class ToDate(Function):
    database_func = Date


@router.get("/", include_in_schema=False)
async def index(request: Request, rearq: ReArq = Depends(get_rearq), redis=Depends(get_redis)):
    task_map = rearq.task_map
    task_num = len(task_map)
    workers_info = await redis.hgetall(constants.WORKER_KEY)
    worker_num = len(workers_info)
    run_times = await JobResult.all().count()
    result = (
        await Job.all()
        .annotate(count=Count("id"), date=ToDate("enqueue_time"))
        .group_by("date", "status")
        .order_by("date")
        .values("date", "status", "count")
    )
    x_axis = []
    series = [
        {
            "name": "Deferred",
            "type": "line",
            "data": [],
            "label": {"show": "true"},
        },
        {
            "name": "Queued",
            "type": "line",
            "data": [],
            "label": {"show": "true"},
        },
        {
            "name": "InProgress",
            "type": "line",
            "data": [],
            "label": {"show": "true"},
        },
        {
            "name": "Success",
            "type": "line",
            "data": [],
            "label": {"show": "true"},
        },
        {
            "name": "Failed",
            "type": "line",
            "data": [],
            "label": {"show": "true"},
        },
        {
            "name": "Expired",
            "type": "line",
            "data": [],
            "label": {"show": "true"},
        },
    ]
    for item in result:
        date = str(item.get("date"))
        if date not in x_axis:
            x_axis.append(date)
        count = item.get("count")
        status = item.get("status")
        if status == "deferred":
            series[0]["data"].append(count)
        elif status == "queued":
            series[1]["data"].append(count)
        elif status == "in_progress":
            series[2]["data"].append(count)
        elif status == "success":
            series[3]["data"].append(count)
        elif status == "failed":
            series[4]["data"].append(count)
        elif status == "expired":
            series[5]["data"].append(count)
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "page_title": "dashboard",
            "worker_num": worker_num,
            "task_num": task_num,
            "run_times": run_times,
            "x_axis": x_axis,
            "series": series,
        },
    )
