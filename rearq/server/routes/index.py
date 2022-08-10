import datetime

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
    end_date = datetime.datetime.today()
    start_date = end_date - datetime.timedelta(days=30)
    result = (
        await Job.annotate(count=Count("id"), date=ToDate("enqueue_time"))
        .filter(date__gt=start_date)
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
    dates = [
        (start_date + datetime.timedelta(days=d)).date()
        for d in range((end_date - start_date).days + 1)
    ]
    for d in dates:
        x_axis.append(str(d))
        has = False
        for item in result:
            date = item.get("date")
            if date == d:
                has = True
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
        if not has:
            series[0]["data"].append(0)
            series[1]["data"].append(0)
            series[2]["data"].append(0)
            series[3]["data"].append(0)
            series[4]["data"].append(0)
            series[5]["data"].append(0)
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
