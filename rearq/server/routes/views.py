import datetime
import json
import os
from typing import Optional

from fastapi import APIRouter, Depends
from starlette.requests import Request
from starlette.templating import Jinja2Templates

from rearq import CronTask, ReArq, constants
from rearq.server.depends import get_rearq
from rearq.server.models import Result

router = APIRouter()
templates = Jinja2Templates(
    directory=os.path.join(constants.BASE_DIR, "rearq", "server", "templates")
)


@router.get("/")
async def index(request: Request):
    return templates.TemplateResponse(
        "dashboard.html", {"request": request, "page_title": "dashboard"}
    )


@router.get("/workers")
async def get_workers(request: Request, rearq: ReArq = Depends(get_rearq)):
    redis = rearq.get_redis
    workers_info = await redis.hgetall(constants.WORKER_KEY)
    workers = []
    for worker_name, value in workers_info.items():
        item = {"name": worker_name}
        item.update(json.loads(value))
        time = datetime.datetime.fromtimestamp(item["ts"])
        item["time"] = time
        item["is_offline"] = (
            datetime.datetime.now() - time
        ).seconds > constants.WORKER_HEARTBEAT_SECONDS + 10
        workers.append(item)
    return templates.TemplateResponse(
        "workers.html", {"request": request, "page_title": "workers", "workers": workers}
    )


@router.get("/tasks")
async def get_tasks(request: Request, rearq: ReArq = Depends(get_rearq)):
    task_map = rearq.task_map
    task_last_time = await rearq.get_redis.hgetall(constants.TASK_LAST_TIME)
    tasks = [
        {
            "name": task_name,
            "queue": task.queue,
            "cron": task.cron if isinstance(task, CronTask) else "",
            "last_time": datetime.datetime.fromtimestamp(int(task_last_time.get(task_name)) / 1000)
            if task_last_time.get(task_name)
            else "Never",
        }
        for task_name, task in task_map.items()
    ]
    return templates.TemplateResponse(
        "tasks.html", {"request": request, "page_title": "tasks", "tasks": tasks}
    )


@router.get("/results")
async def get_results(
    request: Request,
    rearq: ReArq = Depends(get_rearq),
    task: Optional[str] = None,
    job_id: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
):
    qs = Result.all()
    if task:
        qs = qs.filter(task=task)
    if job_id:
        qs = qs.filter(job_id=job_id)
    if start_time:
        qs = qs.filter(start_time=start_time)
    if end_time:
        qs = qs.filter(end_time=end_time)
    results = await qs
    return templates.TemplateResponse(
        "results.html",
        {
            "request": request,
            "page_title": "results",
            "tasks": rearq.task_map.keys(),
            "current_task": task,
            "results": results,
        },
    )
