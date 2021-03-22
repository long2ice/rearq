import datetime
import json

from fastapi import APIRouter, Depends
from starlette.requests import Request
from starlette.templating import Jinja2Templates

from rearq import CronTask, ReArq, constants
from rearq.server.depends import get_rearq

router = APIRouter()
templates = Jinja2Templates(directory="rearq/server/templates")


@router.get("/")
async def index(request: Request):
    return templates.TemplateResponse(
        "dashboard.html", {"request": request, "page_title": "dashboard"}
    )


@router.get("/workers")
async def workers(request: Request, rearq: ReArq = Depends(get_rearq)):
    redis = await rearq.get_redis()
    workers_info = await redis.hgetall(constants.WORKER_KEY)
    workers = []
    for worker_name, value in workers_info.items():
        item = {"name": worker_name}
        item.update(json.loads(value))
        time = datetime.datetime.fromtimestamp(item["ts"])
        item["time"] = time.strftime("%Y-%m-%d %H:%M:%S")
        item["is_offline"] = (
            datetime.datetime.now() - time
        ).seconds > constants.WORKER_HEARTBEAT_SECONDS + 10
        workers.append(item)
    return templates.TemplateResponse(
        "workers.html", {"request": request, "page_title": "workers", "workers": workers}
    )


@router.get("/tasks")
async def tasks(request: Request, rearq: ReArq = Depends(get_rearq)):
    task_map = rearq.task_map
    tasks = [
        {
            "name": task_name,
            "queue": task.queue,
            "cron": task.cron if isinstance(task, CronTask) else "",
        }
        for task_name, task in task_map.items()
    ]
    return templates.TemplateResponse(
        "tasks.html", {"request": request, "page_title": "tasks", "tasks": tasks}
    )


@router.get("/results")
async def results(request: Request):
    return templates.TemplateResponse("results.html", {"request": request, "page_title": "results"})
