from fastapi import APIRouter, Depends
from starlette.requests import Request

from rearq import ReArq, constants
from rearq.server import templates
from rearq.server.depends import get_rearq
from rearq.server.models import Result

router = APIRouter()


@router.get("/")
async def index(request: Request, rearq: ReArq = Depends(get_rearq)):
    task_map = rearq.task_map
    task_num = len(task_map)
    workers_info = await rearq.get_redis.hgetall(constants.WORKER_KEY)
    worker_num = len(workers_info)
    run_times = await Result.all().count()
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "page_title": "dashboard",
            "worker_num": worker_num,
            "task_num": task_num,
            "run_times": run_times,
        },
    )
