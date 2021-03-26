from fastapi import APIRouter, Depends, HTTPException
from starlette.requests import Request
from starlette.status import HTTP_404_NOT_FOUND

from rearq import CronTask, ReArq, constants
from rearq.server import templates
from rearq.server.depends import get_rearq
from rearq.server.schemas import AddJobIn
from rearq.utils import ms_to_datetime

router = APIRouter()


@router.get("")
async def get_tasks(request: Request, rearq: ReArq = Depends(get_rearq)):
    task_map = rearq.task_map
    task_last_time = await rearq.get_redis.hgetall(constants.TASK_LAST_TIME)
    tasks = []
    cron_tasks = []
    for task_name, task in task_map.items():
        item = {
            "name": task_name,
            "queue": task.queue,
        }
        if task_last_time.get(task_name):
            item["last_time"] = ms_to_datetime(int(task_last_time.get(task_name)))
        if isinstance(task, CronTask):
            item["cron"] = task.cron
            task.set_next()
            item["next_time"] = ms_to_datetime(task.next_run)
            cron_tasks.append(item)
        else:
            tasks.append(item)
    return templates.TemplateResponse(
        "task.html",
        {"request": request, "page_title": "task", "tasks": tasks, "cron_tasks": cron_tasks},
    )


@router.post("")
async def add_job(add_job_in: AddJobIn, rearq: ReArq = Depends(get_rearq)):
    task = rearq.task_map.get(add_job_in.task)
    if not task:
        raise HTTPException(status_code=HTTP_404_NOT_FOUND, detail="No such task")
    job = await task.delay(**add_job_in.dict(exclude={"task"}))
    return await job.info()
