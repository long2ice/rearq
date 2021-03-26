import json
import os

import aiofiles
from fastapi import APIRouter, Depends
from starlette.requests import Request
from tortoise import timezone

from rearq import ReArq, constants
from rearq.server import templates
from rearq.server.depends import get_rearq
from rearq.utils import ms_to_datetime

router = APIRouter()


@router.get("")
async def get_workers(request: Request, rearq: ReArq = Depends(get_rearq)):
    redis = rearq.get_redis
    workers_info = await redis.hgetall(constants.WORKER_KEY)
    workers = []
    for worker_name, value in workers_info.items():
        item = {"name": worker_name}
        item.update(json.loads(value))
        time = ms_to_datetime(item["ms"])
        item["time"] = time
        item["is_offline"] = (
            timezone.now() - time
        ).seconds > constants.WORKER_HEARTBEAT_SECONDS + 10
        workers.append(item)
    return templates.TemplateResponse(
        "worker.html", {"request": request, "page_title": "worker", "workers": workers}
    )


@router.delete("")
async def delete_worker(name: str, rearq: ReArq = Depends(get_rearq)):
    redis = rearq.get_redis
    return await redis.hdel(constants.WORKER_KEY, name)


@router.get("/log")
async def logs(name: str):
    log_file = os.path.join(constants.BASE_DIR, "logs", f"worker-{name}.log")
    async with aiofiles.open(log_file, mode="r") as f:
        content = await f.read()
    return content
