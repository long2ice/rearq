import json

from fastapi import APIRouter, Depends
from redis.asyncio.client import Redis
from starlette.requests import Request
from tortoise import timezone
from tortoise.functions import Count

from rearq import constants
from rearq.server import templates
from rearq.server.depends import get_redis
from rearq.server.models import JobResult
from rearq.utils import ms_to_datetime

router = APIRouter()


@router.get("", include_in_schema=False, name="rearq.get_workers")
async def get_workers(request: Request, redis: Redis = Depends(get_redis)):
    workers_info = await redis.hgetall(constants.WORKER_KEY)
    workers = []
    for worker_name, value in workers_info.items():
        job_stat = (
            await JobResult.filter(worker=worker_name)
            .annotate(count=Count("job_id"))
            .group_by("job__status")
            .values(
                "count",
                status="job__status",
            )
        )
        item = {
            "name": worker_name,
            "job_stat": {job["status"]: job["count"] for job in job_stat},
        }
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


@router.delete("", name="rearq.delete_worker")
async def delete_worker(name: str, redis: Redis = Depends(get_redis)):
    return await redis.hdel(constants.WORKER_KEY, name)
