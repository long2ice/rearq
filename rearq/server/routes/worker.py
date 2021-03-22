import os

import aiofiles
from fastapi import APIRouter, Depends

from rearq import ReArq, constants
from rearq.server.depends import get_rearq

router = APIRouter()


@router.delete("")
async def delete_worker(name: str, rearq: ReArq = Depends(get_rearq)):
    redis = rearq.get_redis
    return await redis.hdel(constants.WORKER_KEY, name)


@router.get("/log/{name}")
async def logs(name: str):
    log_file = os.path.join(constants.BASE_DIR, "logs", f"worker-{name}.log")
    async with aiofiles.open(log_file, mode="r") as f:
        content = await f.read()
    return content
