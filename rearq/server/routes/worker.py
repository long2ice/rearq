from fastapi import APIRouter, Depends

from rearq import ReArq, constants
from rearq.server.depends import get_rearq

router = APIRouter()


@router.delete("")
async def delete_worker(name: str, rearq: ReArq = Depends(get_rearq)):
    return await rearq.get_redis().hdel(constants.WORKER_KEY, name)
