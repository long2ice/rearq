from fastapi import APIRouter, Depends

from rearq import ReArq
from rearq.server.depends import get_rearq

router = APIRouter()


@router.get("")
async def get_tasks(rearq: ReArq = Depends(get_rearq)):
    pass
