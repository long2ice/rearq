from aioredis import Redis
from fastapi import Depends
from starlette.requests import Request

from rearq import ReArq


def get_rearq(request: Request) -> ReArq:
    return request.app.rearq


def get_redis(rearq=Depends(get_rearq)) -> Redis:
    return rearq.redis


def get_pager(limit: int = 10, offset: int = 0):
    return limit, offset
