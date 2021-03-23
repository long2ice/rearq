from starlette.requests import Request

from rearq import ReArq


async def get_rearq(request: Request) -> ReArq:
    return request.app.rearq


def get_pager(limit: int = 10, offset: int = 0):
    return limit, offset
