from starlette.requests import Request

from rearq import ReArq


async def get_rearq(request: Request) -> ReArq:
    return request.app.rearq
