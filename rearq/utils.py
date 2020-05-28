import asyncio
import time
from datetime import datetime, timedelta
from typing import AsyncGenerator, Union


def to_ms_timestamp(value: Union[None, int, float, timedelta, datetime]):
    """
    covert to timestamp
    :param value:
    :return:
    """
    if isinstance(value, datetime):
        return round(value.timestamp() * 1000)
    if isinstance(value, timedelta):
        value = value.total_seconds()
    value = value or 0
    return round((time.time() + value) * 1000)


def timestamp_ms_now() -> int:
    """
    now timestamp
    :return:
    """
    return round(time.time() * 1000)


def ms_to_datetime(ms: int) -> datetime:
    """
    ms to datetime
    :param ms:
    :return:
    """
    return datetime.fromtimestamp(ms / 1000)


async def poll(step: float = 0.5) -> AsyncGenerator[float, None]:
    """
    make a async generator
    :param step:
    :return:
    """
    loop = asyncio.get_event_loop()
    start = loop.time()
    while True:
        before = loop.time()
        yield before - start
        after = loop.time()
        wait = max([0, step - after + before])
        await asyncio.sleep(wait)
