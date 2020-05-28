import asyncio
import functools
import logging
from functools import wraps
from ssl import SSLContext
from typing import Any, Callable, List, Optional, Tuple, Union, Set

import aioredis
from aioredis import Redis

from rearq.exceptions import ConfigurationError, UsageError
from rearq.utils import timestamp_ms_now, to_ms_timestamp
from rearq.worker import Task

logger = logging.getLogger("rearq")


class ReArq:
    _redis: Optional[Redis] = None

    _delay_queue = "rearq:queue:delay"
    _health_check_key_suffix = "rearq:health-check"

    _default_queue: str = "rearq:queue:default"
    _task_map = {}
    _on_startup: Set[Callable] = {}
    _on_shutdown: Set[Callable] = {}

    def __init__(
            self,
            redis_host: Union[str, List[Tuple[str, int]]] = "127.0.0.1",
            redis_port: int = 6379,
            redis_password: Optional[str] = None,
            redis_db=0,
            ssl: Union[bool, None, SSLContext] = None,
            sentinel: bool = False,
            sentinel_master: str = "master",
    ):
        self.sentinel = sentinel
        self.ssl = ssl
        self.sentinel_master = sentinel_master
        self.redis_db = redis_db
        self.redis_port = redis_port
        self.redis_password = redis_password
        self.redis_host = redis_host

    async def init(self):
        if self._redis:
            return

        if type(self.redis_host) is str and self.sentinel:
            raise ConfigurationError(
                "str provided for `redis_host` but `sentinel` is true, list of sentinels expected"
            )

        if self.sentinel:
            addr = self.redis_host

            async def pool_factory(*args: Any, **kwargs: Any) -> Redis:
                client = await aioredis.sentinel.create_sentinel_pool(*args, ssl=self.ssl, **kwargs)
                return client.master_for(self.sentinel_master)

        else:
            pool_factory = functools.partial(aioredis.create_pool, ssl=self.ssl)
            addr = self.redis_host, self.redis_port
        pool = await pool_factory(
            addr, db=self.redis_db, password=self.redis_password, encoding="utf8"
        )
        self._redis = Redis(pool)

    @classmethod
    def get_redis(cls):
        if not cls._redis:
            raise UsageError("You must call .init() first!")
        return cls._redis

    def create_task(self, func: Callable, queue: Optional[str] = None):

        if not callable(func):
            raise UsageError("Task must be Callable!")
        if not self._redis:
            raise UsageError("You must call ReArq.init() first!")

        function = func.__name__
        self._task_map[function] = func

        return Task(
            function=function,
            queue=queue or self._default_queue,
            delay_queue=self._delay_queue,
        )

    def task(
            self, queue: Optional[str] = None,
    ):
        def wrapper(func: Callable):
            return self.create_task(func, queue)

        return wrapper

    def on_startup(self, func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            self._on_startup.add(func)
            return func(*args, **kwargs)

        return wrapper

    def on_shutdown(self, func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            self._on_shutdown.add(func)
            return func(*args, **kwargs)

        return wrapper

    async def shutdown(self):
        tasks = []
        for fun in self._on_shutdown:
            tasks.append(fun(self))
        if tasks:
            await asyncio.gather(*tasks)

    async def startup(self):
        tasks = []
        for fun in self._on_startup:
            tasks.append(fun(self))
        if tasks:
            await asyncio.gather(*tasks)

    async def close(self):
        if not self._redis:
            return
        self._redis.close()
        await self._redis.wait_closed()
        self._redis = None
