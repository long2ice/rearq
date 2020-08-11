import asyncio
import functools
import logging
from functools import wraps
from ssl import SSLContext
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import aioredis
from aioredis import Redis

from rearq.constants import default_queue, delay_queue, queue_key_prefix
from rearq.exceptions import ConfigurationError, UsageError
from rearq.task import CronTask, Task

Serializer = Callable[[Dict[str, Any]], bytes]
Deserializer = Callable[[bytes], Dict[str, Any]]

logger = logging.getLogger("rearq")


class ReArq:
    _redis: Optional[Redis] = None
    _task_map: Dict[str, Task] = {}
    _on_startup: Set[Callable] = set()
    _on_shutdown: Set[Callable] = set()

    def __init__(
        self,
        redis_host: Union[str, List[Tuple[str, int]]] = "127.0.0.1",
        redis_port: int = 6379,
        redis_password: Optional[str] = None,
        redis_db=0,
        ssl: Union[bool, None, SSLContext] = None,
        sentinel: bool = False,
        sentinel_master: str = "master",
        job_retry: int = 3,
        max_jobs: int = 10,
        keep_result_seconds: int = 3600,
        job_timeout: int = 300,
    ):
        self.job_timeout = job_timeout
        self.keep_result_seconds = keep_result_seconds
        self.max_jobs = max_jobs
        self.job_retry = job_retry

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

    def get_redis(self) -> Redis:
        if not self._redis:
            raise UsageError("You must call .init() first!")
        return self._redis

    def get_task_map(self) -> Dict[str, Task]:
        return self._task_map

    def create_task(self, func: Callable, queue: Optional[str] = None, cron: Optional[str] = None):

        if not callable(func):
            raise UsageError("Task must be Callable!")

        function = func.__name__
        defaults = dict(
            function=func,
            queue=queue_key_prefix + queue if queue else default_queue,
            rearq=self,
            job_retry=self.job_retry,
        )
        if cron:
            t = CronTask(**defaults, cron=cron)
            CronTask.add_cron_task(function, t)
        else:
            t = Task(**defaults)
        self._task_map[function] = t
        return t

    def task(self, queue: Optional[str] = None, cron: Optional[str] = None):
        def wrapper(func: Callable):
            return self.create_task(func, queue, cron)

        return wrapper

    def on_startup(self, func: Callable):
        self._on_startup.add(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    def on_shutdown(self, func: Callable):
        self._on_shutdown.add(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    async def shutdown(self):
        tasks = []
        for fun in self._on_shutdown:
            tasks.append(fun())
        if tasks:
            await asyncio.gather(*tasks)

    async def startup(self):
        tasks = []
        for fun in self._on_startup:
            tasks.append(fun())
        if tasks:
            await asyncio.gather(*tasks)

    async def close(self):
        if not self._redis:
            return
        self._redis.close()
        await self._redis.wait_closed()
        self._redis = None

    async def cancel(self, job_id: str):
        """
        cancel delay task
        :param job_id:
        :return:
        """
        return await self._redis.zrem(delay_queue, job_id)
