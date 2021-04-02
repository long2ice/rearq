import asyncio
import datetime
import functools
from functools import wraps
from ssl import SSLContext
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import aioredis
from aioredis import ConnectionsPool, Redis
from tortoise import Tortoise

from rearq.constants import DEFAULT_QUEUE, DELAY_QUEUE, QUEUE_KEY_PREFIX
from rearq.exceptions import ConfigurationError, UsageError
from rearq.task import CronTask, Task, check_pending_msgs

Serializer = Callable[[Dict[str, Any]], bytes]
Deserializer = Callable[[bytes], Dict[str, Any]]


class ReArq:
    _pool: Optional[ConnectionsPool] = None
    _redis: Optional[Redis] = None
    _task_map: Dict[str, Task] = {}
    _queue_task_map: Dict[str, List[str]] = {}
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
        job_retry_after: int = 60,
        max_jobs: int = 10,
        job_timeout: int = 300,
        expire: Optional[Union[float, datetime.datetime]] = None,
        tortoise_config: Optional[dict] = None,
    ):
        self.job_timeout = job_timeout
        self.max_jobs = max_jobs
        self.job_retry = job_retry
        self.job_retry_after = job_retry_after
        self.expire = expire
        self.sentinel = sentinel
        self.ssl = ssl
        self.sentinel_master = sentinel_master
        self.redis_db = redis_db
        self.redis_port = redis_port
        self.redis_password = redis_password
        self.redis_host = redis_host
        self.tortoise_config = tortoise_config

    async def init(self):
        if self._pool:
            return

        if type(self.redis_host) is str and self.sentinel:
            raise ConfigurationError(
                "str provided for `redis_host` but `sentinel` is true, list of sentinels expected"
            )

        if self.sentinel:
            addr = self.redis_host

            async def pool_factory(*args: Any, **kwargs: Any) -> ConnectionsPool:
                client = await aioredis.sentinel.create_sentinel_pool(*args, ssl=self.ssl, **kwargs)
                return client.master_for(self.sentinel_master)

        else:
            pool_factory = functools.partial(aioredis.create_pool, ssl=self.ssl)
            addr = self.redis_host, self.redis_port
        self._pool = await pool_factory(
            addr, db=self.redis_db, password=self.redis_password, encoding="utf8"
        )
        self._redis = Redis(self._pool)

        await Tortoise.init(config=self.tortoise_config)
        await Tortoise.generate_schemas()

    @property
    def redis(self):
        if not self._redis:
            raise UsageError("You must call .init() first!")
        return self._redis

    @property
    def task_map(self) -> Dict[str, Task]:
        return self._task_map

    @property
    def queue_task_map(self) -> Dict[str, List[str]]:
        return self._queue_task_map

    def get_queue_tasks(self, queue: str) -> List[str]:
        tasks = self._queue_task_map.get(queue)
        return tasks

    def create_task(
        self,
        bind: bool,
        func: Callable,
        queue: Optional[str] = None,
        cron: Optional[str] = None,
        name: Optional[str] = None,
        job_retry: Optional[int] = None,
        job_retry_after: Optional[int] = None,
        expire: Optional[Union[float, datetime.datetime]] = None,
        run_at_start: Optional[bool] = False,
    ):

        if not callable(func):
            raise UsageError("Task must be Callable!")

        function = name or func.__name__
        if function in (self._queue_task_map.get(queue) or []):
            raise UsageError("Task name must be unique!")

        if function != check_pending_msgs.__name__:
            self._queue_task_map.setdefault(queue, []).append(function)

        defaults = dict(
            function=func,
            queue=QUEUE_KEY_PREFIX + queue if queue else DEFAULT_QUEUE,
            rearq=self,
            job_retry=job_retry or self.job_retry,
            job_retry_after=job_retry_after or self.job_retry_after,
            expire=expire or self.expire,
            bind=bind,
        )
        if cron:
            t = CronTask(**defaults, cron=cron, run_at_start=run_at_start)
            CronTask.add_cron_task(function, t)
        else:
            t = Task(**defaults)
        self._task_map[function] = t
        return t

    def task(
        self,
        bind: bool = True,
        queue: Optional[str] = None,
        cron: Optional[str] = None,
        name: Optional[str] = None,
        job_retry: Optional[int] = None,
        job_retry_after: Optional[int] = None,
        expire: Optional[Union[float, datetime.datetime]] = None,
        run_at_start: Optional[bool] = False,
    ):
        if not cron and run_at_start:
            raise UsageError("run_at_start only work in cron task")

        def wrapper(func: Callable):
            return self.create_task(
                bind, func, queue, cron, name, job_retry, job_retry_after, expire, run_at_start
            )

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
        if not self._pool:
            return
        self._pool.close()
        await self._pool.wait_closed()
        self._pool = None
        await Tortoise.close_connections()

    async def cancel(self, job_id: str):
        """
        cancel delay task
        :param job_id:
        :return:
        """
        return await self._redis.zrem(DELAY_QUEUE, job_id)
