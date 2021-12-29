import asyncio
import datetime
import hashlib
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Set, Union

import aioredis
import aioredis.sentinel
from aioredis import Redis

from rearq import constants
from rearq.constants import DELAY_QUEUE_CHANNEL
from rearq.exceptions import UsageError
from rearq.task import CronTask, Task, is_built_task

Serializer = Callable[[Dict[str, Any]], bytes]
Deserializer = Callable[[bytes], Dict[str, Any]]


class ReArq:
    _redis: Optional[Redis] = None
    _task_map: Dict[str, Task] = {}
    _queue_task_map: Dict[str, List[str]] = {}
    _on_startup: Set[Callable] = set()
    _on_shutdown: Set[Callable] = set()

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        sentinels: Optional[List[str]] = None,
        sentinel_master: str = "master",
        job_retry: int = 3,
        job_retry_after: int = 60,
        max_jobs: int = 10,
        job_timeout: int = 300,
        expire: Optional[Union[float, datetime.datetime]] = None,
        delay_queue_num: int = 1,
        keep_job_days: Optional[int] = None,
    ):
        """
        :param sentinel_master:
        :param job_retry: Default job retry times.
        :param job_retry_after: Default job retry after seconds.
        :param max_jobs: Max concurrent jobs.
        :param job_timeout: Job max timeout.
        :param expire: Job default expire time.
        :param delay_queue_num: How many key to store delay tasks, for large number of tasks, split it to improve performance.
        """
        self.job_timeout = job_timeout
        self.max_jobs = max_jobs
        self.job_retry = job_retry
        self.job_retry_after = job_retry_after
        self.expire = expire
        self.sentinels = sentinels
        self.sentinel_master = sentinel_master
        self.redis_url = redis_url
        self.delay_queue_num = delay_queue_num
        self.keep_job_days = keep_job_days

    async def init(self):
        if self._redis:
            return

        if self.sentinels:
            sentinel = aioredis.sentinel.Sentinel(self.sentinels, decode_responses=True)
            redis = sentinel.master_for(self.sentinel_master)

        else:
            redis = aioredis.from_url(
                self.redis_url,
                decode_responses=True,
            )
        self._redis = redis

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
        return tasks or []

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

        if not is_built_task(function):
            self._queue_task_map.setdefault(queue, []).append(function)

        defaults = dict(
            function=func,
            queue=constants.QUEUE_KEY_PREFIX + queue if queue else constants.DEFAULT_QUEUE,
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
        """
        Task decorator
        :param bind: Bind task obj to first argument.
        :param queue: Default queue.
        :param cron: See https://github.com/josiahcarlson/parse-crontab, if defined, which is a cron task.
        :param name: Task name, default is function name.
        :param job_retry: Override default job retry.
        :param job_retry_after: Override default job retry after.
        :param expire: Override default expire.
        :param run_at_start: Whether run at startup, only available for cron task.
        :return:
        """
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
        if not self._redis:
            return
        await self._redis.close()
        self._redis = None

    def get_delay_queue(self, data: str):
        num = int(hashlib.md5(data.encode()).hexdigest(), 16) % self.delay_queue_num  # nosec:B303
        return f"{constants.DELAY_QUEUE}:{num}"

    @property
    def delay_queues(self):
        for num in range(self.delay_queue_num):
            yield f"{constants.DELAY_QUEUE}:{num}"

    async def zadd(self, score: int, data: str):
        queue = self.get_delay_queue(data)
        return await self.redis.zadd(queue, mapping={data: score})

    async def pub_delay(self, ms: float):
        return await self.redis.publish(DELAY_QUEUE_CHANNEL, ms)
