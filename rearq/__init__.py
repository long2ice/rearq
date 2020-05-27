import asyncio
import datetime
import functools
import json
import logging
from dataclasses import dataclass
from ssl import SSLContext
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from uuid import uuid4

import aioredis
from aioredis import MultiExecError, Redis

from rearq.exceptions import ConfigurationError, UsageError
from rearq.jobs import Job
from rearq.serialize import Serializer, Deserializer, serialize_job
from rearq.utils import to_ms_timestamp, timestamp_ms_now

logger = logging.getLogger("rearq")


@dataclass
class Task:
    function: str
    queue: str
    redis: Redis
    job_serializer: Serializer
    job_deserializer: Deserializer

    _job_key_prefix = 'rearq:job:'
    _in_progress_key_prefix = 'rearq:in-progress:'
    _result_key_prefix = 'rearq:result:'
    _retry_key_prefix = 'rearq:retry:'
    _health_check_key_suffix = 'rearq:health-check'
    _queue_key_prefix = 'rearq:queue:'
    _expires_extra_ms = 86_400_000

    async def delay(
            self,
            args: Tuple[Any, ...],
            kwargs: Dict[str, Any],
            job_id: str = uuid4().hex,
            countdown: Union[float, datetime.timedelta] = 0,
            eta: Optional[datetime.datetime] = None,
            expires: Optional[Union[float, datetime.datetime]] = None,
            retry_times: int = 0,
    ):
        if countdown:
            defer_ts = to_ms_timestamp(countdown)
        elif eta:
            defer_ts = to_ms_timestamp(eta)
        else:
            defer_ts = timestamp_ms_now()
        enqueue_ms = timestamp_ms_now()
        expires_ms = to_ms_timestamp(expires) if expires else defer_ts - enqueue_ms + self._expires_extra_ms
        job_key = self._job_key_prefix + job_id
        queue_name = self._queue_key_prefix + self.queue

        pipe = self.redis.pipeline()
        pipe.unwatch()
        pipe.watch(job_key)
        job_exists = pipe.exists(job_key)
        job_result_exists = pipe.exists(self._result_key_prefix + job_id)
        await pipe.execute()
        if await job_exists or await job_result_exists:
            return None
        job = serialize_job(self.function, args, kwargs, retry_times, enqueue_ms, serializer=self.job_serializer)
        tr = self.redis.multi_exec()
        tr.psetex(job_key, expires_ms, job)
        tr.zadd(queue_name, defer_ts, job_id)
        try:
            await tr.execute()
        except MultiExecError:
            await asyncio.gather(*tr._results, return_exceptions=True)
            return None
        return Job(job_id, redis=self.redis, queue_name=queue_name, deserializer=self.job_deserializer)


class ReArq:
    _redis: Redis = None

    job_serializer: Serializer
    job_deserializer: Deserializer

    _default_queue: str = 'rearq:queue'
    _task_map = {}

    @classmethod
    async def init(
            cls,
            redis_host: Union[str, List[Tuple[str, int]]] = "127.0.0.1",
            redis_port: int = 6379,
            redis_password: Optional[str] = None,
            redis_db=0,
            ssl: Union[bool, None, SSLContext] = None,
            sentinel: bool = False,
            sentinel_master: str = "master",
            job_serializer: Serializer = json.dumps,
            job_deserializer: Deserializer = json.loads,
    ):
        if cls._redis:
            raise UsageError("You can only call ReArq.init once!")

        cls.job_serializer = job_serializer
        cls.job_deserializer = job_deserializer

        if type(redis_host) is str and sentinel:
            raise ConfigurationError(
                "str provided for `redis_host` but `sentinel` is true, list of sentinels expected"
            )

        if sentinel:
            addr = redis_host

            async def pool_factory(*args: Any, **kwargs: Any) -> Redis:
                client = await aioredis.sentinel.create_sentinel_pool(*args, ssl=ssl, **kwargs)
                return client.master_for(sentinel_master)

        else:
            pool_factory = functools.partial(aioredis.create_pool, ssl=ssl)
            addr = redis_host, redis_port
        pool = await pool_factory(addr, db=redis_db, password=redis_password, encoding="utf8")
        cls._redis = Redis(pool)

    @classmethod
    def create_task(cls, func: Callable, queue: Optional[str] = None):

        if not callable(func):
            raise UsageError("Task must be Callable!")
        if not cls._redis:
            raise UsageError("You must call ReArq.init() first!")

        function = func.__name__
        cls._task_map[function] = func

        return Task(
            function=function,
            queue=queue or cls._default_queue,
            redis=cls._redis,
            job_deserializer=cls.job_deserializer,
            job_serializer=cls.job_serializer,
        )

    @classmethod
    def task(
            cls, queue: Optional[str] = None,
    ):
        def wrapper(func: Callable):
            return cls.create_task(func, queue)

        return wrapper

    @classmethod
    def close(cls):
        cls._redis.close()
