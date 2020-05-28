import asyncio
import datetime
import functools
import logging
from dataclasses import dataclass
from functools import wraps
from ssl import SSLContext
from typing import Any, Callable, Dict, Optional, Tuple, Union
from typing import List
from typing import Set
from uuid import uuid4

import aioredis
from aioredis import MultiExecError
from aioredis import Redis
from aioredis.commands import MultiExec

from rearq.exceptions import ConfigurationError, UsageError
from rearq.utils import timestamp_ms_now, to_ms_timestamp
from .constants import queue_key_prefix, default_queue, delay_queue, in_progress_key_prefix, result_key_prefix, \
    job_key_prefix
from .jobs import JobDef, Job
from .utils import timestamp_ms_now, to_ms_timestamp

Serializer = Callable[[Dict[str, Any]], bytes]
Deserializer = Callable[[bytes], Dict[str, Any]]

logger = logging.getLogger("rearq")


@dataclass
class Task:
    function: str
    queue: str
    delay_queue: str
    rearq: 'ReArq'
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
        expires_ms = (
            to_ms_timestamp(expires) if expires else defer_ts - enqueue_ms + self._expires_extra_ms
        )
        job_key = job_key_prefix + job_id
        redis = self.rearq.get_redis()

        pipe = redis.pipeline()
        pipe.unwatch()
        pipe.watch(job_key)
        job_exists = pipe.exists(job_key)
        job_result_exists = pipe.exists(result_key_prefix + job_id)
        await pipe.execute()
        if await job_exists or await job_result_exists:
            return None

        tr = redis.multi_exec()  # type:MultiExec
        tr.psetex(
            job_key,
            expires_ms,
            JobDef(
                function=self.function,
                args=args,
                kwargs=kwargs,
                retry_times=retry_times,
                enqueue_ms=enqueue_ms,
                queue=self.queue
            ).json(),
        )

        if not eta and not countdown:
            tr.xadd(self.queue, {"job_id": job_id})
        else:
            tr.zadd(self.delay_queue, defer_ts, job_id)

        try:
            await tr.execute()
        except MultiExecError as e:
            logger.warning(f"MultiExecError: {e}")
            await asyncio.gather(*tr._results, return_exceptions=True)
            return None
        return Job(
            self.rearq,
            job_id,
            self.queue,
        )


@dataclass
class Worker:
    _redis: Redis
    _main_task: Optional[asyncio.Task] = None

    def __init__(self, rearq: 'ReArq', queues: List[str], max_jobs: int = 10):
        self.max_jobs = max_jobs
        self.rearq = rearq
        self.queues = queues
        self._redis = rearq.get_redis()
        self.loop = asyncio.get_event_loop()
        self.latest_ids = ['0-0' for _ in range(len(queues))]
        self.sem = asyncio.BoundedSemaphore(max_jobs)
        self.queue_read_limit = max(max_jobs * 5, 100)

    async def log_redis_info(self, ) -> None:
        p = self._redis.pipeline()
        p.info()
        p.dbsize()
        info, key_count = await p.execute()

        logger.info(
            f'redis_version={info["server"]["redis_version"]} '
            f'mem_usage={info["memory"]["used_memory_human"]} '
            f'clients_connected={info["clients"]["connected_clients"]} '
            f'db_keys={key_count}'
        )

    async def _main(self) -> None:
        logger.info('Start worker success')
        await self.log_redis_info()
        await self.rearq.startup()

        while True:
            async with self.sem:
                msgs = await self._redis.xread(self.queues, count=self.queue_read_limit, latest_ids=self.latest_ids)
                for msg in msgs:
                    queue, msg_id, job = msg
                    job_id = job.get('job_id')
                    in_progress_key = in_progress_key_prefix + job_id
                    p = self._redis.pipeline()
                    p.unwatch(),
                    p.watch(in_progress_key)
                    p.exists(in_progress_key)
                    _, _, ongoing_exists = await p.execute()
                    if ongoing_exists:
                        self.sem.release()
                        logger.debug('job %s already running elsewhere', job_id)
                        continue
                    tr = self._redis.multi_exec()
                    tr.setex(in_progress_key, b'1')
                    try:
                        await tr.execute()
                    except MultiExecError:
                        self.sem.release()
                        logger.debug('multi-exec error, job %s already started elsewhere', job_id)
                        await asyncio.gather(*tr._results, return_exceptions=True)
                    else:
                        task = self.loop.create_task(self.run_job(job_id))
                        task.add_done_callback(lambda _: self.sem.release())
                        self.tasks.append(task)

    async def run_job(self, job_id: str):
        pass

    def run(self):
        """
        Run main task
        :return:
        """
        self._main_task = self.loop.create_task(self._main())
        try:
            self.loop.run_until_complete(self._main_task)
        except asyncio.CancelledError:
            pass
        finally:
            self.loop.run_until_complete(self.close())

    async def async_run(self):
        """
        Asynchronously run the worker, does not close connections. Useful when testing.
        """
        self._main_task = self.loop.create_task(self._main())
        await self._main_task

    async def close(self):
        if not self._redis:
            return
        await self.rearq.shutdown()
        await self.rearq.close()


class Timer(Worker):
    def poll(self):
        pass


class ReArq:
    _redis: Optional[Redis] = None

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

    def get_redis(self):
        if not self._redis:
            raise UsageError("You must call .init() first!")
        return self._redis

    def create_task(self, func: Callable, queue: Optional[str] = None):

        if not callable(func):
            raise UsageError("Task must be Callable!")

        function = func.__name__
        self._task_map[function] = func

        return Task(
            function=function,
            queue=queue_key_prefix + queue if queue else default_queue,
            delay_queue=delay_queue,
            rearq=self
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
