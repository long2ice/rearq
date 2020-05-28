import asyncio
import datetime
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple, Union
from typing import List
from uuid import uuid4

from aioredis import MultiExecError
from aioredis import Redis
from aioredis.commands import MultiExec
from .jobs import JobDef, Job
from .utils import timestamp_ms_now, to_ms_timestamp

Serializer = Callable[[Dict[str, Any]], bytes]
Deserializer = Callable[[bytes], Dict[str, Any]]

logger = logging.getLogger("rearq.worker")


@dataclass
class Task:
    function: str
    queue: str
    delay_queue: str
    _job_key_prefix = "rearq:job:"
    _in_progress_key_prefix = "rearq:in-progress:"
    _result_key_prefix = "rearq:result:"
    _retry_key_prefix = "rearq:retry:"
    _queue_key_prefix = "rearq:queue:"
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
        redis = ReArq.get_redis()
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
        job_key = self._job_key_prefix + job_id
        queue_name = self._queue_key_prefix + self.queue

        pipe = redis.pipeline()
        pipe.unwatch()
        pipe.watch(job_key)
        job_exists = pipe.exists(job_key)
        job_result_exists = pipe.exists(self._result_key_prefix + job_id)
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
                queue=queue_name
            ).json(),
        )

        if not eta and not countdown:
            tr.xadd(queue_name, {"job_id": job_id})
        else:
            tr.zadd(self.delay_queue, defer_ts, job_id)

        try:
            await tr.execute()
        except MultiExecError as e:
            logger.warning(f"MultiExecError: {e}")
            await asyncio.gather(*tr._results, return_exceptions=True)
            return None
        return Job(
            job_id,
            self._job_key_prefix,
            self._result_key_prefix,
            self._in_progress_key_prefix,
            queue_name,
        )


@dataclass
class Worker:
    _redis: Redis
    _main_task: Optional[asyncio.Task] = None

    def __init__(self, rearq: ReArq, queues: List[str], group_name: str = 'default', max_jobs: int = 10,
                 consumer_name: str = 'default'):
        self.consumer_name = consumer_name
        self.max_jobs = max_jobs
        self.group_name = group_name
        self.rearq = rearq
        self.queues = queues
        self._redis = rearq.get_redis()
        self.loop = asyncio.get_event_loop()

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

    async def _create_group(self):
        tasks = []
        for queue in self.queues:
            tasks.append(self._redis.xgroup_create(queue, self.group_name, mkstream=True, latest_id='>'))
        await asyncio.gather(*tasks)

    async def _main(self) -> None:
        logger.info('Start worker success')
        await self.log_redis_info()
        await self.rearq.startup()
        await self._create_group()

        while True:
            msgs = await self._redis.xread_group(self.group_name, self.consumer_name, self.queues, count=self.max_jobs)
            print(msgs)

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
