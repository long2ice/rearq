import asyncio
import datetime
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple
from typing import List

from aioredis import MultiExecError
from aioredis import Redis
from pydantic import ValidationError

from .constants import in_progress_key_prefix, result_key_prefix, \
    job_key_prefix, retry_key_prefix
from .job import JobResult, JobDef

Serializer = Callable[[Dict[str, Any]], bytes]
Deserializer = Callable[[bytes], Dict[str, Any]]

logger = logging.getLogger("rearq")


@dataclass
class Worker:
    _redis: Redis
    _main_task: Optional[asyncio.Task] = None
    _function_map = {}

    def __init__(self, rearq, queues: List[str], max_jobs: int = 10, keep_result_seconds: int = 3600):
        self.keep_result_seconds = keep_result_seconds
        self.max_jobs = max_jobs
        self.rearq = rearq
        self.queues = queues

        self.loop = asyncio.get_event_loop()
        self.latest_ids = ['0-0' for _ in range(len(queues))]
        self.sem = asyncio.BoundedSemaphore(max_jobs)
        self.queue_read_limit = max(max_jobs * 5, 100)
        self.tasks: List[asyncio.Task[Any]] = []
        self._redis = rearq.get_redis()
        self._function_map = rearq.get_function_map()

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
        p = self._redis.pipeline()
        p.get(job_key_prefix + job_id)
        p.incr(retry_key_prefix + job_id)
        p.expire(retry_key_prefix + job_id, 88400)
        job_data, job_retry, _ = await p.execute()

        now = datetime.datetime.now()
        abort_job = self.abort_job(job_id=job_id, function='unknown', msg='job expired', args=(), kwargs={},
                                   job_retry=job_retry, enqueue_ms=0, queue='unknown', success=False,
                                   start_time=now, finish_time=now)
        if not job_data:
            logger.warning(f'job {job_id} expired')
            return asyncio.shield(abort_job)
        try:
            job = JobDef.parse_obj(job_data)
        except ValidationError:
            logger.exception(f'parse job {job_id} failed')
            return asyncio.shield(abort_job)
        fun = self._function_map.get(job.function)
        if not fun:
            logger.warning(f'job {job_id}, function {fun} not found')
            return await asyncio.shield(self.abort_job(
                job_id=job_id,
                function=job.function,
                msg=f'function {job.function} not found',
                args=job.args,
                kwargs=job.kwargs,
                job_retry=job.job_retry,
                enqueue_ms=job.enqueue_ms,
                queue=job.queue,
                success=False,
                start_time=now,
                finish_time=now
            ))

    async def abort_job(self, job_id: str, function: str, msg: str, args: Optional[Tuple[Any, ...]],
                        kwargs: Optional[Dict[Any, Any]], job_retry: int, enqueue_ms: int, queue: str, success: bool,
                        start_time: datetime, finish_time: datetime):
        job_result = JobResult(
            success=success,
            result=msg,
            start_time=start_time,
            finish_time=finish_time,
            job_id=job_id,
            function=function,
            args=args,
            kwargs=kwargs,
            job_retry=job_retry,
            enqueue_ms=enqueue_ms,
            queue=queue,
        )
        await self._redis.unwatch()
        tr = self._redis.multi_exec()
        tr.delete(retry_key_prefix + job_id, in_progress_key_prefix + job_id, job_key_prefix + job_id)
        tr.zrem(queue, job_id)
        if self.keep_result_seconds > 0:
            tr.setex(result_key_prefix + job_id, self.keep_result_seconds, job_result.json())
        await tr.execute()

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


class TimerWorker(Worker):
    def poll(self):
        pass
