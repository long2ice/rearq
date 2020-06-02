import asyncio
import datetime
import logging
import signal
from dataclasses import dataclass
from functools import partial
from signal import Signals
from typing import Any, Callable, Dict, Optional, Tuple
from typing import List
from uuid import uuid4

import async_timeout
from aioredis import MultiExecError
from aioredis import Redis
from aioredis.commands import MultiExec
from aioredis.errors import BusyGroupError, PipelineError
from crontab import CronTab
from pydantic import ValidationError

from . import timestamp_ms_now, CronTask
from .constants import in_progress_key_prefix, result_key_prefix, \
    job_key_prefix, retry_key_prefix, delay_queue, queue_key_prefix, default_queue
from .job import JobResult, JobDef
from .utils import args_to_string, poll, to_ms_timestamp

Serializer = Callable[[Dict[str, Any]], bytes]
Deserializer = Callable[[bytes], Dict[str, Any]]

logger = logging.getLogger("rearq.worker")
no_result = object()


class Worker:
    _redis: Redis
    _main_task: Optional[asyncio.Task] = None
    _function_map = {}

    def __init__(self, rearq, queues: Optional[List[str]] = None, group_name='default', consumer_name='default'):
        self.consumer_name = consumer_name
        self.group_name = group_name
        self.job_timeout = rearq.job_timeout
        self.keep_result_seconds = rearq.keep_result_seconds
        self.max_jobs = rearq.max_jobs
        self.rearq = rearq
        self.queues = [default_queue] if not queues else [f'{queue_key_prefix}:{queue}' for queue in queues]
        self.loop = asyncio.get_event_loop()
        self.latest_ids = ['>' for _ in range(len(self.queues))]
        self.sem = asyncio.BoundedSemaphore(self.max_jobs)
        self.queue_read_limit = max(self.max_jobs * 5, 100)
        self.tasks: List[asyncio.Task[Any]] = []
        self._redis = rearq.get_redis()
        self._function_map = rearq.get_function_map()
        self.jobs_complete = 0
        self.jobs_retried = 0
        self.jobs_failed = 0
        self.job_retry = rearq.job_retry
        self.in_progress_timeout = self.job_timeout + 10

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

    async def _xgroup_create(self):
        p = self._redis.pipeline()
        for queue in self.queues:
            p.xgroup_create(queue, self.group_name, latest_id='>', mkstream=True)
        try:
            return await p.execute()
        except PipelineError:
            pass

    async def _main(self) -> None:
        logger.info('Start worker success')
        await self.log_redis_info()
        await self.rearq.startup()
        await self._xgroup_create()
        while True:
            async with self.sem:
                msgs = await self._redis.xread_group(self.group_name, self.consumer_name, self.queues,
                                                     count=self.queue_read_limit,
                                                     latest_ids=self.latest_ids)
                for msg in msgs:
                    await self.sem.acquire()
                    queue, msg_id, job = msg
                    job_id = job.get('job_id')
                    in_progress_key = in_progress_key_prefix + job_id
                    p = self._redis.pipeline()
                    p.unwatch(),
                    p.watch(in_progress_key)
                    p.exists(in_progress_key)
                    _, _, ongoing_exists = await p.execute()
                    if ongoing_exists:
                        logger.debug('job %s already running elsewhere', job_id)
                        await self._redis.xack(queue, self.group_name, msg_id)
                        continue
                    tr = self._redis.multi_exec()
                    tr.setex(in_progress_key, self.in_progress_timeout, b'1')
                    try:
                        await tr.execute()
                    except MultiExecError:
                        self.sem.release()
                        logger.debug('multi-exec error, job %s already started elsewhere', job_id)
                        await asyncio.gather(*tr._results, return_exceptions=True)
                    else:
                        task = self.loop.create_task(self.run_job(queue, msg_id, job_id))
                        task.add_done_callback(lambda _: self.sem.release())
                        self.tasks.append(task)
            await asyncio.gather(*self.tasks)

    async def run_job(self, queue: str, msg_id: str, job_id: str):
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
            await self._redis.xack(queue, self.group_name, msg_id)
            return asyncio.shield(abort_job)
        try:
            job = JobDef.parse_raw(job_data.encode())
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
        if job.job_retry and job.job_retry > job_retry:
            job_retry = job.job_retry
            await self._redis.setex(retry_key_prefix + job_id, 88400, str(job_retry))
        ref = f'{job_id}:{job.function}'

        if job_retry > job.job_retry:
            t = (timestamp_ms_now() - job.enqueue_ms) / 1000
            logger.warning('%6.2fs ! %s max retries %d exceeded', t, ref, job.job_retry)
            return await asyncio.shield(self.abort_job(
                job_id=job_id,
                function=job.function,
                msg=f'max {job.job_retry} retries exceeded',
                args=job.args,
                kwargs=job.kwargs,
                job_retry=job.job_retry,
                enqueue_ms=job.enqueue_ms,
                queue=job.queue,
                success=False,
                start_time=now,
                finish_time=now
            ))
        start_ms = timestamp_ms_now()
        result = no_result

        try:
            async with async_timeout.timeout(self.job_timeout):
                result = await self._function_map.get(job.function)(self, *job.args, **job.kwargs)
        except Exception as e:
            success = False
            finish = False
            finished_ms = 0
        else:
            success = True
            finished_ms = timestamp_ms_now()
            logger.info('%6.2fs ← %s ● %s', (finished_ms - start_ms) / 1000, ref, result)
            finish = True
            self.jobs_complete += 1

        result_data = None
        if result is not no_result and self.keep_result_seconds > 0:
            result_data = JobResult(
                success=success,
                result=result,
                start_time=start_ms,
                finish_time=finished_ms,
                job_id=job_id,
                function=job.function,
                args=job.args,
                kwargs=job.kwargs,
                job_retry=job_retry,
                enqueue_ms=job.enqueue_ms,
                queue=job.queue,
            )

        await asyncio.shield(self.finish_job(job_id, finish, result_data, self.keep_result_seconds))

    async def finish_job(
            self,
            job_id: str,
            finish: bool,
            result_data: Optional[bytes],
            result_timeout_s: Optional[float],
    ) -> None:
        await self._redis.unwatch()
        tr = self._redis.multi_exec()
        delete_keys = [in_progress_key_prefix + job_id]
        if finish:
            if result_data:
                tr.setex(result_key_prefix + job_id, result_timeout_s, result_data)
            delete_keys += [retry_key_prefix + job_id, job_key_prefix + job_id]
            tr.delete(*delete_keys)
            await tr.execute()

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
        try:
            await self._main()
        except asyncio.CancelledError:
            pass
        finally:
            await self.close()

    async def close(self):
        if not self._redis:
            return
        await self.rearq.shutdown()
        await self.rearq.close()


class TimerWorker(Worker):
    async def _main(self) -> None:
        logger.info('Start timer worker success')
        await self.log_redis_info()
        await self.rearq.startup()
        await self.init_timer()
        async for _ in poll():
            await self._poll_iteration()

    async def init_timer(self):
        cron_tasks = CronTask.get_cron_tasks()
        tasks = []
        for task in cron_tasks.values():
            tasks.append(
                task.delay(
                    job_id=uuid4().hex,
                    countdown=task.cron.next(default_utc=True),
                    job_retry=task.job_retry,
                )
            )
        if tasks:
            await asyncio.gather(*tasks)
            logger.info(f'Success init timer {list(cron_tasks.values())}')

    async def _poll_iteration(self):
        now = timestamp_ms_now()
        jobs_id = await self._redis.zrangebyscore(
            delay_queue, offset=0, count=self.queue_read_limit, max=now
        )
        if not jobs_id:
            return
        else:
            jobs_key = list(map(lambda x: job_key_prefix + x, jobs_id))
        jobs = await self._redis.mget(*jobs_key) or []  # type:List[str]
        p = self._redis.pipeline()
        for job in jobs:
            job_def = JobDef.parse_raw(job.encode())
            p.xadd(job_def.queue, {"job_id": job_def.job_id})

            cron_task = CronTask.get_cron_tasks().get(job_def.function)
            if cron_task:
                logger.info(f'Timer trigger {cron_task}')
                next_job_id = uuid4().hex
                next_job = JobDef(
                    function=job_def.function,
                    args=job_def.args,
                    kwargs=job_def.kwargs,
                    job_retry=job_def.job_retry,
                    enqueue_ms=timestamp_ms_now(),
                    queue=job_def.queue,
                    job_id=next_job_id
                )
                defer = to_ms_timestamp(cron_task.cron.next(default_utc=True))
                p.zadd(delay_queue, defer, next_job_id)
                p.psetex(
                    job_key_prefix + next_job_id,
                    defer - job_def.enqueue_ms + cron_task.expires_extra_ms,
                    next_job.json(),
                )
        if jobs_id:
            p.zrem(delay_queue, *jobs_id)
            await p.execute()
