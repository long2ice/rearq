import asyncio
import logging
import signal
from functools import partial
from signal import Signals
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from uuid import uuid4

import async_timeout
from aioredis import MultiExecError, Redis
from aioredis.errors import BusyGroupError
from pydantic import ValidationError

from rearq import CronTask
from rearq.constants import (
    default_queue,
    delay_queue,
    in_progress_key_prefix,
    job_key_prefix,
    queue_key_prefix,
    result_key_prefix,
    retry_key_prefix,
)
from rearq.job import JobDef, JobResult
from rearq.task import check_pending_msgs
from rearq.utils import args_to_string, poll, timestamp_ms_now

Serializer = Callable[[Dict[str, Any]], bytes]
Deserializer = Callable[[bytes], Dict[str, Any]]

logger = logging.getLogger("rearq.worker")
no_result = object()


class Worker:
    _redis: Redis
    _main_task: Optional[asyncio.Task] = None
    _task_map = {}

    def __init__(
        self, rearq, queue: Optional[str] = None, group_name="default",
    ):
        self.group_name = group_name
        self.job_timeout = rearq.job_timeout
        self.keep_result_seconds = rearq.keep_result_seconds
        self.max_jobs = rearq.max_jobs
        self.rearq = rearq
        if queue:
            self.queue = queue_key_prefix + queue
        else:
            self.queue = default_queue
        self.loop = asyncio.get_event_loop()
        self.sem = asyncio.BoundedSemaphore(self.max_jobs)
        self.queue_read_limit = max(self.max_jobs * 5, 100)
        self.tasks: Set[asyncio.Task[Any]] = set()
        self._redis = rearq.get_redis()
        self._task_map = rearq.get_task_map()
        self.jobs_complete = 0
        self.jobs_retried = 0
        self.jobs_failed = 0
        self.job_retry = rearq.job_retry
        self.in_progress_timeout = self.job_timeout + 10
        self._add_signal_handler(signal.SIGINT, self.handle_sig)
        self._add_signal_handler(signal.SIGTERM, self.handle_sig)
        self.rearq.create_task(check_pending_msgs, queue, "* * * * *")

    def _add_signal_handler(self, signum: Signals, handler: Callable[[Signals], None]) -> None:
        self.loop.add_signal_handler(signum, partial(handler, signum))

    def handle_sig(self, signum: Signals) -> None:
        sig = Signals(signum)
        logger.info(
            "shutdown on %s ◆ %d jobs complete ◆ %d failed ◆ %d retries ◆ %d ongoing to cancel",
            sig.name,
            self.jobs_complete,
            self.jobs_failed,
            self.jobs_retried,
            len(self.tasks),
        )
        for t in asyncio.Task.all_tasks():
            t.cancel()

    async def log_redis_info(self,) -> None:
        p = self._redis.pipeline()
        p.info()
        p.dbsize()
        info, key_count = await p.execute()

        logger.info(
            f'redis_version={info["server"]["redis_version"]} '
            f'mem_usage={info["memory"]["used_memory_human"]} '
            f'clients_connected={info["clients"]["connected_clients"]} '
            f"db_keys={key_count}"
        )

    async def _main(self) -> None:
        logger.info(f"Start worker success with queue: {self.queue}")
        await self.log_redis_info()
        await self.rearq.startup()
        try:
            await self._redis.xgroup_create(
                self.queue, self.group_name, latest_id="$", mkstream=True
            )
        except BusyGroupError:
            pass
        length = len(await self._redis.xinfo_groups(self.queue))
        self.consumer_name = f"{self.group_name}-{length}"
        while True:
            async with self.sem:
                msgs = await self._redis.xread_group(
                    self.group_name,
                    self.consumer_name,
                    [self.queue],
                    count=self.queue_read_limit,
                    latest_ids=[">"],
                )
                for msg in msgs:
                    await self.sem.acquire()
                    queue, msg_id, job = msg
                    job_id = job.get("job_id")
                    in_progress_key = in_progress_key_prefix + job_id
                    ongoing_exists = await self._redis.exists(in_progress_key)
                    if ongoing_exists:
                        logger.debug("job %s already running elsewhere", job_id)
                        await self._xack(queue, msg_id)
                        continue
                    tr = self._redis.multi_exec()
                    tr.setex(in_progress_key, self.in_progress_timeout, b"1")
                    try:
                        await tr.execute()
                    except MultiExecError:
                        self.sem.release()
                        logger.debug("multi-exec error, job %s already started elsewhere", job_id)
                        await asyncio.gather(*tr._results, return_exceptions=True)
                    else:
                        task = self.loop.create_task(self.run_job(queue, msg_id, job_id))
                        task.add_done_callback(self._task_done)
                        self.tasks.add(task)
            await asyncio.gather(*self.tasks)

    def _task_done(self, task):
        self.sem.release()
        self.tasks.remove(task)

    async def _xack(self, queue: str, msg_id: str):
        await self._redis.xack(queue, self.group_name, msg_id)

    async def run_job(self, queue: str, msg_id: str, job_id: str):
        p = self._redis.pipeline()
        p.get(job_key_prefix + job_id)
        p.incr(retry_key_prefix + job_id)
        p.expire(retry_key_prefix + job_id, 88400)
        job_data, job_retry, _ = await p.execute()

        abort_job_data = dict(
            job_id=job_id,
            function="unknown",
            msg="job expired",
            args=(),
            kwargs={},
            job_retry=job_retry,
            enqueue_ms=0,
            queue="unknown",
            success=False,
            start_ms=timestamp_ms_now(),
            finish_ms=timestamp_ms_now(),
        )
        if not job_data:
            logger.warning(f"job {job_id} expired")
            await self._xack(queue, msg_id)
            return await asyncio.shield(self.abort_job(**abort_job_data))
        try:
            job_def = JobDef.parse_raw(job_data.encode())
        except ValidationError:
            logger.exception(f"parse job {job_id} failed")
            return await asyncio.shield(self.abort_job(**abort_job_data))
        task = self._task_map.get(job_def.function)
        if not task:
            logger.warning(f"job {job_id}, task {job_def.function} not found")
            return await asyncio.shield(
                self.abort_job(
                    job_id=job_id,
                    function=job_def.function,
                    msg=f"task {job_def.function} not found",
                    args=job_def.args,
                    kwargs=job_def.kwargs,
                    job_retry=job_def.job_retry,
                    enqueue_ms=job_def.enqueue_ms,
                    queue=job_def.queue,
                    success=False,
                    start_ms=timestamp_ms_now(),
                    finish_ms=timestamp_ms_now(),
                )
            )

        ref = f"{job_id}:{job_def.function}"

        if job_retry > job_def.job_retry + 1:
            t = (timestamp_ms_now() - job_def.enqueue_ms) / 1000
            logger.warning("%6.2fs ! %s max retries %d exceeded", t, ref, job_def.job_retry)
            return await asyncio.shield(
                self.abort_job(
                    job_id=job_id,
                    function=job_def.function,
                    msg=f"max {job_def.job_retry} retries exceeded",
                    args=job_def.args,
                    kwargs=job_def.kwargs,
                    job_retry=job_def.job_retry,
                    enqueue_ms=job_def.enqueue_ms,
                    queue=job_def.queue,
                    success=False,
                    start_ms=timestamp_ms_now(),
                    finish_ms=timestamp_ms_now(),
                )
            )
        start_ms = timestamp_ms_now()
        result = no_result
        logger.info(
            "%6.2fs → %s(%s)%s",
            (start_ms - job_def.enqueue_ms) / 1000,
            ref,
            args_to_string(job_def.args, job_def.kwargs),
            f" try={job_retry}" if job_retry > 1 else "",
        )
        try:
            async with async_timeout.timeout(self.job_timeout):
                if job_def.function == check_pending_msgs.__name__:
                    result = await task.function(
                        task, self.queue, self.group_name, self.consumer_name, self.job_timeout
                    )
                else:
                    result = await task.function(
                        task, *(job_def.args or []), **(job_def.kwargs or {})
                    )
        except Exception as e:
            success = False
            finish = False
            finished_ms = 0
            logger.error(f"Run task error, function: {job_def.function}, e: {e}", exc_info=True)
        else:
            success = True
            finished_ms = timestamp_ms_now()
            logger.info("%6.2fs ← %s ● %s", (finished_ms - start_ms) / 1000, ref, result)
            finish = True
            self.jobs_complete += 1
            await self._xack(queue, msg_id)
        if result is not no_result and self.keep_result_seconds > 0:
            result_data = JobResult(
                success=success,
                result=result,
                start_ms=start_ms,
                finish_ms=finished_ms,
                job_id=job_id,
                function=job_def.function,
                args=job_def.args,
                kwargs=job_def.kwargs,
                job_retry=job_retry,
                enqueue_ms=job_def.enqueue_ms,
                queue=job_def.queue,
            )

            await asyncio.shield(
                self.finish_job(
                    job_id, finish, result_data.json().encode(), self.keep_result_seconds
                )
            )

    async def finish_job(
        self,
        job_id: str,
        finish: bool,
        result_data: Optional[bytes],
        result_timeout_s: Optional[float],
    ) -> None:
        p = self._redis.pipeline()
        delete_keys = [in_progress_key_prefix + job_id]
        if finish:
            if result_data:
                p.setex(result_key_prefix + job_id, result_timeout_s, result_data)
            delete_keys += [retry_key_prefix + job_id, job_key_prefix + job_id]
            p.delete(*delete_keys)
            await p.execute()

    async def abort_job(
        self,
        job_id: str,
        function: str,
        msg: str,
        args: Optional[Tuple[Any, ...]],
        kwargs: Optional[Dict[Any, Any]],
        job_retry: int,
        enqueue_ms: int,
        queue: str,
        success: bool,
        start_ms: int,
        finish_ms: int,
    ):
        job_result = JobResult(
            success=success,
            result=msg,
            start_ms=start_ms,
            finish_ms=finish_ms,
            job_id=job_id,
            function=function,
            args=args,
            kwargs=kwargs,
            job_retry=job_retry,
            enqueue_ms=enqueue_ms,
            queue=queue,
        )
        p = self._redis.pipeline()
        p.delete(
            retry_key_prefix + job_id, in_progress_key_prefix + job_id, job_key_prefix + job_id
        )
        if self.keep_result_seconds > 0:
            p.setex(result_key_prefix + job_id, self.keep_result_seconds, job_result.json())
        await p.execute()

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
        logger.info(f"Start timer worker success with queue: {self.queue}")
        await self.log_redis_info()
        await self.rearq.startup()

        async for _ in poll():
            await self._poll_iteration()
            await self.run_cron()

    async def run_cron(self):
        """
        run cron task
        :return:
        """
        cron_tasks = CronTask.get_cron_tasks()
        p = self._redis.pipeline()
        execute = False
        for function, task in cron_tasks.items():
            if timestamp_ms_now() >= task.next_run:
                execute = True
                logger.info(f"{task.function.__name__}()")
                next_job_id = uuid4().hex
                job_key = job_key_prefix + next_job_id
                enqueue_ms = timestamp_ms_now()
                p.psetex(
                    job_key,
                    task.expires_extra_ms,
                    JobDef(
                        function=function,
                        args=None,
                        kwargs=None,
                        job_retry=self.job_retry,
                        enqueue_ms=enqueue_ms,
                        queue=task.queue,
                        job_id=next_job_id,
                    ).json(),
                )
                p.xadd(task.queue, {"job_id": next_job_id})
                task.set_next()
        execute and await p.execute()

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
            if job:
                job_def = JobDef.parse_raw(job.encode())
                p.xadd(job_def.queue, {"job_id": job_def.job_id})
        p.zrem(delay_queue, *jobs_id)
        await p.execute()
