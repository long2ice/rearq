import asyncio
import json
import signal
import socket
from functools import partial
from signal import Signals
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from uuid import uuid4

import async_timeout
from aioredis import MultiExecError
from aioredis.errors import BusyGroupError
from aioredlock import Aioredlock
from loguru import logger
from pydantic import ValidationError
from tortoise import timezone

from rearq import CronTask, constants
from rearq.constants import (
    DEFAULT_QUEUE,
    DELAY_QUEUE,
    IN_PROGRESS_KEY_PREFIX,
    JOB_KEY_PREFIX,
    QUEUE_KEY_PREFIX,
    RETRY_KEY_PREFIX,
)
from rearq.job import JobDef, JobResult
from rearq.server.models import Result
from rearq.task import check_pending_msgs
from rearq.utils import args_to_string, ms_to_datetime, poll, timestamp_ms_now

Serializer = Callable[[Dict[str, Any]], bytes]
Deserializer = Callable[[bytes], Dict[str, Any]]

no_result = object()


class Worker:
    _task_map = {}

    def __init__(
        self,
        rearq,
        queue: Optional[str] = None,
        group_name: Optional[str] = None,
        consumer_name: Optional[str] = None,
    ):
        self.group_name = group_name or socket.gethostname()
        self.consumer_name = consumer_name
        self.job_timeout = rearq.job_timeout
        self.max_jobs = rearq.max_jobs
        self.rearq = rearq
        self._redis = rearq.get_redis
        self._lock_manager = Aioredlock([(rearq.redis_host, rearq.redis_port)])
        self.register_tasks = rearq.get_queue_tasks(queue)
        if queue:
            self.queue = QUEUE_KEY_PREFIX + queue
        else:
            self.queue = DEFAULT_QUEUE
        self.loop = asyncio.get_event_loop()
        self.sem = asyncio.BoundedSemaphore(self.max_jobs)
        self.queue_read_limit = max(self.max_jobs * 5, 100)
        self.tasks: Set[asyncio.Task[Any]] = set()
        self._task_map = rearq.task_map
        self.jobs_complete = 0
        self.jobs_retried = 0
        self.jobs_failed = 0
        self.job_retry = rearq.job_retry
        self.in_progress_timeout = self.job_timeout + 10
        self._add_signal_handler(signal.SIGINT, self.handle_sig)
        self._add_signal_handler(signal.SIGTERM, self.handle_sig)
        self.rearq.create_task(True, check_pending_msgs, queue, "* * * * *")

    def _add_signal_handler(self, signum: Signals, handler: Callable[[Signals], None]) -> None:
        self.loop.add_signal_handler(signum, partial(handler, signum))

    def handle_sig(self, signum: Signals) -> None:
        sig = Signals(signum)
        logger.info(
            "shutdown on %s ◆ %d jobs complete ◆ %d failed ◆ %d retries ◆ %d ongoing to cancel"
            % (sig.name, self.jobs_complete, self.jobs_failed, self.jobs_retried, len(self.tasks),)
        )
        for t in asyncio.all_tasks():
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
        logger.add(f"logs/worker-{self.consumer_name}.log", rotation="00:00")
        logger.info(f"Start worker success with queue: {self.queue}")
        logger.info(f"Registered tasks: {', '.join(self.register_tasks)}")
        await self.log_redis_info()
        await self.rearq.startup()
        with await self._redis as redis:
            while True:
                msgs = await redis.xread_group(
                    self.group_name,
                    self.consumer_name,
                    [self.queue],
                    count=self.queue_read_limit,
                    latest_ids=[">"],
                )
                async with self.sem:
                    for msg in msgs:
                        await self.sem.acquire()
                        queue, msg_id, job = msg
                        job_id = job.get("job_id")
                        in_progress_key = IN_PROGRESS_KEY_PREFIX + job_id
                        ongoing_exists = await redis.exists(in_progress_key)
                        if ongoing_exists:
                            logger.debug("job %s already running elsewhere" % job_id)
                            await self._xack(queue, msg_id)
                            continue
                        tr = redis.multi_exec()
                        tr.setex(in_progress_key, self.in_progress_timeout, b"1")
                        try:
                            await tr.execute()
                        except MultiExecError:
                            self.sem.release()
                            logger.debug(
                                "multi-exec error, job %s already started elsewhere" % job_id
                            )
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
        redis = self._redis
        p = redis.pipeline()
        p.get(JOB_KEY_PREFIX + job_id)
        p.incr(RETRY_KEY_PREFIX + job_id)
        p.expire(RETRY_KEY_PREFIX + job_id, 88400)
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
            logger.warning("%6.2fs ! %s max retries %d exceeded" % (t, ref, job_def.job_retry))
            await self._xack(queue, msg_id)
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
        if not self.is_check_task(job_def.function):
            logger.info(
                "%6.2fs → %s(%s)%s"
                % (
                    (start_ms - job_def.enqueue_ms) / 1000,
                    ref,
                    args_to_string(job_def.args, job_def.kwargs),
                    f" try={job_retry}" if job_retry > 1 else "",
                )
            )
        await self._redis.hset(constants.TASK_LAST_TIME, job_def.function, start_ms)
        try:
            task.job_def = job_def
            async with async_timeout.timeout(self.job_timeout):
                if self.is_check_task(job_def.function):
                    result = await task.function(
                        task, self.queue, self.group_name, self.consumer_name, self.job_timeout
                    )
                else:
                    if task.bind:
                        result = await task.function(
                            task, *(job_def.args or []), **(job_def.kwargs or {})
                        )
                    else:
                        result = await task.function(
                            *(job_def.args or []), **(job_def.kwargs or {})
                        )

        except Exception as e:
            success = False
            finished_ms = 0
            self.jobs_failed += 1
            logger.error(f"Run task error, function: {job_def.function}, e: {e}")
        else:
            success = True
            finished_ms = timestamp_ms_now()
            if not self.is_check_task(job_def.function):
                logger.info("%6.2fs ← %s ● %s" % ((finished_ms - start_ms) / 1000, ref, result))
            self.jobs_complete += 1
            await self._xack(queue, msg_id)
        if result is not no_result:
            job_result = JobResult(
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

            await asyncio.shield(self.finish_job(job_result))

    async def finish_job(self, job_result: Optional[JobResult],) -> None:
        job_id = job_result.job_id
        redis = self._redis
        delete_keys = [IN_PROGRESS_KEY_PREFIX + job_id]
        if job_result.success:
            delete_keys += [RETRY_KEY_PREFIX + job_id, JOB_KEY_PREFIX + job_id]
            await redis.delete(*delete_keys)
            await self.create_job_result(job_result)

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
        redis = self._redis
        await redis.delete(
            RETRY_KEY_PREFIX + job_id, IN_PROGRESS_KEY_PREFIX + job_id, JOB_KEY_PREFIX + job_id
        )
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
        await self.create_job_result(job_result)

    @classmethod
    def is_check_task(cls, function: str):
        return function == check_pending_msgs.__name__

    async def create_job_result(self, job_result: JobResult):
        if not self.is_check_task(job_result.function):
            await Result.create(
                task=job_result.function,
                worker=self.consumer_name,
                args=job_result.args,
                kwargs=job_result.kwargs,
                job_retry=job_result.job_retry,
                enqueue_time=ms_to_datetime(job_result.enqueue_ms),
                job_id=job_result.job_id,
                success=job_result.success,
                result=job_result.result,
                start_time=ms_to_datetime(job_result.start_ms),
                finish_time=ms_to_datetime(job_result.finish_ms),
            )

    async def _push_heartbeat(self, is_offline: bool = False):
        value = {
            "queue": self.queue,
            "jobs_complete": self.jobs_complete,
            "jobs_running": len(self.tasks),
            "jobs_failed": self.jobs_failed,
            "jobs_retried": self.jobs_retried,
            "is_timer": isinstance(self, TimerWorker),
        }
        if is_offline:
            ms = 0
        else:
            ms = timestamp_ms_now()
        value.update({"ms": ms})
        await self._redis.hset(constants.WORKER_KEY, self.consumer_name, value=json.dumps(value))

    async def _heartbeat(self):
        """
        keep alive in redis
        """
        while True:
            await self._push_heartbeat()
            await asyncio.sleep(constants.WORKER_HEARTBEAT_SECONDS)

    async def run(self):
        try:
            await self._redis.xgroup_create(
                self.queue, self.group_name, latest_id="$", mkstream=True
            )
        except BusyGroupError:
            pass
        async with await self._lock_manager.lock(constants.WORKER_KEY_LOCK):
            workers = await self._redis.hgetall(constants.WORKER_KEY)
            length = len(
                list(filter(lambda item: not json.loads(item[1]).get("is_timer"), workers.items()))
            )
            self.consumer_name = f"{self.group_name}-{length}"
            await self._push_heartbeat()
        try:
            await asyncio.gather(self._main(), self._heartbeat())
        except asyncio.CancelledError:
            pass
        finally:
            await self._push_heartbeat(True)
            await self.close()

    async def close(self):
        await self.rearq.shutdown()
        await self.rearq.close()


class TimerWorker(Worker):
    async def _main(self) -> None:
        tasks = list(CronTask.get_cron_tasks().keys())
        tasks.remove(check_pending_msgs.__name__)
        self.consumer_name = "timer"
        logger.add(f"logs/worker-{self.consumer_name}.log", rotation="00:00")
        logger.info(f"Registered timer tasks: {', '.join(tasks)}")
        logger.info(f"Start timer worker success with queue: {self.queue}")

        await self.log_redis_info()
        await self.rearq.startup()

        async for _ in poll():
            await self._poll_iteration()
            await self.run_cron()

    async def run(self):
        workers_info = await self._redis.hgetall(constants.WORKER_KEY)
        for worker_name, value in workers_info.items():
            value = json.loads(value)
            time = ms_to_datetime(value["ms"])
            is_offline = (timezone.now() - time).seconds > constants.WORKER_HEARTBEAT_SECONDS + 10
            if value.get("is_timer") and not is_offline:
                logger.error(
                    f"There is a timer worker `{worker_name}` already, you could only start one timer worker"
                )
                break
        else:
            await super(TimerWorker, self).run()

    async def run_cron(self):
        """
        run cron task
        :return:
        """
        redis = self._redis
        cron_tasks = CronTask.get_cron_tasks()
        p = redis.pipeline()
        execute = False
        for function, task in cron_tasks.items():
            if timestamp_ms_now() >= task.next_run:
                execute = True
                if not self.is_check_task(task.function.__name__):
                    logger.info(f"{task.function.__name__}()")
                next_job_id = uuid4().hex
                job_key = JOB_KEY_PREFIX + next_job_id
                enqueue_ms = timestamp_ms_now()
                p.psetex(
                    job_key,
                    task.expires_extra_ms,
                    JobDef(
                        task=function,
                        args=None,
                        kwargs=None,
                        job_retry=self.job_retry,
                        enqueue_ms=enqueue_ms,
                        queue=task.queue,
                        job_id=next_job_id,
                    ).json(),
                )
                p.xadd(task.queue, {"job_id": next_job_id})
                p.hset(constants.TASK_LAST_TIME, function, enqueue_ms)
                self.jobs_complete += 1
                task.set_next()
        execute and await p.execute()

    async def _poll_iteration(self):
        redis = self._redis
        now = timestamp_ms_now()
        jobs_id = await redis.zrangebyscore(
            DELAY_QUEUE, offset=0, count=self.queue_read_limit, max=now
        )
        if not jobs_id:
            return
        else:
            jobs_key = list(map(lambda x: JOB_KEY_PREFIX + x, jobs_id))
        jobs = await redis.mget(*jobs_key) or []  # type:List[str]
        p = redis.pipeline()
        for job in jobs:
            if job:
                job_def = JobDef.parse_raw(job.encode())
                p.xadd(job_def.queue, {"job_id": job_def.job_id})
        p.zrem(DELAY_QUEUE, *jobs_id)
        await p.execute()
