import asyncio
import json
import signal
import socket
from functools import partial
from signal import Signals
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Set
from uuid import uuid4

import async_timeout
from aioredis.errors import BusyGroupError
from aioredlock import Aioredlock
from loguru import logger
from tortoise import timezone
from tortoise.expressions import F

from rearq import CronTask, UsageError, constants
from rearq.constants import DEFAULT_QUEUE, DELAY_QUEUE, QUEUE_KEY_PREFIX
from rearq.job import JobStatus
from rearq.server.models import Job, JobResult
from rearq.task import check_pending_msgs
from rearq.utils import args_to_string, ms_to_datetime, poll, timestamp_ms_now, to_ms_timestamp

if TYPE_CHECKING:
    from rearq import ReArq
Serializer = Callable[[Dict[str, Any]], bytes]
Deserializer = Callable[[bytes], Dict[str, Any]]


class Worker:
    _task_map = {}

    def __init__(
        self,
        rearq: "ReArq",
        queue: Optional[str] = None,
        group_name: Optional[str] = None,
        consumer_name: Optional[str] = None,
    ):
        self.group_name = group_name or socket.gethostname()
        self.consumer_name = consumer_name
        self.job_timeout = rearq.job_timeout
        self.max_jobs = rearq.max_jobs
        self.rearq = rearq
        self._redis = rearq.redis
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
        self.job_retry_after = rearq.job_retry_after
        self._add_signal_handler(signal.SIGINT, self.handle_sig)
        self._add_signal_handler(signal.SIGTERM, self.handle_sig)
        self.rearq.create_task(True, check_pending_msgs, queue, "* * * * *")

    def _add_signal_handler(self, signum: Signals, handler: Callable[[Signals], None]) -> None:
        self.loop.add_signal_handler(signum, partial(handler, signum))

    def handle_sig(self, signum: Signals) -> None:
        sig = Signals(signum)
        logger.info(
            "shutdown on %s ◆ %d jobs complete ◆ %d failed ◆ %d retries ◆ %d ongoing to cancel"
            % (
                sig.name,
                self.jobs_complete,
                self.jobs_failed,
                self.jobs_retried,
                len(self.tasks),
            )
        )
        for t in asyncio.all_tasks():
            t.cancel()

    async def log_redis_info(
        self,
    ) -> None:
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
        logger.add(f"logs/worker-{self.worker_name}.log", rotation="00:00")
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
                    jobs_id = list(map(lambda m: m[2].get("job_id"), msgs))
                    qs = Job.filter(job_id__in=jobs_id)
                    await qs.update(status=JobStatus.in_progress)
                    jobs = await qs
                    jobs_dict = {job.job_id: job for job in jobs}
                    for msg in msgs:
                        await self.sem.acquire()
                        queue, msg_id, job = msg
                        job_id = job.get("job_id")
                        job = jobs_dict.get(job_id)
                        if not job:
                            logger.warning(f"job {job_id} not found")
                            await self._xack(queue, msg_id)
                            self.sem.release()
                            continue
                        task = self.loop.create_task(self.run_job(queue, msg_id, job))
                        task.add_done_callback(self._task_done)
                        self.tasks.add(task)
                await asyncio.gather(*self.tasks)

    def _task_done(self, task):
        self.sem.release()
        self.tasks.remove(task)

    async def _xack(self, queue: str, msg_id: str):
        await self._redis.xack(queue, self.group_name, msg_id)

    async def run_job(self, queue: str, msg_id: str, job: Job):
        if job.expire_time and job.expire_time > timezone.now():
            logger.warning(f"job {job.job_id} is expired, ignore")
            job.status = JobStatus.expired
            await job.save(update_fields=["status"])
            return
        job_id = job.job_id
        job_result = JobResult(
            msg_id=msg_id, job=job, worker=self.worker_name, start_time=timezone.now()
        )
        task = self._task_map.get(job.task)
        if not task:
            logger.warning(f"job {job_id}, task {job.task} not found")
            job_result.result = "task not found"
            await job_result.save()
            return job_result
        ref = f"{job_id}:{job.task}"

        start_ms = timestamp_ms_now()
        logger.info(
            "%6.2fs → %s(%s)%s"
            % (
                (start_ms - to_ms_timestamp(job.enqueue_time)) / 1000,
                ref,
                args_to_string(job.args, job.kwargs),
                f" try={job.job_retries}" if job.job_retries > 1 else "",
            )
        )
        try:
            async with async_timeout.timeout(self.job_timeout):
                if task.bind:
                    result = await task.function(task, *(job.args or []), **(job.kwargs or {}))
                else:
                    result = await task.function(*(job.args or []), **(job.kwargs or {}))

            job_result.success = True
            job_result.finish_time = timezone.now()
            job.status = JobStatus.success
            logger.info("%6.2fs ← %s ● %s" % ((timestamp_ms_now() - start_ms) / 1000, ref, result))
            self.jobs_complete += 1

        except Exception as e:
            job_result.finish_time = timezone.now()
            self.jobs_failed += 1
            result = f"Run task error in NO.{job.job_retries} times, exc: {e}, retry after {self.job_retry_after} seconds"
            logger.error("%6.2fs ← %s ● %s" % ((timestamp_ms_now() - start_ms) / 1000, ref, result))

            if job.job_retries >= job.job_retry:
                t = (timestamp_ms_now() - to_ms_timestamp(job.enqueue_time)) / 1000
                logger.error("%6.2fs ! %s max retries %d exceeded" % (t, ref, job.job_retry))
                job.status = JobStatus.failed
            else:
                job.status = JobStatus.deferred
                job.job_retries = F("job_retries") + 1
                await self.rearq.zadd(to_ms_timestamp(self.job_retry_after), f"{queue}:{job_id}")
        finally:
            await self._xack(queue, msg_id)
            await job.save(update_fields=["status", "job_retries"])

        job_result.result = result
        await job_result.save()
        return job_result

    @property
    def worker_name(self):
        return f"{self.group_name}-{self.consumer_name}"

    async def _push_heartbeat(self, is_offline: bool = False):
        if is_offline:
            await self._redis.hdel(constants.WORKER_KEY, self.worker_name)
        else:
            value = {
                "queue": self.queue,
                "is_timer": isinstance(self, TimerWorker),
                "ms": timestamp_ms_now(),
            }
            await self._redis.hset(constants.WORKER_KEY, self.worker_name, value=json.dumps(value))

    async def _heartbeat(self):
        """
        keep alive in redis
        """
        while True:
            await self._push_heartbeat()
            await asyncio.sleep(constants.WORKER_HEARTBEAT_SECONDS)

    async def _pre_run(self):
        try:
            await self._redis.xgroup_create(
                self.queue, self.group_name, latest_id="$", mkstream=True
            )
        except BusyGroupError:
            pass
        if not self.consumer_name:
            async with await self._lock_manager.lock(constants.WORKER_KEY_LOCK):
                workers = await self._redis.hgetall(constants.WORKER_KEY)
                length = len(
                    list(
                        filter(
                            lambda item: not json.loads(item[1]).get("is_timer"), workers.items()
                        )
                    )
                )
                self.consumer_name = length
                await self._push_heartbeat()

    async def run(self):
        await self._pre_run()
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
    def __init__(self, rearq: "ReArq"):
        super().__init__(rearq)
        self.consumer_name = "timer"
        self.queue = DELAY_QUEUE

    async def _run_at_start(self):
        jobs = []
        p = self._redis.pipeline()
        for function, task in CronTask.get_cron_tasks().items():
            if task.run_at_start:
                logger.info(f"{function}() <- run at start")
                job_id = uuid4().hex
                jobs.append(
                    Job(
                        task=function,
                        job_retry=self.job_retry,
                        queue=task.queue,
                        job_id=job_id,
                        enqueue_time=timezone.now(),
                        job_retry_after=self.job_retry_after,
                        status=JobStatus.queued,
                    )
                )
                p.xadd(task.queue, {"job_id": job_id})
                self.jobs_complete += 1
        if jobs:
            await Job.bulk_create(jobs)
            await p.execute()

    async def _main(self) -> None:
        tasks = list(CronTask.get_cron_tasks().keys())
        tasks.remove(check_pending_msgs.__name__)
        logger.info("Start timer success")
        logger.add(f"logs/worker-{self.consumer_name}.log", rotation="00:00")
        logger.info(f"Registered timer tasks: {', '.join(tasks)}")

        await self.log_redis_info()
        await self.rearq.startup()
        await self._run_at_start()

        async for _ in poll():
            await self._poll_iteration()
            await self.run_cron()

    async def _pre_run(self):
        async with await self._lock_manager.lock(constants.WORKER_KEY_LOCK):
            workers_info = await self._redis.hgetall(constants.WORKER_KEY)
            for worker_name, value in workers_info.items():
                value = json.loads(value)
                time = ms_to_datetime(value["ms"])
                is_offline = (
                    timezone.now() - time
                ).seconds > constants.WORKER_HEARTBEAT_SECONDS + 10
                if value.get("is_timer") and not is_offline:
                    msg = f"There is a timer worker `{worker_name}` already, you can only start one timer worker"

                    logger.error(msg)
                    raise UsageError(msg)
            else:
                await self._push_heartbeat()

    async def run_cron(self):
        """
        run cron task
        :return:
        """
        redis = self._redis
        cron_tasks = CronTask.get_cron_tasks()
        p = redis.pipeline()
        execute = False
        jobs = []
        for function, task in cron_tasks.items():
            if timestamp_ms_now() >= task.next_run:
                execute = True
                job_id = uuid4().hex
                if task.function == check_pending_msgs:
                    asyncio.ensure_future(
                        check_pending_msgs(task, task.queue, self.group_name, self.job_timeout)
                    )
                else:
                    logger.info(f"{task.function.__name__}()")
                    jobs.append(
                        Job(
                            task=function,
                            job_retry=self.job_retry,
                            queue=task.queue,
                            job_id=job_id,
                            enqueue_time=timezone.now(),
                            job_retry_after=self.job_retry_after,
                            status=JobStatus.queued,
                        )
                    )
                    p.xadd(task.queue, {"job_id": job_id})
                    self.jobs_complete += 1
                task.set_next()
        if jobs:
            await Job.bulk_create(jobs)
        if execute:
            await p.execute()

    async def _poll_iteration(self):
        """
        get delay task and put to queue
        :return:
        """
        redis = self._redis
        now = timestamp_ms_now()
        p = redis.pipeline()
        for queue in self.rearq.delay_queues:
            p.zrangebyscore(queue, offset=0, count=self.queue_read_limit, max=now)
        jobs_id_list = await p.execute()
        p = redis.pipeline()
        execute = False
        for jobs_id_info in jobs_id_list:
            for job_id_info in jobs_id_info:
                execute = True
                separate = job_id_info.rindex(":")
                queue, job_id = job_id_info[:separate], job_id_info[separate + 1 :]  # noqa:
                p.xadd(queue, {"job_id": job_id})
                queue = self.rearq.get_delay_queue(job_id_info)
                p.zrem(queue, job_id_info)
        execute and await p.execute()
