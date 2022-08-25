import asyncio
import json
import os
import socket
from math import inf
from signal import Signals
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple
from uuid import uuid4

import async_timeout
from loguru import logger
from redis.asyncio.lock import Lock
from redis.exceptions import LockError, ResponseError
from tortoise import timezone
from tortoise.expressions import F

from rearq import CronTask, UsageError, constants, signals
from rearq.constants import CHANNEL, DEFAULT_QUEUE, DELAY_QUEUE, QUEUE_KEY_PREFIX
from rearq.enums import ChannelType, JobStatus
from rearq.server.models import Job, JobResult
from rearq.task import check_keep_job, check_pending_msgs
from rearq.utils import args_to_string, ms_to_datetime, timestamp_ms_now, to_ms_timestamp

if TYPE_CHECKING:
    from rearq import ReArq
Serializer = Callable[[Dict[str, Any]], bytes]
Deserializer = Callable[[bytes], Dict[str, Any]]


class Worker:
    def __init__(
        self,
        rearq: "ReArq",
        queues: Optional[List[str]] = None,
        group_name: Optional[str] = None,
        consumer_name: Optional[str] = None,
    ):
        self.group_name = group_name or socket.gethostname()
        self.consumer_name = consumer_name
        self.job_timeout = rearq.job_timeout
        self.max_jobs = rearq.max_jobs
        self.rearq = rearq
        self._redis = rearq.redis
        self._lock = Lock(self._redis, name=constants.WORKER_KEY_LOCK)
        self.register_tasks = []
        self.queues = []
        if queues:
            for queue in queues:
                self.register_tasks.extend(rearq.get_queue_tasks(queue))
                self.queues.append(QUEUE_KEY_PREFIX + queue)
        else:
            self.queues.append(DEFAULT_QUEUE)
        self.loop = asyncio.get_event_loop()
        self.sem = asyncio.BoundedSemaphore(self.max_jobs)
        self.queue_read_limit = max(self.max_jobs * 5, 100)
        self._tasks: Set[asyncio.Task[Any]] = set()
        self._running_tasks_map: Dict[str, List[Tuple[str, asyncio.Task[Any]]]] = {}
        self._task_map = rearq.task_map
        self.jobs_complete = 0
        self.jobs_retried = 0
        self.jobs_failed = 0
        self.job_retry = rearq.job_retry
        self.job_retry_after = rearq.job_retry_after
        self._terminated = False
        signals.add_sig_handler(self._handle_sig)

    def _handle_sig(self, signum: Signals) -> None:
        self._terminated = True
        sig = Signals(signum)
        logger.info(
            f"shutdown worker {self.worker_name} on %s ◆ %d jobs complete ◆ %d failed ◆ %d retries ◆ %d ongoing to cancel"
            % (
                sig.name,
                self.jobs_complete,
                self.jobs_failed,
                self.jobs_retried,
                len(self._tasks),
            )
        )
        for tasks in self._running_tasks_map.values():
            for t in tasks:
                t[1].cancel()

    async def _log_redis_info(
        self,
    ) -> None:
        p = self._redis.pipeline()
        p.info()
        p.dbsize()
        info, key_count = await p.execute()
        logger.info(
            f'redis_version={info["redis_version"]} '
            f'mem_usage={info["used_memory_human"]} '
            f'clients_connected={info["connected_clients"]} '
            f"db_keys={key_count}"
        )

    async def _main(self) -> None:
        logger.add(f"{self.rearq.logs_dir}/worker-{self.worker_name}.log", rotation="00:00")
        logger.success(f"Start worker success with queue: {','.join(self.queues)}")
        logger.info(f"Registered tasks: {', '.join(self.register_tasks)}")
        await self._log_redis_info()
        asyncio.ensure_future(self._subscribe_channel())
        while not self._terminated:
            msgs = await self._redis.xreadgroup(
                self.group_name,
                self.consumer_name,
                streams={queue: ">" for queue in self.queues},
                count=self.queue_read_limit,
                block=10000,
            )
            if not msgs:
                continue
            jobs_id = []
            for msg in msgs:
                queue, msg_items = msg
                for msg_item in msg_items:
                    jobs_id.append(msg_item[1].get("job_id"))
            qs = Job.filter(job_id__in=jobs_id, status__in=[JobStatus.queued, JobStatus.deferred])
            jobs = await qs
            await qs.update(status=JobStatus.in_progress)
            jobs_dict = {job.job_id: job for job in jobs}
            for msg in msgs:
                queue, msg_items = msg
                for msg_item in msg_items:
                    msg_id, job = msg_item
                    job_id = job.get("job_id")
                    job = jobs_dict.get(job_id)
                    if not job:
                        logger.warning(f"job {job_id} not found")
                        await self._xack(queue, msg_id)
                        continue
                    task = asyncio.ensure_future(self._run_job(queue, msg_id, job))
                    task.add_done_callback(self._task_done)
                    self._tasks.add(task)

    def _task_done(self, task):
        self.sem.release()
        self._tasks.remove(task)

    async def _xack(self, queue: str, msg_id: str):
        await self._redis.xack(queue, self.group_name, msg_id)

    async def _run_job(self, queue: str, msg_id: str, job: Job):
        await self.sem.acquire()
        if job.expire_time and job.expire_time < timezone.now():
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
        elif await task.is_disabled():
            logger.warning(f"task {job.task} is disabled, ignore")
            return

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
        lock = None
        try:
            if task.run_with_lock:
                lock = Lock(
                    self._redis,
                    name=constants.WORKER_TASK_LOCK.format(task=task.name),
                    blocking=False,
                )
                if not await lock.acquire():
                    lock = None
                    raise LockError("Unable to acquire lock within the time specified")
            async with async_timeout.timeout(task.job_timeout):
                if task.bind:
                    real_task = asyncio.create_task(
                        task.function(task, *(job.args or []), **(job.kwargs or {}))
                    )
                else:
                    real_task = asyncio.create_task(
                        task.function(*(job.args or []), **(job.kwargs or {}))
                    )
                self._running_tasks_map.setdefault(task.name, []).append((job_id, real_task))
                real_task.add_done_callback(
                    lambda x: self._running_tasks_map[task.name].remove((job_id, x))
                )
                result = await real_task

            job_result.success = True
            job_result.finish_time = timezone.now()
            job.status = JobStatus.success
            logger.info("%6.2fs ← %s ● %s" % ((timestamp_ms_now() - start_ms) / 1000, ref, result))
            self.jobs_complete += 1

        except Exception as e:
            if isinstance(e, asyncio.CancelledError):
                e = "asyncio.CancelledError"
            else:
                if self.rearq.trace_exception:
                    logger.exception(e)
            job_result.finish_time = timezone.now()
            self.jobs_failed += 1

            if job.job_retries >= job.job_retry or self.job_retry == 0:
                result = f"Run job error, exc: {e}"
                logger.error(
                    "%6.2fs ← %s ● %s" % ((timestamp_ms_now() - start_ms) / 1000, ref, result)
                )
                t = (timestamp_ms_now() - to_ms_timestamp(job.enqueue_time)) / 1000
                logger.error("%6.2fs ! %s max retries %d exceeded" % (t, ref, job.job_retry))
                job.status = JobStatus.failed
            else:
                result = f"Run job error in NO.{job.job_retries} times, exc: {e}, retry after {self.job_retry_after} seconds"
                logger.error(
                    "%6.2fs ← %s ● %s" % ((timestamp_ms_now() - start_ms) / 1000, ref, result)
                )
                job.status = JobStatus.deferred
                job.job_retries = F("job_retries") + 1
                await self.rearq.zadd(to_ms_timestamp(self.job_retry_after), f"{queue}:{job_id}")
        finally:
            await self._xack(queue, msg_id)
            await job.save(update_fields=["status", "job_retries"])
            if lock:
                await lock.release()

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
            is_timer = isinstance(self, TimerWorker)
            p = self._redis.pipeline()
            for q in self.queues:
                value = {
                    "queue": q,
                    "is_timer": is_timer,
                    "ms": timestamp_ms_now(),
                    "group": self.group_name,
                    "consumer": self.consumer_name,
                }
                p.hset(constants.WORKER_KEY, self.worker_name, value=json.dumps(value))
            if is_timer:
                # To prevent the unpredictable shutdown
                p.expire(
                    constants.WORKER_KEY_TIMER_LOCK,
                    constants.WORKER_HEARTBEAT_SECONDS + 2,
                )
            await p.execute()

    async def _heartbeat(self):
        """
        keep alive in redis
        """
        while not self._terminated:
            await self._push_heartbeat()
            await asyncio.sleep(constants.WORKER_HEARTBEAT_SECONDS)

    async def _subscribe_channel(self):
        """
        Subscribe for task cancel and disable
        """
        channel = self._redis.pubsub()
        await channel.subscribe(CHANNEL)
        async for msg in channel.listen():
            msg_type = msg["type"]
            if msg_type != "message":
                continue
            data = json.loads(msg["data"])
            if data["type"] != ChannelType.cancel_task:
                continue
            task_name = data["task_name"]
            job_id = data["job_id"]
            for name, tasks in self._running_tasks_map.items():
                if name == task_name:
                    for task in tasks:
                        if not job_id or (job_id and job_id == task[0]):
                            task[1].cancel()

    async def _pre_run(self):
        try:
            for q in self.queues:
                await self._redis.xgroup_create(q, self.group_name, mkstream=True)
        except ResponseError as e:
            if "BUSYGROUP Consumer Group name already exists" == str(e):
                pass
            else:
                raise e
        if not self.consumer_name:
            async with self._lock:
                self.consumer_name = os.getpid()
                await self._push_heartbeat()

    async def run(self):
        try:
            await self._pre_run()
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
        self.sleep_until: Optional[float] = None
        self.sleep_task: Optional[asyncio.Task] = None
        self.rearq.create_task(check_pending_msgs, True, self.queue, "* * * * *")
        if rearq.keep_job_days:
            self.rearq.create_task(check_keep_job, True, self.queue, "0 4 * * *")

    def _handle_sig(self, signum: Signals) -> None:
        self._terminated = True
        self.sleep_task.cancel()
        sig = Signals(signum)
        logger.info(f"shutdown timer {self.worker_name} on %s" % (sig.name,))

    async def run(self):
        logger.info(
            "Trying to acquire timer lock because only one timer can be startup at same time..."
        )
        async with Lock(
            self._redis,
            name=constants.WORKER_KEY_TIMER_LOCK,
        ):
            await super(TimerWorker, self).run()

    async def _run_at_start(self):
        jobs = []
        p = self._redis.pipeline()
        for task_name, task in self.rearq.task_map.items():
            if task.run_at_start and await task.is_enabled():
                args = None
                kwargs = None
                if isinstance(task.run_at_start, (tuple, list)):
                    args = task.run_at_start
                elif isinstance(task.run_at_start, dict):
                    kwargs = task.run_at_start
                logger.info(f"run at start → {task_name}({args_to_string(args, kwargs)})")
                job_id = uuid4().hex
                jobs.append(
                    Job(
                        task=task_name,
                        job_retry=self.job_retry,
                        queue=task.queue,
                        job_id=job_id,
                        enqueue_time=timezone.now(),
                        job_retry_after=self.job_retry_after,
                        status=JobStatus.queued,
                        args=args,
                        kwargs=kwargs,
                    )
                )
                p.xadd(task.queue, {"job_id": job_id})
                self.jobs_complete += 1
        if jobs:
            await Job.bulk_create(jobs)
            await p.execute()

    async def _sleep(self):
        next_runs = []
        for task in CronTask.get_cron_tasks().values():
            if await task.is_enabled():
                next_runs.append(task.next_run)
        if next_runs:
            min_next_run = min(next_runs)
        else:
            min_next_run = -1
        redis = self._redis
        p = redis.pipeline()
        for queue in self.rearq.delay_queues:
            p.zrangebyscore(queue, start=0, num=1, withscores=True, min=-1, max=inf)
        jobs_id_list = await p.execute()
        jobs_id_list = list(filter(lambda x: True if x else False, jobs_id_list))
        if jobs_id_list:
            _, min_delay = jobs_id_list[0][0]
        else:
            min_delay = min_next_run
        sleep_until = min(min_next_run, min_delay)
        if sleep_until == -1:
            sleep_seconds = 60
            self.sleep_task = asyncio.ensure_future(asyncio.sleep(sleep_seconds))
            self.sleep_until = timestamp_ms_now() + sleep_seconds * 1000
        else:
            sleep_ms = sleep_until - timestamp_ms_now()
            if sleep_ms > 0:
                self.sleep_until = sleep_until
                self.sleep_task = asyncio.ensure_future(asyncio.sleep(sleep_ms / 1000))
        if self.sleep_task:
            try:
                await self.sleep_task
            except asyncio.CancelledError:
                pass

    async def _subscribe_channel(self):
        """
        Subscribe for delay queue changed
        """
        channel = self._redis.pubsub()
        await channel.subscribe(CHANNEL)
        async for msg in channel.listen():
            msg_type = msg["type"]
            if msg_type != "message":
                continue
            data = json.loads(msg["data"])
            if data["type"] != ChannelType.delay_changed:
                continue
            task_ms = float(data["ms"])
            if self.sleep_until and task_ms < self.sleep_until:  # type:ignore
                self.sleep_task.cancel()  # type:ignore
                self.sleep_task = None
                self.sleep_until = None

    async def _main(self) -> None:
        tasks = list(CronTask.get_cron_tasks().keys())
        tasks.remove(check_pending_msgs.__name__)
        if self.rearq.keep_job_days:
            tasks.remove(check_keep_job.__name__)
        logger.success("Start timer success")
        logger.add(f"{self.rearq.logs_dir}/worker-{self.worker_name}.log", rotation="00:00")
        logger.info(f"Registered timer tasks: {', '.join(tasks)}")

        await self._log_redis_info()
        await self._run_at_start()
        asyncio.ensure_future(self._subscribe_channel())
        while not self._terminated:
            await self._sleep()
            await self._run_delay()
            await self._run_cron()

    async def _pre_run(self):
        async with self._lock:
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

    async def _run_cron(self):
        """
        run cron task
        :return:
        """
        redis = self._redis
        cron_tasks = CronTask.get_cron_tasks()
        p = redis.pipeline()
        jobs = []
        for function, task in cron_tasks.items():
            if await task.is_disabled():
                continue
            if timestamp_ms_now() >= task.next_run:
                job_id = uuid4().hex
                if task.function == check_pending_msgs:
                    asyncio.ensure_future(check_pending_msgs(task, self.job_timeout))
                elif task.function == check_keep_job:
                    asyncio.ensure_future(check_keep_job(task))
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
            await p.execute()

    async def _run_delay(self):
        """
        get delay task and put to queue
        :return:
        """
        redis = self._redis
        now = timestamp_ms_now()
        p = redis.pipeline()
        for queue in self.rearq.delay_queues:
            p.zrangebyscore(queue, start=0, num=self.queue_read_limit, max=now, min=-1)
        jobs_id_list = await p.execute()
        p = redis.pipeline()
        for jobs_id_info in jobs_id_list:
            for job_id_info in jobs_id_info:
                separate = job_id_info.rindex(":")
                queue, job_id = (
                    job_id_info[:separate],
                    job_id_info[separate + 1 :],
                )  # noqa:
                p.xadd(queue, {"job_id": job_id})
                queue = self.rearq.get_delay_queue(job_id_info)
                p.zrem(queue, job_id_info)
        if jobs_id_list:
            await p.execute()
