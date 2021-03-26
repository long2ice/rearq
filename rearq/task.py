import datetime
from typing import Any, Callable, Dict, Optional, Tuple, Union
from uuid import uuid4

from aioredis.commands import MultiExec
from crontab import CronTab
from loguru import logger

from rearq.constants import DELAY_QUEUE, JOB_KEY_PREFIX
from rearq.job import Job, JobDef
from rearq.server.models import Result
from rearq.utils import timestamp_ms_now, to_ms_timestamp


class Task:
    expires_extra_ms = 86_400_000
    job_def: JobDef

    def __init__(self, bind: bool, function: Callable, queue: str, rearq, job_retry: int):

        self.job_retry = job_retry
        self.queue = queue
        self.rearq = rearq
        self.function = function
        self.bind = bind

    async def delay(
        self,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        job_id: str = None,
        countdown: Union[float, datetime.timedelta] = 0,
        eta: Optional[datetime.datetime] = None,
        expires: Optional[Union[float, datetime.datetime]] = None,
        job_retry: int = 0,
    ):
        if not job_id:
            job_id = uuid4().hex
        if countdown:
            defer_ts = to_ms_timestamp(countdown)
        elif eta:
            defer_ts = to_ms_timestamp(eta)
        else:
            defer_ts = timestamp_ms_now()
        enqueue_ms = timestamp_ms_now()
        expires_ms = (
            to_ms_timestamp(expires) if expires else defer_ts - enqueue_ms + self.expires_extra_ms
        )
        job_key = JOB_KEY_PREFIX + job_id
        redis = self.rearq.get_redis
        job_exists = await redis.exists(job_key)
        job_result_exists = await Result.exists(job_id=job_id)
        if job_exists or job_result_exists:
            logger.warning(
                f"Job {job_id} exists, job_exists={job_exists}, job_result_exists={job_result_exists}"
            )
            return Job(redis, job_id, self.queue,)

        p = redis.pipeline()  # type:MultiExec
        p.psetex(
            job_key,
            expires_ms,
            JobDef(
                task=self.function.__name__,
                args=args,
                kwargs=kwargs,
                job_retry=job_retry or self.job_retry,
                enqueue_ms=enqueue_ms,
                queue=self.queue,
                job_id=job_id,
            ).json(),
        )

        if not eta and not countdown:
            p.xadd(self.queue, {"job_id": job_id})
        else:
            p.zadd(DELAY_QUEUE, defer_ts, job_id)

        await p.execute()

        return Job(redis, job_id, self.queue,)


async def check_pending_msgs(
    self: Task, queue: str, group_name: str, consumer_name: str, timeout: int
):
    """
    check pending messages
    :return:
    """
    redis = self.rearq.get_redis
    pending_msgs = await redis.xpending(self.queue, group_name, "-", "+", 10)
    p = redis.pipeline()
    execute = False
    for msg in pending_msgs:
        msg_id, _, idle_time, times = msg
        if int(idle_time / 1000) > timeout * 2:
            execute = True
            p.xclaim(queue, group_name, consumer_name, min_idle_time=1000, id=msg_id)
    if execute:
        return await p.execute()


class CronTask(Task):
    _cron_tasks: Dict[str, "CronTask"] = {}
    next_run: int

    def __init__(
        self, bind: bool, function: Callable, queue: str, rearq, job_retry: int, cron: str
    ):
        super().__init__(bind, function, queue, rearq, job_retry)
        self.crontab = CronTab(cron)
        self.cron = cron
        self.set_next()

    def set_next(self):
        self.next_run = to_ms_timestamp(self.crontab.next(default_utc=False))

    @classmethod
    def add_cron_task(cls, function: str, cron_task: "CronTask"):
        cls._cron_tasks[function] = cron_task

    @classmethod
    def get_cron_tasks(cls):
        return cls._cron_tasks

    def __repr__(self) -> str:
        return f"<rearq CronTask {self.function}>"
