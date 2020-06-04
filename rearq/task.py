import asyncio
import datetime
import logging
from typing import Any, Dict, Optional, Tuple, Union
from uuid import uuid4

from aioredis import MultiExecError
from aioredis.commands import MultiExec
from crontab import CronTab

from . import delay_queue, job_key_prefix, result_key_prefix
from .job import Job, JobDef
from .utils import timestamp_ms_now, to_ms_timestamp

logger = logging.getLogger("arq.jobs")


class Task:
    expires_extra_ms = 86_400_000

    def __init__(self, function: str, queue: str, rearq, job_retry: int):

        self.job_retry = job_retry
        self.queue = queue
        self.rearq = rearq
        self.function = function

    async def delay(
        self,
        args: Optional[Tuple[Any, ...]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        job_id: str = uuid4().hex,
        countdown: Union[float, datetime.timedelta] = 0,
        eta: Optional[datetime.datetime] = None,
        expires: Optional[Union[float, datetime.datetime]] = None,
        job_retry: int = 0,
    ):
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
                job_retry=job_retry or self.job_retry,
                enqueue_ms=enqueue_ms,
                queue=self.queue,
                job_id=job_id,
            ).json(),
        )

        if not eta and not countdown:
            tr.xadd(self.queue, {"job_id": job_id})
        else:
            tr.zadd(delay_queue, defer_ts, job_id)

        try:
            await tr.execute()
        except MultiExecError as e:
            logger.warning(f"MultiExecError: {e}")
            await asyncio.gather(*tr._results, return_exceptions=True)
            return None
        return Job(redis, job_id, self.queue,)


class CronTask(Task):
    _cron_tasks: Dict[str, "CronTask"] = {}
    next_run: int

    def __init__(self, function: str, queue: str, rearq, job_retry: int, cron: CronTab):
        super().__init__(function, queue, rearq, job_retry)
        self.cron = cron
        self.set_next()

    def set_next(self):
        self.next_run = to_ms_timestamp(self.cron.next(default_utc=True))

    @classmethod
    def add_cron_task(cls, function: str, cron_task: "CronTask"):
        cls._cron_tasks[function] = cron_task

    @classmethod
    def get_cron_tasks(cls):
        return cls._cron_tasks

    def __repr__(self) -> str:
        return f"<rearq CronTask {self.function}>"
