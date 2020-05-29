import asyncio
import datetime
import logging
from dataclasses import dataclass
from typing import Any, Optional, Tuple, Dict, Union
from uuid import uuid4

from aioredis import MultiExecError
from aioredis.commands import MultiExec
from crontab import CronTab

from . import job_key_prefix, result_key_prefix
from .job import JobDef, Job
from .utils import timestamp_ms_now, to_ms_timestamp

logger = logging.getLogger("arq.jobs")


@dataclass
class Task:
    function: str
    queue: str
    delay_queue: str
    rearq: Any
    _expires_extra_ms = 86_400_000

    async def delay(
            self,
            args: Tuple[Any, ...],
            kwargs: Dict[str, Any],
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
                job_retry=job_retry,
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
            self.rearq.get_function_map().get(self.function)
        )


class CronTask(Task):
    _cron_task = {}

    @classmethod
    def add_cron_task(cls, function: str, cron: CronTab = None):
        cls._cron_task[function] = cron
