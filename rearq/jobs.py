import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel

from . import job_key_prefix, result_key_prefix, in_progress_key_prefix
from .exceptions import SerializationError
from .utils import poll, timestamp_ms_now

logger = logging.getLogger("arq.jobs")


class JobStatus(str, Enum):
    """
    Enum of job statuses.
    """

    #: job is in the queue, time it should be run not yet reached
    deferred = "deferred"
    #: job is in the queue, time it should run has been reached
    queued = "queued"
    #: job is in progress
    in_progress = "in_progress"
    #: job is complete, result is available
    complete = "complete"
    #: job not found in any way
    not_found = "not_found"


class JobDef(BaseModel):
    function: str
    args: Any
    kwargs: Any
    retry_times: int
    enqueue_ms: int
    queue: str


@dataclass
class JobResult(JobDef):
    success: bool
    result: Any
    start_time: datetime
    finish_time: datetime
    job_id: Optional[str] = None


class Job:
    """
    Holds data a reference to a job.
    """

    def __init__(
            self,
            rearq,
            job_id: str,
            queue_name,
    ):
        self.queue_name = queue_name
        self.job_id = job_id
        self.redis = rearq.get_redis()

    async def result(self, timeout: Optional[float] = None, *, pole_delay: float = 0.5) -> Any:
        """
        Get the result of the job, including waiting if it's not yet available. If the job raised an exception,
        it will be raised here.

        :param timeout: maximum time to wait for the job result before raising ``TimeoutError``, will wait forever
        :param pole_delay: how often to poll redis for the job result
        """
        async for delay in poll(pole_delay):
            info = await self.result_info()
            if info:
                result = info.result
                if info.success:
                    return result
                elif isinstance(result, Exception):
                    raise result
                else:
                    raise SerializationError(result)
            if timeout is not None and delay > timeout:
                raise asyncio.TimeoutError()

    async def info(self) -> Optional[JobDef]:
        """
        All information on a job, including its result if it's available, does not wait for the result.
        """
        info: Optional[JobDef] = await self.result_info()
        if not info:
            v = await self.redis.get(job_key_prefix + self.job_id, encoding=None)
            if v:
                info = JobDef.parse_raw(v)
        if info:
            info.score = await self.redis.zscore(self.queue_name, self.job_id)
        return info

    async def result_info(self) -> Optional[JobResult]:
        """
        Information about the job result if available, does not wait for the result. Does not raise an exception
        even if the job raised one.
        """
        v = await self.redis.get(result_key_prefix + self.job_id, encoding=None)
        if v:
            return JobResult.parse_raw(v)
        else:
            return None

    async def status(self) -> JobStatus:
        """
        Status of the job.
        """
        if await self.redis.exists(result_key_prefix + self.job_id):
            return JobStatus.complete
        elif await self.redis.exists(in_progress_key_prefix + self.job_id):
            return JobStatus.in_progress
        else:
            score = await self.redis.zscore(self.queue_name, self.job_id)
            if not score:
                return JobStatus.not_found
            return JobStatus.deferred if score > timestamp_ms_now() else JobStatus.queued

    def __repr__(self) -> str:
        return f"<rearq job {self.job_id}>"
