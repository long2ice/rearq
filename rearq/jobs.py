import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Optional, Tuple

from aioredis import ConnectionsPool, Redis

from .exceptions import SerializationError

logger = logging.getLogger("arq.jobs")

Serializer = Callable[[Dict[str, Any]], bytes]
Deserializer = Callable[[bytes], Dict[str, Any]]


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


@dataclass
class JobDef:
    function: str
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]
    job_try: int
    enqueue_time: datetime
    score: Optional[int]


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
            job_id: str,
            redis: Redis,
            queue_name,
            deserializer: Optional[Deserializer],
    ):
        self.job_id = job_id
        self.redis = redis
        self.queue_name = queue_name
        self.deserializer = deserializer

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
            v = await self._redis.get(job_key_prefix + self.job_id, encoding=None)
            if v:
                info = deserialize_job(v, deserializer=self._deserializer)
        if info:
            info.score = await self._redis.zscore(self._queue_name, self.job_id)
        return info

    async def result_info(self) -> Optional[JobResult]:
        """
        Information about the job result if available, does not wait for the result. Does not raise an exception
        even if the job raised one.
        """
        v = await self._redis.get(result_key_prefix + self.job_id, encoding=None)
        if v:
            return deserialize_result(v, deserializer=self._deserializer)
        else:
            return None

    async def status(self) -> JobStatus:
        """
        Status of the job.
        """
        if await self._redis.exists(result_key_prefix + self.job_id):
            return JobStatus.complete
        elif await self._redis.exists(in_progress_key_prefix + self.job_id):
            return JobStatus.in_progress
        else:
            score = await self._redis.zscore(self._queue_name, self.job_id)
            if not score:
                return JobStatus.not_found
            return JobStatus.deferred if score > timestamp_ms() else JobStatus.queued

    def __repr__(self) -> str:
        return f"<arq job {self.job_id}>"
