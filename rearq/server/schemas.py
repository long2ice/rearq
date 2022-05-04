import datetime
from typing import Any, List, Optional, Union

from pydantic import BaseModel

from rearq.enums import TaskStatus


class AddJobIn(BaseModel):
    task: str
    args: Optional[List[Any]]
    kwargs: Optional[dict]
    job_id: Optional[str]
    countdown: Optional[Union[float, datetime.timedelta]]
    eta: Optional[datetime.datetime]
    job_retry: int = 0


class UpdateJobIn(BaseModel):
    job_id: str
    args: Optional[List[Any]]
    kwargs: Optional[dict]
    expire_time: Optional[datetime.datetime]
    job_retry: Optional[int]
    job_retry_after: Optional[int]


class UpdateTask(BaseModel):
    status: TaskStatus
