import datetime
from typing import Any, List, Optional, Union

from pydantic import BaseModel

from rearq.enums import TaskStatus


class AddJobIn(BaseModel):
    task: str
    args: Optional[List[Any]] = None
    kwargs: Optional[dict] = None
    job_id: Optional[str] = None
    countdown: Optional[Union[float, datetime.timedelta]] = None
    eta: Optional[datetime.datetime] = None
    job_retry: int = 0


class UpdateJobIn(BaseModel):
    job_id: str
    args: Optional[List[Any]] = None
    kwargs: Optional[dict] = None
    expire_time: Optional[datetime.datetime] = None
    job_retry: Optional[int] = None
    job_retry_after: Optional[int] = None


class CancelJobIn(BaseModel):
    job_id: str


class UpdateTask(BaseModel):
    status: TaskStatus
    task_name: str
