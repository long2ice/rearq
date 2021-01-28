import datetime
from typing import List, Optional, Union, Any

from pydantic import BaseModel


class AddJobIn(BaseModel):
    task: str
    args: Optional[List[Any]]
    kwargs: Optional[dict]
    job_id: Optional[str]
    countdown: Optional[Union[float, datetime.timedelta]]
    eta: Optional[datetime.datetime]
    expires: Optional[Union[float, datetime.datetime]]
    job_retry: int = 0
