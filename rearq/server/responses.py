from typing import List

from pydantic import BaseModel
from tortoise.contrib.pydantic import pydantic_model_creator, pydantic_queryset_creator

from rearq.server.models import Job, JobResult


class JobListOut(BaseModel):
    rows: pydantic_queryset_creator(Job)
    total: int


JobOut = pydantic_model_creator(Job)


class JobResultOut(pydantic_model_creator(JobResult)):
    job: JobOut


class JobResultListOut(BaseModel):
    rows: List[JobResultOut]
    total: int
