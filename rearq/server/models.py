import asyncio
from typing import Optional

from tortoise import Model, fields

from rearq.enums import JobStatus


class Job(Model):
    task = fields.CharField(max_length=200)
    args = fields.JSONField(null=True)
    kwargs = fields.JSONField(null=True)
    job_retry = fields.IntField()
    job_retries = fields.IntField(default=0)
    job_retry_after = fields.IntField()
    job_id = fields.CharField(max_length=200, unique=True)
    enqueue_time = fields.DatetimeField()
    expire_time = fields.DatetimeField(null=True)
    status = fields.CharEnumField(JobStatus)

    class Meta:
        ordering = ["-id"]
        table = "rearq_job"

    async def result(self, timeout: Optional[int] = None):
        while timeout > 0:
            result = await JobResult.filter(job_id=self.pk).order_by("-id").first()
            if result:
                return result
            await asyncio.sleep(1)
            timeout -= 1


class JobResult(Model):
    job: fields.ForeignKeyRelation[Job] = fields.ForeignKeyField("rearq.Job")
    worker = fields.CharField(max_length=200)
    success = fields.BooleanField(default=False)
    msg_id = fields.CharField(max_length=200, unique=True)
    result = fields.TextField(null=True)
    start_time = fields.DatetimeField(null=True)
    finish_time = fields.DatetimeField(null=True)

    class Meta:
        ordering = ["-id"]
        table = "rearq_job_results"
