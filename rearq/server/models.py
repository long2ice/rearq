from tortoise import Model, fields

from rearq import job


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
    status = fields.CharEnumField(job.JobStatus)

    class Meta:
        ordering = ["-id"]


class JobResult(Model):
    job: fields.ForeignKeyRelation[Job] = fields.ForeignKeyField("models.Job")
    worker = fields.CharField(max_length=200)
    success = fields.BooleanField(default=False)
    msg_id = fields.CharField(max_length=200, unique=True)
    result = fields.TextField(null=True)
    start_time = fields.DatetimeField(null=True)
    finish_time = fields.DatetimeField(null=True)

    class Meta:
        ordering = ["-id"]
