from tortoise import Model, fields


class Result(Model):
    task = fields.CharField(max_length=200)
    worker = fields.CharField(max_length=200)
    args = fields.JSONField(null=True)
    kwargs = fields.JSONField(null=True)
    job_retry = fields.IntField()
    enqueue_time = fields.DatetimeField()
    job_id = fields.CharField(max_length=200, unique=True)
    success = fields.BooleanField(default=False)
    result = fields.CharField(max_length=200, null=True)
    start_time = fields.DatetimeField(null=True)
    finish_time = fields.DatetimeField(null=True)
