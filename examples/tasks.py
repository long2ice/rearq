from examples import rearq
from rearq import Task


@rearq.task()
async def add(self: Task, a, b):
    return a + b


@rearq.task(cron="*/5 * * * * * *")
async def timer_add(self: Task):
    return "timer"
