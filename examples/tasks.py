from crontab import CronTab

from examples import rearq
from rearq.worker import Worker


@rearq.task()
async def add(worker: Worker, a, b):
    return a + b


@rearq.task(cron=CronTab("*/5 * * * * * *"))
async def timer_add(worker: Worker):
    return "timer"
