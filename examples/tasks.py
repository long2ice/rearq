from examples import rearq


@rearq.task(bind=False)
async def add(a, b):
    return a + b


@rearq.task(bind=False, cron="*/5 * * * * * *")
async def timer_add():
    return "timer"
