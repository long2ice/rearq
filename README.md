# Rearq

![pypi](https://img.shields.io/pypi/v/rearq.svg?style=flat)

## Introduction

Rearq is a distributed task queue with asyncio and redis, which rewrite from [arq](https://github.com/samuelcolvin/arq) and make improvement.

## Install

Just install from pypi:

```shell
> pip install rearq
```

or install latest code from github:

```shell
> pip install -e git+https://github.com/long2ice/rearq.git
```

## Quick Start

### Task Definition

```python
# main.py
rearq = ReArq()


@rearq.on_shutdown
async def on_shutdown():
    print("shutdown")


@rearq.on_startup
async def on_startup():
    print("startup")


@rearq.task(queue = "myqueue")
async def add(self, a, b):
    return a + b

@rearq.task(cron="*/5 * * * * * *") # run task per 5 seconds
async def timer(self):
    return "timer"
```

### Run rearq worker

```shell
> rearq worker main:rearq -q myqueue
```

```log
2020-06-04 15:37:02 - rearq.worker:92 - INFO - Start worker success with queue: myqueue
2020-06-04 15:37:02 - rearq.worker:84 - INFO - redis_version=6.0.1 mem_usage=1.47M clients_connected=25 db_keys=5
```

### Run rearq timing worker

If you have timeing task, run another command also:

```shell
> rearq worker -t main:rearq
```

```log
2020-06-04 15:37:44 - rearq.worker:346 - INFO - Start timer worker success with queue: myqueue
2020-06-04 15:37:44 - rearq.worker:84 - INFO - redis_version=6.0.1 mem_usage=1.47M clients_connected=25 db_keys=5
```

### Integration in FastAPI

```python
app = FastAPI()

@app.on_event("startup")
async def startup() -> None:
    await rearq.init()

@app.on_event("shutdown")
async def shutdown() -> None:
    await rearq.close()

# then run task in view
@app.get("/test")
async def test():
    job = await add.delay(args=(1,2))
    return job.info()
```

## Why not arq

Thanks great work of `arq`, but that project is not so active now and lack of maintenance. On the other hand, I don't like some solution of `arq` and its api, so I open this project and aims to work better.

## What's the differences

### Api

Rearq provide more friendly api to add register and add task, inspired by `celery`.

Rearq:

```python
rearq = Rearq()

@rearq.task()
async def add(self, a, b):
    return a + b

job = await add.delay(args=(1, 2))
print(job)
```

Arq:

```python
class WorkerSettings:
    functions = [ add ]
    redis_settings = RedisSettings(**settings.ARQ)

async def add(ctx, a,b):
    return a + b

await arq.enqueue_job('add', 1, 2)
```

### Queue implementation

Arq use redis `zset` to make delay queue and timing queue, and Rearq use `zset` and `stream` with `ack`.

## Documentation

See documentation in [https://rearq.long2ice.cn](https://rearq.long2ice.cn).

## ThanksTo

- [arq](https://github.com/samuelcolvin/arq), Fast job queuing and RPC in python with asyncio and redis.

## License

This project is licensed under the [MIT](https://github.com/long2ice/rearq/blob/master/LICENSE) License.
