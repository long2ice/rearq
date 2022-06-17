# ReArq

[![image](https://img.shields.io/pypi/v/rearq.svg?style=flat)](https://pypi.python.org/pypi/rearq)
[![image](https://img.shields.io/github/license/long2ice/rearq)](https://github.com/long2ice/rearq)
[![image](https://github.com/long2ice/rearq/workflows/pypi/badge.svg)](https://github.com/long2ice/rearq/actions?query=workflow:pypi)
[![image](https://github.com/long2ice/rearq/workflows/ci/badge.svg)](https://github.com/long2ice/rearq/actions?query=workflow:ci)

## Introduction

ReArq is a distributed task queue with asyncio and redis, which rewrite from [arq](https://github.com/samuelcolvin/arq)
to make improvement and include web interface.

You can try [Demo Online](https://demo-rearq.long2ice.io) here.

## Screenshots

![dashboard](./images/dashboard.png)
![worker](./images/worker.png)
![task](./images/task.png)
![job](./images/job.png)
![result](./images/result.png)

## Requirements

- Redis >= 5.0
- [TortoiseORM](https://github.com/tortoise/tortoise-orm)

## Install

Use MySQL backend:

```shell
> pip install rearq[mysql]
```

Use PostgreSQL backend:

```shell
> pip install rearq[postgres]
```

## Quick Start

### Task Definition

```python
# main.py
from rearq import ReArq

rearq = ReArq(db_url='mysql://root:123456@127.0.0.1:3306/rearq')


@rearq.on_shutdown
async def on_shutdown():
    # you can do some clean work here like close db and so on...
    print("shutdown")


@rearq.on_startup
async def on_startup():
    # you should do some initialization work here
    print("startup")


@rearq.task(queue="q1")
async def add(self, a, b):
    return a + b


@rearq.task(cron="*/5 * * * * * *")  # run task per 5 seconds
async def timer(self):
    return "timer"
```

### Run rearq worker

```shell
> rearq main:rearq worker -q q1 -q q2 # consume tasks from q1 and q2 as the same time
```

```log
2021-03-29 09:54:50.464 | INFO     | rearq.worker:_main:95 - Start worker success with queue: rearq:queue:default
2021-03-29 09:54:50.465 | INFO     | rearq.worker:_main:96 - Registered tasks: add, sleep, timer_add
2021-03-29 09:54:50.465 | INFO     | rearq.worker:log_redis_info:86 - redis_version=6.2.1 mem_usage=1.43M clients_connected=5 db_keys=6
```

### Run rearq timer

If you have timing task or delay task, you should run another command also:

```shell
> rearq main:rearq timer
```

```log
2021-03-29 09:54:43.878 | INFO     | rearq.worker:_main:275 - Start timer success
2021-03-29 09:54:43.887 | INFO     | rearq.worker:_main:277 - Registered timer tasks: timer_add
2021-03-29 09:54:43.894 | INFO     | rearq.worker:log_redis_info:86 - redis_version=6.2.1 mem_usage=1.25M clients_connected=2 db_keys=6
```

### Integration in FastAPI

```python
from fastapi import FastAPI

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
    job = await add.delay(args=(1, 2))
    # or
    job = await add.delay(kwargs={"a": 1, "b": 2})
    # or
    job = await add.delay(1, 2)
    # or
    job = await add.delay(a=1, b=2)
    result = await job.result(timeout=5)  # wait result for 5 seconds
    print(result.result)
    return result
```

## Start web interface

```shell
> rearq main:rearq server
Usage: rearq server [OPTIONS]

  Start rest api server.

Options:
  --host TEXT         Listen host.  [default: 0.0.0.0]
  -p, --port INTEGER  Listen port.  [default: 8000]
  -h, --help          Show this message and exit..
```

After server run, you can visit [https://127.0.0.1:8000/docs](https://127.0.0.1:8000/docs) to see all apis
and [https://127.0.0.1:8000](https://127.0.0.1:8000) to see web interface.

## ThanksTo

- [arq](https://github.com/samuelcolvin/arq), Fast job queuing and RPC in python with asyncio and redis.

## License

This project is licensed under the [Apache-2.0](./LICENSE) License.
