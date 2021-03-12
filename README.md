# Rearq

![pypi](https://img.shields.io/pypi/v/rearq.svg?style=flat)

## Introduction

Rearq is a distributed task queue with asyncio and redis, which rewrite from [arq](https://github.com/samuelcolvin/arq)
and make improvement.

## Install

Just install from pypi:

```shell
> pip install rearq
```

## Quick Start

### Task Definition

```python
# main.py
rearq = ReArq()


@rearq.on_shutdown
async def on_shutdown():
    # you can do some clean work here like close db and so on...
    print("shutdown")


@rearq.on_startup
async def on_startup():
    # you should do some initialization work here, such tortoise-orm init and so on...
    print("startup")


@rearq.task(queue="myqueue")
async def add(self, a, b):
    return a + b


@rearq.task(cron="*/5 * * * * * *")  # run task per 5 seconds
async def timer(self):
    return "timer"
```

### Run rearq worker

```shell
> rearq main:rearq worker -q myqueue
```

```log
2020-06-04 15:37:02 - rearq.worker:92 - INFO - Start worker success with queue: myqueue
2020-06-04 15:37:02 - rearq.worker:84 - INFO - redis_version=6.0.1 mem_usage=1.47M clients_connected=25 db_keys=5
```

### Run rearq timing worker

If you have timeing task, run another command also:

```shell
> rearq main:rearq worker -t
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
    job = await add.delay(args=(1, 2))
    return job.info()
```

## Rest API

There are several apis to control rearq.

### Start server

```shell
> rearq main:rearq server
Usage: rearq server [OPTIONS]

  Start rest api server.

Options:
  --host TEXT         Listen host.  [default: 0.0.0.0]
  -p, --port INTEGER  Listen port.  [default: 8080]
  -h, --help          Show this message and exit..
```

### API docs

After server run, you can visit [https://127.0.0.1:8080/docs](https://127.0.0.1:8080/docs) to get all apis.

### GET `/job`

Get job information.

### POST `/job`

Add a job for a task.

### DELETE `/job`

Cancel a delay task.

### GET `/job/result`

Get job result.

## Documentation

> Writing...

## ThanksTo

- [arq](https://github.com/samuelcolvin/arq), Fast job queuing and RPC in python with asyncio and redis.

## License

This project is licensed under the [MIT](https://github.com/long2ice/rearq/blob/master/LICENSE) License.
