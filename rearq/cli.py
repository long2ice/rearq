import asyncio
import importlib
import sys
from functools import wraps
from typing import List

import click
import uvicorn
from click import BadArgumentUsage, Context

from rearq import ReArq
from rearq.server.app import app
from rearq.version import VERSION
from rearq.worker import TimerWorker, Worker


def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        try:
            return loop.run_until_complete(f(*args, **kwargs))
        except asyncio.CancelledError:
            pass

    return wrapper


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(VERSION, "-v", "--version")
@click.option("--verbose", default=False, is_flag=True, help="Enable verbose output.")
@click.argument("rearq", required=True)
@click.pass_context
@coro
async def cli(ctx: Context, rearq: str, verbose):
    splits = rearq.split(":")
    rearq_path = splits[0]
    rearq = splits[1]
    try:
        module = importlib.import_module(rearq_path)
        r = getattr(module, rearq, None)  # type:ReArq
        await r.init()
        await r.startup()
        ctx.ensure_object(dict)
        ctx.obj["rearq"] = r
        ctx.obj["verbose"] = verbose

    except (ModuleNotFoundError, AttributeError) as e:
        raise BadArgumentUsage(ctx=ctx, message=f"Init rearq error, {e}.")


@cli.command(help="Start a worker.")
@click.option("-q", "--queue", required=False, multiple=True, help="Queue to consume.")
@click.option(
    "--group-name",
    required=False,
    help="Group name.",
)
@click.option("--consumer-name", required=False, help="Consumer name.")
@click.option("-t", "--with-timer", required=False, is_flag=True, help="Start with timer.")
@click.pass_context
@coro
async def worker(
    ctx: Context, queue: List[str], group_name: str, consumer_name: str, with_timer: bool
):
    rearq = ctx.obj["rearq"]
    w = Worker(rearq, queues=queue, group_name=group_name, consumer_name=consumer_name)
    if with_timer:
        t = TimerWorker(rearq)
        await asyncio.gather(w.run(), t.run())
    else:
        await w.run()


@cli.command(help="Start a timer.")
@click.pass_context
@coro
async def timer(ctx: Context):
    rearq = ctx.obj["rearq"]
    w = TimerWorker(rearq)
    await w.run()


@cli.command(
    help="Start rest api server.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.pass_context
def server(ctx: Context):
    rearq = ctx.obj["rearq"]
    app.set_rearq(rearq)

    verbose = ctx.obj["verbose"]

    @app.on_event("startup")
    async def startup():
        await rearq.init()

    @app.on_event("shutdown")
    async def shutdown():
        await rearq.close()

    kwargs = {
        ctx.args[i][2:].replace("-", "_"): ctx.args[i + 1] for i in range(0, len(ctx.args), 2)
    }
    if "port" in kwargs:
        kwargs["port"] = int(kwargs["port"])
    uvicorn.run("rearq.server.app:app", debug=verbose, **kwargs)


def main():
    sys.path.insert(0, ".")
    cli()


if __name__ == "__main__":
    main()
