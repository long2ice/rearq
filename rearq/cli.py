import importlib
import logging
import logging.config
import sys

import asyncclick as click
from asyncclick import BadOptionUsage, Context

from rearq.log import init_logging
from rearq.version import VERSION
from rearq.worker import TimerWorker, Worker


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(VERSION, "-V", "--version")
@click.option("--rearq", required=True, help="ReArq instance, like main:rearq.")
@click.option("-v", "--verbose", default=False, is_flag=True, help="Enable verbose output.")
@click.pass_context
async def cli(ctx: Context, rearq, verbose):
    init_logging(verbose)
    ctx.ensure_object(dict)
    splits = rearq.split(":")
    rearq_path = splits[0]
    rearq = splits[1]
    try:
        module = importlib.import_module(rearq_path)
        rearq = getattr(module, rearq, None)
        await rearq.init()
        ctx.obj["rearq"] = rearq
    except (ModuleNotFoundError, AttributeError):
        raise BadOptionUsage(ctx=ctx, option_name="--rearq", message=f"No {rearq} found.")


@cli.command(help="Start rearq worker.")
@click.option(
    "-q", "--queues", required=False, help="Queues to consume, multiple are separated by commas."
)
@click.option("-t", "--timer", default=False, is_flag=True, help="Start a timer worker.")
@click.pass_context
async def worker(ctx: Context, queues, timer):
    rearq = ctx.obj["rearq"]
    queues = queues.split(",") if queues else None
    if timer:
        w = TimerWorker(rearq, queues=queues,)
    else:
        w = Worker(rearq, queues=queues)
    await w.async_run()


def main():
    sys.path.insert(0, ".")
    cli(_anyio_backend="asyncio")
