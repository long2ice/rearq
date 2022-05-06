import asyncio

from examples.tasks import add, rearq


async def main():
    await rearq.startup()
    await add.delay(1, 1, countdown=1)
    job = await add.delay(a=2, b=2, countdown=1)
    result = await job.result(timeout=5)
    print(result.result)


if __name__ == "__main__":
    asyncio.run(main())
