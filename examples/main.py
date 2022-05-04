import asyncio

from examples.tasks import add, rearq


async def main():
    await rearq.startup()
    await add.delay(1, 1, countdown=5)
    await add.delay(a=2, b=2, countdown=5)


if __name__ == "__main__":
    asyncio.run(main())
