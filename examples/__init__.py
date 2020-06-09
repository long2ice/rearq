from rearq import ReArq

rearq = ReArq()


@rearq.on_shutdown
async def on_shutdown():
    print("shutdown")


@rearq.on_startup
async def on_startup():
    print("startup")
