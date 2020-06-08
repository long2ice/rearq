from rearq import ReArq

rearq = ReArq()


@rearq.on_shutdown
async def on_shutdown(r: ReArq):
    print("shutdown")


@rearq.on_startup
async def on_startup(r: ReArq):
    print("startup")
