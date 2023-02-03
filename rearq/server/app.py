import asyncio
import os

from fastapi import FastAPI, HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.staticfiles import StaticFiles

from rearq import ReArq, constants
from rearq.server.routes import router
from rearq.worker import TimerWorker, Worker


class App(FastAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rearq = None

    def set_rearq(self, rearq: ReArq):
        self.rearq = rearq

    async def start_worker(self, with_timer=False, block=True):
        w = Worker(rearq=self.rearq)
        if with_timer:
            t = TimerWorker(rearq=self.rearq)
            runner = asyncio.gather(w.run(), t.run())
        else:
            runner = w.run()
        if block:
            await runner
        else:
            asyncio.ensure_future(runner)


app = App(title="API docs of ReArq")
app.mount(
    "/static",
    StaticFiles(directory=os.path.join(constants.STATIC_DIR, "rearq", "server", "static")),
    name="static",
)
app.include_router(router)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"msg": exc.detail},
    )
