import os

from fastapi import FastAPI
from starlette.staticfiles import StaticFiles

from .. import constants
from .routes import router

app = FastAPI(title="API docs of rearq")
app.mount(
    "/static",
    StaticFiles(directory=os.path.join(constants.BASE_DIR, "rearq", "server", "static")),
    name="static",
)
app.include_router(router)
