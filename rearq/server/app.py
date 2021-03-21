from fastapi import FastAPI
from starlette.staticfiles import StaticFiles

from .routes import api_router, view_router

app = FastAPI(title="API docs of rearq")
app.mount("/static", StaticFiles(directory="rearq/server/static"), name="static")
app.include_router(view_router)
app.include_router(api_router, prefix="/api")
